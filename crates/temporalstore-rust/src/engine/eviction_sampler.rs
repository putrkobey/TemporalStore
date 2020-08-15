// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded victim selection for eviction.
//!
//! # Why this exists
//!
//! The straightforward way to pick eviction victims is to enumerate every bucket, sort by
//! recency and take the coldest `batch_limit`. That is what [`super::TemporalEngine`] does when
//! this sampler is off, and its cost grows with the size of the store rather than with the
//! number of victims wanted: building the candidate list walks every live page in the shard
//! (cloning two strings per page), and the sort is `O(buckets log buckets)`. Eviction runs on a
//! background loop, so a large shard pays that repeatedly to choose a handful of victims.
//!
//! This module bounds the work instead. Each pass scans at most
//! `samples * batch_limit * scan_turns` buckets starting from where the previous pass stopped,
//! and keeps what it saw in a candidate pool that survives across passes. Cold buckets
//! therefore accumulate in the pool over successive passes rather than being rediscovered from
//! scratch each time, and a pass costs the same whether the shard holds a thousand buckets or a
//! million.
//!
//! # What it gives up
//!
//! Sampling sees part of the store, so the victims are the coldest buckets *found*, not
//! provably the coldest that exist. That is the intended trade: eviction only needs victims
//! that are cold enough, and the pool plus the resuming cursor mean a genuinely cold bucket is
//! found within a bounded number of passes.
//!
//! # The window is scan-ordered, and that is why a pass does not stop on pool SIZE
//!
//! The scan walks ascending routing buckets from the cursor, so the buckets a pass meets first
//! are the lowest-numbered ones -- a position in the key space, unrelated to how cold a bucket
//! is. A pass that stopped as soon as it held `batch_limit` CANDIDATES would therefore hand the
//! ranking a window chosen by bucket id: when the head of that window is in use, every candidate
//! in it is warm, the ranking faithfully returns the coldest of them, and eviction takes a bucket
//! that is being read while colder buckets sit one scan step past where the pass stopped.
//! Measured on a 4,000-key shard whose 160 lowest-numbered buckets were read just before the
//! round: all 16 victims came out of that band.
//!
//! So the stop reads the pool for candidates that are actually COLD -- not used since the
//! previous pass -- and keeps walking while it holds fewer of those than the batch wants. The
//! ceiling is unchanged and is the one `scan_turns` already sets: a pass still walks at most
//! `samples * batch_limit * scan_turns` buckets and then ranks whatever it has, so a store that
//! is warm everywhere still yields a full batch instead of stalling. The extra walking happens
//! only in the regime that needs it, which is the same reason `scan_turns` exists -- it was
//! already the headroom for a window that is mostly INELIGIBLE, and a window that is mostly IN
//! USE is the same problem with a different predicate. Selection here is pure recency, which is also why
//! it does not need the per-bucket byte weights the full scan computes -- those are needed only
//! for the buckets actually chosen.
//!
//! # Staleness
//!
//! Pool entries can be several passes old, so a bucket in the pool may have been touched,
//! emptied, or dropped since it was seen. Every entry is therefore re-validated against current
//! state at selection time and its recency re-read; a stale entry is discarded rather than
//! evicted. Skipping that step would evict buckets that had since become hot.

use std::collections::HashMap;

/// Where the sampler reads bucket state from.
///
/// This is a trait rather than a slice so a caller backed by an ordered map can walk only the
/// scan window. Materializing every bucket to pick a handful of victims would reintroduce the
/// cost this module exists to remove.
pub(super) trait BucketSource {
    /// Total buckets available, used to tell "covered the store" from "ran out of budget".
    fn bucket_count(&self) -> usize;

    /// Visit buckets in ascending order from `cursor`, wrapping, until `budget` are seen or
    /// `visit` returns false.
    fn scan(
        &self,
        cursor: Option<u32>,
        budget: usize,
        visit: &mut dyn FnMut(&BucketSample) -> bool,
    ) -> ScanResult;

    /// Current state of one bucket, for re-validating a pool entry.
    fn lookup(&self, routing_bucket: u32) -> Option<BucketSample>;
}

/// Where a scan stopped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanResult {
    pub(super) scanned: usize,
    /// Bucket to resume at, or `None` if the whole store was covered.
    pub(super) next_cursor: Option<u32>,
    pub(super) wrapped: bool,
}

/// Slice-backed source, used by tests and by callers holding an ordered vector.
impl BucketSource for &[BucketSample] {
    fn bucket_count(&self) -> usize {
        self.len()
    }

    fn scan(
        &self,
        cursor: Option<u32>,
        budget: usize,
        visit: &mut dyn FnMut(&BucketSample) -> bool,
    ) -> ScanResult {
        if self.is_empty() || budget == 0 {
            return ScanResult::default();
        }
        let start = match cursor {
            Some(cursor) => self
                .iter()
                .position(|sample| sample.routing_bucket >= cursor)
                .unwrap_or(0),
            None => 0,
        };
        let mut scanned = 0usize;
        let mut wrapped = false;
        let mut index = start;
        loop {
            if !visit(&self[index]) {
                scanned += 1;
                index = (index + 1) % self.len();
                break;
            }
            scanned += 1;
            index += 1;
            if index >= self.len() {
                index = 0;
                wrapped = true;
            }
            if scanned >= budget || scanned >= self.len() {
                if scanned >= self.len() {
                    wrapped = true;
                }
                break;
            }
        }
        ScanResult {
            scanned,
            next_cursor: if wrapped && scanned >= self.len() {
                None
            } else {
                Some(self[index].routing_bucket)
            },
            wrapped,
        }
    }

    fn lookup(&self, routing_bucket: u32) -> Option<BucketSample> {
        self.iter()
            .find(|sample| sample.routing_bucket == routing_bucket)
            .copied()
    }
}

/// Tuning for a sampled pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictionSamplerConfig {
    /// Buckets to look at per wanted victim. Higher means better victims and more work.
    pub(crate) samples: usize,
    /// Upper bound on candidates carried between passes.
    pub(crate) pool_size: usize,
    /// Multiplier on the per-pass scan budget, bounding how far a pass may walk when most of
    /// what it sees is ineligible.
    pub(crate) scan_turns: usize,
}

impl Default for EvictionSamplerConfig {
    fn default() -> Self {
        Self {
            samples: 5,
            pool_size: 64,
            scan_turns: 4,
        }
    }
}

/// State carried between passes: where to resume, and the candidates seen so far.
#[derive(Debug, Default, Clone)]
pub(super) struct EvictionSamplerState {
    /// Bucket to resume the next scan at. `None` restarts from the beginning.
    pub(super) cursor: Option<u32>,
    /// Candidate buckets and the recency they carried when last observed.
    pub(super) pool: HashMap<u32, u64>,
    /// Wall-clock ms of the previous pass, which is the line between "cold" and "in use".
    ///
    /// A bucket is cold to this sampler when nothing has touched it since the last time victims
    /// were picked. That is a RELATIVE test and it has to be: recency carries no absolute scale,
    /// so the only thing a pass can say about a candidate is whether it has been used since the
    /// last one looked.
    ///
    /// Zero means no pass has run yet. The floor then falls back to "never touched at all",
    /// which is the one reading that is cold without a previous pass to compare against; a first
    /// pass on a shard whose buckets have all been written therefore finds no cold candidate,
    /// walks its whole budget, and ranks what it saw -- the widest window the bound allows, which
    /// is the right answer for the one pass that has no history.
    ///
    /// Ephemeral with the rest of this state: a reload starts the floor at zero again and the
    /// first pass after it pays one full budget.
    pub(super) last_pass_ms: u64,
}

/// What the sampler needs to know about a bucket. Deliberately not the bucket itself, so the
/// selection logic stays testable without building engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BucketSample {
    pub(super) routing_bucket: u32,
    /// Holds live objects in memory, so evicting it would actually free something.
    pub(super) eligible: bool,
    /// Wall-clock ms of the last access. Zero means never touched, which is coldest.
    pub(super) last_used_ms: u64,
}

/// Outcome of one pass, including the counters that make the bound observable.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
