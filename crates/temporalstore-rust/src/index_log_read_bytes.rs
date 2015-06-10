// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the index-log store READS, against what it says it read.
//!
//! #1936 took a restore's total from the kernel and subtracted the readers the engine names.
//! 965,415,651 bytes -- 95% of that restore -- belonged to a reader with no counter on it, while
//! every in-engine counter reported the restore as perfectly flat. It fixed the largest one and
//! NAMED this one: the index-log store reads 8,119,256 bytes bringing back a 200,000-record
//! shard, in 1,069 reads, against an `index_log_store().stats()` answering `bytes_read: 0`.
//!
//! Reproduced here before anything was changed, at 2,000 and 8,000 records:
//!
//! ```text
//!                                     2,000 records   8,000 records    ratio
//!   pieces                                       20              80    4.000
//!   records the fold handed back              2,000           8,000    4.000
//!   the log on disk                         163,236         655,236    4.014
//!   what the STORE said it read                   0               0       --
//!   what the KERNEL charged                 163,236         655,236    4.014
//!   UNATTRIBUTED                            163,236         655,236    4.014
//! ```
//!
//! ALL of it. Not 0.8% -- at this shape the fold's whole reading was unattributed, because the
//! fold fed no counter at all. A READER WITH NO COUNTER CONTRIBUTES ZERO TO EVERY REPORT, WHICH
//! IS INDISTINGUISHABLE FROM A READER DOING NO WORK, and that is how a 95% blind spot survived
//! every previous measurement of this path.
//!
//! Every number here is a COUNT, never a duration: this box has measured the same round at 490 ms
//! and at 1,077 ms on two afternoons, and a count does not move with the neighbours.

#![cfg(test)]

use super::*;

/// Two sizes and the ratio between them. Four times the log, so a quantity flat per record and
/// one that grows with the log are four times apart and cannot be confused.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;

/// Rolled rather than one piece, so the fold enumerates and opens tens of pieces and the counter
/// is charged across piece boundaries rather than once.
const SEGMENT_BYTES: u64 = 8 * 1024;

const SHARD: ShardId = 21;

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite running tests beside each other charges this
// span whatever another thread happened to read -- it once charged an operation 3,194 bytes a
// background thread wrote. `/proc/thread-self/io` is this thread, and a fold runs on one.
//
// Reading the counter is itself a read the kernel charges, so a reading carries the LENGTH of the
// text it came from and the span subtracts it exactly rather than estimating it. That is what
// makes "exactly" available at the end instead of "about".
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_thread_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let mut rchar = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            rchar = rest.trim().parse().ok();
        }
    }
    Some(IoReading {
        rchar: rchar?,
        self_bytes: text.len() as u64,
    })
}

/// What the kernel says this THREAD read across a span, with the instrument's own reads removed.
///
/// `None` when `/proc` said nothing, so "the kernel was not counting" can never be read as "the
/// kernel counted zero".
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        // The opening reading's own bytes fall inside the span; the closing reading's are charged
        // after it is taken.
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE FIXTURE
// ---------------------------------------------------------------------------------------------

fn small_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.into(),
        model_id: "m".to_string(),
        object_id: 1,
        deleted: false,
        // ADDRESS-LESS BY DESIGN: this item is the smallest a delta record can carry,
        // and an address would make it bigger rather than smaller. The `size` it used
        // to state was never restored on a row with no address to derive it from.
        entry: None,
    }
}

/// Set the rolling threshold for THIS THREAD and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// A log of `records` anchored deltas whose anchors CLIMB one per record.
fn build_log(dir: &std::path::Path, records: usize) {
    let store = LocalIndexLogStore::new(dir);
    for value in 0..records {
        store
            .append_delta(
                SHARD,
                vec![small_item(
                    (value % 64) as u32,
                    // Zero-padded to eight digits at BOTH corpus sizes, so the only field that
                    // differs between the two fixtures is the one being varied.
                    &format!("tenant/1/object/{value:08}"),
                )],
                Vec::new(),
                Some(value as u64 + 1),
                None,
                false,
                false,
            )
            .unwrap();
    }
}

/// On-disk bytes of every piece the fold will open, from `stat` and not from the fold.
///
/// A THIRD row, independent of both the kernel's and the counter's: with nothing declined, a fold
/// that reads each piece exactly once must read exactly this.
fn log_bytes_on_disk(dir: &std::path::Path) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut pieces = 0usize;
    for path in index_log_segment_paths(dir, SHARD) {
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        pieces += 1;
        bytes = bytes.saturating_add(metadata.len());
    }
    (bytes, pieces)
}

/// What one load-path fold of a whole log cost.
#[derive(Debug)]
struct Fold {
    pieces: usize,
    /// Every piece's size from `stat`: what a fold reading each piece once must read.
    on_disk: u64,
    /// Records the fold handed back.
    applied: usize,
    /// What the STORE's record-facing row says -- the one that reported zero.
    store_bytes: u64,
    /// What the counter INSIDE the read says.
    counted_bytes: u64,
    counted_reads: u64,
    /// What the KERNEL charged this thread across the same span -- the independent total.
    kernel_rchar: Option<u64>,
}

impl Fold {
    /// The kernel's number minus the reader that is now counted. Nothing in this engine feeds
    /// `rchar`, so this CAN be wrong and can be SEEN to be wrong -- which is the property a
    /// residual summed from the rows it audits does not have.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.counted_bytes as i64)
    }
}

