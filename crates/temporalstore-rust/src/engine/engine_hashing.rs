// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Stable object/page hashing + routing-bucket helpers, split from engine.rs.
use super::*;

pub(super) fn routing_bucket_count(start_routing_bucket: u32, end_routing_bucket: u32) -> u32 {
    if end_routing_bucket < start_routing_bucket {
        return 0;
    }
    end_routing_bucket
        .saturating_sub(start_routing_bucket)
        .saturating_add(1)
}

pub(super) fn bucket_for_object(key: &str, start_routing_bucket: u32, routing_bucket_count: u32) -> u32 {
    note_routing_bucket_derivation(key);
    if routing_bucket_count == 0 {
        return start_routing_bucket;
    }
    start_routing_bucket + (stable_object_hash(key) % routing_bucket_count as u64) as u32
}

/// WHAT DERIVING A PAGE'S BUCKET COSTS, COUNTED, AND ONLY UNDER TEST.
///
/// The read path used to read a page's bucket off its `BlockAddress` and now derives it from the
/// object key, so what that costs is a number this tree owes rather than an argument. It is a COUNT
/// and not a timing, because a timing on a shared box moves with whatever is building next door.
///
/// `#[cfg(test)]` DELIBERATELY, which is the one place these counters differ from
/// `BLOCK_LOOKUP_ENTRIES_EXAMINED`. That one is charged in production because a page lookup is a walk
/// whose length an operator may need to see. This is one FNV-1a pass over a short borrowed string,
/// and two relaxed atomics per read to measure it would cost more than the thing being measured --
/// which is itself the answer the counters exist to report. A count does not vary with the build
/// profile, so measuring it in a test build measures the shipped number.
#[cfg(test)]
pub(super) static ROUTING_BUCKET_DERIVATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// The bytes FNV-1a was walked over, summed. This is the work: the hash is one xor and one multiply
/// per byte of key, so the byte total IS the instruction count up to a constant.
#[cfg(test)]
pub(super) static ROUTING_BUCKET_KEY_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn note_routing_bucket_derivation(key: &str) {
    ROUTING_BUCKET_DERIVATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ROUTING_BUCKET_KEY_BYTES.fetch_add(key.len() as u64, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_routing_bucket_derivation(_key: &str) {}

#[cfg(test)]
pub(super) fn reset_routing_bucket_derivations() {
    ROUTING_BUCKET_DERIVATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
    ROUTING_BUCKET_KEY_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn routing_bucket_derivations() -> (u64, u64) {
    (
        ROUTING_BUCKET_DERIVATIONS.load(std::sync::atomic::Ordering::Relaxed),
        ROUTING_BUCKET_KEY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
