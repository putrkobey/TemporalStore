// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAGE BIT ANSWERS THE DIRTY-BUCKET QUESTION, so the object-keyed set need not.
//!
//! # WHAT THIS IS FOR
//!
//! The design being followed keys objects directly and keeps no per-object dirty set. Going there
//! has one blocker: `object_manager_stats` walks `dirty_objects` to find buckets that hold a dirty
//! object's *blocks* while not being that object's *routing* bucket. Every other reader of the set
//! wants dirty BUCKETS, which `BucketNode` already records itself.
//!
//! **The replacement is already in the sibling arm of that same function** — the one taken when the
//! object lookup is empty:
//!
//! ```text
//!   bucket.dirty() || bucket.block_index.values().any(|page| page.dirty)
//! ```
//!
//! and it is exact rather than approximate, because `push_object_dirty_onto_its_existing_blocks`
//! runs at **both** sites that mark an object dirty. The invariant is already pinned elsewhere in
//! these words: *a mark that does not reach its pages leaves the bucket reading clean.* So every
//! page of a dirty object carries `page.dirty`, and every bucket holding one of those pages has a
//! dirty page.
//!
//! # BUT THAT IS AN ARGUMENT, AND AN ARGUMENT IS NOT A MEASUREMENT
//!
//! This computes the dirty-bucket set **both ways** on the same shard and asserts they agree. The
//! fixture puts an object's blocks in buckets other than its routing bucket, because that is the
//! only case where the two could differ — a fixture where every object sits alone in its routing
//! bucket would agree trivially and prove nothing.
//!
//! Deleting the object-keyed set is safe exactly as far as this holds, and if it ever stops
//! holding, this fails with both sets printed rather than the removal being found later as a
//! bucket that stopped being dumped.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::BTreeSet;

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;

/// The dirty buckets, found the way the object-keyed set finds them.
fn by_object_set(engine: &crate::TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let mut buckets: BTreeSet<u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(id, bucket)| bucket.dirty().then_some(*id))
        .collect();
    for object_key in shard.dirty_objects.iter() {
        buckets.extend(crate::engine::bucket_index_target_buckets_for_object_key(
            shard, object_key,
        ));
    }
    buckets
}

/// The dirty buckets, found the way the sibling arm finds them: the bucket's own bit, or any page
/// of it carrying one.
fn by_block_bit(engine: &crate::TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(id, bucket)| {
