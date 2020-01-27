// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH OF THE TWO DIRTY RECORDS EACH PRESENCE CONSUMER READS -- forced apart so the answer is
//! observable.
//!
//! # WHAT THIS IS FOR
//!
//! Three consumers asked `BucketStorageSummary::dirty_object_count > 0`, which is a dirty-BUCKET
//! question answered from a count of dirty OBJECTS. The bucket records that fact itself, on its
//! own node, and `whether_a_bucket_can_count_its_own_dirty_objects` measures the two records
//! agreeing on every bucket in both directions -- including the ones carrying dirt no page walk
//! can see -- and measures the node's flag to be an INDEPENDENT record rather than a cache of the
//! set, by neutralising each of its three maintainers in turn.
//!
//! That agreement is what makes the move safe. It is also what makes the move INVISIBLE to an
//! ordinary test: while the two records agree on every bucket, a consumer reading either one
//! satisfies any assertion that compares the consumer against the truth. So this test FORCES THEM
//! APART and asks each consumer again.
//!
//! The divergence is planted in the FIXTURE, never in the code under test. Two buckets are picked
//! out of the live shard and their nodes' flags assigned directly:
//!
//!     forced_dirty   node reads dirty, the object-keyed set holds NOTHING for it
//!     forced_clean   node reads clean, the object-keyed set holds a mark for it
//!
//! Each is a state the two records cannot both describe, so every consumer now reports one answer
//! or the other and cannot report both. That is only legitimate because the two records are
//! independent -- a flag derived from the set could not be held apart from it, and the measurement
//! above is what establishes that it is not.
//!
//! # THE THREE CONSUMERS, AND WHAT EACH ONE'S ERROR COSTS
//!
//!     storage_lifecycle_plan          dump selection      an extra bucket is dumped
//!     storage_recovery_boundary       which buckets the newest dump does not cover
//!     storage_physical_index_report   a per-bucket report flag
//!
//! None of the three decides durability. Dump selection over-reporting costs a dump; the other two
//! are report fields. The two WAL-reclaim checks are NOT here and do not move with them: there
//! `dirty_object_count == 0` is the arm that counts a bucket as covered and contributing no log
//! floor, so a bucket wrongly reading dirty skips that arm, cannot name a claim because its
//! `first_dirty_wal_sequence` was cleared alongside `dirty = false`, and blocks the whole reclaim
//! plan. That move needs the over-reporting direction measured empty on a shard that has actually
//! dumped, which is a different experiment from this one.
//!
//! # AND ONE OF THE THREE WAS ALREADY DEAD, WHICH IS MEASURED HERE RATHER THAN ASSUMED
//!
//! `storage_physical_index_report` built each row with `dirty: summary.dirty_object_count > 0` and
//! then, in a LATER loop over the same `bucket_map`, assigned `bucket.dirty = node.dirty()` over
//! the top of it. So the count reached that field only for a bucket the summaries name and
//! `bucket_map` does not -- and such a bucket has no node for a mark to create, so the set loop
//! contributes no count to it and the initializer could only ever have written `false`. The field
//! has been reading the node all along. That is asserted below as the unreachability it rests on,
//! so the claim is a property of the code rather than a reading of it, and the change at that site
//! is a dead initializer stopping naming a figure it never passed on.
//!
//! rust-internal: reads and writes the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Every bucket's two records of the same fact: `(node.dirty(), set count)`.
fn both_records(engine: &crate::TemporalEngine) -> BTreeMap<u32, (bool, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                (bucket.dirty(), shard.dirty_objects.bucket_count(*routing_bucket)),
            )
        })
        .collect()
}

#[test]
fn which_record_the_dirty_bucket_consumers_read() {
    // ---- A NARROW ROUTING RANGE, so keys SHARE buckets and the removals' non-page-backed records
    // ---- route to buckets of their own. On the default whole-keyspace range every key gets a
    // ---- bucket to itself and the populations stop overlapping.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
