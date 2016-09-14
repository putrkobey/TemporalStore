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
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "which-dirty-record".to_string(),
        shard_uri: "local://which-dirty-record/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1 on 0..{END_ROUTING_BUCKET}: {:?}",
        loaded.status
    );

    for index in 0..KEYS {
        let key = format!("which-record-{index:04}");
        for command in [
            Command::StringSet { key: key.clone(), value: vec![b'v'; 96] },
            Command::HashSet {
                key: key.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 96],
            },
            Command::SetAdd { key: key.clone(), member: vec![b's'; 96] },
        ] {
            let out = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(out.status.ok, "fixture write {index}: {:?}", out.status);
        }
    }

    // ---- DUMP CLEAN FIRST, then dirty a FEW, so most buckets are clean and there is somewhere to
    // ---- plant a node-dirty bucket the set knows nothing about.
    let _dumped = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("which-record-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }
    for index in [2usize, 6] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("which-record-{index:04}") },
        });
        assert!(out.status.ok, "removing {index}: {:?}", out.status);
    }

    // ---- THEN A DUMP OF ONE BUCKET, so the NEWEST manifest names a PROPER SUBSET of the dirty
    // ---- buckets. Without this the newest manifest names every bucket the first dump covered,
    // ---- the dump-coverage report is empty whichever record it reads, and the clause below would
    // ---- be asserting nothing. A partial dump is also the shape `max_dump_buckets_per_round`
    // ---- asks for in production, so this is the ordinary case rather than a contrived one.
    let one_dirty_bucket = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .find(|(_routing_bucket, bucket)| bucket.dirty())
            .map(|(routing_bucket, _bucket)| *routing_bucket)
            .expect("the writes above left at least one bucket dirty")
    };
    let _partial = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![one_dirty_bucket],
        ..Default::default()
    });

    // ---- WHAT THE SHARD LOOKS LIKE BEFORE ANYTHING IS FORCED, which is the state production
    // ---- reaches and the state in which the two records agree.
    let natural = both_records(&engine);
    let naturally_dirty: BTreeSet<u32> = natural
        .iter()
        .filter(|(_bucket, (node_dirty, _count))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    let naturally_counted: BTreeSet<u32> = natural
        .iter()
        .filter(|(_bucket, (_node_dirty, count))| *count > 0)
        .map(|(bucket, _)| *bucket)
        .collect();

    // ---- THE TWO BUCKETS TO FORCE APART, chosen from the live shard rather than named as
    // ---- literals, so the fixture does not depend on where this routing range happens to put a
    // ---- key.
    let forced_dirty = natural
        .iter()
        .find(|(_bucket, (node_dirty, count))| !*node_dirty && *count == 0)
        .map(|(bucket, _)| *bucket);
    let forced_clean = natural
        .iter()
        .find(|(_bucket, (node_dirty, count))| *node_dirty && *count > 0)
        .map(|(bucket, _)| *bucket);

    println!();
    println!("  {KEYS} keys on a {}-bucket routing range", END_ROUTING_BUCKET + 1);
    println!();
    println!("    buckets in the map                      {:>6}", natural.len());
    println!("    node reads dirty                        {:>6}", naturally_dirty.len());
    println!("    the set holds a mark                     {:>6}", naturally_counted.len());
    println!("    the two records agree on every bucket    {:>6}", naturally_dirty == naturally_counted);
    println!();
    println!("    bucket to force DIRTY (set holds none)  {forced_dirty:>6?}");
    println!("    bucket to force CLEAN (set holds a mark){forced_clean:>6?}");
    println!();

    // ---- THE FIXTURE HAS TO REACH THE REGIME. Each clause stands alone so a failure says which
    // ---- one did not hold, and all of them come before any conclusion: a plant stops at the
    // ---- first failing assertion, so a conclusion placed above a regime clause it overlaps would
    // ---- hide it.
    assert!(
        natural.len() < KEYS,
        "[clause: regime_shared_buckets] {KEYS} keys landed in {} buckets, so each key may still have a bucket to itself and the \
         shared-bucket case never arose",
        natural.len()
    );
    assert_eq!(
        naturally_dirty, naturally_counted,
        "[clause: regime_records_agree] the node's flag and the object-keyed set DISAGREE on this shard before anything is \
         forced: node {naturally_dirty:?} against set {naturally_counted:?}. This test's whole \
         method is that they agree naturally and have to be forced apart; a natural disagreement \
         means the premise of the move has changed, and \
         `whether_a_bucket_can_count_its_own_dirty_objects` is the test that owns it"
    );
    assert!(
        !naturally_dirty.is_empty(),
        "[clause: regime_something_dirty] no bucket reads dirty, so nothing is dirty and every consumer below reports nothing \
         whichever record it reads"
    );
    assert!(
        naturally_dirty.len() < natural.len(),
        "[clause: regime_some_clean] all {} buckets read dirty, so there is no clean bucket to force dirty and a consumer that \
         always said \"every bucket\" would pass. The dump above was supposed to leave most of \
         them clean",
        natural.len()
    );
    let Some(forced_dirty) = forced_dirty else {
        panic!(
            "[clause: regime_room_to_force_dirty] every bucket in the map either reads dirty or holds a mark, so there is nowhere to \
             plant a node-dirty bucket the set knows nothing about and the OVER-reporting \
             direction cannot be put to any consumer"
        )
    };
    let Some(forced_clean) = forced_clean else {
        panic!(
            "[clause: regime_room_to_force_clean] no bucket both reads dirty and holds a mark, so there is nowhere to plant a \
             node-clean bucket the set still marks and the UNDER-reporting direction cannot be put \
             to any consumer"
        )
    };

    // ---- NOW FORCE THEM APART. Assigned straight onto the node, which is legitimate here only
    // ---- because the flag is an independent record: a flag derived from the set would be
    // ---- recomputed back into agreement and this plant would measure nothing.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        shard
            .bucket_index
            .bucket_map
            .get_mut(&forced_dirty)
            .expect("the bucket chosen above is in the map")
            .set_dirty(true);
        shard
