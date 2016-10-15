// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MUTATION COUNTER AND THE DIRTY CONTAINER COUNT DIFFERENT THINGS, and the dump cadence
//! already reads the counter.
//!
//! # WHAT THIS IS FOR
//!
//! Dirty bookkeeping in this store is moving toward scalars. Three questions are asked of it, and
//! this measures which of them a scalar already answers, because two of the three turn out to be
//! answered by one already and the third is answered by neither.
//!
//!     is anything dirty          `BucketNode::dirty`, one record per bucket
//!     how much since the dump    `undumped_objects_by_shard`, one u64 per shard in the log
//!     WHICH objects are dirty    `dirty_objects`, a container of keys
//!
//! The first moved to the node's own flag (#2338, #2340) and the presence question now has ONE
//! record. The second is already a scalar: the log keeps a per-shard count of object mutations
//! appended since the last dump, reset only once a dump manifest is durably written, and the dump
//! cadence is a predicate over it and two sequence numbers -- no container is consulted. That is
//! pinned below rather than read off the code.
//!
//! THE THIRD IS THE OPEN ONE, and the point of this measurement is that the second cannot stand in
//! for it. A count of MUTATIONS and a count of DISTINCT DIRTY OBJECTS are different numbers, by
//! whatever factor the writer batches -- so replacing the container's count with the mutation
//! counter is not a substitution of one spelling for another, it is a change of question. The
//! factor is measured here on a fixture built to separate them, rather than left as the remark at
//! `StorageLifecyclePlan::undumped_wal_objects` that the two "disagree by whatever factor the
//! writer batches".
//!
//! # WHY THIS IS NOT THE COUNT BEING DERIVED
//!
//! `whether_a_bucket_can_count_its_own_dirty_objects` measures the per-bucket dirty-object count to
//! be NOT derivable -- a page walk finds 3 of 11 marks at 17x the work. Nothing here derives it.
//! These are two independently maintained counters being compared so that the difference between
//! what they count is a figure, which is what a later change replacing one with the other would
//! need before it could be priced.
//!
//! rust-internal: reads the engine's own index structures and log counters, no product behaviour

use std::collections::BTreeSet;

use crate::types::{Command, ExecuteRequest};

/// Few keys, many writes each, so a count of mutations and a count of objects cannot coincide.
const DISTINCT_KEYS: usize = 6;
const WRITES_PER_KEY: usize = 9;
const END_ROUTING_BUCKET: u32 = 15;

#[test]
fn what_the_mutation_counter_and_the_dirty_container_each_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "mutation-counter".to_string(),
        shard_uri: "local://mutation-counter/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1: {:?}",
        loaded.status
    );

    // ---- OVERWRITE THE SAME FEW KEYS REPEATEDLY. A fixture that writes each key ONCE makes a
    // ---- mutation count and a distinct-object count the same number, and then this test cannot
    // ---- tell one from the other -- which is the whole question.
    for round in 0..WRITES_PER_KEY {
        for index in 0..DISTINCT_KEYS {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("mutation-counter-{index:04}"),
                    value: vec![b'a' + (round as u8 % 26); 96],
                },
            });
            assert!(out.status.ok, "write {index}/{round}: {:?}", out.status);
        }
    }

    // ---- THE THREE FIGURES, each read from the thing that owns it.
    let plan = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let mutations = plan.undumped_wal_objects;
    let log_records = plan.undumped_wal_records;
    let (distinct_dirty_objects, dirty_buckets, bucket_total) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let dirty: BTreeSet<u32> = shard
            .bucket_index
            .bucket_map
            .iter()
            .filter(|(_bucket, node)| node.dirty())
            .map(|(bucket, _node)| *bucket)
