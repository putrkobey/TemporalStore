// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE DIRTY SHARE IS NOT A CONSTANT, AND THE ONE READING #1995 PRICED IS THE WORST POINT ON ITS
//! CURVE.
//!
//! #1995 refuted hoisting `first_dirty_wal_sequence` and `first_dirty_index_log_sequence` into a
//! side map keyed by bucket id, on a measured dirty share of **99.02% and 100.00%** at the shipped
//! `0..1023` range. At that share a map holding one entry per dirty bucket costs more than the
//! sixteen bytes it takes off every node, so it loses.
//!
//! THE FIXTURE DID DUMP, so the obvious objection does not apply. `one_round_at_the_configured_cap`
//! sits between the two write phases of
//! `bucket_sequence_budget::what_hoisting_the_two_transient_claims_would_cost_at_two_corpus_sizes`,
//! and that is worth stating plainly because a never-dumped store would have made the whole figure
//! an artefact of the same kind #1959 was caught by.
//!
//! BUT IT DUMPED ONCE, AT A CAP OF SIXTY-FOUR, OVER ROUGHLY A THOUSAND BUCKETS. `CONFIGURED_DUMP_CAP`
//! is 64 and the shipped range fills about 1,012 of 1,024 buckets at four thousand records, so ONE
//! round can reach about **6.3%** of the store. The fixture then writes the second half of its
//! corpus, re-dirtying what it just cleaned. The reading is taken immediately after that burst.
//!
//! So 99-100% is neither an artefact nor a steady state. It is the dirty share **one capped round
//! into a corpus that needs about sixteen of them**, measured at the moment a write burst has just
//! finished. That is the worst point on the curve, and nothing in #1995 says where the rest of the
//! curve is -- which matters, because a structure that is DRAINED when a bucket is dumped holds only
//! the buckets dirty SINCE THE LAST DUMP, and that is a different set from "every dirty bucket".
//!
//! THIS MODULE MEASURES THE DRAINING CURVE, as a series over rounds rather than one reading, at
//! both corpus sizes and both ranges, and prices three shapes against it at every point. What it
//! does NOT establish is the share a store under CONTINUOUS write load settles at -- that arm was
//! built, could not be believed, and is recorded in the body as an open question. So this module
//! says the 99-100% figure is the undrained state and not a constant; it does not by itself say the
//! side structure wins:
//!
//!   1. **today** -- two `u64` on every node, 16 B x buckets, no allocation;
//!   2. **a drained side structure** keyed by bucket id, holding an entry only while a bucket is
//!      dirty, popped when it is dumped;
//!   3. **one global minimum** -- 16 bytes for the whole shard.
//!
//! WHAT THE THIRD ARM COSTS THAT IS NOT BYTES, stated here because the arithmetic will make it look
//! free: a single minimum gives the reclaim FLOOR correctly -- that floor is the minimum of these
//! claims either way -- but it cannot give the dump ORDER, and `first_dirty_rank` is a sort key over
//! N buckets. `part4::dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id`
//! is the guard that fails if the order is lost, and `engine_storage_state.rs` states the trade in the field's own
//! doc. The arm is priced because it was asked for; it is not recommended by this module.
//!
//! AND ONE CORRECTION TO THE TARGET, because it changes 24 bytes into 16. `dirty_generation` cannot
//! join a structure that starts empty after a load. It is DURABLE -- the node's deserializer refuses
//! a node without it (`missing_field("dirty_generation")`) -- and it is a per-bucket IDENTITY
//! compared against the copy stored in a dump manifest (`storage_reporting.rs:858`), which is why
//! `storage_bucket_internals.rs:1336` preserves it across a rebuild rather than defaulting it. A
//! transient side structure that is empty after a load would silently answer 0 for every bucket and
//! make every stored manifest appear to match. Only the two TRANSIENT claims are movable, and they
//! are 8 bytes each.
//!
//! THE CONTROL IS THE ROUND-ZERO READING at the default range, where the change predicts nothing:
//! a key lands in a bucket of its own there, so the dirty share and the bucket count move together
//! and the side structure holds one entry per bucket whatever the dump does. Reported at its own
//! row rather than argued.

#![allow(clippy::all)]
use super::*;

use crate::engine::state::BucketNode;
use std::mem::size_of;

const SHARE_SHARD: ShardId = 1;

/// The documented production routing range, `TS_SHARD_END_ROUTING_BUCKET=1023`.
const SHARE_END_BUCKET: u32 = 1023;

/// The cap #1995 measured at, and the one an operator is told to configure.
const SHARE_DUMP_CAP: usize = 64;

/// How many capped rounds to run before giving up on draining.
const MAX_ROUNDS: usize = 12;

/// The two transient claims, which are the only movable ones. `dirty_generation` is durable.
const MOVABLE_BYTES_PER_NODE: usize = 16;

fn share_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("share-cache"),
        dir.join("share-blocks"),
        dir.join("share-indexes"),
    )
}

fn share_load(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: SHARE_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

fn one_capped_round(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SHARE_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: SHARE_DUMP_CAP,
        ..StorageManagerCycleRequest::default()
    });
}

/// (buckets, dirty buckets, buckets holding a non-zero WAL claim).
fn dirty_census(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARE_SHARD).expect("shard is loaded");
    let mut buckets = 0usize;
    let mut dirty = 0usize;
    let mut claiming = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        buckets += 1;
        if bucket.dirty() {
            dirty += 1;
        }
        if bucket.first_dirty_wal_sequence > 0 {
            claiming += 1;
        }
    }
    (buckets, dirty, claiming)
}

fn write_corpus(engine: &TemporalEngine, keys: &[String], tag: u8) {
    for chunk in keys.chunks(512) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet { key: key.clone(), value: vec![tag; 48] })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARE_SHARD,
            commands,
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// WHAT THE DIRTY SHARE DOES AS A DUMP DRAINS IT, AND WHAT EACH OF THREE SHAPES COSTS ALONG THE WAY.
///
