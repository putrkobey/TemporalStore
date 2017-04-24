// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The index-log retain floor never passes the durable frontier.
//!
//! WHY THIS EXISTS. Under the shipped default, the served-index delta record is NOT fsync'd on any
//! production path: both callers compute `!raft_applying() && !wal_single_barrier()`, and
//! `wal_single_barrier()` is `!wal_legacy_recovery()`, which reads an environment flag that is off
//! by default. So every single-node write leaves an unsynced delta tail, and what makes that safe is
//! that the WAL barrier has already committed and a lost tail is rebuilt by replay.
//!
//! That safety has a second half: the collector must not truncate such a tail. `append_delta`'s doc
//! asserts it in prose -- "the consumer-aware GC never truncates such a tail (it retains from the
//! durable dump/cursor/snapshot frontier)" -- and the implementation does honour it, at
//! `storage_lifecycle_methods.rs`:
//!
//!     let effective_index_log_frontier = cursor_index_log_floor
//!         .map_or(durable_index_log_frontier, |floor| durable_index_log_frontier.min(floor));
//!     let retain_from_index_log_sequence =
//!         if safe_to_reclaim { effective_index_log_frontier.saturating_add(1) } else { 0 };
//!
//! But nothing FAILS if that `min` is dropped, or if the floor is ever taken from the log's own last
//! sequence instead of the frontier. A prose invariant on a durability path with no guard behind it
//! is the shape this tree has been bitten by repeatedly, so this is the guard.
//!
//! WHAT IT ASSERTS, and what it deliberately does not. It asserts the ORDERING -- the floor never
//! exceeds the durable frontier -- rather than equality, because a cursor legitimately lowers it and
//! a floor below the frontier is the safe direction. It asserts the regime was reached, because a
//! plan that declined to reclaim satisfies the ordering trivially with a floor of zero and would
//! pass against a broken derivation.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 31;

/// Seed, read back, and take a dump so the plan has a durable frontier to derive from.
fn engine_with_a_durable_frontier(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);
    for i in 0..48usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("floor-{i}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "seed must ack at {i}: {response:?}");
    }
    engine
}

#[test]
fn the_index_log_retain_floor_never_passes_the_durable_frontier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_a_durable_frontier(dir.path());

    // A dump is what makes a bucket generation durable, which is what `safe_to_reclaim` needs.
    let _ = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: SHARD,
        ..Default::default()
    });

    // THEN WRITE MORE, and this is the whole fixture rather than a flourish.
    //
    // After the dump alone the durable frontier and the log's current sequence are the SAME number,
    // and a floor derived from the current sequence instead of the frontier would be identical — so
    // the assertions below would hold against exactly the derivation they exist to refuse. These
    // writes put delta records AFTER the durable frontier, which is the unsynced tail the invariant
    // protects, and the regime assertion below requires that separation to exist.
    for i in 48..72usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("floor-{i}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "tail write must ack at {i}: {response:?}");
    }

    let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
    println!(
        "safe_to_reclaim={} frontier={} retain_from={} current={}",
        plan.safe_to_reclaim,
        plan.durable_bucket_generation_frontier_index_log_sequence,
        plan.retain_from_index_log_sequence,
        plan.current_index_log_sequence,
    );

    // THE ORDERING, which is the invariant itself.
    assert!(
        plan.retain_from_index_log_sequence
            <= plan
