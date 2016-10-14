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
