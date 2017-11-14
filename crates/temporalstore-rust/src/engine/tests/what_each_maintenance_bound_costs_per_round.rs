// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT EACH PER-ROUND BOUND COSTS A MAINTENANCE ROUND, AND THE ROUTE THAT ARRIVES WITH NONE.
//!
//! Seven per-round bounds and triggers on `StorageManagerCycleRequest` decide how much one
//! maintenance round does. `0` means NO BOUND in every one of them -- the expiry window walks
//! every due deadline, the eviction sampler keeps every victim it finds, the index-log sweep
//! removes every removable record, and both index-GC triggers fire whatever the log looks like.
//! A round holding zeros is therefore not inert, it is MAXIMALLY AGGRESSIVE, and setting a bound
//! makes each round do LESS. That is the trade measured here: cost per round against how many
//! rounds a backlog takes to drain.
//!
//! `StorageManagerCycleRequest::default()` sets all seven and
//! `every_maintenance_round_is_bounded_by_default` pins the values. THAT IS NOT THE ROUTE A
//! SERVER TAKES. Both cycle endpoints -- `POST /server/storage/manager/cycle` and
//! `POST /storage_manager/cycle` -- build the request with
//! `parse_json::<StorageManagerCycleRequest>(&request.body)`, and the seven carried a bare
//! `#[serde(default)]`. On a `usize` or a `u64` that decodes an ABSENT field to the FIELD TYPE's
//! default, which is 0 -- not to the STRUCT's `Default`, which is where the bounds live. An
//! absent field is what every caller who does not name one sends, so the shipped bounds reached
//! callers of `Default::default()` and no HTTP caller at all.
//!
//! The tree already knew this hazard class, for exactly one field beside them:
//! `omitting_the_commit_before_truncate_guard_still_commits_before_truncating` states it for
//! `index_gc_commit_dirty_buckets_before_truncation`, which is why that one bool carries
//! `#[serde(default = "...")]`. The reasoning was never carried across to the numbers.
//!
//! [`a_cycle_request_that_names_no_bound_still_arrives_bounded`] is the guard, and it asserts the
//! DECODE and not the constant: the whole decoded request must equal the shipped default, so a
//! bound that goes back to meaning "unbounded" on this route goes red whether or not anyone
//! remembers to add it to a list, and a bound added later is covered the day it is added.
//! Asserting `Default` alone -- which the existing guard does -- structurally cannot see this,
//! because `Default` was right the whole time.
//!
//! WHAT EACH BOUND MOVES is measured by [`each_shipped_bound_bites_and_the_round_does_less_work`]
//! on one seeded shard, the same fixture built twice and run with the seven at 0 and at the
//! shipped values. The table it prints is reproduced in the note on that test.
//!
//! Counted, not timed, for every claim that matters: the box these run on carries other tenants,
//! so wall time is printed and never asserted on.

use super::*;

const SHARD: ShardId = 1;

/// Records the fixture writes, and the reason it is this large.
///
/// The index-GC gate is ANDed: at least `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` (768 KiB) of
/// index log AND at least `DEFAULT_INDEX_GC_USAGE_RATIO_TRIGGER_BASIS_POINTS` (40%) of its records
/// removable. An index-log record is about 59 B, so 8,000 records is 477 KB and the byte gate
/// declines -- correctly -- while 16,000 reaches 943 KB and it fires. That measurement is
/// `the_periodic_loop_actually_reclaims_the_index_log`, which is where this number comes from.
/// The gate is exercised at the SHIPPED threshold; tuning the threshold down to suit the fixture
/// would exercise a gate no deployment runs.
const RECORDS: usize = 16_000;

/// One key in this many is written with a deadline already in the past.
///
/// 16,000 / 50 = 320 due keys, which must EXCEED `DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND` (128)
/// or the hot bound cannot be shown to bite at all. A fixture that cannot exceed a bound runs
/// green while measuring nothing, so that margin is ASSERTED below rather than assumed.
const DEADLINE_EVERY: usize = 50;

/// One write per call, deliberately, and not a batch.
///
/// A write BATCH is ONE log record however many objects it carries, so a batched corpus sits on
/// the delayed side of `min_undumped_wal_records` (1,000) for ever: the dump never fires, nothing
/// advances the index-log retain floor, and the index-GC stage finds nothing removable. That is
/// why `round_stage_cost.rs` reports `index_gc` flat at zero and says in as many words that the
/// zero is unexercised rather than cheap. One record per sequence clears the dump threshold and
/// gives the stage real work to be bounded.
fn seed(engine: &TemporalEngine) -> usize {
    let mut due = 0usize;
    for index in 0..RECORDS {
        let command = if index % DEADLINE_EVERY == 0 {
            due += 1;
            Command::StringSetEx {
                key: format!("d-{index:08}"),
                value: vec![b'v'; 64],
                ttl_ms: 1,
            }
        } else {
            Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 64],
            }
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
    due
}

fn seeded(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        1 << 24,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    let due = seed(&engine);
    (engine, due)
}

/// The round both arms run: every stage on, and ONLY the seven bounds differing.
///
/// `eviction_memory_pressure_threshold` is 1 so the eviction gate ADMITS. With the shipped
/// threshold the stage declines on a fixture this size and reports zero victims in both arms,
/// which reads exactly like a bound that changed nothing. `load_cold_buckets_for_expire` is on so
/// the cold window is reached at all rather than being skipped before its bound is consulted.
fn round(bounded: bool) -> StorageManagerCycleRequest {
    let shipped = StorageManagerCycleRequest::default();
    let usize_bound = |value: usize| if bounded { value } else { 0 };
    let u64_bound = |value: u64| if bounded { value } else { 0 };
    StorageManagerCycleRequest {
        shard_id: SHARD,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        eviction_memory_pressure_threshold: 1,
        load_cold_buckets_for_expire: true,
        index_gc_max_entries_per_round: usize_bound(shipped.index_gc_max_entries_per_round),
        index_gc_index_log_bytes_threshold: u64_bound(shipped.index_gc_index_log_bytes_threshold),
        index_gc_usage_ratio_trigger_basis_points: u64_bound(
            shipped.index_gc_usage_ratio_trigger_basis_points,
        ),
        max_expire_hot_buckets_per_round: usize_bound(shipped.max_expire_hot_buckets_per_round),
        max_expire_cold_buckets_per_round: usize_bound(shipped.max_expire_cold_buckets_per_round),
        eviction_batch_limit: usize_bound(shipped.eviction_batch_limit),
        block_gc_min_slab_garbage_basis_points: u64_bound(
            shipped.block_gc_min_slab_garbage_basis_points,
        ),
        ..shipped
    }
}

/// What one round did, read off the report the round itself published.
struct RoundCost {
    /// Index-log records the sweep actually discarded.
    swept: usize,
    /// How many were removable BEFORE the entry budget was applied. This is the denominator for
    /// "the budget bit": equal to `swept` means the budget was never reached.
    removable: usize,
    budget_exhausted: bool,
    index_gc_applied: bool,
    index_gc_skipped_reason: String,
    usage_ratio_bp: u64,
    index_log_bytes_before: u64,
    /// Due keys the hot and cold expiry windows took. Named `*_buckets_scanned` on the report; the
