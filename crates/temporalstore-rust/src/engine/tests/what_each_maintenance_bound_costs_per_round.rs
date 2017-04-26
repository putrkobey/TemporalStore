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
    /// quantity assigned to it is `hot_selected.len()`, which counts KEYS.
    hot_scanned: usize,
    cold_scanned: usize,
    expired: usize,
    /// Victims the eviction stage CHOSE, which is the list `eviction_batch_limit` truncates.
    victims: usize,
    pressure_gate_open: bool,
    index_log_bytes_reclaimed: u64,
    cache_bytes_removed: u64,
    cache_entries_removed: usize,
    /// Printed, never asserted on: see the note at the top of this file.
    duration_ms: u64,
    /// Per-stage wall time, so "the round got cheaper" can say WHICH stage paid. A total on its
    /// own cannot: one stage carrying the whole cost and seven sharing it read the same.
    stage_ms: Vec<(String, u64)>,
}

fn one_round(engine: &TemporalEngine, bounded: bool) -> RoundCost {
    let report = engine.run_storage_manager_cycle(round(bounded));
    let gc = report.index_gc_report.clone().unwrap_or_default();
    let expiry = report.expiry_report.clone().unwrap_or_default();
    let eviction = report.eviction_report.clone().unwrap_or_default();
    RoundCost {
        swept: gc.records_removed,
        removable: gc.removable_records_before_budget,
        budget_exhausted: gc.budget_exhausted,
        index_gc_applied: gc.applied,
        index_gc_skipped_reason: gc.skipped_reason.clone(),
        usage_ratio_bp: gc.usage_ratio_basis_points,
        index_log_bytes_before: gc.bytes_before,
        hot_scanned: expiry.hot_buckets_scanned,
        cold_scanned: expiry.cold_buckets_scanned,
        expired: expiry.expired_records_removed,
        victims: eviction.selected_victims.len(),
        pressure_gate_open: eviction.pressure_gate_open,
        index_log_bytes_reclaimed: gc.bytes_before.saturating_sub(gc.bytes_after),
        cache_bytes_removed: eviction.cache_disk_bytes_removed,
        cache_entries_removed: eviction.cache_entries_removed,
        duration_ms: report.duration_ms,
        stage_ms: report
            .stages
            .iter()
            .map(|stage| (stage.stage.clone(), stage.duration_ms))
            .collect(),
    }
}

fn show(tag: &str, cost: &RoundCost) {
    eprintln!(
        "  {tag:<16} swept={:<6} removable={:<6} exhausted={:<5} applied={:<5} usage={:<6}bp \
         log_before={:<9} hot={:<5} cold={:<4} expired={:<5} victims={:<5} gate={:<5} \
         log_freed={:<9} cache_freed={:<9} cache_entries={:<5} ms={:<5} skip={:?}",
        cost.swept,
        cost.removable,
        cost.budget_exhausted,
        cost.index_gc_applied,
        cost.usage_ratio_bp,
        cost.index_log_bytes_before,
        cost.hot_scanned,
        cost.cold_scanned,
        cost.expired,
        cost.victims,
        cost.pressure_gate_open,
        cost.index_log_bytes_reclaimed,
        cost.cache_bytes_removed,
        cost.cache_entries_removed,
        cost.duration_ms,
        cost.index_gc_skipped_reason,
    );
}

/// A cycle request that names no bound must still arrive bounded.
///
/// THE GUARD, and it reads the DECODE. Both cycle endpoints parse a request body, so what an
/// OMITTED field decodes to is the behaviour every caller gets who does not name it -- and on a
/// `usize` or `u64`, a bare `#[serde(default)]` decodes to 0, which in every one of these means
/// NO BOUND. The struct's own `Default` is not consulted on that route, so a guard that asserts
/// `Default` cannot see a zero reaching the engine through it.
///
/// Asserted as WHOLE-STRUCT equality rather than as a list of seven field names, on purpose. A
/// list has to be maintained: a bound added later with a bare `#[serde(default)]` and a non-zero
/// `Default` is a hole the day it is added and nothing goes red, and a bound RETIRED shrinks the
/// list rather than failing. Equality covers every field there is now and every field there will
/// be, and it names the offenders in the failure message by diffing the two encodings first.
#[test]
fn a_cycle_request_that_names_no_bound_still_arrives_bounded() {
    // THE ROUTE, not a reconstruction of it: `crate::http::parse_json` is the function both
    // endpoints call on `request.body`.
    let silent: StorageManagerCycleRequest = crate::http::parse_json(br#"{"shard_id":7}"#)
        .expect("a body naming only the shard must parse");
    let shipped = StorageManagerCycleRequest {
        shard_id: 7,
        ..StorageManagerCycleRequest::default()
    };

    // Diff the two encodings first, so a failure says WHICH fields rather than printing two
    // forty-field structs and leaving the reader to find the difference.
    let left = serde_json::to_value(&shipped).unwrap();
    let right = serde_json::to_value(&silent).unwrap();
    let mut unbounded: Vec<String> = Vec::new();
    for (field, want) in left.as_object().unwrap() {
        let got = right.get(field).cloned().unwrap_or(serde_json::Value::Null);
        if &got != want {
            unbounded.push(format!("{field}: shipped {want}, decoded {got}"));
        }
    }
    assert!(
        unbounded.is_empty(),
        "a cycle request body that names only the shard decoded to something other than the \
         shipped round. Each of these reached the engine as the caller never asked for, and a 0 \
         in a per-round bound is NO bound rather than a small one:\n  {}",
        unbounded.join("\n  ")
    );
    assert_eq!(
        silent, shipped,
        "the decoded request must be the shipped round with only the shard named"
    );

    // AND THE BOUNDS ARE STILL NON-ZERO, which the equality above does not say on its own: if
    // every bound went to 0 in `Default` as well, the two sides would agree and this test would
    // pass on a wholly unbounded round. This half is what `every_maintenance_round_is_bounded_by_default`
    // pins by value; restated here so one test name defends the whole claim in CI.
    for (name, bound) in [
        (
            "index_gc_max_entries_per_round",
            silent.index_gc_max_entries_per_round as u64,
        ),
        (
            "index_gc_index_log_bytes_threshold",
            silent.index_gc_index_log_bytes_threshold,
        ),
        (
            "index_gc_usage_ratio_trigger_basis_points",
            silent.index_gc_usage_ratio_trigger_basis_points,
        ),
        (
            "max_expire_hot_buckets_per_round",
            silent.max_expire_hot_buckets_per_round as u64,
        ),
        (
            "max_expire_cold_buckets_per_round",
            silent.max_expire_cold_buckets_per_round as u64,
        ),
        ("eviction_batch_limit", silent.eviction_batch_limit as u64),
        (
            "block_gc_min_slab_garbage_basis_points",
            silent.block_gc_min_slab_garbage_basis_points,
        ),
    ] {
        assert!(
            bound > 0,
            "{name} decoded to zero, which is not a bound at all"
        );
    }

    // THE PERIODIC LOOP, the other production path that does not go through a body at all.
    let periodic = crate::data_node::StorageManagerRuntimeOptions::default().request;
    assert_eq!(
