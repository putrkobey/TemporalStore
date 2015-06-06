// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one storage-manager round costs at two corpus sizes.
//!
//! THE RESULT. A round's cost is not flat in the store: it walks the whole live-page set a
//! CONSTANT NUMBER OF TIMES, and that constant is the same at every corpus size measured. Counted
//! rather than timed, because the box these run on is shared and a stopwatch there measures the
//! other tenants:
//!
//! ```text
//!   records   live-page entries materialised by ONE round   ratio
//!     2,000                                        26,000   13.00x
//!    20,000                                       260,000   13.00x
//! ```
//!
//! Exactly 13.00 at both, and the per-site split is identical at both sizes -- seven call sites,
//! summing to 13. The split is PRINTED by `steady_walks_per_record` rather than maintained here
//! by hand, because a table beside a total is a list that goes stale without anything failing:
//!
//! ```text
//!   storage_reporting.rs:171  bucket_storage_summaries            5.00x
//!   recovery_sweep_compact.rs:334 object lifecycle snapshot       2.00x
//!   storage_lifecycle_methods.rs:873 sampling snapshots           2.00x
//!   compaction.rs:61          compaction_utility_report           1.00x
//!   storage_reporting.rs:801  bucket generation fingerprints      1.00x
//!   recovery_sweep_compact.rs:461 the recovery report             1.00x
//!   storage_reporting.rs:1029 feature block layout report         1.00x
//! ```
//!
//! IT READ TWELVE, AND IT WAS ALWAYS THIRTEEN. The last row is not a pass that was added; it is
//! one that could not be counted. `LIVE_BLOCK_SCAN_ENTRIES` was charged in
//! `collect_live_block_entries` and nowhere else, and `storage_feature_block_layout_report`
//! calls `collect_bucket_index_live_block_entries` directly -- so a whole-store pass made on
//! every periodic round was charged to nothing, and the guard here that exists to notice a walk
//! being added could not see it. The charge now happens inside the walks themselves.
//!
//! IT WAS FOURTEEN. Three of those passes were made by ONE function,
//! `storage_recovery_report_without_boundary_sampled`, under ONE shard-table read guard: once for
//! the addresses it probes, once inside `validate_shard_block_ownership`, once inside
//! `storage_object_lifecycle_report`. The guard is held across all three, so the second and third
//! could only rebuild what the first already held. They are now one walk, and the separate
//! `Vec<BlockAddress>` that used to be kept alive beside the entries is not built at all.
//!
//! WHAT COULD NOT BE SHARED, and why this is a hoist under one guard rather than a round-scoped
//! memo. Traced in order, the round's remaining passes are separated by mutation of the very
//! state they read: `apply_storage_lifecycle` writes a dump manifest and then calls
//! `clear_dumped_bucket_dirty_state` between its first walk and its second, and on a dumping
//! round the shard's dirty-bucket flags were measured moving 0 -> 2,000 -> 0 WITHIN one round.
//! `bucket_storage_summaries` reports `dirty_object_count`, so a snapshot taken before that
//! clear and served after it would report dirty buckets that are no longer dirty. The five
//! `storage_reporting.rs:171` passes are five different answers, not one answer fetched five
//! times.
//!
//! So the round is proportional to the store with a constant of 13, and no stage budget bounds
//! it: the budgets in this round bound the DUMP (buckets per round), the EXPIRE sweep (buckets per
//! round) and the readability probe (512 pages), none of which is the walk above. The wall clock
//! corroborates -- a steady round took 1,352-1,625 ms at 8,000 and 8,117-9,657 ms at 80,000, at
//! box load 4.7-6.3 -- but the count is the claim, because it came out to the same three
//! significant figures on every run.
//!
//! AN IDLE SHARD DOES NOT ESCAPE IT. The plan's own whole-shard walk is skipped when no object is
//! dirty, which is true and is what makes `bucket_summaries` an `Option`. It does not make the
//! ROUND cheap: measured on a shard whose dirty set had drained to zero and which had taken no
//! write since, the round still materialised 14.00x the record count, because the other walks
//! never consult the dirty set. `an_idle_round_still_walks_the_live_block_set` is that number.
//!
//! WHAT THESE TESTS ARE FOR. The ratio is asserted at TWO sizes, so the two failures read
//! differently: a ratio that changed at both sizes means a walk was added or removed, and a ratio
//! that differs BETWEEN the sizes means something in the round stopped being proportional to the
//! store and started being proportional to something else. The second is the one worth a page.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;

/// Live-page entries one steady round materialises, per record in the store.
///
/// Measured, at 4,000 / 8,000 / 80,000 records, on a shard whose dump is delayed -- which is the
/// ordinary state of a shard written to in batches, see
/// `the_dump_threshold_counts_log_records_not_dirty_objects`.
const WALKS_PER_ROUND: u64 = 13;

/// The same, once the dirty set has drained. HIGHER, not lower: a shard that has taken a dump has
/// a manifest, and validating it walks the live set again.
const WALKS_PER_IDLE_ROUND: u64 = 14;

fn round_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed_in_batches(engine: &TemporalEngine, shard_id: ShardId, count: usize, batch: usize) {
    let mut index = 0usize;
    while index < count {
        let end = (index + batch).min(count);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

/// The round the periodic driver runs, with the data node's own thresholds.
fn round_request() -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 1_000,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..StorageManagerCycleRequest::default()
    }
}

/// Live-page entries one round materialises on this engine, and the round's own report.
fn walk_volume_of_one_round(
    engine: &TemporalEngine,
) -> (u64, crate::engine::reports::StorageManagerCycleReport) {
    walk_volume_and_sites_of_one_round(engine).0
}

/// The same, plus WHICH call sites walked. The split in the module note above is this, printed,
/// rather than a list maintained by hand beside a total that would not notice it going stale.
fn walk_volume_and_sites_of_one_round(
    engine: &TemporalEngine,
) -> (
    (u64, crate::engine::reports::StorageManagerCycleReport),
    std::collections::BTreeMap<String, u64>,
) {
    crate::engine::reset_live_block_scan_entries();
    crate::engine::reset_live_block_scan_sites();
    let report = engine.run_storage_manager_cycle(round_request());
    let entries = crate::engine::live_block_scan_entries();
    let sites = crate::engine::live_block_scan_sites_snapshot();
    ((entries, report), sites)
}

/// One STEADY round's walk volume per record, at `records` records written in `batch`-sized
/// batches. The first round is the warm-up; the second is the one measured.
fn steady_walks_per_record(records: usize, batch: usize) -> u64 {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed_in_batches(&engine, 1, records, batch);
    let _ = walk_volume_of_one_round(&engine);
    let ((entries, report), sites) = walk_volume_and_sites_of_one_round(&engine);

    // THE DENOMINATOR. A round that walked nothing satisfies any ceiling, and so does one that
    // errored out before its first walk.
    assert!(
        report.errors.is_empty(),
        "{records} records: the round errored, so its walk volume measures nothing: {:?}",
        report.errors
    );
    assert!(
        entries > 0,
        "{records} records: the round materialised no live-page entries at all, so this \
         measures nothing"
    );
    assert_eq!(
        entries % records as u64,
        0,
        "{records} records: {entries} entries is not a whole multiple of the record count, so \
         the walk volume is not a count of whole-store passes and the ratio below would round \
         a real difference away"
    );
    println!(
        "  {records:>6} records in batches of {batch:<4} -> ONE round materialised {entries:>9} \
         live-page entries = {}x the store, from {} call site(s)",
        entries / records as u64,
        sites.len()
    );
    for (site, walked) in &sites {
        println!("    {:>6.2}x  {site}", *walked as f64 / records as f64);
    }
    entries / records as u64
}

/// ONE ROUND COSTS A CONSTANT NUMBER OF WHOLE-STORE WALKS, at both corpus sizes.
///
/// Two sizes an order of magnitude apart. Asserting the RATIO rather than a time is what makes
/// this survive a busy box, and asserting it at two sizes is what separates "someone added a
/// walk" (both move together) from "something stopped being proportional to the store" (they
/// diverge).
#[test]
fn a_round_walks_the_live_block_set_a_constant_number_of_times() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 20_000;

    let small = steady_walks_per_record(SMALL, 1_000);
    let large = steady_walks_per_record(LARGE, 1_000);

    assert_eq!(
        small, large,
        "a round cost {small} whole-store walks at {SMALL} records and {large} at {LARGE}. The \
         two must agree: the round's walk volume is supposed to be a fixed number of passes over \
         the store, so a size-dependent count means some pass is now proportional to something \
         other than the live-page set"
    );
    assert_eq!(
        small, WALKS_PER_ROUND,
        "a round now costs {small} whole-store walks per record, not the measured \
         {WALKS_PER_ROUND}. Each one materialises every live page in the shard and clones two \
         strings per entry, so a walk added here is paid on every periodic round for ever. \
         `live_block_scan_sites_snapshot` names the call sites"
    );
}

/// AN IDLE SHARD STILL WALKS THE WHOLE LIVE-PAGE SET.
///
/// The plan's own walk is skipped when no object is dirty. The round's is not. This drains the
/// dirty set first -- asserted, so the test cannot pass by never reaching the idle state -- then
/// measures a round that has nothing whatever to do.
#[test]
fn an_idle_round_still_walks_the_live_block_set() {
    const RECORDS: usize = 2_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    // One log record per write, so the dump threshold is crossed and the dirty set can drain.
    seed_in_batches(&engine, 1, RECORDS, 1);

    let dirty_before = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.dirty_objects.len())
            .unwrap_or(0)
    };
    assert_eq!(
        dirty_before, RECORDS,
        "the fixture did not land: {dirty_before} dirty objects, expected {RECORDS}"
    );

    let (_, dumping) = walk_volume_of_one_round(&engine);
    assert!(
        !dumping.plan.dump_delayed,
        "the dump was delayed, so the dirty set cannot drain and the round below is not idle"
    );

    let dirty_after = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.dirty_objects.len())
            .unwrap_or(0)
    };
    assert_eq!(
        dirty_after, 0,
        "the dirty set did not drain ({dirty_after} left), so the round measured below is not \
         the idle one this test is about"
    );

    // Now a round with an empty dirty set, no write since, and nothing to dump.
    let (entries, idle) = walk_volume_of_one_round(&engine);
    assert!(idle.errors.is_empty(), "idle round errored: {:?}", idle.errors);
    assert!(
        idle.plan.bucket_summaries.is_none(),
        "the plan took its whole-shard walk, so this is not measuring the case where that walk \
         was skipped"
    );
    let walks = entries / RECORDS as u64;
    println!(
        "  IDLE round over {RECORDS} records, dirty set empty, nothing to dump: {entries} \
         live-page entries = {walks}x the store"
    );
    assert_eq!(
        walks, WALKS_PER_IDLE_ROUND,
        "an idle round now costs {walks} whole-store walks per record, not the measured \
         {WALKS_PER_IDLE_ROUND}. An idle shard pays this every cycle for as long as it is loaded"
    );
}

/// THE DUMP THRESHOLD COUNTS LOG RECORDS; THE WORK IT DEFERS IS THE DIRTY-OBJECT SET.
///
/// `min_undumped_wal_records` is compared against undumped LOG records, and a batch is one log
/// record however many objects it carries. So the same corpus can sit either side of the same
/// threshold depending only on how the writer grouped it -- and on the far side the dirty set
/// never drains, which means the round above pays its whole-store walks for ever and produces
/// nothing.
#[test]
fn the_dump_threshold_counts_log_records_not_dirty_objects() {
    const RECORDS: usize = 2_000;
    const THRESHOLD: u64 = 1_000;

    fn one_arm(records: usize, batch: usize) -> (u64, bool, usize) {
