// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN A ROUND TELL A SMALL BACKLOG FROM A LARGE ONE? IT ALREADY CAN, EXACTLY. ITS COST NEVER ASKS.
//!
//! mx#1934 measured one maintenance round at 20,000 and 100,000 records and named a mechanism for
//! what it found: "after 2,000 writes to a 102,000-record shard, the round still reports 102,000
//! dirty buckets -- it cannot tell a small amount of pending work from a large one."
//!
//! READ `round_scale.rs` BESIDE THIS FILE. IT GOT THERE FIRST, AND THIS FILE SAYS SO.
//! `the_dump_threshold_counts_log_records_not_dirty_objects` already established the mechanism
//! mx#1934 misread -- a write BATCH is one log record however many objects it carries, so a
//! batched corpus sits on the delayed side of `min_undumped_wal_records` and "the dirty set never
//! drains" -- and `an_idle_round_still_walks_the_live_block_set` already established that a round
//! with an empty dirty set walks the store anyway. Neither finding is new here. What this file
//! adds is set equality against a witness from outside the engine, the restart argument, and both
//! of those established results carried up to mx#1934's own fixture and sizes.
//!
//! REPRODUCED FIRST, BIT FOR BIT. Every figure mx#1934 published reproduces on this tree:
//! 377,824 -> 1,897,824 live-page entries for the all-pending round (5.023x), 609,052 ->
//! 2,849,052 for the fixed-pending round (4.678x), and 304.526 -> 1,424.526 entries per record of
//! work retired. Nothing below disputes a number it reported.
//!
//! WHAT IS DISPUTED IS THE MECHANISM, AND THE ANSWER WAS ALREADY IN THE TREE. The 102,000 dirty
//! buckets were REAL. In that fixture the round never dumps, so nothing is ever retired and every
//! bucket in the store genuinely is dirty -- the round was reporting the truth, not failing to
//! distinguish anything. The fixture seeds in batches of 500, so 20,000 records make FORTY log
//! sequences; `min_undumped_wal_records` is 1,000, which is also the production default; 40 <
//! 1,000 delays the dump on every round for ever, `selected_dump_buckets` is empty every round,
//! `clear_dumped_bucket_dirty_state` is never reached, and the dirty set is drained ZERO times in
//! the life of the store. Asserted at mx#1934's own size by
//! `the_measured_fixture_never_dumps_so_the_buckets_it_calls_dirty_really_are_dirty`, with a
//! control arm that lowers only that one threshold and watches the same store drain to nothing.
//!
//! THE TRACKING IS ALREADY THERE AND -- THIS PART IS NEW -- IT IS EXACT AS A SET.
//! `shard.dirty_objects` is keyed by routing bucket at the write itself, drained by the dump that
//! captured the bucket, and held back for any bucket whose generation moved since the capture.
//! Everything measured before this file compared COUNTS. Counts agree for two sets that are both
//! wrong by the same amount, and the failure that matters here is one bucket, so the assertion has
//! to be on the SET. Element by element against a witness built OUTSIDE the engine -- the
//! fixture's own list of keys written since the last dump, hashed with `block_routing_bucket` --
//! the two sets are equal at five points in a write sequence, with no bucket in either one alone,
//! and the two differences are asserted separately because only one of them loses data. No cost
//! moves onto the write path because there is nothing new to maintain: mx#1709 already put the
//! bucket id into the dirty index at `mark_object_dirty`.
//!
//! SO WHAT IS THE DEFECT? THE ROUND'S COST NEVER CONSULTS ANY OF IT. With the dump firing and the
//! dirty set drained to EMPTY -- no pending work of any kind, nothing to dump, nothing to retire
//! -- a round still walks the whole live page set. This is `an_idle_round_still_walks_the_live_block_set`
//! carried to the two sizes above the 100,000-record line, where it can be scaled rather than
//! stated, and set beside a fixed backlog so the two terms can be separated:
//!
//! ```text
//!   BACKLOG      store 20,000   store 100,000    ratio   per record of STORE   slabs
//!   empty             569,724       2,889,724   5.072x     28.486 -> 28.897        5
//!   2,000 records     755,036       3,555,036   4.708x     34.320 -> 34.853        8
//!   marginal cost
//!   of the SAME
//!   2,000 backlog     185,312         665,312   3.590x   92.656 -> 332.656 PER
//!                                                          RECORD OF WORK
//! ```
//!
//! A round with NOTHING TO DO costs 2,889,724 live-page entries at 100,000 records and 569,724 at
//! 20,000: 5.072x for a store 5x larger, and 81% of what the 2,000-record round costs. That figure
//! divided by the work retired is not large, it is UNDEFINED -- the work retired is zero. Per
//! record of WORK retired the backlog's own marginal cost goes 92.656 -> 332.656, 3.590x, because
//! the dump that retires 2,000 buckets re-walks the whole store to do it. Both terms scale with
//! the store and neither asks the dirty set anything.
//!
//! THE MARGINAL ROW CARRIES A SLAB TERM AND THE OTHER TWO DO NOT. Writing the backlog rolls slabs,
//! so the loaded arms run on eight slabs and the empty arms on five. The ratio WITHIN each row is
//! measured at a constant slab count -- asserted, arm against arm -- and is what the verdicts rest
//! on; the marginal row subtracts an eight-slab round from a five-slab one and is reported for its
//! ratio rather than for its absolute size.
//!
//! WHERE THE FLOOR GOES, NAMED. Attributed by calling site on the empty round at 100,000 records,
//! which is the round with no work in it at all:
//!
//! ```text
//!   storage_reporting.rs:171            398,456   whole-store summary, 4 per round
//!   storage_reporting.rs:126            299,100
//!   storage_reporting.rs:801            299,100
//!   recovery_sweep_compact.rs:424       298,844
//!   storage_lifecycle_methods.rs:975    298,844
//!   compaction.rs:61                    199,360
//!   recovery_sweep_compact.rs:1430      199,228
//!   recovery_sweep_compact.rs:551       199,228
//!   storage_reporting.rs:1029           199,228
//! ```
//!
//! Nine sites in five modules, each walking a store that has nothing pending. `round_scale.rs`
//! names seven of them at 2,000 and 20,000 records and holds their per-record split under a guard;
//! this is the same split at 100,000 with nothing pending, and it is reported rather than asserted
//! here so the two files do not both own one list. That is where the work is, and it is NOT dirty
//! tracking -- `bucket_storage_summaries` alone has eighteen callers,
//! several of which publish whole-store report figures that a scoped walk would silently narrow.
//! Nothing here is changed, for the reason the campaign's own rule gives: a round that skips a
//! bucket that was dirty is silent until someone looks for a record that is not there.
//!
//! WHAT WAS PRICED AND DECLINED. Scoping `bucket_storage_summaries` to the dirty bucket set is the
//! obvious change and would take 2,889,724 entries off the empty round at 100,000 records. It is
//! declined here because it is not provably exact. The walk credits each page to
//! `entry.address.routing_bucket()` with a hash fallback over the SHARD'S range, while
//! `upsert_bucket_index_block_inner` files that same page under a fallback over `0..u32::MAX`, so
//! a page whose address carries no explicit routing bucket can be filed under one bucket and
//! summarised under another. A walk scoped to the dirty buckets would then miss it, which is the
//! losing direction. Measured on this fixture the branch does not fire -- zero unrouted pages and
//! zero misfiled pages at both sizes -- but "does not fire on this fixture" is not the same
//! statement as "cannot fire", and the shortfall is the whole argument.
//!
//! SUM THE INTEGRAL. The floor is the term that matters, because it is paid whether or not
//! anything happened. A store grown to N records with one round every `k` records pays about
//! `28.9 N^2 / 2k` live-page entries in floor alone -- 1.445e12 at a million records with a round
//! every 10,000 -- and every one of those rounds truthfully reports zero dirty buckets.
//!
//! CRASH AND RESTART. The dirty set is in memory only and is EMPTY after a restart: 2,000 dirty
//! objects become 0. That is safe for data and is asserted to be -- every sampled record is still
//! readable after the restart, after a round that dumped nothing, and after a second restart --
//! because durability is anchored on the persisted bucket index and the log, not on the dirty set.
//! `load_shard_with` clears every bucket and page dirty flag on load and
//! `refresh_bucket_runtime_flags` recomputes `bucket.dirty` from the empty set, which is the
//! clear-dirty-on-load contract. What a restart DOES take away is dump SELECTION: the round after
//! a restart selects no bucket to dump even though the log still counts those writes as undumped.
//! Measured and asserted, because it is the constraint on anyone who would drive more of the round
//! from this set.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use std::collections::BTreeSet;

/// The two corpus sizes, five times apart -- the same two mx#1934 used, so the tables can be read
/// beside each other.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written immediately before the round in the fixed-backlog arm, at BOTH sizes.
const FIXED_PENDING: usize = 2_000;

/// Records per write batch. This is the number that decides the whole fixture artefact below: the
/// log threshold counts SEQUENCES, and a batch is one sequence.
const SEED_BATCH: usize = 500;

/// `default_storage_manager_min_undumped_wal_records()`, which is also what mx#1934's fixture
/// asked for. Held here as a constant so the arithmetic against `SEED_BATCH` is visible.
const PUBLISHED_MIN_UNDUMPED_WAL_RECORDS: u64 = 1_000;

/// The threshold the control arm uses instead, so the dump actually fires. The ONLY setting that
/// differs between the two arms.
const DUMPING_MIN_UNDUMPED_WAL_RECORDS: u64 = 1;

/// Stages this fixture never gives work to apply. Their zeros are NOT measurements of a cheap
/// stage, and are asserted to be zero so a later fixture that gives them work fails here rather
/// than turning an absent measurement into a verdict.
const UNEXERCISED_STAGES: &[&str] = &["reclaim_page", "index_gc"];

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

/// Write `count` records from `from` and return the keys written, so the caller holds its own list
/// of what is outstanding -- the witness the engine's dirty set is compared against.
///
/// One record in ten carries a deadline so the expire sweep has something to remove, and the data
/// slab is rolled four times on the way so the store is more than one slab. Both match mx#1934's
/// fixture, so the reproduction arm can be compared against its published table.
fn seed(engine: &TemporalEngine, from: usize, count: usize) -> Vec<String> {
    let mut written = Vec::with_capacity(count);
    let mut index = from;
    let to = from + count;
    let roll_every = (count / 4).max(1);
    let mut since_roll = 0usize;
    while index < to {
        let end = (index + SEED_BATCH).min(to);
        let mut commands = Vec::new();
        for cursor in index..end {
            let key = if cursor % 10 == 0 {
                format!("t-{cursor:08}")
            } else {
                format!("k-{cursor:08}")
            };
            if cursor % 10 == 0 {
                commands.push(Command::StringSetEx {
                    key: key.clone(),
                    value: vec![b'v'; 128],
                    ttl_ms: 1,
                });
            } else {
                commands.push(Command::StringSet {
                    key: key.clone(),
                    value: vec![b'v'; 128],
                });
            }
            written.push(key);
            }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        since_roll += end - index;
        index = end;
        if since_roll >= roll_every && index < to {
            since_roll = 0;
            let before = engine.block_store.slab_ids().unwrap_or_default().len();
            engine.block_store.roll_slab().expect("roll a data slab");
            let after = engine.block_store.slab_ids().unwrap_or_default().len();
            assert!(
                after > before,
                "the fixture asked for a slab roll and got none ({before} -> {after}), so the \
                 store is one slab and compaction and page reclaim have no choice to make"
            );
        }
    }
    written
}

/// The round the periodic driver runs, with every stage on, parameterised on the one threshold the
/// two arms differ by.
fn round_request(min_undumped_wal_records: u64) -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        eviction_memory_pressure_threshold: 1,
        eviction_batch_limit: 4,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        index_gc_index_log_bytes_threshold: 1,
        ..StorageManagerCycleRequest::default()
    }
}

/// How many dirty objects the shard is holding, and in how many buckets.
fn dirty_state(engine: &TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    (
        shard.dirty_objects.len(),
        shard.dirty_objects.bucket_ids().count(),
    )
}

/// The buckets the engine says are dirty, read from the dirty index itself.
fn dirty_bucket_set(engine: &TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&1)
        .expect("shard 1")
        .dirty_objects
        .bucket_ids()
        .collect()
}

/// The shard's own routing range, which is what the witness below has to hash with.
fn routing_range(engine: &TemporalEngine) -> (u32, u32) {
    engine
        .infos
        .read()
        .expect("info lock poisoned")
        .get(&1)
        .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
        .unwrap_or((0, u32::MAX))
}

/// Total objects the dirty index has ever had drained out of it, process-wide.
fn drained_total() -> u64 {
    crate::engine::storage_lifecycle_methods::DIRTY_DRAIN_VISITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One round, with the walk instrument read once around the WHOLE call.
///
/// The total comes from a counter the stage rows do not feed: the rows accumulate in their own
/// atomics and are cleared at every stage boundary, while this is read once around the call.
/// Subtracting one from the other is therefore a reading rather than an identity, which is the
/// only reason the residual test below means anything.
struct RoundCost {
    report: StorageManagerCycleReport,
    whole_call_entries: u64,
    store_records: usize,
    backlog: usize,
    retired: usize,
    slabs: usize,
    path_len: usize,
}

impl RoundCost {
    fn stage(&self, name: &str) -> &crate::engine::reports::StorageManagerStageReport {
        self.report
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .unwrap_or_else(|| {
                panic!(
                    "the round reported no stage named {name}; it reported {:?}",
                    self.report
                        .stages
                        .iter()
                        .map(|stage| stage.stage.as_str())
                        .collect::<Vec<_>>()
                )
            })
    }

    fn stage_row_entries(&self) -> u64 {
        self.report
            .stages
            .iter()
            .map(|stage| stage.walk.live_block_entries)
            .sum()
    }

    fn per_record_of_store(&self) -> f64 {
        self.whole_call_entries as f64 / self.store_records as f64
    }
