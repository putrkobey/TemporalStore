// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What ONE maintenance round costs, STAGE BY STAGE, on a large store.
//!
//! The round is what runs continuously: prepare, reclaim the log, expire, evict, reclaim pages,
//! collect the index log, compact, reap metrics. Its stages have each been measured before, at
//! small sizes. The question here is the one only a large store answers -- as the store grows,
//! does one round cost what the STORE is, or what the round has to DO? -- and it is asked of each
//! stage separately, because a total hides the element: one correctly bounded stage holds the sum
//! down while three beside it are not.
//!
//! THE ANSWER IS THE STORE. Counted, not timed; this box is shared and a stopwatch on it measures
//! the other tenants. Two sizes five times apart, both above the ~100,000-record line below which
//! every whole-store cost this campaign has found was invisible, on a four-slab store:
//!
//! ```text
//!   ALL RECORDS PENDING -- every record rewritten before the round
//!   stage                      20,000       100,000    ratio   per record   verdict
//!   plan                       40,000       200,000    5.000x    2.000      GROWING
//!   prepare                         0             0       --     0.000      FLAT
//!   reclaim_wal                99,360       499,360    5.026x    4.968      GROWING
//!   expire                          0             0       --     0.000      FLAT, bounded
//!   evict                           0             0       --     0.000      FLAT, bounded
//!   reclaim_page                    0             0       --     0.000      FLAT, not exercised
//!   index_gc                        0             0       --     0.000      FLAT, not exercised
//!   merged_dump_load_policy   119,232       599,232    5.026x    5.962      GROWING
//!   compact                   119,232       599,232    5.026x    5.962      GROWING
//!   reap_metrics                    0             0       --     0.000      FLAT
//!   ROUND                     377,824     1,897,824    5.023x   18.891      GROWING
//! ```
//!
//! Four of the ten stages walk the whole live page set, each a fixed number of times per round --
//! 2, 5, 6 and 6, summing to 19 -- and the per-record figure moves 18.891 -> 18.978 across a store
//! five times larger, which is 0.5%. Six stages are flat at zero. Two of those six are flat
//! because they are BOUNDED per cycle, one is flat because it does no walking, and TWO ARE NOT
//! MEASURED AT ALL: `reclaim_page` and `index_gc` found no work to apply in this fixture, and
//! their zeros are therefore unmeasured rather than measured. Said here rather than left to be
//! read as cheap stages, which is exactly what an unexercised stage looks like.
//!
//! The cheaper bucket-index walk tells the same story and only `compact` makes it: 79,488 ->
//! 399,488 entries, 5.026x, 3.974 -> 3.995 per record.
//!
//! WHAT IT COSTS TO DO ALMOST NOTHING. The second regime holds the pending work FIXED -- 2,000
//! fresh records whatever the store holds -- which is what a shard mostly does. Per record of
//! STORE the round costs the same as ever; per record of WORK it costs linearly more:
//!
//! ```text
//!   2,000 RECORDS PENDING, eight-slab store
//!                             store 22,000   store 102,000   ratio
//!   plan                            43,744         203,744   4.657x
//!   reclaim_wal                    152,200         712,200   4.680x
//!   merged_dump_load_policy        260,904       1,220,904   4.680x
//!   compact                        152,204         712,204   4.680x
//!   ROUND                          609,052       2,849,052   4.678x
//!   per record of STORE             27.684          27.932   1.009x   FLAT
//!   per record of WORK RETIRED     304.526        1424.526   4.678x   LINEAR IN THE STORE
//! ```
//!
//! The store grew 4.636x and the cost of retiring one record grew 4.678x with it. That is the
//! answer to the question this file exists for, in one line: A ROUND IS NOT PAID FOR BY THE WORK
//! IN FRONT OF IT. The two regimes are NOT directly comparable stage by stage -- the eight-slab
//! store walks more per record than the four-slab one, and slab count is a second term this
//! fixture does not hold still across regimes -- but the scaling WITHIN each regime is measured at
//! a constant slab count and is what the verdicts above rest on.
//!
//! SUM THE INTEGRAL. A per-round cost that is a CONSTANT MULTIPLE of the store is not a flat cost.
//! The round fires on a timer while the store grows under it, so the multiple applies to a subject
//! the round itself watches grow. Building a store to N records with one round per `k` records
//! written costs `19 * k * (N/k)(N/k + 1)/2` live-page entries, which is about `19 N^2 / 2k`. At
//! N = 1,000,000 and a round every 10,000 records that is 9.5e11 entries to build the store once
//! -- quadratic in N, while every individual round reports a perfectly steady 18.9x.
//!
//! IS ANY STAGE BOUNDED PER CYCLE? Three are, by name: `expire` at its hot and cold per-round
//! bucket bounds (128 and 8), `evict` at `eviction_batch_limit`, and `index_gc` at
//! `index_gc_max_entries_per_round` (256). `every_maintenance_round_is_bounded_by_default` already
//! asserts those are set and non-zero, and this measurement confirms they hold: expire takes
//! exactly 128 buckets and evict exactly its batch at BOTH sizes. What they bound is what each
//! stage CHANGES. None of them bounds what it READS, and the four stages in the table above --
//! `plan`, `reclaim_wal`, `merged_dump_load_policy`, `compact` -- have no per-cycle bound of
//! either kind: each does all the work it finds. One bound does live inside
//! `merged_dump_load_policy`, `RECOVERY_READABLE_PROBE_PER_ROUND` at 512 pages, and it is why that
//! stage reads only 512 pages while still walking the index six times.
//!
//! NOTHING SHEDS OR THROTTLES. No stage returns "I stopped short, come back sooner"; the three
//! bounded ones carry a cursor to the next round and say nothing else, and the four unbounded ones
//! have nothing to say. Reported because it was asked, including the part that is "nothing".
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::{
    StageWalkCharges, StorageManagerCycleReport, StorageManagerCycleRequest,
};

/// The two corpus sizes, five times apart.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written before the round in the fixed-pending regime, at BOTH sizes. The knob of this
/// whole measurement: how much work is waiting, relative to the store.
const FIXED_PENDING: usize = 2_000;

/// Whole-store walks each stage makes per round with every record pending. Named individually, so
/// a failure says WHICH stage moved rather than that a total did.
const WALKS_PER_STAGE: &[(&str, u64)] = &[
    ("plan", 2),
    ("prepare", 0),
    ("reclaim_wal", 5),
    ("expire", 0),
    ("evict", 0),
    ("reclaim_page", 0),
    ("index_gc", 0),
    ("merged_dump_load_policy", 6),
    ("compact", 6),
    ("reap_metrics", 0),
];

/// The stages whose zero above is a BOUND doing its job, not an idle stage. Asserted to have done
/// work by `every_stage_this_round_measures_had_work_to_do`.
const BOUNDED_STAGES: &[&str] = &["expire", "evict"];

/// The stages whose zero above is NOT MEASURED: this fixture never gave them work to apply. Named
/// here so the table above cannot be read as saying they are cheap.
const UNEXERCISED_STAGES: &[&str] = &["reclaim_page", "index_gc"];

/// The stages with no per-cycle bound, which walk the whole store every round.
const UNBOUNDED_WALKING_STAGES: &[&str] =
    &["plan", "reclaim_wal", "merged_dump_load_policy", "compact"];

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

/// Roll the active data slab, so the store is more than one slab and page reclaim and compaction
/// have a choice to make.
///
/// The block store's own roll rather than a prepare-only round: a round surveys the whole shard on
/// the way past, and at 100,000 records a fixture built out of rounds costs four times what the
/// measurement does.
fn roll_slab(engine: &TemporalEngine) {
    let before = engine.block_store.slab_ids().unwrap_or_default().len();
    engine.block_store.roll_slab().expect("roll a data slab");
    let after = engine.block_store.slab_ids().unwrap_or_default().len();
    assert!(
        after > before,
        "the fixture asked for a slab roll and got none ({before} -> {after}), so the store is one \
         slab and compaction and page reclaim have no choice to make"
    );
}

/// Write `count` records from `from`, one in ten with a deadline so the expire sweep has something
/// to remove, rolling the data slab four times on the way so the store is not one slab.
fn seed(engine: &TemporalEngine, from: usize, count: usize) {
    let mut index = from;
    let to = from + count;
    let roll_every = (count / 4).max(1);
    let mut since_roll = 0usize;
    while index < to {
        let end = (index + 500).min(to);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            if cursor % 10 == 0 {
                commands.push(Command::StringSetEx {
                    key: format!("t-{cursor:08}"),
                    value: vec![b'v'; 128],
                    ttl_ms: 1,
                });
            } else {
                commands.push(Command::StringSet {
                    key: format!("k-{cursor:08}"),
                    value: vec![b'v'; 128],
                });
            }
            cursor += 1;
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
            roll_slab(engine);
        }
    }
}

/// The round the periodic driver runs, with every stage on.
///
/// `enable_evict` is ON here where the older round measurements left it off. A stage switched off
/// reports a cost of zero that reads exactly like a cheap stage, and the question asked in this
/// file is what the WHOLE round costs.
fn round_request() -> StorageManagerCycleRequest {
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
        min_undumped_wal_records: 1_000,
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

/// One round, with the process-wide instruments read once around the whole call.
///
/// These totals come from counters THE STAGE ROWS DO NOT FEED. The rows accumulate in their own
/// atomics and are cleared at every stage boundary; these are separate counters read once around
/// the call. Subtracting one from the other is therefore a reading and not an identity, which is
/// the only reason the residual below means anything.
struct RoundCost {
    report: StorageManagerCycleReport,
    whole_call: StageWalkCharges,
    store_records: usize,
    pending: usize,
    slabs: usize,
    path_len: usize,
}

fn one_round(engine: &TemporalEngine) -> (StorageManagerCycleReport, StageWalkCharges) {
    crate::engine::reset_live_block_scan_entries();
    crate::engine::reset_bucket_block_index_visits();
    crate::engine::reset_index_encode_counts();
    let report = engine.run_storage_manager_cycle(round_request());
    let encodes = crate::engine::index_encode_counts();
    (
        report,
        StageWalkCharges {
            live_block_entries: crate::engine::live_block_scan_entries(),
            bucket_block_index_visits: crate::engine::bucket_block_index_visits(),
            index_encodes: encodes.encodes_total,
            index_encode_bytes: encodes.encode_bytes_total,
        },
    )
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

    fn entries(&self, name: &str) -> u64 {
        self.stage(name).walk.live_block_entries
    }

    fn total_entries(&self) -> u64 {
        self.report
            .stages
            .iter()
            .map(|stage| stage.walk.live_block_entries)
            .sum()
    }

    fn visits(&self, name: &str) -> u64 {
        self.stage(name).walk.bucket_block_index_visits
    }
}

/// A round measured on a freshly built store, with `pending` records written immediately before it
/// so the round has work waiting.
///
/// The FIRST round is the warm-up and the SECOND is the one reported: a first round on a store
/// that has never been dumped does a different amount of work from every round after it, and the
/// steady round is the one that runs forever.
fn measure(records: usize, pending: usize) -> RoundCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_len = dir.path().as_os_str().len();
    let engine = round_engine(dir.path());
    seed(&engine, 0, records);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _warmup = one_round(&engine);
    let store_records = if pending >= records {
        seed(&engine, 0, records);
        records
    } else {
        seed(&engine, records, pending);
        records + pending
    };
    std::thread::sleep(std::time::Duration::from_millis(30));
    let (report, whole_call) = one_round(&engine);
    assert!(
        report.errors.is_empty(),
        "{records} records / {pending} pending: the round errored, so every figure it reports \
         measures nothing: {:?}",
        report.errors
    );
    // FLOOR THE INSTRUMENT. A counter that counted nothing reads exactly like a measurement of
    // zero, and the whole table below would be the first of those wearing the second's clothes.
    assert!(
        whole_call.live_block_entries > 0,
        "{records} records / {pending} pending: the round materialised no live-page entry at all"
    );
    let slabs = report.plan.live_block_slab_ids.len();
    RoundCost {
        report,
        whole_call,
        store_records,
        pending,
        slabs,
        path_len,
    }
}

/// The four measurements this file rests on, taken once each.
///
/// Shared rather than repeated: the 100,000-record arms take about two minutes apiece, and four
/// tests each building their own would spend eight minutes rebuilding stores that are identical by
/// construction.
fn cost(records: usize, pending: usize) -> &'static RoundCost {
    use std::sync::OnceLock;
    static ALL_SMALL: OnceLock<RoundCost> = OnceLock::new();
    static ALL_LARGE: OnceLock<RoundCost> = OnceLock::new();
    static FIXED_SMALL: OnceLock<RoundCost> = OnceLock::new();
    static FIXED_LARGE: OnceLock<RoundCost> = OnceLock::new();
    match (records, pending >= records) {
        (SMALL, true) => ALL_SMALL.get_or_init(|| measure(SMALL, SMALL)),
        (LARGE, true) => ALL_LARGE.get_or_init(|| measure(LARGE, LARGE)),
        (SMALL, false) => FIXED_SMALL.get_or_init(|| measure(SMALL, FIXED_PENDING)),
        (LARGE, false) => FIXED_LARGE.get_or_init(|| measure(LARGE, FIXED_PENDING)),
        _ => panic!("no measurement is taken for {records} records / {pending} pending"),
    }
}

/// The store path length is held CONSTANT across arms and stated, because a path's length moves
/// allocation counts and has masqueraded as a real difference on this tree before.
fn assert_paths_comparable(small: &RoundCost, large: &RoundCost) {
    assert_eq!(
        small.path_len, large.path_len,
        "the two arms were measured under store paths of different length ({} vs {}); path length \
         moves allocation counts and would sit inside the ratio as if it were a corpus effect",
        small.path_len, large.path_len
    );
    println!("  store path length held at {} bytes in both arms", small.path_len);
}

fn print_table(tag: &str, small: &RoundCost, large: &RoundCost) {
    println!(
        "  {tag}: store {} -> {} records, {} -> {} slabs",
        small.store_records, large.store_records, small.slabs, large.slabs
    );
    println!(
        "  {:<26} {:>12} {:>12} {:>9} {:>10} {:>10}  verdict",
        "stage", "small", "large", "ratio", "/rec small", "/rec large"
    );
    for (name, _) in WALKS_PER_STAGE {
        let s = small.entries(name);
        let l = large.entries(name);
        let verdict = if s == 0 && l == 0 {
            if UNEXERCISED_STAGES.contains(name) {
                "FLAT, not exercised"
            } else if BOUNDED_STAGES.contains(name) {
                "FLAT, bounded"
            } else {
                "FLAT"
            }
        } else {
            "GROWING"
        };
        println!(
            "  {:<26} {:>12} {:>12} {:>9} {:>10.3} {:>10.3}  {verdict}",
            name,
            s,
            l,
            if s == 0 {
                "--".to_string()
            } else {
                format!("{:.3}x", l as f64 / s as f64)
            },
            s as f64 / small.store_records as f64,
            l as f64 / large.store_records as f64,
        );
    }
    println!(
        "  {:<26} {:>12} {:>12} {:>8.3}x {:>10.3} {:>10.3}  ROUND",
        "TOTAL",
        small.total_entries(),
        large.total_entries(),
        large.total_entries() as f64 / small.total_entries() as f64,
        small.total_entries() as f64 / small.store_records as f64,
        large.total_entries() as f64 / large.store_records as f64,
    );
}

#[test]
fn a_round_costs_the_whole_store_stage_by_stage() {
    let small = cost(SMALL, SMALL);
    let large = cost(LARGE, LARGE);
    assert_paths_comparable(small, large);
    print_table("ALL RECORDS PENDING", small, large);

    // THE CONTROL FOR THIS ARM. Its knob is that the pending work GROWS with the store. If it did
    // not, the two rows are one measurement taken twice and every ratio below is an artefact.
    assert_eq!(
        small.stage("reclaim_wal").candidate_count * (LARGE / SMALL),
        large.stage("reclaim_wal").candidate_count,
        "ALL-PENDING control: the pending work did not grow with the store -- {} undumped records \
         at {SMALL} and {} at {LARGE}, where growing with the store means {}. This arm exists to \
         differ from the fixed-pending arm by exactly this, and without it the two arms are the \
         same measurement",
        small.stage("reclaim_wal").candidate_count,
        large.stage("reclaim_wal").candidate_count,
        small.stage("reclaim_wal").candidate_count * (LARGE / SMALL),
    );
    assert_eq!(
        small.slabs, large.slabs,
        "the two arms hold {} and {} slabs. Slab count is a second term in what the round walks, \
         so a ratio taken across different slab counts is not a corpus ratio",
        small.slabs, large.slabs,
    );

    // EVERY STAGE, NAMED, AT BOTH SIZES. Not a total: a total lets one bounded stage hold down
