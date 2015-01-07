// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE AMPLIFICATION SET UNDER REWRITE, and what a reclaim cycle gives back.
//!
//! # EVERY FIGURE BEFORE THIS ONE WAS 1.0x BY CONSTRUCTION
//!
//! Every amplification arm in this area writes each key exactly once. On a write-once corpus
//! nothing is stale, so bytes-ever-written equals bytes-live and the rewrite ratio is 1.0x, and
//! physical equals live so the space ratio is 1.0x too. Those are not findings about good
//! behaviour, they are arithmetic about the fixture. The ceilings these ratios exist to be held
//! against -- 5.0x on space, 3.0x on write -- can only be approached by a workload that produces
//! DEAD bytes, and none had been run.
//!
//! This one overwrites the same keys repeatedly, reports the set after each round, then runs a real
//! reclaim cycle and reports it again. It answers three things the write-once corpus cannot:
//!
//!   1. does the rewrite ratio RISE with rewrite pressure, or is it inert?
//!   2. does space amplification climb as dead blocks accumulate, and how far?
//!   3. does a reclaim cycle bring it back, and back to what?
//!
//! # THERE ARE NOW THREE DENOMINATORS HERE, AND THE PINNED ASSERTION IS KEPT ON PURPOSE
//!
//! This arm was written with two: the slab-granular `live_physical_bytes` the published space ratio
//! divides by, and the block-granular live tally. A third has since landed -- the RECORDED one,
//! where an overwrite appends the byte range it retired to a per-slab list in the same call that
//! discharges the live tally, and `recorded_space_amplification_ppm` is published from it.
//!
//! THE SLAB-GRANULAR ASSERTION BELOW STILL READS 1.000x AND IS NOT LOOSENED. It was written to fail
//! and say what moved if the published denominator were ever re-pointed, and that has deliberately
//! NOT been done: re-pointing `space_amplification_ppm` would change a serde key consumers already
//! read, which the note on that field says must not happen silently. The correctly denominated
//! figure is published BESIDE it instead. So the assertion is still true of the field it names, and
//! what this arm gains is a third column that moves -- asserted, so that a record which stopped
//! recording would fail here as well as in its own arm.
//!
//! # EVERY RATIO DIVIDES STORED BYTES BY STORED BYTES
//!
//! There are two quantities in this store called logical. `logical_bytes_written` counts the user's
//! bytes as offered, BEFORE compression; a block's length -- what the live tally sums -- is what was
//! stored, AFTER it. A ratio with one on each side reports the compression factor multiplied by
//! whatever it claimed to measure. So the rewrite and space ratios here are stored-over-stored, and
//! the compression ratio is reported separately as the one quantity whose subject IS that mismatch.
//!
//! # THE VALUES ARE INCOMPRESSIBLE, AND THAT IS NOT INCIDENTAL
//!
//! `vec![b'v'; n]` compresses to almost nothing, which makes stored bytes framing rather than
//! content: on that fixture `live_physical_bytes` reads the same 70,000 B at value 256, 1,024 and
//! 4,096 alike, so a stored-byte denominator stops tracking the data. Every ratio here stands on
//! stored bytes, so the fixture must produce bytes that survive the write path.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_amplification_set_under_rewrite -- --ignored --nocapture

use crate::engine::reports::StorageManagerCycleRequest;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;
/// SEVERAL RECLAIM ROUNDS, NOT ONE. A compaction rewrites live data before releasing the copy it
/// replaces, so a single round can legitimately end holding more than it began with. Whether
/// rounds CONVERGE is the question, and one round cannot answer it.
const RECLAIM_ROUNDS: usize = 5;

/// Values that cannot compress, so a stored-byte denominator means something.
///
/// An inlined xorshift64 keyed by key index and round, so each round writes genuinely different
/// bytes -- a rewrite with the SAME bytes is a workload a store is allowed to recognise and skip,
/// which would make this arm measure deduplication instead of rewrite pressure.
fn noisy_value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

struct Snapshot {
    label: String,
    stored_written: u64,
    logical_written: u64,
    /// SLAB-GRANULAR, and this is the one the published space ratio divides by.
    ///
    /// `summarize_slabs` adds a slab's whole `physical_bytes` to it when the slab is `Active` or
    /// `Sealed`, so a slab counts fully live however many of its blocks are dead.
    live_slab_granular: u64,
    /// BLOCK-GRANULAR, from the maintained `BlockSlabLiveIndex` tally.
    ///
    /// `SlabLiveTally::bytes` sums `BlockAddress::length` over the blocks the index still points
    /// at, and a block leaves the tally when its index entry does -- so an overwrite decrements
    /// it. `None` when the tally has not been derived, because a provisional zero must not be
    /// divided by.
    live_block_granular: Option<u64>,
    physical: u64,
    reclaimable: u64,
    /// RECORDED, not derived: the bytes an overwrite, a delete or an expiry sweep retired and that
    /// nothing has collected yet, appended where the live tally is discharged. Needs no cycle to
    /// have run, which is the whole difference from the two columns above.
    pending_discard: u64,
    /// `recorded_space_amplification_ppm` as the shard PUBLISHES it, so this arm and the report are
    /// reading one number rather than two that agree by coincidence.
    published_recorded_space_ppm: u64,
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 { 0.0 } else { numerator as f64 / denominator as f64 }
}

impl Snapshot {
    /// Rewrite pressure as the published denominator computes it.
    fn rewrite_slab(&self) -> f64 {
        ratio(self.stored_written, self.live_slab_granular)
    }
    /// Rewrite pressure against the blocks the index still points at.
    fn rewrite_block(&self) -> f64 {
        ratio(self.stored_written, self.live_block_granular.unwrap_or(0))
    }
    /// Space as the published denominator computes it.
    fn space_slab(&self) -> f64 {
        ratio(self.physical, self.live_slab_granular)
    }
    /// Space against the blocks the index still points at.
    fn space_block(&self) -> f64 {
        ratio(self.physical, self.live_block_granular.unwrap_or(0))
    }
    /// Space from the RECORD: the live set plus what has been retired and not collected, over the
    /// live set. Both halves are sums of `ElementEntry::length`, so unlike the two above this one
    /// does not put a file size over a block-length total.
    fn space_recorded(&self) -> f64 {
        let live = self.live_block_granular.unwrap_or(0);
        ratio(live.saturating_add(self.pending_discard), live)
    }
    /// Stored bytes per user byte offered: COMPRESSION, the one ratio that mixes the units on
    /// purpose because that relationship is its subject.
    fn compression(&self) -> f64 {
        if self.logical_written == 0 {
            0.0
        } else {
            self.stored_written as f64 / self.logical_written as f64
        }
    }
}

fn snapshot(engine: &crate::TemporalEngine, label: impl Into<String>) -> Snapshot {
    let store = engine.block_store.stats();
    let slabs = engine.block_store.slab_summary();
    // THE BLOCK-GRANULAR FIGURE, read from the shard's own maintained tally rather than
    // recomputed here, so this arm and the GC's candidate selection are reading one number.
    let live_block_granular = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        if shard.bucket_index.block_slab_live.is_ready() {
            Some(
                shard
                    .bucket_index
                    .block_slab_live
                    .iter()
                    .fold(0_u64, |total, (_, tally)| total.saturating_add(tally.bytes)),
            )
        } else {
            None
        }
    };
    // THE RECORDED COLUMN, from the shard's own retired-range record and from the field the shard
    // publishes out of it. Read together so that a disagreement between them shows up here.
    let pending_discard = engine
        .recorded_stale_summary(1)
        .map(|stale| stale.pending_discard_bytes)
        .unwrap_or_default();
    let published_recorded_space_ppm = engine
        .shard_stats(1)
        .map(|stats| stats.storage.recorded_space_amplification_ppm)
        .unwrap_or_default();
    Snapshot {
        label: label.into(),
        stored_written: store.bytes_written,
        logical_written: store.logical_bytes_written,
        live_slab_granular: slabs.live_physical_bytes,
        live_block_granular,
        physical: slabs.total_known_physical_bytes,
        reclaimable: slabs.reclaimable_physical_bytes,
        pending_discard,
        published_recorded_space_ppm,
    }
}

#[test]
#[ignore = "writes 2,000 keys five times over and runs a reclaim cycle; run by name"]
fn the_amplification_set_under_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let mut shots = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("amp-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        shots.push(snapshot(&engine, if round == 0 {
            "after 1 write".to_string()
        } else {
            format!("after {} writes", round + 1)
        }));
    }

    // THE RECLAIM CYCLE, through the production round rather than by reaching into the store. Every
    // bucket is named because reclaim wants a dump manifest at each bucket's current generation,
    // and a bucket that went clean without being dumped at that generation has none.
    for round in 1..=RECLAIM_ROUNDS {
        // THE BUCKET SET IS RE-READ EACH ROUND. Reclaim wants a dump manifest matching each
        // bucket's CURRENT generation, and a round that compacts changes generations -- a set
        // captured once goes stale and later rounds would ask for manifests that do not exist.
        let buckets: Vec<u32> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            shard.bucket_index.bucket_map.keys().copied().collect()
        };
        assert!(
            !buckets.is_empty(),
            "round {round}: no bucket to dump, so the reclaim round has nothing to act on"
        );
        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_expire: true,
            enable_evict: false,
            enable_block_reclaim: true,
            enable_block_compaction: true,
            enable_index_gc: true,
            selected_dump_buckets: buckets.clone(),
            max_dump_buckets_per_round: buckets.len(),
            min_undumped_wal_records: 1,
            min_undumped_wal_bytes: 1,
            max_expire_hot_buckets_per_round:
                crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
            max_expire_cold_buckets_per_round:
                crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
            index_gc_max_entries_per_round:
                crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
            ..StorageManagerCycleRequest::default()
        });
        std::hint::black_box(&report);

        // THE PURGE IS A SEPARATE PHASE ON A SEPARATE REQUEST TYPE. `purge_delayed_destroy` is a
        // field of `StorageLifecycleRequest`, not of the cycle request, so a fixture that drives
        // only the cycle runs the compaction phase alone and can never free a quarantined slab --
        // however many rounds it runs. Driven here so that "nothing was freed" is a result rather
        // than an omission.
        let purge = engine.apply_storage_lifecycle(
            crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                selected_dump_buckets: buckets.clone(),
                max_dump_buckets_per_round: buckets.len(),
                purge_delayed_destroy: true,
                ..Default::default()
            },
        );
        let purge_metrics: Vec<String> = purge
            .storage_lifecycle_metrics
            .iter()
            .filter(|(name, value)| name.contains("purge") && **value > 0)
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        println!(
            "    round {round}: purge requested, non-zero purge metrics: {}",
            if purge_metrics.is_empty() {
                "NONE".to_string()
            } else {
                purge_metrics.join(" ")
            }
        );

        shots.push(snapshot(&engine, format!("after reclaim round {round}")));
    }

    println!();
    println!("  THE AMPLIFICATION SET UNDER REWRITE -- {KEYS} keys x {VALUE} B, incompressible");
    println!();
    println!(
        "    {:<22} {:>10} {:>10} {:>10} {:>10} {:>11} {:>7} {:>7} {:>7} {:>7} {:>7}",
        "",
        "stored wr",
        "live SLAB",
        "live BLOCK",
        "physical",
        "pending",
        "rw slab",
        "rw blk",
        "sp slab",
        "sp blk",
        "sp REC"
    );
    for shot in &shots {
        println!(
            "    {:<22} {:>10} {:>10} {:>10} {:>10} {:>11} {:>6.3}x {:>6.3}x {:>6.3}x {:>6.3}x {:>6.3}x",
            shot.label,
            shot.stored_written,
            shot.live_slab_granular,
            shot.live_block_granular
                .map(|bytes| bytes.to_string())
                .unwrap_or_else(|| "not ready".to_string()),
            shot.physical,
            shot.pending_discard,
            shot.rewrite_slab(),
            shot.rewrite_block(),
            shot.space_slab(),
            shot.space_block(),
            shot.space_recorded(),
        );
    }
    println!();
    println!(
        "    reclaimable stayed {} throughout; compression {:.3}x",
        shots.iter().map(|s| s.reclaimable).max().unwrap_or(0),
        shots[0].compression()
    );
    println!();

