// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot READS for each byte of applied log it DISCARDS, and what bounds it.
//!
//! #1939 measured a snapshot at 25,000 and 100,000 records and recorded, guarded but unfixed: a
//! snapshot is triggered by the LOG and reads the STORE, and those coincide exactly once -- the
//! first snapshot on a fresh store. On a 100,000-record store, discarding ONE log entry reads
//! 14,000,140 slab bytes. Per record of STORE everything is flat at 140, and that flatness is the
//! camouflage; per record of WORK it is 14,000,140.
//!
//! The READ is irreducible for the same kind of reason #1941 found the index dump's WRITE to be.
//! `a_receiving_peer_cannot_apply_an_increment` establishes it from the installers rather than by
//! assumption: every path that consumes a state image -- `rebuild_snapshot_engine`,
//! `build_installed_engine`, the restart restore -- starts from `TemporalEngine::default()` and
//! REPLACES, and none of them merges into what the receiver already holds. An image carrying only
//! what changed would therefore install as a shard containing only what changed, and the receiver
//! would serve a store with the rest silently missing. Incremental is not available without
//! changing what a receiver does, and `the_slab_read_is_what_a_snapshot_is` prices the read at
//! essentially all of the operation, so there is no second term to attack either.
//!
//! What is NOT irreducible is the CADENCE. Bytes read per byte discarded is a ratio and only its
//! numerator is the store; the denominator was a constant with no relation to the image a snapshot
//! must read to free it. `RaftConfig::snapshot_image_fraction_divisor` makes the configured
//! `max_applied_log_bytes` a FLOOR, raised to `last_image_bytes / divisor`, so a snapshot never
//! reads more than `divisor` bytes of image per byte of log it frees.
//!
//! ```text
//!   SLAB BYTES READ PER BYTE OF APPLIED LOG DISCARDED, store 10,000 -> 40,000
//!     FIXED    (control, divisor 0 -- today's cadence)   GROWS with the store
//!     RELATIVE (subject,  divisor 8)                     FLAT
//!     DISABLED (can_trigger_snapshot = false)            nothing read, nothing released
//! ```
//!
//! COUNTED, never timed, for the reason `snapshot_cost` gives: this box varies about 2.4x in wall
//! time across a day and these counters do not move with load at all.
//!
//! THE CONTROL ARM IS #1939 ITSELF. `force_the_threshold` now pins `snapshot_image_fraction_divisor
//! = 0`, so every measurement in `snapshot_cost` and `snapshot_large_store` goes on measuring the
//! constant cadence it recorded -- a recording of a defect must go on measuring the defect. Those
//! files are the FIXED arm at 25,000 and 100,000 records; this file is the RELATIVE arm beside it.

use super::snapshot_cost::{
    assert_the_fixture_is_populated, cluster_with, deployed_follower_with,
    force_the_threshold,
};
use super::*;
use crate::raft::cluster_snapshot::{
    effective_max_applied_log_bytes, SNAPSHOT_IMAGE_FRACTION_DIVISOR,
};
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records of STORE, at the two sizes every measurement here is taken at. A 4.00x step, as
/// `snapshot_cost` and `snapshot_large_store` use, so a ratio is unambiguous and a per-record
/// identity is worth three significant figures.
///
/// Smaller than #1939's 25,000 / 100,000 because this file runs four rounds per size rather than
/// two snapshots, and the RELATIVE arm must write `image / divisor` bytes of log before its
/// snapshot is admitted -- which is the whole point of it. #1939's own sizes are still measured,
/// by #1939's own module, which is this file's control arm.
const STORE_SMALL: usize = 10_000;
const STORE_LARGE: usize = 40_000;

/// The configured floor every arm here runs with. A round number well above the noise of a single
/// batch and far enough below the crossover that the FIXED arm is genuinely constant at both
/// sizes: at 10,000 records the last image is about 2.0 MB and an eighth of it is about 258,000
/// bytes, so the relative term binds at both sizes and the floor binds at neither.
const FIXED_FLOOR_BYTES: u64 = 16_384;

/// Records appended between one ask of the production cadence and the next.
const BATCH: usize = 250;

/// A ceiling on how many batches one round may write before its cadence fires. A round that never
/// fires is a fixture failure, not a result, and it must say so rather than run forever.
const MAX_BATCHES_PER_ROUND: usize = 400;

/// Slab bytes a record of the fixture corpus occupies. `snapshot_cost` measures 140 at 1,000 and
/// 4,000; `snapshot_large_store` measures 140 at 25,000 and 100,000. Asserted here too.
const SLAB_BYTES_PER_RECORD: u64 = 140;

/// Which cadence a round ran under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cadence {
    /// Today's shipped behaviour: a constant byte threshold, unrelated to the image a snapshot
    /// will read. This is the CONTROL, and it fails below if it ever goes flat.
    Fixed,
    /// The subject: the configured floor raised to `last_image_bytes / divisor`.
    Relative,
}

impl Cadence {
    fn divisor(self) -> u64 {
        match self {
            Cadence::Fixed => 0,
            Cadence::Relative => SNAPSHOT_IMAGE_FRACTION_DIVISOR,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Cadence::Fixed => "FIXED   ",
            Cadence::Relative => "RELATIVE",
        }
    }
}

/// One round: write until the production cadence fires, and price the snapshot it fired.
#[derive(Debug, Clone, Copy)]
struct Round {
    cadence: Cadence,
    /// Records of STORE the shard held when the snapshot fired.
    store_records: u64,
    /// Bytes of applied log the snapshot discarded, read off the trigger report.
    released_bytes: u64,
    /// Log ENTRIES the snapshot discarded -- `applied_index - last_snapshot_index`.
    released_entries: u64,
    /// The threshold that actually decided, read off the trigger report.
    threshold_bytes: u64,
    /// The highest the applied log stood at any ask during this round. This is the trade.
    log_high_water_bytes: u64,
    counts: SnapshotCounts,
    image_payload_bytes: u64,
    image_index_bytes: u64,
    image_slabs: u64,
}

impl Round {
    /// The figure this file is about: slab bytes READ for each byte of applied log DISCARDED.
    fn read_per_byte_released(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.released_bytes.max(1) as f64
    }

    fn read_per_record_of_store(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.store_records.max(1) as f64
    }

    fn read_per_record_of_work(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.released_entries.max(1) as f64
    }

    fn print(&self) {
        println!(
            "  {} store={:>7} threshold={:>10} released={:>9}B/{:>6}e read={:>10}B \
             index={:>9}B image={:>10}B slabs={:>3} log_hw={:>9}B rebuilds={} publishes={} \
             image_builds={}",
            self.cadence.label(),
            self.store_records,
            self.threshold_bytes,
            self.released_bytes,
            self.released_entries,
            self.counts.slab_read_bytes,
            self.image_index_bytes,
            self.image_payload_bytes,
            self.image_slabs,
            self.log_high_water_bytes,
            self.counts.engine_rebuilds,
            self.counts.engine_publishes,
            self.counts.image_builds,
        );
        println!(
            "            READ PER BYTE RELEASED = {:>12.3}   per record of STORE = {:>9.3}   \
             per record of WORK = {:>14.3}",
            self.read_per_byte_released(),
            self.read_per_record_of_store(),
            self.read_per_record_of_work(),
        );
    }
}

/// Append `count` further records, keeping the key shape -- and therefore the key LENGTH --
/// identical to `cluster_with`'s. A record's slab bytes carry its key, so an arm that widened the
/// key by one character would move every byte row without anything on the snapshot path changing.
fn append_records(cluster: &RaftCluster, from: usize, count: usize) {
    for index in from..from + count {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
    }
}

/// Put the cluster on `cadence` with the shared floor, and take the catch-up hold out of the way.
///
/// The hold (`max_retained_log_bytes`) is forced off exactly as `snapshot_cost` forces it off: a
/// lagging peer otherwise holds compaction and the fixture measures "nothing happened". It is a
/// production behaviour and it is not what is under test here.
fn set_cadence(cluster: &RaftCluster, cadence: Cadence, floor: u64) {
    let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
    inner.config.can_trigger_snapshot = true;
    inner.config.max_applied_log_bytes = floor;
    inner.config.max_retained_log_bytes = 0;
    inner.config.snapshot_image_fraction_divisor = cadence.divisor();
}

/// Read the image the snapshot just published, off the node it was published into.
fn installed_image(cluster: &RaftCluster) -> (u64, u64, u64) {
    let installed = cluster
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .get(&1)
        .and_then(|node| node.installed_snapshot.clone())
        .expect("a snapshot that triggered must leave an installed snapshot on the leader");
    let image = installed
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    (
        image.payload_bytes() as u64,
        image.index_bytes.len() as u64,
        image.slabs.len() as u64,
    )
}

/// Write in batches of `BATCH` until the PRODUCTION cadence fires, then price the snapshot it
/// fired. `written` is how many records the store already holds; the new total is returned.
fn run_round(
    cluster: &RaftCluster,
    cadence: Cadence,
    floor: u64,
    written: &mut usize,
) -> (Round, usize) {
    set_cadence(cluster, cadence, floor);
    let mut high_water = 0u64;
    let mut batches = 0usize;
    loop {
        assert!(
            batches < MAX_BATCHES_PER_ROUND,
            "{} round wrote {} records without its cadence firing. That is a fixture failure, \
             not a result: every figure below divides by a snapshot that never happened",
            cadence.label(),
            batches * BATCH
        );
        append_records(cluster, *written, BATCH);
        *written += BATCH;
        batches += 1;
        // Reset immediately before the ask, so the counters hold exactly the snapshot this ask
        // performs. An ask that refuses builds no image and reads no slab, so the resets before
        // the refusals cost nothing and discard nothing.
        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        high_water = high_water.max(report.applied_log_bytes);
        if report.triggered {
            let counts = snapshot_probe::counts();
            let (payload, index_bytes, slabs) = installed_image(cluster);
            let round = Round {
                cadence,
                store_records: *written as u64,
                released_bytes: report.applied_log_bytes,
                released_entries: report
                    .applied_index
                    .checked_sub(report.last_snapshot_index)
                    .expect("applied_index must be at or above the last snapshot index"),
                threshold_bytes: report.max_applied_log_bytes,
                log_high_water_bytes: high_water,
                counts,
                image_payload_bytes: payload,
                image_index_bytes: index_bytes,
                image_slabs: slabs,
            };
            round.print();
            return (round, batches);
        }
    }
}

/// Every under-guard result #1912 and #1914 established, re-derived on this round rather than
/// inherited, plus the vacuity check that stops a zero meaning "nothing installed at all".
fn assert_the_guard_results_hold(round: &Round, where_: &str) {
    assert_eq!(
        round.counts.engine_rebuilds_under_guard, 0,
        "{where_}: #1914's result must hold under this cadence -- no engine may be rebuilt under \
         the cluster write guard, and {} were",
        round.counts.engine_rebuilds_under_guard
    );
    assert_eq!(
        round.counts.slab_install_bytes_under_guard, 0,
        "{where_}: #1912's result must hold under this cadence -- no slab byte may be written \
         under the cluster write guard, and {} were",
        round.counts.slab_install_bytes_under_guard
    );
    assert_eq!(
        round.counts.image_clone_bytes_under_guard, 0,
        "{where_}: #1912's result must hold under this cadence -- no image byte may be copied \
         under the cluster write guard, and {} were",
        round.counts.image_clone_bytes_under_guard
    );
    assert!(
        round.counts.engine_rebuilds > 0 && round.counts.engine_publishes > 0,
        "{where_}: {} rebuilds and {} publishes. A snapshot that installed into nobody would \
         satisfy every under-guard assertion above by doing nothing at all",
        round.counts.engine_rebuilds,
        round.counts.engine_publishes
    );
    assert_eq!(
        round.counts.image_builds, 1,
        "{where_}: one snapshot must build exactly one state image. More than one means the \
         three-attempt loop in create_state_image_snapshot RETRIED -- a path #1912 and #1939 both \
         measured as never taken anywhere in the tree, and one this change could reach by making \
         the build rarer and larger. If this fires, that path is UNTESTED and must be said so: {}",
        round.counts.image_builds
    );
    assert_eq!(
        round.counts.slab_reads, round.image_slabs,
        "{where_}: the build must read each carried slab exactly once -- {} reads for {} slabs",
        round.counts.slab_reads, round.image_slabs
    );
    assert_eq!(
        round.counts.slab_read_bytes,
        SLAB_BYTES_PER_RECORD * round.store_records,
        "{where_}: slab bytes read must still be exactly {SLAB_BYTES_PER_RECORD} per record of \
         STORE, and were {}. The cadence changes HOW OFTEN a snapshot runs and nothing about what \
         one reads; if this moves, the read has changed and every ratio here is against a \
         different subject",
        round.counts.slab_read_bytes
    );
}

/// THE MEASUREMENT. Both cadences, both sizes, in ABBA order on ONE store per size.
///
/// FIXED, RELATIVE, RELATIVE, FIXED -- so the store's growth through the sequence falls on both
/// cadences rather than on whichever went last, and both are priced against the same fixture.
///
/// The FIXED arm is the CONTROL and it is asserted to GROW. A control that has gone flat is an
/// apparatus that can no longer show the defect, and it would report the subject healthy for the
/// wrong reason.
#[test]
fn what_a_snapshot_reads_per_byte_of_log_it_discards_under_each_cadence() {
    let mut fixed_first = Vec::new();
    let mut relative = Vec::new();

    for store_records in [STORE_SMALL, STORE_LARGE] {
        println!("=== store={store_records} ===");
        let cluster = cluster_with(store_records);
        let fixture = assert_the_fixture_is_populated(&cluster, store_records);
        println!(
            "  fixture payload={} slabs={} alive_nodes={}",
            fixture.payload_bytes, fixture.slabs, fixture.alive_nodes
        );

        // Prime: one snapshot under the FIXED cadence, so every round below is a ROUTINE
        // snapshot with an installed image behind it. Without this the first round would be the
        // WHOLE regime -- the one case where the log discarded IS the store -- and the two
        // cadences would be measured on different regimes.
        force_the_threshold(&cluster);
        let priming = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        assert!(
            priming.triggered,
            "the priming snapshot did not fire ({}), so every round below would measure the \
             WHOLE regime rather than the routine one",
            priming.reason
        );

        let mut written = store_records;
        let mut rounds = Vec::new();
        for cadence in [
            Cadence::Fixed,
            Cadence::Relative,
            Cadence::Relative,
            Cadence::Fixed,
        ] {
            let (round, batches) = run_round(&cluster, cadence, FIXED_FLOOR_BYTES, &mut written);
            println!("            (batches written this round: {batches})");
            assert_the_guard_results_hold(
                &round,
                &format!("{} at store={}", cadence.label(), round.store_records),
            );
            rounds.push(round);
        }

        // The live set must still be more than one slab AFTER an install, because every round
        // here measures a REBUILT engine and a rebuild that collapsed the store into one slab
        // would not be expressing what the fixture was built to express. #1939's own note.
        let live_after = cluster
            .node_engine_for_test(1)
            .expect("the leader serves an engine")
            .live_block_slab_ids(1);
        println!("  live slab ids after the rounds = {live_after:?}");
        assert!(
            live_after.len() > 1,
            "the shard's live slab set after these installs is {live_after:?}: with one slab a \
             per-slab cost and a per-snapshot cost are the same number"
        );
        let tallies = cluster
            .node_engine_for_test(1)
            .expect("the leader serves an engine")
            .block_slab_live_tallies(1)
            .expect("the leader's live tally must be derived");
        let holding = tallies.iter().filter(|(_, _, bytes)| *bytes > 0).count();
        println!("  slabs holding live bytes       = {holding} of {}", tallies.len());
        assert!(
            holding > 1,
            "only {holding} slab holds live bytes: a cost spread over slabs cannot be told from \
             a cost concentrated in one"
        );

        fixed_first.push(rounds[0]);
        relative.push(rounds[1]);
    }

    let (fixed_small, fixed_large) = (fixed_first[0], fixed_first[1]);
    let (rel_small, rel_large) = (relative[0], relative[1]);

    println!("=== READ PER BYTE RELEASED, store {STORE_SMALL} -> {STORE_LARGE} ===");
    let fixed_ratio = fixed_large.read_per_byte_released() / fixed_small.read_per_byte_released();
    let rel_ratio = rel_large.read_per_byte_released() / rel_small.read_per_byte_released();
    println!(
        "  FIXED    (control)  {:>10.3} -> {:>10.3}   ({fixed_ratio:.3}x)",
        fixed_small.read_per_byte_released(),
        fixed_large.read_per_byte_released()
    );
    println!(
        "  RELATIVE (subject)  {:>10.3} -> {:>10.3}   ({rel_ratio:.3}x), divisor {}",
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released(),
        SNAPSHOT_IMAGE_FRACTION_DIVISOR
    );

    // ---- EVERY QUANTITY, FLAT OR GROWING ----
    println!("=== EVERY QUANTITY, FLAT OR GROWING (small -> large) ===");
    for (label, small, large) in [
        (
            "slab bytes read          ",
            fixed_small.counts.slab_read_bytes,
            fixed_large.counts.slab_read_bytes,
        ),
        (
            "addresses walked         ",
            fixed_small.counts.live_slab_scan_addresses,
            fixed_large.counts.live_slab_scan_addresses,
        ),
        (
            "slab reads               ",
            fixed_small.counts.slab_reads,
            fixed_large.counts.slab_reads,
        ),
        (
            "slab directory listings  ",
            fixed_small.counts.slab_dir_listings,
            fixed_large.counts.slab_dir_listings,
        ),
        (
            "state images built       ",
            fixed_small.counts.image_builds,
            fixed_large.counts.image_builds,
        ),
        (
            "engine rebuilds          ",
            fixed_small.counts.engine_rebuilds,
            fixed_large.counts.engine_rebuilds,
        ),
        (
            "engine publishes         ",
            fixed_small.counts.engine_publishes,
            fixed_large.counts.engine_publishes,
        ),
        (
            "rebuilds under the guard ",
            fixed_small.counts.engine_rebuilds_under_guard,
            fixed_large.counts.engine_rebuilds_under_guard,
        ),
        (
            "slab bytes under guard   ",
            fixed_small.counts.slab_install_bytes_under_guard,
            fixed_large.counts.slab_install_bytes_under_guard,
        ),
        (
            "image bytes under guard  ",
            fixed_small.counts.image_clone_bytes_under_guard,
            fixed_large.counts.image_clone_bytes_under_guard,
        ),
        (
            "FIXED    released bytes  ",
            fixed_small.released_bytes,
            fixed_large.released_bytes,
        ),
        (
            "FIXED    threshold       ",
            fixed_small.threshold_bytes,
            fixed_large.threshold_bytes,
        ),
        (
            "RELATIVE released bytes  ",
            rel_small.released_bytes,
            rel_large.released_bytes,
        ),
        (
            "RELATIVE threshold       ",
            rel_small.threshold_bytes,
            rel_large.threshold_bytes,
        ),
        (
            "RELATIVE log high water  ",
            rel_small.log_high_water_bytes,
            rel_large.log_high_water_bytes,
        ),
        (
            "FIXED    log high water  ",
            fixed_small.log_high_water_bytes,
            fixed_large.log_high_water_bytes,
        ),
    ] {
        let verdict = if small == large { "FLAT" } else { "GROWING" };
        println!("  {label} {small:>12} -> {large:>12}   {verdict}");
    }
    println!(
        "  FIXED    read per byte released  {:>12.3} -> {:>12.3}   GROWING",
        fixed_small.read_per_byte_released(),
        fixed_large.read_per_byte_released()
    );
    println!(
        "  RELATIVE read per byte released  {:>12.3} -> {:>12.3}   FLAT",
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released()
    );

    // ---- THE CONTROL ARM, WHICH FAILS IF IT GOES FLAT ----
    assert!(
        fixed_ratio > 2.0,
        "CONTROL ARM FIXED did not grow: {:.3} at store={} against {:.3} at store={}, a ratio of \
         {fixed_ratio:.3} over a {:.3}x store. This arm IS #1939's finding, kept alive so the \
         subject's flat reading keeps meaning something. A control that has gone flat is an \
         apparatus that can no longer show the defect, and the subject below would then read \
         healthy for the wrong reason",
        fixed_small.read_per_byte_released(),
        fixed_small.store_records,
        fixed_large.read_per_byte_released(),
        fixed_large.store_records,
        fixed_large.store_records as f64 / fixed_small.store_records as f64,
    );
    assert_eq!(
        fixed_small.threshold_bytes, FIXED_FLOOR_BYTES,
        "the FIXED arm's threshold must be the configured constant and nothing else: {}",
        fixed_small.threshold_bytes
    );
    assert_eq!(
        fixed_large.threshold_bytes, FIXED_FLOOR_BYTES,
        "the FIXED arm's threshold must be the configured constant and nothing else: {}",
        fixed_large.threshold_bytes
    );

    // ---- THE SUBJECT ----
    assert!(
        rel_ratio > 0.8 && rel_ratio < 1.25,
        "RELATIVE is not flat across a {:.3}x store: {:.3} -> {:.3}, a ratio of {rel_ratio:.3}. \
         The claim of this change is that the figure stops moving with the store, and it moved",
        rel_large.store_records as f64 / rel_small.store_records as f64,
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released(),
    );
    for round in [rel_small, rel_large] {
        assert!(
            round.threshold_bytes > FIXED_FLOOR_BYTES,
            "the RELATIVE arm at store={} was decided by the configured floor ({}), not by the \
             relative term. Below the crossover this change does nothing at all, which is correct \
             behaviour and a vacuous measurement: this arm must be ABOVE it",
            round.store_records,
            round.threshold_bytes
        );
        // The image is read BEFORE the round, and the store grows by the round's own writes
        // while the threshold that admitted it stays where it was -- so the cost being divided is
        // the larger one and the figure sits a little above the divisor rather than at it. What
        // matters is that it does not move with the store, which is asserted above.
        assert!(
            round.read_per_byte_released() < (SNAPSHOT_IMAGE_FRACTION_DIVISOR * 2) as f64,
            "the RELATIVE arm at store={} read {:.3} slab bytes per byte released, against a \
             divisor of {SNAPSHOT_IMAGE_FRACTION_DIVISOR}. The bound is on the IMAGE, of which \
             the slab bytes are a part, so this figure must sit below the divisor plus the \
             round's own growth -- well inside twice it",
            round.store_records,
            round.read_per_byte_released(),
        );
    }
    assert!(
        fixed_large.read_per_byte_released() > rel_large.read_per_byte_released() * 2.0,
        "at store={} the FIXED cadence read {:.3} bytes per byte released and the RELATIVE one \
         {:.3}. With the two arms that close together this fixture is not expressing the \
         difference it was built to express",
        fixed_large.store_records,
        fixed_large.read_per_byte_released(),
        rel_large.read_per_byte_released(),
    );
}

/// WHAT THE SLAB READ IS AS A FRACTION OF THE OPERATION -- the question that decides which fix is
/// available.
///
/// #1941 found its subject was 99.43% of the dump, which is what made "cut the cadence, not the
/// work" the right call there. The same accounting here, in bytes, at both sizes: everything a
/// snapshot moves, and the share of it that is priced by the STORE rather than by the ROUND.
#[test]
fn the_slab_read_is_what_a_snapshot_is() {
    let mut rows = Vec::new();
    for store_records in [STORE_SMALL, STORE_LARGE] {
        let cluster = cluster_with(store_records);
        assert_the_fixture_is_populated(&cluster, store_records);
        force_the_threshold(&cluster);
        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        assert!(report.triggered, "the snapshot did not fire: {}", report.reason);
        let counts = snapshot_probe::counts();
        let (payload, index_bytes, slabs) = installed_image(&cluster);

        // Everything a snapshot MOVES, in bytes. The read is the build's half; the installs are
        // what the rebuilt engines write; the copies are the per-node images.
        let read = counts.slab_read_bytes;
        let encoded = index_bytes;
        let installed = counts.slab_install_bytes;
        let copied = counts.image_clone_bytes;
        let total = read + encoded + installed + copied;
        println!("=== WHAT A SNAPSHOT MOVES, store={store_records} ===");
        println!("  {read:>12} B  slab bytes READ by the build");
        println!("  {encoded:>12} B  served index ENCODED by the build");
        println!("  {installed:>12} B  slab bytes INSTALLED into the rebuilt engines");
        println!("  {copied:>12} B  image bytes COPIED per install target");
        println!("  {total:>12} B  total");
        println!(
            "  THE BUILD'S WHOLE-STORE READ IS {:.2}% OF THE BYTES, AND THE SLAB READ ALONE IS \
             {:.2}%",
            (read + encoded) as f64 * 100.0 / total.max(1) as f64,
            read as f64 * 100.0 / total.max(1) as f64,
        );
        println!(
            "  image payload = {payload} B over {slabs} slabs; every one of these four terms is \
             priced by the STORE"
        );
        assert!(total > 0, "a snapshot that moved no bytes measures nothing");
        assert_eq!(
            read,
            SLAB_BYTES_PER_RECORD * store_records as u64,
            "the read must be {SLAB_BYTES_PER_RECORD} slab bytes per record of store"
        );
        rows.push((store_records as u64, read, encoded, installed, copied, total));
    }

    // Every term scales with the store, which is the finding: there is no second term bounded by
    // the round, so there is nothing to remove from the work. Asserted as a ratio against the
    // store ratio rather than as a trend.
    let (small, large) = (rows[0], rows[1]);
    let store_ratio = large.0 as f64 / small.0 as f64;
    for (label, s, l) in [
        ("slab read     ", small.1, large.1),
        ("index encode  ", small.2, large.2),
        ("slab installs ", small.3, large.3),
        ("image copies  ", small.4, large.4),
        ("total         ", small.5, large.5),
    ] {
        let ratio = l as f64 / s.max(1) as f64;
        println!("  {label} {s:>12} -> {l:>12}   {ratio:.3}x against a store ratio of {store_ratio:.3}x");
        assert!(
            ratio > store_ratio * 0.75,
            "{label} grew only {ratio:.3}x over a {store_ratio:.3}x store. If some term of a \
             snapshot were bounded by the ROUND rather than by the store there would be something \
             to remove from the work, and this file's argument for cutting the cadence instead \
             would not hold"
        );
    }
    assert_eq!(
        small.1 * large.0,
        large.1 * small.0,
        "the slab read is not exactly proportional to the store: {} at {} against {} at {}",
        small.1,
        small.0,
        large.1,
        large.0
    );
}

/// WHY INCREMENTAL IS NOT AVAILABLE -- established from the installers, not assumed.
///
/// Every path that consumes a state image builds a FRESH `TemporalEngine` and replaces with it;
/// none merges into what the receiver already holds. So an image carrying only what changed
/// installs as a shard containing only what changed, and the receiver serves a store with the
/// rest silently missing -- which is precisely the failure a snapshot exists to prevent and the
/// one nothing would report.
///
/// Constructed rather than argued: a snapshot is built, its image is reduced to a PROPER SUBSET of
/// its slabs, an engine is rebuilt from it, and the records in the dropped slab are asked for.
#[test]
fn a_receiving_peer_cannot_apply_an_increment() {
    let records = 2_000usize;
    let cluster = cluster_with(records);
    assert_the_fixture_is_populated(&cluster, records);

    let whole = cluster.create_snapshot().expect("create_snapshot must succeed");
    let image = whole
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    assert!(
        image.slabs.len() > 1,
        "the image carries {} slab(s): a PROPER subset of one slab is the empty set, and this \
         test would be dropping everything rather than dropping an increment's complement",
        image.slabs.len()
    );

    // A peer built from the WHOLE image serves every record. This is the positive control: it is
    // what makes the negative below a statement about the missing slab rather than about the
    // rebuild being broken in general.
    let whole_peer = crate::raft::rebuild_snapshot_engine(&whole);
    let mut served_by_whole = 0usize;
    for index in 0..records {
        if read_back(&whole_peer, &format!("key-{index:08}")).is_some() {
            served_by_whole += 1;
        }
    }
    println!("  whole image: {served_by_whole} of {records} records served");
