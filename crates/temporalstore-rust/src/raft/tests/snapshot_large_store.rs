// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one snapshot costs once the store is LARGE, and what that cost is priced by.
//!
//! `snapshot_cost` establishes the shape of a snapshot at 1,000 and 4,000 records: one walk, one
//! encode, one read per slab, and -- since #1914 -- nothing under the cluster write guard. Every
//! one of those is a statement about MULTIPLICITY, and multiplicity is exactly the part of a cost
//! that a small store can tell you about. What a small store cannot tell you is what the cost is
//! priced BY, because on a fresh store of N records the first snapshot discards N log entries and
//! reads N records of store, and those two numbers are the same number.
//!
//! This file separates them. A snapshot is triggered by the LOG -- `max_applied_log_bytes` of
//! applied entries have piled up and want discarding -- and what it reads is the STORE. Those
//! coincide once, on the first snapshot of a fresh store, and never again. So every quantity here
//! is divided by two denominators and both are printed:
//!
//!   * per record of STORE, which is the denominator a whole-store cost hides inside, and
//!   * per record of WORK, which is the log this particular snapshot was triggered to discard.
//!
//! Measured on a 3-node in-process cluster, 128-byte values, `STORE_SMALL` and `STORE_LARGE`
//! records of store, in two regimes -- WHOLE, the first snapshot on a fresh store, and INCREMENT,
//! a second snapshot taken after `INCREMENT_RECORDS` further records, which is the routine case a
//! deployed node spends its life in:
//!
//! ```text
//!                                       WHOLE                     INCREMENT
//!                               25,000      100,000       25,000        100,000
//!   log entries discarded       25,000      100,000            1              1
//!   block addresses walked      25,000      100,000       25,001        100,001
//!   slab bytes read          3,500,000   14,000,000    3,500,140     14,000,140
//!     per record of STORE           140          140          140            140
//!     per record of WORK            140          140    3,500,140     14,000,140
//!   slab reads                       4            4            4              4
//!   served-index bytes       1,651,207    6,660,688    1,519,149      6,125,409
//!   whole image bytes        5,151,207   20,660,688    5,019,289     20,125,549
//!   state images built               1            1            1              1
//!   engine rebuilds                  3            3            3              3
//!   ... under the cluster write guard
//!   engine rebuilds                  0            0            0              0
//!   slab bytes re-installed          0            0            0              0
//!   image bytes copied               0            0            0              0
//! ```
//!
//! Every row above is an exact identity except two: the served-index and whole-image bytes in the
//! INCREMENT arm. Those are the index of a REBUILT engine -- the first snapshot replaced the
//! leader's engine with one reconstructed from its own image -- and they move by about 0.02%
//! between runs of the same code, so nothing asserts them and they are here as scale only. Every
//! figure this file does assert is an identity against something the snapshot path did not
//! compute.
//!
//! The left half is flat under both denominators and says nothing is wrong. The right half is the
//! same store, the same code and one entry of work, and the two denominators disagree by the size
//! of the store. That disagreement is the finding: a snapshot does not read the log it is about to
//! discard, it reads everything the shard holds, and it does that whether the log behind it is one
//! entry or all of them.
//!
//! COUNTED, never timed, for the reason `snapshot_cost` gives: this box varies about 2.4x in wall
//! time across a day and these counters do not move with load at all.
//!
//! Three things follow, and each has a test below.
//!
//! IS A SNAPSHOT BOUNDED IN WHAT IT READS? No. `max_applied_log_bytes` is the only bound on this
//! path and it decides WHETHER a snapshot happens, not how much of the store it touches; once it
//! fires, `build_state_image` reads every slab in the shard's live set and every address the
//! served index holds. It does all the work it finds. `the_snapshot_bound_decides_whether_it_runs_
//! not_how_much_it_reads` holds the store still and moves the bound, and the bytes do not move.
//!
//! WHAT DOES THE TOTAL COME TO? A flat per-snapshot cost over a store that grows between snapshots
//! is a quadratic total. At a fixed cadence of one snapshot per `step` records, reaching N records
//! costs `140 * step * R * (R + 1) / 2` slab bytes read across `R = N / step` snapshots, which
//! `what_a_store_pays_in_total_to_reach_a_size_grows_with_the_square_of_it` asserts as an exact
//! identity rather than as a trend.
//!
//! DOES A PEER COST MORE TO HELP THE FURTHER BEHIND IT IS? No -- and this is a refutation, stated
//! as one. Nothing on this path reads the receiving node at all except to ask whether it may take
//! the snapshot, so the bytes are identical at any lag, and the cost PER ENTRY of lag therefore
//! FALLS as the peer falls further behind. The expensive peer to help is the one that is barely
//! behind. `a_further_behind_peer_costs_no_more_to_help_than_a_barely_behind_one` measures both.

use super::snapshot_cost::{
    assert_the_fixture_is_populated, cluster_with, force_the_threshold, print_counts,
};
use super::*;
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records of STORE, at the two large sizes. A 4.00x step, as `snapshot_cost` uses, so a per-
/// record identity is worth three significant figures and a ratio is unambiguous.
///
/// These are the sizes the whole file runs at, and they are what makes it a large-store
/// measurement rather than a repeat of `snapshot_cost`: the shape this file is looking for -- a
/// cost priced by the store rather than by the work -- is arithmetically present at 1,000 records
/// too, but at 1,000 records it is 1,000x and at 200,000 records it is 200,000x, and only one of
/// those is a thing anyone would notice on a live cluster.
const STORE_SMALL: usize = 25_000;
const STORE_LARGE: usize = 100_000;

/// Records applied between the first snapshot and the second, in the INCREMENT regime.
///
/// ONE. The point of the regime is the smallest backlog a snapshot can legally be triggered on,
/// because the question is whether the path can tell a small backlog from a large one, and one
/// entry against the whole store is the sharpest form of the answer.
const INCREMENT_RECORDS: usize = 1;

/// Slab bytes a record of the fixture corpus occupies, asserted rather than assumed at every size.
/// `snapshot_cost` measures the same 140 at 1,000 and 4,000.
const SLAB_BYTES_PER_RECORD: u64 = 140;

/// Which regime a row was measured in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Regime {
    /// The first snapshot on a fresh store. The log it discards IS the store, so the two
    /// denominators coincide and every per-record figure is flat. This is the control.
    Whole,
    /// A second snapshot, taken after `INCREMENT_RECORDS` further records. The log it discards is
    /// `INCREMENT_RECORDS` entries and the store is everything. This is the subject.
    Increment,
}

impl Regime {
    fn label(self) -> &'static str {
        match self {
            Regime::Whole => "WHOLE",
            Regime::Increment => "INCREMENT",
        }
    }
}

/// One measurement: the counters, and the two denominators they are divided by.
#[derive(Debug, Clone, Copy)]
struct Row {
    regime: Regime,
    store_records: u64,
    /// Log entries this snapshot discarded -- `applied_index - last_snapshot_index`, read off the
    /// trigger report rather than inferred. This is the WORK.
    work_entries: u64,
    counts: SnapshotCounts,
    image_payload_bytes: u64,
    image_slabs: u64,
    image_index_bytes: u64,
}

impl Row {
    fn per_record_of_store(self, quantity: u64) -> f64 {
        quantity as f64 / self.store_records.max(1) as f64
    }

    fn per_record_of_work(self, quantity: u64) -> f64 {
        quantity as f64 / self.work_entries.max(1) as f64
    }

    fn print(&self) {
        println!(
            "  {:9} store={:>7} work={:>7} addresses={:>7} slab_reads={:>3} slab_bytes={:>10} \
             index_bytes={:>9} image_bytes={:>10} slabs={:>3} rebuilds={} publishes={} \
             image_builds={}",
            self.regime.label(),
            self.store_records,
            self.work_entries,
            self.counts.live_slab_scan_addresses,
            self.counts.slab_reads,
            self.counts.slab_read_bytes,
            self.image_index_bytes,
            self.image_payload_bytes,
            self.image_slabs,
            self.counts.engine_rebuilds,
            self.counts.engine_publishes,
            self.counts.image_builds,
        );
        println!(
            "            slab bytes per record of STORE = {:>12.3}   per record of WORK = {:>12.3}",
            self.per_record_of_store(self.counts.slab_read_bytes),
            self.per_record_of_work(self.counts.slab_read_bytes),
        );
        println!(
            "            addresses  per record of STORE = {:>12.3}   per record of WORK = {:>12.3}",
            self.per_record_of_store(self.counts.live_slab_scan_addresses),
            self.per_record_of_work(self.counts.live_slab_scan_addresses),
        );
    }
}

/// Append `count` further records to a cluster that already holds `from` of them, keeping the key
/// shape -- and therefore the key LENGTH -- identical to `cluster_with`'s.
///
/// Key length is held constant on purpose across every arm in this file. A record's slab bytes
/// include its key, so an arm that widened the key by one character would move the slab-bytes
/// rows without anything on the snapshot path having changed.
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

/// The log entries a trigger report says this snapshot discarded.
fn work_entries(report: &RaftSnapshotTriggerReport) -> u64 {
    assert!(
        report.triggered,
        "the snapshot did not fire ({}), so there is no work to divide by and every per-work \
         figure below would be a division of a real cost by a fixture that never ran",
        report.reason
    );
    report
        .applied_index
        .checked_sub(report.last_snapshot_index)
        .expect("applied_index must be at or above the last snapshot index when one fires")
}

/// What the leader's engine currently holds, measured independently of the snapshot path: the
/// shard's live slab set straight off the engine.
fn live_slabs(cluster: &RaftCluster) -> Vec<u64> {
    let mut live = cluster
        .node_engine_for_test(1)
        .expect("the leader serves an engine")
        .live_block_slab_ids(1);
    live.sort_unstable();
    live
}

/// Run ONE `maybe_trigger_snapshot` with the counters reset around it, and return the row.
fn measure_one_snapshot(cluster: &RaftCluster, regime: Regime, store_records: usize) -> Row {
    snapshot_probe::reset();
    let report = cluster
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    let counts = snapshot_probe::counts();
    let work = work_entries(&report);

    // The image the snapshot published, read back off the node it was published into, so the
    // payload figures come from the structure rather than from the counters that measured it.
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

    let row = Row {
        regime,
        store_records: store_records as u64,
        work_entries: work,
        counts,
        image_payload_bytes: image.payload_bytes() as u64,
        image_slabs: image.slabs.len() as u64,
        image_index_bytes: image.index_bytes.len() as u64,
    };
    row.print();
    row
}

/// A snapshot reads the whole store however little log it was triggered to discard.
///
/// TWO REGIMES, each at both sizes. WHOLE is the CONTROL ARM: the first snapshot on a fresh store,
/// where the log discarded and the store read are the same records, so every per-record figure is
/// flat under BOTH denominators and the instrument is shown reading a genuinely proportional cost
/// as proportional. INCREMENT is the SUBJECT: the same store, one entry of work.
///
/// The subject's assertion is deliberately written so that it FAILS IF THE COST EVER GOES FLAT.
/// That is not a mistake. What is recorded here is a refutation of the idea that this path is
/// priced by its work, and a bounded read would refute the recording -- so the day someone bounds
/// it, this test must go red and be re-derived rather than quietly keep passing on `0 == 0`.
#[test]
fn a_snapshot_reads_the_whole_store_however_little_log_it_discards() {
    let mut whole = Vec::new();
    let mut increment = Vec::new();

    for store_records in [STORE_SMALL, STORE_LARGE] {
        println!("=== store={store_records} ===");
        let cluster = cluster_with(store_records);
        let fixture = assert_the_fixture_is_populated(&cluster, store_records);
        println!("  live slab ids before = {:?}", live_slabs(&cluster));
        force_the_threshold(&cluster);

        // REGIME WHOLE -- the control. First snapshot on a fresh store.
        let row = measure_one_snapshot(&cluster, Regime::Whole, store_records);
        assert_eq!(
            row.work_entries, store_records as u64,
            "the first snapshot on a fresh store must discard one log entry per record, which is \
             what makes this the arm where the two denominators coincide: {} entries for \
             {store_records} records",
            row.work_entries
        );

        // REGIME INCREMENT -- the subject. One more record, then snapshot again.
        append_records(&cluster, store_records, INCREMENT_RECORDS);
        let after = store_records + INCREMENT_RECORDS;
        let live_after = live_slabs(&cluster);
        println!("  live slab ids after  = {live_after:?}");
        assert!(
            live_after.len() > 1,
            "the shard's live slab set after the first install is {live_after:?}: with one slab a \
             per-slab cost and a per-snapshot cost are the same number, and the rebuilt engine \
             would not be expressing what the fixture was built to express"
        );
        let row_increment = measure_one_snapshot(&cluster, Regime::Increment, after);
        assert_eq!(
            row_increment.work_entries, INCREMENT_RECORDS as u64,
            "the second snapshot must discard exactly the {INCREMENT_RECORDS} entr(y/ies) applied \
             since the first: {} discarded, so this arm is not measuring a small backlog at all",
            row_increment.work_entries
        );

        println!("  fixture payload bytes = {}", fixture.payload_bytes);
        whole.push(row);
        increment.push(row_increment);
    }

    let (whole_small, whole_large) = (whole[0], whole[1]);
    let (inc_small, inc_large) = (increment[0], increment[1]);

    // ---- EXACT IDENTITIES, at both sizes and in both regimes ----
    //
    // The walk visits one block address per record of STORE. Asserted as an identity against
    // something the snapshot path did not compute -- the record count the fixture was built with
    // -- rather than as a tolerance, because a tolerance is also satisfied by a walk that has
    // stopped happening.
    for row in [whole_small, whole_large, inc_small, inc_large] {
        assert_eq!(
            row.counts.live_slab_scan_addresses, row.store_records,
            "{} at store={}: the live-slab walk must visit exactly one block address per record \
             of STORE, and visited {}",
            row.regime.label(),
            row.store_records,
            row.counts.live_slab_scan_addresses
        );
        assert_eq!(
            row.counts.slab_read_bytes,
            SLAB_BYTES_PER_RECORD * row.store_records,
            "{} at store={}: slab bytes read must be exactly {SLAB_BYTES_PER_RECORD} per record \
             of STORE, and were {}",
            row.regime.label(),
            row.store_records,
            row.counts.slab_read_bytes
        );
        assert_eq!(
            row.counts.image_builds, 1,
            "{} at store={}: one snapshot must build exactly one state image. More than one means \
             the three-attempt loop in create_state_image_snapshot RETRIED, which #1912 measured \
             as never happening anywhere in the tree at 1,000 and 4,000 records; a retry here \
             would be a large-store behaviour that small stores cannot show",
            row.regime.label(),
            row.store_records,
        );
        assert_eq!(
            row.counts.slab_reads, row.image_slabs,
            "{} at store={}: the build must read each carried slab exactly once -- {} reads for \
             {} slabs. It reads every slab it finds and stops at no bound",
            row.regime.label(),
            row.store_records,
            row.counts.slab_reads,
            row.image_slabs
        );
        assert_eq!(
            row.counts.engine_rebuilds_under_guard, 0,
            "{} at store={}: #1914's result must hold at this size -- no engine may be rebuilt \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.engine_rebuilds_under_guard
        );
        assert_eq!(
            row.counts.slab_install_bytes_under_guard, 0,
            "{} at store={}: #1914's result must hold at this size -- no slab byte may be written \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.slab_install_bytes_under_guard
        );
        assert_eq!(
            row.counts.image_clone_bytes_under_guard, 0,
            "{} at store={}: #1912's result must hold at this size -- no image byte may be copied \
             under the cluster write guard, and {} were",
            row.regime.label(),
            row.store_records,
            row.counts.image_clone_bytes_under_guard
        );
        assert!(
            row.counts.engine_rebuilds > 0 && row.counts.engine_publishes > 0,
            "{} at store={}: {} rebuilds and {} publishes. A snapshot that installed into nobody \
             would satisfy every under-guard assertion above by doing nothing at all",
            row.regime.label(),
            row.store_records,
            row.counts.engine_rebuilds,
            row.counts.engine_publishes
        );
    }

    // ---- THE CONTROL ARM: WHOLE ----
    //
    // Named, and with its own failure message. Per record of WORK this arm is flat, because in it
    // the work IS the store. If this arm ever stops being flat, the instrument is reading
    // something other than what it claims to and NOTHING below it can be believed.
    let whole_small_per_work = whole_small.per_record_of_work(whole_small.counts.slab_read_bytes);
    let whole_large_per_work = whole_large.per_record_of_work(whole_large.counts.slab_read_bytes);
    println!("=== CONTROL ARM: WHOLE ===");
    println!(
        "  slab bytes per record of WORK = {whole_small_per_work:.3} -> {whole_large_per_work:.3}"
    );
    assert_eq!(
        whole_small.counts.slab_read_bytes * whole_large.work_entries,
        whole_large.counts.slab_read_bytes * whole_small.work_entries,
        "CONTROL ARM WHOLE is not flat per record of WORK: {whole_small_per_work} at store={} \
         against {whole_large_per_work} at store={}. In this regime the log discarded and the \
         store read are the same records, so this ratio is an identity; if it moves, the \
         denominator is wrong and the SUBJECT arm below cannot be interpreted",
        whole_small.store_records,
        whole_large.store_records,
    );

    // ---- THE SUBJECT ARM: INCREMENT ----
    let inc_small_per_store = inc_small.per_record_of_store(inc_small.counts.slab_read_bytes);
    let inc_large_per_store = inc_large.per_record_of_store(inc_large.counts.slab_read_bytes);
    let inc_small_per_work = inc_small.per_record_of_work(inc_small.counts.slab_read_bytes);
    let inc_large_per_work = inc_large.per_record_of_work(inc_large.counts.slab_read_bytes);
    let store_ratio = inc_large.store_records as f64 / inc_small.store_records as f64;
    println!("=== SUBJECT ARM: INCREMENT ===");
    println!("  store ratio                    = {store_ratio:.3}");
    println!(
        "  work entries                   = {} -> {}",
        inc_small.work_entries, inc_large.work_entries
    );
    println!(
        "  slab bytes read                = {} -> {}  (ratio {:.3})",
        inc_small.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes as f64 / inc_small.counts.slab_read_bytes.max(1) as f64
    );
    println!(
        "  per record of STORE            = {inc_small_per_store:.3} -> {inc_large_per_store:.3}"
    );
    println!(
        "  per record of WORK             = {inc_small_per_work:.3} -> {inc_large_per_work:.3}"
    );

    assert!(
        inc_small.counts.slab_read_bytes > 0 && inc_large.counts.slab_read_bytes > 0,
        "slab bytes read is zero at one of the two sizes ({} and {}), so every ratio above is \
         vacuous",
        inc_small.counts.slab_read_bytes,
        inc_large.counts.slab_read_bytes
    );

    // Per record of STORE, the subject is FLAT -- which is precisely why this cost is invisible
    // to a per-record-of-store denominator, and why it took a large store to make the point.
    assert_eq!(
        inc_small.counts.slab_read_bytes * inc_large.store_records,
        inc_large.counts.slab_read_bytes * inc_small.store_records,
        "per record of STORE the INCREMENT arm must be flat -- {inc_small_per_store} against \
         {inc_large_per_store}. That flatness is the finding's camouflage, not its refutation"
    );

    // Per record of WORK, with the work held at ONE entry in both arms, it grows by exactly the
    // store ratio. THIS IS THE FINDING.
    //
    // It is asserted as a strict inequality as well as an identity: the identity says the growth
    // is exactly proportional to the store, and the inequality is what goes red the day someone
    // BOUNDS the read. A flat reading here does not mean this test passed; it means the tree has
    // stopped doing what this file records, and the recording must be redone.
    assert!(
        inc_large.counts.slab_read_bytes > inc_small.counts.slab_read_bytes,
        "SUBJECT ARM INCREMENT HAS GONE FLAT: {} slab bytes read at store={} and {} at store={}, \
         for one entry of work in both. This file exists to record that a snapshot's read is \
         priced by the store rather than by the log it discards; a flat reading refutes that \
         recording. Do not relax this assertion -- re-derive what the path now reads and rewrite \
         the table in this module's doc comment",
        inc_small.counts.slab_read_bytes,
        inc_small.store_records,
        inc_large.counts.slab_read_bytes,
        inc_large.store_records,
    );
    assert_eq!(
        inc_small.counts.slab_read_bytes * inc_large.store_records,
        inc_large.counts.slab_read_bytes * inc_small.store_records,
        "for one entry of work in both arms, slab bytes read must grow by exactly the STORE \
