// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a replay costs in PIECE METADATA, and why that grew faster than the log.
//!
//! `wal_scale` asked what an append costs and what a replay costs, and found the replay
//! superlinear: 4.01x the bytes for 12.45x the `statx`. This module is that aside taken as its own
//! subject -- the quantity named, the fixture regime that decides whether it is visible at all,
//! and the shape the walk holds now.
//!
//! THE QUANTITY. A replay reads the log in bounded windows, and the walk resumed its RECORDS but
//! never its PIECE LIST. Every window took the piece list from a fresh `read_dir`, then walked it
//! from the FRONT, opening each piece and reading its header to learn where the piece begins --
//! so it could conclude the piece was behind the window and step over it. Both the number of
//! windows and the number of pieces grow linearly with the log's bytes, so the metadata probes
//! grew with their PRODUCT:
//!
//! ```text
//!   probes  ~=  windows x pieces  =  bytes/WAL_REPLAY_WINDOW_BYTES x bytes/TS_WAL_SEGMENT_BYTES
//! ```
//!
//! and a replay of a log twice as long cost four times the metadata. The number the walk went to
//! disk for was in the piece's NAME the whole time: `sealed_wal_path` names a sealed piece for the
//! log id its contents start at, and `wal_segment_paths` already parsed that number to SORT by it
//! before discarding it. A piece is behind the window when the NEXT piece's name says it is.
//!
//! Measured with `strace -f -c` on `wal_cost_scale_harness`, 128 incompressible bytes per record,
//! production constants (512 KiB windows, 256 KiB pieces), whole-log replay:
//!
//! ```text
//!   records   log bytes  pieces  windows  records     statx      total syscalls
//!                                         replayed  before  after   before  after
//!    20,000   3,403,490      14        7    20,000     248     75    1,320    983
//!    80,000  13,663,490      54       27    80,000   3,088    255    8,976  3,341
//!   ratio        4.01x                                12.45x  3.40x
//! ```
//!
//! Same windows, same records replayed, same bytes: what changed is only the metadata. At 80,000
//! records the walk had been spending more syscalls on piece metadata than on reading records.
//!
//! THE FIXTURE REGIME, which decides whether any of that is visible. A replay reads the SUFFIX of
//! the log that the loaded index does not already reflect, and the knob is how that suffix is
//! sized against the log:
//!
//! * PROPORTIONAL SUFFIX -- a watermark of zero, the whole log replayed. Windows AND pieces both
//!   grow, the product is quadratic, and the defect is visible against any denominator.
//! * FIXED SUFFIX -- a watermark near the end, which is what a node that has been dumping its
//!   index actually restores from. The replayed work is CONSTANT: one or two pieces, one window.
//!   The metadata cost was still linear in the WHOLE log, because that single window still walked
//!   every piece from the front. Against BYTES that reads as 4.01x for 4.01x -- perfectly linear,
//!   no defect -- and a measurement taken only in this regime, against that denominator, would
//!   have closed the question. Against the work actually done it is unbounded: four times the log
//!   for the same handful of records.
//!
//! Both regimes are measured and asserted below, so neither can be picked by accident later.
//!
//! REACHABILITY, stated before any saving. `load_shard_with` calls `replay_wal_into_shard` on
//! every shard load, in both barrier arms, with no flag in front of it, and rolling is on by
//! default (`DEFAULT_WAL_SEGMENT_BYTES`, 256 KiB) so a production log is in many pieces. The
//! narrow part is the REGIME, not the path: the fixed-suffix arm is the one a real restore takes,
//! and it is the arm where the old cost was linear in the whole retained log rather than
//! quadratic. The quadratic arm is reached by a shard with no usable index checkpoint -- a fresh
//! or async-only shard, or one whose manifest is older than its log.
//!
//! DIRECTION. Replay is the restore path: reading too FEW records is silent data loss, reading too
//! many is merely slow. A count assertion passes while replaying the wrong records, so the
//! identity test below compares the SEQUENCE the walk hands back, element by element, against the
//! same records taken from a walk that skips nothing.

#![cfg(test)]

use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_extents_for_test, LocalWriteAheadLogStore,
    DEFAULT_WAL_SEGMENT_BYTES, WAL_PIECES_SKIPPED_BY_NAME, WAL_PIECE_BODY_READS,
    WAL_PIECE_TAIL_READS, WAL_READ_FILE_OPENS, WAL_SEGMENT_HEADER_READS,
    WAL_SEGMENT_LISTINGS, WAL_SEGMENT_LISTING_ENTRIES,
};

/// The window the engine replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`) and the size
/// it rolls at (`DEFAULT_WAL_SEGMENT_BYTES`), both scaled down by eight so a test log of a few
/// hundred kilobytes still crosses several windows and several pieces. The shape being measured is
/// windows-against-pieces, and that shape is set by the RATIO of these two, which is preserved:
/// 512 KiB / 256 KiB in production, 64 KiB / 32 KiB here.
const TEST_WINDOW_BYTES: u64 = 64 * 1024;
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;

/// xorshift64*, seeded per record. Records are compressed, so a corpus of repeated bytes holds a
/// fraction of the bytes its value size implies and moves neither the piece count nor the window
/// count where it is meant to.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

fn build_log(dir: &std::path::Path, records: usize, value_bytes: usize) -> LocalWriteAheadLogStore {
    let store = LocalWriteAheadLogStore::new(dir);
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(value_bytes, index as u64),
                },
            )
            .unwrap();
        index += 1;
    }
    store
}

fn reset_counters() {
    WAL_SEGMENT_LISTINGS.with(|value| value.set(0));
    WAL_SEGMENT_LISTING_ENTRIES.with(|value| value.set(0));
    WAL_SEGMENT_HEADER_READS.with(|value| value.set(0));
    WAL_PIECES_SKIPPED_BY_NAME.with(|value| value.set(0));
    WAL_PIECE_BODY_READS.with(|value| value.set(0));
    WAL_PIECE_TAIL_READS.with(|value| value.set(0));
    WAL_READ_FILE_OPENS.with(|value| value.set(0));
}

/// Every counter, as it stands.
#[derive(Clone, Copy, Default)]
struct Counters {
    listings: u64,
    /// Directory ENTRIES the listings walked, summed. The work, as against the call count.
    listing_entries: u64,
    header_reads: u64,
    skipped_by_name: u64,
    body_reads: u64,
    tail_reads: u64,
    read_opens: u64,
}

fn counters() -> Counters {
    Counters {
        listings: WAL_SEGMENT_LISTINGS.with(|value| value.get()),
        listing_entries: WAL_SEGMENT_LISTING_ENTRIES.with(|value| value.get()),
        header_reads: WAL_SEGMENT_HEADER_READS.with(|value| value.get()),
        skipped_by_name: WAL_PIECES_SKIPPED_BY_NAME.with(|value| value.get()),
        body_reads: WAL_PIECE_BODY_READS.with(|value| value.get()),
        tail_reads: WAL_PIECE_TAIL_READS.with(|value| value.get()),
        read_opens: WAL_READ_FILE_OPENS.with(|value| value.get()),
    }
}

/// What one whole replay cost, and what it handed back.
///
/// A replay is TWO walks with two different shapes, and adding them up hides both. The PRE-WALK
/// (`log_id_after_sequence`) runs once and asks each piece for its last SEQUENCE. The WINDOWED
/// SCAN then runs once per window and asks each piece where it starts, which is a LOG ID. Only
/// the second is per-window, so only the second was ever the product; they are kept apart here
/// so a change to one cannot be read as a change to the other.
struct ReplayCost {
    windows: u64,
    /// `(log id, sequence)` for every record the replay was handed, IN ORDER. The identity test
    /// compares these, not their length: a walk that replayed the wrong records has the right
    /// count.
    handed_back: Vec<(u64, u64)>,
    /// What the pre-walk cost, on its own.
    prewalk: Counters,
    /// The whole replay, pre-walk included.
    total: Counters,
}

impl ReplayCost {
    fn records(&self) -> u64 {
        self.handed_back.len() as u64
    }

    /// Header reads taken by the WINDOWED SCAN -- the per-window walk, which is the subject.
    fn scan_header_reads(&self) -> u64 {
        self.total.header_reads - self.prewalk.header_reads
    }

    /// What the windowed scan's header reads WOULD have been without the name-skip,
    /// reconstructed rather than remembered.
    ///
    /// A piece skipped on its name is exactly a piece the old walk opened, read a header from, and
    /// then stepped over on the length test -- one header read each, no more and no fewer. So the
    /// old cost is this sum, at any fixture, with no recorded constant that could go stale. It is
    /// checked against the numbers `wal_scale` printed before the change: 45 and 466.
    fn scan_header_reads_before_the_skip(&self) -> u64 {
        self.scan_header_reads() + self.total.skipped_by_name
    }
}

/// Replay `store` the way `replay_wal_into_shard_windowed` does: start past the pieces the
/// watermark already covers, then bounded windows resuming where the last stopped, verifying the
/// tail only on the first.
fn replay_cost(store: &LocalWriteAheadLogStore, watermark: u64) -> ReplayCost {
    reset_counters();
    let start_at = store
        .replay_start_after_sequence(1, watermark)
        .unwrap_or_else(|_| crate::wal::ReplayPosition::at_start_of_log());
    let prewalk = counters();
    let mut window_start = start_at;
    let mut verify_tail = true;
    let mut windows = 0u64;
    let mut handed_back = Vec::new();
    loop {
        let (scanned, more_to_come, resume_at) = store
            .scan_decoded_window(1, window_start, TEST_WINDOW_BYTES, verify_tail)
            .unwrap();
        verify_tail = false;
        windows += 1;
        for (log_id, record) in &scanned {
            handed_back.push((*log_id, record.sequence));
        }
        if !more_to_come {
            break;
        }
        window_start = resume_at;
    }
    ReplayCost { windows, handed_back, prewalk, total: counters() }
}

/// How many pieces the log is in, and how many records it holds.
fn piece_count(store: &LocalWriteAheadLogStore, dir: &std::path::Path) -> usize {
    let _ = store;
    wal_piece_extents_for_test(dir, 1).len()
}

fn print_cost(label: &str, bytes: u64, pieces: usize, cost: &ReplayCost) {
    println!(
        "    {label:<8} {bytes:>9} B, {pieces:>3} pieces, {:>3} windows, {:>6} records | \
         SCAN {:>5} header reads ({:>5} before the skip), {:>5} skipped by name, {:>4} bodies | \
         PRE-WALK {:>4} piece tails, {:>4} header reads | {:>5} read opens, {:>4} listings",
        cost.windows,
        cost.records(),
        cost.scan_header_reads(),
        cost.scan_header_reads_before_the_skip(),
        cost.total.skipped_by_name,
        cost.total.body_reads,
        cost.prewalk.tail_reads,
        cost.prewalk.header_reads,
        cost.total.read_opens,
        cost.total.listings
    );
}

/// The apparatus and the regime, asserted before anything is concluded from a ratio.
///
/// A cost measured where it cannot occur reads exactly like a low cost: a log in ONE piece has no
/// piece to step over, a replay of an EMPTY suffix walks nothing, and both would report a flat,
/// perfect result about nothing at all.
fn assert_in_regime(label: &str, pieces: usize, cost: &ReplayCost) {
    assert!(
        pieces > 1,
        "{label}: the log is in {pieces} piece(s). A walk that steps over pieces behind the window \
         cannot cost anything on a log that has none, so every figure here would be a zero about \
         nothing"
    );
    assert!(
        cost.records() > 0,
        "{label}: the replay was handed {} records. An empty suffix is not a replay",
        cost.records()
    );
    assert!(
        cost.windows >= 1 && cost.total.body_reads >= 1,
        "{label}: {} windows and {} pieces opened for records; this is not timing a replay",
        cost.windows,
        cost.total.body_reads
    );
    assert!(
        cost.total.listings >= cost.windows,
        "{label}: {} listings for {} windows -- the listing counter is not counting, so the \
         residual below means nothing",
        cost.total.listings,
        cost.windows
    );
}

/// PROPORTIONAL SUFFIX: the whole log replayed, so the replayed portion grows with the store.
///
/// This is the regime `wal_scale`'s aside was measured in, and the one where the old shape is
/// unmissable: four times the bytes cost 10.36x the piece-header reads in-process and 12.45x the
/// `statx` on the real binary. Windows and pieces both grow, and the walk paid their product.
///
/// What is asserted now is that the product is gone -- the header reads track the PIECES, which
/// track the bytes -- with the pre-fix quantity reconstructed from the skip counter beside it so
/// the test says what it fixed rather than only what it holds.
#[test]
fn a_whole_log_replay_reads_each_pieces_header_about_once() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_pieces = piece_count(&small, small_dir.path());
    let small_cost = replay_cost(&small, 0);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_pieces = piece_count(&large, large_dir.path());
    let large_cost = replay_cost(&large, 0);
    set_wal_segment_bytes_for_test(None);

    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let piece_ratio = large_pieces as f64 / small_pieces.max(1) as f64;
    let walk_ratio = large_cost.scan_header_reads() as f64
        / small_cost.scan_header_reads().max(1) as f64;
    let before_ratio = large_cost.scan_header_reads_before_the_skip() as f64
        / small_cost.scan_header_reads_before_the_skip().max(1) as f64;

    println!("  PROPORTIONAL SUFFIX -- watermark 0, the whole log replayed");
    print_cost("small", small_bytes, small_pieces, &small_cost);
    print_cost("large", large_bytes, large_pieces, &large_cost);
    println!(
        "    ratio: {byte_ratio:.2}x bytes, {piece_ratio:.2}x pieces, \
         {walk_ratio:.2}x scan header reads, {before_ratio:.2}x before the skip"
    );
    println!(
        "    per window: {:.1} scan header reads small, {:.1} large",
        small_cost.scan_header_reads() as f64 / small_cost.windows.max(1) as f64,
        large_cost.scan_header_reads() as f64 / large_cost.windows.max(1) as f64
    );

    assert_in_regime("small", small_pieces, &small_cost);
    assert_in_regime("large", large_pieces, &large_cost);
    assert!(
        small_cost.windows >= 2 && large_cost.windows >= 4,
        "the two replays took {} and {} windows; a replay that fits in one window cannot show \
         anything about a PER-WINDOW cost -- and, as the fixed-suffix test beside this one shows, \
         it cannot show the superlinearity either, because one window times P pieces is P",
        small_cost.windows,
        large_cost.windows
    );
    assert!(
        large_cost.total.skipped_by_name > 0,
        "no piece was skipped on its name over {} windows and {large_pieces} pieces. The skip is \
         the thing under test and it did not happen, so the flat ratio below is about nothing",
        large_cost.windows
    );

    // The treatment was applied: the log really is four times longer, in four times the pieces.
    assert!(
        byte_ratio > 3.5 && byte_ratio < 4.5,
        "the two logs differ by {byte_ratio:.2}x in bytes, not the 4x this fixture is sized for"
    );

    // THE RESULT. The header reads track the PIECES, not windows-times-pieces. The bound is
    // deliberately loose enough that ordinary work does not move it and tight enough that the
    // product cannot hide inside it: at this fixture the product would be ~10x.
    assert!(
        walk_ratio < byte_ratio * 1.5,
        "the scan's header reads grew {walk_ratio:.2}x for {byte_ratio:.2}x the bytes. That is the \
         windows-times-pieces shape coming back: every window is walking the piece list from the \
         front again instead of stepping over the pieces the next piece's NAME says are behind it"
    );
    assert!(
        large_cost.scan_header_reads() <= (large_pieces as u64 + large_cost.windows) * 2,
        "the scan read {} headers over {large_pieces} pieces in {} windows. One header read per \
         piece is the shape; a multiple of the WINDOWS on top of it is the old product returning",
        large_cost.scan_header_reads(),
        large_cost.windows
    );
    // The per-window cost is FLAT. This is the identity the ratio above is a consequence of, and
    // it is the one that does not depend on the fixture being sized 4x.
    let small_per_window = small_cost.scan_header_reads() as f64 / small_cost.windows as f64;
    let large_per_window = large_cost.scan_header_reads() as f64 / large_cost.windows as f64;
    assert!(
        large_per_window < small_per_window * 1.5,
        "a window of the four-times-longer log read {large_per_window:.1} piece headers against \
         {small_per_window:.1}. What a window costs must not depend on how much log is in front \
         of it; that dependence IS the product"
    );

    // And what it replaced, reconstructed from the skip counter rather than remembered as a
    // constant. This is the measurement, and it is why the loose bound above is not vacuous.
    assert!(
        before_ratio > byte_ratio * 1.5,
        "without the name-skip this walk would have grown {before_ratio:.2}x for {byte_ratio:.2}x \
         the bytes, which is not the superlinear shape this fixture is supposed to produce. \
         Either the fixture stopped crossing enough pieces or the skip counter is counting \
         something else, and in both cases the {walk_ratio:.2}x above proves nothing"
    );
}

/// FIXED SUFFIX: a watermark near the end, which is the regime a real restore is in -- AND THE
/// REGIME THAT HIDES THE SUPERLINEARITY COMPLETELY.
///
/// This is the trap, and it is the same one `wal_scale`'s proportional fixture would have fallen
/// into with the arms swapped. A node that has been dumping its index restores from a watermark
/// close to the log's tail, so the replayed WORK is a constant -- a piece or two, ONE window --
/// however long the retained log has grown. One window times P pieces is P. The product that makes
/// a whole-log replay quadratic collapses to a straight line, and the numbers come out:
///
/// ```text
///                                   scan header reads   ratio      verdict
///   FIXED SUFFIX   (this test)             9 ->    40   4.44x   "linear in the bytes, healthy"
///   PROPORTIONAL   (the test above)       45 ->   466  10.36x   "superlinear, defective"
/// ```
///
/// Same code, same store, same corpus ratio, opposite conclusions. A measurement taken only here,
/// against bytes, closes the question and reports no defect -- so the knob is the RATIO OF THE
/// REPLAYED SUFFIX TO THE LOG, and this test exists so that neither setting of it can be picked by
/// accident later.
///
/// What is still true in this regime, and what this test asserts against the OTHER denominator:
/// the work does not move and the cost did, four-fold, because a single window walked every piece
/// in front of it. The name-skip removes that. What it does NOT remove is the pre-walk beside it:
/// `log_id_after_sequence` asks every piece for its last SEQUENCE, and a sequence is not in any
/// piece's name. That walk is LINEAR in the log for constant work, it is measured here rather than
/// folded in, and it is deliberately not touched -- see the module header.
#[test]
fn a_fixed_tail_replay_no_longer_pays_for_the_log_in_front_of_it() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_pieces = piece_count(&small, small_dir.path());
    // Everything but the last 200 records already covered by the index. `log_id_after_sequence`
    // is conservative -- it starts at the first piece the watermark does not cover -- so the
    // replayed portion is a piece or so at BOTH sizes, which is the point.
    let small_cost = replay_cost(&small, small_records as u64 - 200);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_pieces = piece_count(&large, large_dir.path());
    let large_cost = replay_cost(&large, large_records as u64 - 200);
    set_wal_segment_bytes_for_test(None);

    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let work_ratio = large_cost.records() as f64 / small_cost.records().max(1) as f64;
    let walk_ratio = large_cost.scan_header_reads() as f64
        / small_cost.scan_header_reads().max(1) as f64;
    let before_ratio = large_cost.scan_header_reads_before_the_skip() as f64
        / small_cost.scan_header_reads_before_the_skip().max(1) as f64;
    let prewalk_ratio =
        large_cost.prewalk.tail_reads as f64 / small_cost.prewalk.tail_reads.max(1) as f64;

    println!("  FIXED SUFFIX -- the last 200 records replayed, however long the log is");
    print_cost("small", small_bytes, small_pieces, &small_cost);
    print_cost("large", large_bytes, large_pieces, &large_cost);
    println!(
        "    ratio: {byte_ratio:.2}x bytes, {work_ratio:.2}x records replayed, \
         {walk_ratio:.2}x scan header reads, {before_ratio:.2}x before the skip, \
         {prewalk_ratio:.2}x pre-walk piece tails"
    );
    println!(
        "    SCAN header reads PER REPLAYED RECORD: {:.4} small, {:.4} large \
         (before the skip: {:.4} and {:.4})",
        small_cost.scan_header_reads() as f64 / small_cost.records().max(1) as f64,
        large_cost.scan_header_reads() as f64 / large_cost.records().max(1) as f64,
        small_cost.scan_header_reads_before_the_skip() as f64 / small_cost.records().max(1) as f64,
        large_cost.scan_header_reads_before_the_skip() as f64 / large_cost.records().max(1) as f64
    );

    assert_in_regime("small", small_pieces, &small_cost);
    assert_in_regime("large", large_pieces, &large_cost);

    // THE CONTROL HELD: the log really did grow four-fold.
    assert!(
        byte_ratio > 3.5 && byte_ratio < 4.5,
        "the two logs differ by {byte_ratio:.2}x in bytes, not the 4x this fixture is sized for"
    );
    // AND THE WORK REALLY DID NOT: this is a FIXED suffix, not a proportional one. Without this
    // the test is the proportional arm again under a different name.
    assert!(
        work_ratio < 1.6,
        "the large replay handed back {} records against {} -- {work_ratio:.2}x. This fixture is \
         supposed to hold the replayed work FIXED while the log grows; it did not, so it is \
         measuring the proportional regime",
        large_cost.records(),
        small_cost.records()
    );
    // AND IT REALLY IS ONE WINDOW, which is what makes this the regime that hides the product.
    assert_eq!(
        (small_cost.windows, large_cost.windows),
        (1, 1),
        "this fixture took {} and {} windows. The hiding is the arithmetic of ONE window: windows \
         times pieces is what grows quadratically, and at one window it is just the pieces",
        small_cost.windows,
        large_cost.windows
    );

    // THE RESULT, against the denominator that does NOT hide it: the same work costs the same
