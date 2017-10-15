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
