// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a WINDOWED replay reads, against the log it is replaying.
//!
//! PR #1931 measured a restore at 20,000 and 200,000 records and found that bringing back a
//! 200,000-record shard with half its writes past the checkpoint read **1,014,680,664 bytes to
//! rebuild an 87.9 MB store**, of which **965,415,651 was read by a reader with no counter on
//! it**. It measured the shape and guarded it; it did not name the reader. This does both, and
//! then removes most of it.
//!
//! ## The reader, named
//!
//! It is the log's own pieces, read by the windowed replay walk, and the bytes were invisible
//! because the walk counts what it HANDS BACK and not what it READS. Attributed by file, from
//! outside the process:
//!
//! ```text
//!   200,000 records, half the writes past the checkpoint, before this change
//!   kernel rchar across load_shard_with        1,034,158,338
//!     the log piece  shard-1.wal.bin             987,705,371   in 120,568 reads
//!     the checkpoint manifests                    38,410,321   in 4 reads
//!     the index log                                8,119,256   in 1,069 reads
//!     pages, /proc, everything else                    13,390
//!   the log store's own bytes_read                 30,184,630
//! ```
//!
//! The log piece on disk is **32,768,000 bytes**. The restore read it **30.14 times over**.
//!
//! ## Why, and why it is not what #1917 fixed
//!
//! #1917 removed a window re-reading the HEADER of every piece behind it, by deciding from the
//! piece's NAME whether it was behind the window. This is the piece's CONTENTS, and that change
//! does not touch it: inside the piece the window still starts at the piece's header. The walk
//! always resumed its RECORDS -- each window picks up at the log id the last one stopped at --
//! and then re-read its way to that log id from the top of the piece, decoding and discarding
//! everything before its own start.
//!
//! So each window read every window before it. Over `W` windows of `B` bytes over a log of `L`
//! bytes that is `B * W(W+1)/2`, not `L`:
//!
//! ```text
//!   W = 59 windows, B = 512 KiB, L = 30,184,630 bytes of records
//!   sum min(i*B, L) for i in 1..=59  =  927,017,324
//!   measured                            987,705,371   (the rest is the tail and header scans)
//! ```
//!
//! **Every in-engine instrument reported that restore as perfectly flat**, because every one of
//! them counts records handed back, manifests parsed or pages fetched -- and not one of them
//! counts a byte pulled off a log piece.
//!
//! ## Two things that make it as bad as it is
//!
//! A log in MANY pieces bounds the re-read: a window can only re-read its way through the piece
//! it starts in, so #1917's name-skip caps the waste at one piece per window. A log in ONE piece
//! does not bound it at all, and **the log under a restore is one piece**: at 200,000 records the
//! shard's log was a single 32,768,000-byte file. `append_batch_as_one_record` -- the data-only
//! batch path the engine takes whenever a batch produced outcomes -- is the one append entry
//! point that never asks `roll_wal_segment_if_due` whether the piece is full. That is not fixed
//! here; it is a write-path change with its own crash-consistency argument, and it is measured
//! and stated so the number beside it is not a guess.
//!
//! ## What this does
//!
//! A window seeks to where it resumes. A log id IS a byte position -- this is the identity
//! `locate_log_id` already resolves random access with -- so the position is arithmetic, not a
//! walk. The only thing that made it unsafe to seek was that a log id a caller invented could
//! land mid-record, and decoding from the middle of a record on the recovery path is the silent
//! direction. So the windowed walk takes a [`crate::wal::ReplayPosition`], which can only have
//! come from the walk itself or from `replay_start_after_sequence`, and the public scans keep
//! the semantics they have always had.
//!
//! ```text
//!   200,000 records, half the writes past the checkpoint
//!                                      before          after     ratio
//!   kernel rchar                  1,034,158,338    81,232,374    0.0786
//!   of it, the log piece            987,705,371    34,779,419    0.0352
//!   store rebuilt                   107,264,952   107,264,952
//!   read per byte of store rebuilt         9.64          0.76
//! ```
//!
//! ## DIRECTION
//!
//! Replaying too FEW records is silent data loss: the shard loads clean and an acknowledged
//! write is simply absent. Replaying too many is merely slow. Every assertion about a COUNT
//! passes just as happily on a walk that replayed the wrong records, so the identity test below
//! compares the `(log id, sequence)` SEQUENCE the windowed walk hands back, element by element,
//! against the same records taken from a walk that skips nothing -- at seven watermarks, in both
//! log shapes, and at three window sizes.

#![cfg(test)]

use crate::types::Command;
use crate::wal::{
    read_piece_for_test, set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread,
    LocalWriteAheadLogStore, ReplayPosition,
};

/// The window the engine replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`) and the
/// size it rolls at (`DEFAULT_WAL_SEGMENT_BYTES`), both scaled down by eight so a test log of a
/// few hundred kilobytes still crosses several windows and several pieces. The shape measured
/// here is windows against the log, and that is set by the RATIO, which is preserved: 512 KiB /
/// 256 KiB in production, 64 KiB / 32 KiB here.
const TEST_WINDOW_BYTES: u64 = 64 * 1024;
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;

/// Two sizes and the ratio between them. Four times the log, so a quantity that is flat per
/// record and one that grows with the log are four times apart and cannot be confused.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;
const VALUE_BYTES: usize = 128;

/// How the log is laid out on disk. Both are reachable and they hide different things.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// One file, however long the log gets. What the engine's own batch path produces, and the
    /// shape in which nothing bounds a window's re-read of the piece it starts in.
    OnePiece,
    /// Rolled at `TEST_SEGMENT_BYTES`. #1917's name-skip caps the re-read at one piece per
    /// window here, so the defect is present but an order of magnitude smaller -- which is
    /// exactly why a measurement taken only in this shape would call it bounded and stop.
    Rolled,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Shape::OnePiece => "one piece",
            Shape::Rolled => "rolled at 32 KiB",
        }
    }

    fn apply(self) {
        match self {
            Shape::OnePiece => set_wal_segment_bytes_for_test(Some(0)),
            Shape::Rolled => set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES)),
        }
    }
}

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

fn build_log(
    dir: &std::path::Path,
    records: usize,
    shape: Shape,
) -> (LocalWriteAheadLogStore, LogOnDisk) {
    shape.apply();
    let store = LocalWriteAheadLogStore::new(dir);
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(VALUE_BYTES, index as u64),
                },
            )
            .expect("the fixture's writes must land");
        index += 1;
    }
    let on_disk = LogOnDisk::of(dir);
    (store, on_disk)
}

/// What the fixture actually put on disk, read back from it rather than assumed.
#[derive(Clone, Debug)]
struct LogOnDisk {
    pieces: usize,
    bytes: u64,
}

impl LogOnDisk {
    fn of(dir: &std::path::Path) -> Self {
        let mut pieces = 0usize;
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.contains(".wal") || name.ends_with(".lock") {
                continue;
            }
            pieces += 1;
            bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        }
        LogOnDisk { pieces, bytes }
    }
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite that runs tests beside each other charges
// this span whatever another thread happened to read. `/proc/thread-self/io` is this thread, and
// a replay runs on one.
//
// Reading the counter is itself a read and the kernel charges it, so the reading carries the
// LENGTH of the text it came from and the span subtracts it exactly rather than estimating it.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_thread_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let mut rchar = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            rchar = rest.trim().parse().ok();
        }
    }
    Some(IoReading {
        rchar: rchar?,
        self_bytes: text.len() as u64,
    })
}

/// What the kernel says this THREAD read across a span, with the instrument's own reads removed.
///
/// `None` when `/proc` said nothing, so "the kernel was not counting" cannot be read as "the
/// kernel counted zero".
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        // The opening reading's own bytes fall inside the span; the closing reading's are
        // charged after it is taken.
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE WALK UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// What one whole replay read, and what it handed back.
#[derive(Clone, Debug)]
struct Replay {
    windows: u64,
    /// `(log id, sequence)` for every record the replay would APPLY, in order. The identity test
    /// compares these, not their length: a walk that replayed the wrong records has the right
    /// count.
    applied: Vec<(u64, u64)>,
    /// Records the walk decoded and the watermark then threw away. Reported because the RELATION
    /// between this and `applied` is what says whether the walk started in the right place.
    decoded_and_dropped: u64,
    /// Bytes pulled off log pieces, from the counter inside the read itself.
    piece_bytes: u64,
    piece_reads: u64,
    /// What the kernel charged this thread across the same span.
    kernel_rchar: Option<u64>,
}

impl Replay {
    /// The kernel's number minus the one reader this walk has. Nothing in the engine feeds
    /// `rchar`, so this CAN be wrong and can be seen to be wrong, which is the property a
    /// residual computed from the rows it audits does not have.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.piece_bytes as i64)
    }
}

/// Replay `store` exactly the way `replay_wal_into_shard_windowed` does: start past the pieces
/// the watermark already covers, then bounded windows resuming where the last one stopped,
/// verifying the tail only on the first.
fn replay(store: &LocalWriteAheadLogStore, watermark: u64, window_bytes: u64) -> Replay {
    let (bytes_before, reads_before) = wal_piece_read_counts_on_this_thread();
    let mut windows = 0u64;
    let mut applied = Vec::new();
    let mut decoded_and_dropped = 0u64;
    let (_, kernel_rchar) = kernel_read_span(|| {
        let mut from = store
            .replay_start_after_sequence(1, watermark)
            .expect("a replay must be able to find where to start");
        let mut verify_tail = true;
        loop {
            let (scanned, more_to_come, resume_at) = store
                .scan_decoded_window(1, from, window_bytes, verify_tail)
                .expect("the walk under measurement must succeed");
            verify_tail = false;
            windows += 1;
            for (log_id, record) in &scanned {
                if record.sequence > watermark {
                    applied.push((*log_id, record.sequence));
                } else {
                    decoded_and_dropped += 1;
                }
            }
            if !more_to_come {
                break;
            }
            assert!(
                resume_at.log_id() > from.log_id(),
                "a window that did not advance would spin: resumed at {} from {}",
                resume_at.log_id(),
                from.log_id()
            );
            from = resume_at;
        }
    });
    let (bytes_after, reads_after) = wal_piece_read_counts_on_this_thread();
    Replay {
        windows,
        applied,
        decoded_and_dropped,
        piece_bytes: bytes_after - bytes_before,
        piece_reads: reads_after - reads_before,
        kernel_rchar,
    }
}

/// The same records, from a walk that skips nothing: one unbounded window from log id zero.
///
/// This is the control the identity test is against. It is asserted to be untruncated and
/// non-empty by every caller, because a control that returned nothing would make any comparison
/// with it pass.
fn skip_free(store: &LocalWriteAheadLogStore, watermark: u64) -> (Vec<(u64, u64)>, bool) {
    let (records, truncated) = store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the skip-free control must be able to read the whole log");
    let kept = records
        .into_iter()
        .filter(|(_, record)| record.sequence > watermark)
        .map(|(log_id, record)| (log_id, record.sequence))
        .collect();
    (kept, truncated)
}

/// What the old walk read: every window reading its way from the top of the piece it starts in.
///
/// Reconstructed from the fixture rather than remembered, so there is no recorded constant to go
/// stale. In a one-piece log a window starting at `s` read `s + its own bytes`, so W windows over
/// a log of L bytes read `sum over i of min(i*B, L)`.
fn what_the_walk_from_the_header_would_read(windows: u64, window_bytes: u64, log_bytes: u64) -> u64 {
    (1..=windows)
        .map(|index| (index * window_bytes).min(log_bytes))
        .sum()
}

fn last_sequence(store: &LocalWriteAheadLogStore) -> u64 {
    let (records, truncated) = store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must be readable");
    assert!(!truncated, "the fixture's whole log must fit one unbounded window");
    records
        .last()
        .map(|(_, record)| record.sequence)
        .expect("the fixture must have written something")
}

// ---------------------------------------------------------------------------------------------
// THE TESTS
// ---------------------------------------------------------------------------------------------

/// Two corpus sizes, both log shapes, with every quantity marked FLAT or GROWING.
#[test]
fn a_windowed_replay_reads_the_log_about_once_at_two_corpus_sizes() {
    let mut report = String::new();
    for shape in [Shape::OnePiece, Shape::Rolled] {
        let mut arms = Vec::new();
        for records in [SMALL, BIG] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (store, on_disk) = build_log(dir.path(), records, shape);
            let last = last_sequence(&store);
            // HALF THE LOG UNREFLECTED, which is the regime a crashed shard is in and the one
            // with more than one window in it.
            let watermark = last / 2;
            let measured = replay(&store, watermark, TEST_WINDOW_BYTES);

            // WHAT THE FIXTURE CAN EXPRESS. A fixture with one window cannot tell a walk that
            // resumes from one that starts over, because one window has nothing behind it.
            assert!(
                measured.windows > 1,
                "{} at {records}: the walk must take more than one window or the shape being \
                 measured is absent by construction -- took {}",
                shape.label(),
                measured.windows
            );
            assert!(
                !measured.applied.is_empty(),
                "{} at {records}: the replayed suffix must not be empty",
                shape.label()
