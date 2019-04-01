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
