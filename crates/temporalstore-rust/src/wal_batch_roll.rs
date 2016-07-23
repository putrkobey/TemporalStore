// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Whether the batch append rolls the piece it is writing, and what starts working when it does.
//!
//! ## What was found
//!
//! #1936 took a restore's kernel reads from 1,034,158,338 to 81,232,374 bytes and, in doing so,
//! named the reason that quadratic was catastrophic rather than merely bad: at 200,000 records
//! the shard's log was **a single 32,768,000-byte file**. It measured that and did not fix it.
//!
//! `append_batch_as_one_record` -- the path the engine takes for every batch that produced
//! outcomes, and `TS_WAL_DATA_ONLY` defaults ON -- was the one append entry point that never
//! asked `roll_wal_segment_if_due` whether the piece was full. Every other one asks:
//! `append_with_sync_inner` after its record, `append_for_group_commit` after its record, and
//! `append_batch_atomic` after its batch, with a comment saying exactly why it is after.
//!
//! The roll was not suppressed and it was not on a bypassed path. It was **absent**.
//!
//! ## Why it is not a size problem
//!
//! Almost every log-walking cost in this engine is expressed per PIECE, and several of the
//! mechanisms decide what to read from a piece's NAME. One enormous piece defeats all of them at
//! once, and each of the three was measured here with rolling off and rolling on:
//!
//! * #1917 declines to open a piece by reading the name of the piece after it -- a one-piece log
//!   has no piece after, so there is nothing to decline;
//! * #1920 found nothing bounds the pieces a log retains except a completed dump -- reclaim
//!   unlinks whole pieces, and a one-piece log has no whole piece it may unlink;
//! * #1936's own pre-walk asks every piece for its last sequence, which is cheap over 125 pieces
//!   and is the entire answer over one -- a one-piece log answers log id ZERO, so the replay
//!   starts at the beginning and decodes every record the watermark then throws away.
//!
//! ## The two-line change, and why it is two
//!
//! Adding the roll ALONE is not safe. `append_batch_as_one_record` also skipped
//! `ensure_active_wal_segment`, which every other entry point calls, and which was free to skip
//! for exactly as long as this path never rolled: a log that is always one piece has no sealed
//! pieces for a headerless new piece to collide with. The roll creates them. A crash between the
//! seal-rename and the new piece's header then leaves the next batch writing into a file with no
//! base header, which reads as starting at log id ZERO -- an address the sealed pieces already
//! own. `a_crash_inside_the_roll_leaves_no_two_records_sharing_a_log_id` drives both fault points
//! and asserts no log id is handed out twice.
//!
//! ## DIRECTION
//!
//! A log that rolls too eagerly costs a few extra files. A log that loses or reorders a record on
//! a piece boundary is silent data loss, and it loads clean. So the boundary is asserted in the
//! strong form: `the_records_a_rolled_batch_log_replays_are_the_ones_an_unrolled_one_holds`
//! compares the `(log id, sequence, record bytes)` SEQUENCE a windowed replay applies, element by
//! element, against the same replay of a log holding the same batches in one piece -- at six
//! watermarks, with the boundary asserted to fall inside the compared span.
//!
//! That comparison is available at all because a roll preserves log ids exactly: a log id is
//! cumulative RECORD bytes, the base header is not a record, and a piece starts at
//! `base + holds`. A rolled log and an unrolled log holding the same records address them
//! identically, so "the same records" can be asserted as equality rather than as a count.

#![cfg(test)]

use crate::fault::{self, FaultAction};
use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread, LocalWriteAheadLogStore,
    ReplayPosition, WalOutcomeItem,
};

/// The size the engine rolls at (`DEFAULT_WAL_SEGMENT_BYTES`, 256 KiB) and the window the engine
/// replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`, 512 KiB), both scaled down by
/// eight so a test log of a few hundred kilobytes still crosses several pieces and several
/// windows. What is measured here is pieces against the log, and that is set by the RATIO.
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;
const TEST_WINDOW_BYTES: u64 = 64 * 1024;

/// Two corpus sizes and the ratio between them. Four times the corpus, so a quantity that is flat
/// and one that grows with the log are four times apart and cannot be confused for each other.
const SMALL: usize = 250;
const BIG: usize = 1_000;
const RATIO: usize = BIG / SMALL;

/// Items in a batch, and the bytes each one carries. Eight is the size #1936's own batch-cost
/// measurement used.
const ITEMS: usize = 8;
const VALUE_BYTES: usize = 128;

const SHARD: crate::types::ShardId = 1;

/// Restores the rolling threshold when it drops, including while a panic unwinds.
///
/// The override is per THREAD and the gate runs with `--test-threads=1`, so a test that set it
/// and then panicked would leave every test after it on that thread rolling at this size.
struct SegmentBytes;

impl SegmentBytes {
    fn set(threshold: u64) -> Self {
        set_wal_segment_bytes_for_test(Some(threshold));
        SegmentBytes
    }
}

impl Drop for SegmentBytes {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(None);
    }
}

/// xorshift64*, seeded per item. Records are compressed, so a corpus of repeated bytes holds a
/// fraction of the bytes its value size implies and moves the piece count where it is not meant
/// to go. Each item needs its OWN payload: sixteen identical payloads in ONE record compress
/// just as well as one, which is the correction #1936's batch-cost measurement had to make.
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

fn outcomes(batch: usize) -> Vec<WalOutcomeItem> {
    (0..ITEMS)
        .map(|item| {
            let id = (batch * ITEMS + item) as u64;
            WalOutcomeItem {
                kind: "string".to_string(),
                object_key: format!("batch-{batch:08}-item-{item:03}"),
                component: None,
                object_id: id,
                routing_bucket: item as u32,
                address: None,
                value: Some(incompressible(VALUE_BYTES, id)),
                ttl: None,
                deleted: false,
                meta: false,
            }
        })
        .collect()
}

/// Which append entry point writes the corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    /// `append_batch_as_one_record`: the subject. The engine's data-only batch path.
    Batch,
    /// `append`: the control. Rolling has always worked here, so an arm that fails to roll on
    /// THIS path is a fixture that never set the threshold, not a defect in the subject.
    Single,
}

impl Writer {
    fn label(self) -> &'static str {
        match self {
            Writer::Batch => "append_batch_as_one_record",
            Writer::Single => "append (control)",
        }
    }

    fn write(self, store: &LocalWriteAheadLogStore, batch: usize) {
        match self {
            Writer::Batch => {
                store
                    .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
                    .expect("the fixture's batch must land");
            }
            Writer::Single => {
                // One record of about the same size as a batch record, so the two arms lay down
                // comparable numbers of bytes and the piece counts are comparable too.
                store
                    .append(
                        SHARD,
                        Command::StringSet {
                            key: format!("single-{batch:08}"),
                            value: incompressible(ITEMS * VALUE_BYTES, batch as u64),
                        },
                    )
                    .expect("the fixture's write must land");
            }
        }
    }
}

fn build(dir: &std::path::Path, batches: usize, writer: Writer) -> LocalWriteAheadLogStore {
    let store = LocalWriteAheadLogStore::new(dir);
    for batch in 0..batches {
        writer.write(&store, batch);
    }
    store
}

// ---------------------------------------------------------------------------------------------
// WHAT IS ON DISK, READ BACK FROM IT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Pieces {
    /// Every piece of the log, the one being written included. Counted from the DIRECTORY, so a
    /// piece the extent listing declined to report would still be seen here.
    count: usize,
    /// Pieces that have been sealed -- the ones with a log id in their name, and the only ones
    /// reclaim may unlink and a walk may decline by name.
    sealed: usize,
    /// RECORD bytes, summed over the pieces. Not file bytes: under preallocation the piece being
    /// WRITTEN is longer than its records by up to a whole reservation chunk, which charges a
    /// small corpus and a large one the same constant and makes a linear total read as
    /// sub-linear. Sealed pieces are trimmed to their contents (`roll_wal_segment_if_due` does it
    /// before the rename, so that a sealed piece's length IS its contents), so only the active
    /// piece differs -- and log ids are record bytes, so this is the quantity every per-piece
    /// mechanism here is expressed in anyway.
    total_bytes: u64,
    /// The most record bytes any one piece holds. The subject of the FLAT claim.
    largest_bytes: u64,
    /// What the files actually occupy, reservation included. Reported rather than asserted on.
    file_bytes: u64,
}

impl Pieces {
    /// `record_end` is one past the log's last record byte -- `log id + length` of the last
    /// record, which is the definition of where the records stop.
    fn of(dir: &std::path::Path, record_end: u64) -> Self {
        let mut count = 0usize;
        let mut sealed = 0usize;
        let mut file_bytes = 0u64;
        let active = format!("shard-{SHARD}.wal.bin");
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.contains(".wal") || name.ends_with(".lock") {
                continue;
            }
            file_bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            count += 1;
            if name != active {
                sealed += 1;
            }
        }
        let extents = crate::wal::wal_piece_extents_for_test(dir, SHARD);
        assert_eq!(
            extents.len(),
            count,
            "the extent listing and the directory must agree on how many pieces there are"
        );
        // Where the RECORDS stop, taken from the records. The extent listing reports the active
        // piece's FILE length, which under preallocation runs past its records by up to a whole
        // reservation chunk -- a constant, charged to every corpus alike, which turns a linear
        // total into a sub-linear one and a flat largest piece into a growing one.
        //
        // A piece holds everything from its own base to the next piece's base, and the last piece
        // holds everything from its base to where the records stop. Sealed pieces are trimmed to
        // their contents before the rename, so for those the two agree; this is the only piece
        // where they can differ, and it is where the reservation lives.
        let mut bases: Vec<u64> = extents.iter().map(|(_, _, base, _)| *base).collect();
        bases.sort_unstable();
        let mut total_bytes = 0u64;
        let mut largest_bytes = 0u64;
        for (index, base) in bases.iter().enumerate() {
            let end = bases.get(index + 1).copied().unwrap_or(record_end);
            let holds = end.saturating_sub(*base);
            total_bytes += holds;
            largest_bytes = largest_bytes.max(holds);
        }
        assert_eq!(
            total_bytes, record_end,
            "the pieces must tile the log's record bytes with no hole and no overlap"
        );
        Pieces {
            count,
            sealed,
            total_bytes,
            largest_bytes,
            file_bytes,
        }
    }
}

/// A record as a reader sees it: where it lives, what it is called, and its exact bytes.
///
/// The element of the element-by-element comparison. A count is not enough -- a walk that drops
/// one record and keeps an extra reports the same count as a correct one -- and neither is a
/// sequence on its own, which a walk replaying the wrong records still gets right.
type Element = (u64, u64, Vec<u8>);

fn elements(store: &LocalWriteAheadLogStore) -> Vec<Element> {
    let raw = store
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must scan");
    let (decoded, truncated) = store
        .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must decode");
    assert!(
        !truncated,
        "the fixture's whole log must fit one unbounded walk or the control is a prefix"
    );
    assert_eq!(
        raw.len(),
        decoded.len(),
        "the byte walk and the decoded walk must see the same records"
    );
    raw.into_iter()
        .zip(decoded)
        .map(|((log_id, bytes), (decoded_log_id, record))| {
            assert_eq!(
                log_id, decoded_log_id,
                "the two walks must address the same record identically"
            );
            (log_id, record.sequence, bytes)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite running tests beside each other charges this
// span whatever another thread happened to read. Reading the counter is itself a read the kernel
// charges, so the reading carries the LENGTH of the text it came from and the span subtracts it
// exactly rather than estimating it.
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
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE REPLAY UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Replay {
    windows: u64,
    /// Where the replay started reading -- #1936's pre-walk answer. Zero means "the beginning of
    /// the log", which is what a one-piece log always answers.
    started_at: u64,
    /// `(log id, sequence)` for every record the replay would APPLY, in order. The bytes are
    /// not here: fetching them needs a second walk, and a second walk inside the measured span
    /// declines the same pieces again and doubles what #1917's counter reports. The
    /// element-by-element comparison takes them from `replay_with_bytes`, outside the span.
    applied: Vec<(u64, u64)>,
    /// Records the walk decoded and the watermark then threw away. The RELATION between this and
    /// `applied` is what says whether the replay started in the right place.
    decoded_and_dropped: u64,
    /// Pieces the walk declined to open by reading the next piece's NAME -- #1917's mechanism.
    pieces_declined_by_name: u64,
    /// Bytes pulled off log pieces, from the counter inside the read itself (#1936's).
    piece_bytes: u64,
    kernel_rchar: Option<u64>,
}

fn counters() -> (u64, u64, u64) {
    let (bytes, _reads) = wal_piece_read_counts_on_this_thread();
    let declined = crate::wal::WAL_PIECES_SKIPPED_BY_NAME.with(|skipped| skipped.get());
    let bodies = crate::wal::WAL_PIECE_BODY_READS.with(|reads| reads.get());
    (bytes, declined, bodies)
}

/// Replay `store` the way `replay_wal_into_shard_windowed` does: start past the pieces the
/// watermark already covers, then bounded windows resuming where the last one stopped.
fn replay(store: &LocalWriteAheadLogStore, watermark: u64, window_bytes: u64) -> Replay {
    let (bytes_before, declined_before, _) = counters();
    let mut windows = 0u64;
    let mut applied: Vec<(u64, u64)> = Vec::new();
    let mut decoded_and_dropped = 0u64;
    let mut started_at = 0u64;
    let (_, kernel_rchar) = kernel_read_span(|| {
        let mut from = store
            .replay_start_after_sequence(SHARD, watermark)
            .expect("a replay must be able to find where to start");
        started_at = from.log_id();
        let mut verify_tail = true;
        loop {
            let (scanned, more_to_come, resume_at) = store
                .scan_decoded_window(SHARD, from, window_bytes, verify_tail)
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
    let (bytes_after, declined_after, _) = counters();
    Replay {
        windows,
        started_at,
        applied,
        decoded_and_dropped,
        pieces_declined_by_name: declined_after - declined_before,
        piece_bytes: bytes_after - bytes_before,
        kernel_rchar,
    }
}

/// The same windowed replay, carrying each record's BYTES -- run OUTSIDE any measured span.
///
/// Getting the bytes needs a second walk over the same window, and a second walk inside the span
/// declines the same pieces again, which doubles what #1917's counter reports for the replay. The
/// bytes are wanted for the element-by-element comparison and the counters are wanted for the
/// table, so the two runs are separate and neither pollutes the other.
fn replay_with_bytes(
    store: &LocalWriteAheadLogStore,
    watermark: u64,
    window_bytes: u64,
) -> Vec<Element> {
    let mut applied: Vec<Element> = Vec::new();
    let mut from = store
        .replay_start_after_sequence(SHARD, watermark)
        .expect("a replay must be able to find where to start");
    let mut verify_tail = true;
    loop {
