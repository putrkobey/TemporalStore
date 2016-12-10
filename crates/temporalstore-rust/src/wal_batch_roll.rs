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
        let (decoded, more_to_come, resume_at) = store
            .scan_decoded_window(SHARD, from, window_bytes, verify_tail)
            .expect("the walk must succeed");
        let raw = store
            .scan(SHARD, from.log_id(), u64::MAX, window_bytes)
            .expect("the byte walk over the same window must succeed");
        let by_log_id: std::collections::HashMap<u64, Vec<u8>> = raw.into_iter().collect();
        verify_tail = false;
        for (log_id, record) in decoded {
            let bytes = by_log_id.get(&log_id).cloned().expect(
                "every record the decoded walk returns must have bytes at the same log id",
            );
            if record.sequence > watermark {
                applied.push((log_id, record.sequence, bytes));
            }
        }
        if !more_to_come {
            break;
        }
        from = resume_at;
    }
    applied
}

/// One past the log's last record byte. A log id IS a byte position in the log's history, so
/// the last record's log id plus its length is where the records stop -- derived from the
/// records rather than from any file's length.
fn record_end(store: &LocalWriteAheadLogStore) -> u64 {
    elements(store)
        .last()
        .map(|(log_id, _, bytes)| log_id + bytes.len() as u64)
        .expect("the fixture must have written something")
}

fn last_sequence(store: &LocalWriteAheadLogStore) -> u64 {
    elements(store)
        .last()
        .map(|(_, sequence, _)| *sequence)
        .expect("the fixture must have written something")
}

/// Compare two record sequences element by element, reporting the FIRST place they differ.
///
/// `assert_eq!` on two thousand-element vectors of kilobyte records prints both of them: the
/// assertion is right and twenty megabytes of hex is not a failure anyone can read. This says
/// which element, at which log id, and how -- and asserts the lengths after, so a prefix that
/// matches and then stops is not read as agreement.
fn same_records(subject: &[Element], control: &[Element], context: &str) {
    for (index, (left, right)) in subject.iter().zip(control).enumerate() {
        assert_eq!(
            (left.0, left.1, left.2.len()),
            (right.0, right.1, right.2.len()),
            "{context}: record {index} differs -- (log id, sequence, bytes) {:?} against {:?}",
            (left.0, left.1, left.2.len()),
            (right.0, right.1, right.2.len())
        );
        assert!(
            left.2 == right.2,
            "{context}: record {index} at log id {} (seq {}) has the same length and different \
             bytes",
            left.0,
            left.1
        );
    }
    assert_eq!(
        subject.len(),
        control.len(),
        "{context}: the sequences agree for {} records and then one of them stops",
        subject.len().min(control.len())
    );
}

/// The same records, from a walk that skips nothing: one unbounded window from log id zero.
///
/// The control the boundary comparison is against, and it is the SAME LOG -- so the comparison
/// is byte for byte, and what it isolates is the windowed walk crossing a piece boundary rather
/// than any difference between two corpora.
fn skip_free(store: &LocalWriteAheadLogStore, watermark: u64) -> Vec<Element> {
    elements(store)
        .into_iter()
        .filter(|(_, sequence, _)| *sequence > watermark)
        .collect()
}

/// Which log id and which sequence each record has, and how long it is.
///
/// What a roll could change, without the one thing it cannot: a record carries
/// `timestamp_ms: current_time_ms()`, so two logs built from the same batches at different
/// moments hold different bytes for the same record. That is a property of the record.
fn addressing(records: &[Element]) -> Vec<(u64, u64, usize)> {
    records
        .iter()
        .map(|(log_id, sequence, bytes)| (*log_id, *sequence, bytes.len()))
        .collect()
}

fn thousands(value: u64) -> String {
    let text = value.to_string();
    let mut out = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index > 0 && (text.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// 1. THE BEHAVIOUR, AT TWO CORPUS SIZES
// ---------------------------------------------------------------------------------------------

/// The batch path rolls, and the piece it writes stays the size the threshold says.
///
/// Every quantity is marked FLAT or GROWING against the 4x corpus ratio, and the ordinary append
/// path runs beside it as a control: rolling has always worked there, so an arm where NEITHER
/// path rolls is a fixture that failed to set the threshold rather than a defect in the subject.
#[test]
fn a_batch_append_rolls_the_log_piece_at_two_corpus_sizes() {
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
    let mut report = String::from("\n  rolling at 32,768 B\n");
    for writer in [Writer::Batch, Writer::Single] {
        let mut arms: Vec<(usize, Pieces, usize)> = Vec::new();
        for batches in [SMALL, BIG] {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = build(dir.path(), batches, writer);
            let on_disk = Pieces::of(dir.path(), record_end(&store));
            let records = elements(&store).len();
            // Full scans of the log taken by the append fast path's fallback. A roll replaces the
            // piece being written, so it also replaces the record-end that the next append's
            // O(1) check is against; both are updated by the roll, and a writer that then put the
            // SEALED piece's length back would make the check disagree with the file and send
            // every post-roll append through a whole-log scan instead.
            //
            // Correctness survives that -- the fallback repairs the cache before the record is
            // placed -- so nothing about the records would show it. This is the quantity that
            // does. `flat_append` is on by default, so the fast path being measured is live.
            let full_scans = store.stats(SHARD).append_full_scans;

            // VACUITY FLOORS. Each of these, absent, makes every assertion below pass on a
            // fixture that measured nothing.
            assert_eq!(
                records, batches,
                "{}: the fixture must have written one record per batch",
                writer.label()
            );
            assert!(
                on_disk.total_bytes > TEST_SEGMENT_BYTES * 2,
                "{} at {batches}: the corpus must be several pieces' worth of bytes or there is \
                 nothing to roll -- {} B",
                writer.label(),
                on_disk.total_bytes
            );
            assert!(
                on_disk.count > 1,
                "{} at {batches}: THE LOG MUST ROLL -- found {} piece(s) holding {} B",
                writer.label(),
                on_disk.count,
                thousands(on_disk.total_bytes)
            );
            // The piece being written is never sealed, so a rolled log has count - 1 sealed --
            // and the sealed ones are the only ones reclaim may unlink and a walk may decline.
            assert_eq!(
                on_disk.sealed,
                on_disk.count - 1,
                "{} at {batches}: every piece but the one being written must be sealed",
                writer.label()
            );
            assert!(
                on_disk.sealed > 2,
                "{} at {batches}: the scan-count assertion below needs several rolls to be about \
                 anything -- {} sealed piece(s)",
                writer.label(),
                on_disk.sealed
            );
            assert!(
                full_scans <= 2,
                "{} at {batches}: {} rolls cost {full_scans} whole-log scans on the append fast \
                 path -- a roll must leave the record-end cache describing the piece it STARTED, \
                 not the one it sealed",
                writer.label(),
                on_disk.sealed,
                full_scans = full_scans
            );
            arms.push((batches, on_disk, records));
        }

        let (small_batches, small, _) = arms[0].clone();
        let (big_batches, big, _) = arms[1].clone();
        report.push_str(&format!(
            "  {:<28} {:>6} recs {:>4} pieces  largest {:>9} B  records {:>11} B  \
files {:>11} B\n\
             \x20 {:<28} {:>6} recs {:>4} pieces  largest {:>9} B  records {:>11} B  \
files {:>11} B\n\
             \x20 {:<28} {:>11} {:>12} {:>18} {:>19} {:>18}\n",
            writer.label(),
            small_batches,
            small.count,
            thousands(small.largest_bytes),
            thousands(small.total_bytes),
            thousands(small.file_bytes),
            "",
            big_batches,
            big.count,
            thousands(big.largest_bytes),
            thousands(big.total_bytes),
            thousands(big.file_bytes),
            "RATIO",
            "",
            format!("{:.2}x", big.count as f64 / small.count as f64),
            format!("{:.2}x", big.largest_bytes as f64 / small.largest_bytes as f64),
            format!("{:.2}x", big.total_bytes as f64 / small.total_bytes as f64),
            format!("{:.2}x", big.file_bytes as f64 / small.file_bytes as f64),
        ));

        // GROWING: the bytes, and the pieces with them.
        assert!(
            big.total_bytes as f64 / small.total_bytes as f64 > RATIO as f64 * 0.8,
            "{}: the log's BYTES must grow with the corpus -- {} -> {}",
            writer.label(),
            small.total_bytes,
            big.total_bytes
        );
        assert!(
            big.count as f64 / small.count as f64 > RATIO as f64 * 0.8,
            "{}: the PIECE COUNT must grow with the corpus, or the pieces are growing instead -- \
             {} -> {}",
            writer.label(),
            small.count,
            big.count
        );

        // FLAT: the largest piece. This is the whole claim. One batch record can straddle the
        // threshold -- the roll is taken AFTER the record lands, so the piece that seals holds
        // the record that filled it -- and that is the only slack allowed.
        let record_bytes = big.total_bytes / big_batches as u64;
        let ceiling = TEST_SEGMENT_BYTES + record_bytes * 2;
        for (batches, on_disk, _) in &arms {
            assert!(
                on_disk.largest_bytes <= ceiling,
                "{} at {batches}: the LARGEST PIECE must stay at the threshold, not grow with \
                 the corpus -- {} B against a ceiling of {} B",
                writer.label(),
                thousands(on_disk.largest_bytes),
                thousands(ceiling)
            );
        }
        assert!(
            big.largest_bytes as f64 / small.largest_bytes as f64 <= 1.25,
            "{}: the largest piece must be FLAT across a {RATIO}x corpus -- {} -> {}",
            writer.label(),
            small.largest_bytes,
            big.largest_bytes
        );
    }
    println!("{report}");
}

/// Never rolling is still reachable, and it is what the batch path used to do unconditionally.
///
/// The negative control for the test above. `TS_WAL_SEGMENT_BYTES=0` means "never roll", and a
/// threshold that stopped being read would make the rolling assertions above pass for the wrong
/// reason -- so this asserts the same fixture, with rolling off, produces exactly ONE growing
/// piece.
#[test]
fn with_rolling_off_the_batch_path_writes_one_piece_that_grows_with_the_corpus() {
    let _threshold = SegmentBytes::set(0);
    let mut largest = Vec::new();
    for batches in [SMALL, BIG] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = build(dir.path(), batches, Writer::Batch);
        let on_disk = Pieces::of(dir.path(), record_end(&store));
        assert_eq!(
            elements(&store).len(),
            batches,
            "the fixture must have written one record per batch"
        );
        assert_eq!(
            on_disk.count, 1,
            "with rolling off the log must be ONE piece -- found {}",
            on_disk.count
        );
        assert_eq!(
            on_disk.sealed, 0,
            "with rolling off nothing may be sealed -- found {} sealed",
            on_disk.sealed
        );
        println!(
            "  rolling off, {batches} batches: 1 piece holding {} B of records ({} B on disk)",
            thousands(on_disk.largest_bytes),
            thousands(on_disk.file_bytes)
        );
        largest.push(on_disk.largest_bytes);
    }
    assert!(
        largest[1] as f64 / largest[0] as f64 > RATIO as f64 * 0.8,
        "with rolling off the ONE piece must grow with the corpus -- {} -> {}",
        largest[0],
        largest[1]
    );
}

// ---------------------------------------------------------------------------------------------
// 2. THE BATCH STILL LANDS WHOLE
// ---------------------------------------------------------------------------------------------

/// A batch record is never split across two pieces, and the piece that sealed holds it whole.
///
/// The correctness constraint the fix has to respect. A batch written this way IS one record, so
/// there is no inside to roll in -- but "there is no inside" is an argument, and this is the
/// measurement: every piece parses into whole frames from its header to its last byte, every
/// record lives in exactly one piece, and the records concatenated across pieces in piece order
/// are the log's records in log order.
#[test]
fn a_batch_record_lands_whole_inside_one_piece() {
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = build(dir.path(), BIG, Writer::Batch);
    let on_disk = Pieces::of(dir.path(), record_end(&store));
    assert!(
        on_disk.count > 2,
        "this test needs several boundaries to be about anything -- found {} piece(s)",
        on_disk.count
    );

    let extents = crate::wal::wal_piece_extents_for_test(dir.path(), SHARD);
    assert_eq!(
        extents.len(),
        on_disk.count,
        "the extent listing must see every piece the directory holds"
    );

    // Every record, and which piece's log-id range it falls in. A record that straddled a
    // boundary would start inside one piece's range and end past it.
    let records = elements(&store);
    assert_eq!(records.len(), BIG, "one record per batch");
    let mut per_piece = vec![0usize; extents.len()];
    for (log_id, sequence, bytes) in &records {
        let end = log_id + bytes.len() as u64;
        let home = extents
            .iter()
            .position(|(_, _, base, piece_end)| *log_id >= *base && *log_id < *piece_end)
            .unwrap_or_else(|| {
                panic!("record seq {sequence} at log id {log_id} lives in no piece's range")
            });
        let (path, named_start, base, piece_end) = &extents[home];
        assert!(
            end <= *piece_end,
            "record seq {sequence} at log id {log_id} is {} B and runs {} B past the end of \
             {} -- a batch record was SPLIT across a piece boundary",
            bytes.len(),
            end - piece_end,
            path.display()
        );
        // A SEALED piece's NAME still describes its contents: the number in the name is the log
        // id the piece's records begin at, and it agrees with the header the piece carries. Every
        // walk that declines a piece by name rests on this.
        if let Some(named_start) = named_start {
            assert_eq!(
                named_start, base,
                "the name of {} says its contents start at {named_start} and its header says \
                 {base}",
                path.display()
            );
        }
        per_piece[home] += 1;
    }
    let placed: usize = per_piece.iter().sum();
    assert_eq!(
        placed,
        records.len(),
        "every record must be placed in exactly one piece"
    );
    assert!(
        per_piece.iter().filter(|count| **count > 0).count() > 2,
        "the records must be spread over several pieces or no boundary was crossed -- {per_piece:?}"
    );

    // The roll fires AFTER the record, so every sealed piece holds at least the threshold. A roll
    // taken BEFORE the record would seal pieces one record short of it.
    for (path, _, base, piece_end) in extents.iter().take(extents.len() - 1) {
        assert!(
            piece_end - base >= TEST_SEGMENT_BYTES,
            "sealed piece {} holds {} B, under the {TEST_SEGMENT_BYTES} B threshold -- the roll \
             was taken BEFORE the record that filled it",
            path.display(),
            piece_end - base
        );
    }
}

// ---------------------------------------------------------------------------------------------
// 3. DURABILITY ACROSS THE NEW BOUNDARY
// ---------------------------------------------------------------------------------------------

/// A crash inside the roll leaves no two records answering to one log id.
///
/// Rolling is a seal-rename and then a create-and-header, two steps with a crash window between
/// them, and the roll is what makes this path have sealed pieces at all. A headerless piece reads
/// as starting at log id ZERO, which the sealed pieces already own -- so without
/// `ensure_active_wal_segment` the batch after the crash is addressed at an id another record
/// already has, and a walk resolving that id reaches whichever it finds first.
///
/// Both fault points are driven, and each is asserted to have been REACHED: a crash test that
/// never reaches its point passes while testing nothing.
#[test]
fn a_crash_inside_the_roll_leaves_no_two_records_sharing_a_log_id() {
    for point in ["wal/roll/after_rename", "wal/roll/after_create"] {
        let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
        let dir = tempfile::tempdir().expect("tempdir");

        // Write with the point armed, until one of the appends reaches it.
        let mut crashed_at = None;
        {
            let store = build(dir.path(), 1, Writer::Batch);
            let mut batch = 1usize;
            // The `Stop` action is a panic and the default hook prints it. This span expects
            // exactly one, so the hook is silenced across it and restored after.
            let previous_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            loop {
                assert!(batch < 10_000, "the fixture must reach a roll");
                let armed = fault::arm(point, FaultAction::Stop);
                assert!(fault::is_armed(point), "the point must be armed");
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    store
                        .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
                        .map(|_| ())
                }));
                drop(armed);
                batch += 1;
                if outcome.is_err() {
                    crashed_at = Some(batch);
                    break;
                }
                outcome.expect("caught").expect("the batch must land");
            }
            std::panic::set_hook(previous_hook);
            // The panic unwound through the roll while the `inner` mutex guard was held, so this
            // store's lock is poisoned and every later call on it panics with that instead of
            // with what this test is asking about. A crash loses the process anyway; the log is
            // read back through a fresh store below, which is what a restart does.
        }
        let crashed_at = crashed_at.expect("the fixture must have reached the fault point");
        assert!(
            crashed_at > 2,
            "{point}: the crash must land after several batches, not on the first"
        );

        // REOPEN, as a restart would. What the log holds now is what the crash left: the record
        // that triggered the roll is on disk, because the roll runs after it.
        let store = LocalWriteAheadLogStore::new(dir.path());
        let before_crash = elements(&store);
        for batch in crashed_at..(crashed_at + 40) {
            store
                .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
