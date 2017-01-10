// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Why the write-ahead-log half of a maintenance round freed nothing, decided at the byte.
//!
//! #1928 measured a dump at 20,000 and 200,000 records and reported, as a side result it
//! deliberately did not explain, that the write-ahead-log half released 0 B in 0 records at
//! both sizes -- while a single-command control arm in the same fixture released 261,933 B.
//! It named three candidates (how many records the batch path wrote, the total bytes in the
//! log, a newline byte inside the payload), drove each, and ruled each out.
//!
//! The decision is here, and it is none of the three. It is the LAST byte of the payload.
//!
//! A record is written as a binary frame -- marker, a varint length, a four-byte digest, then
//! exactly that many payload bytes -- and `TS_WAL_BINARY_FRAME` is ON unless it is switched off,
//! so this is what every record is. A binary frame carries no delimiter: it ends where its
//! declared length ends. The reclaim walk in `gc_before_sequence_unchecked` took what
//! `read_raw_record` handed it and ran `strip_suffix(b"\n")` over it before decoding, which is
//! right for a text record and wrong for a frame. When the payload's final byte happened to be
//! `0x0A` the strip took it, `next_frame` then saw one byte fewer than the frame declared,
//! called it a torn tail, and `decode_line` turned that into `binary record is incomplete` --
//! refusing a sweep over records that were entirely intact on disk.
//!
//! The error then reached `dump_and_reclaim_index_logs_with_min_reclaimable`, which drops the
//! sweep's `Result` with `.ok()` and reads every field through `unwrap_or_default()`. So the
//! refusal arrived at the operator as three zeros: identical, to the byte, to a store whose log
//! held nothing at all. That is the rendering #1928 shipped a guard for, and it is why the
//! failure survived being measured twice.
//!
//! Why the three candidates could not find it: the last byte of one of these payloads is a
//! trailing varint field, so it is a deterministic function of the record's CONTENT and moves
//! with the batch's shape rather than with the number of records, the bytes in the log, or the
//! bytes inside the payload. `b'\n'` written as a VALUE -- candidate three -- lands in the
//! middle of the payload and changes nothing, which is exactly what #1928 measured.
//!
//! The same rule was already written down, correctly, in the sibling walk a few hundred lines
//! away in `wal.rs` (the one that fills in a piece's record count), with a comment saying a
//! binary frame ends where its declared length ends and trimming it would take a byte of the
//! payload. The reclaim walk did not have it. Two copies of one rule, one of them right.
//!
//! DIRECTION. Freeing too much is silent data loss; freeing too little is space held. This
//! defect freed too little -- the sweep refused before it rewrote anything -- so nothing was
//! lost. The strong form is asserted anyway, element by element against a control that never
//! reclaimed, because a count cannot see a sweep that drops one record and keeps an extra.

use super::*;
use crate::wal::{decode_wal_line, LocalWriteAheadLogStore, WalOutcomeItem};
use std::path::Path;

const SHARD: ShardId = 1;

fn release_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

/// Where the search for a batch shape starts. 100 items per batch encoded to a payload ending
/// in `0x0A` when this was written, so the search finds it on the first try; the search exists
/// so that a change to the encoding moves the fixture instead of breaking it, and the assertion
/// that one was FOUND is what stops it quietly testing nothing.
const SEARCH_FROM: usize = 100;
const SEARCH_TRIES: usize = 512;

fn outcomes(count: usize) -> Vec<WalOutcomeItem> {
    (0..count)
        .map(|index| WalOutcomeItem {
            kind: "string".to_string(),
            object_key: format!("batch-key-{index:09}"),
            component: None,
            object_id: index as u64,
            routing_bucket: index as u32,
            address: None,
            value: Some(vec![b'v'; 8]),
            ttl: None,
            deleted: false,
            meta: false,
        })
        .collect()
}

/// The last byte of the payload a batch of `items` encodes to.
///
/// Measured from the file rather than predicted: this is the quantity the whole defect turns on,
/// and the fixture below has to be able to say which side of it each arm sits on.
fn trailing_payload_byte(items: usize) -> u8 {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(dir.path());
    seed_batches(&store, 1, items);
    frames_of(&active_log(dir.path()))
        .first()
        .expect("one batch wrote one frame")
        .last_payload_byte
}

/// The first batch size at or after `SEARCH_FROM` whose payload ends in the delimiter byte, and
/// the first that does not. Both, or the test says so rather than running a one-sided compare.
fn batch_sizes_either_side_of_the_delimiter() -> (usize, usize) {
    let mut ends_in_delimiter = None;
    let mut does_not = None;
    for items in SEARCH_FROM..(SEARCH_FROM + SEARCH_TRIES) {
        let byte = trailing_payload_byte(items);
        if byte == b'\n' && ends_in_delimiter.is_none() {
            ends_in_delimiter = Some(items);
        } else if byte != b'\n' && does_not.is_none() {
            does_not = Some(items);
        }
        if ends_in_delimiter.is_some() && does_not.is_some() {
            break;
        }
    }
    let subject = ends_in_delimiter.unwrap_or_else(|| {
        panic!(
            "APPARATUS: no batch size in {SEARCH_FROM}..{} encoded to a payload ending in 0x0A, \
             so this module cannot build the fixture it measures. The encoding moved; widen the \
             search rather than deleting the arm",
            SEARCH_FROM + SEARCH_TRIES
        )
    });
    let control = does_not.expect("APPARATUS: every batch size ended in 0x0A, which cannot be");
    (subject, control)
}

/// Every piece of the log, oldest first. A log rolls into sealed pieces once it outgrows one, and
/// a measurement that looks only at the active piece stops seeing most of it.
fn log_pieces(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .expect("the log directory is readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .filter(|path| path.extension().map(|ext| ext != "lock").unwrap_or(true))
        .collect();
    found.sort();
    found
}

/// Bytes the log occupies on disk, summed over its pieces.
///
/// The independent quantity in the rounds test below: it comes from the filesystem, never from
/// the sweep's own report, so a sweep that miscounts cannot supply the figure that checks it.
/// It also counts whole pieces the sweep unlinks, which `bytes_before`/`bytes_after` do not --
/// those describe the active piece and report dropped pieces in a separate field.
fn log_bytes(root: &std::path::Path) -> u64 {
    log_pieces(root)
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|meta| meta.len())
        .sum()
}

fn active_log(root: &std::path::Path) -> std::path::PathBuf {
    let mut found: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .expect("the log directory is readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .filter(|path| path.extension().map(|ext| ext != "lock").unwrap_or(true))
        .collect();
    found.sort();
    found.into_iter().next().expect("an active log piece")
}

/// What a walker sees: every frame's start offset, its total length, and its payload's last byte.
///
/// Read from the file's own bytes by this module, never from anything the reclaim reports, so a
/// reclaim that miscounts cannot also supply the count that checks it.
///
/// Walks records only, and so stops at the first block footer: a log is laid out in 128 KiB
/// blocks with a footer slot at the end of each, and stepping over one is the engine's own
/// business. That is enough for the fixtures below, which are a few hundred frames inside the
/// first block, and it is why the rounds test measures bytes off the filesystem instead.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Frame {
    at: u64,
    total: u64,
    payload_len: u64,
    last_payload_byte: u8,
}

fn frames_of(path: &std::path::Path) -> Vec<Frame> {
    let bytes = std::fs::read(path).expect("the log piece is readable");
    let mut frames = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        // A zero where a record would start is preallocated room, never a record.
        if bytes[cursor] == 0 || bytes[cursor] != 0xB3 {
            break;
        }
        let mut declared: u64 = 0;
        let mut shift = 0u32;
        let mut at = cursor + 1;
        loop {
            let byte = bytes[at];
            at += 1;
            declared |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let payload_end = at + 4 + declared as usize;
        if payload_end > bytes.len() {
            break;
        }
        frames.push(Frame {
            at: cursor as u64,
            total: (payload_end - cursor) as u64,
            payload_len: declared,
            last_payload_byte: bytes[payload_end - 1],
        });
        cursor = payload_end;
    }
    frames
}

fn seed_batches(store: &LocalWriteAheadLogStore, batches: usize, items: usize) {
    for _ in 0..batches {
        store
            .append_batch_as_one_record(SHARD, outcomes(items), Vec::new(), true)
            .expect("the batch appends");
    }
}

/// Every record the log holds, in order, as (sequence, the exact bytes on disk).
///
/// The element-by-element subject of the strong-form comparison below. Counts are not enough:
/// a sweep that drops one record and keeps an extra reports the same count as a correct one.
fn records_in_order(store: &LocalWriteAheadLogStore) -> Vec<(u64, Vec<u8>)> {
    store
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the log scans")
        .into_iter()
        .map(|(_, raw)| {
            let sequence = decode_wal_line(crate::log_framing::record_body(&raw))
                .expect("every record the log serves decodes")
                .sequence;
            (sequence, raw)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// THE DECISION
// ---------------------------------------------------------------------------------------------

/// The defect, isolated to the one byte that causes it.
///
/// Two fixtures differing only in how many items each batch carried. The test asserts which
/// side of the byte each landed on -- proving the fixture can express the thing measured -- and
/// then that BOTH reclaim. Before the fix the subject arm returned
/// `binary record is incomplete` and the control arm swept cleanly.
/// rust-internal: drives the write-ahead log's own reclaim, no product behaviour
#[test]
fn a_reclaim_sweeps_a_log_whose_frames_end_in_the_delimiter_byte() {
    let (subject_items, control_items) = batch_sizes_either_side_of_the_delimiter();
    eprintln!(
        "RECLAIM FRAME fixture: {subject_items} items per batch encodes to a payload ending in \
         0x0A, {control_items} items does not"
    );
    let mut swept = Vec::new();
    for (label, items) in [("SUBJECT", subject_items), ("CONTROL", control_items)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalWriteAheadLogStore::new(dir.path());
        seed_batches(&store, 6, items);

        let frames = frames_of(&active_log(dir.path()));
        let ending_in_delimiter = frames
            .iter()
            .filter(|frame| frame.last_payload_byte == b'\n')
            .count();

        eprintln!(
            "RECLAIM FRAME {label}: {} frames, {} of them with a payload ending in 0x0A, \
             last bytes {:02X?}",
            frames.len(),
            ending_in_delimiter,
            frames
                .iter()
                .map(|frame| frame.last_payload_byte)
                .collect::<Vec<_>>(),
        );

        // FIXTURE PROOF, both directions. A stage with nothing to do reports zero and that reads
        // exactly like a stage that refused, so each arm must be shown to be the arm it claims.
        assert!(
            frames.len() >= 6,
            "APPARATUS {label}: the log holds {} frames, so there is nothing for a sweep to \
             decide about",
            frames.len()
        );
        if label == "SUBJECT" {
            assert_eq!(
                ending_in_delimiter,
                frames.len(),
                "APPARATUS SUBJECT: {ending_in_delimiter} of {} frames end in 0x0A. This arm \
                 exists to hold frames on that side of the byte; if the encoding moved, find a \
                 batch shape that puts them there again rather than dropping the arm",
                frames.len()
            );
        } else {
            assert_eq!(
                ending_in_delimiter, 0,
                "APPARATUS CONTROL: {ending_in_delimiter} of {} frames end in 0x0A, so this arm \
                 is not the other side of the byte and the comparison says nothing",
                frames.len()
            );
        }

        let before = records_in_order(&store);
        let report = store
            .gc_before_sequence_unchecked(SHARD, 4)
            .unwrap_or_else(|err| {
                panic!(
                    "{label}: the reclaim refused a log of {} intact frames: {err}. This is the \
                     defect -- the walk stripped a delimiter off a frame that declares its own \
                     length, and the frame then read one byte short of what it says it is",
                    frames.len()
                )
            });

        eprintln!(
            "RECLAIM FRAME {label}: swept before={} after={} removed={} bytes {} -> {} \
             (skipped_not_worth_rewrite={} clamped_by_block_retention={})",
            report.records_before,
            report.records_after,
            report.records_removed,
            report.bytes_before,
            report.bytes_after,
            report.skipped_not_worth_rewrite,
            report.clamped_by_block_retention,
        );

        assert!(
            report.records_removed > 0,
            "{label}: the sweep removed no records from a log of {} frames with a retain floor \
             of 4",
            frames.len()
        );

        // STRONG FORM. Freeing too much is silent, so compare the survivors element by element
        // against what the log held before, not their count.
        let after = records_in_order(&store);
        let expected: Vec<(u64, Vec<u8>)> = before
            .iter()
            .filter(|(sequence, _)| *sequence >= 4)
            .cloned()
            .collect();
        assert_eq!(
            after.len(),
            expected.len(),
            "{label}: the log serves {} records after the sweep where {} sit at or above the \
             retain floor",
            after.len(),
            expected.len()
        );
        for (index, (survivor, wanted)) in after.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                survivor.0, wanted.0,
                "{label}: survivor {index} is sequence {} where {} was retained",
                survivor.0, wanted.0
            );
            assert_eq!(
                survivor.1, wanted.1,
                "{label}: survivor {index} (sequence {}) does not match the bytes it was written \
                 with",
                survivor.0
            );
        }
        swept.push((label, report.records_removed, report.bytes_before - report.bytes_after));
    }

    // The two arms agree, which is the claim: the last byte of a payload must not change what a
    // reclaim does.
    assert_eq!(
        swept[0].1, swept[1].1,
        "the two arms removed {} and {} records from identically-shaped logs; the payload's last \
         byte is still deciding the sweep",
        swept[0].1, swept[1].1
    );
    eprintln!(
        "RECLAIM FRAME both arms removed {} records ({} B and {} B freed)",
        swept[0].1, swept[0].2, swept[1].2
    );
}

/// The rule itself, at the level it lives at, in both directions.
///
/// A text record's delimiter comes off; a frame's last byte -- `0x0A` or not -- does not.
/// rust-internal: reads the log framing layer directly, no product behaviour
#[test]
fn only_a_delimited_record_gives_up_its_trailing_byte() {
    let framed_ending_in_delimiter = crate::log_framing::encode_frame(b"payload ending in \n");
    assert_eq!(
        crate::log_framing::record_body(&framed_ending_in_delimiter),
        framed_ending_in_delimiter.as_slice(),
        "a binary frame declares its own length, so nothing may come off the end of it"
    );
    assert!(
        crate::log_framing::decode_line(crate::log_framing::record_body(
            &framed_ending_in_delimiter
        ))
        .is_ok(),
        "a frame whose payload ends in 0x0A must decode after passing through the walk's trimmer"
    );

    // The negative control: the same bytes with the delimiter actually taken off are exactly the
    // fragment the defect produced, and they must still be refused.
    let damaged = &framed_ending_in_delimiter[..framed_ending_in_delimiter.len() - 1];
    assert!(
        crate::log_framing::decode_line(damaged).is_err(),
        "a frame one byte short of its declared length is a torn frame and must be refused; if \
         this passes, the decode stopped checking the length and the guard above means nothing"
    );

    let delimited = b"#tsf2 2 00000000 {}\n";
    assert_eq!(
        crate::log_framing::record_body(delimited),
        &delimited[..delimited.len() - 1],
        "a text record's trailing delimiter still comes off"
    );
}

/// The refusal term is now sayable, which is the part that does not depend on the trigger.
///
/// An errored sweep and a sweep with nothing to do both report `(0, 0, 0)` for records, bytes
/// before and bytes after, because the `Result` is dropped with `.ok()` and every field read
/// through `unwrap_or_default()`. `wal_sweep_failed` is what tells them apart. Eleven refusal
/// terms behind one number is a report that cannot be tested; this one names which fired.
/// rust-internal: reads the engine's own maintenance entry point, no product behaviour
#[test]
fn a_failed_sweep_and_an_empty_one_no_longer_report_the_same_thing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    let report = engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the dump did not complete, so there is no report to read");

    eprintln!(
        "RECLAIM TERM empty shard: wal_sweep_failed={} index_log_sweep_failed={} \
         wal_records_removed={} wal_bytes_before={} wal_bytes_after={}",
        report.wal_sweep_failed,
        report.index_log_sweep_failed,
        report.wal_records_removed,
        report.wal_bytes_before,
        report.wal_bytes_after,
    );

    assert!(
