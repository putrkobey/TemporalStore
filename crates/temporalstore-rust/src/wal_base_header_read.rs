// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What reading a log piece's base header COSTS, against what the header is.
//!
//! #1936 named this and did not fix it: `read_wal_base` fills 8 KiB to read a header line under
//! 100 bytes, once per piece per replay window, and priced it at 483,328 of the 34,779,419 bytes
//! a 200,000-record restore reads off its log -- 1.4%.
//!
//! A buffered reader filling its buffer is normal and often CORRECT. The question is whether
//! anything after the header read reuses the buffer. In `read_wal_base` nothing does: the
//! `BufReader` is a local, it is dropped at the end of the function, and what escapes is two
//! integers. The fill is bought and thrown away.
//!
//! DIRECTION. Reading too LITTLE on a recovery path is silent data loss -- a header that failed
//! to decode reads as a base of zero, and a base of zero resolves every log id in that piece to
//! the wrong record. Reading too much is merely slow. So the bound is not asserted by LENGTH: the
//! decoded answer is compared FIELD BY FIELD against a control that reads the way the old code
//! did, over every shape a piece can be in.

#![cfg(test)]

use crate::wal::{
    read_wal_base_bounded_for_test, read_wal_base_unbounded_for_test, wal_base_header_probe_bytes,
    wal_piece_read_counts_on_this_thread,
};

/// The longest base header this writer can ever produce, computed from the writer rather than
/// assumed: `#tsb1 ` + a checksum + a space + `u64::MAX` in decimal + a newline.
fn longest_possible_header() -> usize {
    crate::log_framing::encode_base_header(u64::MAX).len()
}

// -------------------------------------------------------------------------------------------
// THE SHAPES A PIECE CAN BE IN
//
// Every one of them is fed to BOTH readers and the two answers are compared field by field. A
// shape this list leaves out is a shape the bound was never checked on.
// -------------------------------------------------------------------------------------------

/// `(name, bytes on disk)` for every shape `read_wal_base` has to answer for.
fn piece_shapes() -> Vec<(&'static str, Vec<u8>)> {
    let header = crate::log_framing::encode_base_header(4_096);
    let big_header = crate::log_framing::encode_base_header(u64::MAX);
    let record = crate::log_framing::encode_line(br#"{"sequence":1,"payload":"x"}"#);

    let mut header_then_records = header.clone();
    for _ in 0..64 {
        header_then_records.extend_from_slice(&record);
    }

    let mut records_only = Vec::new();
    for _ in 0..64 {
        records_only.extend_from_slice(&record);
    }

    // A piece whose first line is longer than the probe window and is NOT a header. The old
    // reader reads to its newline; the bounded one stops. Both must still answer "no header".
    let mut long_first_line = vec![b'x'; wal_base_header_probe_bytes() * 4];
    long_first_line.push(b'\n');
    long_first_line.extend_from_slice(&record);

    // A piece with NO newline anywhere: the old reader reads the whole file to find out.
    let no_newline = vec![b'x'; wal_base_header_probe_bytes() * 4];

    // A first line that OPENS WITH THE HEADER MAGIC and runs past the probe window. This writer
    // cannot produce one, so it is corruption or another writer's file -- and it is the shape the
    // bounded read must NOT quietly call base zero. It is here because it is the only input that
    // reaches the fallback: without it, a fallback that never fired would pass every other shape.
    let mut long_header_like = crate::log_framing::BASE_HEADER_MAGIC.to_vec();
    long_header_like.extend(std::iter::repeat(b'z').take(wal_base_header_probe_bytes() * 3));
    long_header_like.push(b'\n');

    vec![
        ("empty", Vec::new()),
        ("header only", header.clone()),
        ("header with no trailing newline", header[..header.len() - 1].to_vec()),
        ("longest header this writer can make", big_header),
        ("header then 64 records", header_then_records),
        ("records only, no header", records_only),
        ("one record", record.clone()),
        ("a first line longer than the probe window", long_first_line),
        ("a header-like first line past the probe window", long_header_like),
        ("no newline anywhere", no_newline),
        ("a single newline", vec![b'\n']),
        ("the header magic and nothing else", b"#tsb1 ".to_vec()),
    ]
}

// -------------------------------------------------------------------------------------------

/// THE STRONG FORM: the bounded read and the read it replaces AGREE, on every shape, FIELD BY
/// FIELD.
///
/// Not by length and not by "both succeeded". `read_wal_base` answers `(base, header_len)` and
/// both fields are load-bearing -- `base` resolves log ids to records and `header_len` is where
/// a window seeks to. A bound that got `header_len` right and `base` wrong would pass every
/// assertion about cost in this file.
#[test]
fn a_bounded_base_header_read_answers_exactly_what_the_buffered_one_answered() {
    let dir = tempfile::tempdir().unwrap();
    let shapes = piece_shapes();
    assert!(
        shapes.len() >= 10,
        "APPARATUS: {} shapes is not an enumeration of what a piece can be",
        shapes.len()
    );

    let mut decoded_a_header = 0usize;
    let mut answered_no_header = 0usize;
    let mut refused = 0usize;

    for (name, bytes) in &shapes {
        let path = dir.path().join(format!("piece-{}.bin", name.replace(' ', "-")));
        std::fs::write(&path, bytes).unwrap();

        let control = read_wal_base_unbounded_for_test(&path);
        let bounded = read_wal_base_bounded_for_test(&path);
