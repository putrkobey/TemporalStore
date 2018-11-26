// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Per-record integrity framing for the append-only JSON logs (WAL + served-index log).
//!
//! Historically each log record was a bare single-line JSON document terminated by `\n`.
//! A value-preserving bit-flip -- a flipped digit in a sequence/ttl, a flipped byte inside
//! a base64 value -- parses cleanly and would replay as truth on the default recovery path.
//! Framing wraps each line with a magic prefix, the payload length, and a truncated SHA-256
//! digest of the payload, so a silent corruption of an otherwise-complete committed record
//! is detected on read and surfaced as data loss rather than applied.
//!
//! Backward compatibility: a legacy unframed record is a serialized JSON object, so it
//! always begins with `{`. A framed record begins with [`FRAME_MAGIC`]. [`decode_line`]
//! detects the framing by that prefix and, for a legacy record, treats the whole line as the
//! payload -- so a WAL / index-log written before this change still loads unchanged. New
//! writes are framed; the two coexist in one file across an upgrade (and across a GC rewrite,
//! which re-emits every retained record framed).
//!
//! Two framed formats exist. New writes use `#tsf2`, which carries a CRC32C:
//! `#tsf2 <payload_len> <crc32c_hex> <payload_json>\n`
//! `#tsf1` records carry a truncated SHA-256 instead and are still read:
//! `#tsf1 <payload_len> <sha256_prefix_hex> <payload_json>\n`
//!
//! The switch to CRC32C is a write-path cost change, not a weakening. This checksum defends
//! against accidental corruption of a committed record -- a flipped bit that still parses --
//! and never against a forged one; nothing in the recovery path treats it as a signature. A
//! cryptographic digest was simply the wrong tool here: it is computed per record,
//! synchronously, before the durability barrier. CRC32C is what this design
//! uses for the same job (a per-record CRC in the record header plus a running per-block CRC
//! in the block footer).
//! The payload JSON is compact serde output, so it contains neither a literal `\n` (line
//! boundary) nor is its boundary ambiguous: the first two space-delimited fields after the
//! magic are the length and digest, and everything after the third space is the payload
//! (which may itself contain spaces inside JSON string values).

use sha2::{Digest, Sha256};

/// ASCII framing prefix for the current (CRC32C) format, including the trailing space that
/// delimits the fields after it. Chosen so it can never collide with a legacy record: a
/// serialized JSON object always starts with `{`, never `#`.
pub(crate) const FRAME_MAGIC_V2: &[u8] = b"#tsf2 ";

/// Framing prefix for the previous (truncated SHA-256) format. Still decoded so logs written
/// before the switch load unchanged; never written.
pub(crate) const FRAME_MAGIC_V1: &[u8] = b"#tsf1 ";

/// The prefix new writes use.
pub(crate) const FRAME_MAGIC: &[u8] = FRAME_MAGIC_V2;

/// Marker for the binary frame: one byte, then a varint length, then the CRC32C as four raw
/// bytes, then the payload. No delimiter and no trailing newline.
///
/// The text frames spend about twenty bytes to say what this says in seven: a six-byte magic
/// repeated on every record, a decimal length, a checksum written as sixteen hex characters
/// where four bytes carry the same number, and a newline. The newline is the expensive one.
/// It is not the byte -- it is that a reader scanning for `\n` cannot be handed a payload
/// containing one, so binary records have to be byte-stuffed on the way in and unstuffed on
/// the way out: two full copies of every record, plus a byte for every 0x0A and 0x1B in it.
/// A length-framed reader needs none of that, because the record already says how long it is.
///
/// 0xB3 is not a byte any earlier frame can start with: the text frames start with `#` and a
/// legacy unframed record is a JSON document starting with `{`.
pub(crate) const FRAME_MAGIC_V3: u8 = 0xB3;

/// The widest a binary frame's header can be: the marker, a ten-byte varint length, and the
/// four checksum bytes.
///
/// Ten is what a `u64` varint costs at its widest, and `read_raw_record` refuses a longer one as
/// corruption. Held here so the array the header is parsed into cannot drift from the parse.
pub(crate) const MAX_BINARY_HEADER_BYTES: usize = 1 + 10 + 4;

/// The largest a record's DECLARED length may be allowed to size an allocation.
///
/// A declared length is a number read off a file that a corrupt varint can make arbitrarily
/// large, and the checksum that would reject the record cannot be computed until after the bytes
/// are in hand. So the reader reserves from it only up to here and then grows as bytes actually
/// arrive, which is what it did for every record before it reserved at all. One mebibyte is the
/// same ceiling the write-ahead encoder keeps its own scratch buffer under.
pub(crate) const MAX_RESERVED_RECORD_BYTES: usize = 1024 * 1024;

/// Whether new records are written with the binary frame. DEFAULT ON.
///
/// The cost this removes is not the newline byte, it is what a delimiter forces on everything
/// that reads: a reader scanning for `\n` cannot be handed a payload containing one, so every
/// binary record had to be stuffed on the way in and unstuffed on the way out -- two full copies
/// of each record, plus a byte for every 0x0A and 0x1B inside it. Measured over 200 records:
/// 102.3 -> 84.8 bytes each, and `what_each_frame_costs_on_disk` keeps that honest.
///
/// The one thing length framing takes away is the ability to find a log's tail by scanning
/// BACKWARD: a length prefix is only readable from in front of the record it describes, so the
/// tail has to be walked to. That walk reads the file, which would be ruinous if it ran per
/// append -- and it does not: flat append resolves the sequence from the warm cache plus a
/// length stat, leaving the walk for a cold open. (A log format that frames by length usually
/// records where its last record is as it writes, which bounds even that; the descriptors for
/// it exist in `storage_descriptor` and are not yet wired.)
///
/// Off (`TS_WAL_BINARY_FRAME=0`) writes the delimited frame again.
///
/// DECODING a record never depends on this flag: which frame a record uses is a property of the
/// record, and `next_frame` dispatches on its first bytes across all four shapes, so a log holding
/// both -- which is what an upgrade leaves -- reads end to end either way.
///
/// Finding the TAIL does depend on it, and the doc here used to say otherwise.
/// `last_wal_sequence_in` chooses its strategy from this flag rather than from the file: on, it
/// takes the footer hint and walks forward; off, it searches BACKWARDS for the last newline, which
/// is an assumption only a delimited log satisfies. A binary frame carries newlines freely inside
/// its payload, so that search is not looking for a record boundary in a log written binary.
///
/// So the off position belongs to a log whose records are delimited. It is not a switch that can
/// be thrown over an existing binary-framed log and it never was; what was wrong is the sentence
/// that said it could be.
pub(crate) fn binary_frame_enabled() -> bool {
    !matches!(
        std::env::var("TS_WAL_BINARY_FRAME")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn write_varint(value: u64, out: &mut Vec<u8>) {
    let mut value = value;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

/// Read a varint, returning the value and how many bytes it used. `None` when the bytes run out
/// mid-varint, which is a torn tail rather than damage.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (index, byte) in bytes.iter().enumerate() {
        if shift > 63 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
        shift += 7;
    }
    None
}

/// Encode `payload` as a binary frame. The payload travels exactly as given -- no escaping,
/// because nothing downstream scans it for a delimiter.
pub(crate) fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    out.push(FRAME_MAGIC_V3);
    write_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(&crate::checksum::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Encode with whichever frame new writes are configured to use.
pub(crate) fn encode_record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_record_into(payload, &mut out);
    out
}

/// Encode into a caller-owned buffer, so an append can reuse one instead of allocating a frame
/// per record.
///
/// The buffer is CLEARED, not appended to: a caller reusing a scratch buffer wants this record,
/// not this record after the last one. Its capacity survives, which is the point -- a steady
/// workload stops allocating here after the first few writes.
pub(crate) fn encode_record_into(payload: &[u8], out: &mut Vec<u8>) {
    out.clear();
    if binary_frame_enabled() {
        out.reserve(payload.len() + 10);
        out.push(FRAME_MAGIC_V3);
        write_varint(payload.len() as u64, out);
        out.extend_from_slice(&crate::checksum::crc32c(payload).to_le_bytes());
        out.extend_from_slice(payload);
    } else {
        out.extend_from_slice(&encode_line(payload));
    }
}

/// Frame a record whose length is known before it is written, with no payload buffer in between.
///
/// `encode_record_into` takes a payload that already exists, which means the payload was built in
/// one buffer and copied into another. Both buffers are the size of the record. This writes the
/// header, hands `write_payload` the same buffer to append into, and patches the checksum in
/// afterwards -- so a write allocates nothing here and copies the payload once, into the bytes
/// that go to the file.
///
/// The declared length is written BEFORE the payload, because the varint that carries it sits in
/// front of it. That is the whole risk of this function: if `payload_len` disagrees with what
/// `write_payload` appends, the frame declares a length the payload does not have, and every
/// record after it in the file reads as garbage. So the disagreement is checked and returned as an
/// error, with the buffer left cleared -- a caller cannot write the bad frame by ignoring it.
pub(crate) fn encode_framed_into<E>(
    payload_len: usize,
    write_payload: impl FnOnce(&mut Vec<u8>) -> Result<(), E>,
    out: &mut Vec<u8>,
) -> Result<Result<(), E>, FrameLengthMismatch> {
    out.clear();
    if !binary_frame_enabled() {
        // The line framing escapes the payload, so it has to see it whole first. Nothing to fuse.
        let mut payload = Vec::with_capacity(payload_len);
        if let Err(err) = write_payload(&mut payload) {
            return Ok(Err(err));
        }
        out.extend_from_slice(&encode_line(&payload));
        return Ok(Ok(()));
    }
    out.reserve(payload_len + 10);
    out.push(FRAME_MAGIC_V3);
    write_varint(payload_len as u64, out);
    let checksum_at = out.len();
    out.extend_from_slice(&[0u8; 4]);
    let payload_at = out.len();
    if let Err(err) = write_payload(out) {
        out.clear();
        return Ok(Err(err));
    }
    let written = out.len() - payload_at;
    if written != payload_len {
        out.clear();
        return Err(FrameLengthMismatch {
            declared: payload_len,
            written,
