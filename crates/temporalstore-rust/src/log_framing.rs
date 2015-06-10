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
        });
    }
    let checksum = crate::checksum::crc32c(&out[payload_at..]);
    out[checksum_at..checksum_at + 4].copy_from_slice(&checksum.to_le_bytes());
    Ok(Ok(()))
}

/// A record that did not write the number of bytes it said it would.
///
/// Never expected to happen: it means an encoder's own length arithmetic disagrees with its
/// writing. It is returned rather than asserted because the alternative to failing this write is
/// writing a frame that makes the rest of the log unreadable.
#[derive(Debug)]
pub(crate) struct FrameLengthMismatch {
    pub(crate) declared: usize,
    pub(crate) written: usize,
}

impl std::fmt::Display for FrameLengthMismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "record declared {} bytes and wrote {}",
            self.declared, self.written
        )
    }
}

/// Prefix of the reclaim-base header, the optional first line of a log file.
///
/// A reclaim drops a prefix of the file, shifting every surviving record down by the number of
/// bytes removed. This header records the running total of bytes reclaimed so far, which is
/// what lets a stored byte offset keep meaning the same record afterwards. Distinct from the
/// record magics so a reader can tell a header from a record, and absent from files that have
/// never been reclaimed -- which read as a base of zero.
pub(crate) const BASE_HEADER_MAGIC: &[u8] = b"#tsb1 ";

/// Encode the reclaim-base header line, newline-terminated.
///
/// Checksummed like a record: the base is load-bearing for address resolution, so a corrupted
/// one would silently resolve every offset to the wrong record.
pub(crate) fn encode_base_header(base_offset: u64) -> Vec<u8> {
    let value = base_offset.to_string();
    let mut line = Vec::with_capacity(BASE_HEADER_MAGIC.len() + 32);
    line.extend_from_slice(BASE_HEADER_MAGIC);
    line.extend_from_slice(crc_digest_hex(value.as_bytes()).as_bytes());
    line.push(b' ');
    line.extend_from_slice(value.as_bytes());
    line.push(b'\n');
    line
}

/// Decode a reclaim-base header line, verifying its checksum.
///
/// Returns `Ok(None)` when the line is not a header at all, which is how a file that predates
/// reclaim -- or has simply never been reclaimed -- reads as a base of zero.
pub(crate) fn decode_base_header(line: &[u8]) -> Result<Option<u64>, FramingError> {
    if !line.starts_with(BASE_HEADER_MAGIC) {
        return Ok(None);
    }
    let rest = &line[BASE_HEADER_MAGIC.len()..];
    let split = rest
        .iter()
        .position(|byte| *byte == b' ')
        .ok_or_else(|| FramingError("reclaim-base header has no checksum separator".to_string()))?;
    let (digest, value) = rest.split_at(split);
    let value = &value[1..];
    let value_text = std::str::from_utf8(value)
        .map_err(|_| FramingError("reclaim-base header is not utf-8".to_string()))?
        .trim_end();
    let expected = crc_digest_hex(value_text.as_bytes());
    if expected.as_bytes() != digest {
        return Err(FramingError(format!(
            "reclaim-base header checksum mismatch: expected {expected}, found {}",
            String::from_utf8_lossy(digest)
        )));
    }
    value_text
        .parse::<u64>()
        .map(Some)
        .map_err(|_| FramingError(format!("reclaim-base header is not a number: {value_text}")))
}

/// Number of leading SHA-256 bytes retained in a v1 framed digest.
const DIGEST_BYTES: usize = 8;

/// Integrity failure discovered while decoding a framed log line. Carried into the WAL /
/// index-log error types (as their `Corruption` variant) so the recovery path surfaces it as
/// data loss and refuses the load rather than silently applying or skipping the record.
#[derive(Debug, Clone)]
pub(crate) struct FramingError(pub String);

impl std::fmt::Display for FramingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "log record integrity error: {}", self.0)
    }
}

impl std::error::Error for FramingError {}

/// v1 checksum: the leading bytes of a SHA-256, hex encoded. Read-only.
fn sha256_digest_hex(payload: &[u8]) -> String {
    let full = Sha256::digest(payload);
    hex::encode(&full[..DIGEST_BYTES])
}

/// v2 checksum: CRC32C, hex encoded, fixed width.
fn crc_digest_hex(payload: &[u8]) -> String {
    crate::checksum::crc32c_hex(payload)
}

/// Encode `payload` (a single-line JSON document with NO trailing newline) as a framed,
/// newline-terminated log line.
pub(crate) fn encode_line(payload: &[u8]) -> Vec<u8> {
    let digest = crc_digest_hex(payload);
    let header = format!("#tsf2 {} {} ", payload.len(), digest);
    let mut out = Vec::with_capacity(header.len() + payload.len() + 1);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(payload);
    out.push(b'\n');
    out
}

/// Decode one raw log line (as read by a `\n`-delimited reader; a trailing newline is
/// optional and stripped) into its JSON payload bytes, verifying the integrity envelope when
/// present. A legacy unframed record is returned as-is (minus any trailing newline). A framed
/// record whose declared length or digest does not match its payload is COMMITTED corruption
/// and returns `Err`.
pub(crate) fn decode_line(line: &[u8]) -> Result<&[u8], FramingError> {
    // decode_line binary: a binary frame declares its own length, so nothing may be stripped
    // from the end of it. The newline strip below is right for a text record and would silently
    // eat a byte from any binary payload that happens to end in 0x0A.
    if line.first() == Some(&FRAME_MAGIC_V3) {
        return match next_frame(line)? {
            Some((_, payload)) => Ok(payload),
            None => Err(FramingError("binary record is incomplete".to_string())),
        };
    }
    let line = strip_trailing_newline(line);
    // Pick the checksum by prefix so both framed formats round-trip out of one file: an
    // upgrade, or a GC rewrite spanning one, leaves v1 and v2 records interleaved.
    let (checksum_of, rest): (fn(&[u8]) -> String, &[u8]) = if line.starts_with(FRAME_MAGIC_V2) {
        (crc_digest_hex, &line[FRAME_MAGIC_V2.len()..])
    } else if line.starts_with(FRAME_MAGIC_V1) {
        (sha256_digest_hex, &line[FRAME_MAGIC_V1.len()..])
    } else {
        // Legacy unframed record (or a blank line): the whole line is the payload.
        return Ok(line);
    };
    let (len_field, rest) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, payload) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    if declared_len != payload.len() {
        return Err(FramingError(format!(
            "framed record length mismatch: declared {declared_len}, actual {}",
            payload.len()
        )));
    }
    let actual = checksum_of(payload);
    if actual.as_bytes() != digest_field {
        return Err(FramingError(format!(
            "framed record checksum mismatch: declared {}, actual {actual}",
            String::from_utf8_lossy(digest_field)
        )));
    }
    Ok(payload)
}

/// Introduces a record whose payload is binary rather than text.
///
/// A binary payload may contain a newline, so a record carrying one cannot be found by splitting
/// on newlines. Readers use [`next_frame`] or [`read_frame`] instead, which take the length the
/// frame already declares rather than looking for a delimiter.
pub(crate) const BINARY_PAYLOAD_MAGIC: u8 = 0xA7;

/// Read the next record from `bytes`, returning how many bytes it occupied and its payload.
///
/// `Ok(None)` means there is nothing further to read, or that what remains is shorter than the
/// record it declares -- a torn tail from a crash mid-append, which the caller truncates. A
/// complete record whose digest does not match is committed corruption and returns `Err`. The
/// checksum is picked by prefix, exactly as [`decode_line`] picks it.
pub(crate) fn next_frame(bytes: &[u8]) -> Result<Option<(usize, &[u8])>, FramingError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes[0] == FRAME_MAGIC_V3 {
        let Some((declared_len, varint_len)) = read_varint(&bytes[1..]) else {
            return Ok(None); // torn mid-varint
        };
        let header_len = 1 + varint_len + 4;
        if bytes.len() < header_len {
            return Ok(None); // torn before the checksum is complete
        }
        let declared_len = declared_len as usize;
        let mut digest = [0u8; 4];
        digest.copy_from_slice(&bytes[1 + varint_len..header_len]);
        let expected = u32::from_le_bytes(digest);
        if bytes.len() < header_len + declared_len {
            return Ok(None); // fewer bytes than declared: a torn tail
        }
        let payload = &bytes[header_len..header_len + declared_len];
        if crate::checksum::crc32c(payload) != expected {
            return Err(FramingError(
                "framed record digest does not match its payload".to_string(),
            ));
        }
        return Ok(Some((header_len + declared_len, payload)));
    }
    let (checksum_of, header_prefix_len): (fn(&[u8]) -> String, usize) =
        if bytes.starts_with(FRAME_MAGIC_V2) {
            (crc_digest_hex, FRAME_MAGIC_V2.len())
        } else if bytes.starts_with(FRAME_MAGIC_V1) {
            (sha256_digest_hex, FRAME_MAGIC_V1.len())
        } else {
            // Unframed: the record ends at the newline, and the whole line is the payload.
            // Without a newline the record was never finished being written -- a torn tail.
            let Some(position) = bytes.iter().position(|byte| *byte == b'\n') else {
                return Ok(None);
            };
            let line_len = position + 1;
            return Ok(Some((line_len, strip_trailing_newline(&bytes[..line_len]))));
        };
    let rest = &bytes[header_prefix_len..];
    let (len_field, after_len) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, after_digest) = split_once_space(after_len)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    if after_digest.len() < declared_len {
        // Fewer bytes present than the record declares: the append was interrupted. That is a
        // torn tail to be truncated, not damage to a record that was completely written.
        return Ok(None);
    }
    let payload = &after_digest[..declared_len];
    let actual = checksum_of(payload);
    if actual.as_bytes() != digest_field {
        return Err(FramingError(
            "framed record digest does not match its payload".to_string(),
        ));
    }
    let header_len = bytes.len() - after_digest.len();
    let mut consumed = header_len + declared_len;
    if bytes.get(consumed) == Some(&b'\n') {
        consumed += 1; // the newline the encoder appends
    }
    Ok(Some((consumed, payload)))
}

/// Read one record from a stream, returning how many bytes it occupied and its payload.
///
/// The streaming twin of [`next_frame`], for the paths that face the largest logs there are:
/// memory stays bounded by the biggest single record rather than by the file.
pub(crate) fn read_frame<R: std::io::BufRead>(
    reader: &mut R,
) -> Result<Option<(usize, Vec<u8>)>, FramingError> {
    // A binary frame is decided by its first byte, before the text-magic scan below.
    {
        let first = {
            let buffered = reader.fill_buf().map_err(|err| FramingError(err.to_string()))?;
            buffered.first().copied()
        };
        match first {
            None => return Ok(None),
            Some(byte) if byte == FRAME_MAGIC_V3 => {
                let mut marker = [0u8; 1];
                if reader.read_exact(&mut marker).is_err() {
                    return Ok(None);
                }
                let mut length_bytes = Vec::with_capacity(5);
                let declared_len = loop {
                    let mut byte = [0u8; 1];
                    if reader.read_exact(&mut byte).is_err() {
                        return Ok(None); // torn mid-varint
                    }
                    length_bytes.push(byte[0]);
                    if let Some((value, _)) = read_varint(&length_bytes) {
                        break value as usize;
                    }
                    if length_bytes.len() > 10 {
                        return Err(FramingError("record length is not a varint".to_string()));
                    }
                };
                let mut digest = [0u8; 4];
                if reader.read_exact(&mut digest).is_err() {
                    return Ok(None);
                }
                // Allocate what is READ, not what is DECLARED.
                //
                // `declared_len` is a varint out of the file, so a torn or corrupted tail can
                // name any number at all. Sizing the buffer from it asked the allocator for the
                // number the CORRUPTION chose -- and a failed allocation aborts the process,
                // taking down everything else running in it. In the suite that meant a whole
                // 1,700-test run died, which is a louder failure than the corruption being
                // checked. Measured at about one run in twenty of the random torn-tail test.
                //
                // `take` + `read_to_end` grows the buffer as bytes actually arrive, so the cost
                // is bounded by what the file really holds rather than by what it claims. The
                // check below is unchanged and still decides: fewer bytes than declared is a
                // torn tail. No cap is invented -- a cap would have to guess a largest
                // legitimate record, and guessing that wrong rejects good data.
                let mut payload = Vec::new();
                let read = {
                    use std::io::Read as _;
                    match reader.by_ref().take(declared_len as u64).read_to_end(&mut payload) {
                        Ok(read) => read,
                        Err(_) => return Ok(None),
                    }
                };
                if read != declared_len {
                    return Ok(None); // fewer bytes than declared: a torn tail
                }
                if crate::checksum::crc32c(&payload) != u32::from_le_bytes(digest) {
                    return Err(FramingError(
                        "framed record digest does not match its payload".to_string(),
                    ));
                }
                let consumed = 1 + length_bytes.len() + 4 + declared_len;
                return Ok(Some((consumed, payload)));
            }
            Some(_) => {}
        }
    }
    let longest_magic = FRAME_MAGIC_V1.len().max(FRAME_MAGIC_V2.len());
    let mut header = Vec::with_capacity(longest_magic + 32);
    let mut spaces = 0usize;
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Nothing at all is a clean end; a partial header is a torn tail.
                return Ok(None);
            }
            Err(err) => return Err(FramingError(err.to_string())),
        }
        header.push(byte[0]);
        if header.len() <= longest_magic {
            let still_v1 = FRAME_MAGIC_V1.starts_with(&header[..header.len().min(FRAME_MAGIC_V1.len())])
                && header.len() <= FRAME_MAGIC_V1.len();
            let still_v2 = FRAME_MAGIC_V2.starts_with(&header[..header.len().min(FRAME_MAGIC_V2.len())])
                && header.len() <= FRAME_MAGIC_V2.len();
            if header.as_slice() == FRAME_MAGIC_V1 || header.as_slice() == FRAME_MAGIC_V2 {
                spaces = 1; // the magic ends with the space that follows it
                continue;
            }
            if still_v1 || still_v2 {
                continue;
            }
            // Not a framed record: an unframed one runs to the next newline.
            return read_unframed(reader, header);
        }
        if byte[0] == b' ' {
            spaces += 1;
            if spaces == 3 {
                break;
            }
        }
    }
    let checksum_of: fn(&[u8]) -> String = if header.starts_with(FRAME_MAGIC_V2) {
        crc_digest_hex
    } else {
        sha256_digest_hex
    };
    let magic_len = if header.starts_with(FRAME_MAGIC_V2) {
        FRAME_MAGIC_V2.len()
    } else {
        FRAME_MAGIC_V1.len()
    };
    let rest = &header[magic_len..];
    let (len_field, after_len) = split_once_space(rest)
        .ok_or_else(|| FramingError("framed record missing length field".to_string()))?;
    let (digest_field, _) = split_once_space(after_len)
        .ok_or_else(|| FramingError("framed record missing digest field".to_string()))?;
    let declared_len: usize = std::str::from_utf8(len_field)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| FramingError("framed record has an unparseable length field".to_string()))?;
    let digest_expected = digest_field.to_vec();
    // Read what is there, do not reserve what is claimed. See the binary reader above: a length
    // out of the file sizes an allocation, and a failed allocation aborts the process rather
    // than reporting the torn tail this already knows how to report.
    let mut payload = Vec::new();
    let read = {
        use std::io::Read as _;
        match reader.by_ref().take(declared_len as u64).read_to_end(&mut payload) {
            Ok(read) => read,
            Err(_) => return Ok(None),
        }
    };
    if read != declared_len {
        // Fewer bytes than the record declares: the append was interrupted.
        return Ok(None);
    }
    let actual = checksum_of(&payload);
    if actual.as_bytes() != digest_expected.as_slice() {
        return Err(FramingError(
            "framed record digest does not match its payload".to_string(),
        ));
    }
    // PEEK, do not read: this byte belongs to the next record whenever it is not the newline
    // the encoder appends, and read_exact would swallow it. Nothing exercised this before --
    // both readers here had no callers at all -- so the stream corruption it causes had never
    // had the chance to happen.
    let consumed_newline = {
        let buffered = reader.fill_buf().map_err(|err| FramingError(err.to_string()))?;
        if buffered.first() == Some(&b'\n') {
            reader.consume(1);
            true
        } else {
            false
        }
    };
    let consumed = header.len() + declared_len + usize::from(consumed_newline);
    Ok(Some((consumed, payload)))
}

/// Finish reading a record that is not framed: it runs to the next newline, and without one it
/// was never finished being written.
fn read_unframed<R: std::io::BufRead>(
    reader: &mut R,
    mut started: Vec<u8>,
) -> Result<Option<(usize, Vec<u8>)>, FramingError> {
    if started.last() == Some(&b'\n') {
        let consumed = started.len();
        let payload = strip_trailing_newline(&started).to_vec();
        return Ok(Some((consumed, payload)));
    }
    let mut rest = Vec::new();
    let read = reader
        .read_until(b'\n', &mut rest)
        .map_err(|err| FramingError(err.to_string()))?;
    if read == 0 || !rest.ends_with(b"\n") {
        return Ok(None);
    }
    let consumed = started.len() + rest.len();
    started.extend_from_slice(&rest);
    let payload = strip_trailing_newline(&started).to_vec();
    Ok(Some((consumed, payload)))
}

fn strip_trailing_newline(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn split_once_space(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    bytes
        .iter()
        .position(|&byte| byte == b' ')
        .map(|index| (&bytes[..index], &bytes[index + 1..]))
}

#[cfg(test)]
mod torn_length_tests {
    use super::*;

    /// A frame whose declared length is absurd reads as a torn tail instead of sizing a buffer.
    ///
    /// The length is a varint out of the file, so a torn or corrupted tail can name any number.
    /// Sizing the payload buffer from it asked the allocator for the number the CORRUPTION chose;
    /// a failed allocation aborts the process, taking down whatever else is running -- in the
    /// suite, every remaining test.
    ///
