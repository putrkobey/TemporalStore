// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Record framing shared by the write-ahead log and the served-index log.
//!
//! Both are append-only streams in this design, and both are framed by the
//! same stream layer:
//!
//! ```text
//! varint32(payload_len) | little_endian_u32(crc32c) | payload
//! ```
//!
//! This module is that layer. It is shared rather than duplicated because the two streams
//! differ in what they carry, not in how a record is delimited or verified, and a second
//! framing would be a second set of corruption and truncation semantics to keep correct.
//!
//! Length-prefixing is what makes a record addressable by its byte offset — the log id that a
//! block address carries — and it is what allows a payload to contain any byte, including the
//! newline that a line-oriented format cannot survive.

use prost::Message;

use crate::checksum::crc32c;

/// A framing or integrity failure.
///
/// A record that fails to decode is corruption of committed data, not an absent record. In the
/// WAL a block-carrying record IS the durable copy of that block; in the index log a record is
/// the durable statement of where blocks live. Callers must surface this as data loss rather
/// than skipping past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFramingError(pub String);

impl std::fmt::Display for RecordFramingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "record framing error: {}", self.0)
    }
}

impl std::error::Error for RecordFramingError {}

/// Frame one record: `varint32(payload_len) | little_endian_u32(crc32c) | payload`.
pub fn encode_framed<M: Message>(record: &M) -> Vec<u8> {
    let payload = record.encode_to_vec();
    let checksum = crc32c(&payload);
    let mut out = Vec::with_capacity(payload.len() + 9);
    encode_varint32(payload.len() as u32, &mut out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Decode the record starting at `offset`, returning it together with the **absolute offset of
/// the next record**.
///
/// `offset` is the log id — the value a block address carries.
///
/// The second element is deliberately an absolute offset rather than a relative length. Every
/// caller either scans forward (`offset = next_offset`) or stops; returning a length invites
/// `offset = consumed`, which silently walks backwards after the first record and loops
/// forever, and the growing results then take the process out through the OOM killer rather
/// than failing an assertion. That is not hypothetical — it is what the first consumer of this
/// function did — so the signature is shaped to make it unrepresentable.
pub fn decode_framed_at<M: Message + Default>(
    bytes: &[u8],
    offset: usize,
) -> Result<(M, usize), RecordFramingError> {
    let rest = bytes.get(offset..).ok_or_else(|| {
        RecordFramingError(format!("offset {offset} is past the end of the stream"))
    })?;
    let (payload_len, varint_len) = decode_varint32(rest)?;
    let header_len = varint_len + 4;
    let payload_len = payload_len as usize;
    let end = header_len
        .checked_add(payload_len)
        .ok_or_else(|| RecordFramingError("record length overflows".to_string()))?;
    if rest.len() < end {
        return Err(RecordFramingError(format!(
            "record at {offset} claims {payload_len} payload bytes but only {} remain",
            rest.len().saturating_sub(header_len)
        )));
    }
    let declared = u32::from_le_bytes(
        rest[varint_len..header_len]
            .try_into()
            .expect("checksum slice is 4 bytes"),
    );
    let payload = &rest[header_len..end];
    let actual = crc32c(payload);
    if declared != actual {
        return Err(RecordFramingError(format!(
            "record at {offset} checksum mismatch: declared {declared:08x}, actual {actual:08x}"
        )));
    }
    let record = M::decode(payload).map_err(|error| {
        RecordFramingError(format!("record at {offset} failed to decode: {error}"))
    })?;
    Ok((record, offset + end))
}

/// Iterate every record in a framed stream, owning the cursor.
///
/// Callers get records rather than offsets, so they cannot do the offset arithmetic that the
/// signature above exists to prevent. This mirrors this design, whose log iterators advance
/// internally for the same reason. Stops at the first malformed record, yielding the error.
pub struct FramedRecords<'a, M> {
    bytes: &'a [u8],
    offset: usize,
    finished: bool,
    _record: std::marker::PhantomData<M>,
}

impl<'a, M> FramedRecords<'a, M> {
    /// Iterate from `start_offset`, which is a log id.
    pub fn new(bytes: &'a [u8], start_offset: usize) -> Self {
        Self {
            bytes,
            offset: start_offset,
            finished: false,
            _record: std::marker::PhantomData,
        }
    }

    /// The log id of the record the next call will decode.
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl<M: Message + Default> Iterator for FramedRecords<'_, M> {
    /// `(log_id, framed_size, record)` — the log id and size are what a block address is built
    /// from, so a replay can address a block it just read without recomputing anything.
    type Item = Result<(u64, u64, M), RecordFramingError>;
