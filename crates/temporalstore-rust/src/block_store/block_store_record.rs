// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};

use sha2::{Digest, Sha256};

use super::{
    ElementEntry, BlockStoreBlockIndexReport, BlockStoreError, BlockStoreOptions,
    BlockStoreSlabReport,
};

/// A block record carries only what the index cannot tell a reader.
///
/// The index entry already holds the object id, the block id, the routing bucket and the slab,
/// so a copy in every record said nothing new -- and the grouping id was derivable twice over,
/// being the id of the slab the record already sits in. Those are gone.
///
/// What is left is the checksum and the length, at constant offsets, so any field is one slice
/// rather than a walk. The length stays in the record even though the address carries it too:
/// the slab walks step record to record without an index, and the length is what lets them.
///
/// The checksum stays for a reason worth stating, because the comparison design does not keep
/// one: there, the layer beneath the block store checksums. Our slabs are plain files with no
/// such layer, so this CRC is the only thing standing between a corrupted block and a reader
/// that believes it.
/// Two bytes that say a slab holds records.
///
/// The comparison design has no marker, because a zone file there only ever holds blocks. We
/// have `install_slab`, which puts arbitrary bytes in a slab -- a restore path, and thirty test
/// sites -- so "is this a record" is a real question here and something has to answer it. Two
/// bytes answer it; the checksum catches anything that gets past them.
pub(super) const BLOCK_RECORD_MAGIC: &[u8; 2] = b"TB";

pub(super) const BLOCK_RECORD_CHECKSUM_OFFSET: usize = BLOCK_RECORD_MAGIC.len();

/// Length and codec share a word, the way the comparison design packs its status bits rather
/// than spending a byte on each. Thirty bits of length is a gigabyte per block.
pub(super) const BLOCK_RECORD_LENGTH_OFFSET: usize = BLOCK_RECORD_CHECKSUM_OFFSET + 4;

/// Which block of its object this is.
///
/// Two bytes, because a block id is an index INSIDE an object rather than a number handed out
/// across the whole store. An object with more than 65,535 blocks is not a case this design
/// serves; one with a handful is every case it does.
pub(super) const BLOCK_RECORD_BLOCK_ID_OFFSET: usize = BLOCK_RECORD_LENGTH_OFFSET + 4;
pub(crate) const BLOCK_RECORD_LENGTH_MASK: u32 = 0x3FFF_FFFF;
pub(super) const BLOCK_RECORD_CODEC_SHIFT: u32 = 30;

/// The header is one size, always.
pub(crate) const BLOCK_RECORD_HEADER_LEN: usize = BLOCK_RECORD_BLOCK_ID_OFFSET + 2;

/// The checksum is a CRC32C and nothing else, so the field is exactly its width.
///
/// It was 32 bytes holding a SHA-256, then 32 holding a 4-byte CRC32C padded out, then 8 holding
/// the CRC32C plus a `C32C` marker that made the field self-describing across formats. With one
/// format there is nothing to tell apart, so the marker is gone and the field is the checksum.
/// A page checksum guards against a page that corrupted into something still decodable, not
/// against a forged one, and CRC32C is the right tool for that.
pub(super) const BLOCK_RECORD_CHECKSUM_LEN: usize = 4;

/// What the walks use to decide whether what is left can still be a record.
///
/// One header length now, so this is that length. It was a separate constant while the header
/// was variable, and set too high it made a walk read a short record as the end of the slab and
/// lose every record behind it -- which happened twice.
pub(super) const BLOCK_RECORD_SMALLEST_HEADER_LEN: usize = BLOCK_RECORD_HEADER_LEN;

/// The compression codec byte of a record this code writes, found by walking the header.
///
/// v9 had this at a fixed offset. A v10 header is variable length, so there is no constant to
/// name any more and a caller that wants the byte has to step over the varints to reach it.
#[cfg(test)]
pub(super) fn block_record_compression_byte(record: &[u8]) -> u8 {
    let sized = u32::from_le_bytes(
        record[BLOCK_RECORD_LENGTH_OFFSET..BLOCK_RECORD_LENGTH_OFFSET + 4]
            .try_into()
            .expect("block size slice"),
    );
    (sized >> BLOCK_RECORD_CODEC_SHIFT) as u8
}

/// How many bytes `value` takes as a varint.
///
/// Lets a test state the header length it expects from the values that went in, rather than from
/// a constant: with varints the length is a function of the data, and a test that cannot say what
/// it should be cannot notice when it is wrong.
#[cfg(test)]
pub(super) fn block_record_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Writes one LEB128 varint.
fn put_block_record_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Reads one LEB128 varint, advancing `cursor`.
fn read_block_record_varint(
    record: &[u8],
    cursor: &mut usize,
    address: &ElementEntry,
    what: &str,
) -> Result<u64, BlockStoreError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *record
            .get(*cursor)
            .ok_or_else(|| corrupt_block_envelope(address, format!("truncated {what}")))?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(corrupt_block_envelope(address, format!("oversized {what}")));
        }
    }
}
pub(super) const BLOCK_RECORD_COMPRESSION_MIN_BYTES: usize = 256;
pub(super) const BLOCK_RECORD_COMPRESSION_LEVEL: i32 = 0;
pub(super) const BLOCK_RECORD_COMPRESSION_NONE: u8 = 0;
pub(super) const BLOCK_RECORD_COMPRESSION_ZSTD: u8 = 1;

pub(super) fn default_block_record_compression_enabled() -> bool {
    true
}

pub(super) fn default_block_record_compression_min_bytes() -> usize {
    BLOCK_RECORD_COMPRESSION_MIN_BYTES
}

pub(super) fn default_block_record_compression_level() -> i32 {
    BLOCK_RECORD_COMPRESSION_LEVEL
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockRecordCompression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy)]
struct BlockRecordHeader {
    header_len: usize,
    payload_len: usize,
    pub(super) stored_len: usize,
    checksum: [u8; BLOCK_RECORD_CHECKSUM_LEN],
    block_id: Option<u64>,
    object_id: Option<u64>,
    /// ALWAYS `None`, and kept so the header struct still says what the header does NOT carry.
    ///
    /// `parse_block_record_header` sets it unconditionally; `encode_block_record` discards the
    /// argument it is handed. Read `the_header_carries_neither_identity_it_is_handed` for the
    /// assertion, and `the_surviving_cross_check_refuses_a_mismatched_record` for the one arm on
    /// this read path that a record CAN fail.
    routing_bucket: Option<u32>,
    pub(super) compression: BlockRecordCompression,
}

#[derive(Debug)]
pub(super) struct EncodedBlockRecord {
    pub(super) bytes: Vec<u8>,
    pub(super) logical_len: usize,
    pub(super) stored_len: usize,
    pub(super) compression: BlockRecordCompression,
}

#[derive(Debug)]
pub(super) struct DecodedBlockRecord {
    pub(super) payload: Vec<u8>,
    pub(super) logical_len: usize,
    pub(super) compression: BlockRecordCompression,
}

#[derive(Debug)]
pub(super) struct LogicalRangeRead {
    pub(super) bytes: Vec<u8>,
    pub(super) compressed_records_read: u64,
}

pub(super) fn encode_block_record(
    payload: &[u8],
    block_id: u64,
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    options: BlockStoreOptions,
) -> Result<EncodedBlockRecord, BlockStoreError> {
    let checksum_field = block_record_checksum_field(payload);
    let (squeezed, compression) = encode_block_record_payload(payload, options)?;
    // A compressed block states its decompressed size in its own first four bytes. The header
    // carries the STORED size, because that is what a slab walk steps by, and a walk cannot
    // step over a record whose length it does not know. Putting the decompressed size here
    // rather than in the header keeps the header one size for every block, and costs the four
    // bytes only where something was actually compressed.
    let stored_payload = match compression {
        BlockRecordCompression::None => squeezed,
        BlockRecordCompression::Zstd => {
            let mut framed = Vec::with_capacity(squeezed.len() + 4);
            framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            framed.extend_from_slice(&squeezed);
            framed
        }
    };
    let block_size = stored_payload.len();
    if block_size as u64 > u64::from(BLOCK_RECORD_LENGTH_MASK) {
        return Err(corrupt_block_envelope(
            &ElementEntry::default(),
            format!("block of {block_size} bytes does not fit a block size field"),
        ));
    }
    // The object id and the routing bucket are the index's to remember, and neither is written
    // here. They stay in the signature because the callers hold them and a future format that
    // carries one would take it from exactly here; see the note in `decode_block_record` on what
    // not carrying them costs a read.
    let _ = (object_id, routing_bucket);
    let codec = match compression {
        BlockRecordCompression::None => u32::from(BLOCK_RECORD_COMPRESSION_NONE),
        BlockRecordCompression::Zstd => u32::from(BLOCK_RECORD_COMPRESSION_ZSTD),
    };
    let sized = (codec << BLOCK_RECORD_CODEC_SHIFT) | (block_size as u32);
    let mut record = Vec::with_capacity(BLOCK_RECORD_HEADER_LEN + block_size);
    record.extend_from_slice(BLOCK_RECORD_MAGIC);
    record.extend_from_slice(&checksum_field);
    record.extend_from_slice(&sized.to_le_bytes());
    if block_id > u64::from(u16::MAX) {
        return Err(corrupt_block_envelope(
            &ElementEntry::default(),
            format!("block index {block_id} does not fit a block id field"),
        ));
    }
    record.extend_from_slice(&(block_id as u16).to_le_bytes());
    debug_assert_eq!(record.len(), BLOCK_RECORD_HEADER_LEN, "the header is one size");
    let stored_len = block_size;
    record.extend_from_slice(&stored_payload);
    Ok(EncodedBlockRecord {
        bytes: record,
        logical_len: payload.len(),
        stored_len,
        compression,
