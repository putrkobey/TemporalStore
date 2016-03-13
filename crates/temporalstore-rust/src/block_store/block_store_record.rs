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
    })
}

fn encode_block_record_payload(
    payload: &[u8],
    options: BlockStoreOptions,
) -> Result<(Vec<u8>, BlockRecordCompression), BlockStoreError> {
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::BlockBytes, || {
        encode_block_record_payload_inner(payload, options)
    })
}

/// The encode itself. Split out so the allocation class above covers a whole function body rather
/// than a span the early return skips: an uncompressed write takes that return, and it is the one
/// a scope wrapped around the tail would have missed.
fn encode_block_record_payload_inner(
    payload: &[u8],
    options: BlockStoreOptions,
) -> Result<(Vec<u8>, BlockRecordCompression), BlockStoreError> {
    if !options.compression_enabled || payload.len() < options.compression_min_bytes {
        return Ok((payload.to_vec(), BlockRecordCompression::None));
    }
    // Reuse one compression context per thread, the mirror of the decoder below.
    //
    // `zstd::stream::encode_all` constructs a compressor, uses it once and drops it, and a
    // compressor allocates its working buffers up front. Measured by sweeping value sizes
    // through a write: cost is flat at 2.0 B allocated per payload byte on both sides of a
    // step between 192 and 256 bytes worth 32,535 B per write -- and 256 is
    // `BLOCK_RECORD_COMPRESSION_MIN_BYTES`, so the step IS this call. At the 1,244-byte average
    // of a soak corpus that is about 80% of everything a write allocates.
    //
    // The level is per-call rather than fixed, so it is set on the held compressor each time;
    // that is a parameter update, not a rebuild. A compressor is `&mut` to compress, so this is
    // thread-local for the same reason the decompressor is: one shared instance would serialise
    // every writer behind a lock on a path that is otherwise concurrent.
    //
    // The stored bytes CHANGE -- the bulk API frames a stream without the content-size header
    // the streaming call writes, so output is a little smaller. Both are ordinary zstd frames,
    // both decode through the path below and through the streaming fallback, so a record written
    // by either build reads on either. It is still a change to what lands on disk.
    let level = options.compression_level.clamp(-7, 22);
    let compressed = ZSTD_COMPRESSOR.with(|cell| {
        let mut compressor = cell.borrow_mut();
        compressor.set_compression_level(level)?;
        compressor.compress(payload)
    })?;
    if compressed.len() < payload.len() {
        Ok((compressed, BlockRecordCompression::Zstd))
    } else {
        Ok((payload.to_vec(), BlockRecordCompression::None))
    }
}

pub(super) fn decode_block_record(
    record: &[u8],
    address: &ElementEntry,
) -> Result<DecodedBlockRecord, BlockStoreError> {
    if record.len() < BLOCK_RECORD_HEADER_LEN || !record.starts_with(BLOCK_RECORD_MAGIC) {
        return Ok(DecodedBlockRecord {
            payload: record.to_vec(),
            logical_len: record.len(),
            compression: BlockRecordCompression::None,
        });
    }
    if record.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
        return Err(corrupt_block_envelope(address, "short header"));
    }
    let header = parse_block_record_header(record, address)?;
    if let (Some(address_block_id), Some(record_block_id)) = (address.block_id(), header.block_id) {
        if address_block_id != record_block_id {
            return Err(corrupt_block_envelope(
                address,
                format!("page id mismatch: address {address_block_id}, record {record_block_id}"),
            ));
        }
    }
    // THE CHECK BELOW CANNOT FIRE, AND THE ROUTING-BUCKET ARM THAT SAT UNDER IT IS NOW GONE.
    //
    // `parse_block_record_header` returns `object_id: None` and returned `routing_bucket: None` --
    // the record header carries NEITHER FIELD AT ALL, and `encode_block_record` discards both
    // (`let _ = (object_id, routing_bucket);`) -- so both `if let` pairs failed to match on every
    // read ever taken. Mutating each to fail unconditionally (`|| true`) left 89 of 89 block-store
    // and address-footprint tests green; the same mutation on the block-id arm above turned 23 of
    // those 89 red, so the harness does notice a check that breaks.
    //
    // SO THERE WAS NO ADDRESS-VERSUS-RECORD CROSS-CHECK ON THE ROUTING BUCKET TO LOSE. Its address
    // side is gone with the field, and its record side never existed: no record can be built that
    // makes it fire, which is the strongest statement available about a check. What replaces it is
    // not in this function -- `validate_bucket_ownership_index_from_entries` compares where a page
    // is FILED against where its KEY routes, which are two independent answers, and
    // `BucketReleaseRefusal::BlockRoutingMismatch` refuses a release on the same comparison.
    //
    // THE OBJECT-ID ARM IS LEFT IN PLACE rather than deleted, because the payload checksum does
    // NOT cover what it names. `block_record_checksum_field` is a CRC32C over the payload alone:
    // it binds a record to ITSELF, never to the address that reached it. An intact record belonging
    // to a different object verifies perfectly. So the object-id arm is the one detector here worth
    // having, and reinstating it means carrying the object id on the wire -- a FORMAT change and a
    // decision for whoever owns the format, not a tidy-up. The routing bucket would only ever have
    // caught a subset of what the object id catches, being a many-to-one function of it, for four
    // more bytes on every record; if one of the two is ever carried it should be the object id.
    //
    // WHAT WAS DELETED, and why it is not the same case: a third arm compared
    // `address.slab_id()` against `header.slab_id`. That one was vacuous by construction rather
    // than merely dead. `BlockAddress::slab_id()` returns `Some(self.block_slab_id)`
    // unconditionally, and `block_slab_id` is the slab file `BlockStore::read` just opened, so
    // the address side of the comparison was never an independent claim -- it was "which file am
    // I holding". The record side, had it been written, came from the same variable the file path
    // came from (`append_block_of_object` builds both from `inner.block_slab_id`). The two sides
    // agree in every reachable state INCLUDING a reused slab id, where a record in the new slab
    // stamps the reused number and a stale address naming that number compares it against itself.
    // Carrying the field would have bought no discrimination at any offset in the format.
    // THE OBJECT-ID CROSS-CHECK STOOD HERE AND WAS ALREADY UNFIREABLE, which is why it went with
    // the field rather than being rebuilt around a derived value. `encode_block_record` takes an
    // `object_id` and discards it; `parse_block_record_header` returns `None` for it
    // unconditionally. So no record can be built whose header disagrees with anything, and
    // `the_envelope_carries_a_crc_not_a_digest` asserts that `None` in place. Rebuilding the check
    // against an id DERIVED from the key would have been strictly worse than deleting it: both
    // sides would come from the same terms, so it could only ever compare a value with itself.
    // The length check below is not an identity check, but it IS the only thing besides the block
    // ordinal that stands between a stale address and a different record's bytes: the address
    // hands `read_range` a length, and a record at that offset whose header states a different
    // stored size is refused here. A misdirected read therefore has to land on an exact record
    // boundary AND match that record's total encoded length before the block ordinal is even
    // consulted.
    if record.len() != header.header_len + header.stored_len {
        return Err(corrupt_block_envelope(
            address,
            "payload length mismatch".to_string(),
        ));
    }
    let payload = decode_block_record_payload(&record[header.header_len..], &header, address)?;
    verify_block_record_checksum(&payload, &header.checksum, address)?;
    Ok(DecodedBlockRecord {
        payload,
        logical_len: header.payload_len,
        compression: header.compression,
    })
}

pub(super) fn logical_range_from_slab(
    slab: &[u8],
    block_slab_id: u64,
    offset: u64,
    size: u64,
) -> Result<LogicalRangeRead, BlockStoreError> {
    if size == 0 {
        return Ok(LogicalRangeRead {
            bytes: Vec::new(),
            compressed_records_read: 0,
        });
    }
    if slab.len() < BLOCK_RECORD_HEADER_LEN || !slab.starts_with(BLOCK_RECORD_MAGIC) {
        let start = offset as usize;
        let end = start.saturating_add(size as usize).min(slab.len());
        let bytes = if start >= slab.len() {
            Vec::new()
        } else {
            slab[start..end].to_vec()
        };
        return Ok(LogicalRangeRead {
            bytes,
            compressed_records_read: 0,
        });
    }

    let requested_start = offset as usize;
    let requested_end = requested_start.saturating_add(size as usize);
    let mut physical_offset = 0usize;
    let mut logical_offset = 0usize;
    let mut out = Vec::with_capacity(size as usize);
    let mut compressed_records_read = 0_u64;

    while physical_offset < slab.len() && out.len() < size as usize {
        let remaining = &slab[physical_offset..];
        let address = ElementEntry::from_parts(block_slab_id, physical_offset as u64, 0, None, None);
        if remaining.len() < BLOCK_RECORD_HEADER_LEN || !remaining.starts_with(BLOCK_RECORD_MAGIC) {
            return Err(corrupt_block_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
            return Err(corrupt_block_envelope(&address, "short header"));
        }
        let header = parse_block_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_block_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        let address = ElementEntry::from_parts(0, 0, record_len as u64, header.block_id, header.object_id);
        let payload = decode_block_record_payload(
            &remaining[header.header_len..record_len],
            &header,
            &address,
        )?;
        verify_block_record_checksum(&payload, &header.checksum, &address)?;
        if header.compression == BlockRecordCompression::Zstd {
            compressed_records_read += 1;
        }

        let logical_end = logical_offset.saturating_add(header.payload_len);
        let overlap_start = requested_start.max(logical_offset);
        let overlap_end = requested_end.min(logical_end);
        if overlap_start < overlap_end {
            let payload_start = overlap_start - logical_offset;
            let payload_end = overlap_end - logical_offset;
            out.extend_from_slice(&payload[payload_start..payload_end]);
        }

        physical_offset = physical_offset.saturating_add(record_len);
        logical_offset = logical_end;
    }

    Ok(LogicalRangeRead {
        bytes: out,
        compressed_records_read,
    })
}

/// Reads a header, whose fields are varints and so can only be read in order.
///
/// There is no declared header length to check against: the header ends where the walk ends.
fn parse_block_record_header(
    record: &[u8],
    address: &ElementEntry,
) -> Result<BlockRecordHeader, BlockStoreError> {
    if !record.starts_with(BLOCK_RECORD_MAGIC) {
        return Err(corrupt_block_envelope(address, "not a block record"));
    }
    if record.len() < BLOCK_RECORD_HEADER_LEN {
        return Err(corrupt_block_envelope(address, "short header"));
    }
    let checksum = record[BLOCK_RECORD_CHECKSUM_OFFSET..BLOCK_RECORD_CHECKSUM_OFFSET + BLOCK_RECORD_CHECKSUM_LEN]
        .try_into()
        .expect("block checksum slice");
    let sized = u32::from_le_bytes(
        record[BLOCK_RECORD_LENGTH_OFFSET..BLOCK_RECORD_LENGTH_OFFSET + 4]
            .try_into()
            .expect("block size slice"),
    );
    let block_size = (sized & BLOCK_RECORD_LENGTH_MASK) as usize;
    let codec = (sized >> BLOCK_RECORD_CODEC_SHIFT) as u8;
    let block_id = u16::from_le_bytes(
        record[BLOCK_RECORD_BLOCK_ID_OFFSET..BLOCK_RECORD_BLOCK_ID_OFFSET + 2]
            .try_into()
            .expect("block id slice"),
    );
    let compression = match codec {
        BLOCK_RECORD_COMPRESSION_NONE => BlockRecordCompression::None,
        BLOCK_RECORD_COMPRESSION_ZSTD => BlockRecordCompression::Zstd,
        codec => {
            return Err(corrupt_block_envelope(
                address,
                format!("unsupported compression codec {codec}"),
            ));
        }
    };
    // The header carries the STORED size, because that is what a slab walk steps by. For an
    // uncompressed block the stored bytes ARE the block, so the two sizes are one number. For a
    // compressed one the decompressed size is the block's own first four bytes -- which is
    // readable here without decompressing anything, so callers that only want to ACCOUNT for
    // logical bytes (slab summaries, logical range reads) get the right number for free.
    let payload_len = match compression {
        BlockRecordCompression::None => block_size,
        BlockRecordCompression::Zstd => {
            let at = BLOCK_RECORD_HEADER_LEN;
            if record.len() < at + 4 {
                return Err(corrupt_block_envelope(
                    address,
                    "compressed block has no decompressed size",
                ));
            }
            u32::from_le_bytes(
                record[at..at + 4]
                    .try_into()
                    .expect("decompressed size slice"),
            ) as usize
        }
    };
    Ok(BlockRecordHeader {
        header_len: BLOCK_RECORD_HEADER_LEN,
        payload_len,
        stored_len: block_size,
        checksum,
        block_id: Some(u64::from(block_id)),
        // The index holds these. A record that repeated them could only ever agree or be wrong.
        object_id: None,
        routing_bucket: None,
        compression,
    })
}

thread_local! {
    /// One zstd decompression context per thread, reused across page reads.
    ///
    /// Thread-local rather than a shared pool: a decompressor is `&mut` for the duration of a
    /// decompress, so sharing one would serialise every reader behind a lock on a path that is
    /// otherwise concurrent.
    static ZSTD_DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> =
        std::cell::RefCell::new(
            zstd::bulk::Decompressor::new().expect("zstd decompressor construction"),
        );

    /// The write-side mirror. Level is set per call, so the level chosen here is only a starting
    /// point and never the one a record is actually compressed at.
    static ZSTD_COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> =
        std::cell::RefCell::new(
            zstd::bulk::Compressor::new(0).expect("zstd compressor construction"),
        );
}

fn decode_block_record_payload(
    stored_payload: &[u8],
    header: &BlockRecordHeader,
    address: &ElementEntry,
) -> Result<Vec<u8>, BlockStoreError> {
    match header.compression {
        BlockRecordCompression::None => Ok(stored_payload.to_vec()),
        BlockRecordCompression::Zstd => {
            // Reuse one decompression context per thread instead of building one per read.
            //
            // `zstd::stream::decode_all` constructs a fresh streaming decoder each call, and a
            // decoder allocates its window buffer up front. Measured over 120 freshly written
            // summary records: the block-store read allocated ~132 KB per address to return a
            // 344-byte payload, about 380x the data, and that read was 78% of the whole cost of
            // fetching a record. The window is the same size whatever the record is, so the
            // smaller the record the worse the ratio -- which is the wrong way round for a point
            // read.
            //
            // `header.payload_len` is the exact decompressed size, so the bulk API needs no
            // guessed capacity. The length check below still runs: it guards against a record
            // whose header disagrees with its payload, which is corruption, not a size hint.
            // `payload_len` comes out of the record header, and the bulk API allocates that much
            // BEFORE anything is decompressed or checked -- so a header that lies (corruption, a
            // truncated write, a hostile record) would turn into an allocation of whatever it
            // claims. The old streaming call sized its buffer from what it actually decompressed
            // and could not be steered this way, so this bound is guarding a hazard the reuse
            // introduces, not one that was already here.
            //
            // Above the ceiling, fall back to the streaming decoder: it pays the window
            // allocation, but a record that large is not the case being optimised, and the
            // fallback keeps behaviour identical rather than failing a read that used to work.
            const ZSTD_TRUSTED_PAYLOAD_CEILING: usize = 64 << 20;
            // A compressed block states its decompressed size in its own first four bytes.
            if stored_payload.len() < 4 {
                return Err(corrupt_block_envelope(
                    address,
                    "compressed block has no decompressed size",
                ));
            }
            let (size_bytes, frame) = stored_payload.split_at(4);
            let logical_len = u32::from_le_bytes(
                size_bytes.try_into().expect("decompressed size slice"),
            ) as usize;
            let payload = if logical_len <= ZSTD_TRUSTED_PAYLOAD_CEILING {
                ZSTD_DECOMPRESSOR
                    .with(|decompressor| decompressor.borrow_mut().decompress(frame, logical_len))
                    .map_err(|err| {
                        corrupt_block_envelope(address, format!("zstd decompression failed: {err}"))
                    })?
            } else {
                zstd::stream::decode_all(Cursor::new(frame)).map_err(|err| {
                    corrupt_block_envelope(address, format!("zstd decompression failed: {err}"))
                })?
            };
            if payload.len() != logical_len {
                return Err(corrupt_block_envelope(
                    address,
                    format!(
                        "decompressed length {} does not match the size the block states, {}",
                        payload.len(),
                        logical_len
                    ),
                ));
            }
            Ok(payload)
        }
    }
}

/// Build the 32-byte checksum field for a new (v7) record: CRC32C little-endian in the first
/// four bytes, a marker so the field is self-describing, then zero padding.
fn block_record_checksum_field(payload: &[u8]) -> [u8; BLOCK_RECORD_CHECKSUM_LEN] {
    crate::checksum::crc32c(payload).to_le_bytes()
}

/// Verify a page record's payload against its stored CRC32C.
fn verify_block_record_checksum(
    payload: &[u8],
    expected_checksum: &[u8; BLOCK_RECORD_CHECKSUM_LEN],
    address: &ElementEntry,
) -> Result<(), BlockStoreError> {
    let stored = u32::from_le_bytes(*expected_checksum);
    let actual = crate::checksum::crc32c(payload);
    if stored == actual {
        return Ok(());
    }
    Err(BlockStoreError::ChecksumMismatch {
        block_slab_id: address.block_slab_id(),
        offset: address.offset(),
        length: address.length(),
        expected: format!("{stored:08x}"),
        actual: format!("{actual:08x}"),
    })
}

pub(super) fn corrupt_block_envelope(
    address: &ElementEntry,
    reason: impl Into<String>,
) -> BlockStoreError {
    BlockStoreError::CorruptBlockEnvelope {
        block_slab_id: address.block_slab_id(),
        offset: address.offset(),
        reason: reason.into(),
    }
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The digest as the 32 bytes it is, for storing.
///
/// `sha256_hex` remains for the places that want text -- a report, an error message. What it does
/// not do any more is decide the in-memory representation of every page in the index.
pub(super) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SlabSummary {
    pub(super) logical_bytes: u64,
    pub(super) first_block_id: Option<u64>,
    pub(super) last_block_id: Option<u64>,
}

pub(super) fn summarize_slab(
    slab: &[u8],
    block_slab_id: u64,
) -> Result<SlabSummary, BlockStoreError> {
    if slab.len() < BLOCK_RECORD_HEADER_LEN || !slab.starts_with(BLOCK_RECORD_MAGIC) {
        return Ok(SlabSummary {
            logical_bytes: slab.len() as u64,
            first_block_id: None,
            last_block_id: None,
        });
    }
    let mut physical_offset = 0usize;
    let mut summary = SlabSummary::default();
    while physical_offset < slab.len() {
        let remaining = &slab[physical_offset..];
        let address = ElementEntry::from_parts(block_slab_id, physical_offset as u64, 0, None, None);
        if remaining.len() < BLOCK_RECORD_HEADER_LEN || !remaining.starts_with(BLOCK_RECORD_MAGIC) {
            return Err(corrupt_block_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < BLOCK_RECORD_SMALLEST_HEADER_LEN {
            return Err(corrupt_block_envelope(&address, "short header"));
        }
        let header = parse_block_record_header(remaining, &address)?;
        let record_len = header.header_len.saturating_add(header.stored_len);
        if remaining.len() < record_len {
            return Err(corrupt_block_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        if let Some(block_id) = header.block_id {
            summary.first_block_id = Some(
                summary
                    .first_block_id
                    .map_or(block_id, |current| current.min(block_id)),
            );
            summary.last_block_id = Some(
                summary
                    .last_block_id
                    .map_or(block_id, |current| current.max(block_id)),
            );
        }
        summary.logical_bytes = summary
            .logical_bytes
            .saturating_add(header.payload_len as u64);
        physical_offset = physical_offset.saturating_add(record_len);
    }
    Ok(summary)
}

/// The scan reads a prefix big enough to hold a header. There is one header size now.
const BLOCK_RECORD_WIDEST_HEADER_LEN: usize = BLOCK_RECORD_HEADER_LEN;

/// Read buffer for the page-id scan.
///
/// Sized so a bufferful spans many records rather than one: at the ~300-byte records the live
/// store holds, this is ~800 per fill, which is the difference between thousands of reads for a
/// slab and millions.
const BLOCK_RECORD_SCAN_BUFFER_BYTES: usize = 256 * 1024;

/// TS_BLOCK_INDEX_CHECKSUMS: recompute and record a hex digest per page record while inspecting.
///
/// Default OFF. `inspect_slab` runs at every engine open, and this hashes each payload a second
/// time -- `decode_block_record` has already verified the stored checksum -- then allocates a
/// 64-character String for it. Measured on a live-store copy, slab verification ran at 13.5 MB/s
/// against hundreds of MB/s for sha256 alone.
///
/// ONE caller reads the field: `block_address_api_ready` in
/// `StorageDataStructureApiParityReport`, which required `checksum.is_some()` on a block index
/// entry. An earlier version of this
/// comment claimed nothing read it -- wrong by exactly one -- and switching the hashing off left
/// that report permanently `ready: false` behind a `block_address_metadata_incomplete` blocker on
/// every default deployment. The report now asks for the checksum only when this is enabled, so
/// the two agree in both positions of the flag.
