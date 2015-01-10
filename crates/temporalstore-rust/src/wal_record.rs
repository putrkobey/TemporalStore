// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-ahead log record, ported from the design described below.
//!
//! This is a direct port: same record shape, same field numbers, same framing, and the same
//! addressing scheme. Only the names differ, following this crate's vocabulary — a routing
//! bucket rather than a slot, a block rather than a page, a shard rather than a partition.
//!
//! # Why this replaces a command log
//!
//! The existing WAL record carries a `Command`, so recovery re-executes the operation. That
//! only reproduces state if everything that influenced the original execution is reproduced
//! too, which is why config-driven eviction had to be made WAL-sequence-ordered and why expiry
//! needs a replay clock. This design stores the *result* instead: an item describes a
//! mutation at the storage layer, and a block-carrying item holds the block image itself.
//! Replay installs bytes rather than re-deriving them.
//!
//! # Addressing: the log id
//!
//! A block-carrying item does not name a slab. Its address is the **log id** — the byte offset
//! of the record that contains it — exactly as this design sets `address = log_id` when it
//! turns a log item into a block descriptor. Nothing needs to be looked up: a read seeks to the
//! offset and parses the record there. That is why the framing below must be length-prefixed
//! and offset-addressable rather than line-oriented.
//!
//! The address cannot be known while a command executes, because the record has not been
//! appended yet and therefore has no offset. This design resolves this by staging: items
//! accumulate during execution, and at commit the append returns the offset which is then
//! written back into every staged block descriptor. This module provides the pieces for that;
//! see [`block_address_from_item`].
//!
//! # Framing
//!
//! `varint32(payload_len) | little_endian_u32(crc32c) | payload`
//!
//! matching the record header exactly. The payload is protobuf, with the same field
//! numbers as the log item, so the encodings are wire-compatible.

use prost::Message;

use crate::block_store::ElementEntry;
pub use crate::record_framing::{decode_framed_at, encode_framed, RecordFramingError as WalFramingError};

/// Record format version, mirroring the log version.
pub const WAL_RECORD_VERSION: u32 = 1;

/// One write-ahead log record: the unit that is appended, and whose byte offset becomes the log
/// id for every block it carries. Mirrors the operation-log message.
#[derive(Clone, PartialEq, Message)]
pub struct WalRecord {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub items: Vec<WalItem>,
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
}

/// One mutation inside a record. Field numbers match the log item one-for-one, so
/// the two encodings are wire-compatible; only the names are this crate's.
#[derive(Clone, PartialEq, Message)]
pub struct WalItem {
    #[prost(uint64, tag = "1")]
    pub routing_bucket: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub object_key: Vec<u8>,
    #[prost(uint32, tag = "3")]
    pub model_id: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub value: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub timestamp_ms: u64,
    #[prost(uint64, tag = "7")]
    pub cluster_id: u64,
    #[prost(bool, tag = "8")]
    pub deleted: bool,
    #[prost(bool, tag = "9")]
    pub object_deleted: bool,
    /// The item carries a block image.
    #[prost(bool, tag = "10")]
    pub block_log: bool,
    #[prost(uint32, tag = "11")]
    pub object_id: u32,
    #[prost(uint32, tag = "12")]
    pub block_id: u32,
    /// The block image.
    #[prost(bytes = "vec", tag = "13")]
    pub block: Vec<u8>,
    #[prost(uint64, tag = "14")]
    pub ttl: u64,
    #[prost(bool, tag = "15")]
    pub meta_log: bool,
}

/// Build the block address for a block-carrying item, from the log id of the record that
/// contains it.
///
/// This is the port of the log-item-to-block-descriptor conversion: the address is
/// the log id, and the length is the record's framed size. Nothing is looked up — the address
/// alone locates the bytes.
/// FALLIBLE, BECAUSE A LOG ID IS THE ONE OFFSET NOTHING BOUNDS.
///
/// Every other offset in this engine is a position inside a slab, and the slab target is capped
/// so that position fits in 32 bits. A log id is a position in the WAL measured from a reclaim
/// base that only ever grows, so it passes 4 GiB on any store that has written 4 GiB of log --
/// which is an afternoon, not a lifetime. There is nothing to cap here and nothing to clamp to,
/// so the conversion is refused and the caller is told.
///
/// This spelling has no production caller today: the live write path mints the counter-shaped
/// `HOT_BLOCK_SLAB_ID` address instead (see [`is_wal_resident`]). The refusal is therefore what
/// a future caller meets on the day it starts minting positions, rather than a truncation it
/// would meet silently.
pub fn block_address_from_item(
    log_id: u64,
    log_size: u64,
    item: &WalItem,
) -> Result<ElementEntry, crate::block_store::BlockAddressOutOfRange> {
    // The item's own `routing_bucket` is not stamped onto the address any more: the WAL item IS the
    // container that holds it, and every reader of this address reaches it through that item.
    ElementEntry::try_from_parts(
        WAL_LOG_SLAB_ID,
        log_id,
        log_size,
        Some(u64::from(item.block_id)),
        Some(u64::from(item.object_id)),
    )
}

/// Sentinel slab id marking an address that resolves inside the WAL rather than a slab.
/// A reserved slab id carries this distinction through the existing address type without
/// widening it or adding a parallel flag.
/// AT THE TOP OF THE THIRTY-TWO BIT RANGE, NOT THE SIXTY-FOUR BIT ONE -- see the note on
/// `engine::HOT_BLOCK_SLAB_ID`, which moved for the same reason and sits one above this.
pub const WAL_LOG_SLAB_ID: u64 = (u32::MAX as u64) - 1;

/// Whether an address resolves inside the WAL rather than in a slab file.
///
/// Two sentinels answer to this, because the idea got invented twice. [`WAL_LOG_SLAB_ID`] is
/// the ported one, where the address carries the log id of the record holding the block, so a
/// read seeks straight to it and the mapping survives a restart because it lives in the served
/// index. `HOT_BLOCK_SLAB_ID` is what the live write path mints today: a counter rather than a
/// position, which is why resolving it needs a process-local registry and why that mapping is
/// gone after a reload.
///
/// They ask the same question -- is this block in the log? -- so it gets one answer here
/// instead of an open-coded comparison at each site. The sites are then already asking the
/// right question on the day the write path starts minting positions instead of counters.
pub fn is_wal_resident(block_slab_id: u64) -> bool {
    block_slab_id == WAL_LOG_SLAB_ID || block_slab_id == crate::engine::HOT_BLOCK_SLAB_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_item(routing_bucket: u64, block: &[u8]) -> WalItem {
        WalItem {
            routing_bucket,
            block_log: true,
            block: block.to_vec(),
            block_id: 7,
            object_id: 3,
            ..Default::default()
        }
    }

    #[test]
    fn framed_record_round_trips() {
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 42,
            items: vec![block_item(5, b"block bytes with a \n newline inside")],
        };
        let framed = encode_framed(&record);
        let (decoded, next_offset): (WalRecord, usize) = decode_framed_at(&framed, 0).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(next_offset, framed.len());
        // The newline is the whole reason this framing is length-prefixed rather than
        // line-oriented: a line reader would have split this record in two.
        assert!(decoded.items[0].block.contains(&b'\n'));
    }

    #[test]
    fn the_log_id_is_the_byte_offset_of_the_record() {
        // Two records in one log; the second is addressed by where it starts.
        let first = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 1,
            items: vec![block_item(1, b"first")],
        };
        let second = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 2,
            items: vec![block_item(2, b"second")],
        };
        let mut log = encode_framed(&first);
        let second_log_id = log.len();
        log.extend_from_slice(&encode_framed(&second));

        let (decoded, _): (WalRecord, usize) = decode_framed_at(&log, second_log_id).unwrap();
        assert_eq!(decoded.sequence, 2);
        assert_eq!(decoded.items[0].block, b"second");
    }

    #[test]
    fn sequential_scan_walks_every_record() {
        let records: Vec<WalRecord> = (1..=5)
            .map(|sequence| WalRecord {
                version: WAL_RECORD_VERSION,
                sequence,
                items: vec![block_item(sequence, format!("block-{sequence}").as_bytes())],
            })
            .collect();
        let mut log = Vec::new();
        for record in &records {
            log.extend_from_slice(&encode_framed(record));
        }

        let mut offset = 0;
        let mut seen: Vec<WalRecord> = Vec::new();
        while offset < log.len() {
            let (record, next_offset): (WalRecord, usize) =
                decode_framed_at(&log, offset).unwrap();
            // A scan MUST make progress; without this the loop is unbounded and the growing
            // `seen` takes the process out via the OOM killer rather than failing an assert.
            assert!(next_offset > offset, "scan did not advance past {offset}");
            seen.push(record);
            offset = next_offset;
        }
        assert_eq!(seen, records);
    }

    #[test]
    fn a_corrupted_payload_is_rejected_not_skipped() {
        // A block-carrying record IS the durable copy, so corruption must surface as an error
        // rather than as an absent block.
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 9,
            items: vec![block_item(1, b"payload")],
        };
        let mut framed = encode_framed(&record);
        let last = framed.len() - 1;
        framed[last] ^= 0xff;
        assert!(decode_framed_at::<WalRecord>(&framed, 0).is_err());
    }

    #[test]
    fn a_truncated_tail_is_rejected() {
        let record = WalRecord {
            version: WAL_RECORD_VERSION,
            sequence: 9,
            items: vec![block_item(1, b"payload that will be cut short")],
