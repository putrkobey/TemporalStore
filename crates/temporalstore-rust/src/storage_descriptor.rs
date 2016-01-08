// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage-layer descriptors: the per-block header and the segmented-stream layer.
//!
//! Completes the on-disk type set alongside [`crate::wal_record`] (the write-ahead log) and
//! [`crate::index_log_record`] (the served-index log). Three groups live here:
//!
//!   * [`BlockHeader`] — the per-block descriptor. This is what the log-item-to-block
//!     conversion fills in, and it is where [`BlockHeader::block_in_wal`] records that a
//!     block's bytes are still in the WAL rather than in a slab.
//!   * [`SlabInfo`] / [`SlabHeader`] / [`BlockFooter`] — the slabbed-stream layer. A stream
//!     is a chain of slabs; each slab carries a header naming the whole chain, and the
//!     fixed-size blocks inside it end with a footer that makes a torn tail detectable on
//!     reopen.
//!   * [`StreamInfo`] / [`WalInfo`] — the runtime views a follower uses to adopt a leader's
//!     stream state without re-reading the stream itself.
//!
//! Field numbers are fixed by the on-disk layout and must not be renumbered: a stored stream
//! outlives the process that wrote it.

use prost::Message;

/// Block size a stream is written in: 128 KiB.
pub const STREAM_BLOCK_SIZE: u64 = 1 << 17;

/// Footer reserved at the end of every stream block: 128 B.
pub const STREAM_BLOCK_FOOTER_SIZE: u64 = 1 << 7;

/// Magic stamped into slab headers and block footers.
pub const STREAM_MAGIC: u64 = 0xBCBC_BCBC_6666_6666;

/// Stream format version.
pub const STREAM_VERSION: u32 = 1;

/// How a block's payload was compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockCompression {
    None = 0,
    Lz4 = 1,
}

/// Per-block descriptor.
///
/// `raw_data_size` is the size before compression and `data_size` after, so a reader can size
/// its buffer without decompressing first.
#[derive(Clone, PartialEq, Message)]
pub struct BlockHeader {
    /// Fixed-width, so the encoded size does not vary with the bucket value.
    #[prost(fixed64, tag = "1")]
    pub routing_bucket: u64,
    #[prost(uint32, tag = "2")]
    pub object_id: u32,
    #[prost(uint32, tag = "3")]
    pub block_id: u32,
    /// Size before compression.
    #[prost(uint64, tag = "4")]
    pub raw_data_size: u64,
    /// Size after compression.
    #[prost(uint64, tag = "5")]
    pub data_size: u64,
    #[prost(uint64, tag = "6")]
    pub timestamp_ms: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub key: Vec<u8>,
    #[prost(uint32, tag = "8")]
    pub model_id: u32,
    #[prost(enumeration = "BlockCompression", tag = "9")]
    pub compress: i32,
    /// The block's bytes live in the WAL record at [`Self::wal_sequence`], not in a slab. Set
    /// for every block that has not been dumped yet.
    #[prost(bool, tag = "10")]
    pub block_in_wal: bool,
    /// Blocks for one object are ordered by this. It is derived from the log sequence, so a
    /// later write always wins.
    #[prost(uint64, tag = "11")]
    pub version: u64,
    /// Position in the WAL when the block was written.
    #[prost(uint64, tag = "12")]
    pub wal_sequence: u64,
}

/// One slab in a stream's chain.
///
/// `truncated_offset` is how a stream records that its head has been logically discarded
/// without rewriting the slab.
#[derive(Clone, PartialEq, Message)]
pub struct SlabInfo {
    #[prost(uint64, tag = "1")]
    pub slab_id: u64,
    #[prost(uint64, tag = "2")]
    pub start_offset: u64,
    #[prost(uint64, tag = "3")]
    pub slab_start_offset: u64,
    #[prost(uint64, tag = "4")]
    pub slab_end_offset: u64,
    #[prost(uint64, tag = "5")]
    pub end_record_sequence: u64,
    #[prost(uint64, tag = "6")]
    pub freeze_ms: u64,
    #[prost(uint64, tag = "7")]
    pub end_offset: u64,
    #[prost(uint64, tag = "8")]
    pub truncated_offset: u64,
}

/// The header written at the front of every slab.
///
/// It names the whole chain (`data_slabs`), which is what lets a reopen rebuild the stream
/// from the last slab alone rather than scanning all of them.
#[derive(Clone, PartialEq, Message)]
pub struct SlabHeader {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint64, tag = "2")]
    pub slab_id: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub slab_name: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub timestamp_ms: u64,
    #[prost(uint32, tag = "5")]
    pub version: u32,
    #[prost(uint64, tag = "6")]
    pub start_record_sequence: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub client_token: Vec<u8>,
    #[prost(message, repeated, tag = "8")]
    pub data_slabs: Vec<SlabInfo>,
    #[prost(uint64, tag = "9")]
    pub start_offset: u64,
    /// Present in the layout, never computed here. Nothing writes it and nothing reads it.
    ///
    /// It exists to chain one block to the one before it, so a block that went missing or arrived
    /// out of order is caught by the chain rather than by each record separately. This store does
    /// not do that: a record carries its own CRC32C, which is written and verified on every read,
    /// and a missing record shows up as a break in the sequence the loader already refuses.
    ///
    /// Left in place so the layout stays the shape it is read against. Do not add a check against
    /// it without computing it first -- it is always zero, so such a check would either pass
    /// always or fail always, and both look like a working validator.
    #[prost(uint32, tag = "10")]
    pub prev_block_crc32c: u32,
    #[prost(uint64, tag = "11")]
    pub truncated_offset: u64,
    #[prost(fixed32, tag = "12")]
    pub header_size: u32,
    #[prost(message, repeated, tag = "13")]
    pub obsolete_slabs: Vec<SlabInfo>,
}

/// The footer closing every fixed-size block within a slab.
///
/// A tail scan on reopen reads these to recover where the stream really ended: the last
/// complete record, its sequence, and the running CRC. Tag 11 is unused in the on-disk layout
/// and stays unused here.
#[derive(Clone, PartialEq, Message)]
pub struct BlockFooter {
    #[prost(fixed64, tag = "1")]
    pub magic: u64,
    #[prost(uint32, tag = "2")]
    pub version: u32,
    #[prost(uint64, tag = "3")]
    pub timestamp_ms: u64,
    /// Present in the layout, never computed here: the writer passes a literal zero and nothing
    /// reads it back. Integrity comes from the per-record CRC32C instead, which is verified on
    /// every read -- see `prev_block_crc32c` for the same note and the same warning about adding
    /// a check against a field that is always zero.
    #[prost(fixed32, tag = "4")]
    pub block_crc: u32,
    #[prost(uint64, tag = "5")]
    pub block_number: u64,
    #[prost(uint32, tag = "6")]
    pub block_end: u32,
    #[prost(uint64, tag = "7")]
    pub last_record_offset: u64,
    #[prost(uint32, tag = "8")]
    pub last_record_left_size: u32,
    #[prost(uint64, tag = "9")]
    pub last_record_sequence: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub client_token: Vec<u8>,
    // Tag 11 is unused on disk. Do not reuse.
    #[prost(uint64, tag = "12")]
    pub truncated_offset: u64,
}

/// Runtime view of a stream, used to restore a follower's stream state from a leader's.
#[derive(Clone, PartialEq, Message)]
pub struct StreamInfo {
