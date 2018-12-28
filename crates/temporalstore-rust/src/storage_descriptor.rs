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
