// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The served-index log record, ported from the design described below.
//!
//! A FAITHFUL RECORD OF AN EXTERNAL SHAPE, RETAINED FOR COMPARISON, AND **NOT ADOPTABLE AS THE
//! LIVE ROW**. The record shape and the field numbers are faithful and the six tests below pin
//! them; only the names follow this crate's vocabulary -- routing bucket for slot, block for page,
//! slab for zone, WAL for the operation log. TWO CLAIMS THIS HEADER USED TO MAKE WERE FALSE and
//! are corrected below: the framing claim, which would have lost data silently, and the reason it
//! gave for porting [`IndexItem::in_wal`], which is a step this tree took and reversed on purpose.
//!
//! KEPT RATHER THAN DELETED, DELIBERATELY. A port that records a shape this tree diverged from is
//! what documents the divergence, and the live row's own doc comments price themselves against it
//! repeatedly. But it has to say WHICH PARTS WERE REJECTED AND WHY, or the next reader re-derives
//! all of the below -- which is the state this header left the module in.
//!
//! Measured at `df6164e52` by `engine::tests::adopting_the_ported_index_log_record` (seven tests,
//! which carry the controls and the fail-first mutations for every figure quoted here) and by the
//! six tests in this file, which pass and always did.
//!
//! # WHY THE ROW IS REFUSED, ON TWO INDEPENDENT GROUNDS
//!
//! The live row is `index_log::IndexItem` -- a DIFFERENT TYPE WITH THE SAME NAME, which is worth
//! saying out loud because a census by name conflates them (so do `IndexLogRecord` and
//! `WalRecord`, each of which names two types in this crate).
//!
//! GROUND ONE -- `object_id: u32` IS TOO NARROW, AND THE COST IS NOT A BYTE. A durable numeric
//! object id already exists on the live row and is already there: `object_id: u64`, which is
//! `engine::hashing::stable_block_object_id(shard, kind, key)`, an FNV-1a 64. It is durable in the
//! only sense that matters -- a restart recomputes the same number from the same terms -- and the
//! row does not even spend bytes on it, because `strip_object_id_repeat` drops it whenever it
//! agrees with the derivation from the two fields beside it. So this port's premise is not missing
//! from this tree; its WIDTH is. MEASURED: a 32-bit truncation of that hash collides after
//! **296,006 distinct keys**. What that breaks is ownership, not precision --
//! `engine::fold_delta_block_items` files the row's id into `BucketNode::object_index`, a STORED
//! `u64` set whose members are compared for equality against `stable_block_object_id` computed
//! from text at sites all over the engine. A truncation agrees with none of them, and a missing
//! owner does not read as a failure.
//!
//! GROUND TWO -- THE ROW IS WHERE REPLAY GETS THE OBJECT KEY'S CHARACTERS. [`IndexItem`] here
//! carries no characters at all: all seven of its fields encode as protobuf wire type 0, so there
//! is no length-delimited slot a key, an element name or a page handle could ride in.
//! `fold_delta_block_items` reads the live row's `object_key` to rebuild `BlockIndex::object_key`,
//! and `engine::tests::the_entry_cannot_name_its_object_by_slot` establishes that the entry is the
//! ONLY durable home of those characters -- that module's closing sentence names it as the ground
//! it refuses on, the other being a fixable defect. Nothing else stores them, and adding a durable
//! id-to-characters map is the second index the footprint work exists to avoid.
//!
//! > So the field set is reachable only by widening `object_id` to 64 bits AND restoring a
//! > character-bearing field -- at which point it is the live row.
//!
//! # THE FRAMING CLAIM WAS FALSE, AND IT WOULD HAVE LOST DATA SILENTLY
//!
//! This header said "same framing (see [`crate::record_framing`])". The live WAL and served-index
//! log are framed by `crate::log_framing` (crate-private, so no link), not by that module, and the
//! two frames are NOT the same:
//!
//! ```text
//! live (log_framing, binary):  0xB3 | varint64(payload_len) | le_u32(crc32c) | payload
//! this port (record_framing):         varint32(payload_len) | le_u32(crc32c) | payload
//! ```
//!
//! MEASURED: behind the marker byte the two are BYTE-IDENTICAL. The claim is wrong by exactly one
//! byte -- and that byte is the whole mechanism. `log_framing` reads FOUR shapes out of one file
//! and selects among them by the first byte: the `0xB3` binary frame, the `#tsf2` text frame, the
//! `#tsf1` text frame, and a legacy unframed JSON record that ends at a newline. `0xB3` was chosen
//! precisely because no earlier frame can start with it.
//!
//! AND THE CONSEQUENCE IS WORSE THAN A MISMATCH. A frame with no marker is **not refused** by the
//! live reader. `log_framing::next_frame` falls through to the legacy-unframed arm, which treats
//! everything up to the first newline as the payload -- so a record framed by this module is read
//! as a TORN TAIL and DROPPED, which a caller then truncates. Driven: the live reader returns no
//! error at all, with a positive control that it round-trips a live frame exactly and a second
//! control that `record_framing::decode_framed_at` CAN refuse (handed a live frame it reads the
//! marker as the first byte of the length varint and refuses).
//!
//! > A HEADER ASSERTING FRAME COMPATIBILITY WHERE THE LIVE READER SILENTLY DISCARDS THE RECORD IS
//! > THE MOST DANGEROUS KIND OF FALSE CLAIM IN THIS TREE: it tells the next person adoption is
//! > safe at exactly the point where recovery would lose committed data. The framing half IS worth
//! > taking (see below) -- it needs the marker byte and a `varint64` length first.
//!
//! # `in_wal` IS A STEP THIS TREE TOOK AND REVERSED ON PURPOSE
//!
//! This header listed [`IndexItem::in_wal`] as one of three properties making the record
//! load-bearing -- "the flag that makes an address resolvable without a lookup table". THAT IS A
//! REASON TO PORT SOMETHING WE REMOVED DELIBERATELY. The live row has no such field: it computes
//! the flag from the address in its serializer, and `index_log::IndexItem`'s own doc records why,
//! naming `BlockIndex::log_backed()` as "the accessor added when the equivalent stored flag was
//! removed from the RESIDENT entry for exactly this reason". A stored copy of a derivation is the
//! thing the live row has spent several steps shedding; `in_wal` is one of three such slots it
//! stopped carrying as fields.
//!
//! # WHAT IS WORTH TAKING, AND IT TOUCHES NO ROW SHAPE
//!
//! Both of these are semantics the live `index_log::MetaItem` lacks, and neither needs the row to
//! move. They are the adoptable half of this module and are being sequenced separately:
//!
//!   * [`IndexMetaItem::start_wal_id`] is the dump watermark. Replay resumes from it, and WAL
//!     truncation must never pass it -- a record below the watermark has had its blocks
//!     materialised into a slab, one above it has not, and dropping the latter destroys the only
//!     durable copy.
//!   * [`SlabLifecycleInfo`] carries the slab lifecycle. This design pre-allocates a slab in an
//!     INIT state, makes it durable, then creates the stream and moves it to CREATED -- so a crash
//!     between the two leaves a slab that is reused rather than an orphaned stream.
//!
//! The framing half is adoptable too, once corrected: either give [`crate::record_framing`] the
//! marker byte and a `varint64` length, or have it call `log_framing` rather than restate it.
//!
//! # THE CASE AGAINST THE ROW ON THE WIRE, IN TWO SENTENCES
//!
//! MEASURED IN TREE, beside the live encoder: a delta record carrying eight page items is **550
//! bytes positional against 595 as protobuf** -- 8.2% larger, because a tag costs a byte per
//! present field while a position costs nothing. And the live row has banked EIGHT harvests this
//! port expresses none of -- the page handle stripped when derivable (43 B of a 176-B record, its
//! largest single field) and written as a NUMBER when not (20 B of 161, 12.4%, down to about
//! nine); the object id stripped (nine bytes); the size slot an unconditional strip sentinel (4
//! B/row); the model spelling written as a position rather than its name (7 B of 176); the item
//! kind as a number; the address's object-id and routing-bucket repeats gone (18 B of 142, 12.7%);
//! and the object key hoisted onto the record when every item shares it (21 B/item, 24-31% of a
//! timestamped record).
//!
//! > THREE OF THOSE EIGHT ARE BANKED BY WRITING A SENTINEL INTO A SLOT THAT STILL EXISTS, AND A
//! > `prost` FIELD CANNOT DO THAT: it is absent when zero and present otherwise, which is a
//! > different mechanism with a different reader. This port's `size: u32` is a real value, so
//! > adopting it rewrites the length the live row stopped writing.
//!
//! # HOW A NEW ROW SHAPE WOULD EVER LAND -- AND IT IS NOT THE FORMAT STAMP
//!
//! Recorded here because this is where someone looking for a migration path will look.
//!
//! THE INDEX LOG HAS NO VERSION OF ITS OWN. There is no `*_VERSION` constant in `index_log.rs`,
//! and the live row's own doc says the reason: "The index log is POSITIONAL and carries no struct
//! version, so a row whose length does not match the struct decoding it is REFUSED -- and the
//! format stamp cannot help, because it guards the NAMED served index." So `engine::
//! SHARD_INDEX_FORMAT_VERSION` is not the lever, and a row-shape change owes it nothing.
//!
//! THE REAL DISCRIMINATOR IS THE CONTAINER BYTE: `index_log::index_container_byte` packs the
//! payload codec into the high nibble and the record shape into the low one. Two codec ids are
//! taken (`MSGPACK` and `MSGPACK_ZSTD`), so **fourteen are free**, and an unknown codec already
//! returns `IndexLogError::Encoding`, which that enum distinguishes from `Corruption` in as many
//! words: "an unknown codec id (written by a newer binary) ... the bytes arrived intact and this
//! build cannot read them."
//!
//! > THAT IS A READER-FIRST MIGRATION LEVER REQUIRING NO STAMP. Teach the reader a new codec id,
//! > release, then let the writer emit it -- the sequencing the numeric page handle already used,
//! > whose own doc explains that the reverse order "would have been refused outright by msgpack
//! > rather than degrading". For the record, read from the base: values 2, 3, 5, 6, 7, 8, 9, 11, 12
//! > and 13 appear across all history, and 4 and 10 appear nowhere.
//!
//! > CORRECTED, AND THE CORRECTION IS THE USEFUL HALF. This paragraph said "13 is live on four
//! > branches -- so the next free value is 14". It is not, and was read off HISTORY rather than off
//! > the refs: re-measured by reading the constant out of every `matrixark` remote ref, NO TIP
//! > HOLDS 13, and the highest any tip held was 12 -- on `main` and on the three branches that
//! > note was counting. 13 is where the stamp now stands, spent on the index-log row losing its
//! > element-name slot; 11 and 13 are both in history behind reverts, which is the same hazard the
//! > constant's own doc records about 11. The lesson is the one stated above it: a number read out
//! > of `git log` is a number that was true once, and the refs are the thing to read.

use prost::Message;

/// Record format version, mirroring the on-disk index-log version.
pub const INDEX_LOG_RECORD_VERSION: u32 = 1;

/// One served-index log record.
///
/// Field numbers match this design one-for-one, including the gap at 2 where a since-deprecated
/// item-type discriminator sat. The gap is preserved deliberately: reusing the tag would make the
/// two encodings disagree while still parsing, which is worse than a hole.
#[derive(Clone, PartialEq, Message)]
pub struct IndexLogRecord {
    /// On-disk version.
    #[prost(uint32, tag = "1")]
    pub version: u32,
    // Tag 2 is retired. Do not reuse.
    /// Fixed-width.
    #[prost(fixed64, tag = "3")]
    pub routing_bucket: u64,
    /// Block-location entries.
    #[prost(message, repeated, tag = "4")]
    pub items: Vec<IndexItem>,
    /// Present on a meta record.
    #[prost(message, optional, tag = "5")]
    pub meta_item: Option<IndexMetaItem>,
    /// Every object in this routing bucket, on a meta record.
    #[prost(message, repeated, tag = "6")]
    pub object_items: Vec<IndexObjectItem>,
    #[prost(uint64, tag = "7")]
    pub sequence: u64,
    /// The WAL sequence this index state reflects.
    #[prost(uint64, tag = "8")]
    pub wal_sequence: u64,
    #[prost(uint64, tag = "9")]
    pub timestamp_ms: u64,
}

/// Where one block lives.
#[derive(Clone, PartialEq, Message)]
pub struct IndexItem {
    #[prost(uint32, tag = "1")]
    pub object_id: u32,
    #[prost(uint32, tag = "2")]
    pub block_id: u32,
    /// The block's address. When [`Self::in_wal`] is set this is the log id — the byte offset of
    /// the WAL record carrying the block — otherwise it addresses a slab.
    #[prost(uint64, tag = "3")]
    pub address: u64,
    #[prost(uint32, tag = "4")]
    pub size: u32,
    /// The block still lives in the WAL and has not been dumped into a slab yet.
    #[prost(bool, tag = "5")]
    pub in_wal: bool,
    #[prost(bool, tag = "6")]
    pub deleted: bool,
    #[prost(uint32, tag = "7")]
    pub model_id: u32,
}

/// Slab lifecycle state. This design calls a slab a zone.
///
/// Numbering matches this design exactly, including that RECYCLED is 4 and 3 is unused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SlabState {
    /// Reserved and durable, but its stream does not exist yet.
    Init = 0,
    /// Stream created and accepting writes.
    Created = 1,
    /// Sealed; no further writes.
    Frozen = 2,
    // 3 is unused in this design.
    /// Reclaimed and available for reuse.
    Recycled = 4,
}

/// One slab's lifecycle record.
#[derive(Clone, PartialEq, Message)]
pub struct SlabLifecycleInfo {
    #[prost(uint32, tag = "1")]
    pub slab_id: u32,
    #[prost(uint64, tag = "2")]
    pub total_bytes: u64,
    #[prost(enumeration = "SlabState", tag = "3")]
    pub state: i32,
    #[prost(uint64, tag = "4")]
    pub init_time_ms: u64,
    #[prost(uint64, tag = "5")]
    pub created_time_ms: u64,
    #[prost(uint64, tag = "6")]
    pub frozen_time_ms: u64,
