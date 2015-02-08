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
