// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MAKES THE TOMBSTONE CODEPOINT FREE, AND WHAT MAKES DROPPING ONE UNSAFE.
//!
//! Four claims that the rest of this change rests on, each driven here rather than argued in a
//! comment. They are grouped because they are the same subject seen from two ends: the first two are
//! why the second page shape costs nothing, and the second two are why a tombstone cannot be
//! collected whenever it is convenient.
//!
//! # 1. AN EMPTY VALUE IS NOT A TOMBSTONE, AND THE BYTES PROVE IT
//!
//! The whole reason a removal costs no per-item flag byte is that the value tag had an unused
//! codepoint: an empty INLINE value would be tag 1, and the encoder never emits it, because
//! `suffix_offset_of(key, b"")` always answers `Some(key.len())` and takes the suffix branch. If that
//! were ever untrue, a tombstone and an element holding the empty string would be the SAME BYTES, and
//! every empty-valued hash field in the store would decode as a removal. So the assertion is on the
//! bytes and not on the round trip: the two encodings must DIFFER.
//!
//! # 2. AND A LIVE SUFFIX PAGE IS THE SAME BYTES IN BOTH SHAPES
//!
//! Asserted by rewriting the magic in place and decoding the result as the first shape. That is a
//! stronger statement than comparing two encoders' lengths, and it is the one that matters for a
//! store being upgraded: the pages a `set` and a `zset` write are byte-identical across the change
//! apart from one character of magic.
//!
//! # 3. THE ORDER IS THE APPEND POSITION, AND THE FOLD HONOURS IT
//!
//! Fed the same pages in the wrong order, the derivation must give the same answer -- it sorts by
//! append position rather than trusting its input. A derivation that happened to be handed pages in
//! order would pass without sorting at all, so the input here is deliberately reversed.
//!
//! # 4. DROPPING A TOMBSTONE WHILE AN OLDER PAGE SURVIVES RESURRECTS ITS ELEMENT
//!
//! This is the trap, and it is driven as a resurrection that actually happens rather than as a rule
//! that forbids it. The naive collection rule -- "the fold rewrote these elements, so their
//! tombstones are spent" -- is applied, the element comes back, and THEN the rule is asked and
//! declines the same round. A guard that only asserted the rule's return value would pass against a
//! rule that was wrong.

#![allow(clippy::all)]
use crate::block_store::ElementEntry;
use crate::engine::container_membership::{append_position, derive_membership, may_drop_tombstones};
use crate::engine::container_blocks::{
    decode_container_block, encode_container_block_items, ContainerBlockDecode, ContainerBlockShape,
    ContainerBlockWrite, ElementKeySpelling, CONTAINER_BLOCK_MAGIC, CONTAINER_BLOCK_MAGIC_V2,
};

/// An address at a chosen append position, which is what orders pages.
///
/// The block id and object id are set because a real page carries both, and a fixture that left them
/// absent would be exercising a shape the write path does not produce. Neither participates in the
/// ordering, which is the point being driven.
fn at(slab: u64, offset: u64, len: u32) -> ElementEntry {
    ElementEntry::from_parts(slab, offset, u64::from(len), Some(0), Some(7))
}

/// rust-internal: operates on the page codec directly
#[test]
fn the_two_shapes_put_their_spelling_byte_at_one_offset() {
    // EVERY CURSOR IN THE CODEC STARTS AT `MAGIC.len() + 1` and there are two magics. If they were
    // ever different lengths, one shape would be read one byte into the wrong field -- and the symptom
    // would be a spelling byte that happens to be a valid spelling, so the page would decode to
    // plausible nonsense rather than refusing. `container_blocks` carries a const assert on this; this
    // states it where a reader looking for the guard will find it.
    assert_eq!(
        CONTAINER_BLOCK_MAGIC.len(),
        CONTAINER_BLOCK_MAGIC_V2.len(),
        "the two magics are different lengths, so the spelling byte is at two offsets"
    );
    assert_ne!(
        CONTAINER_BLOCK_MAGIC, CONTAINER_BLOCK_MAGIC_V2,
        "the two magics are equal, so nothing DISCRIMINATES the shapes and a tombstone in a new page \
         would be read by the old rule as an empty live value"
    );
    // And they differ in exactly one byte, which is what makes the rewrite in
    // `a_live_suffix_block_is_the_same_bytes_under_either_shape` a magic swap and not a reshaping.
    let differing = CONTAINER_BLOCK_MAGIC
        .iter()
        .zip(CONTAINER_BLOCK_MAGIC_V2.iter())
        .filter(|(left, right)| left != right)
        .count();
    println!(
        "magics {:?} and {:?} differ in {differing} byte(s)",
        String::from_utf8_lossy(CONTAINER_BLOCK_MAGIC),
        String::from_utf8_lossy(CONTAINER_BLOCK_MAGIC_V2)
    );
    assert_eq!(1, differing, "the two magics differ in {differing} bytes, not one");
}

/// rust-internal: operates on the page codec directly
#[test]
fn a_removal_that_cannot_be_framed_is_counted_and_writes_no_block() {
    crate::engine::container_blocks::reset_unframed_container_removal_count();

    // A kind with no element-key spelling: its page is its whole object, so there is no element to
    // tombstone. `string` is such a kind and is not one of the four containers.
    let none = crate::engine::container_blocks::tombstone_block("string", "anything");
    assert!(
        none.is_none(),
        "a kind whose pages name no element produced a tombstone page, which would be a page nothing \
         can interpret"
    );
    // A component the kind's spelling cannot produce: a set's components are hex, and this is not.
    let not_hex = crate::engine::container_blocks::tombstone_block("set", "zz-not-hex");
    assert!(
        not_hex.is_none(),
        "a component a set's spelling cannot name produced a tombstone page"
    );

    let counted = crate::engine::container_blocks::unframed_container_removal_count();
    println!("unframable removals counted: {counted}");
    assert_eq!(
        2, counted,
        "both unframable removals must be COUNTED and not silently dropped -- a removal the pages do \
