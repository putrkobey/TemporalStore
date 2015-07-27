// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE HANDLE MOVED UP A LEVEL AND DID NOT CHANGE VALUE.
//!
//! `block_index_handle` used to be called from inside `BlockIndexMap::insert_unaccounted`: from a
//! scope holding the entry and nothing else. A map cannot see the sibling fields of the bucket that
//! owns it, and serde's `serialize(&self)` cannot be handed one either, so the written key was in
//! the same position. That pinned every term the handle hashes to the entry -- a term moved off it
//! into a per-bucket structure would have become unreachable from the one line that needs it.
//!
//! It is computed at [`BucketNode::insert_block`] now, where the bucket's own structures are in
//! scope, and the written key is composed through `BlockIndexAt`, which holds the whole node.
//!
//! # WHY VALUE EQUALITY IS THE WHOLE SAFETY ARGUMENT
//!
//! Handles are written to disk inside the lookup's refs. Two processes holding the same block must
//! compute the same handle or those refs point at nothing -- which is what a counter did, silently,
//! until a reload lost an object. So this change is only safe if it moved WHERE and not WHAT: same
//! inputs, same order, same hasher, byte-identical `u64`. That is driven here rather than reasoned
//! about, and driven with a negative control, because an equality between two expressions that
//! happen to be the same expression proves nothing.
//!
//! # AND THE FLOOR THAT IS EASY TO OMIT
//!
//! `BlockIndexMap` has an inline arm and a list arm, and the handle is assigned by different code
//! on each. A fixture reaching only one arm is a handle compared once, so both are asserted
//! reached before any equality below is read as general.
//!
//! rust-internal: drives this crate's own handle derivation, no external surface

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::state::{
    block_index_handle, BlockIndex, BlockIndexMap, BlockSlabLiveIndex, BucketNode,
};
use crate::engine::storage_bucket_internals::stored_model_kind;
use std::sync::Arc;

fn block(object: &str, component: Option<&str>, slab: u64, offset: u32, length: u32) -> BlockIndex {
    BlockIndex {
        model_id: stored_model_kind("hash"),
        address: ElementEntry::from_parts(slab, offset as u64, length as u64, Some(7), Some(11)),
        dirty: false,
        deleted: false,
    }
}

/// The entries driven, EACH WITH ITS OBJECT KEY BESIDE IT RATHER THAN ON IT.
///
/// The pair is the shape every production site now uses: `block_index_handle` and
/// `BucketNode::insert_block` both take the identity as a PARAMETER, because the entry is losing
/// its own copy. A fixture handing back bare entries would have to read the key off the page to
/// call either, which is the one thing this change is removing -- so it hands back what the
/// callers need.
///
/// Two share an object and differ by component; the rest differ by address.
fn fixture() -> Vec<(Arc<str>, BlockIndex)> {
    vec![
        (Arc::from("tenant/1/obj/aaa"), block("tenant/1/obj/aaa", Some("f-1"), 7, 0, 64)),
        (Arc::from("tenant/1/obj/aaa"), block("tenant/1/obj/aaa", Some("f-2"), 7, 64, 64)),
        (Arc::from("tenant/1/obj/bbb"), block("tenant/1/obj/bbb", None, 9, 128, 32)),
        (Arc::from("tenant/1/obj/ccc"), block("tenant/1/obj/ccc", Some("f-1"), 9, 160, 48)),
    ]
}

#[test]
fn the_hoisted_handle_is_byte_identical_to_the_one_the_map_used_to_compute() {
    let entries = fixture();
    assert!(entries.len() >= 4, "the fixture must drive more than one entry");

    // --- THE SUBJECT: install through the bucket-level door and compare the handle it returns
    //     against `block_index_handle` read directly off the same entry. ---
    let mut bucket = BucketNode::default();
    let mut live = BlockSlabLiveIndex::default();
    let mut reached_inline = false;
