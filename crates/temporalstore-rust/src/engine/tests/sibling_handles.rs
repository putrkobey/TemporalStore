// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TWO ENTRIES AT ONE ADDRESS DO SHARE A HANDLE, AND THAT IS WHY ONE ENTRY A PAGE IS REQUIRED.
//!
//! # THE MODULE'S PREMISE WAS DISCHARGED, SO ITS CLAIM IS INVERTED RATHER THAN DELETED
//!
//! This module used to assert the opposite: "TWO ELEMENTS OF ONE COMPACTED PAGE MUST NOT SHARE A
//! HANDLE". `block_index_handle` hashed model id, object key, **component** and five address
//! terms, and siblings on a batched page agree on everything except the component -- so the
//! component was the only discriminator, and dropping it collapsed forty hash fields on one page
//! onto one entry with thirty-nine lost. Its own header quoted `index_log.rs` on the condition:
//! the component *"cannot go until the entry stops being per-element."*
//!
//! THE ENTRY HAS STOPPED BEING PER-ELEMENT. `index_entry_names_a_block` answers true for all four
//! container kinds, so the projection emits ONE entry per distinct physical page, and
//! `BlockIndex` no longer has a `component` field at all. The state this module pinned against --
//! two entries of one object at one address -- is not something the index can hold any more.
//!
//! # SO WHAT IS ASSERTED NOW, AND WHY IT IS NOT THE SAME TEST WITH A FLIPPED SIGN
//!
//! A test that said "siblings get distinct handles" would now be a test of a state nothing
//! produces, and one that said "siblings share a handle" with nothing else would be a curiosity.
//! The pair below is what makes the collision MEAN something:
//!
//!   1. THE COLLISION IS REAL. Two entries of one object at one address hash to ONE handle, so a
//!      second insert takes the first one's slot. That is asserted directly, because it is the
//!      reason one entry a page is a correctness requirement and not a matter of footprint --
//!      and because an assertion that it does NOT collide is what this module would have to
//!      become if the handle ever gained a discriminator again.
//!   2. NOTHING FILES THE COLLIDING STATE. Every container kind converges on the page, asked of
//!      `index_entry_names_a_block` itself rather than of a comment, so no path files two entries
//!      of one object at one address for the collision to be reached through.
//!
//! Either one alone is weak: (1) without (2) describes a live defect, and (2) without (1) is a
//! predicate check with no stated consequence. Together they say why the predicate matters.
//!
//! # THE MIRROR IS GONE WITH THE TERM IT ISOLATED
//!
//! A mirror of `block_index_handle` stood here with an `include_component` switch, proved faithful
//! against the real function and then re-run with the component dropped, to show that term was the
//! only discriminator. There is no such term to drop: `block_index_handle` hashes the constant
//! `None::<&str>` in that position -- which is what keeps every handle bit-for-bit what it was --
//! and a constant cannot be isolated by omitting it. The real function is used directly below.

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::state::{block_index_handle, BlockIndex};
use crate::engine::storage_bucket_internals::StoredModelKind;

/// One batched page: every element of the object shares this address.
fn batched_block_address() -> ElementEntry {
    ElementEntry::from_parts(42, 1_048_576, 4096, Some(7), None)
}

/// An entry for one object at one address. There is no element name to vary any more, which is
/// the whole of this module's restatement: two of these differ in nothing at all.
fn entry_at(object_key: &str, address: ElementEntry) -> (std::sync::Arc<str>, BlockIndex) {
    (
        std::sync::Arc::from(object_key),
        BlockIndex {
            model_id: StoredModelKind::Hash,
            address,
            dirty: false,
            deleted: false,
