// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THIRD STRUCTURE ON THE OBJECT SIDE OF A BUCKET, AND WHY IT CANNOT BECOME THE SECOND.
//!
//! `BucketNode` carries three containers: `object_index` (16 B), `deleted_object_index` (8 B) and
//! `block_index` (24 B). A design that encoded deletion IN THE PAGE ENTRY would carry only the
//! first and the last, and `deleted_object_index` sits INSIDE the eight-aligned group, so removing
//! it moves the node BY ITSELF -- 80 B of eight-aligned field becomes 72, and the node goes
//! 88 -> 80. That is the cheapest arithmetic on this struct: one field, one step, no side
//! structure and no relocation. Which is exactly why it is worth establishing whether the
//! structure is needed at all, rather than assuming the width already settled it. #1961 took this
//! field from 16 B to 8 by making absence cost a pointer; that priced its WIDTH and said nothing
//! about whether the set has to exist.
//!
//! IT HAS TO EXIST, AND THE REASON IS A LIFETIME RATHER THAN A COUNT.
//!
//! A per-page `deleted` bit cannot carry this, and NOT because it is missing -- `BlockIndex.deleted`
//! is already a field of the page entry, so the per-page half of deletion is already recorded. It
//! cannot carry it because BY THE TIME THE DELETION MATTERS THERE IS NO PAGE LEFT TO CARRY IT. The
//! delete path retires the pages and keeps the id:
//!
//! ```text
//!   engine.rs, delete_object_key_from_bucket_index:
//!     bucket.block_index.retain(.., |_, page| { .. false })   <- the pages are REMOVED
//!     bucket.object_index.extend(deleted_object_ids)          <- the id STAYS
//!     bucket.deleted_object_index.extend(deleted_object_ids)  <- the tombstone is written
//! ```
//!
//! So a deleted object is, by construction, an id in `object_index` with NO entry in `block_index`.
//! `storage_bucket_internals`'s own note on `settle_released_bucket_object_delete` states the
//! invariant from the other side: "The resident path keeps the id and records it in
//! `deleted_object_index` BECAUSE THE ID STAYS; here the id goes, and a tombstone for an absent id
//! is an entry nothing would ever read."
//!
//! AND THE READER IS NAMED. `object_manager::runtime_report` walks `bucket.object_index` and asks
//! `bucket.deleted() || bucket.deleted_object_index.contains(object_id)`. For an object whose pages
//! are gone, `page.deleted` is not reachable -- there is no page -- and `bucket.deleted()` is set
//! from `bucket.block_index.is_empty()`, so it is FALSE for any bucket still holding another
//! object's pages. In that state the tombstone set is the ONLY thing that can answer, and the
//! figure it answers is published: `delete_marker_object_count`, which
//! `object_manager_runtime_report` fills from it and `native_persistence_workflow` and
//! `recovery_sweep_compact` both reach.
//!
//! A ZERO-LENGTH SENTINEL IS UNAVAILABLE FOR THE SAME REASON, which is worth stating because it is
//! the obvious alternative and the obvious refutation of it is the wrong one. Whether a zero-length
//! block is legitimate in this format does not arise: the sentinel needs a page to sit in, and the
//! delete has removed the page from `block_index` entirely. The question is not whether the
//! encoding has room for a marker, it is whether there is anything left to mark.
//!
//! THE POPULATION, MEASURED HERE, AND THE NUMBER THAT DECIDES IT IS NOT THE BIG ONE. At 4,000
//! records on `0..1023` with a quarter of the keys deleted, 704 of 1,012 buckets (69.57%) carry a
//! tombstoned id that no page carries, over 1,000 such ids. But only 19 of those buckets hold the
//! orphaned id in `object_index`, and those nineteen are the whole of the load-bearing population:
//! `delete_marker_object_count` reads 19, and removing ONE tombstone takes it to 18.
//!
//! A SECOND READING, REPORTED BECAUSE IT IS NOT WHAT THE FIELD'S OWN NOTE IMPLIES. 981 of those
//! 1,000 tombstoned ids are reachable by NEITHER reader: the id is not in `object_index`, so the
//! walk that consults the tombstone never reaches it, and no page carries it either. On this
//! corpus 98.1% of the entries in this field are never read by anything. That is NOT a second
//! argument for removing the structure -- the nineteen that ARE read cannot be answered any other
//! way, which is what item 2 drives -- but it says the field is carrying far more than it is asked
//! about, and the shape of that excess (a rebuild re-attaches the tombstone set while recomputing
//! `object_index` from live pages only, so the two drift apart) is worth its own look. This module
//! reports the figure and changes nothing.
//!
//! WHAT A PRECONDITION PROXY COST, recorded because it is the failure that reads as a refutation.
//! A first version of item 2 required the chosen bucket to still hold OTHER pages, as a proxy for
//! "whole-bucket `deleted()` is not what answers". It found NOTHING, and a bare `None` from a
//! search is indistinguishable from the state not existing. Counting each clause separately said
//! why in one line: the buckets that hold the orphaned id in `object_index` are exactly the ones
//! whose page list is now EMPTY, so the proxy excluded the entire population it was meant to
//! select. The empty-page case is the STRONGER one -- there is no page at all for a per-page bit to
//! sit in -- and `deleted()` is asserted false on it directly, which is the clause that was
//! actually wanted. Every clause is counted and printed for that reason.
//!
//! The other half of the field's economics is already measured elsewhere and is not restated here:
//! `what_the_object_side_of_the_bucket_node_costs` reports that 97.68% of buckets carry no
//! tombstone at all, which is what makes the eight-byte nullable-pointer shape the right WIDTH and
//! is not an argument that the set is unused.
//!
//! WHAT IS HERE:
//!
//!   1. `what_removing_the_tombstone_index_would_make_the_node` -- the arithmetic, per field, with
//!      every width read FROM THE FIELD via `field_width` and every offset from `offset_of!`, the
//!      live mirror as the control, and the reconstruction asserted rather than the total. 88 -> 80.
//!   2. `a_tombstone_outlives_every_block_that_could_have_carried_it` -- the refutation, DRIVEN. It
//!      finds a bucket in the state above, ENFORCES that the state is the one described (id held,
//!      no page carrying it, bucket not itself deleted), then PERTURBS the tombstone away and
//!      asserts the published count falls by exactly one. The control is the unperturbed report on
//!      the same shard, so a count that was already wrong cannot read as the edit's doing.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It proposes no change. `deleted_object_index` stays, at eight
//! bytes, on every routing bucket, and the eight bytes the node would have saved are not available.

#![allow(clippy::all)]
use super::*;

use crate::engine::state::{
    BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};
use std::mem::{align_of, offset_of, size_of};

const TOMB_SHARD: ShardId = 1;

/// The documented production routing range, `TS_SHARD_END_ROUTING_BUCKET=1023`.
const CONFIGURED_END_BUCKET: u32 = 1023;

// -----------------------------------------------------------------------------------------------
// 1. THE ARITHMETIC: WHAT THE FIELD IS WORTH, RECONSTRUCTED RATHER THAN QUOTED.
// -----------------------------------------------------------------------------------------------

/// The node as declared. THE CONTROL: if this is not `size_of::<BucketNode>()` then every row
/// below describes a structure this engine does not have.
#[allow(dead_code)]
struct TombMirrorLive {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES stay the prices: a mirror is the live
    // declaration plus ONE historical difference, and the live node gained this word
    // when a removal's element name left the page entries.
    tombstone_elements: crate::engine::state::TombstoneElements,
}

/// The tombstone set gone. The bytes LEAVE the structure rather than moving to the tail, which is
/// why one field crosses the step where a narrowing of the same width would not.
#[allow(dead_code)]
struct TombMirrorNoTombstones {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES stay the prices: a mirror is the live
    // declaration plus ONE historical difference, and the live node gained this word
    // when a removal's element name left the page entries.
    tombstone_elements: crate::engine::state::TombstoneElements,
}

/// The same eight bytes NARROWED instead of removed, as the control on the claim that removal is
/// what crosses. `DeletedObjectIndex` is already one nullable pointer, so there is nothing to
/// narrow it to that is smaller than a `u32` handle -- and a `u32` here lands in the tail, takes it
/// from 6 to 10, and rounds back to 16. The group gives up a word and hands it straight back.
#[allow(dead_code)]
struct TombMirrorNarrowedToHandle {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index_handle: u32,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES stay the prices: a mirror is the live
    // declaration plus ONE historical difference, and the live node gained this word
    // when a removal's element name left the page entries.
    tombstone_elements: crate::engine::state::TombstoneElements,
}

/// WHAT REMOVING THE TOMBSTONE SET WOULD MAKE THE NODE, and the control that says removing is not
/// the same shape of change as narrowing.
///
/// Every width is read FROM THE FIELD through `field_width`, which infers `T` from the field it is
/// handed: a hand-written table naming the types at the call site still compiles when a field stops
/// being what the table says, and rustc says nothing. Every offset comes from `offset_of!`, because
/// `repr(Rust)` orders by alignment and not by declaration, so the declaration order below is a
/// convention and not a layout.
///
/// THE RECONSTRUCTION IS ASSERTED, NOT THE TOTAL: `eight_aligned + round_up(tail) == size_of` has
/// to hold on every row, so a row that happens to land on the right number for the wrong reason
/// fails.
#[test]
fn what_removing_the_tombstone_index_would_make_the_node() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<TombMirrorLive>(),
        "the mirror of the live declaration is {} B against the declaration's {}; the mirrors have \
         drifted and every price below is fiction",
        size_of::<TombMirrorLive>(),
        size_of::<BucketNode>()
    );

    let align = align_of::<BucketNode>();
    assert_eq!(
        8, align,
        "BucketNode's alignment is {align} and the arithmetic below assumes 8"
    );

    fn field_width<T>(_field: &T) -> usize {
        size_of::<T>()
    }
    let sample = BucketNode::default();
    let fields = [
        (
            "routing_bucket",
            field_width(&sample.routing_bucket),
            offset_of!(BucketNode, routing_bucket),
        ),
        ("layout", field_width(&sample.layout), offset_of!(BucketNode, layout)),
        ("flags", field_width(&sample.flags), offset_of!(BucketNode, flags)),
        ("ttl_ms", field_width(&sample.ttl_ms), offset_of!(BucketNode, ttl_ms)),
        (
            "dirty_generation",
            field_width(&sample.dirty_generation),
            offset_of!(BucketNode, dirty_generation),
        ),
        (
            "first_dirty_wal_sequence",
            field_width(&sample.first_dirty_wal_sequence),
            offset_of!(BucketNode, first_dirty_wal_sequence),
        ),
        (
            "first_dirty_index_log_sequence",
            field_width(&sample.first_dirty_index_log_sequence),
            offset_of!(BucketNode, first_dirty_index_log_sequence),
        ),
        (
            "object_index",
            field_width(&sample.object_index),
            offset_of!(BucketNode, object_index),
        ),
        (
