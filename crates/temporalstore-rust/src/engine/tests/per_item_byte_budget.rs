// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A BYTE BUDGET for the structures that exist once per stored item.
//!
//! Most of this engine reaches for a 64-bit integer by default and that is the right default: a
//! sequence number, a hash, a timestamp and a byte count all want the full width, and a structure
//! that exists once per shard can be as fat as it likes. `ShardState` is 1,888 bytes and there is
//! one of it. What is different about the handful of structures below is that their count is the
//! CORPUS: one per stored address, one per page-index entry, one per routing bucket. At those
//! counts a byte is a megabyte, and nothing in this tree stopped one of them growing.
//!
//! WHAT THIS MODULE IS FOR. Two things, and the second is the durable one:
//!
//!   * `what_the_per_item_structures_cost_at_two_corpus_sizes` measures `size_of` x COUNT at two
//!     corpus sizes ten times apart, so a structure is ranked by what it actually costs the store
//!     rather than by how wide it looks. A fat struct with one instance is not a finding.
//!   * `every_per_item_structure_states_its_width_and_its_padding` pins each width against a
//!     literal and reports the FIELD SUM beside it. The literals are the budget. A field added to
//!     any of these structures fails this test by name, which is the thing that did not exist
//!     before: the widths below have moved several times and nothing ever failed.
//!
//! WIDTH IS NOT THE SAME PROBLEM AS PADDING, and the two want opposite fixes. Rust reorders
//! fields, so a structure whose size exceeds its field sum is wasting ALIGNMENT, and narrowing a
//! field in it changes nothing at all -- `BlockLookupRef` is 12 bytes of field in 16 bytes of
//! struct and no narrowing of either field moves it. A structure whose fields fill it is wasting
//! WIDTH, and narrowing one helps only if the sum crosses an 8-byte boundary. `BlockAddress` is
//! the only candidate here where it does, and only when TWO fields narrow together: one alone
//! takes 53 bytes of field to 49 and leaves the struct at 56.
//!
//! THERE IS A THIRD THING A FIELD CAN BE WASTING, and `BucketNode` was. An `Option<u64>` is 16
//! bytes for 8 bytes of number, because a `u64` has no value it does not use and the
//! discriminant has nowhere to go but a word of its own. That is neither width nor alignment:
//! the number is exactly as wide as it needs to be and the struct has no slack to reclaim. It is
//! a DISCRIMINANT with no niche to sit in, and the fix is to make one -- `Option<NonZeroU64>` is
//! 8. `every_byte_of_the_bucket_node_is_accounted_for` states the whole of that structure field
//! by field, `what_each_declined_shape_of_the_bucket_node_would_cost` prices the three shapes
//! that were considered for it and declined, and
//! `the_stored_spelling_of_a_bucket_node_did_not_move` drives the constraint that decides which
//! of them can be taken at all.
//!
//! THE COUNTS ARE MEASURED, NOT ASSUMED. Every denominator below is read off a seeded shard and
//! asserted non-zero before anything is divided by it, because a walk over an empty shard reports
//! "0 bytes over 0 items", which reads exactly like a structure that costs nothing.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::{ElementEntry, BlockStoreSlabDescriptor};
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs, BucketFlags, BucketLayoutState, BucketNode,
    BucketTtl,
    DeletedObjectIndex, DirtyKeySet, ModelKey, ObjectIndex,
    WalResidentBlock,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;
use crate::index_log::{IndexItem, IndexItemKind, SlabCatalogEntry};

/// One structure in the budget: what it costs, and what its own fields add up to.
///
/// `fields` is written out as a sum of `size_of` over the DECLARED field types rather than as a
/// number, so it is a statement about the declaration and moves when the declaration does.
struct Budgeted {
    name: &'static str,
    size: usize,
    align: usize,
    /// For a struct, the sum of its field widths. For an ENUM, the width of its widest arm --
    /// so the slack reported beside it is the discriminant plus any alignment, not padding
    /// alone. `ObjectIndex` reads as 8 bytes of arm in 16 for exactly that reason, and it is not
    /// waste that a narrower field could reclaim.
    fields: usize,
    /// Whether the count of this structure grows with the corpus. A `false` row is a CONTROL:
    /// it is here to be ranked last, and it is the reason "rank by width" is the wrong ranking.
    per_item: bool,
}

impl Budgeted {
    fn padding(&self) -> usize {
        self.size.saturating_sub(self.fields)
    }
}

/// THE BUDGET. Each `size` literal is the ceiling; each `fields` expression is what the
/// declaration adds up to today.
fn budget() -> Vec<Budgeted> {
    let arc_str = size_of::<Arc<str>>();
    let opt_u64 = size_of::<Option<u64>>();
    let string = size_of::<String>();
    vec![
        Budgeted {
            name: "ElementEntry",
            size: size_of::<ElementEntry>(),
            align: align_of::<ElementEntry>(),
            // address               : u64  (slab id in the high 32 bits, offset in the low 32)
            // length                : u32
            // block_id              : u16
            // present               : u8
            //
            // `object_id` was a second u64 here until it became DERIVED from the terms beside the
            // address -- the shard, the stored model spelling and the object key -- so it is not a
            // field any more and not a row here. It was a WHOLE eight-byte field in the eight-aligned
            // group, which is why shedding it alone paid where no single NARROWING here ever has:
            // payload 23 -> 15, struct 24 -> 16.
            //
            // `generation` was a third u64 here until it became derived from `block_id.or(object_id)`;
            // it is now the block id alone, and still not a row.
            // `block_slab_id` and `offset` were two more u64s until they became the two halves of
            // `address` -- two rows became one for the same reason, and by the same eight bytes.
            // `routing_bucket` was a third u32 until it stopped being held at all: a page's bucket
            // is `block_routing_bucket(object_key, ..)` over the range the store is stamped with,
            // so the container a page is read through answers it. `block_id` narrowed from 32 bits
            // to 16 in the same change, and NEITHER of those is worth anything on its own -- four
            // bytes off 29 is 25 and two off 29 is 27, both of which round back to 32. Together
            // they shed six and the struct crosses to 24.
            fields: size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u8>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndex",
            size: size_of::<BlockIndex>(),
            align: align_of::<BlockIndex>(),
            // model_id             : StoredModelKind, ONE BYTE -- it was a second `Arc<str>`
            //                        until the spelling became the one-byte discriminant of the
            //                        seventeen-element set `model_kind_registry` declares
            // address              : BlockAddress
            // dirty/deleted       : bool x 2
            //
            // `component: Option<Arc<str>>` STOOD BETWEEN `model_id` AND `address` AND WENT ONE LEG
            // AGO: sixteen bytes, the whole of this row's 56 -> 40. `object_key: Arc<str>` goes
            // here: another sixteen, and the whole of 40 -> 24.
            //
            // AND THREE TERMS CAME OUT THAT HAD NO FIELD BEHIND THEM AT ALL. This list carried
            // `kind: IndexItemKind` and `routing_bucket: u32` after `65c7fc562` retired both from
            // `BlockIndex`, and the row's own impossibility check -- fields must fit inside the
            // struct -- did not fire, because 40 bytes of claimed field fit inside a 40-byte
            // struct exactly. It fires the moment the width moves, which is how this was found:
            // the sixth live instance of a decomposition that had stopped describing the type.
            fields: size_of::<crate::engine::storage_bucket_internals::StoredModelKind>()
                + size_of::<ElementEntry>()
                + 2 * size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndexMap",
            size: size_of::<BlockIndexMap>(),
            align: align_of::<BlockIndexMap>(),
            // WIDEST ARM: `Many(Vec<(u64, BlockIndex)>)` -- the page list's header, and nothing
            // wider. The single-page arm holds a handle and a BOX, which is sixteen bytes, and both
            // it and `Empty` ride pointer niches so neither adds a discriminant word.
            //
            // IT USED TO BE `u64 + BlockIndex`, the whole entry held inline, and that is what made
            // this the widest field of `BucketNode`. Leaving that spelling here after the arm was
            // boxed did not fail as a width -- it failed as an IMPOSSIBILITY: the row's fields added
            // up to 80 inside a 24-byte struct, which is the check three lines of arithmetic below
            // and the reason it exists rather than a plain width pin.
            fields: size_of::<Vec<(u64, BlockIndex)>>(),
            per_item: true,
        },
        Budgeted {
            name: "BucketNode",
            size: size_of::<BucketNode>(),
            align: align_of::<BucketNode>(),
            // routing_bucket u32, layout, the packed flag byte, ttl_ms BucketTtl, THREE u64
            // sequences, the live object index, the tombstone index, one BlockIndexMap, and the
            // ELEVENTH FIELD -- the tombstone element rows, one word.
            //
            // THE ELEVENTH WAS MISSING HERE WHILE THE WIDTH PIN BESIDE IT WAS CORRECT, which is the
            // shape this crate has a recorded case of: this row's own `slack` column printed 10 for
            // a structure whose real slack is 2, and it PASSED, because the arm asserts the WIDTH
            // and prints the field sum. A field set that is short by a word and a slack that is
            // long by a word add up to the same total.
            fields: size_of::<u32>()
                + size_of::<BucketLayoutState>()
                + size_of::<BucketFlags>()
                + size_of::<BucketTtl>()
                + 3 * size_of::<u64>()
                + size_of::<ObjectIndex>()
                + size_of::<DeletedObjectIndex>()
                + size_of::<BlockIndexMap>()
                + size_of::<TombstoneElements>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockLookupRef",
            size: size_of::<BlockLookupRef>(),
            align: align_of::<BlockLookupRef>(),
            fields: size_of::<u32>() + size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockRefs",
            size: size_of::<BlockRefs>(),
            align: align_of::<BlockRefs>(),
            // Widest arm: Many(Vec<BlockLookupRef>).
            fields: size_of::<Vec<BlockLookupRef>>(),
            per_item: true,
        },
        // `ComponentBlocks`, `ComponentList` AND `ObjectBlockRefs` HAD ROWS HERE, ALL THREE AT 40.
        //
        // They were the three wrappers between an object and its block refs, and all three are
        // deleted: the lookup now holds a `BlockRefs` per object directly, which is the row above.
        // `ComponentBlocks` was the one row in this list that still had an `opt_arc_str` term --
        // the always-`None` element name -- which is why that binding is gone from the top of this
        // function with it.
        Budgeted {
            name: "ObjectIndex",
            size: size_of::<ObjectIndex>(),
            align: align_of::<ObjectIndex>(),
            // Widest arm: Many(Box<BTreeSet<u64>>), which is one pointer.
            fields: size_of::<Box<std::collections::BTreeSet<u64>>>(),
            per_item: true,
        },
        Budgeted {
            name: "DirtyKeySet",
            size: size_of::<DirtyKeySet>(),
            align: align_of::<DirtyKeySet>(),
            // Widest arm: One(Arc<str>).
            fields: arc_str,
            per_item: true,
        },
        Budgeted {
            name: "WalResidentBlock",
            size: size_of::<WalResidentBlock>(),
            align: align_of::<WalResidentBlock>(),
            fields: 2 * size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "IndexItem",
            size: size_of::<IndexItem>(),
            align: align_of::<IndexItem>(),
            // kind, routing_bucket, TWO String handles (the composite page handle and the
            // model spelling), the Arc<str> object key it shares with the resident entry,
            // object_id, the shared Option<ElementEntry> payload, deleted.
            //
            // THREE FIELDS LEFT AND ONE WAS RENAMED. `block_id`, `size` and `in_log` were each a
            // derivation of the address -- `block_id()`, `length()` and `block_id().is_none()` --
            // and are computed where they are written instead. The address itself is now reached
            // through the payload the resident model map also holds.
            //
            // AND THE ELEMENT NAME LEFT TOO, WHICH IS THE 120 -> 104 PINNED BELOW. The
            // `size_of::<Option<Arc<str>>>()` term that stood in this sum is REMOVED rather than
            // the total adjusted, so the sum is still reconstructed from the declaration and still
            // fails naming a field that moved: 118 -> 102 inside a 104-byte struct. With the term
            // still in, this row printed a field sum of 118 against a size of 104 and the
            // `fields <= size` assertion below would have fired -- masked only because the width
            // pin panicked three lines earlier. No row in this list holds an `Option<Arc<str>>`
            // any more, which is what the `BlockIndex` row above already says of the `opt_arc_str`
            // binding.
            //
            // THERE IS A SECOND, INDEPENDENT COPY OF THIS DECOMPOSITION, and whoever moves this
            // width next needs both. `index_log::tests::what_every_byte_of_an_index_item_is_spent_on`
            // reconstructs the same struct as an eight-aligned group plus a tail, and the same
            // sixteen bytes took that group 112 -> 96. Two copies of one decomposition that DIFFER
            // are not duplicates: correcting one and leaving the other leaves the stale half to be
            // read as current.
            fields: size_of::<IndexItemKind>()
                + size_of::<u32>()
                + 2 * string
                + size_of::<std::sync::Arc<str>>()
                + size_of::<u64>()
                + size_of::<Option<crate::block_store::ElementEntry>>()
                + size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "SlabCatalogEntry",
            size: size_of::<SlabCatalogEntry>(),
            align: align_of::<SlabCatalogEntry>(),
            fields: 4 * size_of::<u64>()
                + size_of::<crate::index_log::SlabCatalogState>()
                + 4 * opt_u64,
            per_item: false,
        },
        Budgeted {
            name: "BlockStoreSlabDescriptor",
            size: size_of::<BlockStoreSlabDescriptor>(),
            align: align_of::<BlockStoreSlabDescriptor>(),
            // Two slab ids, two byte counts and the readable prefix; the lifecycle state; six
            // optional u64s (created, updated, first/last block id, verified mtime, first error
            // offset); the corruption flag; and the first error text.
            fields: 5 * size_of::<u64>()
                + size_of::<crate::block_store::BlockStoreSlabState>()
                + 6 * opt_u64
                + size_of::<bool>()
                + size_of::<Option<String>>(),
            per_item: false,
        },
        // CONTROLS. One instance each, per shard, and the widest things in the engine. They are
        // in the table so the ranking below has something to rank them BELOW: if this module
        // ordered by width instead of by width x count, these two would head the list and the
        // whole exercise would point at the wrong structures.
        Budgeted {
            name: "ShardState (control: one per shard)",
            size: size_of::<crate::engine::state::ShardState>(),
            align: align_of::<crate::engine::state::ShardState>(),
            fields: 0,
            per_item: false,
        },
        Budgeted {
            name: "CoreIndex (control: one per shard)",
            size: size_of::<crate::engine::state::CoreIndex>(),
            align: align_of::<crate::engine::state::CoreIndex>(),
            fields: 0,
            per_item: false,
        },
    ]
}

/// THE BUDGET, PINNED. Every per-item width, with its field sum and its padding beside it.
///
/// The literals here and the `const _: () = assert!(size_of::<T>() == N)` beside each declaration
/// say the same thing twice on purpose. The const assertion is what stops a growth landing: it is
/// a BUILD failure, so it cannot be skipped, filtered out or marked ignored. This test is what
/// says WHY each number is what it is, and separates the two different wastes -- a field that is
/// wider than its range needs, and the bytes the aligner inserted between fields -- because they
/// do not have the same fix and one of them has no fix at this layer at all.
#[test]
fn every_per_item_structure_states_its_width_and_its_padding() {
    let rows = budget();
    // THE ANTI-SHRINK GUARD, WHICH CAUGHT THIS CHANGE AND IS NOW TWO-SIDED.
    //
    // It read `rows.len() >= 15` with the note that "a shrinking list is how a structure stops
    // being watched", and it did its job: three rows went when `ComponentBlocks`, `ComponentList`
    // and `ObjectBlockRefs` were deleted, the list went from 17 to 14, and the arm went red.
    //
    // IT IS NOT LOWERED TO 14. A `>=` is satisfied on whichever side an edit moves, so moving the
    // floor down to meet a shrink is the one repair that cannot fail again for the same reason.
    // The count is stated EXACTLY instead: a row added deliberately edits this number, and a row
    // that quietly disappears does not get to.
    //
    // 14 IS JUSTIFIED AND NOT MERELY MEASURED. The distinction this guard protects is between a
    // structure that stopped being watched and one that no longer EXISTS to watch, and only the
    // first is a defect. All three of the removed rows are gone from `engine_storage_state.rs` entirely -- an
    // object maps straight to its `BlockRefs`, which keeps its own row above -- so there is
    // nothing left for them to be watched for.
    assert_eq!(
        14,
        rows.len(),
        "the budget lists {} structures and should list 14: 17 before the three wrappers between \
         an object and its block refs were deleted, and a list that shrinks for any other reason \
         is how a structure stops being watched",
        rows.len()
    );

    println!(
        "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
        "structure", "size", "align", "fields", "slack", "counted per"
    );
    for row in &rows {
        println!(
            "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
            row.name,
            row.size,
            row.align,
            if row.fields == 0 { 0 } else { row.fields },
            if row.fields == 0 { 0 } else { row.padding() },
            if row.per_item { "stored item" } else { "shard or slab" }
        );
    }

    // --- The pinned widths. ---
    assert_eq!(16, size_of::<ElementEntry>(), "ElementEntry width moved");
    // 40, not the 56 it was before the entry stopped naming its element. The two `assert_ne!`
    // partners that stood below this are gone: they sat under an `assert_eq!` that panics first,
    // so they were unreachable on every change that could have fired them, and their messages
    // -- "BlockIndex is 39, so 40 is an upper bound here" -- asserted a reachability an
    // 8-aligned type does not have. The alignments are pinned below instead, where this
    // module's own `padding()` column is taken over them.
    assert_eq!(24, size_of::<BlockIndex>(), "BlockIndex width moved");
    assert_eq!(24, size_of::<BlockIndexMap>(), "BlockIndexMap width moved");
    assert_eq!(96, size_of::<BucketNode>(), "BucketNode width moved");
    assert_eq!(16, size_of::<BlockLookupRef>(), "BlockLookupRef width moved");
    assert_eq!(24, size_of::<BlockRefs>(), "BlockRefs width moved");
    // THE THREE 40s THAT STOOD HERE ARE GONE WITH THEIR TYPES. `ComponentBlocks`, `ComponentList`
    // and `ObjectBlockRefs` were the wrappers between an object and its refs; the lookup now holds
    // a `BlockRefs` per object, so that is the width to pin -- pinned above as a bare equality,
    // because the `!= 23` / `!= 25` pair that stood here was dominated by it and unreachable for
    // an 8-aligned type besides.
    //
    // THE ALIGNMENTS, WHICH THE WIDTHS ABOVE DO NOT ENTAIL. A width pin admits every alignment
    // that divides it, and this module's `padding()` column is derived from the `align` each row
    // carries -- so the alignment is the premise of every padding figure printed above and was
    // pinned nowhere.
    assert_eq!(8, align_of::<BlockIndex>(), "BlockIndex alignment moved");
    assert_eq!(8, align_of::<BlockRefs>(), "BlockRefs alignment moved");
    assert_eq!(16, size_of::<ObjectIndex>(), "ObjectIndex width moved");
    assert_eq!(8, size_of::<DeletedObjectIndex>(), "DeletedObjectIndex width moved");
    assert_eq!(24, size_of::<DirtyKeySet>(), "DirtyKeySet width moved");
    assert_eq!(16, size_of::<WalResidentBlock>(), "WalResidentBlock width moved");
    // 104, NOT 120, BECAUSE AN ELEMENT NAME LEFT THE ROW. `IndexItem::component` was an
    // `Option<Arc<str>>`; `Arc<str>` is a fat pointer, so the option cost two whole words with its
    // discriminant in the pointer's niche, and those sixteen bytes are the whole of the step. It is
    // the SAME sixteen bytes, for the SAME reason, as the `BlockIndex` row's 56 -> 40 pinned above:
    // both structures held that one name, and they gave it up one step apart.
    //
    // THE EXACTNESS IS PINNED WHERE THE TYPE IS DECLARED, NOT HERE. `index_log.rs` carries
    // `size_of::<IndexItem>() == 104` with `!= 103` and `!= 105` beside it as const assertions the
    // build evaluates, so 104 is exact rather than a bound. A second bracketing pair here would be
    // dominated by the equality below and unreachable for an 8-aligned type besides -- the same
    // reason the note on the three 40s above gives for not keeping one.
    assert_eq!(104, size_of::<IndexItem>(), "IndexItem width moved");
    assert_eq!(104, size_of::<SlabCatalogEntry>(), "SlabCatalogEntry width moved");
    assert_eq!(
        168,
        size_of::<BlockStoreSlabDescriptor>(),
        "BlockStoreSlabDescriptor width moved"
    );

    // --- The field sums, so a width change says whether it was a field or the aligner. ---
    for row in &rows {
        if row.fields == 0 {
            continue;
        }
        assert!(
            row.fields <= row.size,
            "{}: its fields add up to {} inside a {}-byte struct, which is impossible -- the \
             field list in this module has drifted from the declaration",
            row.name,
            row.fields,
            row.size
        );
    }

    // --- Non-vacuity: the padding arithmetic must discriminate, not report a constant. ---
    let with_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() > 0).count();
    let without_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() == 0).count();
    assert!(
        with_padding > 0 && without_padding > 0,
        "the padding column reports {with_padding} padded and {without_padding} unpadded \
         structures; with either at zero it is a constant and says nothing"
    );

    // --- The two named cases, so the classification itself is guarded. ---
    // WIDTH-BOUND: the fields fill the struct, so narrowing one can move it.
    let address = rows.iter().find(|r| r.name == "ElementEntry").expect("row");
    assert!(
        address.padding() < 8,
        "ElementEntry carries {} bytes of padding; at 8 or more it is alignment-bound and the \
         narrowing this module justifies would not have moved it",
        address.padding()
    );
    // ALIGNMENT-BOUND: a quarter of it is padding and no narrowing of either field helps.
    let lookup = rows.iter().find(|r| r.name == "BlockLookupRef").expect("row");
    assert_eq!(
        4,
        lookup.padding(),
        "BlockLookupRef is the worked example of an alignment problem: 12 bytes of field in 16. \
         If that is no longer true the module's own explanation is stale"
    );
}

// -------------------------------------------------------------------------------------------
// TRUNCATION. The two narrowed fields, at and past their boundary.
// -------------------------------------------------------------------------------------------

/// A LENGTH THAT DOES NOT FIT SATURATES; IT NEVER WRAPS.
///
/// This is the mutant that matters for this change. `length` was a `u64` and is a `u32`; the
/// conversion sits in `BlockAddress::from_parts`, which every construction goes through. A
/// conversion written `as u32` compiles, passes every existing test, and turns a 4 GiB length
/// into a small one -- a read that then returns the wrong bytes with no error anywhere. The
/// saturating form cannot do that: what comes back is larger than any length the encoder will
/// ever accept, so it reads as broken rather than as plausible.
///
/// `BLOCK_RECORD_LENGTH_MASK` is why this is a boundary and not a cliff: `encode_block_record`
/// REFUSES a record above 0x3FFF_FFFF bytes, so a real length is at most 2^30 plus a header --
/// a quarter of what the field now holds.
#[test]
fn a_length_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let just_under = ElementEntry::from_parts(1, 0, u64::from(u32::MAX) - 1, None, None);
    assert_eq!(
        u64::from(u32::MAX) - 1,
        just_under.length(),
        "a length inside the field must round-trip exactly"
    );

    for over in [
        u64::from(u32::MAX) + 1,
        u64::from(u32::MAX) + 2,
        1u64 << 32,
        (1u64 << 32) + 7,
        u64::MAX,
    ] {
        let address = ElementEntry::from_parts(1, 0, over, None, None);
        assert_eq!(
            u64::from(u32::MAX),
            address.length(),
            "a length of {over} must saturate to {}, not wrap to {}",
            u32::MAX,
            over as u32
        );
        // The discriminating half: for every value whose low 32 bits are NOT already the
        // saturation value, truncation and saturation give different answers, and this is the
        // one that fails under `as u32`. `u64::MAX` is excluded because its low bits ARE
        // `u32::MAX`, so it cannot tell the two apart -- a case that passes either way is not a
        // test of which one ran.
        if over as u32 != u32::MAX {
            assert_ne!(
                over as u32 as u64,
                address.length(),
                "a length of {over} came back as its low 32 bits -- that is the truncation this \
                 field was narrowed under the promise of not doing"
            );
        }
    }

    // And the saturated value is outside what the encoder will ever produce, so it cannot be
    // mistaken for a real length.
    assert!(
        u64::from(u32::MAX) > u64::from(crate::block_store::BLOCK_RECORD_LENGTH_MASK),
        "the saturation value must sit above the largest length a record can hold, or a \
         saturated address is indistinguishable from a valid one"
    );
}

/// A BLOCK ID THAT DOES NOT FIT SATURATES TOO, and the field has four billion times the headroom
/// the write path allows: `encode_block_record` refuses a block id above `u16::MAX`.
#[test]
fn a_block_id_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let real = ElementEntry::from_parts(1, 0, 64, Some(u64::from(u16::MAX)), None);
    assert_eq!(
        Some(u64::from(u16::MAX)),
        real.block_id(),
        "the largest block id the encoder accepts must round-trip exactly"
    );

    // A BLOCK ID ABOVE THE CEILING IS NOW REFUSED, NOT SATURATED, and the difference is the whole
    // reason the field could narrow to sixteen bits.
    //
    // `length` may saturate: `u32::MAX` is four times the largest length `encode_block_record`
    // accepts, so a saturated length is a value no writer could have produced and reads as broken.
    // `u16::MAX` IS the largest block id the encoder accepts, so saturating there would produce a
    // perfectly legal block id -- the LARGEST LEGAL PAGE of the same object -- and
    // `decode_block_record`'s block-id arm, the one cross-check on a read that can actually fire,
    // would confirm it against that page's own record. That is the address-word hazard one level
    // down, so it takes the address word's answer: refuse.
    for over in [u64::from(u16::MAX) + 1, 1u64 << 33, u64::MAX] {
        let refused = ElementEntry::try_from_parts(1, 0, 64, Some(over), None)
            .expect_err("a block id of {over} must be refused, not narrowed");
        assert_eq!(
            Some(over),
            refused.block_id,
            "the refusal must name the block id that did not fit, not the address word"
        );
        assert!(
            refused.to_string().contains("does not fit the block id field"),
            "the refusal must say which half of the structure refused: {refused}"
        );
    }

    // AND THE VALUE THE OLD SATURATION COULD NOT DISTINGUISH IS STILL THE PROBE VALUE. `u64::MAX`
    // cannot test a narrowing: its low bits ARE the ceiling, so truncation and saturation agree on
    // it and an `as u16` write passes. A mutation run scored exactly that against the old 32-bit
    // field. `MAX + 1` is the value that kills it, in both directions.
    const OVER: u64 = (1u64 << 33) + 5;
    assert_ne!(
        u64::from(OVER as u16),
        u64::from(u16::MAX),
        "the probe value must distinguish truncation from a refusal, or this test cannot fail"
    );
    assert!(
        ElementEntry::try_from_parts(1, 0, 64, Some(OVER), None).is_err(),
        "the constructor must refuse a block id of {OVER}, not narrow it to {}",
        OVER as u16
    );

    // The setter is the second write path into the same field and has to agree with the first --
    // by panicking, which is the same doctrine `from_parts` uses for an out-of-range address word.
    let mut address = ElementEntry::from_parts(1, 0, 64, None, None);
    address.set_block_id(Some(u64::from(u16::MAX)));
    assert_eq!(
        Some(u64::from(u16::MAX)),
        address.block_id(),
        "the largest legal block id must still go through the setter"
    );
    address.set_block_id(None);
    assert_eq!(None, address.block_id(), "clearing the field must still clear it");
}

/// AND THE SETTER REFUSES TOO, which is the half a constructor test cannot reach.
///
/// `set_block_id` is the second write path into the same field. It panics rather than returning,
/// because every caller's value is bounded by construction -- the record decoder's comes off the
/// wire as a `u16` already -- and the doctrine is `from_parts`'s: a truncated block id names a
/// different page of the same object, so it is refused rather than narrowed.
#[test]
#[should_panic(expected = "does not fit the block id field")]
fn the_block_id_setter_refuses_a_value_the_encoder_would_refuse() {
    let mut address = ElementEntry::from_parts(1, 0, 64, None, None);
    address.set_block_id(Some((1u64 << 33) + 5));
}

/// THE CONTROL ON THE EXPLANATION: EXACTLY THE STRUCTURES THAT HOLD AN ADDRESS MOVED.
///
/// The mechanism claimed for this change is that `BlockAddress` lost eight bytes, and that every
/// structure holding one inline lost them with it. That predicts movement for THREE of the per-item
/// structures this module prices and NO movement for the rest -- so the rest are the control, and
/// "no movement" is 0.00% over a stated denominator rather than an absence of complaint.
///
/// WHY A CONTROL IS NEEDED HERE. Three widths moved in one change, and a reader has no way to tell a
/// change that narrowed an address from one that narrowed something all three happen to share. The
/// thirteen structures that do NOT hold an address are what separates the two: if any of them had
/// moved, the explanation would be wrong whatever the three did.
///
/// THREE AND NOT FIVE, AND THE ASSERTION IS WHAT SAID SO. `BlockIndexMap` and `BucketNode` held a
/// page entry -- and therefore an address -- INLINE until #1975 put the single page behind a pointer.
/// They are now 24 and 88 whatever the entry weighs, so they belong in the control group, and this
/// test reported the reclassification rather than absorbing it: it failed with `BlockIndexMap holds 1
/// address(es) inline and moved by 56 bytes, not 8`. That is the row landing in the wrong list, which
/// is the failure the `addresses` column exists to produce. What the eight bytes are worth in those
/// two structures is now a HEAP figure, measured in `inline_arm_trade.rs`: eight bytes on every entry
/// slot in the request column, and -324.44 B a bucket on the chunk column at the shipped routing
/// range and 40,000 records, where every bucket holds a page LIST.
///
/// THE WIDTHS ARE READ, NOT WRITTEN DOWN. Each row states how many addresses the structure holds,
/// and the assertion is that the structures holding one are exactly the ones whose width is
/// consistent with the eight bytes -- so a structure that started holding an address would land in
/// the wrong list rather than quietly widening.
///
/// rust-internal: reads the engine's own declarations, no product behaviour
#[test]
fn only_the_structures_that_hold_an_address_moved() {
    // (name, width now, width before this change, addresses held inline)
    //
    // The "before" column is history and is written down; every "now" is read off the type. A row
    // whose two columns differ by anything but 8 x addresses is a row this change did not explain.
    //
    // THE COLUMN IS RELATIVE TO THE LATEST CHANGE, NOT TO THE OLDEST. It read 32 / 72 / 168 while
    // the previous step was the one being explained; those are now two steps back, and leaving them
    // would make each holder row report a delta of 16 and fail as "something else moved with it".
    // The step this column explains is `object_id` leaving the address: 24 -> 16, 64 -> 56,
    // 160 -> 152.
    let rows: Vec<(&str, usize, usize, usize)> = vec![
        ("ElementEntry", size_of::<ElementEntry>(), 24, 1),
        ("BlockIndex", size_of::<BlockIndex>(), 64, 1),
        ("IndexItem", size_of::<crate::index_log::IndexItem>(), 160, 1),
        // Held an address inline until #1975 boxed the single-page arm; 24 and 88 whatever the
        // entry weighs now, so they are control rows and not holder rows.
        ("BlockIndexMap", size_of::<BlockIndexMap>(), 24, 0),
        // 96 AND 96, AND THE `before` COLUMN MOVED WITH IT. This row's claim is that a structure
        // holding no `ElementEntry` inline did not move when the address shed -- and it did not.
        // What moved it since is a DIFFERENT change, named rather than absorbed: the node gained
        // one word, `tombstone_elements`, when a removal's element name left the page entries. So
        // both columns are 96 and the row still says what it was written to say; freezing the
        // `before` at 88 would have made this arm report an address defect for a field that holds
        // no address.
        ("BucketNode", size_of::<BucketNode>(), 96, 0),
        ("BlockLookupRef", size_of::<BlockLookupRef>(), 16, 0),
        ("BlockRefs", size_of::<BlockRefs>(), 24, 0),
        ("ObjectIndex", size_of::<ObjectIndex>(), 16, 0),
        ("DeletedObjectIndex", size_of::<DeletedObjectIndex>(), 8, 0),
        ("DirtyKeySet", size_of::<DirtyKeySet>(), 24, 0),
        ("WalResidentBlock", size_of::<WalResidentBlock>(), 16, 0),
        ("SlabCatalogEntry", size_of::<SlabCatalogEntry>(), 104, 0),
        (
            "BlockStoreSlabDescriptor",
            size_of::<BlockStoreSlabDescriptor>(),
            168,
            0,
        ),
        // THREE CONTROL ROWS ADDED TO REPLACE THE THREE THE COMPONENT COLLAPSE DELETED, AND THE
        // FLOOR BELOW IS NOT LOWERED TO MEET THE SHRINK.
        //
        // `ComponentBlocks`, `ComponentList` and `ObjectBlockRefs` were control rows here, all
        // three at 40 and all three holding no address. Deleting them took the control group from
        // 13 to 10 and failed the `control >= 12` guard -- correctly, because that guard's reason
        // is about the control's POWER: "below twelve the control cannot distinguish 'nothing else
        // moved' from 'nothing else was looked at'". That reason has nothing to do with which
        // types happen to exist, so lowering the floor would weaken exactly what it protects while
        // making the arm green. The group is refilled instead.
        //
        // Each of these holds no `ElementEntry` inline, so each predicts NO movement, and each has
        // its own `const _: () = assert!(size_of::<T>() == N)` beside its declaration in `engine_storage_state.rs`
        // -- so the literal in the `before` column here is cross-checked by the build rather than
        // being a second, driftable copy of the width.
        ("ModelKey", size_of::<ModelKey>(), 16, 0),
        ("BucketTtl", size_of::<BucketTtl>(), 8, 0),
        ("BucketFlags", size_of::<BucketFlags>(), 1, 0),
    ];

    // DENOMINATORS FIRST, both of them.
    let holders = rows.iter().filter(|row| row.3 > 0).count();
    let control = rows.iter().filter(|row| row.3 == 0).count();
    assert_eq!(
        3, holders,
        "the mechanism predicts movement for the three structures holding an address inline; this \
         list names {holders}. `BlockIndexMap` and `BucketNode` were holders until #1975 boxed the \
         single-page arm -- if either is a holder again, the entry is back inside the node"
    );
    assert!(
        control >= 12,
        "only {control} structures in the control group. Below twelve the control cannot \
         distinguish \"nothing else moved\" from \"nothing else was looked at\" -- and it gained two \
         when #1975 took the page entry out of the node"
    );

    println!("=== what moved, and what the mechanism says should not have ===");
    let mut moved = 0usize;
    for (name, now, before, addresses) in &rows {
        let delta = *before as i64 - *now as i64;
        println!(
            "  {name:<26} {before:>5} -> {now:>5}  ({delta:>+3}) addresses inline {addresses}"
        );
        if delta != 0 {
            moved += 1;
        }
        if *addresses == 0 {
            assert_eq!(
                *before, *now,
                "{name} holds no `ElementEntry` inline and moved from {before} to {now}. The \
                 mechanism this change claims predicts no movement here, so either the claim is \
                 wrong or this structure has started holding an address"
            );
        } else {
            // WHAT A LATER, UNRELATED CHANGE ALSO TOOK. The `before` column is the width before
            // the address shed, and this row's claim is about the shed alone. A structure that
            // has since lost bytes for another reason would make the delta disagree and fail
            // saying "something else moved with it" -- true, but it reads as an address defect.
            // So the other mover is NAMED and subtracted, and the address claim stays checked
            // against a live `size_of` rather than being frozen into a constant or restated as a
            // width the struct never had.
            let later = match *name {
                // `object_key` and `component` became the shared names the resident entry
                // holds: two 24-byte owned strings became a 16-byte pointer each (16 B). And
                // `block_id` and `size` left as fields because both were derivations of the
                // address, which takes two more whole words (16 B) out of the eight-aligned
                // group. `in_log` left too and is worth nothing here: it was a tail bool the
                // aligner was already padding, so the tail went 7 -> 6 of 8. None of this is
                // the address shed, so all 32 are named and subtracted.
                //
                // AND SIXTEEN MORE, BECAUSE THE ELEMENT NAME THEN LEFT THIS ROW TOO. The
                // `component` the paragraph above names as a name this row SHARED with the
                // resident entry is now gone from both. It was an `Option<Arc<str>>` in the
                // eight-aligned group -- a fat pointer plus a niche discriminant, so two whole
                // words -- and it is the whole of this row's 120 -> 104. 32 + 16 = 48.
                //
                // THIS ROW AND THE `BlockIndex` ROW BELOW NOW CARRY THE SAME KIND OF TERM FOR THE
                // SAME KIND OF REASON: one structure each, both of which held that one name, each
                // subtracting its own sixteen bytes so the ADDRESS claim stays checked against a
                // live `size_of` instead of being frozen into a constant. Without this term the row
                // would fail saying "something else moved with it unaccounted" -- true, and it
                // would read as an address defect.
                "IndexItem" => 48i64,
                // THE ENTRY GAVE UP ITS ELEMENT NAME, SIXTEEN BYTES, AND THAT IS NOT THE ADDRESS
                // SHED EITHER. `BlockIndex::component` was an `Option<Arc<str>>` sitting in the
                // eight-aligned group; dropping it takes two whole words out, so this row's total
                // delta is 24 where the address alone accounts for 8. Named and subtracted here
                // for exactly the reason the comment above gives: without it the row would fail
                // saying "something else moved with it unaccounted" -- true, and it would read as
                // an address defect.
                //
                // AND SIXTEEN MORE AGAIN, BECAUSE THE ENTRY THEN GAVE UP ITS OWN COPY OF THE
                // IDENTITY. `BlockIndex::object_key` was an `Arc<str>` in the eight-aligned group
                // -- a fat pointer, two whole words -- so this row's total delta is 40 where the
                // address alone accounts for 8. The two name terms are named and subtracted for
                // one reason: without them the row fails saying "something else moved with it
                // unaccounted", which is true and reads as an address defect.
                "BlockIndex" => 32i64,
                _ => 0,
            };
            assert_eq!(
                8 * *addresses as i64,
                delta - later,
                "{name} holds {addresses} address(es) inline and moved by {delta} bytes, of which \
                 {later} is accounted to a later change, leaving {} and not {}. A structure \
                 holding one address loses exactly the eight bytes the address lost -- more means \
                 something else moved with it unaccounted, less means the address is not held \
                 inline",
                delta - later,
                8 * *addresses
            );
        }
    }
    assert_eq!(
        holders, moved,
        "{moved} of the {} structures moved and {holders} hold an address; the two sets have to be \
         the same set",
        rows.len()
    );
    println!(
        "  {moved} of {} structures moved, and they are exactly the {holders} that hold an address \
         inline; the other {control} are 0 bytes",
        rows.len()
    );
}

/// `set_length` IS A WRITE PATH, and a setter that quietly drops its write is invisible to every
/// test that only ever reads a length back out of a constructor.
///
/// The slab inspector is its one production caller -- it walks a slab and stamps each record's
/// framed length onto the address it reports -- so a no-op here makes an inspection report every
/// record as zero-length. A mutation run found this uncovered.
#[test]
fn setting_a_length_after_the_fact_writes_it_and_saturates_it() {
    let mut address = ElementEntry::from_parts(1, 0, 0, None, None);
    assert_eq!(0, address.length(), "it starts at the length it was built with");

    address.set_length(4_096);
    assert_eq!(4_096, address.length(), "the setter must actually write");

    address.set_length(17);
    assert_eq!(17, address.length(), "and must overwrite what was there");

    // The same discriminating value as above: low bits that are not the saturation value.
    const OVER: u64 = (1u64 << 33) + 5;
    address.set_length(OVER);
    assert_eq!(
        u64::from(u32::MAX),
        address.length(),
        "the setter must saturate, not keep the low 32 bits"
    );
}

/// THE STORED FORM DID NOT MOVE.
///
/// `BlockAddress` serializes through `BlockAddressWire`, which holds `length` as a `u64` and
/// `block_id` as an `Option<u64>` and is untouched by this change. That is what makes the
/// narrowing resident-only: an index written before it reads back identically, and an index
/// written after it is byte-identical to one written before. If this test fails, a stored format
/// has moved and the change is not what its own pull request says it is.
///
/// AND THEN IT MOVED, IN EXACTLY ONE SLOT, AND THIS TEST IS WHAT SAID SO.
///
/// The sentence above is the contract this test was written to hold, and it fired: `object_id` left
/// `BlockAddress`, so `From<BlockAddress> for BlockAddressWire` -- handed an address and nothing
/// else -- can no longer produce a value for `oi`, and the golden below now reads `"oi":null` where
/// it read a real id. Nothing but running this could have said so, which is why the test is restated
/// rather than deleted: it remains the tripwire, now pinned to the shape that is actually written.
///
/// WHAT PAYS FOR IT is `engine::SHARD_INDEX_FORMAT_VERSION` at 6 -- 4 was reserved while main held 3,
/// the tombstone took main to 5, and a stamp may only increase. The slot STAYS on the wire because
/// the index log packs this struct positionally and retiring it would shorten the array and refuse
/// every existing row; it is written empty because there is no identity in scope at the point it is
/// written. An old row carrying a real `oi` beside a `g` that agrees with it still DECODES -- both
/// sides of that cross-check come off the wire -- and then yields an address whose
/// `ADDRESS_HAS_GENERATION` bit is set while `generation()` answers `None`. The stamp refusing the
/// load is the only point at which that is visible, and `block_entry_names::
/// a_store_stamped_with_the_wrong_struct_version_is_refused_before_it_is_decoded` drives it.
///
/// THE EARLIER NOTE, KEPT because it is the other half of the same slot's history: `generation`
/// became DERIVED as `block_id.or(object_id)` rather than stored, and for every address the engine
/// then produced the spelling was unchanged, because every production constructor already passed
/// exactly that expression. The fixture's independent generation was what had to move that time.
#[test]
fn the_stored_form_moved_in_one_slot_and_the_version_stamp_pays_for_it() {
    // THE SLAB AND THE OFFSET AT THEIR ADDRESSABLE MAXIMUMS, taken from the constants rather than
    // written as a literal. This fixture used to carry a slab id of 9,876,543,210 -- above 2^32 --
    // to state that the stored form kept 64-bit slab coordinates whatever the resident fields did.
    // That premise is GONE: the two coordinates are now the two halves of one 32/32 word, so a
    // slab id that large is refused rather than stored, and the largest one there is is what this
    // pins instead.
    let slab = crate::block_store::MAX_ADDRESSABLE_BLOCK_SLAB_ID;
    let offset = crate::block_store::MAX_ADDRESSABLE_BLOCK_OFFSET;
    let address = ElementEntry::from_parts(
        slab,
        offset,
        1_048_576,
        Some(7),
        Some(0xDEAD_BEEF_CAFE_F00D),
    );
    let word = crate::block_store::make_block_address_word(slab as u32, offset as u32);
    let json = serde_json::to_string(&address).expect("an address serializes");
    // `rs` AND `h` ARE GONE, and `g` is the only optional slot left. They used to be written as
    // nil because the index log packs this struct positionally and a field that vanishes when
    // empty moves every field behind it. Neither was ever read, and a slot carrying no meaning
    // does not need holding open: `block_store::shorter_struct_against_an_existing_row` measures
    // that a positional row LONGER than the struct decoding it is refused by length rather than
    // reinterpreted, so a retired dead slot cannot shift anything silently.
    //
    // `g` stays because its PRESENCE is the meaning -- it becomes `ADDRESS_HAS_GENERATION` -- and
    // the named served-index path would drop it in silence, which needs the format-version stamp
    // rather than this reasoning.
    // `oi` IS WRITTEN NULL, AND THAT IS THE MOVE. The address was built WITH an object id -- the
    // parameter is still there and its presence still decides `has_generation` -- and the wire
    // cannot carry the value because the struct no longer holds one to carry.
    assert_eq!(
        format!("{{\"a\":{word},\"l\":1048576,\"pi\":7,\"oi\":null,\"g\":7}}"),
        json,
        "the stored spelling of an address moved AGAIN, beyond the one slot this change moved"
    );
    // AND THE SLOT IS STILL THERE, which is a separate fact from what it holds: a positional row
    // that lost a field shifts every field behind it, so `oi` being PRESENT and null is what keeps
    // the index log readable.
    assert!(
        json.contains("\"oi\":"),
        "the `oi` slot must stay on the wire even written empty, or the index log's positional \
         rows shift: {json}"
    );
    assert!(
        !json.contains("\"ps\"") && !json.contains("\"o\":"),
        "the split slab id and offset are still being written: {json}"
    );
    let back: ElementEntry = serde_json::from_str(&json).expect("it reads back");
    assert_eq!(address, back, "an address must round-trip through its stored form");
    assert_eq!(
        (back.block_slab_id(), back.offset()),
        (slab, offset),
        "the packed word did not unpack to the two numbers that went into it"
    );

    // A stored length or block id above the resident field is already outside what the encoder
    // can have written, so reading one is reading a corrupt index. It must come back saturated,
    // never truncated: a truncated length is a plausible number and a saturated one is not.
    // NO generation key here, deliberately. Besides the saturation it was written for, this
    // is the shape of an index written before the generation existed: an identity and no
    // generation at all. It must LOAD, and it must not acquire one.
    // A STORED LENGTH ABOVE THE RESIDENT FIELD SATURATES; A STORED BLOCK ID IS REFUSED. The two are
    // not the same case and this is where the difference is driven: `u32::MAX` is four times the
    // largest length the encoder accepts, so a saturated length is a value no writer could have
    // produced and reads as broken. `u16::MAX` IS the largest block id it accepts, so a saturated
    // one is a legal block id for a different page of the same object -- which
    // `decode_block_record`'s block-id arm would then confirm against that page's own record.
    //
    // NO generation key here, deliberately. Besides the saturation it was written for, this is the
    // shape of an index written before the generation existed: an identity and no generation at all.
    // It must LOAD, and it must not acquire one.
    let wide_length = "{\"a\":4294967296,\"l\":4294967296}";
    let read: ElementEntry =
        serde_json::from_str(wide_length).expect("a wide stored length still loads");
    assert_eq!(u64::from(u32::MAX), read.length(), "a wide stored length saturates");
    assert_eq!(
        None,
        read.generation(),
        "an index that stored no generation must not acquire one when the field is derived"
    );

    let wide_block_id = "{\"a\":4294967296,\"l\":64,\"pi\":4294967296}";
    let refusal = serde_json::from_str::<ElementEntry>(wide_block_id)
        .expect_err("a stored block id above the field's ceiling must be REFUSED, not narrowed");
    assert!(
        refusal.to_string().contains("does not fit the block id field"),
        "the refusal must name the block id rather than the address word: {refusal}"
    );

    // AND THE LARGEST LEGAL ONE STILL LOADS, or the refusal above is a ceiling set too low.
    let at_ceiling = "{\"a\":4294967296,\"l\":64,\"pi\":65535}";
    let read: ElementEntry =
        serde_json::from_str(at_ceiling).expect("the encoder's own ceiling must load");
    assert_eq!(Some(u64::from(u16::MAX)), read.block_id());
}

// -------------------------------------------------------------------------------------------
// WHAT THEY COST, AT TWO CORPUS SIZES.
// -------------------------------------------------------------------------------------------

/// How many of each structure a seeded shard actually holds.
#[derive(Default)]
struct ItemCounts {
    records: usize,
    bucket_nodes: usize,
    block_index_entries: usize,
    model_map_addresses: usize,
    object_block_refs: usize,
    block_lookup_refs: usize,
    dirty_key_sets: usize,
    wal_resident_blocks: usize,
}

impl ItemCounts {
    /// Every resident `BlockAddress`: the one each model-map entry holds, plus the one inside
    /// every page-index entry. The two are separate copies of the same address, which is why
    /// this structure's count is roughly twice the record count rather than equal to it.
    fn block_addresses(&self) -> usize {
        self.model_map_addresses + self.block_index_entries
    }
}

fn count_items(shard: &crate::engine::state::ShardState) -> ItemCounts {
    let mut counts = ItemCounts::default();
    counts.model_map_addresses = shard.strings().len()
        + shard.features().values().map(|series| series.len()).sum::<usize>()
        + shard.hashes.values().map(|fields| fields.len()).sum::<usize>()
        + shard.sets.values().map(|members| members.len()).sum::<usize>()
        + shard.zsets.values().map(|members| members.len()).sum::<usize>()
        + shard.lists.values().map(|items| items.len()).sum::<usize>();
    counts.records = shard.strings().len() + shard.features().values().map(|s| s.len()).sum::<usize>();
    counts.bucket_nodes = shard.bucket_index.bucket_map.len();
    counts.block_index_entries = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .sum();
    counts.object_block_refs = shard.bucket_index.object_block_lookup.len();
    counts.block_lookup_refs = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(|entry| entry.len())
        .sum();
    // One `DirtyKeySet` per dirty BUCKET, and the measured distribution is one key per bucket at
    // both corpus sizes -- `every_dirty_bucket_holds_its_one_key_inline` walks the shard and
    // reports it. So the dirty-object count is the set count here, and the comment says which
    // measurement that leans on rather than assuming it.
    counts.dirty_key_sets = shard.dirty_objects.len();
    counts.wal_resident_blocks = shard.wal_resident_blocks.len();
    counts
}

/// THE MEASUREMENT. `size_of` x count, at two corpus sizes ten times apart.
///
/// Ranked by the product, which is the only ranking that answers the question this module asks.
/// `BlockStoreSlabDescriptor` is the reason: it is 168 bytes against `BlockLookupRef`'s 16, and a
/// store holding a gigabyte of pages has ONE of it and eighty thousand of the other.
///
/// A per-item figure that is FLAT across the two corpus sizes is what makes the total multiply
/// out to any scale. One that grows is a finding in itself, and the per-structure rows say which
/// structure grew.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT across the two arms and asserted below: `tempfile`
/// names every directory with the same number of characters, and a path length that moved between
/// arms would move allocation counts with it.
#[test]
#[ignore = "seeds 8,000 then 80,000 records; run by name"]
fn what_the_per_item_structures_cost_at_two_corpus_sizes() {
    let mut per_record: Vec<(&'static str, f64, f64)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        budget_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let counts = count_items(shard);

        // --- Denominators, asserted before anything divides by them. ---
        assert_eq!(
            strings_n + series_keys * series_points,
            counts.records,
            "denominator: the shard must hold every record seeded"
        );
        assert!(
            counts.bucket_nodes > 0,
            "denominator: no routing buckets, so every per-bucket figure below is a zero that \
             means nothing"
        );
        assert!(
            counts.block_index_entries > 0,
            "denominator: no page-index entries, so the structure this module is about is absent"
        );
        assert!(
            counts.object_block_refs > 0 && counts.block_lookup_refs > 0,
            "denominator: the object lookup is empty"
        );

        let rows: Vec<(&'static str, usize, usize)> = vec![
            ("ElementEntry", size_of::<ElementEntry>(), counts.block_addresses()),
            ("BucketNode", size_of::<BucketNode>(), counts.bucket_nodes),
            ("BlockIndex (inside BucketNode)", size_of::<BlockIndex>(), counts.block_index_entries),
            ("BlockLookupRef", size_of::<BlockLookupRef>(), counts.block_lookup_refs),
            ("BlockRefs (one per object in the lookup)", size_of::<BlockRefs>(), counts.object_block_refs),
            ("DirtyKeySet", size_of::<DirtyKeySet>(), counts.dirty_key_sets),
            ("WalResidentBlock", size_of::<WalResidentBlock>(), counts.wal_resident_blocks),
        ];

        let mut ranked = rows.clone();
        ranked.sort_by_key(|(_, width, count)| std::cmp::Reverse(width * count));

        println!("\n=== {label}: per-item structures ranked by size_of x count ===");
        println!("  records={} buckets={} page entries={} model-map addresses={}",
            counts.records, counts.bucket_nodes, counts.block_index_entries,
            counts.model_map_addresses);
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12}",
            "structure", "width", "count", "total bytes", "per record"
        );
        let mut total = 0usize;
        for (name, width, count) in &ranked {
            let bytes = width * count;
            total += bytes;
            println!(
                "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
                name,
                width,
                count,
                bytes,
                bytes as f64 / counts.records as f64
            );
        }
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
            "TOTAL (structure payload only)",
            "",
            "",
            total,
            total as f64 / counts.records as f64
        );

        // BlockIndex is NOT added into the total separately: every page entry lives inside a
        // BucketNode's own `BlockIndexMap`, so its bytes are already inside the BucketNode row.
        // Counting it twice would report a win twice.
        let counted: usize = rows
            .iter()
            .filter(|(name, _, _)| *name != "BlockIndex (inside BucketNode)")
            .map(|(_, width, count)| width * count)
            .sum();
        for (name, width, count) in &rows {
            per_record.push((
                name,
                (width * count) as f64 / counts.records as f64,
                counts.records as f64,
            ));
        }
        println!(
            "  payload without double-counting the nested page entries: {} B, {:.2} B/record",
            counted,
            counted as f64 / counts.records as f64
        );
    }

    assert_eq!(
        2,
        path_lengths.len(),
        "both arms must have run, or the flatness claim below compares one arm with itself"
    );
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation counts move with it",
        path_lengths[0], path_lengths[1]
    );

    // --- Flatness. Every per-item figure must hold across a tenfold corpus. ---
    println!("\n=== flatness across a tenfold corpus ===");
    let half = per_record.len() / 2;
    for index in 0..half {
        let (name, small, _) = per_record[index];
        let (name_big, big, _) = per_record[index + half];
        assert_eq!(name, name_big, "the two arms reported structures in different orders");
        let ratio = if small > 0.0 { big / small } else { 1.0 };
        println!("  {name:<32} {small:>8.2} -> {big:>8.2} B/record  ({ratio:.3}x)");
        assert!(
            ratio < 1.25,
            "{name} costs {small:.2} B/record at 8,000 records and {big:.2} at 80,000 ({ratio:.3}x) \
             -- it is growing with the corpus, which is a finding and not a budget"
        );
    }
}

fn budget_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn budget_seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }
}

// -------------------------------------------------------------------------------------------
// THE WIDEST PER-ITEM STRUCTURE, BYTE BY BYTE.
//
// `BucketNode` exists once per routing bucket, which is once per key that routes to a bucket of
// its own, and the ranking above puts it first at both corpus sizes. This section says what its
// bytes ARE -- every field with its own width and alignment, the sum beside `size_of`, and the
// slack named separately -- and then prices the three shapes that were considered for it and
// declined, so the decline is a number rather than an opinion.
// -------------------------------------------------------------------------------------------

/// One declared field of `BucketNode`.
///
/// The widths are `size_of` over the DECLARED types, never literals, so this table moves when the
/// declaration does instead of going quietly stale beside it.
struct Field {
    name: &'static str,
    ty: &'static str,
    size: usize,
    align: usize,
}

fn bucket_node_fields() -> Vec<Field> {
    macro_rules! field {
        ($name:literal, $ty:ty) => {
            Field {
                name: $name,
                ty: stringify!($ty),
                size: size_of::<$ty>(),
                align: align_of::<$ty>(),
            }
        };
    }
    vec![
        field!("routing_bucket", u32),
        field!("layout", BucketLayoutState),
        field!("flags", BucketFlags),
        field!("ttl_ms", BucketTtl),
        field!("dirty_generation", u64),
        field!("first_dirty_wal_sequence", u64),
        field!("first_dirty_index_log_sequence", u64),
        field!("object_index", ObjectIndex),
        field!("deleted_object_index", DeletedObjectIndex),
        field!("block_index", BlockIndexMap),
        field!("tombstone_elements", TombstoneElements),
    ]
}

/// EVERY BYTE OF THE BUCKET NODE, ACCOUNTED FOR.
///
/// Ten fields, their widths summed, and the difference against `size_of` named as what it is:
/// the aligner's, not any field's. The sum is the discriminating half. A width that is stated
/// without its field sum cannot tell a structure that is FULL from one that is half padding, and
/// those two want opposite fixes -- one wants a narrower field, the other cannot be helped by any
/// narrowing at all.
///
/// HOW RUST LAYS THIS OUT, and it is the whole explanation of the number. Fields reorder freely,
/// so the layout is two groups: everything of alignment 8 packs solid, and everything smaller
/// fills the tail, which is then rounded up to the struct's own alignment. Here that is 160 bytes
/// of eight-aligned field and 6 bytes of small field rounded to 8.
///
/// THE TAIL USED TO BE TEN BYTES AND THE RULE WRITTEN HERE WAS TOO STRONG. It said NOTHING in the
/// ten-byte tail could be narrowed to any effect, because the tail was already inside a rounding.
/// That is true of narrowing ONE field and it is what the per-field verdict below still says --
/// 9, 6 and 4 all round back to 16 -- but it was read as though it applied to the tail as a
/// whole, and it does not. Five `bool` became five BITS, four bytes left at once, and the tail
/// crossed the step: 10-in-16 became 6-in-8 and the structure lost eight bytes. The rule that
/// survives is the last clause, which was right all along: only a change that takes the tail to 8
/// bytes or fewer, or that takes a whole word out of the eight-aligned group, moves this
/// structure at all. Packing was such a change; narrowing any single tail field is not.
///
/// AND THE OTHER CLAUSE IS WHAT TOOK IT TO 168. `last_dump_sequence` was a whole word of the
/// eight-aligned group, and removing it takes that word out without touching the tail: 160 + 8.
/// Removed bytes LEAVE, where a narrowed field's bytes move into the tail and are handed straight
/// back at six -- which is why a removal crosses the step here and a narrowing does not.
#[test]
fn every_byte_of_the_bucket_node_is_accounted_for() {
    let fields = bucket_node_fields();
    assert_eq!(
        11,
        fields.len(),
        "the field table lists {} fields; `BucketNode` has eleven and a table that has drifted \
         from the declaration proves nothing about it",
        fields.len()
    );

    println!("\n=== BucketNode, field by field ===");
    println!("  {:<34} {:<20} {:>5} {:>6}", "field", "declared type", "size", "align");
    let mut eight_aligned = 0usize;
    let mut tail = 0usize;
    for field in &fields {
        println!(
            "  {:<34} {:<20} {:>5} {:>6}",
            field.name, field.ty, field.size, field.align
        );
        if field.align == align_of::<BucketNode>() {
            eight_aligned += field.size;
        } else {
            tail += field.size;
        }
    }
    let sum: usize = fields.iter().map(|field| field.size).sum();
    let size = size_of::<BucketNode>();
    let slack = size - sum;
    println!("  {:<34} {:<20} {:>5}", "SUM OF FIELDS", "", sum);
    println!("  {:<34} {:<20} {:>5}", "size_of::<BucketNode>()", "", size);
    println!(
        "  {:<34} {:<20} {:>5}   <- the aligner, not any field",
        "SLACK", "", slack
    );
    println!(
        "  groups: {eight_aligned} B of eight-aligned field + {tail} B of small field, the tail \
         rounded up to {}",
        size - eight_aligned
    );

    // 94 AND 96, NOT 86 AND 88, AND THE EXTRA WORD IS NAMED. `tombstone_elements` is the eleventh
    // field: ONE WORD, holding the element name of each tombstone this bucket carries, which the
    // page ENTRIES stopped carrying. It is `Option<Box<Vec<_>>>` rather than a `Vec` for exactly
    // the reason this table exists -- the node's count is the bucket count, so three words a
    // bucket to carry nothing in almost every bucket is not a trade and one word is.
    //
    // IT LANDED IN THE EIGHT-ALIGNED GROUP AND COST A WHOLE WORD. The tail was 6 rounded to 8 and
    // is untouched, so the slack is still 2 and the step is the full eight bytes: this structure
    // had no room to absorb it. Against it, `BlockIndex` -- one per PAGE, which is what the index
    // is made of -- went 56 to 40. Eight bytes a bucket for sixteen bytes a page.
    assert_eq!(94, sum, "the fields of BucketNode add up to {sum}, not 94");
    assert_eq!(96, size, "BucketNode is {size} bytes wide, not 96");
    assert_eq!(2, slack, "BucketNode carries {slack} bytes of alignment slack, not 2");

    // The layout rule itself, asserted rather than described: the eight-aligned group packs
    // solid and the rest is one rounding.
    let align = align_of::<BucketNode>();
    assert_eq!(8, align, "BucketNode's alignment moved, and the arithmetic below assumes 8");
    // 80, AND EVERY BYTE THAT HAS EVER LEFT THIS STRUCTURE LEFT THIS GROUP -- none has come out of
    // the tail. In order: the address inside the inline page entry shed a derived `generation`;
    // `last_dump_sequence` left the node; that same address merged its slab id and its offset into
    // ONE WORD; the page entry's model spelling stopped being a sixteen-byte fat pointer to a string
    // from a closed set of seventeen, which was the first step to take TWO words at once (168 -> 152
    // -> 136); and then the page index stopped holding that entry INLINE at all, which took SEVEN
    // words in one step, 136 -> 80. The tail is still six because the five flags became five bits,
    // which is the other half of the structure entirely and no change to the page entry can reach it.
    // 88 AND NOT 80: the eight-aligned group gained `tombstone_elements`, one word, when a
    // removal's element name left the page entries for the bucket that holds them. It is the only
    // byte this structure has ever GAINED, and it came into the same group every earlier step came
    // out of -- which is why it cost a whole word rather than disappearing into the tail's rounding.
    assert_eq!(88, eight_aligned, "the eight-aligned group is {eight_aligned} B, not 88");
    assert_eq!(6, tail, "the tail group is {tail} B, not 6");
    assert_eq!(
        eight_aligned + tail.div_ceil(align) * align,
        size,
        "the two groups plus one rounding must reconstruct the width exactly, or the layout is \
         not what this test says it is"
    );

    // --- WIDTH OR ALIGNMENT, field by field, and the verdict is arithmetic. ---
    //
    // A field is a WIDTH candidate only if taking bytes off it takes bytes off the struct.
    // Inside the tail that is false until the tail itself drops past a multiple of the
    // alignment, and every field in the tail here is one byte or four.
    println!("\n=== which fields could move this structure, and which could not ===");
    for field in &fields {
        let verdict = if field.align < align {
            // In the rounding. Narrowing it cannot cross the boundary on its own -- but note
            // that REMOVING enough of them at once can, which is how the tail got to six.
            "ALIGNMENT -- inside a 6-in-8 rounding; narrowing it alone moves nothing"
        } else if field.size % align == 0 && field.size > align {
            "WIDTH -- a whole number of words, and every word of it is paid per bucket"
        } else {
            "WIDTH -- one word, and losing it would take a word off the struct"
        };
        println!("  {:<34} {}", field.name, verdict);
    }
    let tail_fields = fields.iter().filter(|field| field.align < align).count();
    assert_eq!(
        3,
        tail_fields,
        "three fields sit in the tail rounding -- the routing bucket, the layout and the packed \
         flag byte -- and this test found {tail_fields}"
    );
    let tail_bytes: usize = fields
        .iter()
        .filter(|field| field.align < align)
        .map(|field| field.size)
        .sum();
    assert!(
        tail_bytes.div_ceil(align) * align > tail_bytes,
        "the tail is {tail_bytes} B and fills its rounding exactly; the claim that narrowing a \
         tail field changes nothing is only true while it does not"
    );

    // --- THE BIGGEST SINGLE FIELD, AND IT IS NO LONGER THE PAGE INDEX. ---
    //
    // This assertion used to read the other way: `block_index` held a whole page entry INLINE and
    // was over HALF the structure, which is what made it the field an accounting had to lead with.
    // The entry is behind a pointer now, so the page index is a quarter of the node and the widest
    // field is whichever the table says -- asserted to be found rather than named, so a future
    // narrowing moves it instead of leaving this comment describing the wrong one.
    let block = fields
        .iter()
        .find(|field| field.name == "block_index")
        .expect("block_index is a field of BucketNode");
    assert!(
        block.size * 2 < size,
        "the page index is {} of {size} bytes. It is supposed to be well under half the structure \
         now that its single-page arm holds a pointer rather than a whole entry; if it is over half \
         again, the inline entry has come back or another payload has joined it",
        block.size
    );
    let widest = fields
        .iter()
        .max_by_key(|field| field.size)
        .expect("BucketNode has fields");
    println!(
        "  widest field: {} at {} B of {size} ({:.1}% of the structure) -- the page index is {} B",
        widest.name,
        widest.size,
        100.0 * widest.size as f64 / size as f64,
        block.size
    );
    assert!(
        widest.size * 2 < size,
        "the widest single field is {} at {} B of {size}. No single field is supposed to dominate \
         this structure any more, and one that does is the next thing to account for",
        widest.name,
        widest.size
    );
}

// -------------------------------------------------------------------------------------------
// THE THREE SHAPES CONSIDERED AND DECLINED, PRICED.
//
// Each is a MIRROR: a struct built from the same field types, so it is a statement about widths
// and not a guess. `mirror_is_faithful` is the control -- it fails if a mirror stops describing
// the declaration, which is the only way these numbers could quietly become fiction.
// -------------------------------------------------------------------------------------------

/// The node as it stands. If this is not `size_of::<BucketNode>()` every mirror below is fiction.
#[allow(dead_code)]
struct MirrorLive {
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
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The node as it was before #1958, with the countdown spending a word on its discriminant AND
/// the tombstone index spending sixteen bytes on a case it is almost never in.
#[allow(dead_code)]
struct MirrorWideTtl {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: Option<u64>,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The node immediately before this change: the live shape with the tombstone index still held
/// as the full enum.
#[allow(dead_code)]
struct MirrorWideTombstone {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The five flags folded into one byte -- the shape that SHIPPED, held here as a control on the
/// live mirror rather than as a proposal.
#[allow(dead_code)]
struct MirrorPackedFlags {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: u8,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The node immediately BEFORE this change: the same fields with the five flags as five
/// independent `bool`, which is ten bytes of tail rounded to sixteen.
#[allow(dead_code)]
struct MirrorLooseFlags {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The two transient log claims moved out of the node into a side map of dirty buckets.
#[allow(dead_code)]
struct MirrorHoistedClaims {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// The inline page entry held behind a pointer instead.
///
/// ITS `Many` ARM IS THE SHIPPED LIST, which is what makes the only difference between this mirror
/// and the declaration the arm under discussion. It used to spell that arm as a `BTreeMap`, which
/// stopped being true at #1963 and went unnoticed because a tree header and a list header are both
/// 24 bytes -- a mirror can be stale in TYPE while agreeing in WIDTH, and this one was.
#[allow(dead_code)]
enum MirrorBoxedBlockIndexMap {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(Vec<(u64, BlockIndex)>),
}

/// The page index WITH the inline single-page arm -- the shape that was dropped.
///
/// Kept beside the boxed one because the three shapes are what a proposal to bring the arm back
/// would have to argue between, and a shape that exists only as an absence cannot be priced.
/// `inline_arm_trade.rs` prices all three at both routing ranges.
#[allow(dead_code)]
enum MirrorInlineBlockIndexMap {
    Empty,
    One(u64, BlockIndex),
    Many(Vec<(u64, BlockIndex)>),
}

/// The node as it was before the single-page entry was boxed: every field the same, and the page index
/// still holding a whole entry inline.
#[allow(dead_code)]
struct MirrorInlineBlock {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: MirrorInlineBlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

#[allow(dead_code)]
struct MirrorBoxedBlock {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: MirrorBoxedBlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES below stay the prices. This module's own
    // rule: a mirror is the live declaration plus ONE historical difference, so a word the
    // live node gains has to be gained here too or every price is measured against a shape
    // this engine does not have.
    tombstone_elements: TombstoneElements,
}

/// WHAT EACH DECLINED SHAPE WOULD ACTUALLY BUY, IN BYTES PER BUCKET.
///
/// THE CONTROL COMES FIRST. `MirrorLive` is built from the same field types as the declaration
/// and has to agree with it exactly; if it does not, every other row here is describing a
/// structure this engine does not have, and the test says so rather than printing numbers.
///
/// THE ONE THAT WAS TAKEN. `MirrorWideTtl` is the shape before this change and `MirrorLive` is
/// the shape after: eight bytes, and they came off because the countdown stopped needing a word
/// for its discriminant. It is the only one of the four whose stored spelling does not move --
/// the countdown is one JSON key either way, and the serde impls unbias across it.
///
/// THE OTHER ONE THAT WAS TAKEN, AND IT WAS DECLINED HERE FIRST. Folding the five flags into a
/// byte is worth the same eight bytes. The decline was written on blast radius -- `dirty`,
/// `deleted`, `meta_loaded`, `loading` and `in_memory` are five keys of the stored index and 90
/// sites across the engine, and holding the stored spelling still means a hand-written
/// serializer for the node. Every clause of that was accurate and it still cost eight bytes a
/// bucket to believe. What made it payable was doing the rewrite from the COMPILER'S error
/// positions rather than from a grep: the flags became accessors of the same name, so the sites
/// are still `dirty` and `meta_loaded` to anything that reads this tree, and the enumeration ran
/// to zero errors rather than to zero sites of a shape -- which is what caught the four
/// multi-line assignments a line-oriented pass had quietly turned into reads.
///
/// THE ONE THAT WAS DECLINED ON WHERE THE COST WOULD GO. The two `#[serde(skip)]` log claims are
/// sixteen bytes and touch no stored shape at all, which makes them the cheapest bytes here to
/// reach -- but only if they move somewhere cheaper, and a side map keyed by bucket costs its own
/// key, its own pair and its own B-tree node for every bucket that is dirty. The measurement that
/// decides it is the dirty FRACTION, which the corpus probe prints; at the fraction this store
/// runs at it is not a win.
///
/// THE ONE THAT IS A LOSS, AND THE ONLY ONE WHOSE SIGN IS NOT OBVIOUS. Putting the inline page
/// entry behind a pointer takes the most off the struct of anything here -- and then pays it
/// back with interest, because almost every bucket holds exactly one page and would allocate for
/// it. The allocation is rounded up by the allocator, so the pair costs MORE than the inline form
/// it replaced, plus an indirection on every page read and an allocation on every bucket.
#[test]
fn what_each_declined_shape_of_the_bucket_node_would_cost() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorLive>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<MirrorLive>(),
        size_of::<BucketNode>()
    );

    let live = size_of::<BucketNode>();
    let wide_ttl = size_of::<MirrorWideTtl>();
    let wide_tombstone = size_of::<MirrorWideTombstone>();
    let loose = size_of::<MirrorLooseFlags>();
    let packed = size_of::<MirrorPackedFlags>();
    let hoisted = size_of::<MirrorHoistedClaims>();
    let boxed = size_of::<MirrorBoxedBlock>();
    let inline_block = size_of::<MirrorInlineBlock>();

    println!("\n=== what each shape makes the node ===");
    println!("  {:<44} {:>5} {:>9}", "shape", "bytes", "vs live");
    for (name, bytes) in [
        ("the node as it stands", live),
        ("with the five flags back as five bools (before)", loose),
        ("with the tombstone index back at the full enum as well", wide_tombstone),
        ("with the countdown back at two words as well", wide_ttl),
        ("the packed-flag mirror, which must equal the live shape", packed),
        ("with the two transient log claims hoisted out", hoisted),
        ("with the page entry held INLINE again (before)", inline_block),
        ("with a single-page arm re-added, behind a pointer", boxed),
    ] {
        println!(
            "  {:<44} {:>5} {:>+9}",
            name,
            bytes,
            bytes as i64 - live as i64
        );
    }

    // --- The change this module documents. ---
    //
    // EVERY ABSOLUTE FIGURE HERE IS EIGHT BYTES LOWER THAN IT WAS, TWICE OVER, AND NO DIFFERENCE
    // MOVED. Each mirror is the live declaration plus one historical difference, so when the live
    // node loses a word every mirror loses it too -- and it has to, or the differences below would
    // be measured against a shape this engine does not have. `last_dump_sequence` left the
    // eight-aligned group whole, and the address inside the inline page entry merged its two slab
    // coordinates into one word out of that same group, which is the same reason the two earlier
    // eight-byte changes came out of it whole.
    // AND EVERY ABSOLUTE FIGURE IS NOW SEVENTY-TWO BYTES LOWER AGAIN, FOR THE SAME REASON AND ON
    // THE LARGEST STEP YET: the page index stopped holding its single entry inline. Every mirror here
    // carries `BlockIndexMap` (or a variant of it that differs only in the arm under discussion),
    // so all of them lost the same 72 bytes and not one DIFFERENCE below moved.
    assert_eq!(
        120, wide_ttl,
        "the shape before #1958 was 208 bytes, 200 once the address inside the inline page entry \
         shed its derived generation, 192 once the node stopped carrying a per-bucket \
         last_dump_sequence, 184 once that address merged its two slab coordinates, 168 once the \
         page entry's model spelling became one byte, and 112 once the page index stopped holding \
         that entry inline; it reads as {wide_ttl}, so the mirrors have drifted from the history \
         they claim to price"
    );
    assert_eq!(
        112, wide_tombstone,
        "the shape before #1961 was 200 bytes, 192 once the address shed its derived generation, \
         184 once the node stopped carrying a per-bucket last_dump_sequence, 176 once that address \
         merged its two slab coordinates, 160 once the page entry's model spelling became one byte, \
         and 104 once the page index stopped holding that entry inline; it reads as \
         {wide_tombstone}, so the eight bytes that change claims are not the eight bytes it took"
    );
    assert_eq!(96, live, "the node is {live} bytes, not 96");
    assert_eq!(
        104, loose,
        "the shape before the flags were packed was 184 bytes, 176 once the node stopped carrying a \
         per-bucket last_dump_sequence, 168 once the address inside the inline page entry merged \
         its two slab coordinates, 152 once that entry's model spelling became one byte, and 96 \
         once the page index stopped holding the entry inline -- this mirror holds the same page \
         index, so it moves with the live shape every time and the EIGHT BYTES BETWEEN THEM is \
         still what packing the flags is worth. Which is the point of pricing by the DISTANCE \
         between mirrors rather than by either width: FIVE changes to the page entry and its index \
         have moved both, and not one of them has moved this price. It reads as {loose}, so the row \
         that prices this change is not describing the shape it replaced"
    );
    assert_eq!(
        8,
        wide_tombstone - loose,
        "holding the tombstone index as one nullable pointer is priced at eight bytes a bucket, \
         against the loose-flag shape it was measured on; it measured {}",
        wide_tombstone - loose
    );

    // --- WHAT THIS CHANGE TOOK, and the control that says the mirror describes it. ---
    //
    // `MirrorPackedFlags` is built from a bare `u8` where the declaration now has `BucketFlags`.
    // They must come out the same width, or the newtype is costing something a byte does not.
    assert_eq!(
        live, packed,
        "the packed-flag mirror is {packed} B against the declaration's {live}; `BucketFlags` is \
         not laying out as the byte it wraps"
    );
    assert_eq!(
        8,
        loose - live,
        "folding the five flags into one byte is priced at eight bytes a bucket; it measured {}",
        loose - live
    );

    // --- The one still on the table. ---
    assert_eq!(
        16,
        live - hoisted,
        "hoisting the two transient claims is priced at sixteen bytes a bucket; it measured {}",
        live - hoisted
    );

    // --- WHAT THE INLINE ENTRY COST, AND WHY BOXING IT TURNED OUT TO BE THE ANSWER. ---
    //
    // THIS ROW USED TO PRICE A PROPOSAL AND NOW PRICES A CHANGE THAT HAPPENED. The page index held
    // its single page INLINE, and the row asked whether that entry should go behind a POINTER. The
    // answer was no, on a measurement, and the measurement was taken over a population where every
    // bucket held exactly one page -- which was a property of `load_shard` defaulting the routing
    // range to the whole `u32` keyspace, not of any workload. #1973 made 1023 the default, the
    // population inverted, and the entry went behind the pointer after all.
    //
    // AND THE INTERESTING READING IS THAT BOXING COSTS THE SAME WIDTH AS DROPPING THE ARM. Measured,
    // a boxed single-page arm lays out in the SAME bytes as a two-arm shape with no single-page case
    // at all -- the compiler finds room for the third tag without growing the enum -- so boxing
    // recovers exactly the width dropping would. The two are separated by the HEAP, not by the
    // struct: a boxed entry is one allocation of the entry's own size, and a one-entry list is one
    // allocation of the growth policy's first block. `inline_arm_trade.rs` measures both at both
    // routing ranges in both allocator columns rather than ranking them here.
    //
    // THE CHUNK FIGURE STAYS DERIVED, WHICH IS #1974's CORRECTION AND IT SURVIVES THIS CHANGE. It
    // was the literal 112 that a 104-byte entry needed, and the entry has been 88 and is now 72 --
    // so the literal had been describing a shape this engine did not have for two changes. glibc
    // serves a request from a chunk of `max(32, round_up(request + 8, 16))`, and #1969 corrected
    // that to a FLOOR rather than an equality: a 104-byte request read 128 against the documented
    // 112. It is a FLOOR that is wanted here, and it is printed rather than added to a struct width,
    // because the pair arithmetic that used to consume it was pricing a proposal that has since been
    // taken.
    const ALLOCATOR_CHUNK_FOR_A_BLOCK_ENTRY: usize =
        if size_of::<BlockIndex>() + 8 < 32 { 32 } else { (size_of::<BlockIndex>() + 8 + 15) / 16 * 16 };
    assert!(
        ALLOCATOR_CHUNK_FOR_A_BLOCK_ENTRY >= size_of::<BlockIndex>(),
        "the derived chunk floor is {ALLOCATOR_CHUNK_FOR_A_BLOCK_ENTRY} B for a {} B entry, which is \
         below the entry itself and would under-price every boxed arm",
        size_of::<BlockIndex>()
    );
    assert_eq!(
        inline_block - live,
        size_of::<MirrorInlineBlockIndexMap>() - size_of::<BlockIndexMap>(),
        "the node's saving must be exactly the page index's: it is {} B against the index's {} B, \
         so something other than the arm moved between these two mirrors",
        inline_block - live,
        size_of::<MirrorInlineBlockIndexMap>() - size_of::<BlockIndexMap>()
    );
    assert!(
        inline_block > live,
        "the node with the entry held inline is {inline_block} B against the shipped {live} B; if \
         the two are equal the inline entry was never costing width and the change that boxed it \
         bought nothing"
    );
    println!(
        "\n  boxing the inline entry took the node {inline_block} -> {live} B ({:+} B a bucket), \
         which is exactly the page index's own {} -> {} B",
        live as i64 - inline_block as i64,
        size_of::<MirrorInlineBlockIndexMap>(),
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        live, boxed,
        "re-adding a boxed single-page arm reads as {boxed} B against the shipped {live} B. \
         Measured, the two lay out identically -- the third arm's tag rides the same niche -- and if \
         they have separated then boxing has started to cost or save width on its own and this row \
         is no longer describing why the two were decided on the heap instead"
    );
    assert!(
        boxed < inline_block,
        "a boxed arm is {boxed} B against {inline_block} B held inline; boxing is supposed to \
         recover the width, which is what made it a candidate at all"
    );
    println!(
        "  re-adding that arm BOXED would cost {boxed} B -- the SAME width as the shipped shape, \
         so the two differ only on the heap: a boxed entry is one allocation of {} B, a one-entry \
         list is one allocation of the growth policy's first block",
        size_of::<BlockIndex>()
    );
    println!(
        "  the page entry is {} B and its derived chunk FLOOR is \
         {ALLOCATOR_CHUNK_FOR_A_BLOCK_ENTRY} B; what a chunk holding one actually costs is READ from \
         the allocator in `inline_arm_trade.rs`, over the real bucket population, rather than \
         assumed here -- a carried chunk figure is how a published byte table in this campaign got \
         its sign wrong",
        size_of::<BlockIndex>()
    );
}

// -------------------------------------------------------------------------------------------
// THE STORED SHAPE, DRIVEN IN BOTH DIRECTIONS.
// -------------------------------------------------------------------------------------------

/// A bucket node with every field set to something distinguishable, for the wire tests.
fn wire_fixture(ttl_ms: Option<u64>) -> BucketNode {
    BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::SingleBlockObject,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, false).with(BucketFlags::DELETED, false).with(BucketFlags::META_LOADED, true).with(BucketFlags::LOADING, false).with(BucketFlags::IN_MEMORY, true),
        ttl_ms: BucketTtl::from_ms(ttl_ms),
        dirty_generation: 3,
        first_dirty_wal_sequence: 41,
        first_dirty_index_log_sequence: 42,
        object_index: [42u64].into_iter().collect(),
        deleted_object_index: DeletedObjectIndex::default(),
        block_index: BlockIndexMap::default(),
        // NO TOMBSTONE IN THIS FIXTURE, so the wire tests measure the ordinary node: the key is
        // skipped when the rows are empty, which is what keeps a bucket that has never had a
        // removal spelling exactly what it always did.
        tombstone_elements: TombstoneElements::default(),
    }
}

/// THE STORED SPELLING OF A BUCKET NODE DID NOT MOVE.
///
/// This is the constraint the whole subject runs into, and it is why the accounting above ends
/// where it does: `BucketNode` is written into the shard index, so its field names ARE a stored
/// format and a resident-layout change that alters one of them is not a resident-layout change.
///
/// The countdown is the one field this change touched, and it is held differently in memory and
/// written identically to disk. Both directions are DRIVEN rather than argued:
///
///   * NEW WRITE. The node serializes to the exact bytes below -- `ttl_ms` as a bare number or
///     `null`, in the same position, with no key added and none removed.
///   * OLD READ. The spellings an index written before this change can contain -- a number, a
///     zero, `null`, and the key absent entirely -- all load, and load to the value they meant.
///
/// THE ZERO IS THE CASE THAT MATTERS. A countdown of zero is a bucket whose expiry is due, and
/// the biased representation has to keep it distinct from absent; a version of this change that
/// read zero as absent passes every other assertion in this module.
#[test]
fn the_stored_spelling_of_a_bucket_node_did_not_move() {
    // --- NEW WRITE: the exact bytes. ---
    // `wire_fixture` HOLDS NO PAGES, so there is no object to name in any arm of this module.
    let with_ttl =
        serde_json::to_string(&crate::engine::state::NodeNamedBy(&wire_fixture(Some(5_000)), &[])).expect("a node serializes");
    assert_eq!(
        "{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\"deleted\":false,\
         \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
         \"dirty_generation\":3,\"object_index\":[42],\
         \"deleted_object_index\":[],\"page_index\":{}}",
        with_ttl,
        "the stored spelling of a bucket node moved"
    );
    // TWELVE KEYS, NOT THIRTEEN, AND THAT IS THE DELIBERATE PART OF THIS CHANGE.
    // `last_dump_sequence` is not written any more -- the node does not hold it. It is still
    // ACCEPTED on the way in, which is what the OLD READ fixtures below carry it for.
    assert!(
        !with_ttl.contains("last_dump_sequence"),
        "the node still writes a last_dump_sequence it does not hold: {with_ttl}"
    );
    let without =
        serde_json::to_string(&crate::engine::state::NodeNamedBy(&wire_fixture(None), &[])).expect("a node serializes");
    assert!(
        without.contains("\"ttl_ms\":null"),
        "an absent countdown must still be written as null, not omitted: {without}"
    );
    let at_zero =
        serde_json::to_string(&crate::engine::state::NodeNamedBy(&wire_fixture(Some(0)), &[])).expect("a node serializes");
    assert!(
        at_zero.contains("\"ttl_ms\":0"),
        "a countdown of zero must be written as 0, not as null and not as 1: {at_zero}"
    );

    // --- The two sides must not agree by accident. ---
    assert_ne!(
        without, at_zero,
        "absent and zero must not write the same bytes, or the wire has lost the distinction \
         this representation was built to keep"
    );

    // --- OLD READ: every spelling an index written before this change can hold. ---
    for (stored, expected) in [
        ("5000", Some(5_000u64)),
        ("0", Some(0)),
        ("null", None),
        ("18446744073709551615", Some(u64::MAX - 1)),
    ] {
        let json = format!(
            "{{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\
             \"deleted\":false,\"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\
             \"ttl_ms\":{stored},\"dirty_generation\":3,\"last_dump_sequence\":11,\
             \"object_ids\":[42],\"page_refs\":{{}}}}"
        );
        let node: BucketNode = serde_json::from_str(&json).expect("a stored node loads");
        assert_eq!(
            expected,
            node.ttl_ms.ms(),
            "a stored countdown of {stored} loaded as {:?}",
            node.ttl_ms.ms()
        );
    }

    // The key absent entirely: an index older than the field.
    let missing = "{\"routing_slot\":7,\"layout\":\"Empty\",\"dirty\":false,\"deleted\":false,\
                   \"meta_loaded\":true,\"loading\":false,\"in_memory\":false,\
                   \"dirty_generation\":0,\"last_dump_sequence\":0}";
    let node: BucketNode = serde_json::from_str(missing).expect("a node without the key loads");
    assert_eq!(None, node.ttl_ms.ms(), "a missing countdown key must load as absent");

    // --- ROUND TRIP, at the values that discriminate. ---
    for ms in [None, Some(0), Some(1), Some(5_000), Some(u64::MAX - 1)] {
        let node = wire_fixture(ms);
        let json = serde_json::to_string(&crate::engine::state::NodeNamedBy(&node, &[])).expect("a node serializes");
        let back: BucketNode = serde_json::from_str(&json).expect("a node loads");
        assert_eq!(ms, back.ttl_ms.ms(), "a countdown of {ms:?} did not round-trip");
    }
}

/// THE BIAS SATURATES AT THE TOP, AND NOWHERE ELSE.
///
/// A representation that gains a byte by giving up a value has to say which value, and has to
/// come back wrong in a direction that reads as wrong. Every countdown below `u64::MAX` survives
/// exactly; `u64::MAX` alone comes back one millisecond short, which is 584,542,046 years after
/// the deadline either way.
///
/// THE PROBE VALUES ARE CHOSEN SO TRUNCATION AND SATURATION DISAGREE. A wrapping bias would turn
/// `u64::MAX` into a countdown of zero -- an expiry that is due -- which is both plausible and
/// wrong, and is the mutant this test exists to kill.
#[test]
fn the_biased_countdown_saturates_at_the_top_and_is_exact_below_it() {
    for ms in [0u64, 1, 2, 5_000, 1 << 32, u64::MAX - 2, u64::MAX - 1] {
        assert_eq!(
            Some(ms),
            BucketTtl::from_ms(Some(ms)).ms(),
