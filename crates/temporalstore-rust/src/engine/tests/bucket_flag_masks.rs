// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! FIVE FLAGS IN ONE BYTE, AND THE ONE WAY THAT GOES WRONG.
//!
//! Packing `dirty`, `deleted`, `meta_loaded`, `loading` and `in_memory` into `BucketFlags` takes
//! `BucketNode` from 184 bytes to 176. It also replaces five independent fields, which the
//! compiler kept apart for free, with five masks over one byte -- and masks are hand-written. A
//! flag that answers another flag's question is not a crash and not a failing type check: it is a
//! bucket that reports itself resident while it is loading, or clean while it is dirty, and the
//! engine acts on the answer.
//!
//! So the packing is only as good as the claim that each accessor reads its OWN bit. This module
//! makes that claim checkable three ways, and every one of them has a control that fails.
//!
//!   1. THE MASKS THEMSELVES. Five single-bit masks, pairwise distinct, five bits set between
//!      them. A duplicated mask -- the copy-paste this shape invites -- is caught here before any
//!      accessor is called.
//!   2. THE ACCESSORS, EXHAUSTIVELY. Not one fixture but all thirty-two states of five bits,
//!      written through the setters and read back through the getters. A fixture that happens to
//!      set two flags the same way cannot tell them apart; thirty-two states can, because every
//!      pair of flags disagrees in some state.
//!      THE CONTROL: `MisMaskedNode` is the same five accessors with `loading` reading
//!      `in_memory`'s bit. It is fed to the SAME checker, and the checker must reject it. A guard
//!      that cannot fail is not a guard, and this one is shown failing.
//!   3. THE WIRE, IN BOTH DIRECTIONS AND ALSO EXHAUSTIVELY. The five flags are five separate
//!      boolean keys of the stored index and one byte in memory, and the hand-written serde impls
//!      are the join between them -- a second place to wire a flag to the wrong name. All
//!      thirty-two states round-trip, and a hand-built index with two of the keys SWAPPED must
//!      decode to a different node than the one that wrote it.
//!
//! WHAT THIS MODULE DOES NOT DO. It does not measure bytes; `per_item_byte_budget` owns the
//! accounting and asserts the reconstruction. It proposes no change. It exists because the change
//! it guards replaced a type-checked fact with an arithmetic one.

#![allow(clippy::all)]

use crate::engine::state::{BucketFlags, BucketNode};

/// The five flags, under the names the engine reads them by, each with its own accessor pair.
///
/// Hand-written on purpose and CHECKED AGAINST `BucketFlags::MASKS` below rather than trusted: a
/// flag added to the declaration and not to this table would otherwise be a flag no test here
/// ever reads, and the count check is what refuses that.
type Getter = fn(&BucketNode) -> bool;
type Setter = fn(&mut BucketNode, bool);

fn accessors() -> Vec<(&'static str, Getter, Setter)> {
    vec![
        ("dirty", BucketNode::dirty, BucketNode::set_dirty),
        ("deleted", BucketNode::deleted, BucketNode::set_deleted),
        (
            "meta_loaded",
            BucketNode::meta_loaded,
            BucketNode::set_meta_loaded,
        ),
        ("loading", BucketNode::loading, BucketNode::set_loading),
        ("in_memory", BucketNode::in_memory, BucketNode::set_in_memory),
    ]
}

// -------------------------------------------------------------------------------------------
// 1. THE MASKS.
// -------------------------------------------------------------------------------------------

/// EVERY MASK IS ONE BIT, AND NO TWO ARE THE SAME BIT.
///
/// This is the whole of the arithmetic the accessors rest on, and it is the check that catches
/// the mistake this shape actually invites: a mask copied from the line above and not edited.
/// Two flags sharing a bit is not a type error and every single-flag fixture still passes.
#[test]
fn the_five_flag_masks_are_five_distinct_single_bits() {
    let masks = BucketFlags::MASKS;
    assert_eq!(
        5,
        masks.len(),
        "BucketFlags::MASKS lists {} flags; the node packs five, and a table that has drifted \
         from the declaration proves nothing about it",
        masks.len()
    );

    println!("\n=== the five masks ===");
    let mut union = 0u8;
    for (name, mask) in masks {
        println!("  {name:<12} {mask:#010b}");
        assert_eq!(
            1,
            mask.count_ones(),
            "the mask for `{name}` is {mask:#010b} and sets {} bits; every flag must be exactly \
             one bit or the accessors cannot be independent",
            mask.count_ones()
        );
        assert_eq!(
            0,
            union & mask,
            "the mask for `{name}` ({mask:#010b}) overlaps a mask already taken ({union:#010b}); \
             two flags on one bit answer each other's questions"
        );
        union |= mask;
    }
    assert_eq!(
        5,
        union.count_ones(),
        "five distinct single-bit masks must cover five bits; these cover {} ({union:#010b})",
        union.count_ones()
    );

    // The table the rest of this module drives has to name the same five.
    let accessor_names: Vec<&str> = accessors().iter().map(|(name, _, _)| *name).collect();
    let mask_names: Vec<&str> = masks.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        mask_names, accessor_names,
        "the accessor table and `BucketFlags::MASKS` name different flags, in different orders \
         or counts; one of them is not describing the node"
    );
}

// -------------------------------------------------------------------------------------------
// 2. THE ACCESSORS, AND A MIRROR THAT GETS THEM WRONG.
// -------------------------------------------------------------------------------------------

/// A flag byte read through five accessors, whichever five those are.
///
/// The live node and the deliberately broken mirror both implement it, so the checker below is
/// the SAME code for both and cannot be accidentally weakened for one of them.
trait FlagView {
    fn fresh() -> Self;
    fn put(&mut self, index: usize, on: bool);
    fn take(&self, index: usize) -> bool;
    /// The packed byte, for diagnostics only.
    fn raw(&self) -> u8;
}

impl FlagView for BucketNode {
    fn fresh() -> Self {
        BucketNode::default()
    }

