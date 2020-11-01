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
