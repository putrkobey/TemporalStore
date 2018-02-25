// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PAGE INDEX ENTRY WEIGHS AT EACH STEP -- AND STEP ONE HAS LANDED.
//!
//! The target shape was twenty-four bytes carrying NO IDENTITY OF ITS OWN: a flag group, a model
//! spelling, and an address, with the identity supplied by the position the entry is held at. THE
//! ENTRY IS THAT SHAPE. `engine_storage_state.rs` pins it at twenty-four with nineteen bytes of field and five of
//! slack, and the identity is resolved through `BlockKeysByPosition` from the position the page is
//! filed at.
//!
//! # SO STEP ZERO AND STEP ONE ARE ONE SHAPE, AND THE MIRROR FOLDED INTO THE LIVE TYPE
//!
//! `StepOneTheIdentityLeaves` existed to measure what the entry WOULD weigh with the identity
//! gone. It is gone, so the thing that mirror was a model of is the live entry, and two
//! declarations of one shape are two things to keep in step. The mirror is deleted and step zero
//! measures the live type; every row that was "composed on step one" is composed on step zero,
//! which is the same field set under its real name.
//!
//! THAT IS THE SECOND FOLD THIS FILE HAS TAKEN FOR THE SAME REASON, and `engine_storage_state.rs` has taken two:
//! a probe whose justification is "the number has to be real before anyone starts" is spent the
//! moment the number is the live type's own.
//!
//! This file still measures what each REMAINING step costs, because every per-field reading of
//! this structure in this campaign has been wrong in the same direction -- `size_of` steps at a
//! word, so one to seven bytes of field saving moves it by nothing, and two savings that are each
//! worth nothing can compose to a whole word.
//!
//! # EVERY MIRROR HERE DECLARED TWO FIELDS THE LIVE ENTRY NO LONGER HAS
//!
//! They carried `kind: IndexItemKind` and `routing_bucket: u32` -- five bytes between them -- and
//! the commit that removed both from `BlockIndex` did not remove them from here. THE CONTROL WENT
//! ON PASSING, which is the whole reason this is written down rather than quietly fixed: the live
//! entry went from forty bytes of field to thirty-five, forty and thirty-five BOTH ROUND TO FORTY,
//! and the step-zero mirror kept measuring the live entry while describing a field set the live
//! entry had lost. The control's own doc says it discriminates at word granularity and no finer;
//! this is that limit being reached in practice rather than in a note.
//!
//! WHAT THAT HID WAS NOT ONE NUMBER BUT THREE CONCLUSIONS, each now restated from a measurement:
//!
//!   * step one had "twenty-four bytes of field in twenty-four ... no slack for packing to
//!     reclaim". It is NINETEEN bytes of field in twenty-four, with FIVE bytes of slack;
//!   * step two said packing "buys nothing". Packed, the same field set measures NINETEEN -- the
//!     step is worth five bytes, not zero;
//!   * step two-A said a declared order "gives up the width step" at forty. It measures
//!     THIRTY-TWO: the penalty is one word, not two.
//!
//! The widths of steps zero, one, three and four did not move at all, which is why the staleness
//! was invisible: the five bytes the retired fields contributed were absorbed by the rounding in
//! every one of those rows.
//!
//! # WHY A MIRROR AND NOT THE LIVE TYPE
//!
//! Each step is a STRUCT DECLARED HERE with the field set that step would leave behind. The live
//! entry cannot be measured at a width it does not have yet, and a prose decomposition is the one
//! form of this that nothing can fail. What makes these mirrors evidence rather than arithmetic is
//! the pair of controls below: the step-zero mirror must measure the LIVE entry exactly, and a
//! deliberately wrong mirror must NOT -- so the comparison is shown to discriminate before any
//! step is read off it.
//!
//! AND THE STEP-ZERO CONTROL NOW READS OFFSETS AND NOT ONLY WIDTHS, for the reason the staleness
//! above gives: a width is a sum, a sum hides its terms, and two different field lists round to
//! the same width. An offset names one term and where it sits, so a mirror carrying a field the
//! live entry does not have cannot place the rest where the live entry places them. The field sum
//! is pinned beside the widths as well, but it is a different claim -- it moves when a field's own
//! WIDTH moves, not when the list does.
//!
//! # ON THE SHAPE OF THE PINS
//!
//! Each width is pinned with `==` and each alignment with its own `==`. No neighbouring `!=` is
//! written beside a width: `size_of` is always a multiple of `align_of`, so a `== N` entails every
//! neighbour inequality and the neighbour asserts nothing. The alignment pin is NOT dominated by
//! the width pin, because a width admits every alignment that divides it, which is exactly why it
//! is written separately.
//!
//! The `!=` in the NEGATIVE CONTROL is a different claim and not that pattern: it is asserted
//! about a DIFFERENT TYPE, to prove the instrument can refuse, and nothing else here entails it.

use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// THE FIELD SUM OF THE LIVE ENTRY, built from the types its fields are declared with.
///
/// Spelled as types rather than as a literal so that a field whose own width moves elsewhere fails
/// here instead of leaving this file describing the old one. Every row below is this sum with
/// terms taken away, which is what makes the differences readable as the cost of one step.
const LIVE_FIELD_SUM: usize = size_of::<StoredModelKind>() // model_id
    + size_of::<ElementEntry>()                            // address
    + size_of::<bool>()                                    // dirty
    + size_of::<bool>(); // deleted
const _: () = assert!(LIVE_FIELD_SUM == 19);

/// STEP ZERO: the field set the live entry has, mirrored field for field and in declaration order.
///
/// FOUR fields, nineteen bytes of field in twenty-four, and so five bytes of slack. AND IT IS WHAT
/// STEP ONE USED TO MEASURE: the identity has left, so "the live entry" and "the entry with the
/// identity gone" are one field set and this is it.
///
/// ITS HISTORY IS THE WARNING THE MODULE DOC CARRIES. It read "Seven fields, forty bytes of field
/// in forty" while two of those seven had already left the live entry, then "FIVE fields,
/// thirty-five bytes of field in forty" while the key was still on it. Each was a decomposition
/// that had stopped describing the type, and each survived a leg because `size_of` steps at a
/// word. The OFFSET pins below are what that bought.
#[allow(dead_code)]
struct StepZeroTheLiveEntry {
