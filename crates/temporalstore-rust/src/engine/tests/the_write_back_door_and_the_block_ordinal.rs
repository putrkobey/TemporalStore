// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-back door, the page ordinal it cannot hold, and the two halves of one contract that
//! used to disagree about it.
//!
//! THE ASYMMETRY. `ObjectRecord`'s inline arm keeps a page ordinal's PRESENCE and not its value,
//! which is correct and measured: a single-page object has page zero by construction, and 400 of
//! 400 live addresses on a real store carried ordinal 0. The conversion path refuses a NONZERO
//! ordinal by name, as `NonZeroBlockOrdinalDoesNotFitInline`. `set_address` -- the compactor's
//! write-back door -- did not refuse it; it wrote the presence bit and dropped the value.
//!
//! WHY NOTHING CAUGHT IT. No test fed either path a nonzero ordinal. The existing fixture at
//! `where_the_characters_are_durable_per_kind` passes `Some(9)` and never reads it back, so the
//! round trip was never compared, so the divergence between the documented behaviour and the actual
//! behaviour was invisible. A named `TODO` would have been no better; an instrument is what tells.
//!
//! rust-internal: guards this crate's own record shape, no product behaviour

use crate::block_store::ElementEntry;
use crate::engine::object_record::{
    dropped_nonzero_block_ordinals, reset_dropped_nonzero_block_ordinals, ObjectRecord,
    ObjectRecordRefusal,
};

/// An entry whose page ordinal is `at`, with everything else fixed so only the ordinal varies.
fn entry_with_ordinal(ordinal: Option<u64>) -> ElementEntry {
    ElementEntry::from_parts(1, 64, 32, ordinal, Some(11))
}

/// THE REFUSING DOOR REFUSES, BY THE SAME NAME THE CONVERSION PATH USES.
///
/// Two halves of one contract have to give one answer. `try_set_address` is the half that was
/// missing: the conversion path already answered
/// `NonZeroBlockOrdinalDoesNotFitInline`, and the write-back door answered nothing at all.
///
/// THE POSITIVE CONTROLS ARE THE POINT OF THE SECOND HALF. A refusal that fires on every input
/// would pass the first assertion just as well, so ordinal zero and ordinal absent must both still
/// be accepted -- and accepted means the address actually lands, not merely that no error came
/// back.
#[test]
fn the_write_back_door_refuses_a_block_ordinal_it_cannot_hold() {
    let mut record = ObjectRecord::default();

    let refusal = record
        .try_set_address(&entry_with_ordinal(Some(7)))
        .expect_err("a nonzero page ordinal has no room in the inline arm and must be refused");
    assert_eq!(
        refusal,
        ObjectRecordRefusal::NonZeroBlockOrdinalDoesNotFitInline { block_id: 7 },
        "the write-back door must refuse by the SAME named variant the conversion path uses, or \
         the two halves of one contract give two different answers to one question"
    );
    assert!(
        refusal.to_string().contains('7'),
        "the refusal does not name the ordinal it refused: {refusal}"
    );

    // --- POSITIVE CONTROL ONE: ordinal zero, which is what a single-page object carries.
    let mut record = ObjectRecord::default();
    record
