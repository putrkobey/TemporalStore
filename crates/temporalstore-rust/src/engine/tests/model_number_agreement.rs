// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TIE BETWEEN THE TWO LISTS OF MODEL SPELLINGS, AND WHAT THE MISSING ONES COST.
//!
//! `index_log::MODEL_ID_NUMBERS` decides which spellings a delta row may write as a NUMBER rather
//! than as a string. `model_kind_registry!` decides which spellings a stored entry may hold at
//! all. They are two hand-maintained lists of the same vocabulary, and the registry already states
//! the hazard about itself: it derives its retired half as the complement of its live half "so it
//! cannot disagree with the declaration".
//!
//! The number list had no such tie, and it did go stale. `"zset"` and `"list"` were added to the
//! registry as LIVE kinds -- taking codes 6 and 12, the two the old packed report had skipped --
//! and nobody added them to the number list, so every index-log row naming either kind wrote its
//! full string spelling instead of one byte. Nothing failed. That is what a second list with no
//! tie costs, and these tests are the tie.
//!
//! WHY THIS LIVES IN `engine` AND NOT BESIDE THE LIST IT GUARDS. `StoredModelKind` is
//! `pub(super)` inside `engine`, and `index_log` sits BELOW `engine` and cannot see it. The guard
//! has to be where both lists are visible, and that is here.
//!
//! AND WHY THE TWO DIRECTIONS ARE TWO TESTS. "Every live spelling can be numbered" and "every
//! numbered spelling is declared" are different failures with different costs -- the first is
//! bytes, the second is a number that decodes to a kind no entry can hold -- and one assertion
//! covering both would report whichever it noticed first.

use crate::engine::storage_bucket_internals::{ModelKind, StoredModelKind};
use crate::index_log::MODEL_ID_NUMBERS;

/// EVERY LIVE SPELLING THE REGISTRY DECLARES CAN BE WRITTEN AS A NUMBER.
///
/// This is the defect `"zset"` and `"list"` were: live kinds the engine writes on ordinary paths,
/// absent from the number list, paying a string on every row. A kind added to the registry and not
/// to the number list fails here rather than quietly costing bytes.
///
/// rust-internal: reads two of the engine's own declarations, no product behaviour
#[test]
fn every_live_model_spelling_can_be_written_as_a_number() {
    let numbered: std::collections::BTreeSet<&str> = MODEL_ID_NUMBERS.iter().copied().collect();
    assert_eq!(
        MODEL_ID_NUMBERS.len(),
        numbered.len(),
        "the number list holds a duplicate spelling, so one position is unreachable: {MODEL_ID_NUMBERS:?}",
    );

    // THE DENOMINATOR, PRINTED. A sweep over an empty set passes while asserting nothing.
    let live: Vec<&'static str> = ModelKind::ALL
        .iter()
        .map(|kind| StoredModelKind::from(*kind).as_str())
        .collect();
    assert!(
        live.len() >= 15,
        "the registry's live half reports {} kinds, too few to be it",
        live.len(),
    );

    let missing: Vec<&str> = live
        .iter()
        .copied()
        .filter(|name| !numbered.contains(name))
        .collect();
    println!(
        "{} live spellings checked against {} numbered",
        live.len(),
        MODEL_ID_NUMBERS.len()
    );
    assert!(
        missing.is_empty(),
        "{} LIVE spelling(s) cannot be written as a number and pay a full string on every row: \
         {missing:?}. Append them to `MODEL_ID_NUMBERS` -- at the END, because the position IS \
         the wire value and inserting renumbers every row already written.",
        missing.len(),
    );
}

/// AND EVERY SPELLING THE NUMBER LIST NAMES IS ONE THE REGISTRY DECLARES, LIVE OR RETIRED.
///
/// A number list naming a spelling the registry has never heard of encodes a row that decodes back
/// to a string no entry can hold -- a number that round-trips into nothing. Retired names are
/// legitimate here: a row written before a retirement still carries one, which is why the registry
/// keeps them rather than deleting them.
///
/// rust-internal: reads two of the engine's own declarations, no product behaviour
#[test]
fn every_numbered_model_spelling_is_one_the_registry_declares() {
    assert!(
        StoredModelKind::ALL.len() >= 17,
        "the registry declares {} spellings across both halves, too few to be it",
        StoredModelKind::ALL.len(),
    );
    assert!(
        !MODEL_ID_NUMBERS.is_empty(),
        "an empty number list would make this sweep vacuous",
    );

    let undeclared: Vec<&str> = MODEL_ID_NUMBERS
        .iter()
        .copied()
        .filter(|name| StoredModelKind::from_stored_name(name).is_none())
        .collect();
    assert!(
        undeclared.is_empty(),
        "the number list names {} spelling(s) the registry does not declare: {undeclared:?}. A \
         number that decodes to a spelling no entry can hold is worse than a string.",
        undeclared.len(),
    );

    // The retired half IS allowed here, and this says so rather than leaving it to be inferred
    // from the sweep passing: a row written before a retirement still names one.
    let retired_and_numbered: Vec<&str> = MODEL_ID_NUMBERS
        .iter()
        .copied()
