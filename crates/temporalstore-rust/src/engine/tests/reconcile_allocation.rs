// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What reconciling a timestamped series map against its block-derived view ALLOCATES.
//!
//! WHY THIS IS A LOAD-PATH MEASUREMENT AND NOT A REFACTOR NOTE. `reconcile_timestamped_series_membership`
//! takes the persisted map by reference, takes the block-derived view by value, and RETURNS A NEW MAP.
//! That signature is what makes it hard to generalise over a container -- but the signature is not the
//! cost. The cost is what the shape forces it to build:
//!
//!   * for every key present in BOTH, a fresh `BTreeMap` collected from the persisted series, because
//!     the merge is expressed as building a new series rather than refreshing addresses in the old one;
//!   * for every key present in the PERSISTED map only, a full deep `clone` of its series;
//!   * and a fresh outer map to put them in.
//!
//! On a normal reload the blocks exist for every key, so the first of those is the dominant one: the
//! function REBUILDS THE WHOLE SERIES MAP in order to refresh addresses inside it. Mutating the
//! persisted map in place refreshes the addresses and allocates none of it -- and the persisted-only
//! loop disappears entirely, because a key nothing touched needs no clone to survive.
//!
//! So the in-place form is not a concession made to get a trait through. It is cheaper on the path
//! every shard load takes, and the generalisation comes free with it. This fixture is here so that
//! stays recorded: a buried win gets reverted by the next person optimising this path, because nothing
//! tells them the shape was load-bearing.
//!
//! ONE ARM IS THE SHIPPED FUNCTION; ONLY THE OTHER IS TRANSCRIBED. The rebuilding form no longer
//! exists in the tree, so pricing it needs a transcription -- and a transcription that had drifted
//! would price a function nobody runs. So it is asserted to produce the SAME MAP as the shipped form
//! on the same inputs, over all-covered, half-covered and none-covered, plus a check that a covered
//! address really did change so the agreement cannot be two forms agreeing on having done nothing.
//! The in-place arm is NOT transcribed: it delegates to the shipped function, because a fixture that
//! prices a copy of live code can agree with itself forever while the live code moves. A worked
//! example in a test is not inert, and confining it to the arm that has no original is what makes it
//! safe.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::block_store::ElementEntry;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

type Series = BTreeMap<u64, ElementEntry>;
type SeriesMap = HashMap<String, Series>;

fn address(id: u64) -> ElementEntry {
    ElementEntry::from_parts(id, 0, 64, Some(1), Some(id))
}

/// The shape the function had before this change, transcribed so both can be measured at once.
fn reconcile_by_rebuilding(persisted: &SeriesMap, block_derived: SeriesMap) -> SeriesMap {
    let mut result: SeriesMap = HashMap::new();
    for (key, block_series) in block_derived {
        match persisted.get(&key) {
            Some(persisted_series) => {
                let merged = persisted_series
                    .iter()
