// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE EVENT SERIES ON THE OWNED SORTED ARRAY, and the decode-only landing spot beside it.
//!
//! # WHAT IS BEING ASSERTED, AND WHAT IS DELIBERATELY NOT
//!
//! Three claims, each with its own test, because one test covering all three would pass on two:
//!
//!   1. **The array is a timestamped series.** Sorted whatever order points arrive in, including
//!      duplicates and removals, and it gives its allocation back when it empties.
//!   2. **The legacy key has NO DOOR**, which is what retired the compatibility field this module
//!      was built around. The old claim here was that the field is drained and never refilled --
//!      one live copy at every moment after a decode returns. There is no field now, and the
//!      claim that replaced it is stronger: nothing can deliver a top-level `context_events` key.
//!      A pre-stamp image is refused on every one of `decode_index_bytes`'s six callers, and an
//!      image at stamp 20 or later does not carry the key because the series moved into `objects`
//!      there. The second half is the one driven below, because the stamp test is `<` and so cannot
//!      refuse a current-stamp image over a key it does not know -- what makes that safe is that no
//!      binary at or above 20 produces one.
//!   3. **The capture path writes the array map with nothing decoded.** That path is unchanged: a
//!      key-state blob carries the series under its own name and `apply_key_states` converts the
//!      stored B-tree shape into the array, with no decode and no landing spot in between.
//!
//! What is NOT claimed is a footprint win on a live shard. The compatibility field costs nothing
//! after the fold drains it, but the array and the B-tree are never both resident long enough for
//! the shard-level bracket to see either -- and that bracket is seeded with `strings` and
//! `features` and holds no context events at all, so it does not move for this map in either
//! direction. The measurement below prices the two representations against each other directly,
//! which is the honest form of that claim.

use crate::engine::timestamped_series::TimestampedSeries as _;
use super::super::state::ShardState;
use super::super::timestamped_series::OwnedTimestampedSeries;
use crate::block_store::ElementEntry;
use std::collections::BTreeMap;

/// A legal address. `ElementEntry` has no public constructor and `try_from_parts` REFUSES a block
/// id wider than `u16::MAX` rather than truncating it, so the id is masked exactly as the other
/// address fixtures in this crate mask it.
fn address(i: u64) -> ElementEntry {
    ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
}

fn series_of(points: &[u64]) -> BTreeMap<u64, ElementEntry> {
    points.iter().map(|at| (*at, address(*at))).collect()
}

/// Ascending order is an INVARIANT and not a property of the input, so the input is deliberately
/// adversarial: descending, interleaved, duplicated, and then drained.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_owned_array_stays_sorted_whatever_order_the_points_arrive_in() {
    let mut series = OwnedTimestampedSeries::default();
    assert!(series.is_empty(), "a default series holds nothing");
    assert_eq!(
        0,
        series.allocation_count(),
        "an empty series must hold no allocation at all -- a per-key `Vec` header kept for a series \
         with no points is exactly the per-key constant this representation exists to remove"
    );

    // DESCENDING, then interleaved into the holes between them. A representation that appended
    // and sorted later would pass a test that wrote ascending input.
    for at in [900u64, 700, 500, 300, 100] {
        assert_eq!(None, series.insert(at, address(at)), "{at} is new");
    }
    for at in [800u64, 600, 400, 200] {
        assert_eq!(None, series.insert(at, address(at)), "{at} is new");
    }
    let walked: Vec<u64> = series.keys().copied().collect();
    assert_eq!(
        vec![100u64, 200, 300, 400, 500, 600, 700, 800, 900],
        walked,
        "the walk must be ascending after out-of-order inserts, because `points` PROMISES that \
         order and the stored-image collectors depend on it"
    );
    assert_eq!(9, series.len(), "nine distinct instants were written");

    // A DUPLICATE INSTANT REPLACES, it does not grow the array. A representation that appended
    // would read back the same instant twice and the walk would still be ascending, so the length
    // is what separates the two.
