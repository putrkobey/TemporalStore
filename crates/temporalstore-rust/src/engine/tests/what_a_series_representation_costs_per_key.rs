// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SERIES REPRESENTATION COSTS PER KEY, READ FROM THE ALLOCATOR AND NOT FROM THE STRUCTURE.
//!
//! # THE DECISION THIS IS FOR
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)` as
//! `HashMap<String, BTreeMap<u64, ElementEntry>>`, and the campaign to move them off the per-key
//! B-tree has to choose what to move them TO. Two decisions hide inside that one: the CONTAINER
//! (tree against flat array) and, if an array, its GROWTH POLICY (amortised doubling against exact
//! capacity). Folding them into a single comparison hides the larger of the two, so they are
//! measured as separate arms here.
//!
//! This module takes no position on which maps should move, and converts none of them. It prices
//! the candidate shapes so that the choice is made on a reading rather than on arithmetic.
//!
//! # WHY IT IS MEASURED AT THE ALLOCATOR AND NOT FROM `size_of`
//!
//! `size_of` sees the owning field and nothing else, and an allocation count is not a count of
//! BYTES. An allocator serves a request out of a chunk at least as large and usually larger, so a
//! shape that moves bytes out of line pays a header word and the rounding on every allocation, and
//! neither `size_of` nor a call count can see any of the three. The largest single effect in the
//! table below -- a `Vec` whose first allocation holds four elements when one was asked for -- is
//! invisible to both.
//!
//! # WHAT THE INSTRUMENT IS, STATED SO IT IS NOT "CORRECTED" LATER
//!
//! `ALLOC_CHUNK_BYTES` is `malloc_usable_size(ptr) + size_of::<usize>()`. Those are not two
//! candidate instruments to choose between: the chunk column ALREADY adds the header word that a
//! bare usable-size reading omits, so "chunk bytes" and "usable size plus the header" are the same
//! number here and the eight bytes per live allocation are inside it. `ALLOC_BYTES`, the request
//! column, is read beside it as a control, because the difference between the two IS what an
//! out-of-line shape pays and an inline one does not -- and an equality between them would mean
//! the chunk column is not reading the allocator at all.
//!
//! # WHY ELEVEN OCCUPANCIES AND NOT ONE
//!
//! The curves have DIFFERENT STEPS, so any single occupancy describes one regime and misdescribes
//! the others. A `BTreeMap` leaf holds up to eleven pairs before it splits; a doubling `Vec` steps
//! at every power of two. So 9, 10 and 11 are in the set because that is where the tree leaf is
//! nearly full and the doubling array has just overshot -- the one band where a growing array can
//! LOSE outright -- and 65 and 129 sit just past a doubling. A set that skipped them would report
//! a uniform win that is not uniform.
//!
//! # THE PER-ROW ABSOLUTE IS NOT A PROPERTY OF THE REPRESENTATION
//!
//! Bytes per row is a function of the OCCUPANCY DISTRIBUTION as much as of the shape: the same
//! total number of points, redistributed, moves it substantially. So what this table publishes is
//! the RATIO between shapes AT A STATED OCCUPANCY, and every absolute is labelled with the
//! occupancy it was read at. A single per-row figure quoted without its occupancy is the
//! denominator error this table exists to prevent, and three figures that were circulating in
//! earlier prose do not reproduce here at all.

use crate::block_store::ElementEntry;
use std::collections::BTreeMap;

/// A legal address. `ElementEntry` has no public constructor and `from_parts` refuses a block id
/// wider than `u16::MAX` rather than truncating, so the id is masked as the other address fixtures
/// in this crate mask it.
fn address(i: u64) -> ElementEntry {
    ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
}

/// The pair a timestamped series stores: one instant and one page address.
type Point = (u64, ElementEntry);

/// The occupancies, and why each is here. 9/10/11 bracket the B-tree leaf split and are where a
/// doubling array can lose; 65 and 129 sit just past a doubling; 1 is the regime with the most
/// keys; 1000 is the steady state.
#[cfg(feature = "alloc-probe")]
const OCCUPANCIES: [usize; 11] = [1, 2, 5, 9, 10, 11, 12, 65, 100, 129, 1000];

/// Which arm a column holds. Named rather than indexed so a row cannot be read off by position.
#[cfg(feature = "alloc-probe")]
const ARMS: [&str; 5] = [
    "tree_insert",
    "tree_collect",
    "array_doubling",
    "array_exact",
    "array_hybrid",
];

/// WHAT ONE KEY SERIES COSTS IN FIVE SHAPES -- chunk bytes AND allocation count, at eleven
/// occupancies, read from the allocator.
///
/// # THE DENOMINATOR IS ONE KEY
///
/// Every figure is for a SINGLE object key series, measured on the series object itself with no
/// `HashMap` in the span. That is the per-key cost a representation change moves. The inline width
/// of each shape is reported separately and added in, because a shape that is cheaper on the heap
/// can be wider in the map value and the total is what a shard pays.
///
/// rust-internal: measures this crate own resident structures, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn what_one_keys_series_costs_in_five_shapes_at_the_allocator() {
    // NON-VACUITY FIRST, because every row after this is a difference of counter readings and a
    // counter that never moves reports a table of zeros that looks like a free change.
    let probe = crate::alloc_probe::Probe::start();
    let canary: Vec<u64> = (0..64).collect();
    let moved = probe.stop();
    std::hint::black_box(&canary);
    assert!(
        moved.allocs >= 1 && moved.chunk_bytes >= 1,
        "the counting allocator is not installed -- every row below would read zero, and a zero \
         here means the opposite of a free change (allocs {} chunk_bytes {})",
        moved.allocs,
        moved.chunk_bytes
    );
    // AND THE CHUNK COLUMN MUST READ STRICTLY ABOVE THE REQUEST COLUMN for this canary, or it is
    // not reading the allocator and every out-of-line arm below is being charged at request size,
    // which systematically flatters exactly the arms this table exists to separate.
    assert!(
        moved.chunk_bytes > moved.alloc_bytes,
        "the chunk column must read ABOVE the request column -- equality means it is not reading \
         malloc_usable_size and the instrument is not what it says (chunk {} request {})",
        moved.chunk_bytes,
        moved.alloc_bytes
    );

    // THE INLINE WIDTHS, WHICH ARE THE HALF THE HEAP TABLE CANNOT SEE. `Box<[T]>` is a FAT
    // pointer: exact capacity costs eight more bytes in the map value than a boxed `Vec` does,
    // and buys back more than that on the heap. Both halves are reported so neither can be quoted
    // alone.
    let inline_tree = std::mem::size_of::<BTreeMap<u64, ElementEntry>>();
    let inline_doubling = std::mem::size_of::<Option<Box<Vec<Point>>>>();
    let inline_exact = std::mem::size_of::<Option<Box<[Point]>>>();
    println!(
        "instrument: ALLOC_CHUNK_BYTES = malloc_usable_size + {} B header; canary {} B requested, \
         {} B set aside in {} allocation(s)",
