// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE RESIDENT BUCKET INDEX HOLDS, AGAINST WHAT IT REPORTS.
//!
//! `bucket_index_resident_bytes` is the MOVING index figure. It is published to an operator under
//! that name, and -- the part that decides behaviour rather than a dashboard row -- it is a term
//! in the eviction pressure score `storage_manager_cycle` reads to decide whether to release a
//! bucket at all. A figure that reads low makes the engine hold an index it should have released.
//!
//! It is derived, not measured: `nodes * size_of::<BucketNode>() + pages * size_of::<BlockIndex>()`.
//! This module measures the same index four ways -- two corpus sizes crossed with the two routing
//! ranges this engine ships -- against the counting allocator, and reports the shortfall.
//!
//! THREE THINGS THE DERIVATION WAS EXPECTED TO MISS, AND WHAT THE ARITHMETIC BELOW DOES ABOUT
//! EACH. The measured findings are printed by
//! `the_resident_index_report_is_measured_against_the_allocator`.
//!
//!   1. STRIDE, NOT ENTRY WIDTH -- AND IT IS ARM-DEPENDENT, WHICH IS WHY IT IS NOT A CONSTANT.
//!      `BlockIndexMap` has three arms. `Many` holds `Vec<(u64, BlockIndex)>`, whose element is
//!      the PAIR: `size_of::<(u64, BlockIndex)>()`, not `size_of::<BlockIndex>()`. `One` holds
//!      `Box<BlockIndex>` -- the handle rides in the enum, so that arm's allocation really is one
//!      entry wide and the shipped arithmetic is RIGHT for it. Charging the pair stride to every
//!      page would over-charge a single-page bucket, and on the whole-keyspace range EVERY bucket
//!      is single-page. No per-page constant can be correct at both ranges, in either direction,
//!      which is the actual defect: the report reads a container's cost off a type name instead
//!      of off the container.
//!   2. CAPACITY, NOT LENGTH. `reserve_one_more` steps the page list by `BLOCK_LIST_GROWTH_STEP`
//!      with `reserve_exact`, so a list of five entries owns a buffer of eight. The report counts
//!      `len()`.
//!   3. NOTHING OUT OF LINE IS COUNTED. `object_key: Arc<str>` and `component: Option<Arc<str>>`
//!      are fat pointers; the bytes behind them are not in `size_of::<BlockIndex>()`. The LENGTH
//!      rides in the fat pointer, so charging them costs no heap access -- but an `Arc` is SHARED,
//!      and a walk that charges one per entry charges a shared name once per holder. The census
//!      below counts distinct allocations by data pointer and reports both figures.
//!
//! AND ONE NOTHING DERIVED CAN FIX, WHICH THE FIXED REPORT SAYS IN ITS OWN DOC. `ALLOC_BYTES`
//! charges `layout.size()`; `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, and #1969 measured a
//! 104-byte request served from a 128-byte chunk. A request-bytes estimate is a FLOOR on what the
//! allocator holds. The fixed report charges request bytes, because that is the only quantity it
//! can compute without asking the allocator, and states the measured shortfall rather than
//! modelling a rounding rule into a production figure.
//!
//! WHY THIS IS DERIVED AND NOT COUNTED. Exact accounting in this engine lives behind
//! `--features alloc-probe`, which installs a counting global allocator and says in its own doc
//! that it is off by default and never part of a normal `cargo test`. A global allocator wrapper
//! adds two atomics to EVERY allocation in the process, which is not a cost a serving binary can
//! carry for an index figure. So the shipped number stays derived. What this module adds is the
//! calibration: the derived figure is checked against the exact one here, so its error is known
//! and signed rather than unknown.
#![allow(clippy::all)]
use super::bucket_fill::{
    engine_on, load_on, seed_container, seed_routed, LARGE, NARROW_END, SMALL, WIDE_END,
};
use super::*;
use crate::engine::state::{BlockIndex, BlockIndexMap, BucketNode, BLOCK_LIST_GROWTH_STEP};
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled at the call site: the counting-allocator gate walks back
// from every line quoting the probe's full path to the nearest `#[test]`, and a helper spelling it
// out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The two counter words `Arc` puts in front of its payload: `strong` and `weak`, both `usize`.
///
/// Derived from `size_of::<usize>()` rather than written as 16, so a target with a different
/// pointer width moves it.
pub(super) const ARC_HEADER_BYTES: usize = 2 * std::mem::size_of::<usize>();

/// What one `Arc<str>` of `len` payload bytes asks the allocator for.
///
/// THE ROUNDING IS NOT DECORATION AND IT WAS NOT GUESSED. This was first written as
/// `ARC_HEADER_BYTES + len` and `the_arc_payload_rule_used_here_is_recovered_from_the_allocator`
/// refuted it on the first length it tried: a one-byte name charged 24 bytes, not 17. A `Layout`
/// size is always a multiple of the alignment, and the counter words make the inner value
/// `usize`-aligned, so the payload rounds up to a whole word before the allocator ever sees it.
/// The control is why this rule is a reading rather than a belief.
pub(super) fn arc_str_request(len: usize) -> usize {
    let align = std::mem::align_of::<usize>();
    (ARC_HEADER_BYTES + len).div_ceil(align) * align
}

// =============================================================================================
// THE WALK
// =============================================================================================

/// One shard's page index, walked from the containers rather than from a type name.
#[derive(Debug, Default, Clone)]
pub(super) struct IndexWalk {
    pub(super) buckets: u64,
    pub(super) blocks: u64,
    pub(super) arm_empty: u64,
    pub(super) arm_one: u64,
    pub(super) arm_many: u64,
    /// Summed `len()` over `Many` arms.
    pub(super) list_len: u64,
    /// Summed `capacity()` over `Many` arms -- what the buffer was allocated at.
    pub(super) list_capacity: u64,
    /// Request bytes of the page-list buffers and the boxed single entries.
    pub(super) block_heap_request: u64,
    /// Distinct heap allocations behind `object_key`, counted by data pointer.
    pub(super) key_allocs: u64,
    /// Request bytes of those distinct key allocations.
    pub(super) key_request: u64,
    /// The same names charged once per ENTRY rather than once per allocation -- what a walk with
    /// no set of seen pointers would charge.
    pub(super) key_request_per_entry: u64,
    pub(super) component_allocs: u64,
    pub(super) component_request: u64,
    pub(super) component_request_per_entry: u64,
    /// Distinct key allocations whose strong count exceeds the number of index entries holding
    /// them -- that is, which something OUTSIDE this index also owns.
    pub(super) keys_held_outside: u64,
    /// The same census for component names.
    pub(super) components_held_outside: u64,
    /// Page-list length histogram over the `Many` arms: length -> how many buckets.
    pub(super) list_lengths: BTreeMap<usize, u64>,
    /// The REQUEST size of every allocation the page index owns, one entry per allocation.
    ///
    /// Kept so the chunk column can be measured rather than modelled: replaying these sizes
    /// through the counting allocator reads what the allocator actually set aside for this exact
    /// size distribution, which a single average could not.
    pub(super) alloc_sizes: Vec<usize>,
}

impl IndexWalk {
    /// Nodes, inline. The term the fix does not touch, and the one the control reads.
    pub(super) fn node_inline_request(&self) -> u64 {
        self.buckets
            .saturating_mul(std::mem::size_of::<BucketNode>() as u64)
    }

    /// What the SHIPPED report answers: nodes by node width plus pages by ENTRY width.
    pub(super) fn reported_before(&self) -> u64 {
        self.node_inline_request().saturating_add(
            self.blocks
                .saturating_mul(std::mem::size_of::<BlockIndex>() as u64),
        )
    }

    /// Everything the containers hold out of line that the entry width excludes.
    pub(super) fn out_of_line_request(&self) -> u64 {
        self.key_request.saturating_add(self.component_request)
    }

    pub(super) fn percentile(&self, q: f64) -> usize {
        let total: u64 = self.list_lengths.values().sum();
        if total == 0 {
            return 0;
        }
        let want = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (len, count) in self.list_lengths.iter() {
            seen += count;
            if seen >= want {
                return *len;
            }
        }
        self.list_lengths.keys().next_back().copied().unwrap_or(0)
    }

    pub(super) fn max_list(&self) -> usize {
        self.list_lengths.keys().next_back().copied().unwrap_or(0)
    }
}

/// Walk shard 1's bucket index and account for every allocation it owns.
pub(super) fn walk(engine: &TemporalEngine) -> IndexWalk {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut out = IndexWalk::default();
    // Distinct allocations, by DATA POINTER. Two entries of one object share one allocation and it
    // must be charged once, which is exactly what a per-entry charge gets wrong.
    let mut seen_keys: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    let mut seen_components: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    let pair_stride = std::mem::size_of::<(u64, BlockIndex)>() as u64;
    let entry_width = std::mem::size_of::<BlockIndex>() as u64;

    for bucket in shard.bucket_index.bucket_map.values() {
        out.buckets += 1;
        match &bucket.block_index {
            BlockIndexMap::Empty => out.arm_empty += 1,
            BlockIndexMap::One(_, _) => {
                out.arm_one += 1;
                // The handle rides in the enum; the allocation is one entry wide.
                out.block_heap_request += entry_width;
                out.alloc_sizes.push(entry_width as usize);
            }
            BlockIndexMap::Many(blocks) => {
                out.arm_many += 1;
                out.list_len += blocks.len() as u64;
                out.list_capacity += blocks.capacity() as u64;
                out.block_heap_request += blocks.capacity() as u64 * pair_stride;
                if blocks.capacity() > 0 {
                    out.alloc_sizes
                        .push(blocks.capacity() * pair_stride as usize);
                }
                *out.list_lengths.entry(blocks.len()).or_insert(0) += 1;
            }
        }
        for (_handle, _block) in bucket.block_index.iter() {
            out.blocks += 1;
            // NO KEY REQUEST PER ENTRY EITHER, AND THIS CENSUS IS THE PAGE INDEX'S OWN BYTES.
            //
            // This asked the allocator-request model what one entry's `Arc<str>` of key text
            // costs, and deduplicated by pointer so that pages of one object charged it once.
            // The entry has no key, so the page index requests NOTHING on that account -- the
            // counter is a structural zero and the ownership pass below has nothing to attribute.
            //
            // THE BYTES DID NOT VANISH AND THIS IS NOT WHERE THEY ARE. One allocation per
            // (kind, object) lives in the bucket index's object lookup, which this walk does not
            // enter: it walks `bucket_map` and reports what the PAGE INDEX holds resident.
            // `index_bytes_per_key::walk_bucket_index_strings` is the walk that enters both and
            // charges each allocation once, and it is where the key text is counted now.
            //
            // THE COUNTER IS KEPT AT ZERO RATHER THAN REMOVED because the arms below print it as
            // a column of the per-entry request table, beside the page-list and entry strides
            // that still have a subject.
            //
            // NO COMPONENT REQUEST PER ENTRY: the entry carries no element name, so it asks the
            // allocator for nothing on that account. The counter and the `seen_components` census
            // therefore stay at zero by construction rather than by measurement.
        }
    }
    // THE SECOND PASS IS GONE WITH THE FIRST, AND ITS THREE COUNTERS ARE STRUCTURAL ZEROES.
    //
    // It existed to settle how many entries held each key allocation, which can only be known
    // after the whole index has been walked, and then to ask whether anything OUTSIDE the page
    // index held that allocation too -- `strong_count` against the count of entries holding it.
    // With no key on the entry, `seen_keys` is empty, `key_allocs` and `key_request` are zero,
    // and `keys_held_outside` has no question left: the page index holds no key allocation at
    // all, so there is no "outside" to compare against. Every key in this store is held outside
    // it, which is the change rather than a finding this census could report.
    //
    // RETIRED RATHER THAN LEFT RUNNING OVER AN EMPTY MAP. A loop over `seen_keys` would still
    // compile, still pass, and print three zeroes that read as a measurement.
    debug_assert!(
        seen_keys.is_empty(),
        "the first pass recorded a key allocation on a page entry, which has no key field"
    );
    out
}

// =============================================================================================
// THE EXACT INSTRUMENT
// =============================================================================================

/// What the allocator gives back when shard 1's page indexes are dropped.
///
/// THE INDEX IS DESTROYED BY THIS. It is the last thing any fixture does.
///
/// WHY A DROP AND NOT A CLONE. A clone of the map charges the `Vec` at its LENGTH, not at the
/// source's capacity, and does not charge the shared names at all -- cloning an `Arc` bumps a
/// counter and allocates nothing. A drop frees the `Vec` at the capacity it actually owns, and
/// frees a name exactly when this index held its last owner, which is the same question the
/// ownership census asks from the other side.
///
/// Returns `(free_bytes, frees)` with the carrier vector's own free already discharged.
#[cfg(feature = "alloc-probe")]
pub(super) fn freed_by_dropping_block_indexes(engine: &TemporalEngine) -> (u64, u64) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let buckets = shard.bucket_index.bucket_map.len();
    // Allocated OUTSIDE the probe window and never grown inside it, so the carrier contributes
    // exactly one free of a known size and nothing else.
    let mut taken: Vec<BlockIndexMap> = Vec::with_capacity(buckets);
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        taken.push(std::mem::take(&mut bucket.block_index));
    }
    assert_eq!(
        taken.capacity(),
        buckets,
        "the carrier grew inside the measurement and its own buffer is now an unknown term"
    );
    let carrier_bytes = (taken.capacity() * std::mem::size_of::<BlockIndexMap>()) as u64;
    let probe = Probe::start();
    drop(taken);
    let counts = probe.stop();
    (
        counts.free_bytes.saturating_sub(carrier_bytes),
        counts.frees.saturating_sub(1),
    )
}

/// What the allocator SETS ASIDE for a list of request sizes, against what was asked for.
///
/// THE CHUNK COLUMN IS MEASURED HERE, NOT MODELLED INTO THE SHIPPED FIGURE. `ALLOC_BYTES` charges
/// `layout.size()`; `ALLOC_CHUNK_BYTES` reads `malloc_usable_size` and adds the header word. The
/// two differ by a rounding that is a FLOOR above the request and not an equality -- #1969 measured
/// a 104-byte request served from a 128-byte chunk -- so the shipped report, which can only compute
/// requests, is a floor on what the process actually holds. This replays the page index's OWN size
/// distribution so the size of that floor is a reading rather than an average applied to a guess.
///
/// Returns `(request_bytes, chunk_bytes)`.
#[cfg(feature = "alloc-probe")]
pub(super) fn chunk_for_sizes(sizes: &[usize]) -> (u64, u64) {
    // Allocated OUTSIDE the window and never grown inside it, so the carrier contributes nothing.
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(sizes.len());
    let probe = Probe::start();
    for size in sizes {
        held.push(Vec::<u8>::with_capacity(*size));
    }
    let counts = probe.stop();
    assert_eq!(
        held.capacity(),
        sizes.len(),
        "the carrier grew inside the measurement and its own buffer is now an unknown term"
    );
    std::hint::black_box(&held);
    drop(held);
    (counts.alloc_bytes, counts.chunk_bytes)
}

// =============================================================================================
// THE INSTRUMENT'S OWN CONTROLS
// =============================================================================================

/// THE PLANTED MARKER FOR THE DROP INSTRUMENT. Recovered exactly, or every figure below is noise.
///
/// The failure this guards against reads as good news: an instrument reporting near zero makes a
/// shape look free.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_drop_instrument_used_here_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let probe = Probe::start();
    drop(marker);
    let counts = probe.stop();
    println!(
        "planted {PLANTED} B, drop instrument returned {} B in {} free(s)",
        counts.free_bytes, counts.frees
    );
    assert_eq!(
        PLANTED as u64, counts.free_bytes,
        "the drop instrument returned {} B for a planted {PLANTED} B; it is not measuring what it \
         is being read as measuring",
        counts.free_bytes
    );
    assert_eq!(
        1, counts.frees,
        "one planted vector freed {} times, not once",
        counts.frees
    );
}

/// AND THE `Arc<str>` RULE IS RECOVERED, NOT ASSUMED.
///
/// The fixed report charges `ARC_HEADER_BYTES + len` for a name held out of line. That is a claim
/// about `Arc`'s layout, which is not a guaranteed thing to know; this recovers it from the
/// allocator at three lengths, so a standard library that changed it fails here rather than
/// silently moving every out-of-line figure this module and the shipped report both charge.
///
/// rust-internal: measures the standard library's allocation for a shared name
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_arc_payload_rule_used_here_is_recovered_from_the_allocator() {
    // ONE is the length that refuted the first version of the rule, and it is kept for that.
    // The other three straddle a word boundary in both directions.
    for len in [1usize, 8, 17, 4096] {
        let source: String = "k".repeat(len);
        let probe = Probe::start();
        let shared: std::sync::Arc<str> = std::sync::Arc::from(source.as_str());
        let counts = probe.stop();
        std::hint::black_box(&shared);
        println!(
            "  Arc<str> of {len} B: allocator charged {} B in {} call(s), rule says {} B",
            counts.alloc_bytes,
            counts.allocs,
            arc_str_request(len)
        );
        assert_eq!(
            arc_str_request(len) as u64,
            counts.alloc_bytes,
            "an Arc<str> of {len} B charged {} B; the rule this module charges out-of-line names \
             with says {} B",
            counts.alloc_bytes,
            arc_str_request(len)
        );
        assert_eq!(
            1, counts.allocs,
            "one shared name took {} allocations",
            counts.allocs
        );
    }
}

// =============================================================================================
// THE MEASUREMENT
// =============================================================================================

/// THE SHIPPED REPORT AGAINST THE ALLOCATOR, AT BOTH RANGES AND TWO CORPUS SIZES.
///
/// Four arms. Each seeds a store, walks its bucket index, reads the SHIPPED figure, then destroys
/// the index inside a probe window and reads back what the allocator actually returned. The
/// denominator on every row is the exact instrument, never another `size_of` product.
///
/// WRITTEN DOWN, BECAUSE A PER-PAGE FIGURE NEEDS ITS POPULATION STATED. The page population is one
/// page per routed string key, asserted against the seed's own key count; the bucket population is
/// decided by the routing range and differs by three orders of magnitude between the two arms,
/// which is the whole reason both are run.
///
/// rust-internal: measures the engine's own index report, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds four stores up to 40,000 records; the counting allocator is process-wide; run by name"]
fn the_resident_index_report_is_measured_against_the_allocator() {
    println!(
        "size_of: BucketNode {} B, BlockIndex {} B, (u64, BlockIndex) {} B, BlockIndexMap {} B, \
         growth step {BLOCK_LIST_GROWTH_STEP}",
        std::mem::size_of::<BucketNode>(),
        std::mem::size_of::<BlockIndex>(),
        std::mem::size_of::<(u64, BlockIndex)>(),
        std::mem::size_of::<BlockIndexMap>()
    );
    // ONE PAGE PER HASH FIELD, so the container arm's page population is keys * members and not
    // one per key. #1959's whole measurement was an artefact of a page population nobody wrote
    // down; both denominators are asserted below against the seed that produced them.
    const MEMBERS: usize = 100;
    for shape in [Shape::Routed, Shape::Container] {
        for records in [SMALL, LARGE] {
            for end_routing_bucket in [WIDE_END, NARROW_END] {
                let dir = tempfile::tempdir().expect("tempdir");
                let engine = engine_on(dir.path());
                load_on(&engine, end_routing_bucket);
                let seeded = match shape {
                    Shape::Routed => seed_routed(&engine, records).len(),
                    Shape::Container => {
                        let keys = seed_container(&engine, records / MEMBERS, MEMBERS);
                        keys.len() * MEMBERS
                    }
                };
                let seen = walk(&engine);
                assert_eq!(
                    seen.blocks, seeded as u64,
                    "{shape:?}/{records}/{end_routing_bucket}: the index holds {} pages for a seed \
                     of {seeded}, so the per-record divisor below is not this fixture's",
                    seen.blocks
                );
                assert!(
                    seen.buckets > 0,
                    "{shape:?}/{records}/{end_routing_bucket}: no buckets to divide by"
                );
                let reported = engine.bucket_index_resident_bytes(1);
                let range = if end_routing_bucket == NARROW_END {
                    "0..1023"
                } else {
                    "whole keyspace"
                };
                println!(
                    "\n== {shape:?}, {records} pages on {range} ==\n  buckets {} (Empty {} / One \
                     {} / Many {}), pages {}, list len {} vs capacity {} (p50 {}, p90 {}, MAX {}, \
                     {} lists)",
                    seen.buckets,
                    seen.arm_empty,
                    seen.arm_one,
                    seen.arm_many,
                    seen.blocks,
                    seen.list_len,
                    seen.list_capacity,
                    seen.percentile(0.50),
                    seen.percentile(0.90),
                    seen.max_list(),
                    seen.list_lengths.values().sum::<u64>()
                );
                println!(
                    "  names: {} distinct key allocations for {} entries ({} B once per \
                     allocation, {} B once per entry), {} also held outside this index; \
                     components {} ({} B once per allocation, {} B once per entry), {} held \
                     outside",
                    seen.key_allocs,
                    seen.blocks,
                    seen.key_request,
                    seen.key_request_per_entry,
                    seen.keys_held_outside,
                    seen.component_allocs,
                    seen.component_request,
                    seen.component_request_per_entry,
                    seen.components_held_outside
                );
                println!(
                    "  nodes inline {} B, page heap at capacity {} B, out of line {} B",
                    seen.node_inline_request(),
                    seen.block_heap_request,
                    seen.out_of_line_request()
                );
                // THE SHIPPED PAGE TERM ON ITS OWN. The whole-report ratio mixes in the node term,
                // which is INLINE in the bucket map and is not freed by the drop below -- comparing
                // the two would be comparing different populations and reading the difference as an
                // error. This is the term the change actually moves.
                let shipped_block_term = seen
                    .blocks
                    .saturating_mul(std::mem::size_of::<BlockIndex>() as u64);
                let (freed_bytes, frees) = freed_by_dropping_block_indexes(&engine);
                assert!(
                    freed_bytes > 0 && frees > 0,
                    "{shape:?}/{records}/{end_routing_bucket}: the drop instrument returned \
                     {freed_bytes} B in {frees} frees; a zero reading is the instrument failing, \
                     not the index being free"
                );
                println!(
                    "  whole report {reported} B (nodes {} + pages {shipped_block_term})",
                    seen.node_inline_request()
                );
                println!(
                    "  PAGE TERM shipped {shipped_block_term} B | walked at capacity {} B | \
                     allocator returned {freed_bytes} B in {frees} frees",
                    seen.block_heap_request
                );
                println!(
                    "    shipped/returned {:.4}   shortfall {} B, {:.2} B a page",
                    shipped_block_term as f64 / freed_bytes as f64,
                    freed_bytes as i64 - shipped_block_term as i64,
                    (freed_bytes as i64 - shipped_block_term as i64) as f64 / seen.blocks as f64
                );
                println!(
                    "    walked/returned  {:.4}   difference {} B",
                    seen.block_heap_request as f64 / freed_bytes as f64,
                    freed_bytes as i64 - seen.block_heap_request as i64
                );
                // THE CHUNK COLUMN, over this index's own allocation size distribution.
                let (replayed_request, replayed_chunk) = chunk_for_sizes(&seen.alloc_sizes);
                assert_eq!(
                    replayed_request, seen.block_heap_request,
                    "{shape:?}/{records}/{end_routing_bucket}: the replay asked for \
                     {replayed_request} B where the walk accounts for {} B, so the chunk column \
                     below is not this index's size distribution",
                    seen.block_heap_request
                );
                println!(
                    "    CHUNK over {} allocations: request {replayed_request} B, allocator set \
                     aside {replayed_chunk} B ({:.4}x, +{} B, +{:.2} B a page)",
                    seen.alloc_sizes.len(),
                    replayed_chunk as f64 / replayed_request as f64,
                    replayed_chunk - replayed_request,
                    (replayed_chunk - replayed_request) as f64 / seen.blocks as f64
                );
                println!(
                    "    shipped/chunk    {:.4}   fixed/chunk {:.4}",
                    shipped_block_term as f64 / replayed_chunk as f64,
                    seen.block_heap_request as f64 / replayed_chunk as f64
                );
                assert!(
                    replayed_chunk >= replayed_request,
                    "{shape:?}/{records}/{end_routing_bucket}: the allocator set aside \
                     {replayed_chunk} B for {replayed_request} B of request, which is below the \
                     request. The chunk counter is not reading what it is documented to read"
                );
                // THE CALIBRATION. The walk is the arithmetic the fixed report ships; the drop is
                // the allocator. If these ever stop agreeing, the shipped figure has acquired an
                // error nobody measured -- which is the state this module exists to leave behind.
                assert_eq!(
                    seen.block_heap_request, freed_bytes,
                    "{shape:?}/{records}/{end_routing_bucket}: the walk accounts for {} B of page \
                     index and the allocator returned {freed_bytes} B. The fixed report ships this \
                     walk, so a difference here is the shipped figure's error and not the test's",
                    seen.block_heap_request
                );
                // AND THE SHIPPED FUNCTION IS THE WALK. Everything above calibrates an
                // arithmetic; this is the assertion that the arithmetic calibrated is the one the
                // engine publishes, at every one of the eight arms rather than at a chosen one.
                assert_eq!(
                    reported,
                    seen.node_inline_request() + seen.block_heap_request,
                    "{shape:?}/{records}/{end_routing_bucket}: the engine published {reported} B \
                     where the walk this module calibrated against the allocator accounts for \
                     {} B. The calibration is then of something other than the shipped figure",
                    seen.node_inline_request() + seen.block_heap_request
                );
                // AND THE NAMES ARE NOT THIS INDEX'S TO CHARGE. The drop above returned the page
                // buffers and NOTHING ELSE: every shared name survived it, because something other
                // than the bucket index still owns one. Charging them to this report would be
                // double counting, which is the correction this arm exists to establish.
                assert_eq!(
                    seen.key_allocs, seen.keys_held_outside,
                    "{shape:?}/{records}/{end_routing_bucket}: {} of {} key allocations are held \
                     ONLY by this index. The fixed report declines to charge out-of-line names on \
                     the measured ground that the index never owns them; if that has stopped being \
                     true, the report is now under-counting by whatever those names weigh",
                    seen.key_allocs - seen.keys_held_outside,
                    seen.key_allocs
                );
            }
        }
    }
