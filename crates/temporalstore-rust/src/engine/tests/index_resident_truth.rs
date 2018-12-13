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
