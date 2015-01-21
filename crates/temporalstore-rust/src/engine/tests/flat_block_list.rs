// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAGE LIST OF ONE BUCKET, AS A CONTAINER.
//!
//! `BlockIndexMap::Many` held a `BTreeMap<u64, BlockIndex>` from #654 until #1963. This module is
//! the measurement that replaced it with a flat list, and the guards that make the replacement a
//! container change rather than a behaviour change.
//!
//! WHAT A TREE COSTS HERE. A `BTreeMap` leaf carries eleven value slots whether or not it fills
//! them. At a 104-byte page entry that is 16 + 11x8 + 11x104 = 1,248 bytes of node per eleven
//! pages, and the fill was measured at 63% -- so a page in a tree pays for about one and a half
//! slots. A list pays for its own entry and for whatever capacity slack the growth policy leaves,
//! and the growth policy is something this engine chooses. That is the whole of the argument, and
//! every figure below is a measurement of one side of it.
//!
//! WHAT THE LIST MUST NOT CHANGE, and what each guard here covers:
//!
//!   * ORDER. A `BTreeMap<u64, _>` walks in ascending key order and FIVE production readers take
//!     that order as given -- see `an_unsorted_block_list_would_reorder_every_walk_of_this_index`,
//!     which names them. The list is kept sorted by the same handle, so every walk yields the
//!     identical sequence. This is the central risk of the change and it is answered by the
//!     representation, not by a sort at each reader.
//!   * DUPLICATES. A map deduplicated by construction; a list does not.
//!     `a_second_insert_of_the_same_block_replaces_it_rather_than_adding_beside_it` drives the
//!     rewrite that would otherwise file a page twice, on both arms.
//!   * THE STORED SHAPE. `BlockIndexMap` serializes as the same map of rendered string keys, in
//!     the same string order, that it always did.
//!     `the_stored_block_index_is_byte_for_byte_what_the_tree_wrote` holds that with an injection
//!     control.
//!
//! WHAT THE FIXTURE HAS TO REACH. `TemporalEngine::load_shard` defaults the end routing bucket to
//! `u32::MAX`, and on the whole keyspace every key lands in a bucket of its own BY CONSTRUCTION --
//! no `Many` arm is ever built, so a measurement taken there cannot say anything about the shape
//! this module changes. `docs/runtime_tuning.md` tells an operator to set
//! `TS_SHARD_END_ROUTING_BUCKET=1023`. Both ranges are measured at both corpus sizes, and the
//! narrow arm ASSERTS it reached a multi-page bucket before any figure taken from it is believed.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::{
    bisect_block, block_index_handle, find_block, block_lookup_entries_examined,
    reset_block_lookup_entries_examined, scan_block, BlockIndex, BlockIndexMap, BlockSlabLiveIndex,
    BucketNode,
};
use std::mem::size_of;

/// The shortest list the `Many` arm ever holds. A bucket with one page is held inline and a
/// bucket with none is `Empty`, so no shorter list exists to choose a strategy for.
const SHORTEST_MANY: usize = 2;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023`.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses. Every key lands alone here by construction.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "flat-page-list".to_string(),
        shard_uri: "local://flat-page-list/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("flat-{i:06}")).collect();
    run_batch(
        engine,
        keys.iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
    keys
}

// =============================================================================================
// 1. PRICE THE LENGTH FIRST: THE DISTRIBUTION, NEVER A MEAN
// =============================================================================================

/// Pages held per routing bucket, as bucket counts keyed by pages held.
#[derive(Debug, Default, Clone)]
struct Lengths {
    counts: BTreeMap<usize, usize>,
}

impl Lengths {
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn blocks(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    /// The length at or below which `fraction` of BUCKETS sit.
    ///
    /// Over buckets and not over pages, because the question a representation asks is "how long is
    /// the list I am about to walk", and every bucket asks it once.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let want = (fraction * buckets as f64).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= want {
                return *held;
            }
        }
        self.widest()
    }

    /// Reported ALONGSIDE the percentiles and the maximum, never instead of them. A mean of 1.98
    /// in this engine once contained no bucket holding two.
    fn mean(&self) -> f64 {
        if self.buckets() == 0 {
            0.0
        } else {
            self.blocks() as f64 / self.buckets() as f64
        }
    }

    fn report(&self, label: &str) {
        println!(
            "  {label}: {} buckets, {} pages | p50 {} p90 {} p99 {} MAX {} | mean {:.4}",
            self.buckets(),
            self.blocks(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.widest(),
            self.mean()
        );
    }
}

/// Lengths of the `Many` arm only -- the lists a scan actually walks.
fn many_arm_lengths(engine: &TemporalEngine) -> Lengths {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut lengths = Lengths::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        if matches!(bucket.block_index, BlockIndexMap::Many(_)) {
            *lengths.counts.entry(bucket.block_index.len()).or_default() += 1;
        }
    }
    lengths
}

/// (Empty, One, Many) bucket counts.
///
/// The single-page arm holds a POINTER rather than an inline entry now, so `One` costs eight bytes
/// in the node plus one allocation instead of the width of a whole entry. The three-way split is
/// unchanged, and it is still what the container decision turns on.
fn arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut counts = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            BlockIndexMap::Empty => counts.0 += 1,
            BlockIndexMap::One(_, _) => counts.1 += 1,
            BlockIndexMap::Many(_) => counts.2 += 1,
        }
    }
    counts
}

/// THE ARM HISTOGRAM AND THE PAGES-PER-KEY DISTRIBUTION, AT BOTH RANGES, AT TWO CORPUS SIZES.
///
/// The number a container decision rests on is the LENGTH DISTRIBUTION, and a mean cannot carry
/// it: a scan's cost lives entirely in the tail, and the tail is what a mean hides. So this
/// publishes the arm split, then the percentiles and the MAXIMUM of the `Many` arm, and asserts
/// the fixture reached that arm before any of it is read as a fact about it.
///
/// rust-internal: measures the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds four stores up to 40,000 records each; run by name"]
fn the_block_list_length_distribution_is_reported_as_a_histogram() {
    let mut narrow_reached_many = 0usize;
    let mut widest_seen = 0usize;
    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let (empty, one, many) = arms(&engine);
            let lengths = many_arm_lengths(&engine);
            let range = if end_routing_bucket == WIDE_END {
                "whole".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            println!(
                "{records} records on {range}: arms Empty {empty} / One {one} / Many {many}"
            );
            lengths.report("Many arm");
            for (held, count) in &lengths.counts {
                if *held <= 4 || held % 8 == 0 || *held == lengths.widest() {
                    println!(
                        "      {held:>4} page(s): {count:>6} buckets ({:>7.3}%)",
                        100.0 * *count as f64 / lengths.buckets().max(1) as f64
                    );
                }
            }
            let indexed: usize = {
                let shards = engine.shards.read().expect("engine lock poisoned");
                let shard = shards.get(&1).expect("shard is loaded");
                shard
                    .bucket_index
                    .bucket_map
                    .values()
                    .map(|bucket| bucket.block_index.len())
                    .sum()
            };
            assert_eq!(
                indexed,
                keys.len(),
                "{records}/{range}: the index holds {indexed} pages for {} written keys, so the \
                 distribution above is not this fixture's",
                keys.len()
            );
            if end_routing_bucket == NARROW_END {
                narrow_reached_many += many;
                widest_seen = widest_seen.max(lengths.widest());
                assert!(
                    many > 0,
                    "{records} records on {range} built no `Many` arm at all; a fixture that \
                     never builds the shape this module changes cannot measure it"
                );
                assert!(
                    lengths.widest() >= 2,
                    "{records} records on {range}: the widest `Many` list holds \
                     {} page(s), which is not a multi-page list",
                    lengths.widest()
                );
            } else {
                assert_eq!(
                    many, 0,
                    "{records} records on the whole keyspace put {many} bucket(s) on the `Many` \
                     arm; at one page a bucket none should be, and if any are then this arm is \
                     not the single-page shape it is being read as"
                );
            }
        }
    }
    assert!(
        narrow_reached_many > 0 && widest_seen >= 2,
        "no multi-page list was built at either corpus size; every figure above would be vacuous"
    );
    println!(
        "  the widest page list seen across both corpus sizes on 0..{NARROW_END} holds \
         {widest_seen} pages"
    );
}

// =============================================================================================
// 2. ORDER -- THE CENTRAL RISK, ANSWERED BY THE REPRESENTATION
// =============================================================================================

/// One page WITH ITS OBJECT KEY BESIDE IT rather than on it.
///
/// The pair is the shape every production site uses: `BlockIndexMap::insert`, `insert_released`
/// and `block_index_handle` all take the identity as a PARAMETER, because the entry is losing its
/// own copy. A fixture handing back a bare entry would force each caller to read the key off the
/// page, which is the one thing this change removes.
fn block(
    object_key: &str,
    component: Option<&str>,
    slab: u64,
    offset: u64,
) -> (std::sync::Arc<str>, BlockIndex) {
    (
        std::sync::Arc::from(object_key),
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
            address: crate::block_store::ElementEntry::from_parts(
                slab,
                offset,
                32,
                Some(7),
                Some(11),
            ),
            dirty: false,
            deleted: false,
        },
    )
}

fn built_from(blocks: &[(std::sync::Arc<str>, BlockIndex)]) -> BlockIndexMap {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    for (object_key, entry) in blocks {
        index.insert(object_key, entry.clone(), &mut live);
    }
    index
}

fn walk(index: &BlockIndexMap) -> Vec<u64> {
    index.iter().map(|(handle, _)| *handle).collect()
}

/// THE ORDER CONTROL.
///
/// A `BTreeMap<u64, _>` walks in ascending key order whatever order it was filled in. FIVE
/// production readers depend on that, and none of them would fail a test if it changed -- each
/// would quietly produce a different answer:
///
///   1. `engine::collect_command_index_items_for` emits one index-log item per page IN THIS
///      ORDER, so the order is what goes into the index log.
///   2. `storage_bucket_internals::collect_live_block_entries` materialises the live page set in
///      this order; the dump manifest, WAL reclaim, compaction and the GC snapshot all read it.
///   3. `storage_bucket_internals`' storage-topology sampler walks it and BREAKS at
///      `MAX_STORAGE_TOPOLOGY_SAMPLES`, so the order decides WHICH pages are sampled.
///   4. `engine::bucket_index_shape_for_test` renders it as the string that a shard built by
///      commands is compared against a shard rebuilt from records -- two different fill orders.
///   5. `bucket_store::bucket_index_block_address`'s whole-scan fallback takes the FIRST match.
///
/// Keeping the list sorted by the handle the tree was keyed by makes all five see the identical
/// sequence. This asserts exactly that, by filling the same pages in three different orders.
///
/// AND IT CARRIES ITS OWN NEGATIVE CONTROL: the same pages appended UNSORTED are shown to give a
/// different walk, so a run in which the sort silently stopped happening cannot pass.
///
/// rust-internal: page index ordering, no product behaviour
#[test]
fn an_unsorted_block_list_would_reorder_every_walk_of_this_index() {
    let blocks: Vec<(std::sync::Arc<str>, BlockIndex)> = (0..24)
        .map(|i| block("bag", Some(&format!("f{i}")), 1, i as u64 * 64))
        .collect();

    let forwards = walk(&built_from(&blocks));
    let mut backwards_input = blocks.clone();
    backwards_input.reverse();
    let backwards = walk(&built_from(&backwards_input));
    let mut shuffled_input = blocks.clone();
    // A fixed permutation, not a random one: this has to fail the same way every run.
    shuffled_input
        .sort_by_key(|(object_key, entry)| block_index_handle(object_key, entry).rotate_left(29));
    let shuffled = walk(&built_from(&shuffled_input));

    assert_eq!(
        forwards.len(),
        blocks.len(),
        "the fixture filed {} of {} pages, so the walk below is not over the set it claims",
        forwards.len(),
        blocks.len()
    );
    assert_eq!(
        forwards, backwards,
        "filling the same pages in the opposite order produced a different walk"
    );
    assert_eq!(
        forwards, shuffled,
        "filling the same pages in a third order produced a different walk"
    );
    assert!(
        forwards.windows(2).all(|pair| pair[0] < pair[1]),
        "the walk is not strictly ascending by handle, which is the order the tree walked in and \
         the order the five readers named above were written against"
    );

    // NEGATIVE CONTROL: the order this guard exists to rule out must actually differ from the
    // order it asserts, or the assertions above hold trivially.
    let appended: Vec<u64> = backwards_input
        .iter()
        .map(|(object_key, entry)| block_index_handle(object_key, entry))
        .collect();
    assert_ne!(
        forwards, appended,
        "appending in fill order gave the same sequence as sorting by handle, so this fixture \
         cannot tell a sorted list from an unsorted one and proves nothing"
    );
}

/// The walk survives a page being REMOVED from the middle and another inserted after it.
///
/// A sorted list inserts in the middle; a guard that only ever appends would not notice a policy
/// that pushed to the back.
///
/// rust-internal: page index ordering, no product behaviour
#[test]
fn the_walk_stays_ascending_across_removals_and_middle_inserts() {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    let blocks: Vec<(std::sync::Arc<str>, BlockIndex)> = (0..32)
        .map(|i| block("bag", Some(&format!("g{i}")), 2, i as u64 * 64))
        .collect();
    for (object_key, entry) in &blocks {
        index.insert(object_key, entry.clone(), &mut live);
    }
    let mut removed_any = false;
    for (object_key, entry) in blocks.iter().step_by(3) {
        let handle = block_index_handle(object_key, entry);
        if index.remove(&handle, &mut live, crate::engine::state::StaleCause::Removed).is_some() {
            removed_any = true;
        }
    }
    assert!(removed_any, "the fixture removed nothing, so nothing was exercised");
    for i in 32..48 {
        let (object_key, entry) = block("bag", Some(&format!("g{i}")), 2, i as u64 * 64);
        index.insert(&object_key, entry, &mut live);
    }
    let handles = walk(&index);
    assert!(
        handles.len() > 16,
        "only {} pages survived the churn; the list is too short to say anything about order",
        handles.len()
    );
    assert!(
        handles.windows(2).all(|pair| pair[0] < pair[1]),
        "the walk stopped being ascending after removals and middle inserts: {handles:?}"
    );
}

// =============================================================================================
// 3. DUPLICATES -- WHAT A MAP DID BY CONSTRUCTION AND A LIST HAS TO DO ON PURPOSE
// =============================================================================================

/// A map deduplicated by key; a list appends unless told otherwise.
///
/// EVERY INSERT PATH REACHES `insert_unaccounted`: `BlockIndexMap::insert` (the accounted write
/// path, four production call sites), `insert_released` (the one caller that re-files a released
/// bucket's already-counted pages), and the `FromIterator` a load arrives through. So the
/// duplicate decision is made in one place, and it is REPLACE -- which is what the map did, and
/// what a rewrite of the same page needs, because the lookup refs on disk hold the handle and a
/// second entry under it would be a page that can be found twice or lost on the next rewrite.
///
/// Driven on BOTH arms: a duplicate arriving at `One` and a duplicate arriving at `Many`.
///
/// rust-internal: page index insert semantics, no product behaviour
#[test]
fn a_second_insert_of_the_same_block_replaces_it_rather_than_adding_beside_it() {
    let mut live = BlockSlabLiveIndex::default();

    // --- The One arm, whose entry is now behind a pointer. A rewrite of the same page replaces the
    // --- entry INSIDE the box it already has, so it allocates nothing and does not spill.
    //
    // WHAT A "REWRITE" IS HERE, BECAUSE IT IS NARROWER THAN IT SOUNDS. `block_index_handle` hashes
    // the model kind, the object key, the component AND the address -- slab, offset, length, block id
    // and generation. So a page that MOVES gets a different handle and is a different entry; the only
    // thing that reaches the replace branch is re-filing the BYTE-IDENTICAL page, which is what a
    // repeated append of an unchanged value produces.
    //
    // AND THAT IS WHY THE OBSERVABLE IS THE TALLY RATHER THAN THE ENTRY. A mutation run replaced
    // `mem::replace(&mut **held, page)` with a no-op returning `None` and it SURVIVED: the stored
    // entry is byte-identical either way, so no amount of reading it back can tell the two apart.
    // What CAN tell them apart is the accounting. `insert` discharges the displaced address and
    // charges the new one, which for an identical page nets to zero; a branch that reports no
    // displaced address charges without discharging and DOUBLE-COUNTS the page. One page filed twice
    // must leave one live ref, not two, and that is what this asserts.
    let mut index = BlockIndexMap::default();
    let (only_key, only) = block("solo", None, 5, 128);
    let first = index.insert(&only_key, only.clone(), &mut live);
    assert_eq!(
        1,
        live.tally(5).block_refs,
        "one page filed once left {} live ref(s) on its slab, not 1",
        live.tally(5).block_refs
    );
    let again = index.insert(&only_key, only.clone(), &mut live);
    assert_eq!(first, again, "the same page took two different handles");
    assert_eq!(1, index.len(), "a rewrite on the One arm filed {} pages", index.len());
    assert!(
        matches!(index, BlockIndexMap::One(..)),
        "a rewrite on the One arm spilled the bucket onto a list"
    );
    assert_eq!(
        1,
        live.tally(5).block_refs,
        "one page filed TWICE left {} live ref(s) on its slab. The replace branch must report the \
         address it displaced so the tally can discharge it; a branch that reports none charges \
         without discharging and the store believes it holds twice the pages it does",
        live.tally(5).block_refs
    );
    assert_eq!(
        only.address.length(),
        live.tally(5).bytes,
        "one page filed twice left {} live bytes on its slab against the page's own {}; the \
         displaced address was not discharged",
        live.tally(5).bytes,
        only.address.length()
    );

    // --- The Many arm, at the shortest list it holds and again at a long one. ---
    for length in [SHORTEST_MANY, 4usize, 24] {
        // A FRESH TALLY PER LENGTH. The tally below is asserted against `length`, and one shared
        // across the three iterations accumulates -- a list of four read 6 refs because the previous
        // iteration's two were still charged to the same slab. That is the denominator error this
        // campaign keeps finding in a different costume, so the counter is scoped to what it counts.
        let mut live = BlockSlabLiveIndex::default();
        let mut index = BlockIndexMap::default();
        let blocks: Vec<(std::sync::Arc<str>, BlockIndex)> = (0..length)
            .map(|i| block("bag", Some(&format!("h{i}")), 9, i as u64 * 64))
            .collect();
        for (object_key, entry) in &blocks {
            index.insert(object_key, entry.clone(), &mut live);
        }
        assert_eq!(length, index.len(), "the fixture filed the wrong number of pages");
        assert!(
            matches!(index, BlockIndexMap::Many(_)),
            "a list of {length} did not reach the Many arm"
        );
        let (target_key, target) = &blocks[length / 2];
        let handle = index.insert(target_key, target.clone(), &mut live);
        assert_eq!(
            length,
            index.len(),
            "re-filing a page at length {length} grew the list to {}; the list is holding the \
             same page twice",
            index.len()
        );
        assert_eq!(
            block_index_handle(target_key, target),
            handle,
            "the rewrite was filed under a handle that is not the page's own"
        );
        assert!(
            walk(&index).windows(2).all(|pair| pair[0] < pair[1]),
            "the list stopped being sorted after a duplicate insert at length {length}"
        );
        // And the page that came back is the one just written, not the one it displaced.
        let stored = index.get(&handle).expect("the rewritten page is findable");
        assert_eq!(
            target.address, stored.address,
            "the duplicate insert left the displaced address in place"
        );
        // AND THE TALLY DID NOT DOUBLE-COUNT IT -- the same blind spot the One arm had. A handle
        // hashes the address, so a re-filed page is byte-identical and reading the entry back cannot
        // distinguish a replacement from a no-op. The accounting can: this list's pages each sit on
        // slab 9, so `length` refs is right and `length + 1` is the overwrite failing to discharge.
        assert_eq!(
            length as u64,
            live.tally(9).block_refs,
            "a list of {length} with one page re-filed holds {} live ref(s) on its slab, not \
             {length}; the overwrite charged the new address without discharging the one it displaced",
            live.tally(9).block_refs
        );
    }
}

/// A rewrite that MOVES the page charges the tally the move, which is what a replace has to do
/// and what an append would get wrong twice over.
///
/// The handle is a hash of identity AND address, so a page whose address moved takes a NEW handle
/// and is a genuinely new entry -- the old one is removed by its own path. What this covers is the
/// other case: the same handle arriving twice, where the map used to discharge the displaced
/// address and charge the new one. `insert` returns the handle and discharges through
/// `BlockSlabLiveIndex`, so the tally is the observable.
///
/// rust-internal: page index accounting, no product behaviour
#[test]
fn replacing_a_block_discharges_the_address_it_displaced() {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    let blocks: Vec<(std::sync::Arc<str>, BlockIndex)> = (0..6)
        .map(|i| block("bag", Some(&format!("k{i}")), 4, i as u64 * 64))
        .collect();
    for (object_key, entry) in &blocks {
        index.insert(object_key, entry.clone(), &mut live);
    }
    let before = live.tally(4);
    assert_eq!(
        6, before.block_refs,
        "the fixture charged {} pages to slab 4, not the six it filed",
        before.block_refs
    );
    let (replayed_key, replayed) = &blocks[3];
    index.insert(replayed_key, replayed.clone(), &mut live);
    let after = live.tally(4);
    assert_eq!(
        before, after,
        "re-filing the same page moved the live tally from {before:?} to {after:?}; a replace is \
         supposed to discharge exactly what it displaces"
    );
}

// =============================================================================================
// 4. THE LOOKUP: TWO STRATEGIES ON ONE SORTED LIST, AND WHERE THEY CROSS
// =============================================================================================

/// THE LIST, AND THE ONE OBJECT KEY EVERY ENTRY IN IT BELONGS TO.
///
/// Every page here is a component of the same object, so ONE key names the whole list and the
/// element type stays exactly `(u64, BlockIndex)`. That is load-bearing rather than tidy: two
/// callers below measure `Vec<(u64, BlockIndex)>`'s own growth against this list's, and a key
/// folded into the element would widen the very thing being measured and move both columns.
fn sorted_list(length: usize) -> (std::sync::Arc<str>, Vec<(u64, BlockIndex)>) {
    let object_key: std::sync::Arc<str> = std::sync::Arc::from("bag");
    let mut blocks: Vec<(u64, BlockIndex)> = (0..length)
        .map(|i| {
            let (_, entry) = block("bag", Some(&format!("m{i}")), 3, i as u64 * 64);
            (block_index_handle(&object_key, &entry), entry)
        })
        .collect();
    blocks.sort_by_key(|(handle, _)| *handle);
    (object_key, blocks)
}

/// BOTH STRATEGIES ANSWER IDENTICALLY, AT EVERY LENGTH, ON EVERY KEY.
///
/// They are two ways of asking one sorted list the same question, and both return the found
/// position or the INSERT position -- which is what makes the choice between them a performance
/// choice and nothing else. If this ever fails, the measurement that picked one of them was
/// answering a correctness question it had no business answering.
///
/// Covers hits and MISSES: the miss arm is the one that decides where an insert lands, so a
/// strategy that got misses wrong would corrupt the sort rather than return a wrong page.
///
/// rust-internal: page index lookup, no product behaviour
#[test]
fn the_walk_and_the_bisection_answer_identically_at_every_length() {
    let mut checked = 0usize;
    let mut hits = 0usize;
    let mut misses = 0usize;
    for length in 0..=48 {
        let (_, blocks) = sorted_list(length);
        for (handle, _) in &blocks {
            assert_eq!(
                scan_block(&blocks, handle),
                bisect_block(&blocks, handle),
                "the two strategies disagreed on a present handle at length {length}"
            );
            assert_eq!(
                find_block(&blocks, handle),
                scan_block(&blocks, handle),
                "the shipped lookup disagreed with the walk on a present handle at length {length}"
            );
            checked += 1;
            hits += 1;
        }
        for absent in [0u64, 1, u64::MAX, u64::MAX / 2, 0x5555_5555_5555_5555] {
            if blocks.iter().any(|(handle, _)| *handle == absent) {
                continue;
            }
            assert_eq!(
                scan_block(&blocks, &absent),
                bisect_block(&blocks, &absent),
                "the two strategies disagreed on an absent handle at length {length}"
            );
            assert_eq!(
                find_block(&blocks, &absent),
                bisect_block(&blocks, &absent),
                "the shipped lookup disagreed with the bisection on an absent handle at length \
                 {length}"
            );
            checked += 1;
            misses += 1;
        }
    }
    // A selector matching nothing is vacuous, not a pass -- and this one has to have run BOTH
    // arms, not only the cheap one.
    assert!(
        hits > 100 && misses > 100,
        "the comparison ran {hits} hits and {misses} misses; one of the two arms is not being \
         exercised and the agreement it reports is partly empty"
    );
    println!("  the walk and the bisection agreed on {checked} lookups ({hits} hits, {misses} misses)");
}

/// WHERE THE WALK STOPS BEING THE CHEAPER OF THE TWO, COUNTED -- AND WHY THE SHIPPED LOOKUP IS
/// THE BISECTION.
///
/// COUNTS, NOT TIMES. A timing ratio in this campaign read 485x on an idle box and 11x on a busy
/// one off identical code. What the two strategies differ in is the ENTRIES they touch, and that
/// repeats exactly -- so the entry counter inside the two functions is the instrument, and the
/// shipped one is read through the door production calls rather than through a copy.
///
/// THE TABLE RUNS EVERY LENGTH FROM ONE, so the crossover is located rather than asserted from a
/// single point, and it runs well past the measured maximum of 50 so the tail is visible too.
///
/// WHAT IT SETTLES. The bisection becomes the cheaper of the two at a list of THREE, and the
/// `Many` arm exists only from TWO -- where the two tie exactly. There is therefore no length
/// this arm ever holds at which a walk is ahead, and no ceiling worth carrying. At the measured
/// p50 of 39 pages a bucket the walk touches twenty entries against the bisection's five, which
/// is far outside any correction contiguity could make to a count.
///
/// rust-internal: page index lookup cost, no product behaviour
#[test]
fn the_walk_and_the_bisection_cross_over_where_the_measurement_says() {
    println!(
        "  {:>7} {:>16} {:>16}  {}",
        "length", "walk entries/hit", "bisect entries/hit", "cheaper"
    );
    let mut crossover: Option<usize> = None;
    let mut rows = 0usize;
    let mut at_thirty_nine: Option<(f64, f64)> = None;
    for length in [1usize, 2, 3, 4, 6, 8, 12, 16, 24, 32, 39, 50, 64] {
        let (_, blocks) = sorted_list(length);
        assert_eq!(length, blocks.len(), "the fixture built a list of the wrong length");
        let walk_entries = counted(|| {
            for (handle, _) in &blocks {
                let _ = std::hint::black_box(scan_block(&blocks, handle));
            }
        }) as f64
            / length as f64;
        let bisect_entries = counted(|| {
            for (handle, _) in &blocks {
                let _ = std::hint::black_box(bisect_block(&blocks, handle));
            }
        }) as f64
            / length as f64;
        println!(
            "  {length:>7} {walk_entries:>16.3} {bisect_entries:>16.3}  {}",
            if walk_entries < bisect_entries {
                "walk"
            } else if bisect_entries < walk_entries {
                "bisect"
            } else {
                "tie"
            }
        );
        if crossover.is_none() && bisect_entries < walk_entries {
            crossover = Some(length);
        }
        if length == 39 {
            at_thirty_nine = Some((walk_entries, bisect_entries));
        }
        rows += 1;
    }
    assert!(rows >= 10, "the table has {rows} rows; too few to locate a crossover");
    let crossover = crossover.expect(
        "the bisection never became the cheaper of the two at any length up to 64; the instrument \
         is not reading what it is being read as reading",
    );
    println!(
        "  the bisection first reads cheaper at {crossover} entries; the shortest list the Many \
         arm holds is {SHORTEST_MANY}"
    );

    // THE SHIPPED CHOICE, AS THE CLAIM IT IS. A walk would be ahead only on lists SHORTER than
    // the crossover, and the shortest list this arm holds is two. So the crossover must sit at or
    // below the first length where the arm could prefer a walk, or there is a range of real
    // lengths the shipped lookup is spending entries on.
    assert!(
        crossover <= SHORTEST_MANY + 1,
        "the counts cross at {crossover}, so lists of {SHORTEST_MANY}..{} are cheaper to walk and \
         the shipped bisection is spending entries on them -- a ceiling is owed",
        crossover - 1
    );

    // AND THE MARGIN AT THE MEASURED p50, which is the number the decision actually rests on.
    let (walk_39, bisect_39) = at_thirty_nine.expect("the table covers the measured p50 of 39");
    println!(
        "  at the measured p50 of 39 pages a bucket: walk {walk_39:.3} entries a hit, bisection \
         {bisect_39:.3} -- {:.2}x",
        walk_39 / bisect_39
    );
    assert!(
        walk_39 > bisect_39 * 3.0,
        "at 39 entries the walk costs {walk_39:.3} against the bisection's {bisect_39:.3}, under \
         three times; below that margin contiguity could plausibly close the difference and the \
         choice would need a timing instrument this campaign does not trust"
    );

    // THE SHIPPED DOOR IS THE BISECTION, asserted by what it CHARGES rather than by reading the
    // source: a door that quietly walked would charge the walk's entries here.
    let (_, long) = sorted_list(39);
    let through_the_door = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(find_block(&long, handle));
        }
    });
    let through_the_bisection = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(bisect_block(&long, handle));
        }
    });
    let through_the_walk = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(scan_block(&long, handle));
        }
    });
    assert_eq!(
        through_the_bisection, through_the_door,
        "the shipped lookup charged {through_the_door} entries over 39 pages and the bisection \
         charged {through_the_bisection}; production is not running the strategy this test picked"
    );
    assert_ne!(
        through_the_walk, through_the_door,
        "the walk and the shipped lookup charged the same {through_the_door} entries, so this \
         control cannot tell them apart and its agreement above means nothing"
    );
}

/// Entries the shipped lookup functions examined while `body` ran.
fn counted(body: impl FnOnce()) -> u64 {
    reset_block_lookup_entries_examined();
    body();
    block_lookup_entries_examined()
}

/// THE PLANTED MARKER for the entry counter, recovered exactly.
///
/// A counter that reads zero would make every strategy look free, which is the failure that reads
/// as good news. A walk over a list of N that stops at the LAST entry touches exactly N, and a
/// walk for a handle above every one in the list is exactly that walk.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[test]
fn the_entry_counter_recovers_a_planted_walk_exactly() {
    const PLANTED: usize = 37;
    let (_, blocks) = sorted_list(PLANTED);
    assert_eq!(PLANTED, blocks.len(), "the fixture built the wrong list");
