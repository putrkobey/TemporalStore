// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY BLOCKS A ROUTING BUCKET HOLDS, and what that decides about the block index's shape.
//!
//! #1958 accounted for every byte of `BucketNode` and found that over half of it -- 112 of 200 --
//! is the `BlockIndexMap` it carries inline. It also reported, in passing, that its fixture held
//! 40,080 block entries against 40,040 buckets: a MEAN of 1.001 blocks a bucket. A mean that close
//! to one invites an obvious conclusion -- that a bucket holding two blocks is a rarity worth
//! nothing, so the index should be shaped entirely around the single-block case.
//!
//! A MEAN OF 1.001 IS CONSISTENT WITH TWO DISTRIBUTIONS THAT WANT OPPOSITE ANSWERS, and this
//! module measures which one this engine produces. It produces BOTH, in different workloads, and
//! that is the finding:
//!
//!   * A store of keys that route one to a bucket -- strings, and the timestamped series whose
//!     points are held in the model maps rather than as blocks -- is 99.90% single-block.
//!   * A store of CONTAINER keys -- hashes, sets, sorted sets, lists -- is 0% single-block. Every
//!     field, member and element is its own block, and they all route to their container key's one
//!     bucket. Measured at 100.0000 blocks a bucket for a hundred-member container.
//!
//! So "almost every bucket holds exactly one block" is a statement about ONE workload. The fixture
//! #1958 ranked its structures on contains no container key at all, and the histogram below is
//! reported as counts rather than as a mean for exactly that reason.
//!
//! WHAT THAT DECIDED, AND THE PREMISE IT RESTED ON HAS SINCE BEEN REPLACED. The block index has
//! been a tagged representation since #654. It held the single block INLINE -- `Empty | One(u64,
//! BlockIndex) | Many(..)` -- and the question this module answered was whether that inline arm
//! should go behind a pointer instead. It answered NO, on a measurement, and the measurement was
//! sound: at the distribution it sampled, a boxed arm pays one allocation for 99.90% of buckets in
//! the workload where buckets are numerous and buys nothing in the workload where they are not.
//!
//! THE DISTRIBUTION IT SAMPLED WAS A PROPERTY OF A DEFAULT, NOT OF THE WORKLOAD. Every routed
//! fixture in this module loads its shard on `end_routing_bucket = u32::MAX`, which divides 4.29
//! billion buckets among a few thousand keys: EVERY KEY LANDS ALONE BY CONSTRUCTION, and no
//! workload on that range can produce anything but a single-block bucket. #1973 measured the
//! alternative and made `TS_SHARD_END_ROUTING_BUCKET=1023` the shipped default. At 1,024 buckets
//! the same routed keys FILL them -- 5.273% single-block at 4,000 records, 0.000% at 40,000, p50 39
//! and MAX 50 -- so the arm's width became a toll on nearly every bucket and a benefit to almost
//! none.
//!
//! SO THE INLINE ENTRY IS GONE AND THE SHAPE THIS MODULE DECLINED IS THE ONE THAT SHIPPED: the
//! single-block arm survives, holding a POINTER. The third option this module never priced is what
//! settles it against simply removing the arm -- dropping it recovers the IDENTICAL width, measured,
//! but makes a single-page bucket hold a one-entry list whose first block is a whole growth step,
//! which on the whole-keyspace range costs 482.0 B a bucket against boxing's 178.3. Boxing is the
//! only one of the shapes that wins at BOTH ranges. `inline_arm_trade.rs` re-derives both halves of
//! that arithmetic at both ranges, in both allocator columns, four shapes wide.
//!
//! WHAT SURVIVES HERE, AND IT IS THE PART THAT WAS NEVER ABOUT THE DEFAULT. The two-population
//! finding stands and is the reason this module exists: a mean of 1.001 was two distributions, and
//! the histogram is reported as counts. The shape rule stands too, restated -- boxing the UNCOMMON
//! arm is a win, boxing the COMMON one is a loss, and WHICH ARM IS COMMON is a deployment setting
//! rather than a property of the type. That is the clause #1958 and this module both read as fixed.
//!
//! THE CONTRAST WITH `ObjectIndex` IS UNCHANGED. It boxes its MULTI-ENTRY arm, because there that
//! arm is the wide one (a collection at 24 bytes) and the single-entry arm is a bare `u64`. The
//! block index's wide arm was the one whose commonness moved.
//!
//! ONE CORRECTION TO THAT CONTRAST, measured since: the object index's multi-entry arm is NOT
//! rare in every workload. An object id is hashed over `shard:kind:key:component` while the
//! routing bucket is hashed over the key alone, so a collection key files one object id per
//! MEMBER into one bucket -- 46.46% of buckets hold two or more on a store with collections in
//! it, against 0.00% on a store of strings and series.
//! `how_many_objects_a_bucket_holds_at_two_corpus_sizes_and_two_shape_mixes` reports both. What
//! survives is the rule above, which is about which arm is WIDE and which is COMMON, not about
//! either being rare; the object index's boxed arm holds a sorted run rather than a tree for
//! exactly that reason.
//!
//! THE DENOMINATORS ARE READ OFF THE SHARD and asserted before anything divides by them, and the
//! fixture is asserted to REACH multi-block buckets: a fixture that only ever produced one block per
//! bucket could not tell a correct implementation of this type from a constant.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockSlabLiveIndex, BucketLayoutState, BucketNode, BucketTtl,
    DeletedObjectIndex,
    TombstoneElements,
    ObjectIndex,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// ---------------------------------------------------------------------------------------------
// THE DISTRIBUTION.
// ---------------------------------------------------------------------------------------------

/// Blocks held per routing bucket, as bucket COUNTS keyed by the number of blocks held.
///
/// A histogram and not a mean, because the mean is what hid this: 1.001 blocks a bucket describes
/// a store that is 99.9% single-block and it describes a store with a handful of enormous buckets,
/// and the two want opposite representations.
#[derive(Debug, Default, Clone)]
struct BlocksPerBucket {
    counts: BTreeMap<usize, usize>,
}

impl BlocksPerBucket {
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn blocks(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn holding(&self, blocks: usize) -> usize {
        self.counts.get(&blocks).copied().unwrap_or_default()
    }

    /// Buckets holding strictly more than one block.
    fn multi_block(&self) -> usize {
        self.counts
            .iter()
            .filter(|(held, _)| **held > 1)
            .map(|(_, count)| *count)
            .sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn mean(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.blocks() as f64 / buckets as f64
    }

    fn single_block_fraction(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.holding(1) as f64 / buckets as f64
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  buckets={} pages={} mean={:.4} widest bucket={} page(s)",
            self.buckets(),
            self.blocks(),
            self.mean(),
            self.widest()
        );
        let buckets = self.buckets();
        for (held, count) in self.counts.iter() {
            println!(
                "  {held:>6} page(s) : {count:>8} buckets  ({:>7.3}%)",
                if buckets == 0 {
                    0.0
                } else {
                    100.0 * *count as f64 / buckets as f64
                }
            );
        }
    }
}

fn blocks_per_bucket(engine: &TemporalEngine) -> BlocksPerBucket {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = BlocksPerBucket::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        *hist.counts.entry(bucket.block_index.len()).or_default() += 1;
    }
    hist
}

fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn ack(response: &crate::types::BatchExecuteResponse) {
    assert!(response.status.ok, "seed must ack: {:?}", response.status);
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        ack(&response);
    }
}

/// KEYS THAT ROUTE ONE TO A BUCKET -- the shape #1958's fixture is made of.
///
/// `strings_n` string keys, plus `series_keys` timestamped series of `series_points` points each.
/// The series points are held in the model maps, not as blocks, which is why a series key's bucket
/// holds two blocks and not a thousand -- and those series buckets are what makes this fixture
/// reach a multi-block bucket at all.
fn seed_routed_keys(
    engine: &TemporalEngine,
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) {
    run_batch(
        engine,
        (0..strings_n)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "series seed must ack: {:?}", response.status);
        }
    }
}

/// CONTAINER KEYS -- a hash, a set, a sorted set and a list, each of `members` elements.
///
/// Every element is its own block and they all route to the container key's one bucket, so this
/// workload produces buckets holding exactly `members` blocks and no single-block bucket at all.
fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        match k % 4 {
            0 => {
                for f in 0..members {
                    commands.push(Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: vec![b'v'; 32],
                    });
                }
            }
            1 => {
                for m in 0..members {
                    commands.push(Command::SetAdd {
                        key: format!("t{k}"),
                        member: format!("m{m}").into_bytes(),
                    });
                }
            }
            2 => {
                for m in 0..members {
                    commands.push(Command::ZSetAdd {
                        key: format!("z{k}"),
                        member: format!("m{m}").into_bytes(),
                        score: m as f64,
                    });
                }
            }
            _ => {
                for m in 0..members {
                    commands.push(Command::ListPush {
                        key: format!("l{k}"),
                        member: format!("m{m}").into_bytes(),
                        left: false,
                    });
                }
            }
        }
    }
    run_batch(engine, commands);
}

/// THE MEASUREMENT THAT DECIDES EVERYTHING BELOW.
///
/// Three workloads at two corpus sizes ten times apart, each reported as a HISTOGRAM WITH COUNTS.
///
/// THE ANTI-CONSTANT ASSERTION. Each arm asserts the fixture reaches a bucket holding more than
/// one block. A fixture in which every bucket held exactly one block cannot tell a correct
/// implementation of `BlockIndexMap` from a constant that answers `One` -- the `Many` arm, the
/// promotion into it and the shrink back out of it would all be unreached, and every test over
/// them would pass against an engine that had deleted them.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted: `tempfile` names every
/// directory with the same number of characters, and allocation bytes move at about six bytes a
/// character. Bucket and block COUNTS are immune to it, which is why the counts carry the claim.
#[test]
#[ignore = "seeds six stores up to 40,000 records each; run by name"]
fn the_blocks_a_bucket_holds_are_two_populations_and_not_one_mean() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut routed: Vec<BlocksPerBucket> = Vec::new();
    let mut container: Vec<BlocksPerBucket> = Vec::new();
    let mut mixed: Vec<BlocksPerBucket> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        // --- Keys that route one to a bucket. #1958's fixture shape, exactly. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_routed_keys(&engine, records, records / 1_000, 1_000);
            let hist = blocks_per_bucket(&engine);
            hist.report(&format!("{label}: keys that route one to a bucket"));
            routed.push(hist);
        }

        // --- Container keys: one block per field, member or element. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_container_keys(&engine, records / 100, 100);
            let hist = blocks_per_bucket(&engine);
            hist.report(&format!("{label}: container keys, 100 elements each"));
            container.push(hist);
        }

        // --- Both, in one store. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_routed_keys(&engine, records / 2, records / 2_000, 1_000);
            seed_container_keys(&engine, records / 200, 100);
            let hist = blocks_per_bucket(&engine);
            hist.report(&format!("{label}: both shapes in one store"));
            mixed.push(hist);
        }
    }

    assert_eq!(
        6,
        path_lengths.len(),
        "all six arms must have run, or the comparisons below compare an arm with itself"
    );
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms ({} then {length}); allocation bytes move \
             with it at about six bytes a character",
            path_lengths[0]
        );
    }

    // --- Denominators. ---
    for hist in routed.iter().chain(container.iter()).chain(mixed.iter()) {
        assert!(
            hist.buckets() > 0,
            "denominator: no routing buckets, so every fraction below is a zero that means nothing"
        );
        assert!(
            hist.blocks() > 0,
            "denominator: no page entries, so the structure this module is about is absent"
        );
    }

    // --- The routed arm: overwhelmingly single-block, and it REACHES a multi-block bucket. ---
    for (index, hist) in routed.iter().enumerate() {
        assert!(
            hist.single_block_fraction() > 0.99,
            "routed arm {index}: {:.4} of buckets hold exactly one page; the single-page case is \
             not dominant here and the inline arm would be carrying the rare shape",
            hist.single_block_fraction()
        );
        assert!(
            hist.multi_block() > 0,
            "routed arm {index}: NO bucket holds more than one page, so this fixture cannot tell \
             a correct `BlockIndexMap` from a constant that always answers One"
        );
        assert!(
            hist.widest() >= 2,
            "routed arm {index}: the widest bucket holds {} page(s)",
            hist.widest()
        );
    }

    // --- The container arm: the opposite population, from the same engine. ---
    for (index, hist) in container.iter().enumerate() {
        assert_eq!(
            0,
            hist.holding(1),
            "container arm {index}: {} buckets hold exactly one page; this arm exists to be the \
             population where the single-page case does NOT dominate",
            hist.holding(1)
        );
        assert!(
            hist.mean() >= 2.0,
            "container arm {index}: mean {:.4} pages a bucket",
            hist.mean()
        );
    }

    // --- The mixed arm holds BOTH populations at once. ---
    for (index, hist) in mixed.iter().enumerate() {
        assert!(
            hist.holding(1) > 0 && hist.multi_block() > 0,
            "mixed arm {index}: holds {} single-page and {} multi-page buckets; it is supposed to \
             be bimodal and it is not",
            hist.holding(1),
            hist.multi_block()
        );
    }

    // --- Flat across a tenfold corpus, in both directions. ---
    println!("\n=== the two populations, across a tenfold corpus ===");
    println!(
        "  routed    : {:.5} -> {:.5} single-page fraction",
        routed[0].single_block_fraction(),
        routed[1].single_block_fraction()
    );
    println!(
        "  container : {:.5} -> {:.5} single-page fraction  (mean {:.2} -> {:.2} pages/bucket)",
        container[0].single_block_fraction(),
        container[1].single_block_fraction(),
        container[0].mean(),
        container[1].mean()
    );
    println!(
        "  mixed     : {:.5} -> {:.5} single-page fraction",
        mixed[0].single_block_fraction(),
        mixed[1].single_block_fraction()
    );
    assert!(
        (routed[0].single_block_fraction() - routed[1].single_block_fraction()).abs() < 0.01,
        "the routed arm's single-page fraction moved with the corpus, so it is not a property of \
         the workload"
    );
    assert!(
        container[0].mean() >= 2.0 && container[1].mean() >= 2.0,
        "the container arm stopped being multi-page at one of the two sizes"
    );
    assert!(
        routed[1].single_block_fraction() > container[1].single_block_fraction() + 0.9,
        "the two arms are supposed to be the two ends of this distribution and they are not: \
         routed {:.4}, container {:.4}",
        routed[1].single_block_fraction(),
        container[1].single_block_fraction()
    );
}

// ---------------------------------------------------------------------------------------------
// THE BLOCK INDEX, BYTE BY BYTE, AND THE SHAPES IT COULD TAKE.
// ---------------------------------------------------------------------------------------------

/// Every byte of the block entry and of the index that holds it, as a RECONSTRUCTION.
///
/// `eight_aligned + round_up(tail) == size_of` for each, so a field added to either lands in a
/// named group and the failure says which group it landed in rather than only that a total moved.
/// A total-only assertion cannot tell "a field was added" from "the aligner rounded differently".
#[test]
fn every_byte_of_the_block_index_is_accounted_for() {
    // --- BlockIndex: TWO shared names and an address pack solid; the one-byte model spelling,
    // TWO flags and the two locating fields the entry absorbed from the index-log row fill the
    // tail EXACTLY, with nothing spare. The model spelling used to be a second `Arc<str>` in the
    // eight-aligned group; it is now one byte in the tail, which is why that step took SIXTEEN
    // bytes off the structure and not the twelve a field-width subtraction would predict: four of
    // them came out of the rounding the flags were THEN still sitting in -- a rounding that has no
    // room left in it, which is why the claim below is asserted rather than read off the room.
    //
    // THE ADDRESS THEN TOOK A WORD OUT OF THE SAME GROUP, and that step is the other direction of
    // the same lesson: SIX bytes of its payload left, in two narrowings of which neither crosses a
    // multiple of eight alone, so the group moved by a whole word and the tail did not move at
    // all. A field-width subtraction would have predicted nothing here twice over. ---
    let arc_str = size_of::<Arc<str>>();
    let opt_arc_str = size_of::<Option<Arc<str>>>();
    // TWO EIGHT-ALIGNED FIELDS, NOT THREE: `opt_arc_str` was the component and it is gone, which
    // is the 56 -> 40 step. `opt_arc_str` stays bound above as the width the name USED to cost,
    // because the paragraphs here are stated against that arithmetic.
    // ONE EIGHT-ALIGNED FIELD NOW, AND THE TAIL LOST TWO TERMS IT SHOULD NEVER HAVE HAD. The
    // address is the only eight-aligned field left -- `arc_str` stays bound above as the width the
    // KEY used to cost, for the same reason `opt_arc_str` does for the name.
    //
    // AND `kind` AND `routing_bucket` WERE IN THIS TAIL WITH NO FIELD BEHIND THEM. `65c7fc562`
    // retired both from `BlockIndex` and this reconstruction kept adding their five bytes: the
    // tail came to eight and rounded to eight, so it reached forty and passed. Three terms round
    // to the same word as eight, which is exactly the blindness this file's own
    // `every_byte_of_the_block_index_is_accounted_for` exists to narrow -- and it was the
    // reconstruction that finally caught it, on a leg where the width moved.
    let index_eight_aligned = size_of::<ElementEntry>();
    let index_tail = size_of::<crate::engine::storage_bucket_internals::StoredModelKind>()
        + 2 * size_of::<bool>();
    let index_rounded_tail = round_up_to(index_tail, 8);
    println!(
        "BlockIndex: {index_eight_aligned} B eight-aligned + {index_tail} B tail rounded to \
         {index_rounded_tail} = {} B, size_of = {}",
        index_eight_aligned + index_rounded_tail,
        size_of::<BlockIndex>()
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        index_eight_aligned + index_rounded_tail,
        "the page entry no longer reconstructs as {index_eight_aligned} bytes of eight-aligned \
         field plus {index_tail} bytes of flag rounded up to {index_rounded_tail}"
    );
    // FORTY, AND PINNED AT ITS ALIGNMENT INSTEAD OF BRACKETED. 56 before the entry stopped
    // naming its element: `opt_arc_str` above is the sixteen bytes that left.
    //
    // WHAT STOOD HERE AND WHY IT IS GONE. The note read "a bare equality is satisfied by
    // whatever the type measures; the two neighbours refuse 39 and 41 so this reads as a
    // measurement and not as a number adjusted until it passed", and its two `assert_ne!`
    // partners carried messages -- "the page entry is 39, so 40 is an upper bound here" --
    // asserting that 39 and 41 were reachable. They are not: the type is 8-aligned, so they are
    // 7 and 1 mod 8. And the partners could not have run anyway, because the `assert_eq!` below
    // panics first on every change that could have fired them.
    //
    // THE ALIGNMENT PIN IS NOT DOMINATED, and here it is a premise rather than a flourish: the
    // reconstruction above rounds the tail up to `align_of::<BlockIndex>()`, and the per-bucket
    // arithmetic later in this module uses it as the modulus again. A width pin admits every
    // alignment that divides it, so `== 24` is equally true at align 1, 2, 4 and 8 and says
    // nothing about the modulus those sums are taken over.
    assert_eq!(24, size_of::<BlockIndex>(), "the page entry's budgeted width moved");
    assert_eq!(
        8,
        std::mem::align_of::<BlockIndex>(),
        "the page entry is {}-aligned; the tail rounding above and the per-bucket sums below are \
         taken to this modulus",
        std::mem::align_of::<BlockIndex>()
    );

    // NARROWING THE FLAGS STILL RECLAIMS NOTHING, BUT NOT FOR THE REASON IT USED TO.
    //
    // This asserted `index_tail < index_rounded_tail` -- the flags fit INSIDE the rounding, with
    // room to spare. The tail now fills its rounding EXACTLY: the entry absorbed two locating
    // fields from the index-log row, so the tail is model_id + two flags + kind + routing_bucket,
    // which is eight bytes in eight. There is no slack left anywhere in this structure.
    //
    // So the old proxy is false while the CLAIM it stood for is still true, and the claim is now
    // asserted as itself: packing the two flags into one byte leaves a tail that rounds to the
    // same eight. A `<` here would go red on a true statement, which is what it just did.
    let flags_packed_tail = index_tail - 2 * size_of::<bool>() + 1;
    // AND THE SLACK IS FIVE, NOT ZERO, WHICH IS WHAT THIS ASSERTION NOW SAYS.
    //
    // It asserted `index_tail == index_rounded_tail` -- "no slack left anywhere in this
    // structure" -- and it held while the tail carried `kind` and `routing_bucket` on top of the
    // model byte and the two flags: eight bytes in eight. Those two had already left `BlockIndex`
    // and this reconstruction kept adding them, so the equality went on passing over a tail the
    // entry did not have. The tail is three bytes and the rounding is eight.
    //
    // IT IS STILL AN EQUALITY, FOR THE REASON THE PARAGRAPH IT REPLACES GIVES. A `<=` cannot
    // state a slack claim at all -- `index_rounded_tail` IS `round_up_to(index_tail, 8)`, so the
    // comparison holds for every tail any structure could have and no change could turn it red.
    // An equality on the DIFFERENCE can go red in either direction, which is what makes it a
    // check: it is the same claim `engine_storage_state.rs` carries beside the width as `size_of - field_sum`.
    assert_eq!(
        5,
        index_rounded_tail - index_tail,
        "the tail is {index_tail} B inside a {index_rounded_tail} B rounding, so this entry has \
         {} B of slack rather than the five it is priced against",
        index_rounded_tail - index_tail
    );
    assert_eq!(
        round_up_to(flags_packed_tail, 8),
        index_rounded_tail,
        "packing the two flags into one byte takes the tail from {index_tail} to \
         {flags_packed_tail}, which rounds to {} rather than {index_rounded_tail} -- so packing \
         them WOULD now reclaim a whole word and the note above has gone stale",
        round_up_to(flags_packed_tail, 8)
    );

    // --- BlockIndexMap: ONE ARM WITH A PAYLOAD, and the tag rides the list pointer's niche. ---
    //
    // THE WIDEST ARM USED TO BE THE COMMON ONE AND THAT WAS THE WHOLE ARGUMENT. A single block was
    // held INLINE, so the enum was a handle plus a whole entry and every bucket in the map paid
    // that width. #1958 read the common case as the wide one and concluded that boxing it would be
    // a loss; the reading was correct and the POPULATION it was taken over was the whole `u32`
    // routing keyspace, where every key lands in a bucket of its own by construction. #1973 made
    // 1023 the default and the common case stopped being single-block. `inline_arm_trade.rs` prices
    // all three shapes at the shipped range.
    //
    // WHAT IS LEFT TO RECONSTRUCT is the narrowest statement this type can make: it is exactly its
    // list, because `Empty` rides in the list pointer's niche and there is no other payload.
    let declined_inline_arm = size_of::<u64>() + size_of::<BlockIndex>();
    let many_arm = size_of::<Vec<(u64, BlockIndex)>>();
    println!(
        "BlockIndexMap: list arm {many_arm} B, size_of = {} (the declined inline arm would have \
         been {declined_inline_arm} B)",
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BlockIndexMap>(),
        many_arm,
        "the page index is no longer exactly its list arm; either `Empty` has stopped riding the \
         list pointer's niche or a payload has been added beside it"
    );
    assert!(
        declined_inline_arm > many_arm,
        "holding one entry inline is supposed to be WIDER than holding a list -- that is what the \
         width recovered by dropping it was, and if the two are equal there was nothing to recover"
    );
    assert_eq!(24, size_of::<BlockIndexMap>(), "the page index's budgeted width moved");

    // --- AND IT IS NO LONGER THE DOMINANT TERM OF THE NODE, WHICH IS THE POINT. ---
    //
    // This assertion used to read the other way: the inline block index was over HALF the node, and
    // that was what made it the first thing to account for. Dropping the inline arm took it from
    // the widest field to a quarter of the structure, so the claim that survives is the inverse --
    // and stating it as an assertion rather than a comment is what stops the module going on
    // describing the block index as the place to look.
    assert!(
        size_of::<BlockIndexMap>() * 2 < size_of::<BucketNode>(),
        "the page index is {} of the node's {} bytes; if it is more than half again, the inline \
         arm has come back or another payload has joined it",
        size_of::<BlockIndexMap>(),
        size_of::<BucketNode>()
    );
}

fn round_up_to(value: usize, multiple: usize) -> usize {
    value.div_ceil(multiple) * multiple
}

// --- The mirrors. Each is built from the same field types as the declaration, so the widths
// --- below are statements about the declaration and not estimates. The control comes first.

/// The block index as it stands. If this is not `size_of::<BlockIndexMap>()` every row is fiction.
///
/// TWO ARMS SINCE THE INLINE ONE WAS DROPPED, and a FLAT LIST since #1963 -- every mirror below
/// names the shipped list type for the same reason. A mirror whose arms have drifted from the
/// declaration is not a statement about widths, and this one is the control that says so.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorLiveBlockIndex {
    Empty,
    Many(Vec<(u64, BlockIndex)>),
}

/// THE SHAPE #1958 ARGUED FOR AND #1973's DEFAULT REFUTED: the single block held INLINE.
///
/// Kept as a mirror rather than deleted, because the width it costs is the width dropping it
/// recovered, and a proposal to bring it back should have to argue against a number rather than
/// against an absence. `inline_arm_trade.rs` prices it, this one, and the boxed arm together at
/// both routing ranges.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorInlineBlockIndex {
    Empty,
    One(u64, BlockIndex),
    Many(Vec<(u64, BlockIndex)>),
}

/// The single block behind a pointer: the shape whose sign is not visible in its width.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorBoxedBlockIndex {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(Vec<(u64, BlockIndex)>),
}

/// No tag at all: one map, always, for every bucket including the empty ones.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorAlwaysMapped(Vec<(u64, BlockIndex)>);

/// One map behind a pointer, absent for an empty bucket. The narrowest shape available, and the
/// one that allocates for every bucket that holds anything at all.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorAlwaysIndirect(Option<Box<Vec<(u64, BlockIndex)>>>);

/// Two blocks inline before the map is earned -- the inline-storage answer, which makes the common
/// case bigger to avoid an allocation the common case was never going to make.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorInlineTwo {
    Empty,
    One(u64, BlockIndex),
    Two([(u64, BlockIndex); 2]),
    Many(Vec<(u64, BlockIndex)>),
}

/// The handle narrowed to a non-zero word, in case the tag could be made to ride it instead.
///
/// IDENTICAL TO THE SHIPPED SHAPE EXCEPT FOR THE HANDLE'S TYPE, which is what makes its width a
/// statement about the handle and nothing else. Its single-block arm is BOXED, like the declaration's
/// -- a mirror that also changed how the entry is held would be answering two questions at once and
/// attributing the answer to the wrong one.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorNonZeroHandle {
    Empty,
    One(std::num::NonZeroU64, Box<BlockIndex>),
    Many(Vec<(u64, BlockIndex)>),
}

/// THE SHAPE WHERE THE COMMON CASE HOLDS NO BLOCK STRUCTURE AT ALL: one field in the node that is
/// EITHER the single block's address OR a pointer to an out-of-line structure, tiered 0 -> fixed
/// -> variable. The single-block bucket allocates nothing, which is what makes it different from
/// every boxed shape above.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorOneWordInline {
    Empty,
    /// The single block's whole entry, held in ONE WORD.
    One(u64),
    /// The rare case, and the only one that allocates.
    Many(Box<Vec<(u64, BlockIndex)>>),
}

/// The same tiering, but holding what THIS engine's single block actually needs: its address.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorInlineAddress {
    Empty,
    One(u64, ElementEntry),
    Many(Box<Vec<(u64, BlockIndex)>>),
}

/// The block entry with `deleted` removed -- encoded instead as a length of zero, which removes a
/// field rather than narrowing one.
#[allow(dead_code)]
struct MirrorBlockNoDeleted {
    model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    log_backed: bool,
}

/// The block entry with all three flags folded into a single byte.
#[allow(dead_code)]
struct MirrorBlockOneFlagByte {
    model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    address: ElementEntry,
    flags: u8,
}

/// The block entry with all three flags gone entirely.
#[allow(dead_code)]
struct MirrorBlockNoFlags {
    model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    address: ElementEntry,
}

/// The block entry with the flags gone AND the model spelling with them -- an EMPTY tail.
///
/// The only shape that still crosses the rounding, and it is here to bound the negative claim
/// below rather than as a proposal: the spelling is not removable, it is what names the kind.
#[allow(dead_code)]
struct MirrorBlockEmptyTail {
    address: ElementEntry,
}

/// The block entry with its three shared names gone -- the largest group in it.
#[allow(dead_code)]
struct MirrorBlockAddressOnly {
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// A node mirroring the live declaration field for field, so a node width can be quoted beside
/// each index shape without asserting anything about a structure this engine does not have.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorNode<I> {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
    tombstone_elements: TombstoneElements,
}

/// WHAT EACH SHAPE OF THE BLOCK INDEX WOULD COST, IN WIDTH.
///
/// THE CONTROL COMES FIRST, twice: the mirror of the live index has to equal the declaration, and
/// the mirror of the live NODE has to equal `BucketNode`. Without both, every row is describing a
/// structure this engine does not have and the numbers are fiction.
///
/// WIDTH IS NOT THE RANKING. Four of these five shapes are narrower than what is there, and four
/// of them allocate for a bucket that holds one block. The distribution above says 99.90% of the
/// buckets in a routed store hold exactly one, so "narrower" and "cheaper" point in opposite
/// directions here, and only the allocator settles it. That is the next test.
#[test]
fn what_each_shape_of_the_block_index_would_cost() {
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorLiveBlockIndex>(),
        "the mirror of the live page index is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<MirrorLiveBlockIndex>(),
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the live node is {} bytes against the declaration's {}; the node widths \
         below are fiction",
        size_of::<MirrorNode<BlockIndexMap>>(),
        size_of::<BucketNode>()
    );

    let live_index = size_of::<BlockIndexMap>();
    let live_node = size_of::<BucketNode>();
    let rows: Vec<(&'static str, usize, usize, bool)> = vec![
        (
            "as it stands: Empty | One inline | Many",
            live_index,
            live_node,
            false,
        ),
        (
            "the single page behind a pointer",
