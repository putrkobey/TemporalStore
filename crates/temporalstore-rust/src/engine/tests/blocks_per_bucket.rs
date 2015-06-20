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
            size_of::<MirrorBoxedBlockIndex>(),
            size_of::<MirrorNode<MirrorBoxedBlockIndex>>(),
            true,
        ),
        (
            "one map always, no tag",
            size_of::<MirrorAlwaysMapped>(),
            size_of::<MirrorNode<MirrorAlwaysMapped>>(),
            true,
        ),
        (
            "one map behind a pointer, absent when empty",
            size_of::<MirrorAlwaysIndirect>(),
            size_of::<MirrorNode<MirrorAlwaysIndirect>>(),
            true,
        ),
        (
            "two pages inline before the map is earned",
            size_of::<MirrorInlineTwo>(),
            size_of::<MirrorNode<MirrorInlineTwo>>(),
            false,
        ),
        (
            "the handle narrowed to a non-zero word",
            size_of::<MirrorNonZeroHandle>(),
            size_of::<MirrorNode<MirrorNonZeroHandle>>(),
            false,
        ),
    ];

    println!("\n=== shapes of the page index, by width ===");
    println!(
        "  {:<44} {:>7} {:>7} {:>9} {:>26}",
        "shape", "index", "node", "vs today", "single-page bucket pays"
    );
    for (name, index, node, allocates) in &rows {
        println!(
            "  {:<44} {:>7} {:>7} {:>9} {:>26}",
            name,
            index,
            node,
            format!("{:+}", *node as isize - live_node as isize),
            if *allocates {
                "one allocation"
            } else {
                "nothing"
            }
        );
    }

    // THE BOXED SHAPE IS WHAT SHIPPED, SO IT NO LONGER NARROWS THE NODE -- IT IS THE NODE. This
    // assertion used to read `<` and priced boxing as a proposal; the proposal was taken, so the
    // boxed mirror and the live node are now the same width and the row prices nothing against
    // itself. Asserting the EQUALITY is what keeps the mirror honest: if the two separate, either the
    // mirror has drifted from the declaration or the live shape has stopped being the boxed one, and
    // both are things this module must notice rather than print a difference for.
    assert_eq!(
        live_node,
        size_of::<MirrorNode<MirrorBoxedBlockIndex>>(),
        "the boxed mirror is {} B against the live node's {live_node} B. Boxing the single page is \
         what SHIPPED, so these must be equal; a difference means the mirror no longer describes the \
         declaration",
        size_of::<MirrorNode<MirrorBoxedBlockIndex>>()
    );
    // And the shape that was replaced must still be the wider one, or there was nothing to recover.
    assert!(
        size_of::<MirrorNode<MirrorInlineBlockIndex>>() > live_node,
        "the node with its page entry held INLINE is {} B against the shipped {live_node} B; if the \
         two are equal the inline entry was never costing width",
        size_of::<MirrorNode<MirrorInlineBlockIndex>>()
    );

    // Inline storage for a second block makes the common case wider to avoid an allocation the
    // common case was never going to make: 99.90% of routed buckets hold one block, so the second
    // slot is carried by every bucket and used by one in a thousand.
    assert!(
        size_of::<MirrorInlineTwo>() > live_index,
        "the inline-two shape is supposed to be WIDER than what is there; it is the cost of \
         reaching the second page without a map"
    );

    // The handle already has no spare niche to give: the tag rides a pointer inside the block.
    assert_eq!(
        live_index,
        size_of::<MirrorNonZeroHandle>(),
        "narrowing the handle to a non-zero word moved the index width, which would mean the tag \
         had been costing a word and is not"
    );
}

/// THE SHAPE WHERE THE COMMON CASE HOLDS NO BLOCK STRUCTURE AT ALL, AND WHAT FORBIDS IT HERE.
///
/// Every candidate in the table above still stores a whole `BlockIndex` for a single-block bucket,
/// inline or behind a pointer. The shape worth asking about is the one that stores NOTHING extra:
/// one field in the node that is either the single block's address or a pointer to an out-of-line
/// structure, tiered 0 -> a fixed structure -> a variable one, with the single-block bucket
/// allocating nothing at all.
///
/// IT IS A LARGE WIN IF IT FITS, AND IT DOES NOT FIT HERE. The tiering asks that a single block's
/// entry be one word. This engine's block entry is 104 bytes, and the smallest part of it that a
/// read cannot do without -- the address -- is 48 on its own. The gate is not the flags or the
/// tagging; it is that `BlockAddress` is six words, and it is six words because #1937 already
/// narrowed it and could not get it below that: two 64-bit slab coordinates, two 64-bit
/// identities, two narrow 32-bit fields, the routing bucket and a presence byte. Packing the
/// single block into one word would mean a different stored address format, not a different
/// resident layout.
///
/// THE TWO ECONOMIES THAT REMOVE A FIELD RATHER THAN NARROWING ONE, priced here on their merits
/// because both are real ideas and both are worth knowing the answer to:
///
///   * **a size of zero MEANS deleted**, so no `deleted` field exists. Measured below: it removes
///     one of the three flag bytes and moves the entry's width by NOTHING, because all three
///     already sit inside one 8-byte rounding. Removing ALL THREE moves 8.
///   * **the flags occupy one byte rather than one word**. Measured below: they already do. Three
///     `bool` fields are three bytes, not three words, and folding them into a single `u8` moves
///     the entry by nothing at all.
///
/// So of the two, one is already banked and the other is worth 8 bytes only if all three flags go
/// -- and of those three, `dirty` and `deleted` are keys of the stored block entry while
/// `log_backed` is NOT one any more: the entry shed it and the log-resident fact is derived from
/// the address. The mirrors below still declare three flags OF THEIR OWN, and a three-flag tail is
/// what they price; that is no longer the live entry's field set, while the width each of them is
/// compared against is the live entry's.
#[test]
fn what_the_common_case_could_hold_inline_and_what_this_engines_block_entry_forbids() {
    // --- The controls, first and twice. ---
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorLiveBlockIndex>(),
        "the mirror of the live page index has drifted from the declaration"
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the live node has drifted from the declaration"
    );

    let word = size_of::<u64>();
    println!("\n=== the tiering, 0 -> a fixed structure -> a variable one ===");
    println!(
        "  {:<52} {:>7} {:>7} {:>10}",
        "shape", "index", "node", "vs today"
    );
    for (name, index, node) in [
        (
            "as it stands: the whole page entry inline",
            size_of::<BlockIndexMap>(),
            size_of::<MirrorNode<BlockIndexMap>>(),
        ),
        (
            "the single page's ADDRESS inline, map behind a pointer",
            size_of::<MirrorInlineAddress>(),
            size_of::<MirrorNode<MirrorInlineAddress>>(),
        ),
        (
            "ONE WORD: the page in the field, or a pointer",
            size_of::<MirrorOneWordInline>(),
            size_of::<MirrorNode<MirrorOneWordInline>>(),
        ),
    ] {
        println!(
            "  {:<52} {:>7} {:>7} {:>10}",
            name,
            index,
            node,
            format!("{:+}", node as isize - size_of::<BucketNode>() as isize)
        );
    }
    println!(
        "  and a hand-rolled union stealing the tag from the pointer's low bit would be {} -- one \
         word, no tag of its own",
        word
    );

    // --- WHAT FORBIDS IT. The single block's smallest irreducible payload. ---
    println!("\n=== what a single page's entry cannot do without ===");
    println!(
        "  the address alone                     : {:>4} B  ({} words)",
        size_of::<ElementEntry>(),
        size_of::<ElementEntry>() / word
    );
    println!(
        "  the address plus its three flags      : {:>4} B",
        size_of::<MirrorBlockAddressOnly>()
    );
    println!(
        "  the whole page entry as declared      : {:>4} B",
        size_of::<BlockIndex>()
    );
    assert!(
        size_of::<ElementEntry>() > word,
        "the address fits in one word, so the one-word tiering IS available here and this test is \
         arguing against a shape that is not blocked"
    );
    assert!(
        size_of::<MirrorOneWordInline>() <= 2 * word,
        "the one-word shape is supposed to be a word and its tag; it is {} B",
        size_of::<MirrorOneWordInline>()
    );
    // The gate, stated as a ratio so the message says how far away it is rather than only that it
    // is far.
    println!(
        "  THE GATE: the one-word tiering needs the single page's entry to be {} B. The address \
         alone is {}x that, and the entry as declared is {}x. Reaching it is a change to the \
         STORED address format, which is 13 of the node's 15 fields' problem and not a resident \
         layout decision.",
        word,
        size_of::<ElementEntry>() / word,
        size_of::<BlockIndex>() / word
    );

    // --- THE TWO ECONOMIES, each measured. ---
    println!("\n=== removing a field rather than narrowing one, priced ===");
    for (name, width) in [
        ("the page entry as declared", size_of::<BlockIndex>()),
        (
            "with `deleted` gone (a size of zero means deleted)",
            size_of::<MirrorBlockNoDeleted>(),
        ),
        (
            "with all three flags folded into one byte",
            size_of::<MirrorBlockOneFlagByte>(),
        ),
        ("with all three flags gone", size_of::<MirrorBlockNoFlags>()),
        (
            "with the three shared names gone",
            size_of::<MirrorBlockAddressOnly>(),
        ),
    ] {
        println!(
            "  {:<52} {:>4} B  {:>6}",
            name,
            width,
            format!("{:+}", width as isize - size_of::<BlockIndex>() as isize)
        );
    }
    // A CONCLUSION MOVED HERE, not a literal. Until the model spelling became one byte the tail
    // was three flag bytes in an eight-byte rounding, and removing ALL THREE was the one flag
    // change that crossed it -- 0 in 0. The spelling now shares that rounding, so the tail is four
    // bytes and cannot reach zero: the spelling is not removable, it is what names the kind. So
    // NOTHING in the tail moves this entry any more. Packing does not, removing one does not, and
    // removing all three does not either.
    //
    // That is a stronger statement than the one it replaces and it is the same shape of correction
    // #1968 had to make in the other direction: a claim about what a tail can do is a claim about
    // the tail's WIDTH, and it goes stale the moment anything else lands in it.
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorBlockNoDeleted>(),
        "removing the `deleted` field moved the entry's width; the tail is no longer sitting \
         inside one rounding and the note above is stale"
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorBlockOneFlagByte>(),
        "folding the three flags into one byte moved the entry's width, which would mean they had \
         been costing more than three bytes and they are not"
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorBlockNoFlags>(),
        "removing ALL THREE flags moved the entry from {} B to {} B. That used to be the one flag \
         change that crossed the rounding, and it stopped being one when the model spelling joined \
         the tail -- so if it crosses again, something has left the tail and the claim below is \
         stale",
        size_of::<BlockIndex>(),
        size_of::<MirrorBlockNoFlags>()
    );
    // AND THE BOUND ON THAT NEGATIVE. A claim that nothing in the tail can pay is worth having
    // only if something COULD: emptying the tail entirely does cross, by a whole word.
    assert!(
        size_of::<MirrorBlockEmptyTail>() < size_of::<BlockIndex>(),
        "an entry with NO tail at all is {} B against {} B; if even that does not cross, this \
         reconstruction is not describing the rounding it claims to",
        size_of::<MirrorBlockEmptyTail>(),
        size_of::<BlockIndex>()
    );
    println!(
        "  THE TAIL IS NOW DEAD WEIGHT AND CANNOT BE SPENT: {} B of tail in an {} B rounding, so \
         packing the flags, dropping one and dropping all three all leave the entry at {} B. Only \
         an EMPTY tail crosses ({} B), and the model byte is what stops it being emptied.",
        size_of::<crate::engine::storage_bucket_internals::StoredModelKind>() + 3,
        align_of::<BlockIndex>(),
        size_of::<BlockIndex>(),
        size_of::<MirrorBlockEmptyTail>()
    );

    // --- AND THE SAVING THE WHOLE TIERING WOULD BUY, IF THE ENTRY EVER GOT THERE. ---
    //
    // Stated so the next step has a ceiling rather than an aspiration: this is what the node would
    // be if the single block's entry did fit in a word, which it does not today.
    let ceiling = size_of::<BucketNode>() - size_of::<BlockIndexMap>() + 2 * word;
    println!(
        "\n  CEILING: were the entry ever to fit in a word, the node would be {} B against \
         today's {} -- {} B a bucket, and every single-page bucket would allocate nothing. That is \
         the prize, and the address format is what stands between this engine and it.",
        ceiling,
        size_of::<BucketNode>(),
        size_of::<BucketNode>() - ceiling
    );
    assert!(
        ceiling < size_of::<BucketNode>(),
        "the tiering would not narrow the node at all, so there is no prize to describe"
    );
}

// ---------------------------------------------------------------------------------------------
// THE COUNTING ALLOCATOR, WHICH IS THE INSTRUMENT THAT SETTLES IT.
// ---------------------------------------------------------------------------------------------

/// What one `clone()` of a value charges the allocator.
///
/// Cloning a `BTreeMap` rebuilds it node for node, so the bytes charged across one clone of a
/// bucket map are the map's OWN allocations -- measured, and owing nothing to `size_of` x count.
/// #1958 measured the allocator charging 1.58x its own arithmetic on this very map, because a
/// B-tree node holds eleven value slots whether or not it fills them.
#[cfg(feature = "alloc-probe")]
fn clone_alloc_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.alloc_bytes
}

/// What one `clone()` charges in allocation CALLS, which is immune to the store path length.
#[cfg(feature = "alloc-probe")]
fn clone_allocs<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.allocs
}

/// THE PLANTED MARKER. Recovered exactly, or every figure below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument reporting
/// near zero makes a shape look free, and a decline argued on a blind instrument is worse than no
/// decline at all.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_clone_instrument_used_here_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let measured = clone_alloc_bytes(&marker);
    println!(
        "planted {PLANTED} B, instrument charged {measured} B ({:.4}x)",
        measured as f64 / PLANTED as f64
    );
    assert_eq!(
        PLANTED as u64, measured,
        "the clone instrument charged {measured} B for a planted {PLANTED} B; it is not measuring \
         what it is being read as measuring"
    );
}

/// Build the mirror node holding the live index, from a real node.
fn mirror_live_node(node: &BucketNode) -> MirrorNode<BlockIndexMap> {
    MirrorNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        object_index: node.object_index.clone(),
        deleted_object_index: node.deleted_object_index.clone(),
        block_index: node.block_index.clone(),
        tombstone_elements: node.tombstone_elements.clone(),
    }
}

/// The same node with its block index in the boxed shape, holding the SAME block set.
fn mirror_boxed_node(node: &BucketNode) -> MirrorNode<MirrorBoxedBlockIndex> {
    let entries: Vec<(u64, BlockIndex)> = node
        .block_index
        .iter()
        .map(|(handle, block)| (*handle, block.clone()))
        .collect();
    let block_index = match entries.len() {
        0 => MirrorBoxedBlockIndex::Empty,
        1 => {
            let (handle, block) = entries.into_iter().next().expect("length is one");
            MirrorBoxedBlockIndex::One(handle, Box::new(block))
        }
        _ => MirrorBoxedBlockIndex::Many(entries.into_iter().collect()),
    };
    MirrorNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        object_index: node.object_index.clone(),
        deleted_object_index: node.deleted_object_index.clone(),
        block_index,
        tombstone_elements: node.tombstone_elements.clone(),
    }
}

/// The order production builds the bucket map in, approximated: NOT ascending.
///
/// This matters more than it looks, and the first version of this test got it wrong. A `BTreeMap`
/// COLLECTED from a sorted iterator is built with full nodes; one built by `insert` as keys arrive
/// is about two thirds full, which is exactly the 63.2% / 63.6% #1958 measured. `clone()`
/// reproduces whichever shape its source has, so a mirror built by `collect` measures a tree with
/// a third fewer nodes and almost no unfilled slot.
///
/// THE UNFILLED SLOTS ARE WHERE A NARROWER VALUE TYPE WINS. A node holds eleven value slots
/// whether or not it fills them, so eighty bytes off the value is eighty bytes off every slot
/// including the empty ones -- and at two thirds full there are half again as many slots per
/// bucket as at full. The fill therefore decides how much the narrower shape saves, and the saving
/// is what the added allocation has to be set against. Measuring the candidate on a full tree and
/// the incumbent on production's tree would have compared two different questions.
fn production_order(keys: &[u32]) -> Vec<u32> {
    let mut order: Vec<u32> = keys.to_vec();
    order.sort_by_key(|key| (*key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    order
}

/// Buckets per allocated B-tree node, and what fraction of the eleven value slots that fills.
///
/// `node_allocations` must EXCLUDE any allocation that is not a tree node -- the boxed arm makes
/// one per single-block bucket, and counting those would report a tree that was 8% full.
#[cfg(feature = "alloc-probe")]
fn fill(entries: usize, node_allocations: u64) -> (f64, f64) {
    if node_allocations == 0 {
        return (0.0, 0.0);
    }
    let per_node = entries as f64 / node_allocations as f64;
    (per_node, per_node / 11.0)
}

/// BOXING THE SINGLE-BLOCK ARM, MEASURED BY THE ALLOCATOR RATHER THAN ARGUED FROM ITS WIDTH.
///
/// #1958 priced this shape from its width and the allocator's rounding: minus eighty bytes on the
/// struct, plus a 104-byte request served out of a chunk of at least 112 B for very nearly every bucket. It
/// never ran the allocator over it, and its own headline result -- that the allocator charges
/// 1.58x the `size_of` arithmetic on this map -- is precisely why arithmetic is not enough here.
///
/// AND THE ANSWER IS NOT THE ONE #1958 PUBLISHED. Its row set the node saving at 80 bytes a
/// bucket -- `size_of` arithmetic -- against an allocation of 112 bytes a bucket, which is an
/// allocator figure, and concluded +32 a bucket. The two are not the same instrument, and the
/// whole point of #1958's own headline result is that they differ by 1.58x here: a B-tree node
/// carries eleven value slots whether or not it fills them, so eighty bytes off the value type is
/// eighty bytes off EVERY slot, filled or not. At the fill this map actually runs at that is about
/// 127 bytes a bucket saved, not 80, and the row's sign reverses. Measured below rather than
/// re-argued.
///
/// THE CONTROL COMES FIRST AND IS NOT OPTIONAL. The mirror node holding the LIVE index has to
/// charge EXACTLY what a map of real `BucketNode` built the same way charges. The first version of
/// this test built the mirrors by `collect` and the control failed by 4.8 MB -- which is the whole
/// reason `production_order` exists, and a good deal more interesting than the assertion passing
/// would have been.
///
/// THE ONE THING THE HARNESS CANNOT REPRODUCE IS PRODUCTION'S EXACT FILL, so the claim is made in
/// the CONSERVATIVE direction instead of being fitted to it. A lower fill means more nodes, more
/// unfilled slots, and a larger saving for the narrower value -- so a harness tree that is FULLER
/// than production's understates the boxed shape's byte win. The test asserts that it is fuller,
/// and the number it reports is therefore a floor on the win and not a fit to it.
///
/// WHAT THE COUNTING ALLOCATOR DOES NOT SEE, stated because it eats most of the difference: it
/// charges the bytes REQUESTED, and a 104-byte request is served out of a larger chunk once the
/// allocator has taken its header and rounded to a size class. Every added allocation below is
/// therefore charged at least eight bytes short of what it costs. The COUNT is the figure that
/// carries, because it moves with nothing.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds 40,000 records and clones six bucket maps; run by name"]
fn boxing_the_single_block_arm_costs_an_allocation_for_every_bucket_that_holds_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_length = dir.path().as_os_str().len();
    let engine = probe_engine(dir.path());
    seed_routed_keys(&engine, 40_000, 40, 1_000);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let hist = blocks_per_bucket(&engine);
    hist.report("the store this is measured on");

    let buckets = hist.buckets();
    let single_block = hist.holding(1);
    assert!(buckets > 0, "denominator: the shard holds no buckets");
    assert!(
        hist.multi_block() > 0,
        "the fixture reaches no multi-page bucket, so the Many arm is unexercised on both sides"
    );
    assert!(
        single_block > 0,
        "denominator: no bucket holds exactly one page, so the shape under test is unreached"
    );

    let keys: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();
    let order = production_order(&keys);
    assert_eq!(buckets, order.len(), "the insert order lost buckets");

    // --- Built the way production builds it: one insert at a time, not in key order. ---
    //
    // What the CONTROL is measured against: real `BucketNode`s, the type production stores, in a
    // tree built exactly as the mirrors are. If the mirror does not charge what this charges, the
    // mirror is not a mirror.
    let mut partner: BTreeMap<u32, BucketNode> = BTreeMap::new();
    let mut live_mirror: BTreeMap<u32, MirrorNode<BlockIndexMap>> = BTreeMap::new();
    let mut boxed_mirror: BTreeMap<u32, MirrorNode<MirrorBoxedBlockIndex>> = BTreeMap::new();
    for routing_bucket in &order {
        let node = shard
            .bucket_index
            .bucket_map
            .get(routing_bucket)
            .expect("the bucket is in the map it was keyed from");
        partner.insert(*routing_bucket, node.clone());
        live_mirror.insert(*routing_bucket, mirror_live_node(node));
        boxed_mirror.insert(*routing_bucket, mirror_boxed_node(node));
    }

    // --- And built full, from a sorted iterator, as the second row. ---
    let live_full: BTreeMap<u32, MirrorNode<BlockIndexMap>> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, node)| (*routing_bucket, mirror_live_node(node)))
        .collect();
    let boxed_full: BTreeMap<u32, MirrorNode<MirrorBoxedBlockIndex>> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, node)| (*routing_bucket, mirror_boxed_node(node)))
        .collect();

    for (name, held) in [
        ("control partner", partner.len()),
        ("live mirror", live_mirror.len()),
        ("boxed mirror", boxed_mirror.len()),
        ("live mirror, full", live_full.len()),
        ("boxed mirror, full", boxed_full.len()),
    ] {
        assert_eq!(buckets, held, "the {name} lost buckets on the way in");
    }

    let real_bytes = clone_alloc_bytes(&shard.bucket_index.bucket_map);
    let real_calls = clone_allocs(&shard.bucket_index.bucket_map);
    let partner_bytes = clone_alloc_bytes(&partner);
    let partner_calls = clone_allocs(&partner);
    let live_bytes = clone_alloc_bytes(&live_mirror);
    let live_calls = clone_allocs(&live_mirror);
    let boxed_bytes = clone_alloc_bytes(&boxed_mirror);
    let boxed_calls = clone_allocs(&boxed_mirror);
    let live_full_bytes = clone_alloc_bytes(&live_full);
    let live_full_calls = clone_allocs(&live_full);
    let boxed_full_bytes = clone_alloc_bytes(&boxed_full);
    let boxed_full_calls = clone_allocs(&boxed_full);

    // A boxed arm allocates once per single-block bucket; those are not tree nodes and must come
    // out before any fill is reported, or the boxed rows read as an 8%-full tree.
    let boxes = single_block as u64;

    println!("\n=== one clone of the bucket map, by the counting allocator ===");
    println!("  store path length held at {path_length} characters");
    println!(
        "  buckets={buckets}  single-page={single_block}  pages={}",
        hist.blocks()
    );
    println!(
        "  {:<40} {:>12} {:>12} {:>12} {:>22}",
        "map", "bytes", "allocations", "tree nodes", "buckets/node (of 11)"
    );
    for (name, bytes, calls, node_calls) in [
        ("the real bucket map, as production built it", real_bytes, real_calls, real_calls),
        ("control partner: real nodes, harness order", partner_bytes, partner_calls, partner_calls),
        ("mirror, live index           [CONTROL]", live_bytes, live_calls, live_calls),
        ("mirror, single page boxed", boxed_bytes, boxed_calls, boxed_calls - boxes),
        ("mirror, live index, FULL tree", live_full_bytes, live_full_calls, live_full_calls),
        ("mirror, boxed, FULL tree", boxed_full_bytes, boxed_full_calls, boxed_full_calls - boxes),
    ] {
        let (per_node, fraction) = fill(buckets, node_calls);
        println!(
            "  {name:<40} {bytes:>12} {calls:>12} {node_calls:>12} {:>15.2} ({:>4.1}%)",
            per_node,
            100.0 * fraction
        );
    }

    // --- THE CONTROL, and it is exact. The mirror node has to be indistinguishable from the
    // --- real node to the allocator when the two trees are built identically. ---
    assert_eq!(
        partner_bytes, live_bytes,
        "the mirror of the live index charges {live_bytes} B where a tree of REAL BucketNodes \
         built the same way charges {partner_bytes} B; the mirror is not a mirror and every \
         figure below is fiction"
    );
    assert_eq!(
        partner_calls, live_calls,
        "the mirror of the live index makes {live_calls} allocations where a tree of real \
         BucketNodes built the same way makes {partner_calls}"
    );
    assert!(
        real_calls > 0 && live_calls > 0,
        "a clone charged zero allocations; the probe is not counting and every row is a zero that \
         reads as good news"
    );

    // --- AND THE DIRECTION. The harness tree must be FULLER than production's, so the byte
    // --- figure below is a floor on the boxed shape's win rather than a fit to it. ---
    let (real_per_node, real_fraction) = fill(buckets, real_calls);
    let (harness_per_node, harness_fraction) = fill(buckets, live_calls);
    println!(
        "\n  fill: production {real_per_node:.2} buckets a node ({:.1}%), harness \
         {harness_per_node:.2} ({:.1}%)",
        100.0 * real_fraction,
        100.0 * harness_fraction
    );
    assert!(
        live_calls <= real_calls,
        "the harness tree has {live_calls} nodes against production's {real_calls} -- it is EMPTIER \
         than production's, so the narrower value type has more unfilled slot to save here than it \
         would in production and the figure below would be an overstatement rather than a floor"
    );

    // --- THE CANDIDATE, at production's fill and at full. ---
    let per_bucket_saving = size_of::<BucketNode>() - size_of::<MirrorNode<MirrorBoxedBlockIndex>>();
    let arithmetic_saving = per_bucket_saving * buckets;
    let measured = boxed_bytes as i64 - live_bytes as i64;
    let measured_full = boxed_full_bytes as i64 - live_full_bytes as i64;
    let extra_calls = boxed_calls as i64 - live_calls as i64;
    let extra_calls_full = boxed_full_calls as i64 - live_full_calls as i64;

    println!("\n=== what boxing the single page actually costs ===");
    println!(
        "  by size_of arithmetic      : {:>+12} B  ({per_bucket_saving} B a bucket over {buckets} \
         buckets)",
        -(arithmetic_saving as i64)
    );
    println!(
        "  by the allocator, as built : {measured:>+12} B  {extra_calls:>+12} allocations  \
         ({:+.2} B a bucket)",
        measured as f64 / buckets as f64
    );
    println!(
        "  by the allocator, full tree: {measured_full:>+12} B  {extra_calls_full:>+12} \
         allocations  ({:+.2} B a bucket)",
        measured_full as f64 / buckets as f64
    );

    // WHAT #1958 SAID, RECONSTRUCTED, AND WHERE IT PARTS COMPANY WITH THE ALLOCATOR.
    //
    // Its row priced the node saving by `size_of` (80 B a bucket) and the allocation by the
    // allocator (112 B a bucket) and subtracted one from the other. The node saving does not stop
    // at the node: the value slot it sits in is one of eleven a tree node carries whether or not
    // it fills them, so the saving per bucket is the width saving times the SLOTS per bucket.
    let slots_per_bucket = 11.0 * live_calls as f64 / buckets as f64;
    println!("\n=== the two instruments, side by side, per bucket ===");
    println!(
        "  node saving by size_of                : {per_bucket_saving:>8} B a bucket"
    );
    println!(
        "  slots a bucket actually occupies      : {slots_per_bucket:>8.3}  (11 a node, \
         {:.2} buckets a node)",
        buckets as f64 / live_calls as f64
    );
    println!(
        "  node saving as the allocator sees it  : {:>8.1} B a bucket  ({:.2}x the arithmetic)",
        per_bucket_saving as f64 * slots_per_bucket,
        slots_per_bucket
    );
    println!(
        "  the boxed page, charged               : {:>8} B a bucket that holds one page",
        size_of::<BlockIndex>()
    );
    println!(
        "  net, measured                         : {:>+8.1} B a bucket",
        measured as f64 / buckets as f64
    );

    // ONE ALLOCATION PER SINGLE-BLOCK BUCKET, at both fills. This is the figure that does not move
    // with the tree shape, the store path, or the box, and it is the cost the width cannot show.
    for (label, calls) in [("as built", extra_calls), ("full tree", extra_calls_full)] {
        assert!(
            calls as usize >= single_block,
            "boxing charged {calls} extra allocations ({label}) against {single_block} single-page \
             buckets; it is supposed to be at least one apiece, and fewer means the shape under \
             test is not being built"
        );
    }
    println!(
        "\n  the allocation count is the durable figure: {:+} allocations for {single_block} \
         single-page buckets, {:.4} apiece, and it does not move with the tree shape",
        extra_calls,
        extra_calls as f64 / single_block as f64
    );

    // WHAT THE ALLOCATOR UNDERCHARGES. Each added request is 104 bytes and is served out of a
    // larger chunk; at eight bytes of rounding apiece that is what the counted figure omits.
    let undercount = (single_block * 8) as i64;
    println!(
        "  and the counted bytes omit the allocator's own rounding: {single_block} requests of {} \
         B, at least {undercount} B of chunk beyond what is charged above",
        size_of::<BlockIndex>()
    );
    println!(
        "  net of that rounding, as built: {:+} B ({:+.2} B a bucket)",
        measured + undercount,
        (measured + undercount) as f64 / buckets as f64
    );

    // THE SIGN, REPORTED RATHER THAN ASSERTED IN ONE DIRECTION.
    //
    // This test is not here to confirm a conclusion. It asserts what it can prove -- the added
    // allocation per single-block bucket, which is what the width cannot show -- and reports the
    // byte figure with the instruments' disagreement laid out beside it. The byte figure depends
    // on the tree's fill and the allocator's rounding, both of which are measured above; the count
    // does not depend on either.
    if measured + undercount < 0 {
        println!(
            "\n  SIGN: by bytes alone the boxed shape is CHEAPER here by {} B ({:.2} B a bucket), \
             which is the opposite of the +32 B a bucket #1958 computed -- because its node saving \
             was `size_of` arithmetic and its allocation was an allocator figure, and at this \
             tree's fill a bucket occupies {slots_per_bucket:.2} value slots and not one.",
            -(measured + undercount),
            -(measured + undercount) as f64 / buckets as f64
        );
    } else {
        println!(
            "\n  SIGN: by bytes the boxed shape costs {} B more ({:.2} B a bucket)",
            measured + undercount,
            (measured + undercount) as f64 / buckets as f64
        );
    }
    println!(
        "  WHAT IT BUYS THAT WITH: {single_block} allocations that did not exist, one for every \
         bucket that holds a single page -- a per-bucket allocation on the write path, one \
         dependent load on every page read, and {single_block} small scattered chunks for the \
         allocator to fragment around. The bytes are within this instrument's own rounding; the \
         allocations are not."
    );

    // --- THE RESIDUAL, reported rather than assumed away, with its own planted marker in
    // --- `the_clone_instrument_used_here_recovers_a_planted_megabyte_exactly`. ---
    let node_widths = (size_of::<BucketNode>() * buckets) as i64;
    let residual = real_bytes as i64 - node_widths;
    println!(
        "\n  residual: the map charges {residual} B beyond {node_widths} B of node width \
         ({:.2} B a bucket) -- key arrays, node headers, the value slots nothing filled, and what \
         each node owns on the heap",
        residual as f64 / buckets as f64
    );
    assert!(
        residual > 0,
        "the residual is {residual}; at or below zero the two instruments are not independent and \
         the comparison cannot notice anything"
    );
}

// ---------------------------------------------------------------------------------------------
// THE READ PATH, WHICH A FOOTPRINT MEASUREMENT CANNOT SEE.
// ---------------------------------------------------------------------------------------------

/// READING A BLOCK: THE DECLINED INLINE ARM AGAINST THE SHIPPED LIST AND AGAINST A BOX.
///
/// A footprint measurement cannot see this, and a shape that makes the common path slower to make
/// the structure smaller is not a win however the bytes come out. An INLINE entry answers a lookup
/// out of the node's own bytes; anything held out of line has to follow a pointer into a separate
/// allocation first, and that is a DEPENDENT load -- the address of the block is not known until the
/// node has been read, so it cannot be issued in parallel with reading the node.
///
/// THE FINDING THIS TEST WAS WRITTEN TO MAKE STILL HOLDS, AND THE SHAPE IT REFUSED IS NOW THE
/// SHIPPED ONE. It was written to refuse a BOXED single-block arm on the grounds that the inline arm
/// did not pay that load. The reading was correct; what it could not see is that the inline arm's
/// WIDTH was paid by every bucket in the map, and at the shipped routing range almost none holds
/// exactly one block. So the entry is behind a pointer now, and the dependent load is a cost this
/// change ACCEPTED with its eyes open rather than one it avoided. Saying so is the point of keeping
/// this test: it is the read-path half of that trade, measured.
///
/// AND IT IS WHY THE READ PATH DOES NOT SEPARATE BOXING FROM DROPPING THE ARM. A box and a one-entry
/// list are the SAME indirection here -- one dependent load each -- and they are the same width too.
/// The two are separated entirely by what they allocate, which is a byte question and is priced in
/// `inline_arm_trade.rs`. What the read path DOES say is that both keep the single-block lookup out of
/// `find_block`... for the box only: a list would send it through a bisection. That asymmetry is
/// counted in `inline_arm_trade::the_read_path_examines_no_entry_for_a_single_block_bucket_and_one_if_the_arm_were_dropped`.
///
/// MEASURED ABBA, not A then B: this box runs other work, and a drift between the first and second
/// half of a run lands entirely on whichever arm ran second. ABBA cancels a linear drift; an A/B
/// does not. The claim carried is the structural fact of the extra dependent load and the WIDTHS,
/// both immune to load; the timing is reported as supporting and its own spread is printed so a
/// reader can see whether it separated at all.
#[test]
#[ignore = "times four million lookups; run by name"]
fn reading_a_block_out_of_line_costs_a_dependent_load_an_inline_entry_did_not() {
    const BUCKETS: usize = 20_000;
    const ROUNDS: usize = 50;

    let mut live = BlockSlabLiveIndex::default();
    // THE SHIPPED SHAPE: the single-block arm holding a BOX, built through the production insert.
    let mut listed: Vec<(u64, BlockIndexMap)> = Vec::with_capacity(BUCKETS);
    for i in 0..BUCKETS {
        let mut index = BlockIndexMap::default();
        let (object_key, entry) = block_for(i as u64);
        let handle = index.insert(&object_key, entry, &mut live);
        listed.push((handle, index));
    }
    // THE DECLINED SHAPE, as a mirror: the entry held in the node's own bytes.
    let inline: Vec<(u64, MirrorInlineBlockIndex)> = listed
        .iter()
        .map(|(handle, index)| {
            let block = index.get(handle).expect("the page is there").clone();
            (*handle, MirrorInlineBlockIndex::One(*handle, block))
        })
        .collect();
    let boxed: Vec<(u64, MirrorBoxedBlockIndex)> = listed
        .iter()
        .map(|(handle, index)| {
            let block = index.get(handle).expect("the page is there").clone();
            (*handle, MirrorBoxedBlockIndex::One(*handle, Box::new(block)))
        })
        .collect();

    // --- Proof every arm is in the shape it is named for. ---
    assert!(
        listed
            .iter()
            .all(|(_, index)| matches!(index, BlockIndexMap::One(..))),
        "the shipped arm is not holding its pages behind pointers, so the treatment did not run"
    );
    assert!(
        inline
            .iter()
            .all(|(_, index)| matches!(index, MirrorInlineBlockIndex::One(..))),
        "the inline mirror is not holding its pages inline, so the treatment did not run"
    );
    assert!(
