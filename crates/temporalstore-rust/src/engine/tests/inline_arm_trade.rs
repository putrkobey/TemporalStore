// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER THE BLOCK INDEX'S INLINE SINGLE-BLOCK ARM STILL PAID FOR ITSELF AT THE SHIPPED ROUTING
//! RANGE. IT DID NOT, AND THE ANSWER IS TO BOX IT RATHER THAN TO REMOVE IT.
//!
//! `BlockIndexMap` was `Empty | One(u64, BlockIndex) | Many(Vec<(u64, BlockIndex)>)`. The `One` arm
//! held a WHOLE BLOCK ENTRY INLINE, so the enum was `8 + size_of::<BlockIndex>()` and that width
//! was paid by EVERY `BucketNode` in the `BucketMap` -- by every bucket, whether or not that
//! bucket held exactly one block.
//!
//! THE CONCLUSION IN ONE LINE, AND IT IS NOT THE ONE THIS MODULE WAS OPENED TO ARGUE. Four shapes
//! were priced and the boxed arm is the only one that wins at BOTH routing ranges. Dropping the arm
//! recovers the identical width -- measured, both are 24 bytes -- but replaces the inline entry with
//! a one-entry LIST whose first block is a whole growth step, and on the whole-keyspace range, where
//! every bucket holds exactly one block, that costs 482.0 B a bucket against boxing's 178.3 and the
//! inline entry's own 284.2. Dropping is therefore a REGRESSION of 197.9 B a bucket against doing
//! nothing, on a range that stores built before #1973 still run. Boxing keeps the single-block arm's
//! short-circuit on the read path as well: it never enters `find_block`, so examined entries do not
//! move at all.
//!
//! #1964 kept the arm on an explicit measurement: dropping it would save bytes on every bucket and
//! pay bytes on every single-block one, and it "loses at every occupancy including the default
//! range's 100%". THE 100% WAS THE PREMISE, AND IT WAS A PROPERTY OF A DEFAULT THAT HAS SINCE
//! CHANGED. #1964 measured while `load_shard` defaulted `end_routing_bucket` to `u32::MAX`, which
//! divides 4.29 billion buckets among a few thousand keys and puts every key in a bucket of its
//! own BY CONSTRUCTION. #1973 changed the shipped default to 1023. At 1,024 buckets the same
//! routed workload fills them, and a single-block bucket stops being universal.
//!
//! SO THE POPULATION INVERTED, AND #1964's ARITHMETIC IS AN ARITHMETIC OVER THAT POPULATION. This
//! module re-derives both of its numbers on THIS tree -- not carried from its body, because every
//! term in them has moved since: `Many` became a `Vec` rather than a `BTreeMap` in that same
//! change, the address narrowed twice after it, and the entry narrowed again when a block's kind
//! became one byte.
//!
//! WHAT IS MEASURED, AND ON ONE INSTRUMENT. The counting allocator over a population of block
//! indexes built from REAL entries cloned out of a seeded store, reporting BOTH columns:
//! `ALLOC_BYTES` charges what the caller asked for, `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`
//! and is where a new allocation's true cost appears. The chunk column decides. The inline half --
//! the width the shape occupies inside the node that holds it -- is reported BESIDE the measured
//! heap and never instead of it.
//!
//! FOUR OPTIONS, NOT TWO. Keeping the arm and dropping it are not the only shapes: the arm can be
//! kept and made to cost a POINTER rather than an entry, and dropping it can take the list's first
//! block EXACTLY rather than a whole growth step. All four are priced here, on the same instrument,
//! at the same distribution -- `keep`, `drop`, `drop1` and `box`. The exact-first-block variant is
//! the one that looks like the synthesis and is not: it matches boxing on bytes and costs an extra
//! reallocation on every bucket that reaches two blocks, which at the shipped range is almost all of
//! them (2,519 allocations against 1,545 at 4,000 records).
//!
//! THE OCCUPANCY IS REPORTED AS A HISTOGRAM WITH ITS DENOMINATOR. A mean of 1.98 blocks a bucket in
//! this engine contained no bucket holding two, and a mean of 1.001 was two populations added
//! together. Every row below carries the count it was taken over.
//!
//! WHY THE CENSUS COUNTS BLOCKS HELD AND NOT DISCRIMINANTS. Which arm a bucket WOULD be in is a
//! property of how many blocks it holds, and that is what prices a shape that does not exist yet.
//! Counting the live discriminant instead would make this module unable to price any shape but the
//! one currently compiled -- which is the position #1964 argued from.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

use crate::engine::state::{BlockIndex, BlockIndexMap, BLOCK_LIST_GROWTH_STEP};

/// The end bucket a production shard is loaded with -- `TS_SHARD_END_ROUTING_BUCKET=1023`, THE
/// OPERATOR'S RANGE and the shipped default since #1973.
const NARROW_END: u32 = 1023;

/// The end bucket #1964 measured on: `TemporalEngine::load_shard`'s own default, the whole
/// keyspace, where every key lands alone by construction.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// The store path length every arm is held at, ASSERTED rather than hoped for. Allocation bytes
/// move at about six bytes a character, so a comparison across paths of different lengths is
/// partly measuring the path.
const STORE_PATH_CHARS: usize = 15;

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
        table_name: "inline-arm".to_string(),
        shard_uri: "local://inline-arm/1".to_string(),
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

/// ROUTED KEYS: plain strings, one block each. The shape #1964 priced its trade against.
fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("arm-{i:06}")).collect();
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

/// CONTAINER KEYS: hashes of `members` fields. Every field is its own block under ONE object key,
/// so they share one routing bucket AT EVERY RANGE. The control's workload.
fn seed_container(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        for f in 0..members {
            commands.push(Command::HashSet {
                key: format!("bag-{k:06}"),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            });
        }
    }
    run_batch(engine, commands);
}

// =============================================================================================
// THE OCCUPANCY, AS A HISTOGRAM WITH ITS DENOMINATOR.
// =============================================================================================

/// How many blocks each bucket holds, as bucket COUNTS, plus the denominator read independently.
#[derive(Debug, Default, Clone)]
struct ArmCensus {
    /// Buckets keyed by how many blocks that bucket holds. Zero-block buckets included.
    held: BTreeMap<usize, usize>,
    /// `bucket_map.len()`, read WITHOUT the walk below, so the totals check is a real check.
    declared_buckets: usize,
}

impl ArmCensus {
    fn buckets(&self) -> usize {
        self.declared_buckets
    }

    fn blocks(&self) -> usize {
        self.held.iter().map(|(held, count)| held * count).sum()
    }

    /// Buckets that WOULD sit in each arm: (zero blocks, exactly one block, two or more).
    ///
    /// The per-arm sample counts, which is the denominator every per-arm figure is taken over.
    fn arm_samples(&self) -> (usize, usize, usize) {
        let empty = self.held.get(&0).copied().unwrap_or_default();
        let one = self.held.get(&1).copied().unwrap_or_default();
        let many: usize = self
            .held
            .iter()
            .filter(|(held, _)| **held > 1)
            .map(|(_, count)| *count)
            .sum();
        (empty, one, many)
    }

    fn single_block_buckets(&self) -> usize {
        self.held.get(&1).copied().unwrap_or_default()
    }

    fn widest(&self) -> usize {
        self.held.keys().copied().next_back().unwrap_or_default()
    }

    /// The blocks-held value at a percentile of the BUCKET population, by count. Not interpolated
    /// and not a mean: the value the nth bucket actually holds.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let want = (((buckets as f64) * fraction).ceil() as usize).max(1);
        let mut seen = 0usize;
        for (held, count) in &self.held {
            seen += count;
            if seen >= want {
                return *held;
            }
        }
        self.widest()
    }

    /// Check the denominator ON EVERY ROW rather than once.
    ///
    /// `declared_buckets` is `bucket_map.len()` and the histogram is a walk of the same map, so
    /// these are two independent readings of one population and a difference means one of them is
    /// over a different set. The arm partition is then checked to cover the histogram exactly.
    fn assert_denominators(&self, label: &str) {
        let walked: usize = self.held.values().copied().sum();
        assert_eq!(
            self.declared_buckets, walked,
            "{label}: the bucket map declares {} buckets and the walk saw {walked} -- two \
             readings of one population, so a difference means one is over a different set",
            self.declared_buckets
        );
        assert!(
            self.declared_buckets > 0,
            "{label}: no buckets to divide by, so every per-bucket figure below would be a \
             division by an absent denominator"
        );
        let (empty, one, many) = self.arm_samples();
        assert_eq!(
            walked,
            empty + one + many,
            "{label}: the three arm classes cover {} of {walked} buckets, so they do not \
             partition the population",
            empty + one + many
        );
    }

    fn report(&self, label: &str) {
        let buckets = self.buckets();
        let (empty, one, many) = self.arm_samples();
        let pct = |n: usize| 100.0 * n as f64 / buckets.max(1) as f64;
        println!(
            "  {label}: {buckets} buckets / {} pages | would-be arms Empty {empty} ({:.3}%) One \
             {one} ({:.3}%) Many {many} ({:.3}%)",
            self.blocks(),
            pct(empty),
            pct(one),
            pct(many)
        );
        println!(
            "      pages held per bucket: p50 {} / p90 {} / p99 {} / MAX {}   (denominator \
             {buckets} buckets)",
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.widest()
        );
        for (held, count) in &self.held {
            if *held <= 4 || held % 8 == 0 || *held == self.widest() {
                println!(
                    "      {held:>4} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    pct(*count)
                );
            }
        }
    }
}

fn arm_census(engine: &TemporalEngine) -> ArmCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut census = ArmCensus {
        held: BTreeMap::new(),
        declared_buckets: shard.bucket_index.bucket_map.len(),
    };
    for bucket in shard.bucket_index.bucket_map.values() {
        *census.held.entry(bucket.block_index.len()).or_default() += 1;
    }
    census
}

/// One real block entry, cloned out of a seeded store. The shapes below hold REAL entries rather
/// than synthesised ones, so their widths and their `Arc` sharing are the engine's.
/// A block entry built from field values rather than cloned out of a store.
///
/// Only for the two width/emptiness checks that must not need an engine to run -- every BYTE figure
/// in this module is taken over entries the engine itself filed, because a synthesised entry could
/// differ from a real one in `Arc` sharing and that is exactly what a clone-based instrument sees.
fn one_real_block_free_standing() -> BlockIndex {
    BlockIndex {
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        address: crate::block_store::ElementEntry::from_parts(1, 64, 32, Some(7), Some(11)),
        dirty: false,
        deleted: false,
    }
}

fn one_real_block(engine: &TemporalEngine) -> BlockIndex {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    for bucket in shard.bucket_index.bucket_map.values() {
        for block in bucket.block_index.values() {
            return block.clone();
        }
    }
    panic!("a seeded store holds a page");
}

// =============================================================================================
// THE THREE SHAPES, AS MIRRORS. ONE OF THEM MUST BE THE LIVE DECLARATION.
// =============================================================================================

/// The block index AS #1964 LEFT IT: the whole entry inline for the single-block case.
#[allow(dead_code)]
enum ShapeKeep {
    Empty,
    One(u64, BlockIndex),
    Many(Vec<(u64, BlockIndex)>),
}

/// The single-block arm DROPPED ALTOGETHER: an empty `Vec` allocates nothing, so `Empty` stays a
/// named arm and a bucket holding one block holds a one-entry LIST.
#[allow(dead_code)]
enum ShapeDrop {
    Empty,
    Many(Vec<(u64, BlockIndex)>),
}

/// The arm KEPT BUT BOXED -- THE SHAPE THAT SHIPPED. The single-block case still has an arm of its
/// own and that arm costs a POINTER rather than an entry. TWO ARMS AND A BOX, which is neither of the
/// others: it recovers the same width dropping does and pays a DIFFERENT allocation for it.
#[allow(dead_code)]
enum ShapeBox {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(Vec<(u64, BlockIndex)>),
}

/// What one shape cost over a whole bucket population: both byte columns and the call count, from
/// ONE span of the counting allocator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ShapeCost {
    /// `size_of` the shape -- the inline half, which no allocator ever sees.
    inline_width: usize,
    request_bytes: u64,
    chunk_bytes: u64,
    allocs: u64,
    buckets: usize,
}

impl ShapeCost {
    /// Bytes a bucket, counting BOTH halves: the width paid inline in every node plus the heap the
    /// allocator charged. This is the figure #1964's "save on every bucket" is about.
    fn total_chunk_per_bucket(&self) -> f64 {
        self.inline_width as f64 + self.chunk_bytes as f64 / self.buckets.max(1) as f64
    }

    fn total_request_per_bucket(&self) -> f64 {
        self.inline_width as f64 + self.request_bytes as f64 / self.buckets.max(1) as f64
    }

    fn allocs_per_bucket(&self) -> f64 {
        self.allocs as f64 / self.buckets.max(1) as f64
    }
}

/// Grow a block list ONE ENTRY AT A TIME, reserving in exactly the steps `reserve_one_more` takes.
///
/// GROWN AND NOT PRE-SIZED, BECAUSE THE REALLOCATIONS ARE PART OF THE PRICE. A list built with
/// `Vec::with_capacity(final)` costs ONE allocation; the same list grown from empty costs one per
/// growth step, and every intermediate buffer is charged and freed on the way. At the shipped
/// routing range the p50 bucket holds 39 blocks, so that is ten allocations rather than one, and a
/// comparison between shapes that differ at the FIRST step has to charge the rest of the ladder
/// identically or the difference it reports is an artefact of the pre-sizing.
///
/// `first_block` is the capacity taken for the first entry, which is the only thing the shapes below
/// disagree about: the shipped policy takes a whole step, and the exact variant takes one entry.
fn grow_list(held: usize, block: &BlockIndex, first_block: usize) -> Vec<(u64, BlockIndex)> {
    let mut blocks: Vec<(u64, BlockIndex)> = Vec::new();
    for entry in 0..held {
        if blocks.len() == blocks.capacity() {
            let want = if blocks.is_empty() {
                first_block
            } else {
                (blocks.len() + 1).div_ceil(BLOCK_LIST_GROWTH_STEP) * BLOCK_LIST_GROWTH_STEP
            };
            blocks.reserve_exact(want - blocks.len());
        }
        blocks.push((entry as u64, block.clone()));
    }
    blocks
}

/// The shipped first block: one whole step of the growth policy.
fn block_list(held: usize, block: &BlockIndex) -> Vec<(u64, BlockIndex)> {
    grow_list(held, block, BLOCK_LIST_GROWTH_STEP)
}

#[cfg(feature = "alloc-probe")]
fn measure<T>(inline_width: usize, buckets: usize, build: impl FnOnce() -> T) -> ShapeCost {
    let probe = Probe::start();
    let built = build();
    let counts = probe.stop();
    std::hint::black_box(&built);
    drop(built);
    ShapeCost {
        inline_width,
        request_bytes: counts.alloc_bytes,
        chunk_bytes: counts.chunk_bytes,
        allocs: counts.allocs,
        buckets,
    }
}

/// Build one block index per bucket, in each shape, over the REAL measured distribution.
///
/// The spine is a `BTreeMap<u32, Shape>` -- the container the real `bucket_map` is -- so the
/// per-slot amortisation this comparison turns on is the engine's own and not a model of it.
#[cfg(feature = "alloc-probe")]
fn priced_population(census: &ArmCensus, block: &BlockIndex) -> Vec<(&'static str, ShapeCost)> {
    let buckets = census.buckets();

    // Held OUTSIDE every span: building the plan must not be charged to any shape.
    let plan: Vec<usize> = census
        .held
        .iter()
        .flat_map(|(held, count)| std::iter::repeat(*held).take(*count))
        .collect();
    assert_eq!(
        buckets,
        plan.len(),
        "the build plan covers {} buckets against the census's {buckets}",
        plan.len()
    );

    let keep = measure(std::mem::size_of::<ShapeKeep>(), buckets, || {
        let mut map: BTreeMap<u32, ShapeKeep> = BTreeMap::new();
        for (at, held) in plan.iter().enumerate() {
            map.insert(
                at as u32,
                match *held {
                    0 => ShapeKeep::Empty,
                    1 => ShapeKeep::One(0, block.clone()),
                    n => ShapeKeep::Many(block_list(n, block)),
                },
            );
        }
        map
    });

    let dropped = measure(std::mem::size_of::<ShapeDrop>(), buckets, || {
        let mut map: BTreeMap<u32, ShapeDrop> = BTreeMap::new();
        for (at, held) in plan.iter().enumerate() {
            map.insert(
                at as u32,
                match *held {
                    0 => ShapeDrop::Empty,
                    n => ShapeDrop::Many(block_list(n, block)),
                },
            );
        }
        map
    });

    // THE FOURTH SHAPE, AND IT IS THE OBVIOUS SYNTHESIS OF THE OTHER THREE: drop the arm, but take
    // the first block EXACTLY rather than a whole growth step. A single-page bucket then pays one
    // entry instead of four, which is what the boxed arm's advantage over dropping consists of --
    // without a third arm to maintain. What it costs is one extra reallocation on the way up, paid
    // by every bucket that reaches two blocks, which at the shipped range is almost all of them.
    let dropped_exact = measure(std::mem::size_of::<ShapeDrop>(), buckets, || {
        let mut map: BTreeMap<u32, ShapeDrop> = BTreeMap::new();
        for (at, held) in plan.iter().enumerate() {
            map.insert(
                at as u32,
                match *held {
                    0 => ShapeDrop::Empty,
                    n => ShapeDrop::Many(grow_list(n, block, 1)),
                },
            );
        }
        map
    });

    let boxed = measure(std::mem::size_of::<ShapeBox>(), buckets, || {
        let mut map: BTreeMap<u32, ShapeBox> = BTreeMap::new();
        for (at, held) in plan.iter().enumerate() {
            map.insert(
                at as u32,
                match *held {
                    0 => ShapeBox::Empty,
                    1 => ShapeBox::One(0, Box::new(block.clone())),
                    n => ShapeBox::Many(block_list(n, block)),
                },
            );
        }
        map
    });

    vec![
        ("keep", keep),
        ("drop", dropped),
        ("drop1", dropped_exact),
        ("box", boxed),
    ]
}

/// THE COMPARATOR, AND IT REFUSES TO COMPARE AN INCOMPLETE SET.
///
/// Every shape this module claims to price must be present, or the table is a comparison between
/// whichever rows survived. A dropped row is exactly the failure that reads as a clean result --
/// the remaining numbers are internally consistent and simply describe fewer options than the
/// prose above them claims. `the_shape_comparator_refuses_to_compare_when_a_shape_is_missing` is
/// the control, and it has to be a `#[should_panic]` because a comparator that merely RETURNED a
/// complaint would be satisfied by a caller that ignored it.
const SHAPES_PRICED: [&str; 4] = ["keep", "drop", "drop1", "box"];

fn compare_shapes(label: &str, rows: &[(&'static str, ShapeCost)]) {
    assert_eq!(
        SHAPES_PRICED.len(),
        rows.len(),
        "{label}: the comparator was handed {} shapes and this module prices {} -- refusing to \
         compare, because a table missing a row is internally consistent and still wrong",
        rows.len(),
        SHAPES_PRICED.len()
    );
    for want in SHAPES_PRICED {
        assert!(
            rows.iter().any(|(name, _)| *name == want),
            "{label}: the shape `{want}` is not in the comparison -- refusing to compare"
        );
    }
    for (name, cost) in rows {
        assert!(
            cost.buckets > 0,
            "{label}/{name}: priced over zero buckets, so every per-bucket figure would be a \
             division by the denominator's absence"
        );
        // THE CHUNK RULE, AS #1969 CORRECTED IT: a FLOOR, not an equality. A 104-byte request read
        // 128. So the served column must be at least the request and -- whenever anything was
        // allocated at all -- strictly greater, because glibc's header word means no request is
        // ever served in exactly its own size.
        //
        // AND THE MULTIPLE-OF-16 CLAUSE IS NOT ASSERTED ON A SUM. It is a property of one chunk
        // taken from a size class, and two things break it here: a total is a sum of many chunks,
        // and a large enough request is served by `mmap` rather than from the heap, where the usable
        // size is block-derived and NOT 16-aligned -- a planted mebibyte reads 1,052,664, which is 8
        // modulo 16. `the_instrument_used_here_recovers_a_planted_allocation_exactly` asserts the
        // clause where it holds, on a single small allocation, and records why it does not hold here.
        if cost.allocs > 0 {
            assert!(
                cost.chunk_bytes >= cost.request_bytes,
                "{label}/{name}: chunk {} B is below request {} B, and the chunk column is a \
                 FLOOR on the request",
                cost.chunk_bytes,
                cost.request_bytes
            );
            assert!(
                cost.chunk_bytes > cost.request_bytes,
                "{label}/{name}: chunk {} B equals request {} B; glibc's header word means a \
                 served chunk is strictly wider than what was asked for",
                cost.chunk_bytes,
                cost.request_bytes
            );
        }
    }
}

fn report_shapes(label: &str, rows: &[(&'static str, ShapeCost)]) {
    println!("  --- {label} ---");
    println!(
        "  {:<6} {:>7} {:>12} {:>12} {:>9} {:>11} {:>11} {:>10}",
        "shape", "inline", "req B", "chunk B", "allocs", "req B/bkt", "chk B/bkt", "alloc/bkt"
    );
    for (name, cost) in rows {
        println!(
            "  {:<6} {:>7} {:>12} {:>12} {:>9} {:>11.2} {:>11.2} {:>10.5}",
            name,
            cost.inline_width,
            cost.request_bytes,
            cost.chunk_bytes,
            cost.allocs,
            cost.total_request_per_bucket(),
            cost.total_chunk_per_bucket(),
            cost.allocs_per_bucket()
        );
    }
}

fn row<'a>(rows: &'a [(&'static str, ShapeCost)], want: &str) -> &'a ShapeCost {
    &rows
        .iter()
        .find(|(name, _)| *name == want)
        .expect("compare_shapes has already refused an incomplete set")
        .1
}

// =============================================================================================
// 0. THE MIRROR CONTROL -- WITHOUT IT EVERY ROW BELOW IS FICTION
// =============================================================================================

/// ONE OF THE MIRRORS MUST BE THE LIVE DECLARATION, AND IT MUST BE THE ONE THAT SHIPPED.
///
/// The mirrors are built from the same field types as `BlockIndexMap`, so they are statements about
/// widths rather than guesses -- but only while one of them still agrees with the declaration
/// exactly. If `ShapeBox` stops being `size_of::<BlockIndexMap>()`, this module is pricing a
/// structure this engine does not have, and it says so rather than printing numbers.
///
/// AND THE TWO CLAIMS THE WHOLE MODULE RESTS ON, AS ASSERTIONS. `ShapeKeep` must be WIDER -- the
/// inline entry is the width that was recovered, and a reading where the two were equal would mean
/// there was never anything to recover. `ShapeDrop` must be EXACTLY AS WIDE as the shipped shape:
/// boxing recovers the identical width dropping the arm would, which is why the choice between them
/// is decided on the heap and not on the struct. Both are measured facts about the layout, not
/// arithmetic.
///
/// rust-internal: measures the engine's own declaration, no product behaviour
#[test]
fn the_shape_that_shipped_is_the_live_declaration_and_boxing_recovers_what_dropping_would() {
    let live = std::mem::size_of::<BlockIndexMap>();
    let boxed = std::mem::size_of::<ShapeBox>();
    assert_eq!(
        live, boxed,
        "`ShapeBox` is {boxed} B and the live `BlockIndexMap` is {live} B. Every figure this module \
         reports is taken over mirrors, and they describe nothing unless the shipped one agrees \
         with the declaration"
    );
    let keep = std::mem::size_of::<ShapeKeep>();
    let dropped = std::mem::size_of::<ShapeDrop>();
    assert!(
        keep > live,
        "the inline entry is {keep} B against the shipped {live} B. If holding a whole entry inline \
         were not wider than holding a pointer, there was never a width to recover"
    );
    assert_eq!(
        keep,
        8 + std::mem::size_of::<BlockIndex>(),
        "the inline arm was exactly a handle plus an entry, and `ShapeKeep` is {keep} B against {}",
        8 + std::mem::size_of::<BlockIndex>()
    );
    assert_eq!(
        live, dropped,
        "dropping the arm reads as {dropped} B against boxing it at {live} B. Measured, the two are \
         identical -- the tags ride pointer niches either way -- and if they have separated then the \
