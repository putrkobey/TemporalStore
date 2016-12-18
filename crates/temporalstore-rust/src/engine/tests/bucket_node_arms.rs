// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WOULD A BUCKET THAT HOLDS ONE BLOCK BE CHEAPER AS ONE TAGGED WORD?
//!
//! THE PROPOSAL. `BucketNode` is the widest per-item structure in the engine and there is one
//! per routing bucket. #1958 took it 208 -> 200, #1961 took it 200 -> 192, #1966 took it
//! 192 -> 184 and the address merge took it 184 -> 176, all by narrowing or merging fields. The
//! shape proposed next does not narrow a field: it
//! replaces the node's three container members -- `object_index`, `deleted_object_index` and
//! `block_index`, 120 of the 168 bytes -- with ONE TAGGED WORD whose tag says whether the bucket
//! is simple or general, and puts the payload of each arm behind that word. The node is then 56
//! bytes, which is asserted here and reconstructs field by field.
//!
//! THE ANSWER IS STILL NO. IT IS NO FOR DIFFERENT REASONS THAN IT WAS, AND THE CHANGE IN REASONS
//! IS THE POINT OF THIS REVISION. When this module was written it gave three grounds; one of them
//! has since stopped being true, a second was measured on a tree that has moved, and a fourth --
//! the one that actually settles it -- was quoted as arithmetic and never measured. All four are
//! now measured, at the corpus sizes and the routing ranges below.
//!
//!   * GONE, THEN BACK, AND BIGGER: THE READ COST. This module declined the shape partly because
//!     "a tagged simple arm touches 2.0000 lines to reach a block's address against the live arm's
//!     1.5200 -- +0.4800 a read". That became +0.0000 when the address moved to 72 bytes into the
//!     node: past a whole cache line, so the node and the address it resolves through could not
//!     share a line at any alignment, inline or not, and both arms read 2.0000. IT IS **+0.6167**
//!     NOW, measured over 4,000 buckets: live 1.3833 against the tagged arm's 2.0000, with the
//!     live arm keeping both in ONE line for 2,467 of 4,000. The address sits 24 bytes into the
//!     node again -- the flag packing took four bytes off the tail ahead of it and the address
//!     merge took eight out of the inline block entry -- so a node that starts early enough in its
//!     line keeps both. The guard states the offset it depends on and REFUSES an offset past a
//!     line rather than carrying a number whose mechanism has moved; it refused the opposite
//!     direction before, and it is what caught this. WHAT THE COUNT CANNOT SEE IS STATED WITH IT:
//!     a line count is not a cost and an unequal count is not a measure of the difference, because
//!     the live arm's second line is inside the node's own allocation and the tagged arm's is in a
//!     separate one. The unmeasured remainder can only run against the tagged shape.
//!   * MOVED: THE PUBLISHED TABLE. The four cells here were measured before #1966, which took
//!     eight bytes out of the address inside the inline block entry. The assertions were retargeted
//!     then; the numbers in this comment were not. Three of the four allocation ratios still
//!     reproduce exactly and one does not, and both byte columns at the configured range moved.
//!   * SETTLED, AND IT IS THE ONE THAT DECIDES IT: THE NODE NARROWS AND THE KEY DOES NOT. A
//!     simple bucket costs 168 B inline today. Tagged, it costs a 56 B node plus the chunk the
//!     allocator serves a 104 B payload from, READ BACK FROM THE ALLOCATOR rather than taken from
//!     a formula. The bytes do not leave the key; they move from a field into a chunk.
//!     RE-MEASURED TWICE SINCE, AND THE BYTE MARGIN HAS CLOSED TO AT BEST ZERO: at a 184-byte node
//!     the pair was 192 and the key held 8 B MORE; the flag packing took both sides down by eight
//!     (176 against 184) and the address merge took both down by eight again (168 against 168),
//!     because each of them shrinks the boxed payload by exactly what it shrinks the node by. "At
//!     best" is load-bearing: the chunk a 104-byte payload is served from is not a function of the
//!     request alone, so the pair reads 168 in one process and 184 in one that allocated
//!     differently first. The BYTE argument against the shape is therefore spent at best and
//!     standing at worst -- and the line count above has come back to +0.6167 a read, which is now
//!     the ground that decides it.
//!     `the_bytes_a_tagged_key_saves_are_not_bytes_a_tagged_key_stops_holding` reads the chunk
//!     back and asserts the pair never comes in UNDER the inline width, which is the claim being
//!     refuted, rather than pinning a delta that moves with the rounding step.
//!   * STANDING: THE SAVING THAT DOES EXIST IS NOT A PER-KEY SAVING AND IT IS NOT THE SHAPE'S. It
//!     is B-tree slot waste. A `BTreeMap` node holds eleven value slots whether they are filled or
//!     not, so a narrower value fits more of them into one node -- which is worth real bytes at
//!     the DEFAULT routing range, where every key sits in a bucket of its own, and almost nothing
//!     at the range an operator is told to configure.
//!
//! THE CELLS, ON ONE INSTRUMENT, BOTH SIDES, RE-MEASURED. The REQUEST column is what the caller
//! asked the allocator for; the CHUNK column is what the allocator set aside to answer. Only the
//! second is what a key actually holds, and the tagged side is the side that allocates, so the
//! request column flatters it at every cell:
//!
//! ```text
//!   corpus / range                            simple    request      CHUNK  allocations
//!   4000 routed keys on the whole keyspace  100.000%     0.580x     0.628x       7.265x
//!   4000 routed keys on 0..1023               4.510%     0.874x     0.898x       1.317x
//!   40000 routed keys on the whole keyspace 100.000%     0.602x     0.653x       7.559x
//!   40000 routed keys on 0..1023              0.000%     0.980x     0.983x       1.304x
//! ```
//!
//! READ THE FIRST COLUMN AGAINST THE REST AND THE WHOLE FINDING IS THERE. The saving is not a
//! property of the shape; it is a property of HOW MANY BUCKETS ARE IN THE ARM THE SHAPE IS FOR,
//! and that number is decided by the routing range rather than by the workload. Two things follow,
//! and each of them on its own is the decline:
//!
//!   1. WHERE THE SHAPE WINS, A KNOB THAT ALREADY SHIPS WINS TWO AND A HALF TIMES AS MUCH FOR
//!      NOTHING. On the same instrument and the same corpus the LIVE shape reads 301.895 B/record
//!      on the whole keyspace and 120.091 B/record on 0..1023 -- 0.398x, against the tagged
//!      shape's best cell of 0.653x -- and `docs/runtime_tuning.md` already tells an operator to
//!      set exactly that before the first ingest. The knob costs no allocation, no indirection and
//!      no line of code. (This module previously published 0.632x for the same comparison, from
//!      the pre-#1966 tree.)
//!   2. WHERE THE KNOB IS SET, THE ARM IS GONE AND SO IS THE SAVING. `single_block_object` is
//!      4.510% of occupied buckets at 4,000 records on 0..1023 and 0.000% at 40,000, where
//!      `multi_object` is 100.000%. At 0.000% simple the tagged shape saves 1.7% of the chunk
//!      bytes and still costs 1.304x the allocations. That cell is the control on the explanation:
//!      a configuration where the mechanism predicts the saving should be absent, measured.
//!
//! THE PREMISE IS ALSO WRONG, AND THAT MATTERS SEPARATELY FROM THE PRICE. The proposal says the
//! node "carries the whole general case for every key". It does not. Every one of the three
//! container members is ALREADY a biased `Empty`/`One`/`Many` shape whose general arm is the
//! only one that allocates: `BlockIndexMap` holds its single block inline, `ObjectIndex` holds
//! its single id inline, `DeletedObjectIndex` is one nullable pointer that is null in the case
//! it is almost always in. A simple bucket allocates NOTHING for the general case today, so
//! there is no general case for a tagged word to take out of it. What the word moves out of
//! line is the simple bucket's OWN data -- and since that data is already a line away from the
//! node, moving it costs no read and saves no resident byte.
//!
//! AND THAT DATA DOES NOT FIT IN A WORD -- THOUGH THE ADDRESS INSIDE IT NOW DOES. A block's
//! slab id and offset are one 64-bit word here as well, but a `BlockAddress` is 32 bytes around
//! it: an identity, a length, a block id, a routing bucket and a presence byte all sit beside the
//! word, and the block entry around THAT carries three shared names, for 88. #1962 established
//! that those three names are per-object and per-block facts that cannot be hoisted to the bucket:
//! hoisting them "would have passed every test at the default range and lost blocks silently at the
//! cluster range". So the simple arm cannot be a word here, and the most it can be is a pointer to
//! 104 bytes in a chunk of AT LEAST 112 bytes -- which is the shape this module prices and
//! declines.
//!
//! WHAT WOULD CHANGE THE ANSWER, STATED SO THE NEXT REVISIT DOES NOT START FROM NOTHING. Not a
//! narrower node: the node is not what costs. The per-key figure moves only if the BLOCK ENTRY
//! gets smaller, and the entry is 88 bytes of which 48 are three `Arc<str>` names. Those are
//! per-block facts and must stay per-block -- #1962 is not in dispute -- but a per-block NAME HANDLE
//! is still a per-block fact, and four bytes rather than sixteen. That is a different change with a
//! different risk, and it is the one with the arithmetic behind it.
//!
//! THE MEASUREMENT DOES NOT DEPEND ON THE DECLINE BEING RIGHT. Every figure is printed with its
//! denominator, the mirror of the live declaration is asserted against the declaration itself
//! before any price is quoted, the store path length is held constant and stated, and the
//! instrument recovers a planted marker exactly. If the numbers move the decline should be
//! revisited rather than re-argued -- as it was here.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};

use crate::engine::state::{
    BlockIndex, BlockIndexMap, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};
use crate::engine::storage_bucket_internals::classify_bucket_layout;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023`, the
/// setting `docs/runtime_tuning.md` tells an operator to set before the first ingest.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses, and `startup_load_shard_request`'s default.
const WIDE_END: u32 = u32::MAX;

/// How many buckets the narrow range has. Derived from the range, not written down twice.
const NARROW_BUCKETS: usize = (NARROW_END as usize) + 1;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

// =============================================================================================
// FIXTURE
// =============================================================================================

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
        table_name: "node-arms".to_string(),
        shard_uri: "local://node-arms/1".to_string(),
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

/// ROUTED KEYS: plain strings, one block each.
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

/// CONTAINER KEYS: hashes of `members` fields each. Every field is its own block, and every one
/// of them routes to the container key's single bucket.
fn seed_container(engine: &TemporalEngine, keys: usize, members: usize) -> Vec<String> {
    let container_keys: Vec<String> = (0..keys).map(|k| format!("sack-{k:06}")).collect();
    let mut commands = Vec::with_capacity(keys * members);
    for key in &container_keys {
        for f in 0..members {
            commands.push(Command::HashSet {
                key: key.clone(),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            });
        }
    }
    run_batch(engine, commands);
    container_keys
}

// =============================================================================================
// THE HISTOGRAM
// =============================================================================================

/// The five layout arms with a bucket count each, plus the per-bucket block counts the
/// percentiles and the maximum are taken over.
#[derive(Debug, Default, Clone)]
struct ArmHistogram {
    /// Bucket counts keyed by the arm `classify_bucket_layout` puts them in.
    arms: BTreeMap<&'static str, usize>,
    /// Blocks held, one entry per OCCUPIED bucket, so a percentile is over buckets that exist.
    blocks: Vec<usize>,
    /// Objects held, one entry per occupied bucket.
    objects: Vec<usize>,
}

fn arm_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object_no_block",
        BucketLayoutState::SingleBlockObject => "single_block_object",
        BucketLayoutState::MultiBlockObject => "multi_block_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

/// Every arm this module ever claims a figure for. A claim about an arm with no samples is not a
/// measurement, and `assert_every_claimed_arm_has_samples` refuses one.
const EVERY_ARM: [&str; 5] = [
    "empty",
    "single_object_no_block",
    "single_block_object",
    "multi_block_object",
    "multi_object",
];

impl ArmHistogram {
    fn buckets(&self) -> usize {
        self.arms.values().copied().sum()
    }

    fn occupied(&self) -> usize {
        self.blocks.len()
    }

    fn total_blocks(&self) -> usize {
        self.blocks.iter().sum()
    }

    fn count(&self, arm: &str) -> usize {
        self.arms.get(arm).copied().unwrap_or_default()
    }

    /// The fraction of OCCUPIED buckets in one arm. Empty buckets are excluded from the
    /// denominator deliberately: a released bucket holds no blocks and would dilute every arm.
    fn occupied_fraction(&self, arm: &str) -> f64 {
        let occupied = self.occupied();
        if occupied == 0 {
            return 0.0;
        }
        self.count(arm) as f64 / occupied as f64
    }

    /// The two arms a tagged word would hold inline: one object with one block, and one object
    /// with none. Together, the fraction of buckets the proposal would actually help.
    fn simple_fraction(&self) -> f64 {
        self.occupied_fraction("single_block_object") + self.occupied_fraction("single_object_no_block")
    }

    fn percentile(sorted: &[usize], q: f64) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        // Nearest-rank, which for a count distribution answers a value that actually occurs
        // rather than an interpolation between two that do.
        let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
        sorted[rank.min(sorted.len()) - 1]
    }

    fn report(&self, label: &str) {
        let mut blocks = self.blocks.clone();
        blocks.sort_unstable();
        let mut objects = self.objects.clone();
        objects.sort_unstable();
        println!(
            "\n{label}: {} buckets, {} occupied, {} pages",
            self.buckets(),
            self.occupied(),
            self.total_blocks()
        );
        for arm in EVERY_ARM {
            println!(
                "    {arm:<22} {:>7} buckets  ({:>7.3}% of occupied)",
                self.count(arm),
                100.0 * self.occupied_fraction(arm)
            );
        }
        println!(
            "    pages/bucket   p50 {:>6}  p90 {:>6}  p99 {:>6}  MAX {:>6}",
            Self::percentile(&blocks, 0.50),
            Self::percentile(&blocks, 0.90),
            Self::percentile(&blocks, 0.99),
            blocks.last().copied().unwrap_or_default()
        );
        println!(
            "    objects/bucket p50 {:>6}  p90 {:>6}  p99 {:>6}  MAX {:>6}",
            Self::percentile(&objects, 0.50),
            Self::percentile(&objects, 0.90),
            Self::percentile(&objects, 0.99),
            objects.last().copied().unwrap_or_default()
        );
    }
}

/// The arm histogram, read off the engine's OWN classifier rather than recomputed here. A
/// separate copy of the rule would be measuring this module instead of the engine.
fn arm_histogram(engine: &TemporalEngine) -> ArmHistogram {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = ArmHistogram::default();
    for arm in EVERY_ARM {
        hist.arms.insert(arm, 0);
    }
    for node in shard.bucket_index.bucket_map.values() {
        let blocks = node.block_index.len();
        let objects = node.object_index.object_count();
        *hist
            .arms
            .entry(arm_name(classify_bucket_layout(objects, blocks)))
            .or_default() += 1;
        if blocks > 0 {
            hist.blocks.push(blocks);
            hist.objects.push(objects);
        }
    }
    hist
}

/// Refuses a run in which an arm this module quotes has no samples at all.
fn assert_every_claimed_arm_has_samples(claimed: &[&str], reached: &ArmHistogram, label: &str) {
    for arm in claimed {
        assert!(
            reached.count(arm) > 0,
            "{label}: this module quotes a figure for `{arm}` and the fixture reached ZERO \
             buckets in it; an arm with no samples is not a measurement"
        );
    }
}

/// THE ARM DISTRIBUTION, AT TWO CORPUS SIZES AND AT BOTH ROUTING RANGES.
///
/// COUNTS, PERCENTILES AND A MAXIMUM -- never a mean. #1959 published a mean of 1.98 blocks a
/// bucket over a store that contained not one bucket holding two, and priced a shape against it.
///
/// THE TWO RANGES ARE THE WHOLE POINT. A block's bucket is
/// `block_routing_bucket(object_key, start, end)`, whose modulus is the RANGE WIDTH, so the
/// engine's default of `0..u32::MAX` puts every key in a bucket of its own BY CONSTRUCTION and
/// no workload can do otherwise. `docs/runtime_tuning.md` tells an operator to set
/// `TS_SHARD_END_ROUTING_BUCKET=1023` before the first ingest, and at 1,024 buckets the same
/// keys fill them. A proposal priced only at the default is priced on a fixture, not on a
/// deployment.
///
/// THE CONTAINER WORKLOAD IS HERE BECAUSE IT IS THE ONE THE RANGE CANNOT SPLIT. Routing takes the
/// object key and never the component, so a hash's hundred fields are a hundred blocks under one
/// key and therefore in one bucket at ANY range. It is the arm distribution a narrow range
/// cannot reach and a wide range cannot escape, and it is where the maximum comes from.
///
/// THE DENOMINATOR IS ASSERTED. A run that wrote nothing would report a clean histogram of zero
/// buckets, which reads as a beautifully simple store.
///
/// rust-internal: reads the engine's own bucket index and its own classifier, no product
/// behaviour
#[test]
#[ignore = "seeds six stores up to 40,000 records each; run by name"]
fn the_arm_a_bucket_node_lands_in_is_decided_by_the_routing_range_not_by_the_workload() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut routed: BTreeMap<(usize, u32), ArmHistogram> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = arm_histogram(&engine);
            let width = if end_routing_bucket == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            hist.report(&format!("{records} routed keys on {width}"));
            assert_eq!(
                hist.total_blocks(),
                keys.len(),
                "{records}/{end_routing_bucket}: the index holds {} pages for {} written keys, so \
                 the histogram above is not over the fixture",
                hist.total_blocks(),
                keys.len()
            );
            routed.insert((records, end_routing_bucket), hist);
        }
    }

    // THE WORKLOAD THE RANGE CANNOT SPLIT, at both sizes, on the wide range -- where a
    // single-block reading would otherwise be automatic.
    let mut container: BTreeMap<usize, ArmHistogram> = BTreeMap::new();
    for (keys, members) in [(SMALL / 100, 100), (LARGE / 100, 100)] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, WIDE_END);
        let written = seed_container(&engine, keys, members);
        let hist = arm_histogram(&engine);
        hist.report(&format!(
            "{} container keys x {members} fields on the whole keyspace",
            written.len()
        ));
        assert_eq!(
            hist.total_blocks(),
            keys * members,
            "the container fixture wrote {} pages for {} expected",
            hist.total_blocks(),
            keys * members
        );
        container.insert(keys * members, hist);
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes \
         at about six bytes a character"
    );
    println!("\n  store path length held at {first} characters across all six stores");

    // --- THE CLAIM, ARM BY ARM. ---
    for records in [SMALL, LARGE] {
        let wide = routed.get(&(records, WIDE_END)).expect("wide arm");
        let narrow = routed.get(&(records, NARROW_END)).expect("narrow arm");

        assert_every_claimed_arm_has_samples(
            &["single_block_object"],
            wide,
            &format!("{records} routed keys on the whole keyspace"),
        );
        assert_every_claimed_arm_has_samples(
            &["multi_object"],
            narrow,
            &format!("{records} routed keys on 0..{NARROW_END}"),
        );

        // ON THE DEFAULT RANGE THE SIMPLE ARM IS EVERYTHING.
        assert!(
            wide.simple_fraction() > 0.99,
            "on the whole keyspace {records} routed keys put {:.5} of occupied buckets in a \
             simple arm; at that range every key lands alone by construction and this arm is \
             meant to show it",
            wide.simple_fraction()
        );

        // ON THE CONFIGURED RANGE IT IS NOT. This is the half a fixture on `load_shard(1)`
        // cannot see.
        assert!(
            narrow.simple_fraction() < 0.25,
            "on 0..{NARROW_END} {records} routed keys put {:.5} of occupied buckets in a simple \
             arm; if a configured shard were still simple-dominated the proposal would be worth \
             writing and this decline would be wrong",
            narrow.simple_fraction()
        );

        // AND THE BUCKET COUNT MOVES THE OTHER WAY, which is the whole of the second reason.
        assert!(
            narrow.buckets() <= NARROW_BUCKETS,
            "a shard on 0..{NARROW_END} reported {} buckets, more than the {NARROW_BUCKETS} the \
             range has; the modulus is not the range width",
            narrow.buckets()
        );
        println!(
            "  {records} routed keys: whole keyspace {} buckets, {:.3}% simple; 0..{NARROW_END} \
             {} buckets, {:.3}% simple -- the arm the proposal helps and the bucket count move in \
             OPPOSITE directions",
            wide.buckets(),
            100.0 * wide.simple_fraction(),
            narrow.buckets(),
            100.0 * narrow.simple_fraction()
        );
    }

    // --- THE CONTAINER WORKLOAD REACHES THE GENERAL ARM ON THE WIDE RANGE TOO. ---
    //
    // AND IT NOW REACHES `multi_block_object` RATHER THAN `multi_object`, WHICH IS THE IDENTITY
    // CHANGE SHOWING UP IN THE CLASSIFIER.
    //
    // This test asserted `multi_object` and said why: a hash's hundred fields looked like one
    // object with a hundred blocks and were not, because the object id took the COMPONENT, so a
    // hundred fields were a hundred distinct ids filed under one routing key and `object_index`
    // held a hundred rows.
    //
    // Since the component left `stable_block_object_id`, `object_index` holds ONE row for that
    // key and `classify_bucket_layout(1, many)` answers `MultiBlockObject`. So the arm the
    // container workload lands in has moved, and `multi_block_object` -- which this module used to
    // report as reached by nothing -- is now exactly what a container reaches.
    for (blocks, hist) in &container {
        assert_every_claimed_arm_has_samples(
            &["multi_block_object"],
            hist,
            &format!("{blocks} container pages on the whole keyspace"),
        );
        assert!(
            hist.occupied_fraction("single_block_object") < 0.01,
            "the container store put {:.5} of occupied buckets in `single_block_object`; it is \
             meant to be the workload the range cannot split",
            hist.occupied_fraction("single_block_object")
        );
    }

    // --- WHICH ARMS THE CORPORA NEVER REACHED, NAMED. ---
    //
    // An arm with no samples is not a measurement, so the arms this module quotes figures for
    // are asserted non-empty above and the rest are reported here as the zeroes they are. Both
    // are reachable in principle and neither changes the pricing:
    //
    //   * `single_object_no_block` is a RELEASED bucket -- `release_bucket_blocks` empties the
    //     block index and keeps `object_index` -- which holds no block in either representation
    //     and so cannot decide between them;
    //   * `multi_object` is what a container used to reach and no longer does, because its
    //     blocks now share one object id -- the routed corpora reach it on the narrow range,
    //     so it is named here only for the wide-range container stores.
    let mut never_reached: Vec<&str> = Vec::new();
    for arm in EVERY_ARM {
        let reached: usize = routed
            .values()
            .chain(container.values())
            .map(|hist| hist.count(arm))
            .sum();
        if reached == 0 {
            never_reached.push(arm);
        }
    }
    println!("  arms with ZERO samples across all six stores: {never_reached:?}");

    // THE DENOMINATOR FOR THE ARM COLUMNS. Every occupied bucket landed in exactly one arm, so
    // a percentage above is over the whole population and not over part of it.
    for hist in routed.values().chain(container.values()) {
        let counted: usize = EVERY_ARM.iter().map(|arm| hist.count(arm)).sum();
        assert_eq!(
            hist.buckets(),
            counted,
            "the arm columns account for {counted} of {} buckets; a percentage over part of a \
             population is not the population's",
            hist.buckets()
        );
    }

    // AND THE CLASSIFIER ITSELF DISTINGUISHES ALL FIVE, so a zero above is the corpus not
    // reaching an arm rather than the rule not having one.
    let distinguished: std::collections::BTreeSet<&str> = [(0, 0), (1, 0), (1, 1), (1, 4), (3, 9)]
        .into_iter()
        .map(|(objects, blocks)| arm_name(classify_bucket_layout(objects, blocks)))
        .collect();
    assert_eq!(
        EVERY_ARM.len(),
        distinguished.len(),
        "the classifier answered {} distinct arms over the five inputs that select them: \
         {distinguished:?}",
        distinguished.len()
    );

    // --- THE MAXIMUM, WHICH A MEAN HIDES. ---
    let narrow_large = routed.get(&(LARGE, NARROW_END)).expect("narrow large");
    let widest = narrow_large.blocks.iter().copied().max().unwrap_or_default();
    assert!(
        widest > 11,
        "the widest bucket on 0..{NARROW_END} at {LARGE} records holds {widest} pages; below \
         twelve a `BTreeMap` node is not even filled once and the general arm would be priced \
         against a shape it never reaches"
    );
    println!("  widest bucket on 0..{NARROW_END} at {LARGE} records: {widest} pages");
}

// =============================================================================================
// WHAT A SIMPLE BUCKET ALREADY CARRIES
// =============================================================================================

/// A SIMPLE BUCKET ALREADY ALLOCATES NOTHING FOR THE GENERAL CASE.
///
/// The proposal's premise is that the node "carries the whole general case for every key". Every
/// one of the node's three container members is already a biased shape whose general arm is the
/// only one that allocates, and this asserts it from the arms themselves rather than from the
/// declaration's comments:
///
///   * `BlockIndexMap::One` holds its block BEHIND A POINTER -- no map and no node, but one
///     allocation, which is the one member of the three whose simple spelling is not free;
///   * `ObjectIndex::One` holds its single id INLINE, and only `Many` takes a box;
///   * `DeletedObjectIndex` is one nullable pointer, null in the case it is almost always in.
///
/// So there is no general-case container for a tagged word to take out of a simple node. What it
/// would move out of line is the simple bucket's OWN block entry -- and that entry is ALREADY out of
/// line, which is the next test's subject and the reason the proposal has less left to take.
///
/// rust-internal: reads the engine's own declarations, no product behaviour
#[test]
fn a_simple_bucket_holds_no_general_case_to_take_away() {
    // Each member's EMPTY and SINGLE spellings, and the width they occupy in the node.
    assert_eq!(
        24,
        size_of::<BlockIndexMap>(),
        "the page index is {} bytes in the node, not 24; the accounting below is stale",
        size_of::<BlockIndexMap>()
    );
    assert_eq!(16, size_of::<ObjectIndex>(), "the object index moved");
    assert_eq!(
        8,
        size_of::<DeletedObjectIndex>(),
        "the tombstone index moved"
    );

    // The three members sum to 48 of the node's 88 -- the bytes the proposal would replace with one
    // word. It was 136 of 192, then 128 of 184 when the address inside the inline block entry shed
    // its derived generation, 128 of 176 when the five flags became five bits (the node moved and
    // the members did not -- the flags are not in them), 120 of 168 when that address merged its
    // slab id and its offset into one word, 104 of 144 when the block entry's model spelling stopped
    // being a sixteen-byte fat pointer to a string from a closed set, and 48 of 88 when the block
    // index stopped holding that entry inline at all. So of the five steps, FOUR took the same bytes
    // off the members AND off the node, and one took eight off the node alone. What the proposal
    // would replace is unchanged in kind and smaller in size every time, which is the direction that
    // makes the proposal worse rather than better -- and the fifth step is the largest of them by
    // far, because the block index gave up 56 bytes at once.
    let members = size_of::<BlockIndexMap>() + size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>();
    assert_eq!(
        48, members,
        "the three container members are {members} bytes, not 48"
    );
    assert!(
        members < size_of::<BucketNode>(),
        "the members cannot be wider than the node that holds them"
    );

    // AND THE SIMPLE SPELLING OF EACH IS THE ONE THAT DOES NOT ALLOCATE -- EXCEPT THE BLOCK INDEX,
    // WHICH NO LONGER HAS ONE. Its inline single-block arm was dropped: the arm's width was paid by
    // every bucket in the map and at the shipped routing range almost none holds exactly one block.
    // So a single block costs one allocation here, deliberately, and the object and tombstone
    // indexes are the two members whose simple spelling is still free.
    let simple_block = BlockIndexMap::Many(vec![(7, block_fixture())]);
    assert_eq!(1, simple_block.len(), "a one-entry page list holds exactly one page");
    assert!(
        simple_block.get(&7).is_some(),
        "a one-entry page list must answer for the handle it holds"
    );

    let mut ids = ObjectIndex::default();
    assert!(matches!(ids, ObjectIndex::Empty), "an empty object index allocates nothing");
    ids.insert(42);
    assert!(
        matches!(ids, ObjectIndex::One(42)),
        "a single object id must stay in the inline arm rather than taking a box"
    );

    let tombstones = DeletedObjectIndex::default();
    assert!(
        tombstones.is_empty(),
        "the tombstone index of a live bucket is the null pointer"
    );

    // THE ONE FACT THAT DECIDES THE SHAPE: what a simple bucket's data actually is.
    assert_eq!(
        16,
        size_of::<crate::block_store::ElementEntry>(),
        "an address is {} bytes, not 16",
        size_of::<crate::block_store::ElementEntry>()
    );
    // 24, HAVING BEEN 40 AND 56: the entry stopped naming its element, and then stopped carrying
    // its object identity at all. The proposal this arm refuses -- a page's address fitting in a
    // tagged 64-bit word -- is refused by the ADDRESS being 16 bytes on its own, asserted just
    // above, so the entry's width moving does not soften it: the entry is still three words, the
    // address inside it is still two, and a 64-bit word is one.
    //
    // AND THE REFUSAL IS NOW CARRIED ENTIRELY BY THE ADDRESS, which is the thing to notice rather
    // than the literal. The entry has shed thirty-two bytes across two changes and the refusal has
    // not moved an inch, because it never rested on the entry's total: 16 > 8 is the whole of it.
    assert_eq!(
        24,
        size_of::<BlockIndex>(),
        "a page entry is {} bytes, not 24",
        size_of::<BlockIndex>()
    );
    assert!(
        size_of::<crate::block_store::ElementEntry>() > size_of::<u64>(),
        "the whole proposal rests on a page's address fitting in a tagged 64-bit word; here the \
         address ALONE is {} bytes, and the entry around it carries a shared name and an address as well",
        size_of::<crate::block_store::ElementEntry>()
    );
}

fn block_fixture() -> BlockIndex {
    BlockIndex {
        // Was `"m"`, a spelling no arm of the walk emits and the registry does not declare.
        // The field's type could not say so when it was a free-form string.
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        address: crate::block_store::ElementEntry::from_parts(
            1,
            64,
            32,
            Some(9),
            Some(11),
        ),
        dirty: false,
        deleted: false,
    }
}

// =============================================================================================
// THE TAGGED WORD, BUILT
// =============================================================================================

/// Which arm a tagged word is pointing at. Two bits, and the invariant that makes them free is
/// asserted rather than assumed.
const KIND_MASK: usize = 0b11;
const KIND_EMPTY: usize = 0;
const KIND_SIMPLE: usize = 1;
const KIND_GENERAL: usize = 2;

/// WHERE THE TAG LIVES, AND WHETHER IT IS FREE.
///
/// #1958 found a discriminant with no niche spending sixteen bytes to carry eight bytes of
/// number, so a tag's home has to be stated. This one rides the LOW TWO BITS OF THE POINTER.
/// Both payloads have alignment 8, so their addresses have three zero bits to spare and the tag
/// costs nothing: the word is eight bytes and holds both the tag and the address.
///
/// THE INVARIANT IS GUARDED, NOT ASSUMED. `tag` refuses any address whose low bits are already
/// set -- which is what a payload of alignment 1 or 2 would hand it -- and
/// `the_tag_guard_refuses_an_address_whose_low_bits_are_not_free` feeds it one and asserts the
/// refusal. A guard nothing has ever failed is a guard nothing has ever tested.
fn tag(raw: usize, kind: usize) -> usize {
    assert_eq!(
        0,
        raw & KIND_MASK,
        "the tag rides the low two bits of the payload address and address {raw:#x} has one of \
         them set; the payload's alignment is not what this representation requires"
    );
    assert!(kind <= KIND_MASK, "a two-bit tag cannot hold kind {kind}");
    raw | kind
}

/// The simple arm's payload, out of line: one object, one block.
struct SimpleLayout {
    object_id: u64,
    handle: u64,
    block: BlockIndex,
}

/// The general arm's payload, out of line: everything the node holds today.
struct GeneralLayout {
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// ONE TAGGED WORD, which is the whole of the proposal.
struct TaggedLayout(usize);

impl TaggedLayout {
    fn empty() -> Self {
        TaggedLayout(KIND_EMPTY)
    }

    fn simple(layout: SimpleLayout) -> Self {
        let raw = Box::into_raw(Box::new(layout)) as usize;
        TaggedLayout(tag(raw, KIND_SIMPLE))
    }

    fn general(layout: GeneralLayout) -> Self {
        let raw = Box::into_raw(Box::new(layout)) as usize;
        TaggedLayout(tag(raw, KIND_GENERAL))
    }

    fn kind(&self) -> usize {
        self.0 & KIND_MASK
    }

    fn as_simple(&self) -> Option<&SimpleLayout> {
        if self.kind() != KIND_SIMPLE {
            return None;
        }
        // SAFETY: the word was built by `simple`, which boxed a `SimpleLayout` and masked the
        // tag into bits the alignment guarantees are zero; the kind check above is what says the
        // pointer is that one.
        Some(unsafe { &*((self.0 & !KIND_MASK) as *const SimpleLayout) })
    }

    fn as_general(&self) -> Option<&GeneralLayout> {
        if self.kind() != KIND_GENERAL {
            return None;
        }
        // SAFETY: as above, for the general arm.
        Some(unsafe { &*((self.0 & !KIND_MASK) as *const GeneralLayout) })
    }

    /// The block entry a read has to reach, whichever arm holds it.
    fn block(&self, handle: u64) -> Option<&BlockIndex> {
        if let Some(simple) = self.as_simple() {
            return (simple.handle == handle).then_some(&simple.block);
        }
        self.as_general()
            .and_then(|general| general.block_index.get(&handle))
    }
}

impl Clone for TaggedLayout {
    fn clone(&self) -> Self {
        match self.kind() {
            KIND_SIMPLE => {
                let held = self.as_simple().expect("the kind says simple");
                TaggedLayout::simple(SimpleLayout {
                    object_id: held.object_id,
                    handle: held.handle,
                    block: held.block.clone(),
                })
            }
            KIND_GENERAL => {
                let held = self.as_general().expect("the kind says general");
                TaggedLayout::general(GeneralLayout {
                    object_index: held.object_index.clone(),
                    deleted_object_index: held.deleted_object_index.clone(),
                    block_index: held.block_index.clone(),
                })
            }
            _ => TaggedLayout::empty(),
        }
    }
}

impl Drop for TaggedLayout {
    fn drop(&mut self) {
        let raw = self.0 & !KIND_MASK;
        match self.kind() {
            // SAFETY: the pointer came from `Box::into_raw` on this exact type and is taken back
            // exactly once, because `self.0` is consumed here.
            KIND_SIMPLE => drop(unsafe { Box::from_raw(raw as *mut SimpleLayout) }),
            // SAFETY: as above, for the general arm.
            KIND_GENERAL => drop(unsafe { Box::from_raw(raw as *mut GeneralLayout) }),
            _ => {}
        }
        self.0 = KIND_EMPTY;
    }
}

/// THE NODE UNDER THE PROPOSAL: the same header, and one word where the three containers were.
#[derive(Clone)]
struct TaggedNode {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    // THE SAME WORD THE LIVE NODE GAINED, on the node and not inside the heap arm: the
    // tombstone rows are a per-BUCKET fact, so the proposal would hold them exactly where
    // the live declaration does. Putting them in the general arm's payload instead would
    // make this proposal look one word cheaper than it is.
    tombstone_elements: crate::engine::state::TombstoneElements,
    payload: TaggedLayout,
}

/// THE CONTROL FOR EVERY WIDTH BELOW: the live declaration, rebuilt from its own field types. If
/// this is not `size_of::<BucketNode>()` then the tagged node is being compared against a
/// structure this engine does not have.
#[allow(dead_code)]
struct LiveNodeMirror {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
    // CARRIED BY EVERY MIRROR, so the DISTANCES stay the prices: a mirror is the live
    // declaration plus ONE historical difference, and the live node gained this word
    // when a removal's element name left the page entries.
    tombstone_elements: crate::engine::state::TombstoneElements,
}

/// Rebuild one live node in the tagged shape, arm chosen by the engine's OWN classifier.
fn retag(node: &BucketNode) -> TaggedNode {
    let blocks = node.block_index.len();
    let objects = node.object_index.object_count();
    let simple = matches!(
        classify_bucket_layout(objects, blocks),
        BucketLayoutState::SingleBlockObject
    ) && node.deleted_object_index.is_empty();
    let payload = if blocks == 0 && objects == 0 {
        TaggedLayout::empty()
    } else if simple {
        let (handle, block) = node
            .block_index
            .iter()
            .next()
            .map(|(handle, block)| (*handle, block.clone()))
            .expect("the simple arm holds exactly one page");
        TaggedLayout::simple(SimpleLayout {
            object_id: node.object_index.iter().next().copied().unwrap_or_default(),
            handle,
            block,
        })
    } else {
        TaggedLayout::general(GeneralLayout {
            object_index: node.object_index.clone(),
            deleted_object_index: node.deleted_object_index.clone(),
            block_index: node.block_index.clone(),
        })
    };
    TaggedNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        tombstone_elements: node.tombstone_elements.clone(),
        payload,
    }
}

/// THE TAG'S HOME, AND THE WIDTH OF EACH ARM, WITH THE RECONSTRUCTION ASSERTED FOR EACH.
///
/// A width quoted without its field sum cannot tell a structure that is FULL from one that is
/// half padding, and those two want opposite fixes. Every structure here is reported as
/// `eight_aligned + round_up(tail) == size_of`, which is the layout rule itself rather than a
/// number that happens to match.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn the_tagged_node_is_fifty_six_bytes_and_every_arm_reconstructs() {
    // --- THE CONTROL COMES FIRST. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<LiveNodeMirror>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; every \
         price below would be fiction",
        size_of::<LiveNodeMirror>(),
        size_of::<BucketNode>()
    );

    // --- THE TAG IS FREE, AND THE ALIGNMENT THAT MAKES IT FREE IS ASSERTED. ---
    assert_eq!(
        8,
        size_of::<TaggedLayout>(),
        "the tagged word is {} bytes; a tag with nowhere free to live costs eight of its own and \
         the whole saving with it",
        size_of::<TaggedLayout>()
    );
    assert!(
        align_of::<SimpleLayout>() > KIND_MASK,
        "the simple payload has alignment {}, which leaves fewer than the two low bits the tag \
         rides in",
        align_of::<SimpleLayout>()
    );
    assert!(
        align_of::<GeneralLayout>() > KIND_MASK,
        "the general payload has alignment {}, which leaves fewer than the two low bits the tag \
         rides in",
        align_of::<GeneralLayout>()
    );

    // --- EACH ARM, RECONSTRUCTED. ---
    for (name, size, eight_aligned, tail) in [
        (
            "BucketNode (live)",
            size_of::<BucketNode>(),
            size_of::<BucketTtl>()
                + 3 * size_of::<u64>()
                + size_of::<ObjectIndex>()
                + size_of::<DeletedObjectIndex>()
                + size_of::<BlockIndexMap>()
                // THE ELEVENTH FIELD, one word, in the eight-aligned group.
                + size_of::<crate::engine::state::TombstoneElements>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + size_of::<BucketFlags>(),
        ),
        (
            "TaggedNode (proposed)",
            size_of::<TaggedNode>(),
            size_of::<BucketTtl>()
                + 3 * size_of::<u64>()
                + size_of::<TaggedLayout>()
                + size_of::<crate::engine::state::TombstoneElements>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + size_of::<BucketFlags>(),
        ),
        (
            "SimpleLayout (out of line)",
            size_of::<SimpleLayout>(),
            2 * size_of::<u64>() + size_of::<BlockIndex>(),
            0,
        ),
        (
            "GeneralLayout (out of line)",
            size_of::<GeneralLayout>(),
            size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>() + size_of::<BlockIndexMap>(),
            0,
        ),
    ] {
        let reconstructed = eight_aligned + tail.div_ceil(8) * 8;
        println!(
            "  {name:<28} {size:>4} B = {eight_aligned:>4} B eight-aligned + {:>2} B tail rounded",
            tail.div_ceil(8) * 8
        );
        assert_eq!(
            reconstructed, size,
            "{name} is {size} B and its fields reconstruct to {reconstructed} B; the accounting \
             is not describing the structure"
        );
    }

    assert_eq!(96, size_of::<BucketNode>(), "the live node moved");
    // 48, and it has not moved while the live node has gone 168 -> 160 -> 144 -> 88: this mirror
    // carries the node's HEADER only, and the header lost a whole word when `last_dump_sequence`
    // left it. (56 was itself 64 until the five flags became one byte and a ten-byte tail became
    // six.) Every change to the block ENTRY takes bytes off the LIVE node and off this mirror's boxed
    // payload alike, because the payload holds that entry -- the address merge took eight from each,
    // and the model spelling took sixteen from each.
    //
    // AND THE LAST STEP IS THE ONE THAT DID NOT TOUCH BOTH SIDES, WHICH IS WHY IT IS CALLED OUT.
    // Taking the entry out of the node took 56 bytes off the LIVE node and NOTHING off this mirror,
    // whose payload was already out of line. So the difference the verdict below turns on has
    // SHRUNK by 56 rather than staying put, and that is a reason to re-read the verdict rather than
    // to assume it survived: what the tagged word would replace is now a 24-byte list header, not a
    // whole inline entry.

    // AND 56, NOT 48, SINCE THE NODE'S HEADER GAINED A WORD. `tombstone_elements` is a per-BUCKET
    // fact, so the proposal has to hold it exactly where the live declaration does -- on the node,
    // not inside the general arm's boxed payload, which would have made this proposal look one word
    // cheaper than it is. So the header moved on BOTH sides, and the difference the verdict below
    // turns on is unchanged by it.
    assert_eq!(
        56,
        size_of::<TaggedNode>(),
        "the tagged node is {} bytes, not 56",
        size_of::<TaggedNode>()
    );
    // 40, not 56 and not 72: this payload holds a page entry, and the entry has shed sixteen bytes
    // for the second time -- it stopped naming its element, and then stopped carrying its object
    // identity -- on top of the eight its address shed with the object id. The reconstruction above
    // is symbolic and followed on its own ALL THREE times; this literal has had to be edited all
    // three, which is the standing argument for reconstructing rather than totalling, now made by
    // a third instance of exactly the edit it predicts.
    assert_eq!(
        40,
        size_of::<SimpleLayout>(),
        "the simple payload is {} bytes, not 40",
        size_of::<SimpleLayout>()
    );

    // --- AND THE ARITHMETIC THAT READS AS A WIN, NAMED AS ARITHMETIC. ---
    //
    // 104 bytes off the struct, and then an 88-byte allocation for very nearly every bucket at
    // the default range. glibc serves an 88-byte request from a chunk of AT LEAST 96 B once it has
    // taken its own header word and rounded to a multiple of sixteen, so the PAIR is 48 + 96 = 144
    // -- exactly what it replaced, before the allocation itself and before the pointer chase. The
    // conclusion has now survived the flag packing, both address narrowings AND the model
    // spelling unchanged, and for the same reason each time: the node and the out-of-line payload
    // shrink together, so the pair tracks the inline form step for step. FOUR chances for this
    // decline to become a win and it has not taken one. This is `size_of` arithmetic and is quoted
    // as such; the measured figure is in `what_the_tagged_node_actually_costs_the_allocator`.
    let chunk = |request: usize| (request + 8).div_ceil(16) * 16;
    let pair = size_of::<TaggedNode>() + chunk(size_of::<SimpleLayout>());
    println!(
        "\n  ARITHMETIC ONLY: a simple bucket is {} B inline today against {} B of node + {} B of \
         chunk = {pair} B tagged ({:+} B), plus one allocation and one indirection",
        size_of::<BucketNode>(),
        size_of::<TaggedNode>(),
        chunk(size_of::<SimpleLayout>()),
        pair as i64 - size_of::<BucketNode>() as i64
    );
    assert!(
        pair >= size_of::<BucketNode>(),
        "the tagged pair costs {pair} B against {} B inline; if that has become a win the decline \
         recorded here is stale and should be revisited",
        size_of::<BucketNode>()
    );
}

/// THE TAG GUARD FAILS ON A VIOLATING ADDRESS.
///
/// The invariant that makes the tag free is that the payload's address has its low two bits
