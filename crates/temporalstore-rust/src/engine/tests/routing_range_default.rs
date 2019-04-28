// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CHOOSING THE SHIPPED ROUTING-RANGE DEFAULT FROM A MEASUREMENT, AND WHAT IT WOULD COST.
//!
//! The shipped default is the whole 32-bit keyspace. `docs/runtime_tuning.md` then tells an
//! operator to set 1,024 buckets before the first ingest, so the shipped default is not the
//! configuration the documentation says to run. #1960 established that the fill is a property of
//! the RANGE and not of the workload, and priced 1,024 against the whole keyspace. This module
//! asks the next question -- WHICH range should ship -- and it does not assume the answer is the
//! one the documentation happens to show.
//!
//! # WHAT 1023 IS, AND WHAT IT IS NOT
//!
//! 1023 is an example in a document. It is not a measured optimum, and nothing in the tree ever
//! swept the range to find one. #1960 compared two points; two points cannot locate a crossover.
//! So the range is swept here over four candidates at two corpus sizes, and every column an
//! operator would need to choose with is reported at every point.
//!
//! # THE CROSSOVER THE SWEEP IS LOOKING FOR
//!
//! `BlockIndexMap::One` holds its block inline and allocates nothing; `Many` is a flat sorted
//! `Vec`. Filling a bucket trades one bucket node per BLOCK for one bucket node plus one vector
//! per BUCKET, so there is a fill below which the trade is a loss. That crossover is a property
//! of BLOCKS PER BUCKET, and blocks per bucket is
//!
//! ```text
//!     records x pages-per-record / bucket-count
//! ```
//!
//! -- which contains the RECORD COUNT. A fixed bucket count therefore lands at a different fill
//! for every corpus, and the sweep is run at two corpus sizes precisely so that a candidate which
//! only looks good at one of them cannot pass as an optimum.
//! `the_fill_a_candidate_range_produces_moves_with_the_corpus_not_only_with_the_range` is the
//! assertion that this is so, and it is the finding this module exists to establish.
//!
//! # NEVER A MEAN
//!
//! #1959 published a mean of 1.98 blocks a bucket for a store containing NOT ONE bucket holding
//! two. Every distribution here is reported as a histogram with p50, p90, p99 and MAX over a
//! stated denominator, and the mean is printed only beside them.
//!
//! # ONE INSTRUMENT, BOTH COLUMNS
//!
//! `bucket_fill.rs` reports `alloc_bytes` -- what the caller ASKED the allocator for. #1967 added
//! `ALLOC_CHUNK_BYTES`, which reads `malloc_usable_size` and charges what the allocator actually
//! handed over. The two differ by the rounding, and the rounding is exactly what a change of
//! container shape moves, so every byte figure here is reported in BOTH columns.
//!
//! # THE READ PATH IS COUNTED, NOT TIMED
//!
//! `BLOCK_LOOKUP_ENTRIES_EXAMINED` counts the entries the shipped bisection touches, incremented
//! inside `find_block`, the one door every `Many`-arm lookup goes through. A deeper fill means a
//! longer list means more probes, and that is the read-path price of the fill. It is a count, so
//! it does not move with the load on the box.
//!
//! # THE CONTROL ON THE EXPLANATION
//!
//! The mechanism claimed is that the RANGE WIDTH is the modulus, so narrowing it groups keys.
//! `narrowing_the_range_changes_nothing_for_a_store_whose_blocks_share_one_object_key` is the
//! workload where that mechanism predicts NO effect: routing takes the object key and never the
//! component, so a single key's many component blocks sit in one bucket at every range. If the
//! sweep's effect showed up there too, the explanation would be wrong.

#![allow(clippy::all)]
use super::*;

/// The whole keyspace: what `load_shard`'s end bucket defaulted to before the default moved.
///
/// OWNED BY THIS MODULE, not by the engine. It used to be
/// `routing_range_stamp::OLD_DEFAULT_END_ROUTING_BUCKET`, a production constant justifying the arm that
/// ADOPTED this range for a store carrying no stamp. That arm is now a refusal, so the production
/// constant went with it -- keeping it would have left a named "legacy range" in the engine with
/// nothing reading it and a docstring for a decision the engine no longer takes. What the tests
/// need is only a name for the old default, and that is a fact about the tests.
const OLD_DEFAULT_END_ROUTING_BUCKET: u32 = u32::MAX;
const OLD_DEFAULT_START_ROUTING_BUCKET: u32 = 0;
use std::collections::{BTreeMap, BTreeSet};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The candidate ranges swept, as END buckets. 1023 is the one `docs/runtime_tuning.md` shows;
/// the other three bracket it by a factor of four each way so a crossover between them is visible
/// rather than inferred.
const CANDIDATES: [u32; 4] = [255, 1023, 4095, 65535];

/// The shipped default, and the control every candidate is measured against.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every arm the sweep runs: the four candidates plus the shipped default.
fn swept_ranges() -> Vec<u32> {
    let mut ranges: Vec<u32> = CANDIDATES.to_vec();
    ranges.push(WIDE_END);
    ranges
}

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
        table_name: "routing-range-default".to_string(),
        shard_uri: "local://routing-range-default/1".to_string(),
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

/// ROUTED KEYS: plain strings, one block each -- the shape every figure below is over.
fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("fill-{i:06}")).collect();
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

fn read_back(engine: &TemporalEngine, keys: &[String]) -> usize {
    keys.iter()
        .filter(|key| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: (*key).clone(),
                },
            });
            response.status.ok
                && matches!(
                    &response.response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
        })
        .count()
}

// ---------------------------------------------------------------------------------------------
// THE DISTRIBUTION: a histogram with percentiles and a MAX, over a stated denominator.
// ---------------------------------------------------------------------------------------------

/// Blocks held per routing bucket, as bucket COUNTS keyed by blocks held.
///
/// NEVER REPORTED AS A MEAN ALONE. #1959's mean of 1.98 blocks a bucket described a store holding
/// no bucket with two blocks in it. The percentiles below are taken over the BUCKET population --
/// `p50` is the blocks held by the median occupied bucket -- and the denominator is printed with
/// every row so that a percentile over four buckets cannot read as one over four thousand.
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

    fn mean(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.blocks() as f64 / buckets as f64
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn min(&self) -> usize {
        self.counts.keys().copied().next().unwrap_or_default()
    }

    /// The blocks held by the bucket at `fraction` of the way through the bucket population,
    /// ordered by blocks held. A count off the histogram, not an interpolation: every value it can
    /// return is a block count some bucket actually holds.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * fraction).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.max()
    }

    fn buckets_holding_more_than_one(&self) -> usize {
        self.counts
            .iter()
            .filter(|(held, _)| **held > 1)
            .map(|(_, count)| *count)
            .sum()
    }

    /// Every row, no collapsing. `bucket_fill.rs`'s reporter prints a row only when the block count
    /// is at most eight, a multiple of eight, or the widest -- which at a fill of 39 hides most of
    /// the distribution and makes the printed rows fail to sum to the denominator. A histogram
    /// whose rows do not add up is not one, so this prints every row and ASSERTS the sum.
    fn report(&self, label: &str) {
        let buckets = self.buckets();
        println!(
            "  {label}: {buckets} occupied buckets over {} pages | mean {:.4} min {} p50 {} \
             p90 {} p99 {} MAX {} | buckets holding >1 page: {} of {buckets}",
            self.blocks(),
            self.mean(),
            self.min(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
            self.buckets_holding_more_than_one(),
        );
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, buckets,
            "{label}: the histogram's rows sum to {printed} buckets over a denominator of \
             {buckets}"
        );
        for (held, count) in &self.counts {
            println!(
                "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                100.0 * *count as f64 / buckets.max(1) as f64
            );
        }
    }

    /// The same line without the rows, for the sweep table.
    fn line(&self, label: &str) {
        println!(
            "  {label:<34} buckets {:>6} pages {:>6} | mean {:>8.3} p50 {:>5} p90 {:>5} \
             p99 {:>5} MAX {:>5}",
            self.buckets(),
            self.blocks(),
            self.mean(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
    }
}

fn blocks_per_bucket(engine: &TemporalEngine) -> BlocksPerBucket {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = BlocksPerBucket::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        if bucket.block_index.is_empty() {
            continue;
        }
        *hist.counts.entry(bucket.block_index.len()).or_default() += 1;
    }
    hist
}

/// How many buckets sit in each arm of `BlockIndexMap`: (Empty, One, Many).
///
/// The `One` arm holds its block behind a POINTER rather than inline, so it costs eight bytes in the
/// node plus one allocation instead of the width of a whole entry. `inline_arm_trade.rs` is the
/// measurement that moved it, and this range is the reason it moved.
fn block_index_arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut arms = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            crate::engine::state::BlockIndexMap::Empty => arms.0 += 1,
            crate::engine::state::BlockIndexMap::One(_, _) => arms.1 += 1,
            crate::engine::state::BlockIndexMap::Many(_) => arms.2 += 1,
        }
    }
    arms
}

/// The store path length every arm is held at, asserted equal across arms because allocation
/// bytes move at about six bytes a character.
fn store_path_length(dir: &std::path::Path) -> usize {
    dir.to_string_lossy().len()
}

// =============================================================================================
// 1. THE SWEEP: BLOCKS PER BUCKET AT EVERY CANDIDATE, AT TWO CORPUS SIZES
// =============================================================================================

/// THE RANGE SWEEP AS A HISTOGRAM WITH PERCENTILES AND A MAX, AT TWO CORPUS SIZES.
///
/// The question a default has to answer is "how full is a bucket", and the answer is not a
/// property of the range alone. `records x pages-per-record / bucket-count` contains the record
/// count, so the same candidate range lands at a different fill for every corpus -- and a
/// candidate chosen at one corpus size is chosen for that size only. This reports the whole
/// distribution at each of the five arms at each of the two sizes so that the dependence is
/// visible rather than argued.
///
/// PER-ARM SAMPLE COUNTS AND THE DENOMINATOR are printed on every row. An arm with no bucket in
/// it is not a measurement, and the `Many`-arm count says whether the fill happened at all.
///
/// THE STORE PATH LENGTH is asserted equal across arms: it moves allocation bytes at about six
/// bytes a character, and while block and bucket COUNTS are immune to it, the allocator table in
/// section 3 is not and shares this fixture's shape.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds ten stores up to 40,000 records each; run by name"]
fn the_blocks_a_bucket_holds_at_every_candidate_range_as_percentiles_and_max() {
    let mut observed: BTreeMap<(usize, u32), BlocksPerBucket> = BTreeMap::new();
    let mut path_lengths: BTreeSet<usize> = BTreeSet::new();

    for records in [SMALL, LARGE] {
        println!("=== {records} routed records ===");
        for end_routing_bucket in swept_ranges() {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.insert(store_path_length(dir.path()));
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            assert_eq!(
                records,
                read_back(&engine, &keys),
                "on 0..{end_routing_bucket} at {records} records the fixture cannot read its own \
                 store back, so the distribution below is over a store that does not work"
            );
            let hist = blocks_per_bucket(&engine);
            let arms = block_index_arms(&engine);
            assert_eq!(
                records,
                hist.blocks(),
                "on 0..{end_routing_bucket} at {records} records the index holds {} pages for \
                 {records} one-page records; the denominator is not what it is read as",
                hist.blocks()
            );
            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (shipped default)".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            hist.line(&format!("{label} [arms E{} O{} M{}]", arms.0, arms.1, arms.2));
            observed.insert((records, end_routing_bucket), hist);
        }
    }

    assert_eq!(
        1,
        path_lengths.len(),
        "the arms ran at {} different store path lengths ({:?}); allocation bytes move at about \
         six bytes a character, so arms at different path lengths are not comparable",
        path_lengths.len(),
        path_lengths
    );
    println!(
        "  store path length held at {} characters across all {} arms",
        path_lengths.iter().next().copied().unwrap_or_default(),
        observed.len()
    );

    // THE FULL HISTOGRAM AT THE DOCUMENTED CANDIDATE, at both sizes, rows and all.
    for records in [SMALL, LARGE] {
        observed
            .get(&(records, 1023))
            .expect("the documented candidate ran")
            .report(&format!("0..1023 at {records} records"));
    }

    // THE SHIPPED DEFAULT IS ONE BLOCK A BUCKET AT BOTH SIZES, by construction and at every
    // percentile. Asserted because it is the premise of the whole campaign.
    for records in [SMALL, LARGE] {
        let wide = observed.get(&(records, WIDE_END)).expect("the wide arm ran");
        assert_eq!(
            1,
            wide.max(),
            "on the shipped default at {records} records the widest bucket holds {} pages. Every \
             key lands alone by construction at a modulus of 4.29 billion, so a bucket holding \
             two means the fixture is not the shape every figure here is compared against",
            wide.max()
        );
        assert_eq!(
            0,
            wide.buckets_holding_more_than_one(),
            "on the shipped default at {records} records {} buckets hold more than one page",
            wide.buckets_holding_more_than_one()
        );
    }

    // EVERY CANDIDATE REACHED THE ARM IT IS BEING READ AS MEASURING. A candidate whose buckets are
    // all still single-block measures the shipped default under a different name.
    for records in [SMALL, LARGE] {
        for candidate in CANDIDATES {
            let hist = observed
                .get(&(records, candidate))
                .expect("every candidate ran");
            if (records as f64) / ((candidate as f64) + 1.0) < 1.0 {
                // Fewer records than buckets: a fill is not expected and its absence is not a
                // fixture defect. Printed rather than asserted, and named as the reason the
                // candidate is a poor one at this size.
                println!(
                    "  NOTE 0..{candidate} at {records} records: {} of {} buckets hold more than \
                     one page -- at {:.3} records a bucket this candidate is the shipped default \
                     in all but name",
                    hist.buckets_holding_more_than_one(),
                    hist.buckets(),
                    (records as f64) / ((candidate as f64) + 1.0),
                );
                continue;
            }
            assert!(
                hist.buckets_holding_more_than_one() > 0,
                "0..{candidate} at {records} records produced no bucket holding more than one \
                 page, so this arm did not fill and every figure taken from it describes the \
                 single-page shape instead"
            );
        }
    }
}

/// THE FILL A CANDIDATE PRODUCES MOVES WITH THE CORPUS, WHICH IS WHY NO FIXED DEFAULT IS RIGHT.
///
/// THIS IS THE FINDING. The crossover the container shapes have is a property of BLOCKS PER
/// BUCKET, and blocks per bucket is `records x pages-per-record / bucket-count`. A shipped default
/// fixes the denominator and nothing else, so the fill it lands at is set by the corpus -- which
/// the engine does not know when it loads a shard. The same candidate is therefore below the
/// crossover at one corpus size and above it at another, and this asserts that it genuinely is
/// rather than leaving it as arithmetic.
///
/// Asserted as a RATIO between the two corpus sizes at each candidate, which is the form that
/// cannot be true by accident: ten times the records over the same bucket count must be about ten
/// times the fill.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn the_fill_a_candidate_range_produces_moves_with_the_corpus_not_only_with_the_range() {
    let mut fills: BTreeMap<(usize, u32), (f64, usize)> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for candidate in CANDIDATES {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, candidate);
            let keys = seed_routed(&engine, records);
            assert_eq!(records, read_back(&engine, &keys), "the arm cannot read itself back");
            let hist = blocks_per_bucket(&engine);
            fills.insert((records, candidate), (hist.mean(), hist.buckets()));
            println!(
                "  0..{candidate} at {records} records: p50 {} pages, MAX {} pages over {} \
                 occupied buckets of {} in the range",
                hist.percentile(0.50),
                hist.max(),
                hist.buckets(),
                (candidate as usize) + 1
            );
        }
    }

    // THE MEAN REPORTED ABOVE IS OVER OCCUPIED BUCKETS, and that matters here. Below saturation a
    // hash leaves buckets empty, so ten times the records lands in more DISTINCT buckets as well
    // as more blocks each, and the occupied-bucket mean moves by LESS than the corpus ratio. The
    // strict prediction therefore holds only where both arms are saturated -- every bucket in the
    // range occupied -- and where they are not, the occupancy is printed and the weaker bound is
    // used. Measured: 0..4095 goes 1.452 -> 9.766 (6.72x, not 10x) precisely because its small arm
    // occupies 2,754 of 4,096 buckets.
    let corpus_ratio = LARGE as f64 / SMALL as f64;
    for candidate in CANDIDATES {
        let range_buckets = (candidate as usize) + 1;
        let (small, small_occupied) = *fills.get(&(SMALL, candidate)).expect("small arm");
        let (large, large_occupied) = *fills.get(&(LARGE, candidate)).expect("large arm");
        let ratio = large / small;
        let saturated = small_occupied == range_buckets && large_occupied == range_buckets;
        println!(
            "  0..{candidate}: fill {small:.3} -> {large:.3} pages a bucket as the corpus goes \
             {SMALL} -> {LARGE} ({ratio:.2}x against a corpus ratio of {corpus_ratio:.2}x); \
             occupancy {small_occupied}/{range_buckets} -> {large_occupied}/{range_buckets}, \
             saturated at both sizes: {saturated}"
        );
        if saturated {
            assert!(
                ratio > corpus_ratio * 0.95,
                "0..{candidate} is saturated at both sizes ({small_occupied} and \
                 {large_occupied} of {range_buckets} buckets occupied), so the fill is exactly \
                 records over buckets and ten times the records must be ten times the fill. It \
                 moved {ratio:.2}x against {corpus_ratio:.2}x"
            );
        } else if large_occupied == range_buckets {
            // Saturated at the large corpus only. The small arm's occupied-bucket mean is over a
            // subset, which understates the ratio, so the strict prediction cannot be asserted --
            // but the fill must still move substantially with the corpus.
            assert!(
                ratio > 2.0,
                "0..{candidate} moved its fill only {ratio:.2}x for ten times the records while \
                 reaching saturation ({large_occupied} of {range_buckets}) at the large corpus. \
                 Below saturation the occupied-bucket mean understates the ratio -- occupancy \
                 {small_occupied} -> {large_occupied} -- but it must still move, or the fill does \
                 not depend on the corpus at all"
            );
        } else {
            // SATURATED AT NEITHER SIZE. This candidate has more buckets than the corpus has
            // records, so it is not filling buckets at all: it is the shipped default under
            // another name, and the honest thing to assert about it is exactly that. An occupancy
            // ratio here is not a fill measurement and must not be read as one.
            assert!(
                large < 11.0,
                "0..{candidate} is saturated at neither corpus size (occupancy {small_occupied} \
                 -> {large_occupied} of {range_buckets}) yet reached a fill of {large:.3} pages a \
                 bucket. A candidate with more buckets than the corpus has records is supposed to \
                 stay near one page a bucket; if it does not, the classification this arm rests \
                 on is wrong"
            );
            println!(
                "        0..{candidate} is saturated at NEITHER size: {range_buckets} buckets for \
                 at most {LARGE} records, so it never fills and its fill ratio of {ratio:.2}x is \
                 occupancy growth, not a fill. It is the shipped default under another name."
            );
        }
    }

    // AND THE CONSEQUENCE, STATED AS THE ASSERTION A DEFAULT DECISION RESTS ON: at the two sizes
    // measured, no single candidate sits in the same regime. The crossover between `One` and a
    // filled `Many` is around eleven blocks; a candidate below it at 4,000 records is above it at
    // 40,000 and the reverse.
    let straddlers: Vec<u32> = CANDIDATES
        .into_iter()
        .filter(|candidate| {
            let (small, _) = *fills.get(&(SMALL, *candidate)).expect("small arm");
            let (large, _) = *fills.get(&(LARGE, *candidate)).expect("large arm");
            small < 11.0 && large >= 11.0
        })
        .collect();
    println!(
        "  candidates whose fill straddles the eleven-page crossover between {SMALL} and {LARGE} \
         records: {straddlers:?} of {:?}",
        CANDIDATES
    );
    assert!(
        !straddlers.is_empty(),
        "no candidate's fill crosses the eleven-page mark between {SMALL} and {LARGE} records. \
         The claim this module makes is that a fixed bucket count cannot hold a fill across corpus \
         sizes; if no candidate straddles, that claim is not established by this fixture and the \
         recommendation must not lean on it"
    );
}

// =============================================================================================
// 2. THE READ PATH, COUNTED
// =============================================================================================

/// WHAT MORE BLOCKS A BUCKET COSTS THE READ PATH, AS ENTRIES EXAMINED PER LOOKUP.
///
/// COUNTED, NOT TIMED. A timing-ratio instrument in this campaign read 485x idle against 11x busy
/// off identical code. `BLOCK_LOOKUP_ENTRIES_EXAMINED` is incremented inside `find_block` -- the one
/// door every `Many`-arm lookup goes through -- so this counts the entries the shipped bisection
/// actually touched on the production path.
///
/// The `One` arm is not a list and examines nothing, so the shipped default reads ZERO entries a
/// lookup and every candidate reads more. That is the price, and it is bounded: a bisection over a
/// list of `n` touches about `log2(n)` entries, so ten times the fill is between three and four
/// more probes rather than ten times the work. Reported per lookup at every candidate so the
/// bound is a measurement and not an appeal to the asymptotics.
///
/// rust-internal: reads the engine's own lookup counter, no product behaviour
#[test]
#[ignore = "seeds ten stores up to 40,000 records each; run by name"]
fn the_read_path_entries_examined_a_lookup_grow_with_the_fill_a_candidate_produces() {
    let mut observed: BTreeMap<(usize, u32), (f64, usize, usize)> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in swept_ranges() {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = blocks_per_bucket(&engine);

            crate::engine::state::reset_block_lookup_entries_examined();
            let readable = read_back(&engine, &keys);
            let examined = crate::engine::state::block_lookup_entries_examined();
            assert_eq!(
                records, readable,
                "on 0..{end_routing_bucket} at {records} records only {readable} records read \
                 back, so the entry count below is over a partial read"
            );

            let per_lookup = examined as f64 / records as f64;
            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (shipped default)".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            println!(
                "  {records} records {label:<30} p50 fill {:>5} MAX {:>5} | {examined:>8} \
                 entries examined over {records} lookups = {per_lookup:.4} a lookup",
                hist.percentile(0.50),
                hist.max()
            );
            observed.insert(
                (records, end_routing_bucket),
                (per_lookup, hist.percentile(0.50), hist.max()),
            );
        }
    }

    // THE SHIPPED DEFAULT EXAMINES NOTHING, because every bucket is on the inline `One` arm and
    // `find_block` is never reached. This is the denominator of the whole section and it is zero,
    // so the cost is reported as an absolute count a lookup rather than as a ratio -- a ratio
    // against zero is not a number.
    for records in [SMALL, LARGE] {
        let (wide_per_lookup, _, wide_max) =
            *observed.get(&(records, WIDE_END)).expect("the wide arm ran");
        assert_eq!(
            1, wide_max,
            "the shipped default at {records} records holds {wide_max} pages in its widest \
             bucket, so it is not the single-page shape this section's zero is attributed to"
        );
        assert_eq!(
            0.0, wide_per_lookup,
            "the shipped default at {records} records examined {wide_per_lookup} entries a \
             lookup. The `One` arm holds its page inline and never reaches `find_block`, so a \
             non-zero here means the counter is reading a path other than the one named"
        );
    }

    // AND EVERY CANDIDATE PAYS, MONOTONICALLY IN THE FILL. Narrower range, deeper fill, more
    // probes. Asserted at the large corpus, where every candidate has filled.
    let mut previous: Option<(u32, f64)> = None;
    for candidate in CANDIDATES {
        let (per_lookup, p50, max) = *observed.get(&(LARGE, candidate)).expect("candidate ran");
        if let Some((wider_earlier, wider_cost)) = previous {
            // CANDIDATES ascends, so each successive candidate is WIDER and must be cheaper.
            assert!(
                per_lookup <= wider_cost,
                "0..{candidate} examined {per_lookup:.4} entries a lookup against \
                 0..{wider_earlier}'s {wider_cost:.4}, yet 0..{candidate} is the WIDER range and \
                 therefore the shallower fill. The read-path cost is supposed to follow the list \
                 length; if it does not, this section is not measuring the walk"
            );
        }
        println!(
            "  0..{candidate} at {LARGE} records: p50 fill {p50}, MAX {max}, \
             {per_lookup:.4} entries examined a lookup"
        );
        previous = Some((candidate, per_lookup));
    }

    let (narrowest, _, _) = *observed.get(&(LARGE, CANDIDATES[0])).expect("narrowest");
    let (widest_candidate, _, _) = *observed
        .get(&(LARGE, CANDIDATES[CANDIDATES.len() - 1]))
        .expect("widest candidate");
    println!(
        "  the read-path price of the fill at {LARGE} records: 0.0000 entries a lookup on the \
         shipped default, {widest_candidate:.4} at 0..{}, {narrowest:.4} at 0..{} -- a bisection, \
         so a 256-fold narrowing is eight more probes and not 256 times the work",
        CANDIDATES[CANDIDATES.len() - 1],
        CANDIDATES[0]
    );
}

// =============================================================================================
// 3. BOTH BYTE COLUMNS, AND THE ALLOCATION COUNT
// =============================================================================================

/// THE PLANTED MARKER FOR BOTH COLUMNS. Recovered exactly, or every figure in section 3 is noise.
///
/// The plant is a size the allocator ROUNDS -- 100 bytes, served from a 112-byte chunk -- because
/// that is the only kind of plant that can separate the two failures the two columns have:
///
///   * the request column recovers `COPIES x 100` EXACTLY, so an instrument that is not running
///     cannot report a comfortable zero; and
///   * the chunk column recovers `COPIES x documented_glibc_chunk(100)` EXACTLY, which is MORE.
///     A chunk column that merely copied the request column -- the pre-#1967 bias every byte
///     comparison in this campaign carried -- fails here rather than flattering the out-of-line
///     shapes this module measures.
///
/// A LARGE PLANT IS DELIBERATELY NOT USED. A megabyte request is served through `mmap` with its
/// own block rounding rather than from a heap chunk, so its chunk reading is larger than
/// `documented_glibc_chunk` predicts and an exact assertion over it would be an assertion about
/// the mapping granularity instead of about the instrument. Measured at 1 MiB: request 1,048,576 B
/// against chunk 1,052,664 B.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn both_byte_columns_recover_a_planted_size_and_the_chunk_column_is_not_the_request_column() {
    const ROUNDED: usize = 100;
    const COPIES: usize = 4_096;
    let expected_chunk = crate::alloc_probe::documented_glibc_chunk(ROUNDED);
    assert_ne!(
        ROUNDED, expected_chunk,
        "the planted size must be one the allocator rounds, or this control cannot tell a chunk \
         reading from a request reading"
    );

    // THE OUTER VECTOR IS RESERVED BEFORE THE PROBE STARTS. Reserving inside the span charges its
    // own `COPIES x 24` bytes to the plant and the request column then reads 507,904 for a planted
    // 409,600 -- an instrument that looks broken when it is the fixture that is.
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(COPIES);
    let probe = Probe::start();
    for _ in 0..COPIES {
        held.push(vec![0xA5u8; ROUNDED]);
    }
    let counts = probe.stop();
    std::hint::black_box(&held);
    println!(
        "  planted {COPIES} x {ROUNDED} B: request column {} B (expected {}), chunk column {} B \
         (expected {}), {} call(s)",
        counts.alloc_bytes,
        (COPIES * ROUNDED) as u64,
        counts.chunk_bytes,
        (COPIES * expected_chunk) as u64,
        counts.allocs
    );
    assert_eq!(
        COPIES as u64, counts.allocs,
        "{COPIES} planted vectors charged {} allocations; anything else means the span caught \
         work that is not the plant",
        counts.allocs
    );
    assert_eq!(
        (COPIES * ROUNDED) as u64,
        counts.alloc_bytes,
        "the request column charged {} B for {COPIES} planted {ROUNDED}-byte requests",
        counts.alloc_bytes
    );
    assert_eq!(
        (COPIES * expected_chunk) as u64,
        counts.chunk_bytes,
        "the chunk column charged {} B for {COPIES} planted {ROUNDED}-byte requests, which glibc \
         serves from {expected_chunk}-byte chunks. If this reads {} -- the request total -- then \
         the chunk column is the request column under another name and every byte comparison in \
         this module carries the bias #1967 corrected",
        counts.chunk_bytes,
        (COPIES * ROUNDED) as u64
    );
    assert!(
        counts.chunk_bytes > counts.alloc_bytes,
        "the chunk column charged {} B and the request column {} B; the chunk is never smaller \
         and for a rounded size never equal",
        counts.chunk_bytes,
        counts.alloc_bytes
    );
    drop(held);
}

/// What the bucket map charges the allocator, in both columns and in calls.
#[cfg(feature = "alloc-probe")]
struct Arm {
