// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT WOULD A LAYOUT STATE SELECT, IF IT SELECTED A REPRESENTATION?
//!
//! `BucketLayoutState` names five shapes and every reader of it is a report string or a counter.
//! The question this module answers is whether making it select a REPRESENTATION -- one arm
//! keeping a page inline, another keying its pages on a per-bucket object list so the entry can
//! carry a one-byte ordinal instead of a name -- would be worth the two representations.
//!
//! IT WOULD NOT, AND THE REASON WAS ONE NUMBER: PAGES PER OBJECT WAS 1. THAT NUMBER HAS MOVED.
//!
//! An object list amortises. It costs a row per object and pays for itself by letting every page
//! of that object carry an ordinal instead of a name. The saving is therefore
//! `(name - ordinal) * blocks_per_object - row_cost`, and everything depends on
//! `blocks_per_object` being greater than one.
//!
//! IT WAS NOT, AND THAT WAS NOT AN ACCIDENT: a page's object identity was
//! `stable_block_object_id(shard_id, kind, object_key, component)` -- THE COMPONENT WAS IN THE
//! HASH -- so a hash field, a set member, a list element and a sorted-set member were each an
//! object of their own holding exactly one page. `MultiBlockObject`, the one arm the proposal is
//! for, was therefore UNREACHABLE for every component-bearing kind: a bucket holding many pages
//! of one key held many OBJECTS of one key, which classifies `MultiObject`.
//!
//! THE COMPONENT HAS SINCE LEFT THE HASH. One key is one object, a container key is `(1 object,
//! many pages)`, and this module's two findings invert with it -- both are inverted below rather
//! than deleted, keeping their denominators and their controls:
//!
//!   * the arm IS reached now, small by bucket share and large by page share, and both numbers
//!     are asserted because either one alone misstates the size;
//!   * the stored `object_index` IS the list the proposal wanted -- one row per `(kind, key)` --
//!     so the proposal's real cost, a SECOND durable structure beside it, is no longer a cost.
//!
//! What has NOT moved is the shipped range: at 40,000 records on `0..1023` the arm is still
//! exactly absent and exactly one arm is live, which is still what makes a five-way dispatch
//! select nothing there.
//!
//! THE POSITIVE CONTROL IS WHAT MAKES THAT A MEASUREMENT RATHER THAN AN EMPTY FIXTURE. A
//! component-free kind CAN hold several pages under one object id, and the series kind does: a
//! timestamped series is appended under one `(kind, key, None)` identity and spills into more than
//! one block. If this module reported "no multi-page objects" without reaching one, the report
//! would be indistinguishable from a fixture that could not produce one -- so the series arm is
//! seeded, its pages-per-object MAX is asserted above one, and the refutation is stated only for
//! the kinds where the ceiling is structural.
//!
//! THE NULL CONTROL, AT 0.00%. Grouping pages by `object_id` (component IN the hash) and grouping
//! them by `(model_id, object_key)` (component OUT) must give byte-identical histograms for any
//! component-free kind, and must diverge for containers. The routed-string arm is that control: it
//! predicts exactly no difference, and a difference there would mean the two groupings are not
//! measuring what they are named for.
//!
//! EVERY ROW CARRIES ITS OWN DENOMINATOR and the arm rows are asserted to sum to the population,
//! because a run that wrote nothing reports a beautifully simple store. Percentiles and a MAX,
//! never a mean: #1959 published 1.98 pages a bucket over a store holding not one bucket with two.
//!
//! rust-internal: reads the engine's own bucket index and its own classifier; no product behaviour

use super::*;
use crate::engine::state::BucketLayoutState;
use crate::engine::storage_bucket_internals::classify_bucket_layout;
use std::collections::BTreeMap;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses by default.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every arm this module quotes a figure for.
const EVERY_ARM: [&str; 5] = [
    "empty",
    "single_object_no_block",
    "single_block_object",
    "multi_block_object",
    "multi_object",
];

fn arm_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object_no_block",
        BucketLayoutState::SingleBlockObject => "single_block_object",
        BucketLayoutState::MultiBlockObject => "multi_block_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
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
        table_name: "layout-arms".to_string(),
        shard_uri: "local://layout-arms/1".to_string(),
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

fn percentile(sorted: &[usize], fraction: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

/// THE REAL KIND MIX, in ONE store, so the arms are measured over a population a deployment could
/// actually have rather than over three separate single-kind stores.
///
/// Returns the number of pages each kind is expected to contribute, so the denominator is checked
/// against the fixture and not against itself.
struct SeedCounts {
    strings: usize,
    container_blocks: usize,
    series_keys: usize,
}

fn seed_mixed(engine: &TemporalEngine, records: usize) -> SeedCounts {
    // Half plain strings: one page each, no component.
    let strings = records / 2;
    run_batch(
        engine,
        (0..strings)
            .map(|i| Command::StringSet {
                key: format!("mix-s-{i:06}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );

    // Two fifths containers, across all four container kinds: one page PER ELEMENT, each element a
    // component and therefore an object of its own.
    let container_blocks = records * 2 / 5;
    let members = 100usize;
    let container_keys = container_blocks / members;
    let mut commands = Vec::with_capacity(container_blocks);
    for k in 0..container_keys {
        for m in 0..members {
            commands.push(match k % 4 {
                0 => Command::HashSet {
                    key: format!("mix-h-{k:06}"),
                    field: format!("f{m}"),
                    value: vec![b'v'; 32],
                },
                1 => Command::SetAdd {
                    key: format!("mix-t-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                },
                2 => Command::ZSetAdd {
                    key: format!("mix-z-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                    score: m as f64,
                },
                _ => Command::ListPush {
                    key: format!("mix-l-{k:06}"),
                    member: format!("m{m}").into_bytes(),
                    left: false,
                },
            });
        }
    }
    run_batch(engine, commands);

    // THE POSITIVE CONTROL. A timestamped series is appended under ONE (kind, key, None) identity
    // and spills into more than one block, so it is the one kind in this engine that can reach a
    // pages-per-object above one. Without it a null result here would be a fixture artefact.
    let series_keys = 20usize;
    let series_points = 2_000usize;
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
                    key: format!("mix-f-{k:06}"),
                    points,
                },
            });
            assert!(response.status.ok, "series seed must ack: {:?}", response.status);
        }
    }

    SeedCounts { strings, container_blocks, series_keys }
}

/// Everything read off one store: the arm population by bucket AND by page, and pages-per-object
/// under the two groupings whose difference is the whole question.
#[derive(Default)]
struct Population {
    /// arm -> (buckets in it, pages held by those buckets)
    arms: BTreeMap<&'static str, (usize, usize)>,
    blocks_per_bucket: Vec<usize>,
    objects_per_bucket: Vec<usize>,
    /// Pages grouped by the engine's own object identity -- component IN the hash.
    by_object_id: BTreeMap<u64, usize>,
    /// Pages grouped by (kind, key) -- component OUT. What an object list would key on.
    by_kind_key: BTreeMap<String, usize>,
    /// Pages grouped by (kind, key) for COMPONENT-FREE pages only: the null control's population.
    component_free_by_kind_key: BTreeMap<String, usize>,
    component_free_by_object_id: BTreeMap<u64, usize>,
    total_buckets: usize,
    total_blocks: usize,
    blocks_without_object_id: usize,
}

fn population(engine: &TemporalEngine) -> Population {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut out = Population::default();
    for arm in EVERY_ARM {
        out.arms.insert(arm, (0, 0));
    }
    for node in shard.bucket_index.bucket_map.values() {
        let blocks = node.block_index.len();
        let objects = node.object_index.object_count();
        let slot = out
            .arms
            .entry(arm_name(classify_bucket_layout(objects, blocks)))
            .or_default();
        slot.0 += 1;
        slot.1 += blocks;
        out.total_buckets += 1;
        out.total_blocks += blocks;
        if blocks > 0 {
            out.blocks_per_bucket.push(blocks);
            out.objects_per_bucket.push(objects);
        }
        for (handle, entry) in &node.block_index {
            let object_key = named.at(node.routing_bucket, *handle);
            // DERIVED, not read off the address. `blocks_without_object_id` is therefore structurally
            // zero and is left at zero rather than removed from the shape: every page can name its
            // object now, which is the fact that used to be worth counting.
            let id = entry.object_id(1, object_key);
            *out.by_object_id.entry(id).or_default() += 1;
            // EVERY page is component-free now -- an entry has no element name -- so this control
            // group is the whole population rather than a subset of it. Kept because the CONTROL
            // it feeds still has a subject: it compares grouping by the engine's object id against
            // grouping by (kind, key), and those two can still disagree.
            *out.component_free_by_object_id.entry(id).or_default() += 1;
            let kind_key = format!("{:?}\u{1}{}", entry.model_id, object_key);
            *out.by_kind_key.entry(kind_key.clone()).or_default() += 1;
            *out.component_free_by_kind_key.entry(kind_key).or_default() += 1;
        }
    }
    out.blocks_per_bucket.sort_unstable();
    out.objects_per_bucket.sort_unstable();
    out
}

fn report_group(label: &str, groups: &BTreeMap<impl Ord, usize>) -> (usize, usize) {
    let mut counts: Vec<usize> = groups.values().copied().collect();
    counts.sort_unstable();
    let total: usize = counts.iter().sum();
    let max = counts.last().copied().unwrap_or_default();
    println!(
        "    {label:<34} {:>7} groups over {:>7} pages  p50 {:>4}  p90 {:>4}  p99 {:>4}  MAX {:>4}",
        counts.len(),
        total,
        percentile(&counts, 0.50),
        percentile(&counts, 0.90),
        percentile(&counts, 0.99),
        max
    );
    (counts.len(), max)
}

// `field_width` IS GONE WITH ITS ONE CALLER. It took a width FROM THE FIELD rather than from a
// type named at the call site, which is how `blocks_per_bucket` came to charge two fat pointers for
// a seventeen-byte group (#1994, paid down in #1997). Its only use here was
// `name_pointer_width`, which read the entry's name field; the entry has no name field, so there
// is no field left to take a width from and the helper has nothing to infer from.

/// For each occupied bucket, the page count of every distinct `(kind, key)` inside it.
///
/// This is the breakdown a per-bucket dispatch would have to be uniform over, and the reason it
/// cannot be: the object list's win or loss is decided PER KEY and the layout state is PER BUCKET.
fn bucket_key_breakdown(engine: &TemporalEngine) -> Vec<Vec<usize>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut out: Vec<Vec<usize>> = Vec::new();
    for node in shard.bucket_index.bucket_map.values() {
        if node.block_index.len() == 0 {
            continue;
        }
        let mut per_key: BTreeMap<String, usize> = BTreeMap::new();
        for (handle, entry) in &node.block_index {
            *per_key
                .entry(format!(
                    "{:?}\u{1}{}",
                    entry.model_id,
                    named.at(node.routing_bucket, *handle)
                ))
                .or_default() += 1;
        }
        out.push(per_key.into_values().collect());
    }
    out
}

/// THE WIDTH A PAGE ENTRY SPENDS ON A NAME POINTER -- ZERO, AND IT IS READ OFF THE TYPE NOW.
///
/// It read `field_width(&entry.object_key)` off a real entry, which is what made it a measurement
/// rather than a literal. The entry holds no name pointer at all, so there is no field to read and
/// the honest answer is zero.
///
/// IT IS KEPT AS A FUNCTION RATHER THAN FOLDED INTO ITS CALLER because the arm below prints it as
/// a column and divides by it in one place, and a zero that arrives through the same door the
/// measurement did is a zero a reader can trace. What it costs is the engine argument, which is
/// now unused and is named so.
///
/// AND WHERE THOSE SIXTEEN BYTES WENT IS NOT NOWHERE: one shared pointer per (kind, object) in
/// `CoreIndex::object_block_lookup`, which `block_entry_name_pointer`'s surviving allocation arm
/// prices. A per-PAGE pointer became a per-OBJECT one; this column is the per-page half.
fn name_pointer_width(_engine: &TemporalEngine) -> usize {
    0
}

/// THE ARM POPULATION, AND THE ONE NUMBER AN OBJECT LIST AMORTISES OVER.
///
/// Four stores: the real kind mix at two corpus sizes ten times apart, on both routing ranges.
/// Every arm row carries the bucket count, its share of buckets, the pages those buckets hold and
/// its share of pages, and both sets of rows are asserted to sum to the store's own totals.
#[test]
#[ignore = "seeds four mixed stores up to 40,000 records each; run by name"]
fn the_layout_arm_an_object_list_would_pay_for_is_now_the_container_arm_and_still_absent_at_the_shipped_range()
{
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut widest_bucket_share_of_multi_block_object = 0.0f64;
    let mut widest_block_share_of_multi_block_object = 0.0f64;
    let mut widest_object_by_id = 0usize;
    let mut widest_object_by_kind_key = 0usize;
    let mut series_max_seen = 0usize;
    let mut shipped_range_at_scale_multi_block_object: Option<usize> = None;
    let mut shipped_range_at_scale_live_arms: Option<usize> = None;
    let mut stores = 0usize;

    for records in [SMALL, LARGE] {
        for end in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end);
            let seeded = seed_mixed(&engine, records);
            let pop = population(&engine);
            stores += 1;

            let width = if end == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end}")
            };
            println!(
                "\n{records} records of the real kind mix on {width}: {} buckets, {} pages",
                pop.total_buckets, pop.total_blocks
            );
            println!(
                "  (seeded {} string keys, {} container pages, {} series keys)",
                seeded.strings, seeded.container_blocks, seeded.series_keys
            );

            // --- THE ARM POPULATION, BY BUCKET AND BY PAGE, EACH WITH ITS DENOMINATOR. ---
            let mut arm_buckets = 0usize;
            let mut arm_blocks = 0usize;
            let mut live_arms = 0usize;
            for arm in EVERY_ARM {
                let (buckets, blocks) = pop.arms.get(arm).copied().unwrap_or_default();
                arm_buckets += buckets;
                arm_blocks += blocks;
                if buckets > 0 {
                    live_arms += 1;
                }
                let bucket_share = if pop.total_buckets == 0 {
                    0.0
                } else {
                    buckets as f64 * 100.0 / pop.total_buckets as f64
                };
                let block_share = if pop.total_blocks == 0 {
                    0.0
                } else {
                    blocks as f64 * 100.0 / pop.total_blocks as f64
                };
                println!(
                    "    {arm:<22} {buckets:>7} buckets ({bucket_share:>7.3}% of {})  {blocks:>8} pages ({block_share:>7.3}% of {})",
                    pop.total_buckets, pop.total_blocks
                );
                if arm == "multi_block_object" {
                    widest_bucket_share_of_multi_block_object =
                        widest_bucket_share_of_multi_block_object.max(bucket_share);
                    widest_block_share_of_multi_block_object =
                        widest_block_share_of_multi_block_object.max(block_share);
                    if records == LARGE && end == NARROW_END {
                        shipped_range_at_scale_multi_block_object = Some(buckets);
                    }
