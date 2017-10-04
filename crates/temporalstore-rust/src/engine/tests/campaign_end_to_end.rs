// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE WHOLE CAMPAIGN SAVED, MEASURED AT BOTH ENDS ON ONE INSTRUMENT.
//!
//! Every figure published during the bucket-node campaign is a DELTA taken by one change against
//! its own immediate base, on its own fixture. Deltas do not add: two changes can each remove the
//! same byte, and a config default moved in the middle of the sequence can carry a saving that
//! reads as struct work. Nobody has taken a direct reading at each END of the sequence with one
//! workload and one configuration, and that is what this module does.
//!
//! This file is written to compile UNCHANGED at both ends of the campaign. It touches no field
//! name that moved and no enum payload that moved -- only `len()`, `matches!(.., Arm(..))`,
//! `size_of`, and the public command surface. A measurement whose two arms are two different
//! programs is not a measurement.
//!
//! # FOUR CELLS, NOT TWO
//!
//! The shipped routing-range default moved from the whole 32-bit keyspace to 1023 DURING the
//! sequence. A before/after taken at each tree's own default therefore charges a configuration
//! change to the struct work. The range is held as a parameter and both values are run at both
//! ends, so the column effect (the default) and the row effect (the structures) separate:
//!
//! ```text
//!                         0..u32::MAX        0..1023
//!     pre-campaign        cell A             cell B
//!     post-campaign       cell C             cell D
//! ```
//!
//! Down a column is what the structures bought at a fixed range. Across a row is what the range
//! bought at a fixed tree. A + D against A alone is the number that has been quoted, and it is the
//! two effects multiplied together with no way to tell which did the work.
//!
//! # THE NODE IS PER BUCKET AND THE ENTRY IS PER PAGE
//!
//! At the whole keyspace every key routes to a bucket of its own, so one node per bucket IS one
//! node per record and its width divides by one. At 1023 a bucket holds tens of pages and the same
//! node divides by the fill. The two structures therefore scale differently with the range, and a
//! single "bytes per record" line hides it. Both contributions are reported separately.
//!
//! # NEVER A MEAN
//!
//! Pages per bucket is a histogram with every row printed, percentiles counted off the histogram,
//! a MAX, and the denominator on the line. A mean of 1.98 pages a bucket was once published for a
//! store containing no bucket holding two.
//!
//! # THE CONTROL
//!
//! `the_range_buys_nothing_for_a_store_whose_blocks_share_one_object_key` is the workload where the
//! column effect's mechanism predicts NOTHING: routing takes the object key and never the
//! component, so one key's many component pages sit in one bucket at every range. If the column
//! effect showed up there too, the explanation attributed to it here would be wrong.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The pre-campaign shipped default: the whole 32-bit keyspace.
const WIDE_END: u32 = u32::MAX;
/// The post-campaign shipped default.
const NARROW_END: u32 = 1023;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every store path is held at this many characters at every arm. Allocation bytes move at about
/// six bytes a character on this engine, so an arm at a different path length is a different
/// measurement wearing the same label.
const STORE_PATH_CHARS: usize = 15;

/// Base for the arm directories. Chosen so that base + a two-digit arm id is exactly
/// `STORE_PATH_CHARS` characters, which the helper below ASSERTS rather than trusting.
const ARM_BASE: &str = "/tmp/q8112dx";

fn arm_dir(arm: usize) -> std::path::PathBuf {
    assert!(arm < 100, "arm ids are two digits so the path length is fixed");
    let path = std::path::PathBuf::from(format!("{ARM_BASE}/{arm:02}"));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("arm directory");
    assert_eq!(
        STORE_PATH_CHARS,
        path.to_string_lossy().chars().count(),
        "the store path is {} characters at arm {arm} and {STORE_PATH_CHARS} everywhere else; \
         allocation bytes move at about six bytes a character, so an arm at a different length \
         cannot be compared with one at this length",
        path.to_string_lossy().chars().count()
    );
    path
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
        table_name: "campaign-end-to-end".to_string(),
        shard_uri: "local://campaign-end-to-end/1".to_string(),
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

/// The workload every cell is measured over: distinct routed string keys, one page each.
fn seed_commands(count: usize) -> (Vec<String>, Vec<Command>) {
    let keys: Vec<String> = (0..count).map(|i| format!("fill-{i:06}")).collect();
    let commands = keys
        .iter()
        .map(|key| Command::StringSet {
            key: key.clone(),
            value: vec![b'v'; 32],
        })
        .collect();
    (keys, commands)
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

fn range_label(end_routing_bucket: u32) -> String {
    if end_routing_bucket == WIDE_END {
        "0..u32::MAX".to_string()
    } else {
        format!("0..{end_routing_bucket}")
    }
}

// =============================================================================================
// THE DISTRIBUTION
// =============================================================================================

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

    /// Counted off the histogram, never interpolated: every value this can return is a page count
    /// that some bucket actually holds.
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

    fn line(&self, label: &str) {
        println!(
            "    {label:<40} buckets {:>6} pages {:>6} | mean {:>8.3} min {:>4} p50 {:>4} \
             p90 {:>4} p99 {:>4} MAX {:>4}",
            self.buckets(),
            self.blocks(),
            self.mean(),
            self.min(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
    }

    /// Every row, and the rows are ASSERTED to sum to the denominator. A histogram whose printed
    /// rows do not add up to its own denominator is not one.
    fn report(&self, label: &str) {
        self.line(label);
        let buckets = self.buckets();
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, buckets,
            "{label}: rows sum to {printed} over a denominator of {buckets}"
        );
        if self.counts.len() > 24 {
            println!(
                "        {} distinct page counts; printing the sixteen widest and the sixteen \
                 narrowest over a denominator of {buckets}",
                self.counts.len()
            );
            let rows: Vec<(usize, usize)> =
                self.counts.iter().map(|(h, c)| (*h, *c)).collect();
            for (held, count) in rows.iter().take(16) {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
            println!("        ...");
            for (held, count) in rows.iter().rev().take(16).rev() {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
        } else {
            for (held, count) in &self.counts {
                println!(
                    "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                    100.0 * *count as f64 / buckets.max(1) as f64
                );
            }
        }
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

/// (Empty, One, Many) arm counts over the whole bucket map -- released and empty buckets included,
/// because a node exists whether or not its bucket holds a page and a node is what the per-bucket
/// contribution is charged on.
fn block_index_arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut arms = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            crate::engine::state::BlockIndexMap::Empty => arms.0 += 1,
            crate::engine::state::BlockIndexMap::One(..) => arms.1 += 1,
            crate::engine::state::BlockIndexMap::Many(..) => arms.2 += 1,
        }
    }
    arms
}

/// Nodes in the bucket map, whether or not their bucket holds a page.
fn node_count(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard.bucket_index.bucket_map.len()
}

// =============================================================================================
// WIDTHS AND STRIDES -- NO FEATURE NEEDED, SO THIS RUNS AT BOTH ENDS UNCONDITIONALLY
// =============================================================================================

/// THE WIDTHS AT THIS END, READ WITH `size_of` RATHER THAN QUOTED, AND THE STRIDE BESIDE THEM.
///
/// A struct width is not what memory costs. The page list holds PAIRS, and a pair re-aligns to the
/// wider of its members' alignments and pads to a multiple of it -- so the number that multiplies
/// by the page count is `size_of::<(u64, BlockIndex)>()` and not `size_of::<BlockIndex>()`. A
/// narrowing that takes the entry below the next multiple of its alignment buys nothing at all in
/// the container, and only the stride can say so.
///
/// This test ASSERTS NOTHING about particular numbers on purpose: it has to pass at both ends of
/// the campaign, where every one of these is different, and an assertion pinning either end would
/// make the other end red instead of measured.
///
/// rust-internal: reads `size_of` on the engine's own structures, no product behaviour
#[test]
fn the_widths_and_the_container_stride_at_this_end_of_the_campaign() {
    use crate::engine::state::{BlockIndex, BlockIndexMap, BucketNode};
    use std::mem::{align_of, size_of};

    let node = size_of::<BucketNode>();
    let entry = size_of::<BlockIndex>();
    let map = size_of::<BlockIndexMap>();
    let address = size_of::<crate::ElementEntry>();
    let stride = size_of::<(u64, BlockIndex)>();

    println!("WIDTHS AT THIS END");
    println!(
        "  BucketNode      {node:>4} B  (align {})   PER BUCKET",
        align_of::<BucketNode>()
    );
    println!(
        "  BlockIndex      {entry:>4} B  (align {})   PER PAGE",
        align_of::<BlockIndex>()
    );
    println!(
        "  BlockIndexMap   {map:>4} B  (align {})   PER BUCKET, inside the node",
        align_of::<BlockIndexMap>()
    );
    println!(
        "  ElementEntry    {address:>4} B  (align {})   PER PAGE, inside the entry",
        align_of::<crate::ElementEntry>()
    );
    println!("CONTAINER STRIDE");
    println!(
        "  (u64, BlockIndex) {stride:>4} B  (align {})  <- THIS is what multiplies by the page \
         count, not {entry}",
        align_of::<(u64, BlockIndex)>()
    );
    println!(
        "  the pair costs {} B more than the entry alone: the key word plus {} B of tail padding",
        stride - entry,
        stride - entry - size_of::<u64>()
    );

    // The one thing that is true at both ends and worth pinning: the stride is at least the entry
    // plus its key, and it is a multiple of the pair's alignment. A stride that came out smaller
    // than its own members would mean the reading is not a stride.
    assert!(
        stride >= entry + size_of::<u64>(),
        "the pair stride {stride} B is smaller than the entry {entry} B plus its key word, which \
         cannot be a stride"
    );
    assert_eq!(
        0,
        stride % align_of::<(u64, BlockIndex)>(),
        "the pair stride {stride} B is not a multiple of the pair's own alignment"
    );
}

// =============================================================================================
// THE INSTRUMENT ITSELF -- BOTH COLUMNS, AND PROOF THE CHUNK COLUMN IS NOT A COPY
// =============================================================================================

/// THE CHUNK COLUMN READS THE ALLOCATOR AT THIS END TOO, AND IS NOT A COPY OF THE REQUEST COLUMN.
///
/// The chunk counter was added late in the campaign. To read both ends on one instrument it has to
/// be present at both, and a counter that quietly degraded to echoing the request would make the
/// pre-campaign chunk column a copy of its request column -- which would under-charge every
/// out-of-line shape on exactly one side of the comparison and in the direction that flatters the
/// later tree. So a size whose chunk is KNOWN and different from its request is planted here and
/// the difference is recovered. This test is the licence to print a chunk column at this end.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn the_chunk_column_reads_the_allocator_at_this_end_and_is_not_a_copy_of_the_request() {
    const REQUEST: usize = 100;
    const PLANTED: usize = 64;
    let expected = crate::alloc_probe::documented_glibc_chunk(REQUEST);
    assert_ne!(
        REQUEST, expected,
        "the planted size must be one the allocator rounds, or this control cannot tell a chunk \
         reading from a request reading"
    );

    let probe = Probe::start();
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(PLANTED);
    for _ in 0..PLANTED {
        held.push(vec![0xA5u8; REQUEST]);
    }
    let counts = probe.stop();
    std::hint::black_box(&held);

    let over = counts.chunk_bytes as i64 - counts.alloc_bytes as i64;
    println!(
        "planted {PLANTED} x {REQUEST} B: request {} B, chunk {} B, over {over} B (documented \
         chunk for {REQUEST} B is {expected} B)",
        counts.alloc_bytes, counts.chunk_bytes
    );
    assert!(
        counts.chunk_bytes > counts.alloc_bytes,
        "the chunk column read {} B against the request column's {} B, so at this end it is a \
         copy of the request column and the two ends of this campaign are two instruments",
        counts.chunk_bytes,
        counts.alloc_bytes
    );
    assert!(
        over >= (PLANTED * (expected - REQUEST)) as i64,
        "the chunk column read {over} B over the request column for {PLANTED} planted requests \
         whose documented rounding is {} B each; the chunk rule is a FLOOR, so a reading BELOW \
         the floor means the counter is not reading the allocator",
        expected - REQUEST
    );
}

// =============================================================================================
// THE FOUR CELLS
// =============================================================================================

#[derive(Debug, Clone, Copy)]
struct Cell {
    records: usize,
    end_routing_bucket: u32,
    allocs: u64,
    request_bytes: u64,
    chunk_bytes: u64,
    freed_bytes: u64,
    nodes: usize,
    blocks: usize,
    occupied: usize,
}

impl Cell {
    fn request_per_record(&self) -> f64 {
        self.request_bytes as f64 / self.records as f64
    }
    fn chunk_per_record(&self) -> f64 {
        self.chunk_bytes as f64 / self.records as f64
    }
    fn retained_per_record(&self) -> f64 {
        (self.request_bytes as i64 - self.freed_bytes as i64) as f64 / self.records as f64
    }
    fn allocs_per_record(&self) -> f64 {
        self.allocs as f64 / self.records as f64
    }
    /// The per-BUCKET structure's share, per record: one node per node, divided by records.
    fn node_bytes_per_record(&self, node_width: usize) -> f64 {
        (self.nodes * node_width) as f64 / self.records as f64
    }
    /// The per-PAGE structure's share, per record: one container slot per page.
    fn block_bytes_per_record(&self, stride: usize) -> f64 {
        (self.blocks * stride) as f64 / self.records as f64
    }
}

#[cfg(feature = "alloc-probe")]
fn measure_cell(arm: usize, records: usize, end_routing_bucket: u32) -> (Cell, BlocksPerBucket) {
    let dir = arm_dir(arm);
    let engine = engine_on(&dir);
    load_on(&engine, end_routing_bucket);

    // Built OUTSIDE the probe span. The command vector is the fixture's own cost, not the
    // engine's, and charging it to the engine would put the same constant in every cell and
    // flatten every ratio towards one.
    let (keys, commands) = seed_commands(records);
