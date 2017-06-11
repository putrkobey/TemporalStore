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
