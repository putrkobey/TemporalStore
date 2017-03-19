// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What `page_reads`/`block_reads` and `page_writes`/`block_writes` actually count.
//!
//! A committed fixture (`compat/storage_unified_case_report_pair.json`) recorded
//! `page_reads: 4` next to `block_reads: 2` -- a clean factor of two, in six places, in both
//! halves of the pair. That reads as two different measurements, and a rename treating one as
//! a stale spelling of the other would then halve a real number.
//!
//! It is not two measurements. `engine/engine_persistence.rs` assigns all four fields from the SAME
//! `BlockStore` counters, four lines apart:
//!
//! ```text
//! page_reads:   block_store.reads,
//! page_writes:  block_store.writes,
//! block_reads:  block_store.reads,
//! block_writes: block_store.writes,
//! ```
//!
//! `BlockStore` carries exactly one read counter (`stats.reads`, incremented at three call
//! sites in `block_store/block_store_read.rs`) and one write counter (`stats.writes`, two sites in
//! `block_store/block_store_append.rs`). There is no second measurement for the second name to carry, so
//! the ratio is one -- the fixture's two was authored, never observed.
//!
//! These tests drive the engine over nine shaped workloads and assert the RATIO rather than
//! "a count was recorded", so an edit making one of the pair count something else fails here
//! instead of being rediscovered in a fixture.
#![allow(clippy::all)]
use super::*;

#[derive(Debug, Clone, Copy)]
struct CounterSample {
    name: &'static str,
    page_reads: u64,
    block_reads: u64,
    page_writes: u64,
    block_writes: u64,
}

fn counter_ratio(numerator: u64, denominator: u64) -> String {
    if denominator == 0 {
        format!("undefined (denominator {denominator})")
    } else {
        format!("{:.3}", numerator as f64 / denominator as f64)
    }
}

fn counter_sample(name: &'static str, engine: &TemporalEngine) -> CounterSample {
    let stats = engine.loaded_shard_stats();
    assert_eq!(
        stats.len(),
        1,
        "{name}: expected exactly one loaded shard, got {}",
        stats.len()
    );
    let storage = &stats[0].storage;
    let observed = CounterSample {
        name,
        page_reads: storage.page_reads,
        block_reads: storage.block_reads,
        page_writes: storage.page_writes,
        block_writes: storage.block_writes,
    };
    println!(
        "workload {name}: page_reads={} block_reads={} read_ratio={} \
         page_writes={} block_writes={} write_ratio={}",
        observed.page_reads,
        observed.block_reads,
        counter_ratio(observed.page_reads, observed.block_reads),
        observed.page_writes,
        observed.block_writes,
        counter_ratio(observed.page_writes, observed.block_writes),
    );
    observed
}

fn counter_engine(dir: &std::path::Path, cache_name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache_name),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn counter_set(engine: &TemporalEngine, key: &str, value: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value,
        },
    });
    assert!(
        response.status.ok,
        "write of {key} failed: {:?}",
        response.status
    );
}

fn counter_get(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
