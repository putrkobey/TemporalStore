// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A PAGE A VIEW REBUILD CANNOT READ IS COUNTED AND SKIPPED, NOT TURNED INTO AN EMPTY SERIES.
//!
//! # THE CLAIM
//!
//! `insert_timestamped_secondary_view` and `insert_context_event_views` rebuild a
//! `timestamp -> address` map by READING each page and decoding the points in it. Both began
//! `block_store.read(&address).ok()` and ended `.unwrap_or_default()`, so three different outcomes
//! -- a read that failed, a payload from before the packed format, and a payload that is packed and
//! corrupt -- all became the SAME empty vector, and the key still got an entry.
//!
//! # WHY IT IS LOSS AND NOT DEGRADATION, WHICH IS THE PART WORTH DRIVING
//!
//! `reconcile_timestamped_series_membership` keeps a persisted series the derived view could not
//! produce, and its comment says exactly that: "so a transient read failure never drops a durable
//! series". That consolation is REAL for `features`, whose map is serialized.
//!
//! It does not exist for `context_events` or `context_indexes`. Both are `skip_serializing` on
//! `ShardState`, so the persisted map is EMPTY BY CONSTRUCTION on every load and the derived view is
//! the only copy there is. A swallowed read on those does not weaken an answer, it removes the
//! points. That is the same argument #2016 made for `hashes` -- "THE CONSOLATION THE OTHER THREE ARMS
//! RELY ON DOES NOT EXIST HERE" -- reaching two more maps by a different route.
//!
//! So the fixtures below CLEAR the durable map before reconciling, which is not a trick: it is what a
//! load does for a `skip_serializing` map, every time.
//!
//! # HOW THE READ IS MADE TO FAIL, AND WHY THERE IS NO SEAM
//!
//! The slab files the store was written into are TRUNCATED TO ZERO, so `block_store.read` fails for
//! real, on the arm that runs in production.
//!
//! A thread-local seam was written first, modelled on `fail_compaction_block_read_after_for_test`.
//! A mutant that deleted the counter from the REAL `Err(_)` arm then SURVIVED -- every fixture failed
//! through the seam, so nothing exercised the production arm at all, and the seam was buying a
//! `cfg(test)` hook to cover a path it was not on. Truncation covers the real arm and the seam is
//! gone from the engine.
//!
//! # THE CONTROL, AT ZERO, WITH ITS DENOMINATOR
//!
//! A clean load of the same fixture must leave BOTH counters at zero AND must have rebuilt a
//! non-empty series from a non-zero number of live pages. A control over a store with no pages
//! reports zero for the wrong reason, so the page count is asserted first -- and the fault arm below
//! reconciles CLEANLY FIRST on its own fixture, so the before/after pair is the same store rather
//! than two stores that might differ for some other reason.
#![allow(clippy::all)]
use super::*;

/// Points per series. Several, so the series is non-empty by more than one and a partial rebuild is
/// visible as a count rather than only as presence.
const POINTS: u64 = 12;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "view-rebuild-reads".to_string(),
        shard_uri: "local://view-rebuild-reads/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// A feature series of `POINTS` points under one key, written through the engine's own command
/// surface.
fn seed_series(engine: &TemporalEngine, key: &str) {
    for index in 0..POINTS {
        write(
            engine,
            Command::FeatureAppend {
                key: key.to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 1_000 + index,
                    value: format!("v{index:04}").into_bytes(),
                }],
            },
        );
    }
}

/// Truncate every file under the store's page directory to zero bytes, so `block_store.read` fails
/// for real. Returns how many files it tore, which is a DENOMINATOR: tearing nothing leaves the
/// fault arm reading a healthy store.
fn tear_the_blocks(dir: &std::path::Path) -> usize {
    let mut torn = 0usize;
    let mut stack = vec![dir.join("blocks")];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                stack.push(child);
            } else if std::fs::File::create(&child).is_ok() {
                torn += 1;
            }
        }
    }
    torn
}

/// Live, undeleted pages in the settled index. The DENOMINATOR for every arm: a reconcile over an
/// empty index rebuilds nothing and would make both a clean control and a fault arm look right.
fn live_blocks(shard: &ShardState) -> usize {
    shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| {
            bucket
                .block_index
                .values()
                .filter(|block| !block.deleted)
                .count()
        })
        .sum()
}

fn rebuilt_points(shard: &ShardState, key: &str) -> usize {
    shard.features().get(key).map(|s| s.len()).unwrap_or(0)
}

/// rust-internal: drives the engine's own command surface
#[test]
fn a_clean_view_rebuild_reports_zero_unreadable_blocks_over_a_store_that_has_some() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_series(&engine, "clean");

    crate::engine::storage_bucket_internals::reset_view_rebuild_block_failure_counts();
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard is loaded");
    let blocks = live_blocks(shard);
    assert!(
        blocks > 0,
        "DENOMINATOR: {blocks} live pages, so this control is a zero because nothing was read"
    );

