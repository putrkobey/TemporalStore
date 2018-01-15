// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Crash reconstruction across the index-log + base-snapshot boundary, ON THE DEFAULT PATH.
//!
//! The index-log is always-on: every write appends a delta record, and this file asserts that a
//! store reopened after a crash comes back with what it acked -- before any dump (no base at all)
//! and after one (base plus the writes that followed it). The index-log itself is bounded by the
//! consumer-aware storage-manager index GC, exercised by the lib test
//! `storage_wal_index_gc_reclaim_requires_durable_generation_and_retention_release` (records
//! removed + budget + restart reconstruction).
//!
//! WHAT THESE TWO TESTS DO NOT COVER, SO NOBODY READS THE FILENAME AND ASSUMES THEY DO. They do
//! not exercise the load-time delta FOLD. Under the single-barrier default -- which is what runs
//! here, and in production -- `load_shard_with` calls `load_index_base_only`, which passes
//! `fold_deltas = false`; `fold_index_log_deltas` is not on that path at all. Reconstruction
//! below is the durable base plus a WAL replay of the tail beyond its anchor, and the delta
//! records are written, retained, GC'd and never read. Measured, not inferred: replacing the body
//! of `fold_index_log_deltas` with `panic!` leaves both tests green.
//!
//! The fold is reached only through `load_index_checked`, i.e. only under the
//! `TS_WAL_LEGACY_RECOVERY` escape hatch, and what it does is asserted by the lib test
//! `a_delta_fold_recovery_applies_the_tombstones_a_stale_base_still_denies` (engine::tests::
//! expiry_scale), which folds an expiry delta onto a deliberately stale base and fails when the
//! fold applies nothing. Add a fold claim HERE only with a fixture that makes the delta the only
//! source of its answer -- against a fresh base and a live WAL, replay produces the same answer
//! with the fold deleted.

use std::fs;
use std::path::{Path, PathBuf};

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, LocalIndexLogStore, TemporalEngine,
};

const SHARD: u64 = 1;

fn root(tag: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-idxlog-gc-{tag}-{}", std::process::id()));
    root
}

fn build(root: &Path) -> TemporalEngine {
    for sub in ["cache", "pages", "indexes"] {
        fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    TemporalEngine::with_local_dirs(4096, root.join("cache"), root.join("blocks"), root.join("indexes"))
}

fn set(engine: &TemporalEngine, key: &str, value: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
        },
    });
    assert!(response.status.ok, "set {key}: {response:?}");
}

fn get(engine: &TemporalEngine, key: &str) -> Option<String> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::Bytes { value } => value.map(|v| String::from_utf8_lossy(&v).to_string()),
        other => panic!("unexpected get response for {key}: {other:?}"),
