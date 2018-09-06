// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Integration coverage for the in-memory, coalesced summary-dirty tracking.
//!
//! Summary-dirty nodes are no longer persisted as one `ctx:dirty` page per event.
//! They are tracked in an in-memory, coalescing hashmap keyed by dirty object key so
//! repeated marks for the same node collapse into a single entry. These tests exercise
//! the public engine API to prove:
//!   * repeated marks for one node coalesce into a single queried marker,
//!   * the coalesced marker carries the latest event time and the max propagate depth,
//!   * distinct nodes remain independently queryable,
//!   * the time-window filter still applies,
//!   * coalescing is bounded (N marks -> 1 marker), which is the whole point of the change.

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ContextDirtyNode, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 4242;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    // Avoid Instant/SystemTime nondeterminism concerns: use pid + a per-name suffix.
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-inmemory-dirty-{name}-{pid}"));
    root
}

fn new_engine(name: &str) -> TemporalEngine {
    let root = unique_root(name);
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("blocks"),
