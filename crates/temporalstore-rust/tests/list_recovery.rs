// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A list's order must survive a restart.
//!
//! Elements are pages named by a sortable sequence component in the bucket index, so recovery
//! rebuilds the in-memory list purely from the index -- including elements pushed on the LEFT,
//! whose sequences are negative. This drives pushes on both ends, reopens the engine from disk,
//! and requires the exact order back; then proves pops persist too (a popped element must not
//! resurrect on the next restart).

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, LoadShardRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-list-recovery-{name}-{pid}"));
    root
}

fn new_engine(root: &PathBuf) -> TemporalEngine {
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    // `load_shard` builds this request and then drops the answer on the floor:
    //
    //     let _ = self.load_shard_with(request);
    //
    // So a load that fails is invisible until the first command comes back shard_not_loaded, and
    // that status says only that the shard is absent, never why. Both of these tests fail exactly
    // that way on main. Asking through load_shard_with makes the load report for itself.
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD_ID,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
