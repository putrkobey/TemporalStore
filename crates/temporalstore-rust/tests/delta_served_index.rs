// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Delta / incremental served-index path (now the only served-index mechanism).
//!
//! A write no longer rewrites the whole `shard-{id}.index.json` (O(store) per write); the
//! base snapshot is materialized only at compaction points (dump / flush / gc / unload).
//! Between compactions the in-memory shard is the authoritative served index, which every
//! reader reaches through the served-index funnel (`export_index_bytes` /
//! `read_stream(Index)`), and a cold reload reconstructs current state by folding the base
//! with the index-log deltas beyond the last materialized anchor.

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, StreamKind, StreamReadRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;

fn root(tag: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-delta-served-index-{tag}-{}", std::process::id()));
    root
}

fn build_engine(root: &PathBuf) -> (TemporalEngine, PathBuf) {
    let _ = std::fs::remove_dir_all(root);
    let index_dir = root.join("indexes");
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine =
        TemporalEngine::with_local_dirs(4096, root.join("cache"), root.join("blocks"), &index_dir);
    (engine, index_dir)
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(response.status.ok, "set {key} failed: {response:?}");
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "get {key} failed: {response:?}");
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response for get {key}: {other:?}"),
    }
}

#[test]
fn delta_path_defers_base_write_but_funnel_and_reload_see_current_state() {
    // The delta served-index is now the only path (no flag) -- this validates its contract.
    let root = root("roundtrip");
    let (engine, index_dir) = build_engine(&root);
    let index_path = index_dir.join(format!("shard-{SHARD_ID}.index.json"));
    engine.load_shard(SHARD_ID);
