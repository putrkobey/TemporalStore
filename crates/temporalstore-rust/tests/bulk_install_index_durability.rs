// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Regression: manifest install-on-load must durably materialize the restored index even
//! under bulk-ingest mode.
//!
//! `persist_index_bytes` is deliberately a no-op under `MATRIXARK_BULK_INGEST` to turn the
//! per-record served-index rewrite from O(n^2) into one write per batch. But manifest
//! *install* is a one-shot recovery step, not the hot path: if it skips the durable index
//! write, `load_index()` keeps reading the stale pre-manifest index while the advanced
//! replay watermark (the manifest's wal_sequence) suppresses replay of the very records
//! the manifest embeds -- records that were already WAL-GC'd and now live ONLY in the
//! manifest. The net effect is silent data loss on a bulk-mode recovering load. Install must
//! therefore bypass the bulk gate (`persist_index_bytes_durable`).
//!
//! This test runs in its own integration-test process, so toggling the process-wide
//! `MATRIXARK_BULK_INGEST` env var cannot race the (single-process) library unit tests.

use std::path::PathBuf;

use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};

const SHARD_ID: u64 = 1;

fn root() -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-bulk-install-durability-{}", std::process::id()));
    root
}

#[test]
fn manifest_install_materializes_index_under_bulk_mode() {
    let root = root();
    let _ = std::fs::remove_dir_all(&root);
    let index_dir = root.join("indexes");
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        4096,
        root.join("cache"),
        root.join("blocks"),
        &index_dir,
    );
    engine.load_shard(SHARD_ID);
