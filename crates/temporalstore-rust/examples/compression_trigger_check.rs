// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Runtime proof for the configurable auto temporal-compression trigger.
//!
//! Run with: cargo run -q -p temporalstore-rust --example compression_trigger_check
//!
//! Compiles only the (clean) library plus this example. Enables the policy via env with
//! small thresholds, writes events on one node, and verifies that older windows get
//! folded into ContextCompressionEvents while the newest `keep_recent` stay raw.

use temporalstore_rust::{
    Command, CommandResponse, ContextEvent, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 7777;
const NODE: u64 = 4242;

fn engine() -> TemporalEngine {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-compress-trigger-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        4096,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    engine
}
