// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Runtime proof for the in-memory, coalesced summary-dirty tracking.
//!
//! Run with: cargo run -q -p temporalstore-rust --example inmemory_dirty_check
//!
//! This compiles only the (clean) library plus this example — it does not build the
//! other binaries or the internal unit-test modules, so it verifies the dirty change
//! independently of unrelated pre-existing breakage elsewhere in the crate.

use temporalstore_rust::{
    Command, CommandResponse, ContextDirtyNode, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 4242;

fn engine() -> TemporalEngine {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-inmemory-dirty-example-{}", std::process::id()));
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

fn mark(engine: &TemporalEngine, node: u64, ts: u64, depth: u32) {
    let r = engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                node_hash: node,
                event_time_ms: ts,
                reason: 1,
                propagate_depth: depth,
            },
        })
        .response;
    assert!(matches!(r, CommandResponse::ContextObjectKey { .. }));
}

fn query(engine: &TemporalEngine, node: u64, start: u64, end: u64) -> Vec<ContextDirtyNode> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash: node,
                start_time_ms: start,
                end_time_ms: end,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => nodes,
        other => panic!("unexpected: {other:?}"),
    }
}

fn main() {
