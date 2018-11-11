// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the serving read fast path does while it holds the shard-table read guard.
#![allow(clippy::all)]
use super::*;

/// Fields on the one hash the measurement reads. Chosen larger than any plausible per-call
/// constant so a hold that scales with the value's WIDTH cannot be mistaken for a fixed one.
const FIELDS: usize = 64;

/// A shard holding one wide hash whose pages are all OUT of cache.
fn cold_wide_hash(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..FIELDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{index:04}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Without this every page is already resident and the read below never reaches the block
    // store -- the count would be zero and the claim would hold for the wrong reason.
    let _ = engine.cache.invalidate_shard(1);
    engine
}

/// One `HashGetAll` down the durable serving route, with the page reads counted.
///
/// Returns the tally and the FIELD COUNT the response actually carried. Those are two different
/// claims -- "N reads happened under the guard" and "the value had N fields" -- and a single
/// total passes when two errors cancel, so they are asserted apart below.
fn measured_hash_get_all(
    engine: &TemporalEngine,
) -> (crate::engine::MaintenanceBlockReadCounts, usize) {
