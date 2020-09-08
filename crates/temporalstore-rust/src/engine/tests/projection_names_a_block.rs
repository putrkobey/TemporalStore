// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PROJECTION EMITS PER OBJECT, AND THAT RE-DERIVING TWICE DOES NOT MOVE IT.
//!
//! OBSERVED BEFORE IT IS ASSERTED. This module prints the entry count first and asserts only the
//! fixture's floor, because writing "assert one entry" before running it would be fitting the test
//! to a guess -- and if the count comes out as something else, the pressure is then to adjust the
//! expectation rather than to understand it. The assertions come in a second pass, over the
//! numbers this prints.
//!
//! THE HEADER USED TO READ "GATE OFF AGAINST GATE ON", AND THE GATE IS GONE. Both of this module's
//! columns went through the same `rederive`, so the comparison was between a projection and
//! itself, and the two assertions pinning the "ungated" column to one entry per ELEMENT described
//! a projection that no longer existed. The fixture is kept and asked a question it can still
//! answer wrongly -- whether the derivation is IDEMPOTENT, which matters because a compaction
//! sweep re-derives twice. The paragraph about owning a process-global gate variable went with the
//! gate: this module sets nothing global any more.

#![allow(clippy::all)]
use super::*;

const VALUE_WIDTH: usize = 24;

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
        table_name: "projection".to_string(),
        shard_uri: "local://projection/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("m-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

/// Distinct live pages, live entries, and tombstoned entries this set object resolves to.
fn blocks_and_entries(engine: &TemporalEngine, object_key: &str) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut blocks = std::collections::BTreeSet::new();
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
