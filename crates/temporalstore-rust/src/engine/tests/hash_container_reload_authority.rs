// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DOES A CONTAINER'S RESIDENT MAP STILL HOLD ITS ELEMENTS AFTER A RELOAD?
//!
//! #2054 made `hashes` durable -- `#[serde(default)]` with no `skip_serializing` -- which is what
//! makes it possible for `HashLen` to answer from the container instead of from the page-index entry
//! count. `HashLen` answers from the entry count today
//! (`bucket_index_component_block_addresses(..).len()`), and that fact is what closed the batching
//! route in #1999: an entry that stopped naming an element would silently change what HLEN returns.
//!
//! SO THE REROUTE DEPENDS ON A PROPERTY NOBODY HAS ASSERTED IN GENERAL. #2054 drove exactly one
//! case -- a page entry carrying no component, where only the durable map can supply the element --
//! and asserted both fields present after a reload. That is the mechanism working, not the general
//! claim that the map's LENGTH is right after a reload for an arbitrary container. This module
//! asserts the general claim, for the four container kinds, before anything is pointed at it.
//!
//! WHY LENGTH AND NOT MEMBERSHIP. A reroute of `HashLen` reads `len()`, so `len()` is what has to be
//! right. Membership is asserted beside it so a map that came back with the right COUNT of the wrong
//! elements cannot pass -- a count and a set are different questions and this module needs both.
//!
//! THE DENOMINATOR IS TAKEN BEFORE THE UNLOAD. Every arm asserts the map holds what was written
//! while the engine is still up, so a failure after the reload is about the reload and not about the
//! write. Without that, a write path that never populated the map would read as a durability hole.
//!
//! AND THE ENTRY COUNT IS REPORTED BESIDE THE MAP LENGTH, because the whole point is whether the two
//! agree. Where they disagree, the reroute CHANGES AN ANSWER, and that is a fact a reviewer needs
//! rather than a surprise after the fact.
#![allow(clippy::all)]
use super::*;

/// The shipped default since #1973, and the range an operator is told to set.
const NARROW_END: u32 = 1023;

/// Elements per container. Small: this module is about durability, not scale.
const ELEMENTS: usize = 12;

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
        table_name: "reload-authority".to_string(),
        shard_uri: "local://reload-authority/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard must load for any figure below to mean anything: {:?}",
        response.status
    );
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write must ack: {:?}", response.status);
}

/// (map length, map's element names) for one container, read straight off the resident map.
fn map_state(engine: &TemporalEngine, kind: &str, key: &str) -> (usize, Vec<String>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    match kind {
        "hash" => shard
            .hashes
            .get(key)
            .map(|fields| {
                let mut names: Vec<String> = fields.keys().map(|k| k.to_string()).collect();
                names.sort();
                (fields.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "set" => shard
            .sets
            .get(key)
            .map(|members| {
                let mut names: Vec<String> = members
                    .keys()
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .collect();
                names.sort();
                (members.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "zset" => shard
            .zsets
            .get(key)
            .map(|members| {
                let mut names: Vec<String> = members
                    .keys()
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .collect();
                names.sort();
                (members.len(), names)
            })
            .unwrap_or((0, Vec::new())),
        "list" => shard
            .lists
            .get(key)
            .map(|elements| (elements.len(), Vec::new()))
            .unwrap_or((0, Vec::new())),
        other => panic!("unclassified kind {other}"),
    }
}

/// Live page-index entries for one container -- what `HashLen` answers from TODAY.
fn live_entries(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    shard
        .bucket_index
        .bucket_map
        .values()
        .flat_map(|bucket| {
            bucket
                .block_index
                .iter()
                .map(move |(handle, block)| (bucket.routing_bucket, *handle, block))
        })
        .filter(|(routing_bucket, handle, block)| {
            !block.deleted
                && block.model_id.as_str() == kind
                && &**named.at(*routing_bucket, *handle) == key
        })
        .count()
}

fn hash_len_served(engine: &TemporalEngine, key: &str) -> i64 {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashLen {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "HashLen must ack: {:?}", response.status);
    match response.response {
        CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?} instead of an integer"),
    }
}

/// THE CONTAINER'S RESIDENT MAP STILL HOLDS ITS ELEMENTS AFTER AN UNLOAD AND A RELOAD.
///
/// Four kinds, `ELEMENTS` elements each, the population asserted BEFORE the unload so a failure
/// afterwards is about the reload. The page-index entry count is printed beside the map length at
/// both points, because whether those two agree is exactly what decides whether pointing `HashLen` at
/// the map changes an answer.
///
/// rust-internal: drives the four container writes and one reload
#[test]
fn a_containers_resident_map_still_holds_its_elements_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let kinds = [("hash", "rl-h"), ("set", "rl-t"), ("zset", "rl-z"), ("list", "rl-l")];
    for index in 0..ELEMENTS {
        write_to(
            &engine,
            Command::HashSet {
                key: "rl-h".to_string(),
                field: format!("f{index:04}"),
                value: vec![b'v'; 24],
