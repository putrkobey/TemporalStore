// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE COLLAPSE EXTENDED TO `list`, AND THE MEASURED REASONS `zset` AND `hash` ARE NOT WITH IT.
//!
//! # WHAT WAS ATTEMPTED AND WHAT THE TREE SAID
//!
//! This module was first written to convert `hash`, `zset` and `list` together. Driving it
//! converted exactly one of the three, and the two refusals are the useful half -- both were
//! refused ON MEASUREMENT rather than on sequencing, and each for its own reason:
//!
//!   * `zset` REFUSED, 12 tests across 5 modules. A zset's component is not a name for the
//!     element, it IS THE SCORE: `{biased_score:016x}{hex(member)}`. Dropping it deletes the
//!     index's only copy of the score, so a member whose durable entry the fold has not delivered
//!     cannot be rebuilt from its page at all -- `durable_outranks_derived::an_empty_zset_member_
//!     is_a_whole_component_and_survives_the_fold_shape` came back `None` where `Some(3.0)` was
//!     written. And a RESCORE is an in-place rewrite from the index's side: the old element key is
//!     tombstoned while its live entry is not superseded, leaving TWO live entries for one member
//!     at one score, which is precisely the shape `index_entry_names_a_block` warns an
//!     address-keyed predicate mishandles.
//!   * `hash` REFUSED on its readers. Four of them resolve through the page index BY COMPONENT
//!     with no resident-map fallback, so a nameless hash entry makes a present field unreachable
//!     rather than merely unnamed.
//!
//! `list` is the one that converts, and it converts because its component carries no data the
//! engine reads back: the sequence that orders a list lives in `shard.lists`, which is keyed BY
//! that sequence, and every list reader (`ListLen`, `ListRange`) reads the resident map. `list`
//! also has no `LSET`, `LINSERT` or `LREM`, so it never rewrites an element in place and the
//! two-live-entries shape cannot arise for it.
//!
//! # TWO ARMS, BECAUSE A LOSS GUARD IS NOT A RESURRECTION GUARD
//!
//! Every arm of this series up to here asked whether a PRESENT element survives, and a change with
//! a source proof and a working negative control still resurrected a REMOVED member -- because
//! nothing removed anything. So the converted kind gets both halves: a present element is still
//! served, and a removed one is still gone. The removed half is asked a second time ACROSS A STORE
//! BOUNDARY, because re-derivation is where a resurrection actually happens, and a third time
//! through WAL REPLAY, where a refusal does not shorten an answer but fails the whole shard load.
//!
//! # THE FOLD IS NOT OPTIONAL AND ITS ABSENCE WOULD MAKE EVERY ARM VACUOUS
//!
//! One entry per PAGE is one entry per ELEMENT until elements SHARE a page. Without
//! `compact_shard_blocks` every element is its own page, the collapsed count equals the
//! per-element count, and every assertion below would pass over a gate that changed nothing. The
//! fold runs in every arm and the entry floor (`live < ELEMENTS`) is what proves it did.
//!
//! # BOTH GATE DIRECTIONS ARE SET EXPLICITLY, NEVER BY REMOVING THE VARIABLE
//!
//! The gate reads through `env_flag_default_on`, so it is `env_bool(name, true)` and REMOVING the
//! variable selects ON rather than off. An arm that cleared the variable to get the ungated path
//! would silently test the gated one.

#![allow(clippy::all)]
use super::*;

const ELEMENTS: usize = 40;
const LIST_KEY: &str = "collapse/list";
const HASH_KEY: &str = "collapse/hash";
const ZSET_KEY: &str = "collapse/zset";

/// Holds the gate at an explicit value and puts back whatever was there, panic or not.



fn engine_on(
    cache: &std::path::Path,
    blocks: &std::path::Path,
    indexes: &std::path::Path,
) -> TemporalEngine {
    TemporalEngine::with_local_dirs(64 * 1024 * 1024, cache, blocks, indexes)
}

fn load_request() -> crate::control::LoadShardRequest {
    crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "collapse-for-list".to_string(),
        shard_uri: "local://collapse-for-list/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    }
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(load_request());
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn respond(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

fn element_bytes(index: usize) -> Vec<u8> {
    format!("element-{index:03}").into_bytes()
}

fn field_name(index: usize) -> String {
    format!("field-{index:03}")
}

fn integer(engine: &TemporalEngine, command: Command) -> i64 {
    match respond(engine, command) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    }
}

fn members(engine: &TemporalEngine, command: Command) -> Vec<Vec<u8>> {
    match respond(engine, command) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    }
}

fn bytes(engine: &TemporalEngine, command: Command) -> Option<Vec<u8>> {
    match respond(engine, command) {
        crate::types::CommandResponse::Bytes { value } => value,
        other => panic!("expected Bytes, got {other:?}"),
    }
}

fn list_elements(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    members(
        engine,
        Command::ListRange {
            key: LIST_KEY.to_string(),
            start: 0,
            stop: -1,
        },
    )
}

fn list_len(engine: &TemporalEngine) -> i64 {
    integer(
        engine,
        Command::ListLen {
            key: LIST_KEY.to_string(),
        },
    )
}

/// Live index entries for one kind and object.
///
/// THE SECOND TERM IS GONE. It counted entries that NAME an element, and `BlockIndex` has no field
/// for a name -- so the count could only read zero and the assertions on it could only pass.
fn live_entries(engine: &TemporalEngine, model_id: &str, object_key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut live = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted
                || block.model_id.as_str() != model_id
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
            live += 1;
        }
    }
    live
}

/// (live entries, tombstoned entries) for one kind and object.
fn live_and_tombstoned(
    engine: &TemporalEngine,
    model_id: &str,
    object_key: &str,
) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != model_id
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
            if block.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

fn write_list(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
            engine,
            Command::ListPush {
                key: LIST_KEY.to_string(),
                member: element_bytes(index),
                left: false,
            },
        );
    }
}

fn write_hash(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
            engine,
            Command::HashSet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
                value: element_bytes(index),
            },
        );
    }
}

fn write_zset(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write(
