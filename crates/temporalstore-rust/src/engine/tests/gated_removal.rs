// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A GATED REMOVAL STAYS REMOVED, THOUGH THE PAGE STILL STATES THE MEMBER.
//!
//! # WHY THIS IS DRIVEN RATHER THAN BUILT
//!
//! Deletion as a property of the element in the payload is already implemented:
//! `CONTAINER_BLOCK_MAGIC_V2` writes a `TAG_REMOVED` item per removed element, and
//! `ContainerBlockItem::deleted` carries it. The module says why in the place a reader would look --
//! "a removal has to be a WRITTEN ITEM rather than an absence, because a page is never rewritten by
//! the removal path" -- and records the failure it closed: twelve members folded onto one page, one
//! removed, the index naming eleven and the page holding twelve, so a membership derived from pages
//! resurrected the removed member.
//!
//! So the page format needs nothing, the payload carries its own version in its magic rather than in
//! `SHARD_INDEX_FORMAT_VERSION`, and no stamp moves. What reading CANNOT answer is whether a removal
//! survives when the index entry no longer names elements, and that is this test.
//!
//! # THE TRAP THIS IS AIMED AT
//!
//! `mark_bucket_index_block_deleted` finds the entry to tombstone BY COMPONENT. Under one entry per
//! page the entry carries none, so the match cannot select it -- the same selection-versus-meaning
//! shape that produced an orphan at step two and five entries at step five-a. A removal that cannot
//! find its entry, over a page that still states the member as live, is exactly the arrangement a
//! resurrection comes out of.
//!
//! # PRESENCE AND ABSENCE ARE TWO PROPERTIES
//!
//! This campaign has two independent demonstrations that a presence-only guard cannot see a
//! resurrection -- a listing with a working source-proof AND a working negative control still served
//! a removed member. So the arm is three-part and the middle part is the one that matters:
//!
//!   1. remove through the REAL surface, not by reaching into the index;
//!   2. **prove the page still states the member**, so the two sources demonstrably disagree before
//!      the reader is consulted -- otherwise this is an agreement test and can discriminate nothing;
//!   3. assert the reader sides with the AUTHORITATIVE source.
//!
//! Plus a reload, because the durable question is not what a live engine answers but what comes
//! back.

#![allow(clippy::all)]
use super::*;

const MEMBERS: usize = 40;
const VALUE_WIDTH: usize = 24;

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "removal".to_string(),
        shard_uri: "local://removal/1".to_string(),
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
    let stamp = format!("r-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// THE AUTHORITATIVE SOURCE: the members the durable map holds for this object.
fn resident_members(engine: &TemporalEngine, object_key: &str) -> std::collections::BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .sets
        .get(object_key)
        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
        .unwrap_or_default()
}

/// THE OTHER SOURCE: every element key a live page of this object still states as PRESENT.
///
/// Read off the bytes, so the disagreement below is observed rather than assumed.
fn components_the_blocks_state(
    engine: &TemporalEngine,
    object_key: &str,
) -> std::collections::BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut stated = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&block.address) else {
                continue;
            };
            if let crate::engine::container_blocks::ContainerBlockDecode::Framed {
                spelling,
                items,
                ..
            } = crate::engine::container_blocks::decode_container_block(&bytes)
            {
                for item in items {
                    if item.deleted {
                        continue;
                    }
                    if let Some(component) =
                        crate::engine::container_blocks::component_from_element_key(
                            spelling, &item.key,
                        )
                    {
                        stated.insert(component);
                    }
                }
            }
        }
    }
    stated
}

/// Live and tombstoned set entries for this object.
fn entry_counts(engine: &TemporalEngine, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
