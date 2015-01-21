// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SET LISTING DECODES EACH PAGE ONCE AND STILL TAKES ITS IDENTITY FROM THE ENTRIES.
//!
//! # WHAT CHANGED, AND WHAT DELIBERATELY DID NOT
//!
//! The listing read one page per entry. After a fold every entry of an object names the SAME page,
//! so a forty-member set read one page forty times and each read was a linear walk of that page's
//! packed keys. It now decodes each distinct page ONCE and answers each entry out of that.
//!
//! WHAT DID NOT MOVE IS WHERE IDENTITY COMES FROM. The entry walk still says which members exist.
//! That is not conservatism, it is the correctness argument, and it is here because the first
//! version of this change got it wrong.
//!
//! # THE REJECTED VERSION, RECORDED BECAUSE IT LOOKED RIGHT
//!
//! The first attempt enumerated members out of the payload and dropped the entry walk. It read one
//! page instead of forty. It had a source-proof -- drop an entry, the member survives -- and a
//! working negative control. It also **resurrected a removed member**, and `tombstone_reload_path`
//! caught it.
//!
//! The mechanism is the thing to remember: a fold followed by a removal leaves the folded page
//! still naming the member as LIVE, while the removal lives in a SEPARATE tombstone page and a
//! DELETED index entry. A reader enumerating the payload sees neither of those, so it serves an
//! element the store was told to forget.
//!
//! And the guard that missed it was not badly built -- it was built over the wrong property.
//! **Presence and absence are two properties.** A loss guard asks "is everything that should be
//! here, here?"; a resurrection guard asks "is everything that should be gone, gone?" The
//! source-proof and the negative control were both about presence. So this module asserts both,
//! and the absence arm is the one that would have failed.

#![allow(clippy::all)]
use super::*;

/// Forty on one compacted page is the measured, real shape.
const MEMBERS: usize = 40;

/// Member payload width, wide enough that a member is not confusable with framing.
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
        table_name: "set-listing-source".to_string(),
        shard_uri: "local://set-listing-source/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("member-{index:04}");
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
    assert!(response.status.ok, "a fixture write failed: {response:?}");
}

/// A loaded shard holding one set of `MEMBERS` members, folded onto as few pages as the batcher
/// will use. The fold is asserted, because every arm here rests on members sharing a page.
fn folded_set(object_key: &str) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    for index in 0..MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: object_key.to_string(),
                member: member_bytes(index),
            },
        );
    }
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the compaction round failed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "the fixture folded nothing: {batches} batch(es), {folded} page(s). Every arm below rests \
         on the members sharing a page, so this is not a fixture detail."
    );
    (engine, dir)
}

/// Distinct page addresses and LIVE index entries this object resolves to, plus tombstoned ones.
fn block_and_entry_counts(
    engine: &TemporalEngine,
    object_key: &str,
) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
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
            if block.deleted {
                tombstoned += 1;
                continue;
            }
            live += 1;
            blocks.insert((
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            ));
        }
    }
    (blocks.len(), live, tombstoned)
}

/// Every element key a live page of this object still states as PRESENT, read off the bytes.
///
/// This is the other source. The point of the absence arm is that it disagrees with the listing.
fn components_the_blocks_still_state(
    engine: &TemporalEngine,
    object_key: &str,
) -> std::collections::BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
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

/// The addresses of this object's LIVE pages, captured so they can still be read after the index
/// entry that names them is gone.
fn live_block_addresses(engine: &TemporalEngine, object_key: &str) -> Vec<ElementEntry> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut addresses = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != object_key
                || block.deleted
            {
                continue;
            }
            addresses.push(block.address.clone());
        }
    }
    addresses
}

/// Every element key a given set of PAGE ADDRESSES still states as present, read straight off the
/// block store rather than through the index.
///
/// IT HAS TO BYPASS THE INDEX, which is the whole reason it exists beside
/// `components_the_blocks_still_state`: `drop_live_entries` removes the index ENTRY and leaves the
/// block, so a page it un-names cannot be reached through `block_index` at all. Reading addresses
/// captured beforehand is what lets an arm say the bytes outlived the name.
fn components_at(
    engine: &TemporalEngine,
    addresses: &[ElementEntry],
) -> std::collections::BTreeSet<String> {
    let mut stated = std::collections::BTreeSet::new();
    for address in addresses {
        let Ok(bytes) = engine.block_store.read(address) else {
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
                    crate::engine::container_blocks::component_from_element_key(spelling, &item.key)
                {
