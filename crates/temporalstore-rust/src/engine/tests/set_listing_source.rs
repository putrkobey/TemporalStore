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
                    stated.insert(component);
                }
            }
        }
    }
    stated
}


/// Drop `how_many` of this object's live set entries, leaving their PAGE untouched.
fn drop_live_entries(engine: &TemporalEngine, object_key: &str, how_many: usize) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    // TAKEN BEFORE THE `retain`, AND OWNING ITS KEYS SO IT CAN BE. The keys live in a sibling
    // field of the index whose bucket map this holds mutably; a map that borrowed would be
    // refused here.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut dropped = 0usize;
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        let routing_bucket = bucket.routing_bucket;
        bucket
            .block_index
            .retain(
                &mut shard.bucket_index.block_slab_live,
                crate::engine::state::StaleCause::Removed,
                |handle, block| {
                let mine = block.model_id.as_str() == "set"
                    && &**named.at(routing_bucket, *handle) == object_key
                    && !block.deleted;
                if mine && dropped < how_many {
                    dropped += 1;
                    return false;
                }
                true
            });
    }
    dropped
}

fn listed_members(engine: &TemporalEngine, object_key: &str) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: object_key.to_string(),
        },
    });
    assert!(response.status.ok, "the listing failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("the listing answered {other:?} rather than members"),
    }
}

fn all_members() -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = (0..MEMBERS).map(member_bytes).collect();
    all.sort();
    all
}

// =================================================================================================
// PRESENCE
// =================================================================================================

/// EVERY MEMBER IS STILL LISTED AFTER A FOLD, AND THE ORDER IS THE ONE CALLERS HAD.
#[test]
fn a_folded_set_still_lists_every_member_in_the_order_it_always_did() {
    let (engine, _dir) = folded_set("listing/whole");
    let (blocks, live, tombstoned) = block_and_entry_counts(&engine, "listing/whole");
    println!(
        "[presence] {blocks} distinct page(s), {live} live entr(ies), {tombstoned} tombstoned, \
         for {MEMBERS} members"
    );
    // RE-ATTRIBUTED, BECAUSE `pages < live` BECAME UNREACHABLE RATHER THAN FALSE. All four
    // container kinds now file ONE index entry a page, so every live entry of this object names a
    // distinct page and `pages == live` holds BY CONSTRUCTION -- the old form can only ever fail,
    // and its obvious repair, `pages == live`, is a tautology that would read like a guard while
    // asserting nothing, both sides being the same walk counted twice.
    //
    // THE SHARING THIS ARM NEEDS IS A PROPERTY OF THE PAGE PAYLOAD, which is where the members
    // went, so it is asserted there: strictly more members stated by the pages than there are
    // pages. That compares two independent artefacts -- the index's page count, and the bytes
    // inside those pages -- and the UNFOLDED shape FAILS it, at MEMBERS pages stating MEMBERS
    // members, which is what makes this a measurement of the fold rather than a restatement of it.
    let stated = components_the_blocks_still_state(&engine, "listing/whole");
    assert_eq!(
        MEMBERS,
        stated.len(),
        "the live pages state {} member(s), not the {MEMBERS} written, so the payload side of the \
         comparison below is not this whole set",
        stated.len()
    );
    assert!(
        blocks < stated.len(),
        "the members did not come to share a page: {blocks} page(s) stating {} member(s), so \
         nothing here is exercising a page read that used to happen several times",
        stated.len()
    );
    let listed = listed_members(&engine, "listing/whole");
    // ORDER AND NOT JUST CONTENT. The walk is sorted by component, a set's component is its member
    // in hex, and hex preserves byte order -- so this is the order the listing always returned.
    assert_eq!(
        all_members(),
        listed,
        "the folded set did not list its members, in order"
    );
}

// =================================================================================================
// ABSENCE -- the property the rejected version failed
// =================================================================================================

/// A MEMBER REMOVED AFTER THE FOLD STAYS REMOVED, THOUGH ITS PAGE STILL SAYS OTHERWISE.
///
/// THIS IS THE ARM THAT WOULD HAVE CAUGHT THE REJECTED VERSION, and it is built so that it can.
/// The removal lands AFTER the fold, so the folded page still states the member as live while the
/// removal lives in a tombstone page and a deleted entry. The two sources therefore DISAGREE, and
/// that disagreement is asserted before the listing is consulted -- otherwise this would be an
/// agreement test and could not discriminate.
///
/// A reader enumerating the payload answers with the removed member. A reader taking identity from
/// the live entries cannot.
#[test]
