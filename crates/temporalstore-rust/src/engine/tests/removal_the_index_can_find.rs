// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A REMOVAL THE INDEX CAN FIND.
//!
//! # THE STATE THIS IS ABOUT
//!
//! Under `TS_CONTAINER_ONE_ENTRY_A_PAGE` a removal still appends a tombstone page, and that page
//! still names the element in its payload -- step six established both. Then
//! `mark_bucket_index_block_deleted_recording` goes looking for the entry to tombstone **by
//! component**, and a page-named entry has none. Nothing is selected, nothing is filed, and the
//! tombstone page ends up **unreachable from the index**.
//!
//! Step six measured exactly that and recorded it as harmless: `1 live entry, 0 tombstoned`, with
//! the pages still stating the removed member. It *was* harmless, because the durable map was the
//! authority and the listing never consulted the pages.
//!
//! # WHY IT STOPS BEING HARMLESS NOW
//!
//! The listing today asks the ENTRIES which members exist and the page only what each one holds,
//! and it is written that way because an earlier attempt enumerated the payload instead and
//! **resurrected a removed member**. Under the gate there are no per-element entries, so a gated
//! listing has to enumerate the payload -- the direction that failed before. The thing that makes
//! it safe is `container_membership::derive_membership`, which folds every page of an object by
//! `append_position`, last writer wins, treating a value and a tombstone as the same kind of
//! statement. That is the precedence a hand-rolled loop lacks, and it is why that function has no
//! production callers yet: it was built for this step.
//!
//! But a fold can only fold what it is given, and it is given the pages THE INDEX NAMES.
//!
//! # UNREACHABLE IS WORSE THAN ABSENT, WHICH IS WHY IT IS ITS OWN TEST
//!
//! A tombstone page that does not exist and a tombstone page nothing points at produce the same
//! silence, and a reader cannot tell "no removal happened" from "a removal I cannot see". The
//! second is worse, because the store did everything right -- the bytes are on disk, correct and
//! decodable -- and the only broken thing is that no entry mentions them.
//!
//! # AND `is_complete()` CANNOT CATCH IT
//!
//! This is the part worth saying out loud rather than routing around. `DerivedMembership::is_complete`
//! is false when a page could not be read, could not be decoded, was unframed, or held an item whose
//! key would not render. **Every one of those is a page the fold TRIED.** An unreachable page is
//! never tried, so nothing is counted, and the derivation reports itself complete while being wrong.
//!
//! So the completeness signal has a hole exactly where this defect lives, and the test asserts
//! `is_complete()` is TRUE alongside the wrong answer -- because a guard that passed here is the
//! evidence, not a nuisance.

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use std::collections::BTreeSet;

const MEMBERS: usize = 40;
const KEY: &str = "reachable-set";

/// Sets the gate while held and restores it on the way out, including during a panic.
///
/// The suite is one process and `--test-threads=1` runs these in order, so a variable left set is
/// seen by every later test. A bare remove at the end of a test covers only the happy path.



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
        table_name: "a-removal-the-index-can-find".to_string(),
        shard_uri: "local://a-removal-the-index-can-find/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:03}").into_bytes()
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// Every page address THE INDEX NAMES for this object, live entries and tombstoned alike, with the
/// two counted separately so the shape of the index is visible and not merely its size.
fn addresses_the_index_names(engine: &TemporalEngine) -> (Vec<ElementEntry>, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut addresses = Vec::new();
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != KEY
            {
                continue;
            }
            if block.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
            addresses.push(block.address.clone());
        }
    }
    (addresses, live, tombstoned)
}

/// The live components the PAGES THE INDEX NAMES state, read off the bytes directly.
///
/// This is the middle step of the arm: it establishes that the two sources disagree BEFORE any
/// reader is consulted. Without it this would be an agreement test, and a presence-only guard
/// cannot see a resurrection -- a listing with a working source-proof and a working negative
/// control still served a removed member earlier in this campaign.
fn components_the_named_blocks_state(engine: &TemporalEngine) -> BTreeSet<String> {
    let (addresses, _, _) = addresses_the_index_names(engine);
    let mut stated = BTreeSet::new();
    for address in &addresses {
        let Ok(bytes) = engine.block_store.read(address) else {
