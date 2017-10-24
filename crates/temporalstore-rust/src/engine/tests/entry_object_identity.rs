// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHY A PAGE ENTRY CARRIES ITS OBJECT'S KEY BESIDE AN ID DERIVED FROM THAT SAME KEY.
//!
//! `BlockIndex` holds `object_key: Arc<str>` and `address: BlockAddress`, and the address holds an
//! `object_id: u64` computed from the key. That reads like the identity stated twice, and #1974
//! established the derivation -- `stable_block_object_id(shard, kind, key)`, computed at
//! about thirty sites. This module is the refutation of removing the key, and it exists because the
//! refutation is NOT the one the shape suggests.
//!
//! THE ID DOES NOT NAME AN OBJECT. It names a TRIPLE: the component is folded into it alongside the
//! kind and the key, so two pages of one object under different components carry DIFFERENT ids and
//! no page of a multi-component object shares an id with any other. `address_footprint` recorded
//! that as a note about a fixture ("a component is its own object rather than another block") and
//! `bucket_fill` recorded it as a property of the page handle. Neither drew the consequence, and the
//! consequence is what decides this:
//!
//!   * A reader that matches a page against a caller's kind AND key AND component could be served
//!     by the id. The caller holds all three and `ShardState` holds the shard, so the id is
//!     computable where the comparison happens. Five readers are of that shape.
//!   * A reader that is COMPONENT-BLIND cannot. `bucket_index_component_block_addresses` is the
//!     sharpest: it is the whole-object read path, it asks for EVERY COMPONENT of one key, and its
//!     caller holds a key and no component list. The ids it would have to match are not derivable
//!     from what it is given -- computing them needs the components, which are the answer it was
//!     called to produce. That is not a reader missing a parameter. It is a reader asking a question
//!     the id cannot express.
//!
//! AND THE STORED SPELLING IS THE KEY'S CHARACTERS. `block_index_written_key` renders the page
//! index's own map key through `block_ref_key_from_parts` as `kind:object_key:component:...`, and
//! that string is what a dump writes and what the replay log shares. `blocks_per_bucket` pinned the
//! COMPONENT's place in it -- found by mutation, because dropping a field from that key leaves every
//! key distinct (the addresses differ), so uniqueness, key counts and ordering all still pass. The
//! object key's place in it was pinned by nothing, and a mutation dropping it would have survived
//! for exactly the same reason. It is pinned here.
//!
//! WHAT IS NOT CLAIMED, because it is only a missing parameter. `release_bucket_blocks` compares a
//! resident page against one derived from the model maps through `released_block_identity`, whose
//! own doc excludes `object_id` because "the model map's copy of the same page may not carry it".
//! That exclusion is real but it is not a pin on the KEY: `derive_released_block_identities` already
//! takes the shard, so it could compute the id on the derived side and compare on that. The two
//! sites in that function that DO need the characters are its lookup probes -- `block_refs_for` and
//! `remove_object_block_lookup_entry` -- because `ObjectBlockLookup` is character-keyed, per object,
//! above the component. This distinction is drawn because the campaign has now three times found a
//! reader that "cannot be given what it needs" to merely lack an argument.
//!
//! AND THE FALLBACK SITES PIN NOTHING HERE. Every
//! `unwrap_or_else(|| stable_block_object_id(..))` reads its key from the model-map walk or from a
//! command parameter, never from a page entry -- so `reload_released_bucket` does not read this field
//! at all. Its one mention is a struct-literal WRITE, fed by
//! `collect_model_live_block_entries_in_bucket`: the model maps supply the characters and reload uses
//! them to recompute the id. The "needs no hash fallback to answer" precondition on `BucketNode` is
//! about the ROUTING BUCKET, not about this field.
//!
//! WHAT A HANDLE WOULD COST IS MEASURED ELSEWHERE AND STILL DECLINED.
//! `a_handle_for_the_object_key_pays_only_above_the_measured_break_even` prices a four-byte index
//! into a shared table at one entry per distinct object against sixteen bytes a page, and the
//! break-even is 3.38 pages per object: it PAYS on containers at 100 and LOSES on routed keys at 1
//! and on a mixed store at 1.98. A handle would satisfy every reader here -- keeping the characters
//! reachable is what a handle does -- so this module refutes REMOVING the field, not interning it.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::control::LoadShardRequest;
use crate::engine::hashing::stable_block_object_id;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// The range a production shard is loaded on: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const OPERATOR_END: u32 = 1023;

/// The kind every fixture here uses. Hash is the kind whose objects carry real components, which is
/// the population the whole module is about.
const FIXTURE_KIND: StoredModelKind = StoredModelKind::Hash;

/// Container keys and members. Every denominator below is derived from these rather than restated
/// as a literal at the assertion that checks it.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// Routed keys for the CONTROL arm, where an object holds exactly one page.
const ROUTED_KEYS: usize = 200;

// =================================================================================================
// FIXTURE. The same shapes `block_entry_handles` seeds, at the size these assertions need rather than
// the size a byte measurement needs.
// =================================================================================================

fn identity_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ))
}

fn load_operator_shard(engine: &TemporalEngine) {
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        response.status.ok,
        "shard must load over 0..={OPERATOR_END}: {:?}",
        response.status
    );
}

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// Hash containers only: one key, `MEMBERS_PER_KEY` fields, each field its own page and its own
/// component. The population where the id's granularity and the object's disagree.
fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            });
        }
    }
    ack_batch(engine, commands);
}

/// Keys holding exactly one page each: the CONTROL population, where the mechanism predicts no
/// effect at all.
fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Every live page in shard 1, as the ids filed under each object key. The denominator for every
/// row below.
fn ids_by_object_key(engine: &TemporalEngine) -> BTreeMap<String, Vec<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // THE KEY A PAGE IS FILED UNDER COMES FROM THE LOOKUP BESIDE IT. It is both the grouping
    // key below and the argument the id is derived from, so there is exactly one source for it
    // here where there used to be one source read twice off the page.
    let keys = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    let mut by_key: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted {
                continue;
            }
            let Some((_, named)) = keys.live_identity(bucket.routing_bucket, *handle) else {
                panic!(
                    "a live page at handle {handle} is named by nothing beside it, so this                      denominator would be short by one"
                )
            };
            by_key
                .entry(named.to_string())
                .or_default()
                .push(block.object_id(1, named));
        }
    }
    by_key
}

/// A free-standing entry at a FIXED address, whose object id is the one the engine would compute
/// for it. Taken from `stable_block_object_id` rather than invented, so a comparison between two of
/// these is about the field that differs and the id that follows from it.
fn entry_named(object_key: &str, component: Option<&str>) -> (Arc<str>, BlockIndex) {
    let object_id = stable_block_object_id(1, FIXTURE_KIND.as_str(), object_key);
    (
        Arc::from(object_key),
        BlockIndex {
            model_id: FIXTURE_KIND,
