// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// DOES THE FOLD ROUTE LEAVE THE DURABLE HASH MAP NAMING EVERY FIELD THE PAGE INDEX NAMES?
// =================================================================================================

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "fold-hash-completeness".to_string(),
        shard_uri: "local://fold-hash-completeness/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
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

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// What the SERVED path answers for this key, which is the vacuity floor for both tests.
fn served_hash_fields(engine: &TemporalEngine, key: &str) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll { key: key.to_string() },
        })
        .response
    {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    }
}

/// The two sources, read the way each is read in production.
///
/// THE SECOND SOURCE IS RE-ATTRIBUTED, AND THAT IS THIS MODULE'S WHOLE EDIT. It was
/// `bucket_index_component_block_addresses`, which recovered each field's name from the page
/// entry's element-name field. `BlockIndex` has no such field now, so that walk answers for NO
/// fields and both arms failed on their own vacuity floors -- "the page index names 0 field(s) ...
/// so the comparison below would be vacuous". The floors were right to fire: with one side empty
/// the set equality would have passed on nothing.
///
/// THE NAMES DID NOT LEAVE STORAGE, THEY MOVED. A container page SPELLS its elements in its
/// payload -- that is what makes several of them foldable into one page at all -- so the pages
/// themselves are the second source, derived with the engine's own
/// `container_membership::derive_membership`, which is what `folded_block_membership` reads.
///
/// AND THE PAIR IS STRONGER THAN THE ONE IT REPLACES, which is worth saying because a
/// re-attribution usually is not. The entry's element name and the page's payload were written by
/// the SAME append, so their agreement said little about either. The durable map is maintained by
/// the command path and the pages by the append path, so these two can genuinely disagree -- which
/// is the property that makes this module's claim, that the map is complete, worth asserting at
/// all. The module header's "prove the map is complete FIRST, because the removal is what makes
/// the map load-bearing" is now the past tense: the removal has landed and the map is load-bearing.
///
/// COMPLETENESS IS ENFORCED HERE RATHER THAN RETURNED, so neither arm can compare a derivation that
/// fell short. An incomplete derivation that happened to agree would agree by luck.
fn both_sources(
    engine: &TemporalEngine,
    key: &str,
) -> (std::collections::BTreeSet<String>, std::collections::BTreeSet<String>) {
    let (durable, addresses) = {
        let shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let durable: std::collections::BTreeSet<String> = shard
            .hashes
            .get(key)
            .map(|fields| fields.keys().map(|name| name.to_string()).collect())
            .unwrap_or_default();
        // TOMBSTONE ENTRIES INCLUDED, no `page.deleted` skip. A tombstone is how a page STATES a
        // removal, so a walk that filtered them would read the page set as it stood before the
        // removal and would derive the removed field as live.
        let named = super::BlockObjects::of(&shard.bucket_index);
        let mut addresses: Vec<crate::block_store::ElementEntry> = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for (handle, block) in &bucket.block_index {
                if block.model_id.as_str() == "hash"
                    && &**named.at(bucket.routing_bucket, *handle) == key
                {
                    addresses.push(block.address.clone());
                }
            }
        }
        (durable, addresses)
    };
    assert!(
        !addresses.is_empty(),
        "DENOMINATOR: no page entries at all for {key}, so the derived side below would be empty \
         for a reason that has nothing to do with what the fold restored"
    );
    let derived = crate::engine::container_membership::derive_membership("hash", addresses, |address| {
        engine.block_store.read(address).ok()
    });
    assert!(
        derived.is_complete(),
        "the derivation from the pages is incomplete ({} failure(s)), so its agreement with the \
         durable map would be luck: {} read, {} undecodable, {} unframed, {} unrenderable",
        derived.failures(),
        derived.read_failures,
        derived.undecodable,
