// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SHARD A SERVED STATE IS SERVED UNDER, AND WHY IT HAD TO BE THE ROUTING KEY ITSELF.
//!
//! A page's object id is `stable_block_object_id(shard, kind, key)`. `BlockIndex` carries the kind
//! and the key beside the address, so of the three terms only the FIRST was out of reach of a
//! `&ShardState` -- and four functions that take a shard and never `&self` were stopped by exactly
//! that:
//!
//! ```text
//!   BlockIndex::object_id                       engine/engine_storage_state.rs
//!   object_manager::runtime_report              engine/object_manager.rs
//!   settle_released_bucket_object_delete        engine/storage_bucket_internals.rs
//!   bucket_generation_fingerprints_by_bucket    engine/storage_reporting.rs
//! ```
//!
//! The third one had already written the finding down in its own body -- "recomputing one needs a
//! shard id this path does not carry" -- and skipped the id it could not rebuild. NONE OF THE FOUR
//! IS CHANGED HERE. This module establishes the precondition and nothing else: the term is now
//! reachable from a served shard, and it is the RIGHT term.
//!
//! # WHY A PRESENT-BUT-WRONG ID WOULD BE WORSE THAN THE ABSENCE
//!
//! `stable_block_object_id` is a hash. Handed a zero, or handed the neighbouring shard's id, it
//! returns a perfectly well-formed `u64` that belongs to a different object, and no caller can tell
//! it from the right one -- the absence at least announces itself. So the field is not asserted to
//! EXIST here; it is asserted to EQUAL the id its owner routes it under, and the two
//! `ShardState::shard_id` answers are `Some(id)` or `None`, never a guess.
//!
//! # TWO INDEPENDENT AUTHORITIES, BECAUSE ONE WOULD BE A TAUTOLOGY
//!
//! `install_shard_state` inserts the state into `self.shards` under a key and stamps it from the
//! same argument, so comparing the stamp to the key is nearly -- but not quite -- circular: it
//! still catches an installer that stamps something it derived rather than what it inserted under.
//! The second authority is not circular at all. Every object id ALREADY STORED on a live page entry
//! was computed by the write path from the shard id the engine was routing that write to, long
//! before this field existed. Re-deriving those ids from the shard's OWN stamp and comparing them
//! is a check the stamp cannot pass by being self-consistent:
//! `the_id_a_served_shard_carries_derives_the_object_id_already_stored_on_its_blocks`.
//!
//! Both run over a MULTI-SHARD engine, on deliberately non-contiguous ids, and both print their
//! denominator. Shard 0 is in the fixture on purpose and so are four shards that are not 0: a stamp
//! stuck at zero satisfies the first and fails the other four, which is the mutation this module
//! was written to kill.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// THE FIXTURE'S SHARDS. Non-contiguous, so a stamp that is really the loop index, the count, or
/// the position in the map is a different number from the id at every shard but the first.
///
/// 0 is present because 0 IS a real shard id -- that is the whole reason the carried id needs a
/// flag beside it rather than a sentinel -- and because a stamp stuck at zero has to be caught by
/// the shards that are not zero rather than by luck.
const SHARD_IDS: [crate::types::ShardId; 5] = [0, 1, 7, 41, 1000];

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;

/// Records per shard. Enough page entries for the derivation floor below to mean something.
const RECORDS: usize = 24;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load(engine: &TemporalEngine, shard_id: crate::types::ShardId) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id,
        table_name: "shard-carried-identity".to_string(),
        shard_uri: format!("local://shard-carried-identity/{shard_id}"),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard {shard_id}: {:?}",
        response.status
    );
}

/// Writes that produce page entries of two kinds under distinct object keys on one shard.
fn seed(engine: &TemporalEngine, shard_id: crate::types::ShardId) {
    for index in 0..RECORDS {
        let key = format!("ident-{shard_id}-{index:04}");
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 48],
            },
        });
        assert!(
            response.status.ok,
            "string write {index} on shard {shard_id}: {:?}",
            response.status
        );
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::HashSet {
                key: format!("h-{key}"),
                field: "f".to_string(),
                value: vec![b'h'; 48],
            },
        });
        assert!(
            response.status.ok,
