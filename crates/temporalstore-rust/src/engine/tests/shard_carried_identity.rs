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
            "hash write {index} on shard {shard_id}: {:?}",
            response.status
        );
    }
}

/// A loaded, seeded engine holding every shard in [`SHARD_IDS`].
fn multi_shard_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = engine_on(dir);
    for shard_id in SHARD_IDS {
        load(&engine, shard_id);
        seed(&engine, shard_id);
    }
    engine
}

/// THE PREDICATE UNDER TEST, named once so the control below can exercise the SAME code the verdict
/// rests on. A state carries `routed_under` when it answers `Some` and answers that exact id.
fn carries(state: &crate::engine::state::ShardState, routed_under: crate::types::ShardId) -> bool {
    state.shard_id() == Some(routed_under)
}

// =================================================================================================
// 1. THE STAMP EQUALS THE KEY THE SERVED MAP ROUTES BY
// =================================================================================================

/// EVERY SHARD THE ENGINE SERVES CARRIES THE ID ITS OWNER ROUTES IT UNDER.
///
/// The engine's shard map IS the routing authority: `self.shards` is keyed by `ShardId` and every
/// read, write and report reaches a shard by looking it up under that key. So the property is an
/// equality between the key and the stamp, over every entry the map holds, on a fixture holding
/// five shards at once.
///
/// THE PREDICATE'S OWN CONTROL RUNS FIRST. A check that cannot fail is indistinguishable from one
/// that passes, and this one compares two numbers that an installer bug would make equal by
/// accident, so `carries` is driven on a right answer, on a wrong-shard answer and on an unstamped
/// state before it is pointed at the engine.
#[test]
fn every_served_shard_carries_the_id_its_owner_routes_it_under() {
    // --- THE CONTROL, before any verdict. ---
    let mut planted = crate::engine::state::ShardState::default();
    assert!(
        !carries(&planted, 7),
        "`carries` reports an UNSTAMPED state as carrying shard 7, so it cannot distinguish a \
         stamped shard from one that never entered the engine and every verdict below is empty"
    );
    planted.set_shard_id(7);
    assert!(
        carries(&planted, 7),
        "`carries` does not report a state stamped with 7 as carrying 7, so it reports nothing"
    );
    assert!(
        !carries(&planted, 41),
        "`carries` reports a state stamped with 7 as carrying 41 -- it is not comparing the id at \
         all, and a shard holding the WRONG shard's id would pass the loop below"
    );
    assert!(
        !carries(&planted, 0),
        "`carries` reports a state stamped with 7 as carrying 0; a stamp stuck at zero is the \
         first mutation this guard exists to kill and it would survive"
    );

    // --- THE TREE. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = multi_shard_engine(dir.path());
    let shards = engine.shards.read().expect("engine lock poisoned");

    let observed: Vec<crate::types::ShardId> = shards.keys().copied().collect();
    let distinct: BTreeSet<crate::types::ShardId> = observed.iter().copied().collect();

    // --- VACUITY FLOORS, BEFORE THE VERDICT. An empty map satisfies a for-loop. ---
    assert_eq!(
        SHARD_IDS.len(),
        shards.len(),
        "the fixture loaded {} shards and the served map holds {}; the loop below would be \
         checking a different set from the one this module reasons about",
        SHARD_IDS.len(),
        shards.len()
    );
    assert!(
        shards.len() >= 5,
        "the served map holds {} shard(s); a single-shard fixture cannot tell an id from a \
         constant and this whole module would pass on a stamp that ignores its argument",
        shards.len()
    );
    assert_eq!(
        shards.len(),
        distinct.len(),
        "the served map holds {} shards under {} distinct ids, so at least two entries share a \
         key and the equality below is not per-shard",
        shards.len(),
        distinct.len()
    );
    assert!(
        distinct.iter().filter(|id| **id != 0).count() >= 4,
        "only {} served shard(s) have a non-zero id; a stamp stuck at 0 needs at least two \
         non-zero shards to be caught and this fixture would let it through",
        distinct.iter().filter(|id| **id != 0).count()
    );
    assert!(
        distinct.contains(&0),
        "shard 0 is not in the served map; the fixture no longer proves that a REAL zero id is \
         distinguishable from an unstamped state"
    );

    let mut verdict_lines = Vec::new();
    let mut unstamped = 0usize;
    let mut mismatched = Vec::new();
    for (routed_under, state) in shards.iter() {
        let carried = state.shard_id();
        verdict_lines.push(format!(
            "    routed under {routed_under:>5}  carries {carried:?}"
        ));
        if carried.is_none() {
            unstamped += 1;
        }
        if !carries(state, *routed_under) {
            mismatched.push(format!("{routed_under} carries {carried:?}"));
        }
    }
    println!(
        "  {} served shards, {} distinct ids, ids {:?}",
        shards.len(),
        distinct.len(),
        distinct
    );
    for line in &verdict_lines {
        println!("{line}");
    }
    assert_eq!(
        0, unstamped,
        "{unstamped} of {} served shards entered the map without a shard id stamped. There is one \
         function that installs a ShardState (`install_shard_state`) and it stamps; if another way \
         in has appeared, route it through that one.",
        shards.len()
    );
    assert!(
        mismatched.is_empty(),
        "{} of {} served shards carry an id that is NOT the key the served map routes them under: \
         {mismatched:?}. A wrong id here is worse than none: \
         `stable_block_object_id(shard, kind, key)` would return a well-formed id belonging to a \
         different shard's object.",
        mismatched.len(),
        shards.len()
    );
}

// =================================================================================================
// 2. THE STAMP DERIVES IDS THE WRITE PATH ALREADY STORED
// =================================================================================================

/// THE ID A SERVED SHARD CARRIES DERIVES THE OBJECT ID ALREADY STORED ON ITS OWN PAGES.
///
/// The check in part 1 compares the stamp to the key it was inserted under, which an installer that
/// stamps the wrong thing could still satisfy if it derived both from one argument. This one cannot
/// be satisfied that way. Every `object_id` on a live page entry was computed by the WRITE PATH from
/// the shard the engine was routing that write to, before this field existed and without consulting
/// it. Re-deriving them from the shard's own stamp is therefore an equality between the new field
/// and the engine's pre-existing routing behaviour, page by page, on five shards at once.
///
/// It is also the payoff stated as a fact: this is `stable_block_object_id(shard, kind, key)`
/// computed from a `&ShardState` and NOTHING ELSE, which is precisely what the four functions named
/// in this module's header could not do.
#[test]
fn the_id_a_served_shard_carries_derives_the_object_id_already_stored_on_its_blocks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = multi_shard_engine(dir.path());
    let shards = engine.shards.read().expect("engine lock poisoned");

    let mut checked = 0usize;
    let mut without_stored_id = 0usize;
    let mut shards_with_blocks = 0usize;
    let mut differed = Vec::new();

    for (routed_under, state) in shards.iter() {
        // THE ONLY INPUT. Not the map key -- the shard's own answer, which is the reachability
        // this module is about.
        let carried = state
            .shard_id()
            .expect("a served shard carries its id; part 1 of this module holds that");
        let named = super::BlockObjects::of(&state.bucket_index);
        let mut blocks_here = 0usize;
        for bucket in state.bucket_index.bucket_map.values() {
            for (handle, block) in bucket.block_index.iter() {
                let object_key = named.at(bucket.routing_bucket, *handle);
                // THE INDEPENDENT SIDE IS NOW THE WRITE PATH'S OWN COPY, and it has to be: the
                // address used to hold one and does not any more. `bucket.object_index` was filled
                // by the write path from the id it ACTUALLY USED, before this field existed and
                // without consulting it -- so comparing a fresh derivation against that membership
                // is still an equality between two answers rather than between one and itself.
                let derived = crate::engine::hashing::stable_block_object_id(
                    carried,
                    block.model_id.as_str(),
                    object_key,
                );
                blocks_here += 1;
                checked += 1;
                if !bucket.object_index.contains(&derived) {
                    without_stored_id += 1;
                    differed.push(format!(
                        "shard {routed_under} kind {} key {} derived {derived}, which the bucket's \
                         own object index does not hold",
                        block.model_id.as_str(),
                        object_key
                    ));
                }
            }
        }
        if blocks_here > 0 {
            shards_with_blocks += 1;
        }
    }

    println!(
        "  derived from the shard's own id on {checked} live page entries across \
         {shards_with_blocks} shards ({without_stored_id} not held by their bucket's object index), \
         {} differed",
        differed.len()
    );

    // --- DENOMINATORS. A zero-entry walk satisfies the emptiness assertion below. ---
    assert!(
        checked >= 100,
        "the walk compared {checked} page entries; the fixture writes {} records of two kinds on \
         {} shards, so below 100 it is not reading the page index and `differed` is empty for the \
         wrong reason",
        RECORDS,
        SHARD_IDS.len()
    );
    assert_eq!(
        SHARD_IDS.len(),
        shards_with_blocks,
        "only {shards_with_blocks} of {} shards contributed a page entry, so the derivation was \
         never exercised on the other shards' ids -- which is where a wrong id shows up",
        SHARD_IDS.len()
    );
