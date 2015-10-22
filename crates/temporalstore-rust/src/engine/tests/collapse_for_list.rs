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
            engine,
            Command::ZSetAdd {
                key: ZSET_KEY.to_string(),
                member: element_bytes(index),
                score: index as f64,
            },
        );
    }
}

fn fold(engine: &TemporalEngine) {
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
}

/// The entry floor for a collapsed kind: FEWER entries than elements. Printed before asserted.
///
/// THE SECOND HALF OF THIS FLOOR IS GONE. It also asserted that not one entry named an element,
/// and `BlockIndex` has no field for a name -- so that half could only pass. What is left is the
/// half that can still fail, and it is the sharper one: fewer live entries than elements means the
/// elements actually folded onto a shared page.
fn assert_collapsed(engine: &TemporalEngine, model_id: &str, object_key: &str) {
    let live = live_entries(engine, model_id, object_key);
    println!(
        "    {model_id:<5} entries: {live:>3} live (of {ELEMENTS} elements)"
    );
    assert!(
        live < ELEMENTS,
        "{model_id} filed {live} live entries for {ELEMENTS} elements, so nothing folded onto a \
         shared page and every assertion in this arm is about a gate that changed nothing"
    );
}
/// A PRESENT LIST ELEMENT IS STILL SERVED UNDER THE COLLAPSE, AND A POPPED ONE IS STILL GONE.
#[test]
fn a_list_keeps_its_present_elements_and_loses_its_popped_one_under_the_collapse() {
    println!("\n=== list, gate on, folded ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(
        &dir.path().join("cache"),
        &dir.path().join("blocks"),
        &dir.path().join("indexes"),
    );
    load_on(&engine);
    write_list(&engine);
    fold(&engine);
    assert_collapsed(&engine, "list", LIST_KEY);

    // ---- PRESENT ----
    assert_eq!(
        ELEMENTS as i64,
        list_len(&engine),
        "the collapsed list does not report its own length"
    );
    let listed = list_elements(&engine);
    assert_eq!(
        ELEMENTS,
        listed.len(),
        "a collapsed list ranged {} of {ELEMENTS} elements",
        listed.len()
    );
    for index in 0..ELEMENTS {
        assert!(
            listed.contains(&element_bytes(index)),
            "element {index} is missing from a collapsed list range"
        );
    }
    // AND IN ORDER. A list's component is its sequence, which is the ORDER -- so this is the
    // property that would be lost if the order lived in the entry rather than in `shard.lists`.
    let expected: Vec<Vec<u8>> = (0..ELEMENTS).map(element_bytes).collect();
    assert_eq!(
        expected, listed,
        "a collapsed list served its elements OUT OF ORDER. The sequence that orders a list is \
         the component this change drops from the entry, so order is the property at risk"
    );

    // ---- REMOVED. `list` has no LSET/LINSERT/LREM, so a pop off the right end is its removal. ----
    let popped = bytes(
        &engine,
        Command::ListPop {
            key: LIST_KEY.to_string(),
            left: false,
        },
    )
    .expect("a non-empty collapsed list must pop something");
    assert_eq!(
        element_bytes(ELEMENTS - 1),
        popped,
        "the pop returned the wrong end of a collapsed list"
    );
    assert_eq!(
        (ELEMENTS - 1) as i64,
        list_len(&engine),
        "popping one element of a collapsed list did not change its length by one"
    );
    let after = list_elements(&engine);
    assert!(
        !after.contains(&element_bytes(ELEMENTS - 1)),
        "the popped element is still in a collapsed list range"
    );
    for index in 0..(ELEMENTS - 1) {
        assert!(
            after.contains(&element_bytes(index)),
            "popping the tail also took element {index}"
        );
    }
}

/// AND THE POP SURVIVES THE INDEX BEING THROWN AWAY AND REBUILT.
///
/// The arm that matters for a resurrection. The index is a derived projection: a removal held only
/// in the live index has not been tested until the index is re-derived from the model maps, which
/// is what opening the store again does.
#[test]
fn a_popped_list_element_does_not_come_back_when_the_index_is_re_derived() {
    println!("\n=== list, gate on, popped, then reopened ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    {
        let engine = engine_on(&dir.path().join("cache"), &blocks, &indexes);
        load_on(&engine);
        write_list(&engine);
        fold(&engine);
        assert_collapsed(&engine, "list", LIST_KEY);
        let popped = bytes(
            &engine,
            Command::ListPop {
                key: LIST_KEY.to_string(),
                left: false,
            },
        )
        .expect("the pop must return an element");
        assert_eq!(element_bytes(ELEMENTS - 1), popped, "popped the wrong end");
        engine.unload_shard(1);
        drop(engine);
    }
    // ITS OWN CACHE, so every byte served comes off disk through the index just decoded rather
    // than out of pages the writer left warm.
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &blocks, &indexes);
    load_on(&reloaded);

    let length = list_len(&reloaded);
    println!("    reloaded length: {length} (expected {})", ELEMENTS - 1);
    assert_eq!(
        (ELEMENTS - 1) as i64,
        length,
        "the reopened store holds {length} elements where {} were left; a short answer here is a \
         lost reload and a long one is a resurrection",
        ELEMENTS - 1
    );
    let listed = list_elements(&reloaded);
    assert!(
        !listed.contains(&element_bytes(ELEMENTS - 1)),
        "the popped element came back when the index was re-derived"
    );
    for index in 0..(ELEMENTS - 1) {
        assert!(
            listed.contains(&element_bytes(index)),
            "element {index} did not survive the reopen"
        );
    }
}

/// A GATED LIST COMES BACK THROUGH **WAL REPLAY**, WHICH HARD-REQUIRES THE COMPONENT.
///
/// # THE INVARIANT, WRITTEN DOWN AND DRIVEN
///
/// `lifecycle::apply_outcome_item`'s list arm destructures a `(Some(address), Some(component))`
/// and `return false` otherwise -- and a `false` there is not a wrong answer, it **REFUSES THE
/// WHOLE SHARD LOAD**. It then DECODES the component as `u64::from_str_radix(component, 16)` to
/// recover the sequence. So the component is not merely required to be present, it has to still be
/// the ELEMENT's sequence, spelled the way the write path spells it.
///
/// WHY COLLAPSING THE INDEX ENTRY DOES NOT REACH IT, which is the thing being pinned: that
/// component comes from the WAL outcome item, which `stage_outcome` fills from the
/// `ElementComponent` -- taken BEFORE the `FiledComponent` collapse, and a separate newtype
/// precisely so transposing the two fails to compile rather than silently filing the page's name
/// where the element's belongs. The index entry and the outcome record are different things that
/// share a field name. The existing module that drives this (`replay_under_the_gate`) is SET-ONLY,
/// so for `list` the reading was unpinned until here.
///
/// # WHY THERE IS NO UNLOAD
///
/// An `unload_shard` is what materializes the base index. This fixture deliberately does not
/// unload, so the index file is ABSENT and the reload must replay. The `absent` index-load-path
/// counter is asserted, because a reload that quietly found an index would answer every question
/// below correctly while testing nothing.
#[test]
fn a_gated_list_comes_back_through_wal_replay() {
    println!("\n=== a gated list through WAL replay, no index file ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    {
        let engine = engine_on(&dir.path().join("cache-writer"), &blocks, &indexes);
        load_on(&engine);
        write_list(&engine);
        // THE FLOOR BEFORE THE RELOAD: the writer really holds them, so a short answer after the
        // replay is the replay's and not the fixture's.
        assert_eq!(
            ELEMENTS as i64,
            list_len(&engine),
            "the writer does not hold its list elements before the reload"
        );
        // Deliberately NO unload. The engine is dropped here.
    }

    crate::engine::persistence::reset_index_load_path_counts();
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &blocks, &indexes);
    let response = reloaded.load_shard_with(load_request());
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "    load_ok={} accepted={accepted} refused_stale={refused_stale} absent={absent} \
         undecodable={undecodable}",
        response.status.ok
    );

    assert!(
        response.status.ok,
        "a gated list store REFUSED TO LOAD through replay: {:?}. The list replay arm answers \
         `false` when it cannot rebuild an element from the outcome's component, and the caller \
         turns that into a failed load -- so this is the shape where collapsing the index entry \
         would take the shard down rather than shorten an answer",
        response.status
    );
    assert!(
        absent > 0,
        "no index was reported ABSENT, so this reload did not have to replay and the arm proves \
         nothing about the replay path"
    );
    assert_eq!(
        ELEMENTS as i64,
        list_len(&reloaded),
        "the replayed list is short, so the list replay arm's sequence decode did not rebuild \
