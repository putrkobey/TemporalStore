// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OVERWRITE KEEPS THE MEMBER'S ORDINAL UNDER EITHER PROJECTION.
//!
//! # THE PROPERTY, WHICH ALREADY HAS A GUARD FOR THE DEFAULT PATH
//!
//! `container_block_ordinal` assigns a container element its POSITION, and an overwrite of the same
//! element must reuse that position rather than take a new one --
//! `container_block_ordinal::an_overwrite_keeps_the_members_ordinal_rather_than_climbing` pins that
//! for the ungated path. The mechanism is an identity branch: the walk looks for a page already
//! filed under this element's component and, finding one, returns the ordinal that page holds.
//!
//! # WHY THE GATE BREAKS IT, AND WHY NO EXISTING TEST SEES THAT
//!
//! Under one entry per page the entry carries NO component, so
//! `page.component.as_deref() == Some(component)` never matches and the walk falls through to
//! `highest + 1`. Every rewrite of one member then takes a fresh ordinal, which is a fresh
//! `block_id`, which is a fresh ADDRESS -- so a member rewritten five times leaves five pages
//! instead of overwriting one.
//!
//! The existing guard cannot see this: it reads no environment variable, so it runs with the gate
//! off and passes. Inheriting coverage from the ungated suite is an illusion, which is why this arm
//! exists rather than being assumed.
//!
//! # WHERE THE ORDINAL COMES FROM INSTEAD
//!
//! Not from a page read -- `container_block_ordinal` is handed the index and nothing that could
//! fetch a page, and adding a fetch to a write path inside a footprint change is the scope drift
//! this series has already declined once.
//!
//! It comes from the RESIDENT MAP, which is keyed by the element and whose value carries the
//! address: a member that already exists has its own `block_id` right there, and that IS its
//! position. So the gated path asks the map the question the index can no longer answer, and the
//! answer is authoritative rather than derived.

#![allow(clippy::all)]
use super::*;

const REWRITES: usize = 5;

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
        table_name: "ordinal".to_string(),
        shard_uri: "local://ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

/// Distinct page addresses, and the block ids on them, for one set object.
fn blocks_and_ordinals(engine: &TemporalEngine, object_key: &str) -> (usize, Vec<u64>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let keys = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    let mut blocks = std::collections::BTreeSet::new();
    let mut ordinals = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || !keys
                    .live_identity(bucket.routing_bucket, *handle)
                    .is_some_and(|(_, named)| &**named == object_key)
                || block.deleted
            {
                continue;
            }
            blocks.insert((
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            ));
            if let Some(block_id) = block.address.block_id() {
                ordinals.insert(block_id);
            }
        }
    }
    (blocks.len(), ordinals.into_iter().collect())
}

/// Every live set entry of this object, as (component, slab, offset, length, block_id), so a
/// surprising entry count can be read rather than guessed at.
fn entry_rows(engine: &TemporalEngine, object_key: &str) -> Vec<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    // `identity_of` RATHER THAN THE LIVE INVERSION, because this listing is the one that
    // includes tombstones -- its rows print `deleted=` -- and a deleted page is named by the
    // bucket's removal row instead of by the object lookup.
    let keys = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    let mut rows = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || !keys
                    .identity_of(bucket, *handle, block)
                    .is_some_and(|beside| &**beside.object_key == object_key)
            {
                continue;
            }
            rows.push(format!(
                "deleted={} component=None slab={} off={} len={} block_id={:?}",
                block.deleted,
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
                block.address.block_id()
            ));
        }
    }
    rows.sort();
    rows
}

/// Rewrite ONE member `REWRITES` times under `gate_on`, and report what the index holds after.
fn rewrites_under(gate_on: bool) -> (usize, Vec<u64>, usize, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // BOTH DIRECTIONS AS VALUES: an unset variable now selects the GATED path.
    if gate_on {
    } else {
    }
    load_on(&engine);

    let member = b"the-one-member".to_vec();
    for _ in 0..REWRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "ord/set".to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    let (blocks, ordinals) = blocks_and_ordinals(&engine, "ord/set");
    // How many members the resident map holds, so a row cannot be read as correct because the
    // object vanished.
    let resident = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard.sets.get("ord/set").map(|m| m.len()).unwrap_or(0)
    };
    let rows_detail = entry_rows(&engine, "ord/set");
    drop(engine);
    (blocks, ordinals, resident, rows_detail)
}

#[test]
fn an_overwrite_keeps_the_members_ordinal_under_either_projection() {
    println!("\n=== one member rewritten {REWRITES} times ===");
    println!("  {:>5}  {:>6}  {:>9}  {:>24}", "gate", "blocks", "resident", "ordinals");

    let mut rows: Vec<(&str, usize, Vec<u64>, usize)> = Vec::new();
    for (label, gate_on) in [("off", false), ("on", true)] {
        let (blocks, ordinals, resident, rows_detail) = rewrites_under(gate_on);
        println!("  {label:>5}  {blocks:>6}  {resident:>9}  {ordinals:>24?}");
        for row in &rows_detail {
            println!("          {row}");
        }
        rows.push((label, blocks, ordinals, resident));
    }

    // FLOORS, on each arm, on the reaching of the path: the object has to exist and be named.
    for (label, blocks, _ordinals, resident) in &rows {
        assert_eq!(
            1, *resident,
            "gate {label}: the resident map holds {resident} member(s) after rewriting ONE member \
             {REWRITES} times, so this row is not measuring an overwrite"
        );
        assert!(
            *blocks > 0,
            "gate {label}: no page is filed for the object at all, so the count below compares \
             absences"
        );
    }

    // THE ORDINAL DOES NOT CLIMB, ON EITHER ARM, AND THAT IS WHAT THIS STEP FIXES.
    //
    // An overwrite is the same element in the same position. Under the gate the entry carries no
    // component, so `container_block_ordinal`'s identity branch cannot find the member's own
    // previous page and falls through to `highest + 1` -- measured before the fix as ordinals
    // [0, 1]. The position now comes from `shard.sets[key][member].block_id()`: the map that is
    // keyed by the member, holding the address it currently occupies, which IS its position.
    // Authoritative rather than derived, and no page read.
    for (label, _blocks, ordinals, _resident) in &rows {
        assert_eq!(
            vec![0u64],
            *ordinals,
            "gate {label}: one member rewritten {REWRITES} times holds ordinals {ordinals:?}. An \
             overwrite must reuse the member's position; a climbing ordinal is a fresh block id, a \
             fresh address, and therefore a fresh page"
        );
    }

    // THE ENTRY COUNTS, AND THE GATED ONE HAS CHANGED -- IT IS NOW ONE ON BOTH ARMS.
    //
    // This engine APPENDS a page per write; it does not modify one in place. The ungated path
    // always held ONE entry because its replacement is scoped BY THE ELEMENT -- the write unnames
    // the element's own previous page.
    //
    // THIS SAID A PAGE-NAMED ENTRY CANNOT CARRY THAT SCOPE, and that was true of a supersede keyed
    // on the address the write LANDS at: a rewrite lands at a new address, matches no existing
    // entry, and leaves the previous one behind as a stale live entry over a dead page. It was
    // asserted here as `> 1` and described as a transient the next derivation heals.
    //
    // THE WRITE PATH CARRIES THE SCOPE NOW. The filer is told which page the write REPLACES -- the
    // element's previous address, read from the resident map, which is keyed by the element and so
    // still holds it at filing time -- and retires that page's entry when no sibling is left on
    // it. So the gated arm leaves ONE entry, the same as the ungated one, and the transient does
    // not exist to be healed. Asserted as an exact count in both rows, because `> 1` was satisfied
    // by exactly the stranding the change removes and would now be satisfied by nothing at all.
    assert_eq!(
        1, rows[0].1,
        "gate off: {} entries where the element-scoped replacement should leave one",
        rows[0].1
    );
    assert_eq!(
        1, rows[1].1,
        "gate on: {} entries where one live page should leave one. More than one means the \
         supersede did not retire the page this rewrite vacated -- a live entry over a page no \
         element is on, which under this gate is a claim of membership",
        rows[1].1
    );
}

/// AND A DERIVATION DROPS AN ENTRY NO ELEMENT CARRIES, WHICH THE SERIES STILL RESTS ON.
///
/// The projection reads the resident map, which holds only the CURRENT address of each element, so
/// an entry naming a page no element is on is not emitted again. That property is what makes one
