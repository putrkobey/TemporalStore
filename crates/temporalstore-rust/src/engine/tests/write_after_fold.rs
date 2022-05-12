// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE AFTER A DERIVATION, WHICH IS THE ONLY THING THAT CAN SEE TWO FILERS DISAGREE.
//!
//! # WHY THIS SHAPE AND NOT A ROUND TRIP
//!
//! Two different pieces of code file a container's index entries: the PROJECTION
//! (`visit_model_live_blocks`, which a load and a compaction re-derive the whole index from) and
//! the WRITE PATH (`install_element` -> `upsert_bucket_index_block`, which files one entry as each
//! element is written). Under the one-entry-a-page gate the projection files one entry per PHYSICAL
//! PAGE carrying no component; the write path files one entry per ELEMENT carrying one.
//!
//! A test that writes, then reloads, cannot see that. A reload re-derives, so it reads the index
//! back the way the projection wrote it and the write path's filing is simply overwritten --
//! which is exactly why the store-boundary corpus test in this crate passes while the two routes
//! disagree, and why five landed steps that only RE-DERIVED could not find it. It takes a WRITE
//! AFTER A DERIVATION: fold the pages so the projection files one entry, then write again so the
//! write path files beside it, and compare.
//!
//! # THE THREE THINGS ASSERTED, AND WHY COUNTS ALONE WOULD NOT DO
//!
//!   1. ONE ENTRY PER DISTINCT PHYSICAL PAGE. Not "N entries", which means the collapse did not
//!      reach the write path; and the count is checked against the number of distinct page
//!      ADDRESSES rather than against a constant, so a fixture that happens to fold differently
//!      cannot make this vacuous.
//!
//!      THIS USED TO READ "and every entry carrying no component", asserted as a `named` counter
//!      beside each entry count. `BlockIndex` has no component field, so that half is held by the
//!      TYPE now and the counters are gone -- see `entry_census` for what enforces it instead.
//!      Each assertion that paired the two has been narrowed to the count, because pairing a live
//!      number with a structural constant hides which half failed.
//!   2. EVERY ELEMENT READS BACK, BY MEMBERSHIP AND NOT BY COUNT. This is the arm that catches the
//!      dangerous repair: filing one entry per element with NO component makes every element of one
//!      folded page share `(kind, object_key, component, address)`, so they collapse onto one
//!      handle and each write displaces the last. A container answering the right NUMBER of the
//!      wrong elements must fail here, so the members are compared as a set.
//!   3. THE WRITTEN KEY MATCHES WHAT A DERIVATION PRODUCES for the same page. `block_ref_key` is
//!      stored inside the lookup refs, so two routes that spell it differently file a page under a
//!      handle the other route will never compute -- a durably acknowledged write that reads
//!      MISSING. This is the arm the two `part4` rebuild-equivalence tests fail on.
//!
//! # EVERY FLOOR IS ON REACHING THE PATH
//!
//! `count == 1` is satisfied by an index that filed nothing at all, and `0 <= 0` has already passed
//! once in this campaign as "no regression" over a path nothing reached. So each arm first
//! establishes that the fixture actually folded, that the object actually has entries, and that the
//! durable map actually holds the elements -- and only then compares.

#![allow(clippy::all)]
use super::*;

const FIRST_BATCH: usize = 4;
const VALUE_WIDTH: usize = 24;
const KEY: &str = "waf/set";

/// Holds the gate at one value and puts back whatever was there -- on a normal drop AND while
/// unwinding, so a failing arm cannot leak it into every later test in the process.



fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("w-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

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
        table_name: "waf".to_string(),
        shard_uri: "local://waf/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn add(engine: &TemporalEngine, member: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetAdd {
            key: KEY.to_string(),
            member,
        },
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// This object's live entries: how many, the distinct physical pages
/// they resolve to, and the written key of each.
fn entry_census(engine: &TemporalEngine) -> (usize, std::collections::BTreeSet<(u64, u64, u64)>, Vec<String>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    // THE `named` TERM IS GONE FROM THIS CENSUS, AND THE CLAIM IT CARRIED IS NOW STRUCTURAL.
    //
    // It counted entries whose `component` was `Some`, and the arms below asserted it was zero --
    // "no live entry of any kind is filed under an element name". `BlockIndex` has no `component`
    // field, so the count could only ever be zero and an assertion on it could only ever pass:
    // nothing it observes could disagree, which is the definition of the vacuity this campaign
    // keeps deleting rather than leaving as apparent coverage.
    //
    // WHAT ENFORCES THE CLAIM INSTEAD, so it is not merely dropped: `engine_storage_state.rs` pins
    // `size_of::<BlockIndex>() == 40` with `!= 39 && != 41` beside it AND asserts the field sum
    // equals the width -- zero slack. Re-adding a name to the entry costs eight bytes at minimum
    // and sixteen for an `Option<Arc<str>>`, so it fails const-evaluation at the pin rather than
    // being caught by a counter here. The `written` keys this census still collects are the
    // second guard: `block_index_written_key` renders the stored spelling, and a name on an entry
    // would show up in it.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut blocks = std::collections::BTreeSet::new();
    let mut written = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            let object_key = named.at(bucket.routing_bucket, *handle);
            if block.deleted || block.model_id.as_str() != "set" || &**object_key != KEY {
                continue;
            }
            live += 1;
            blocks.insert((
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            ));
            written.push(crate::engine::state::block_index_written_key(object_key, block));
        }
    }
    written.sort();
    (live, blocks, written)
}

/// What the durable model map holds -- the authority for existence, independent of any entry.
fn durable_members(engine: &TemporalEngine) -> std::collections::BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .sets
        .get(KEY)
        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
        .unwrap_or_default()
}

/// What a client gets.
fn listed_members(engine: &TemporalEngine) -> std::collections::BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "the listing failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("expected Members, got {other:?}"),
    }
}

/// Re-derive the whole index from the model maps, which is what a load and a compaction do.
fn rederive(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 loaded");
    let (start, end) = shard.routing_range();
    crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);
}

/// THE WRITE PATH AND THE PROJECTION MUST FILE THE SAME ENTRY FOR THE SAME PAGE.
#[test]
fn a_write_after_a_fold_files_the_same_entry_the_projection_would() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // ---- Write a batch, then FOLD it, so the projection files one entry for one page. ----
    for index in 0..FIRST_BATCH {
        add(&engine, member_bytes(index));
    }
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (folded_live, folded_blocks, _) = entry_census(&engine);
    println!(
        "\n=== after the fold: {folded_live} live entr(ies), \
         {} distinct page(s)",
        folded_blocks.len()
    );

    // FLOOR: THE FIXTURE ACTUALLY FOLDED. Without this, "one entry per page" is trivially true
    // because every element is still its own page and the gate is doing nothing.
    assert_eq!(
        1,
        folded_blocks.len(),
        "the {FIRST_BATCH} members resolve to {} distinct pages after the fold, so they never came \
         to share one and this test has no collapse to observe",
        folded_blocks.len()
    );
    // RESTATED TO THE ONE TERM THAT CAN STILL VARY. This was `(1, 0) == (folded_live,
    // folded_named)`, pairing the entry count with the number of entries naming an element. The
    // second term is structurally zero -- `BlockIndex` has no name to carry -- so pairing it in
    // weakened the assertion rather than strengthening it: a wrong entry count and a constant zero
    // read as one failure, and the zero half could never contribute one. The count stands alone,
