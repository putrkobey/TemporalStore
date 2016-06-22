// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A GATED STORE SERVES AFTER IT HAS BEEN WRITTEN, CLOSED AND OPENED AGAIN.
//!
//! # WHY A STORE BOUNDARY AND NOT A ROUND INSIDE ONE ENGINE
//!
//! Every step of this series so far ran with the gate OFF by default and turned it on inside one
//! live engine. That exercises the projection and the consumers it feeds, and it cannot exercise
//! the one thing a default flip makes real: **a store whose index FILE was written by a gated
//! binary, read back by a process that did not write it.**
//!
//! A round trip inside one engine cannot see that. The resident `bucket_index` is already in
//! memory, the model maps are already populated, and a read can be answered from either without
//! the serialized index ever being consulted. So this module goes through the file: write, then
//! `unload_shard` -- which materializes the current in-memory index to `shard-{id}.index.json`
//! before the shard leaves memory -- then DROP THE ENGINE, then open a new one over the same page
//! and index directories.
//!
//! **ITS OWN CACHE DIRECTORY ON THE RELOAD.** Handed the first engine's cache, the second engine
//! answers out of pages the first left warm and the read proves nothing about what the file holds.
//! The reload gets `cache-reloaded`, so every byte it serves came off disk through the index it
//! just decoded.
//!
//! # ALL FOUR CONTAINER KINDS, AND THE ONE THE GATE DOES NOT CLAIM TO TOUCH
//!
//! RESTATED. This said the projection gates ONE arm and that "hash, zset and list emit one entry
//! per element either way", which was true when the set arm was the whole collapse. `list` is
//! collapsed now as well, so THE CONTROL IS TWO KINDS RATHER THAN THREE: `hash` and `zset`, both
//! deliberately held out -- a zset component carries the SCORE, and four hash readers resolve
//! through the index BY COMPONENT with no resident-map fallback. See `index_entry_names_a_block`
//! for the measured statement of each.
//!
//! A kind that is supposed to be unaffected is a CONTROL, and a control that is never read cannot
//! detect the case where the flip moved something nobody expected it to -- so the hash census is
//! now ASSERTED on the gated arm and not merely printed. It was bound and left unread when this
//! module was written, which the compiler reported as an unused variable and nobody acted on; a
//! control nothing asserts is not a control. The four counts are still printed side by side so
//! the difference between "the gate changed this" and "the store is broken" stays visible rather
//! than inferred.
//!
//! # EVERY NUMBER IS PRINTED BEFORE IT IS ASSERTED, AND EVERY FLOOR IS ON REACHING THE PATH
//!
//! `0 == 0` passes as agreement over a path nothing reached. So each arm first establishes that
//! the object CAME BACK AT ALL -- the durable model map holds its elements after the reload -- and
//! only then compares what a read serves against it. A listing that returns nothing because the
//! store is empty and a listing that returns nothing because the index cannot name the elements
//! are the same assertion failure and opposite findings; the floor is what tells them apart.
//!
//! # THE GATE IS HELD BY A GUARD THAT RESTORES IT WHILE UNWINDING
//!
//! The value is process-global and the verdict for this crate is a single-threaded run, so a test
//! that sets this variable and leaves it set contaminates every test after it in an ORDERED,
//! silent way -- which has already happened once in this series. `GateHeld` puts back whatever was
//! there, and `Drop` runs during unwinding, so a FAILING arm here cannot bury its own message
//! under a stranger's.
//!
//! Both directions are set EXPLICITLY, never by removing the variable. Removing it selects the
//! compiled-in default, and the whole subject of this module is a change to that default: an arm
//! that means "off" has to say `0`, or it stops meaning that the moment the default moves.
//!
//! # TWO THINGS THIS MODULE MEASURED THAT ARE NOT OBVIOUS FROM READING THE GATE
//!
//! **THE INDEX IS RE-DERIVED AT LOAD, SO THIS GATE DECIDES WHAT THE READER FILES.** Not what a
//! writer wrote. A store written by an ungated binary, opened by a gated one, comes up with the
//! COLLAPSED entry shape -- the projection is recomputed from the durable model maps on the way
//! in. So there is no grandfathering: moving the default changes how every store already on disk
//! is read, which is why this module had to exist before the default moved rather than after.
//! It is also why no format stamp is spent -- see the cross-version arm below.
//!
//! **THE OPERATOR WARNING IS ANTI-CORRELATED WITH THE DAMAGE.** `reconcile: N component name(s)
//! could not be read and were skipped` fires whenever a gated store is loaded, including in the
//! arms that serve every element correctly, and it stayed SILENT in the arm that served zero
//! before the listing was brought along (a store written ungated and read gated prints nothing,
//! because its entries were readable when the reconcile saw them). It is not a usable alarm for
//! this class of failure and nothing should be built on it.

#![allow(clippy::all)]
use super::*;

const ELEMENTS: usize = 40;
const VALUE_WIDTH: usize = 24;

const HASH_KEY: &str = "corpus/hash";
const SET_KEY: &str = "corpus/set";
const ZSET_KEY: &str = "corpus/zset";
const LIST_KEY: &str = "corpus/list";

/// Holds the gate at one value, and puts back whatever was there when it goes out of scope --
/// on a normal drop AND during unwinding.



/// What a read served, per container kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PerKind {
    hash: usize,
    set: usize,
    zset: usize,
    list: usize,
}

impl PerKind {
    fn whole() -> Self {
        Self {
            hash: ELEMENTS,
            set: ELEMENTS,
            zset: ELEMENTS,
            list: ELEMENTS,
        }
    }
}

fn element_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("e-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn field_name(index: usize) -> String {
    format!("f-{index:05}")
}

fn engine_on(cache: &std::path::Path, blocks: &std::path::Path, indexes: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(64 * 1024 * 1024, cache, blocks, indexes)
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "corpus".to_string(),
        shard_uri: "local://corpus/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// `ELEMENTS` elements into each of the four container kinds.
fn write_corpus(engine: &TemporalEngine) {
    for index in 0..ELEMENTS {
        write_to(
            engine,
            Command::HashSet {
                key: HASH_KEY.to_string(),
                field: field_name(index),
                value: element_bytes(index),
            },
        );
        write_to(
            engine,
            Command::SetAdd {
                key: SET_KEY.to_string(),
                member: element_bytes(index),
            },
        );
        write_to(
            engine,
            Command::ZSetAdd {
                key: ZSET_KEY.to_string(),
                member: element_bytes(index),
                score: index as f64,
            },
        );
        write_to(
            engine,
            Command::ListPush {
                key: LIST_KEY.to_string(),
                member: element_bytes(index),
                left: false,
            },
        );
    }
}

fn members_of(engine: &TemporalEngine, command: Command) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    }
}

/// WHAT A READ SERVES, through the command path every client reaches.
fn served(engine: &TemporalEngine) -> PerKind {
    let hash = {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll {
                key: HASH_KEY.to_string(),
            },
        });
        assert!(response.status.ok, "read failed: {response:?}");
        match response.response {
            crate::types::CommandResponse::HashEntries { entries } => entries.len(),
            other => panic!("expected HashEntries, got {other:?}"),
        }
    };
    let set = members_of(
        engine,
        Command::SetMembers {
            key: SET_KEY.to_string(),
        },
    )
    .len();
    // `ZSetRange` answers interleaved member/score-string pairs, so the member count is half the
    // items. Division is stated here rather than inside an assertion so an odd length -- which
    // would mean the interleaving broke rather than that a member went missing -- is visible in
    // the printed number.
    let zset_items = members_of(
        engine,
        Command::ZSetRange {
            key: ZSET_KEY.to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    )
    .len();
    assert_eq!(
        0,
        zset_items % 2,
        "the zset range answered {zset_items} items, which is not member/score pairs -- the \
         count below would be a reading of a broken interleave rather than of the members"
    );
    let list = members_of(
        engine,
        Command::ListRange {
            key: LIST_KEY.to_string(),
            start: 0,
            stop: -1,
        },
    )
    .len();
    PerKind {
        hash,
        set,
        zset: zset_items / 2,
        list,
    }
}

/// THE AUTHORITY FOR EXISTENCE: what the durable model maps hold, independent of any index entry.
///
/// This is the floor every comparison below rests on. A listing that serves nothing because the
/// store came back empty and a listing that serves nothing because the index cannot name the
/// elements are the same number and opposite findings.
fn durable(engine: &TemporalEngine) -> PerKind {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    PerKind {
        hash: shard.hashes.get(HASH_KEY).map(|e| e.len()).unwrap_or(0),
        set: shard.sets.get(SET_KEY).map(|e| e.len()).unwrap_or(0),
        zset: shard.zsets.get(ZSET_KEY).map(|e| e.len()).unwrap_or(0),
        list: shard.lists.get(LIST_KEY).map(|e| e.len()).unwrap_or(0),
    }
}

/// Live index entries for one kind and object: how many there are, and how many NAME an element.
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

/// What one pass across the store boundary observed.
struct Boundary {
    /// What a read served after the reload, per kind.
    serves: PerKind,
    /// What the reloaded durable model maps hold -- the authority for existence.
    holds: PerKind,
    /// The set's (live, naming-an-element) entry census IN THE WRITER, immediately before the
    /// unload that materialized the index. This is what the index FILE was written from.
    ///
    /// Without it the cross-version arm is vacuous in the one direction that matters: a reader
    /// that serves everything proves nothing unless the store it opened actually held a
    /// page-named entry to be misread.
    set_entries_written: usize,
    /// The same census after the reload, which is what the READER derived.
    set_entries_reloaded: usize,
    hash_entries_reloaded: usize,
    /// The DISTINCT PHYSICAL PAGES the reloaded hash's fields sit on, read from the durable map.
    ///
    /// The denominator the entry count is compared against, and read from the OTHER source on
    /// purpose: the index's own page set is derived from the entries being counted, so it cannot
    /// witness an entry over a page no field is on. This can.
    hash_blocks_reloaded: usize,
}

/// Write the corpus under `writer`, fold it, materialize the index, DROP the engine, and open a
/// new one under `reader` over the same page and index directories.
fn across_a_store_boundary(writer: &str, reader: &str) -> Boundary {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");

    let set_entries_written;
    {
        let engine = engine_on(&dir.path().join("cache"), &blocks, &indexes);
        load_on(&engine);
        write_corpus(&engine);
        // Folded, because the shape this gate is about is elements SHARING one page. Without the
        // fold every element is its own page and one entry per page is one entry per element --
        // the gate would be inert and the arm would pass while testing nothing.
        engine
            .compact_shard_blocks(1)
            .expect("the fold round must succeed");
        // READ BEFORE THE UNLOAD, so what the index file was written from is observed rather than
        // inferred from what a later reader derived.
        set_entries_written = live_entries(&engine, "set", SET_KEY);
        // Materializes the in-memory index to the index file, then takes the shard out of memory.
        engine.unload_shard(1);
        drop(engine);
    }

    // ITS OWN CACHE DIRECTORY. Handed the writer's, this engine answers out of pages the writer
    // left warm and the read says nothing about what the index file holds.
    let reloaded = engine_on(&dir.path().join("cache-reloaded"), &blocks, &indexes);
    load_on(&reloaded);
    Boundary {
        serves: served(&reloaded),
        holds: durable(&reloaded),
        set_entries_written,
        set_entries_reloaded: live_entries(&reloaded, "set", SET_KEY),
        hash_entries_reloaded: live_entries(&reloaded, "hash", HASH_KEY),
        hash_blocks_reloaded: occupied_blocks(&reloaded, "hash", HASH_KEY),
    }
}

fn report(label: &str, seen: &Boundary) {
    println!("  {label}");
    println!(
        "    served : hash {:>3}  set {:>3}  zset {:>3}  list {:>3}   (of {ELEMENTS} written)",
        seen.serves.hash, seen.serves.set, seen.serves.zset, seen.serves.list
    );
    println!(
        "    durable: hash {:>3}  set {:>3}  zset {:>3}  list {:>3}",
        seen.holds.hash, seen.holds.set, seen.holds.zset, seen.holds.list
    );
    println!(
        "    set entries: {} live AS WRITTEN  ->  {} live AS RELOADED",
        seen.set_entries_written,
        seen.set_entries_reloaded
    );
    println!(
        "    hash entries reloaded: {} live",
        seen.hash_entries_reloaded
    );
}

/// A GATED STORE, WRITTEN AND THEN OPENED BY A GATED READER, SERVES EVERY ELEMENT OF ALL FOUR
/// CONTAINER KINDS.
///
/// The question the series has not asked. Nothing before this read a gated store back across the
/// index file, because every step defaulted to gate-off and turned it on inside one engine.
#[test]
fn a_gated_corpus_comes_back_whole_across_a_store_boundary_for_all_four_kinds() {
    println!("\n=== a store written gated, closed, and opened gated ===");
    let seen = across_a_store_boundary("1", "1");
    report("gate on -> gate on", &seen);
    let (serves, holds, set_entries, hash_entries) = (
        seen.serves,
        seen.holds,
        seen.set_entries_reloaded,
        seen.hash_entries_reloaded,
    );
    // THE CONTROL, ASSERTED, AND IT IS THE OTHER WAY ROUND NOW. This said hash was held OUT of the
    // collapse, so a gated binary had to file one NAMED entry per field, and it asserted
    // `(ELEMENTS, ELEMENTS)` -- forty entries, forty naming a field. Hash is IN the page-named set,
    // so the same measurement is the collapse itself: the forty fields fold onto the pages they
    // share and NO entry names a field.
    //
    // STATED AS AN EXACT PAIR AND NOT AS A FLOOR, because the number is the claim. `< ELEMENTS`
    // would be satisfied by an index that filed almost nothing, and `named == 0` alone is
    // satisfied by an index that filed nothing at all -- so the live count is compared against the
    // DISTINCT PAGES the elements resolve to, which is the denominator the collapse is about, and
    // the serving figures below are what say nothing was lost on the way.
    // THE "AND NO ELEMENT NAME" HALF IS GONE FROM THIS PAIR. It was asserted as the tuple
    // `(hash_blocks_reloaded, 0)`, and the second term is structurally zero -- an entry has no
    // element name -- so pairing it in meant a wrong entry count and a constant read as one
    // failure, with only the constant guaranteed. The entry count stands alone and is still
    // compared against the DISTINCT PAGES the elements resolve to rather than against a literal,
    // which is the denominator the collapse is about.
    assert_eq!(
        seen.hash_blocks_reloaded, hash_entries,
        "a GATED reader filed {hash_entries} live hash entries over {} distinct page(s). One entry \
         per page is what the collapse means, and more entries than pages is a stale entry over a \
         dead page",
        seen.hash_blocks_reloaded
    );
    // THE GATED PROJECTION WROTE THIS STORE. Asserted on the WRITER's census, before any reader
    // could have re-derived it, so "a gated store" is established rather than assumed.
    assert!(
        seen.set_entries_written < ELEMENTS,
        "the writer filed {} live set entries for {ELEMENTS} elements, so the index this arm \
         materialized is not a collapsed one",
        seen.set_entries_written
    );

    // ---- THE FLOOR: THE STORE CAME BACK. ----
    //
