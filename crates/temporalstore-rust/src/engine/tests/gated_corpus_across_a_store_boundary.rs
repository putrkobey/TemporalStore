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
