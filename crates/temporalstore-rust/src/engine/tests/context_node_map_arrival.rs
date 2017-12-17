// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW A `context_nodes` ENTRY CAN STILL ARRIVE, AND WHICH DOORS THE FORMAT STAMP GUARDS.
//!
//! # THE QUESTION
//!
//! `ShardState::context_nodes` has no production key-creating writer -- the move to
//! `hashes[key][CONTEXT_NODE_FIELD]` took it out in June 2026 -- and every production read of it
//! is an `.or_else()` fallback after `hashes`. So it is a DECODE PATH for blocks written before
//! that move, and the question for consolidating it away is not "does anything write it" but
//! "can the data still ARRIVE in a shard this binary serves".
//!
//! It is on the wire (`#[serde(default)]`) and `ShardState` sets no `deny_unknown_fields`, so
//! deleting the field would make a stored `"context_nodes": {...}` decode into nothing, silently.
//! That is removable only if every door the data can come through refuses the artefacts that
//! carry it.
//!
//! # WHAT IS MEASURED HERE, AND WHY THREE ARMS AND NOT ONE
//!
//! The stamp (`index_format_version` against `SHARD_INDEX_FORMAT_VERSION`) is the obvious gate,
//! and `persistence::load_index_inner` does refuse a stale one BEFORE the index-log fold runs.
//! Checking only that loader answers the question for ONE door and reads as if it answered it for
//! all of them. `decode_index_bytes` has other callers.
//!
//!   * [`a_stale_stamped_base_index_does_not_deliver_a_context_node_entry`] -- the base-index
//!     door, stale stamp. REFUSED, and the entry does not arrive.
//!   * [`a_current_stamped_base_index_does_deliver_a_context_node_entry`] -- the same bytes at
//!     the CURRENT stamp. ACCEPTED, and the entry arrives. This is the positive control: without
//!     it the arm above passes just as well when the plant never worked, or when the reconcile
//!     drops the entry for a reason that has nothing to do with the stamp.
//!   * [`the_dump_manifest_door_refuses_a_stale_stamped_index_image`] -- the dump manifest
//!     restore door. This arm recorded a HOLE: the index image was handed to
//!     `decode_index_bytes` directly with no `index_format_version` comparison anywhere on that
//!     path, so a stale image installed and the entry arrived. #2150 closed it, and the arm is
//!     now a regression guard on the refusal -- asserted by status code, because four other
//!     integrity bindings on this path refuse with codes of their own.
//!
//! The third arm is what decided the consolidation question when this landed, and it decided it
//! the other way from the stamp reasoning: the map was NOT removable on the ground that the stamp
//! forecloses its population, because one of the doors was not stamped at all.
//!
//! #2150 then stamped that door, and #2151/#2152 measured the remaining one -- a load with no
//! base index, which reads no stamp because it reads nothing. That one is held shut by the
//! recovery arm (`load_shard` reaches `load_index_base_only` with `fold_deltas: false`) rather
//! than by anything about this map, which makes the closure structural but conditional on
//! `TS_WAL_LEGACY_RECOVERY` staying unset. The verdict of NOT REMOVABLE therefore stands on that
//! condition rather than on this door.

#![allow(clippy::all)]
use super::*;

use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};

/// The key the planted entry is filed under. Deliberately NOT the key the string write below
/// uses: `apply_key_states` treats a key-state blob with no `context_nodes` field as a tombstone
/// for that key, so planting under the written key would be removed by the delta fold for a
/// reason that is not the stamp and would make both arms say the wrong thing.
const PLANTED: &str = "ctx:node:legacy";

/// The key the string write uses, so the base index has a live page whose entry can be cloned.
const WRITTEN: &str = "stamped";

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

/// Write one string on shard 7 and flush the base index, so there is a real index file with a
/// real live page in it.
fn write_and_flush(engine: &TemporalEngine) {
    engine.load_shard(7);
    let answer = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: WRITTEN.to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(
        answer.status.ok,
        "the setup write failed: {}",
        answer.status.message
    );
    engine.flush_shard_index(7);
}

/// Rewrite shard 7's base index so it carries a `context_nodes` entry, at `stamp`.
///
/// The entry is a CLONE of the live string entry rather than a hand-built one: `ElementEntry`
/// has no public constructor, and more to the point a planted address that no slab holds would be
/// dropped by the load for a reason that is not the stamp.
fn plant_in_base_index(dir: &std::path::Path, stamp: u32) {
    let path = dir.join("indexes").join("shard-7.index.json");
    let bytes = std::fs::read(&path).expect("a flushed base index exists");
    let mut restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    assert_eq!(
        restored.index_format_version,
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        "denominator: the index just written must carry the CURRENT stamp, or changing it below \
         changes nothing"
    );
    let live = restored
        .strings()
        .get(WRITTEN)
        .expect("the written string is in the flushed index")
        .clone();
    restored.context_nodes_mut().insert(PLANTED.into(), live.address());
    restored.index_format_version = stamp;
    std::fs::write(&path, crate::engine::encode_index_bytes(&restored)).expect("write");
}

/// Is `PLANTED` in shard 7's `context_nodes` map, as the engine now holds it?
fn planted_arrived(engine: &TemporalEngine) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&7)
        .map_or(false, |shard| shard.context_nodes().contains_key(PLANTED))
}

/// THE BASE-INDEX DOOR IS STAMPED, AND A STALE STAMP KEEPS THE ENTRY OUT.
#[test]
fn a_stale_stamped_base_index_does_not_deliver_a_context_node_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    write_and_flush(&engine);
    plant_in_base_index(dir.path(), crate::engine::SHARD_INDEX_FORMAT_VERSION - 1);
    drop(engine);

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(7);
    // Bound by position against the accessor's own signature, not by the order that reads nicely:
    // all four are `u64` and swapping two compiles.
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    assert!(
        stale_n > 0,
        "an index stamped {} against a current {} must be counted as a STALE-STAMP refusal; got \
         stale {stale_n} decode {decode_n} absent {absent_n} accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION - 1,
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        !planted_arrived(&reloaded),
        "a refused index must deliver NO part of itself, and this one delivered a \
         `context_nodes` entry"
    );
}

/// THE POSITIVE CONTROL. The same plant at the CURRENT stamp DOES arrive, which is what makes
/// the arm above a statement about the stamp rather than about the plant.
