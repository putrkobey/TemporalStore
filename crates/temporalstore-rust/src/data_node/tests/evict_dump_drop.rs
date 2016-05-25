// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MEMORY RECLAMATION UNDER PRESSURE IS ALLOWED TO DO TO A LIVE RECORD.
//!
//! The rule this module exists to hold: RELIEVING MEMORY PRESSURE NEVER DESTROYS THE ONLY
//! REACHABLE COPY OF LIVE DATA. A read after a reclaiming round loads the value back.
//!
//! WHY THAT IS A RULE AND NOT A PREFERENCE, in this store's own terms. A record's payload lives in
//! the block store and is reached through an ADDRESS held in the shard's model maps
//! (`engine.rs`, the `FastPathRead::String` arm: `shard.string_address(key)` and then
//! `read_block_bytes`). So the resident cost of a record splits in two, and the two halves are not
//! alike:
//!
//!   * THE DERIVED HALF -- a bucket's `block_index` and its object-block lookup entries. This is
//!     droppable, because `reload_released_bucket` rebuilds it FROM the model maps. Dropping it is
//!     reclamation: `release_bucket_blocks` takes it and the next read puts it back.
//!   * THE AUTHORITATIVE HALF -- the model-map entry itself, which holds the address. Nothing in
//!     the engine can rebuild one on demand. There is no per-bucket durable address map to fault
//!     in: the persisted index is written whole, from a clone of the entire shard
//!     (`serialize_index(&snapshot)`), so there is nothing a single read could load.
//!
//! A round that takes the derived half is dump-then-drop. A round that takes the authoritative half
//! is a DELETE, whatever it is called, because the payload bytes it leaves behind in the block store
//! have nothing pointing at them any more.
//!
//! The two tests below are that distinction, measured on matched fixtures, and the third is the
//! guard that keeps the memory-pressure stage on the droppable half.

#![allow(clippy::all)]
use super::*;

/// The shipped routing range, so a released bucket is one that keys SHARE rather than a per-key
/// node that only the bare `load_shard` default produces.
const SHIPPED_END_BUCKET: u32 = 1023;

const VALUE_LEN: usize = 96;

/// A value that is a function of its key, so a read resolved through the WRONG block fails here
/// instead of passing because the length happened to match.
fn keyed_value(index: usize) -> Vec<u8> {
    let mut value = format!("{index:06}:").into_bytes();
    value.resize(VALUE_LEN, b'v');
    value[VALUE_LEN - 1] = (index % 251) as u8;
    value
}

fn key_of(index: usize) -> String {
    format!("dumpdrop-{index:06}")
}

/// A key this fixture never writes. THE NEGATIVE CONTROL: without it, "every key read back" is also
/// what a store that answers every lookup with a value would report.
fn never_written_key(index: usize) -> String {
    format!("dumpdrop-never-written-{index:06}")
}

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_shard_on_shipped_range(engine: &TemporalEngine, load_version: u64) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "dump-drop".to_string(),
        shard_uri: "local://dump-drop/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{SHIPPED_END_BUCKET}: {:?}",
        response.status
    );
}

/// A shard on the shipped range holding `keys` ACKNOWLEDGED string records -- every write's status
/// is asserted, so "acknowledged" below is a fact about this fixture and not a hope.
fn seeded_engine(dir: &std::path::Path, keys: usize) -> TemporalEngine {
    let engine = engine_at(dir);
    load_shard_on_shipped_range(&engine, 1);
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key_of(index),
                value: keyed_value(index),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    engine
}

/// What one key answered. `Whole` is byte-identical; `Missing` is the store saying the key is not
/// there; `Wrong` is bytes that are not this key's value.
#[derive(Debug, Default)]
struct ReadBack {
    whole: usize,
    missing: Vec<String>,
    wrong: Vec<String>,
}

impl ReadBack {
    fn verified(&self) -> usize {
        self.whole
    }
}

/// Read `keys` keys back and sort the answers into the three outcomes. BOTH DIRECTIONS are recorded
/// -- the count that answered whole AND the lists that did not -- because an empty mismatch list is
/// also what a loop that never ran produces.
fn read_back_all(engine: &TemporalEngine, keys: usize) -> ReadBack {
    let mut outcome = ReadBack::default();
    for index in 0..keys {
        let key = key_of(index);
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.clone() },
        });
        if !answer.status.ok {
            outcome.wrong.push(format!("{key}: read failed {:?}", answer.status));
            continue;
        }
        match answer.response {
            CommandResponse::Bytes { value: Some(value) } if value == keyed_value(index) => {
                outcome.whole += 1;
            }
            CommandResponse::Bytes { value: Some(value) } => outcome.wrong.push(format!(
                "{key}: {} bytes that are not this key's value",
                value.len()
            )),
            CommandResponse::Bytes { value: None } => outcome.missing.push(key),
            other => outcome
                .wrong
                .push(format!("{key}: a string read answered {other:?}")),
        }
    }
    outcome
}

/// THE NEGATIVE CONTROL, run on the same build at the same moment as the hits it qualifies: keys
/// this fixture never wrote must read ABSENT. Returns how many of them answered with a value.
fn never_written_keys_that_answered(engine: &TemporalEngine, probes: usize) -> usize {
    let mut answered = 0usize;
    for index in 0..probes {
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: never_written_key(index),
            },
        });
        if let CommandResponse::Bytes { value: Some(_) } = answer.response {
            answered += 1;
        }
    }
    answered
}

const CONTROL_PROBES: usize = 16;

/// THE FINDING: THE DROP ARM DELETES ACKNOWLEDGED RECORDS, AND THE DELETION SURVIVES A RELOAD.
///
/// This is a characterization, not a proposal. `apply_storage_eviction`'s `delete_drop` arm calls
/// `delete_record` on every live object in its victim buckets -- dirty or clean, dumped or not --
/// which removes the key from every model map, from the bucket index, and from the expiry and
/// seen maps (`engine.rs`, `delete_record_exact`). It then writes a `CommonDelete` tombstone per
/// dropped key and anchors `applied_wal_sequence` past them, so replay observes the deletion
/// rather than the earlier write. BOTH of those are durability work: the arm does not merely drop a
/// resident copy, it makes the removal stick.
///
/// SIX THINGS ASSERTED, IN THIS ORDER, because the later ones say nothing without the earlier:
///
///   1  THE CORPUS READ BACK BEFORE THE ROUND, cold. Keys missing afterwards are then the round's
///      doing and not the fixture's.
///   2  THE ARM FIRED AND THE FIXTURE REACHED THE REGIME: the gate opened, victims were taken, the
///      mode is the drop arm's own, and `dropped_object_count` is positive. A round that declined
///      proves nothing about what the arm does.
///   3  ACKNOWLEDGED KEYS ARE NOW MISSING, and exactly as many as the arm says it dropped. That
///      equality is the accounting: the losses are the arm's deletions and not a read path that
///      stopped answering.
///   4  THE NEGATIVE CONTROL, same build, same moment: keys never written read absent. Without it
///      "missing" is not a reading.
///   5  KEYS OUTSIDE THE VICTIM BUCKETS STILL READ BACK WHOLE, so this is a selective deletion and
///      not a store that has stopped working.
///   6  AND THE MISSING KEYS ARE STILL MISSING AFTER A RELOAD on a fresh engine over the same
///      directories. That is what separates this finding from "a resident copy was dropped": a
///      dropped copy comes back, a deleted record does not.
#[test]
fn the_drop_arm_deletes_acknowledged_records_and_the_deletion_survives_a_reload() {
    const KEYS: usize = 600;
    const BATCH: usize = 16;

    let dir = tempdir().unwrap();
    let (dropped, missing_before_reload, resident_before, resident_after) = {
