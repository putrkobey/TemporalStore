// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE GUARD THAT FAILS IF THE THIRD DOOR STARTS DELIVERING A CONTEXT NODE.
//!
//! #2151 measured that a load with NO BASE INDEX FILE -- the path that reads no stamp, folds the
//! whole index log, and puts every record's `key_states` through
//! `apply_key_state_field(&mut shard.context_nodes, ...)` -- delivers no entry. That closure is
//! what a `context_nodes` removal would rest on, and it was EMPIRICAL: a measurement of today,
//! with nothing to fail if the behaviour moved.
//!
//! # WHY THIS GUARDS THE OUTCOME AND NOT THE CALL-SITE SET
//!
//! The obvious mechanical guard is a census of the thirteen `apply_key_state_field` sites,
//! failing when a fourteenth appears. IT WOULD GUARD THE WRONG VARIABLE, and the reason is
//! specific rather than stylistic: **`context_nodes` is already one of the thirteen.**
//! `engine.rs`'s `apply_key_states` calls
//! `apply_key_state_field(&mut shard.context_nodes, key, blob.get("context_nodes"))` today, on
//! this very path, and the entry still does not arrive. So the call site existing is NOT what
//! closes the door -- a census over the sites is green now and would stay green after a
//! re-opening, which is the one thing a guard must not do.
//!
//! What this does instead is pin the OUTCOME at the fold boundary, with the fold's own probe
//! establishing that the fold was given the record rather than merely that the load ended up
//! without the entry. Two different changes would both be caught: one that makes the applied
//! field stick, and one that stops the fixture producing a record to apply.
//!
//! # THE FIVE THINGS ASSERTED, AND WHAT EACH ONE RULES OUT
//!
//!   1. THE PLANT IS DELIVERABLE. A twin directory holding the same fixture WITH its base index
//!      flushed loads and the entry is readable. Rules out a fixture that never planted anything
//!      -- which from the far end is indistinguishable from a shut door.
//!   2. THE CAPTURE REACHED THE LOG. The index-log file on disk contains the literal
//!      `context_nodes`. Rules out a capture that never fired. This is the check whose earlier
//!      absence let the first version of #2151 fail on its own denominator when the log store
//!      turned out to live at `index_dir/indexlogs` and the suffix had moved from `jsonl` to
//!      `bin`; a fixture control that cannot find its own artefact reads as "the door is shut",
//!      which is the worst available wrong answer.
//!   3. THIS IS THE THIRD DOOR. No base index file exists at the moment of the load. Asserted on
//!      the FILE, because `index_load_path_counts` is process-global and nothing can attribute
//!      it back across threads.
//!   4. THE FOLD WAS GIVEN THE RECORD. `index_log::probe` says the fold was asked about at least
//!      one record for this shard and declared NONE of them already reflected, and that it
//!      APPLIED at least one -- and `note_fold_applied` is called after the page items are
//!      re-attached and the key states are applied, so that is the fold having run
//!      `apply_key_states` on this shard's records. Rules out the quiet version of a shut door:
//!      the fold skipping the record and the outcome looking identical.
//!   5. AND THE ENTRY IS STILL NOT THERE.
//!
//! # THE PROBE IS PROCESS-GLOBAL, SO THE SHARD ID IS THE KEY THAT MAKES IT ATTRIBUTABLE
//!
//! `FOLD_DECISIONS` and `FOLD_APPLIED` are statics. A second test folding on another thread
//! pushes into the same vectors, and filtering by shard id only helps if the id is this test's
//! alone -- which is why this uses **4071** rather than 1 or 7. It appears nowhere else in the
//! crate, so every row this test reads back is its own. A capture is only as good as the key it
//! is stored under, and a shared key here would have produced a guard that passes on somebody
//! else's fold.
//!
//! The armed flag is put back by a DROP GUARD rather than by a line at the end. Unarmed the probe
//! is one relaxed load per record; armed it grows a vector per record, and `index_log_scale`
//! measures what the fold holds at 10,000 records. A panicking test that left it armed would be
//! measured along with it, ordered and silent.

#![allow(clippy::all)]
use super::*;

/// This test's own shard id. Unique in the crate, which is what makes the process-global fold
/// probe attributable -- see the module note.
const SHARD: ShardId = 4071;

const KEY: &str = "ctx:node:legacy";
const HOUR_MS: u64 = 3_600_000;

/// Holds the fold decision/application probe armed for as long as it lives, and puts it back on a
/// normal drop AND during unwinding.
struct FoldProbeArmed;

impl FoldProbeArmed {
    fn arm() -> Self {
        crate::index_log::probe::arm_decisions();
        Self
    }
}

impl Drop for FoldProbeArmed {
    fn drop(&mut self) {
        crate::index_log::probe::disarm_decisions();
    }
}

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn base_index_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes").join(format!("shard-{SHARD}.index.json"))
}

/// Every file under the index-log root. A recursive walk rather than a name pattern: the store
/// lives at `index_dir/indexlogs`, the suffix has already moved once, and sealed segments carry a
/// span in the name. The content check is what decides.
fn index_log_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(at: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(at) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("indexes").join("indexlogs"), &mut out);
    out.sort();
    out
}

/// `StringSet` for a live page, a `context_nodes` entry planted under the same key as a clone of
/// that live entry, then `CommonExpire` to arm a deadline -- which is one of the two halves
/// `delta_key_state_change` fires on, so the capture runs with the entry in place and writes a
/// blob carrying `context_nodes`.
///
/// `CommonExpire` and not `StringSetEx`: a value-replacing write relocates the string's page and
/// leaves the plant naming a superseded address, which is a second variable.
fn build_fixture(dir: &std::path::Path, flush_base: bool) {
    let engine = engine_at(dir);
    engine.load_shard(SHARD);

    let wrote = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: KEY.to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(wrote.status.ok, "the setup write failed: {}", wrote.status.message);

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        let live = shard
            .strings()
            .get(KEY)
            .expect("the written string is resident")
            .clone();
        shard.context_nodes_mut().insert(KEY.into(), live.address());
    }

    let expired = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::CommonExpire {
            key: KEY.to_string(),
            ttl_ms: HOUR_MS,
        },
    });
    assert!(
        expired.status.ok,
        "arming the deadline failed: {}",
        expired.status.message
    );

    if flush_base {
        engine.flush_shard_index(SHARD);
    }
    drop(engine);
}

fn planted_arrived(engine: &TemporalEngine) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&SHARD)
        .map_or(false, |shard| shard.context_nodes().contains_key(KEY))
}

fn decisions_for_this_shard() -> Vec<(ShardId, u64, bool)> {
    crate::index_log::probe::decisions()
        .into_iter()
        .filter(|(shard_id, _, _)| *shard_id == SHARD)
        .collect()
}

fn applied_for_this_shard() -> Vec<(ShardId, u64)> {
    crate::index_log::probe::applied()
        .into_iter()
        .filter(|(shard_id, _)| *shard_id == SHARD)
        .collect()
}

/// Build the fixture with no base index, and return the directory holding it.
///
/// The two fixture controls live here so neither arm can run without them: the index log must
/// contain the literal `context_nodes`, and no base index file may exist.
fn fixture_with_no_base_index() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path(), false);

    let logs = index_log_files(dir.path());
    assert!(
        !logs.is_empty(),
        "no file at all under the index-log root, so there is no fold to drive"
    );
    let carried = logs.iter().any(|path| {
        std::fs::read(path).map_or(false, |bytes| {
            bytes
                .windows(b"context_nodes".len())
                .any(|window| window == b"context_nodes")
        })
    });
    assert!(
        carried,
        "no index-log file contains the literal `context_nodes`, so the capture never wrote the \
         blob and nothing below says anything about the fold. Files checked: {logs:?}"
    );

    let base = base_index_path(dir.path());
    let _ = std::fs::remove_file(&base);
    assert!(
        !base.exists(),
        "a base index file is still present at {base:?}, so this is not the base-absent path"
    );
    dir
}

/// THE PLANT IS DELIVERABLE AND READABLE, through a path that is open.
///
