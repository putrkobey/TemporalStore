// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SKIPPED SERVED-INDEX CHECKPOINT MUST NOT RESURRECT A DROPPED KEY.
//!
//! WHAT THIS IS FOR. A delete_drop eviction round re-encoded and rewrote the whole served index
//! every time it dropped anything, at a cost set by the size of the STORE rather than the size of
//! the round -- `what_one_eviction_round_writes_grows_with_the_store_not_the_round` prices that at
//! 609,692 encoded bytes to drop 2 objects on an 8,000-key shard. The round now spends its
//! deletions against `ShardState::index_flush_countdown` and only checkpoints when they run it
//! out, so most rounds write no index at all.
//!
//! THE WHOLE SAFETY CLAIM IS THAT REPLAY RE-DERIVES WHAT THE CHECKPOINT DEFERRED, and it is an
//! argument until something drives it. The round appends a `CommonDelete` WAL tombstone per
//! dropped key under the shard-table write guard, before any flush, and anchors
//! `applied_wal_sequence` past them; the served index on disk therefore keeps an OLDER anchor
//! while the tombstones sit beyond it, which is exactly the state a reload replays through. This
//! test puts a store into that state on purpose -- rounds whose checkpoint was skipped, counted,
//! not assumed -- drops the engine so only the disk is left, reopens over the same directories,
//! and asks whether the keys those rounds deleted are still deleted.
//!
//! THE CONTESTED CASE IS ASSERTED, NOT HOPED FOR. If every round in the arm happened to be due
//! then nothing was deferred and a clean reload proves nothing at all, so the arm counts the
//! rounds that dropped objects and encoded no index and refuses to conclude without at least one.
//! It also refuses to conclude without at least one key actually dropped: a store that evicted
//! nothing reloads identically whatever the checkpoint did.
//!
//! THE COUNTERS ARE THREAD-LOCAL AND THE DUMP STAGE IS OFF. `index_encode_counts` tallies per
//! thread and the checkpoint runs on the calling thread after the guard drops. The dump stage is
//! the round's OTHER whole-index encoder, so it is turned off here: with it off, a round that
//! dropped objects and encoded zero bytes is a skipped checkpoint and nothing else.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 23;
const KEYS: usize = 1_200;
const ROUNDS: usize = 40;

/// An engine over `dir`, with the shard loaded. Reopening over the same `dir` is the reload.
fn engine_over(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

/// Seed string records and read each one back, so they are cached and therefore evictable.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let key = format!("deferred-{item}");
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet { key },
                })
                .status
                .ok,
            "the read-back must ack at {item}, or the record is not cached and cannot be evicted"
        );
    }
