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
}

/// The keys the served index holds for this shard, which is the thing a reload has to re-derive.
///
/// Read off the index rather than through `StringGet` on purpose: the question is what the SERVED
/// INDEX says after a replay, and a read answered from a cache tier or a block-store fallback
/// would answer a different one.
fn live_keys(engine: &TemporalEngine) -> std::collections::BTreeSet<String> {
    engine
        .shards
        .read()
        .expect("engine lock poisoned")
        .get(&SHARD)
        .expect("shard is loaded")
        .strings()
        .keys()
        .map(|key| key.to_string())
        .collect()
}

#[test]
fn a_deferred_checkpoint_still_reloads_with_the_keys_dropped() {
    let dir = tempfile::tempdir().expect("tempdir");

    let (before_reload, dropped_keys, skipped, materialized, rounds_with_drops) = {
        let engine = engine_over(dir.path());
        seed(&engine, KEYS);

        let seeded = live_keys(&engine);
        assert_eq!(
            KEYS,
            seeded.len(),
            "the seed must have landed, or nothing below means anything"
        );

        // THE INTERVAL, READ BACK OUT. A knob the round consults under a different name than this
        // arm sets leaves the arm silently not running, and it would then agree with whatever it
        // was asked. This is the shipped default, deliberately: the deferral this test is about is
        // the one a deployment gets.
        let interval = engine.logged_deletion_index_flush_interval();
        assert!(
            interval > 1,
            "the engine's interval is {interval}, so every round is due and nothing is deferred \
             -- this test would pass while measuring the behaviour it was written to replace"
        );

        let mut skipped = 0usize;
        let mut materialized = 0usize;
        let mut rounds_with_drops = 0usize;
        for _ in 0..ROUNDS {
            crate::engine::shard_write_guard::reset_index_encode_counts();
            let report = engine.apply_storage_eviction(SHARD, 1, 2, false, true);
            let counts = crate::engine::shard_write_guard::index_encode_counts();
            if report.dropped_object_count == 0 {
                continue;
            }
            assert_eq!("delete_drop", report.mode, "{report:?}");
            rounds_with_drops += 1;
            if counts.encodes_total > 0 {
                materialized += 1;
            } else {
                skipped += 1;
            }
        }

        let before_reload = live_keys(&engine);
        let dropped_keys: std::collections::BTreeSet<String> =
            seeded.difference(&before_reload).cloned().collect();
        println!();
        println!(
            "  interval={interval} rounds={ROUNDS} rounds_with_drops={rounds_with_drops} \
             checkpoints_taken={materialized} checkpoints_skipped={skipped}"
        );
        println!(
            "  seeded={} live_before_reload={} dropped={}",
