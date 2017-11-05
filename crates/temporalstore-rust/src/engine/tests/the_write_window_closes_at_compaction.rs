// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WRITE-AMPLIFICATION WINDOW CLOSES AT A COMPACTION, AND THE LIFETIME FIGURE DOES NOT.
//!
//! # WHY A WINDOWED RATIO EXISTS AT ALL
//!
//! `lifetime_write_amplification_ppm` divides a LIFETIME numerator by a CURRENT denominator.
//! Nothing resets `bytes_written`, so on a store whose live set is flat that figure rises without
//! limit as a function of uptime: a gate on it eventually fires on a healthy store that has merely
//! been up a long time. Its own field note says so.
//!
//! Worse, compaction is itself a writer. `whether_a_dead_slab_can_be_reclaimed` measured one round
//! writing 3,697,200 B to make 1,232,400 B reclaimable. Charged against a lifetime numerator those
//! bytes are pure amplification, so the operation that LOWERS space amplification RAISES the write
//! figure and the two published readings pull against each other.
//!
//! `write_amplification_since_compaction_ppm` is the same arithmetic over a window that opens
//! whenever the live set is consolidated. That is what makes a fixed trigger meaningful, and it is
//! what makes a compaction look like progress on both ratios instead of a trade.
//!
//! # BOTH HALVES ARE ASSERTED, BECAUSE EITHER ALONE PASSES ON THE WRONG STATE
//!
//! "the windowed figure fell" would also pass if BOTH fields had been re-pointed at the window, in
//! which case the lifetime reading is gone and nothing records the store's history. "the lifetime
//! figure did not fall" would also pass if the windowed field were never wired and simply returned
//! the same number. So this asserts the DIVERGENCE: the windowed one falls, the lifetime one does
//! not, and they were EQUAL before the first compaction -- which is the third fact, and the one
//! that proves the window started as the whole lifetime rather than as something else.
//!
//! # THE FIXTURE REWRITES HALVING PREFIXES, NOT THE WHOLE STORE
//!
//! Rewriting every key and rolling leaves each older slab with nothing live on it, every candidate
//! comes back an orphan with zero live bytes, and nothing is relocatable -- so a fixture built that
//! way cannot issue a compaction round at all. Halving the rewritten prefix gives each slab a
//! different amount of dead space while leaving live pages on it, which is the state a compaction
//! can actually act on. That shape is taken from `reclaiming_space_is_its_own_pass`, which
//! measured the orphan case first.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_write_window_closes_at_compaction -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 400;
const VALUE_LEN: usize = 1024;

/// Halving prefixes, so each slab keeps live pages while accumulating different dead space.
const REWRITE_ROUNDS: &[usize] = &[KEYS, KEYS / 2, KEYS / 4, KEYS / 8];

fn key_of(index: usize) -> String {
    format!("window-{index:06}")
}

/// Incompressible bytes, distinct per (key, round), so no round's write can be absorbed by the
/// codec or deduplicated against an earlier one.
fn noisy_value(index: usize, round: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..VALUE_LEN)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

#[test]
fn the_write_window_closes_at_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // --- BEFORE ANY WRITE: the window is the whole lifetime, so the two agree trivially. Taken
    // --- first because the equality after seeding is the interesting one and this says the
    // --- starting state is not a special case.
    let fresh = engine.shard_stats(1).expect("a loaded shard reports stats").storage.clone();
    assert_eq!(
        fresh.bytes_written_since_compaction, 0,
        "a store that has written nothing must report 0 bytes in its window, and reported {}",
        fresh.bytes_written_since_compaction
    );

    for (round, prefix) in REWRITE_ROUNDS.iter().copied().enumerate() {
        for index in 0..prefix {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key_of(index),
                    value: noisy_value(index, round),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }

    let before = engine.shard_stats(1).expect("a loaded shard reports stats").storage.clone();
    let store_before = engine.block_store.stats();

    // --- THE WINDOW IS STILL THE WHOLE LIFETIME, because nothing has compacted yet. This is the
    // --- fact that makes the divergence below attributable to the compaction rather than to the
    // --- two fields having been different all along.
    assert_eq!(
        store_before.bytes_written, before.bytes_written_since_compaction,
        "before the first compaction the window must be the whole lifetime: {} written against a \
         window of {}",
        store_before.bytes_written, before.bytes_written_since_compaction
    );
    assert_eq!(
        before.lifetime_write_amplification_ppm, before.write_amplification_since_compaction_ppm,
        "before the first compaction the two ratios must be equal, and read {} lifetime against \
         {} windowed",
        before.lifetime_write_amplification_ppm, before.write_amplification_since_compaction_ppm
    );
    assert!(
        before.write_amplification_since_compaction_ppm > 1_000_000,
        "the fixture rewrote {} keys over {} rounds, so write amplification must exceed 1.0x \
         before any compaction -- it read {} ppm, and a window that is already at the floor \
         cannot be observed falling",
        KEYS,
        REWRITE_ROUNDS.len(),
        before.write_amplification_since_compaction_ppm
    );

    // --- THE COMPACTION MUST ACTUALLY RELOCATE SOMETHING, or no window opens and the assertions
    // --- below would be testing a no-op.
    let plan = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..crate::StorageLifecycleRequest::default()
    });
    let drain = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
    assert!(
        !drain.is_empty(),
        "no slab is drainable, so a compaction round cannot relocate anything and this arm would \
         assert over a no-op"
    );
    let compaction = engine
        .compact_shard_blocks_draining(1, drain.clone())
        .expect("a compaction round over a loaded shard");
    assert!(
        compaction.rewritten_block_refs > 0,
        "the compaction rewrote no block refs, so it consolidated nothing and deliberately does \
         NOT open a window -- the fixture needs a shape the collector can act on"
    );

    let after = engine.shard_stats(1).expect("a loaded shard reports stats").storage.clone();
