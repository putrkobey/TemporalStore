// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RECLAIMING DISK SPACE IS ITS OWN PASS, WITH ITS OWN TRIGGER, AND THE TRIGGER IS NOT A RATIO.
//!
//! # TWO OPERATIONS, NOT ONE
//!
//! Relieving memory pressure and reclaiming disk space are different operations in this store and
//! run as different stages. `apply_storage_eviction` dumps and releases a bucket's resident page
//! list; the bytes on disk are exactly where they were when it returns. `compact_pages` rewrites
//! live pages off the slabs carrying dead space so the collector can destroy them. Neither is a
//! side effect of the other, and the third test below is the measurement that says so.
//!
//! # THE RATIO THE TRIGGER DOES NOT READ, AND WHY THAT IS THE CORRECT CHOICE
//!
//! `space_amplification_ppm` is `total_known_physical_bytes / live_physical_bytes`, and both halves
//! are blind to reclamation: the denominator adds a slab's WHOLE file while its state is `Active`
//! or `Sealed`, so a slab counts fully live however many of its blocks are dead, and the numerator
//! accumulates before the state match, so a purged slab still contributes its full size
//! (`a_space_ratio_whose_numerator_falls`, `the_live_and_stale_shares_are_a_partition`).
//!
//! Measured again here, on this module's own fixture: the published figure holds at **1.000x
//! through every round** while the block-granular figure beside it goes 1.000x -> 4.000x and the
//! published stale share goes 0.00% -> 75.00%. A reclaimer keyed on the published ratio would
//! never fire.
//!
//! # WHAT THE SHIPPED TRIGGER IS KEYED ON, MEASURED
//!
//! `stale_block_pressure` in the periodic loop is a disjunction of three terms, and the first test
//! below reads all three on the same sweep:
//!
//! ```text
//!   stale_block_slab_count        >= stale_block_slab_pressure            (1 by default)
//!   reclaim_candidate_count       >= stale_block_slab_pressure            (1 by default)
//!   reclaimable_physical_bytes    >= reclaimable_physical_bytes_pressure  (1 by default)
//! ```
//!
//! The term that sees dead space inside a LIVE slab is the candidate COUNT. A slab enters
//! `reclaim_candidates` as soon as `physical_bytes - live_physical_bytes` is non-zero, so the
//! count rises from the first byte of garbage -- measured 0 -> 1 -> 2 -> 3 over the same rounds the
//! published ratio sits still through. At a pressure setting of 1 that is as sensitive as a trigger
//! can be, and strictly more sensitive than a threshold at "roughly twice the logical size" would
//! be: every state such a threshold would catch is already inside the set this one catches.
//!
//! `reclaimable_physical_bytes` is NOT that term, and the first test pins it. It sums the
//! DELAYED-DESTROY reports -- slabs already quarantined for destruction -- so it reads **0** on a
//! shard carrying 220,000 bytes of dead space inside live slabs. It is a true statement about
//! quarantined files and it is not a statement about garbage, so the byte half of that disjunction
//! contributes nothing on the shape that matters and the count half carries it.
//!
//! # AND THE VICTIM IS THE ONE HOLDING THE MOST GARBAGE
//!
//! `storage_reclaim_candidates_from_slab_reports` scores each slab
//! `stale_physical_bytes * (1 - live_ref_density) + stale_block_estimate` and sorts DESCENDING, so
//! the plan hands the heaviest-garbage slab first. The second test is the gate on that order, on a
//! fixture whose slabs carry deliberately different amounts of dead space.
//!
//! # WHAT THESE TESTS ARE FOR
//!
//! The three measurements fix the quantities. The fourth is a SOURCE guard, and it is here because
//! a declined design is only durable if something fails when it is re-attempted: keying any
//! reclamation branch on a published `*_space_amplification_ppm` field is the design this store has
//! declined, and a comment saying so does not fail.

#![allow(clippy::all)]
use super::*;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 400;
const VALUE_LEN: usize = 256;

/// Incompressible bytes, so a rewrite genuinely occupies new space rather than being folded away
/// by the block codec -- a compressible fixture makes every byte figure here a figure about zstd.
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

fn key_of(index: usize) -> String {
    format!("reclaim-{index:06}")
}

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// The rounds this fixture writes, each naming how many keys of the PREFIX it rewrites.
///
/// WHY A HALVING PREFIX AND NOT A WHOLE-STORE REWRITE. Rewriting every key and rolling leaves each
/// older slab with NOTHING live on it: every candidate comes back `orphan_segment` with
/// `live_physical_bytes` 0, every score is identical, and nothing is relocatable -- so a fixture
/// built that way cannot tell an ordering from any other order and cannot issue a compaction round
/// at all. Measured on that shape first: three candidates, all at score 107,600, 0 relocatable
/// page refs.
///
/// Halving the rewritten prefix gives each slab a DIFFERENT amount of dead space while leaving
/// live pages on it, which is the state the collector actually has to choose between.
const REWRITE_ROUNDS: &[usize] = &[KEYS, KEYS / 2, KEYS / 4, KEYS / 8];

/// Seed the fixture and return, per key, the round whose value it now holds.
///
/// The returned vector is what a read-back is checked against: without it a later arm would have
/// to guess which of four values a key should carry, and a check that guesses is a check that
/// passes on the wrong bytes.
fn seed_overlapping_slabs(engine: &TemporalEngine) -> Vec<usize> {
    let mut written_in = vec![0usize; KEYS];
    for (round, prefix) in REWRITE_ROUNDS.iter().copied().enumerate() {
        for index in 0..prefix {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key_of(index),
                    value: noisy_value(index, round),
                },
            });
            assert!(
                response.status.ok,
                "round {round} write {index}: {:?}",
                response.status
            );
            written_in[index] = round;
        }
        // Each round's bytes land on their own slab, so the dead space the NEXT round creates is
        // attributable to a slab rather than smeared across the active one.
        let _ = engine.block_store.prepare_next_slab_with_target(1);
    }
    written_in
}

/// The reclaim plan, asked the same way the periodic loop asks it.
fn plan_of(engine: &TemporalEngine) -> crate::StorageLifecyclePlan {
    engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..crate::StorageLifecycleRequest::default()
    })
}

/// Dead bytes the plan can actually see: summed over the candidates it named.
///
/// NOT `plan.reclaimable_physical_bytes`, which sums the delayed-destroy reports and reads 0 here.
fn dead_bytes(plan: &crate::StorageLifecyclePlan) -> u64 {
    plan.reclaim_candidates
        .iter()
        .map(|candidate| candidate.stale_physical_bytes)
        .sum()
}

fn as_x(ppm: u64) -> f64 {
    ppm as f64 / 1_000_000.0
}

/// ONE SWEEP, AND THE PUBLISHED RATIO DOES NOT MOVE WHILE THE TRIGGER'S OWN TERM DOES.
///
/// Rewriting keys that are already there is the one instrument that separates "nothing is dead"
/// from "dead is not being counted": the live set never changes size, only its storage does. The
/// sweep is what makes this a statement about the quantities rather than about one reading of them.
///
/// FIVE THINGS ASSERTED, and the first two are each other's control:
///   1. the published `space_amplification_ppm` stays at 1.000x through every round, so a
///      reclaimer keyed on it never sees the dead space and never fires;
///   2. the block-granular published figure and the published stale share both RISE through the
///      same rounds, which attributes (1) to the ratio's own denominator rather than to a fixture
///      with no garbage in it;
///   3. the candidate COUNT rises, which is the term in the shipped disjunction that sees dead
///      space inside a live slab;
///   4. `reclaimable_physical_bytes` stays 0, because it sums quarantined slabs and not garbage --
///      stated so that reading the byte half of that disjunction as a garbage figure fails here;
///   5. the shipped trigger is OPEN at the default settings and something is relocatable, so the
///      stage this test is about would actually run.
#[test]
fn the_reclamation_trigger_reads_dead_bytes_where_the_published_ratio_reads_one_x() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());

    struct Row {
        round: usize,
        prefix: usize,
        published_space_ppm: u64,
        published_block_space_ppm: u64,
        stale_share_ppm: u64,
        plan_reclaimable_bytes: u64,
        candidate_dead_bytes: u64,
        candidates: usize,
        top_stale_bytes: u64,
        top_score: u64,
        relocatable_refs: u64,
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut written_in = vec![0usize; KEYS];
    for (round, prefix) in REWRITE_ROUNDS.iter().copied().enumerate() {
        for index in 0..prefix {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key_of(index),
                    value: noisy_value(index, round),
                },
            });
            assert!(response.status.ok, "round {round} write {index}");
            written_in[index] = round;
        }
        let _ = engine.block_store.prepare_next_slab_with_target(1);

        let published = engine
            .shard_stats(1)
            .expect("a loaded shard reports stats")
            .storage;
        let plan = plan_of(&engine);
        let top = plan.reclaim_candidates.first();
        rows.push(Row {
            round,
            prefix,
            published_space_ppm: published.space_amplification_ppm,
            published_block_space_ppm: published.block_space_amplification_ppm,
            stale_share_ppm: published.stale_bytes_ppm,
            plan_reclaimable_bytes: plan.reclaimable_physical_bytes,
            candidate_dead_bytes: dead_bytes(&plan),
            candidates: plan.reclaim_candidates.len(),
            top_stale_bytes: top.map(|c| c.stale_physical_bytes).unwrap_or(0),
            top_score: top.map(|c| c.reclaim_score).unwrap_or(0),
            relocatable_refs: crate::engine::compaction_relocatable_block_refs(
                &plan.reclaim_candidates,
            ),
        });
    }
    let _ = written_in;

    eprintln!();
    eprintln!(
        "  [reclaim-trigger] {KEYS} keys x {VALUE_LEN} B incompressible; each round rewrites a \
halved prefix onto its own slab"
    );
    eprintln!(
        "    {:>5} {:>7} {:>11} {:>11} {:>11} {:>13} {:>13} {:>11} {:>12} {:>12} {:>11}",
        "round",
        "rewrote",
        "space_ppm",
        "block_ppm",
        "stale_shr",
        "plan_reclm_B",
        "cand_dead_B",
        "candidates",
        "top_stale_B",
        "top_score",
        "reloc_refs",
    );
    for row in &rows {
        eprintln!(
            "    {:>5} {:>7} {:>10.3}x {:>10.3}x {:>10.2}% {:>13} {:>13} {:>11} {:>12} {:>12} {:>11}",
