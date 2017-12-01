// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE BEST COMPACTION CANDIDATE, held to the inequalities a maximum cannot escape.
//!
//! # WHAT THIS FIGURE IS FOR
//!
//! `compaction_candidate_bytes` says how much is recoverable and `_slabs` over how many slabs.
//! Neither says whether compacting is worth it, because that depends on CONCENTRATION: 600 KB over
//! thirty slabs at 2% each returns almost nothing per relocation, while the same 600 KB on one slab
//! at 60% returns most of it — and both report identical totals. This is the figure a
//! minimum-ratio gate reads.
//!
//! # WHY INEQUALITIES RATHER THAN A PINNED NUMBER
//!
//! A "best candidate" is easy to get wrong in a way no range check catches: computed over the wrong
//! set, or against the wrong denominator, it still lands somewhere plausible. Two things hold of a
//! maximum by construction, whatever the workload — it is at least the average over the same
//! population, and its bytes are part of that population's sum. Both are exact and free, and both
//! fail the moment the maximum is taken over a different set than the aggregates.
//!
//! # IT TOOK TWO VACUOUS PASSES TO BUILD A FIXTURE THAT TESTS THAT
//!
//! Recorded because they are the interesting part, and because each passed:
//!
//! 1. **Rewriting every key three times produced no candidate at all.** A candidate must be partly
//!    LIVE while carrying holes under `MIN_DISCARD_RANGE_BYTES` (4,096). Rewriting everything leaves
//!    each old slab *wholly dead* — a discard candidate the collector drops whole — and 600
//!    *contiguous* retirements **merge into one range** too large to be fragmented. The record's own
//!    note says it: *"A MERGE CAN CLOSE A HOLE."* The arm printed "nothing to rank" and returned.
//! 2. **Rewriting every third key once produced one candidate slab.** With a single member the
//!    maximum *is* the average, so `best >= aggregate` held as an equality — and would have held for
//!    an implementation that returned the aggregate instead of ranking anything.
//!
//! **A maximum needs a population of more than one, with members that differ.** So two groups are
//! seeded, each sealed onto its own slab with `roll_slab()` — slabs do not seal on their own at
//! these sizes — then rewritten at *different* densities, and the inequality is asserted **strict**.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_best_candidate_is_at_least_the_average -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 600;
/// The two groups are rewritten at DIFFERENT densities, so their slabs carry different fractions of
/// fragmented bytes and the maximum lands strictly above the average. Every Nth key, so each hole
/// keeps live blocks on both sides and cannot merge into a range too large to be fragmented.
const DENSE_EVERY: usize = 2;
const SPARSE_EVERY: usize = 5;
/// Well under `MIN_DISCARD_RANGE_BYTES` (4,096), so an unmerged hole is fragmented.
const VALUE: usize = 256;

fn value_of(index: usize, round: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..VALUE)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
#[ignore = "seeds two sealed slabs and rewrites them at two densities; run by name"]
fn the_best_candidate_is_at_least_the_average() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
