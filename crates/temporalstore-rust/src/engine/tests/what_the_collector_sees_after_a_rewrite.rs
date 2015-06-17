// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE GARBAGE COLLECTOR SEES AFTER A REWRITE, which is not what the report shows.
//!
//! # THE SEPARATION THIS ARM EXISTS TO MAKE
//!
//! Overwriting every key leaves the displaced blocks dead inside slabs that are still live, and
//! the published figures do not notice: `summarize_slabs` adds a slab's whole `physical_bytes` to
//! `live_physical_bytes` whenever the slab is `Active` or `Sealed`, so the denominator grows in
//! step with the numerator, `space_amplification_ppm` sits at 1.000x however much is dead, and
//! `reclaimable_physical_bytes` stays at zero.
//!
//! A blind REPORT and a blind COLLECTOR are very different problems, and they have different
//! fixes. Candidate selection does not read that summary -- it reads `slab_live_fractions()`,
//! which reports, per slab, `logical_bytes` (everything ever appended), `live_bytes` (from the
//! maintained block-level tally, which decrements when an index entry is replaced) and
//! `garbage_basis_points` derived from the two. That is block granularity.
//!
//! So this asks the collector directly. If it sees the garbage, what the report misses is a
//! DENOMINATOR, with a working collector underneath. If it does not, the store cannot reclaim an
//! overwritten block at all, which is a far larger finding. The arm states which rather than
//! assuming either.
//!
//! # THE VALUES ARE INCOMPRESSIBLE
//!
//! `vec![b'v'; n]` compresses to nothing, which makes stored bytes framing rather than content and
//! stops a stored-byte figure tracking the data at all. Every figure here is in stored bytes.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_the_collector_sees_after_a_rewrite -- --ignored --nocapture

use crate::engine::reports::StorageManagerCycleRequest;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;

/// Values that cannot compress, keyed by index AND round so each pass writes different bytes --
/// a rewrite with identical bytes is something a store may legitimately recognise and skip, which
/// would make this measure deduplication instead of rewrite.
fn noisy_value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
#[ignore = "writes 2,000 keys five times over and runs a reclaim cycle; run by name"]
fn what_the_collector_sees_after_a_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gc-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }

    let report_fractions = |label: &str| -> (u64, u64, u64, u64) {
        let fractions = engine
            .block_store
            .slab_live_fractions()
            .expect("the slab live fractions read");
        let logical: u64 = fractions.iter().map(|f| f.logical_bytes).sum();
        let live: u64 = fractions.iter().map(|f| f.live_bytes).sum();
        let physical: u64 = fractions.iter().map(|f| f.physical_bytes).sum();
        println!();
        println!("  {label} -- {} slab(s)", fractions.len());
        println!(
            "    {:>6} {:>12} {:>12} {:>12} {:>8} {:>8}",
            "slab", "physical", "logical(ever)", "live", "live bp", "garbage bp"
        );
        for f in &fractions {
            println!(
                "    {:>6} {:>12} {:>12} {:>12} {:>8} {:>8}",
                f.block_slab_id,
                f.physical_bytes,
                f.logical_bytes,
                f.live_bytes,
                f.live_basis_points,
                f.garbage_basis_points
            );
        }
        println!(
            "    totals: physical {physical}  logical(ever) {logical}  live {live}"
        );
        let summary = engine.block_store.slab_summary();
        println!(
            "    the published figures beside them: live_physical {}  reclaimable {}  total {}",
            summary.live_physical_bytes,
            summary.reclaimable_physical_bytes,
            summary.total_known_physical_bytes
        );
        (
            physical,
            logical,
            live,
            fractions.iter().map(|f| f.garbage_basis_points).max().unwrap_or(0),
        )
    };

    let (physical_before, logical_before, live_before, worst_garbage_before) =
        report_fractions("AFTER THE REWRITES");

    // ---- THE COLLECTOR'S OWN VIEW HAS TO BE NON-VACUOUS BEFORE ANY CLAIM ABOUT IT. A fractions
    // ---- read that returned nothing, or that reports no logical bytes, says nothing either way.
    assert!(
        logical_before > 0,
        "the per-slab fractions report no logical bytes at all, so the collector's view is empty \
         and this arm cannot say whether it sees garbage"
    );

