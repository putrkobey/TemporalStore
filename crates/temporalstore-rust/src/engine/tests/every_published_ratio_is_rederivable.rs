// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY PUBLISHED AMPLIFICATION RATIO IS RE-DERIVABLE FROM PUBLISHED COUNTERS.
//!
//! # WHY THIS IS THE PROPERTY AND NOT THE FIELD COUNT
//!
//! The shard status publishes four amplification ratios, and until the denominators landed beside
//! them a consumer could re-derive only one. `logical_bytes_written` -- the denominator of the
//! storage-write ratio -- appeared in `control.rs` exactly once, inside a doc comment explaining
//! that ratio, and never as a field. The lifetime ratio's denominator, the live blocks the index
//! still points at, was not published either.
//!
//! That matters because every amplification figure corrected in this area was wrong in a way no
//! range check catches, and each was caught by having the raw counters to hand:
//!
//!   * a payload write ratio that was really the compression factor, its halves in different units;
//!   * a cache figure that multiplied the cache's own overhead by that same factor;
//!   * an index disk ratio that divided the image BY ITSELF -- which passes a range check, a
//!     non-zero check and a self-consistency identity alike.
//!
//! A consumer holding ratios without counters can find none of those. One that can recompute every
//! ratio from published fields can. This arm makes that recomputation a gate.
//!
//! # EXACT EQUALITY, AND DENOMINATORS FIRST
//!
//! The comparisons are exact: these are integer ppm figures produced by the same ratio helper the
//! engine uses, so a mismatch means the published counter is not the one the ratio divided by --
//! exactly the defect being guarded, and exactly what a tolerance would admit.
//!
//! Each denominator is required non-zero before its identity is checked, because the engine
//! reports 0 ppm on a zero denominator by convention and `0 == 0` would hold vacuously. The arm
//! prints which ratios it exercised, so a pass cannot mean it checked nothing.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib every_published_ratio_is_rederivable -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 400;
const VALUE: usize = 256;

/// The same arithmetic the engine's own `ppm_ratio` performs, including its zero-denominator
/// convention. Restated here rather than borrowed, so the arm compares the published figure
/// against an INDEPENDENT computation instead of against the function that produced it.
fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

#[test]
#[ignore = "seeds a store and reads its published status; run by name"]
fn every_published_ratio_is_rederivable_from_published_counters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("rederive-{index:06}"),
                value: vec![b'v'; VALUE],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let published = engine
        .shard_stats(1)
        .expect("a loaded shard reports stats")
        .storage;

    println!();
    println!("  PUBLISHED COUNTERS");
    println!("    bytes_written          {:>12}", published.bytes_written);
    println!("    logical_bytes_written  {:>12}", published.logical_bytes_written);
    println!("    live_block_bytes       {:>12}", published.live_block_bytes);
    println!("    storage_slab_total     {:>12}", published.storage_slab_total_bytes);
    println!("    storage_slab_used      {:>12}", published.storage_slab_used_bytes);
    println!("    non_purged_physical    {:>12}", published.non_purged_physical_bytes);
    println!("    pending_discard_bytes  {:>12}", published.pending_discard_bytes);
