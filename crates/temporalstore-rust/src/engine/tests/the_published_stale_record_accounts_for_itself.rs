// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE STALE RECORD'S TWO PARTITIONS, CHECKED FROM THE PUBLISHED STATUS.
//!
//! # WHY NOT THE ARM THAT ALREADY CHECKS THEM
//!
//! `the_overwrite_records_what_it_retired` asserts both identities over `SlabStaleSummary`, inside
//! the engine, where every term is in hand. This asserts them over `ShardCanonicalStorageStats` —
//! what a consumer receives — and until the eight unpublished figures were carried onto it, neither
//! identity was checkable there, because each was missing a term.
//!
//! The two arms fail for different reasons, which is the point of having both: the inner one if the
//! record's arithmetic breaks, this one if the status stops carrying a term that arithmetic needs.
//! **An assignment that drops a field passes the inner arm.**
//!
//! # THE TWO PARTITIONS
//!
//! ```text
//! pending_discard_bytes + stale_bytes_discarded          == stale_bytes_recorded
//! by_overwrite + by_removal + by_sweep                   == stale_bytes_recorded
//! ```
//!
//! Integers on both sides, so these are exact — no rounding, no tolerance. A partition constrains
//! itself in a way a ratio cannot, which is why the published figures are shaped this way.
//!
//! # AND A SUM THAT ACCOUNTS CAN STILL BE ATTRIBUTED WRONGLY
//!
//! Three terms summing to the right total, with the weight in the wrong term, is a passing sum and a
//! wrong answer. So this also asserts that the cause the fixture CREATES is the cause the status
//! attributes: a pure-overwrite workload must put every retired byte in `by_overwrite` and none in
//! the other two. That is the shape of a cause-label error measured once at 100% wrong — a pure
//! overwrite corpus reporting its bytes under removal — and a sum alone would not have caught it.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_published_stale_record_accounts_for_itself -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 1_000;
const VALUE: usize = 256;
const ROUNDS: usize = 3;

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

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128 * 1_000_000u128) / denominator as u128) as u64
}

#[test]
#[ignore = "rewrites 1,000 keys three times; run by name"]
fn the_published_stale_record_accounts_for_itself() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A PURE OVERWRITE WORKLOAD, deliberately: no deletes and no expiry, so every retired byte has
    // exactly one possible cause and a misattribution cannot hide behind a mixed fixture.
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("accounted-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }

    let s = engine
        .shard_stats(1)
        .expect("a loaded shard reports stats")
        .storage;

    println!();
    println!("  THE PUBLISHED STALE RECORD -- {KEYS} keys x {VALUE} B, {ROUNDS} rewrites, overwrite only");
    println!();
    println!("    recorded                       {:>12}", s.stale_bytes_recorded);
    println!("      pending                      {:>12}", s.pending_discard_bytes);
    println!("      discarded                    {:>12}", s.stale_bytes_discarded);
    println!("      ---- sum                     {:>12}", s.pending_discard_bytes + s.stale_bytes_discarded);
    println!();
    println!("      by overwrite                 {:>12}", s.stale_bytes_by_overwrite);
    println!("      by removal                   {:>12}", s.stale_bytes_by_removal);
    println!("      by sweep                     {:>12}", s.stale_bytes_by_sweep);
    println!(
        "      ---- sum                     {:>12}",
        s.stale_bytes_by_overwrite + s.stale_bytes_by_removal + s.stale_bytes_by_sweep
    );
    println!();
    println!("    pending block refs             {:>12}", s.pending_discard_block_refs);
    println!("    merged ranges                  {:>12}", s.merged_discard_range_count);
    println!("    compaction candidate ranges    {:>12}", s.compaction_candidate_range_count);
    println!("    fragmented ranges              {:>12}", s.fragmented_range_count);
    println!("    DROPPED ranges (placement)     {:>12}", s.dropped_discard_range_count);
    println!("    slabs tracked / pruned         {:>12} / {}", s.stale_slabs_tracked, s.stale_slabs_pruned);
    println!("    discard backlog                {:>12} ppm", s.discard_backlog_ppm);
    println!();

    // ---- NON-VACUITY. Every assertion below holds at zero.
