// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LIVE AND STALE SHARES ARE A PARTITION OF THE BYTES ON DISK, and must behave like one.
//!
//! # WHY A PARTITION IS A STRONGER PUBLISHED SHAPE THAN A RATIO
//!
//! `live_bytes_ppm` and `stale_bytes_ppm` are computed over the same whole —
//! `non_purged_physical_bytes` — so they **sum to 1,000,000** within integer floor rounding. That
//! identity is checkable from the published numbers alone.
//!
//! None of the four amplification ratios on the same struct has that property. Each divides one
//! quantity by another with nothing constraining the answer, which is why
//! `logical_write_amplification_ppm` could carry a name that did not describe its own arithmetic for
//! as long as it did: no identity over the set would have failed, and
//! `the_published_amplification_ratios_are_the_ratio_they_name` pins self-consistency rather than
//! agreement with any definition.
//!
//! # TWO ASSERTIONS, BECAUSE EACH CATCHES WHAT THE OTHER CANNOT
//!
//!   * **The parts sum.** A term computed over the wrong whole fails here immediately.
//!   * **The stale share rises under rewrite, while the live set does not grow.** A figure can sum
//!     correctly and still be constant for the wrong reason — which is exactly how the
//!     slab-granular `space_amplification_ppm` passed every check it had while pinned at 1.000x
//!     over five rewrites of every key. Rewriting the same keys is the only instrument that
//!     separates "nothing is stale" from "stale is not being counted", and asserting BOTH halves is
//!     what makes either one evidence.
//!
//! # THE FLOOR IS EXACTLY ZERO, WHICH THIS ARM FOUND AND ITS OWN DOC GOT WRONG FIRST
//!
//! This section predicted a small non-zero stale share on the first round, because the live term is
//! block-payload bytes and the whole is file bytes, so framing would fall on the stale side.
//! **Measured: exactly 0.00%**, with the parts summing to 1,000,000.
//!
//! The terms agree because `live_block_bytes` sums `BlockAddress::length` — what was STORED, after
//! compression and including the block envelope — and `non_purged_physical_bytes` is the file size
//! of those same bytes. Both sides are stored bytes.
//!
//! Framing is real against a different denominator: 536,000 B on disk for 512,000 B of USER bytes
//! (`the_payload_on_all_three_media`), which is stored bytes over PRE-COMPRESSION bytes — the
//! mixed-unit division these fields exist to avoid. The equality is now asserted below, so if a
//! framing term ever does appear on one side it fails here instead of looking like a documented
//! floor.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_live_and_stale_shares_are_a_partition -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 1_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;

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

fn as_pct(ppm: u64) -> f64 {
    ppm as f64 / 10_000.0
}

#[test]
#[ignore = "rewrites 1,000 keys four times; run by name"]
fn the_live_and_stale_shares_are_a_partition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // (label, live_ppm, stale_ppm, live bytes, whole bytes)
    let mut rows: Vec<(String, u64, u64, u64, u64)> = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("partition-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        let published = engine
            .shard_stats(1)
            .expect("a loaded shard reports stats")
            .storage;
        rows.push((
            format!("after {} write(s)", round + 1),
            published.live_bytes_ppm,
            published.stale_bytes_ppm,
            published.live_block_bytes,
            published.non_purged_physical_bytes,
        ));
    }

    println!();
    println!("  THE STORED IMAGE AS PARTS OF A WHOLE -- {KEYS} keys x {VALUE} B, incompressible");
    println!();
    println!(
        "    {:<20} {:>11} {:>11} {:>10} {:>10} {:>12}",
        "", "live B", "whole B", "live %", "stale %", "sum (ppm)"
    );
    for (label, live_ppm, stale_ppm, live, whole) in &rows {
        println!(
            "    {:<20} {:>11} {:>11} {:>9.2}% {:>9.2}% {:>12}",
            label,
            live,
            whole,
            as_pct(*live_ppm),
            as_pct(*stale_ppm),
            live_ppm + stale_ppm
        );
    }
    println!();

