// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE USER-BYTE WRITE RATIO: COMPRESSIBILITY CANCELS, AND COVERAGE IS PUBLISHED.
//!
//! `user_write_amplification_ppm` is `logical_bytes_written / live_user_bytes` -- uncompressed
//! bytes written per uncompressed byte kept. It is the one write ratio whose halves are both in
//! the caller's units, which is what makes it measure REWRITE where
//! `storage_write_amplification_ppm` measures the CODEC.
//!
//! # THE PROPERTY THAT DISTINGUISHES IT, ASSERTED ON TWO STORES
//!
//! Two stores are written with the SAME user bytes and wildly different compressibility. Their
//! storage-write figures must differ -- that is the codec showing up -- and this ratio must be
//! IDENTICAL, because compression appears in neither of its halves. One store alone cannot show
//! that: a single compressibility makes every ratio agree by construction, which is the trap the
//! denominator arm hit first.
//!
//! # AND COVERAGE IS A FIRST-CLASS TERM
//!
//! The size class is installed by the string write path. An object written another way, or decoded
//! from an image predating the field, carries none and contributes to neither term -- which makes
//! the ratio read HIGH, not low, and nothing in the ppm value says so. So the arm asserts the
//! coverage count equals the live object count on a corpus written entirely through that path, and
//! asserts it FALLS when a row is deleted. A ratio whose denominator can be silently partial needs
//! its completeness published beside it.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_user_byte_write_ratio -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 300;
/// EXACTLY REPRESENTABLE AS A SIZE CLASS -- `704 == (16 + 6) << 5` -- so every figure below is an
/// equality rather than a tolerance.
///
/// This is deliberate and it narrows what the arm covers: at 700 bytes the class rounds to 704 and
/// the ratio reads 2.983x instead of 3.000x, which is the encoding behaving correctly and would
/// force every assertion here into a band. The rounding itself is measured by
/// `what_a_live_user_bytes_denominator_would_cost` on lengths chosen to fall BETWEEN classes; this
/// arm tests the plumbing -- that the halves are in user units, that coverage is counted, that a
/// delete subtracts -- and wants exact arithmetic to do it.
const VALUE_LEN: usize = 704;

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

fn noisy(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn engine_in(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// Write `rounds` passes over the same `KEYS` keys, each value `VALUE_LEN` bytes.
fn seed(engine: &TemporalEngine, rounds: usize, compressible: bool) {
    for round in 0..rounds {
        for index in 0..KEYS {
            let value = if compressible {
                vec![b'v'; VALUE_LEN]
            } else {
                noisy((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ round as u64, VALUE_LEN)
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("uw-{index:06}"), value },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }
}

#[test]
fn the_user_byte_write_ratio() {
    const ROUNDS: usize = 3;
    let live_user_bytes_expected = (KEYS * VALUE_LEN) as u64;

    let flat_dir = tempfile::tempdir().expect("tempdir");
    let noisy_dir = tempfile::tempdir().expect("tempdir");
    let flat = engine_in(flat_dir.path());
    let sharp = engine_in(noisy_dir.path());
    seed(&flat, ROUNDS, true);
    seed(&sharp, ROUNDS, false);

    let flat_stats = flat.shard_stats(1).expect("stats").storage.clone();
    let sharp_stats = sharp.shard_stats(1).expect("stats").storage.clone();

    println!();
    println!("  THE SAME USER BYTES, TWO COMPRESSIBILITIES");
    println!(
        "    {:<42} {:>14} {:>14}",
        "", "compressible", "incompressible"
    );
