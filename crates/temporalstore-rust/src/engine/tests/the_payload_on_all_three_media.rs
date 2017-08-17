// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAYLOAD ON ALL THREE MEDIA — logical, in memory, on disk — against ONE denominator.
//!
//! # WHY ONE DENOMINATOR
//!
//! The amplification set reports ratios. This reports the three QUANTITIES the payload occupies,
//! each divided by the same thing: the user bytes offered to the store. Every payload figure that
//! went wrong in this area went wrong because its denominator was not the one its numerator was in
//! — a write ratio that was really the compression factor, a cache figure multiplied by that same
//! factor, an index ratio dividing its own numerator.
//!
//!   * **logical** — `logical_bytes_written`, the bytes the caller handed over, before compression.
//!   * **on disk** — the slab files, which depend entirely on how the values compress.
//!   * **in memory** — `cache.memory_bytes`, which sums cached value lengths.
//!
//! # BOTH FIXTURES, BECAUSE ONE OF THE THREE DEPENDS ON THE DATA
//!
//! A run of `b'v'` compresses to almost nothing, so the disk figure reads far below its logical
//! size; incompressible bytes make it slightly above. Reporting one fixture would state a disk cost
//! that is really a statement about the letter `'v'`.
//!
//! The memory figure is indifferent to the distribution since #2279 made the cached payload a
//! msgpack byte string that charges by length alone. Before that change the two fixtures differed
//! there too, by half the value size, so the agreement is worth showing rather than assuming.
//!
//! # THE SHAPE THIS PUTS IN ONE PLACE
//!
//! Memory used to be the dearest of the three media, at 2.082x, because a point read filled TWO
//! cache layers that both held the payload: the command response under `CacheKey::string` and the
//! block frame under `CacheKey::page_with_slot`. **The logical copy is gone**, so memory is now
//! exactly 1.000x — one copy, charged by length — and disk is the only medium whose figure depends
//! on the data.
//!
//! WHICH COPY WENT WAS NOT SETTLED BY THE FRAME-SHARING COUNT, though an earlier reading of this
//! arm said it was. `how_many_keys_a_cached_frame_serves` shows a frame never amortises across
//! keys; it does not show the frame entry is unreachable. The frame key is read by 25 call paths
//! and is the key of the persistent block cache, while the response copy was memory-only with a
//! single reader — so the response copy was the one to drop, and it was the larger of the two by the
//! msgpack envelope. `what_the_frame_layer_saves_under_eviction` holds the before-and-after.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_payload_on_all_three_media -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;

fn noisy_value(index: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x1234_5678_9ABC_DEF0)
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

/// (label, logical, on disk, in memory)
fn weigh(compressible: bool) -> (&'static str, u64, u64, u64) {
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
                key: format!("media-{index:06}"),
                value: if compressible {
                    vec![b'v'; VALUE]
