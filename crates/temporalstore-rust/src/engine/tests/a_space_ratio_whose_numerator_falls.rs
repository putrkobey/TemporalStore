// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SPACE RATIO THAT WOULD MOVE, measured from counters that exist today.
//!
//! # THE PUBLISHED ONE IS BLIND AT BOTH ENDS
//!
//! `space_amplification_ppm` is `total_known_physical_bytes / live_physical_bytes`, and each half
//! fails to reflect reclamation:
//!
//!   * the DENOMINATOR is slab-granular. `summarize_slabs` adds a slab's whole `physical_bytes`
//!     when its state is `Active` or `Sealed`, so a slab counts fully live however many of its
//!     blocks are dead. Measured: it tracked `bytes_written` to the byte across five rewrites of
//!     every key, holding the ratio at 1.000x where the block-granular truth was 5.000x.
//!   * the NUMERATOR is accumulated BEFORE the state match, so a `Purged` slab whose files are
//!     deleted still contributes its full size. Measured: a purge freed 1,232,400 B and
//!     `reclaimable` fell to 0, while this figure did not move.
//!
//! Both are monotonically increasing. The field name is honest -- "total **known**" is a
//! known-slabs figure, not disk occupancy -- so what is wrong is reading it as a space numerator.
//!
//! # THE CORRECT PAIR NEEDS NO NEW ACCOUNTING
//!
//! ```text
//! numerator   = total_known_physical_bytes - purged_physical_bytes   // files still on disk
//! denominator = the maintained block-granular live tally             // blocks the index points at
//! ```
//!
//! The numerator falls when a purge deletes files; the denominator falls when an overwrite retires
//! a block's index entry. Both move in the direction the ratio claims to measure, and both are
//! already computed -- `purged_physical_bytes` on the slab summary, the tally on the shard.
//!
//! # ITS FLOOR IS ABOVE 1.0x, AND THAT IS A PROPERTY RATHER THAN A DEFECT
//!
//! The numerator is FILE bytes and the denominator is BLOCK PAYLOAD bytes, so the ratio includes
//! slab headers, block envelopes and record framing the denominator excludes. A reader expecting
//! 1.0x at zero garbage will misread it. This is the same structure the ratio is modelled on --
//! bytes on disk per byte of live data -- so it is stated rather than corrected.
//!
//! # IT IS PUBLISHED NOW, AND THIS ARM CHECKS THE FIELD
//!
//! This used to say that publishing meant a field on `ShardCanonicalStorageStats`, an assignment in
//! `persistence.rs` and another identity in the re-derivability arm, with another lane mid-flight in
//! all three. That lane landed, and the three pieces exist: `block_space_amplification_ppm`, its
//! numerator `non_purged_physical_bytes`, and the fourth identity in
//! `every_published_ratio_is_rederivable`.
//!
//! So the final section no longer measures a proposal — it **restates both terms independently** (a
//! local fold over the tally, a local subtraction over the slab summary) and asserts the published
//! field equals them. An arm that computes a figure the engine also publishes and never compares
//! the two is exactly how a ratio carried one name while computing another for as long as it did.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib a_space_ratio_whose_numerator_falls -- --ignored --nocapture

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

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 { 0 } else { numerator.saturating_mul(1_000_000) / denominator }
}

fn as_x(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

#[test]
#[ignore = "rewrites 1,000 keys four times; run by name"]
fn a_space_ratio_whose_numerator_falls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // (label, published numerator, published denominator, proposed numerator, proposed denominator)
    let read = |label: String| -> (String, u64, u64, u64, u64) {
        let slabs = engine.block_store.slab_summary();
        let live_blocks = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            if shard.bucket_index.block_slab_live.is_ready() {
                shard
                    .bucket_index
                    .block_slab_live
                    .iter()
                    .fold(0_u64, |total, (_, tally)| total.saturating_add(tally.bytes))
            } else {
                0
            }
        };
        (
            label,
            slabs.total_known_physical_bytes,
            slabs.live_physical_bytes,
            slabs
                .total_known_physical_bytes
                .saturating_sub(slabs.purged_physical_bytes),
            live_blocks,
        )
    };

    let mut rows = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("space-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
