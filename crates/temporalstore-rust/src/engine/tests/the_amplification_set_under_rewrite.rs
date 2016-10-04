// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE AMPLIFICATION SET UNDER REWRITE, and what a reclaim cycle gives back.
//!
//! # EVERY FIGURE BEFORE THIS ONE WAS 1.0x BY CONSTRUCTION
//!
//! Every amplification arm in this area writes each key exactly once. On a write-once corpus
//! nothing is stale, so bytes-ever-written equals bytes-live and the rewrite ratio is 1.0x, and
//! physical equals live so the space ratio is 1.0x too. Those are not findings about good
//! behaviour, they are arithmetic about the fixture. The ceilings these ratios exist to be held
//! against -- 5.0x on space, 3.0x on write -- can only be approached by a workload that produces
//! DEAD bytes, and none had been run.
//!
//! This one overwrites the same keys repeatedly, reports the set after each round, then runs a real
//! reclaim cycle and reports it again. It answers three things the write-once corpus cannot:
//!
//!   1. does the rewrite ratio RISE with rewrite pressure, or is it inert?
//!   2. does space amplification climb as dead blocks accumulate, and how far?
//!   3. does a reclaim cycle bring it back, and back to what?
//!
//! # THERE ARE NOW THREE DENOMINATORS HERE, AND THE PINNED ASSERTION IS KEPT ON PURPOSE
//!
//! This arm was written with two: the slab-granular `live_physical_bytes` the published space ratio
//! divides by, and the block-granular live tally. A third has since landed -- the RECORDED one,
//! where an overwrite appends the byte range it retired to a per-slab list in the same call that
//! discharges the live tally, and `recorded_space_amplification_ppm` is published from it.
//!
//! THE SLAB-GRANULAR ASSERTION BELOW STILL READS 1.000x AND IS NOT LOOSENED. It was written to fail
//! and say what moved if the published denominator were ever re-pointed, and that has deliberately
//! NOT been done: re-pointing `space_amplification_ppm` would change a serde key consumers already
//! read, which the note on that field says must not happen silently. The correctly denominated
//! figure is published BESIDE it instead. So the assertion is still true of the field it names, and
//! what this arm gains is a third column that moves -- asserted, so that a record which stopped
//! recording would fail here as well as in its own arm.
//!
//! # EVERY RATIO DIVIDES STORED BYTES BY STORED BYTES
//!
//! There are two quantities in this store called logical. `logical_bytes_written` counts the user's
//! bytes as offered, BEFORE compression; a block's length -- what the live tally sums -- is what was
//! stored, AFTER it. A ratio with one on each side reports the compression factor multiplied by
//! whatever it claimed to measure. So the rewrite and space ratios here are stored-over-stored, and
//! the compression ratio is reported separately as the one quantity whose subject IS that mismatch.
//!
//! # THE VALUES ARE INCOMPRESSIBLE, AND THAT IS NOT INCIDENTAL
//!
//! `vec![b'v'; n]` compresses to almost nothing, which makes stored bytes framing rather than
//! content: on that fixture `live_physical_bytes` reads the same 70,000 B at value 256, 1,024 and
//! 4,096 alike, so a stored-byte denominator stops tracking the data. Every ratio here stands on
//! stored bytes, so the fixture must produce bytes that survive the write path.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_amplification_set_under_rewrite -- --ignored --nocapture

use crate::engine::reports::StorageManagerCycleRequest;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;
/// SEVERAL RECLAIM ROUNDS, NOT ONE. A compaction rewrites live data before releasing the copy it
/// replaces, so a single round can legitimately end holding more than it began with. Whether
/// rounds CONVERGE is the question, and one round cannot answer it.
const RECLAIM_ROUNDS: usize = 5;

/// Values that cannot compress, so a stored-byte denominator means something.
///
/// An inlined xorshift64 keyed by key index and round, so each round writes genuinely different
/// bytes -- a rewrite with the SAME bytes is a workload a store is allowed to recognise and skip,
/// which would make this arm measure deduplication instead of rewrite pressure.
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

struct Snapshot {
    label: String,
    stored_written: u64,
    logical_written: u64,
    /// SLAB-GRANULAR, and this is the one the published space ratio divides by.
    ///
    /// `summarize_slabs` adds a slab's whole `physical_bytes` to it when the slab is `Active` or
    /// `Sealed`, so a slab counts fully live however many of its blocks are dead.
    live_slab_granular: u64,
    /// BLOCK-GRANULAR, from the maintained `BlockSlabLiveIndex` tally.
    ///
    /// `SlabLiveTally::bytes` sums `BlockAddress::length` over the blocks the index still points
    /// at, and a block leaves the tally when its index entry does -- so an overwrite decrements
    /// it. `None` when the tally has not been derived, because a provisional zero must not be
    /// divided by.
    live_block_granular: Option<u64>,
    physical: u64,
    reclaimable: u64,
    /// RECORDED, not derived: the bytes an overwrite, a delete or an expiry sweep retired and that
    /// nothing has collected yet, appended where the live tally is discharged. Needs no cycle to
    /// have run, which is the whole difference from the two columns above.
    pending_discard: u64,
    /// `recorded_space_amplification_ppm` as the shard PUBLISHES it, so this arm and the report are
    /// reading one number rather than two that agree by coincidence.
    published_recorded_space_ppm: u64,
}

