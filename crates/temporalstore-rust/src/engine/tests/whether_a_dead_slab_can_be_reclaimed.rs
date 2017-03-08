// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A WHOLLY DEAD SLAB CAN BE RECLAIMED, and what the footprint does when it is.
//!
//! # EVERY EARLIER ARM MEASURED A RATIO THAT COULD NOT MOVE
//!
//! `the_amplification_set_under_rewrite` showed the published space ratio pinned at 1.000x while
//! the block-granular figure reached 5.000x, and showed five reclaim rounds each adding one copy
//! of the live set. Neither says whether the footprint can come DOWN, for two reasons:
//!
//!   * every key landed in one slab, so overwriting them all left that slab holding the live
//!     copies -- no slab could become wholly dead, and a collector with nothing to collect is not
//!     evidence either way;
//!   * the purge phase was never driven. `purge_delayed_destroy` is a field of
//!     `StorageLifecycleRequest`, NOT of `StorageManagerCycleRequest`, so a fixture driving only
//!     the cycle runs the compaction phase alone however many rounds it runs.
//!
//! # SO THIS KILLS ONE WHOLE SLAB
//!
//! Write until several slabs are sealed, find the oldest, overwrite only the keys whose blocks
//! live in it, and leave every other slab untouched. That slab is then entirely dead, which is the
//! only shape where a purge has something to do.
//!
//! HOW MUCH TO WRITE IS DISCOVERED, not hardcoded against a capacity constant: the arm writes
//! batches until the summary reports sealed slabs. A fixture built on a guessed threshold that is
//! never reached runs green and measures nothing.
//!
//! # AND THE AGE GATE IS OVERRIDDEN DELIBERATELY
//!
//! `DELAYED_DESTROY_MIN_AGE_MS` is one hour, so no test can outwait the quarantine. The final purge
//! uses `purge_delayed_destroy_slabs_older_than(0)` -- an explicit override of the production
//! delay, which existing block-store tests also use. What it proves is narrower than a production
//! purge: that the slab was ELIGIBLE and the destroy path works, not that the delay would elapse.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib whether_a_dead_slab_can_be_reclaimed -- --ignored --nocapture

use crate::engine::reports::{StorageLifecycleRequest, StorageManagerCycleRequest};
use crate::types::{Command, ExecuteRequest};

const VALUE: usize = 4_096;
/// Keys per slab. Small, because the slab boundary is forced by a roll rather than reached by
/// volume -- 20,000 keys at 4 KiB wrote 82 MB and sealed nothing.
const BATCH: usize = 300;
/// Slabs to fill and seal before the one that dies. Two sealed plus one active is the minimum
/// shape: a slab that can die while others live.
const SEALED_SLABS: usize = 2;

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
#[ignore = "seals several slabs then kills one; run by name"]
fn whether_a_dead_slab_can_be_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
