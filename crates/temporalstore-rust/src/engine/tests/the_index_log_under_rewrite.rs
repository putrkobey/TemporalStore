// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE INDEX'S AMPLIFICATION UNDER REWRITE, and whether its log reclaim bounds it.
//!
//! # EVERY INDEX FIGURE SO FAR WAS TAKEN WHERE RECLAIM COULD NOT FIRE
//!
//! The published set reports the index log's storage-write ratio and the index's disk space ratio
//! on a corpus where every key is written once. The index log's reclaim has a TWO-PART gate --
//! `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` is 768 KiB, and at least 40% of the log must be
//! reclaimable -- and that corpus produces on the order of 105 KB of log. An order of magnitude
//! short of the first condition, so reclaim could not have fired even if it had been asked, and
//! neither ratio could move.
//!
//! That is the same shape as the payload side, where a write-once corpus made both ratios 1.0x by
//! arithmetic rather than by merit. This rewrites until the log crosses the threshold, reports the
//! set after every round, then runs the cycle and reports what reclaim did.
//!
//! # THE ON-DISK NUMERATOR EXCLUDES THE WAL, DELIBERATELY
//!
//! The engine keeps `wals/` under the directory it is handed for indexes. Summing that directory
//! put 262,144 B of write-ahead log inside an index ratio once already -- 71% of the numerator --
//! so the walk here partitions the WAL out and reports it beside the index's own files rather than
//! folding it in.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_index_log_under_rewrite -- --ignored --nocapture

use crate::engine::reports::{
    StorageManagerCycleRequest, DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD,
};
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
/// Enough rounds to carry the log past 768 KiB: one pass over this corpus writes roughly 105 KB of
/// index log, so eight passes clear the threshold with margin.
const ROUNDS: usize = 8;

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
