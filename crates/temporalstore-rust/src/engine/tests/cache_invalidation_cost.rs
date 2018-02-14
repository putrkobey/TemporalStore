// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE THE THIRTY-FIVE ALLOCATIONS OF ONE CACHE INVALIDATION GO.
//!
//! `alloc_class_scale` measured a value write and found one class larger by CALL COUNT than the
//! other eight together: `cache_invalidation`, 35.06 allocations for 995 bytes,
//! flat per record at 2,000 and at 20,000 and again at 400,000 and 4,000,000. Flat is the
//! important word. This is not a sink that grows with the store; it is a constant paid on every
//! single write, and nobody had asked what it is made of.
//!
//! This module asks. It is a decomposition by DIFFERENCE over the serving cache's public surface,
//! which is the only instrument available: the charge sits in `invalidate_cache_key`, but the work
//! is in a separate crate pinned by revision, so no counter can be placed inside the thing being
//! measured.
//!
//! THE LADDER. Same cache, built exactly as `TemporalEngine::with_local_dirs` builds it, cold, and
//! every key absent from it -- which is the regime of the write path that pays the 54, because a
//! write invalidates a key the store has just written and no reader has yet asked for.
//!
//! ```text
//!   arm                         what it adds over the arm above
//!   key_only                    CacheKey::string, and nothing else
//!   peek                        + the read-side tier probe
//!   invalidate_memory_only      + memory and pmem map removal, pins, access record
//!   invalidate                  + THE PERSISTENCE BOOKKEEPING: disk index, manifest op,
//!                                 pmem delete line, ssd block delete
//! ```
//!
//! WHAT IS NOT IN THE 35, and it matters for reading the table: the class is charged INSIDE
//! `invalidate_cache_key`, whose key argument is built by the caller. `CacheKey::string` is
//! therefore outside it. The quantity to compare against 35.06 is `invalidate` MINUS `key_only`.
//!
//! THE RECONCILIATION IS AGAINST AN INSTRUMENT THIS LADDER DOES NOT FEED. Every figure here comes
//! from `alloc_probe::Probe`, the process-wide counter, over a synthetic loop. The 35.06 it is
//! checked against comes from the per-class ledger, charged inside the primitive, during a real
//! `batch_execute` ingest in `alloc_class_scale`. Two instruments, two workloads, one number; the
//! residual between them is reported and asserted small rather than computed from the rows.
//!
//! THE STORE PATH IS HELD CONSTANT AND IT IS `/tmp/.tmpXXXXXX`, fifteen characters, the same
//! `tempfile::tempdir()` the class table is taken under. #1922 found by accident that what a write
//! allocates MOVES WITH THE LENGTH OF THE STORE'S DIRECTORY PATH -- `cache_invalidation` went
//! 1,474 -> 1,546 -> 1,606 bytes a record across three paths of 15, 27 and 37 characters -- and
//! read it as a one percent debug-against-release difference until the pair was run properly. One
//! test here varies the path DELIBERATELY, to say which part of the cost that effect lives in.

// Everything below except the two-build guard at the end of this file is measurement apparatus,
// and measurement apparatus only exists in a build that installs the counting allocator. Gated
// item by item rather than as a whole module, because one guard here MUST run in a build without
// the feature -- that is the whole point of it.
#[cfg(feature = "alloc-probe")]
use super::*;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::{AllocCounts, Probe};
#[cfg(feature = "alloc-probe")]
use matrixcache::{CacheKey, MultiLayerCache};

#[cfg(feature = "alloc-probe")]
const SMALL: usize = 2_000;
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 20_000;
#[cfg(feature = "alloc-probe")]
const MEMORY_CAPACITY: usize = 64 * 1024 * 1024;
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;
#[cfg(feature = "alloc-probe")]
const BATCH: usize = 100;

/// The key shape the ingest that measured the 54 uses: `k-` and eight digits, on shard 1.
///
/// Key LENGTH is an input to this measurement -- `CacheKey::string` copies the key and the
/// manifest encoders format it again -- so a ladder built on keys of a different width would
/// report a different number and reconcile against nothing.
#[cfg(feature = "alloc-probe")]
fn corpus(n: usize) -> Vec<String> {
    (0..n).map(|index| format!("k-{index:08}")).collect()
}

/// Allocations attributable to `work`. The corpus is built BEFORE the span opens.
#[cfg(feature = "alloc-probe")]
fn measure(work: impl FnOnce()) -> AllocCounts {
    let probe = Probe::start();
    work();
    probe.stop()
}

/// A cache built the way the engine builds its serving cache.
#[cfg(feature = "alloc-probe")]
fn engine_cache(dir: &std::path::Path) -> MultiLayerCache {
    MultiLayerCache::new(MEMORY_CAPACITY, dir)
}

/// What the SHIPPED call site charges, per stored record, on a real ingest.
///
/// This is the other instrument. The ladder above drives the cache crate directly and counts with
/// the process-wide span counter; this drives `batch_execute` and reads the per-class ledger that
/// `invalidate_cache_key` charges from inside itself. Neither feeds the other, and the difference
/// between them is the residual -- which is the only reason a residual here means anything.
///
/// It is also what makes this module a guard on the write path rather than a microbenchmark:
/// a change that stops the engine invalidating, or stops it charging what it invalidates, moves
/// this number and not the ladder.
#[cfg(feature = "alloc-probe")]
fn in_situ_charge_per_record(records: usize) -> (f64, f64) {
    use crate::alloc_probe::{AllocClass, ClassSpan};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        MEMORY_CAPACITY,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // One write before the span, so a lazily built table is not charged to this class.
    engine.execute(crate::types::ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm".to_string(),
            value: vec![7u8; VALUE_BYTES],
        },
    });

    // THE CORPUS IS BUILT BEFORE THE SPAN OPENS, for the reason `alloc_class_scale` gives: a probe
    // that assembles its commands inside the measured window charges the store for its own fixture.
    let keys = corpus(records);
    let mut batches: Vec<Vec<Command>> = Vec::new();
    for chunk in keys.chunks(BATCH) {
        batches.push(
            chunk
                .iter()
                .map(|key| Command::StringSet {
                    key: key.clone(),
                    value: vec![7u8; VALUE_BYTES],
                })
                .collect(),
        );
    }

    let span = ClassSpan::open();
    for commands in batches.drain(..) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "ingest failed: {:?}", response.status);
    }
    let counts = span
        .close()
        .expect("built with `alloc-probe`, so these are measurements");
    let row = counts.classes.row(AllocClass::CacheInvalidation);
    drop(engine);
    (
        row.allocs as f64 / records as f64,
        row.alloc_bytes as f64 / records as f64,
