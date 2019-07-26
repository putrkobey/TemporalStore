// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for phase-1 flat append (`LocalWriteAheadLogStore::flat_append`): the two per-write
//! O(store) costs that
//! run under the engine `shards` write lock -- the WAL append's full-file `last_wal_sequence_at`
//! rescan and the per-execute `promote_model_maps_to_bucket_index_authority` reconcile scan -- are
//! made O(1) so phase-1 stops aging O(n) with data size. These tests prove:
//!   (a) AGING: with the gate ON, neither scan grows with the write count (both counters stay a
//!       small constant over 5k writes), while with the gate OFF each fires once per command
//!       (O(writes)) -- and every write is still byte-exact;
//!   (b) DURABILITY/RECOVERY: with the gate ON, dropping the engine and reloading over the same
//!       dirs (WAL replay) recovers every acked write byte-exact (the fast-append length cache is
//!       cold on the fresh process, so the first append reconciles via the full scan);
//!   (c) COALESCING: with the gate ON plus the concurrent-commit path, group commit still engages
//!       (fewer fdatasyncs than writes) and all writes are correct.
#![allow(clippy::all)]
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

// The gates are process-global env vars; serialize the sub-tests that toggle them so an OFF-baseline
// measurement never observes a sibling's ON window (and vice versa). Other tests are unaffected: the
// gate only alters an internal fast path and the per-engine/per-store counters read here are
// isolated to the engine under test.

fn new_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn write_n_strings(engine: &TemporalEngine, n: usize) {
    for i in 0..n {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            },
        });
        assert!(resp.status.ok, "write k{i} must ack ok: {:?}", resp.status);
    }
}

fn assert_present_sampled(engine: &TemporalEngine, n: usize, step: usize) {
    let mut i = 0;
    while i < n {
        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("k{i}") },
        });
        match get.response {
            CommandResponse::Bytes { value: Some(value) } => {
                assert_eq!(value, format!("v{i}").into_bytes(), "wrong value for k{i}")
            }
            other => panic!("lost write k{i}: {other:?}"),
        }
        i += step;
    }
}

/// DIAGNOSTIC (ignored by default): time per-1000-write cost under the gate to see whether phase-1
/// is flat. Run with `cargo test ... phase1_flat_diag_timing -- --ignored --nocapture`.
#[test]
#[ignore]
