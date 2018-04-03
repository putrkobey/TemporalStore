// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one shard LOAD writes, and why the whole-index encode inside it is not the same defect.
//!
//! WHY THIS EXISTS. `replay_wal_into_shard_windowed` ends by encoding the whole served index and
//! writing it (`lifecycle.rs`' `let _ = self.persist_index_bytes(shard_id, &index_bytes)`), which
//! is the same whole-store encode that a per-operation site is charged for. It is NOT the same
//! shape, and the difference is a count rather than an argument: this one happens ONCE PER LOAD,
//! inside a lock-held block that has just run `promote_model_maps_to_bucket_index_authority` and
//! `rebuild_bucket_first_index` over the whole shard, in an operation that decoded the whole index
//! to begin with. An interval in front of it would never fire differently -- a countdown that is
//! serde-skipped reads as DUE on every fresh load, and there is only ever one of these per load --
//! so putting one there would change nothing and cost a knob.
//!
//! IT IS PRODUCTION-REACHABLE, which is the claim that had to be established before it could be
//! left alone: the arm below reaches it on an ordinary reopen of an ordinary store, with no test
//! hook and no `#[cfg(test)]` setter in the path.
//!
//! THE ATTRIBUTION IS A CONTROL, NOT A READING OF THE CALL GRAPH. The site is reached only when
//! `replayed_through > watermark` -- a WAL tail beyond what the persisted index already covers. A
//! store flushed with `flush_shard_index()` before it closed has no such tail, so the second arm
//! is the same load with the condition false, and the difference between the two arms is this
//! site.
//!
//! THE COUNTERS ARE THREAD-LOCAL and the load runs on the calling thread.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 37;

/// Seed through the async-storage arm, the same regime the publish measurement uses, so the
//// records are in the log and the base index on disk is behind them.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: format!("loaded-{item:06}"),
                    value: vec![b'v'; 128],
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
