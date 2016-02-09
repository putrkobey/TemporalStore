// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one visibility publish writes, against what it published.
//!
//! WHY THIS EXISTS. `publish_shard_index_snapshot_for_keys` ends by encoding the WHOLE served
//! index out of the shard and writing it through `persist_index_bytes`, however few keys the call
//! named -- its own comment calls that "the O(n^2) rewrite". The call takes a key list and
//! republishes only those keys' pages, so the PAGE half of the work is already O(published). This
//! measures the other half: the encode, swept over the size of the store with the published set
//! held at one key.
//!
//! THE DENOMINATOR IS KEYS PUBLISHED, and the sweep varies the store underneath it. If the encoded
//! bytes track the store while the published set does not move, the cost of a publish is set by
//! the size of the STORE and not by the size of the publish.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A publish that found nothing to republish returns zero
//! and writes nothing, and would report a flattering zero here; every arm asserts the seed landed
//! and asserts the publish reported bytes before any ratio is taken.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the encode happens
//! on the calling thread, so a single-threaded arm sees all of it and only it.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 29;

/// Seed `keys` string records THROUGH THE ASYNC-STORAGE ARM, which is the only write path that
/// mints a log-resident block address -- `append_value_inner` writes straight to the block store
/// unless `async_storage` is set. A publish's whole job is to move those blocks out, so a fixture
/// seeded through the synchronous arm leaves it nothing to republish and it reports zero.
///
/// This is also the regime the production caller runs in: the proxy's `matrixark_publish_visibility`
/// exists for stores opened with `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let key = format!("publish-{item:06}");
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key,
                    value: vec![b'v'; 128],
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
    }
}

/// One reading: the store size, how many keys the publish named, and what the publish encoded.
struct PublishCost {
    keys: usize,
    named: usize,
    encodes: u64,
    encode_bytes: u64,
    reported_bytes: usize,
}

/// Publish `named` keys out of a store of `keys`, and read what the publish encoded.
fn publish_cost(keys: usize, named: usize) -> PublishCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
