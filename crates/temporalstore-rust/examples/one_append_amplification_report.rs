// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE APPEND COSTS, as a line rather than a point.
//!
//! # WHY A SWEEP AND NOT ONE WRITE
//!
//! One value size cannot separate the per-entry envelope from the per-byte cost. At 5 bytes of
//! payload a 90-byte envelope reads as 19x; at 4 KiB the same envelope reads as 1.02x. Both are
//! "the amplification" and neither is a fact about the system. So the value size is swept and
//! `stored = rate * payload + envelope` is fitted over it, with the residual printed so a
//! non-linear term cannot hide inside a quoted slope.
//!
//! Both command records and outcomes captured from an engine write are measured separately.
//! Every append is read back through a fresh reader before its size is accepted.
//!
//! # THE TWO LEGS ARE PRICED SEPARATELY
//!
//! One append in `ProtobufAppendBlob` mode writes exactly TWO objects and they are not the same
//! cost, so folding them would hide whichever is larger:
//!
//! ```text
//!   the log frame   the entry itself: a 4-byte length prefix and a protobuf record
//!   the sidecar     one frame in the per-shard offset index, saying WHERE the entry is
//! ```
//!
//! There is NO third leg per append. A checkpoint is a whole-index serialize, and nothing on this
//! path provokes one: `publish_checkpoint` is driven by the operator endpoint and once at start,
//! never by an append. So one append's cost is these two objects and the replica count, and this
//! program asserts the object count rather than assuming it.
//!
//! # FOUR DENOMINATORS, NOT INTERCHANGEABLE
//!
//! ```text
//!   user bytes      what the caller handed us -- key + value
//!   entry bytes     one WAL entry's extent length: the protobuf encoding
//!   blob bytes      every extent the append blob holds -- BOTH legs
//!   replicated bytes  blob bytes times the replica count -- a model, not disk usage
//! ```
//!
//! A ratio without its pair named is a number nobody can reproduce, so every figure below says
//! which two it is over.
//!
//! rust-internal: measures the shared store's own layout, no product behaviour

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const KEY: &str = "amp-key";

/// The cluster id the sweep runs under, and a much longer one used to show the envelope no longer
/// depends on it. These differ by 40 characters.
const CLUSTER: &str = "cluster-one-append";
const LONG_CLUSTER: &str = "cluster-one-append-with-a-deliberately-much-longer-name";

#[derive(Clone, Copy, Debug)]
enum Shape {
    Command,
    Outcome,
}

// Capture the engine's own record: invented object IDs or omitted pages change the cost.
fn entry_for(shape: Shape, value: Vec<u8>) -> SharedStoreWalEntry {
    let command = Command::StringSet {
        key: KEY.to_string(),
        value,
    };
    if matches!(shape, Shape::Command) {
        return SharedStoreWalEntry {
            shard_id: SHARD,
            wal_index: 1,
            command: Some(command),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        };
    }
    let dir = tempfile::tempdir().expect("bounded engine fixture");
    let primary = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("index"),
    );
    primary.load_shard(SHARD);
    assert!(
        primary
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command
            })
            .status
            .ok
    );
    let records = primary
        .write_ahead_log_store()
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan the engine append");
    let mut writes = records
        .iter()
        .map(|(_, bytes)| {
            temporalstore_rust::wal::decode_wal_line(bytes).expect("decode engine record")
        })
        .filter(|record| !record.outcomes.is_empty());
    let record = writes.next().expect("the engine must write outcomes");
    assert!(
        writes.next().is_none(),
        "one write must produce one outcome record"
    );
    assert!(
        record.command.is_none(),
        "the outcome sweep must not carry a command"
    );
    assert_eq!(
        record.outcomes.len(),
        1,
        "one string write must yield one outcome"
    );
    let mut staged_blocks = record.staged_blocks;
    if staged_blocks.is_empty() {
        for item in &record.outcomes {
            if let Some(address) = item.resolved_address() {
                staged_blocks.push(temporalstore_rust::wal::StagedBlock {
                    object_id: item.object_id,
                    component: item.component.as_deref().map(Arc::from),
                    bytes: primary.block_store().read(&address).expect("carried page"),
                });
            }
        }
    }
    SharedStoreWalEntry {
        shard_id: SHARD,
        wal_index: 1,
        command: record.command,
        staged_blocks,
        outcomes: record.outcomes,
    }
}

/// One append of `payload` bytes into a store of its own, and what the store then holds.
async fn one_append(
    shape: Shape,
    cluster: &str,
    payload: usize,
    options: &StoreOptions,
) -> (usize, u64, u64, usize) {
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"),
    );
    let replicator = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    // Deterministic dense bytes keep page compression from hiding per-byte storage costs.
    let mut seed = 0x1234_5678u32;
    let value: Vec<u8> = (0..payload)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let user_bytes = KEY.len() + value.len();

    let expected = entry_for(shape, value);
    replicator
        .publish_wal_entry(expected.clone())
        .await
        .unwrap_or_else(|err| panic!("append of {payload} bytes: {err:?}"));
    // A fresh reader cannot answer from the writer's state. Every measured byte must recover.
    let reader = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    let read = reader
        .read_wal_entry_by_offset_metadata(SHARD, 1)
        .await
        .expect("indexed read")
        .expect("the measured entry must exist");
    assert_eq!(
        read.entry, expected,
        "the measured append must read back intact"
    );

    // ---- THE ENTRY, and then EVERY object the append left behind. The sidecar is a second
    // ---- object and it is part of what an append costs, so it is counted rather than ignored.
    let blob_key = format!("{cluster}/shards/{SHARD}/shared/wal/wal.protobuf.blob");
    let objects = store
        .list(&format!("{cluster}/shards/{SHARD}/shared/wal/"))
        .await
        .expect("the wal listing");

    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");

    let entry_bytes: u64 = guard
        .get_object(BUCKET, &blob_key)
        .expect("the append blob")
        .metadata
        .extents
        .iter()
        .map(|extent| extent.length())
        .sum();

    let mut blob_bytes = 0u64;
    for key in &objects {
        let object = guard
            .get_object(BUCKET, key)
            .expect("every listed object must be counted");
        blob_bytes += object
            .metadata
            .extents
            .iter()
            .map(|e| e.length())
            .sum::<u64>();
    }

    (user_bytes, entry_bytes, blob_bytes, objects.len())
}

/// Least squares over the sweep, and the largest residual it leaves.
fn fit(points: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = points.len() as f64;
    let sx: f64 = points.iter().map(|p| p.0).sum();
    let sy: f64 = points.iter().map(|p| p.1).sum();
    let sxx: f64 = points.iter().map(|p| p.0 * p.0).sum();
    let sxy: f64 = points.iter().map(|p| p.0 * p.1).sum();
    let denom = n * sxx - sx * sx;
    let rate = if denom == 0.0 {
        0.0
    } else {
        (n * sxy - sx * sy) / denom
    };
    let envelope = (sy - rate * sx) / n;
    let worst = points
        .iter()
        .map(|p| (p.1 - (rate * p.0 + envelope)).abs())
        .fold(0.0f64, f64::max);
    (rate, envelope, worst)
}

/// Sweep the value size under one cluster id and return the fitted line for both legs.
async fn sweep(
    shape: Shape,
    cluster: &str,
    sizes: &[usize],
    options: &StoreOptions,
    print: bool,
) -> (f64, f64, f64) {
    let mut blob_points: Vec<(f64, f64)> = Vec::new();
    let mut entry_points: Vec<(f64, f64)> = Vec::new();

    if print {
        println!("    payload   user B   entry B   sidecar B   blob B   objects   blob/user   replicated/user");
    }
    for &payload in sizes {
        let (user, entry, blob, objects) = one_append(shape, cluster, payload, options).await;
        // ---- THE OBJECT COUNT IS ASSERTED, NOT ASSUMED. A listing that came back short would
        // ---- make every ratio below a ratio over the wrong denominator, and an empty one would
        // ---- read as "0 bytes over 0 objects", which looks free.
        assert_eq!(
            objects, 2,
