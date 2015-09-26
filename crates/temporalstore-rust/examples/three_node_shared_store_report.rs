// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Three nodes over one shared object store, on each backend, and what each backend can do.
//!
//! The shared store is a **fenced single writer**: `acquire_shard_lease` is a durable
//! compare-and-set, so ownership is decided by the store rather than by any node's local view.
//! "Three nodes" therefore means one writer, two replayers, and a handover.
//!
//! `ObjectStore::compare_and_swap` has a **fail-closed default** — a backend without a real atomic
//! conditional put refuses rather than degrading the fence to an unfenced read-modify-write.
//! `FileObjectStore` overrides it with a cross-process CAS over an atomic `mkdir`. The MatrixObject
//! adapter does not override it, so the fence is unavailable there and the run says so instead of
//! stopping.

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore};

const CLUSTER: &str = "cluster-three-node";
const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;

const FIRST: [(u64, &str, &[u8]); 4] = [
    (1, "alpha", b"first"),
    (2, "bravo", b"second"),
    (3, "charlie", b"third"),
    (4, "delta", b"fourth"),
];
const SECOND: [(u64, &str, &[u8]); 3] = [
    (5, "echo", b"fifth"),
    (6, "foxtrot", b"sixth"),
    (7, "golf", b"seventh"),
];

#[derive(Default)]
struct Outcome {
    backend: String,
    wrote_first: usize,
    replayed: Vec<(String, u64, u64, u64, u64)>,
    equal_version_refused: Option<String>,
    higher_version_taken: Option<String>,
    displaced_writer_refused: Option<String>,
    wrote_second: usize,
    converged: Vec<(String, u64)>,
    objects: Vec<String>,
    note: Vec<String>,
}

fn node_engine(root: &std::path::Path, node: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024,
        root.join(format!("{node}-cache")),
        root.join(format!("{node}-blocks")),
        root.join(format!("{node}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

fn entry(wal_index: u64, key: &str, value: &[u8]) -> SharedStoreWalEntry {
    SharedStoreWalEntry {
        shard_id: SHARD,
        wal_index,
        command: Some(Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        }),
        staged_blocks: Vec::new(),
        outcomes: Vec::new(),
    }
}

async fn run_backend<O>(backend: &str, store: Arc<O>, root: &std::path::Path) -> Outcome
where
    O: ObjectStore + Send + Sync + 'static,
{
    let mut out = Outcome { backend: backend.to_string(), ..Default::default() };
    let rep = || {
        SharedStoreReplicator::new(CLUSTER, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
    };

    let node_b = node_engine(root, "node-b");
    let node_c = node_engine(root, "node-c");

    // ---- PHASE 1  node-a claims the lease and writes. If the fence is unavailable the writer
    // ---- proceeds unfenced, because the DATA path is a separate question from ownership.
    let fenced = rep().with_fence(1, "node-a");
    let writer_a = match fenced.acquire_shard_lease(SHARD, 1, "node-a").await {
        Ok(()) => {
            out.note.push("fence available: node-a holds the lease at load_version 1".into());
            fenced
        }
        Err(err) => {
            out.note.push(format!("FENCE UNAVAILABLE: {err:?}"));
            out.note
