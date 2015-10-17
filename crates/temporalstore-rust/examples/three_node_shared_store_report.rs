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
                .push("writing UNFENCED -- nothing stops a second writer on this backend".into());
            rep()
        }
    };

    for (wal_index, key, value) in FIRST {
        match writer_a.publish_wal_entry(entry(wal_index, key, value)).await {
            Ok(_) => out.wrote_first += 1,
            Err(err) => out.note.push(format!("publish {wal_index} failed: {err:?}")),
        }
    }

    // ---- PHASE 2  the other two replay from the shared store alone.
    for (name, engine) in [("node-b", &node_b), ("node-c", &node_c)] {
        match rep().replay_wal_strict(SHARD, 0, engine).await {
            Ok(r) => {
                out.replayed.push((
                    name.to_string(),
                    // `applied` and `offset_index_reads` are counts, so usize; the other two
                    // are wire quantities and already u64.
                    r.applied as u64,
                    r.last_wal_index,
                    r.offset_index_reads as u64,
                    r.range_bytes_read,
                ));
                for (_, key, value) in FIRST {
                    let got = engine.execute(ExecuteRequest {
                        shard_id: SHARD,
                        command: Command::StringGet { key: key.to_string() },
                    });
                    assert_eq!(
                        got.response,
                        CommandResponse::Bytes { value: Some(value.to_vec()) },
                        "{backend}/{name} replayed but cannot serve {key}"
                    );
                }
            }
            Err(err) => out.note.push(format!("{name} replay failed: {err:?}")),
        }
    }

    // ---- PHASE 3  an EQUAL load_version must be refused.
    out.equal_version_refused = Some(match rep().acquire_shard_lease(SHARD, 1, "node-b").await {
        Err(err) => format!("{err:?}"),
        Ok(()) => "ACCEPTED -- an equal load_version must not win the lease".into(),
    });

    // ---- PHASE 4  a strictly higher one must be accepted.
    let writer_b = rep().with_fence(2, "node-b");
    out.higher_version_taken = Some(match writer_b.acquire_shard_lease(SHARD, 2, "node-b").await {
        Ok(()) => "accepted".into(),
        Err(err) => format!("REFUSED: {err:?}"),
    });

    // ---- PHASE 5  the displaced writer must not be able to append.
    out.displaced_writer_refused =
        Some(match writer_a.publish_wal_entry(entry(5, "echo", b"from-a")).await {
            Err(err) => format!("refused: {err:?}"),
            Ok(_) => "APPENDED -- the displaced writer was not stopped".into(),
        });

    // ---- PHASE 6  the successor writes.
    for (wal_index, key, value) in SECOND {
        match writer_b.publish_wal_entry(entry(wal_index, key, value)).await {
            Ok(_) => out.wrote_second += 1,
            Err(err) => out.note.push(format!("successor publish {wal_index} failed: {err:?}")),
        }
    }

    // ---- PHASE 7  everyone converges.
    let fresh = node_engine(root, "node-a-fresh");
    for (name, engine, from) in [
        ("node-a", &fresh, 0),
        ("node-b", &node_b, 4),
        ("node-c", &node_c, 4),
    ] {
        match rep().replay_wal_strict(SHARD, from, engine).await {
            Ok(r) => out.converged.push((name.to_string(), r.last_wal_index)),
            Err(err) => out.note.push(format!("{name} converge failed: {err:?}")),
        }
    }

    let mut objects = store.list(&format!("{CLUSTER}/")).await.unwrap_or_default();
    objects.extend(store.list("leases/").await.unwrap_or_default());
    objects.sort();
    out.objects = objects;
    out
}

fn report(out: &Outcome) {
    println!();
    println!("================ BACKEND: {}", out.backend);
    for note in &out.note {
        println!("  * {note}");
    }
    println!();
    println!("  PHASE 1  first writer published        {} of 4 entries", out.wrote_first);
    println!("  PHASE 2  replay by the other two nodes");
    if out.replayed.is_empty() {
        println!("             (none succeeded)");
    }
    for (name, applied, last, idx_reads, range_bytes) in &out.replayed {
        println!(
            "             {name:<8} applied {applied:>2}  last_wal_index {last:>2}  offset_index_reads {idx_reads:>2}  range_bytes_read {range_bytes:>6}"
