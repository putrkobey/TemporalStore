// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStore over a shared object store at scale: five nodes, a corpus sweep, and the cost.
//!
//! The three-node report answered *does it work*. This answers *what does it cost, and does the
//! cost hold its shape as the corpus grows* — the only version of a scale question worth asking,
//! because one corpus size cannot tell a linear cost from a quadratic one.
//!
//! # FIVE NODES, ONE WRITER
//!
//! The shared store is a **fenced single writer**, so scale in the node dimension means more
//! *replayers*, not more writers: one node holds the lease and appends, four replay from the store
//! alone. Each replayer's own figures are printed rather than averaged — an average over nodes
//! hides the one node that read twice as much as the others.
//!
//! # WHAT IS ASSERTED, AND WHY IT IS NOT A TIMING
//!
//! The timings are printed and never asserted, because a timing is a fact about the machine that
//! produced it. What is asserted is deterministic: bytes per entry, extents per entry, and — added
//! when this report's own finding was acted on — **page reads per entry on both sides**. Every byte
//! the store serves comes from a page read, so reads per entry is countable and is exactly where a
//! per-operation walk of the log appears.
//!
//! That column is what the earlier version of this report was missing. It showed per-entry time
//! growing with nothing deterministic to pin it on, because the byte figures count the range that
//! is *read* — correctly indexed all along — and not the work of *locating* it. Two walks were
//! found once the reads were counted, one on each side, and both are gone:
//!
//! * an append read the whole object back through its extents to recompute its etag
//!   (MatrixObject #213, now a resumed digest);
//! * a replay re-read and re-decoded the whole per-shard offset index once per entry, while
//!   holding the decoded records already.
//!
//! **Append per-entry cost still grows, and these counts cannot see why** — the remaining work
//! does no reading. The closing section says what it is and how it was measured.
//!
//! rust-internal: measures the shared store's own counters, no product behaviour

use std::sync::Arc;
use std::time::Instant;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const VALUE_BYTES: usize = 256;
const REPLAYERS: usize = 4;

struct NodeReplay {
    node: String,
    applied: u64,
    last_index: u64,
    index_reads: u64,
    range_bytes: u64,
    chunk_reads: u64,
    micros: u128,
}

struct Round {
    entries: usize,
    user_bytes: u64,
    blob_bytes: u64,
    extents: usize,
    objects: usize,
    append_micros: u128,
    append_chunk_reads: u64,
    replays: Vec<NodeReplay>,
}

/// How many page reads the store has served since it opened.
///
/// # WHY THE REPORT NEEDS A COUNT AND NOT ONLY A CLOCK
///
/// A timing is a fact about the box this runs on, so the assertions at the bottom cannot
/// be made on microseconds. But the work behind the timing is countable: every byte the
/// store hands back comes from a page read, and the read cache is asked once per read
/// whether it has it. Hits plus misses is therefore the number of page reads the whole
/// run performed, and it is exactly as deterministic as the extent count.
///
/// That is what makes "per-entry cost does not grow" assertable here. A per-operation walk
/// of something that grows with the log shows up as reads per entry climbing, whatever the
/// machine is doing at the time.
fn chunk_reads(store: &MatrixObjectObjectStore) -> u64 {
    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");
    let stats = guard.read_cache_stats();
    stats.hits + stats.misses
}

fn node_engine(root: &std::path::Path, node: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        root.join(format!("{node}-cache")),
        root.join(format!("{node}-blocks")),
        root.join(format!("{node}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

async fn round(entries: usize, options: &StoreOptions) -> Round {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = format!("cluster-scale-{entries}");
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("the shared store"),
    );
    let rep = || {
        SharedStoreReplicator::new(cluster.clone(), store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
    };

    // ---- ONE FENCED WRITER. The lease is taken durably, so this is the same path a real
    // ---- deployment uses rather than an unfenced shortcut that would understate the cost.
    let writer = rep().with_fence(1, "node-writer");
    writer
