// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    ReplayReport, SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::ObjectStore;

fn test_engine(root: &std::path::Path, role: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        root.join(format!("{role}-cache")),
        root.join(format!("{role}-blocks")),
        root.join(format!("{role}-index")),
    )
}

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        MatrixObjectObjectStore::new(
            "temporalstore-shared",
            StoreOptions {
                // A PAGE SITS BELOW A CHUNK, so `page_size` must be < `chunk_size`; the old
                // `slab_size` was its own tier and carried no such relation. Kept tiny so the
                // blob still spans several extents, which is what this example asserts.
                page_size: 4,
                max_extent_bytes: 4,
                chunk_size: 16,
                ..StoreOptions::default()
            },
        )
        .unwrap(),
    );
