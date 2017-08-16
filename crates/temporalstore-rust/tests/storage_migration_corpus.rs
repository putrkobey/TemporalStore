// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use temporalstore_rust::{
    execute_redis_command, Command, CommandResponse, ExecuteRequest, RaftCluster, RaftConfig,
    RespValue, SharedStoreReplicator, SharedStoreStorageMode, BucketDumpFollowerReplayCursor,
    StorageLifecycleRequest, TemporalEngine,
};
use temporalstore_snapshot::FileObjectStore;

#[derive(Debug, Deserialize)]
struct StorageMigrationCorpus {
    schema_version: u32,
    name: String,
    source_format: String,
    format_compatibility: String,
    cases: Vec<StorageMigrationCase>,
}

#[derive(Debug, Deserialize)]
struct StorageMigrationCase {
    name: String,
    shard_id: u64,
    operations: Vec<StorageMigrationStep>,
    expected_reads: Vec<StorageMigrationStep>,
}

#[derive(Debug, Clone, Deserialize)]
struct StorageMigrationStep {
    name: String,
    #[serde(default)]
    storage_mutation: bool,
    command: Command,
    #[serde(default)]
    expect: Option<CommandResponse>,
}

#[tokio::test]
async fn rust_storage_replays_migration_corpus_across_lifecycle_paths() {
    let corpus = load_corpus();

    for case in &corpus.cases {
        verify_engine_dump_load_recovery(case);
        verify_shared_store_replay(case, SharedStoreStorageMode::Sync).await;
        verify_shared_store_replay(case, SharedStoreStorageMode::Async).await;
        verify_raft_replication(case);
    }
}

fn load_corpus() -> StorageMigrationCorpus {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/storage_migration_corpus.json");
    let corpus_bytes = fs::read(&corpus_path).expect("storage migration corpus should be readable");
    let corpus: StorageMigrationCorpus =
        serde_json::from_slice(&corpus_bytes).expect("storage migration corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-storage-migration-corpus");
    assert_eq!(corpus.source_format, "native_exported_logical_artifacts_v1");
    assert_eq!(
        corpus.format_compatibility,
        "migration_only_rust_native_pages"
    );
    assert!(
        !corpus.cases.is_empty(),
        "storage corpus must contain cases"
    );
    corpus
}

fn verify_engine_dump_load_recovery(case: &StorageMigrationCase) {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let mut engine = new_engine(dir.path(), &block_dir, &index_dir, case.shard_id);

