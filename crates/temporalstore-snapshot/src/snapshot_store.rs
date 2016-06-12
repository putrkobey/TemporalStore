// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

use crate::metrics::SnapshotMetrics;
use crate::object_store::{ObjectStore, ObjectStoreError};
use crate::types::{
    ChecksumEntry, LocalSnapshot, BlockSlabManifest, ShardId, SnapshotManifest, SnapshotRef,
    SnapshotRetention, SnapshotStatus,
};

const MANIFEST: &str = "manifest.json";
const INDEX: &str = "index.bin";
const CHECKSUMS: &str = "checksums.json";

#[derive(Debug, Error)]
pub enum SnapshotStoreError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("snapshot is older than local state: snapshot={snapshot_index}, local={local_index}")]
    StaleSnapshot {
        snapshot_index: u64,
        local_index: u64,
    },
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn create_local_snapshot(
        &self,
        shard_id: ShardId,
        last_log_id: String,
    ) -> Result<LocalSnapshot, SnapshotStoreError>;
    async fn upload_snapshot(
        &self,
        snapshot: LocalSnapshot,
    ) -> Result<SnapshotRef, SnapshotStoreError>;
    async fn download_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError>;
    async fn list_snapshots(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError>;
    async fn delete_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError>;
    async fn verify_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError>;
}

pub struct S3SnapshotStore<O> {
    cluster_id: String,
    engine_version: String,
    local_root: PathBuf,
    object_store: Arc<O>,
    metrics: Option<SnapshotMetrics>,
    retention: SnapshotRetention,
}

impl<O> S3SnapshotStore<O>
where
    O: ObjectStore + 'static,
{
    pub fn new(
        cluster_id: impl Into<String>,
        engine_version: impl Into<String>,
        local_root: impl Into<PathBuf>,
        object_store: Arc<O>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            engine_version: engine_version.into(),
            local_root: local_root.into(),
            object_store,
            metrics: None,
            retention: SnapshotRetention::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: SnapshotMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_retention(mut self, retention: SnapshotRetention) -> Self {
        self.retention = retention;
        self
    }

    pub async fn garbage_collect(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError> {
        let mut snapshots = self.list_snapshots(shard_id).await?;
        snapshots.sort_by_key(|s| s.created_at);
        snapshots.reverse();

        let cutoff = Utc::now() - Duration::seconds(self.retention.keep_newer_than_secs as i64);
        let mut deleted = Vec::new();
        for (idx, snapshot) in snapshots.iter().enumerate() {
            if idx < self.retention.keep_last {
                continue;
            }
            if snapshot.created_at >= cutoff {
                continue;
            }
            self.delete_snapshot(snapshot).await?;
            deleted.push(snapshot.clone());
        }
        Ok(deleted)
    }

    pub async fn install_snapshot_guard(
        &self,
        snapshot_ref: &SnapshotRef,
        local_last_log_index: u64,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        if snapshot_ref.last_log_index < local_last_log_index {
            if let Some(metrics) = &self.metrics {
                metrics.observe_restore(snapshot_ref.shard_id, SnapshotStatus::Failure);
            }
            return Err(SnapshotStoreError::StaleSnapshot {
                snapshot_index: snapshot_ref.last_log_index,
                local_index: local_last_log_index,
            });
        }
        let snapshot = self.download_snapshot(snapshot_ref, destination).await;
        if let Some(metrics) = &self.metrics {
            metrics.observe_restore(
                snapshot_ref.shard_id,
                if snapshot.is_ok() {
                    SnapshotStatus::Success
                } else {
                    SnapshotStatus::Failure
                },
            );
        }
        snapshot
    }

    pub async fn download_snapshot_by_uri(
        &self,
        manifest_uri: &str,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let manifest_key = key_from_uri(manifest_uri)
            .trim_end_matches(MANIFEST)
            .to_string();
        let manifest_bytes = self
            .object_store
            .get(&format!("{manifest_key}{MANIFEST}"))
            .await?;
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        let snapshot_ref =
            snapshot_ref_from_manifest(self.object_store.as_ref(), &manifest).await?;
        self.download_snapshot(&snapshot_ref, destination).await
    }

    pub async fn create_local_snapshot_with_index_bytes(
        &self,
        shard_id: ShardId,
        last_log_id: String,
        index_bytes: Bytes,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let root = self.local_root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(root.join("page_segments")).await?;
        let index_path = root.join(INDEX);
        tokio::fs::write(&index_path, index_bytes).await?;
        let checksums_path = root.join(CHECKSUMS);
        if !checksums_path.exists() {
            tokio::fs::write(&checksums_path, b"[]").await?;
        }

        let block_slabs = list_local_block_slabs(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            block_slabs,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.block_slabs = block_slab_manifests(&snapshot).await?;
        snapshot.manifest.checksums = checksum_entries(&snapshot).await?;
        snapshot.manifest.record_count = snapshot.manifest.checksums.len() as u64;
        snapshot.manifest.object_count = snapshot.manifest.block_slabs.len() as u64;

        if let Some(metrics) = &self.metrics {
            metrics.observe_create(shard_id, SnapshotStatus::Success);
        }
        Ok(snapshot)
    }

    fn snapshot_prefix(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}/snapshots/", self.cluster_id, shard_id)
    }
}

#[async_trait]
impl<O> SnapshotStore for S3SnapshotStore<O>
where
    O: ObjectStore + 'static,
{
    async fn create_local_snapshot(
        &self,
        shard_id: ShardId,
        last_log_id: String,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let root = self.local_root.join(format!("shard-{shard_id}"));
        tokio::fs::create_dir_all(root.join("page_segments")).await?;
        let index_path = root.join(INDEX);
        if !index_path.exists() {
            tokio::fs::write(&index_path, []).await?;
        }
        let checksums_path = root.join(CHECKSUMS);
        if !checksums_path.exists() {
            tokio::fs::write(&checksums_path, b"[]").await?;
        }

        let block_slabs = list_local_block_slabs(&root.join("page_segments")).await?;
        let mut snapshot = LocalSnapshot::new(
            self.cluster_id.clone(),
            shard_id,
            0,
            parse_log_index(&last_log_id),
            last_log_id,
            root,
            index_path,
            checksums_path,
            block_slabs,
        );
        snapshot.manifest.engine_version = self.engine_version.clone();
        snapshot.manifest.block_slabs = block_slab_manifests(&snapshot).await?;
        snapshot.manifest.checksums = checksum_entries(&snapshot).await?;

        if let Some(metrics) = &self.metrics {
            metrics.observe_create(shard_id, SnapshotStatus::Success);
        }
        Ok(snapshot)
    }

    async fn upload_snapshot(
        &self,
        snapshot: LocalSnapshot,
    ) -> Result<SnapshotRef, SnapshotStoreError> {
        let started = Instant::now();
        let stable_prefix = snapshot.manifest.stable_prefix();
        let temp_prefix = format!("{stable_prefix}.tmp-{}/", uuid::Uuid::new_v4());
        let result = upload_snapshot_inner(
            self.object_store.as_ref(),
            &snapshot,
            &stable_prefix,
            &temp_prefix,
        )
        .await;

        match result {
            Ok(snapshot_ref) => {
                if let Some(metrics) = &self.metrics {
                    metrics.observe_upload(
                        snapshot.manifest.shard_id,
                        SnapshotStatus::Success,
                        snapshot_ref.byte_size,
                        started.elapsed().as_secs(),
                    );
                    metrics.set_last_success_log_index(
                        snapshot.manifest.shard_id,
                        snapshot.manifest.last_log_index,
                    );
                }
                Ok(snapshot_ref)
            }
            Err(err) => {
                let _ = delete_prefix(self.object_store.as_ref(), &temp_prefix).await;
                if let Some(metrics) = &self.metrics {
                    metrics.observe_upload(
                        snapshot.manifest.shard_id,
                        SnapshotStatus::Failure,
                        0,
                        started.elapsed().as_secs(),
                    );
                }
                Err(err)
            }
        }
    }

    async fn download_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
        destination: PathBuf,
    ) -> Result<LocalSnapshot, SnapshotStoreError> {
        let started = Instant::now();
        let result =
            download_snapshot_inner(self.object_store.as_ref(), snapshot_ref, destination).await;
        if let Some(metrics) = &self.metrics {
            metrics.observe_download(
                snapshot_ref.shard_id,
                if result.is_ok() {
                    SnapshotStatus::Success
                } else {
                    SnapshotStatus::Failure
                },
                started.elapsed().as_secs(),
            );
        }
        result
    }

    async fn list_snapshots(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SnapshotRef>, SnapshotStoreError> {
        let prefix = self.snapshot_prefix(shard_id);
        let keys = self.object_store.list(&prefix).await?;
        let mut snapshots = Vec::new();
        for key in keys {
            if !key.ends_with(MANIFEST) || key.contains("/.tmp-") {
                continue;
            }
            let manifest_bytes = self.object_store.get(&key).await?;
            let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
            snapshots
                .push(snapshot_ref_from_manifest(self.object_store.as_ref(), &manifest).await?);
        }
        snapshots.sort_by_key(|s| (s.last_log_index, s.created_at));
        Ok(snapshots)
    }

    async fn delete_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let prefix = prefix_from_ref(snapshot_ref);
        delete_prefix(self.object_store.as_ref(), &prefix).await?;
        Ok(())
    }

    async fn verify_snapshot(&self, snapshot_ref: &SnapshotRef) -> Result<(), SnapshotStoreError> {
        let manifest_key = format!("{}{}", prefix_from_ref(snapshot_ref), MANIFEST);
        let manifest_bytes = self.object_store.get(&manifest_key).await?;
        let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes)?;
        for entry in &manifest.checksums {
            let key = format!("{}{}", manifest.stable_prefix(), entry.relative_path);
            let bytes = self.object_store.get(&key).await?;
            let actual = sha256_hex(&bytes);
            if actual != entry.sha256 {
                return Err(SnapshotStoreError::ChecksumMismatch {
                    path: entry.relative_path.clone(),
                    expected: entry.sha256.clone(),
                    actual,
                });
            }
        }
        Ok(())
    }
}

async fn upload_snapshot_inner<O: ObjectStore>(
    object_store: &O,
    snapshot: &LocalSnapshot,
    stable_prefix: &str,
    temp_prefix: &str,
) -> Result<SnapshotRef, SnapshotStoreError> {
    put_file(
        object_store,
        &format!("{temp_prefix}{INDEX}"),
        &snapshot.index_path,
    )
    .await?;
    put_file(
        object_store,
        &format!("{temp_prefix}{CHECKSUMS}"),
        &snapshot.checksums_path,
    )
