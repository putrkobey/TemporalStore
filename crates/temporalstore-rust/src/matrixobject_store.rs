// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use matrixobjectstore_rs::{
    MatrixObjectStore, ObjectError as MatrixObjectError, PutOptions, StoreOptions,
};
use temporalstore_snapshot::object_store::{AppendBlobReceipt, ObjectStore, ObjectStoreError};

#[derive(Debug, Clone)]
pub struct MatrixObjectObjectStore {
    bucket: String,
    content_type: String,
    inner: Arc<Mutex<MatrixObjectStore>>,
}

impl MatrixObjectObjectStore {
    pub fn new(bucket: impl Into<String>, options: StoreOptions) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        let mut inner = MatrixObjectStore::new(options).map_err(map_matrixobject_error)?;
        inner
            .create_bucket(&bucket)
            .map_err(map_matrixobject_error)?;
        Ok(Self {
            bucket,
            content_type: "application/octet-stream".to_string(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn with_default_options(bucket: impl Into<String>) -> Result<Self, ObjectStoreError> {
        Self::new(bucket, StoreOptions::default())
    }

    /// Construct a **durable** store rooted at `dir`.
    ///
    /// `StoreOptions::default()` keeps the entire object store in memory, so the
    /// bytes vanish when the process exits. This constructor points MatrixObject
    /// at an on-disk snapshot (`<dir>/matrixobject.snapshot`) with
    /// flush-on-commit enabled: every mutating operation atomically rewrites the
    /// snapshot, and `MatrixObjectStore::new` reads it back at startup. That is
    /// what lets a datanode recover shard data from shared storage after its
    /// local dirs are wiped and it restarts.
    ///
    /// The `create_bucket` call in [`Self::new`] is idempotent, so restoring a
    /// snapshot that already contains `bucket` is safe.
    pub fn with_persistent_dir(
        bucket: impl Into<String>,
        dir: impl AsRef<Path>,
    ) -> Result<Self, ObjectStoreError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|err| {
            ObjectStoreError::InvalidKey(format!(
                "failed to create matrixobject store dir {}: {err}",
                dir.display()
            ))
        })?;
        let snapshot_path = dir.join("matrixobject.snapshot");
        let options = StoreOptions {
            persistent_snapshot_path: snapshot_path.to_string_lossy().into_owned(),
            persistent_snapshot_flush_on_commit: true,
            ..StoreOptions::default()
        };
        Self::new(bucket, options)
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn from_snapshot_bytes(
        bucket: impl Into<String>,
        options: StoreOptions,
        snapshot_bytes: &[u8],
    ) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        let mut inner = MatrixObjectStore::new(options).map_err(map_matrixobject_error)?;
        inner
            .install_snapshot_bytes(snapshot_bytes)
            .map_err(map_matrixobject_error)?;
        Ok(Self {
            bucket,
