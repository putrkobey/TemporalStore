// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};
use temporalstore_snapshot::{
    FileObjectStore, LocalSnapshot, ObjectStore, ObjectStoreError, S3SnapshotStore, SnapshotStore,
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
struct AwsCliObjectStore {
    bucket: String,
    prefix: String,
}

impl AwsCliObjectStore {
    fn from_env() -> Option<Self> {
        let bucket = std::env::var("TS_SNAPSHOT_AWS_BUCKET").ok()?;
        let prefix = std::env::var("TS_SNAPSHOT_AWS_PREFIX").unwrap_or_else(|_| {
            format!("temporalstore-rust-snapshot-smoke/{}", uuid::Uuid::new_v4())
        });
        Some(Self {
            bucket,
            prefix: prefix.trim_matches('/').to_string(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    fn s3_uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, self.full_key(key))
    }

    fn run_aws(&self, args: &[&str]) -> Result<String, ObjectStoreError> {
        let output = Command::new("aws")
            .args(args)
            .output()
            .map_err(ObjectStoreError::Io)?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(ObjectStoreError::InvalidKey(format!(
                "aws {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )))
        }
    }
}

#[async_trait]
impl ObjectStore for AwsCliObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut tmp = NamedTempFile::new().map_err(ObjectStoreError::Io)?;
        std::io::Write::write_all(&mut tmp, &bytes).map_err(ObjectStoreError::Io)?;
        let path = tmp.path().to_string_lossy().to_string();
        self.run_aws(&["s3", "cp", &path, &self.s3_uri(key)])?;
