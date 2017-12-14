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
