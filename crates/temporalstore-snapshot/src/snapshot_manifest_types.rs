// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub type ShardId = u64;
pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionFormat {
    None,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockSlabManifest {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: String,
    pub relative_path: String,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumEntry {
    pub relative_path: String,
    pub sha256: String,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub snapshot_id: String,
    pub raft_term: u64,
    pub last_log_index: u64,
    pub last_applied_log_id: String,
    pub created_at: DateTime<Utc>,
    pub engine_version: String,
    pub block_slabs: Vec<BlockSlabManifest>,
    pub object_count: u64,
    pub record_count: u64,
    pub checksums: Vec<ChecksumEntry>,
    pub compression: CompressionFormat,
