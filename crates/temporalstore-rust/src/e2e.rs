// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::meta::ShardLocation;
use crate::raft::{MetaCommand, MetaRaftCluster, RaftCluster, RaftError, RaftNodeId};
use crate::shared_store::SharedStoreStorageMode;
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse, ShardId, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaReadPolicy {
    PinPrimary,
    AnyReplica,
    Replica(RaftNodeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalStoreClientOptions {
    pub pin_primary: bool,
    pub replica_read_policy: ReplicaReadPolicy,
}

impl Default for TemporalStoreClientOptions {
    fn default() -> Self {
        Self {
            pin_primary: true,
            replica_read_policy: ReplicaReadPolicy::PinPrimary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Raft,
    SharedStore,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Raft
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftWriteMode {
    Sync,
    Async,
}

impl Default for RaftWriteMode {
    fn default() -> Self {
        Self::Async
    }
}

impl RaftWriteMode {
    pub fn from_sync_flag(sync: bool) -> Self {
        if sync {
            Self::Sync
        } else {
            Self::Async
        }
    }

    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EndToEndWorkflowOptions {
    pub replication_mode: ReplicationMode,
    pub storage_mode: SharedStoreStorageMode,
    pub raft_write_mode: RaftWriteMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitches {
    pub proxy_enabled: bool,
    pub client_enabled: bool,
    pub data_nodes_enabled: bool,
    pub writes_enabled: bool,
    pub reads_enabled: bool,
    pub replication_enabled: bool,
    pub async_storage_enabled: bool,
    pub secondary_promotion_enabled: bool,
    pub scale_changes_enabled: bool,
}

impl Default for KillSwitches {
    fn default() -> Self {
        Self {
            proxy_enabled: true,
            client_enabled: true,
            data_nodes_enabled: true,
            writes_enabled: true,
            reads_enabled: true,
            replication_enabled: true,
            async_storage_enabled: true,
            secondary_promotion_enabled: true,
            scale_changes_enabled: true,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowError {
    #[error("kill switch disabled: {0}")]
    KillSwitch(&'static str),
    #[error("metaserver route is missing for shard {0}")]
    MissingRoute(ShardId),
    #[error("raft error: {0}")]
    Raft(String),
}

impl From<RaftError> for WorkflowError {
    fn from(value: RaftError) -> Self {
        WorkflowError::Raft(value.to_string())
    }
}

#[derive(Debug, Default, Clone)]
pub struct AsyncStorageJournal {
    inner: Arc<Mutex<AsyncStorageInner>>,
}

#[derive(Debug, Default)]
struct AsyncStorageInner {
    queued: VecDeque<ExecuteRequest>,
    flushed: Vec<ExecuteRequest>,
}

impl AsyncStorageJournal {
    pub fn enqueue(&self, request: ExecuteRequest) {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .queued
            .push_back(request);
    }

    pub fn flush_all(&self) -> usize {
        let mut inner = self.inner.lock().expect("async storage lock poisoned");
        let mut flushed = 0;
        while let Some(request) = inner.queued.pop_front() {
            inner.flushed.push(request);
            flushed += 1;
        }
        flushed
    }

    pub fn queued_len(&self) -> usize {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .queued
            .len()
    }

    pub fn flushed_len(&self) -> usize {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .flushed
            .len()
    }
}

#[derive(Debug, Clone)]
pub struct EndToEndWorkflow {
    shard_id: ShardId,
    meta: MetaRaftCluster,
