// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs;
use std::path::PathBuf;

use super::{
    is_write_command, now_ms, Command, DataNodeLifecyclePersistenceReport,
    DataNodeLifecycleSnapshot, DataNodeRuntimeInner, DataNodeShardLifecycleState, ShardId, Status,
};

pub(super) fn lifecycle_snapshot_path_from_env() -> Option<PathBuf> {
    std::env::var_os("TS_DATA_NODE_LIFECYCLE_SNAPSHOT").map(PathBuf::from)
}

pub(super) fn lifecycle_persistence_report_for_path(
    path: Option<&PathBuf>,
) -> DataNodeLifecyclePersistenceReport {
    DataNodeLifecyclePersistenceReport {
        enabled: path.is_some(),
        path: path.map(|path| path.display().to_string()),
        ..DataNodeLifecyclePersistenceReport::default()
    }
}

pub(super) fn lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) -> DataNodeLifecycleSnapshot {
    let mut transitions = inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    transitions.sort_by_key(|state| (state.shard_id, state.operation.clone()));
    let mut tokens = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    tokens.sort_by_key(|token| (token.shard_id, token.operation.clone()));
    DataNodeLifecycleSnapshot {
        format_version: 1,
        transitions,
        tokens,
    }
}

pub(super) fn restore_lifecycle_snapshot_inner(
    inner: &DataNodeRuntimeInner,
    snapshot: DataNodeLifecycleSnapshot,
) -> Status {
    if snapshot.format_version != 1 {
