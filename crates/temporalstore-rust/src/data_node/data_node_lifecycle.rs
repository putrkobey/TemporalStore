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
        return Status::error(
            "bad_lifecycle_snapshot",
            "unsupported data node lifecycle snapshot version",
        );
    }
    {
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned");
        lifecycle.clear();
        for state in snapshot.transitions {
            lifecycle.insert(state.shard_id, state);
        }
    }
    {
        let mut tokens = inner
            .lifecycle_tokens
            .lock()
            .expect("runtime lifecycle token lock poisoned");
        tokens.clear();
        for token in snapshot.tokens {
            tokens.insert((token.shard_id, token.operation.clone()), token);
        }
    }
    Status::ok()
}

pub(super) fn restore_lifecycle_snapshot_from_path_inner(inner: &DataNodeRuntimeInner) {
    let Some(path) = &inner.lifecycle_snapshot_path else {
        return;
    };
    let status = match fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<DataNodeLifecycleSnapshot>(&bytes) {
            Ok(snapshot) => restore_lifecycle_snapshot_inner(inner, snapshot),
            Err(err) => Status::error("bad_lifecycle_snapshot", err.to_string()),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Status::ok(),
        Err(err) => Status::error("lifecycle_snapshot_io", err.to_string()),
    };
    let mut report = inner
        .lifecycle_persistence
        .lock()
        .expect("runtime lifecycle persistence lock poisoned");
    report.last_restore_at_ms = now_ms();
    report.last_restore_status = Some(status.clone());
    if status.ok {
        report.restore_success_total += 1;
    } else {
        report.restore_failure_total += 1;
    }
}

pub(super) fn persist_lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) {
