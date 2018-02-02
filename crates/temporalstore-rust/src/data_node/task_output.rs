// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Task cancellation guard + task output/status helpers, extracted from data_node.rs.

use super::*;

pub(super) struct TaskCancellation<'a> {
    pub(super) inner: &'a DataNodeRuntimeInner,
    pub(super) job_id: u64,
}

impl TaskCancellation<'_> {
    pub(super) fn is_requested(&self) -> bool {
        is_cancel_requested(self.inner, self.job_id)
    }
}

pub(super) fn task_timeout_output(kind: DataNodeTaskKind) -> DataNodeTaskOutput {
    let status = Status::error("deadline_exceeded", "data node task deadline exceeded");
    match kind {
        DataNodeTaskKind::Load => DataNodeTaskOutput::Load(LoadShardResponse { status }),
        DataNodeTaskKind::Reload => DataNodeTaskOutput::Reload(LoadShardResponse { status }),
        DataNodeTaskKind::Unload => DataNodeTaskOutput::Unload(UnloadShardResponse { status }),
        DataNodeTaskKind::Execute => DataNodeTaskOutput::Execute(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        }),
        DataNodeTaskKind::CheckedExecute => {
            DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            })
        }
        DataNodeTaskKind::Dump => DataNodeTaskOutput::Dump(DumpShardResponse {
            status,
            shard_id: 0,
            index_bytes: 0,
            dirty_objects_flushed: 0,
            bucket_dump_manifest: None,
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id: 0,
