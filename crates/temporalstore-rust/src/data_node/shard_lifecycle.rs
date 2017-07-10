// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shard load/reload/unload helpers, extracted from data_node.rs.

use super::*;
use crate::control::{LoadShardRequest, LoadShardResponse, UnloadShardRequest, UnloadShardResponse};
use crate::types::Status;

pub(super) fn load_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: LoadShardRequest,
) -> LoadShardResponse {
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "load", request.load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "load",
            request.load_version,
            Some(status.clone()),
        );
        return LoadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "loading",
