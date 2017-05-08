// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction/GC task execution, extracted from data_node.rs.

use super::*;

pub(super) fn run_compaction_inner(
    inner: &DataNodeRuntimeInner,
    request: CompactionRequest,
) -> CompactionResponse {
    run_compaction_inner_draining(inner, request, None)
}

/// The same task, optionally restricted to the slabs the caller wants emptied.
///
/// `None` relocates every live page, which is what a queued compaction task and the operator
/// RPC mean. `Some` is what the periodic maintenance round issues: it already knows which
/// slabs carry dead space that objects still hold pages on, and those are the only slabs a
/// relocation can recover anything for.
pub(super) fn run_compaction_inner_draining(
    inner: &DataNodeRuntimeInner,
    request: CompactionRequest,
    drain_block_slab_ids: Option<BTreeSet<u64>>,
) -> CompactionResponse {
    let compaction = match drain_block_slab_ids {
        Some(drain_block_slab_ids) => inner
            .engine
            .compact_shard_blocks_draining(request.shard_id, drain_block_slab_ids),
        None => inner.engine.compact_shard_blocks(request.shard_id),
    };
    let (
        status,
        compacted_objects,
        relocated_bytes,
        rewritten_object_blocks,
        delete_marked_object_ids_before,
        delete_marked_object_ids_after,
        model_layouts,
        previous_block_slab_id,
        compacted_block_slab_id,
        stale_block_slab_ids,
        before,
        after,
    ) = match compaction {
        Ok(report) => (
            Status::ok(),
            report.rewritten_block_refs,
            report.relocated_bytes,
            report.rewritten_object_blocks,
            report.delete_marked_object_ids_before,
            report.delete_marked_object_ids_after,
            report.model_layouts,
            report.previous_block_slab_id,
            report.compacted_block_slab_id,
            report.stale_block_slab_ids,
            report.before,
            report.after,
        ),
        Err(status) => (
            status,
            0,
            0,
            0,
            0,
            0,
            Vec::new(),
            0,
            0,
            Vec::new(),
            ShardCompactionUtilityReport::default(),
            ShardCompactionUtilityReport::default(),
        ),
    };
    inner
        .stats
