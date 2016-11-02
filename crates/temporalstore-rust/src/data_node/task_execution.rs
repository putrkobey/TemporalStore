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
        .lock()
        .expect("runtime stats lock poisoned")
        .compaction_runs += 1;
    CompactionResponse {
        status,
        shard_id: request.shard_id,
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
    }
}

pub(super) fn run_gc_inner(inner: &DataNodeRuntimeInner, request: GcRequest) -> GcResponse {
    // REFUSE WHILE THE SHARD IS RECOVERING (WAL replay still to run, or running).
    //
    // This is the guard the storage-manager cycle has always had, on the reclaim that actually
    // ships. Both callers of this function can reach a recovering shard:
    //
    //   1. the periodic loop -- run_storage_manager_once, driven by the storage-manager
    //      scheduler, which iterates loaded_shard_ids(). That reads the shards map, and
    //      load_shard_with inserts into the shards map BEFORE it replays the WAL, precisely so
    //      a concurrent writer can observe the shard and be refused. loaded_shard_ids() does not
    //      consult the recovery flag, so a recovering shard is picked up like any other.
    //   2. the operator reclaim RPC -- an operator can call it at any moment, including during
    //      a restart, and nothing between the route and this function looks at recovery.
    //
    // What goes wrong, measured on this tree rather than reasoned about: the LOG is the sharper
    // exposure, not the slabs. This function also reclaims the write-ahead log, and a shard that
    // has never dumped gets an unproven durable anchor whose through_sequence is u64::MAX, so
    // nothing clamps the ask. Taken during the recovery window that deletes the records replay
    // has not applied yet -- 31 of 32 in the characterization test below -- and replay then
    // aborts on the hole it made, which unwinds load_shard_with and refuses the shard outright.
    // The log has no quarantine of any kind.
    //
    // The slabs are exposed the same way and more mildly: the live set comes from
    // live_block_slab_ids_all_shards(), derived from the in-memory index, so during the window a
    // slab whose referents are still waiting in the log reads as DEAD. The periodic loop asks
    // for delayed destroy and the purge re-checks liveness, so that path can be restored from
    // quarantine -- but the operator RPC leaves delayed destroy off and unlinks there and then.
    //
    // Refused rather than silently skipped: the caller asked for a reclaim and did not get one,
    // and a reclaim that reports ok while doing nothing is how a maintenance loop goes quiet
    // without anyone noticing. The window is bounded by replay, so the next round runs normally.
    if inner.engine.shard_is_recovering(request.shard_id) {
        return GcResponse {
            status: Status::error(
                "shard_recovering",
                format!(
                    "gc refused for shard {}: recovery (WAL replay) is in progress; reclaim must not interleave with replay",
                    request.shard_id
                ),
            ),
            shard_id: request.shard_id,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            wal_records_removed: 0,
            index_log_records_removed: 0,
            block_slabs_removed: 0,
            block_slabs_removed_physical_bytes: 0,
            block_slabs_retained_physical_bytes: 0,
            block_slabs_retained_live: 0,
            block_slabs_retained_live_physical_bytes: 0,
            gc_durable_index_backed: false,
            wal_gc_clamped_by_durable_index: false,
            index_log_gc_clamped_by_durable_index: false,
            // No plan: building one reads the same half-reconstructed index this is refusing
            // to act on, so a plan here would be a measurement of the partial state.
            lifecycle_plan: None,
