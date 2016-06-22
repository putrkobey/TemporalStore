// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage-manager runtime scheduling/reporting helpers, extracted from data_node.rs.

use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(super) fn storage_manager_runtime_initial_report(
    options: &StorageManagerRuntimeOptions,
) -> StorageManagerRuntimeReport {
    StorageManagerRuntimeReport {
        running: true,
        paused: false,
        stopped: false,
        interval_ms: options.interval_ms,
        jitter_percent: options.jitter_percent,
        current_backoff_ms: options.initial_backoff_ms,
        last_delay_ms: options.interval_ms,
        phase_prepare_enabled: options.request.enable_prepare,
        phase_wal_reclaim_enabled: options.request.enable_wal_reclaim,
        phase_expire_enabled: options.request.enable_expire,
        phase_evict_enabled: options.request.enable_evict,
        phase_block_gc_enabled: options.request.enable_block_reclaim,
        phase_compaction_enabled: options.request.enable_block_compaction,
        phase_index_gc_enabled: options.request.enable_index_gc,
        bounded_max_dump_buckets_per_round: options.request.max_dump_buckets_per_round,
        configured_follower_cursor_count: options.request.follower_replay_cursors.len(),
        configured_raft_snapshot_ref_count: options.request.raft_snapshot_refs.len(),
        configured_block_gc_raft_install_floor_slab_id: options
            .request
            .block_gc_raft_install_floor_slab_id,
        ..StorageManagerRuntimeReport::default()
    }
}

pub(super) fn wait_for_storage_manager_cycle_completion(
    runtime: &DataNodeRuntime,
    job_id: u64,
    timeout_ms: u64,
) -> Option<StorageManagerCycleReport> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
    loop {
        if let Some(status) = runtime.job_status(job_id) {
            if status.finished_at_ms.is_some() {
                if let Some(DataNodeTaskOutput::StorageManager(response)) = status.output {
                    return Some(response.report);
                }
                return None;
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub(super) fn apply_storage_manager_cycle_to_runtime_report(
    report: &mut StorageManagerRuntimeReport,
    cycle: StorageManagerCycleReport,
) {
    let mut selected_buckets = BTreeSet::new();
    let mut skipped_reasons = BTreeSet::new();
    let mut phase_blockers = BTreeSet::new();
    let mut bytes_reclaimed = 0u64;
    let mut pressure_before = 0u64;
    let mut pressure_after = 0u64;
    let mut wal_floor_sequence = 0u64;
    let mut index_log_floor_sequence = 0u64;
    let mut retention_blockers = 0usize;
    for stage in &cycle.stages {
        selected_buckets.extend(stage.selected_buckets.iter().copied());
        if stage.skipped && !stage.reason.is_empty() {
            skipped_reasons.insert(stage.reason.clone());
        }
        if !stage.skipped_reason.is_empty() {
            skipped_reasons.insert(stage.skipped_reason.clone());
        }
        if stage.retention_blockers > 0 {
            phase_blockers.insert(format!("{}:retention_blockers", stage.stage));
        }
        for error in &stage.errors {
            phase_blockers.insert(error.clone());
        }
        bytes_reclaimed = bytes_reclaimed
            .saturating_add(stage.bytes_reclaimed)
            .saturating_add(stage.block_bytes_reclaimed)
            .saturating_add(stage.cache_disk_bytes_removed)
            .saturating_add(stage.before_bytes.saturating_sub(stage.after_bytes));
        pressure_before = pressure_before.max(stage.pressure_before);
        pressure_after = pressure_after.max(stage.pressure_after);
        wal_floor_sequence = wal_floor_sequence.max(stage.wal_floor_sequence);
        index_log_floor_sequence = index_log_floor_sequence.max(stage.index_log_floor_sequence);
        retention_blockers = retention_blockers.max(stage.retention_blockers);
