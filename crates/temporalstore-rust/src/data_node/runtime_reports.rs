// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DataNodeRuntime reporting/topology methods, extracted from data_node.rs.

use super::*;

impl DataNodeRuntime {
    /// QUEUE FIRST, THEN DIRTY, THEN STATS. The order is the point, not the contents.
    ///
    /// This holds three of the runtime's mutexes at once so the report is one cut rather than
    /// three readings taken at different instants. Holding three is only safe while every other
    /// site that holds two of them agrees on the order, and the rest of the runtime takes
    /// `queue` before `stats`: `submit` holds `queue` across the two rejection paths that charge
    /// `stats`, and `cancel_job` holds `jobs` then `queue` and charges `stats` inside both.
    ///
    /// Read the other way round -- stats, dirty, queue, which is what this was -- the two paths
    /// deadlock against each other outright: a reporting thread holding `stats` waits for
    /// `queue` while a submitting thread holding `queue` waits for `stats`, and neither ever
    /// yields. Nothing about the report changes by reordering the three acquisitions; the
    /// bindings below are all read after the last of them is taken.
    ///
    /// If a fourth lock is ever added to this cut, put it where the rest of the runtime already
    /// takes it rather than where it reads best here. The guard
    /// `the_data_node_runtime_locks_have_one_global_order` walks these files for exactly this.
    pub fn stats(&self) -> DataNodeRuntimeStats {
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let dirty = self
            .inner
            .dirty
            .lock()
            .expect("dirty tracker lock poisoned");
        let stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        let queue_depth = queue.queued_total;
        let queued_shard_count = queue.by_shard.len();
        let running_shard_count = queue.running_shards.len();
        let dirty_shard_count = dirty
            .by_key
            .values()
            .map(|info| info.shard_id)
            .collect::<BTreeSet<_>>()
            .len();
        DataNodeRuntimeStats {
            worker_threads: self.inner.options.worker_threads,
            max_queue_depth: self.inner.options.max_queue_depth,
            max_background_queue_depth: self.inner.options.max_background_queue_depth,
            submitted_total: stats.submitted_total,
            completed_total: stats.completed_total,
            rejected_total: stats.rejected_total,
            rejected_background_total: stats.rejected_background_total,
            timed_out_total: stats.timed_out_total,
            canceled_total: stats.canceled_total,
            queue_depth,
            background_queue_depth: queue.background_queued_total,
            queued_shard_count,
            running_shard_count,
            dirty_object_count: dirty.by_key.len(),
            dirty_shard_count,
            expiry_sweeps: stats.expiry_sweeps,
            expired_records_removed: stats.expired_records_removed,
            dump_runs: stats.dump_runs,
            compaction_runs: stats.compaction_runs,
            gc_runs: stats.gc_runs,
            storage_lifecycle_runs: stats.storage_lifecycle_runs,
            storage_manager_runs: stats.storage_manager_runs,
            storage_manager_loops: stats.storage_manager_loops,
            storage_manager_last_shard_id: stats.storage_manager_last_shard_id,
            storage_manager_prepare_runs: stats.storage_manager_prepare_runs,
            storage_manager_reclaim_wal_runs: stats.storage_manager_reclaim_wal_runs,
            storage_manager_reclaim_memory_runs: stats.storage_manager_reclaim_memory_runs,
            storage_manager_expire_runs: stats.storage_manager_expire_runs,
            storage_manager_reclaim_block_runs: stats.storage_manager_reclaim_block_runs,
            storage_manager_compact_runs: stats.storage_manager_compact_runs,
            storage_manager_index_gc_runs: stats.storage_manager_index_gc_runs,
            storage_manager_evict_runs: stats.storage_manager_evict_runs,
        }
    }

    pub fn preflight_report(&self) -> DataNodePreflightReport {
        let stats = self.stats();
        let metaserver = self.metaserver_heartbeat_report();
        let mut degraded_reasons = Vec::new();
        if stats.queue_depth >= stats.max_queue_depth && stats.max_queue_depth > 0 {
            degraded_reasons.push("foreground_queue_full".to_string());
        }
        if stats.background_queue_depth >= stats.max_background_queue_depth
            && stats.max_background_queue_depth > 0
        {
            degraded_reasons.push("background_queue_full".to_string());
        }
        if stats.rejected_total > 0 {
            degraded_reasons.push("rejected_requests".to_string());
        }
        if stats.timed_out_total > 0 {
            degraded_reasons.push("timed_out_requests".to_string());
        }
        if metaserver.last_heartbeat_at_ms > 0 && !metaserver.last_status.ok {
            degraded_reasons.push("metaserver_heartbeat_failed".to_string());
        }
        if metaserver.forbid_auto_register {
            degraded_reasons.push("metaserver_forbid_auto_register".to_string());
        }
        let lifecycle_persistence = self.lifecycle_persistence_report();
        if lifecycle_persistence
            .last_restore_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_restore_failed".to_string());
        }
        if lifecycle_persistence
            .last_persist_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_persist_failed".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        DataNodePreflightReport {
            status,
            stats,
            lifecycle: self.lifecycle_report(),
            lifecycle_persistence,
            topology_validation: self.topology_validation_report(&metaserver),
            metaserver,
            queued_workers: self.queued_shard_worker_infos(),
            dirty_shards: self.dirty_shards(),
            dirty_objects: self.dirty_objects(),
            degraded_reasons,
        }
    }

    pub fn lifecycle_report(&self) -> DataNodeLifecycleReport {
        let stats = self.stats();
        let mut shards = self.shard_serving_states();
        shards.sort_by_key(|state| state.shard_id);
        let mut transitions = self.lifecycle_states();
        transitions.sort_by_key(|state| state.shard_id);
        let loaded_shard_count = shards.iter().filter(|state| state.loaded).count();
        let serving_count = shards
            .iter()
            .filter(|state| state.serving_state == "serving")
            .count();
        let readonly_count = shards.iter().filter(|state| state.readonly).count();
        let queued_count = shards
            .iter()
            .filter(|state| state.serving_state == "queued")
            .count();
        let running_count = shards
            .iter()
            .filter(|state| state.serving_state == "running")
            .count();
        let unloading_count = shards
            .iter()
            .filter(|state| state.serving_state == "unloading")
            .count();
        let failed_shards = shards
            .iter()
            .filter(|state| state.serving_state == "failed")
            .map(|state| state.shard_id)
            .chain(
                transitions
                    .iter()
                    .filter(|state| state.state == "failed")
                    .map(|state| state.shard_id),
            )
            .collect::<BTreeSet<_>>();
        let failed_count = failed_shards.len();
        let max_load_version = shards
            .iter()
            .map(|state| state.load_version)
            .chain(transitions.iter().map(|state| state.load_version))
            .max()
            .unwrap_or_default();
        let mut storage_lifecycle_metrics = data_node_storage_lifecycle_metrics(&stats);
        apply_shard_storage_metrics(&mut storage_lifecycle_metrics, &shards);
        let (
            storage_write_contract,
            storage_read_contract,
            storage_cold_scan_contract,
            storage_manager_contract,
            storage_index_contract,
            storage_cache_contract,
            storage_reclaim_contract,
        ) = data_node_storage_contracts_from_metrics(&storage_lifecycle_metrics);
        DataNodeLifecycleReport {
            public_storage_contract: PublicStorageContract::default(),
            public_storage_feature_shapes: PublicStorageFeatureShapes::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: storage_lifecycle_metrics.clone(),
            storage_write_contract,
            storage_read_contract,
            storage_cold_scan_contract,
            storage_manager_contract,
            storage_index_contract,
            storage_cache_contract,
            storage_reclaim_contract,
            storage_safety_snapshot: storage_safety_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_watermark_snapshot: storage_watermark_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_gc_snapshot: storage_gc_snapshot_from_metrics(&storage_lifecycle_metrics),
            storage_index_snapshot: storage_index_snapshot_from_metrics(&storage_lifecycle_metrics),
            storage_topology_snapshot: storage_topology_snapshot_from_metrics(
                &storage_lifecycle_metrics,
            ),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: default_storage_reclaim_scope(),
            loaded_shard_count,
            serving_count,
            readonly_count,
            queued_count,
            running_count,
            unloading_count,
            failed_count,
            max_load_version,
            shards,
            transitions,
        }
    }

    pub fn grpc_streaming_contract(&self) -> DataNodeGrpcStreamingContract {
        DataNodeGrpcStreamingContract::default()
    }

    pub fn distributed_admission_decision(
        &self,
        shard_id: ShardId,
        peer_snapshots: &[DistributedAdmissionPeerSnapshot],
        read_budget: u64,
        write_budget: u64,
        min_topology_version: u64,
    ) -> DistributedAdmissionDecision {
        distributed_admission_decision(
            shard_id,
            peer_snapshots,
            read_budget,
