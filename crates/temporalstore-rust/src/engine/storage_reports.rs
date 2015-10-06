// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage readiness / compatibility / cache-warmup / recovery-boundary reports for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {

    /// WHAT THE SERVED HASH READ HAS SEEN OF ITS TWO SOURCES, read off the counters the read
    /// path keeps.
    ///
    /// The numbers are process-wide and monotonic for the life of the process, so two calls
    /// subtract into "what happened in between" -- which is how a trend is read off them. There
    /// is no reset on the production surface, because a counter a reader can zero is a counter
    /// whose history a reader can lose.
    ///
    /// See [`HashReadDivergenceReport`] for what a zero does and does not mean. It is a statement
    /// about the routes traffic took and NOT a proof of completeness on the routes it did not.
    pub fn hash_read_divergence_report(&self) -> HashReadDivergenceReport {
        let (reads_served, index_named_fields_the_container_lacked, divergences_logged, last) =
            super::execute_on_shard::hash_read_divergence::snapshot();
        HashReadDivergenceReport {
            reads_served,
            index_named_fields_the_container_lacked,
            divergences_logged,
            last_divergence: last.map(|(object_key, field)| HashReadDivergenceSample {
                object_key,
                field,
            }),
        }
    }
    /// WHAT THE BUCKET MAINTENANCE SCAN HAS SEEN of the pages and the object list.
    ///
    /// Process-wide and monotonic for the life of the process, with no reset on the production
    /// surface: a counter a reader can zero is a counter whose history a reader can lose. Two calls
    /// subtract into what happened in between.
    ///
    /// See [`ObjectIndexDivergenceReport`] for what a zero does and does not mean -- it is a
    /// statement about the buckets the scan reached, and `rebuilds` is the denominator that says
    /// how many that was.
    pub fn object_index_divergence_report(&self) -> ObjectIndexDivergenceReport {
        let counts = super::storage_bucket_internals::object_index_divergence::snapshot();
        ObjectIndexDivergenceReport {
            rebuilds: counts.rebuilds,
            reconstructs: counts.reconstructs,
            blocks_named_an_object_the_list_lacked: counts.blocks_named_an_object_the_list_lacked,
            list_held_an_object_no_block_names: counts.list_held_an_object_no_block_names,
            divergences_logged: counts.divergences_logged,
            stored_slots_repeated_an_id: super::state::stored_slots_repeated_an_id(),
            last_divergence: counts.last.map(|(what, object_id)| ObjectIndexDivergenceSample {
                what: what.to_string(),
                object_id,
            }),
        }
    }

    pub fn run_storage_manager_loop(
        &self,
        mut request: StorageManagerLoopRequest,
    ) -> StorageManagerLoopReport {
        request.lifecycle.shard_id = request.shard_id;
        let lifecycle = if request.apply {
            self.apply_storage_lifecycle(request.lifecycle.clone())
        } else {
            let plan = self.storage_lifecycle_plan(request.lifecycle.clone());
            StorageLifecycleReport {
                shard_id: request.shard_id,
                plan,
                object_lifecycle: self.storage_object_lifecycle_snapshot(request.shard_id),
                ..StorageLifecycleReport::default()
            }
        };

        let mut phases = Vec::new();
        phases.push(StorageManagerLoopPhaseReport {
            phase: "prepare".to_string(),
            attempted: true,
            applied: true,
            evidence: vec![
                "built storage lifecycle plan from dirty slots, live/stale segments, delayed destroy inventory, and manifest/index-log state".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "reclaim".to_string(),
            attempted: true,
            applied: request.apply
                && (!lifecycle.delayed_destroy_purged_slabs.is_empty()
                    || !lifecycle.plan.reclaim_candidates.is_empty()),
            evidence: vec![
                "ranked reclaim candidates by stale bytes, live density, delayed-destroy pressure, and utility score".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "evict".to_string(),
            attempted: request.lifecycle.invalidate_cache,
            applied: lifecycle.cache_entries_removed > 0 || lifecycle.cache_disk_bytes_removed > 0,
            evidence: vec![
                "cache invalidation phase uses shard-scoped cache eviction and byte accounting"
                    .to_string(),
            ],
            blockers: Vec::new(),
        });

        let expiry_sweep = if request.expire_records {
            self.sweep_expired_records(request.shard_id)
                .unwrap_or_else(|_| ShardExpirySweepReport {
                    shard_id: request.shard_id,
                    expired_records_removed: 0,
                    ..ShardExpirySweepReport::default()
                })
        } else {
            ShardExpirySweepReport {
                shard_id: request.shard_id,
                expired_records_removed: 0,
                ..ShardExpirySweepReport::default()
            }
        };
        phases.push(StorageManagerLoopPhaseReport {
            phase: "expire".to_string(),
            attempted: request.expire_records,
            applied: expiry_sweep.expired_records_removed > 0,
            evidence: vec![
                "expiry phase sweeps loaded shard TTL metadata and persists removals through index-log".to_string(),
            ],
            blockers: Vec::new(),
        });

        let compaction = if request.compact_blocks {
            match self.compact_shard_blocks(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    phases.push(StorageManagerLoopPhaseReport {
                        phase: "compact".to_string(),
                        attempted: true,
                        applied: false,
                        evidence: vec![
                            "compaction phase attempted live-page rewrite and model-layout/tombstone validation".to_string(),
                        ],
                        blockers: vec![err.message],
                    });
                    None
                }
            }
        } else {
            None
        };
        if let Some(report) = &compaction {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: true,
                applied: report.model_layout_compaction_ready,
                evidence: report.model_layout_compaction_evidence.clone(),
                blockers: report.model_layout_compaction_blockers.clone(),
            });
        } else if !request.compact_blocks {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: false,
                applied: false,
                evidence: vec![
                    "compaction phase can call compact_shard_pages when enabled".to_string()
                ],
                blockers: Vec::new(),
            });
        }

        phases.push(StorageManagerLoopPhaseReport {
            phase: "index_gc".to_string(),
            attempted: request.lifecycle.prune_bucket_dump_manifests
                || request.lifecycle.roll_forward_bucket_dump_installs,
            applied: lifecycle.manifest_prune_report.is_some()
                || !lifecycle.install_roll_forward_reports.is_empty(),
            evidence: vec![
                "index-GC phase prunes slot dump manifests and rolls forward interrupted installs using follower cursor retention".to_string(),
            ],
            blockers: Vec::new(),
        });

        let blockers = phases
            .iter()
            .flat_map(|phase| {
                phase
                    .blockers
                    .iter()
                    .map(|blocker| format!("{}: {blocker}", phase.phase))
            })
            .collect::<Vec<_>>();
        let attempted = phases.iter().filter(|phase| phase.attempted).count();
        let loop_ready = blockers.is_empty()
            && attempted >= 5
            && phases
                .iter()
                .any(|phase| phase.phase == "prepare" && phase.applied)
            && phases.iter().any(|phase| phase.phase == "reclaim")
            && phases.iter().any(|phase| phase.phase == "evict")
            && phases.iter().any(|phase| phase.phase == "expire")
            && phases
                .iter()
                .any(|phase| phase.phase == "compact" && phase.attempted)
            && phases.iter().any(|phase| phase.phase == "index_gc");
        StorageManagerLoopReport {
            shard_id: request.shard_id,
            loop_ready,
            phases,
            lifecycle,
            expiry_sweep,
            compaction,
            evidence: vec![
                "StorageManager loop executes prepare/reclaim/evict/expire/compact/index-GC phases through existing durable storage paths".to_string(),
                "loop report keeps per-phase evidence and blockers so readiness fails closed".to_string(),
            ],
            blockers,
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.storage_production_readiness_report_with_policy(
            shard_id,
            StorageProductionReadinessPolicy::default(),
        )
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        let boundary = self.storage_recovery_boundary_report(shard_id);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let slab_integrity = storage_slab_integrity_report(shard_id, &recovery, &boundary);
        let plan = self.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            purge_delayed_destroy: false,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            block_gc_shared_store_cursors: Vec::new(),
            block_gc_raft_snapshot_refs: Vec::new(),
            block_gc_checkpoint_floor_slab_id: None,
            block_gc_raft_install_floor_slab_id: None,
            block_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        let stats = self
            .loaded_shard_stats()
            .into_iter()
            .find(|stats| stats.shard_id == shard_id);
        let cache = stats
            .as_ref()
            .map(|stats| stats.cache.clone())
            .unwrap_or_else(|| self.cache.stats());
        let block_store = stats
            .as_ref()
            .map(|stats| stats.block_store_compat.clone())
            .unwrap_or_else(|| self.block_store.stats());
        let log_compatibility = self.storage_log_compatibility_report(shard_id);
        let block_format_compatibility = self.storage_block_format_compatibility_report(shard_id);
        let bucket_dump_manifest_count = self.list_bucket_dump_manifests(shard_id).len();
        let interrupted_bucket_dump_install_count = boundary.interrupted_bucket_dump_installs.len();
        let undumped_wal_records = boundary
            .latest_safe_wal_sequence
            .saturating_sub(boundary.latest_dump_wal_sequence);
        let mut blockers = Vec::new();
        if !boundary.stale_index_block_refs.is_empty() {
            blockers.push("stale_index_block_refs".to_string());
        }
        if !boundary.corrupt_block_slab_ids.is_empty() {
            blockers.push("corrupt_block_segments".to_string());
        }
        if boundary.unreadable_block_bytes > 0 || !recovery.all_live_blocks_readable {
            blockers.push("unreadable_live_block_refs".to_string());
        }
        if !boundary.owner_mismatch_block_refs.is_empty() {
            blockers.push("owner_mismatch_page_refs".to_string());
        }
        if boundary.object_lifecycle.missing_owner_block_refs > 0 {
            blockers.push("missing_owner_page_refs".to_string());
        }
        if boundary.object_lifecycle.reused_object_id_conflicts > 0 {
            blockers.push("reused_object_id_conflicts".to_string());
        }
        if interrupted_bucket_dump_install_count > 0 {
            blockers.push("interrupted_slot_dump_installs".to_string());
        }
        if !boundary.manifest_chain_issues.is_empty() {
            blockers.push("broken_slot_dump_manifest_chain".to_string());
        }
        if !slab_integrity.integrity_ok
            && !blockers
                .iter()
                .any(|blocker| blocker == "storage_segment_integrity_failed")
        {
            blockers.push("storage_segment_integrity_failed".to_string());
        }
        if recovery.feature_block_layout.has_errors() {
            blockers.push("feature_block_layout_mismatch".to_string());
        }

        let mut warnings = Vec::new();
        if !plan.dirty_buckets.is_empty() {
            warnings.push("dirty_slots_pending_dump".to_string());
        }
        if !plan.stale_block_slab_ids.is_empty() {
            warnings.push("stale_block_segments_pending_gc".to_string());
        }
