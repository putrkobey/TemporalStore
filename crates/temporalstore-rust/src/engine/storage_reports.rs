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
