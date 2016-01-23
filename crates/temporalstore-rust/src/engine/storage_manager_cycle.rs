// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage-manager cycle + merged-dump policy report for TemporalEngine, split from engine.rs.
//!
//! CROSS-SHARD RECLAIM, always on. Page-slab reclaim retains any slab referenced by ANY shard
//! loaded into this engine, not only the shard whose cycle is running: one engine shares a single
//! page_store across all its shards with a global slab cursor, so a slab can hold committed pages
//! from multiple shards, and driving GC off a single shard's live set deletes another shard's live
//! pages (silent data loss). `TS_CROSS_SHARD_RECLAIM_GUARD` used to be able to restore the legacy
//! per-shard live set; nothing selected that position, and it was unsafe under multi-shard hosting
//! by construction. For a single loaded shard the union equals that shard's live set, so
//! single-shard behavior was byte-identical either way.

use super::*;
use crate::engine::reports::StageWalkCharges;

/// One stage's measured span: how long its WORK took, and how much of the store that work walked.
///
/// WHY THE SPANS ARE CLOSED AT THE WORK AND NOT AT THE ROW. The stage rows are not built in the
/// order the stages RUN. `expire`, `reclaim_page`, `evict` and `index_gc` all do their work before
/// `reclaim_wal`'s row is pushed, so a clock that simply restarted at every push charged all four
/// of them to `reclaim_wal` -- which then reported `skipped: true` beside the largest cost in the
/// round, while the four stages that had actually done the work reported nothing.
///
/// Measured before this existed, on a store of 8,000 records taking a proportional round:
/// `reclaim_wal` 503 ms and 39,360 live-page entries with `applied: false`, against `expire` 0 ms
/// / 0 entries having removed 128 records, and `reclaim_page`, `evict` and `index_gc` at 0 ms and
/// 0 entries each. This is the same defect mx#1435 fixed for `prepare` by giving the plan its own
/// row -- `prepare` was reporting 328 ms at 32,000 records for pre-allocating one slab -- and it
/// was still live for four more stages, which is why the round's own breakdown could not be used
/// to decide which stage to bound.
///
/// A stage whose work is in two pieces (`reclaim_wal` dumps before it reclaims, with `evict`
/// between) closes a span for each and adds them, so the row reports the stage rather than the
/// interval.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StageSpan {
    duration_ms: u64,
    walk: StageWalkCharges,
}

impl StageSpan {
    /// Close the span running since `clock` was last restarted, and start the next one.
    ///
    /// Read-and-restart in one call for both the clock and the walk accumulators, so no interval
    /// is charged twice and none is dropped between two stages.
    fn close(clock: &mut std::time::Instant) -> Self {
        let duration_ms = clock.elapsed().as_millis() as u64;
        *clock = std::time::Instant::now();
        Self {
            duration_ms,
            walk: crate::engine::take_stage_walk_charges(),
        }
    }

    /// The two pieces of a stage whose work is not one interval.
    fn plus(self, other: Self) -> Self {
        Self {
            duration_ms: self.duration_ms.saturating_add(other.duration_ms),
            walk: StageWalkCharges {
                live_block_entries: self
                    .walk
                    .live_block_entries
                    .saturating_add(other.walk.live_block_entries),
                bucket_block_index_visits: self
                    .walk
                    .bucket_block_index_visits
                    .saturating_add(other.walk.bucket_block_index_visits),
                index_encodes: self
                    .walk
                    .index_encodes
                    .saturating_add(other.walk.index_encodes),
                index_encode_bytes: self
                    .walk
                    .index_encode_bytes
                    .saturating_add(other.walk.index_encode_bytes),
            },
        }
    }
}

/// How many live pages one maintenance round checks for readability.
///
/// The round used to check every one. A bound makes the cost of the check independent of the
/// store's size, and 512 is chosen the way the other round bounds were: large enough that a
/// small store is still fully checked every round, small enough that a large one pays a fixed
/// price. At the measured ~18 microseconds per page read that is about 9 ms.
pub(super) const RECOVERY_READABLE_PROBE_PER_ROUND: usize = 512;

impl TemporalEngine {
    pub fn run_storage_manager_cycle(
        &self,
        request: StorageManagerCycleRequest,
    ) -> StorageManagerCycleReport {
        let cycle_started_unix_ms = now_ms();
        // The same instant, on the clock that cannot be adjusted. The pair is deliberate: one
        // says when, the other says how long.
        let cycle_started_at = std::time::Instant::now();
        // Each stage is timed from the end of the previous one, so the durations tile the cycle
        // instead of overlapping it. Read and restart in the same expression, exactly once per
        // stage, where that stage's report is built.
        //
        // Deliberately not a closure. A `move` closure capturing only an `Instant` captures a Copy
        // type, which makes the closure itself Copy -- the restart then applies to a copy and every
        // stage reports the time since the CYCLE began instead of since the previous stage. That
        // was the first version here, and the totals gave it away: eight stages summing to eight
        // times the wall clock of the call that produced them.
        let mut stage_clock = std::time::Instant::now();
        // The stage accumulators start this round at zero for the same reason `stage_clock` is
        // taken here: `plan` is charged from the top of the round, and whatever ran before the
        // round is not the round's cost.
        crate::engine::reset_stage_walk_charges();
        // Short-circuit while the shard is RECOVERING (WAL replay in progress). The cycle
        // mutates shard state (eviction / page + WAL reclaim / compaction); a GC or compaction
        // round interleaved with an in-flight replay would observe a half-reconstructed bucket
        // index and could mis-reclaim a page that is still live -> silent durable loss. The
        // synchronous POST /load path calls load_shard_with directly (it never registers in
        // running_shards), so `recovering` is the reliable, path-independent guard: it is set
        // for the whole replay window by both the async and the synchronous load. A recovering
        // shard yields an inert (no-op) report; the next cycle after recovery runs normally.
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.recovering)
            .unwrap_or(false)
        {
            return StorageManagerCycleReport {
                shard_id: request.shard_id,
                dry_run: request.dry_run,
                completed: false,
                errors: vec![format!(
                    "storage manager cycle skipped for shard {}: recovery (WAL replay) is in progress; GC/compaction must not interleave with replay",
                    request.shard_id
                )],
                ..StorageManagerCycleReport::default()
            };
        }
        let native_stage_order = [
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let plan_request = StorageLifecycleRequest {
            shard_id: request.shard_id,
            selected_dump_buckets: request.selected_dump_buckets.clone(),
            max_dump_buckets_per_round: request.max_dump_buckets_per_round,
            min_undumped_wal_records: if request.enable_wal_reclaim {
                request.min_undumped_wal_records
            } else {
                u64::MAX
            },
            // Unreachable when reclaim is off, exactly as the record threshold is. A byte
            // threshold left live here would release a dump the operator switched off -- the
            // two thresholds are alternatives for WHEN to dump, not for WHETHER to.
            min_undumped_wal_bytes: if request.enable_wal_reclaim {
                request.min_undumped_wal_bytes
            } else {
                u64::MAX
            },
            purge_delayed_destroy: request.enable_block_reclaim,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: request.enable_index_gc,
            roll_forward_bucket_dump_installs: request.enable_index_gc,
            follower_replay_cursors: request.follower_replay_cursors.clone(),
            block_gc_shared_store_cursors: request.block_gc_shared_store_cursors.clone(),
            block_gc_raft_snapshot_refs: request.raft_snapshot_refs.clone(),
            block_gc_checkpoint_floor_slab_id: request.block_gc_checkpoint_floor_slab_id,
            block_gc_raft_install_floor_slab_id: request.block_gc_raft_install_floor_slab_id,
            block_gc_delayed_destroy_grace_ms: request.block_gc_delayed_destroy_grace_ms,
            invalidate_cache: false,
            warm_cache: request.warm_cache,
        };
        let plan = self.storage_lifecycle_plan(plan_request.clone());
        let block_gc_dependency_plan = self.storage_block_gc_dependency_plan(
            request.shard_id,
            plan.reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id),
            request.block_gc_shared_store_cursors.clone(),
            request.raft_snapshot_refs.clone(),
            request.block_gc_checkpoint_floor_slab_id,
            request.block_gc_raft_install_floor_slab_id,
            request.block_gc_delayed_destroy_grace_ms,
        );
        // THE ONE PLACE "did not look" COLLAPSES TO A ZERO IN THIS FUNCTION.
        //
        // `plan.bucket_summaries` is `None` when the round skipped the whole-shard walk because
        // no bucket was dirty. That is NOT "the shard holds nothing": a settled shard with
        // millions of live pages reports `None` every round. The stage figures below are derived
        // from it and therefore read 0 on such a round.
        //
        // What keeps that 0 honest is that the plan travels WITH them:
        // `StorageManagerCycleReport.plan.bucket_summaries` is the same `Option`, so a reader of
        // any of these byte or count figures can tell an unmeasured 0 from a measured one without
        // a second source. `live_block_summaries_measured` below is that test, named once.
        let bucket_summaries = plan.bucket_summaries.as_deref();
        let live_block_summaries_measured = bucket_summaries.is_some();
        let bucket_logical_bytes = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.logical_bytes)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_physical_bytes = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.physical_bytes)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_block_ref_count = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.block_ref_count)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_count = bucket_summaries.map(|summaries| summaries.len()).unwrap_or(0);
        let reclaim_live_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.live_physical_bytes)
            .sum::<u64>();
        let reclaim_stale_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.stale_physical_bytes)
            .sum::<u64>();
        let reclaim_candidate_count = plan.reclaim_candidates.len();
        let reclaim_skipped_count = plan
            .stale_block_slab_ids
            .len()
            .saturating_sub(reclaim_candidate_count);
        let log_pressure = self.storage_log_pressure_report(request.shard_id);
        let cache_pressure = self.storage_cache_inspection_report(request.shard_id);
        let block_slab_total_bytes = reclaim_live_bytes.saturating_add(reclaim_stale_bytes);
        let block_slab_stale_density_basis_points = if block_slab_total_bytes == 0 {
            0
        } else {
            reclaim_stale_bytes.saturating_mul(10_000) / block_slab_total_bytes
        };
        let delayed_destroy_slab_count = plan.delayed_destroy_block_slab_ids.len();
        let delayed_destroy_bytes = plan
            .reclaim_candidates
            .iter()
            .filter(|candidate| candidate.reason == "delayed_destroy")
            .map(|candidate| candidate.physical_bytes)
            .sum::<u64>();
        let expired_bucket_object_scan_debt = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| shard.expires_at_ms().len())
            .unwrap_or_default();
        let compaction_utility = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| compaction_utility_report(&self.block_store, shard))
            .unwrap_or_default();
        let compaction_debt_model_count = compaction_utility
            .model_policies
            .iter()
            .filter(|policy| {
                policy.stale_block_estimate > 0
                    || policy.stale_density_basis_points > 0
                    || policy.delete_marker_density_basis_points > 0
            })
            .count()
            .max(usize::from(block_slab_stale_density_basis_points > 0));
        let compaction_debt_score = compaction_utility
            .model_policies
            .iter()
            .map(|policy| {
                policy
                    .stale_block_estimate
                    .saturating_add(policy.stale_density_basis_points)
                    .saturating_add(policy.delete_marker_density_basis_points)
            })
            .sum::<u64>()
            .saturating_add(compaction_utility.stale_block_estimate)
            .saturating_add(reclaim_stale_bytes)
            .saturating_add(block_slab_stale_density_basis_points);
        let retention_prune_plan = self.bucket_dump_manifest_prune_plan_with_retention_refs(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let manifest_retention_blockers = retention_prune_plan
            .follower_blocks
            .len()
            .saturating_add(retention_prune_plan.raft_snapshot_blocks.len());
        let memory_cache_pressure_score = cache_pressure
            .stats
            .memory_bytes
            .saturating_add(cache_pressure.stats.pinned_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_depth);
        // The bucket index, which no memory term above can see. Kept out of
        // `memory_cache_pressure_score` on purpose -- see the field doc on
        // `StorageManagerPressureSignals::bucket_index_resident_bytes`: that score is re-derived
        // after the warm, by subtracting itself out of the total, and anything folded into it
        // would be subtracted away with it.
        let bucket_index_resident_bytes = self.bucket_index_resident_bytes(request.shard_id);
        let total_pressure_score = plan
            .dirty_buckets
            .len()
            .saturating_add(expired_bucket_object_scan_debt)
            .saturating_add(delayed_destroy_slab_count)
            .saturating_add(compaction_debt_model_count) as u64
            + plan.undumped_wal_records
            + log_pressure.wal_bytes
            + log_pressure.index_log_bytes
            + reclaim_stale_bytes
            + cache_pressure.stats.disk_bytes
            + memory_cache_pressure_score
            + bucket_index_resident_bytes
            + delayed_destroy_bytes
            + manifest_retention_blockers as u64
            + compaction_debt_score;
        let mut pressure_signals = StorageManagerPressureSignals {
            live_block_summaries_measured,
            dirty_bucket_count: plan.dirty_buckets.len(),
            undumped_wal_records: plan.undumped_wal_records,
            wal_bytes: log_pressure.wal_bytes,
            index_log_bytes: log_pressure.index_log_bytes,
            stale_block_bytes: reclaim_stale_bytes,
            live_block_bytes: reclaim_live_bytes,
            block_slab_stale_density_basis_points,
            memory_cache_bytes: cache_pressure.stats.memory_bytes,
            disk_cache_bytes: cache_pressure.stats.disk_bytes,
            bucket_index_resident_bytes,
            memory_cache_pressure_score,
            expired_bucket_object_scan_debt,
            delayed_destroy_slab_count,
            delayed_destroy_bytes,
            follower_cursor_retention_blockers: retention_prune_plan.follower_blocks.len(),
            raft_snapshot_retention_blockers: retention_prune_plan.raft_snapshot_blocks.len(),
            compaction_debt_model_count,
            compaction_debt_score,
            total_pressure_score,
        };
        let plan_span = StageSpan::close(&mut stage_clock);
        let mut stages = Vec::new();
        let mut errors = Vec::new();
        // Everything above gets its own stage, for the reason mx#1435 gave for the dump-load
        // policy report: `stage_clock` runs from the previous stage's push, and this is the FIRST
        // stage boundary in the round. So `storage_lifecycle_plan` (which surveys the shard),
        // `storage_block_gc_dependency_plan` and the pressure-snapshot arithmetic were all being
        // charged to `prepare` -- which pre-allocates the next slab and surveys nothing, yet
        // reported 328 ms at 32,000 records.
        stages.push(StorageManagerStageReport {
            duration_ms: plan_span.duration_ms,
            walk: plan_span.walk,
            stage: "plan".to_string(),
            enabled: true,
            applied: true,
            skipped: false,
            reason: "lifecycle plan, page-GC dependency plan and the pressure snapshot"
                .to_string(),
            ..StorageManagerStageReport::default()
        });
        // PREPARE ROLLS THE NEXT DATA SLAB, which is the only thing this stage has to do that
        // nothing else in the round does.
        //
        // Rolling is not cheap: it fsyncs the outgoing slab, scans the slab directory to pick
        // the next id, creates and fsyncs the new file, fsyncs the parent directory and
        // persists the slab manifest. Left to `append`, all of that lands on one unlucky client
        // write as a latency outlier unrelated to the size of the write that triggered it.
        //
        // TWO ROUND DRIVERS, AND ONLY ONE OF THEM DID THIS. `run_storage_manager_once` has
        // pre-allocated here since the stage was written; this one -- the driver the data-node
        // worker and the embedded proxy both run -- reported `applied: true` for a stage whose
        // body was a report and nothing else. A deployment driven by this cycle therefore paid
        // the inline roll on the write path on every slab boundary, which is the exact cost the
        // stage exists to move off it.
        //
        // A FAILURE IS RECORDED AND DOES NOT STOP THE ROUND, matching the other driver: the
        // inline roll in `append` is still there, so the only consequence of a failure here is
        // that the next append pays for the roll exactly as it does today. A stronger reaction --
        // returning before any other stage runs -- would be right if this failure invalidated the
        // stages after it, and it does not: nothing else in this round reads the active slab's
        // identity, so stopping the round would cost the other stages for nothing.
        //
        // A no-op while the active slab is under target, so the ordinary round pays one lock
        // and one comparison.
        let mut prepared_block_slab_id = None;
        if request.enable_prepare && !request.dry_run {
            let slab_target_bytes = if request.prepare_slab_target_bytes == 0 {
                crate::storage_config::effective_block_slab_target_bytes()
            } else {
                request.prepare_slab_target_bytes
            };
            match self
                .block_store()
                .prepare_next_slab_with_target(slab_target_bytes)
            {
                Ok(Some(roll)) => prepared_block_slab_id = Some(roll.new_block_slab_id),
                Ok(None) => {}
                Err(error) => errors.push(format!("prepare: {error}")),
            }
        }
        let prepare_span = StageSpan::close(&mut stage_clock);
        stages.push(StorageManagerStageReport {
            duration_ms: prepare_span.duration_ms,
            walk: prepare_span.walk,
            stage: "prepare".to_string(),
            enabled: request.enable_prepare,
            applied: request.enable_prepare && !request.dry_run,
            skipped: !request.enable_prepare,
            reason: if request.enable_prepare {
                "prepared storage lifecycle pressure view and index/page-store metadata".to_string()
            } else {
                "prepare disabled".to_string()
            },
            selected_block_slab_ids: plan.live_block_slab_ids.clone(),
            prepared_block_slab_id,
            pressure_signal:
                "dirty_slots+wal_bytes+index_log_bytes+stale_density+cache_pressure+expire_debt+delayed_destroy+retention_blockers+model_compaction_debt"
                    .to_string(),
            pressure_score: pressure_signals.total_pressure_score,
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.total_pressure_score > 0,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count,
            before_bytes: bucket_physical_bytes,
            after_bytes: bucket_physical_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            dirty_bucket_count: plan.dirty_buckets.len(),
            undumped_wal_records: plan.undumped_wal_records,
            metrics_bucket_count: bucket_count,
            metrics_block_ref_count: bucket_block_ref_count,
            ..StorageManagerStageReport::default()
        });

        let mut expiry_report = None;
        if request.enable_expire && !request.dry_run {
            match self.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: request.shard_id,
                hot_cursor: request.expire_hot_cursor.clone(),
                cold_cursor: request.expire_cold_cursor.clone(),
                max_hot_buckets_per_round: request.max_expire_hot_buckets_per_round,
                max_cold_buckets_per_round: request.max_expire_cold_buckets_per_round,
                load_cold_buckets: request.load_cold_buckets_for_expire,
            }) {
                Ok(report) => expiry_report = Some(report),
                Err(err) => errors.push(format!("expire: {}", err.message)),
            }
        }

        let expire_span = StageSpan::close(&mut stage_clock);

        // ONE PINNED SLAB NO LONGER SUPPRESSES THE REST.
        //
        // The dependency plan already works out, per slab, which candidates are pinned and which
        // are free, and `reclaimable_block_slab_ids` has always named the free subset. Both
        // reclaim stages then threw that away and consulted `safe_to_reclaim` -- a single
        // store-wide boolean that is false whenever ANY candidate is blocked. A shard with one
        // slab behind a lagging follower's replay cursor collected nothing at all, for as long
        // as that follower stayed behind, however many other slabs were free.
        //
        // Gating per slab instead requires the purge below to take a LIST, and the two changes
        // only make sense together: a purge that ran because some slab was free, but still swept
        // the whole trash directory, would destroy exactly the quarantined slabs the plan had
        // blocked. That is a loss path, not an inefficiency, which is why the collector's
        // selection and the purge's selection move in the same commit.
        // HAND THE COLLECTOR THE TALLY BEFORE IT PLANS.
        //
        // Without a publish the block store falls back to its older `used_bytes` figure, which
        // sums the file sizes of the NEIGHBOURING slabs that are not collectable -- a quantity
        // that is zero for every candidate by construction and would be wrong rather than zero if
        // it ever were not. Published, `used_bytes` is the live page bytes on the slab itself.
        //
        // Costs the number of slabs, not the number of pages: the shards keep the tally running
        // on their own mutation path and this only sums them.
        self.publish_block_slab_live_bytes();
        let reclaimable_block_slab_ids = block_gc_dependency_plan
            .reclaimable_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if request.enable_block_reclaim
            && !request.dry_run
            && !plan.reclaim_candidates.is_empty()
            && !reclaimable_block_slab_ids.is_empty()
        {
            let retain_from_block_slab_id = plan
                .reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id)
                .max()
                .unwrap_or_default()
                .saturating_add(1);
            // Retain slabs live in ANY shard sharing this engine's page_store, not just the
            // shard being cycled (see the module header): otherwise a slab whose pages belong to
            // another shard is absent from this shard's live set and gets deleted.
            let reclaim_live_refs = self.live_block_slab_ids_all_shards();
            match self.block_store.gc_slabs_before_with_live_refs_policy_limited(
                retain_from_block_slab_id,
                reclaim_live_refs,
                // garbage-ratio victim selection: collect the highest-garbage slabs first,
                // keeping slabs below the garbage floor. Floor 0 (the default) collects every
                // eligible slab as before.
                BlockStoreGcPolicy::with_slab_garbage_floor(
                    request.block_gc_min_slab_garbage_basis_points,
                    None,
                ),
                true,
                Some(reclaimable_block_slab_ids.clone()),
            ) {
                Ok(report) => {
                    for block_slab_id in report
                        .removed_block_slab_ids
                        .iter()
                        .chain(report.delayed_destroy_block_slab_ids.iter())
                    {
                        let _ = self
                            .cache
                            .invalidate_page_segment(request.shard_id, *block_slab_id);
                    }
                    // AND THE RECORDED STALE RANGES FOR THOSE SLABS GO WITH THEM. The prune that
                    // bounds the record by the collector's own progress rather than only by its
                    // cap: a slab that has been removed, or quarantined for destruction, is no
                    // longer carrying bytes PENDING a discard -- it is carrying bytes the collector
                    // has taken, which `reclaimable_physical_bytes` already reports. Leaving the
                    // ranges here would count the same garbage in two fields of one report and
                    // would keep `compaction_recommended` true over slabs that no longer exist.
                    //
                    // The same iteration as the cache invalidation above, and for the same reason
                    // it is written beside it: both are "this slab is gone, forget what you knew
                    // about it", and splitting them invites one to be updated without the other.
                    let _ = self.discard_recorded_stale_ranges(
                        report
                            .removed_block_slab_ids
                            .iter()
                            .chain(report.delayed_destroy_block_slab_ids.iter())
                            .copied()
                            .collect::<Vec<_>>(),
                    );
                }
                Err(err) => errors.push(format!("reclaim_page: {err}")),
            }
        }

        let reclaim_block_span = StageSpan::close(&mut stage_clock);
        let lifecycle_report = if request.dry_run {
            None
        } else {
            let mut lifecycle_request = plan_request.clone();
            // The other half of the per-slab gate. The purge now runs whenever ANY quarantined
            // slab is free -- but it is handed the free set, so the slabs the plan blocked are
            // passed over rather than swept up by a round that was let in on someone else's
            // behalf. Flipping the condition without passing the list would turn a suppressed
            // reclaim into a destroyed dependency.
            lifecycle_request.purge_delayed_destroy = lifecycle_request.purge_delayed_destroy
                && !reclaimable_block_slab_ids.is_empty();
            lifecycle_request.purge_delayed_destroy_slab_ids =
                Some(reclaimable_block_slab_ids.iter().copied().collect());
            let report = Some(self.apply_storage_lifecycle(lifecycle_request));
            // AND AGAIN AFTERWARDS. The round just relocated pages onto a fresh slab and
            // destroyed others; the tally published above describes the state before it. A caller
            // that inspects the store after the round -- which is what every reclaim assertion
            // does -- should read what the round left, not what it found.
            self.publish_block_slab_live_bytes();
            report
        };
        // The dump belongs to `reclaim_wal`: the log cannot be reclaimed past what a dump has
        // made durable, so the dump is the price this stage pays and is charged to it.
        let reclaim_wal_dump_span = StageSpan::close(&mut stage_clock);
        // apply_storage_lifecycle's warm phase brings freshly-dumped pages into DRAM.
        // The pressure snapshot above was captured during prepare (pre-warm), so
        // re-measure memory residency here so the emitted snapshot reflects the warmed
        // cache. Only the memory fields are refreshed; disk_cache_bytes stays at its
        // prepare value (warmed pages also write through to SSD, and downstream
        // reclamation assertions compare against that pre-warm disk figure).
        if !request.dry_run {
            let warmed_cache = self.storage_cache_inspection_report(request.shard_id).stats;
            let warmed_memory_pressure = warmed_cache
                .memory_bytes
                .saturating_add(warmed_cache.pinned_bytes)
                .saturating_add(warmed_cache.async_writeback_queue_bytes)
                .saturating_add(warmed_cache.async_writeback_queue_depth);
            pressure_signals.total_pressure_score = pressure_signals
                .total_pressure_score
                .saturating_sub(pressure_signals.memory_cache_pressure_score)
                .saturating_add(warmed_memory_pressure);
            pressure_signals.memory_cache_bytes = warmed_cache.memory_bytes;
            pressure_signals.memory_cache_pressure_score = warmed_memory_pressure;
        }
        let eviction_report = if request.enable_evict {
            Some(if request.dry_run {
                StorageEvictionReport {
                    shard_id: request.shard_id,
                    // ONE MODE, because there is one arm. The dry run used to echo the request's
                    // flag and name `delete_drop`, which is now a mode this stage cannot reach --
                    // a dry run that predicts an arm the real round will not take is worse than no
                    // prediction.
                    mode: "evict_cache".to_string(),
                    pressure_before: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    pressure_after: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    memory_pressure_threshold: request.eviction_memory_pressure_threshold,
                    batch_limit: request.eviction_batch_limit,
                    dump_before_evict: request.eviction_dump_before_evict,
                    skipped_reason: "dry_run".to_string(),
                    ..StorageEvictionReport::default()
                }
            } else {
                self.apply_storage_eviction(
                    request.shard_id,
                    request.eviction_memory_pressure_threshold,
                    request.eviction_batch_limit,
                    request.eviction_dump_before_evict,
                    // NEVER, and the request no longer carries a field that could make it true --
                    // the same reading as the periodic loop's (`data_node/runtime_storage.rs`).
                    // `delete_drop` deletes the model-map entry that is the only thing pointing at
                    // a record's payload block, and writes a tombstone so the deletion survives
                    // replay. No memory-pressure stage may reach it. The literal is the route's
                    // absence.
                    false,
                )
            })
        } else if request.index_resident_limit_bytes > 0 && !request.dry_run {
            // THE NARROW BOUND RUNS WITH THE WIDE STAGE OFF, which is the whole reason it is a
            // separate arm. `enable_evict` carries a threshold, and the shipped threshold is 0 --
            // "evicts whenever it runs" -- so a bound that inherited the flag's state would have
            // inherited an always-open gate. On the periodic loop the two are reconciled by taking
            // the higher bar; here, where the flag is explicit in the request, the flag wins and
            // this arm is what a caller gets by setting only the bound.
            //
            // It reports through the same field because it reaches the same release, so an
            // operator reads one eviction report either way -- `mode` says which gate opened.
            Some(self.apply_index_resident_bound(
                request.shard_id,
                request.index_resident_limit_bytes,
                request.eviction_batch_limit,
            ))
        } else {
            None
        };
        // The warm re-measure above feeds this stage's own pressure gate, so it is charged here
        // rather than to the dump that warmed the cache.
        let evict_span = StageSpan::close(&mut stage_clock);
        // Pages whose only durable copy is a WAL record leave the log HERE, before the reclaim
        // plan is computed, because `min_registered_sequence` pins retention to the LOWEST
        // registration: a page still resident holds the floor down and no retention policy can
        // pass it. That is the order this stage's counterpart uses in the design this
        // follows -- dump the dirty slots, advance the dumped-log id, and only then reclaim.
        //
        // Until this ran, the only caller that moved a page out of the log was the write path,
        // which sweeps once registrations pass `TS_WAL_RESIDENT_PAGES` and therefore stops the
        // moment writes stop. An idle shard kept every page it had ever written that way.
        // Measured on 48 async writes: a flush left all 48 registered, and so did a full
        // eight-stage cycle, with the log floor still at sequence 1.
        //
        // Bounded per cycle at `max_dump_buckets_per_round`, so a shard sitting at the write
        // path's ceiling drains over several cycles instead of turning one into a long pause.
        // Oldest first, because the oldest registration is the one holding the floor down. The
        // bytes are already durable in the log, so this moves a copy that is safe either way.
        let wal_resident_blocks_before = self.wal_resident_block_count(request.shard_id);
        let wal_resident_blocks_materialised = if request.enable_wal_reclaim
            && !request.dry_run
            && wal_resident_blocks_before > 0
        {
            // Zero is how this request spells "no bound" -- its own default is 0 while the
            // scheduler's is 64 -- so a cycle asked for no bound drains the shard rather than
            // one page per pass.
            let per_round = if request.max_dump_buckets_per_round == 0 {
                usize::MAX
            } else {
                request.max_dump_buckets_per_round
            };
            self.materialize_oldest_resident_blocks(
                request.shard_id,
                wal_resident_blocks_before.saturating_sub(per_round),
            )
        } else {
            0
        };

        let wal_reclaim_plan = self.storage_wal_reclaim_plan(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let wal_reclaim_report = if request.enable_wal_reclaim {
            Some(if request.dry_run {
                StorageWalReclaimReport {
                    plan: wal_reclaim_plan.clone(),
                    applied: false,
                    ..StorageWalReclaimReport::default()
                }
            } else {
                self.apply_storage_wal_reclaim(wal_reclaim_plan.clone())
            })
        } else {
            None
        };
        let reclaim_wal_span = reclaim_wal_dump_span.plus(StageSpan::close(&mut stage_clock));
        let index_gc_report = Some(self.storage_index_gc_report(
            &plan,
            &wal_reclaim_plan,
            lifecycle_report.as_ref(),
            &request,
        ));

        let index_gc_span = StageSpan::close(&mut stage_clock);
        stages.push(StorageManagerStageReport {
            duration_ms: reclaim_wal_span.duration_ms,
            walk: reclaim_wal_span.walk,
            stage: "reclaim_wal".to_string(),
            wal_resident_blocks_materialised,
            enabled: request.enable_wal_reclaim,
            applied: wal_reclaim_report
                .as_ref()
                .map(|report| report.applied)
                .unwrap_or(false),
            skipped: !request.enable_wal_reclaim
                || plan.dump_delayed
                || wal_reclaim_report
                    .as_ref()
                    .map(|report| !report.plan.safe_to_reclaim)
                    .unwrap_or(true),
            reason: if !request.enable_wal_reclaim {
                "wal reclaim disabled".to_string()
            } else if plan.dump_delayed {
                "dirty slot dump delayed until the configured undumped log threshold is reached"
                    .to_string()
            } else if wal_reclaim_report
                .as_ref()
                .map(|report| !report.plan.safe_to_reclaim)
                .unwrap_or(true)
            {
                format!(
                    "WAL/index-log reclaim blocked until durable slot generations and retention cursors allow it: {}",
                    wal_reclaim_report
                        .as_ref()
                        .map(|report| report.plan.blocker_reasons.join(","))
                        .unwrap_or_default()
                )
            } else {
                "reclaimed WAL/index-log through the slot-generation durable dump frontier"
                    .to_string()
            },
            selected_buckets: plan.selected_dump_buckets.clone(),
            pressure_signal:
                "durable_slot_generation_frontier+follower_snapshot_retention+wal_bytes+index_log_bytes"
                    .to_string(),
            pressure_score: pressure_signals
