// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DataNodeRuntime storage-manager scheduling/lifecycle methods, extracted from data_node.rs.

use super::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// The four terms of the storage-eviction loop's continue-decision, named so the count and the
/// vacuity of individual terms can be asserted instead of described in prose.
///
/// Two of the four are constants at the shipped default threshold of 0; see the comment at the
/// loop in `run_storage_manager_round_inner` for which and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictionLoopTerms {
    /// `apply_storage_eviction` did not take its `pressure_before < threshold` early return.
    pub(crate) pressure_gate_open: bool,
    /// The per-call victim count budget has room left.
    pub(crate) more_budget: bool,
    /// `pressure_after >= threshold` -- the loop has not reached its goal.
    pub(crate) still_over: bool,
    /// The last batch took victims and was not in cooldown.
    pub(crate) made_progress: bool,
}

/// The terms that can be `false` at `threshold`, i.e. the ones that can actually stop the loop.
///
/// At a threshold of 0 the two threshold-derived terms are constants -- no `u64` is below 0, so
/// `apply_storage_eviction` cannot take its early return and `pressure_after >= 0` cannot be
/// false -- and only the budget and the progress checks remain. This is the DENOMINATOR for the
/// guard: 2 of 4 live at the shipped default, 4 of 4 once an operator raises the threshold.
pub(crate) fn eviction_loop_live_stopping_conditions(threshold: u64) -> &'static [&'static str] {
    if threshold == 0 {
        &["more_budget", "made_progress"]
    } else {
        &[
            "pressure_gate_open",
            "more_budget",
            "still_over",
            "made_progress",
        ]
    }
}

/// How many ROUNDS the evict stage may take, derived from the victim-count budget.
///
/// A round that is doing its job takes at least one victim, so after `count_limit` rounds the
/// victim budget is spent anyway and this bound has changed nothing. It exists for the round
/// that takes NONE -- see `eviction_loop_terms_for_batch`.
pub(crate) fn eviction_loop_round_limit(count_limit: usize) -> usize {
    count_limit
}

/// The four terms, computed FROM a batch report -- the wiring, as opposed to the predicate.
///
/// WHY THIS IS A FUNCTION. The predicate had unit tests, and they construct `EvictionLoopTerms`
/// directly: nothing exercised the step that turns a report into terms, and that step is where
/// the loop could fail to terminate. Forcing `made_progress` true there spins the stage for ever
/// -- 27 of 29 tests in the area still passed, two ran unbounded, and libtest has no per-test
/// timeout, so the run was CANCELLED rather than failed. A cancelled run names no test.
///
/// WHY `more_budget` COUNTS ROUNDS AS WELL AS VICTIMS. `taken` is `victims.len()`, and victims
/// only grow when a batch returns some. A batch that returns NONE leaves `taken` exactly where
/// it was, so at the shipped budget of 100 the term `count_limit > taken` never goes false
/// however many times the loop goes round: the budget bounds WORK TAKEN, not WORK ATTEMPTED.
/// With `still_over` and `pressure_gate_open` both vacuous at the shipped threshold of 0, that
/// left `made_progress` as the entire termination argument -- one term, untested at this
/// boundary. The round bound makes the budget bound iterations too, so the worst a broken
/// progress term can now do is run `count_limit` rounds and stop, which a test can assert on.
pub(crate) fn eviction_loop_terms_for_batch(
    report: &crate::engine::reports::StorageEvictionReport,
    threshold: u64,
    count_limit: usize,
    taken: usize,
    rounds: usize,
) -> EvictionLoopTerms {
    EvictionLoopTerms {
        pressure_gate_open: report.pressure_gate_open,
        more_budget: count_limit > taken && rounds < eviction_loop_round_limit(count_limit),
        still_over: report.pressure_after >= threshold,
        // A batch that freed nothing, or found nobody, means the next will not do better.
        made_progress: !report.cooldown && !report.selected_victims.is_empty(),
    }
}

/// What the evict STAGE did, accumulated across its batches.
///
/// The stage reports what the STAGE did, not what its last batch did. Returning the final report
/// alone would describe the batch that stopped the loop -- usually the unproductive one that
/// tripped `cooldown` -- and hide every victim the earlier batches took, which is the shape of
/// misreporting #1453-#1455 fixed elsewhere.
pub(crate) struct EvictionStageTally {
    pub(crate) victims: Vec<crate::engine::reports::StorageEvictionVictim>,
    pub(crate) dump_manifest_ids: Vec<String>,
    pub(crate) cache_entries_removed: usize,
    pub(crate) cache_disk_bytes_removed: u64,
    pub(crate) dropped_object_count: usize,
    /// Batches taken AFTER the first. Bounded by `eviction_loop_round_limit`, and the number a
    /// termination guard asserts on.
    pub(crate) rounds: usize,
    pub(crate) first_pressure_before: u64,
    pub(crate) last: crate::engine::reports::StorageEvictionReport,
}

impl EvictionStageTally {
    /// The stage's report: counts accumulated, `pressure_before` from the FIRST batch and
    /// `pressure_after` from the last.
    pub(crate) fn into_stage_report(self) -> crate::engine::reports::StorageEvictionReport {
        let first_pressure_before = self.first_pressure_before;
        crate::engine::reports::StorageEvictionReport {
            pressure_before: first_pressure_before,
            selected_victims: self.victims,
            dump_manifest_ids: self.dump_manifest_ids,
            cache_entries_removed: self.cache_entries_removed,
            cache_disk_bytes_removed: self.cache_disk_bytes_removed,
            dropped_object_count: self.dropped_object_count,
            // `cooldown` means the STAGE freed nothing, not that its last batch did.
            cooldown: self.last.pressure_after >= first_pressure_before,
            ..self.last
        }
    }
}

/// Take batches until the pressure is back under the threshold, the budget is spent, or a batch
/// stops helping. `next_batch` is handed the remaining victim budget and returns the next batch.
///
/// The loop lives here, apart from the engine, so that a test can drive it with a batch sequence
/// of its own and assert how many ROUNDS it ran. Driving the real control rather than a copy of
/// it is the point: a test that re-implemented the decision would agree with itself whatever the
/// stage did.
pub(crate) fn run_eviction_stage<F>(
    threshold: u64,
    count_limit: usize,
    first: crate::engine::reports::StorageEvictionReport,
    mut next_batch: F,
) -> EvictionStageTally
where
    F: FnMut(usize) -> crate::engine::reports::StorageEvictionReport,
{
    let mut tally = EvictionStageTally {
        victims: first.selected_victims.clone(),
        dump_manifest_ids: first.dump_manifest_ids.clone(),
        cache_entries_removed: first.cache_entries_removed,
        cache_disk_bytes_removed: first.cache_disk_bytes_removed,
        dropped_object_count: first.dropped_object_count,
        rounds: 0,
        first_pressure_before: first.pressure_before,
        last: first,
    };
    let round_limit = eviction_loop_round_limit(count_limit);
    loop {
        let taken = tally.victims.len();
        let terms =
            eviction_loop_terms_for_batch(&tally.last, threshold, count_limit, taken, tally.rounds);
        debug_assert!(
            terms.stops_only_on_live_conditions(threshold),
            "eviction loop stopped on a condition that cannot fire at threshold {threshold}: {terms:?}",
        );
        if !terms.should_continue() {
            break;
        }
        debug_assert!(
            tally.rounds < round_limit,
            "the eviction loop ran {} rounds against a limit of {round_limit}",
            tally.rounds,
        );
        let report = next_batch(count_limit.saturating_sub(taken));
        tally.rounds = tally.rounds.saturating_add(1);
        tally.victims.extend(report.selected_victims.iter().cloned());
        tally
            .dump_manifest_ids
            .extend(report.dump_manifest_ids.iter().cloned());
        tally.cache_entries_removed = tally
            .cache_entries_removed
            .saturating_add(report.cache_entries_removed);
        tally.cache_disk_bytes_removed = tally
            .cache_disk_bytes_removed
            .saturating_add(report.cache_disk_bytes_removed);
        tally.dropped_object_count = tally
            .dropped_object_count
            .saturating_add(report.dropped_object_count);
        tally.last = report;
    }
    tally
}

impl EvictionLoopTerms {
    /// Take another batch only if every term holds.
    pub(crate) fn should_continue(self) -> bool {
        self.pressure_gate_open && self.more_budget && self.still_over && self.made_progress
    }

    /// True unless the loop is stopping ONLY because of a term that cannot fire at `threshold`.
    ///
    /// A stated precondition that is printed but not enforced is not one, so this is checked at
    /// the loop rather than left to the comment. At a threshold of 0 a stop must be attributable
    /// to `more_budget` or `made_progress`; a stop attributable only to `still_over` or
    /// `pressure_gate_open` means someone has made a vacuous term load-bearing.
    pub(crate) fn stops_only_on_live_conditions(self, threshold: u64) -> bool {
        if self.should_continue() {
            return true;
        }
        let live = eviction_loop_live_stopping_conditions(threshold);
        live.iter().any(|name| match *name {
            "pressure_gate_open" => !self.pressure_gate_open,
            "more_budget" => !self.more_budget,
            "still_over" => !self.still_over,
            "made_progress" => !self.made_progress,
            other => unreachable!("unknown eviction stopping condition {other}"),
        })
    }
}

impl DataNodeRuntime {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        self.inner.engine.storage_lifecycle_plan(request)
    }

    pub fn storage_manager_pressure_snapshot(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
    ) -> (StorageManagerPressureSnapshot, StorageLifecyclePlan) {
        let plan = self
            .inner
            .engine
            .storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                min_undumped_wal_bytes: options.min_undumped_wal_bytes,
                purge_delayed_destroy: options.enable_block_gc,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: options.enable_index_gc,
                roll_forward_bucket_dump_installs: options.enable_index_gc,
                follower_replay_cursors: options.follower_replay_cursors.clone(),
                block_gc_shared_store_cursors: Vec::new(),
                block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                block_gc_checkpoint_floor_slab_id: None,
                block_gc_raft_install_floor_slab_id: None,
                block_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
        let cache = self.inner.engine.cache().stats();
        // THE NUMBER THAT WAS MISSING. Every memory term below was a cache term, so a shard whose
        // bucket index is large and whose cache is small -- the exact shape eviction exists for --
        // read as carrying no memory and was never asked to give any back.
        //
        // The moving figure (nodes + per-page entries), not the node-only floor, because the
        // per-page entries are what a release frees: the pressure this reports falls when the
        // evict stage acts on it, which is what makes it a gate and not a klaxon.
        let bucket_index_resident_bytes = self.inner.engine.bucket_index_resident_bytes(shard_id);
        // Mirrors `apply_storage_eviction`'s own gate term for term, so the reported signal and
        // the real decision cannot drift apart. ONE TERM, because that gate now reads one term:
        // this shard's resident index. The four cache terms it used to carry came from
        // `MultiLayerCache::stats()`, which is not filtered by shard, so what this field published
        // as "the shard's eviction pressure" was mostly other shards' cache -- 99.93% of it on a
        // ten-key shard beside an eight-thousand-key one, measured in
        // `the_evict_gates_cache_terms_are_process_wide_not_per_shard`.
        let eviction_memory_pressure_bytes = bucket_index_resident_bytes;
        // THE NODE MEMORY SCORE KEEPS THE WIDE COMPOSITION, and is now a different number from the
        // evict gate's input rather than an alias of it. It is published for an operator watching
        // the node, and `reclaim_memory` -- the stage that invalidates cached pages -- is the thing
        // it describes; narrowing it to the index would make it report a debt that stage cannot pay.
        // The two were one field only because the gate used to read the wide number.
        let memory_cache_pressure_score = cache
            .memory_bytes
            .saturating_add(cache.disk_bytes)
            .saturating_add(cache.async_writeback_queue_bytes)
            .saturating_add(cache.async_writeback_queue_depth)
            .saturating_add(bucket_index_resident_bytes);
        let log_pressure = self.inner.engine.storage_log_pressure_report(shard_id);
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        (
            StorageManagerPressureSnapshot {
                shard_id,
                dirty_bucket_count: plan.dirty_buckets.len(),
                selected_dirty_bucket_count: plan.selected_dump_buckets.len(),
                undumped_wal_records: plan.undumped_wal_records,
                wal_bytes: plan.undumped_wal_records,
                index_log_bytes: log_pressure.index_log_bytes,
                stale_block_slab_count: plan.stale_block_slab_ids.len(),
                reclaim_candidate_count: plan.reclaim_candidates.len(),
                reclaimable_physical_bytes: plan.reclaimable_physical_bytes,
                block_slab_stale_density_basis_points: 0,
                cache_memory_bytes: cache.memory_bytes,
                cache_disk_bytes: cache.disk_bytes,
                bucket_index_resident_bytes,
                eviction_memory_pressure_bytes,
                memory_cache_pressure_score,
                live_block_summaries_measured: plan.bucket_summaries.is_some(),
                // 0 when the round did not walk. The flag above is what says which 0 this is.
                expired_bucket_object_scan_debt: plan
                    .bucket_summaries
                    .as_ref()
                    .map(|summaries| summaries.len())
                    .unwrap_or(0),
                delayed_destroy_slab_count: plan.delayed_destroy_block_slab_ids.len(),
                delayed_destroy_bytes: plan.reclaimable_physical_bytes,
                follower_cursor_retention_blockers: 0,
                raft_snapshot_retention_blockers: 0,
                compaction_debt_model_count: usize::from(!plan.stale_block_slab_ids.is_empty()),
                compaction_debt_score: plan.reclaimable_physical_bytes,
                total_pressure_score: plan
                    .dirty_buckets
                    .len()
                    .saturating_add(plan.selected_dump_buckets.len())
                    as u64
                    + plan.undumped_wal_records
                    + log_pressure.index_log_bytes
                    + plan.reclaimable_physical_bytes
                    + cache.memory_bytes
                    + cache.disk_bytes
                    + bucket_index_resident_bytes,
                background_queue_depth: queue.background_queued_total,
                foreground_queue_depth: queue
                    .queued_total
                    .saturating_sub(queue.background_queued_total),
            },
            plan,
        )
    }

    pub fn run_storage_manager_once(
        &self,
        shard_id: ShardId,
        options: StorageManagerOptions,
    ) -> StorageManagerLoopReport {
        let (pressure, lifecycle_plan) = self.storage_manager_pressure_snapshot(shard_id, &options);
        let mut executed_stages = Vec::new();
        let mut skipped_stages = Vec::new();
        let mut pressure_decisions = Vec::new();
        let mut lifecycle_report = None;
        let mut compaction_report = None;
        let mut gc_report = None;
        let mut expired_records_removed = 0;
        let mut status = Status::ok();

        if options.enable_prepare {
            executed_stages.push("prepare".to_string());
            // Pre-allocate the next data slab so a client append never has to roll inline.
            //
            // Rolling costs several fsyncs plus a slab-directory scan (see
            // BlockStore::prepare_next_slab). Left to the write path it lands on one
            // unlucky write as a latency outlier unrelated to that write's size. Doing it
            // here -- the stage that already exists and is already named "prepare" -- is what
            // this design does with PrepareNewZone in the same position of
            // its background cycle.
            //
            // A no-op while the active slab has room, so this costs a lock and a comparison
            // on the overwhelming majority of cycles. A failure is recorded and does not stop
            // the cycle: the inline roll in `append` remains the fallback, so the only
            // consequence is that the next append pays for the roll, exactly as it does today.
            match self.inner.engine.block_store().prepare_next_slab() {
                Ok(Some(roll)) => {
                    tracing::debug!(
                        previous_block_slab_id = roll.previous_block_slab_id,
                        new_block_slab_id = roll.new_block_slab_id,
                        "storage manager pre-allocated the next data slab"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "storage manager could not pre-allocate the next data slab;                          the next append will roll inline"
                    );
                }
            }
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_prepare_runs += 1;
        } else {
            skipped_stages.push("prepare_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "prepare",
            options.enable_prepare,
            true,
            options.enable_prepare,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_block_slab_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "background_queue_depth",
                    pressure.background_queue_depth as u64,
                    1,
                ),
            ],
            vec!["continuous_prepare_plan_refresh".to_string()],
            (!options.enable_prepare).then(|| "prepare_disabled".to_string()),
        ));

        let dump_pressure = pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1)
            || pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1);
        let cache_pressure = pressure.cache_memory_bytes
            >= options.cache_memory_bytes_pressure.max(1)
            || pressure.cache_disk_bytes >= options.cache_disk_bytes_pressure.max(1);
        let stale_block_pressure = pressure.stale_block_slab_count
            >= options.stale_block_slab_pressure.max(1)
            || pressure.reclaim_candidate_count >= options.stale_block_slab_pressure.max(1)
            || pressure.reclaimable_physical_bytes
                >= options.reclaimable_physical_bytes_pressure.max(1);

        // A shard with no writer has no dump pressure, and for that reason alone this stage
        // used to vanish from `executed_stages` after the first round and never come back --
        // while the log still held every one of its records. The stage list is how an operator
        // reads whether maintenance is alive, so a stage that stops because its trigger is the
        // WRONG QUESTION is indistinguishable from maintenance that has died.
        //
        // Dump pressure asks "is there undumped work?". Reclaim asks "does the log hold records
        // nothing needs?" -- and on an idle shard the answer to the first is always no and to the
        // second is usually yes, which is exactly when reclaim matters most and exactly when it
        // was not run. Nothing else in a shipped server reclaims on a timer, so an idle shard's
        // log was permanent.
        //
        // The plan is the only thing that can answer the second question, so ask it -- but only
        // when the cheap trigger has already declined, so a busy shard pays nothing. An idle
        // shard is by definition not doing anything else with this round.
        let wal_reclaim_pressure = dump_pressure
            || (options.enable_wal_reclaim
                && self
                    .inner
                    .engine
                    .storage_wal_reclaim_plan(
                        shard_id,
                        options.follower_replay_cursors.clone(),
                        options.raft_snapshot_refs.clone(),
                    )
                    .safe_to_reclaim);
        if options.enable_wal_reclaim && wal_reclaim_pressure {
            let response = self.apply_storage_lifecycle(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                min_undumped_wal_bytes: options.min_undumped_wal_bytes,
                purge_delayed_destroy: false,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: options.follower_replay_cursors.clone(),
                block_gc_shared_store_cursors: Vec::new(),
                block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                block_gc_checkpoint_floor_slab_id: None,
                block_gc_raft_install_floor_slab_id: None,
                block_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
            if let Some(manifest) = response.report.dump_manifest.as_ref() {
                clear_dirty_shard_buckets(
                    &self.inner.dirty,
                    &self.inner.engine,
                    shard_id,
                    &manifest.bucket_ids,
                );
                let mut stats = self
                    .inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned");
                stats.dump_runs += 1;
                stats.storage_manager_reclaim_wal_runs += 1;
            }
            lifecycle_report = Some(response.report);
            // Move the oldest pages that live only inside a WAL record into the block store,
            // before anything tries to reclaim the log. `min_registered_sequence` pins retention
            // to the LOWEST registration, so one resident page holds the floor down whatever the
            // retention policy says. The dump this stage just took does NOT do it: measured on 48
            // async writes, a flush left all 48 registered and so did a full eight-stage cycle,
            // with the log floor still at sequence 1.
            //
            // Same bound and same order as the one-shot cycle in `engine::storage_manager_cycle`,
            // which is the other implementation of these stages -- at most
            // `max_dump_buckets_per_round` per pass, oldest first. This is the copy that runs on a
            // timer, so it is the one that decides whether an IDLE shard ever becomes reclaimable.
            let resident = self.inner.engine.wal_resident_block_count(shard_id);
            if resident > 0 {
                let moved = self.inner.engine.materialize_oldest_resident_blocks(
                    shard_id,
                    resident.saturating_sub(if options.max_dump_buckets_per_round == 0 {
                        usize::MAX
                    } else {
                        options.max_dump_buckets_per_round
                    }),
                );
                tracing::debug!(
                    shard_id,
                    moved,
                    resident,
                    "storage manager moved log-resident pages into the block store"
                );
            }
            // Dumping and spilling make the log RECLAIMABLE. Neither frees a byte of it.
            //
            // This stage was named for a reclaim it never performed: `gc_before_sequence` is the
            // only thing that truncates the log, and nothing above reaches it. Its production
            // callers were the cycle endpoint, an explicit /gc request, and the embedded proxy's
            // own reclaim thread -- so a server started as shipped, with this scheduler running,
            // grew its log for ever unless something outside asked. Measured on an identical
            // fixture: six rounds of this loop freed 0 of 1,512 records, six rounds of the
            // on-demand cycle freed 1,511.
            //
            // Same plan and same call the cycle applies, and now with the same retention inputs
            // too, so this is the cycle's reclaim running on the timer rather than a second
            // policy. `apply_storage_wal_reclaim` declines unless the plan proves itself safe, so
            // the floor is the plan's, not this stage's.
            //
            // This is the call whose answer is APPLIED -- `gc_before_sequence` below it is what
            // unlinks segments -- so it is the one that must see the cursors. The probe further up
            // only decides whether the stage runs.
            let wal_reclaim_plan = self.inner.engine.storage_wal_reclaim_plan(
                shard_id,
                options.follower_replay_cursors.clone(),
                options.raft_snapshot_refs.clone(),
            );
            let wal_reclaim = self.inner.engine.apply_storage_wal_reclaim(wal_reclaim_plan);
            if wal_reclaim.applied {
                tracing::debug!(
                    shard_id,
                    wal_records_removed = wal_reclaim.wal_records_removed,
                    "storage manager reclaimed write-ahead log records"
                );
            }
            executed_stages.push("reclaim_wal".to_string());
        } else if !options.enable_wal_reclaim {
            skipped_stages.push("reclaim_wal_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_wal_no_pressure".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_wal",
            options.enable_wal_reclaim,
            wal_reclaim_pressure,
            options.enable_wal_reclaim && wal_reclaim_pressure,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "undumped_wal_records",
                    pressure.undumped_wal_records,
                    options.min_undumped_wal_records.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1),
                    "dirty_slot_pressure",
                ),
                (
                    pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1),
                    "undumped_wal_pressure",
                ),
                (
                    wal_reclaim_pressure && !dump_pressure,
                    "reclaimable_wal_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_wal_reclaim,
                wal_reclaim_pressure,
                "reclaim_wal",
            ),
        ));

        if options.enable_expire {
            expired_records_removed = self.sweep_expired_records_bounded(
                options.max_expire_hot_buckets_per_round,
                options.max_expire_cold_buckets_per_round,
            );
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_expire_runs += 1;
            executed_stages.push("expire".to_string());
        } else {
            skipped_stages.push("expire_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "expire",
            options.enable_expire,
            expired_records_removed > 0,
            options.enable_expire,
            vec![storage_manager_pressure_signal(
                "expired_records_removed",
                expired_records_removed as u64,
                1,
            )],
            if expired_records_removed > 0 {
                vec!["expired_records_present".to_string()]
            } else {
                vec!["continuous_expiry_sweep".to_string()]
            },
            (!options.enable_expire).then(|| "expire_disabled".to_string()),
        ));

        // Evict, when the operator has asked for it -- beside expire, and before the stages
        // that rewrite pages.
        //
        // Expire drops what has died of old age and evict drops what is merely cold; they are two
        // halves of relieving memory, and the design runs them together at this point in the
        // round. This stage ran LAST, after the metrics reap, which put it after every stage that
        // rewrites pages: it decided what to drop from a state compaction had just churned, and
        // the memory it frees arrived too late to spare those stages any work. It has no data
        // dependency on any of them -- it reads options and engine state only -- so the position
        // was incidental.
        //
        // reclaim_memory above relieves memory by invalidating cached pages, which frees the cache
        // and leaves the index untouched. This is the stage that can actually free a bucket -- and
        // dump it first, so freeing does not strand the log.
        // Keep taking batches until the pressure is back under the threshold, the count budget
        // is spent, or a batch stops helping.
        //
        // The stage used to take ONE batch and return, however far the pressure still was from the
        // threshold -- 16 victims freeing about 4,800 bytes against a 1,775,000-byte overage, so
        // roughly 370 rounds. The design being followed loops instead and stops on a per-call
        // count budget.
        //
        // FOUR terms decide whether the loop takes another batch, and at the shipped default
        // threshold of 0 only TWO of them can ever be false. Counting them as "three stopping
        // conditions, all three needed" -- which is what this comment used to say -- reads the
        // guard as stronger than it is, so the terms are named in `EvictionLoopTerms` and the
        // count is asserted rather than described.
        //
        //   `more_budget`        -- bounds the work: the victim budget AND the round count.
        //                           It has to be both. `taken` is `victims.len()`, which only
        //                           grows when a batch RETURNS victims, so a batch that returns
        //                           none leaves the victim half of this term exactly where it
        //                           was -- at a budget of 100 it never goes false, however many
        //                           rounds the loop takes. Live at every threshold.
        //   `made_progress`      -- a batch that freed nothing (`cooldown`) or found no victims
        //                           means the next will not do better; this is what stops the loop
        //                           spinning on a shard whose memory eviction cannot reclaim.
        //                           Live at every threshold.
        //   `still_over`         -- `pressure_after >= threshold`. The goal. VACUOUS at threshold
        //                           0: both are `u64`, so the comparison is a constant `true`.
        //   `pressure_gate_open` -- set only on the path that did not take the early return at
        //                           `storage_lifecycle_methods.rs`, whose test is
        //                           `pressure_before < threshold`. Also VACUOUS at threshold 0,
        //                           and for the same reason: no `u64` is below 0, so the early
        //                           return is unreachable and the field is a constant `true`.
        //
        // Neither vacuous term is deleted, and on the SHIPPED path neither is vacuous any more.
        // Both vacuity arguments above are about the threshold, and `stage_threshold` below is the
        // bar the gate actually used: the shipped default pairs `enable_evict: true` with a 64 MiB
        // `index_resident_limit_bytes`, the bound is the higher bar, so the number these terms are
        // read against is 64 MiB rather than 0. They are vacuous only on the wide arm, which the
        // shipped settings cannot reach; see `data_node.rs`. What is NOT supported is leaning on
        // `still_over` to terminate the loop while the bar in force is 0, because at 0 it cannot
        // terminate anything -- the guard below pins that, and
        // `eviction_loop_live_stopping_conditions` is its denominator.
        // WHICH GATE OPENS THE STAGE, and there is exactly one of them.
        //
        // `index_resident_limit_bytes` and `eviction_memory_pressure_threshold` bound the SAME
        // quantity since the evict signal was narrowed: `bucket_index_resident_bytes` for this
        // shard. So the higher bar wins, and `mode` on the report says which one it was. Blending
        // them arithmetically would publish a bar no operator set.
        //
        // AND THE BOUND IS GATED ON ITSELF, NOT ON `enable_evict`. That is the point of it:
        // `apply_index_resident_bound` is the narrow per-shard gate, and until this field existed
        // it had no route from this loop at all -- its one non-test caller is inside
        // `run_storage_manager_cycle`, which runs on an inbound `TaskRequest::StorageManager`,
        // while this is the loop `start_storage_manager_scheduler_for_all_shards` starts. A safe
        // trigger reachable only by also switching on the stage it was written to replace is not
        // reachable.
        let index_bound_is_the_gate = options.index_resident_limit_bytes > 0
            && options.index_resident_limit_bytes > options.eviction_memory_pressure_threshold;
        let eviction = (index_bound_is_the_gate || options.enable_evict).then(|| {
            let engine = &self.inner.engine;
            // ONE batch function for both arms, so the repeat path cannot call a different gate
            // from the first one. It did not, and could not, when there was only one gate; with
            // two it is the obvious way to get a stage that opens on one and loops on the other.
            let batch = |budget: usize| {
                if index_bound_is_the_gate {
                    engine.apply_index_resident_bound(
                        shard_id,
                        options.index_resident_limit_bytes,
                        budget,
                    )
                } else {
                    engine.apply_storage_eviction(
                        shard_id,
                        options.eviction_memory_pressure_threshold,
                        budget,
                        options.eviction_dump_before_evict,
                        // NEVER, AND THERE IS NO LONGER A SETTING THAT COULD MAKE IT TRUE. This
                        // took `options.eviction_delete_drop`, and `delete_drop` calls
                        // `delete_record` on every live object in its victim buckets and then
                        // writes a `CommonDelete` tombstone per key -- so it does not drop a
                        // resident copy, it removes the model-map entry that is the only thing
                        // pointing at the payload block, and makes the removal survive replay.
                        // Measured on 600 acknowledged keys: 18 deleted, 18 missing cold, still
                        // missing after a reload. Relieving memory pressure must never destroy the
                        // only reachable copy of live data. The literal is the route's absence.
                        false,
                    )
                }
            };
            // The bar `still_over` is read against has to be the one the gate used, or the stage
            // keeps taking batches against a number nobody set. At the bound this term stops being
            // vacuous for the first time: a non-zero bar can actually be reached.
            let stage_threshold = if index_bound_is_the_gate {
                options.index_resident_limit_bytes
            } else {
                options.eviction_memory_pressure_threshold
            };
            let first = batch(options.eviction_batch_limit);
            run_eviction_stage(
                stage_threshold,
                options.eviction_count_limit,
                first,
                |remaining_budget| batch(options.eviction_batch_limit.min(remaining_budget)),
            )
            .into_stage_report()
        });
        if eviction.is_some() {
            executed_stages.push("evict".to_string());
            self.inner
