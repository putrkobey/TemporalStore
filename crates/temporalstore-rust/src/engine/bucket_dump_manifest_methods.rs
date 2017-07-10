// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bucket-dump manifest lifecycle methods for TemporalEngine, split from engine.rs.
use super::*;

/// Fault seam: stop `apply_bucket_dump_manifest_prune_with_retention_refs` dead between its two
/// phases, as a crash there would.
///
/// The prune detaches the survivors whose parent it is about to remove, then unlinks the pruned
/// manifests. Which of those happens first is a durability decision -- the wrong order leaves
/// invented corruption on disk for the width of the window between them -- and nothing in
/// production can stop the function between the two loops on demand, so that window is
/// untestable without a seam. A guard that cannot be made to fail checks nothing, so here it is.
///
/// Thread-local, matching `fail_compaction_block_read_after_for_test`: a prune runs on its
/// caller's thread, and a test arming this must not reach a prune another test is driving.
#[cfg(test)]
thread_local! {
    static STOP_PRUNE_AFTER_FIRST_PHASE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arm (`true`) or disarm (`false`) the seam above.
#[cfg(test)]
pub(crate) fn stop_bucket_dump_prune_after_first_phase_for_test(stop: bool) {
    STOP_PRUNE_AFTER_FIRST_PHASE.with(|cell| cell.set(stop));
}

impl TemporalEngine {
    pub fn create_bucket_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
    ) -> Result<BucketDumpManifest, Status> {
        // Make the pages + WAL the manifest is about to reference durable before we
        // capture their slab ids / wal_sequence. In live mode each append already
        // fsyncs, but under MATRIXARK_BULK_INGEST appends defer fsync, so a dump firing
        // mid-bulk would otherwise name slabs whose bytes are not yet on disk
        // (the dump path always commits blocks durably before updating the index).
        // The durability barrier MUST succeed before we capture slab ids / wal_sequence and
        // advance the dumped-log frontier -- the commit step refuses to touch
        // the index if the page/wal commit failed. Swallowing these errors let a dump whose
        // pages never reached disk (e.g. a sync_data EIO/ENOSPC under bulk mode) still clear
        // the bucket dirty and let WAL reclaim truncate the records backing those pages ->
        // silent data loss on crash. Propagate: an Err here skips clear_dumped_bucket_dirty_state
        // (apply_storage_lifecycle consumes the manifest via .ok()), so the bucket stays dirty
        // and the reclaim frontier does not advance.
        self.block_store.sync_durable().map_err(|err| {
            Status::error(
                "slot_dump_failed",
                format!("page durability barrier failed before dump capture: {err}"),
            )
        })?;
        self.wal_store.flush(shard_id).map_err(|err| {
            Status::error(
                "slot_dump_failed",
                format!("wal flush failed before dump capture: {err}"),
            )
        })?;
        let selected_buckets = selected_buckets.into_iter().collect::<BTreeSet<_>>();
        let summaries = self.bucket_storage_summaries(shard_id);
        if summaries.is_empty()
            && !self
                .shards
                .read()
                .expect("engine lock poisoned")
                .contains_key(&shard_id)
        {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        }
        let mut bucket_summaries = summaries
            .into_iter()
            .filter(|summary| {
                selected_buckets.is_empty() || selected_buckets.contains(&summary.routing_bucket)
            })
            .collect::<Vec<_>>();
        bucket_summaries.sort_by_key(|summary| summary.routing_bucket);
        let index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let index_bytes = self
            .export_index_bytes(shard_id)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        let index_sha256 = sha256_hex_bytes(&index_bytes);
        let dump_index_state = crate::engine::decode_index_bytes(&index_bytes)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        // The manifest's replay watermark MUST equal the WAL sequence the EMBEDDED index
        // actually reflects -- NOT the live WAL tail. Under MATRIXARK_BULK_INGEST the on-disk
        // index (what export_index_bytes returns) stays frozen at the last flush_shard_index
        // anchor while the WAL tail races ahead (per-command index persist is a no-op in bulk
        // mode). Taking the live tail here made install set replay_watermark PAST records the
        // embedded index never captured, so on reload WAL replay skipped them and reclaim
        // truncated them -> silent data loss. couples the two by construction: Load replays
        // from the dumped-log id stored INSIDE the dumped index, so the
        // replay cursor always equals the state the index captured. Derive the watermark from the
        // embedded index's own anchor so reload replays exactly the WAL suffix it lacks. (None
        // only for a shard never written/flushed -> empty index -> fall back to the tail, 0.)
        let wal_sequence = dump_index_state
            .applied_wal_sequence
            .unwrap_or_else(|| self.wal_store.stats(shard_id).last_sequence);
        let created_unix_ms = now_ms();
        let manifest_id = format!("{shard_id}-{index_log_sequence}-{created_unix_ms}");
        let parent_manifest_id = latest_bucket_dump_manifest_at(&self.index_dir, shard_id)
            .map(|manifest| manifest.manifest_id);
        // EVERY SLAB THE EMBEDDED INDEX WILL INSTALL, not only the dumped buckets'.
        //
        // `index_bytes` above is the WHOLE-SHARD index, and #1642 proved it has to stay whole:
        // install writes the decoded index as THE durable index for the shard, retention keeps the
        // newest manifest and nothing else, and a manifest carrying only its own buckets installs
        // a shard missing every bucket it did not name. `block_slab_ids` is the whole of what the
        // collector holds back -- `run_gc_inner` extends its live set with it,
        // `storage_block_gc_dependency_plan` blocks on it, and the page-GC retain floor steps over
        // it -- so deriving it from the DUMPED buckets alone left the slabs behind the unnamed
        // buckets' pages pinned by nothing. Compaction relocates those pages, the slab goes stale,
        // the sweep destroys it, and this manifest still points at it; neither
        // `validate_bucket_dump_manifest` nor the install preflight notices, because both filter
        // what they probe down to `bucket_ids`. Measured on that shape: install returns Ok, the
        // named bucket restores 5/5 and the unnamed buckets restore 0/8.
        //
        // The two scopes have to agree, and #1642 closed the other direction. So they agree by
        // WIDENING the slab set to everything the index can install.
        //
        // Derived from the EMBEDDED index rather than from the bucket summaries, because the
        // embedded index is what install decodes and what `validate_bucket_dump_manifest` checks
        // this against -- under bulk ingest the exported index and the volatile summaries are not
        // the same state. ONE live-page walk, handed on to the object-lifecycle report that was
        // already taking one of its own, so the walk count per dump is unchanged.
        let dump_live_block_entries = collect_live_block_entries(&dump_index_state);
        let block_slab_ids = dump_live_block_entries
            .iter()
            .map(|entry| entry.address.block_slab_id())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let object_lifecycle = object_lifecycle_report_from_entries(
            shard_id,
            &dump_index_state,
            dump_live_block_entries,
            &selected_buckets,
            |key| self.routing_bucket_for_key(shard_id, key),
        );
        let mut manifest = BucketDumpManifest {
            version: 3,
            shard_id,
            manifest_id,
            manifest_kind: "slot_dump".to_string(),
            dump_generation_id: String::new(),
            source_manifest_ids: Vec::new(),
            source_manifest_coverage: Vec::new(),
            parent_manifest_id,
            load_version_handoff: None,
            created_unix_ms,
            bucket_ids: bucket_summaries
                .iter()
                .map(|summary| summary.routing_bucket)
                .collect(),
            block_slab_ids,
            wal_sequence,
            index_log_sequence,
            live_block_refs: bucket_summaries
                .iter()
                .map(|summary| summary.block_ref_count)
                .sum(),
            logical_bytes: bucket_summaries
                .iter()
                .map(|summary| summary.logical_bytes)
                .sum(),
            physical_bytes: bucket_summaries
                .iter()
                .map(|summary| summary.physical_bytes)
                .sum(),
            bucket_summaries,
            object_lifecycle,
            index_bytes,
            index_sha256,
            checksum: String::new(),
        };
        manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
        let manifest_checksum = bucket_dump_manifest_checksum_in_place(&mut manifest)?;
        manifest.checksum = manifest_checksum;
        self.persist_bucket_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        // The manifest is durable from here, so this shard's log is dumped up to its current
        // durable length. Marked AFTER the write and behind its `?`, never before: a watermark
        // that ran ahead of the manifest would let a restart mid-dump skip records the failed
        // dump never captured. Only this path marks it -- a MERGED manifest consolidates
        // existing dumps and its coverage is bounded by its sources, so it makes nothing newer
        // dumped and must not reset the growth counter.
        self.wal_store.mark_dumped(shard_id);
        // The index-log is bounded by the storage-manager's consumer-aware index GC (see
        // `storage_wal_reclaim_plan` / `run_storage_manager_cycle`), which retains from the
        // durable dump frontier -- the min anchor across durable manifests, held back by any
        // lagging follower replay cursor or raft snapshot. That is the correct layer for GC
        // (this manifest-create path has no visibility into those consumers), and
        // `install_latest_manifest_if_newer_on_load` bridges the on-disk base to the manifest
        // anchor on reload, so fold(base + retained deltas) stays complete.
        Ok(manifest)
    }

    pub fn create_merged_bucket_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
        source_manifest_ids: impl IntoIterator<Item = String>,
        next_load_version: Option<u64>,
    ) -> Result<BucketDumpManifest, Status> {
        let selected_buckets = selected_buckets.into_iter().collect::<BTreeSet<_>>();
        let mut source_manifest_ids = source_manifest_ids.into_iter().collect::<Vec<_>>();
        source_manifest_ids.sort();
        source_manifest_ids.dedup();
        if source_manifest_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_missing_sources",
                "merged slot dump requires at least one source manifest",
            ));
        }
        let (source_manifest_count, missing_source_manifest_ids, source_manifest_bucket_ids) =
            self.bucket_dump_source_manifest_coverage(shard_id, &source_manifest_ids);
        if !missing_source_manifest_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_missing_sources",
                format!(
                    "merged slot dump source manifests are missing or invalid: {:?}",
                    missing_source_manifest_ids
                ),
            ));
        }
        let source_bucket_set = source_manifest_bucket_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let target_buckets = self
            .bucket_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| {
                selected_buckets.is_empty() || selected_buckets.contains(&summary.routing_bucket)
            })
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        let missing_coverage = target_buckets
            .iter()
            .copied()
            .filter(|bucket| !source_bucket_set.contains(bucket))
            .collect::<Vec<_>>();
        if !missing_coverage.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_source_coverage",
                format!(
                    "merged slot dump sources do not cover target slots: {:?}",
                    missing_coverage
                ),
            ));
        }
        let mut manifest = self.create_bucket_dump_manifest(shard_id, target_buckets)?;
        manifest.manifest_kind = "merged_slot_dump".to_string();
        // Capture each validated source's bucket coverage into the manifest so a
        // target engine that does not hold the source files can preflight coverage.
        manifest.source_manifest_coverage = source_manifest_ids
            .iter()
            .filter_map(|manifest_id| {
                bucket_dump_manifest_at(&self.index_dir, shard_id, manifest_id)
                    .ok()
                    .flatten()
                    .map(|source| BucketDumpSourceCoverage {
                        manifest_id: manifest_id.clone(),
                        bucket_ids: source.bucket_ids,
                    })
            })
            .collect();
        manifest.source_manifest_ids = source_manifest_ids;
        if let Some(next_load_version) = next_load_version {
            let previous_load_version = self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&shard_id)
                .map(|info| info.load_version)
                .unwrap_or_default();
            manifest.load_version_handoff = Some(BucketDumpLoadVersionHandoff {
                previous_load_version,
                next_load_version,
                applied: false,
            });
        }
        manifest.checksum = bucket_dump_manifest_checksum(&manifest)?;
        let (_, missing_source_manifest_ids, _, missing_bucket_ids) =
            self.bucket_dump_merged_source_preflight(&manifest);
        if !missing_source_manifest_ids.is_empty() || !missing_bucket_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_source_preflight",
                format!(
                    "merged source preflight failed sources={:?} missing_slots={:?}",
                    missing_source_manifest_ids, missing_bucket_ids
                ),
            ));
        }
        debug_assert!(source_manifest_count >= 1);
        self.persist_bucket_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        Ok(manifest)
    }

    #[track_caller]
    pub fn list_bucket_dump_manifests(&self, shard_id: ShardId) -> Vec<BucketDumpManifest> {
        list_bucket_dump_manifests_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    /// Land a manifest in this node's index dir durably, without installing it.
    ///
    /// This is deliberately NOT [`install_bucket_dump_manifest`](Self::install_bucket_dump_manifest):
    /// installing rewrites shard state from the manifest, which is what a restore-onto-this-shard
    /// wants. This one only records that the dump EXISTS, for a node inheriting a shard whose
    /// data is already in shared storage -- the lineage arrives with the data, and whether to
    /// install any of it stays a separate decision.
    pub fn store_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), std::io::Error> {
        self.persist_bucket_dump_manifest(manifest)
    }

    pub fn interrupted_bucket_dump_installs(&self, shard_id: ShardId) -> Vec<BucketDumpInstallMarker> {
        interrupted_bucket_dump_installs_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn bucket_dump_manifest_prune_plan(&self, shard_id: ShardId) -> BucketDumpManifestPrunePlan {
        self.bucket_dump_manifest_prune_plan_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn bucket_dump_manifest_prune_plan_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
    ) -> BucketDumpManifestPrunePlan {
        self.bucket_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn bucket_dump_manifest_prune_plan_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> BucketDumpManifestPrunePlan {
        let follower_cursors = follower_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        bucket_dump_manifest_prune_plan_at(
            &self.index_dir,
            shard_id,
            &follower_cursors,
            &raft_snapshot_refs,
        )
        .unwrap_or_else(|err| BucketDumpManifestPrunePlan {
            shard_id,
            reasons: vec![format!("slot_dump_prune_plan_failed:{err}")],
            ..BucketDumpManifestPrunePlan::default()
        })
    }

    pub fn bucket_dump_install_roll_forward_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<BucketDumpInstallRollForwardReport> {
        self.interrupted_bucket_dump_installs(shard_id)
            .into_iter()
            .map(|marker| self.bucket_dump_install_roll_forward_report(&marker))
            .collect()
    }

    pub fn roll_forward_bucket_dump_installs(
        &self,
        shard_id: ShardId,
    ) -> Vec<BucketDumpInstallRollForwardReport> {
        self.interrupted_bucket_dump_installs(shard_id)
            .into_iter()
            .map(|marker| {
                let mut report = self.bucket_dump_install_roll_forward_report(&marker);
                if report.can_retry_install {
                    match bucket_dump_manifest_at(
                        &self.index_dir,
                        marker.shard_id,
                        &marker.manifest_id,
                    )
                    .ok()
                    .flatten()
                    .map(|manifest| {
                        if manifest.manifest_kind == "merged_slot_dump" {
                            let merged_report = self.install_merged_bucket_dump_manifest(&manifest);
                            if merged_report.installed {
                                Ok(())
                            } else {
                                Err(Status::error(
                                    merged_report.status_code,
                                    "merged slot dump roll-forward failed",
                                ))
                            }
                        } else {
                            self.install_bucket_dump_manifest(&manifest)
                        }
                    }) {
                        Some(Ok(())) => {
                            report.completed_install = true;
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_bucket_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "install_retried".to_string();
                        }
                        Some(Err(status)) => {
                            report.can_retry_install = false;
                            report.reason = format!("install_retry_failed:{}", status.code);
                        }
                        None => {
                            report.can_retry_install = false;
                            report.reason = "missing_manifest".to_string();
                        }
                    }
                } else if report.can_roll_forward {
                    match self.persist_bucket_dump_install_marker_by_fields(
                        marker.shard_id,
                        &marker.manifest_id,
                        "commit",
                        marker.wal_sequence,
                        marker.index_log_sequence,
                    ) {
                        Ok(()) => {
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_bucket_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "commit_marker_written".to_string();
                        }
                        Err(err) => {
                            report.can_roll_forward = false;
                            report.reason = format!("commit_marker_failed:{err}");
                        }
                    }
                }
                report
            })
            .collect()
    }

    pub fn apply_bucket_dump_manifest_prune(&self, shard_id: ShardId) -> BucketDumpManifestPruneReport {
        self.apply_bucket_dump_manifest_prune_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn apply_bucket_dump_manifest_prune_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
    ) -> BucketDumpManifestPruneReport {
        self.apply_bucket_dump_manifest_prune_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn apply_bucket_dump_manifest_prune_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> BucketDumpManifestPruneReport {
        let plan = self.bucket_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            raft_snapshot_refs,
        );
        // DETACH FIRST, THEN UNLINK. Pruning an ancestor leaves whatever survives pointing at a
        // manifest that is no longer there, which `bucket_dump_manifest_chain_issues` reports as
        // `missing_parent_manifest` -- indistinguishable from real corruption, and a
        // `broken_slot_dump_manifest_chain` blocker on the production-readiness report. Detach
        // the survivors whose parent is about to go, so a dangling link keeps meaning exactly one
        // thing. Nothing reconstructs through the chain (each manifest embeds a complete
        // index), so dropping the link loses no recovery capability.
        //
        // The ORDER is the point, not just the detaching. These two phases were once the other
        // way round, and a crash in the window between them left on disk exactly the invented
        // corruption the paragraph above exists to prevent: manifests already unlinked, survivors
        // not yet detached. Detach-then-unlink has no such window. A crash between the phases now
        // leaves a survivor whose parent link is gone while its parent file is still present --
        // which is indistinguishable from a prune that ran to completion, and consistent either
        // way. A crash PART WAY THROUGH the unlink loop is equally safe for the same reason.
        //
        // Keyed on the PLAN rather than on what was actually unlinked, because the unlink has not
        // happened yet. That makes the set a superset: a survivor whose parent removal then fails
        // gets detached anyway. Over-detaching costs lineage metadata and nothing else;
        // under-detaching is the defect this ordering exists to prevent, so the asymmetry is
        // deliberate. Manifests that are themselves prunable are detached too -- if their own
        // unlink fails while their parent's succeeds, they are survivors, and they are already
        // consistent.
        let pruned_manifest_ids = plan
            .prunable_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if !pruned_manifest_ids.is_empty() {
            if let Ok(surviving) = list_bucket_dump_manifests_at(&self.index_dir, shard_id) {
                for mut manifest in surviving {
                    let parent_pruned = manifest
                        .parent_manifest_id
                        .as_ref()
                        .is_some_and(|parent| pruned_manifest_ids.contains(parent));
                    if !parent_pruned {
                        continue;
                    }
                    manifest.parent_manifest_id = None;
                    manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
                    let detached_checksum = bucket_dump_manifest_checksum_in_place(&mut manifest);
                    if let Ok(checksum) = detached_checksum {
                        manifest.checksum = checksum;
                        let _ = self.persist_bucket_dump_manifest(&manifest);
                    }
                }
            }
        }
        // The crash window itself. See `stop_bucket_dump_prune_after_first_phase_for_test`.
        #[cfg(test)]
        {
            if STOP_PRUNE_AFTER_FIRST_PHASE.with(|cell| cell.get()) {
                return BucketDumpManifestPruneReport {
                    shard_id,
                    plan,
                    removed_manifest_ids: Vec::new(),
                    removed_marker_files: 0,
                };
            }
        }
        let mut removed_manifest_ids = Vec::new();
        for manifest_id in &plan.prunable_manifest_ids {
            let path = bucket_dump_manifest_path(&self.index_dir, shard_id, manifest_id);
            if fs::remove_file(path).is_ok() {
                removed_manifest_ids.push(manifest_id.clone());
            }
        }
        let mut removed_marker_files = 0usize;
        if let Ok(marker_files) = bucket_dump_install_marker_files_at(&self.index_dir, shard_id) {
            let prunable_marker_manifest_ids = plan
                .prunable_marker_manifest_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            for (marker, path) in marker_files {
                if prunable_marker_manifest_ids.contains(&marker.manifest_id)
                    && fs::remove_file(path).is_ok()
                {
                    removed_marker_files = removed_marker_files.saturating_add(1);
                }
            }
        }
        BucketDumpManifestPruneReport {
            shard_id,
            plan,
            removed_manifest_ids,
            removed_marker_files,
        }
    }

    pub(super) fn bucket_dump_install_roll_forward_report(
        &self,
        marker: &BucketDumpInstallMarker,
    ) -> BucketDumpInstallRollForwardReport {
        if marker.phase != "install" && marker.phase != "prepare" {
            return BucketDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "unknown_interrupted_phase".to_string(),
            };
        }
        let Some(manifest) =
            bucket_dump_manifest_at(&self.index_dir, marker.shard_id, &marker.manifest_id)
                .ok()
                .flatten()
        else {
            return BucketDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "missing_manifest".to_string(),
            };
        };
        let reason = match self.validate_bucket_dump_manifest(&manifest) {
            Ok(()) if marker.phase == "install" => "commit_ready".to_string(),
            Ok(()) => "install_retry_ready".to_string(),
            Err(status) => format!("manifest_invalid:{}", status.code),
        };
        BucketDumpInstallRollForwardReport {
            shard_id: marker.shard_id,
            manifest_id: marker.manifest_id.clone(),
            interrupted_phase: marker.phase.clone(),
            can_roll_forward: reason == "commit_ready",
            can_retry_install: reason == "install_retry_ready",
            completed_commit: false,
            completed_install: false,
            obsolete_marker_files_removed: 0,
            reason,
        }
    }

    pub fn validate_bucket_dump_manifest(&self, manifest: &BucketDumpManifest) -> Result<(), Status> {
        let expected = bucket_dump_manifest_checksum(manifest)
            .map_err(|_| Status::error("slot_dump_corrupt", "slot dump manifest is corrupt"))?;
        if manifest.checksum != expected {
            return Err(Status::error(
                "slot_dump_checksum_mismatch",
                "slot dump manifest checksum mismatch",
            ));
        }
        if manifest.version >= 2 && manifest.dump_generation_id.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_generation",
                "slot dump manifest is missing dump generation id",
            ));
        }
        if manifest.index_bytes.is_empty() {
            return Err(Status::error(
                "slot_dump_partial_manifest",
                "slot dump manifest is missing index bytes",
            ));
        }
        let actual_index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
        if manifest.index_sha256 != actual_index_sha256 {
            return Err(Status::error(
                "slot_dump_index_checksum_mismatch",
                "slot dump manifest index checksum mismatch",
            ));
        }
        let existing_slabs = self
            .block_store
            .slab_ids()
            .map_err(|err| Status::error("slot_dump_invalid", err.to_string()))?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing = manifest
            .block_slab_ids
            .iter()
            .copied()
            .filter(|id| !existing_slabs.contains(id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_block_segments",
                format!("slot dump references missing page segments: {missing:?}"),
            ));
        }
        let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        // THE STAMP, ON THIS DOOR TOO. Every install calls this function first
        // (`install_bucket_dump_manifest` line 1 of its body), so refusing here is what gates the
        // install -- and refusing BEFORE `rebuild_unserialized_model_maps_from_bucket_index`
        // below, because that call starts reinterpreting the image.
        //
        // WHY A REFUSAL AND NOT A MIGRATION. The base-index door answers `Ok(None)` and the
        // caller replays the write-ahead log instead: slower, and correct. This door takes the
        // same trade. A migration here would be a SECOND place that has to know what every stamp
        // bump means, and the two would drift -- which is the defect the single predicate in
        // `persistence.rs` exists to stop, not one to reproduce.
        //
        // THIS REFUSES RESTORES THAT PREVIOUSLY SUCCEEDED, deliberately. What they did instead
        // was trust an image whose fields may have changed meaning; the measured instance is a
        // pre-fold index's `sequences` map, whose rows an install destroyed silently with every
        // load counter at zero. A loud, counted refusal that falls back to replay is the better
        // failure.
        if crate::engine::persistence::refuse_stale_index_stamp(&restored) {
            return Err(Status::error(
                "slot_dump_stale_index_stamp",
                format!(
                    "slot dump manifest index is stamped {} and this binary is {}; the dump                      cannot be trusted and the shard must recover from its log instead",
                    restored.index_format_version,
                    crate::engine::SHARD_INDEX_FORMAT_VERSION
                ),
            ));
        }
        // `hashes` IS carried by a freshly deserialized index now, so this call no longer supplies
        // a map that was absent -- it MERGES the bucket-index derivation into the decoded one. That
        // is still needed, and for the original reason read the other way round: an index can hold a
        // hash field whose page entry names no field, and the derivation can hold a field written
        // after the decoded map was. Taking either side wholesale loses the other's elements. The
        // other serialized maps are still left exactly as decoded, or the cross-check would repair
        // the very tampering it exists to catch. The function NAME is historical: `hashes` is no
        // longer unserialized.
        rebuild_unserialized_model_maps_from_bucket_index(&mut restored);
        let restored = restored;
        let manifest_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        if manifest_buckets.len() != manifest.bucket_ids.len()
            || manifest.bucket_ids != manifest_buckets.iter().copied().collect::<Vec<_>>()
        {
            return Err(Status::error(
                "slot_dump_slot_ids_not_canonical",
                "slot dump manifest slot ids must be sorted and unique",
            ));
        }
        let canonical_block_slab_ids = manifest
            .block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if manifest.block_slab_ids != canonical_block_slab_ids {
            return Err(Status::error(
                "slot_dump_block_segment_ids_not_canonical",
                "slot dump manifest page segment ids must be sorted and unique",
            ));
        }
        let all_live_block_entries = collect_live_block_entries(&restored);
        // The slabs the WHOLE embedded index needs, which is what `block_slab_ids` now carries and
        // what the collector holds back. Taken off the same walk the per-bucket entries below come
        // from, so this costs no extra pass over the index.
        let index_block_slab_ids = all_live_block_entries
            .iter()
            .map(|entry| entry.address.block_slab_id())
            .collect::<BTreeSet<_>>();
        // THE RESTORED IMAGE'S OWN FILING, NOT THIS SHARD'S RANGE, and the difference is the whole
        // reason a manifest install is not a reconciliation.
        //
        // `manifest.bucket_ids` are the SOURCE shard's buckets, written under the SOURCE's routing
        // range; the embedded index is that shard's own `bucket_map`, so the bucket a page sits in is
        // the map key the walk just returned. Deriving the bucket from the key over THIS shard's
        // range names a bucket the manifest never mentions whenever the two ranges differ -- which
        // matched nothing, refused every cross-range install with `slot_dump_live_ref_mismatch`, and
        // is exactly the truncation mx#1945 refused to allow. The fallback is kept for an entry from
        // the model-map arm of the walk, where there is no filing to read.
        let live_block_entries = all_live_block_entries
            .into_iter()
            .filter(|entry| {
                let routing_bucket = entry.filed_bucket().unwrap_or_else(|| {
                    self.routing_bucket_for_key(manifest.shard_id, &entry.object_key)
                });
                manifest_buckets.is_empty() || manifest_buckets.contains(&routing_bucket)
            })
            .collect::<Vec<_>>();
        if live_block_entries.len() as u64 != manifest.live_block_refs {
            return Err(Status::error(
                "slot_dump_live_ref_mismatch",
                format!(
                    "slot dump expected {} live page refs but index has {}",
                    manifest.live_block_refs,
                    live_block_entries.len()
                ),
            ));
        }
        let expected_bucket_summaries =
            bucket_dump_manifest_comparable_summaries(&restored, &manifest_buckets);
        let actual_bucket_summaries = comparable_bucket_dump_summaries(manifest.bucket_summaries.clone());
        if actual_bucket_summaries != expected_bucket_summaries {
            return Err(Status::error(
                "slot_dump_slot_summary_mismatch",
                format!(
                    "slot dump slot summaries do not match restored index page ownership: manifest={actual_bucket_summaries:?} restored={expected_bucket_summaries:?}"
                ),
            ));
        }
        let expected_logical_bytes = expected_bucket_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let expected_physical_bytes = expected_bucket_summaries
            .iter()
            .map(|summary| summary.physical_bytes)
            .sum::<u64>();
        if manifest.logical_bytes != expected_logical_bytes
            || manifest.physical_bytes != expected_physical_bytes
        {
            return Err(Status::error(
                "slot_dump_byte_accounting_mismatch",
                format!(
                    "slot dump byte totals logical={} physical={} do not match restored index logical={} physical={}",
                    manifest.logical_bytes,
                    manifest.physical_bytes,
                    expected_logical_bytes,
                    expected_physical_bytes
                ),
            ));
        }
        if manifest.version >= 3 {
            let expected_object_lifecycle = storage_object_lifecycle_report_for_buckets(
                manifest.shard_id,
                &restored,
                &manifest_buckets,
                |key| self.routing_bucket_for_key(manifest.shard_id, key),
            );
            // Also derive the lifecycle from the serialized model maps. For a
            // consistent index the two agree; a manifest whose model maps disagree
            // with its bucket index (e.g. a tampered object_id that the bucket-index
            // derivation alone would miss) is rejected.
            let model_object_lifecycle = storage_object_lifecycle_report_for_buckets_from_model_maps(
                manifest.shard_id,
                &restored,
                &manifest_buckets,
                |key| self.routing_bucket_for_key(manifest.shard_id, key),
            );
            if manifest.object_lifecycle != expected_object_lifecycle
                || manifest.object_lifecycle != model_object_lifecycle
            {
                // Say WHICH of the two derivations disagrees, and how. There are two comparisons
                // here and the message named neither, so a refusal said only that a dump would
                // not install -- and the two have different causes: the index-derived report
                // disagreeing means the manifest metadata is stale or wrong, while the model-map
                // one disagreeing means a map the manifest does not carry was compared against a
                // populated index.
                let which = if manifest.object_lifecycle != expected_object_lifecycle {
                    "bucket-index derivation"
                } else {
                    "model-map derivation"
                };
                let other = if manifest.object_lifecycle != expected_object_lifecycle {
                    &expected_object_lifecycle
                } else {
                    &model_object_lifecycle
                };
                return Err(Status::error(
                    "slot_dump_object_lifecycle_mismatch",
                    format!(
                        "slot dump object lifecycle metadata does not match restored index; the {which} disagrees. manifest={:?} derived={:?}",
                        manifest.object_lifecycle, other
                    ),
                ));
            }
        }
        // AGAINST THE WHOLE INDEX, not the dumped buckets' share of it. The manifest must name
        // every slab the index it installs can reach, or the collector -- which holds back exactly
        // what this field lists -- destroys a slab that index still points at, and the pages of a
        // bucket the dump did not name are gone. Scoping this comparison to `bucket_ids` is what
        // let the narrow field through; the page-by-page readability probe below deliberately
        // stays scoped, because a slab that is gone or corrupt is already caught by the two checks
        // over this set and reading every live page of the shard is the whole-store pass the
