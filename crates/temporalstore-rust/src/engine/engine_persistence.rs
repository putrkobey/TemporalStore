// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index/manifest persistence + load/flush helper methods for TemporalEngine, split from engine.rs.
use super::*;

/// WHICH WAY A LOAD OF THE PERSISTED INDEX WENT.
///
/// # WHY THIS IS NOT TEST SCAFFOLDING
///
/// A reload that silently rebuilds from the WAL and one that read the index correctly END IN THE SAME
/// STATE: both leave `index_format_version` at the current constant, both serve the right answers, and
/// nothing afterwards distinguishes them. So "the reload worked" has never been a statement about
/// which code ran, and a change that moves the index's MEANING cannot otherwise say whether the path
/// it cares about was exercised at all.
///
/// This change bumps `SHARD_INDEX_FORMAT_VERSION` from 3 to 5, so EVERY EXISTING STORE'S INDEX IS
/// REFUSED ON ITS FIRST LOAD and rebuilt from the log. That is correct -- it is the whole purpose of
/// the stamp -- and it is the largest operational consequence of this change, costing whatever a full
/// replay costs on a large store. Before this counter nothing reported it: the refusal is a
/// `return Ok(None)` that the caller cannot tell from an absent index.
///
/// `a_reload_after_the_stamp_bump_replays_rather_than_reading_the_index` turns the counter into a
/// proof, and asserts the membership is right on BOTH paths -- a counter saying "replayed" beside a
/// wrong answer would be worse than no counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLoadPath {
    /// The file was there, decoded, and its stamp was current. The index was USED.
    Accepted,
    /// The file was there and decoded and its stamp was OLD, so it was refused and the caller
    /// replays. This is the arm the stamp exists for and the one nothing could see before.
    RefusedStaleStamp,
    /// There is no index file. A crash before the first compaction looks like this.
    Absent,
    /// The file was there and could not be decoded at all.
    Undecodable,
}

static INDEX_LOADS_ACCEPTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_REFUSED_STALE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_ABSENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_UNDECODABLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// IS THIS IMAGE'S STAMP TOO OLD TO TRUST? THE COMPARISON, WRITTEN ONCE.
///
/// `index_format_version` exists because a stale index decodes CLEANLY -- the types are
/// unchanged -- while a field's MEANING has moved under it, so trusting it serves wrong data with
/// no error anywhere. The constant is therefore a claim about every door that turns stored bytes
/// into a `ShardState` somebody acts on, and it used to be enforced at exactly one of them.
///
/// MEASURED, which is why this exists: `decode_index_bytes` has six production callers and only
/// `load_index_inner` read the stamp. A stamp-13 image installed through
/// `install_bucket_dump_manifest` reached a live shard with all four counters below at ZERO -- it
/// did not skip the refusal, it never entered the code the refusal lived in. See
/// `engine::tests::the_stamp_refuses_a_legacy_sequence_map`.
///
/// Spelled as a predicate rather than copied inline at each door so the ORDER of the comparison
/// cannot drift between them: `<` and not `!=`, because a FUTURE stamp is a different question
/// (`decode_index_bytes`'s binary arm refuses that one on equality) and because the direction
/// that fails silently is a stamp that is too LOW.
pub(super) fn index_stamp_is_stale(shard: &ShardState) -> bool {
    shard.index_format_version < super::SHARD_INDEX_FORMAT_VERSION
}

/// The same question at a door that INSTALLS state, which also counts the refusal.
///
/// Counted so a refusal is distinguishable from an absence: every door here answers its caller
/// with the same "no usable index" it would give for a missing file, so without the counter
/// nothing downstream can tell "refused because stale" from "there was nothing there". The
/// report and plan paths deliberately use the bare predicate above instead -- they install
/// nothing, and counting them would make the tally mean something other than "an image was
/// refused on the way in".
pub(super) fn refuse_stale_index_stamp(shard: &ShardState) -> bool {
    if index_stamp_is_stale(shard) {
        note_index_load(IndexLoadPath::RefusedStaleStamp);
        return true;
    }
    false
}

fn note_index_load(path: IndexLoadPath) {
    let counter = match path {
        IndexLoadPath::Accepted => &INDEX_LOADS_ACCEPTED,
        IndexLoadPath::RefusedStaleStamp => &INDEX_LOADS_REFUSED_STALE,
        IndexLoadPath::Absent => &INDEX_LOADS_ABSENT,
        IndexLoadPath::Undecodable => &INDEX_LOADS_UNDECODABLE,
    };
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// (accepted, refused-stale, absent, undecodable) since the last reset.
///
/// A TUPLE AND NOT A SUM. A guard that only had the total could not tell a store that read its index
/// from one that replayed, which is the entire distinction this exists to draw.
pub fn index_load_path_counts() -> (u64, u64, u64, u64) {
    (
        INDEX_LOADS_ACCEPTED.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_REFUSED_STALE.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_ABSENT.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_UNDECODABLE.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget the counts, so a test measures its own loads rather than the suite before it.
pub fn reset_index_load_path_counts() {
    INDEX_LOADS_ACCEPTED.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_REFUSED_STALE.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_ABSENT.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_UNDECODABLE.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl TemporalEngine {
    pub(super) fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    /// Bytes of base index a dump of this shard would have to write, as the last completed dump
    /// left it on disk.
    ///
    /// A `stat`, not a serialize. The cadence is asked on every background round -- the embedded
    /// proxy's reclaim thread asks it once a second -- and what it needs is a SCALE, not a current
    /// answer. The file lags the live index by exactly the accrual the cadence is about to weigh,
    /// which is the quantity being bounded, and a shard that has never dumped has no file and
    /// reports 0, which leaves the configured floor deciding on its own.
    pub(super) fn base_index_bytes_on_disk(&self, shard_id: ShardId) -> u64 {
        fs::metadata(self.index_path(shard_id))
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    /// The accrual this shard's next threshold dump must have to show for itself.
    ///
    /// The ONE place a production cadence check gets its threshold, which is what makes
    /// `every_production_dump_cadence_check_reads_the_store_for_its_threshold` able to say so by
    /// reading this file: every production caller passes this, and the deployment's configured
    /// value is read here and nowhere else.
    pub(super) fn index_dump_threshold_bytes(&self, shard_id: ShardId) -> u64 {
        // The configured value is a FLOOR. Raise it so a dump does not write more than
        // INDEX_DUMP_BASE_FRACTION_DIVISOR bytes of base index per byte of index log it frees.
        crate::index_log::effective_index_dump_threshold_bytes(
            crate::storage_config::index_dump_wal_gap_bytes(),
            self.base_index_bytes_on_disk(shard_id),
            crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR,
        )
    }

    /// The single funnel through which every consumer reads the COMPLETE served index for
    /// a shard. It always returns a full, current `serialize_index`-shaped byte image of
    /// the `ShardState`, so callers never see a partial or stale index.
    ///
    /// - Shard loaded, not bulk: serialize the LIVE in-memory shard. This is the
    ///   authoritative current state and is what makes deferring the per-write base rewrite
    ///   safe -- readers reconstruct from memory rather than from the on-disk base.
    /// - Otherwise: read the on-disk base `shard-{id}.index.json`. This is the correct source
    ///   in bulk mode, where the base is deliberately frozen at the last flush anchor while
    ///   the WAL tail races ahead (serving the live tail would over-advance the manifest
    ///   replay watermark), and for a shard not currently loaded.
    pub(super) fn load_served_index_bytes(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<u8>, std::io::Error> {
        if !bulk_ingest_mode() {
            if let Some(shard) = self
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&shard_id)
            {
                return Ok(serialize_index(shard));
            }
        }
        fs::read(self.index_path(shard_id))
    }

    pub(super) fn persist_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            bucket_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        // Compact, not pretty. This document carries one summary per bucket -- thousands of them
        // -- and pretty-printing spends a newline and an indent on every field of every one. It is
        // machine-read on load and on install preflight; nobody reads it by eye at this size.
        // Whitespace is not semantic in JSON, so every reader that could parse the pretty form
        // parses this one, which is why this needs no version gate.
        let bytes = serde_json::to_vec(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        note_manifest_file_write(bytes.len());
        // The manifest is the durable reclaim watermark (wal_sequence) + recovery
        // index; WAL reclaim durably truncates the WAL based on it. It MUST be written
        // durably and atomically -- a bare fs::write left it in the page cache, so a
        // crash after the (durable) WAL reclaim but before the manifest reached disk
        // lost the records in between (the dumped index must be committed before
        // advancing the dumped-log id; both are waited on together). Atomic temp+rename
        // also prevents a torn manifest from hiding the whole listing on load.
        atomic_write_bytes(&path, &bytes)
    }

    pub(super) fn persist_bucket_dump_install_marker(
        &self,
        manifest: &BucketDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_bucket_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.wal_sequence,
            manifest.index_log_sequence,
        )
    }

    pub(super) fn persist_bucket_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        wal_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_bucket_dump_install_marker(
            &self.index_dir,
            &BucketDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                wal_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    pub(super) fn validate_bucket_dump_generation_for_install(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        let source_manifest_ids = manifest
            .source_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for existing in self.list_bucket_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || source_manifest_ids.contains(&existing.manifest_id)
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_buckets = existing.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_buckets.is_empty()
                || existing_buckets.is_empty()
                || !requested_buckets.is_disjoint(&existing_buckets);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.wal_sequence >= manifest.wal_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn load_index(&self, shard_id: ShardId, warm_cache: bool) -> Option<ShardState> {
        // Tolerant wrapper for probe/report callers (manifest-anchor probe, recovery reports):
        // a corrupt index-log delta yields None here. The authoritative load path
        // (`load_shard_with`) calls `load_index_checked` instead so it can REFUSE the load on
        // corruption rather than silently serving a base-only prefix.
        self.load_index_inner(shard_id, warm_cache, true)
            .ok()
            .flatten()
    }

    /// Fold-aware load that surfaces a corrupt served-index delta as `Err` so the caller can
    /// refuse the load. Used by the authoritative recovery path.
    pub(super) fn load_index_checked(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
    ) -> Result<Option<ShardState>, Status> {
        self.load_index_inner(shard_id, warm_cache, true)
    }

    /// Load ONLY the durable base snapshot (materialized at the last dump/unload), WITHOUT
    /// folding the served-index delta. Used by single-barrier recovery: the delta-log fdatasync
    /// is deferred, so the delta tail (and the anchor it advances) may reference pages that were
    /// never fsync'd -- trusting it could skip WAL replay and leave dangling references. The base
    /// is a durable checkpoint at its own watermark (flush_shard_index fsyncs every page before
    /// advancing that watermark); the WAL tail beyond it is re-derived by replaying each record
    /// exactly once (no delta fold means no double-apply of non-idempotent commands).
    pub(super) fn load_index_base_only(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
    ) -> Option<ShardState> {
        // base_only never folds the delta log, so this never fails on delta corruption.
        self.load_index_inner(shard_id, warm_cache, false)
            .ok()
            .flatten()
    }

    fn load_index_inner(
        &self,
        shard_id: ShardId,
        warm_cache: bool,
        fold_deltas: bool,
    ) -> Result<Option<ShardState>, Status> {
        let read = fs::read(self.index_path(shard_id));
        let base_present = read.is_ok();
        // The base snapshot is materialized only at compaction/unload, so a crash before the
        // first compaction leaves no base -- start empty and rebuild from the index-log
        // deltas below.
        let mut shard = match read {
            Ok(bytes) => match crate::engine::decode_index_bytes(&bytes) {
                Ok(shard) => {
                    // Refuse an index written in an older on-disk shape. A stale index decodes
                    // CLEANLY -- the types are unchanged -- while a field's meaning has moved
                    // under it, so trusting it serves wrong data with no error anywhere. The
                    // concrete case: before version 2, context_events was keyed by timeline_key;
                    // it is keyed by event_id_hash now, so a v1 index yields timeline keys in the
                    // event-id slot and an empty context_event_timeline, and every time-windowed
                    // read silently returns nothing.
                    //
                    // Refusing is treated exactly like a corrupt or absent index: the caller
                    // falls back to WAL replay, which rebuilds both maps correctly through
                    // insert_context_event_views. Slower, and correct.
                    //
                    // COUNTED, because all three outcomes below return the same `Ok(None)` and a
                    // caller cannot tell a refusal from an absence. See
                    // `index_load_path_counts` just above.
                    if refuse_stale_index_stamp(&shard) {
                        return Ok(None);
                    }
                    note_index_load(IndexLoadPath::Accepted);
                    shard
                }
                Err(_) => {
                    note_index_load(IndexLoadPath::Undecodable);
                    return Ok(None);
                }
            },
            Err(_) => {
                note_index_load(IndexLoadPath::Absent);
                ShardState::default()
            }
        };
        // Fold the index-log deltas beyond the base snapshot's anchor into the bucket index
        // (reconstructing the exact on-disk page layout at the ORIGINAL addresses) and apply
        // the captured per-key non-page state, BEFORE reconcile. This is what lets a crash
        // reload reconstruct the served index WITHOUT re-executing the WAL -- re-execution
        // would write fresh pages and relocate them to the active slab, doubling physical
        // page counts and losing the recorded slab layout.
        if fold_deltas {
            self.fold_index_log_deltas(shard_id, &mut shard)?;
        }
        // object_*_lookup is a derived serving accelerator and is no longer persisted -- and it
        // is MAINTAINED now rather than rebuilt here. The decoder files each page as it decodes,
        // and the delta fold above files what it places and un-files what it drops, so by this
        // line the lookup already describes the index and the whole-shard rebuild that stood here
        // had nothing left to discover.
        //
        // THE DEBUG CROSS-CHECK THAT STOOD HERE IS SPENT WITH ITS SCAFFOLD. It compared the
        // object lookup against the pages' OWN copies of the identity, and the pages have none;
        // `engine_storage_state.rs` carries why a version of it over one source would compare that source with
        // itself. What licensed removing the whole-shard rebuild from this path is that every
        // producer files as it places and un-files as it drops, and that obligation sits at the
        // producers rather than in a pass after them.
        // No base and nothing to fold -> genuinely nothing persisted yet.
        if !base_present
            && shard.bucket_index.bucket_map.is_empty()
            && shard.applied_wal_sequence.is_none()
        {
            return Ok(None);
        }
        // Fold disk->memory cache promotion into reconcile's page reads on a warming
        // load (normal restart): the pages reconcile reads to rebuild the secondary
        // views are promoted into the cache tier in the same pass, so no second warm
        // pass re-reads them under the mutex-serialized block store. Callers that only
        // need the index (e.g. the manifest anchor probe) pass warm_cache=false.
        let warm = if warm_cache {
            Some((&self.cache, shard_id))
        } else {
            None
        };
        reconcile_secondary_views_from_bucket_index(&self.block_store, &mut shard, warm);
        // Reloaded data is durable, hence clean: clear the transient per-page/bucket
        // dirty flags that were persisted before unload (the durable dirty_generation
        // identity is preserved). refresh_bucket_runtime_flags below then recomputes
        // dirty purely from the live (empty on load) dirty_objects set. This follows
        // the clear-dirty-on-load contract and keeps `bucket.dirty |= ...` from
        // resurrecting a stale persisted dirty flag.
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            bucket.set_dirty(false);
            for block in bucket.block_index.blocks_mut_unaccounted() {
                block.dirty = false;
            }
        }
        refresh_bucket_runtime_flags(&mut shard);
        Ok(Some(shard))
    }

    /// Fold the append-only index-log deltas onto a (possibly empty) base `ShardState`,
    /// reconstructing the served index for the delta path. Only deltas with a WAL anchor
    /// beyond the base snapshot's own anchor are applied -- deltas already captured by the
    /// base are skipped (their pages may have been relocated/reclaimed by the compaction
    /// that produced the base). Each delta's page items are re-attached at their ORIGINAL
    /// addresses (authoritative replace per covered key) and its per-key non-page state is
    /// applied, then the reconstructed anchor advances so WAL replay re-executes only the
    /// uncaptured (e.g. async) tail rather than relocating the pages the deltas already pin.
    fn fold_index_log_deltas(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
    ) -> Result<(), Status> {
        // Propagate a corrupt/holed delta stream instead of silently folding a prefix. A
        // silently-skipped delta advances the reconstructed anchor past a removal/eviction that
        // lives ONLY in the delta (not the WAL), so it is recovered from neither source -- a
        // silent loss / dangling ref. The caller (load_shard_with) refuses the load on Err.
        let base_anchor = shard.applied_wal_sequence.unwrap_or(0);
        let mut max_anchor = base_anchor;
        let mut applied = false;
        // Every carried container-element blob this fold meets, applied once at the end. Only the
        // blobs that carry one are kept, so a fold of records that predate the carry holds nothing.
        let mut carried_container_elements: Vec<serde_json::Value> = Vec::new();
        // STREAM the records rather than collecting them.
        //
        // `read_delta_records` decodes the whole log into a vector and hands it over. This caller
        // folds each record into `shard` and drops it, so that vector was the ENTIRE DECODED LOG
        // held live for the length of the fold -- which is larger than the file, because a
        // decoded record carries its items as owned values. `for_each_delta_record` was written
        // for this caller and its own note says the folding caller is the load path; it simply
        // was never wired to it. Measured over 10,000 one-item records, the collecting shape
        // leaves the whole log outstanding and the streaming shape leaves one record; see
        // `index_log_scale::what_the_two_fold_shapes_hold_at_two_corpus_sizes`.
        //
        // Nothing else moves. The walk, the decode, the per-record integrity check and the
        // sequence-continuity refusal are the same code either way -- `read_delta_records` is
        // that same walk with a vector on top -- so a corrupt or holed stream is still refused
        // here, below, exactly as it was.
        //
        // The old `records.is_empty()` early return is gone rather than translated: with no
        // record the closure never runs, `applied` stays false, and the block below is skipped.
        //
        // ONE DIFFERENCE IS REAL AND IS WHY THIS IS SAFE HERE RATHER THAN IN GENERAL. Collecting
        // first meant a corrupt stream was refused before a single record had been folded;
        // streaming folds records into `shard` as it goes and can be refused part-way through, so
        // `shard` may be half-folded when the error is returned. It never escapes: `shard` is a
        // local of `load_shard_with`, the `?` at that call site returns before it is published,
        // and the load then falls back to replay exactly as a refusal always did. A caller that
        // folded into something it did not own would not have this property.
        if let Err(err) = self
            .index_log_store
            .for_each_delta_record_above_anchor(shard_id, 0, base_anchor, |record| {
                // A present base already reflects everything at/below its anchor; fold only
                // the suffix. An absent base (anchor 0) folds the whole log. This read
                // `applied_wal_sequence.unwrap_or(0) <= base_anchor` inline, which made a
                // MISSING anchor into anchor 0 -- at or below every anchor -- and skipped every
                // anchor-less record. That is the reading #1832 took out of the post-dump record
                // walk and #1842 out of the piece name, surviving at the third site: #1842 holds
                // a piece of anchor-less deltas OPEN so this fold can see them, and this fold
                // then dropped them. `delta_record_is_reflected_by` is the predicate all three
                // now ask, so they cannot disagree about a record again.
                if crate::index_log::delta_record_is_reflected_by(&record, base_anchor) {
                    return;
                }
                let covered = delta_record_covered_keys(&record);
                fold_delta_block_items(
                    &mut shard.bucket_index,
                    &covered,
                    &record.items,
                    record.upsert,
                );
                apply_key_states(shard, &record.key_states);
                // The carried container elements are COLLECTED here and applied once the fold has
                // finished, not folded in as they arrive. A fold replays a suffix of the log, so one
                // fold can both add an element and take its page away again; applying per record
                // would leave the element in the durable map, and `fill_absent_elements` would then
                // serve it after it was deleted. `fold_carried_container_elements` states the whole
                // argument, including why matching the record's tombstones does not substitute.
                for blob in &record.key_states {
                    if CARRIED_CONTAINER_FIELDS
                        .iter()
                        .any(|field| blob.get(field).is_some())
                    {
                        carried_container_elements.push(blob.clone());
                    }
                }
                // A MISSING anchor takes no part in this max, and must not be given one. The
                // reconstructed anchor is where WAL replay starts ABOVE, so a record that says
                // nothing about the WAL must not move it -- in either direction. (`unwrap_or(0)`
                // happened to be harmless here; `unwrap_or(u64::MAX)`, the shape #1842 chose for
                // a piece NAME, would suppress replay of the entire tail.)
                if let Some(anchor) = record.applied_wal_sequence {
                    max_anchor = max_anchor.max(anchor);
                }
                #[cfg(test)]
                crate::index_log::probe::note_fold_applied(shard_id, record.sequence);
                applied = true;
            })
        {
            return Err(Status::error(
                "index_log_delta_corruption",
                format!(
                    "served-index delta log for shard {shard_id} is corrupt; refusing load: {err}"
                ),
            ));
        }
        if applied {
            // THE LOOKUP IS REBUILT BEFORE THE LAYOUT PASS NOW, NOT AFTER IT.
            //
            // The pass below derives each bucket's live object set from the identity held beside
            // its pages, and the lookup IS that identity -- so it has to describe the index AS
            // FOLDED. It does: the fold files what it places and un-files what it drops, in that
            // order, so there is no window between the fold and this pass in which a position
            // names a page that has gone or a page has no position. A whole-shard rebuild stood
            // here to close that window and is no longer closing anything.
            //
            // AND THE DEBUG CROSS-CHECK IS SPENT TOO, for the reason `engine_storage_state.rs` records: it read
            // the pages' own copies of the identity, which are gone.
            // THE INVERSION IS HOISTED, AND THE TWO BORROWS ARE SPLIT BY NAMING THE FIELDS. The derive
            // reads the object lookup while this loop holds the bucket map mutably, and both are fields of
            // one index -- so the split has to be written where the places are named, rather than inside a
            // method that would borrow the whole index. Built ONCE above a whole-shard pass, which is the
            // order that pass already pays; built per bucket it would be that order per bucket.
            let index = &mut shard.bucket_index;
            let keys =
                crate::engine::state::BlockKeysByPosition::from_lookup(&index.object_block_lookup);
