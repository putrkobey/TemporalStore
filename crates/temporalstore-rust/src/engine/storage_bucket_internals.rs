// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage snapshot/sample reporting + bucket-index maintenance internals, split from engine.rs.
use crate::engine::timestamped_series::TimestampedSeries as _;
use super::*;
use crate::engine::reports::StageWalkCharges;
use std::sync::Arc;


/// Drift checks run so far, and how many of them found a disagreement.
///
/// Process-wide and monotonic, so a soak can ask whether the maintained tally has EVER been wrong
/// without a report having to be plumbed anywhere. Reset only by the tests that read them.
pub static BLOCK_SLAB_LIVE_RECONCILES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static BLOCK_SLAB_LIVE_DRIFTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The per-slab live tally AS THE WALK SEES IT. This is the DEFINITION; the maintained counter is
/// held to it.
///
/// Deliberately built from `collect_live_block_entries` and not from the model maps: that walk is
/// what every existing per-slab live figure is built from -- the bucket index, plus the model-map
/// supplement for released buckets -- so the two sides of the drift check are the same question
/// asked twice, not two different questions that happen to be close.
pub(super) fn recompute_block_slab_live(shard: &ShardState) -> BTreeMap<u64, SlabLiveTally> {
    let mut tallies: BTreeMap<u64, SlabLiveTally> = BTreeMap::new();
    for entry in collect_live_block_entries(shard) {
        let tally = tallies.entry(entry.address.block_slab_id()).or_default();
        tally.block_refs = tally.block_refs.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(entry.address.length());
    }
    tallies
}

/// Compare the maintained tally against the walk, CORRECT the maintained one, and REPORT.
///
/// Three things in a fixed order, and the order is the design:
///
///   1. RECOMPUTE. The walk is the definition.
///   2. CORRECT. Recomputation wins. A maintained counter that has drifted is worse than one that
///      was never maintained, because everything downstream believes it; leaving it wrong to
///      preserve the evidence would be preserving evidence in the serving path.
///   3. REPORT. The difference is returned and tallied in `BLOCK_SLAB_LIVE_DRIFTS`. Never an
///      assert: a counting bug must not become an outage.
///
/// Called where the index is rebuilt wholesale -- a load, a manifest install, a bucket-ownership
/// rebuild -- which is where a walk is already being paid for, and on demand by the guard.
pub(super) fn reconcile_block_slab_live(shard: &mut ShardState) -> BlockSlabLiveDriftReport {
    let recomputed = recompute_block_slab_live(shard);
    let was_ready = shard.bucket_index.block_slab_live.is_ready();
    let mut report = BlockSlabLiveDriftReport {
        was_ready,
        ..BlockSlabLiveDriftReport::default()
    };
    let mut slabs: BTreeSet<u64> = recomputed.keys().copied().collect();
    slabs.extend(shard.bucket_index.block_slab_live.iter().map(|(id, _)| id));
    report.slabs_compared = slabs.len() as u64;
    if was_ready {
        let mut worst_bytes = 0_i64;
        for block_slab_id in slabs {
            let maintained = shard.bucket_index.block_slab_live.tally(block_slab_id);
            let walked = recomputed.get(&block_slab_id).copied().unwrap_or_default();
            let refs = maintained.block_refs as i64 - walked.block_refs as i64;
            let bytes = maintained.bytes as i64 - walked.bytes as i64;
            if refs == 0 && bytes == 0 {
                continue;
            }
            report.drifted_slabs = report.drifted_slabs.saturating_add(1);
            report.block_ref_drift = report.block_ref_drift.saturating_add(refs);
            report.byte_drift = report.byte_drift.saturating_add(bytes);
            if bytes.abs() > worst_bytes.abs() || report.worst_block_slab_id.is_none() {
                worst_bytes = bytes;
                report.worst_block_slab_id = Some(block_slab_id);
            }
        }
        BLOCK_SLAB_LIVE_RECONCILES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !report.is_clean() {
            BLOCK_SLAB_LIVE_DRIFTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    shard.bucket_index.block_slab_live.reset_from(recomputed);
    report
}

/// Seed the maintained tally from the index, without reporting anything.
///
/// For the load and rebuild paths, where there is nothing to have drifted from: the index has just
/// been built or replaced, so a comparison against the walk would compare the walk with itself.
pub(super) fn seed_block_slab_live(shard: &mut ShardState) {
    let recomputed = recompute_block_slab_live(shard);
    shard.bucket_index.block_slab_live.reset_from(recomputed);
}

pub(super) fn storage_slab_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSlabIntegrityReport {
    let indexed_block_slab_count = recovery.active_block_slab_ids.len();
    let discovered_block_slab_count = recovery.block_slab_reports.len();
    let live_block_slab_count = recovery.live_block_slab_ids.len();
    let orphan_block_slab_count = boundary.orphan_block_slab_ids.len();
    let stale_block_ref_count = boundary.stale_index_block_refs.len();
    let corrupt_block_slab_count = boundary.corrupt_block_slab_ids.len();
    let unreadable_block_ref_count = recovery.unreadable_block_refs.len();
    let unreadable_block_bytes = boundary.unreadable_block_bytes;
    let owner_mismatch_block_ref_count = boundary.owner_mismatch_block_refs.len();
    let missing_owner_block_ref_count = boundary.missing_owner_block_refs;
    let reclaim_required = orphan_block_slab_count > 0
        || recovery
            .block_slab_live_reports
            .iter()
            .any(|report| report.stale_block_estimate > 0);
    let integrity_ok = stale_block_ref_count == 0
        && corrupt_block_slab_count == 0
        && unreadable_block_ref_count == 0
        && unreadable_block_bytes == 0
        && owner_mismatch_block_ref_count == 0
        && missing_owner_block_ref_count == 0
        && recovery.all_live_blocks_readable;

    StorageSlabIntegrityReport {
        shard_id,
        indexed_block_slab_count,
        discovered_block_slab_count,
        live_block_slab_count,
        orphan_block_slab_count,
        stale_block_ref_count,
        corrupt_block_slab_count,
        unreadable_block_ref_count,
        unreadable_block_bytes,
        owner_mismatch_block_ref_count,
        missing_owner_block_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

/// The slabs worth reclaiming, chosen from the per-slab live/stale tally.
///
/// This took a whole `StorageRecoveryReport` and read one field off it. Taking that field
/// directly is what lets the planner build the tally without the report's whole-store block
/// scan -- see `storage_reclaim_slab_reports`.
pub(super) fn storage_reclaim_candidates_from_slab_reports(
    block_slab_live_reports: &[StorageRecoverySlabLiveReport],
    fully_stale_slab_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = block_slab_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_slab_ids.contains(&report.block_slab_id);
            let stale_block_estimate = if fully_stale {
                report.block_count
            } else {
                report.stale_block_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_block_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_block_estimate);
            Some(StorageReclaimCandidate {
                block_slab_id: report.block_slab_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                block_count: report.block_count,
                live_block_refs: report.live_block_refs,
                stale_block_estimate,
                live_ref_density_basis_points: report.live_ref_density_basis_points,
                reclaim_score,
                reason: if fully_stale {
                    "orphan_segment".to_string()
                } else {
                    "low_live_density".to_string()
                },
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .reclaim_score
            .cmp(&left.reclaim_score)
            .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
            .then_with(|| left.block_slab_id.cmp(&right.block_slab_id))
    });
    candidates
}

pub(super) fn annotate_storage_manager_admin_stage_fields(
    stages: &mut [StorageManagerStageReport],
    last_run_unix_ms: u64,
    duration_ms: u64,
    errors: &[String],
    retention_blockers: usize,
) {
    // `duration_ms` is deliberately NOT set here. It used to be, to the whole round's duration, for
    // every stage -- so all eight phases reported the same number and the published
    // `..._phase_duration_ms` series could not say which phase was slow, which is the only question
    // a per-phase duration exists to answer. Each stage now times itself as it is recorded, and the
    // round total lives on the cycle report instead of being copied across the stages.
    let _ = duration_ms;
    for stage in stages {
        stage.last_run_unix_ms = last_run_unix_ms;
        if stage.skipped && stage.skipped_reason.is_empty() {
            stage.skipped_reason = stage.reason.clone();
        }
        if !errors.is_empty() {
            let prefix = format!("{}:", stage.stage);
            stage.errors = errors
                .iter()
                .filter(|error| error.starts_with(&prefix))
                .cloned()
                .collect();
        }
        stage.bytes_reclaimed = stage
            .block_bytes_reclaimed
            .max(stage.cache_disk_bytes_removed)
            .max(stage.before_bytes.saturating_sub(stage.after_bytes));
        stage.blocks_compacted = stage.rewritten_block_refs;
        if stage.wal_floor_sequence == 0 {
            stage.wal_floor_sequence = stage.retain_from_wal_sequence;
        }
        if stage.index_log_floor_sequence == 0 {
            stage.index_log_floor_sequence = stage.retain_from_index_log_sequence;
        }
        if stage.retention_blockers == 0 {
            stage.retention_blockers = retention_blockers;
        }
        if stage.pressure_before == 0 {
            stage.pressure_before = stage.eviction_pressure_before.max(stage.before_bytes);
        }
        if stage.pressure_after == 0 {
            stage.pressure_after = stage.eviction_pressure_after.max(stage.after_bytes);
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StorageManagerPhaseExecutor {
    round_started_unix_ms: u64,
    /// Monotonic start, for the DURATION.
    ///
    /// The unix millisecond above says WHEN the round began and is comparable across processes.
    /// It cannot say how LONG the round took: the clock behind it is adjustable, and an
    /// adjustment mid-round is indistinguishable from work -- which showed up as a round whose
    /// reported duration was shorter than the stages inside it.
    ///
    /// Passed in rather than captured here, because this executor is constructed at the END of a
    /// cycle. Capturing it here measures the finish and reports a round of zero, which is how the
    /// first version of this fix was caught.
    round_started_at: std::time::Instant,
}

impl StorageManagerPhaseExecutor {
    pub(super) fn new(round_started_unix_ms: u64, round_started_at: std::time::Instant) -> Self {
        Self {
            round_started_unix_ms,
            round_started_at,
        }
    }

    pub(super) fn annotate_reports(
        &self,
        stages: &mut [StorageManagerStageReport],
        errors: &[String],
        retention_blockers: usize,
    ) -> u64 {
        let round_duration_ms = self.round_started_at.elapsed().as_millis() as u64;
        annotate_storage_manager_admin_stage_fields(
            stages,
            self.round_started_unix_ms,
            round_duration_ms,
            errors,
            retention_blockers,
        );
        round_duration_ms
    }
}

#[derive(Debug, Clone)]
pub(super) struct LiveBlockEntry {
    pub(super) object_key: Arc<str>,
    pub(super) kind: StoredModelKind,
    pub(super) component: Option<Arc<str>>,
    pub(super) address: ElementEntry,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
    /// THE BUCKET THIS BLOCK IS ACTUALLY FILED UNDER, when the walk that produced the entry knew
    /// it -- which is whenever the entry came out of the bucket index.
    ///
    /// An address carries no routing bucket of its own: the wire slot `rs` is retired from
    /// `BlockAddressWire` entirely, so every index decodes into blocks that carry none -- one
    /// written before the field existed, one written while it did, and one written now all alike
    /// -- and `rebuild_bucket_first_index` stamps only the
    /// object id onto an address, so a block comes out of a reconstruct still unrouted. Five
    /// readers then had to answer "which bucket is this block in?" for themselves, and each
    /// answered it with `block_routing_bucket(key, 0, u32::MAX)` -- a hash over the WHOLE range,
    /// which is the bucket the block is filed under only when the shard is loaded on the whole
    /// range too. On a shard loaded `0..1023` it names a bucket the shard does not hold.
    ///
    /// `collect_bucket_index_live_block_entries` walks `bucket_map` and therefore HAS the answer;
    /// it was iterating `.values()` and discarding the key. Carrying it costs NOTHING -- see the
    /// note on the two-field shape below -- and removes the guess. An unset `filing_is_known`
    /// means the entry came from the model maps, where there is no filing to report; those
    /// callers keep the fallback they had.
    ///
    /// NOT the same question as `address.routing_bucket()`, which is what the BLOCK claims. This
    /// is where the INDEX has it. `validate_bucket_ownership_index_from_entries` exists to
    /// report when those two disagree, so the two must stay separately answerable.
    ///
    /// A BARE `u32` AND A FLAG RATHER THAN AN `Option<u32>`, AND THE DIFFERENCE IS 8 BYTES PER
    /// LIVE BLOCK. This struct aligns to 8 -- three `Arc` pointers and a `BlockAddress` -- so its
    /// three flags sat in three bytes with five of tail padding. A `u32` and a fourth flag fit
    /// that padding; an `Option<u32>` is eight bytes of its own and took the struct from 104 to
    /// 112. `a_live_block_entry_carries_pointers_not_text_and_the_hoist_lowered_the_peak` caught
    /// that, and it is the guard on a walk that materializes EVERY live block in the shard, so the
    /// right answer was to stop paying the eight bytes rather than to widen the bound.
    ///
    /// Read through [`LiveBlockEntry::filed_bucket`], never as the two fields.
    pub(super) filed_routing_bucket: u32,
    pub(super) filing_is_known: bool,
}

impl LiveBlockEntry {
    /// The bucket the INDEX has this block under, when the walk that produced the entry knew it.
    pub(super) fn filed_bucket(&self) -> Option<u32> {
        self.filing_is_known.then_some(self.filed_routing_bucket)
    }
}

#[derive(Debug, Default)]
pub(super) struct StorageBlockOwnershipValidation {
    pub(super) mismatches: Vec<StorageRecoveryBlockOwnerMismatch>,
    pub(super) missing_owner_block_refs: usize,
}

/// A key as a walk arm has it: a SHARED HANDLE where one exists, characters where none does.
///
/// WHY AN ENUM AND NOT A TYPE CHANGE. The `emit` closure took `&str`, which meant every arm handed
/// over characters and `collect_model_live_block_entries` rebuilt a handle from them -- measured at
/// two of the three allocations it makes per entry, 67% of the count on a whole-shard walk
/// (`what_a_whole_shard_live_block_walk_allocates_per_entry`).
///
/// A uniform `&ModelKey` parameter is impossible, and that is the whole reason this is an enum.
/// Every arm but one lends the handle it holds. The exception is the `context_entities` arm, which
/// COMPOSES its key from the collection key and the entity hash -- so the key it emits exists
/// nowhere to be borrowed, and no signature reaches that.
///
/// THIS LIST USED TO NAME TWO EXCEPTIONS AND IT WAS WRONG WITHIN TWO CHANGES. It also named the
/// timestamped-series arm, "generic over its map's key type and reaches `&str` through `Borrow`, so
/// it cannot produce a `&ModelKey` at all". That was true of the signature as written and not of
/// the code: all seven of its callers pass a `HashMap<ModelKey, _>`, so the bound was wider than
/// any of its uses, and narrowing it let the arm lend with zero call-site edits. The deferral was
/// an inference about callers I had not checked.
///
/// The same mistake sat in `emit_one_entry_a_block`'s note, which claimed its callers had no handle
/// and was corrected the same way. Two stale claims in this file, both mine, both written while
/// generalising from the arms I had looked at to the ones I had not.
///
/// So the enum carries whichever the arm actually has, and the caller decides what to do with it:
/// `as_str` to inspect, `to_handle` to keep. A handle-lending arm costs the caller a refcount bump
/// where it used to cost a `String` and an `Arc` built from it.
#[derive(Clone, Copy)]
pub(super) enum KeyRef<'a> {
    Shared(&'a ModelKey),
    Borrowed(&'a str),
}

impl<'a> KeyRef<'a> {
    pub(super) fn as_str(self) -> &'a str {
        match self {
            KeyRef::Shared(handle) => handle,
            KeyRef::Borrowed(text) => text,
        }
    }

    /// The handle to keep. Free for a lending arm -- a refcount bump -- and one allocation for an
    /// arm that had only characters, which is what it used to cost for every arm.
    pub(super) fn to_handle(self) -> ModelKey {
        match self {
            KeyRef::Shared(handle) => Arc::clone(handle),
            KeyRef::Borrowed(text) => Arc::from(text),
        }
    }
}

pub(super) fn live_block_entry(
    object_key: ModelKey,
    kind: impl AsRef<str>,
    component: Option<String>,
    address: ElementEntry,
) -> LiveBlockEntry {
    LiveBlockEntry {
        object_key,
        kind: stored_model_kind(kind.as_ref()),
        component: component.map(Arc::from),
        // A block materialized in the block store carries a real page_id; a block
        // backed only by the hot/append-log buffer does not. Evaluate before the
        // `address` field moves it.
        log_backed: address.block_id().is_none(),
        address,
        dirty: false,
        deleted: false,
        // A model-map walk reads the blocks an object owns, not the index that files them, so
        // this walk genuinely does not know. Says so rather than guessing.
        filed_routing_bucket: 0,
        filing_is_known: false,
    }
}

/// The same entry from a walk that DOES know the filing.
///
/// A bucket-scoped model-map walk selected this block BY its bucket, so it knows the answer as
/// exactly as the bucket-index walk does -- and an entry that reported "not filed" would send the
/// five `filed_bucket()` readers to a hash over the WHOLE keyspace, which names a bucket a shard
/// loaded on a narrow range does not hold. That fallback used to be unreachable because the address
/// answered first; it is reachable now, so the walks that know have to say so.
pub(super) fn live_block_entry_filed(
    object_key: ModelKey,
    kind: impl AsRef<str>,
    component: Option<String>,
    address: ElementEntry,
    routing_bucket: u32,
) -> LiveBlockEntry {
    let mut entry = live_block_entry(object_key, kind, component, address);
    entry.filed_routing_bucket = routing_bucket;
    entry.filing_is_known = true;
    entry
}

pub(super) fn storage_page_address_sample(
    shard_id: ShardId,
    address: &ElementEntry,
) -> StoragePageAddressSample {
    StoragePageAddressSample {
        shard_id,
        stored_slab_id: address.block_slab_id(),
        slab_id: address.block_slab_id(),
        block_id: address.block_id().unwrap_or(address.block_slab_id()),
        offset: address.offset(),
        length: address.length(),
        generation: address.generation().unwrap_or(0),
    }
}

pub(super) fn storage_block_address_sample(
    shard_id: ShardId,
    address: &ElementEntry,
) -> StorageBlockAddressSample {
    StorageBlockAddressSample {
        shard_id,
        stored_slab_id: address.block_slab_id(),
        block_id: address.block_slab_id(),
        offset: address.offset(),
        length: address.length(),
        // Not carried in the index any more; the block envelope holds it.
        checksum: String::new(),
    }
}

pub(super) fn storage_index_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    storage_index_snapshot_with_samples_from_entries(
        shard_id,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has.
///
/// All four `*_snapshot_with_samples` builders run back to back inside ONE read lock in
/// `apply_storage_lifecycle`, and each was calling `collect_live_block_entries` for its own copy of
/// every live block -- then sorting all of it to take EIGHT samples. `who_walks_the_shard` measured
/// the four at 1.0x the shard apiece.
///
/// They differ only in sort order, so one walk serves all four and each sorts a vector of
/// REFERENCES rather than owning entries -- the shared walk is not traded for four clones.
///
/// Safe for the same reason as #1586: one lock, an unchanged `&ShardState`, nothing mutating
/// between them. And these feed report SAMPLES rather than a decision, so unlike the freshness
/// walk in `clear_dumped_bucket_dirty_state` (#1607) a shared snapshot cannot change what the
/// system DOES -- it only makes the four samples describe one moment instead of four.
pub(super) fn storage_index_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.kind.as_str(),
            left.object_key.as_ref(),
            left.component.as_deref().unwrap_or(""),
            left.address.block_slab_id(),
            left.address.offset(),
        )
            .cmp(&(
                right.kind.as_str(),
                right.object_key.as_ref(),
                right.component.as_deref().unwrap_or(""),
                right.address.block_slab_id(),
                right.address.offset(),
            ))
    });

    const MAX_STORAGE_INDEX_SAMPLES: usize = 8;
    snapshot.page_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            StoragePageIndexEntrySample {
                logical_key: entry.object_key.clone().to_string(),
                timestamp_range: None,
                block_addresses: vec![page_address],
                append_watermark: entry.address.offset(),
                generation: expected_live_block_object_id(shard_id, entry),
            }
        })
        .collect();
    snapshot.block_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            let block_address = storage_block_address_sample(shard_id, &entry.address);
            StorageBlockIndexEntrySample {
                stored_slab_id: entry
                    .address
                    .slab_id()
                    .unwrap_or(entry.address.block_slab_id()),
                checksum: String::new(),
                generation: expected_live_block_object_id(shard_id, entry),
                page_address,
                block_address,
            }
        })
        .collect();

    let mut object_entries: BTreeMap<(String, String, String), StorageObjectIndexEntrySample> =
        BTreeMap::new();
    for entry in entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES.saturating_mul(4))
    {
        let key = (
            entry.kind.to_string(),
            entry.kind.to_string(),
            entry.object_key.to_string(),
        );
        let sample = object_entries
            .entry(key)
            .or_insert_with(|| StorageObjectIndexEntrySample {
                model: entry.kind.to_string(),
                table: entry.kind.to_string(),
                object_key: entry.object_key.to_string(),
                block_chain: Vec::new(),
                delete_marker: entry.deleted,
                generation: expected_live_block_object_id(shard_id, entry),
            });
        if sample.block_chain.len() < MAX_STORAGE_INDEX_SAMPLES {
            sample
                .block_chain
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        sample.delete_marker |= entry.deleted;
        sample.generation = sample.generation.max(expected_live_block_object_id(shard_id, entry));
    }
    snapshot.object_index_entry_samples = object_entries
        .into_iter()
        .map(|(_, sample)| sample)
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .collect();
    snapshot
}

pub(super) fn storage_gc_ref(entry: &LiveBlockEntry) -> String {
    match entry.component.as_deref() {
        Some(component) if !component.is_empty() => {
            format!("{}:{}:{}", entry.kind, entry.object_key, component)
        }
        _ => format!("{}:{}", entry.kind, entry.object_key),
    }
}

pub(super) fn storage_watermark_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    storage_watermark_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe.
///
/// This one is shaped differently from the other three and the difference is worth keeping in
/// view: it does NOT sort. It folds every entry into a per-bucket watermark map, so it reads the
/// entries once in whatever order they arrive. Anything that rewrites these four as a group has to
/// notice that -- treating them as one template is how a bulk edit of this set goes wrong.
pub(super) fn storage_watermark_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    const MAX_STORAGE_WATERMARK_SAMPLES: usize = 8;
    let timestamp_ms = now_ms();
    let mut bucket_watermarks = BTreeMap::<u32, u64>::new();

    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        bucket_watermarks.insert(*bucket_id, runtime_bucket.dirty_generation);
    }
    for entry in entries {
        let bucket_id = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let generation = expected_live_block_object_id(shard_id, entry);
        bucket_watermarks
            .entry(bucket_id)
            .and_modify(|current| *current = (*current).max(generation))
            .or_insert(generation);
    }

    snapshot.append_watermark_samples = bucket_watermarks
        .iter()
        .take(MAX_STORAGE_WATERMARK_SAMPLES)
        .map(|(bucket_id, generation)| StorageAppendWatermarkSample {
            shard_id,
            bucket_id: *bucket_id,
            log_index: (*generation).max(snapshot.append_watermark),
            timestamp_ms,
        })
        .collect();
    if snapshot.append_watermark_samples.is_empty() && snapshot.append_watermark > 0 {
        snapshot
            .append_watermark_samples
            .push(StorageAppendWatermarkSample {
                shard_id,
                bucket_id: 0,
                log_index: snapshot.append_watermark,
                timestamp_ms,
            });
    }

    snapshot.compaction_watermark_samples = vec![StorageCompactionWatermarkSample {
        shard_id,
        safe_generation: snapshot.compaction_watermark,
        safe_timestamp_ms: snapshot.follower_cursor_safe_watermark,
        follower_floor: snapshot.follower_cursor_retention_floor,
    }];
    snapshot
}

pub(super) fn storage_gc_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    storage_gc_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe: one lock, an unchanged `&ShardState`, and these feed report SAMPLES
/// rather than a decision.
pub(super) fn storage_gc_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.deleted,
            left.kind.as_str(),
            left.object_key.as_ref(),
            left.component.as_deref().unwrap_or(""),
            left.address.block_slab_id(),
            left.address.offset(),
        )
            .cmp(&(
                right.deleted,
                right.kind.as_str(),
                right.object_key.as_ref(),
                right.component.as_deref().unwrap_or(""),
                right.address.block_slab_id(),
                right.address.offset(),
            ))
    });

    const MAX_STORAGE_GC_SAMPLES: usize = 8;
    let now = now_ms();
    snapshot.delete_marker_samples = entries
        .iter()
        .filter(|entry| entry.deleted)
        .take(MAX_STORAGE_GC_SAMPLES)
        .map(|entry| StorageDeleteMarkerSample {
            ref_id: storage_gc_ref(entry),
            generation: expected_live_block_object_id(shard_id, entry),
            deleted_at_ms: now,
            reason: "object_tombstone".to_string(),
        })
        .collect();

    let follower_safe = snapshot.follower_cursor_safe_to_reclaim;
    let mut eligibility_samples: Vec<StorageGcEligibilitySample> = entries
        .iter()
        .filter_map(|entry| {
            let eligible_after_ms = shard
                .expires_at_ms()
                .get(entry.object_key.as_ref())
                .copied()
                .unwrap_or(0);
            let has_delete_marker = entry.deleted;
            let ttl_eligible = eligible_after_ms > 0 && eligible_after_ms <= now;
            if !has_delete_marker && !ttl_eligible {
                return None;
            }
            Some(StorageGcEligibilitySample {
                ref_id: storage_gc_ref(entry),
                eligible_after_ms,
                has_delete_marker,
                follower_safe,
                reclaimable_bytes: if follower_safe {
                    entry.address.length()
                } else {
                    0
                },
            })
        })
        .take(MAX_STORAGE_GC_SAMPLES)
        .collect();

    if eligibility_samples.is_empty() && snapshot.gc_eligible_record_count > 0 {
        eligibility_samples.push(StorageGcEligibilitySample {
            ref_id: "aggregate:gc_eligible_records".to_string(),
            eligible_after_ms: 0,
            has_delete_marker: snapshot.delete_marker_records > 0,
            follower_safe,
            reclaimable_bytes: if follower_safe {
                snapshot.reclaimable_bytes
            } else {
                0
            },
        });
    }
    snapshot.gc_eligibility_samples = eligibility_samples;

    snapshot.follower_cursor_safety_samples = vec![StorageFollowerCursorSafetySample {
        min_follower_cursor: snapshot.follower_cursor_retention_floor,
        blocked_reclaim_bytes: if follower_safe {
            0
        } else {
            snapshot.reclaimable_bytes
        },
        safe_to_reclaim: follower_safe,
    }];
    snapshot
}

pub(super) fn storage_topology_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    storage_topology_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe: one lock, an unchanged `&ShardState`, and these feed report SAMPLES
/// rather than a decision.
pub(super) fn storage_topology_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.address
                .slab_id()
                .unwrap_or(left.address.block_slab_id()),
            left.address.block_slab_id(),
            left.address.offset(),
            left.kind.as_str(),
            left.object_key.as_ref(),
        )
            .cmp(&(
                right
                    .address
                    .slab_id()
                    .unwrap_or(right.address.block_slab_id()),
                right.address.block_slab_id(),
                right.address.offset(),
                right.kind.as_str(),
                right.object_key.as_ref(),
            ))
    });

    const MAX_STORAGE_TOPOLOGY_SAMPLES: usize = 8;
    #[derive(Default)]
    struct SlabUsageAcc {
        used_bytes: u64,
        stale_bytes: u64,
        slabs: BTreeSet<u64>,
        generation: u64,
    }
    #[derive(Default)]
    struct SlabAcc {
        stored_slab_id: u64,
        start_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct SlabAccumulator {
        min_offset: u64,
        max_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct BucketAcc {
        dirty_generation: u64,
        object_refs: BTreeSet<u64>,
        block_refs: Vec<StoragePageAddressSample>,
        delete_markers: BTreeSet<String>,
    }

    let mut slabs_usage = BTreeMap::<u64, SlabUsageAcc>::new();
    let mut slabs = BTreeMap::<u64, SlabAcc>::new();
    let mut slab_ranges = BTreeMap::<u64, SlabAccumulator>::new();
    let mut buckets = BTreeMap::<u32, BucketAcc>::new();

    for entry in &entries {
        let stored_slab_id = entry
            .address
            .slab_id()
            .unwrap_or(entry.address.block_slab_id());
        let slab_id = entry.address.block_slab_id();
        let generation = expected_live_block_object_id(shard_id, entry);
        let usage = slabs_usage.entry(stored_slab_id).or_default();
        usage.slabs.insert(slab_id);
        usage.generation = usage.generation.max(generation);
        if entry.deleted {
            usage.stale_bytes = usage.stale_bytes.saturating_add(entry.address.length());
        } else {
            usage.used_bytes = usage.used_bytes.saturating_add(entry.address.length());
        }

        let slab = slabs.entry(slab_id).or_insert_with(|| SlabAcc {
            stored_slab_id,
            start_offset: entry.address.offset(),
            ..SlabAcc::default()
        });
        slab.start_offset = slab.start_offset.min(entry.address.offset());
        slab.generation = slab.generation.max(generation);
        if entry.deleted {
            slab.deleted_refs = slab.deleted_refs.saturating_add(1);
        } else {
            slab.live_refs = slab.live_refs.saturating_add(1);
        }

        let range = slab_ranges
            .entry(stored_slab_id)
            .or_insert_with(|| SlabAccumulator {
                min_offset: entry.address.offset(),
                max_offset: entry.address.offset().saturating_add(entry.address.length()),
                ..SlabAccumulator::default()
            });
        range.min_offset = range.min_offset.min(entry.address.offset());
        range.max_offset = range
            .max_offset
            .max(entry.address.offset().saturating_add(entry.address.length()));
        range.generation = range.generation.max(generation);
        if entry.deleted {
            range.deleted_refs = range.deleted_refs.saturating_add(1);
        } else {
            range.live_refs = range.live_refs.saturating_add(1);
        }

        let bucket_id = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let bucket = buckets.entry(bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(generation);
        bucket.object_refs.insert(generation);
        if bucket.block_refs.len() < MAX_STORAGE_TOPOLOGY_SAMPLES {
            bucket.block_refs
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        if entry.deleted {
            bucket.delete_markers.insert(storage_gc_ref(entry));
        }
    }

    // THE IDENTITY COMES FROM BESIDE THE PAGE. The delete marker below states an object, and
    // the entry is losing its own copy; the inversion is built once for the whole walk, which
    // `shard` being immutable here makes free of any borrow to split.
    let identities = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        let bucket = buckets.entry(*bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(runtime_bucket.dirty_generation);
        bucket.object_refs
            .extend(runtime_bucket.object_index.iter().copied());
        for (handle, block) in runtime_bucket.block_index.iter() {
            if bucket.block_refs.len() >= MAX_STORAGE_TOPOLOGY_SAMPLES {
                break;
            }
            bucket.block_refs
                .push(storage_page_address_sample(shard_id, &block.address));
            // A DELETED PAGE IS NAMED BY THIS BUCKET'S OWN TOMBSTONE ROW, which is the source
            // `identity_of` reaches for one -- the lookup never holds a deleted page. A marker for
            // a page nothing names would name no object, so it is not written.
            if let Some(beside) = block
                .deleted
                .then(|| identities.identity_of(runtime_bucket, *handle, block))
                .flatten()
            {
                bucket.delete_markers
                    .insert(format!("{}:{}", block.model_id, beside.object_key));
            }
        }
    }

    snapshot.storage_slab_usage_samples = slabs_usage
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(stored_slab_id, usage)| StorageSlabUsageSample {
            stored_slab_id,
            total_bytes: usage.used_bytes.saturating_add(usage.stale_bytes),
            used_bytes: usage.used_bytes,
            stale_bytes: usage.stale_bytes,
            slabs: usage.slabs.into_iter().collect(),
        })
        .collect();
    let stream_slabs = slabs.keys().copied().collect::<Vec<_>>();
    snapshot.stream_samples = (!stream_slabs.is_empty())
        .then(|| StorageStreamSample {
            stream_id: format!("shard:{shard_id}:page_stream"),
            rollover_count: snapshot.slab_open_count.saturating_sub(1),
            sealed_slab_count: snapshot.slab_sealed_count,
            slabs: stream_slabs
                .iter()
                .copied()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
        })
        .into_iter()
        .collect();
    snapshot.slab_samples = slabs
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(slab_id, slab)| StorageSlabSample {
            slab_id,
            stored_slab_id: slab.stored_slab_id,
            start_offset: slab.start_offset,
            sealed: slab.live_refs == 0 || slab.deleted_refs > 0,
            generation: slab.generation,
        })
        .collect();
    snapshot.slab_range_samples = slab_ranges
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(stored_slab_id, range)| StorageSlabSlabSample {
            stored_slab_id,
            block_range: vec![range.min_offset, range.max_offset],
            reclaim_state: if range.deleted_refs > 0 && range.live_refs == 0 {
                "reclaimable".to_string()
            } else if range.deleted_refs > 0 {
                "mixed_live_stale".to_string()
            } else {
                "live".to_string()
            },
            generation: range.generation,
        })
        .collect();
    snapshot.bucket_samples = buckets
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(bucket_id, bucket)| StorageBucketSample {
            bucket_id,
            dirty_generation: bucket.dirty_generation,
            object_refs: bucket
                .object_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            block_refs: bucket
                .block_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            delete_markers: bucket
                .delete_markers
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            owner_mismatch_count: 0,
        })
        .collect();
    snapshot
}

/// Running total of live-block entries materialized by [`collect_live_block_entries`].
///
/// This walk is `O(live pages)` and clones two strings per entry, and several callers run it on
/// a background loop, so its cost is easy to introduce and hard to notice. The counter makes it
/// measurable: a test can assert that a code path's scan volume does not grow with the store.
static LIVE_BLOCK_SCAN_ENTRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Live-block entries materialized since the last reset.
pub fn live_block_scan_entries() -> u64 {
    LIVE_BLOCK_SCAN_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the scan counter. For tests measuring one operation's scan volume.
pub fn reset_live_block_scan_entries() {
    LIVE_BLOCK_SCAN_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Live-block entries MATERIALIZED by the two bucket-scoped model-map walks, across every call.
///
/// [`release_bucket_blocks`] and [`reload_released_bucket`] each ask one question of the model
/// maps about a NAMED set of buckets. Nothing indexes the model maps by routing bucket, so both
/// have to walk them to ask it -- but neither has to build an owned entry for every live block in
/// the store on the way past, and until this counter existed nothing said which they did.
///
/// Distinct from [`LIVE_BLOCK_SCAN_ENTRIES`], which counts only what the wrapper
/// `collect_live_block_entries` materializes. Both of these paths call
/// `collect_model_live_block_entries` DIRECTLY and are invisible to that counter, which is why a
/// round whose release allocated four times per object in the store could report zero entries
/// scanned.
///
/// COUNTED, not timed, and independent of the counting allocator: a guard can assert this number
/// tracks the victims without `alloc-probe` being on. Process-wide, so a reader must reset it
/// immediately before the call it is measuring, and the suite it is read in runs single-threaded.
static BUCKET_SCOPED_MODEL_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Entries materialized by the bucket-scoped model-map walks since the last reset.
pub fn bucket_scoped_model_entries() -> u64 {
    BUCKET_SCOPED_MODEL_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the bucket-scoped walk counter. Pairs with [`bucket_scoped_model_entries`].
pub fn reset_bucket_scoped_model_entries() {
    BUCKET_SCOPED_MODEL_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Model-map addresses a walk LOOKED AT, across every tally.
///
/// [`BUCKET_SCOPED_MODEL_ENTRIES`] counts what a bucket-scoped walk EMITS, and that is bounded by
/// the buckets it was asked about. The pass underneath it is not: the maps are keyed by object
/// and the routing bucket is a field of the ADDRESS, so `accept` runs on every live address in
/// the shard before any of them is filtered away. A release of four buckets and a release of four
/// buckets on a store ten times the size therefore report the same emitted count while doing ten
/// times the work, and until this counter existed nothing in the tree could tell them apart.
///
/// Charged once per walk from a local tally, so the walk pays one atomic and not one per address.
/// Process-wide like its neighbours: reset immediately before the call being measured, and read
/// it in a single-threaded run.
static MODEL_MAP_ADDRESSES_VISITED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Model-map addresses visited since the last reset.
pub fn model_map_addresses_visited() -> u64 {
    MODEL_MAP_ADDRESSES_VISITED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the model-map visit counter. Pairs with [`model_map_addresses_visited`].
pub fn reset_model_map_addresses_visited() {
    MODEL_MAP_ADDRESSES_VISITED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Times [`release_bucket_blocks`] actually ran its whole-store derivation.
///
/// [`MODEL_MAP_ADDRESSES_VISITED`] says how big the pass was; this says whether it happened. The
/// two are different questions and only one of them survives a store of any particular size: a
/// release that never reaches the model-map comparison visits zero addresses, and zero is also
/// what a counter wired to nothing reads. Counting the derivations separately means a guard can
/// assert the pass did NOT run without that assertion being satisfied by the counter being dead.
///
/// At most one per `release_bucket_blocks` call by construction -- the derivation covers the whole
/// candidate set in one pass and is memoized for the rest of the batch -- so the identity a guard
/// pins is `derivations == 1 if any candidate reached the comparison else 0`, whatever the batch
/// size and whatever the store size.
static BUCKET_RELEASE_MODEL_DERIVATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Whole-store derivations run by the bucket release since the last reset.
pub fn bucket_release_model_derivations() -> u64 {
    BUCKET_RELEASE_MODEL_DERIVATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the release-derivation counter. Pairs with [`bucket_release_model_derivations`].
pub fn reset_bucket_release_model_derivations() {
    BUCKET_RELEASE_MODEL_DERIVATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Bucket-index entries visited by [`bucket_index_resident_bytes`]: one per node plus one per
/// resident block.
///
/// Deliberately its own counter rather than a site on [`BUCKET_BLOCK_INDEX_VISITS`]. That total
/// is asserted on by existing guards around maintenance rounds, and folding a new walk into it
/// would move their numbers without any of them being about this walk. What this one is for is
/// the eviction round, which reads resident bytes twice -- once before its gate and once after
/// its actuator -- and pays the whole bucket index each time.
static BUCKET_INDEX_RESIDENT_BYTES_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bucket-index entries visited by the resident-bytes walk since the last reset.
pub fn bucket_index_resident_bytes_visits() -> u64 {
    BUCKET_INDEX_RESIDENT_BYTES_VISITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the resident-bytes walk counter. Pairs with [`bucket_index_resident_bytes_visits`].
pub fn reset_bucket_index_resident_bytes_visits() {
    BUCKET_INDEX_RESIDENT_BYTES_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Running total of bucket `block_index` entries visited by the bucket-maintenance walks.
///
/// Distinct from [`LIVE_BLOCK_SCAN_ENTRIES`], which counts materialized live-block entries. This
/// one counts the cheaper-looking `bucket.block_index.values()` passes -- `update_bucket_layout`
/// and the per-object dirty-state clear. Each is `O(pages in the bucket)` and they run inside
/// loops over buckets, so their cost is a product, not a sum, and does not show up in any single
/// obvious place.
static BUCKET_BLOCK_INDEX_VISITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bucket `block_index` entries visited since the last reset.
pub fn bucket_block_index_visits() -> u64 {
    BUCKET_BLOCK_INDEX_VISITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the bucket-visit counter. For tests measuring one operation's maintenance volume.
pub fn reset_bucket_block_index_visits() {
    BUCKET_BLOCK_INDEX_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The same two walks, accumulated for the STAGE of a maintenance round that is running.
///
/// Separate atomics rather than a second read of the two above, because the two questions have
/// different spans. [`LIVE_BLOCK_SCAN_ENTRIES`] answers "how much did this whole call walk", and
/// a reader resets it once around the call. These answer "how much did THIS STAGE walk", and are
/// read-and-cleared at every stage boundary -- the same discipline the round's `stage_clock`
/// already uses for `duration_ms`, so the stage rows tile the round rather than overlapping it.
///
/// KEEPING THEM SEPARATE IS THE POINT, not an accident of implementation. A residual computed as
/// "the whole call minus the stages" is only a reading if the two sides are measured by different
/// instruments; if the stage rows were slices of the same counter the subtraction would be an
/// identity, and an identity cannot notice a stage boundary that has drifted or a walk made
/// outside every stage. Both are charged by the same primitives below, and neither is derived
/// from the other.
static STAGE_LIVE_BLOCK_SCAN_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static STAGE_BUCKET_BLOCK_INDEX_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Served-index encodes charged to the stage that is running, and their bytes.
///
/// A WHOLE-STORE COST NEITHER WALK COUNTER CAN SEE. `serialize_index` encodes the entire served
/// index for the shard; its cost is the store, and it materialises no live-block entry and visits
/// no bucket `block_index`, so both counters above read zero across it. It is charged here so the
/// round's own rows account for it -- and so the part of the round that does it OUTSIDE every
/// stage shows up as a residual instead of as nothing.
static STAGE_INDEX_ENCODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static STAGE_INDEX_ENCODE_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Charge one served-index encode to the running stage. Called by `note_index_encode`.
pub(super) fn note_stage_index_encode(bytes: usize) {
    STAGE_INDEX_ENCODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODE_BYTES.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Read the current stage's walk charges and clear them for the next stage.
///
/// Read-and-clear in one call, so a stage cannot be charged twice and the next stage cannot
/// inherit this one's total.
pub fn take_stage_walk_charges() -> StageWalkCharges {
    StageWalkCharges {
        live_block_entries: STAGE_LIVE_BLOCK_SCAN_ENTRIES.swap(0, std::sync::atomic::Ordering::Relaxed),
        bucket_block_index_visits: STAGE_BUCKET_BLOCK_INDEX_VISITS
            .swap(0, std::sync::atomic::Ordering::Relaxed),
        index_encodes: STAGE_INDEX_ENCODES.swap(0, std::sync::atomic::Ordering::Relaxed),
        index_encode_bytes: STAGE_INDEX_ENCODE_BYTES
            .swap(0, std::sync::atomic::Ordering::Relaxed),
    }
}

/// Clear both stage accumulators without reporting them, so the first stage of a round is not
/// charged for whatever ran before the round started.
pub fn reset_stage_walk_charges() {
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_BUCKET_BLOCK_INDEX_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODES.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODE_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Charge both the whole-call and the per-stage live-block counters by `count`.
///
/// Tests only, and it exists for one job: planting a known quantity into the residual instrument
/// so a test can prove the subtraction recovers it exactly, rather than trusting that a zero
/// means nothing was missed.
#[cfg(test)]
pub fn plant_live_block_scan_entries_for_test(count: u64) {
    LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}

/// Which tally a model-map walk is charged to.
///
/// Named by the caller, applied by [`visit_model_live_blocks`] itself. The charge happens where
/// the blocks are EMITTED, not at the call site, because a call site that counts is a call site
/// the next caller forgets: [`LIVE_BLOCK_SCAN_ENTRIES`] was charged in exactly one place --
/// `collect_live_block_entries` -- while seven production call sites reached the same two walks
/// directly and were charged nothing at all.
///
/// There is deliberately NO uncounted variant. Every way of walking the model maps names one of
/// these three, so a walk added later cannot compile without saying where it is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ModelWalkTally {
    /// Materializing every live block in the shard: [`LIVE_BLOCK_SCAN_ENTRIES`].
    WholeShardEntries,
    /// Materializing the blocks of NAMED routing buckets: [`BUCKET_SCOPED_MODEL_ENTRIES`].
    BucketScopedEntries,
    /// The promotion check's borrow-only pass: `PROMOTE_MODEL_MAP_BLOCKS`.
    PromotionCheckBlocks,
}

/// Charge live-block entries to the scan counter AND to the caller that asked for them.
///
/// `#[track_caller]` all the way down from the public walks, so a charge moved inward still
/// attributes to the same source line it did when the wrapper counted.
#[track_caller]
fn note_live_block_scan(count: usize) {
    LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    let caller = std::panic::Location::caller();
    if let Ok(mut sites) = live_block_scan_sites().lock() {
        *sites
            .entry(format!("{}:{}", caller.file(), caller.line()))
            .or_insert(0) += count as u64;
    }
}

fn note_bucket_block_visits(count: usize) {
    BUCKET_BLOCK_INDEX_VISITS.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    STAGE_BUCKET_BLOCK_INDEX_VISITS.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Per-site attribution for [`BUCKET_BLOCK_INDEX_VISITS`], so a scaling result names the walk that
/// caused it rather than leaving it to be inferred from arithmetic.
pub mod bucket_visit_sites {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) static LAYOUT: AtomicU64 = AtomicU64::new(0);
    pub(super) static CLEAR_DIRTY: AtomicU64 = AtomicU64::new(0);
    /// THE TTL ROLLUP, NOT THE WHOLE REFRESH -- which the name does not say and a reader needs to
    /// know before drawing a conclusion from a zero.
    ///
    /// This is charged inside `refresh_one_bucket_runtime_flags`, but inside the
    /// `BucketTtlRollup::Recompute` arm AND inside the `else` of `if expires_at_ms.is_empty()`. So
    /// it counts pages only when the caller asked for a TTL recompute and the shard holds at least
    /// one expiry. The write path passes `LeaveStale`, and a store that never sets a TTL takes the
    /// short-circuit, so on both of those this counter is zero no matter how many pages the refresh
    /// walked.
    ///
    /// The refresh's own two folds are counted by [`FLAG_FOLDS`]. A probe reading this one to price
    /// them reported 0 against a bucket of 321 pages, and a positive control -- calling the sweep
    /// deliberately -- also reported 0, which is how the gap was found.
    pub(super) static REFRESH_FLAGS: AtomicU64 = AtomicU64::new(0);
    pub(super) static REMOVE_ALL_BUCKETS: AtomicU64 = AtomicU64::new(0);
    /// Pages actually visited by the two unconditional folds in `refresh_one_bucket_runtime_flags`:
    /// `all(page.deleted)` and `any(page.dirty)`.
    ///
    /// ITERATIONS TAKEN, NOT BUCKET LENGTH. Both folds short-circuit -- the comment at the fold says
    /// so -- so a bucket of 321 pages can decide on page one. Charging `block_index.len()` would
    /// report the walk this refresh did NOT do; `inspect` charges what it did.
    pub(super) static FLAG_FOLDS: AtomicU64 = AtomicU64::new(0);

    /// `(layout, clear_dirty, refresh_flags, remove_all_buckets)` visits since the last reset.
    ///
    /// `refresh_flags` is the TTL rollup only -- see [`REFRESH_FLAGS`]. For the refresh's own folds
    /// call [`flag_folds`]; it is deliberately not in this tuple, so that adding it cannot silently
    /// change what an existing destructuring of four means.
    pub fn snapshot() -> (u64, u64, u64, u64) {
        (
            LAYOUT.load(Ordering::Relaxed),
            CLEAR_DIRTY.load(Ordering::Relaxed),
            REFRESH_FLAGS.load(Ordering::Relaxed),
            REMOVE_ALL_BUCKETS.load(Ordering::Relaxed),
        )
    }

    /// Pages visited by the refresh's own folds since the last reset. See [`FLAG_FOLDS`].
    pub fn flag_folds() -> u64 {
        FLAG_FOLDS.load(Ordering::Relaxed)
    }

    pub fn reset() {
        for counter in [
            &LAYOUT,
            &CLEAR_DIRTY,
            &REFRESH_FLAGS,
            &REMOVE_ALL_BUCKETS,
            &FLAG_FOLDS,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

fn note_site(site: &std::sync::atomic::AtomicU64, count: usize) {
    site.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    note_bucket_block_visits(count);
}

/// Per-CALLER attribution for [`LIVE_BLOCK_SCAN_ENTRIES`], keyed by the source location that asked.
///
/// The total alone says a round walks the shard N times; it does not say WHO. Attributing it by
/// hand hit a wall: `apply_storage_lifecycle` measures 10.0x while every one of its callees sums
/// to 6.0x, and the remaining four walks are inside the body where no probe row can reach them.
/// Two earlier attributions failed the same way and were only closed by finding a call that no
/// grep had matched -- once a bare expression at the end of a function.
///
/// `#[track_caller]` gives the answer with NO call-site changes, which matters because this
/// function has about twenty of them and a threaded-through label would have to be right at every
/// one to be trustworthy. The location is resolved at compile time; the cost here is one map
/// update per CALL, on a path that is already walking every live block in the shard.
fn live_block_scan_sites() -> &'static std::sync::Mutex<std::collections::BTreeMap<String, u64>> {
    static SITES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
    > = std::sync::OnceLock::new();
    SITES.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

/// Entries materialized per calling site since the last reset, as `file:line -> entries`.
pub fn live_block_scan_sites_snapshot() -> std::collections::BTreeMap<String, u64> {
    live_block_scan_sites()
        .lock()
        .map(|sites| sites.clone())
        .unwrap_or_default()
}

/// Clear the per-site tallies. Pairs with [`reset_live_block_scan_entries`].
pub fn reset_live_block_scan_sites() {
    if let Ok(mut sites) = live_block_scan_sites().lock() {
        sites.clear();
    }
}

/// Both arms charge themselves, so this wrapper is no longer the only counted way in. It stays
/// `#[track_caller]` so a charge made further down still attributes to ITS caller.
#[track_caller]
pub(super) fn collect_live_block_entries(shard: &ShardState) -> Vec<LiveBlockEntry> {
    if !shard.bucket_index.bucket_map.is_empty() {
        collect_bucket_index_live_block_entries(shard)
    } else {
        collect_model_live_block_entries(shard)
    }
}

/// Mark one object, and its bucket, dirty.
///
/// # IT IS NOT ASYNC-SPECIFIC, WHATEVER ITS OLD NAME SAID
///
/// This was `mark_async_dirty_object`, and the name cost a wrong conclusion: reading it, the
/// synchronous write path looked as though it already wrote through. It does not.
/// `what_a_synchronous_write_leaves_pending` measures both paths leaving the whole corpus pending,
/// 64 of 64, and the synchronous site called straight into here.
///
/// So the name described the caller it was first written for rather than a condition, and dirtying
/// is unconditional. Removing the write-back set needs the write path to become write-through,
/// which is a build and not a setting.
pub(super) fn mark_object_dirty(
    shard: &mut ShardState,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    let routing_bucket = block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    // Recorded WITH the bucket that was just computed for it. Every consumer that asks which
    // bucket a dirty object belongs to used to recompute this hash for itself.
    // ONE COPY OF THE CHARACTERS -- see `DirtyObjectIndex::insert_shared`. The handle is read out
    // before the mutable borrow so the two do not overlap.
    let shared = shard.bucket_index.object_block_lookup.any_shared_object_key(object_key);
    match shared {
        Some(shared) => shard.dirty_objects.insert_shared(shared, routing_bucket),
        None => shard.dirty_objects.insert(object_key, routing_bucket),
    }
    // PUSHED, NOT PULLED -- see `push_object_dirty_onto_its_existing_blocks`. This is the second of
    // exactly two sites that mark an object dirty, and the invariant the tests pin is that BOTH
    // push: a mark that does not reach its pages leaves the bucket reading clean.
    super::push_object_dirty_onto_its_existing_blocks(shard, object_key);
    let bucket = shard
        .bucket_index
        .bucket_map
        .entry(routing_bucket)
        .or_insert_with(|| BucketNode {
            routing_bucket,
            flags: BucketFlags::default().with(BucketFlags::META_LOADED, true),
            ..BucketNode::default()
        });
    bucket.set_dirty(true);
    bucket.dirty_generation = bucket.dirty_generation.saturating_add(1).max(1);
    note_bucket_flags_stale(shard, routing_bucket);
}

pub(super) fn rebuild_bucket_block_ownership(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    // Preserve the durable per-bucket watermark across the clear+rebuild. load_index keeps
    // dirty_generation; rebuilding with a fresh BucketNode::default() would zero it, making a
    // restored shard (e.g. after a manifest install) mismatch its own dump-manifest generation
    // and forcing unnecessary re-dumps (and mis-driving the index/GC reclaim watermarks). Carry
    // the prior value over via the snapshot below.
    //
    // ONE watermark, where this carried two. `last_dump_sequence` used to ride along and no
    // longer exists on the node: it was read into two reports and nothing else, and the report
    // takes it from the newest dump manifest instead. Nothing here has to preserve a figure a
    // manifest already holds.
    let preserved_dirty_generations: HashMap<u32, u64> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.dirty_generation))
        .collect();
    // AND THE TOMBSTONE ENTRIES, WHICH THE MODEL MAPS CANNOT RE-DERIVE.
    //
    // This function rebuilds every bucket from `collect_model_live_block_entries`, and those maps hold
    // only LIVE elements -- a removed one is gone from `shard.sets` by construction. So a rebuild
    // produces one entry per live element and NONE for a removal, and the entry that keeps a removal's
    // tombstone page reachable is erased by any round that rebuilds. `compact_shard_blocks` calls this
    // directly, twice, and `promote_model_maps_to_bucket_index_authority` calls it as well; so before
    // this, a removal stopped being recorded in the pages at the next compaction round and a
    // page-derived membership would have resurrected the element.
    //
    // THE ENTRIES ARE CARRIED OVER RATHER THAN RE-DERIVED, because they cannot be re-derived: nothing
    // outside the index knows a removal happened except the WAL, and a rebuild is not a replay. They
    // are filed back after the live rebuild, by the same function the removal path uses, so the
    // bucket accounting and the lookup skip are the ones that own those rules.
    //
    // Serialization is what makes this enough on the reload path: `block_index` carries `deleted` on
    // the wire, so a tombstone entry read back from a stored index survives, and a REPLAY re-files it
    // from the outcome's address instead. Between them the two paths cover every way an index arrives.
    // AND THE ELEMENT ROW BESIDE EACH, because the entry no longer names its element. A tombstone
    // carried across without its row survives as an entry nothing can identify: the next re-add
    // cannot find it to clear, so the removal is restated for ever and the retention stops being
    // bounded at one row per distinct element removed.
    let preserved_tombstones: Vec<(u32, BlockIndex, Option<TombstoneElement>)> = shard
        .bucket_index
        .bucket_map
        .iter()
        .flat_map(|(routing_bucket, bucket)| {
            bucket
                .block_index
                .values()
                .filter(|block| block.deleted)
                .map(move |block| {
                    (
                        *routing_bucket,
                        block.clone(),
                        bucket.tombstone_element_at(&block.address).cloned(),
                    )
                })
        })
        .collect();
    shard.bucket_index.bucket_map.clear();
    // AND THE LOOKUP WITH IT, because it describes the map that was just emptied.
    //
    // It used to survive this clear and be rebuilt wholesale at the end of the function. Now that
    // the refile below files each page as it goes, a surviving pre-compaction entry would be a
    // position naming a page that no longer exists -- so the structure has to start empty for the
    // filing to be the whole story. The ref total goes back to the zero a fresh index carries, for
    // the same reason: `insert_object_block_lookup` counts up from it, and a total carried over
    // from the emptied map would count pages twice.
    shard.bucket_index.object_block_lookup.clear();
    shard.bucket_index.object_component_block_refs = Some(0);
    // The tally counts the map that was just emptied. Emptied with it, and re-earned by the
    // charges the inserts below make -- not by a walk afterwards, which is the walk this whole
    // change exists to remove and which a compaction round would pay twice.
    shard.bucket_index.block_slab_live.clear();
    // The rebuild re-derives every bucket from the model maps, which is what a reload does one
    // bucket at a time. Nothing is released afterwards, and a registry that outlived the map it
    // names would make the block walk supplement buckets that are already whole.
    shard.bucket_index.released_buckets.clear();
    // WHAT THE REFILE PLACES, SO THE LOOKUP CAN BE FILED FROM IT.
    //
    // Collected rather than filed inside the loop: the filing takes the whole index mutably and
    // the loop holds one bucket out of it, so it cannot happen there -- the same shape
    // `reload_released_bucket` already uses for the same reason.
    let mut filed_blocks: Vec<(u32, u64, Arc<str>, BlockIndex)> = Vec::new();
    for entry in collect_model_live_block_entries(shard) {
        let routing_bucket =
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket);
        // THE OUT-OF-RANGE FILTER THAT STOOD HERE CANNOT FIRE ANY MORE, so it is gone rather than
        // kept as a `continue` nothing reaches.
        //
        // It read `if routing_bucket < start || routing_bucket > end { continue; }` and its only
        // possible input was an EXPLICIT bucket carried on the address -- a block whose stored bucket
        // fell outside the range the shard was loaded on. There are no explicit buckets: the line
        // above DERIVES the bucket as `start + FNV-1a-64(key) % (end - start + 1)`, which is inside
        // `start..=end` by construction (and `start` itself for a degenerate range).
        // `a_derived_bucket_is_always_inside_the_range_it_was_derived_on` drives that over many keys
        // and several ranges rather than leaving it as arithmetic in a comment.
        //
        // What this removes is not a check but a HAZARD: mx#1974 measured this filter dropping a
        // block from the index entirely when an explicit bucket sat outside the range, and bounded it
        // by showing the engine does not produce that state. It now cannot be produced at all.
        let object_id = expected_live_block_object_id(shard_id, &entry);
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| {
                let dirty_generation = preserved_dirty_generations
                    .get(&routing_bucket)
                    .copied()
                    .unwrap_or_default();
                BucketNode {
                    routing_bucket,
                    flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                    dirty_generation,
                    ..BucketNode::default()
                }
            });
        bucket.object_index.insert(object_id);
        // Charged as it goes, and then replaced wholesale by the `seed_block_slab_live` at the end
        // of this rebuild. Both, deliberately: the charge keeps this site honest if the shape of
        // the function changes, and the seed is what makes the result independent of whatever the
        // tally held before `bucket_map.clear()` above.
        let object_key = Arc::clone(&entry.object_key);
        let handle = bucket.insert_block(
            &object_key,
            BlockIndex {
                model_id: entry.kind,
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
            },
            &mut shard.bucket_index.block_slab_live,
        );
        // THE PAGE IS TAKEN BACK OUT BY ITS HANDLE rather than cloned on the way in, so the
        // entry the lookup records is the entry the map HOLDS. A page whose handle collided
        // with an earlier one is not in the map twice and must not be in the lookup twice
        // either; that is the decoder's rule, read off the same reasoning.
        if let Some(placed) = bucket.block_index.get(&handle) {
            filed_blocks.push((routing_bucket, handle, object_key, placed.clone()));
        }
    }
    for (routing_bucket, handle, object_key, block) in &filed_blocks {
        shard
            .bucket_index
            .insert_object_block_lookup(*routing_bucket, *handle, object_key, block);
    }
    // FILE THE TOMBSTONE ENTRIES BACK, after the live rebuild and before the flag pass below, so the
    // `every_block_deleted` computation sees the finished bucket rather than a half-built one.
    //
    // Into the bucket each was FILED IN, not one recomputed from the key: the removal recorded that
    // bucket deliberately, because `block_routing_bucket(key, 0, u32::MAX)` -- the form the WAL outcome
    // uses -- is a different number from the shard's own range, and filing a tombstone under it put the
    // entry in a bucket holding nothing else for its object. That made the bucket all-tombstone (so a
    // twelve-member set reported as a deleted object) and made ownership validation refuse a whole
    // compaction round. Carrying the recorded bucket cannot reintroduce either.
    //
    // A tombstone whose element is LIVE AGAIN is dropped rather than filed. A re-add clears the
    // tombstone through `upsert_bucket_index_block_inner`'s retain, and a rebuild has to reach the same
    // state or it would resurrect a removal the store has already undone -- the mirror image of the
    // defect this whole change is about, and the one direction nothing else here would catch.
    let mut tombstones_refiled = 0usize;
    for (routing_bucket, tombstone, element) in preserved_tombstones {
        let Some(element) = element else {
            // A TOMBSTONE NAMING NO ELEMENT CANNOT BE REFILED, and this is where that becomes
            // visible rather than theoretical. The handle a page is filed under is hashed from its
            // identity; a deleted page is not in the object lookup, so its row is the only thing
            // beside it that states one -- and a tombstone with no row states nothing. It is
            // dropped rather than filed under a guessed identity.
            //
            // MEASURED NOT TO HAPPEN, over 272 tests in the nine removal-shaped filters: the one
            // producer, `insert_container_tombstone_entry`, was never once reached with no element
            // to name. The detector was proved live by inverting it -- 22 hits under
            // `container_tombstone` when it asked for the opposite -- so the zero is a measurement
            // and not a sweep that matched nothing. `insert_container_tombstone_entry` now refuses
            // one in a debug build, which is what keeps this branch unreachable rather than rare.
            continue;
        };

        // IS THE ELEMENT LIVE AGAIN -- ASKED OF THE RESIDENT MAP, NOT OF THE LIVE ENTRIES.
        //
        // This compared the tombstone's name against the names on this object's LIVE entries. A
        // live entry carries no element name, so that test answered `Some(x) == None` for every
        // pair and never fired: the branch read as a correctness check and was unreachable. The
        // resident map is where the elements are named, and it is what the live entries above were
        // just derived from, so asking it is reading the same authority one element at a time.
        //
        // A tombstone with NO element row cannot be asked at all. It is refiled, which is the safe
        // direction: a refiled tombstone over a live element is still outranked by the live page's
        // later append position, where a DROPPED tombstone over a removed element resurrects it.
        let live_again = resident_holds_component(
            shard,
            tombstone.model_id,
            &element.object_key,
            &element.component,
        );
        if live_again {
            continue;
        }
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| {
                let dirty_generation = preserved_dirty_generations
                    .get(&routing_bucket)
                    .copied()
                    .unwrap_or_default();
                BucketNode {
                    routing_bucket,
                    flags: BucketFlags::default()
                        .with(BucketFlags::META_LOADED, true)
                        .with(BucketFlags::IN_MEMORY, true),
                    dirty_generation,
                    ..BucketNode::default()
                }
            });
        let address = tombstone.address.clone();
        let model_id = tombstone.model_id;
        bucket.insert_block(
            &element.object_key,
            tombstone,
            &mut shard.bucket_index.block_slab_live,
        );
        // AND ITS ELEMENT ROW TRAVELS WITH IT -- UNCONDITIONALLY NOW, because a tombstone with no
        // row was dropped above rather than refiled. Without the row the tombstone survives the
        // rebuild as an entry whose element nothing can name, so the next re-add cannot find it to
        // clear and the retention bound is gone.
        bucket.record_tombstone_element(
            &element.object_key,
            model_id,
            &element.component,
            &address,
        );
        tombstones_refiled += 1;
    }
    note_tombstones_refiled(tombstones_refiled);
    // THE WHOLE-SHARD REBUILD IS GONE FROM HERE, and the claim it stood for is checked instead.
    //
    // It cleared the lookup and re-filed every page in the shard, reading the object key off each
    // one. The refile above files as it goes, so there is nothing left for it to discover -- and
    // the rebuild was the last whole-shard reader of the identity ON a page, rebuilding the very
    // structure that is the identity BESIDE a page, which is circular and is why it has to go
    // rather than be rewritten to take the identities as a parameter.
    //
    // In a release build this is one fewer O(blocks) pass per compaction round, and the debug
    // verification that earned the removal has now been spent: it compared the lookup against the
    // pages' own copies of the identity and the pages have none. `engine_storage_state.rs` carries why a version
    // of it over one source would compare that source with itself.
    // THE INVERSION IS HOISTED, AND THE TWO BORROWS ARE SPLIT BY NAMING THE FIELDS. The derive
    // reads the object lookup while this loop holds the bucket map mutably, and both are fields of
    // one index -- so the split has to be written where the places are named, rather than inside a
    // method that would borrow the whole index. Built ONCE above a whole-shard pass, which is the
    // order that pass already pays; built per bucket it would be that order per bucket.
    let index = &mut shard.bucket_index;
    let keys = crate::engine::state::BlockKeysByPosition::from_lookup(&index.object_block_lookup);
    for bucket in index.bucket_map.values_mut() {
        bucket.set_meta_loaded(true);
        bucket.set_loading(false);
        bucket.set_in_memory(!bucket.block_index.is_empty());
        let every_block_deleted =
            !bucket.block_index.is_empty() && bucket.block_index.values().all(|block| block.deleted);
        bucket.set_deleted(every_block_deleted);
        update_bucket_layout(shard_id, bucket, &keys);
    }
    // Every block above was charged as it was filed, and the tally started empty, so it now
    // describes exactly what `bucket_map` holds. Declaring that is the last step; confirming it
    // with a walk would cost a compaction round two whole-shard scans it does not need.
    shard.bucket_index.block_slab_live.mark_ready();
}

/// Tombstone entries carried across an index rebuild.
///
/// A COUNTER AND NOT A DERIVED FIGURE, for the reason every counter in this area exists: "the tombstone
/// count did not change" is also what a rebuild that never ran reports, and a guard needs to tell the
/// two apart. A rebuild that carried none over a store that had removals is the defect; a rebuild that
/// carried none because a re-add had cleared them is correct.
static TOMBSTONES_REFILED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_tombstones_refiled(count: usize) {
    TOMBSTONES_REFILED.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Tombstone entries an index rebuild has carried over since the last reset.
pub fn tombstones_refiled_count() -> u64 {
    TOMBSTONES_REFILED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own rebuilds.
pub fn reset_tombstones_refiled_count() {
    TOMBSTONES_REFILED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// What the promotion check decided, since the last reset.
///
/// #1888 reported that on a restore the answer is always "the index already names everything",
/// so the whole walk is spent returning `false`. That was MEASURED in its configurations, which
/// is not the same as the positive arm being unreachable -- and a precondition whose positive arm
/// nobody can construct is a precondition nobody has tested. These make the question answerable
/// from a guard rather than from a reading of the four call sites.
///
/// `PAGES` is what the walk actually offered the test. A guard asserting the check is cheap needs
/// it: a walk that visited nothing is cheap for the wrong reason, and would pass the same guard.
///
/// COUNTED, not timed, and independent of the counting allocator. Process-wide and monotonic, so
/// a reader resets immediately before the call it is measuring and reads it in a single-threaded
/// suite -- the same contract as [`bucket_scoped_model_entries`] above.
static PROMOTE_MODEL_MAP_CHECKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PROMOTE_MODEL_MAP_REBUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PROMOTE_MODEL_MAP_BLOCKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Promotion checks run, promotions that REBUILT, and model-map blocks the checks walked.
pub(super) fn promote_model_map_check_counts() -> (u64, u64, u64) {
    (
        PROMOTE_MODEL_MAP_CHECKS.load(std::sync::atomic::Ordering::Relaxed),
        PROMOTE_MODEL_MAP_REBUILDS.load(std::sync::atomic::Ordering::Relaxed),
        PROMOTE_MODEL_MAP_BLOCKS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Reset the promotion-check counters. Pairs with [`promote_model_map_check_counts`].
pub(super) fn reset_promote_model_map_check_counts() {
    PROMOTE_MODEL_MAP_CHECKS.store(0, std::sync::atomic::Ordering::Relaxed);
    PROMOTE_MODEL_MAP_REBUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
    PROMOTE_MODEL_MAP_BLOCKS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Make the bucket index name every live model-map block, if it does not already.
///
/// A PRECONDITION, and preconditions on this path have teeth: #1822 was the default recovery arm
/// calling `rebuild_bucket_block_ownership` without one of these in front of it, and a restart
/// served 0 of 6 hash fields against 6 of 6 strings. So WHAT THIS DECIDES is fixed, and the note
/// below about cost changes none of it.
///
/// The decision, in three parts:
///
///   1. no live model-map block at all -> `false`, having established nothing. (The per-execute
///      caller in `engine.rs` reads exactly this to know not to latch its fast-skip flag.)
///   2. otherwise: is `bucket_map` empty, OR is there a live block the index does not name at the
///      same address? A block routing to a RELEASED bucket is absent on purpose and is not one.
///   3. only if so, rebuild ownership over the routing range, refresh the runtime flags, `true`.
///
/// WHAT IS NOT MATERIALISED, and why that changes nothing above. This opened with
/// `collect_model_live_block_entries(shard)`, which walks the same blocks this walks and turns
/// every one of them into an owned `LiveBlockEntry` -- an owned key and an owned kind, each built
/// as a `String` and then copied into an `Arc<str>`, plus the vector holding them -- to ask an
/// `any()` a question answerable from the borrowed fields the walk is already holding. Since
/// step 2's answer is normally "no", the vector was built in full to return `false`: 4.0
/// allocations per record the shard holds, 22% of a restore's index fold and 10% of the whole
/// restore, and again on the per-execute path in `engine.rs` whenever its fast-skip is not
/// latched.
///
/// `visit_model_live_blocks` offers `emit` the kind, key, component and address as borrows, and
/// those are exactly the four arguments `contains_object_block_address` takes -- the same values
/// the old `any()` tested, in the same order, block for block. The walk IS the check and stays
/// whole; only the vector goes.
///
/// It does not stop the WALK at the first missing block. `any()` stopped there, and the visitor
/// four callers share has no way to be told to stop; what it does stop is the LOOKUP, which is
/// the part that costs more than a field read. On the path this was measured on nothing is ever
/// missing, so the early exit was never reached and removing it costs nothing measured.
pub(super) fn promote_model_maps_to_bucket_index_authority(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> bool {
    PROMOTE_MODEL_MAP_CHECKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (saw_model_entry, bucket_index_missing_entry) = {
        let shard: &ShardState = shard;
        // Read once, ahead of the walk, because the old form was
        // `bucket_map.is_empty() || entries.iter().any(..)` and the left arm short-circuited the
        // right one. An empty index names nothing, so every block would test missing anyway --
        // asking per block would be an index lookup per block for an answer already in hand.
        let bucket_map_empty = shard.bucket_index.bucket_map.is_empty();
        let mut saw_model_entry = false;
        let mut missing_entry = false;
        visit_model_live_blocks(
            shard,
            ModelWalkTally::PromotionCheckBlocks,
            |_, _| true,
            |kind, object_key, component, address| {
                saw_model_entry = true;
                // The early return below bypasses the LOOKUP, not the count. `visit_model_live_blocks`
                // charges every block it emits before this body runs at all, so a walk over a large
                // shard can no longer read as a walk over a small one by returning early -- and the
                // guard that reads the block count can no longer pass because the check looked cheap.
                if bucket_map_empty || missing_entry {
                    return;
                }
                // A RELEASED bucket is absent on purpose. Without this the first command after a
                // release would find every released block "missing" from the index and rebuild the
                // whole shard -- which is a correct index and a release that never survives one
                // execute.
                let released = shard.bucket_index.released_buckets.contains(
                    &block_routing_bucket(
                        object_key.as_str(),
                        start_routing_bucket,
                        end_routing_bucket,
                    ),
                );
                if !released
                    && !shard.bucket_index.contains_object_block_address(
                        kind.as_str(),
                        object_key.as_str(),
                        component,
                        address,
                    )
                {
                    missing_entry = true;
                }
            },
        );
        (saw_model_entry, bucket_map_empty || missing_entry)
    };
    if !saw_model_entry {
        return false;
    }
    if !bucket_index_missing_entry {
        return false;
    }
    PROMOTE_MODEL_MAP_REBUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    rebuild_bucket_block_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
    refresh_bucket_runtime_flags(shard);
    true
}

/// Rebuild ONLY the model maps that are `skip_serializing`, from the durable bucket index. A
/// freshly deserialized index never carries them, so anything deriving state from the model maps
/// would see a shard with no objects of those kinds at all.
///
/// THIS COVERS ONE OF THREE, and the sentence here used to say there was only one. Checked field
/// by field against `collect_model_live_block_entries`, which is what the manifest cross-check
/// reads: of the twelve maps it walks, three are `skip_serializing` --
///
///   * `hashes`            rebuilt below
///   * `context_events`    NOT rebuilt
///   * `context_indexes`   NOT rebuilt
///
/// so a slot dump whose buckets hold context-event or context-index blocks decodes with those maps
/// empty, the model-map derivation misses every one of those blocks, the bucket-index derivation
/// does not, and `install_bucket_dump_manifest` rejects a perfectly good manifest with
/// `slot_dump_object_lifecycle_mismatch`. That is the live failure of
/// `rust_executes_temporalstore_corpus` and
/// `rust_storage_replays_migration_corpus_across_lifecycle_paths`, both on case
/// `native_logical_storage_models_packed_timestamped_pages` -- timestamped being the shape of
/// exactly these two maps, which are keyed by `u64`.
///
/// NOT fixed here, because the two obvious repairs are not equivalent and the choice is not a
/// detail:
///
///   1. Rebuild them too. But these maps are keyed by TIMESTAMP and the bucket-index entries for
///      them carry `component: None` (see the `context_event` arm of
///      `collect_model_live_block_entries`), so the keys are not recoverable from the index.
///      Synthesising keys would make the cross-check agree while putting invented timestamps into
///      a time-keyed map, which is worse than the failure it cures.
///   2. Have the cross-check compare only the maps that SURVIVE serialization. A map the manifest
///      does not carry cannot disagree with the manifest, so it is outside what the check is for
///      -- its stated purpose is a manifest whose serialized model maps contradict its bucket
///      index. On this reading the `hashes` rebuild below is also unnecessary for the check.
///
/// The second looks right, but it narrows a durability check, so it wants the owner of the dump
/// format rather than an inference from a failing test.
///
/// Deliberately narrow: the serialized maps are left exactly as decoded. Rebuilding those from
/// the bucket index too would overwrite whatever the index actually said, which is precisely
/// the disagreement a manifest cross-check exists to detect -- a tampered `strings` object_id
/// would be silently repaired instead of rejected.
pub(super) fn rebuild_unserialized_model_maps_from_bucket_index(shard: &mut ShardState) {
    if shard.bucket_index.bucket_map.is_empty() {
        return;
    }
    // THE DERIVED VIEW IS EMPTY NOW, AND IT IS EMPTY STRUCTURALLY RATHER THAN BY CIRCUMSTANCE.
    //
    // This built a `hashes` map by walking the index and reading each entry's field name out of
    // `BlockIndex::component`. An entry has no such field, so there is nothing to derive FROM: not
    // "no fields were found this time" but "a field name is not representable on an entry". The
    // durable map is the sole source, which is what the four `durable_outranks_derived` arms are
    // about.
    //
    // WHAT WENT WITH THE LOOP, AND WHY REMOVING IT IS NOT THE SAME AS REMOVING THE RECONCILE. The
    // loop also counted entries that named no field and printed a line per load saying they had
    // been skipped rather than defaulted to the empty field name -- the right thing while an entry
    // COULD name one and some did not. With no entry able to name one, that count is simply the
    // number of live hash pages in the shard and the line would fire on every load of every
    // populated store, reporting a structural fact as an anomaly. So it goes with the loop that
    // produced it.
    //
    // THE RECONCILE BELOW STAYS, AND IT IS STILL LOAD-BEARING. `reconcile_from_durable` is
    // `fill_absent_elements(derived, persisted, live, refused)`: the persisted map is deserialized
    // BEFORE it and the call FILTERS that map against the live page set as well as merging the
    // derived view into it. Dropping the call would therefore leave MORE in the map, not less --
    // every durable element whose page the settled index no longer holds would be restored. An
    // empty derived view is passed instead, so the merge contributes nothing and the live-page
    // refusal still runs, which is the half this function keeps.
    let hashes = HashMap::<super::state::ModelKey, super::hash_field_map::HashFieldMap>::new();
    // THE SAME LIVE-ADDRESS SET THE RECONCILE BUILDS, and built from the same walk the derived view
    // below is built from, so the filter can only ever remove what the derived view also lacks.
    let walked = collect_bucket_index_live_block_entries(shard);
    let live_blocks_by_address: std::collections::HashSet<super::LiveBlockKey> = walked
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| super::live_block_key(&entry.address))
        .collect();
    // MERGED, NOT ASSIGNED, AND THE `is_empty` GATE WENT WITH THE ASSIGNMENT. While `hashes` was
    // `skip_serializing` there was nothing in `shard.hashes` to protect, so overwriting it whenever
    // the index said anything was harmless and the gate only avoided clearing it to empty. It is
    // durable now, so a wholesale assignment here would discard exactly the elements this function's
    // own header warns about discarding -- a field whose block the index cannot NAME is a field the
    // durable map is now the only record of. The merge keeps it; the live-address filter still drops
    // a durable element whose page the settled index does not hold.
    let mut resurrections_refused = 0usize;
    shard.hashes.reconcile_from_durable(
        hashes,
        &live_blocks_by_address,
        &mut resurrections_refused,
    );
    if resurrections_refused > 0 {
        eprintln!(
            "rebuild_unserialized_model_maps: {resurrections_refused} persisted hash field(s) named              a page the bucket index does not hold and were not restored"
        );
    }
}

#[track_caller]
pub(super) fn collect_bucket_index_live_block_entries(shard: &ShardState) -> Vec<LiveBlockEntry> {
    let mut entries = Vec::new();
    // Charged here rather than by whoever called: three production callers reach this walk
    // without going through `collect_live_block_entries`, and the wrapper's charge could not see
    // the supplement walk below either -- it charged what was RETURNED, while the walk
    // materializes the indexed blocks and then, whenever anything is released, the whole shard.
    let mut from_index = 0usize;
    // THE IDENTITY COMES FROM BESIDE THE PAGE, once for the walk. `shard` is immutable here.
    let identities = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for (handle, block) in bucket.block_index.iter() {
            // A ROW THIS WALK CANNOT NAME IS NOT EMITTED. `LiveBlockEntry::object_key` is the
            // whole point of the row -- five readers downstream key off it -- so a row carrying
            // no object would be worse than an absent one. The skip is refused outside the one
            // window that explains it.
            // NAMED WHEREVER IT IS FILED, which is what this walk's consumer needs.
            //
            // `identity_of` asks by POSITION and so cannot name a page filed in a bucket the
            // lookup does not record it under -- and that page is exactly the subject of
            // `validate_bucket_ownership_index_from_entries`, which these rows feed. Dropping it
            // would hand that check an empty set and have it report a damaged index as healthy.
            // So: the position index first, for a deleted page's tombstone row as well, then the
            // handle index for a live page the lookup names somewhere else.
            let beside = identities
                .identity_of(bucket, *handle, block)
                .map(|named| named.object_key)
                .or_else(|| identities.identity_anywhere(*handle).map(|(_, key)| key));
            let Some(beside) = beside else {
                debug_assert!(
                    identities.names_no_position(),
                    "bucket {} holds a page the index cannot name at any position while the \
                     lookup DOES name positions, so this walk is dropping a row outside the one \
                     window that explains it",
                    bucket.routing_bucket
                );
                continue;
            };
            from_index += 1;
            entries.push(LiveBlockEntry {
                object_key: Arc::clone(beside),
                kind: block.model_id,
                // `None`: THIS WALK IS OVER THE INDEX, AND AN INDEX ENTRY NAMES A PAGE.
                //
                // This cloned `page.component`, under a note about avoiding a double allocation
                // through a `String`. There is no name on an entry to clone, and there was no
                // value in it before the field was removed either -- every entry's component was
                // `None`. So a `LiveBlockEntry` produced from the INDEX carries no element name,
                // which is what makes the two derived-view rebuilds below unable to recover a
                // container element from it: see `rebuild_unserialized_model_maps_from_bucket_index`.
                component: None,
                address: block.address.clone(),
                dirty: block.dirty,
                deleted: block.deleted,
                log_backed: block.log_backed(),
                // The key of the map being walked. This walk always knew it; it was iterating
                // `.values()` and throwing it away, which is why five readers downstream had to
                // guess it back out of the object key.
                filed_routing_bucket: *routing_bucket,
                filing_is_known: true,
            });
        }
    }
    note_live_block_scan(from_index);
    // A RELEASED bucket holds no block entries, and every caller of this walk -- the dump
    // manifest, WAL reclaim, compaction, the GC snapshot -- reads "no entries" as "no live
    // blocks". Left alone that is not a cheaper index, it is a block whose backing record may be
    // reclaimed. So the released buckets are supplemented from the model maps, which is the same
    // source `reload_released_bucket` would rebuild them from: what this returns is what the
    // bucket index WOULD say if nothing were released.
    //
    // Exact, not approximate, because a release refuses any bucket holding a block whose KEY does
    // not route to it -- `BucketReleaseRefusal::BlockRoutingMismatch` -- so the bucket computed
    // below is the bucket the released node held the block under, and cannot claim a block for the
    // wrong bucket.
    if !shard.bucket_index.released_buckets.is_empty() {
        let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
        for mut entry in collect_model_live_block_entries(shard) {
            let routing_bucket = block_routing_bucket(
                &entry.object_key,
                start_routing_bucket,
                end_routing_bucket,
            );
            if shard.bucket_index.released_buckets.contains(&routing_bucket) {
                // The filing is known exactly here too, and a supplemented entry must not read as
                // "not filed": the five readers downstream would then fall back to a hash over the
                // WHOLE keyspace and name a bucket a narrow shard does not hold.
                entry.filed_routing_bucket = routing_bucket;
                entry.filing_is_known = true;
                entries.push(entry);
            }
        }
    }
    entries
}

/// The identity a released block is compared by, so a release can prove it is reversible.
///
/// `object_id` is not part of it, and no longer COULD be: an address does not carry one. The reason
/// it was excluded while it did is worth keeping, because it is why removing the field was safe
/// here -- the bucket index stamped an id into the address it filed while the model map's copy of
/// the same block did not, so comparing on it would have refused every release over a difference
/// reload reproduces on its own. Two stored copies that could disagree became one derivation that
/// cannot, which is the same conclusion arrived at for free.
type ReleasedBlockIdentity = (String, String, Option<String>, u64, u64, u64, Option<u64>, Option<u64>);

fn released_block_identity(
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
    address: &ElementEntry,
) -> ReleasedBlockIdentity {
    released_block_identity_owned(
        model_id.to_string(),
        object_key.to_string(),
        component.map(str::to_string),
        address,
    )
}

/// The same identity, for a caller that already owns its strings.
///
/// The derivation below is handed owned keys by the walk and would otherwise copy each one a
/// second time to hand it to the borrowing form. One definition of the tuple, so the resident side
/// and the derived side cannot drift into comparing differently-ordered fields.
fn released_block_identity_owned(
    model_id: String,
    object_key: String,
    component: Option<String>,
    address: &ElementEntry,
) -> ReleasedBlockIdentity {
    (
        model_id,
        object_key,
        component,
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id(),
        address.generation(),
    )
}

/// Model kinds a bucket may be released while holding.
///
/// An ALLOW-list, not a deny-list, and deliberately short. Two independent things have to be true
/// of a kind before a bucket holding it can lose its block entries, and both are properties of the
/// kind rather than of the bucket:
///
///   1. ITS MAP MUST SURVIVE SERIALIZATION. `context_events` and `context_indexes` are
///      `skip_serializing` on `ShardState` and are rebuilt FROM the bucket index on load, so a
///      released bucket of one of those kinds would have nothing to rebuild from the moment the
///      index was written and read back. `hashes` WAS in that list and is not any more -- it is
///      durable now -- so this term no longer holds it out. Term 2 does, and it is the only thing
///      holding it out, which is worth knowing before anyone reads this list as settled.
///   2. A READ MUST STILL RESOLVE IT. `bucket_index_block_address` -- the slow read path -- looks
///      an address up THROUGH the bucket index, so a released block has to be findable in its model
///      map by `(kind, object_key, component)` alone. `model_map_block_address` is that lookup, and
///      it is a point lookup, not a scan. Kinds whose objects span components or timestamps
///      (`set`, `zset`, `list`, `feature`, the context series) are also read whole through
///      `bucket_index_component_block_addresses`, which has no equivalent point lookup, so they
///      stay out until one exists.
///
/// That leaves the two component-less, single-block, serialized kinds -- which is also where the
/// index cost being reclaimed actually is: one block and one node per key.
fn released_model_kind_is_addressable(kind: &str) -> bool {
    matches!(kind, "string" | "context_node")
}

/// The address of a block held by a RELEASED bucket, from the model map the block lives in.
///
/// The counterpart to `bucket_index_block_address`: same question, asked of the maps instead of the
/// index. Only the kinds `released_model_kind_is_addressable` admits are answerable here, and that
/// is not a coincidence -- it is the same list, for this reason.
/// IS THIS ELEMENT RESIDENT -- asked of the map that is keyed BY THE ELEMENT.
///
/// # WHO NEEDS IT AND WHY NOTHING ELSE WILL DO
///
/// Both rebuilds carry an object's TOMBSTONE entries across a rebuild they cannot re-derive, and
/// each has to decide whether a tombstone's element has since been written back -- a tombstone
/// restated over a live element would undo a re-add the store has already accepted. They used to
/// answer it by comparing the tombstone's name against the LIVE ENTRIES' names. A live entry does
/// not name its element, so that comparison answers `Some(x) == None` for every pair and the test
/// is dead: it never fires, and a rebuild restates every tombstone it ever held.
///
/// The resident maps DO name elements, and they are what both rebuilds derive their live entries
/// FROM -- so this is the same source those rebuilds already trust, read one element at a time
/// rather than in bulk.
///
/// # THE SPELLINGS ARE THE WRITE PATH'S, AND AN UNREADABLE ONE ANSWERS FALSE
///
/// `hex::encode` for a set member and for a zset member, base sixteen for a list's biased
/// sequence, the field itself for a hash -- each the same rendering the write path files. A name
/// this cannot parse answers FALSE rather than defaulting to element zero or to the empty member:
/// an unreadable name names nothing, which is the rule every arm of the derived view already
/// follows, and the direction that errs towards keeping a tombstone rather than dropping one.
pub(super) fn resident_holds_component(
    shard: &ShardState,
    model_id: StoredModelKind,
    object_key: &str,
    component: &str,
) -> bool {
    // THROUGH THE ONE SPELLING TABLE. This had its own four-arm decode; `container_block_ordinal`
    // needed the same four arms for a different question, and two copies of a decode is how two
    // readers come to disagree about what a component means. `state::resident_component_address`
    // is the single table, and the value it finds -- an address -- is what the other callers want
    // anyway.
    crate::engine::state::resident_component_address(
        shard,
        model_id.as_str(),
        object_key,
        component,
    )
    .is_some()
}

pub(super) fn model_map_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<ElementEntry> {
    match (model_id, component) {
        ("string", None) => shard.string_address(object_key),
        ("context_node", None) => shard.context_nodes().get(object_key).cloned(),
        _ => None,
    }
}

/// The same, but only when the block really does belong to a released bucket.
///
/// The guard matters: without it this would answer for a block whose bucket is resident and whose
/// index entry is absent for some other reason -- which is a disagreement the promote reconcile
/// exists to find and repair, not one to paper over on the read path.
pub(super) fn released_bucket_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<ElementEntry> {
    if shard.bucket_index.released_buckets.is_empty() {
        return None;
    }
    let address = model_map_block_address(shard, model_id, object_key, component)?;
    // THE CONTAINER'S ABSENCE IS THE CONDITION; THE BUCKET ID IS STILL KNOWN. This asks whether a
    // SPECIFIC bucket is released, and which bucket that is comes from the key, not from the block:
    // `released_buckets` is keyed by bucket id and the key's bucket is what a release recorded.
    // The address used to carry it and an address carrying none returned `None` here, which was a
    // second answer to a question the key already answers.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket =
        block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    shard
        .bucket_index
        .released_buckets
        .contains(&routing_bucket)
        .then_some(address)
}

/// The model kinds a released bucket may hold, in the form the delete path needs them.
///
/// `released_model_kind_is_addressable` answers the question one kind at a time, which is what the
/// read path asks. A whole-object delete names a key and no kind at all, so settling the released
/// side of it means asking each releasable kind whether this key is one of its blocks. Same list,
/// same reasons, stated once.
pub(super) const RELEASABLE_MODEL_KINDS: [&str; 2] = ["string", "context_node"];

/// Drop a deleted object's id from its RELEASED bucket's object index.
///
/// `release_bucket_blocks` empties `block_index` and KEEPS `object_index`; the keeping is the only
/// thing that tells a released bucket from one legitimately holding nothing, and since
/// `classify_bucket_layout` was corrected the object count is the sole authority for whether a
/// bucket is empty at all. Both delete paths remove an object by walking `block_index` -- which a
/// release has already emptied -- so a delete arriving while the bucket is released dropped the
/// block from the model map and left the id claimed. The node then reported an object that no
/// longer existed, and reported it as LIVE: the resident path tombstones what it removes in
/// `deleted_object_index`, and none of that ran either.
///
/// `reload_released_bucket` re-derives the set from the model maps and settles it -- but only
/// whenever a reload happens, and the point of a release is that one may not for a long time. So
/// this is that same re-derivation, for the one id, performed at the delete. It must run BEFORE
/// the model map entry goes, which is the order every delete path already uses, because the map
/// is where the block's address -- and with it the routing bucket and the object id -- is read.
///
/// TWO THINGS ARE DELIBERATELY NOT DONE HERE.
///
///   * The node is left in the map when its last object goes. `reload_released_bucket` removes a
///     node it finds no blocks for, and bringing that removal forward would take a bucket out of
///     the reclaim plan's view earlier than anything has asked for. Leaving it costs one node and
///     the bucket now reports `empty`, which is true.
///   * No tombstone is written. The resident path keeps the id and records it in
///     `deleted_object_index` because the id stays; here the id goes, and a tombstone for an
///     absent id is an entry nothing would ever read. A reload derives no tombstone either, which
///     is the state this is bringing forward.
pub(super) fn settle_released_bucket_object_delete(
    shard: &mut ShardState,
    object_key: &str,
) -> bool {
    if shard.bucket_index.released_buckets.is_empty() {
        return false;
    }
    // NO SHARD, NO DERIVATION. A state that never entered the engine has no id to derive with, and
    // there is no id that is safe to guess here: a zero would name a member of a different shard.
    let Some(shard_id) = shard.shard_id() else {
        return false;
    };
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let mut settled = false;
    for model_id in RELEASABLE_MODEL_KINDS {
        // Answers only for a block whose bucket really is released -- a resident bucket with a
        // missing index entry is a disagreement for the promote reconcile, not for a delete.
        let Some(address) = released_bucket_block_address(shard, model_id, object_key, None) else {
            continue;
        };
        // The bucket is the KEY's bucket -- `released_bucket_block_address` above answered for
        // exactly that bucket, so asking again here gets the same number.
        //
        // THE ID IS NOW DERIVED RATHER THAN SKIPPED, and that IS the behaviour it always had. The
        // skip existed because an address could carry no object id; an address carries none at all
        // now, so skipping unconditionally would mean never removing a member -- which is a
        // behaviour change, where deriving is not. Every address this path saw was written by a
        // write path that put the derived id on it, so the derivation answers what the field held.
        let routing_bucket =
            block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
        let object_id = stable_block_object_id(shard_id, model_id, object_key);
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        if bucket.object_index.remove(&object_id) {
            classify_bucket_layout_in_place(bucket);
            settled = true;
        }
    }
    settled
}

/// WHICH precondition a refusal failed, at the granularity of the term that decided it.
///
/// `refused_buckets` is one number for eleven different answers, and that is not enough to
/// assert on: a test that pins it to 1 passes when the WRONG term fired, so every term was
/// verifiable only by however pure the scenario a test happened to build was. Each term counts
/// itself here, and each has a guard that fails on its own number.
///
/// Refusal is attributed to the FIRST failing term, in the order the function tests them. A
/// bucket that is both dirty and deleted is one refusal, counted as dirty, and the totals stay
/// equal to `refused_buckets` -- which is the invariant `refusals_total_matches_refused_buckets`
/// pins.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BucketReleaseRefusals {
    /// Not resident, so there is no resident block list to drop.
    pub(super) not_in_memory: usize,
    /// A load is in flight and the block list is not settled.
    pub(super) loading: usize,
    /// The bucket still pins the log. Releasing it while `eviction_dump_before_evict` is false
    /// would strand undumped writes with nothing to rebuild them from.
    pub(super) bucket_dirty: usize,
    /// Deleted: its blocks are not a set a reload should rebuild.
    pub(super) bucket_deleted: usize,
    /// Nothing resident to release.
    pub(super) empty_block_index: usize,
    /// A block carries an unwritten change the model maps do not record.
    pub(super) block_dirty: usize,
    /// A block is delete-marked, which the model maps do not record either.
    pub(super) block_deleted: usize,
    /// A block's address does not name THIS bucket, so which entries are this bucket's could
    /// only be answered by a hash fallback.
    pub(super) block_routing_mismatch: usize,
    /// A block's kind is not one `released_model_kind_is_addressable` admits.
    pub(super) block_kind_not_addressable: usize,
    /// A block's lookup refs are held by some other bucket.
    pub(super) lookup_not_local: usize,
    /// The model maps would not rebuild what is resident.
    pub(super) model_map_disagreement: usize,
}

impl BucketReleaseRefusals {
    /// Every refusal counted, whatever the reason. Equal to `refused_buckets` by construction.
    pub(super) fn total(&self) -> usize {
        self.not_in_memory
            + self.loading
            + self.bucket_dirty
            + self.bucket_deleted
            + self.empty_block_index
            + self.block_dirty
            + self.block_deleted
            + self.block_routing_mismatch
            + self.block_kind_not_addressable
            + self.lookup_not_local
            + self.model_map_disagreement
    }
}

/// The term a refusal failed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BucketReleaseRefusal {
    NotInMemory,
    Loading,
    BucketDirty,
    BucketDeleted,
    EmptyBlockIndex,
    BlockDirty,
    BlockDeleted,
    BlockRoutingMismatch,
    BlockKindNotAddressable,
    LookupNotLocal,
    ModelMapDisagreement,
}

/// What one call to [`release_bucket_blocks`] managed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BucketReleaseOutcome {
    pub(super) released_buckets: Vec<u32>,
    pub(super) released_blocks: usize,
    /// Candidates that failed a precondition. A release that quietly did nothing and a release
    /// that was refused are different answers, and the eviction report publishes both.
    pub(super) refused_buckets: usize,
    /// The same number, broken out by the term that decided it.
    pub(super) refusals: BucketReleaseRefusals,
}

impl BucketReleaseOutcome {
    /// Record one refusal against the term that caused it.
    fn refuse(&mut self, reason: BucketReleaseRefusal) {
        self.refused_buckets = self.refused_buckets.saturating_add(1);
        let counter = match reason {
            BucketReleaseRefusal::NotInMemory => &mut self.refusals.not_in_memory,
            BucketReleaseRefusal::Loading => &mut self.refusals.loading,
            BucketReleaseRefusal::BucketDirty => &mut self.refusals.bucket_dirty,
            BucketReleaseRefusal::BucketDeleted => &mut self.refusals.bucket_deleted,
            BucketReleaseRefusal::EmptyBlockIndex => &mut self.refusals.empty_block_index,
            BucketReleaseRefusal::BlockDirty => &mut self.refusals.block_dirty,
            BucketReleaseRefusal::BlockDeleted => &mut self.refusals.block_deleted,
            BucketReleaseRefusal::BlockRoutingMismatch => {
                &mut self.refusals.block_routing_mismatch
            }
            BucketReleaseRefusal::BlockKindNotAddressable => {
                &mut self.refusals.block_kind_not_addressable
            }
            BucketReleaseRefusal::LookupNotLocal => &mut self.refusals.lookup_not_local,
            BucketReleaseRefusal::ModelMapDisagreement => {
                &mut self.refusals.model_map_disagreement
            }
        };
        *counter = counter.saturating_add(1);
    }
}

/// Dump-and-release: drop the named buckets' resident block lists, keeping the nodes routable.
///
/// This is the half that was missing. `evict_cache` drops CACHED BLOCKS and leaves every
/// `BucketNode` whole, so eviction could free only what the cache held and the index -- the part
/// that actually grows one entry per record -- was untouchable. Releasing a bucket frees its
/// `block_index` and its lookup refs while the node, its `object_index` and its durable watermarks
/// stay, so the next read through `reload_released_bucket` rebuilds exactly what was dropped.
///
/// Every precondition is CHECKED here, against this shard's live state, rather than assumed from
/// how the caller chose its candidates. See the residency-flag doc on `BucketNode` for why each
/// one is needed.
pub(super) fn release_bucket_blocks(
    shard: &mut ShardState,
    candidates: &[u32],
) -> BucketReleaseOutcome {
    let mut outcome = BucketReleaseOutcome::default();
    if candidates.is_empty() {
        return outcome;
    }
    let wanted: BTreeSet<u32> = candidates.iter().copied().collect();
    // One model-map walk for the whole batch, not one per bucket. This is the set a reload would
    // rebuild from, so comparing the resident blocks against it is the proof the release is
    // reversible.
    // WHAT A RELOAD WOULD REBUILD for each wanted bucket, and nothing else. Comparing the
    // resident blocks against it is the proof the release is reversible.
    //
    // This walks the model maps and it has to: the maps are keyed by object, the routing bucket
    // is a field of the ADDRESS, and no index runs the other way. What the walk no longer does is
    // build an owned entry for every live block in the store before throwing all but the wanted
    // ones away -- four allocations per object in the store, to release four buckets.
    //
    // THE REMAINING COST IS THE VISIT, AND IT IS PAID ONLY WHEN A CANDIDATE ASKS FOR IT. #1884
    // removed what this pass ALLOCATES; the pass still runs `accept` on every live address in the
    // shard, which is 200,000 field reads at 200,000 records to answer a question about at most
    // `batch_limit` buckets, and no allocation table can see it because the walk is borrow-only.
    // Every term tested below the derivation -- the five residency terms, the four per-block
    // terms, and the lookup-locality term -- reads the CANDIDATE's own state and nothing the
    // derivation produces, so a candidate that fails one of them never consults the derived map.
    // At the shipped `eviction_dump_before_evict` false a freshly written bucket is dirty and
    // every candidate is refused on `bucket_dirty`, so the whole-store pass was run and then
    // entirely discarded, once per round, for ever.
    //
    // WHAT IS NOT CHANGED, and the reason this is a memoization rather than a reorder: the terms
    // keep their order relative to one another, the derivation still covers the WHOLE candidate
    // set in one pass, and it is still computed before any bucket is mutated -- the first
    // candidate that reaches the comparison has not released anything yet, and a release touches
    // only `bucket_index`, which the derivation does not read. So the map built here is the map
    // that was built eagerly, at the same shard state, and every outcome field is identical.
    // `refuse` is a set of counters with no ordering, and `released_buckets` is still pushed in
    // candidate order.
    let mut derived: Option<BTreeMap<u32, BTreeSet<ReleasedBlockIdentity>>> = None;
    let lookup_established = !shard.bucket_index.object_block_lookup.is_empty();
    // The shard's own range, for the per-block routing term below.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    // Iterated by reference rather than consumed, because the derivation below is handed the same
    // set and is now reached from inside the loop. Same order, same elements, no allocation.
    for routing_bucket in wanted.iter().copied() {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        // The residency terms, tested in order so a refusal can name the one that decided it.
        // Same set, same order, same answer as the `||` chain this replaces -- what is new is
        // that the outcome can say WHICH, which is what makes one guard per term possible.
        let residency_refusal = if !bucket.in_memory() {
            Some(BucketReleaseRefusal::NotInMemory)
        } else if bucket.loading() {
            Some(BucketReleaseRefusal::Loading)
        } else if bucket.dirty() {
            Some(BucketReleaseRefusal::BucketDirty)
        } else if bucket.deleted() {
            Some(BucketReleaseRefusal::BucketDeleted)
        } else if bucket.block_index.is_empty() {
            Some(BucketReleaseRefusal::EmptyBlockIndex)
        } else {
            None
        };
        if let Some(reason) = residency_refusal {
            outcome.refuse(reason);
            continue;
        }
        // The per-block terms, likewise. `find_map` stops at the first block that fails, which
        // is the same set as the `all` it replaces: `all` is false exactly when `find_map` is
        // Some.
        // THE IDENTITY OF EVERY PAGE IN THIS BUCKET, TAKEN ONCE FOR THE FOUR READERS BELOW.
        //
        // All four asked each page which object it belonged to. The projection is OWNED, so it
        // outlives the immutable borrow of the bucket and is still in hand when the un-filing
        // further down takes the index mutably.
        let names = crate::engine::state::block_identities_in(
            bucket,
            &crate::engine::state::BlockKeysByPosition::from_lookup(
                &shard.bucket_index.object_block_lookup,
            ),
        );
        let block_refusal = bucket.block_index.iter().find_map(|(handle, block)| {
            let Some(beside) = names.get(handle) else {
                // A PAGE THE INDEX CANNOT NAME REFUSES THE RELEASE, and `LookupNotLocal` is
                // exactly what that is: the lookup holds no entry for this page, so dropping this
                // bucket's lookup entries cannot be shown to drop only its own.
                return Some(BucketReleaseRefusal::LookupNotLocal);
            };
            if block.dirty {
                Some(BucketReleaseRefusal::BlockDirty)
            } else if block.deleted {
                Some(BucketReleaseRefusal::BlockDeleted)
            } else if block_routing_bucket(
                beside,
                start_routing_bucket,
                end_routing_bucket,
            ) != routing_bucket
            {
                // STILL A CHECK THAT CAN FAIL, and it now compares two INDEPENDENT things rather
                // than a block's copy of its bucket against the bucket holding it. Where the block
                // IS (the key of the map being walked) against where its KEY routes: a block filed
                // under a stale range, or moved by hand, fails here. The old form compared a field
                // the filing site had just written against the filing site's own key.
                Some(BucketReleaseRefusal::BlockRoutingMismatch)
            } else if !released_model_kind_is_addressable(block.model_id.as_str()) {
                Some(BucketReleaseRefusal::BlockKindNotAddressable)
            } else {
                None
            }
        });
        if let Some(reason) = block_refusal {
            outcome.refuse(reason);
            continue;
        }
        // The lookup refs for each block must point at THIS bucket, or dropping the block's lookup
        // entry below would also drop a ref some other bucket still owns.
        let lookup_is_local = !lookup_established
            || bucket.block_index.iter().all(|(handle, block)| {
                names.get(handle).is_some_and(|beside| {
                    shard
                        .bucket_index
                        // `None`: the lookup files every page of an object under one nameless
                        // slot, which is the slot an entry has always been filed in here.
                        .block_refs_for(block.model_id.as_str(), beside, None)
                        .map(|refs| {
                            refs.iter().all(|block_ref| block_ref.routing_bucket == routing_bucket)
                        })
                        .unwrap_or(false)
                })
            });
        if !lookup_is_local {
            outcome.refuse(BucketReleaseRefusal::LookupNotLocal);
            continue;
        }
        let resident: BTreeSet<ReleasedBlockIdentity> = bucket
            .block_index
            .iter()
            .filter_map(|(handle, block)| {
                // A PAGE NOTHING NAMES CONTRIBUTES NO IDENTITY, which makes the resident set
                // smaller than the derived one and refuses the release below. That is the safe
                // direction: releasing across a disagreement loses blocks.
                names.get(handle).map(|beside| {
                    released_block_identity(
                        block.model_id.as_str(),
                        beside,
                        // `None`: the identity an entry can state is its page's, not its
                        // element's.
                        None,
                        &block.address,
                    )
                })
            })
            .collect();
        // THE WHOLE-STORE PASS, on first demand and once. Nothing above this line consulted it.
        let derived = derived.get_or_insert_with(|| {
            BUCKET_RELEASE_MODEL_DERIVATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            derive_released_block_identities(shard, &wanted)
        });
        if derived.get(&routing_bucket) != Some(&resident) {
            // The model maps would not rebuild what is resident. Whatever the disagreement is,
            // it is not this function's to resolve -- and releasing across it would lose blocks.
            outcome.refuse(BucketReleaseRefusal::ModelMapDisagreement);
            continue;
        }
        let dropped: Vec<(StoredModelKind, Arc<str>)> = bucket
            .block_index
            .iter()
            .filter_map(|(handle, block)| {
                names.get(handle).map(|beside| (block.model_id, Arc::clone(beside)))
            })
            .collect();
        let block_count = dropped.len();
        if lookup_established {
            for (model_id, object_key) in &dropped {
                shard.bucket_index.remove_object_block_lookup_entry(
                    model_id.as_str(),
                    object_key,
                    // `None` -- the slot the lookup filed every page of this object under. The
                    // triple this walked used to carry `page.component` as its third term and the
                    // term was `None` for every entry, so the un-filing it drives does not move.
                    None,
                );
            }
        }
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("bucket read immutably above");
        bucket.block_index = crate::engine::state::BlockIndexMap::Empty;
        bucket.set_meta_loaded(true);
        bucket.set_loading(false);
        bucket.set_in_memory(false);
        // `object_index` is deliberately kept: it is what keeps the bucket countable and is the
        // only thing distinguishing a released bucket from one that legitimately holds nothing.
        bucket.layout = classify_bucket_layout(bucket.object_index.object_count(), 0);
        shard.bucket_index.released_buckets.insert(routing_bucket);
        outcome.released_buckets.push(routing_bucket);
        outcome.released_blocks = outcome.released_blocks.saturating_add(block_count);
    }
    outcome
}

/// Load a released bucket's block list back, from the maps a read already resolves through.
///
/// The mirror of [`release_bucket_blocks`], and the reason releasing is safe. Returns false when
/// the bucket is not released -- an already-resident bucket is a no-op, not an error, which is
/// what lets every mutation site call this unconditionally.
pub(super) fn reload_released_bucket(
    shard: &mut ShardState,
    shard_id: ShardId,
    routing_bucket: u32,
) -> bool {
    if !shard.bucket_index.released_buckets.contains(&routing_bucket) {
        return false;
    }
    if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
        // Held across the derivation below. Under the shard write lock nothing can observe it
        // today; it is the state a queued concurrent loader would wait on if this ever became
        // asynchronous, and setting it is what makes that a wiring change rather than a design.
        bucket.set_loading(true);
    } else {
        shard.bucket_index.released_buckets.remove(&routing_bucket);
        return false;
    }
    // THE IDENTITY TRAVELS WITH THE PAGE, in the tuple, because the model-map entry it is read
    // off is the only place it exists on this path and the page is about to stop carrying one.
    let mut blocks: Vec<(Arc<str>, BlockIndex, u64)> = Vec::new();
    // ONE bucket's blocks, filtered inside the walk. This used to materialize every live block in
    // the shard and then drop all but this bucket's -- a batch reload over N released buckets
    // would have paid the whole store N times. The walk now filters by bucket, so one reload
    // costs one bucket's blocks.
    for entry in collect_model_live_block_entries_in_bucket(shard, routing_bucket) {
        let object_id = expected_live_block_object_id(shard_id, &entry);
        let address = entry.address;
        blocks.push((
            Arc::clone(&entry.object_key),
            BlockIndex {
                model_id: entry.kind,
                address,
                // The model maps carry no per-block dirty/deleted bit, which is exactly why
                // release refuses a bucket holding either. Reloaded blocks are clean and live,
                // which is the state they were released in.
                dirty: false,
                deleted: false,
            },
            object_id,
        ));
    }
    let bucket = shard
        .bucket_index
        .bucket_map
        .get_mut(&routing_bucket)
        .expect("bucket present: checked above and the shard is locked");
    let mut installed: Vec<(u64, Arc<str>, BlockIndex)> = Vec::with_capacity(blocks.len());
    for (object_key, block, object_id) in blocks {
        bucket.object_index.insert(object_id);
        // NOT charged. `release_bucket_blocks` did not discharge these -- the blocks stayed live
        // the whole time it held them out of the index -- so counting them here would double
        // every released bucket the first time anything touched it again.
        let handle = bucket.insert_released_block(&object_key, block.clone());
        installed.push((handle, object_key, block));
    }
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(!bucket.block_index.is_empty());
    bucket.set_loading(false);
    if bucket.block_index.is_empty() {
        // Everything the bucket held was deleted while it was released. Nothing routes here any
        // more, so the node goes rather than lingering with a stale object index.
        shard.bucket_index.bucket_map.remove(&routing_bucket);
        shard.bucket_index.released_buckets.remove(&routing_bucket);
        return true;
    }
    // THE LIVE SET FROM WHAT WAS JUST INSTALLED, which is every page this bucket now holds: the
    // release emptied the page index, so `installed` is not a subset of it but the whole of it. The
    // identity is in hand here, and the object lookup has nothing to say yet -- the refiling in the
    // loop below is what puts these pages back into it.
    let live_object_ids: BTreeSet<u64> = installed
        .iter()
        .filter(|(_, _, block)| !block.deleted)
        .map(|(_, object_key, block)| {
            crate::engine::hashing::stable_block_object_id(
                shard_id,
                block.model_id.as_str(),
                object_key,
            )
        })
        .collect();
    update_bucket_layout_with_live_objects(bucket, &live_object_ids);
    for (handle, object_key, block) in installed {
        shard
            .bucket_index
            .insert_object_block_lookup(routing_bucket, handle, &object_key, &block);
    }
    shard.bucket_index.released_buckets.remove(&routing_bucket);
    note_bucket_flags_stale(shard, routing_bucket);
    true
}

/// The node-only floor: one `BucketNode` width per resident bucket.
///
/// ONE AUTHORITY, BECAUSE THERE ARE TWO READERS AND THEY MUST NOT DRIFT. `persistence` publishes
/// this as `bucket_index_resident_bytes_floor` and [`bucket_index_resident_bytes`] uses it as its
/// own first term; both used to spell `bucket_map.len() * size_of::<BucketNode>()` out for
/// themselves. Two copies of one product agree until one of them is edited, and the failure that
/// produces is the published floor ceasing to be a floor of the published total -- which nothing
/// would have failed on, because each site would still be internally consistent.
/// `the_published_floor_is_the_node_term_of_the_published_total` is the standing assertion that
/// the two are one quantity.
pub(super) fn bucket_index_node_bytes(shard: &ShardState) -> u64 {
    (shard.bucket_index.bucket_map.len() as u64)
        .saturating_mul(std::mem::size_of::<BucketNode>() as u64)
}

/// What the resident bucket index costs: one node per bucket plus what that bucket's block index
/// owns on the heap.
///
/// The published `bucket_index_resident_bytes_floor` counts NODES only, so it cannot move when a
/// bucket is released -- the node stays. The per-block entries are the part that grows with the
/// corpus and the part a release actually frees, so the eviction gate needs this number and not
/// that one.
///
/// THE BLOCK TERM IS ASKED OF THE CONTAINER, NOT COMPUTED FROM A BLOCK COUNT. It was
/// `pages * size_of::<BlockIndex>()`, and measured against the counting allocator that is right on
/// one of the three arms and wrong on the one the shipped routing range puts every bucket into:
/// 0.6817 of what the allocator held at 4,000 records on `0..1023` and 0.8542 at 40,000, because
/// a `Many` arm's element is the eight-byte-wider PAIR and its buffer is owned at CAPACITY. The
/// arithmetic now lives on `BlockIndexMap::resident_heap_bytes`, beside the arms, where each term
/// is the width of the type that arm holds. `the_resident_index_report_is_measured_against_the_allocator`
/// measures this against the allocator at two corpus sizes, both routing ranges and both block
/// populations, and `the_resident_index_report_reconstructs_from_the_widths_the_containers_declare`
/// fails if a field changes width without this moving.
///
/// WHY IT MATTERS MORE THAN A DASHBOARD ROW. This is a term in the eviction pressure score
/// `storage_manager_cycle` reads. A figure that reads low is an engine holding an index it should
/// have released.
///
/// WHAT IT STILL CANNOT SEE, STATED RATHER THAN IMPLIED. Four things, and all four push the same
/// way -- this remains a FLOOR:
///
///   1. REQUEST BYTES, NOT CHUNK BYTES. Every term here is what the container asked for.
///      `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, which is a floor above the request and not
///      an equality (#1969: a 104-byte request read 128). Asking the allocator is not something a
///      serving binary can do per figure, and modelling a rounding rule here would make this a
///      claim about the platform's allocator rather than about this engine's containers.
///   2. THE BUCKET MAP'S OWN SPINE. `nodes * size_of::<BucketNode>()` charges the node's WIDTH.
///      The `BTreeMap` that holds it allocates in whole nodes sized for eleven entries whether or
///      not they fill, and that overhead is not here.
///   3. THE OTHER TWO INDEXES ON EVERY NODE. `object_index` and `deleted_object_index` are inside
///      `size_of::<BucketNode>()` as containers, so whatever THEY hold out of line is not counted.
///   4. THE SHARED NAMES, AND THIS ONE IS DELIBERATE. `resident_heap_bytes` says why: measured,
///      this index never owns them, so charging them here would double-count the model maps that
///      do.
pub(super) fn bucket_index_resident_bytes(shard: &ShardState) -> u64 {
    let nodes = shard.bucket_index.bucket_map.len() as u64;
    let mut blocks = 0u64;
    let mut block_heap = 0u64;
    for bucket in shard.bucket_index.bucket_map.values() {
        blocks = blocks.saturating_add(bucket.block_index.len() as u64);
        block_heap = block_heap.saturating_add(bucket.block_index.resident_heap_bytes());
    }
    // One entry per node plus one per resident block, which is what the walk above touched.
    // Charged here rather than at the call sites: the eviction round reaches this twice per
    // round and the lifecycle plan reaches it again, and a charge at any one of those would
    // describe a fraction of the walking that actually happens.
    //
    // UNCHANGED BY THE BLOCK-TERM FIX, ON PURPOSE. Both the old arithmetic and this one visit one
    // node per bucket and read a length that is O(1) on every arm; neither has ever walked the
    // blocks one by one. The charge names the entries the index HOLDS, which this change does not
    // move, so the eviction round's pinned visit counts stay comparable across it.
    BUCKET_INDEX_RESIDENT_BYTES_VISITS
        .fetch_add(nodes.saturating_add(blocks), std::sync::atomic::Ordering::Relaxed);
    bucket_index_node_bytes(shard).saturating_add(block_heap)
}

/// Cheap O(1)-per-map check for whether the shard holds ANY live model-map entry that
/// `collect_model_live_block_entries` would enumerate. Used to avoid latching the phase-1
/// `promote_scan_done` fast-skip flag before the shard has any state to reconcile. Short-circuits
/// on the first non-empty map; never clones.
pub(super) fn shard_has_model_entries(shard: &ShardState) -> bool {
    !shard.strings().is_empty()
        || !shard.hashes.is_empty()
        || !shard.sets.is_empty()
        || !shard.lists.is_empty()
        || !shard.zsets.is_empty()
        || !shard.features().is_empty()
        || !shard.control_state_blocks().is_empty()
        || !shard.context_nodes().is_empty()
        || !shard.context_events().is_empty()
        || !shard.context_indexes().is_empty()
        || !shard.context_audits().is_empty()
        || !shard.context_entities().is_empty()
        || !shard.context_children().is_empty()
        || !shard.context_summaries().is_empty()
        || !shard.context_compressions().is_empty()
}

/// Bring the bucket index up to date for ONE object key, across every context kind.
///
/// A context write does not register its block. The shard rebuilds the whole first-index afterwards
/// instead -- `rebuild_bucket_first_index`, which walks every live block in the store -- and with
/// several context writes per add that was the last term in an add that grows with the corpus.
/// Measured before coalescing: 5 762 400 block visits across 600 adds, per-add cost doubling as the
/// corpus doubled.
///
/// Feature and Sequence writes already maintain the index this way on the write path, and REPLAY
/// already does it for these very kinds (`lifecycle.rs`, via the same `sync_bucket_index_object_blocks`).
/// The context write path was the one that did not.
///
/// The kinds and the maps below mirror `collect_model_live_block_entries` arm for arm, deliberately:
/// maintenance and rebuild then derive from the same source and cannot disagree about which kind a
/// block belongs to. `context_entity` composes its key from the collection key and the entity hash,
/// which is exactly the sort of detail a hand-written command-to-kind mapping gets wrong.
///
/// Returns whether anything was synced, so the caller can fall back to a rebuild for a write this
/// does not cover rather than silently leaving the index stale.
/// Keys `sync_context_blocks_for_object` found nothing for, recorded so they can be named.
///
/// One uncovered key forces a rebuild for the whole write, so what matters is WHICH keys are
/// uncovered, not how many. Reading the command list to guess at them has already been wrong more
/// than once in this area.
#[cfg(test)]
pub mod uncovered_maintenance {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    pub(super) static UNCOVERED_MAINTENANCE_KEYS: Mutex<Option<BTreeSet<String>>> =
        Mutex::new(None);

    pub(super) fn note(object_key: &str) {
        let mut guard = UNCOVERED_MAINTENANCE_KEYS.lock().expect("uncovered key tally poisoned");
        guard.get_or_insert_with(BTreeSet::new).insert(object_key.to_string());
    }

    pub fn reset() {
        *UNCOVERED_MAINTENANCE_KEYS.lock().expect("uncovered key tally poisoned") =
            Some(BTreeSet::new());
    }

    pub fn snapshot() -> Vec<String> {
        UNCOVERED_MAINTENANCE_KEYS
            .lock()
            .expect("uncovered key tally poisoned")
            .as_ref()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// Sync only the components a command actually wrote.
///
/// [`sync_context_blocks_for_object`] is given an object key and nothing else, so it re-upserts
/// EVERY field that object holds. For a record hash carrying one field per record that is one
/// upsert per stored record on every write, and each upsert removes and reinserts an entry in the
/// object's component vector -- two `Vec` shifts whose tails are the whole object. The cost is
/// quadratic in the object's field count.
///
/// Measured on a production-corpus one-box at a 260 MB store: a single-message ingest cost 40.1 s
/// of datanode CPU out of a 52 s wall, and a DWARF-unwound profile put 60.9% of engine CPU in
/// memmove under `remove_object_block_lookup_entry`, reached from here. With this path taken the
/// same ingest cost 0.13 s of datanode CPU.
///
/// The batch path already knows the exact `(kind, object_key, component)` a command wrote --
/// `command_upsert_components` computes it and the index-log delta has been built from it all
/// along. Given that list the maintenance is one upsert per WRITTEN component instead of one per
/// STORED component.
///
/// Returns false if any component's address is not in the shard maps; the caller then runs the
/// whole-object sync exactly as before, so a shape this does not model costs a fallback rather
/// than a stale index.
pub(super) fn sync_blocks_for_written_components(
    shard: &mut ShardState,
    shard_id: ShardId,
    components: &[(&'static str, String, Option<String>)],
) -> bool {
    if components.is_empty() {
        return false;
    }
    for (kind, object_key, component) in components {
        // Read the address back from the map the write just updated, exactly as
        // `collect_upsert_index_items` does, so the block filed here is the block a reload serves.
        let address = match (*kind, component.as_deref()) {
            ("hash", Some(field)) => shard
                .hashes
                .get(object_key)
                .and_then(|fields| fields.get(field))
                .cloned(),
            ("string", None) => shard.string_address(object_key.as_str()),
            _ => None,
        };
        let Some(address) = address else {
            return false;
        };
        // dirty: true, stage: false -- the flags the whole-object sync uses for a hash field: the
        // write staged its own outcome already and a second would have replay install the same
        // block twice.
        upsert_bucket_index_block_with(
            shard,
            shard_id,
            kind,
            object_key,
            component.clone(),
            address,
            true,
            false,
        );
    }
    true
}

pub(super) fn sync_context_blocks_for_object(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
) -> bool {
    // Read every kind's live addresses first, so the shared borrow ends before the sync below
    // takes a mutable one.
    let mut groups: Vec<(&'static str, String, Vec<ElementEntry>)> = Vec::new();

    if let Some(address) = shard.context_nodes().get(object_key) {
        groups.push(("context_node", object_key.to_string(), vec![address.clone()]));
    }
    // THE EVENT SERIES IS NO LONGER THE SAME TYPE AS THE OTHER FIVE, so it cannot ride in the
    // array below -- an array needs one element type, and this coupling is exactly the "all six or
    // none" the series trait was introduced to break. Lifted out, with the SAME selection rule.
    //
    // THE RULE IS `max_by` OVER THE ADDRESS ORDERING AND NOT THE LAST INSTANT. Those are different
    // answers: the loop below takes the greatest (slab, offset, length), which is what the
    // deduplicated sorted list it replaced ended on, and the greatest INSTANT is a different
    // address whenever a later point was written to an earlier slab. Written as `next_back()`
    // first, which is the ordering this representation makes convenient rather than the one the
    // caller needs.
    // THE NEWEST ADDRESS A SERIES HOLDS, over whichever representation holds it.
    //
    // Three callers below want exactly this and they no longer agree on the container:
    // `context_events` and `context_indexes` hold the owned sorted array, the four maps after them
    // still hold a B-tree a key. Taken through the shared trait, so the answer cannot drift between
    // the one written twice and the one written once.
    //
    // ONLY THE NEWEST BLOCK IS FILED, which is what makes a maximum sufficient: the caller pops the
    // last address and drops the rest, because these kinds carry no component and the index holds
    // one ref per object. Collecting every address the series ever held, into a HashSet and then a
    // sorted Vec, made a write carry the node's whole history -- 8,322 bytes to add a summary to a
    // node holding 20, 78,626 to add one to a node holding 320, 90.7% of the write and still
    // climbing. The maximum equals the last element of that deduplicated sorted list, so this is
    // the same address by the same ordering, without the set or the sort.
    fn newest_filed_address<S>(series: &S) -> Option<ElementEntry>
    where
        S: super::timestamped_series::TimestampedSeries,
    {
        series
            .addresses()
            .max_by(|left, right| {
                left.block_slab_id()
                    .cmp(&right.block_slab_id())
                    .then(left.offset().cmp(&right.offset()))
                    .then(left.length().cmp(&right.length()))
            })
            .cloned()
    }

    if let Some(newest) = shard
        .context_events()
        .get(object_key)
        .and_then(newest_filed_address)
    {
        groups.push(("context_event", object_key.to_string(), vec![newest]));
    }
    // `context_indexes` IS NOT IN THE LITERAL BELOW, because it no longer shares a type with the
    // four that are. Pulled out rather than converted at the call, so this files from the array it
    // holds instead of building a B-tree to read once and drop -- the same split `context_events`
    // above already has, for the same reason.
    if let Some(newest) = shard
        .context_indexes()
        .get(object_key)
        .and_then(newest_filed_address)
    {
        groups.push(("context_index", object_key.to_string(), vec![newest]));
    }
    // `context_children` IS NOT IN THE LITERAL ANY MORE, for the reason `context_events` and
    // `context_indexes` are not: it lives in the consolidated map now, so its value type is
    // `ObjectSlot` and the literal's members must share one. Pulled out rather than converted at
    // the call, so this files from the slot it holds.
    if let Some(newest) = shard
        .context_children()
        .get(object_key)
        .and_then(newest_filed_address)
    {
        groups.push(("context_child", object_key.to_string(), vec![newest]));
    }
    for (kind, series) in [
        ("context_audit", shard.context_audits()),
        ("context_summary", shard.context_summaries()),
        ("context_compression", shard.context_compressions()),
    ] {
        if let Some(newest) = series.get(object_key).and_then(newest_filed_address) {
            groups.push((kind, object_key.to_string(), vec![newest]));
        }
    }
    // Entities live grouped by node but index one entry per entity, under the composed key.
    if let Some(series) = shard.context_entities().get(object_key) {
        for (entity_hash, address) in series.points() {
            groups.push((
                "context_entity",
                format!("{object_key}:{entity_hash}"),
                vec![address.clone()],
            ));
        }
    } else if let Some((collection_key, entity_hash)) = split_context_entity_key(object_key) {
        // A PERSISTED per-entity key names one entity inside a node's collection, and that is
        // what an entity write reports as its object key. The index groups entities under the
        // COLLECTION key, so the lookup above finds nothing, the object reports as uncovered,
        // and the caller rebuilds the whole bucket index -- on every entity upsert. Measured at
        // 4.27 MB per upsert into a 3,200-entity store, 99.8% of the write, all of it the
        // rebuild and its flag refresh.
        //
        // File the ONE block this key names. The composed key above is
        // `{collection_key}:{entity_hash}`, which IS this key, so both paths file the same shape.
        if let Some(address) = shard
            .context_entities()
            .get(&collection_key[..])
            .and_then(|series| series.get(entity_hash))
        {
            groups.push((
                "context_entity",
                object_key.to_string(),
                vec![address.clone()],
            ));
        }
    }

    // A context node's block lives in `shard.hashes` under a single field, so the rebuild derives
    // it as kind "hash" with that field as the component -- a different shape from the kinds
    // above, which carry no component. It is filed here the same way the rebuild would file it.
    //
    // Only the fields whose block is not already filed. This ran on every write and re-filed
    // EVERY field of the object each time -- cloning each field name to do it -- so writing a
    // hash cost work proportional to the fields it already had: 800 allocations per write at 100
    // fields, 8,388 at 1,600. Filtering before the clone makes the ordinary case, where the write
    // path already registered its own block, cost nothing here.
    //
    // `had_hash_blocks` still asks whether the object HAS hash blocks, not how many needed filing.
    // Those differ once the filter can empty the list, and answering the second question would
    // report an already-synced object as uncovered -- which sends the caller into a full rebuild.
    let had_hash_blocks = shard
        .hashes
        .get(object_key)
        .is_some_and(|fields| !fields.is_empty());
    let hash_fields: Vec<(super::hash_field_map::FieldName, ElementEntry)> = shard
        .hashes
        .get(object_key)
        .map(|fields| {
            fields
                .iter()
                .filter(|(field, address)| {
                    !shard.bucket_index.contains_object_block_address(
                        "hash",
                        object_key,
                        Some(&field[..]),
                        address,
                    )
                })
                .map(|(field, address)| (field.clone(), address.clone()))
                .collect()
        })
        .unwrap_or_default();
    for (field, address) in hash_fields {
        // `stage: false` -- the write staged its own outcome under its own kind already, and a
        // second one would have replay install the same block twice.
        upsert_bucket_index_block_with(
            shard,
            shard_id,
            "hash",
            object_key,
            // The COMPONENT position is still an owned name; that one belongs to the
            // bucket index and is converted separately.
            Some(field.to_string()),
            address,
            true,
            false,
        );
    }

    if groups.is_empty() && !had_hash_blocks {
        #[cfg(test)]
        uncovered_maintenance::note(object_key);
        return false;
    }
    for (kind, key, mut live) in groups {
        // File the newest block, not every block the object has ever had.
        //
        // These kinds carry no component, so all of an object's blocks file under the same
        // (kind, key, None). `upsert_bucket_index_block_with` drops that entry's existing refs
        // before inserting, so filing a list leaves only its last element -- the other entries
        // are removed again on the way past. The index holds ONE ref per object here either way;
        // this reaches it without the removals and inserts in between.
        //
        // That is worth stating plainly because the list is the object's whole series and this
        // runs on every write: a node holding 850 events re-filed 850 blocks to add its 851st, so
        // filling a node cost the square of its length -- 2,072 allocations per message at 50
        // events, 23,822 at 800.
        //
        // Whether the index SHOULD hold every block of a series rather than the newest is a
        // separate question. It holds one today, and this keeps that.
        if live.len() > 1 {
            let newest = live.pop().expect("length checked");
            live.clear();
            live.push(newest);
        }
        sync_bucket_index_object_blocks(shard, shard_id, kind, &key, live, true);
    }
    true
}

/// THE MODEL KINDS A LIVE-BLOCK WALK CAN EMIT, AND THE BYTE EACH ONE PACKS AS, DECLARED ONCE.
///
/// WHAT THIS REPLACES, AND THE TWO SETS THAT WERE NEVER COMPARED. `storage_model_code` in
/// `storage_reporting.rs` hand-listed fifteen `&str` arms beside the walk below, which emits a
/// DIFFERENT fifteen. Both differences mattered, and in opposite ways:
///
///   * `zset` and `list` ARE emitted by the arms below and had no entry, so both fell through the
///     list's `_ => 0` arm. Zero is also what that arm handed a model id the reporting path
///     cannot name at all, so the packed block-index byte could not tell a zset block from a block
///     whose kind the engine does not recognise. Three different answers behind one byte.
///   * `sequence` and `context_embedding` had entries and are NOT emitted -- and those two are
///     not drift. Both are RETIRED spellings, and a byte code any binary has ever written keeps
///     its meaning here whether or not a live structure still spells the kind: Sequence rows live
///     in `features` under a typed row codec, and `restore_model_maps_from_bucket_index` says in
///     as many words that `context_embedding` entries from pre-retirement indexes fall through to
///     its ignore arm. A report has to be able to NAME what it finds rather than calling it
///     unknown. They are declared as retired rather than deleted, each with its reason beside it,
///     because an exemption list whose entries are not justified is a hiding place.
///
/// HOW THE SET IS CLOSED AT COMPILE TIME, which is the part a table beside a walk cannot do.
/// [`visit_model_live_blocks`] hands `emit` a `ModelKind`, not a `&str`. So an arm added to the
/// walk cannot name a kind this declaration does not have -- that is a type error, not a missing
/// table row -- and each variant's report code comes off the same declaration as the variant, so
/// a kind that exists has a code by construction. Model ids are `&'static str` literals and this
/// is the only place they are written, which is what makes the set genuinely closed rather than
