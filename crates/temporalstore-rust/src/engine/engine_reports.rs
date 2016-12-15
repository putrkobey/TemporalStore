// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::block_store::{
    BlockStoreSlabReport, BlockStoreStats, BlockStoreSlabDescriptor, BlockStoreSlabSummary,
};
use crate::storage_config::{
    StorageTuningConfig, TS_BLOCK_SLAB_TARGET_BYTES, TS_COLD_SCAN_NO_CACHE_FILL,
    TS_COMPACTION_WATERMARK_BYTES, TS_CONTEXT_BLOCK_TARGET_BYTES, TS_STREAM_MAX_BLOB_SIZE,
};
use crate::types::{ShardId, Status};
use matrixcache::{CacheEntryInfo, CacheStats};

fn public_storage_strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// The most recent field the two sources of a hash read disagreed about.
///
/// One sample and not a list: the point of the surface is the COUNT, and an unbounded list of
/// every divergent field is a memory cost paid by a production read path to hold evidence of a
/// condition whose first example is as diagnostic as its thousandth. The log carries the rest,
/// under the rate limit `hash_read_divergence` documents.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashReadDivergenceSample {
    pub object_key: String,
    pub field: String,
}

/// WHAT THE SERVED HASH WHOLE-OBJECT READ SAW OF ITS TWO SOURCES, PROCESS-WIDE.
///
/// `Command::HashGetAll` resolves field names from the durable container `shard.hashes`, still
/// consults the page index, and serves the UNION of the two. This is what it observed while doing
/// so.
///
/// READ `reads_served` FIRST. It is the denominator, and without it
/// `index_named_fields_the_container_lacked` is unreadable: zero divergences over zero reads is
/// not a clean bill of health, it is an absence of evidence.
///
/// WHAT A ZERO MEANS, AND WHAT IT DOES NOT. Zero means NO DIVERGENCE ON THE ROUTES THE TRAFFIC
/// TOOK. IT IS NOT PROOF OF COMPLETENESS ON THE ROUTES IT DID NOT TAKE. A field installed by a
/// write path that no read ever asked for is never sampled by this counter and cannot be, because
/// the counter only sees reads. Anyone who reads a zero here as "the container holds every field
/// the fold installs" has made the same substitution #2078 made when it read a fixture's green as
/// a statement about the engine, and a correction comment on #2078 says so.
///
/// WHY THERE IS NO `shard_id` ON IT. The figures are a property of the read path and are
/// accumulated process-wide; a shard label would describe something this report did not measure.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashReadDivergenceReport {
    /// THE DENOMINATOR. Whole-object hash reads that got as far as consulting both sources --
    /// reads turned away by the expiry door are not counted, because they consulted neither.
    pub reads_served: u64,
    /// THE COUNT. Fields the PAGE INDEX named that the durable container did NOT hold, summed
    /// per (read, field). Only this direction: it is the one that loses a read once the index
    /// stops being consulted. The opposite direction is served from the container by the union
    /// and is not a loss.
    pub index_named_fields_the_container_lacked: u64,
    /// How many of those reached the log, which is rate-limited. Reported so a quiet log cannot
    /// be mistaken for a zero count.
    pub divergences_logged: u64,
    #[serde(default)]
    pub last_divergence: Option<HashReadDivergenceSample>,
}

/// WHAT THE BUCKET MAINTENANCE SCAN SAW OF THE PAGES AND THE OBJECT LIST.
///
/// The scan used to ASSIGN the list from the pages, so a list that disagreed was overwritten and
/// the disagreement was unobservable. It reconciles now, and these are the figures that makes the
/// claim "the list already matches a from-scratch recompute" something a reader can check rather
/// than something a replaced value implied.
///
/// A ZERO IS NOT A PROOF OF AGREEMENT unless `rebuilds` is non-zero: it is a statement about the
/// buckets the scan reached. Read the count beside its denominator, which is why they are one type.
///
/// WHY THERE IS NO `shard_id`. The figures accumulate process-wide, as the hash-read divergence
/// figures do; a shard label would describe something this report did not measure.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndexDivergenceReport {
    /// THE DENOMINATOR: buckets whose pages were scanned and compared against their object list.
    pub rebuilds: u64,
    /// Buckets whose list held nothing and was derived from the pages -- a load or recovery path,
    /// not a disagreement. Counted apart so neither can be read as the other.
    pub reconstructs: u64,
    /// A live page named an object its bucket's list did not hold. The list gains it; nothing
    /// already filed moves.
    #[serde(rename = "blocks_named_an_object_the_list_lacked", alias = "pages_named_an_object_the_list_lacked")]
    pub blocks_named_an_object_the_list_lacked: u64,
    /// The list held an object no live page names. The list drops it, leaving a placeholder.
    #[serde(rename = "list_held_an_object_no_block_names", alias = "list_held_an_object_no_page_names")]
    pub list_held_an_object_no_block_names: u64,
    /// How many of those reached the log, which is rate-limited, so a quiet log cannot be mistaken
    /// for a zero count.
    pub divergences_logged: u64,
    /// Stored slots that named an id another slot in the same array already named. The first slot
    /// wins; the rest become placeholders so nothing above them renumbers.
    pub stored_slots_repeated_an_id: u64,
    #[serde(default)]
    pub last_divergence: Option<ObjectIndexDivergenceSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndexDivergenceSample {
    /// Which direction disagreed, in words, because an id alone does not say.
    pub what: String,
    pub object_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StorageContractValue {
    Bool(bool),
    U64(u64),
    String(String),
    StringList(Vec<String>),
    StringMap(BTreeMap<String, String>),
    U64Map(BTreeMap<String, u64>),
}

fn contract_bool(value: bool) -> StorageContractValue {
    StorageContractValue::Bool(value)
}

fn contract_u64(value: u64) -> StorageContractValue {
    StorageContractValue::U64(value)
}

fn contract_text(value: &str) -> StorageContractValue {
    StorageContractValue::String(value.to_string())
}

fn contract_string_list(values: &[&str]) -> StorageContractValue {
    StorageContractValue::StringList(public_storage_strings(values))
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub model_layout_compaction_ready: bool,
    #[serde(default)]
    pub model_layout_compaction_evidence: Vec<String>,
    #[serde(default)]
    pub model_layout_compaction_blockers: Vec<String>,
    pub previous_block_slab_id: u64,
    pub compacted_block_slab_id: u64,
    pub rewritten_block_refs: usize,
    /// Bytes this round copied onto the fresh slab, verbatim.
    ///
    /// The round's real cost, and the figure that says how much a round on a node leading
    /// nothing would have thrown away. `rewritten_block_refs` beside it counts the relocations;
    /// this counts what they moved, and two rounds with the same ref count can differ by orders
    /// of magnitude here.
    #[serde(default)]
    pub relocated_bytes: u64,
    /// Pages this round left where they were because it spent a budget -- bytes OR page refs,
    /// whichever ran out first. In practice it is the ref budget: the byte one is 256 MiB and a
    /// store that large is rare, while the ref budget is sized to bound the shard write lock.
    ///
    /// Non-zero means the round did NOT finish, and the next one continues filling the same slab
    /// rather than rolling a new one. Zero means compaction completed inside one round, which is
    /// what every store smaller than a round's budget does.
    #[serde(default)]
    pub blocks_left_by_budget: usize,
    #[serde(default)]
    pub bytes_left_by_budget: u64,
    /// Pages this round left where they were because emptying their slab was not asked for.
    ///
    /// Zero for a DIRECT compaction, which relocates every live page by instruction. Non-zero for
    /// the PERIODIC one, which is told which slabs it is draining and leaves the rest alone --
    /// relocating a page off a slab that carries no dead space recovers nothing, because the page
    /// is copied verbatim and the slab it left becomes the one carrying the dead space.
    ///
    /// Unlike `pages_left_by_budget` this does NOT mean the round is unfinished: a later round
    /// will not want these pages either.
    #[serde(default)]
    pub blocks_left_off_drain_set: usize,
    #[serde(default)]
    pub cold_block_rewrite_refs: usize,
    #[serde(default)]
    pub object_block_pack_group_count: usize,
    pub stale_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub reclaimable_stale_block_slab_count: usize,
    #[serde(default)]
    pub model_policy_family_count: usize,
    #[serde(default)]
    #[serde(rename = "tombstone_policy_model_count")]
    pub delete_marker_policy_model_count: usize,
    #[serde(default)]
    pub stale_density_policy_model_count: usize,
    #[serde(default)]
    pub layout_aware_policy_model_count: usize,
    #[serde(default)]
    pub rewritten_object_blocks: usize,
    #[serde(default)]
    pub bucket_layout_transition_count: u64,
    #[serde(default)]
    pub bucket_layout_states_after: Vec<BucketLayoutStateCount>,
    #[serde(default)]
    #[serde(rename = "tombstoned_object_ids_before")]
    pub delete_marked_object_ids_before: u64,
    #[serde(default)]
    #[serde(rename = "tombstoned_object_ids_after")]
    pub delete_marked_object_ids_after: u64,
    #[serde(default)]
    pub model_layouts: Vec<ShardCompactionModelLayoutReport>,
    #[serde(default)]
    pub before: ShardCompactionUtilityReport,
    #[serde(default)]
    pub after: ShardCompactionUtilityReport,
    #[serde(default)]
    pub model_rewrite_policies: Vec<ModelCompactionRewriteReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketLayoutStateCount {
    pub state: String,
    pub object_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionModelLayoutReport {
    pub kind: String,
    pub object_count: usize,
    pub index_refs: usize,
    pub unique_block_refs: usize,
    pub packed_timestamped_blocks: usize,
    pub legacy_value_blocks: usize,
    pub stale_block_estimate: u64,
    pub live_ref_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionUtilityReport {
    pub live_block_slab_count: usize,
    pub total_block_count: u64,
    pub live_block_refs: u64,
    pub stale_block_estimate: u64,
    pub live_ref_density_basis_points: u64,
    #[serde(default)]
    pub model_policies: Vec<ModelCompactionPolicyReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompactionPolicyReport {
    pub model_id: String,
    pub layout_policy: String,
    pub live_block_refs: u64,
    pub deleted_block_refs: u64,
    pub total_slab_blocks: u64,
    pub stale_block_estimate: u64,
    pub stale_density_basis_points: u64,
    #[serde(rename = "tombstone_density_basis_points")]
    pub delete_marker_density_basis_points: u64,
    #[serde(default)]
    pub object_block_packing_enabled: bool,
    #[serde(default)]
    pub object_block_pack_group_count: u64,
    #[serde(default)]
    pub cold_block_rewrite_eligible_refs: u64,
    #[serde(default)]
    pub compaction_action: String,
    #[serde(default)]
    pub stale_density_triggered: bool,
    #[serde(default)]
    #[serde(rename = "tombstone_compaction_triggered")]
    pub delete_marker_compaction_triggered: bool,
    #[serde(default)]
    pub layout_aware_rewrite_required: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompactionRewriteReport {
    pub model_id: String,
    pub layout_policy: String,
    pub rewritten_block_refs: usize,
    pub cold_block_rewrite_refs: usize,
    pub object_block_pack_group_count: usize,
    #[serde(rename = "tombstone_density_basis_points")]
    pub delete_marker_density_basis_points: u64,
    pub stale_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepReport {
    pub shard_id: ShardId,
    pub expired_records_removed: usize,
    #[serde(default)]
    pub hot_buckets_scanned: usize,
    #[serde(default)]
    pub cold_buckets_scanned: usize,
    #[serde(default)]
    pub scanned_records: usize,
    #[serde(default)]
    pub skipped_records: usize,
    #[serde(default)]
    pub loaded_for_expire: usize,
    #[serde(default)]
    pub next_hot_cursor: Option<String>,
    #[serde(default)]
    pub next_cold_cursor: Option<String>,
    #[serde(default)]
    pub round_limit: usize,
    #[serde(default)]
    pub load_on_expire_only_when_needed: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub hot_cursor: Option<String>,
    #[serde(default)]
    pub cold_cursor: Option<String>,
    #[serde(default)]
    pub max_hot_buckets_per_round: usize,
    #[serde(default)]
    pub max_cold_buckets_per_round: usize,
    #[serde(default)]
    pub load_cold_buckets: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustStorageObservation {
    pub shard_id: ShardId,
    pub cache: CacheStats,
    #[serde(alias = "page_store")]
    pub block_store: BlockStoreStats,
    pub observed_memory_hit: bool,
    pub observed_block_cache_hit: bool,
    pub observed_local_file_read: bool,
    pub observed_cache_invalidation: bool,
    pub observed_memory_eviction: bool,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub local_block_bytes_written: u64,
    pub local_block_bytes_read: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryReport {
    pub shard_id: ShardId,
    pub index_bytes: u64,
    #[serde(default)]
    pub index_write_atomic: bool,
    #[serde(rename = "wal_records")]
    pub wal_records: usize,
    pub index_log_records: usize,
    pub active_block_slab_ids: Vec<u64>,
    pub live_block_slab_ids: Vec<u64>,
    pub slab_descriptors: Vec<BlockStoreSlabDescriptor>,
    #[serde(default)]
    pub slab_summary: BlockStoreSlabSummary,
    #[serde(default)]
    pub block_slab_reports: Vec<BlockStoreSlabReport>,
    #[serde(default)]
    pub block_slab_live_reports: Vec<StorageRecoverySlabLiveReport>,
    pub total_block_refs: usize,
    pub readable_block_refs: usize,
    /// How many live pages this call actually READ, as opposed to tallied.
    ///
    /// `readable_page_refs` counts the ones that read back, and `all_live_pages_readable`
    /// compares the two -- so without this number a reader cannot tell a call that read
    /// everything from a call that read a sample, and "all readable" means different things in
    /// the two cases. Equal to `total_page_refs` for an unbounded call.
    #[serde(default)]
    pub probed_block_refs: usize,
    /// The index into this shard's live-page vector where this call STARTED reading.
    ///
    /// A bounded probe reads a window rather than the whole shard, and successive rounds move
    /// the window along and wrap. Reporting where the window sat is what lets a reader tell
    /// "this round covered a different part of the store" from "this round repeated the last
    /// one" -- the second is what the probe used to do, silently. Always 0 for an unbounded
    /// call, which starts at the beginning and reads everything.
    #[serde(default)]
    pub readable_probe_cursor: usize,
    #[serde(alias = "unreadable_page_refs", default)]
    pub unreadable_block_refs: Vec<StorageRecoveryBlockError>,
    #[serde(default)]
    pub owner_mismatch_block_refs: Vec<StorageRecoveryBlockOwnerMismatch>,
    #[serde(default)]
    pub missing_owner_block_refs: usize,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
    pub all_live_blocks_readable: bool,
    #[serde(default)]
    pub boundary: StorageRecoveryBoundaryReport,
    #[serde(default)]
    #[serde(alias = "segment_integrity")]
    pub slab_integrity: StorageSlabIntegrityReport,
    #[serde(rename = "feature_block_layout", default)]
    pub feature_block_layout: StorageFeatureBlockLayoutReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryBlockError {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryBlockOwnerMismatch {
    pub object_key: String,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub expected_object_id: u64,
    pub actual_object_id: Option<u64>,
    pub expected_routing_bucket: u32,
    pub actual_routing_bucket: Option<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeatureBlockLayoutReport {
    #[serde(default)]
    pub indexed_timestamped_points: usize,
    #[serde(default)]
    pub unique_timestamped_block_refs: usize,
    #[serde(default)]
    pub packed_timestamped_blocks: usize,
    #[serde(default)]
    pub legacy_timestamped_value_blocks: usize,
    #[serde(default)]
    pub families: Vec<StorageTimestampedBlockFamilyReport>,
    pub indexed_feature_points: usize,
    pub unique_feature_block_refs: usize,
    pub packed_feature_blocks: usize,
    pub legacy_feature_value_blocks: usize,
    #[serde(default)]
    pub corrupt_packed_feature_blocks: Vec<StorageFeatureBlockError>,
    #[serde(default)]
    pub missing_indexed_timestamps: Vec<StorageFeatureBlockTimestampMismatch>,
    #[serde(default)]
    pub orphan_packed_timestamps: Vec<StorageFeatureBlockTimestampMismatch>,
    #[serde(default)]
    pub duplicate_packed_timestamps: Vec<StorageFeatureBlockTimestampMismatch>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTimestampedBlockFamilyReport {
    pub kind: String,
    pub indexed_points: usize,
    pub unique_block_refs: usize,
    pub packed_blocks: usize,
    pub legacy_value_blocks: usize,
    pub corrupt_blocks: usize,
    pub mismatch_count: usize,
}

impl StorageFeatureBlockLayoutReport {
    pub(crate) fn has_errors(&self) -> bool {
        !self.corrupt_packed_feature_blocks.is_empty()
            || !self.missing_indexed_timestamps.is_empty()
            || !self.orphan_packed_timestamps.is_empty()
            || !self.duplicate_packed_timestamps.is_empty()
    }

    pub(crate) fn mismatch_count(&self) -> usize {
        self.missing_indexed_timestamps.len()
            + self.orphan_packed_timestamps.len()
            + self.duplicate_packed_timestamps.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeatureBlockError {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    pub key: String,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeatureBlockTimestampMismatch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    pub key: String,
    pub timestamp_ms: u64,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObjectLifecycleReport {
    pub live_object_ids: u64,
    pub live_block_refs: u64,
    pub stale_object_ids: u64,
    #[serde(rename = "tombstoned_object_ids")]
    pub delete_marked_object_ids: u64,
    pub reused_object_id_conflicts: u64,
    pub missing_owner_block_refs: u64,
    pub owner_mismatch_block_refs: u64,
    #[serde(default)]
    pub reused_object_ids: Vec<u64>,
    #[serde(default)]
    #[serde(rename = "tombstoned_object_keys")]
    pub delete_marked_object_keys: Vec<String>,
}

impl StorageObjectLifecycleReport {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoverySlabLiveReport {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub block_count: u64,
    pub live_block_refs: u64,
    pub readable_live_block_refs: u64,
    pub unreadable_live_block_refs: u64,
    pub stale_block_estimate: u64,
    pub live_physical_bytes: u64,
    pub live_logical_bytes: u64,
    pub live_object_count: u64,
    pub live_routing_bucket_count: u64,
    pub live_ref_density_basis_points: u64,
}


/// What a drift check found between the MAINTAINED per-slab live tally and the walk.
///
/// Produced by `reconcile_block_slab_live`, which corrects the maintained tally from the walk and
/// hands this back. It is a REPORT and never a panic: a maintained counter that has drifted is a
/// counting bug, and turning a counting bug into an outage is a worse trade than serving a figure
/// that was just corrected.
///
/// `slabs_compared` is the DENOMINATOR. A reconcile over a shard with no live pages agrees
/// trivially, and a zero drift is only evidence when something was compared.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockSlabLiveDriftReport {
    /// False when the maintained tally had never been derived, so this reconcile SEEDED it rather
    /// than checked it. A seed is not agreement and must not be counted as any.
    pub was_ready: bool,
    /// Slabs named by either side. The denominator for every count below.
    pub slabs_compared: u64,
    /// Slabs where the two sides disagreed about page refs, bytes, or both.
    pub drifted_slabs: u64,
    /// Maintained minus recomputed, summed over every slab. Signed on purpose: an over-count and
    /// an under-count are different bugs, and a sum of absolute values hides which one happened.
    pub block_ref_drift: i64,
    pub byte_drift: i64,
    /// The slab with the largest absolute byte disagreement, when there was one.
    pub worst_block_slab_id: Option<u64>,
}

impl BlockSlabLiveDriftReport {
    pub fn is_clean(&self) -> bool {
        self.drifted_slabs == 0 && self.block_ref_drift == 0 && self.byte_drift == 0
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlabIntegrityReport {
    pub shard_id: ShardId,
    pub indexed_block_slab_count: usize,
    pub discovered_block_slab_count: usize,
    pub live_block_slab_count: usize,
    pub orphan_block_slab_count: usize,
    pub stale_block_ref_count: usize,
    pub corrupt_block_slab_count: usize,
    pub unreadable_block_ref_count: usize,
    pub unreadable_block_bytes: u64,
    pub owner_mismatch_block_ref_count: usize,
    pub missing_owner_block_ref_count: usize,
    pub reclaim_required: bool,
    pub integrity_ok: bool,
}

/// One summary a bucket, and a dump manifest carries one per bucket.
///
/// The short aliases below were the first half of a two-step change: every reader accepts them,
/// and this still WRITES the long names by default. A dump manifest is read by other engines
/// during install, so a writer that emitted short names unconditionally would hand a document to
/// a reader that has never heard of them -- and these fields carry no serde default, so that
/// reader fails rather than degrades.
///
/// Measured on a 4,000-bucket manifest: the field names are 592,046 bytes of a 1,367,875-byte
/// document -- 43% of it, once the whitespace came out.
///
/// The second step has been taken and the switch that guarded it is gone: the writer emits the
/// short names, which is worth 43% of the document.
///
/// Reading never depended on that switch and still does not. Both spellings deserialize, so a
/// directory holding manifests written either way -- which every fleet that has run both has --
/// loads end to end. What no longer exists is a way to ASK for the long spelling: producing a
/// manifest for a reader older than the release that taught the aliases now means running such a
/// build, not setting a variable on this one.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
pub struct BucketStorageSummary {
    #[serde(rename = "routing_slot", alias = "rs")]
    pub routing_bucket: u32,
    #[serde(alias = "oc")]
    pub object_count: u64,
    #[serde(alias = "prc")]
    pub block_ref_count: u64,
    #[serde(alias = "lb")]
    pub logical_bytes: u64,
    #[serde(alias = "pb")]
    pub physical_bytes: u64,
    #[serde(alias = "doc")]
    pub dirty_object_count: u64,
    #[serde(alias = "dg")]
    pub dirty_generation: u64,
    #[serde(alias = "lds")]
    pub last_dump_sequence: u64,
    #[serde(default)]
    #[serde(alias = "psi")]
    pub block_slab_ids: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "lcz")]
    pub last_compacted_slab: Option<u64>,
}

/// Whether the manifest writer spells its fields short. **On by default.**
///
/// Shipped opt-in first, because turning it on writes a document an engine older than the
/// release that taught readers the short names cannot load -- those fields carry no serde
/// default, so such a reader fails rather than degrades. Which engines are deployed is the one
/// fact this code cannot check for itself, so the flip was left as an operator decision, and it
/// has now been taken.
///
/// What the code CAN check, and does: every reader in this repository accepts both spellings.
/// The aliases are on the struct, the round-trip is asserted in both directions below, and no
/// Python tool reads this document -- the `logical_bytes`/`physical_bytes` names in `tools/`
/// belong to the slab extent manifest, which is a different file with different fields.
///
impl BucketStorageSummary {
    /// Write the summary under one spelling or the other.
    ///
    /// Takes the choice as an argument rather than reading the switch, so a test can exercise
    /// both spellings without touching process environment -- which is shared, and which several
    /// hundred sites in this crate already race over.
    fn serialize_named<S>(&self, serializer: S, short: bool) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut fields = 9;
        if self.last_compacted_slab.is_some() {
            fields += 1;
        }
        let mut out = serializer.serialize_struct("BucketStorageSummary", fields)?;
        out.serialize_field(
            if short { "rs" } else { "routing_slot" },
            &self.routing_bucket,
        )?;
        out.serialize_field(if short { "oc" } else { "object_count" }, &self.object_count)?;
        out.serialize_field(
            if short { "prc" } else { "block_ref_count" },
            &self.block_ref_count,
        )?;
        out.serialize_field(
            if short { "lb" } else { "logical_bytes" },
            &self.logical_bytes,
        )?;
        out.serialize_field(
            if short { "pb" } else { "physical_bytes" },
            &self.physical_bytes,
        )?;
        out.serialize_field(
            if short { "doc" } else { "dirty_object_count" },
            &self.dirty_object_count,
        )?;
        out.serialize_field(
            if short { "dg" } else { "dirty_generation" },
            &self.dirty_generation,
        )?;
        out.serialize_field(
            if short { "lds" } else { "last_dump_sequence" },
            &self.last_dump_sequence,
        )?;
        out.serialize_field(
            if short { "psi" } else { "block_slab_ids" },
            &self.block_slab_ids,
        )?;
        // Absent stays absent: the derived writer skipped this when None, and a manifest that
        // started emitting nulls would be bigger, not smaller.
        match self.last_compacted_slab.as_ref() {
            Some(slab) => {
                out.serialize_field(if short { "lcz" } else { "last_compacted_slab" }, slab)?
            }
            None => out.skip_field(if short { "lcz" } else { "last_compacted_slab" })?,
        }
        out.end()
    }
}

impl serde::Serialize for BucketStorageSummary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.serialize_named(serializer, true)
    }
}

/// A summary written under an explicit spelling, for tests and for anything that must not depend
/// on the process-wide switch.
pub(crate) struct SummaryNamed<'a>(pub(crate) &'a BucketStorageSummary, pub(crate) bool);

impl serde::Serialize for SummaryNamed<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize_named(serializer, self.1)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalBlockIndex {
    pub object_key: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, alias = "page_id", skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(rename = "zone_id", default, skip_serializing_if = "Option::is_none")]
    pub stored_slab_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    pub dirty: bool,
    pub deleted: bool,
    pub log_backed: bool,
    pub native_packed_block_index_len: usize,
    pub native_packed_block_index_hex: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalBucketNode {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub layout: String,
    pub dirty: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub object_count: u64,
    pub block_ref_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub native_packed_bucket_node_len: usize,
    pub native_packed_bucket_node_hex: String,
    #[serde(default)]
    pub block_indexes: Vec<StoragePhysicalBlockIndex>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalIndexReport {
    pub shard_id: ShardId,
    pub bucket_first: bool,
    pub bucket_index_authority: bool,
    #[serde(default)]
    pub secondary_views_reconciled_from_bucket_index: bool,
    pub bucket_count: usize,
    pub block_index_count: usize,
    pub dirty_bucket_count: usize,
    pub missing_object_id_count: usize,
    pub missing_routing_bucket_count: usize,
    pub missing_block_id_count: usize,
    pub missing_checksum_count: usize,
    pub native_packed_block_index_size: usize,
    pub native_packed_bucket_node_size: usize,
    pub native_packed_layout_compatible: bool,
    #[serde(default)]
    pub bucket_nodes: Vec<StoragePhysicalBucketNode>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketObjectBlockOwnershipReport {
    pub shard_id: ShardId,
    pub first_class_index_present: bool,
    pub derived_from_model_maps: bool,
    pub block_ref_count: usize,
    pub missing_owner_block_ref_count: usize,
    pub owner_mismatch_block_ref_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectManagerRuntimeReport {
    pub shard_id: ShardId,
    pub runtime_ready: bool,
    pub routing_bucket_count: u64,
    pub object_count: u64,
    pub block_ref_count: u64,
    pub hot_object_count: u64,
    pub cold_object_count: u64,
    pub mixed_residency_object_count: u64,
    #[serde(rename = "tombstone_object_count")]
    pub delete_marker_object_count: u64,
    pub dirty_object_count: u64,
    pub loading_object_count: u64,
    pub meta_object_count: u64,
    pub ttl_object_count: u64,
    pub dirty_bucket_count: u64,
    pub max_dirty_generation: u64,
    #[serde(default)]
    pub object_block_transition_count: u64,
    pub layout_transition_count: u64,
    #[serde(default)]
    pub layout_states: Vec<BucketLayoutStateCount>,
    pub object_block_count: u64,
    pub packed_timestamped_block_count: u64,
    pub multi_block_object_count: u64,
    pub missing_owner_block_ref_count: usize,
    pub owner_mismatch_block_ref_count: usize,
    pub reused_object_id_conflict_count: u64,
    pub evidence: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifest {
    pub version: u32,
    pub shard_id: ShardId,
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub manifest_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dump_generation_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_manifest_ids: Vec<String>,
    // Self-describing source coverage for a merged dump: each source's
    // (manifest_id, bucket_ids), captured at create time when the sources were
    // validated. Lets a target engine that does not hold the source manifest files
    // preflight source coverage from the manifest itself (falls back to on-disk
    // sources when empty). Rust-native, skip-if-empty so non-merged manifests are
    // byte-for-byte unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_manifest_coverage: Vec<BucketDumpSourceCoverage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_version_handoff: Option<BucketDumpLoadVersionHandoff>,
    pub created_unix_ms: u64,
    pub bucket_ids: Vec<u32>,
    pub block_slab_ids: Vec<u64>,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
    pub live_block_refs: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub bucket_summaries: Vec<BucketStorageSummary>,
    #[serde(
        default,
        skip_serializing_if = "StorageObjectLifecycleReport::is_empty"
    )]
    pub object_lifecycle: StorageObjectLifecycleReport,
    /// The index image itself. Encoded, not written as an array of numbers: the array shape costs
    /// three to four characters per byte, and this field carries the whole image. The reader
    /// accepts either shape, so a manifest written before this still loads.
    #[serde(default, with = "crate::bytes_serde")]
    pub index_bytes: Vec<u8>,
    #[serde(default)]
    pub index_sha256: String,
    pub checksum: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpSourceCoverage {
    pub manifest_id: String,
    pub bucket_ids: Vec<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpLoadVersionHandoff {
    pub previous_load_version: u64,
    pub next_load_version: u64,
    pub applied: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallMarker {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub phase: String,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
    pub created_unix_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallPreflightReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub install_safe: bool,
    pub blockers: Vec<String>,
    #[serde(rename = "current_wal_sequence")]
    pub current_wal_sequence: u64,
    pub current_index_log_sequence: u64,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    pub missing_block_slab_ids: Vec<u64>,
    pub corrupt_block_slab_ids: Vec<u64>,
    pub unreadable_block_ref_count: usize,
    pub unreadable_block_bytes: u64,
    pub stale_manifest: bool,
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_block_conflict_count: usize,
    #[serde(default)]
    pub stale_object_conflicts: Vec<String>,
    #[serde(default)]
    pub stale_block_conflicts: Vec<String>,
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub missing_source_manifest_ids: Vec<String>,
    #[serde(default)]
    pub source_manifest_bucket_ids: Vec<u32>,
    #[serde(default)]
    pub source_bucket_coverage_missing_bucket_ids: Vec<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpMergedInstallReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub source_manifest_ids: Vec<String>,
    pub bucket_ids: Vec<u32>,
    pub preflight: BucketDumpInstallPreflightReport,
    pub rollback_marker_written: bool,
    pub prepare_marker_written: bool,
    pub install_marker_written: bool,
    pub commit_marker_written: bool,
    pub load_version_handoff: Option<BucketDumpLoadVersionHandoff>,
    pub installed: bool,
    pub status_code: String,
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub missing_source_manifest_ids: Vec<String>,
    #[serde(default)]
    pub source_bucket_coverage_missing_bucket_ids: Vec<u32>,
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_block_conflict_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFaultMatrixReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub production_ready_slice: bool,
    pub scenario_count: usize,
    pub passed_count: usize,
    pub failed_scenarios: Vec<BucketDumpFaultScenarioReport>,
    pub scenarios: Vec<BucketDumpFaultScenarioReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFaultScenarioReport {
    pub scenario: String,
    pub passed: bool,
    pub expected_code: String,
    pub actual_code: String,
    pub blockers: Vec<String>,
    pub install_safe: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestChainIssue {
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestPrunePlan {
    pub shard_id: ShardId,
    pub retained_manifest_ids: Vec<String>,
    pub prunable_manifest_ids: Vec<String>,
    pub prunable_marker_manifest_ids: Vec<String>,
    pub blocked_manifest_ids: Vec<String>,
    #[serde(default)]
    pub follower_blocks: Vec<BucketDumpFollowerRetentionBlock>,
    #[serde(default)]
    pub raft_snapshot_blocks: Vec<BucketDumpRaftSnapshotRetentionBlock>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestPruneReport {
    pub shard_id: ShardId,
    pub plan: BucketDumpManifestPrunePlan,
    pub removed_manifest_ids: Vec<String>,
    pub removed_marker_files: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallRollForwardReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub interrupted_phase: String,
    pub can_roll_forward: bool,
    #[serde(default)]
    pub can_retry_install: bool,
    pub completed_commit: bool,
    #[serde(default)]
    pub completed_install: bool,
    #[serde(default)]
    pub obsolete_marker_files_removed: usize,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFollowerReplayCursor {
    pub follower_id: String,
    pub shard_id: ShardId,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpRaftSnapshotRef {
    pub snapshot_id: String,
    pub shard_id: ShardId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFollowerRetentionBlock {
    pub follower_id: String,
    pub manifest_id: String,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "cursor_wal_sequence")]
    pub cursor_wal_sequence: u64,
    pub cursor_index_log_sequence: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpRaftSnapshotRetentionBlock {
    pub snapshot_id: String,
    pub manifest_id: String,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "snapshot_wal_sequence")]
    pub snapshot_wal_sequence: u64,
    pub snapshot_index_log_sequence: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecyclePlan {
    pub shard_id: ShardId,
    #[serde(alias = "dirty_slots")]
    pub dirty_buckets: Vec<u32>,
    pub selected_dump_buckets: Vec<u32>,
    #[serde(rename = "undumped_wal_records", default)]
    pub undumped_wal_records: u64,
    /// The same suffix, counted in OBJECT MUTATIONS rather than in log records.
    ///
    /// Reported beside `undumped_wal_records` and NOT compared against any threshold. The two
    /// disagree by whatever factor the writer batches -- 2,000 against 4 on the same 2,000
    /// objects -- and only this one is the work a restart redoes. Switching
    /// `min_undumped_wal_records` to compare against it is a one-line change that needs a default
    /// expressed in the new unit; it can only ever make the gate fire EARLIER, because a record
    /// carries at least one mutation, so it cannot lengthen the window a restart must replay.
    #[serde(default)]
    pub undumped_wal_objects: u64,
    #[serde(default)]
    pub dump_delayed: bool,
    /// Every bucket's storage summary, or `None` when this round did not look.
    ///
    /// AN EMPTY VECTOR AND "I DID NOT LOOK" ARE DIFFERENT ANSWERS, and before this field became
    /// an `Option` they were the same value. Populating it means walking every live page in the
    /// shard, which the plan did unconditionally because this is an OUTPUT field -- so an idle
    /// shard paid a whole-shard walk every maintenance round to fill a vector that every consumer
    /// reduces to a count or a sum.
    ///
    /// The walk is now conditional (see `storage_lifecycle_plan`), and `None` is what a round
    /// that skipped it reports. It is NOT "the shard has no buckets": a settled shard holding
    /// millions of live pages reports `None` here on every round. Consumers must not paper over
    /// it with `unwrap_or_default()` -- a zero derived that way is indistinguishable from a
    /// measured zero, and publishing one as a metric is how an idle shard comes to look empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_summaries: Option<Vec<BucketStorageSummary>>,
    pub live_block_slab_ids: Vec<u64>,
    pub stale_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub reclaim_candidates: Vec<StorageReclaimCandidate>,
    #[serde(alias = "delayed_destroy_page_segment_ids")]
    pub delayed_destroy_block_slab_ids: Vec<u64>,
    pub reclaimable_physical_bytes: u64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReclaimCandidate {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub stale_physical_bytes: u64,
    pub block_count: u64,
    pub live_block_refs: u64,
    pub stale_block_estimate: u64,
    pub live_ref_density_basis_points: u64,
    pub reclaim_score: u64,
    pub reason: String,
}

/// What a compaction round would relocate, and why, composed from per-object answers.
///
/// `relocatable_block_refs` is the whole decision: zero means no object holds a page on a slab
/// anyone wants emptied, so a round would roll a fresh slab, copy every live page onto it
/// verbatim, and leave the slab it emptied behind as the reason the next round runs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionRelocationHint {
    pub shard_id: ShardId,
    /// Objects the hint was asked about. The denominator: zero here makes every count below
    /// trivially zero for a reason that has nothing to do with compaction.
    pub examined_object_count: u64,
    pub relocatable_object_count: u64,
    pub relocatable_block_refs: u64,
    /// Slabs carrying dead space that objects still hold pages on: compaction's job.
    pub drain_block_slab_ids: Vec<u64>,
    /// Slabs holding nothing but dead space: the collector's job, and nothing compaction can act
    /// on. A round triggered by these is the self-retrigger.
    pub collector_only_block_slab_ids: Vec<u64>,
    pub relocatable_block_refs_by_model: BTreeMap<String, u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockGcReplayCursor {
    pub cursor_id: String,
    pub shard_id: ShardId,
    pub retain_from_block_slab_id: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockGcDependencyBlock {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub dependency: String,
    #[serde(default)]
    pub owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_from_block_slab_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_until_unix_ms: Option<u64>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockGcDependencyPlan {
    pub shard_id: ShardId,
    pub safe_to_reclaim: bool,
    pub candidate_block_slab_ids: Vec<u64>,
    pub reclaimable_block_slab_ids: Vec<u64>,
    pub blocked_block_slab_ids: Vec<u64>,
    pub live_block_slab_ids: Vec<u64>,
    pub manifest_block_slab_ids: Vec<u64>,
    pub shared_store_cursor_count: usize,
    pub checkpoint_snapshot_floor: Option<u64>,
    pub raft_snapshot_install_floor: Option<u64>,
    pub delayed_destroy_grace_ms: u64,
    #[serde(default)]
    pub live_ref_block_count: usize,
    #[serde(default)]
    pub bucket_dump_manifest_block_count: usize,
    #[serde(default)]
    pub shared_store_cursor_block_count: usize,
    #[serde(default)]
    pub raft_snapshot_ref_block_count: usize,
    #[serde(default)]
    pub checkpoint_snapshot_floor_block_count: usize,
    #[serde(default)]
    pub raft_snapshot_install_floor_block_count: usize,
    #[serde(default)]
    pub delayed_destroy_grace_block_count: usize,
    pub dependency_blocks: Vec<StorageBlockGcDependencyBlock>,
    pub blocker_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicStorageContract {
    pub page_address: String,
    pub block_address: String,
    pub page_index_entry: String,
    pub block_index_entry: String,
    pub object_index_entry: String,
    pub storage_zone: String,
    pub stream: String,
    #[serde(alias = "segment")]
    pub slab: String,
    /// NOT folded into [`Self::slab`], deliberately.
    ///
    /// This struct is a CATALOG of the public storage vocabulary, one entry per published name,
    /// and its keys are the wire. `slab` sits right beside it, so folding the two would delete a
    /// published entry rather than rename a field -- a wire change wearing a rename's clothes.
    /// Everywhere else in the crate a band is spelled slab; here the older name is the payload.
    pub band: String,
    #[serde(alias = "slot")]
    pub bucket: String,
    pub append_watermark: String,
    pub compaction_watermark: String,
    #[serde(rename = "tombstone")]
    pub delete_marker: String,
    pub gc_eligibility: String,
    pub follower_cursor_safety: String,
    pub compatibility_aliases: BTreeMap<String, String>,
}

impl Default for PublicStorageContract {
    fn default() -> Self {
        fn text(value: &str) -> String {
            value.to_string()
        }

        let mut compatibility_aliases = BTreeMap::new();
        compatibility_aliases.insert(text("page_store"), text("StorageZone"));
        compatibility_aliases.insert(text("block_store"), text("ElementEntry"));
        compatibility_aliases.insert(text("object_index"), text("ObjectIndexEntry"));
        compatibility_aliases.insert(text("page_segment_id"), text("segment_id"));
        compatibility_aliases.insert(text("wal"), text("AppendWatermark"));
        compatibility_aliases.insert(text("stream_blob"), text("Stream"));

        Self {
            page_address: text("ElementEntry"),
            block_address: text("ElementEntry"),
            page_index_entry: text("PageIndexEntry"),
            block_index_entry: text("BlockIndexEntry"),
            object_index_entry: text("ObjectIndexEntry"),
            storage_zone: text("StorageZone"),
            stream: text("Stream"),
            slab: text("Segment"),
            band: text("Band"),
            bucket: text("Slot"),
            append_watermark: text("AppendWatermark"),
            compaction_watermark: text("CompactionWatermark"),
            delete_marker: text("Tombstone"),
            gc_eligibility: text("GcEligibility"),
            follower_cursor_safety: text("FollowerCursorSafety"),
            compatibility_aliases,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicStorageFeatureShapes {
    pub page_address_fields: Vec<String>,
    pub block_address_fields: Vec<String>,
    pub page_index_entry_fields: Vec<String>,
    pub block_index_entry_fields: Vec<String>,
    pub object_index_entry_fields: Vec<String>,
    pub storage_zone_fields: Vec<String>,
    pub stream_fields: Vec<String>,
    #[serde(alias = "segment_fields")]
    pub slab_fields: Vec<String>,
    /// NOT folded into [`Self::slab_fields`], for the reason on [`PublicStorageContract::band`].
    pub band_fields: Vec<String>,
    #[serde(alias = "slot_fields")]
    pub bucket_fields: Vec<String>,
    pub append_watermark_fields: Vec<String>,
    pub compaction_watermark_fields: Vec<String>,
    #[serde(rename = "tombstone_fields")]
    pub delete_marker_fields: Vec<String>,
    pub gc_eligibility_fields: Vec<String>,
    pub follower_cursor_safety_fields: Vec<String>,
}

impl Default for PublicStorageFeatureShapes {
    fn default() -> Self {
        Self {
            page_address_fields: public_storage_strings(&[
                "shard_id",
                "zone_id",
                "segment_id",
                "page_id",
                "offset",
                "length",
                "generation",
            ]),
            block_address_fields: public_storage_strings(&[
                "shard_id", "zone_id", "block_id", "offset", "length", "checksum",
            ]),
            page_index_entry_fields: public_storage_strings(&[
                "logical_key",
                "timestamp_range",
                "page_addresses",
                "append_watermark",
                "generation",
            ]),
            block_index_entry_fields: public_storage_strings(&[
                "page_address",
                "block_address",
                "band",
                "checksum",
                "generation",
            ]),
            object_index_entry_fields: public_storage_strings(&[
                "model",
                "table",
                "object_key",
                "page_chain",
                "tombstone",
                "generation",
            ]),
            storage_zone_fields: public_storage_strings(&[
                "zone_id",
                "total_bytes",
                "used_bytes",
                "stale_bytes",
                "segments",
            ]),
            stream_fields: public_storage_strings(&[
                "stream_id",
                "segments",
                "rollover_count",
                "sealed_segment_count",
            ]),
            slab_fields: public_storage_strings(&[
                "segment_id",
                "band",
                "start_offset",
                "sealed",
                "generation",
            ]),
            band_fields: public_storage_strings(&[
                "band",
                "block_range",
                "reclaim_state",
                "generation",
            ]),
            bucket_fields: public_storage_strings(&[
                "slot_id",
                "dirty_generation",
                "object_refs",
                "page_refs",
                "tombstones",
                "owner_mismatch_count",
            ]),
            append_watermark_fields: public_storage_strings(&[
                "shard_id",
                "slot_id",
                "log_index",
                "timestamp_ms",
            ]),
            compaction_watermark_fields: public_storage_strings(&[
                "shard_id",
                "safe_generation",
                "safe_timestamp_ms",
                "follower_floor",
            ]),
            delete_marker_fields: public_storage_strings(&[
                "ref",
                "generation",
                "deleted_at_ms",
                "reason",
            ]),
            gc_eligibility_fields: public_storage_strings(&[
                "ref",
                "eligible_after_ms",
                "has_tombstone",
                "follower_safe",
                "reclaimable_bytes",
            ]),
            follower_cursor_safety_fields: public_storage_strings(&[
                "min_follower_cursor",
                "blocked_reclaim_bytes",
                "safe_to_reclaim",
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecycleReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub public_storage_contract: PublicStorageContract,
    #[serde(default)]
    pub public_storage_feature_shapes: PublicStorageFeatureShapes,
    #[serde(default = "effective_storage_tuning_from_env")]
    pub effective_storage_tuning: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_lifecycle_metrics")]
    pub storage_lifecycle_metrics: BTreeMap<String, u64>,
    #[serde(default = "default_storage_write_contract_empty")]
    pub storage_write_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_read_contract_empty")]
    pub storage_read_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cold_scan_contract_empty")]
    pub storage_cold_scan_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_manager_contract_empty")]
    pub storage_manager_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_index_contract_empty")]
    pub storage_index_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cache_contract_empty")]
    pub storage_cache_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_reclaim_contract_empty")]
    pub storage_reclaim_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_safety_snapshot")]
    pub storage_safety_snapshot: StorageSafetySnapshot,
    #[serde(default = "default_storage_watermark_snapshot")]
    pub storage_watermark_snapshot: StorageWatermarkSnapshot,
    #[serde(default = "default_storage_gc_snapshot")]
    pub storage_gc_snapshot: StorageGcSnapshot,
    #[serde(default = "default_storage_index_snapshot")]
    pub storage_index_snapshot: StorageIndexSnapshot,
    #[serde(default = "default_storage_topology_snapshot")]
    pub storage_topology_snapshot: StorageTopologySnapshot,
    #[serde(default = "default_storage_write_sequence")]
    pub storage_write_sequence: Vec<String>,
    #[serde(default = "default_storage_read_sequence")]
    pub storage_read_sequence: Vec<String>,
    #[serde(default = "default_storage_cold_scan_sequence")]
    pub storage_cold_scan_sequence: Vec<String>,
    #[serde(default = "default_storage_lifecycle_phases")]
    pub storage_lifecycle_phases: Vec<String>,
    #[serde(default = "default_storage_cache_layers")]
    pub storage_cache_layers: Vec<String>,
    #[serde(default = "default_storage_cache_semantics")]
    pub storage_cache_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_semantics")]
    pub storage_reclaim_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_scope")]
    pub storage_reclaim_scope: StorageReclaimScope,
    pub plan: StorageLifecyclePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dump_manifest: Option<BucketDumpManifest>,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    #[serde(rename = "cache_warmup_block_refs", default)]
    pub cache_warmup_block_refs: usize,
    #[serde(default)]
    pub cache_warmup: StorageCacheWarmupReport,
    #[serde(alias = "delayed_destroy_purged_segments")]
    pub delayed_destroy_purged_slabs: Vec<u64>,
    pub delayed_destroy_purged_bytes: u64,
    /// Slabs the purge's last-chance re-check found LIVE and put back in the store.
    ///
    /// Reported because it is an alarm an operator has to be able to see: it means the collector
    /// quarantined a slab something still needed, and the re-check is the only reason the bytes
    /// are still here. A round that restores is not a round that reclaimed nothing -- it is a
    /// round that caught a bug, and the bug is upstream of the purge.
    #[serde(default)]
    pub delayed_destroy_restored_slabs: Vec<u64>,
    #[serde(default)]
    pub delayed_destroy_restored_bytes: u64,
    #[serde(default)]
    pub manifest_prune_plan: BucketDumpManifestPrunePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_prune_report: Option<BucketDumpManifestPruneReport>,
    #[serde(default)]
    pub install_roll_forward_reports: Vec<BucketDumpInstallRollForwardReport>,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
}

impl Default for StorageLifecycleReport {
    fn default() -> Self {
        Self {
            shard_id: ShardId::default(),
            public_storage_contract: PublicStorageContract::default(),
            public_storage_feature_shapes: PublicStorageFeatureShapes::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: default_storage_safety_snapshot(),
            storage_watermark_snapshot: default_storage_watermark_snapshot(),
            storage_gc_snapshot: default_storage_gc_snapshot(),
            storage_index_snapshot: default_storage_index_snapshot(),
            storage_topology_snapshot: default_storage_topology_snapshot(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: default_storage_reclaim_scope(),
            plan: StorageLifecyclePlan::default(),
            dump_manifest: None,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            cache_warmup_block_refs: 0,
            cache_warmup: StorageCacheWarmupReport::default(),
            delayed_destroy_purged_slabs: Vec::new(),
            delayed_destroy_purged_bytes: 0,
            delayed_destroy_restored_slabs: Vec::new(),
            delayed_destroy_restored_bytes: 0,
            manifest_prune_plan: BucketDumpManifestPrunePlan::default(),
            manifest_prune_report: None,
            install_roll_forward_reports: Vec::new(),
            object_lifecycle: StorageObjectLifecycleReport::default(),
        }
    }
}

impl StorageLifecycleReport {
    pub fn refresh_public_lifecycle_metrics(&mut self) {
        let mut metrics = default_storage_lifecycle_metrics();
        let plan = &self.plan;
        let object_lifecycle = &self.object_lifecycle;
        let follower_retention_block_count = self.manifest_prune_plan.follower_blocks.len() as u64;

        fn put(metrics: &mut BTreeMap<String, u64>, name: &str, value: u64) {
            metrics.insert(name.to_string(), value);
        }
        fn flag(value: bool) -> u64 {
            if value {
                1
            } else {
                0
            }
        }

        put(&mut metrics, "storage_manager_prepare_count", 1);
        put(&mut metrics, "storage_manager_reclaim_count", 1);
        put(
            &mut metrics,
            "storage_manager_evict_count",
            flag(self.cache_entries_removed > 0),
        );
        put(
            &mut metrics,
            "storage_manager_expire_count",
            flag(object_lifecycle.delete_marked_object_ids > 0),
        );
        put(
            &mut metrics,
            "storage_manager_page_gc_count",
            flag(plan.reclaimable_physical_bytes > 0),
        );
        put(
            &mut metrics,
            "storage_manager_block_gc_count",
            flag(plan.reclaimable_physical_bytes > 0),
        );
        put(
            &mut metrics,
            "storage_manager_compaction_count",
            flag(!plan.reclaim_candidates.is_empty()),
        );
        put(
            &mut metrics,
            "storage_manager_index_gc_count",
            flag(
                !self.manifest_prune_plan.prunable_manifest_ids.is_empty()
                    || !self
                        .manifest_prune_plan
                        .prunable_marker_manifest_ids
                        .is_empty(),
            ),
        );
        put(
            &mut metrics,
            "storage_manager_delayed_destroy_count",
            flag(
                !plan.delayed_destroy_block_slab_ids.is_empty()
                    || !self.delayed_destroy_purged_slabs.is_empty(),
            ),
        );
        put(
            &mut metrics,
            "storage_manager_follower_cursor_safety_count",
            flag(follower_retention_block_count > 0),
        );
        put(&mut metrics, "storage_manager_watermark_progress_count", 1);
        put(
            &mut metrics,
            "segment_open_count",
            plan.selected_dump_buckets.len() as u64,
        );
        put(
            &mut metrics,
            "segment_sealed_count",
            self.delayed_destroy_purged_slabs.len() as u64,
        );
        put(
            &mut metrics,
            "storage_zone_stale_bytes",
            plan.reclaimable_physical_bytes,
        );
        put(
            &mut metrics,
            "storage_zone_count",
            plan.live_block_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "active_storage_zones",
            plan.live_block_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "stream_segment_count",
            plan.live_block_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "slot_tombstone_count",
            object_lifecycle.delete_marked_object_ids,
        );
        put(
            &mut metrics,
            "slot_stale_ref_count",
            object_lifecycle
                .stale_object_ids
                .saturating_add(object_lifecycle.missing_owner_block_refs)
                .saturating_add(object_lifecycle.owner_mismatch_block_refs),
        );
        put(
            &mut metrics,
            "slot_owner_mismatch_count",
            object_lifecycle.owner_mismatch_block_refs,
        );
        // OMITTED, not zeroed, when the round did not walk the live pages. In a metrics map an
        // absent key is "not measured" and a present 0 is "measured none" -- publishing the
        // second for the first is how a settled shard comes to look like an empty one on a graph.
        // `selected_dump_buckets` alone cannot stand in: it is empty on exactly these rounds.
        if let Some(bucket_summaries) = plan.bucket_summaries.as_ref() {
            put(
                &mut metrics,
                "slot_index_entry_count",
                bucket_summaries
                    .len()
                    .max(plan.selected_dump_buckets.len()) as u64,
            );
        }
        put(
            &mut metrics,
            "slot_object_ref_count",
            object_lifecycle.live_object_ids,
        );
        put(
            &mut metrics,
            "slot_page_ref_count",
            object_lifecycle.live_block_refs,
        );
        put(
            &mut metrics,
            "object_index_entry_count",
            object_lifecycle.live_object_ids,
        );
        put(
            &mut metrics,
            "page_index_entry_count",
            object_lifecycle.live_block_refs,
        );
        put(
            &mut metrics,
            "block_index_entry_count",
            object_lifecycle.live_block_refs,
        );
        put(
            &mut metrics,
            "page_address_count",
            object_lifecycle.live_block_refs,
        );
        put(
            &mut metrics,
            "missing_owner_ref_count",
            object_lifecycle.missing_owner_block_refs,
        );
        put(
            &mut metrics,
            "owner_mismatch_count",
            object_lifecycle.owner_mismatch_block_refs,
        );
        put(
            &mut metrics,
            "page_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "block_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "object_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "cache_evictions",
            self.cache_entries_removed as u64,
        );
        put(
            &mut metrics,
            "cache_rehydrates",
            self.cache_warmup_block_refs as u64,
        );
        put(
            &mut metrics,
            "cache_refills",
            self.cache_warmup.warmed_block_refs as u64,
        );
        put(
            &mut metrics,
            "cache_invalidations",
            flag(self.cache_entries_removed > 0 || self.cache_disk_bytes_removed > 0),
        );
        put(
            &mut metrics,
            "cold_scan_no_cache_reads",
            self.cache_warmup.skipped_block_refs as u64,
        );
        put(
            &mut metrics,
            "tombstone_records",
            object_lifecycle.delete_marked_object_ids,
        );
        put(
            &mut metrics,
            "stale_page_tombstones",
            plan.reclaim_candidates
                .iter()
                .filter(|candidate| candidate.reason.contains("stale"))
                .count() as u64,
        );
        put(
            &mut metrics,
            "stale_block_tombstones",
            plan.reclaim_candidates
                .iter()
                .filter(|candidate| candidate.reason.contains("stale"))
                .count() as u64,
        );
        put(
            &mut metrics,
            "stale_pages_skipped",
            follower_retention_block_count,
        );
        put(
            &mut metrics,
            "stale_blocks_skipped",
            follower_retention_block_count,
        );
        put(
            &mut metrics,
            "delayed_destroy_backlog",
            plan.delayed_destroy_block_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "follower_cursor_retention_floor",
            self.manifest_prune_plan
                .follower_blocks
                .iter()
                .map(|block| block.cursor_wal_sequence)
                .min()
                .unwrap_or_default(),
        );
        put(
            &mut metrics,
            "reclaimable_bytes",
            plan.reclaimable_physical_bytes,
        );
        put(
            &mut metrics,
            "compaction_reclaimed_bytes",
            self.delayed_destroy_purged_bytes,
        );
        put(
            &mut metrics,
            "physical_reclaimed_bytes",
            self.delayed_destroy_purged_bytes,
        );

        self.storage_lifecycle_metrics = metrics;
        self.effective_storage_tuning = effective_storage_tuning_from_env();
        self.storage_write_contract =
            default_storage_write_contract(&self.storage_lifecycle_metrics);
        self.storage_read_contract = default_storage_read_contract(&self.storage_lifecycle_metrics);
        self.storage_cold_scan_contract =
            default_storage_cold_scan_contract(&self.storage_lifecycle_metrics);
        self.storage_manager_contract =
            default_storage_manager_contract(&self.storage_lifecycle_metrics);
        self.storage_index_contract =
            default_storage_index_contract(&self.storage_lifecycle_metrics);
        self.storage_cache_contract =
            default_storage_cache_contract(&self.storage_lifecycle_metrics);
        self.storage_reclaim_contract =
            default_storage_reclaim_contract(&self.storage_lifecycle_metrics);
        self.storage_safety_snapshot =
            storage_safety_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_watermark_snapshot =
            storage_watermark_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_gc_snapshot =
            storage_gc_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_index_snapshot =
            storage_index_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_topology_snapshot =
            storage_topology_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_reclaim_scope = default_storage_reclaim_scope();
    }
}

pub fn default_storage_write_sequence() -> Vec<String> {
    public_storage_strings(&[
        "append_record",
        "route_shard_slot",
        "choose_page",
        "append_page_buffer",
        "update_page_index",
        "flush_page_block_segment",
        "update_block_index",
        "publish_append_watermark",
    ])
}

pub fn default_storage_read_sequence() -> Vec<String> {
    public_storage_strings(&[
        "logical_key_timestamp_range",
        "object_page_index_lookup",
        "page_address_list",
        "block_index_lookup",
        "page_read",
        "decode_records",
        "return_filtered_result",
    ])
}

pub fn default_storage_cold_scan_sequence() -> Vec<String> {
    public_storage_strings(&[
        "timestamp_page_index_scan",
        "no_cache_page_read",
        "bounded_decode",
        "no_hot_cache_promotion",
    ])
}

pub fn default_storage_lifecycle_phases() -> Vec<String> {
    public_storage_strings(&[
        "prepare",
        "reclaim",
        "evict",
        "expire",
        "page_gc",
        "block_gc",
        "compaction",
        "index_gc",
        "delayed_destroy",
        "follower_cursor_safety",
        "watermark_progress",
    ])
}

pub fn default_storage_cache_layers() -> Vec<String> {
    public_storage_strings(&[
        "memory_object_cache",
        "page_index_cache",
        "block_index_cache",
        "disk_block_cache",
        "shared_store_read_through",
    ])
}

pub fn default_storage_cache_semantics() -> Vec<String> {
    public_storage_strings(&[
        "lookup_hot_to_cold",
        "refill_from_durable_on_miss",
        "invalidate_on_append_watermark",
        "invalidate_on_compaction_watermark",
        "cold_scan_no_promote",
        "writeback_backpressure_reported",
    ])
}

pub fn default_storage_reclaim_semantics() -> Vec<String> {
    public_storage_strings(&[
        "cache_eviction_memory_only",
        "logical_tombstone_required",
        "stale_pages_blocks_rewritten_or_skipped",
        "reclaimed_bytes_reported",
        "physical_reclaim_errors_zero",
    ])
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReclaimScope {
    pub owner: String,
    pub matrixark_context_gc_role: String,
    pub physical_reclaim_context_specific: bool,
}

impl Default for StorageReclaimScope {
    fn default() -> Self {
        Self {
            owner: "temporalstore_storage_lifecycle".to_string(),
            matrixark_context_gc_role: "marks_logical_raw_event_eligibility_only".to_string(),
            physical_reclaim_context_specific: false,
        }
    }
}

pub fn default_storage_reclaim_scope() -> StorageReclaimScope {
    StorageReclaimScope::default()
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSafetySnapshot {
    pub append_watermark: u64,
    pub compaction_watermark: u64,
    #[serde(rename = "tombstone_records")]
    pub delete_marker_records: u64,
    pub gc_eligible_record_count: u64,
    pub reclaimable_bytes: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_blocked_reclaim_count: u64,
    pub follower_cursor_safe_to_reclaim: bool,
    pub physical_reclaim_errors: u64,
}

pub fn storage_safety_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageSafetySnapshot {
    let follower_cursor_blocked_reclaim_count = metric(metrics, "stale_pages_skipped")
        .saturating_add(metric(metrics, "stale_blocks_skipped"));
    StorageSafetySnapshot {
        append_watermark: metric(metrics, "append_watermark"),
        compaction_watermark: metric(metrics, "compaction_watermark"),
        delete_marker_records: metric(metrics, "tombstone_records")
            .saturating_add(metric(metrics, "stale_page_tombstones"))
            .saturating_add(metric(metrics, "stale_block_tombstones")),
        gc_eligible_record_count: metric(metrics, "tombstone_records")
            .saturating_add(metric(metrics, "stale_page_tombstones"))
            .saturating_add(metric(metrics, "stale_block_tombstones")),
        reclaimable_bytes: metric(metrics, "reclaimable_bytes"),
        follower_cursor_retention_floor: metric(metrics, "follower_cursor_retention_floor"),
        follower_cursor_blocked_reclaim_count,
        follower_cursor_safe_to_reclaim: follower_cursor_blocked_reclaim_count == 0,
        physical_reclaim_errors: metric(metrics, "physical_reclaim_errors"),
    }
}

pub fn default_storage_safety_snapshot() -> StorageSafetySnapshot {
    storage_safety_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAppendWatermarkSample {
    pub shard_id: ShardId,
    #[serde(alias = "slot_id")]
    pub bucket_id: u32,
    pub log_index: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCompactionWatermarkSample {
    pub shard_id: ShardId,
    pub safe_generation: u64,
    pub safe_timestamp_ms: u64,
    pub follower_floor: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWatermarkSnapshot {
    pub append_watermark: u64,
    pub compaction_watermark: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_safe_watermark: u64,
    pub page_index_rebuild_watermark: u64,
    pub block_index_rebuild_watermark: u64,
    pub object_index_rebuild_watermark: u64,
    #[serde(default)]
    pub append_watermark_samples: Vec<StorageAppendWatermarkSample>,
    #[serde(default)]
    pub compaction_watermark_samples: Vec<StorageCompactionWatermarkSample>,
}

pub fn storage_watermark_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageWatermarkSnapshot {
    let append_watermark = metric(metrics, "append_watermark");
    let compaction_watermark = metric(metrics, "compaction_watermark");
    let follower_cursor_retention_floor = metric(metrics, "follower_cursor_retention_floor");
    let follower_cursor_safe_watermark = if follower_cursor_retention_floor > 0 {
        compaction_watermark.min(follower_cursor_retention_floor)
    } else {
        compaction_watermark
    };
    StorageWatermarkSnapshot {
        append_watermark,
        compaction_watermark,
        follower_cursor_retention_floor,
        follower_cursor_safe_watermark,
        page_index_rebuild_watermark: metric(metrics, "page_index_rebuild_count"),
        block_index_rebuild_watermark: metric(metrics, "block_index_rebuild_count"),
        object_index_rebuild_watermark: metric(metrics, "object_index_rebuild_count"),
        append_watermark_samples: Vec::new(),
        compaction_watermark_samples: Vec::new(),
    }
}

pub fn default_storage_watermark_snapshot() -> StorageWatermarkSnapshot {
    storage_watermark_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageDeleteMarkerSample {
    #[serde(rename = "ref")]
    pub ref_id: String,
    pub generation: u64,
    pub deleted_at_ms: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGcEligibilitySample {
    #[serde(rename = "ref")]
    pub ref_id: String,
    pub eligible_after_ms: u64,
    #[serde(rename = "has_tombstone")]
    pub has_delete_marker: bool,
    pub follower_safe: bool,
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFollowerCursorSafetySample {
    pub min_follower_cursor: u64,
    pub blocked_reclaim_bytes: u64,
    pub safe_to_reclaim: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGcSnapshot {
    #[serde(rename = "tombstone_records")]
    pub delete_marker_records: u64,
    // THIS PAIR NEEDS NO WRITER FLIP, AND ITS ALIAS WAS A COLLISION.
    //
    // The canonical spelling is already published, by the SIBLING FIELD below -- these two are a
    // twin pair carrying one quantity under both words, not one field with two names. So the
    // reader-widening alias added here claimed a name a sibling already emitted: a document
    // carrying `stale_block_tombstones` is then ambiguous, serde resolves it to the FIRST
    // declaration, and `stale_block_delete_markers` silently stops being populated from the wire
    // with every other value intact. Flipping this field's `rename` on top of that made both
    // fields emit one key.
    //
    // `no_two_fields_of_one_struct_claim_one_wire_name` in `block_store.rs` now fails on exactly
    // this shape. Retiring the page-spelled twin is a published-report change and is not this
    // change.
    #[serde(rename = "stale_page_tombstones")]
    pub stale_page_delete_markers: u64,
    #[serde(rename = "stale_block_tombstones")]
    pub stale_block_delete_markers: u64,
    pub gc_eligible_record_count: u64,
    pub reclaimable_bytes: u64,
    pub compaction_reclaimed_bytes: u64,
    pub physical_reclaimed_bytes: u64,
    pub physical_reclaim_errors: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_blocked_reclaim_count: u64,
    pub follower_cursor_safe_to_reclaim: bool,
    #[serde(default)]
    #[serde(rename = "tombstone_samples")]
    pub delete_marker_samples: Vec<StorageDeleteMarkerSample>,
    #[serde(default)]
    pub gc_eligibility_samples: Vec<StorageGcEligibilitySample>,
    #[serde(default)]
    pub follower_cursor_safety_samples: Vec<StorageFollowerCursorSafetySample>,
}

pub fn storage_gc_snapshot_from_metrics(metrics: &BTreeMap<String, u64>) -> StorageGcSnapshot {
    let stale_page_delete_markers = metric(metrics, "stale_page_tombstones");
    let stale_block_delete_markers = metric(metrics, "stale_block_tombstones");
    let delete_marker_records = metric(metrics, "tombstone_records");
    let follower_cursor_blocked_reclaim_count = metric(metrics, "stale_pages_skipped")
        .saturating_add(metric(metrics, "stale_blocks_skipped"));
    StorageGcSnapshot {
        delete_marker_records,
        stale_page_delete_markers,
        stale_block_delete_markers,
        gc_eligible_record_count: delete_marker_records
            .saturating_add(stale_page_delete_markers)
            .saturating_add(stale_block_delete_markers),
        reclaimable_bytes: metric(metrics, "reclaimable_bytes"),
        compaction_reclaimed_bytes: metric(metrics, "compaction_reclaimed_bytes"),
        physical_reclaimed_bytes: metric(metrics, "physical_reclaimed_bytes"),
        physical_reclaim_errors: metric(metrics, "physical_reclaim_errors"),
        follower_cursor_retention_floor: metric(metrics, "follower_cursor_retention_floor"),
        follower_cursor_blocked_reclaim_count,
        follower_cursor_safe_to_reclaim: follower_cursor_blocked_reclaim_count == 0,
        delete_marker_samples: Vec::new(),
        gc_eligibility_samples: Vec::new(),
        follower_cursor_safety_samples: Vec::new(),
    }
}

pub fn default_storage_gc_snapshot() -> StorageGcSnapshot {
    storage_gc_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageAddressSample {
    pub shard_id: u64,
    #[serde(rename = "zone_id")]
    pub stored_slab_id: u64,
    #[serde(alias = "segment_id")]
    pub slab_id: u64,
    #[serde(alias = "page_id")]
    pub block_id: u64,
    pub offset: u64,
    pub length: u64,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockAddressSample {
    pub shard_id: u64,
    #[serde(rename = "zone_id")]
    pub stored_slab_id: u64,
    pub block_id: u64,
    pub offset: u64,
    pub length: u64,
    pub checksum: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageIndexEntrySample {
    pub logical_key: String,
    pub timestamp_range: Option<(u64, u64)>,
    #[serde(alias = "page_addresses")]
    pub block_addresses: Vec<StoragePageAddressSample>,
    pub append_watermark: u64,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockIndexEntrySample {
    pub page_address: StoragePageAddressSample,
    pub block_address: StorageBlockAddressSample,
    #[serde(rename = "band")]
    pub stored_slab_id: u64,
    pub checksum: String,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObjectIndexEntrySample {
    pub model: String,
    pub table: String,
    pub object_key: String,
    #[serde(alias = "page_chain")]
    pub block_chain: Vec<StoragePageAddressSample>,
    #[serde(rename = "tombstone")]
    pub delete_marker: bool,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexSnapshot {
    pub page_index_entry_count: u64,
    pub block_index_entry_count: u64,
    pub object_index_entry_count: u64,
    #[serde(alias = "slot_index_entry_count")]
    pub bucket_index_entry_count: u64,
    #[serde(alias = "slot_object_ref_count")]
    pub bucket_object_ref_count: u64,
    #[serde(alias = "slot_page_ref_count")]
    pub bucket_block_ref_count: u64,
    #[serde(alias = "page_address_count")]
    pub block_address_count: u64,
    #[serde(alias = "unreadable_page_refs")]
    pub unreadable_block_refs: u64,
    pub checksum_mismatches: u64,
    pub missing_owner_ref_count: u64,
    pub owner_mismatch_count: u64,
    pub restart_rebuild_verified: bool,
    #[serde(default)]
    pub page_index_entry_samples: Vec<StoragePageIndexEntrySample>,
    #[serde(default)]
    pub block_index_entry_samples: Vec<StorageBlockIndexEntrySample>,
    #[serde(default)]
    pub object_index_entry_samples: Vec<StorageObjectIndexEntrySample>,
}

pub fn storage_index_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageIndexSnapshot {
    StorageIndexSnapshot {
        page_index_entry_count: metric(metrics, "page_index_entry_count"),
        block_index_entry_count: metric(metrics, "block_index_entry_count"),
        object_index_entry_count: metric(metrics, "object_index_entry_count"),
        bucket_index_entry_count: metric(metrics, "slot_index_entry_count"),
        bucket_object_ref_count: metric(metrics, "slot_object_ref_count"),
        bucket_block_ref_count: metric(metrics, "slot_page_ref_count"),
        block_address_count: metric(metrics, "page_address_count"),
        unreadable_block_refs: metric(metrics, "unreadable_page_refs"),
        checksum_mismatches: metric(metrics, "checksum_mismatches"),
        missing_owner_ref_count: metric(metrics, "missing_owner_ref_count"),
        owner_mismatch_count: metric(metrics, "owner_mismatch_count"),
        restart_rebuild_verified: metric(metrics, "page_index_rebuild_count") > 0
            || metric(metrics, "block_index_rebuild_count") > 0
            || metric(metrics, "object_index_rebuild_count") > 0,
        page_index_entry_samples: Vec::new(),
        block_index_entry_samples: Vec::new(),
        object_index_entry_samples: Vec::new(),
    }
}

pub fn default_storage_index_snapshot() -> StorageIndexSnapshot {
    storage_index_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A slab's byte accounting and the slabs it holds.
///
/// Keyed by `stored_slab_id`, the same key as [`StorageSlabSlabSample`], which carries that
/// slab's block range and reclaim state. Two shapes over one entity, not two levels -- "zone"
/// and "band" were both older names for a slab, and the wire still uses them.
pub struct StorageSlabUsageSample {
    #[serde(rename = "zone_id")]
    pub stored_slab_id: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub stale_bytes: u64,
    #[serde(alias = "segments")]
    pub slabs: Vec<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageStreamSample {
    pub stream_id: String,
    #[serde(alias = "segments")]
    pub slabs: Vec<u64>,
    pub rollover_count: u64,
    #[serde(alias = "sealed_segment_count")]
    pub sealed_slab_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlabSample {
    #[serde(alias = "segment_id")]
    pub slab_id: u64,
    /// The same number as [`Self::slab_id`], always -- see
    /// [`crate::block_store::BlockStoreSlabDescriptor::stored_slab_id`]. Kept because it
    /// serializes, under the older key, and the compat corpora carry it.
    #[serde(rename = "band")]
    pub stored_slab_id: u64,
    pub start_offset: u64,
    pub sealed: bool,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlabSlabSample {
    #[serde(rename = "band")]
    pub stored_slab_id: u64,
    pub block_range: Vec<u64>,
    pub reclaim_state: String,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBucketSample {
    #[serde(alias = "slot_id")]
    pub bucket_id: u32,
    pub dirty_generation: u64,
    pub object_refs: Vec<u64>,
    #[serde(alias = "page_refs")]
    pub block_refs: Vec<StoragePageAddressSample>,
    #[serde(rename = "tombstones")]
    pub delete_markers: Vec<String>,
    pub owner_mismatch_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTopologySnapshot {
    #[serde(alias = "storage_zone_count")]
    pub storage_slab_count: u64,
    #[serde(alias = "active_storage_zones")]
    pub active_storage_slabs: u64,
    #[serde(alias = "sealed_storage_zones")]
    pub sealed_storage_slabs: u64,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    #[serde(alias = "segment_open_count")]
    pub slab_open_count: u64,
    #[serde(alias = "segment_sealed_count")]
    pub slab_sealed_count: u64,
    pub delayed_destroy_backlog: u64,
    #[serde(alias = "storage_zone_total_bytes")]
    pub storage_slab_total_bytes: u64,
    #[serde(alias = "storage_zone_used_bytes")]
    pub storage_slab_used_bytes: u64,
    #[serde(alias = "storage_zone_stale_bytes")]
    pub storage_slab_stale_bytes: u64,
    pub append_log_replay_records: u64,
    pub append_log_reclaimed_records: u64,
    #[serde(default)]
    #[serde(rename = "storage_zone_samples")]
    pub storage_slab_usage_samples: Vec<StorageSlabUsageSample>,
    #[serde(default)]
    pub stream_samples: Vec<StorageStreamSample>,
    #[serde(default)]
    #[serde(alias = "segment_samples")]
    pub slab_samples: Vec<StorageSlabSample>,
    #[serde(default)]
    pub slab_range_samples: Vec<StorageSlabSlabSample>,
    #[serde(default)]
    #[serde(alias = "slot_samples")]
    pub bucket_samples: Vec<StorageBucketSample>,
}

pub fn storage_topology_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageTopologySnapshot {
    StorageTopologySnapshot {
        storage_slab_count: metric(metrics, "storage_zone_count"),
        active_storage_slabs: metric(metrics, "active_storage_zones"),
        sealed_storage_slabs: metric(metrics, "sealed_storage_zones"),
        stream_slab_count: metric(metrics, "stream_segment_count"),
        slab_open_count: metric(metrics, "segment_open_count"),
        slab_sealed_count: metric(metrics, "segment_sealed_count"),
        delayed_destroy_backlog: metric(metrics, "delayed_destroy_backlog"),
        storage_slab_total_bytes: metric(metrics, "storage_zone_total_bytes"),
        storage_slab_used_bytes: metric(metrics, "storage_zone_used_bytes"),
        storage_slab_stale_bytes: metric(metrics, "storage_zone_stale_bytes"),
        append_log_replay_records: metric(metrics, "append_log_replay_records"),
        append_log_reclaimed_records: metric(metrics, "append_log_reclaimed_records"),
        storage_slab_usage_samples: Vec::new(),
        stream_samples: Vec::new(),
        slab_samples: Vec::new(),
        slab_range_samples: Vec::new(),
        bucket_samples: Vec::new(),
    }
}

pub fn default_storage_topology_snapshot() -> StorageTopologySnapshot {
    storage_topology_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

pub fn default_storage_lifecycle_metrics() -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::new();
    for name in [
        "storage_manager_prepare_count",
        "storage_manager_reclaim_count",
        "storage_manager_evict_count",
        "storage_manager_expire_count",
        "storage_manager_page_gc_count",
        "storage_manager_block_gc_count",
        "storage_manager_compaction_count",
        "storage_manager_index_gc_count",
        "storage_manager_delayed_destroy_count",
        "storage_manager_follower_cursor_safety_count",
        "storage_manager_watermark_progress_count",
        "storage_manager_loop_ms",
        "stream_rollover_count",
        "segment_open_count",
        "segment_sealed_count",
        "storage_zone_count",
        "active_storage_zones",
        "sealed_storage_zones",
        "stream_segment_count",
        "storage_zone_total_bytes",
        "storage_zone_used_bytes",
        "storage_zone_stale_bytes",
        "append_log_replay_records",
        "append_log_reclaimed_records",
        "slot_dirty_generation_count",
        "slot_tombstone_count",
        "slot_stale_ref_count",
        "slot_owner_mismatch_count",
        "slot_index_entry_count",
        "slot_object_ref_count",
        "slot_page_ref_count",
        "object_index_entry_count",
        "page_index_entry_count",
        "block_index_entry_count",
        "page_address_count",
        "unreadable_page_refs",
        "checksum_mismatches",
        "missing_owner_ref_count",
        "owner_mismatch_count",
        "page_index_rebuild_count",
        "block_index_rebuild_count",
        "object_index_rebuild_count",
        "cache_admissions",
        "cache_evictions",
        "cache_rehydrates",
        "memory_cache_hits",
        "memory_cache_misses",
        "page_index_cache_hits",
        "page_index_cache_misses",
        "block_index_cache_hits",
        "block_index_cache_misses",
        "disk_cache_hits",
        "disk_cache_misses",
        "shared_store_read_throughs",
        "cache_refills",
        "cache_invalidations",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "tombstone_records",
        "stale_page_tombstones",
        "stale_block_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
        "delayed_destroy_backlog",
        "follower_cursor_retention_floor",
        "reclaimable_bytes",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
        "append_watermark",
        "compaction_watermark",
    ] {
        metrics.insert(name.to_string(), 0);
    }
    metrics
}

pub fn effective_storage_tuning_from_env() -> BTreeMap<String, StorageContractValue> {
    let tuning = StorageTuningConfig::from_env();
    let mut values = BTreeMap::new();
    values.insert(
        TS_CONTEXT_BLOCK_TARGET_BYTES.to_string(),
        contract_u64(tuning.context_block_target_bytes as u64),
    );
    values.insert(
        TS_BLOCK_SLAB_TARGET_BYTES.to_string(),
        contract_u64(tuning.block_slab_target_bytes),
    );
    values.insert(
        TS_STREAM_MAX_BLOB_SIZE.to_string(),
        contract_u64(tuning.stream_max_blob_size),
    );
    values.insert(
        TS_COMPACTION_WATERMARK_BYTES.to_string(),
        contract_u64(tuning.compaction_watermark_bytes),
    );
    values.insert(
        TS_COLD_SCAN_NO_CACHE_FILL.to_string(),
        contract_bool(tuning.cold_scan_no_cache_fill),
    );
    // TS_PAGE_INDEX_CACHE_BYTES and TS_BLOCK_INDEX_CACHE_BYTES used to be published here. They
    // were retired because neither bounded a cache -- this report was their only reader, so
    // publishing them told an operator a budget was in force when none was. A report recorded
    // before the retirement still carries both keys and still decodes: this map is a
    // BTreeMap<String, _>, so an unknown key is an entry rather than a decode failure, and the
    // committed corpora that recorded them are left exactly as they were.
    values.insert(
        "effective_block_segment_target_bytes".to_string(),
        contract_u64(tuning.effective_slab_target_bytes()),
    );
    values
}

fn metric(metrics: &BTreeMap<String, u64>, name: &str) -> u64 {
    metrics.get(name).copied().unwrap_or_default()
}

pub fn default_storage_write_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_write_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_read_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_read_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_cold_scan_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_cold_scan_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_manager_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_manager_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_index_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_index_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_cache_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_cache_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_reclaim_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_reclaim_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_write_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert("shard_id".to_string(), contract_u64(0));
    contract.insert("slot".to_string(), contract_text("slot:0"));
    contract.insert("placement_key".to_string(), contract_text("storage:parity"));
    contract.insert("page_address".to_string(), contract_text("ElementEntry"));
    contract.insert("block_address".to_string(), contract_text("ElementEntry"));
    contract.insert(
        "append_watermark".to_string(),
        contract_u64(metric(metrics, "append_watermark")),
    );
    contract.insert("durability".to_string(), contract_text("async"));
    contract.insert("storage_family".to_string(), contract_text("shared_store"));
    contract.insert("write_mode".to_string(), contract_text("async"));
    contract.insert("index_generation".to_string(), contract_u64(0));
    contract.insert(
        "batch_watermark".to_string(),
        contract_u64(metric(metrics, "append_watermark")),
    );
    contract.insert("records_appended".to_string(), contract_u64(1));
    contract.insert(
        "append_queue_wait_ms".to_string(),
        contract_u64(metric(metrics, "append_queue_wait_ms")),
    );
    contract.insert(
        "append_engine_ms".to_string(),
        contract_u64(metric(metrics, "append_engine_ms")),
    );
    contract.insert(
        "append_queue_depth".to_string(),
        contract_u64(metric(metrics, "append_queue_depth")),
    );
    contract.insert(
        "append_batch_size".to_string(),
        contract_u64(metric(metrics, "append_batch_size").max(1)),
    );
    contract.insert(
        "append_batch_bytes".to_string(),
        contract_u64(metric(metrics, "append_batch_bytes")),
    );
    contract.insert(
        "append_coalesced_writes".to_string(),
        contract_u64(metric(metrics, "append_coalesced_writes").max(1)),
    );
    contract.insert(
        "append_durability_failures".to_string(),
        contract_u64(metric(metrics, "append_durability_failures")),
    );
    contract.insert(
        "page_writes".to_string(),
        contract_u64(metric(metrics, "page_writes")),
    );
    contract.insert(
        "block_writes".to_string(),
        contract_u64(metric(metrics, "block_writes")),
    );
    contract.insert(
        "bytes_written".to_string(),
        contract_u64(metric(metrics, "bytes_written")),
    );
    contract
}

pub fn default_storage_read_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let decoded = metric(metrics, "records_decoded");
    let returned = metric(metrics, "records_returned").max(decoded);
    let mut contract = BTreeMap::new();
    contract.insert("logical_key".to_string(), contract_text("storage:parity"));
    contract.insert("timestamp_range".to_string(), contract_text("all"));
    contract.insert(
        "object_index_entry".to_string(),
        contract_text("ObjectIndexEntry"),
    );
    contract.insert("page_index_entries".to_string(), contract_u64(1));
    contract.insert("page_addresses".to_string(), contract_u64(1));
    contract.insert("block_index_entries".to_string(), contract_u64(1));
    contract.insert("records_decoded".to_string(), contract_u64(decoded));
    contract.insert("records_returned".to_string(), contract_u64(returned));
    contract.insert(
        "tombstones_filtered".to_string(),
        contract_u64(metric(metrics, "tombstones_filtered")),
    );
    contract.insert(
        "stale_generations_filtered".to_string(),
        contract_u64(metric(metrics, "stale_generations_filtered")),
    );
    contract.insert("filter_policy".to_string(), contract_text("normal"));
    contract.insert(
        "object_page_index_lookup_count".to_string(),
        contract_u64(metric(metrics, "object_page_index_lookup_count")),
    );
    contract.insert(
        "object_page_index_lookup_ms".to_string(),
        contract_u64(metric(metrics, "object_page_index_lookup_ms")),
    );
    contract.insert(
        "page_address_count".to_string(),
        contract_u64(metric(metrics, "page_address_count")),
    );
    contract.insert(
        "block_index_lookup_count".to_string(),
        contract_u64(metric(metrics, "block_index_lookup_count")),
    );
    contract.insert(
        "block_index_lookup_ms".to_string(),
        contract_u64(metric(metrics, "block_index_lookup_ms")),
    );
    contract.insert(
        "page_reads".to_string(),
        contract_u64(metric(metrics, "page_reads")),
    );
    contract.insert(
        "decode_records_ms".to_string(),
        contract_u64(metric(metrics, "decode_records_ms")),
    );
    contract
}

pub fn default_storage_cold_scan_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let decoded = metric(metrics, "cold_scan_records_decoded");
    let returned = metric(metrics, "cold_scan_records_returned").max(decoded);
    let no_cache_reads = metric(metrics, "cold_scan_no_cache_reads");
    let mut contract = BTreeMap::new();
    contract.insert("timestamp_range".to_string(), contract_text("cold"));
    contract.insert("page_index_scan".to_string(), contract_text("PageIndex"));
    contract.insert(
        "no_cache_page_reads".to_string(),
        contract_u64(no_cache_reads),
    );
    contract.insert(
        "decode_batch_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_batch_limit")),
    );
    contract.insert(
        "decode_byte_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_byte_limit")),
    );
    contract.insert("deadline_ms".to_string(), contract_u64(0));
    contract.insert("records_decoded".to_string(), contract_u64(decoded));
    contract.insert("records_returned".to_string(), contract_u64(returned));
    contract.insert(
        "hot_cache_promotions".to_string(),
        contract_u64(metric(metrics, "hot_cache_promotions")),
    );
    contract.insert("cache_fill".to_string(), contract_bool(false));
    contract.insert("promotion_policy".to_string(), contract_text("no_promote"));
    contract.insert(
        "cold_scan_no_cache_reads".to_string(),
        contract_u64(no_cache_reads),
    );
    contract.insert(
        "cold_scan_page_index_scan_count".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_count")),
    );
    contract.insert(
        "cold_scan_page_index_scan_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_ms")),
    );
    contract.insert(
        "cold_scan_page_reads".to_string(),
        contract_u64(no_cache_reads),
    );
    contract.insert(
        "cold_scan_decode_records_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_records_ms")),
    );
    contract.insert(
        "cold_scan_records_decoded".to_string(),
        contract_u64(decoded),
    );
    contract.insert(
        "cold_scan_records_returned".to_string(),
        contract_u64(returned),
    );
    contract.insert(
        "cold_scan_decode_batch_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_batch_limit")),
    );
    contract.insert(
        "cold_scan_decode_byte_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_byte_limit")),
    );
    contract
}

pub fn default_storage_manager_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let phase_metric_pairs = [
        ("prepare", "storage_manager_prepare_count"),
        ("reclaim", "storage_manager_reclaim_count"),
        ("evict", "storage_manager_evict_count"),
        ("expire", "storage_manager_expire_count"),
        ("page_gc", "storage_manager_page_gc_count"),
        ("block_gc", "storage_manager_block_gc_count"),
        ("compaction", "storage_manager_compaction_count"),
        ("index_gc", "storage_manager_index_gc_count"),
        ("delayed_destroy", "storage_manager_delayed_destroy_count"),
        (
            "follower_cursor_safety",
            "storage_manager_follower_cursor_safety_count",
        ),
        (
            "watermark_progress",
            "storage_manager_watermark_progress_count",
        ),
    ];
    let mut phase_metrics = BTreeMap::new();
    let mut phase_counts = BTreeMap::new();
    for (phase, metric_name) in phase_metric_pairs {
        phase_metrics.insert(phase.to_string(), metric_name.to_string());
        phase_counts.insert(phase.to_string(), metric(metrics, metric_name));
    }
    let mut contract = BTreeMap::new();
    contract.insert(
        "manager_identity".to_string(),
        contract_text("StorageManager/StoreManager"),
    );
    contract.insert(
        "native_public_name".to_string(),
        contract_text("StorageManager"),
    );
    contract.insert(
        "rust_public_name".to_string(),
        contract_text("StoreManager"),
    );
    contract.insert(
        "phase_order".to_string(),
        StorageContractValue::StringList(default_storage_lifecycle_phases()),
    );
    contract.insert(
        "phase_metrics".to_string(),
        StorageContractValue::StringMap(phase_metrics),
    );
    contract.insert(
        "phase_counts".to_string(),
        StorageContractValue::U64Map(phase_counts),
    );
    contract.insert(
        "loop_metric".to_string(),
        contract_text("storage_manager_loop_ms"),
    );
    contract.insert(
        "loop_ms".to_string(),
        contract_u64(metric(metrics, "storage_manager_loop_ms")),
    );
    contract.insert("phase_order_enforced".to_string(), contract_bool(true));
    contract.insert("missing_phase_count".to_string(), contract_u64(0));
    contract
}

pub fn default_storage_index_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert(
        "page_address_codec".to_string(),
        contract_text("ElementEntry"),
    );
    contract.insert(
        "block_address_codec".to_string(),
        contract_text("ElementEntry"),
    );
    contract.insert(
        "stable_order".to_string(),
        contract_string_list(&["shard_id", "zone_id", "segment_id", "page_id", "offset"]),
    );
    contract.insert(
        "slot_index".to_string(),
        contract_text("slot -> object/page refs"),
    );
    contract.insert(
        "object_index_entry".to_string(),
        contract_text("{model/table/object_key} -> current page chain"),
    );
    contract.insert(
        "page_index".to_string(),
        contract_text("logical timestamp/key ranges -> page addresses"),
    );
    contract.insert(
        "block_index".to_string(),
        contract_text("page addresses -> physical durable locations"),
    );
    contract.insert(
        "required_behaviors".to_string(),
        contract_string_list(&[
            "page_address_encode_decode",
            "page_address_stable_order",
            "timestamp_range_page_lookup",
            "slot_index_maps_slot_to_object_page_refs",
            "object_index_maps_model_table_object_key_to_page_chain",
            "page_index_maps_logical_ranges_to_page_addresses",
            "block_index_maps_page_addresses_to_durable_locations",
            "restart_rebuilds_page_block_object_indexes",
        ]),
    );
    contract.insert(
        "page_address_encode_decode".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "block_address_encode_decode".to_string(),
        contract_bool(true),
    );
    contract.insert("stable_order_verified".to_string(), contract_bool(true));
    contract.insert(
        "timestamp_range_lookup_verified".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "slot_index_entry_count".to_string(),
        contract_u64(metric(metrics, "slot_index_entry_count").max(1)),
    );
    contract.insert(
        "slot_object_ref_count".to_string(),
        contract_u64(metric(metrics, "slot_object_ref_count").max(1)),
    );
    contract.insert(
        "slot_page_ref_count".to_string(),
        contract_u64(metric(metrics, "slot_page_ref_count").max(1)),
    );
    contract.insert(
        "object_index_entry_count".to_string(),
        contract_u64(metric(metrics, "object_index_entry_count").max(1)),
    );
    contract.insert(
        "page_index_entry_count".to_string(),
        contract_u64(metric(metrics, "page_index_entry_count").max(1)),
    );
    contract.insert(
        "block_index_entry_count".to_string(),
        contract_u64(metric(metrics, "block_index_entry_count").max(1)),
    );
    contract.insert("restart_rebuild_verified".to_string(), contract_bool(true));
    contract.insert(
        "unreadable_page_refs".to_string(),
        contract_u64(metric(metrics, "unreadable_page_refs")),
    );
    contract.insert(
        "checksum_mismatches".to_string(),
        contract_u64(metric(metrics, "checksum_mismatches")),
    );
    contract
}

pub fn default_storage_cache_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert(
        "layers".to_string(),
        StorageContractValue::StringList(default_storage_cache_layers()),
    );
    contract.insert(
        "semantics".to_string(),
        StorageContractValue::StringList(default_storage_cache_semantics()),
    );
    contract.insert(
        "metrics".to_string(),
        contract_string_list(&[
            "memory_cache_hits",
            "memory_cache_misses",
            "page_index_cache_hits",
            "page_index_cache_misses",
            "block_index_cache_hits",
            "block_index_cache_misses",
            "disk_cache_hits",
            "disk_cache_misses",
            "shared_store_read_throughs",
            "cache_refills",
            "cache_invalidations",
            "cache_writeback_queue_depth",
            "cache_writeback_rejections",
        ]),
    );
    contract.insert("hot_to_cold_lookup".to_string(), contract_bool(true));
    contract.insert("durable_refill_on_miss".to_string(), contract_bool(true));
    contract.insert(
        "append_watermark_invalidation".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "compaction_watermark_invalidation".to_string(),
        contract_bool(true),
    );
    contract.insert("cold_scan_no_promote".to_string(), contract_bool(true));
    contract.insert(
        "writeback_backpressure_measured".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "cache_refills".to_string(),
        contract_u64(metric(metrics, "cache_refills")),
    );
    contract.insert(
        "cache_invalidations".to_string(),
        contract_u64(metric(metrics, "cache_invalidations")),
    );
    contract.insert(
        "cache_writeback_queue_depth".to_string(),
        contract_u64(metric(metrics, "cache_writeback_queue_depth")),
    );
    contract.insert(
        "cache_writeback_rejections".to_string(),
        contract_u64(metric(metrics, "cache_writeback_rejections")),
    );
    contract.insert(
        "hot_cache_promotions".to_string(),
        contract_u64(metric(metrics, "hot_cache_promotions")),
    );
    contract
}

pub fn default_storage_reclaim_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert(
        "cache_eviction_frees_memory_only".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "logical_gc_marks_expired_deletable".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "physical_reclaim_requires_compaction_or_safe_skip".to_string(),
        contract_bool(true),
    );
    for field in [
        "cache_evictions",
        "tombstone_records",
        "stale_page_tombstones",
        "stale_block_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
        "reclaimable_bytes",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
    ] {
        contract.insert(field.to_string(), contract_u64(metric(metrics, field)));
    }
    contract
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWalReclaimPlan {
    pub shard_id: ShardId,
    pub safe_to_reclaim: bool,
    pub durable_bucket_generation_frontier_wal_sequence: u64,
    pub durable_bucket_generation_frontier_index_log_sequence: u64,
    #[serde(rename = "retain_from_wal_sequence")]
    pub retain_from_wal_sequence: u64,
    pub retain_from_index_log_sequence: u64,
    #[serde(rename = "current_wal_sequence")]
    pub current_wal_sequence: u64,
    pub current_index_log_sequence: u64,
    pub covered_bucket_count: usize,
    pub uncovered_bucket_count: usize,
    pub follower_cursor_block_count: usize,
    pub raft_snapshot_block_count: usize,
    pub missing_bucket_generations: Vec<u32>,
    pub retained_manifest_ids: Vec<String>,
    pub blocker_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWalReclaimReport {
    pub plan: StorageWalReclaimPlan,
    pub applied: bool,
    #[serde(rename = "wal_records_removed")]
    pub wal_records_removed: usize,
    pub index_log_records_removed: usize,
    #[serde(rename = "wal_bytes_before")]
    pub wal_bytes_before: u64,
    #[serde(rename = "wal_bytes_after")]
    pub wal_bytes_after: u64,
    pub index_log_bytes_before: u64,
    pub index_log_bytes_after: u64,
    /// Whole rolled segments unlinked by this pass.
    ///
    /// Separate from `wal_records_removed` because they are different work: that counts records
    /// rewritten out of the ACTIVE segment, while these are files dropped entire, never read and
    /// never copied. A pass that drops segments and rewrites nothing is the normal case once a log
    /// has rolled, and reporting only the rewrite made such a pass indistinguishable from one that
    /// did nothing at all.
    #[serde(default)]
    pub wal_segments_dropped: usize,
    /// Bytes held by those segments. `wal_bytes_before`/`_after` describe only the active
    /// segment, so without this the size a reclaim recovered cannot be read off the report.
    #[serde(default)]
    pub wal_segment_bytes_dropped: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexGcReport {
    pub shard_id: ShardId,
    pub enabled: bool,
    pub applied: bool,
    pub dirty_buckets_committed_before_truncate: bool,
    pub bytes_threshold: u64,
    pub usage_ratio_trigger_basis_points: u64,
    pub usage_ratio_basis_points: u64,
    pub max_entries_per_round: usize,
    pub retain_from_index_log_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub removable_records_before_budget: usize,
    pub budget_exhausted: bool,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub threshold_triggered: bool,
    pub usage_ratio_triggered: bool,
    pub safe_to_truncate: bool,
    pub skipped_reason: String,
}

/// What one threshold catalog dump reclaimed from the shard's logs (embedded path).
///
/// After [`dump_index_catalog`](crate::TemporalEngine::dump_index_catalog) durably
/// materializes the base index at `wal_anchor` and folds the catalog anchor, everything the
/// base reflects is redundant: index-log records whose own WAL anchor is at or below
/// `wal_anchor`, and WAL records at or below it (clamped by the block-retention floor, which
/// pins any record still holding the only copy of a served page).
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDumpReclaimReport {
    pub shard_id: ShardId,
    pub wal_anchor: u64,
    pub index_log_records_removed: usize,
    pub index_log_bytes_before: u64,
    pub index_log_bytes_after: u64,
    /// The index-log sweep read the log and left it alone: nothing was reclaimable, or too
    /// little was to be worth the rewrite. Without this a caller cannot tell a log that had
    /// nothing to give from one that was rewritten for nothing -- they report identical bytes.
    #[serde(default)]
    pub index_log_rewrite_skipped: bool,
    /// The index-log sweep returned an error and every field above is a default, not a reading.
    ///
    /// Same reason as `index_log_rewrite_skipped` one step further out: without it a sweep that
    /// FAILED and a sweep that had nothing to do are byte-identical, because the `Result` is
    /// dropped with `.ok()` and each field then read through `unwrap_or_default()`.
    #[serde(default)]
    pub index_log_sweep_failed: bool,
    pub wal_records_removed: usize,
    pub wal_bytes_before: u64,
    pub wal_bytes_after: u64,
    /// The write-ahead-log sweep returned an error and the three fields above are defaults.
    ///
    /// This is the field that makes the difference sayable. An errored sweep and an empty one
    /// both report `(0, 0, 0)`, so a zero release could mean the log had nothing to give, or
    /// that the reclaim refused the log and the refusal was discarded -- and the embedded
    /// proxy's reclaim loop, which is the only thing bounding that store's logs, prints exactly
    /// this report.
    #[serde(default)]
    pub wal_sweep_failed: bool,
    /// The block-retention floor in force during the WAL sweep (`None` = unconstrained): the
    /// lowest WAL sequence a live block-in-WAL registration still depends on.
    pub wal_retention_floor: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEvictionVictim {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub object_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub dirty_object_count: u64,
    pub weight: u64,
    /// Wall-clock ms this bucket was last read or written (0 == never touched).
    /// Eviction victims are ranked least-recently-used first.
    #[serde(default)]
    pub last_touched_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEvictionReport {
    pub shard_id: ShardId,
    pub mode: String,
    pub pressure_before: u64,
    pub pressure_after: u64,
    pub memory_pressure_threshold: u64,
    pub pressure_gate_open: bool,
    pub batch_limit: usize,
    pub dump_before_evict: bool,
    pub dump_manifest_ids: Vec<String>,
    pub selected_victims: Vec<StorageEvictionVictim>,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    pub dropped_object_count: usize,
    /// Victims whose resident page list was RELEASED: the node stays routable, its per-page
    /// entries are gone, and the next read through it loads them back.
    ///
    /// Distinct from `cache_entries_removed`, which is what `evict_cache` has always freed. Until
    /// this existed the two were the same number by construction -- eviction could free only what
    /// the cache held, and the index, which grows one entry per record and is never in the cache,
    /// was untouchable at any setting.
    #[serde(default)]
    pub bucket_index_buckets_released: usize,
    #[serde(default)]
    pub bucket_index_blocks_released: usize,
    /// Victims a release was attempted on and refused -- dirty, deleted, already released, or
    /// holding a page the model maps could not rebuild. A release that did nothing and a release
    /// that was declined are different answers and a round that reports only the first cannot be
    /// acted on.
    #[serde(default)]
    pub bucket_index_release_refused: usize,
    #[serde(default)]
    pub bucket_index_bytes_before: u64,
    #[serde(default)]
    pub bucket_index_bytes_after: u64,
    pub cooldown: bool,
    pub skipped_reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyRequest {
    pub lifecycle: StorageLifecycleRequest,
    #[serde(default)]
    pub create_dump_manifest: bool,
    #[serde(default)]
    pub install_dump_manifest: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyReport {
    pub shard_id: ShardId,
    pub policy_ready: bool,
    pub dump_manifest_created: bool,
    pub load_preflight_safe: bool,
    pub load_installed: bool,
    pub replay_boundary_safe: bool,
    pub manifest_chain_valid: bool,
    pub follower_retention_safe: bool,
    pub index_gc_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_id: Option<String>,
    pub manifest_bucket_ids: Vec<u32>,
    pub manifest_block_slab_ids: Vec<u64>,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "selected_replay_wal_sequence")]
    pub selected_replay_wal_sequence: u64,
    pub selected_replay_index_log_sequence: u64,
    pub lifecycle: StorageLifecycleReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_preflight: Option<BucketDumpInstallPreflightReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_status: Option<Status>,
