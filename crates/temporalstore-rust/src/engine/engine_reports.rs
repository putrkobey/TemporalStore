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
