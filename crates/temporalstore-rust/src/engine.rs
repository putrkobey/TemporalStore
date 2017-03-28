// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

#[path = "engine/engine_golden.rs"]
pub mod golden;
#[path = "engine/engine_reports.rs"]
pub mod reports;

mod admin_report;
#[path = "engine/engine_constants.rs"]
mod constants;
mod container_membership;
mod container_blocks;
mod execute_on_shard;
#[path = "engine/engine_context.rs"]
mod context;
#[path = "engine/engine_lifecycle.rs"]
mod lifecycle;
mod object_manager;
mod packed_blocks;
mod product_model;
// Step one of three: the container lands with nothing using it, so the design can be reviewed
// without a call-site conversion riding on it. The allow is the honest consequence and comes off
// in the step that converts a map to it.
#[allow(dead_code)]
mod grouped_series_index;
mod set_index_serde;
mod zset_index_serde;
mod seen_index_serde;
mod bucket_dump_manifest_methods;
mod storage_lifecycle_methods;
pub use storage_lifecycle_methods::{
    eviction_sampler_buckets_scanned, reset_eviction_sampler_buckets_scanned,
    reset_storage_plan_build_counts, storage_plan_build_counts,
};
mod timestamped_series;
mod storage_manager_cycle;
mod storage_reports;
mod prometheus_metrics;
mod stream_batch_methods;
mod recovery_sweep_compact;
#[path = "engine/engine_persistence.rs"]
mod persistence;
mod bucket_dump_io;
pub use self::bucket_dump_io::{
    bucket_dump_manifest_io_counts, bucket_dump_manifest_listing_sites,
    bucket_dump_manifest_memo_len,
    reset_bucket_dump_manifest_io_counts, BucketDumpManifestIoCounts,
};
mod command_validation;
pub mod resource_blobs;
#[path = "engine/engine_quota.rs"]
pub mod quota;
pub(crate) mod eviction_sampler;
// Single source of truth for write-command classification (shared with the data_node layer,
// which previously kept a drifted subset that mis-classified context/control-state writes
// as reads -> lifecycle-write-barrier bypass + missing dump scheduling).
pub(crate) use command_validation::{command_object_keys, is_write_command};
#[cfg(test)]
pub(crate) use storage_bucket_internals::uncovered_maintenance;
mod storage_bucket_internals;
pub use storage_bucket_internals::{
    bucket_block_index_visits, bucket_index_resident_bytes_visits, bucket_scoped_model_entries,
    bucket_release_model_derivations, bucket_visit_sites, layout_by_caller,
    live_block_scan_entries, live_block_scan_sites_snapshot, model_map_addresses_visited,
    reset_bucket_block_index_visits, reset_bucket_index_resident_bytes_visits,
    reset_bucket_release_model_derivations, reset_bucket_scoped_model_entries,
    reset_live_block_scan_entries, reset_live_block_scan_sites, reset_model_map_addresses_visited,
    reset_stage_walk_charges, take_stage_walk_charges,
    BLOCK_SLAB_LIVE_DRIFTS, BLOCK_SLAB_LIVE_RECONCILES,
};
pub use storage_reports::{
    reset_storage_cache_inspection_listings, storage_cache_inspection_listing_entries,
    storage_cache_inspection_listings,
};
pub use state::{block_slab_live_charges, reset_block_slab_live_charges};
pub use shard_write_guard::{
    index_encode_counts, maintenance_mirror_sink_lookups, maintenance_block_read_counts,
    reset_index_encode_counts, reset_maintenance_mirror_sink_lookups,
    reset_maintenance_block_read_counts, IndexEncodeCounts, MaintenanceBlockReadCounts,
};
#[path = "engine/engine_compaction.rs"]
mod compaction;
// The maintenance round in `data_node` asks this before compacting; see the function's doc.
pub use compaction::{
    compaction_drain_block_slab_ids, compaction_relocatable_block_refs, container_batch_counts,
    reset_container_batch_counts, CONTAINER_BATCH_ELEMENT_CAP,
};
mod storage_reporting;
#[path = "engine/engine_hashing.rs"]
pub(crate) mod hashing;
mod bucket_store;
mod control_rollup;
#[path = "engine/engine_hll.rs"]
mod hll;
mod hot_block_spill;
mod block_in_wal;
#[path = "engine/engine_storage_state.rs"]
mod state;
mod hash_field_map;
mod recorded_map;
mod object_record;
// `pub(crate)`: a replication payload names the range it carries, and those payloads live
// outside this module.
pub(crate) mod routing_range_stamp;

// shared-corpus: storage_bucket_first_physical_index storage_object_manager_bucketstore_runtime_authority storage_model_layout_compaction_policies storage_merged_dump_load_lifecycle storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
// shared-corpus: storage_stale_page_density_compaction storage_merged_dump_load_restart_interruption storage_gc_eviction_cold_reads storage_manager_real_pressure_signals storage_manager_wal_reclaim_bucket_generation_retention storage_manager_expire_cursor_scan_limits
// shared-corpus: storage_manager_active_eviction_runtime storage_manager_page_gc_dependency_refusal storage_manager_index_gc_thresholds_recovery storage_control_state_context_page_backed_parity

use self::admin_report::*;
use self::constants::*;
// Re-exported so `wal_record::is_wal_resident` can answer for this sentinel too, rather than
// every site comparing against it by hand.
pub(crate) use self::constants::HOT_BLOCK_SLAB_ID;
use self::execute_on_shard::execute_on_shard;
use self::context::*;
use self::packed_blocks::*;
use self::product_model::*;
use self::reports::*;
use self::command_validation::*;
use self::compaction::*;
use self::hashing::*;
use self::storage_reporting::*;
use self::storage_bucket_internals::*;
use self::bucket_dump_io::*;
use self::bucket_store::{read_bucket_index_value, bucket_index_component_block_addresses};
use self::state::*;
use crate::block_store::BlockAppendRecord;
// FOR `set` ON A SLOT. A migrated series map's writer records a point through the write
// trait, since `ObjectSlot` has no inherent `insert`. Imported anonymously because only the
// method is wanted here.
use crate::engine::timestamped_series::TimestampedSeriesWrite as _;
use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ObjectManagerStats,
    ShardStatInfo, ScanStreamRequest, ScanStreamResponse, SetConfigRequest, ShardInfo,
    ShardStats, StreamKind, StreamReadRequest, StreamReadResponse, StreamRecord,
    UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::block_store::{BlockStore, ElementEntry, BlockStoreError, BlockStoreGcPolicy, BlockStoreOptions, BlockStoreSlabLive};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextCompressionEvent,
    ContextEntity, ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit,
    context_node_summary_vector_enabled, context_vector_as_stored, ContextSummaryVector,
    CONTEXT_SUMMARY_LEVEL_L1,
    ContextDirtyNode, EventReplicationMode, EventReplicationSelectionReport,
    ExecuteRequest, ExecuteResponse, FeaturePoint, FeatureWritePolicy, InternalContextIndex,
    ReplicatedBatchExecuteRequest, ReplicatedBatchExecuteResponse,
    ReplicatedExecuteRequest, ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status, StringSetCondition,
};
use crate::wal::{LocalWriteAheadLogStore, WriteAheadLogRecord};
use context::{context_index_ref_identity, validate_context_index_lookup};
use matrixcache::{CacheEntryInfo, CacheGcReport, CacheKey, MultiLayerCache};
// FOR THE SLOT'S SERIES SURFACE: `ObjectSlot` implements these, and the names it keeps
// (`len`, `get`, `is_empty`) already matched the call sites -- only the trait was out of
// scope here. Anonymous, because only the methods are wanted.
use crate::engine::timestamped_series::TimestampedSeries as _;

#[derive(Debug, Clone)]
pub struct TemporalEngine {
    shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    cache: MultiLayerCache,
    block_store: BlockStore,
    wal_store: LocalWriteAheadLogStore,
    index_log_store: LocalIndexLogStore,
    index_dir: PathBuf,
    // Set only when the engine minted its own index_dir (no caller-supplied one): the
    // engine owns that scratch directory, and the last clone's drop removes it.
    index_scratch: Option<Arc<crate::scratch::ScratchDirGuard>>,
    configs: Arc<RwLock<HashMap<ShardId, Config>>>,
    infos: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    admissions: Arc<RwLock<HashMap<AdmissionScope, AdmissionState>>>,
    /// Per-shard read and write rate limits. Empty and inert unless something sets a limit, or the
    /// environment carries a default.
    quotas: Arc<RwLock<quota::QuotaTable>>,
    /// Whether a synchronous write takes its durable WAL barrier OUTSIDE the `shards` write lock.
    ///
    /// True everywhere but a test measuring what the other side of the lock costs.
    /// `TS_ENGINE_CONCURRENT_COMMIT` used to decide it for every engine in the process at once,
    /// which is why the tests that measure both sides had to be serialised against each other: a
    /// baseline could otherwise observe a window its sibling had opened. Shared across clones,
    /// because a clone is the same engine.
    /// The slab an unfinished compaction round is still filling, per shard, with the slab it
    /// rolled away from.
    ///
    /// A round rolls a fresh slab and relocates live blocks onto it. Once a round is BOUNDED it
    /// can stop before every block has moved -- and rolling again next round would re-move
    /// everything the last one moved, so the next round continues filling this slab instead.
    /// That is what makes a bounded round make progress rather than shuffle the same blocks.
    /// Absent means no round is in flight, so the next one rolls.
    ///
    /// Not durable: a restart loses at most the knowledge that a round was open, and the next
    /// round then rolls, which is correct if wasteful once.
    compaction_rounds: Arc<RwLock<HashMap<ShardId, (u64, u64)>>>,
    /// Where the bounded readability probe should START reading on this shard's next round.
    ///
    /// The probe reads at most `RECOVERY_READABLE_PROBE_PER_ROUND` live blocks per round so its
    /// cost does not grow with the store. Without a resume position it began at the FIRST live
    /// block every round, so it re-read the same prefix forever and a block past that prefix was
    /// never read at all -- on a shard holding more live blocks than the budget, the periodic
    /// loop could not discover an unreadable block outside the first window in any number of
    /// rounds, while the report it returned said the blocks it HAD read were fine.
    ///
    /// Holding the index the next round resumes from turns that fixed prefix into a window that
    /// sweeps the whole shard and wraps, which is what makes a bounded round make PROGRESS
    /// rather than repeat itself -- the same reason `compaction_rounds` above is carried.
    ///
    /// An INDEX into the live-block vector, not a block identity: the vector is rebuilt each round
    /// and entries move, so a resumed round is not promised the exact block the previous one
    /// stopped before. That is acceptable for a sampler whose job is to cover the store over
    /// time; nothing durable depends on this position.
    ///
    /// Not durable, for the same reason `compaction_rounds` is not: a restart costs one repeated
    /// window, not correctness. Only BOUNDED callers advance it -- an unbounded call
    /// (`readable_probe_limit == 0`) reads every block anyway and leaves the position alone, so a
    /// diagnostic call cannot move the periodic loop's window out from under it.
    recovery_probe_cursors: Arc<RwLock<HashMap<ShardId, usize>>>,
    concurrent_commit: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the expiry sweep encodes and writes its served-index checkpoint while still
    /// holding the shard-table write guard.
    ///
    /// False everywhere but the control arm of the guard that measures what the in-lock flush
    /// costs. A guard that can only observe zero cannot distinguish a shortened hold from a
    /// counter that stopped counting; this is how an engine takes the other side.
    expiry_index_flush_under_lock: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the cache warm-up reads its blocks while still holding the shard-table read guard.
    ///
    /// False everywhere but the control arm of the guard that measures it. Same reason as the
    /// flag above: an assertion that no block was read under a guard is satisfied just as well by
    /// a stage that read nothing, so the guard runs an arm that still reads them all under the
    /// lock and checks that the counter can reach the other answer.
    warm_cache_under_shard_guard: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the expiry sweep's served-index checkpoint is the WHOLE index rather than a
    /// delta of the keys the round removed.
    ///
    /// False everywhere but the arm that measures what the whole-index checkpoint cost. The
    /// whole-index write is kept reachable for exactly one reason: the claim "a round now
    /// persists what changed" is only falsifiable against an engine that still persists
    /// everything, measured in the same process on the same fixture. Without it a guard
    /// asserting flat bytes would pass just as well against a round that stopped writing.
    expiry_index_flush_whole: Arc<std::sync::atomic::AtomicBool>,
    /// Whether loading a shard warms the in-memory cache tier from the block store as part of
    /// the load, rather than leaving it to be warmed in the background.
    ///
    /// Starts from `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD`, which the shipped config and several
    /// binaries set, so this is a deployment knob and not a retired switch. The proxy turns it
    /// off for its own engine, and used to do that by writing the variable into the process.
    eager_cache_warm: Arc<std::sync::atomic::AtomicBool>,
    /// Whether eviction picks its victims by a sampled scan instead of enumerating and sorting
    /// every bucket.
    ///
    /// FALSE. Sampling changes WHICH buckets are chosen, not only how fast they are found, so it
    /// wants deliberate enabling and measurement per deployment. One test turns it on, of the
    /// engine it built, to measure how the scan volume grows with the store either way.
    evict_sampled_lru: Arc<std::sync::atomic::AtomicBool>,
    /// Whether a committed raft batch shares ONE durable engine-WAL barrier.
    ///
    /// True everywhere but a test measuring the per-entry loop. `TS_RAFT_APPLY_COALESCE` used to
    /// decide it for every engine at once. Shared across clones, because a clone is the same
    /// engine.
    raft_apply_coalesce: Arc<std::sync::atomic::AtomicBool>,
    /// How many logically deleted keys a shard may accumulate before its next served-index
    /// checkpoint re-encodes the whole base index.
    ///
    /// See [`LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT`] for what the number is and why it is
    /// what it is. Read through [`TemporalEngine::logged_deletion_index_flush_interval`], never
    /// directly, so a test that sets it can read the same value the round will. Shared across
    /// clones, because a clone is the same engine.
    logged_deletion_index_flush_interval: Arc<std::sync::atomic::AtomicU64>,
    /// How many published keys a shard may accumulate before its next served-index
    /// checkpoint re-encodes the whole base index.
    ///
    /// See [`VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT`] for what the number is and why
    /// it is what it is. Read through
    /// [`TemporalEngine::visibility_publish_index_flush_interval`], never directly, so a test
    /// that sets it can read the same value the publish will. Shared across clones, because a
    /// clone is the same engine.
    visibility_publish_index_flush_interval: Arc<std::sync::atomic::AtomicU64>,
    /// Diagnostics: number of per-execute `promote_model_maps_to_bucket_index_authority` full
    /// O(store) reconcile scans this engine has run at the hot-path call site. Without
    /// `flat_append()` this fires once per command (O(writes)); with it on the
    /// `promote_scan_done` fast-skip holds it to a small constant once warm. Read by the phase-1
    /// aging test to prove the per-write O(n) reconcile scan is gone.
    promote_scans: Arc<std::sync::atomic::AtomicU64>,
    /// Diagnostics: how many recorded outcomes this engine has INSTALLED during WAL replay.
    ///
    /// Recovery falls back to re-executing a record's command when it carries no outcomes, so a
    /// restart test comparing shard shapes passes either way and proves nothing about which path
    /// ran. This makes the claim checkable: a test can require that recovery installed what the
    /// writes recorded rather than quietly replaying commands again.
    replay_installs: Arc<std::sync::atomic::AtomicU64>,
    /// Where to mirror writes this engine performs OUTSIDE the request path.
    ///
    /// Request-path writes are mirrored a layer up, by the data node, which sees each command
    /// as it arrives. Maintenance never passes through there: eviction and the expiry sweep
    /// append their own tombstones straight to the WAL. In shared mode those deletions therefore
    /// reached the local log and no other, so a successor replaying the shared log never saw
    /// them and the key came back -- the same failure the tombstone was introduced to fix, one
    /// level up from where it was fixed.
    maintenance_mirror: Arc<RwLock<Option<Arc<dyn crate::data_node::SharedWalSink>>>>,
}


/// How many times a sweep has moved blocks out, for tests that need to know the bound fired
/// rather than that the count merely happened to stay low.
pub(crate) static RESIDENT_SWEEPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// TS_WAL_RESIDENT_BLOCKS: how many log-resident blocks one shard may hold before the oldest are
/// written out to the block store. Zero disables the bound entirely. `TS_WAL_RESIDENT_PAGES` is
/// the previous spelling and is still read.
///
/// Resident blocks are bounded because an unbounded set costs twice. Each one is a registration,
/// which is memory; each one also pins `min_registered_sequence`, and reclaim may not truncate
/// below the lowest registration — so a set that only grows is a log that can never be reclaimed
/// whatever the retention policy says. Measured on an ingest of distinct keys, registrations
/// tracked writes ONE FOR ONE: 200 writes 200 held, 1200 writes 1200 held.
///
/// The default is deliberately generous. The recent ones are worth keeping where they are: a block
/// written moments ago is the one a read is most likely to want, and its bytes are already in the
/// record just written. This is a ceiling on how far behind the dump can fall, not a cache policy.
fn wal_resident_block_limit() -> usize {
    crate::env_flag::env_number_first(&["TS_WAL_RESIDENT_BLOCKS", "TS_WAL_RESIDENT_PAGES"], 4096)
}

#[cfg(test)]
mod wal_resident_env_name_tests {
    /// The previous spelling of this bound must keep configuring it.
    ///
    /// Nothing errors when a variable stops being read. `TS_WAL_RESIDENT_PAGES` sets a CEILING on
    /// how far behind the dump may fall, and the default is deliberately generous -- so a
    /// deployment that lowered it on purpose and then silently got 4096 back would hold far more
    /// in the log than it asked to, with no message anywhere. The value asserted is 64, which
    /// differs from the default, because reading the previous name has to be distinguishable from
    /// falling through to it.
    #[test]
    fn the_previous_wal_resident_env_name_still_bounds_the_set() {
        let keys = ["TS_WAL_RESIDENT_BLOCKS", "TS_WAL_RESIDENT_PAGES"];
        for key in keys {
            std::env::remove_var(key);
        }
        let default_limit = super::wal_resident_block_limit();

        std::env::set_var("TS_WAL_RESIDENT_PAGES", "64");
        assert_eq!(
            super::wal_resident_block_limit(),
            64,
            "the previous spelling must still bound the resident set, not fall back to the default"
        );
        assert_ne!(
            super::wal_resident_block_limit(), default_limit,
            "a value read from the previous name must differ from the default, or this proves              nothing"
        );

        std::env::set_var("TS_WAL_RESIDENT_BLOCKS", "128");
        assert_eq!(
            super::wal_resident_block_limit(),
            128,
            "the current spelling must win when both are set, or a deployment mid-migration gets              whichever the lookup tried first"
        );

        for key in keys {
            std::env::remove_var(key);
        }
    }
}

/// How far below the limit a sweep goes, so a shard sitting exactly at the ceiling does not
/// materialise on every single append.
fn wal_resident_block_floor(limit: usize) -> usize {
    limit.saturating_sub(limit / 4).max(1)
}

impl TemporalEngine {
    /// Rebuild the bucket first-index once, for a caller that deferred it across a window.
    ///
    /// Only meaningful after an `IngestReconstructWindow` has held off the per-record rebuilds;
    /// calling it otherwise just repeats work the writes already did.
    pub(crate) fn reconstruct_bucket_index_now(&self, shard_id: ShardId) {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return;
        };
        storage_bucket_internals::rebuild_bucket_first_index(
            shard_id,
            shard,
            start_routing_bucket,
            end_routing_bucket,
        );
        // The rebuild above just recomputed every bucket's object index by scanning that bucket's
        // block index. The full sweep's own rebuild would scan the same blocks again, from the same
        // source, with nothing in between that could change the answer -- so the flags refresh,
        // and the duplicate scan does not.
        storage_bucket_internals::refresh_bucket_runtime_flags_after_reconstruct(shard);
    }

    /// Take the shard table's write lock, marking the region for the lock-hold measurement.
    ///
    /// Identical to `self.shards.write()` in every observable way except that the served-index
    /// encode can tell whether it is running inside the region. Maintenance paths take this one
    /// so a guard can state what they do while holding it.
    fn shards_write_marked(&self) -> MarkedShardWriteGuard<'_> {
        MarkedShardWriteGuard::new(self.shards.write().expect("engine lock poisoned"))
    }

    /// Take the shard table's read lock, marking the region for the lock-hold measurement.
    ///
    /// Identical to `self.shards.read()` in every observable way except that the block-read
    /// counter can tell whether it is running inside the region.
    fn shards_read_marked(&self) -> MarkedShardReadGuard<'_> {
        MarkedShardReadGuard::new(self.shards.read().expect("engine lock poisoned"))
    }

    /// Read one live block off the block store, counted against the shard-table guard.
    ///
    /// The maintenance stages that read blocks -- the cache warm-up and the recovery report's
    /// readable probe -- go through here so a guard can state whether the reads happened while
    /// the shard table was held. The count is taken here and the REGION is marked at the
    /// acquisition, so moving a read out of a guarded region moves it out of the under-guard
    /// tally without anything at this call site changing.
    pub(crate) fn read_block_counted(
        &self,
        address: &ElementEntry,
    ) -> Result<Vec<u8>, BlockStoreError> {
        shard_write_guard::note_block_read();
        self.block_store.read(address)
    }

    /// Mirror the deletions this engine emits on its own -- eviction drops, expiry sweeps --
    /// to the same place request-path writes go.
    ///
    /// Opt-in. With nothing attached the engine behaves exactly as it did.
    pub fn set_maintenance_wal_mirror(&self, sink: Arc<dyn crate::data_node::SharedWalSink>) {
        *self
            .maintenance_mirror
            .write()
            .expect("maintenance mirror lock poisoned") = Some(sink);
    }

    /// Hand a maintenance-generated command to the mirror, if one is attached.
    ///
    /// Called after the local append succeeds, so the mirror never learns of a deletion the
    /// local log does not already hold.
    pub(crate) fn mirror_maintenance_write(&self, shard_id: ShardId, command: &Command) {
        if let Some(sink) = self.maintenance_mirror_sink() {
            sink.record_write(shard_id, command);
        }
    }

    /// Look the mirror sink up ONCE, for a caller that is about to mirror a run of commands.
    ///
    /// The per-key form above takes the mirror lock and bumps an `Arc` refcount for every
    /// command, and the two callers that mirror a run of them -- the expiry sweep and the
    /// delete-drop eviction -- do it from inside the shard-table WRITE guard, so a round
    /// removing N keys took N+1 locks while holding the one lock that excludes everyone.
    ///
    /// Hoisting is not merely cheaper, it is more correct for a run: a sink swapped by
    /// `set_maintenance_wal_mirror` halfway through a loop would send some of one round's
    /// tombstones to the old sink and the rest to the new one, and neither would have the whole
    /// deletion. One lookup gives the whole run one destination.
    pub(crate) fn maintenance_mirror_sink(
        &self,
    ) -> Option<Arc<dyn crate::data_node::SharedWalSink>> {
        shard_write_guard::note_mirror_sink_lookup();
        self.maintenance_mirror
            .read()
            .expect("maintenance mirror lock poisoned")
            .clone()
    }

    /// Set a shard's read and write rate limits, on a running engine.
    ///
    /// A rate of zero leaves that direction unlimited. Replacing a limit rebuilds the bucket, so
    /// credit accumulated under the previous rate is dropped -- credit earned at one rate does not
    /// mean anything at another.
    pub fn set_shard_quota(&self, shard_id: ShardId, config: quota::ShardQuotaConfig) {
        self.quotas
            .write()
            .expect("quota lock poisoned")
            .set(shard_id, config);
    }

    /// What a shard's limits currently are, if any were set.
    pub fn shard_quota(&self, shard_id: ShardId) -> Option<quota::ShardQuotaConfig> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .config_of(shard_id)
    }

    /// How far behind the log each loaded shard's durable index is, in records.
    ///
    /// Records land in the log first and the index accounts for them afterwards; this is the
    /// distance between. It explains two symptoms that otherwise look unrelated to each other: a
    /// restart that takes much longer than usual, because everything past the index anchor is
    /// replayed, and reclaim that frees nothing, because it will not pass that anchor.
    ///
    /// Every shard in one pass. Callers that already hold a read lock on the shard table must not
    /// ask per shard: a second read on the same lock, with a writer queued between them, deadlocks.
    pub fn shard_index_lags(&self) -> Vec<(ShardId, u64)> {
        let applied: Vec<(ShardId, u64)> = {
            let shards = self.shards.read().expect("engine lock poisoned");
            shards
                .iter()
                .map(|(shard_id, shard)| (*shard_id, shard.applied_wal_sequence.unwrap_or(0)))
                .collect()
        };
        applied
            .into_iter()
            .map(|(shard_id, applied)| {
                let appended = self.wal_store.cached_last_sequence(shard_id);
                (shard_id, appended.saturating_sub(applied))
            })
            .collect()
    }

    /// How many keys each loaded shard is holding an expiry deadline for.
    ///
    /// The sweep's own report says what it REMOVED, which looks equally healthy whether the backlog
    /// behind it is draining or growing. This says which. The engine already decides how hard to
    /// work from this number -- it becomes the expiry component of the storage cycle's pressure
    /// signal -- so publishing it only makes visible what is already being acted on.
    ///
    /// Every shard in one pass, for the same reason as the trailing distance: a caller inside the
    /// metrics loop already holds a read lock on the shard table.
    pub fn shard_expiry_backlogs(&self) -> Vec<(ShardId, u64)> {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .iter()
            .map(|(shard_id, shard)| (*shard_id, shard.expires_at_ms().len() as u64))
            .collect()
    }

    /// What a shard's rate limit has allowed and refused, if it has one.
    ///
    /// Absent means the shard is not limited, which is different from a limit that has refused
    /// nothing -- and the difference is the one an operator actually wants.
    pub fn shard_quota_counters(&self, shard_id: ShardId) -> Option<quota::QuotaCounters> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .counters_of(shard_id)
    }

    /// Every shard carrying a rate limit, for reporting.
    pub fn rate_limited_shards(&self) -> Vec<ShardId> {
        self.quotas
            .read()
            .expect("quota lock poisoned")
            .limited_shards()
    }

    /// Take one token for `kind`. True when the command may proceed.
    ///
    /// The overwhelmingly common case is a shard with no limit, and that case must not pay for
    /// this. The environment default is read once for the process rather than per command, and a
    /// shard that is not limited settles under a READ lock -- taking the write lock on every
    /// command would serialise the engine on a feature almost nobody has turned on.
    fn charge_quota(&self, shard_id: ShardId, kind: quota::QuotaKind) -> bool {
        static DEFAULT: std::sync::OnceLock<quota::ShardQuotaConfig> = std::sync::OnceLock::new();
        let default = *DEFAULT.get_or_init(quota::ShardQuotaConfig::from_env);
        if default.is_unlimited() {
            let table = self.quotas.read().expect("quota lock poisoned");
            if !table.limits(shard_id) {
                return true;
            }
        }
        self.quotas
            .write()
            .expect("quota lock poisoned")
            .try_consume(shard_id, kind, default)
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, None, Vec::new())
    }

    /// Apply `request`, attaching `pages` to its log record instead of whatever this node
    /// would derive for it.
    ///
    /// For replaying a write that was already acked somewhere else. A block can be derived
    /// state -- a serialized counter series, a hash map -- and re-executing the command that
    /// produced it reconstructs it only from a state this node may no longer have. When the
    /// original bytes travelled with the command, they are the truth, and re-deriving would
    /// quietly substitute a reconstruction for what was actually acknowledged.
    ///
    /// An empty `pages` is exactly [`execute`](Self::execute).
    pub fn execute_with_carried_blocks(
        &self,
        request: ExecuteRequest,
        blocks: Vec<crate::wal::StagedBlock>,
    ) -> ExecuteResponse {
        self.execute_with_storage_override(request, None, blocks)
    }

    pub fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, Some(false), Vec::new())
    }

    /// Apply a committed raft entry to the state machine, durably (fsync'd WAL) but with a
    /// NON-BLOCKING index-log append: on the raft path the raft log is the durability +
    /// reconstruction source, so the per-apply index-log fsync is redundant. Removing it off
    /// the critical replication path shortens apply latency (which otherwise widens the
    /// snapshot-transfer / backpressure window). A crash that loses the non-fsync'd index-log
    /// tail is safe -- raft-log replay on restart re-applies and rebuilds the served index.
    pub fn execute_raft_apply(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_raft_apply_at(request, None)
    }

    /// `execute_raft_apply`, resolving time-dependent values against the instant the LEADER
    /// admitted the command rather than against this node's clock.
    ///
    /// A relative deadline becomes an absolute one while the command executes, and on this path
    /// the command executes once per replica at whatever moment that replica applied. Without the
    /// leader's instant, one committed entry produces as many absolute deadlines as there are
    /// replicas. `None` (or a zero stamp, from an entry written before the log carried one) keeps
    /// the live clock, which is exactly what this path did before.
    pub fn execute_raft_apply_at(
        &self,
        request: ExecuteRequest,
        leader_time_ms: Option<u64>,
    ) -> ExecuteResponse {
        let _guard = RaftApplyGuard::enter();
        let _clock = ReplayClockGuard::enter(leader_time_ms);
        self.execute_with_storage_override(request, Some(false), Vec::new())
    }

    /// Apply a batch of committed raft entries to the state machine. Under
    /// `TS_RAFT_APPLY_COALESCE` the per-entry engine-WAL fdatasync is coalesced into ONE barrier
    /// for the whole batch (an AppendEntries batch on a follower, a recovery replay, or a
    /// pipelined-propose group): every entry appends its WAL bytes with sync=false and RESERVES its
    /// sequence, then a single `commit_barrier` makes the whole batch durable. The raft log stays
    /// the durability + reconstruction source, and the coalesced barrier still completes here --
    /// inside apply -- BEFORE the raft runtime advances the durable `applied_index`
    /// (persist_configured_wal runs after apply), so `applied => engine-WAL-durable` holds exactly
    /// as with the per-entry path; a crash before the barrier leaves applied_index below the batch
    /// so raft replay re-applies it. Gate OFF (or a single-entry batch) -> a plain per-entry
    /// `execute_raft_apply` loop (byte-identical).
    pub fn execute_raft_apply_batch(&self, requests: Vec<ExecuteRequest>) -> Vec<ExecuteResponse> {
        self.execute_raft_apply_batch_at(
            requests.into_iter().map(|request| (request, None)).collect(),
        )
    }

    /// `execute_raft_apply_batch`, each entry carrying the instant its LEADER admitted it.
    ///
    /// The stamp is per ENTRY, not per batch: a batch is whatever run of committed entries this
    /// node happened to apply together, and those were admitted at different moments. Applying one
    /// batch-wide timestamp would replace a per-node drift with a per-batch one. The coalesced
    /// barrier is unaffected -- it is still taken once, after the loop.
    pub fn execute_raft_apply_batch_at(
        &self,
        requests: Vec<(ExecuteRequest, Option<u64>)>,
    ) -> Vec<ExecuteResponse> {
        if !self
            .raft_apply_coalesce
            .load(std::sync::atomic::Ordering::Relaxed)
            || requests.len() <= 1
        {
            return requests
                .into_iter()
                .map(|(request, leader_time_ms)| self.execute_raft_apply_at(request, leader_time_ms))
                .collect();
        }
        let _apply_guard = RaftApplyGuard::enter();
        let batch_guard = RaftApplyBatchGuard::enter();
        let mut responses = Vec::with_capacity(requests.len());
        for (request, leader_time_ms) in requests {
            let _clock = ReplayClockGuard::enter(leader_time_ms);
            responses.push(self.execute_with_storage_override(request, Some(false), Vec::new()));
        }
        let barrier = batch_guard.take_barrier();
        drop(batch_guard);
        if let Some((shard_id, sequence)) = barrier {
            if let Err(err) = self.wal_store.commit_barrier(shard_id, sequence) {
                // The coalesced batch barrier failed: none of these writes are durable. Fail every
                // otherwise-ok response so raft apply surfaces the durability failure instead of
                // acking (mirrors the single-write commit_barrier failure path).
                for response in responses.iter_mut() {
                    if response.status.ok {
                        *response = ExecuteResponse {
                            status: Status::error(
                                "wal_commit_failed",
                                format!("durable WAL commit barrier failed: {err}"),
                            ),
                            response: CommandResponse::Empty,
                        };
                    }
                }
            }
        }
        responses
    }

    pub fn execute_replicated(&self, request: ReplicatedExecuteRequest) -> ExecuteResponse {
        let replication_mode = request.replication_mode;
        let request = ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        };
        match replication_mode {
            EventReplicationMode::SyncStorage => {
                self.execute_with_storage_override(request, Some(false), Vec::new())
            }
            EventReplicationMode::AsyncStorage => {
                self.execute_with_storage_override(request, Some(true), Vec::new())
            }
            EventReplicationMode::Raft | EventReplicationMode::Inherit => self.execute(request),
        }
    }

    pub fn replication_selection_report(
        &self,
        command: &Command,
        requested_mode: EventReplicationMode,
    ) -> EventReplicationSelectionReport {
        let write_command = is_write_command(command);
        let effective_mode = if write_command {
            requested_mode
        } else {
            EventReplicationMode::Inherit
        };
        EventReplicationSelectionReport {
            requested_mode,
            effective_mode,
            write_command,
            accepted: true,
            restart_required: requested_mode.requires_restart(),
            reason: if !write_command {
                "read_command_does_not_replicate".to_string()
            } else if requested_mode == EventReplicationMode::Inherit {
                "using_current_runtime_default_without_restart".to_string()
            } else {
                "event_selected_replication_mode_without_restart".to_string()
            },
        }
    }

    fn execute_with_storage_override(
        &self,
        request: ExecuteRequest,
        async_storage_override: Option<bool>,
        mut carried_blocks: Vec<crate::wal::StagedBlock>,
    ) -> ExecuteResponse {
        // Charged before anything else, including the read-only fast path -- a read served without
        // taking the shard lock still costs the shard, and a limit the cheapest reads slip past is
        // not a limit.
        //
        // Not charged while applying a replicated entry or replaying the log. Refusing either is
        // not shedding load: a follower that rejects what the leader committed diverges from it,
        // and a replay that rejects a record already in the log cannot rebuild the shard.
        if !raft_applying() && !replaying_wal() {
            let kind = if command_validation::is_write_command(&request.command) {
                quota::QuotaKind::Write
            } else {
                quota::QuotaKind::Read
            };
            if !self.charge_quota(request.shard_id, kind) {
                return ExecuteResponse {
                    status: Status::error(
                        "quota_exhausted",
                        format!(
                            "shard {} is over its {} rate limit",
                            request.shard_id,
                            match kind {
                                quota::QuotaKind::Write => "write",
                                quota::QuotaKind::Read => "read",
                            }
                        ),
                    ),
                    response: CommandResponse::Empty,
                };
            }
        }
        // Blob commands run before the shard lock: blobs live beside the engine, not inside
        // any shard's record state, and a large upload must never hold the shard write lock.
        if let Some(response) = self.execute_resource_blob_command(&request) {
            return response;
        }
        if async_storage_override.is_some() {
            if let Some(response) = self.execute_read_only_fast_path(&request) {
                return response;
            }
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            };
        };
        // While a shard is replaying its WAL on load it is present in `shards` but not yet
        // serving (keeps it in PartitionLoadStage::LOADING). Reject client commands with
        // a retryable status so a concurrent write cannot interleave with replay -- which
        // would regress the WAL anchor and double-apply on the next restart. The replay
        // thread re-drives records under replaying_wal(), which bypasses this gate.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "shard_not_loaded",
                    "shard is recovering (WAL replay in progress)",
                ),
                response: CommandResponse::Empty,
            };
        }
        let command = request.command;
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false)
            && is_write_command(&command)
        {
            return ExecuteResponse {
                status: Status::error("readonly_shard", "readonly shard rejects write command"),
                response: CommandResponse::Empty,
            };
        }
        let mut config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if let Some(async_storage) = async_storage_override {
            config.async_storage = async_storage;
        }
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let start_routing_bucket = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end_routing_bucket = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        // Bulk backfill AND WAL replay defer this model-map -> bucket-index promotion
        // (an O(store) scan plus secondary-view rebuild) to a single reconstruct pass
        // (flush_shard_index() / replay_wal_into_shard()'s tail). Run per command it is
        // the dominant O(n^2) cost of a large ingest/reload; fresh writes live in the
        // model maps, so the single reconstruct rebuilds bucket_index and the secondary
        // views losslessly.
        // Phase-1 flat-append fast-skip: once a promote scan has confirmed `bucket_index` is in
        // sync with the model maps, skip the O(store) re-scan on every subsequent command. The
        // live write path keeps `bucket_index` authoritative in lock-step (each mutating command
        // upserts its block before returning), so the repeat scan can only re-confirm sync. The
        // flag is `#[serde(skip)]` (false on any fresh load), so the first live command after a
        // reload still pays one full reconcile. Gate OFF -> the scan runs every command as before.
        if !defer_bucket_index_reconstruct()
            && !(self.wal_store.flat_append() && shard.promote_scan_done)
        {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                request.shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.block_store, shard, None);
            }
            // Mark the reconcile confirmed only once the shard actually holds model-map state:
            // `promote` returns false (without establishing anything) on an empty shard, so
            // guarding on non-emptiness avoids latching the flag before the first real write.
            if self.wal_store.flat_append() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let write_command = is_write_command(&command);
        // Exempt for exactly the reason the token-bucket limit at the top of this function is
        // exempt, and the reason is not symmetry for its own sake. A follower that refuses what
        // its leader committed diverges from the leader. A replay that refuses a record already
        // in the log does not degrade the shard, it loses it: replay_wal_into_shard turns any
        // failed response into wal_replay_failed, and load_shard_with unwinds the shard and
        // refuses the load on it.
        //
        // Measured before this guard existed: a shard configured write_qps=3 applied 0 of 40
        // committed entries, and a replay of 40 records refused the 4th.
        if !raft_applying() && !replaying_wal() {
            if let Err(status) =
                self.check_admission(request.shard_id, write_command, &config, &info)
            {
                return ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                };
            }
        }
        // Exempt under raft apply and under replay, with the admission limits just above. The
        // ceiling reads the known physical bytes of the WHOLE store, which is not a number a
        // follower applying a committed entry, or a load rebuilding a shard, can do anything
        // about -- and a shard that cannot replay is not a shard held under a ceiling, it is a
        // shard that will not load, so it never reaches the reclamation that would clear it.
        if !raft_applying()
            && !replaying_wal()
            && write_command
            && config
                .maxmemory_bytes
                // Gate on the CURRENT on-disk footprint (decremented by GC/compaction/
                // purge), not the cumulative-ever `bytes_written` counter. The old gate
                // was a monotonic tripwire that reclamation could never clear, so a
                // long-running node permanently rejected all writes once it tripped.
                // compares live resident size and evicts; this at least lets
                // reclamation re-admit writes.
                .map(|limit| self.block_store.slab_summary().total_known_physical_bytes >= limit)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "storage_quota_exceeded",
                    "shard maxmemory_bytes limit has been reached",
                ),
                response: CommandResponse::Empty,
            };
        }
        // Command preconditions are a LEADER-time gate: a command only reaches the WAL after
        // passing them on the leader. WAL replay re-applies already-committed effects (like
        // ReplayWal, which does not re-check preconditions), so re-validating here
        // against reconstructed state + the restart clock is both redundant and unsafe --
        // e.g. a replayed EXPIRE whose earlier deadline has since lapsed would fail the
        // liveness precondition and abort the whole shard load. Skip validation during
        // replay, mirroring the WAL-append / index-anchor guards below.
        if !replaying_wal() {
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.block_store,
                request.shard_id,
                shard,
                &command,
            ) {
                return ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                };
            }
        }
        // Start this write with nothing staged, so a block put aside by a command that never
        // appended cannot ride along on the next command's record.
        block_in_wal::begin_write();
        // What the touched keys held before this command, so the capture below can be
        // skipped when nothing was removed. Sizes and one `u64` -- no allocation.
        //
        // THE DEADLINE'S VALUE IS CARRIED SEPARATELY BECAUSE `key_membership_size` ONLY COUNTS
        // ITS PRESENCE. It adds `expires_at_ms.contains_key(key)` as one unit, which notices a
        // deadline being removed (a shrink) but not one being ARMED (a growth) or MOVED (no
        // change at all). Keeping the millisecond itself is what lets those two be seen. See
        // `delta_key_state_change` below.
        let membership_before: Vec<(String, usize, Option<u64>)> = command_object_keys(&command)
            .into_iter()
            .map(|key| {
                let size = key_membership_size(shard, &key);
                let deadline = shard.expires_at_ms().get(key.as_str()).copied();
                (key, size, deadline)
            })
            .collect();

        let outcome = execute_on_shard(
            &self.cache,
            &self.block_store,
            config.feature_max_size,
            config.async_storage,
            config.control_rollup_enabled(),
            config.control_coalesce_persist_enabled(),
            config.control_distinct_sketch_enabled(),
            request.shard_id,
            start_routing_bucket,
            end_routing_bucket,
            shard,
            command.clone(),
        );
        // LRU recency: record that this command touched its
        // bucket(s), read or write, so eviction can prefer least-recently-used buckets.
        {
            let now = now_ms();
            crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::RecencyStamp, || {
                for_each_touched_key(&command, |key| {
                    let recency_bucket =
                        block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                    shard.bucket_recency.insert(recency_bucket, now);
                });
            });
        }
        // Set to the reserved WAL sequence when the concurrent-commit path defers this
        // write's durable barrier out of the `shards` lock (TS_ENGINE_CONCURRENT_COMMIT).
        // The barrier is awaited AFTER the lock is released, just before the ack.
        let mut pending_barrier_seq: Option<u64> = None;
        // The sequence this write took, whichever append path assigned it. Separate from
        // `pending_barrier_seq`, which means "the barrier for this sequence is deferred" and is
        // set on only one of the two paths.
        let mut appended_sequence: Option<u64> = None;
        if outcome.mutated {
            let object_keys = command_object_keys(&command);
            // Capture this write's touched keys for the O(delta) index-log append below
            // (the command is moved into the WAL append before we reach that point).
            let delta_command_keys = object_keys.clone();
            // Captured here for the same reason as the keys above: the command is moved into
            // the WAL append before the index-log items are built, so anything the items need
            // from it has to be taken while it is still owned.
            let removed_component = command_removed_component(&command);
            let upsert_components = command_upsert_components(&command, shard);
            // Taken here, beside the components, for the same ownership reason and -- for a list
            // push -- at the same point, because the sequence it files under is only knowable
            // post-apply.
            let touched_container_elements =
                command_touched_container_elements(&command, shard);
            if object_keys.is_empty() {
                rebuild_bucket_block_ownership(
                    request.shard_id,
                    shard,
                    info.as_ref()
                        .map(|info| info.start_routing_bucket)
                        .unwrap_or_default(),
                    info.as_ref()
                        .map(|info| info.end_routing_bucket)
                        .unwrap_or(u32::MAX),
                );
            } else {
                for object_key in object_keys {
                    let start_routing_bucket = info
                        .as_ref()
                        .map(|info| info.start_routing_bucket)
                        .unwrap_or_default();
                    let end_routing_bucket = info
                        .as_ref()
                        .map(|info| info.end_routing_bucket)
                        .unwrap_or(u32::MAX);
                    // ONE MARKING PATH, CALLED ONCE.
                    //
                    // This site used to do four things -- compute the routing bucket, fetch the
                    // shared key handle, insert into the dirty index, push dirtiness onto the
                    // object's pages -- and then call `mark_object_dirty`, which does all four
                    // again before doing the bucket-node work only it does. Every object on every
                    // write paid two routing hashes, two lookup probes, two inserts and two page
                    // pushes for one marking.
                    //
                    // The CALL is what survived, not the block: it alone marks the bucket dirty,
                    // bumps `dirty_generation` and notes the flags stale, so keeping the block
                    // instead would have dropped those three silently.
                    //
                    // AND THE BRANCH AROUND IT WAS DEAD. It read
                    // `if config.async_storage { mark(..) } else { mark(..) }` -- the same call in
                    // both arms. The synchronous branch was evidently meant to differ and never
                    // did, which is why a synchronous write leaves as much pending as an async one
                    // (measured: 64 of 64 either way). Writing through is still to be built; this
                    // removes the branch that suggested it already had been.
                    mark_object_dirty(
                        shard,
                        &object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                }
            }
            // Rebuild the first-index only outside the deferred-reconstruct windows
            // (bulk backfill / WAL replay). In those windows the promote step that
            // would populate bucket_map is deferred to the single reconstruct, so
            // bucket_map stays empty; the `is_empty()` clause (and the context path,
            // which never updates bucket_index directly) would then fire a full
            // O(store) rebuild on EVERY record -> O(n^2). The single reconstruct
            // rebuilds the first-index once at the end, so deferring here is
            // correctness-preserving.
            // Maintain the index for the keys this write touched, and rebuild only if that did
            // not cover them.
            //
            // A context write does not register its block, so the branch below fired a full
            // O(store) rebuild for every one -- the last term in an add that grew with the corpus.
            // Feature and Sequence writes already maintain on the write path, and replay already
            // maintains these same kinds; this closes the one path that did neither.
            //
            // `sync_context_blocks_for_object` mirrors `collect_model_live_block_entries` arm for
            // arm and reports whether it found anything, so a write it does not cover still gets
            // the rebuild rather than a quietly stale index. An empty bucket_map still rebuilds:
            // maintenance updates an index, it does not construct one.
            let maintained_bucket_index = !shard.bucket_index.bucket_map.is_empty()
                && !delta_command_keys.is_empty()
                && delta_command_keys.iter().all(|object_key| {
                    storage_bucket_internals::sync_context_blocks_for_object(
                        shard,
                        request.shard_id,
                        object_key,
                    )
                });
            let rebuilt_bucket_index = !maintained_bucket_index
                && !defer_bucket_index_reconstruct()
                // A command that writes no block cannot have changed the block index, so rebuilding it
                // recomputes what it already held -- measured at twice the shard's block count per
                // call for SeenCheck and the control-state change/selection writes.
                && !command_writes_no_block(&command)
                && (!command_updates_bucket_index_directly(&command)
                    || shard.bucket_index.bucket_map.is_empty());
            if rebuilt_bucket_index {
                rebuild_bucket_first_index(
                    request.shard_id,
                    shard,
                    start_routing_bucket,
                    end_routing_bucket,
                );
            }
            if !defer_bucket_index_reconstruct() {
                if rebuilt_bucket_index {
                    // The rebuild replaced bucket_map wholesale, so the record of which buckets
                    // changed no longer describes it; recompute everything.
                    refresh_bucket_runtime_flags(shard);
                } else {
                    // Refresh only what this write touched. Sweeping the shard here cost
                    // O(total blocks) on EVERY write, which made ingestion quadratic in the corpus.
                    refresh_pending_bucket_runtime_flags(shard);
                }
            }
            // ONE COPY OF A CONTEXT EVENT KEY, AND THIS IS THE FIRST POINT WHERE IT IS POSSIBLE.
            // The event pair files its two maps during execution, while the page index does not
            // yet hold the key -- counted, not assumed: an adoption attempted at the end of the
            // write arm found no handle on 400 of 400 writes. Both branches above put the key in
            // (maintenance through `sync_context_blocks_for_object`, otherwise the rebuild), so
            // the handle exists here and nowhere earlier. Same shape as the dirty index above,
            // and for the same reason: ask once the thing you borrow from exists.
            for object_key in &delta_command_keys {
                shard.adopt_object_key_allocations(object_key);
            }
            // Every
            // write records a WAL entry before any block is written.
            // async_storage only changes whether the commit BLOCKS: sync -> fsync,
            // async (or bulk backfill) -> buffered, no fsync (a fire-and-forget
            // commit). Block/index materialization stays deferred to dump.
            // Drain what this execution recorded, on EVERY path -- not only the ones that go
            // on to append. Staging happens during execution, but the append below is guarded,
            // so any write that does not reach it left its items sitting in the thread's
            // buffer for whatever wrote next on that thread to adopt as its own. Replay is the
            // loud case: it re-executes commands with `replaying_wal()` true, stages an item
            // for everything it re-applies, and appends none of it -- so the first write after
            // a recovery inherited the recovery's items and recorded them as changes it had
            // made itself. A later replay then installs them, and if one cannot be applied it
            // aborts the whole shard load, taking unrelated keys down with it.
            // Nothing can be staged while recording is off, because the staging function is
            // where that is now decided -- so there is nothing left here to clear.
            let mut staged_outcomes = block_in_wal::take_outcomes();
            if write_command && !replaying_wal() {
                // A write that changed the shard and recorded nothing cannot be replaced by
                // its record. Every existing test that writes anything is a probe for that,
                // which covers far more of the mutating surface than a hand-listed fixture per
                // command would -- so this is checked in every debug build rather than behind
                // `TS_WAL_OUTCOME_STRICT`, which nothing ever set. Measured before making it
                // standing: 1628 lib tests, no violation. A release build pays nothing.
                //
                // Still conditional on records carrying results at all: with none there is
                // nothing for the check to be about.
                if crate::wal::wal_outcome_items_enabled()
                    && cfg!(debug_assertions)
                    && staged_outcomes.is_empty()
                {
                    let rendered = format!("{command:?}");
                    let label = rendered
                        .split_once(' ')
                        .map(|(head, _)| head.to_string())
                        .unwrap_or(rendered);
                    panic!(
                        "{label} changed shard {} and recorded nothing about what it did, so its record cannot replace it",
                        request.shard_id
                    );
                }
                let sync = !config.async_storage && !bulk_ingest_mode();
                // Concurrent-commit path, DEFAULT ON -- `concurrent_commit` is constructed
                // `true` and is false only in the test that measures what the in-lock barrier
                // costs (see the note on that field). This comment said "gated, default OFF",
                // which made the branch below read as the unusual one when it is the one every
                // synchronous separate write takes -- and the rule it applies to a record's
                // operation is not the same as the other branch's, so believing it is off points
                // a durability reading at the wrong call site.
                // For a synchronous write, only
                // RESERVE the WAL sequence + append the bytes here (under the `shards` lock);
                // the durable fdatasync barrier is deferred to `commit_barrier` AFTER the lock
                // is released, so concurrent same-shard writers reach the group-commit queue in
                // parallel and coalesce their fsyncs (see wal.rs::group_commit_sync). WAL
                // sequence order still equals in-memory apply order because the reservation +
                // byte-append stay under this same lock, exactly as append_with_sync did; only
                // the order-independent fsync moves out. Off -> byte-identical append_with_sync.
                // In a raft apply batch (TS_RAFT_APPLY_COALESCE) reuse the same reserve-only
                // append: each committed entry appends its bytes with sync=false and RESERVES its
                // WAL sequence here; the single coalesced fdatasync is issued once for the whole
                // batch in `execute_raft_apply_batch` (see `raft_apply_batch_active`). WAL order
                // still equals apply order (reservation + byte-append stay under this lock).
                // The reserve-only branch appends bytes without blocks, so a write carrying
                // blocks must take the staged branch or they would be dropped on the floor.
                // Staged blocks still force the other branch -- their addresses are back-patched
                // once the record's log id exists, which this path does not do. Outcomes no
                // longer do: they are resolved before they are staged, so they ride along and a
                // recording write keeps its place in the group-commit queue.
                let concurrent_commit = sync
                    && carried_blocks.is_empty()
                    && (self
                        .concurrent_commit
                        .load(std::sync::atomic::Ordering::Relaxed)
                        || raft_apply_batch_active());
                // Where each block this write stages ends up, so the index can carry it. Filled
                // by the append below, which is the first moment the log id exists.
                let mut wal_resident_updates: Vec<(u64, crate::engine::state::WalResidentBlock)> =
                    Vec::new();
                let append_result = if concurrent_commit {
                    self.wal_store
                        .append_for_group_commit(
                            request.shard_id,
                            command,
                            std::mem::take(&mut staged_outcomes),
                            if carried_blocks.is_empty() {
                                block_in_wal::take_staged()
                            } else {
                                let _ = block_in_wal::take_staged();
                                std::mem::take(&mut carried_blocks)
                            },
                        )
                        .map(|record| {
                            appended_sequence = Some(record.sequence);
                            Some(record.sequence)
                        })
                } else {
                    self.wal_store
                        .append_with_outcomes(
                            request.shard_id,
                            command,
                            sync,
                            if carried_blocks.is_empty() {
                                block_in_wal::take_staged()
                            } else {
                                // The caller handed us the blocks the original write produced.
                                // Drop whatever this execute re-derived rather than letting a
                                // reconstruction win over the bytes that were acked.
                                let _ = block_in_wal::take_staged();
                                std::mem::take(&mut carried_blocks)
                            },
                            std::mem::take(&mut staged_outcomes),
                        )
                        .map(|(record, log_id)| {
                            appended_sequence = Some(record.sequence);
                            // Point every block this record carries at the record, keyed on the
                            // object id the write derived -- which is what the stored address
                            // carries, so a read finds it by identity rather than by timing.
                            block_in_wal::register_record(
                                &self.block_store,
                                request.shard_id,
                                &record.staged_blocks,
                                log_id,
                                record.sequence,
                                &self.wal_store,
                            );
                            // Same fact, written down where it survives this process.
                            wal_resident_updates.extend(record.staged_blocks.iter().map(|block| {
                                (
                                    // One entry per BLOCK, not per object: the record says which
                                    // element each block is, and a record carrying several
                                    // elements of one key files several entries here.
                                    block_in_wal::wal_resident_key(
                                        block.object_id,
                                        block.component.as_deref(),
                                    ),
                                    crate::engine::state::WalResidentBlock {
                                        log_id,
                                        sequence: record.sequence,
                                    },
                                )
                            }));
                            None
                        })
                };
                match append_result {
                    Ok(deferred_seq) => {
                        // The index carries where each staged block landed, so a reload can hand
                        // the mapping back rather than leaving the address unresolvable until a
                        // full replay re-derives the block.
                        for (object_id, placement) in wal_resident_updates.drain(..) {
                            shard.wal_resident_blocks.insert(object_id, placement);
                        }
                        // Record where this write sits in the log, for every bucket it dirtied
                        // that did not already have a claim.
                        //
                        // Done HERE rather than where the bucket is marked dirty, because the
                        // mark happens before the append and the sequence does not exist yet at
                        // that point. Both the sync and the async mark paths run above this, so
                        // one pass covers them.
                        //
                        // Only when unset: the field is the OLDEST undumped write, so a second
                        // write to an already-dirty bucket must not move it forward.
                        if let Some(sequence) = appended_sequence {
                            for key in &delta_command_keys {
                                let routing_bucket = block_routing_bucket(
                                    key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                );
                                if let Some(bucket) =
                                    shard.bucket_index.bucket_map.get_mut(&routing_bucket)
                                {
                                    if bucket.first_dirty_wal_sequence == 0 {
                                        bucket.first_dirty_wal_sequence = sequence;
                                    }
                                }
                            }
                        }
                        // In concurrent-commit mode remember the reserved sequence; its durable
                        // barrier is awaited after the `shards` lock is dropped (below). The ack
                        // is returned strictly AFTER that barrier succeeds -- never before.
                        pending_barrier_seq = deferred_seq;
                    }
                    Err(err) => {
                    if sync {
                        // A synchronous write whose durable WAL commit failed is NOT durable: the
                        // WAL is the recovery source of truth (replayed on load), so returning ok
                        // would tell the client a write that is gone after a crash succeeded. The
                        // failed commit status is surfaced to the client instead of acking a write
                        // that is not durable, so the error is never swallowed. (Async/bulk mode is
                        // a fire-and-forget commit, so its append errors stay best-effort and do
                        // not fail the command.) We also skip the index anchor + persist below, so
                        // durable state never advances past a write the WAL did not accept.
                        return ExecuteResponse {
                            status: Status::error(
                                "wal_commit_failed",
                                format!("durable WAL commit failed: {err}"),
                            ),
                            response: CommandResponse::Empty,
                        };
                    } else {
                        // Async / bulk mode is a fire-and-forget commit, so an append error does
                        // NOT fail the command (the ack path is intentionally best-effort here).
                        // But it must never be swallowed silently: a dropped async append means
                        // the recovery source of truth is missing this write, so surface it in the
                        // logs (with the failing shard) so operators can see acked-but-undurable
                        // writes instead of discovering them only as post-crash data loss.
                        tracing::error!(
                            shard_id = request.shard_id,
                            error = %err,
                            "async WAL append failed: write acked to the client is NOT durable \
                             and will be lost on a crash before the next flush"
                        );
                    }
                    }
                }
            }
            if !config.async_storage && !bulk_ingest_mode() && !replaying_wal() {
                // Anchor the (in-memory) served index to the WAL sequence it now reflects, so a
                // later load replays only records written after this point (the
                // dumped-log-id anchor read back on load). Reading the sequence via `stats()`
                // triggers a full-file `last_wal_sequence_at` rescan -- an O(records)-per-write cost
                // under this lock (stack-sampling shows it dominates a warm ingest). Under
                // flat append anchor off the O(1) cached last sequence (authoritative right after
                // this write's append) instead; without it the exact `stats()` value is kept.
                shard.applied_wal_sequence = Some(if self.wal_store.flat_append() || raft_apply_batch_active() {
                    self.wal_store.cached_last_sequence(request.shard_id)
                } else {
                    self.wal_store.stats(request.shard_id).last_sequence
                });
                // Append ONLY the blocks this write changed (O(delta)) to the index-log,
                // advancing the index-log sequence and populating the served-index delta
                // stream. The whole base index is NOT rewritten per write (that O(store) path
                // is gone); the base is materialized at compaction/unload, the funnel serves
                // the live in-memory shard between them, and cold reload folds base + deltas.
                let (items, upsert_record) = match upsert_components
                    .as_ref()
                {
                    Some(components) => (
                        collect_upsert_index_items(
                            shard,
                            request.shard_id,
                            components,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                        true,
                    ),
                    None => (
                        collect_command_index_items_for(
                            shard,
                            &delta_command_keys,
                            start_routing_bucket,
                            end_routing_bucket,
                            // A removal that can name its component states that one block, not
                            // every block the object holds.
                            removed_component
                                .as_ref()
                                .map(|(kind, component)| (*kind, component.as_deref())),
                        ),
                        false,
                    ),
                };
                // Capture the authoritative per-key state only when this write produced some
                // that reconstruction from blocks cannot redo: a membership SHRINK, or a
                // DEADLINE CHANGE. See `delta_key_state_change` for both halves and for why
                // the second one has to be asked separately.
                //
                // The capture exists so a reload after WAL replay does not resurrect an entry
                // the write evicted or tombstoned -- reconstruction from physical blocks would
                // otherwise find it again. A write that only ADDED leaves nothing to resurrect:
                // replay rebuilds the same membership from the same blocks.
                //
                // It is not free. Capturing serializes every entry the per-key maps hold for the
                // key, so appending to a node that held 850 events serialized all 850 -- 8,647 of
                // the 8,838 allocations a message write cost, and the reason filling a node cost
                // the square of its length.
                let key_state_changed = delta_key_state_change(shard, &membership_before);
                let mut key_states = if key_state_changed {
                    capture_key_states(shard, &delta_command_keys)
                } else {
                    Vec::new()
                };
                // The container elements this write touched, carried whether or not the capture
                // above ran. NOT gated on `key_state_changed`, and that is the point: that gate
                // asks whether membership SHRANK or a deadline moved, and a pure ADD -- which is
                // the write whose element the fold could not restore -- answers no. The gate's own
                // reasoning is that an add "leaves nothing to resurrect: replay rebuilds the same
                // membership from the same blocks", which is true only because rebuilding decodes
                // the member out of the component. This is what stops that being load-bearing.
                //
                // It costs one blob per touched key holding one entry per touched element, so it
                // does not reintroduce the whole-collection capture that gate exists to avoid.
                key_states.extend(capture_container_element_states(
                    shard,
                    &touched_container_elements,
                ));
                // `durable` fsyncs the delta record before returning. Deferred on the raft
                // apply path (raft log is the durability source) and, under the single-barrier
                // default, on the single-node path too: the record is still written (so the
                // served-index stream is unchanged), but the durable WAL barrier already
                // committed above makes the lost delta tail recoverable by base-only WAL replay,
                // so its fdatasync leaves the ack critical path. Restored to a synchronous
                // barrier only under the TS_WAL_LEGACY_RECOVERY escape hatch (wal_single_barrier
                // false -> delta-fold recovery, which trusts the durable delta).
                let index_log_durable = !raft_applying() && !wal_single_barrier();
                let appended_index_log_sequence = self
                    .index_log_store
                    .append_delta(
                        request.shard_id,
                        items,
                        key_states,
                        shard.applied_wal_sequence,
                        None,
                        upsert_record,
                        index_log_durable,
                    )
                    .unwrap_or(0);
                // The index-log half of what each dirtied bucket holds, stamped the same way as
                // the WAL half above and for the same reason: the reclaim plan keeps two
                // frontiers, counted in two different sequences, and a bucket that can place
                // itself in only one of them can hold neither.
                //
                // `append_delta` returns 0 when it wrote nothing (bulk ingest, or the log
                // disabled), which is exactly "no claim" -- so the guard is the same one.
                if appended_index_log_sequence > 0 {
                    for key in &delta_command_keys {
                        let routing_bucket =
                            block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                        if let Some(bucket) =
                            shard.bucket_index.bucket_map.get_mut(&routing_bucket)
                        {
                            if bucket.first_dirty_index_log_sequence == 0 {
                                bucket.first_dirty_index_log_sequence =
                                    appended_index_log_sequence;
                            }
                        }
                    }
                }
            }
        }
        // Release the `shards` write lock BEFORE the durable barrier. A concurrent same-shard
        // writer can now acquire it, mutate + reserve its own WAL sequence, and enter the
        // group-commit queue WHILE this writer's fdatasync is in flight -- the coalescing window
        // that makes group commit engage (fewer fsyncs than writes). The in-memory mutation and
        // WAL sequence reservation already completed under the lock above, so WAL order == apply
        // order holds regardless of how the (order-independent) barriers interleave. When the
        // concurrent-commit gate is OFF, `pending_barrier_seq` is None and the barrier below is a
        // no-op, so this is byte-identical to the prior in-lock append_with_sync path.
        drop(shards);
        if let Some(barrier_seq) = pending_barrier_seq {
            if raft_apply_batch_active() {
                // Defer to the single coalesced barrier issued for the whole batch by
                // `execute_raft_apply_batch` (the record bytes are already reserved + buffered).
                record_raft_apply_batch_barrier(request.shard_id, barrier_seq);
            } else if let Err(err) = self.wal_store.commit_barrier(request.shard_id, barrier_seq) {
                // The coalesced durable barrier failed: this synchronous write is NOT durable, so
                // surface the failure instead of acking (mirrors the append_with_sync sync-failure
                // path -- the ack is returned strictly after a successful barrier, never before).
                return ExecuteResponse {
                    status: Status::error(
                        "wal_commit_failed",
                        format!("durable WAL commit barrier failed: {err}"),
                    ),
                    response: CommandResponse::Empty,
                };
            }
        }
        // Locks are released and the barrier is done: a safe point to bound the resident set.
        // Not inside the write lock -- moving a block takes the same lock -- and not before the
        // barrier, because the write this call is acking must reach disk first.
        if write_command && !replaying_wal() {
            let limit = wal_resident_block_limit();
            if limit > 0
                && block_in_wal::registration_count(&self.block_store, request.shard_id) > limit
            {
                let moved = self.materialize_oldest_resident_blocks(
                    request.shard_id,
                    wal_resident_block_floor(limit),
                );
                if moved > 0 {
                    RESIDENT_SWEEPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    fn execute_read_only_fast_path(&self, request: &ExecuteRequest) -> Option<ExecuteResponse> {
        let read_command = matches!(
            request.command,
            Command::StringGet { .. } | Command::HashGetAll { .. }
        );
        if !read_command {
            return None;
        }
        // A shard replaying its WAL on load is present but not yet serving (keeps it in
        // PartitionLoadStage::LOADING). The durable / replicated read routes reach this fast
        // path BEFORE execute_with_storage_override's recovering gate, so without this a read
        // would be served from half-reconstructed state (and skip admission). Decline the fast
        // path while recovering so the slow path rejects uniformly with a retryable
        // shard_not_loaded. The replay thread reads under replaying_wal(), which bypasses this.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            return None;
        }
        // The addresses come from the shard index and the BYTES come from the block store, so
        // the guard covers the lookup only. A read guard admits other readers and excludes every
        // WRITER, and this path held it across one block-store read PER FIELD -- so a wide hash
        // stopped all writes on the shard for as many reads as the value had fields. See
        // `FastPathRead` for why releasing first is safe and what the one scope still buys.
        let hold_guard_across_reads = shard_write_guard::serving_holds_guard_across_reads();
        let (plan, _held_across_reads) = {
            let shards = self.shards_read_marked();
            let Some(shard) = shards.get(&request.shard_id) else {
                return Some(ExecuteResponse {
                    status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                    response: CommandResponse::Empty,
                });
            };
            let plan = match &request.command {
                Command::StringGet { key } => {
                    if shard
                        .expires_at_ms()
                        .get(key.as_str())
                        .map(|expires_at| *expires_at <= resolve_now_ms())
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
                    FastPathRead::String {
                        key: key.as_str(),
                        address: shard.string_address(key.as_str()),
                        routing_bucket: block_routing_bucket(
                            key,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                    }
                }
                Command::HashGetAll { key } => {
                    if shard
                        .expires_at_ms()
                        .get(key.as_str())
                        .map(|expires_at| *expires_at <= resolve_now_ms())
                        .unwrap_or(false)
                    {
                        return None;
                    }
                    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
                    FastPathRead::Hash {
                        key: key.as_str(),
                        fields: shard
                            .hashes
                            .get(key)
                            .map(|fields| {
                                fields
                                    .iter()
                                    .map(|(field, address)| (field.clone(), address.clone()))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default(),
                        routing_bucket: block_routing_bucket(
                            key,
                            start_routing_bucket,
                            end_routing_bucket,
                        ),
                    }
                }
                _ => return None,
            };
            // The measurement's control arm carries the guard out of this block so the reads
            // below still happen inside the region. `None` in every shipped build, which is what
            // makes the guard drop here.
            let held = if hold_guard_across_reads {
                Some(shards)
            } else {
                None
            };
            (plan, held)
        };

        match plan {
            FastPathRead::String {
                key,
                address,
                routing_bucket,
            } => {
                // THE SHARD HAS ALREADY SAID NO, SO NOTHING BELOW IT IS ASKED.
                //
                // `address` is `shard.string_address(key)` -- one map get against `shard.strings`,
                // taken under the shard lock in the block above -- and it is the ONLY thing the
                // source closure below consults. `None` from it is the answer this arm returns
                // whatever any cache tier holds. Letting the cache be asked first therefore paid
                // for a question the authority that owns it had already answered.
                //
                // WHAT THAT COST, MEASURED. `cached_response` is cache-first, so an absent key
                // descended every tier: the memory tier, the persistent-memory tier, the SSD
                // tier's store, and then `fs::read` on the entry's own disk path -- a failing
                // `openat` for a key that was never written. One descent per distinct absent key,
                // and then one cache WRITE, because the negative answer was admitted to the memory
                // tier. `what_an_absent_key_costs` measured 1.000 descents and 1.000 writes per
                // lookup, flat across 500, 2,000 and 8,000-key stores, in all three bucket
                // residencies -- resident, released, and no bucket at all. That is the shape a
                // retry burst after a cold start or a failover multiplies: a set of answers that
                // are all "no", each one paying a tier descent and admitting an entry that evicts
                // a live one to hold it.
                //
                // AND THE BLOCK STORE WAS NEVER IN IT, WHICH IS THE OTHER HALF OF THE READING.
                // The same measurement reads 0 block-store reads per absent lookup in every arm,
                // so an absent key never forced a bucket reload or a page read, and releasing a
                // bucket never made absence cost more. A release drops a bucket's `block_index`
                // and its object-lookup entries and leaves the model maps alone -- they are what
                // `reload_released_bucket` rebuilds FROM -- so `string_address` answers a released
                // bucket's keys exactly as it answers a resident one's. This path can tell
                // "absent" from "not resident" because it never has to ask.
                //
                // IT IS ALSO THE SAFER ORDER, NOT MERELY THE CHEAPER ONE. #1907 refused to defer
                // the delete_drop cache invalidation past its guard on the grounds that
                // `cached_response` is cache-first and a record `CacheKey` carries no generation,
                // sequence or version stamp -- so a reader inside that window is answered a value
                // the shard has already deleted, 8 of 533 reads as measured there. For a key the
                // shard no longer holds, this arm now consults the shard's verdict first and the
                // cache cannot answer at all.
                //
                // THE SIBLING ARM ALREADY DOES THIS, which is why it is not a new rule. The
                // `FastPathRead::Hash` arm below takes its fields from `shard.hashes` and
                // `filter_map`s over them, so an absent hash makes no cache call whatever. This is
                // the string arm being brought level with the one beside it.
                let Some(address) = address else {
                    return Some(ExecuteResponse {
                        status: Status::ok(),
                        response: CommandResponse::Bytes { value: None },
                    });
                };
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: cached_response(
                        &self.cache,
                        CacheKey::string(request.shard_id, key),
                        || CommandResponse::Bytes {
                            value: read_block_bytes(
                                &self.cache,
                                &self.block_store,
                                request.shard_id,
                                &address,
                                // A string block IS its whole object, so the element is `None`.
                                BlockIdentity::of(request.shard_id, "string", key, None),
                                Some(routing_bucket),
                            ),
                        },
                    ),
                })
            }
            FastPathRead::Hash {
                key,
                fields,
                routing_bucket,
            } => {
                let mut entries = fields
                    .iter()
                    .filter_map(|(field, address)| {
                        read_block_bytes(
                            &self.cache,
                            &self.block_store,
                            request.shard_id,
                            address,
                            BlockIdentity::of(request.shard_id, "hash", key, Some(&field[..])),
                            Some(routing_bucket),
                        )
                        .map(|value| (field.to_string(), value))
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                Some(ExecuteResponse {
                    status: Status::ok(),
                    response: CommandResponse::HashEntries { entries },
                })
            }
        }
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        });
        CheckedExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    fn check_admission(
        &self,
        shard_id: ShardId,
        write_command: bool,
        config: &Config,
        info: &Option<ShardInfo>,
    ) -> Result<(), Status> {
        let limits = admission_limits(shard_id, write_command, config, info);
        if limits.is_empty() {
            return Ok(());
        }
        let now_sec = now_epoch_seconds();
        let mut admissions = self.admissions.write().expect("admission lock poisoned");
        for limit in &limits {
            if limit.limit == 0 {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} is zero", limit.label),
                ));
            }
            let admission = admissions.entry(limit.scope.clone()).or_default();
            reset_admission_window(admission, now_sec);
            let count = admission_count(admission, write_command);
            if *count >= limit.limit {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} limit exceeded", limit.label),
                ));
            }
        }
        for limit in limits {
            let admission = admissions.entry(limit.scope).or_default();
            reset_admission_window(admission, now_sec);
            *admission_count(admission, write_command) += 1;
        }
        Ok(())
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        if !self.is_shard_loaded(request.shard_id) {
            return Status::error("shard_not_found", "shard is not loaded");
        }
        let mut configs = self.configs.write().expect("config lock poisoned");
        let current = configs.get(&request.shard_id).cloned().unwrap_or_default();
        if request.config.version < current.version {
            return Status::error("failed_precondition", "legacy config version");
        }
        if request.config.version == current.version {
            return Status::ok();
        }
        let applied = request.config.clone();
        configs.insert(request.shard_id, request.config);
        // Drop the config lock before touching the WAL/disk so the durable append never runs
        // under the config mutex.
        drop(configs);
        // Durably log the config so it survives reload REGARDLESS of barrier mode. Runtime config
        // (feature_max_size + the representation-changing extend gate flags: control_rollup /
        // coalesce / distinct_sketch) is NOT carried in the served-index checkpoint, so without a
        // durable config-log a reload defaults `Config` and silently resets these -- which for the
        // representation flags can misread already-written data. Stamp the change with the current
        // WAL frontier (effective for every write with a strictly greater sequence) and fsync it.
        // Config changes are rare admin ops, so this barrier is off the per-write hot path. In
        // single-barrier mode WAL-tail replay additionally re-derives config-driven trims at this
        // exact frontier; in every other mode the last entry is simply restored as the live config
        // on load (see load_shard_with / replay_wal_into_shard).
        let after_seq = self.wal_store.stats(request.shard_id).last_sequence;
        if let Err(err) = self.append_config_log_entry(request.shard_id, after_seq, &applied) {
            tracing::warn!(
                shard_id = request.shard_id,
                error = %err,
                "failed to persist config-log entry"
            );
        }
        Status::ok()
    }

    /// Durable, WAL-sequence-ordered config-log path for a shard (single-barrier mode).
    pub(super) fn config_log_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir
            .join(format!("shard-{shard_id}.configlog.jsonl"))
    }

    /// Append one config-log entry `{after_seq, config}` and fsync it. `after_seq` is the WAL
    /// sequence the config became effective AFTER (it applies to writes with sequence >
    /// after_seq). Append-only + fsync'd so a crash cannot lose an acked config change.
    pub(super) fn append_config_log_entry(
        &self,
        shard_id: ShardId,
        after_seq: u64,
        config: &Config,
    ) -> std::io::Result<()> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.index_dir)?;
        let entry = ConfigLogEntry {
            after_seq,
            config: config.clone(),
        };
        let mut bytes = serde_json::to_vec(&entry)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        bytes.push(b'\n');
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config_log_path(shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_data()?;
        Ok(())
    }

    /// Read the config-log entries for a shard, sorted by ascending `after_seq` (stable). Empty
    /// when the shard has no config-log (no single-barrier config change was ever persisted).
    pub(super) fn config_log_entries(&self, shard_id: ShardId) -> Vec<ConfigLogEntry> {
        let path = self.config_log_path(shard_id);
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(_) => return Vec::new(),
        };
        let mut entries: Vec<ConfigLogEntry> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<ConfigLogEntry>(line).ok())
            .collect();
        entries.sort_by_key(|entry| entry.after_seq);
        entries
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
        if !self.is_shard_loaded(shard_id) {
            return GetConfigResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
                config: Config::default(),
            };
        }
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        GetConfigResponse {
            status: Status::ok(),
            config,
        }
    }

    fn is_shard_loaded(&self, shard_id: ShardId) -> bool {
        self.infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
    }

    pub fn get_info(&self, shard_id: ShardId) -> GetInfoResponse {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        GetInfoResponse {
            status: if info.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            info,
        }
    }

    pub fn update_membership(&self, request: MembershipUpdateRequest) -> Status {
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            if request.membership_version < info.membership_version {
                return Status::error("failed_precondition", "legacy membership info");
            }
            let global_update = request.membership_version > info.membership_version;
            if !global_update
                && request.replica_membership_version == info.replica_membership_version
            {
                return Status::ok();
            }
            if request.replica_membership_version < info.replica_membership_version {
                return Status::error("failed_precondition", "legacy membership unit info");
            }
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            info.membership_version = request.membership_version;
            info.replica_membership_version = request.replica_membership_version;
            info.membership_valid = info
                .local_node_id
                .map(|node_id| info.replica_node_ids.contains(&node_id))
                .unwrap_or(true);
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let stats = self.shard_stats(shard_id);
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn rust_storage_observation(&self, shard_id: ShardId) -> Option<RustStorageObservation> {
        self.shard_stats(shard_id)
            .map(|stats| RustStorageObservation {
                shard_id,
                observed_memory_hit: stats.cache.memory_hits > 0,
                observed_block_cache_hit: stats.cache.disk_hits > 0,
                observed_local_file_read: stats.block_store_compat.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_block_bytes_written: stats.block_store_compat.bytes_written,
                local_block_bytes_read: stats.block_store_compat.bytes_read,
                cache: stats.cache,
                block_store: stats.block_store_compat,
            })
    }

    #[doc(hidden)]
    pub fn string_block_cache_key_for_test(&self, shard_id: ShardId, key: &str) -> Option<CacheKey> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id)?;
        let address = shard.strings().get(key)?;
        let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
        Some(CacheKey::page_with_slot(
            shard_id,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(block_routing_bucket(key, start_routing_bucket, end_routing_bucket))))
    }

    #[doc(hidden)]
    pub fn clear_string_model_view_for_test(&self, shard_id: ShardId, key: &str) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        shards
            .get_mut(&shard_id)
            .and_then(|shard| shard.remove_string_row(key))
            .is_some()
    }

    pub fn loaded_shard_stats(&self) -> Vec<ShardStats> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.shard_stats(shard_id))
            .collect()
    }

    pub fn loaded_shard_ids(&self) -> Vec<ShardId> {
        let mut shard_ids = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids
    }

    pub fn bucket_storage_summaries(&self, shard_id: ShardId) -> Vec<BucketStorageSummary> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        let summaries = bucket_storage_summaries(shard, start, end);
        if let Some(manifest) = latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id) {
            merge_last_dump_sequence(summaries, &manifest)
        } else {
            summaries
        }
    }

    pub fn storage_physical_index_report(&self, shard_id: ShardId) -> StoragePhysicalIndexReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StoragePhysicalIndexReport {
                shard_id,
                bucket_first: true,
                ..StoragePhysicalIndexReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        let summaries = bucket_storage_summaries(shard, start, end);
        let summaries =
            if let Some(manifest) = latest_bucket_dump_manifest_shared_at(&self.index_dir, shard_id) {
                merge_last_dump_sequence(summaries, &manifest)
            } else {
                summaries
            };
        storage_physical_index_report(shard_id, shard, summaries)
    }

    pub fn bucket_object_block_ownership_report(
        &self,
        shard_id: ShardId,
    ) -> BucketObjectBlockOwnershipReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return BucketObjectBlockOwnershipReport {
                shard_id,
                ..BucketObjectBlockOwnershipReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        bucket_object_block_ownership_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_bucket)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_bucket)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn object_manager_runtime_report(&self, shard_id: ShardId) -> ObjectManagerRuntimeReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return ObjectManagerRuntimeReport {
                shard_id,
                blockers: vec!["shard is not loaded".to_string()],
                ..ObjectManagerRuntimeReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        object_manager_runtime_report(
            shard_id,
            shard,
            info.as_ref()
                .map(|info| info.start_routing_bucket)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_bucket)
                .unwrap_or(u32::MAX),
        )
    }

    pub fn storage_data_structure_api_parity_report(
        &self,
        shard_id: ShardId,
    ) -> StorageDataStructureApiParityReport {
        let physical_index = self.storage_physical_index_report(shard_id);
        let ownership = self.bucket_object_block_ownership_report(shard_id);
        let object_manager = self.object_manager_runtime_report(shard_id);
        let slab_reports = self.block_store.slab_reports().unwrap_or_default();
        let block_index_count = slab_reports
            .iter()
            .map(|slab| slab.block_index_count)
            .sum::<u64>();
        // The checksum is asked for only when checksum recording is on. It is an opt-in
        // diagnostic (TS_BLOCK_INDEX_CHECKSUMS, default off, because recomputing it at every
        // engine open dominated slab verification), and the integrity it would attest to is
        // already verified by decode_block_record on the way in. Requiring it unconditionally
        // made this report ready: false on every default deployment. The addressing fields are
        // what a complete block address API means; the digest is a hand-inspection aid.
        let checksums_recorded = crate::block_store::block_index_checksums_enabled();
        // What a slab walk can know, which is what a block record carries: where the block is,
        // which block of its object it is, and its checksum. The object id and the routing
        // bucket are NOT among them any more -- they live in the index, and a record that
        // repeated them could only ever agree with the index or be wrong. Asking a slab-derived
        // entry for them would report the address API permanently incomplete for a reason that
        // is by design.
        let block_address_api_ready = slab_reports.iter().any(|slab| {
            slab.block_index_entries.iter().any(|entry| {
                entry.compact_slab_address.is_some()
                    && entry.compact_slab_id.is_some()
                    && entry.compact_slab_offset.is_some()
                    && entry.block_id.is_some()
                    && (!checksums_recorded || entry.checksum.is_some())
            })
        });
        let slab_report = self.block_store.stream_backed_slab_runtime_report().ok();
        let stream_backed_slab_api_ready = slab_report
            .as_ref()
            .map(|report| {
                report.slab_manifest_ready
                    && report.slab_manifest_disk_consistent
                    && report.slab_stats_ready
                    && report.stream_record_count > 0
                    && report.blockers.iter().all(|blocker| {
                        blocker.contains("append/roll") || blocker.contains("purge lifecycle")
                    })
            })
            .unwrap_or(false);
        let storage_manager = self.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id,
            dry_run: true,
            ..StorageManagerCycleRequest::default()
        });
        let expected_stages = [
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ];
        let storage_manager_phase_api_ready = storage_manager.completed
            && expected_stages.iter().all(|stage| {
                storage_manager
                    .native_stage_order
                    .iter()
                    .any(|observed| observed == stage)
                    && storage_manager
                        .stages
                        .iter()
                        .any(|observed| observed.stage == *stage)
            });
        let storage_manager_pressure_api_ready =
            storage_manager.pressure_signals.total_pressure_score
                >= storage_manager.pressure_signals.dirty_bucket_count as u64
                && storage_manager
                    .stages
                    .iter()
                    .any(|stage| stage.pressure_triggered || stage.pressure_score > 0);
        let storage_manager_merged_dump_load_api_ready =
            storage_manager.merged_dump_load_policy.policy_ready
                || storage_manager
                    .merged_dump_load_policy
                    .blockers
                    .iter()
                    .all(|blocker| blocker.contains("no dirty slots"));
        let bucket_store_layout_api_ready = physical_index.bucket_nodes.iter().any(|bucket| {
            matches!(
                bucket.layout.as_str(),
                "single_object" | "single_block_object" | "multi_block_object" | "multi_object"
            )
        });
        let mut blockers = Vec::new();
        if !physical_index.bucket_index_authority || !ownership.first_class_index_present {
            blockers.push("slot_object_block_authority_missing".to_string());
        }
        if !bucket_store_layout_api_ready {
            blockers.push("slot_store_layout_states_missing".to_string());
        }
        if !object_manager.runtime_ready {
            blockers.push("object_manager_runtime_not_ready".to_string());
        }
        if !block_address_api_ready {
            blockers.push("block_address_metadata_incomplete".to_string());
        }
        if block_index_count == 0 {
            blockers.push("block_store_segment_index_missing".to_string());
        }
        if !stream_backed_slab_api_ready {
            blockers.push("stream_backed_band_api_not_ready".to_string());
        }
        if !storage_manager_phase_api_ready {
            blockers.push("storage_manager_phase_api_incomplete".to_string());
        }
        if !storage_manager_pressure_api_ready {
            blockers.push("storage_manager_pressure_api_incomplete".to_string());
        }
        if !storage_manager_merged_dump_load_api_ready {
            blockers.push("storage_manager_merged_dump_load_api_incomplete".to_string());
        }
        let legacy_block_slab_aliases_ready = true;
        StorageDataStructureApiParityReport {
            shard_id,
            ready: blockers.is_empty() && legacy_block_slab_aliases_ready,
            bucket_object_block_authority_ready: physical_index.bucket_index_authority
                && ownership.first_class_index_present
                && !ownership.derived_from_model_maps,
            bucket_store_layout_api_ready,
            object_manager_runtime_api_ready: object_manager.runtime_ready,
            block_address_api_ready,
            block_store_slab_api_ready: block_index_count > 0,
            stream_backed_slab_api_ready,
            legacy_block_slab_aliases_ready,
            storage_manager_phase_api_ready,
            storage_manager_pressure_api_ready,
            storage_manager_merged_dump_load_api_ready,
            bucket_count: physical_index.bucket_count,
            page_index_count: physical_index.block_index_count,
            block_index_count,
            stream_slab_count: slab_report
                .as_ref()
                .map(|report| report.slab_count)
                .unwrap_or_default(),
            stream_record_count: slab_report
                .as_ref()
                .map(|report| report.stream_record_count)
                .unwrap_or_default(),
            storage_manager_stage_order: storage_manager.native_stage_order,
            blockers,
            evidence: vec![
                "slot/object/page authority is reported from the first-class slot index"
                    .to_string(),
                "block addresses expose segment, offset, length, block id, object id, routing slot and band id, plus a payload checksum when checksum recording is enabled"
                    .to_string(),
                "stream-backed storage exposes active/sealed/delayed-destroy/purged band lifecycle while accepting legacy zone aliases"
                    .to_string(),
                "StorageManager exposes standard prepare/reclaim/expire/evict/reclaim-page/index-GC/compact/reap-metrics phases"
                    .to_string(),
            ],
        }
    }

    /// THE SHARD'S OWN ROUTING RANGE, from the info row the engine publishes when it loads it.
    ///
    /// Every rebuild of the bucket index takes this range as an argument, and it decides WHERE A
    /// BLOCK IS FILED: an address that arrives carrying no routing bucket of its own is filed under
    /// `block_routing_bucket(key, start, end)`, and `bucket_for_object` reduces the key's hash
    /// modulo the bucket COUNT. So the range is not a filter over a fixed answer, it is an
    /// argument to the placement function -- pass `0, u32::MAX` on a shard loaded as `0..1023` and
    /// every unrouted block is filed under a bucket the shard does not hold.
    ///
    /// THE MISSING-INFO DEFAULT IS THE WHOLE RANGE, deliberately. That is what a caller which
    /// could not reach the info row passed unconditionally before, so a shard whose info row is
    /// absent -- a dump manifest installed before its shard is loaded -- keeps exactly the
    /// behaviour it had, and a shard that IS loaded gets the range it was loaded with. The same
    /// default `routing_bucket_for_key` has always used, now shared rather than restated.
    pub(crate) fn shard_routing_range(&self, shard_id: ShardId) -> (u32, u32) {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        (start, end)
    }

    pub fn routing_bucket_for_key(&self, shard_id: ShardId, key: &str) -> u32 {
        let (start, end) = self.shard_routing_range(shard_id);
        block_routing_bucket(key, start, end)
    }

    /// THE ONE WAY A `ShardState` ENTERS THE SERVED MAP, and the one place its routing range is
    /// stamped.
    ///
    /// The two functions that decide where a block is FILED never see `&self`, so the range has to
    /// reach them on the shard itself. Making the stamp part of the install, rather than a line
    /// every install site has to remember, is what makes "every served shard knows its range" a
    /// property of the code instead of a convention -- and there is nothing to forget at a new
    /// install site, because there is no other way to insert one.
    ///
    /// THE INFO ROW IS READ OUTSIDE THE SHARDS WRITE LOCK, the same order
    /// `storage_dump_shard_index` and `materialize_resident_blocks_where` already take:
    /// `shard_routing_range` takes `infos.read()`, and taking it while holding `shards.write()`
    /// would be a second lock in a new order. Every caller publishes the info row BEFORE handing
    /// the state over, so the range read here is the one the shard was loaded with.
    pub(crate) fn install_shard_state(
        &self,
        shard_id: ShardId,
        mut state: crate::engine::state::ShardState,
    ) {
        let (start_routing_bucket, end_routing_bucket) = self.shard_routing_range(shard_id);
        state.set_routing_range(start_routing_bucket, end_routing_bucket);
        // The id it is being INSERTED UNDER, so the shard and the key the served map routes it by
        // can never be two different numbers.
        state.set_shard_id(shard_id);
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(shard_id, state);
    }

    /// What the resident bucket index costs on this shard: one node per bucket, plus one entry
    /// per block the bucket holds.
    ///
    /// The published `bucket_index_resident_bytes_floor` counts NODES only. That is a floor and
    /// says so, but it also cannot move when a bucket is released -- the node is exactly what a
    /// release keeps. The per-block entries are what grows with the corpus and what a release
    /// frees, so this is the number to gate on and to assert against.
    pub fn bucket_index_resident_bytes(&self, shard_id: ShardId) -> u64 {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(crate::engine::storage_bucket_internals::bucket_index_resident_bytes)
            .unwrap_or_default()
    }

    /// Release the named buckets' resident block lists, keeping each node routable and reloadable.
    ///
    /// Returns `(buckets released, pages released, candidates refused)`. Every precondition is
    /// checked against live state inside; naming a bucket that cannot be released is refused, not
    /// forced.
    pub fn release_bucket_index_blocks(
        &self,
        shard_id: ShardId,
        buckets: Vec<u32>,
    ) -> (usize, usize, usize) {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return (0, 0, 0);
        };
        let outcome = crate::engine::storage_bucket_internals::release_bucket_blocks(shard, &buckets);
        (
            outcome.released_buckets.len(),
            outcome.released_blocks,
            outcome.refused_buckets,
        )
    }

    /// Offer every bucket on the shard for release. The whole-shard form of the call above.
    pub fn release_all_releasable_bucket_index_blocks(
        &self,
        shard_id: ShardId,
    ) -> (usize, usize, usize) {
        let candidates = {
            let shards = self.shards.read().expect("engine lock poisoned");
            shards
                .get(&shard_id)
                .map(|shard| shard.bucket_index.bucket_map.keys().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        self.release_bucket_index_blocks(shard_id, candidates)
    }

    /// Load a released bucket's block list back. False when the bucket was not released.
    pub fn reload_released_bucket_index_blocks(
        &self,
        shard_id: ShardId,
        routing_bucket: u32,
    ) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return false;
        };
        crate::engine::storage_bucket_internals::reload_released_bucket(
            shard,
            shard_id,
            routing_bucket,
        )
    }

    /// The buckets currently released on this shard, in routing order.
    pub fn released_bucket_index_buckets(&self, shard_id: ShardId) -> Vec<u32> {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(|shard| {
                shard
                    .bucket_index
                    .released_buckets
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

}

/// Inclusive `[start, end]` timestamp bounds for `BTreeMap::range` that yield an EMPTY range
/// (never panic) when `start > end`. `BTreeMap::range` panics on reversed bounds, and every
/// range query runs under the shard write lock, so a client sending `start_ms > end_ms` would
/// poison the lock and take the whole shard down (every later `.lock().expect()` panics).
/// `RangeGet` simply iterates and returns an empty result with Status::OK when `min > max`
/// so match that: reversed bounds → empty range, not a crash. For
/// `start <= end` this is byte-for-byte the same set as `start..=end`.
pub(crate) fn timestamp_range_bounds(
    start: u64,
    end: u64,
) -> (std::ops::Bound<u64>, std::ops::Bound<u64>) {
    use std::ops::Bound;
    if start <= end {
        (Bound::Included(start), Bound::Included(end))
    } else {
        // Empty, non-panicking: `[1, 1)` contains nothing and is a valid (not both-excluded) range.
        (Bound::Included(1), Bound::Excluded(1))
    }
}

/// Whether this process is running a bulk backfill, which defers per-append durability work
/// to an explicit `sync_durable`.
///
/// The one reader of `MATRIXARK_BULK_INGEST`. block_store, index_log and wal each parsed it
/// themselves and gate their own half of the same decision; a copy that drifted would leave one
/// subsystem in bulk mode and the rest on the live path.
pub(crate) fn bulk_ingest_mode() -> bool {
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether the per-command model-map -> bucket-index promotion and first-index
/// rebuild should be DEFERRED to a single reconstruct pass. True during bulk
/// backfill (MATRIXARK_BULK_INGEST) and during WAL replay on load: both re-drive
/// many already-committed writes with no interleaved client reads, so running the
/// O(store) promote/rebuild per command is the dominant O(n^2) cost. Deferring is
/// lossless because point string/hash/set reads+writes maintain the bucket_map /
/// object_page_lookup directly via upsert_bucket_index_block/read_bucket_index_value,
/// and the deferred context (model-map) records are append-only until the single
/// reconstruct folds them in (bulk: flush_shard_index(); replay: replay_wal_into_shard()).
fn defer_bucket_index_reconstruct() -> bool {
    bulk_ingest_mode() || replaying_wal() || coalescing_index_reconstruct()
}

/// Whether load_shard should eagerly warm the in-memory cache tier from the block
/// store after reconstructing the index (disk->memory promotion on restart).
/// Defaults ON; set MATRIXARK_EAGER_CACHE_WARM_ON_LOAD to 0/false/off/no to disable.
pub(crate) fn eager_cache_warm_on_load() -> bool {
    !matches!(
        std::env::var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// How many logically deleted keys a shard may accumulate before its next served-index
/// checkpoint re-encodes the whole base index.
///
/// WHY THERE IS AN INTERVAL AT ALL. `apply_storage_eviction`'s delete_drop round re-encodes and
/// rewrites the ENTIRE served index once per round, however few objects the round dropped.
/// `what_one_eviction_round_writes_grows_with_the_store_not_the_round` prices that: a round that
/// dropped 2 objects wrote 36,097 encoded bytes at 500 resident keys, 155,793 at 2,000 and
/// 609,692 at 8,000 -- about 76 bytes per RESIDENT key, so the cost of a round is set by the size
/// of the store and not by the size of the round.
///
/// WHY DEFERRING IT IS SOUND, in the words the write path already uses for the same decision
/// (`atomic_write_bytes_synced`): "the WAL (durably synced before ack) is the recovery source of
/// truth and replay rebuilds the served index from it, so a stale-on-crash index just replays a
/// longer WAL suffix -- no acked write is lost". A delete_drop round emits a `CommonDelete`
/// tombstone per dropped key BEFORE it flushes and anchors `applied_wal_sequence` past them, so a
/// checkpoint that lands late, lands stale or never lands is re-derived by replay. The flush is
/// bounding REPLAY LENGTH, not carrying durability, and a count of deletions bounds replay length
/// just as well per interval as per round.
///
/// WHY 64. The interval is counted in DELETED KEYS rather than in rounds, which makes it a bound
/// on the index bytes a single deletion can be charged: at 8,000 resident keys one flush is
/// 309,398 bytes, so one flush per 64 deletions charges 4.8 kB per deletion instead of 154,699.
/// It also makes a large round pay promptly -- a round dropping 4,000 keys is due immediately,
/// which is right, because its flush is already amortized over 4,000 deletions. The price is the
/// replay suffix: at most 63 un-checkpointed `CommonDelete` records, tens of bytes each, against
/// the ~9.9 MB of whole-index encoding those same 64 deletions cost before.
///
/// Must be greater than 1: at 1 every round is due and the behaviour is exactly what it was, so a
/// default of 1 would land this inert. `the_logged_deletion_flush_interval_is_a_real_setting`
/// holds that.
pub(crate) const LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT: u64 = 64;

/// The configured interval, read through the crate's one numeric-flag vocabulary.
///
/// Zero is not a request for zero: an interval of zero has no meaning for a countdown and would
/// read as "already due" on every round, so it is clamped to 1. One IS meaningful and is left
/// reachable -- it is the per-round flush this change replaces, and the measurement needs an arm
/// that still pays it.
pub(crate) fn logged_deletion_index_flush_interval_from_env() -> u64 {
    let configured: u64 = crate::env_flag::env_number(
        "TS_LOGGED_DELETION_INDEX_FLUSH_INTERVAL",
        LOGGED_DELETION_INDEX_FLUSH_INTERVAL_DEFAULT,
    );
    configured.max(1)
}

/// How many published keys a shard may accumulate before its next served-index checkpoint
/// re-encodes the whole base index.
///
/// WHY THERE IS AN INTERVAL AT ALL. `publish_shard_index_snapshot_for_keys` republishes the pages
/// of the keys it was NAMED and then encodes and rewrites the ENTIRE served index, however few
/// keys that was -- its own comment calls that "the O(n^2) rewrite".
/// `what_one_visibility_publish_writes_grows_with_the_store_not_the_publish` prices it from both
/// sides: holding the named set at one key and widening the store multiplies the encode, while
/// holding the store and widening the named set 256-fold does not. So the page half of a publish
/// is already O(published) and the index half was O(store).
///
/// WHY DEFERRING IT IS SOUND, in the words the write path already uses for the same decision
/// (`atomic_write_bytes_synced`): "the WAL (durably synced before ack) is the recovery source of
/// truth and replay rebuilds the served index from it, so a stale-on-crash index just replays a
/// longer WAL suffix -- no acked write is lost". A publish relocates log-resident blocks into the
/// block store and repoints the index rows at them; it does NOT retire the blocks' registrations
/// and it does NOT clear any bucket's `first_dirty_wal_sequence`, which is written to zero at
/// exactly one site and only by a durable dump. Both of the WAL retention floors therefore still
/// sit below the records the publish rewrote: `min_registered_sequence` (the registry) and the
/// per-bucket first-dirty sequence (the reclaim plan). A checkpoint that lands late, lands stale
/// or never lands is re-derived by replaying those records, which is what
/// `a_deferred_publish_checkpoint_still_reloads_with_the_published_values` drives rather than
/// argues.
///
/// WHY 64. The interval is counted in PUBLISHED KEYS rather than in calls, which makes it a bound
/// on the index bytes a single published key can be charged, and makes a large publish pay
/// promptly -- a call publishing 4,000 keys is due immediately, which is right, because its
/// encode is already amortized over 4,000 keys. The price is the replay suffix: at most 63
/// un-checkpointed keys' records, against the whole-store encode each of those keys cost before.
///
/// Must be greater than 1: at 1 every publish is due and the behaviour is exactly what it was, so
/// a default of 1 would land this inert.
/// `the_visibility_publish_flush_interval_is_a_real_setting_whose_default_is_not_one` holds that.
pub(crate) const VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT: u64 = 64;

/// The configured interval, read through the crate's one numeric-flag vocabulary.
///
/// Zero is not a request for zero: an interval of zero has no meaning for a countdown and would
/// read as "already due" on every publish, so it is clamped to 1. One IS meaningful and is left
/// reachable -- it is the per-publish encode this change replaces, and the measurement needs an
/// arm that still pays it.
pub(crate) fn visibility_publish_index_flush_interval_from_env() -> u64 {
    let configured: u64 = crate::env_flag::env_number(
        "TS_VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL",
        VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT,
    );
    configured.max(1)
}

/// Current on-disk shape of a shard index.
///
/// 1 = pre-rekey: context_events keyed by timeline_key.
/// 2 = context_events keyed by event_id_hash, with context_event_timeline carrying time order.
/// 3 = the object id is `(shard, kind, key)`: an object, not one of its blocks.
///
/// Bump this whenever a field's MEANING changes, not only when its type does -- a same-typed
/// reinterpretation is the case that decodes cleanly and serves wrong data.
///
/// 3 IS THAT CASE, EXACTLY AS THE SENTENCE ABOVE DESCRIBES IT, and this is why the bump is not
/// optional. `oi` is a `u64` before and a `u64` after, so an index written by the previous binary
/// decodes without complaint and every value in it is intact. What moved is what the number
/// MEANS: it used to name a block (shard, kind, key, component) and now names an object
/// (shard, kind, key).
///
/// Nothing in the decode can see that. `BlockAddress::try_from` compares the stored `g` against
/// `block_id.or(object_id)`, and BOTH sides of that comparison come off the wire -- an old row
/// agrees with itself and is admitted. The disagreement appears later and elsewhere:
/// `bucket.object_index` is filled from `address.object_id()`, the STORED value, so it would hold
/// component-folded ids, while `validate_bucket_ownership_index_from_entries` recomputes
/// `stable_block_object_id(shard, kind, key)` under the new rule and asks
/// `object_index.contains(..)`. Every container page in such a store answers no. That runs on a
/// recovery path (`recovery_sweep_compact`, four call sites), not in a test.
///
/// A stale stamp is treated as an ABSENT index and the caller replays the log, which rebuilds
/// every id under one rule. Slower and correct, which is the trade this stamp exists to make.
///
/// 5 = a container page may state that one of its items was REMOVED, and a removal therefore leaves
///     a page and an entry behind rather than only dropping the entry.
///
/// # WHY 5 AND NOT 4, WHICH IS FREE
///
/// 4 IS RESERVED AND UNSPENT, and this is the number that is not chosen by looking at the tree. A
/// concurrent change takes `object_id` out of `BlockAddress` and needs 4 for it; at the time this
/// landed that change had no open pull request, so nothing in `git log` or `gh pr list` claims 4 and
/// a reader checking would conclude 4 was available. Taking it anyway would be the error.
///
/// The asymmetry is what decides it. A HOLE in the sequence costs nothing: the stamp is only ever
/// compared for equality with the constant, never ordered or ranged, so 4 never existing is
/// invisible to every reader. A COLLISION costs a silent misread -- two different stored shapes both
/// stamped 4, each admitted by the other's binary because the stamp agrees, which is precisely the
/// failure this constant exists to make impossible and which it would then be causing. So the
/// expensive mistake is the one the tree's silence invites, and the cheap one is the one to make.
///
/// # WHAT THE BUMP DOES TO A FOLLOWER, AND TO REPLICATION
///
/// `SharedStoreStagedBlockProto` is the frame a follower reads, and a container page's payload
/// travels inside it as opaque bytes. A follower on the previous binary handed a second-shape page
/// therefore stages it intact and can serve it -- and would then read its tombstone item as an empty
/// live value, resurrecting the element. That is the mixed-version case, and it is refused earlier
/// than the page: this stamp makes such a follower treat the published index as ABSENT and replay,
/// and mixed-version clusters are already documented as unsupported. The page format is not what
/// makes them unsupported; it is one more reason they are.
///
/// The four encoders are NOT equally affected and the choice of which to touch was deliberate. The
/// index log is positional (`encode_index_payload_into`, plain `rmp_serde`) and REFUSES on length, so
/// a field added to a stored row breaks it loudly -- none is added here. The served index is named
/// (`.with_struct_map()`) and SILENTLY DROPS a retired key, which is what this stamp guards and why
/// the bump is not optional. The WAL record is protobuf plus a `serde_json` document, and the only
/// change it carries is an `Option<BlockAddress>` that was always present and always `None` on a
/// removal -- so it is an absent key becoming a present one, which both halves already tolerate.
/// Nothing in the page payload passes through any of the three: it is bytes to all of them.
///
/// 5 -> 6 WHEN `object_id` LEFT `BlockAddress`, and the hazard has the same shape as both bumps
/// before it: the decode cannot see the disagreement. `generation` was `block_id.or(object_id)` and
/// is the block id alone now. For a BLOCK-RESIDENT page that is the value it always was. For a
/// WAL-RESIDENT page -- where the block id is absent -- the generation WAS the object id and is now
/// `None`, and `state::block_index_handle` hashes the generation while
/// `state::block_index_written_key` renders it into the ref key already on disk. So every
/// WAL-resident ref moves.
///
/// AND AN OMITTED REF KEY RESTORES WRONG, which is the half that costs data rather than bytes.
/// `index_log::strip_block_ref_key_repeat` OMITS a page's composite handle whenever it is derivable,
/// and `restore_block_ref_key_repeat` rebuilds it -- both through `block_ref_key_from_parts`, one of
/// whose eight parts is the generation. A row already on disk whose handle was omitted under the old
/// rule therefore rebuilds to a DIFFERENT handle, and the page is then looked up under a key nothing
/// wrote: a durably acknowledged write reads MISSING. See
/// `block_entry_names::an_omitted_block_handle_restores_to_a_different_key_under_the_new_generation_rule`.
///
/// # THE RESERVATION OF 4 IS VOID, AND IS VOIDED HERE RATHER THAN SILENTLY
///
/// The paragraph above this one reserved 4 for exactly this change, and that reservation was correct
/// WHEN IT WAS MADE: the constant was 3, so 4 was the next value and holding it cost nothing. It did
/// not survive main moving. The constant is 5 now, so spending 4 would LOWER it -- and lowering is
/// not symmetric with raising. `persistence.rs` compares the base snapshot with `<`, so a binary
/// carrying 4 computes `5 < 4 == false`, reaches `IndexLoadPath::Accepted`, and reads a 5-stamped
/// store as its own. That is the silent misread this constant exists to prevent, arriving by the one
/// route a one-sided comparison leaves open.
///
/// So 4 is a PERMANENT HOLE, 6 was taken when the object id left the address, and THIS change takes
/// 7 -- the object list now stores its SLOTS rather than a sorted set of its ids, so the sequence a
/// bucket writes is in slot order and carries `null` for a placeholder. The next takes 8. The rule
/// the three together imply: a stamp may only ever INCREASE -- read the constant from the tree
/// immediately before committing and take the next value above it, never a number reserved while
/// the tree held less. 7 was read as 6 in the tree at the moment this line was written.
///
/// AND THE CLAIM THAT A HOLE IS INVISIBLE IS NOT QUITE TRUE, which is why the rule is "increase"
/// rather than "any unused number". The paragraph above says the stamp is "only ever compared for
/// equality with the constant, never ordered or ranged". `engine::decode_index_bytes` does compare
/// with `!=` -- but only in its MSGPACK arm, and `persistence.rs` compares with `<`. #2051 tracks
/// that asymmetry and the decision it needs; nothing here depends on the equality claim.
///
/// 8 -> 9 WHEN A ZSET COMPONENT STOPPED SPELLING THE SCORE. `state::block_index_written_key`
/// renders `component` into the map key the served index is serialized under, and a zset entry's
/// `component` moved from `{biased:016x}` then the member in hex to the member alone -- so a page
/// that used to render `"0000000000000000<member>"` now renders `"<member>"`, a different key under
/// the same named map. The decode itself stays clean either way: `component` keeps its `String`
/// type on both sides of the change, so an old-stamped index still deserializes, and what disagrees
/// is what the text MEANS, on the recovery path that reads it back as a page's element -- the same
/// shape the three bumps above this one each describe. The positional index log is untouched: it
/// keeps its slot and refuses only on length mismatch, so nothing there needed a bump to stay
/// correct. `wal_proto` is untouched too -- the score rides the WAL outcome's existing `value` byte
/// slot now, never the component, and that slot's presence was already optional on the wire.
///
/// 9 -> 11 WHEN A TOMBSTONE'S ELEMENT NAME LEFT THE ENTRY. A tombstone entry no longer carries
/// `component` -- it files `None` -- and the element it is about is a new key on the BUCKET NODE,
/// `tombstone_elements`. Both halves move the served index: the entry's rendered key drops the
/// element text it used to carry (`block_index_written_key`), and the node grows a key a reader
/// without this change does not know. The named served index DROPS an undeclared key silently, so
/// a binary before this change reading an index written after it would load every tombstone with no
/// element at all -- a removal nothing can identify, which is precisely the resurrection this
/// change exists to prevent. That is what the stamp has to stop, and it stops it in both codecs:
/// `persistence` refuses a lower stamp with `<` and falls back to WAL replay, and the msgpack
/// container carries the stamp in its header and refuses a mismatch before a byte is decoded.
///
/// ELEVEN AND NOT TEN, AND THE READ IS RECORDED. `matrixark/main` holds 8, read from
/// `git show matrixark/main:...` immediately before this commit and never from this tree -- a
/// self-read returns the value just written and confirms itself. 9 is this branch's own carried zset
/// change. 10 is taken by an UNCOMMITTED edit in another working tree (`/root/oi_prod`), which no
/// ref holds and no sweep of refs can see: a previous lane swept 2,567 local and remote refs,
/// found nothing above 9, and was wrong for exactly that reason. So the trees were read, not the
/// refs. The direction that is lethal is TOO LOW, because `persistence` compares with `<` and a
/// stale value falls through to Accepted.
///
/// 11 -> 12 WHEN HASH AND ZSET JOINED THE PAGE-NAMED SET. `block_index_written_key` renders
/// `component` into the map key the served index is serialized under, so a hash page that used to
/// render under its field name and a zset page that used to render under its member now render
/// under neither -- a different key in the same named map, for two more kinds. Same shape as 8 -> 9,
/// which moved that key for zset alone, and the same reason a bump is needed rather than optional:
/// the row decodes cleanly either way and what disagrees is what the key MEANS on the recovery path
/// that reads it back as a page's element. ELEVEN WAS THIS BRANCH'S OWN PREVIOUS STEP and TEN is
/// still claimed by an uncommitted edit in another working tree, so this is the next free number
/// above both; main was re-read at 8 immediately before this commit.
///
/// AND REMOVING THE ELEMENT-NAME FIELD OWES NO FURTHER BUMP. TWELVE IS ENOUGH, AND THIS IS WHY.
///
/// The step after the one above took `BlockIndex::component` off the entry altogether -- 56 bytes
/// of field to 40. That is a change to the stored form: the named served index stops writing a
/// `"component"` key, and `block_index_written_key` renders the element slot of the map key empty.
/// Both are exactly the kind of movement the four paragraphs above each paid a number for, so the
/// question is a real one rather than a formality.
///
/// WHAT SETTLES IT IS THE REFUSAL FLOOR, NOT THE SIZE OF THE CHANGE. `persistence` refuses an index
/// stamped BELOW this constant and falls back to WAL replay, with `<` -- so when this was written
/// the set of indexes this binary would decode at all was exactly those stamped 12. Twelve was
/// stamped BY the commit that
/// made hash and zset page-named, which is the commit after which nothing files an element name on
/// an entry. So every index this binary accepts was written by a binary that filed no element name,
/// and the key this step stops writing is a key no accepted index contains. An index that really
/// does carry one is stamped 10 or lower, is refused before a byte is decoded, and is rebuilt from
/// the WAL.
///
/// THE CONSTANT HAS MOVED SINCE, AND THE ARGUMENT SURVIVES THE MOVE -- said here because the
/// paragraph above reads as present tense and its number is not. The floor only ever RISES, and
/// the claim it rests on is about the floor's lower bound: every index this binary accepts was
/// written at or above 12, so it was written by a binary filing no element name. A later bump
/// narrows that set further and cannot admit an older image. What a bump DOES invalidate is any
/// assertion that wrote the pair of numbers out by hand instead of deriving them -- one in
/// `engine::tests::context_node_map_arrival` did, and went red for two bumps while the door it
/// checks worked.
///
/// THE DIRECTION THAT WOULD HAVE NEEDED A NUMBER IS THE OPPOSITE ONE: a binary BEFORE the collapse
/// reading an index written after it. That is what 11 and 12 above already stop, and they stop it
/// in both codecs. Spending a thirteenth number here would refuse every index this binary itself
/// wrote at 12 and send every store through WAL replay once, for a key none of them carries.
///
/// SO THE THREE STORED-FORMAT ARMS THAT WENT RED ARE FIXTURES, NOT PRODUCTION OUTPUT MOVING, and
/// that distinction is written into each of them in `engine::tests::block_entry_names`. All three
/// HAND-CONSTRUCT an element name: two canonicalise a stored row that carries one so they can
/// assert what this binary writes back, and the third goldened a `BlockIndex` built with a
/// component argument that the struct literal had already stopped using. The combination they
/// exercise -- an element name beside a CURRENT stamp -- is one no production binary has ever
/// written, which is the whole reason the floor argument above holds.
///
/// ONE HAZARD WORTH RECORDING ABOUT THE NUMBERS THEMSELVES. On this branch the constant went 9 to
/// 11, back to 9 when the collapse was refused for hash and zset again, then to 11, then to 12. So
/// STAMP 11 NAMES TWO DIFFERENT ON-DISK SHAPES in this branch's history. Nothing reads an 11, because
/// the floor is 13 and the comparison is `<` -- but anything that ever starts treating a stamp below
/// the floor as meaningful has to know that one of them is ambiguous.
///
/// # 13, AND WHAT IT IS AND IS NOT BUYING
///
/// THIRTEEN IS SPENT ON THE INDEX-LOG ROW LOSING A SLOT. `index_log::INDEX_ITEM_SLOTS` went twelve
/// to eleven: slot 5 held the element name, then a constant nil for one step, and now nothing. A
/// stored twelve-slot row is REFUSED by that file's arity gate, naming both arities, and a named row
/// carrying the key is refused by `deny_unknown_fields` -- so a store written by a twelve-slot
/// binary cannot be half-read.
///
/// AND THE NUMBER IS NOT WHAT REFUSES THE ROW. That has to be said here, because this is where
/// someone will look for the mechanism and it is not here. This guard is one-sided -- `if
/// shard.index_format_version < SHARD_INDEX_FORMAT_VERSION` -- and it guards the NAMED served
/// index, not the positional log. What it buys is that a store written before the slot came off is
/// DISCARDED and rebuilt rather than read, so the refusal in `index_log` is a tripwire on a path
/// nothing should reach, rather than the thing a load depends on. `index_log_record`'s module doc
/// makes the general version of this point -- that a row-shape change's real lever is the container
/// codec byte and not this stamp -- and it is right; the number is spent anyway, because discarding
/// the stale index is worth having and costs one WAL replay on a store nothing is serving.
///
/// THIRTEEN IS FREE, MEASURED RATHER THAN ASSUMED. `index_log_record`'s doc records that "13 is live
/// on four branches -- so the next free value is 14". That reading has EXPIRED: over every
/// `matrixark` remote ref, no tip holds 13, the maximum any tip holds is this constant's own 12, and
/// the four branches at 12 are the ones that note was counting. Checked by reading the constant out
/// of every remote ref, not by grepping history -- history still contains a 13 that a revert took
/// back, exactly as it still contains the ambiguous 11.
/// FIFTEEN IS SPENT ON MERGING THE TWO CHANGE MAPS, AND IT COVERS BOTH OF THEM.
///
/// `control_state_change_sketch` LEAVES the stored image entirely, and `control_state_changes`
/// keeps its name while its bucket VALUE changes from a bare member array to a
/// [`hll::DistinctBucket`] -- so this one number pays for a departing key and a changed value
/// shape, which is the honest description of "more than one tenant" here.
///
/// WHY THE STAMP AND NOT A TOLERANT DECODE. `decode_index_bytes` checks the version BEFORE
/// decoding, deliberately, because the payload is addressed by field order and decoding it
/// against a different struct shape "does not error, it produces a plausible and wrong
/// `ShardState`". A pre-bump bucket value is a member ARRAY, which matches neither variant of the
/// new enum; the sketch map is a field no reader claims. Neither is a state a decode should be
/// asked to adjudicate.
///
/// THE REPLAY CONSEQUENCE, PLAINLY. Every index written before this is refused on load, counted
/// as `IndexLoadPath::RefusedStaleStamp`, and the shard rebuilds from its WAL. Slower and
/// correct, and the same trade every bump in this tree has made.
///
/// FIFTEEN IS FREE, MEASURED THE WAY FOURTEEN'S NOTE PRESCRIBES -- by reading this constant out
/// of every `matrixark` remote ref rather than by grepping history. The maximum any tip holds is
/// 14 (on seventeen refs); no tip holds 15.
/// MOVED 26 -> 27 WHEN A PAGE ENTRY STOPPED RESTATING THE KEY IT IS FILED UNDER.
///
/// `object_key` and `model_id` are the second and first slots of the composite key the page index
/// is keyed by, and the entry wrote both again as named fields. #2350 measured that the key's
/// characters were NOT READ BACK -- every page key in a 2,000-key index replaced by the decimal of
/// its handle, fed to the production decoder and re-encoded, produced a byte-identical `slot_index`
/// -- so each fact was stored twice and read once, from the copy that costs 4.4x more. The decoder
/// parses the key now (`index_log::block_ref_key_parts`) and the entry writes three keys instead of
/// five.
///
/// THIS BUMP HAS BOTH OF THE TWO SHAPES THIS CONSTANT'S HISTORY DISTINGUISHES, one per direction,
/// and the direction that needs it is NOT the one a reader expects.
///
///   * A NEW BINARY READING OLD BYTES IS 13'S SHAPE AND NEEDS NO STAMP AT ALL. The stored entry's
///     two extra keys are dropped by the named decoder, silently, and the reconstruction reads the
///     SAME two values out of the key beside them -- so an old index decodes to exactly the index
///     it decoded to before. That is not assumed: `block_entry_names`'s `OLD_STORE_INDEX` is bytes a
///     pre-change binary wrote, and `an_index_written_before_this_change_loads_block_for_block`
///     compares every page of it field by field against a committed table.
///   * AN OLD BINARY READING NEW BYTES IS WHAT THE STAMP IS SPENT ON, and the msgpack container
///     refuses on EQUALITY in its header before decoding anything -- so 27 makes that refusal
///     clean, where without a bump the old binary would reach `StoredBlockEntry` and fail on a
///     missing required field, reporting corruption for a format step.
///
/// So the trade is unusual for this constant: the bump costs every existing store a replay it did
/// not strictly need, and buys a rollback that refuses rather than one that looks corrupt. It is
/// spent that way deliberately, and the half that would have been silent is the half that is driven
/// against committed bytes instead.
///
/// `SHARD_INDEX_FORMAT_VERSION` 26 and below is what every paragraph below describes.
pub(super) const SHARD_INDEX_FORMAT_VERSION: u32 = 28;

/// Serialize a shard index, stamping the current format version.
///
/// Stamped here rather than held on the struct so ShardState keeps its derived Default: an
/// in-memory shard would otherwise default the field to 0 and write itself out looking legacy.
pub(super) fn stamp_index_format_version(shard: &ShardState) -> serde_json::Value {
    let mut value = serde_json::to_value(shard).expect("shard index should serialize");
    if let Some(map) = value.as_object_mut() {
        map.insert(
            "index_format_version".to_string(),
            serde_json::Value::from(SHARD_INDEX_FORMAT_VERSION),
        );
    }
    value
}

/// What the serving read fast path decided to serve: chosen under the shard-table read guard,
/// read off the block store after it drops.
///
/// THE GUARD PROTECTS THE INDEX, NOT THE BYTES. Once an address is copied out, no writer can
/// invalidate it into something wrong: compaction relocates a block by APPENDING the new copy and
/// repointing the index, leaving the old bytes where they were, and slab ids are handed out
/// strictly monotonically, so a stale address never resolves to a DIFFERENT record. The one path
/// that destroys bytes quarantines the slab for `DELAYED_DESTROY_MIN_AGE_MS` first -- an hour,
/// documented as covering exactly "a reader holding a stale address" -- and a read that somehow
/// lost even that race fails the record's block-id and checksum check and answers ABSENT, which is
/// what a concurrently deleted key answers anyway.
///
/// What the single guard scope still buys, and what must not be narrowed away: a `HashGetAll`
/// snapshots EVERY field's address at one instant. Taking the guard once per field instead would
/// let a concurrent write land between two fields and serve half of one hash and half of another.
enum FastPathRead<'a> {
    String {
        key: &'a str,
        address: Option<ElementEntry>,
        /// The bucket the page cache is keyed by, resolved while the shard guard is HELD.
        ///
        /// It has to be: the bucket is `block_routing_bucket(key, ..)` over the range the shard is
        /// stamped with, and the whole point of this plan is that the reads below happen after the
        /// guard drops. Carrying the number rather than the shard is what keeps that true.
        routing_bucket: u32,
    },
    Hash {
        /// THE OBJECT KEY, which this variant did not carry and the read below now needs.
        ///
        /// The bucket beside it was ALREADY derived from this key while the guard was held, so the
        /// key was in hand at plan time and was simply not carried past it. A block's identity is
        /// `(shard, kind, key)` and the field names only the ELEMENT, so `fields` alone cannot say
        /// which object these blocks belong to -- a `string` and a `hash` may share a key, and it is
        /// the pair that tells their blocks apart. Borrowed, as `String`'s is: the plan is consumed
        /// inside the same call that built it.
        key: &'a str,
        /// HANDLES, NOT OWNED NAMES. The plan is a snapshot taken under the shard
        /// guard, so it has to copy the map's entries out; with a handle that copy is a
        /// refcount bump per field instead of an allocation per field. The response
        /// below still needs an owned name, and needed one before this change too.
        fields: Vec<(crate::engine::hash_field_map::FieldName, ElementEntry)>,
        routing_bucket: u32,
    },
}

/// What the served-index encode did while this thread held the shard-table WRITE guard.
///
/// The thing worth shortening is the HOLD, and on a shared box the hold cannot be timed: a build
/// running next door moves wall-clock by an order of magnitude, and both arms of a comparison do
/// not move by the same amount. What can be counted is the WORK done while the guard is held, and
/// on the maintenance paths the dominant item is the whole-index encode -- the shard's entire
/// served index through serde and then zstd, once per round, ahead of two file writes.
///
/// Everything here is THREAD-LOCAL, both the depth and the tallies. A process-global flag would
/// attribute another thread's guard to this one, and a process-global tally would attribute
/// another test's encode to this one; every test in this crate shares a process, and the tests
/// that drive concurrent writers share it with threads that are encoding for their own reasons.
/// Thread-local means a measurement means what it says however the suite is scheduled.
pub mod shard_write_guard {
    use std::cell::Cell;

    thread_local! {
        /// How many marked shard-table write guards this thread currently holds.
        static DEPTH: Cell<u32> = const { Cell::new(0) };
        static ENCODES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static ENCODE_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static ENCODES_TOTAL: Cell<u64> = const { Cell::new(0) };
        static ENCODE_BYTES_TOTAL: Cell<u64> = const { Cell::new(0) };
    }

    /// Served-index encodes on THIS thread since the last reset.
    ///
    /// The `_total` pair is the denominator. An assertion that nothing encoded under the guard is
    /// satisfied just as well by a path that did not encode at all -- or did not run -- so the
    /// totals have to be reported beside it and checked.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct IndexEncodeCounts {
        pub encodes_under_guard: u64,
        pub encode_bytes_under_guard: u64,
        pub encodes_total: u64,
        pub encode_bytes_total: u64,
    }

    pub(super) fn entered() {
        DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    }

    pub(super) fn left() {
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }

    /// Whether this thread is inside a marked shard-table write-guard region.
    pub(super) fn held() -> bool {
        DEPTH.with(|depth| depth.get() > 0)
    }

    pub(super) fn note_index_encode(bytes: usize) {
        // The per-STAGE tally, in the primitive rather than at the call sites, for the reason the
        // module note on `LIVE_BLOCK_SCAN_ENTRIES` gives: a counter charged at call sites is a
        // counter the next call site forgets.
        super::storage_bucket_internals::note_stage_index_encode(bytes);
        ENCODES_TOTAL.with(|count| count.set(count.get().saturating_add(1)));
        ENCODE_BYTES_TOTAL.with(|total| total.set(total.get().saturating_add(bytes as u64)));
        if held() {
            ENCODES_UNDER_GUARD.with(|count| count.set(count.get().saturating_add(1)));
            ENCODE_BYTES_UNDER_GUARD
                .with(|total| total.set(total.get().saturating_add(bytes as u64)));
        }
    }

    pub fn index_encode_counts() -> IndexEncodeCounts {
        IndexEncodeCounts {
            encodes_under_guard: ENCODES_UNDER_GUARD.with(|count| count.get()),
            encode_bytes_under_guard: ENCODE_BYTES_UNDER_GUARD.with(|total| total.get()),
            encodes_total: ENCODES_TOTAL.with(|count| count.get()),
            encode_bytes_total: ENCODE_BYTES_TOTAL.with(|total| total.get()),
        }
    }

    /// Clear this thread's tallies. For a test measuring one operation.
    pub fn reset_index_encode_counts() {
        ENCODES_UNDER_GUARD.with(|count| count.set(0));
        ENCODE_BYTES_UNDER_GUARD.with(|total| total.set(0));
        ENCODES_TOTAL.with(|count| count.set(0));
        ENCODE_BYTES_TOTAL.with(|total| total.set(0));
    }

    thread_local! {
        /// How many marked shard-table READ guards this thread currently holds.
        ///
        /// Kept apart from the write depth because the two answer different questions. A write
        /// guard excludes everyone; a read guard admits other readers and excludes only writers.
        /// A maintenance stage that reads blocks while holding this one is not blocking other
        /// reads -- it is blocking every WRITE on the shard, including the rest of its own round.
        static READ_DEPTH: Cell<u32> = const { Cell::new(0) };
        static BLOCK_READS_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
        static BLOCK_READS_TOTAL: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn entered_read() {
        READ_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    }

    pub(super) fn left_read() {
        READ_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }

    /// Whether this thread is inside a marked shard-table region of EITHER kind.
    fn held_any() -> bool {
        held() || READ_DEPTH.with(|depth| depth.get() > 0)
    }

    /// The same, for counters outside this module. A manifest file read or write taken while
    /// this is true blocks every write on the shard for the duration of the syscall.
    pub(crate) fn any_shard_guard_held() -> bool {
        held_any()
    }

    /// Block-store reads the maintenance paths performed, and how many of them were performed
    /// while this thread held a shard-table guard.
    ///
    /// Same reasoning as the encode counters above, applied to the other thing a maintenance
    /// stage does that scales with the STORE rather than with the round's budget: reading live
    /// blocks off the block store. The `_total` is the denominator -- "no reads happened under a
    /// guard" is satisfied just as well by a stage that read nothing, or did not run.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct MaintenanceBlockReadCounts {
        pub block_reads_under_guard: u64,
        pub block_reads_total: u64,
    }

    thread_local! {
        /// Whether the serving read fast path should keep the shard-table read guard alive
        /// across its block reads, the way it did before they were moved out.
        ///
        /// Kept for the reason the other control arms are kept: a guard asserting ZERO reads
        /// under the lock is vacuous unless an arm in the same process, on the same fixture,
        /// can still produce a non-zero one. Thread-local so one arm cannot leak into another.
        static SERVE_UNDER_SHARD_GUARD: Cell<bool> = const { Cell::new(false) };
    }

    /// Serve the read fast path with the guard held across the reads, for the measurement.
    #[cfg(test)]
    pub fn serve_fast_path_under_shard_guard_for_test(hold: bool) {
        SERVE_UNDER_SHARD_GUARD.with(|flag| flag.set(hold));
    }

    #[cfg(test)]
    pub(super) fn serving_holds_guard_across_reads() -> bool {
        SERVE_UNDER_SHARD_GUARD.with(|flag| flag.get())
    }

    /// Always false outside tests: the shipped path reads its blocks after the guard drops.
    #[cfg(not(test))]
    pub(super) fn serving_holds_guard_across_reads() -> bool {
        false
    }

    pub(super) fn note_block_read() {
        BLOCK_READS_TOTAL.with(|count| count.set(count.get().saturating_add(1)));
        if held_any() {
            BLOCK_READS_UNDER_GUARD.with(|count| count.set(count.get().saturating_add(1)));
        }
    }

    pub fn maintenance_block_read_counts() -> MaintenanceBlockReadCounts {
        MaintenanceBlockReadCounts {
            block_reads_under_guard: BLOCK_READS_UNDER_GUARD.with(|count| count.get()),
            block_reads_total: BLOCK_READS_TOTAL.with(|count| count.get()),
        }
    }

    /// Clear this thread's block-read tallies. For a test measuring one stage.
    pub fn reset_maintenance_block_read_counts() {
        BLOCK_READS_UNDER_GUARD.with(|count| count.set(0));
        BLOCK_READS_TOTAL.with(|count| count.set(0));
    }

    thread_local! {
        /// Times this thread took the maintenance-mirror lock to look the sink up.
        ///
        /// A different shape of cost from the two above: not one big thing under the guard but a
        /// small one repeated per item. A round that deletes N keys can take this lock once or N
        /// times, and only a count distinguishes them -- N uncontended acquisitions are
        /// invisible to wall-clock on a loaded box and exactly as invisible when they are gone.
        static MIRROR_SINK_LOOKUPS: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn note_mirror_sink_lookup() {
        MIRROR_SINK_LOOKUPS.with(|count| count.set(count.get().saturating_add(1)));
    }

    /// Maintenance-mirror lock acquisitions on this thread since the last reset.
    pub fn maintenance_mirror_sink_lookups() -> u64 {
        MIRROR_SINK_LOOKUPS.with(|count| count.get())
    }

    /// Clear this thread's mirror-lookup tally. For a test measuring one round.
    pub fn reset_maintenance_mirror_sink_lookups() {
        MIRROR_SINK_LOOKUPS.with(|count| count.set(0));
    }
}

/// The shard table's write guard, with the region it covers marked for measurement.
///
/// Marking at the ACQUISITION rather than around each expensive call is what makes the counter
/// hard to blind: a call that moves into the region starts being counted because it is in the
/// region, not because someone remembered to wrap it, and a call that moves out stops for the
/// same reason. The mark is released by the same `Drop` that releases the lock, so an early
/// `drop(guard)` shortens the measured region exactly as much as it shortens the real one.
struct MarkedShardWriteGuard<'a> {
    guard: std::sync::RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>,
}

impl<'a> MarkedShardWriteGuard<'a> {
    fn new(guard: std::sync::RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>) -> Self {
        shard_write_guard::entered();
        Self { guard }
    }
}

impl std::ops::Deref for MarkedShardWriteGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for MarkedShardWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for MarkedShardWriteGuard<'_> {
    fn drop(&mut self) {
        shard_write_guard::left();
    }
}

/// The shard table's READ guard, with the region it covers marked the same way.
///
/// A read guard looks harmless and is not: it admits other readers but excludes every writer, so
/// a maintenance stage holding one across per-block I/O stops all writes on the shard for as long
/// as the I/O takes. Marked at the acquisition for the same reason the write guard is -- work
/// moving out of the region stops being counted because of WHERE IT IS, not because a wrapper
/// was remembered.
struct MarkedShardReadGuard<'a> {
    guard: std::sync::RwLockReadGuard<'a, HashMap<ShardId, ShardState>>,
}

impl<'a> MarkedShardReadGuard<'a> {
    fn new(guard: std::sync::RwLockReadGuard<'a, HashMap<ShardId, ShardState>>) -> Self {
        shard_write_guard::entered_read();
        Self { guard }
    }
}

impl std::ops::Deref for MarkedShardReadGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl Drop for MarkedShardReadGuard<'_> {
    fn drop(&mut self) {
        shard_write_guard::left_read();
    }
}

/// Serialize a shard whose stamp is already current, straight to bytes.
///
/// The stamping path builds an entire intermediate `serde_json::Value` tree of the whole index
/// before encoding it -- serializing a multi-megabyte structure twice, once into a tree of
/// allocations and once into bytes, on every snapshot. A shard whose version field is already
/// correct needs none of that; one whose field is stale still takes the stamping path, so the
/// bytes are identical either way.
pub(super) fn serialize_index_stamped(shard: &mut ShardState) -> Vec<u8> {
    shard.index_format_version = SHARD_INDEX_FORMAT_VERSION;
    let bytes = encode_index_bytes(shard);
    shard_write_guard::note_index_encode(bytes.len());
    bytes
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    let bytes = if shard.index_format_version == SHARD_INDEX_FORMAT_VERSION {
        encode_index_bytes(shard)
    } else {
        wrap_index_json(
            serde_json::to_vec(&stamp_index_format_version(shard))
                .expect("shard index should serialize"),
        )
    };
    // Counted at the two production entry points rather than inside `encode_index_bytes`, so the
    // tally is one per served-index encode whichever branch produced it -- and so the handful of
    // tests that call the encoder directly to check a container shape do not register as engine
    // work that some path did.
    shard_write_guard::note_index_encode(bytes.len());
    bytes
}

/// Container magic for a non-JSON served index. A JSON index starts with `{`, so a reader can
/// tell the two apart from the first byte and never has to be told which it is holding.
const INDEX_CONTAINER_MAGIC: &[u8] = b"TSIDX\x01";

/// Payload codec ids inside the container. The whole point of the container is that this is an
/// enumeration rather than a decision: each payload is an id whose decoder lands beside the
/// others, and every index written before it keeps loading.
const INDEX_CODEC_ZSTD_JSON: u8 = 1;
/// The struct's own serde model in a binary encoding, then compressed.
///
/// Two decisions are baked in here, and measurement forced both.
///
/// NOT a hand-written protobuf schema. The served index IS a `ShardState` and has to round-trip
/// one exactly. A schema mirroring its ~26 fields and their nested maps is a second definition of
/// the same type, and the two drifting apart fails SILENTLY -- a field added here and forgotten
/// there simply disappears from the durable image, and the loss surfaces as missing data after a
/// reload. Riding the existing derives means a new field participates because it exists, not
/// because someone remembered to add it in two places.
///
/// NOT a field-ORDER encoding either, which is what an earlier attempt at this used. These structs
/// lean on `#[serde(skip_serializing_if)]` and `#[serde(default)]` -- `BlockAddress` alone skips
/// six optional fields -- so the writer omits fields that a positional decoder still expects and
/// the stream slides out of alignment. Tried directly here: the round-trip fails with "tag for
/// enum is not valid, found 9". A self-describing encoding (field names, struct-as-map) keeps
/// exactly the semantics JSON had, which is the only way those attributes stay honest.
const INDEX_CODEC_ZSTD_MSGPACK: u8 = 2;

/// Binary payloads carry the struct version they were written from, big-endian, right after the
/// codec id. A name-free encoding read against a different struct does not fail, it MIS-READS --
/// so the version is checked before a byte is decoded, and a mismatch is refused. This is also the
/// trap that sank the previous attempt from the other end: the struct's own
/// `index_format_version` field lives INSIDE the payload, so it cannot be consulted until after
/// the decode it is supposed to guard, and a fresh shard carries 0 there regardless.
const INDEX_BINARY_VERSION_BYTES: usize = 4;

/// Compression level for the container payload. The served index is written whole, in the
/// background, and read whole -- so this trades a little CPU on a path that is not the request
/// path for a large cut in bytes written and bytes read at load.
const INDEX_ZSTD_LEVEL: i32 = 3;

/// Does this look like a served index, in either of the two formats a reader may be handed?
///
/// A JSON index starts with `{`; a container starts with its magic. Callers that only need to
/// know "these bytes are an index" -- rather than to decode one -- ask this instead of parsing.
pub(crate) fn bytes_look_like_served_index(bytes: &[u8]) -> bool {
    bytes.first() == Some(&b'{') || bytes.starts_with(INDEX_CONTAINER_MAGIC)
}

/// TS_INDEX_CODEC: which payload to write when the container is on. `msgpack` (the default when
/// the container is enabled) encodes the struct itself; `zstd-json` keeps the compressed-JSON
/// payload, which any reader can still inspect with a decompressor and a JSON parser.
fn index_container_codec() -> u8 {
    match std::env::var("TS_INDEX_CODEC")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "zstd-json" | "json" => INDEX_CODEC_ZSTD_JSON,
        _ => INDEX_CODEC_ZSTD_MSGPACK,
    }
}

/// Encode a shard with the container's binary payload: the serde model as a binary map,
/// compressed, behind the magic, the codec id and the struct version.
fn encode_index_msgpack(shard: &ShardState) -> Option<Vec<u8>> {
    // struct-as-MAP, not struct-as-array: the array form is positional, and positional is what
    // mis-reads a struct that skipped an absent optional.
    let mut encoded = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded).with_struct_map();
    serde::Serialize::serialize(shard, &mut serializer).ok()?;
    let payload = zstd::stream::encode_all(encoded.as_slice(), INDEX_ZSTD_LEVEL).ok()?;
    let mut out = Vec::with_capacity(
        payload.len() + INDEX_CONTAINER_MAGIC.len() + 1 + INDEX_BINARY_VERSION_BYTES,
    );
    out.extend_from_slice(INDEX_CONTAINER_MAGIC);
    out.push(INDEX_CODEC_ZSTD_MSGPACK);
    out.extend_from_slice(&SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&payload);
    Some(out)
}

/// The single place the served index becomes bytes, and it always writes the container.
///
/// The measured cost of raw JSON here is real: a 1 000-memory store carries a 74 MB index, and a
/// dump rewrites it whole. Compressing the same JSON keeps ONE representation of the struct --
/// no schema mirrored by hand, no second definition to drift -- while cutting what is written and
/// what must be read back at load.
///
///
/// Reading is unconditional and sniffed, so this flag only ever controls what is WRITTEN, and an
/// index written either way loads in either setting.
///
///
/// The container was built, measured and then left switched off, so every store written since has
/// carried a plain-JSON served index. Measured at 300 adds into one subject, which is the shape
/// that grows an index rather than merely touching it:
///
/// ```text
///                    index      WAL    durable per memory   add p50
///     JSON          19.9 MB  15.4 MB          227.0 KB      383.1 ms
///     container      2.2 MB   2.3 MB          122.0 KB      367.8 ms
/// ```
///
/// 46% less durable disk per memory. The WAL falls with it because index deltas ride the WAL, and
/// block bytes do not move at all -- the data is unchanged, only the way the index is written.
///
/// A format default is a durability decision, not a size one, so the flip is gated on recovery
/// rather than on the table above. Measured over 120 memories per case, comparing full retrieval
/// snapshots across a restart:
///
///   * written by the container, reopened by it -- identical.
///   * written as JSON, reopened with the container on -- identical, and the index on disk becomes
///     a container, so an existing store upgrades in place with no migration step.
///
/// A third case -- a container-written index reopened with raw-JSON writing selected -- measured
/// identical too, and being reversible in both directions is what made the container safe to adopt
/// as a default rather than an opt-in. That switch has since been removed, so only the two cases
/// above are reachable now.
///
/// A reader never has to be told which it is holding: JSON starts with `{`, a container with its
/// magic, so both formats stay loadable.
///
/// Writing raw JSON was a switch until nothing selected it: reading is sniffed either way, so the
/// only thing the off position produced was an index an older build could read, and producing one
/// now means running such a build. `encode_index_bytes_as_plain_json` keeps that shape reachable
/// from tests, which is where the claim "both shapes still load" has to be proved.
pub(super) fn encode_index_bytes(shard: &ShardState) -> Vec<u8> {
    if index_container_codec() == INDEX_CODEC_ZSTD_MSGPACK {
        // A binary payload only works from the struct itself, so the version-stamping path (which
        // patches a `Value`) keeps to JSON; both still land inside the same container.
        if let Some(encoded) = encode_index_msgpack(shard) {
            return encoded;
        }
    }
    wrap_index_json(serde_json::to_vec(shard).expect("shard index should serialize"))
}

/// Put already-serialized index JSON into the container. Separate from `encode_index_bytes`
/// because the version-stamping path serializes a patched `Value` rather than the struct, and both
/// must produce the same on-disk shape.
fn wrap_index_json(json: Vec<u8>) -> Vec<u8> {
    match zstd::stream::encode_all(json.as_slice(), INDEX_ZSTD_LEVEL) {
        Ok(payload) => {
            let mut out = Vec::with_capacity(payload.len() + INDEX_CONTAINER_MAGIC.len() + 1);
            out.extend_from_slice(INDEX_CONTAINER_MAGIC);
            out.push(INDEX_CODEC_ZSTD_JSON);
            out.extend_from_slice(&payload);
            out
        }
        // A compression failure must not cost the index: fall back to the bytes that always work.
        Err(_) => json,
    }
}

/// The served index as an older build wrote it: the struct's JSON, in no container.
///
/// Production has no way to produce this any more, and that is the point -- but the reader still
/// takes it, and a claim about what a reader accepts is worth only as much as the bytes used to
/// test it. This is those bytes.
#[cfg(test)]
pub(super) fn encode_index_bytes_as_plain_json(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec(shard).expect("shard index should serialize")
}

/// The single place served-index bytes become a `ShardState`, whatever wrote them.
///
/// Sniffs the container magic, so JSON written by any earlier binary keeps loading unchanged and
/// a container written by a newer one is refused with a clear error rather than mis-parsed. Every
/// decode site goes through here; the previous attempt at a binary index failed precisely because
/// the decoders were scattered and could not move together.
///
/// And it is where a decoded index gets PACKED. `serde` fills a `BTreeMap` one `insert` at a time
/// from bytes written in key order, which is the one insertion order that leaves a B-tree half
/// empty for the rest of the process's life; `repack_decoded_btrees` puts the entries back filling
/// every leaf. It rides the load rather than a timer or a flag, because the load is the moment the
/// whole structure is in hand and already paid for.
pub(super) fn decode_index_bytes(bytes: &[u8]) -> Result<ShardState, String> {
    // WRAPPED RATHER THAN EDITED PER ARM. Every codec path below returns its own
    // `ShardState`, and a fifth one added later would return another; a repack written into
    // each arm would be a thing to remember. Written here it is a thing nobody can forget.
    let mut state = decode_index_bytes_inner(bytes)?;
    state::repack_decoded_btrees(&mut state);
    // THE LEGACY CONTROL-STATE FOLD, HERE FOR THE SAME REASON THE REPACK IS HERE.
    //
    // Written in this wrapper it covers every codec arm and -- which is the half that matters --
    // every CALLER -- all six of which read `index_format_version` now, so a PRE-stamp image is
    // refused on every path. What the fold is for is an image at the CURRENT stamp: every one of
    // them still carries the top-level key, none is stale, and `objects` defaults to empty. A fold
    // written into one arm, or reached from one caller, would miss the rest.
    // THE LEGACY FOLD, HERE FOR THE REASON THE REPACK ABOVE IS HERE. Written per codec arm it
    // would be a thing to remember; written on the wrapper it covers every arm and every caller,
    // which is what matters because an image carrying the legacy key is not necessarily STALE --
    // the staleness test is `<`, so a current-stamp image is accepted at every door.
    // THE LEGACY EVENT FOLD IS GONE. Its own purpose, recorded above, was an image at the
    // CURRENT stamp still carrying the top-level `context_events` key -- false since stamp
    // 20 moved the event series into `objects`. Pre-stamp images are refused on every
    // caller, so the key arrives from no door.
    Ok(state)
}

/// The codec arms themselves. Reached only through `decode_index_bytes` above, which packs what
/// this fills -- so a fourth arm added here is covered without being told to be.
fn decode_index_bytes_inner(bytes: &[u8]) -> Result<ShardState, String> {
    if !bytes.starts_with(INDEX_CONTAINER_MAGIC) {
        return serde_json::from_slice::<ShardState>(bytes).map_err(|error| error.to_string());
    }
    let body = &bytes[INDEX_CONTAINER_MAGIC.len()..];
    let (codec, payload) = body
        .split_first()
        .ok_or_else(|| "served index container is truncated".to_string())?;
    match *codec {
        INDEX_CODEC_ZSTD_JSON => {
            let json = zstd::stream::decode_all(payload)
                .map_err(|error| format!("served index payload did not decompress: {error}"))?;
            serde_json::from_slice::<ShardState>(&json).map_err(|error| error.to_string())
        }
        INDEX_CODEC_ZSTD_MSGPACK => {
            if payload.len() < INDEX_BINARY_VERSION_BYTES {
                return Err("served index binary payload is truncated".to_string());
            }
            let (version_bytes, body) = payload.split_at(INDEX_BINARY_VERSION_BYTES);
            let version = u32::from_be_bytes(
                version_bytes
                    .try_into()
                    .map_err(|_| "served index version stamp is malformed".to_string())?,
            );
            // Checked BEFORE decoding, deliberately. This payload is addressed by field order, so
            // decoding it against a different struct shape does not error, it produces a plausible
            // and wrong `ShardState`. Refusing is treated like an absent index: the caller replays
            // the WAL and the index-log deltas, which is slower and correct.
            if version != SHARD_INDEX_FORMAT_VERSION {
                return Err(format!(
                    "served index was written from struct version {version}, this binary is {}",
                    SHARD_INDEX_FORMAT_VERSION
                ));
            }
            let decoded = zstd::stream::decode_all(body)
                .map_err(|error| format!("served index payload did not decompress: {error}"))?;
            rmp_serde::from_slice::<ShardState>(&decoded).map_err(|error| error.to_string())
        }
        other => Err(format!(
            "served index uses payload codec {other}, which this binary cannot read"
        )),
    }
}

/// Collect the served-index delta items for exactly the object keys a single write
/// touched. Looks up only the routing buckets those keys map to (never the whole store),
/// so the result is O(delta): one `IndexItem` per live/tombstoned block currently backing a
/// touched key. Deleted blocks ride as `deleted` tombstones so a fold applies the removal.
/// Empty when the command has no object keys (e.g. a context rebuild command); the caller
/// still appends the record so the index-log sequence advances per write.
/// The (kind, object_key, component) writes a command performs when -- and only when -- every
/// one of them lands through the block-upsert path (one new block per component, predecessor
/// replaced). `None` = the command's write shape is not a pure upsert (deletes, features,
/// rewrites), and the caller must fall back to the whole-object snapshot record.
fn command_upsert_components(
    command: &Command,
    shard: &ShardState,
) -> Option<Vec<(&'static str, String, Option<String>)>> {
    match command {
        Command::HashSet { key, field, .. } => {
            Some(vec![("hash", key.clone(), Some(field.clone()))])
        }
        Command::HashMultiSet { key, entries } => Some(
            entries
                .iter()
                .map(|(field, _)| ("hash", key.clone(), Some(field.clone())))
                .collect(),
        ),
        Command::HashIncrBy { key, field, .. } => {
            Some(vec![("hash", key.clone(), Some(field.clone()))])
        }
        Command::StringSet { key, .. } => Some(vec![("string", key.clone(), None)]),
        // A feature append writes ONE NEW BLOCK PER POINT, and each one's component is that
        // point's timestamp -- which is in the command. Same shape as the zset arm below, and it
        // was in the `None` bucket until now, so every append wrote a record holding one
        // `IndexItem` per point the series already had.
        //
        // WHAT THAT COST, MEASURED: one append to a 3,200-point series moves 3,272,240 bytes, of
        // which the cross in `is_the_appends_cost_the_series_or_the_shard` puts ~1,333 KB on the
        // series in ~9,459 extra allocations -- about 3 per point already present. The same defect
        // is recorded for the container maps one function below, at 3.53 GB of index log against
        // 16 MB for the same 20,001 messages.
        //
        // A TRIM DOES NOT NEED HANDLING HERE, and this is the part worth checking rather than
        // assuming, because declaring a delta over a write that DELETED is the one way this loses
        // data. An append past `feature_max_size` evicts the oldest points, and a rebuild from
        // blocks would resurrect them -- but the caller already refuses the delta path on a
        // membership shrink: `delta_key_state_change` compares `key_membership_size` across the
        // write, and that function counts `shard.features.get(key).len()`. So a trimming append
        // takes the whole-object record whatever this arm returns.
        //
        // DEDUPED, because `sorted_feature_points` collapses repeated timestamps before the write
        // and the components have to be the ones that actually landed.
        Command::FeatureAppend { key, points } => {
            let mut seen = std::collections::BTreeSet::new();
            Some(
                points
                    .iter()
                    .filter(|point| seen.insert(point.timestamp_ms))
                    .map(|point| {
                        (
                            "feature",
                            key.clone(),
                            Some(crate::engine::packed_blocks::timestamped_component(
                                point.timestamp_ms,
                                None,
                            )),
                        )
                    })
                    .collect(),
            )
        }
        // A zset add writes exactly one component, and it is derivable from the command. Declaring
        // it takes the O(1) delta path; without it the record snapshots every block of the object,
        // which cost 4 allocations per member already present.
        Command::ZSetAdd { key, member, score: _ } => Some(vec![(
            "zset",
            key.clone(),
            Some(crate::engine::execute_on_shard::zset_component(member)),
        )]),
        Command::SetAdd { key, member } => {
            Some(vec![("set", key.clone(), Some(hex::encode(member)))])
        }
        // A push files its block under its sequence number, which is only knowable once the
        // write has landed: post-apply the pushed element is the list's FIRST entry for a left
        // push and its LAST for a right one. That is why this needs shard state and the other
        // arms do not.
        Command::ListPush { key, left, .. } => {
            let seq = shard.lists.get(key).and_then(|list| {
                if *left {
                    list.keys().next().copied()
                } else {
                    list.keys().next_back().copied()
                }
            })?;
            Some(vec![(
                "list",
                key.clone(),
                Some(format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64))),
            )])
        }
        _ => None,
    }
}

/// Build the exact index items for an upsert record from post-apply shard state: each written
/// component's address is read back from the map the write just updated, so the logged block is
/// precisely the one a reload must serve. A component absent from the map (its append failed)
/// is skipped -- it produced no block to pin.
fn collect_upsert_index_items(
    shard: &ShardState,
    shard_id: ShardId,
    components: &[(&'static str, String, Option<String>)],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<crate::index_log::IndexItem> {
    let mut items = Vec::with_capacity(components.len());
    for (kind, object_key, component) in components {
        let address = match (*kind, component) {
            ("hash", Some(field)) => shard
                .hashes
                .get(object_key)
                .and_then(|fields| fields.get(field))
                .cloned(),
            ("string", None) => shard.string_address(object_key.as_str()),
            // THE ARM #2232 ASSUMED WAS HERE. That change made `command_upsert_components` name a
            // component for `FeatureAppend` so an append takes the O(1) delta path instead of
            // snapshotting the whole series -- and this match, which turns a component back into
            // the address the item carries, had no `feature` arm. A declared feature component
            // therefore fell to `_ => None`, the item was dropped, and the delta record named no
            // page for the append at all. Part of that change's measured byte saving was the
            // record becoming empty.
            //
            // THE SUITE ALREADY SPELLED THIS HAZARD OUT, for the hash pairing, in the failure
            // message of `replay_under_the_gate`: `collect_upsert_index_items` "would fall to its
            // `_ => None` arm, build no item, and the write would never reach the index log".
            // Declaring a component is half of a pair; this is the other half, and the test below
            // this function's own guard now asserts both halves together.
            //
            // The component is the point's stored timestamp in decimal -- `timestamped_component`
            // with no identity is `stored_key.to_string()` -- so the series is read at exactly the
            // key the write filed it under.
            ("feature", Some(component)) => component
                .parse::<u64>()
                .ok()
                .and_then(|at| {
                    shard
                        .features()
                        .get(object_key.as_str())
                        .and_then(|series| series.get(at))
                        .cloned()
                }),
            // `zset_component` is `hex::encode(member)` -- the score no longer rides this string
            // -- so the member the map is keyed by is recoverable from the whole of it.
            ("zset", Some(component)) => hex::decode(component)
                .ok()
                .and_then(|member| {
                    shard
                        .zsets
                        .get(object_key)
                        .and_then(|members| members.get(&member))
                        .map(|(_, address)| address.clone())
                }),
            // `hex::encode(member)` is the component a set add files its block under, so
            // the member the map is keyed by is recoverable from the component itself.
            // `{biased:016x}` of the entry's sequence, so the key the list map is keyed by
            // is recoverable from the component it was filed under.
            ("list", Some(component)) => u64::from_str_radix(component, 16)
                .ok()
                .map(|biased| biased.wrapping_add(i64::MIN as u64) as i64)
                .and_then(|seq| {
                    shard
                        .lists
                        .get(object_key)
                        .and_then(|entries| entries.get(&seq))
                        .cloned()
                }),
            ("set", Some(component)) => hex::decode(component)
                .ok()
                .and_then(|member| {
                    shard
                        .sets
                        .get(object_key)
                        .and_then(|members| members.get(&member))
                        .cloned()
                }),
            _ => None,
        };
        let Some(address) = address else { continue };
        let routing_bucket =
            block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
        let object_id = stable_block_object_id(shard_id, kind, object_key);
        // THE AUTHORITY, CALLED RATHER THAN COPIED. `block_ref_key_from_parts` says of itself:
        // "One definition, called from the page index's serialization and from the replay log, so
        // the two cannot drift into different spellings of the same page." This site was a hand
        // rolled second copy of exactly those eight parts in exactly that order -- which is how a
        // derivation drifts, and this one did: when `generation` stopped being
        // `block_id.or(object_id)` the two copies began disagreeing for WAL-resident pages, and
        // `strip_block_ref_key_repeat` -- which clears the stored key only on an EXACT match against
        // the authority -- started declining on every such item. Measured as `index_log_delta`
        // going from 100 allocations and 4,520 bytes to 2,202 and 82,653 over twenty `SetAdd`s at a
        // 3,200-member set, and identical at 200: a scaling defect invisible per call.
        let block_ref_key = crate::index_log::block_ref_key_from_parts(
            kind,
            object_key,
            // `None`, AND THIS IS THE HALF THAT CANNOT BE LEFT BEHIND. `strip_block_ref_key_repeat`
            // clears the stored handle only on an EXACT match against its own derivation, and that
            // derivation passes `None` now. Naming the element here would make the strip decline on
            // every container row -- the same drift, with the same cost, that the paragraph above
            // measured at 2,202 allocations and 82,653 bytes. The two derivations of a page's
            // handle AGREE from here on: this one and `state::block_index_written_key`.
            None,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            address.block_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
        items.push(crate::index_log::IndexItem {
            kind: crate::index_log::IndexItemKind::Block,
            routing_bucket,
            block_ref_key,
            object_key: std::sync::Arc::from(object_key.as_str()),
            model_id: (*kind).to_string(),
            object_id,
            deleted: false,
            // THE WHOLE PAYLOAD. The three slots this used to fill beside the address --
            // `block_id`, `size`, `in_log` -- were each a derivation OF the address, so they are
            // computed where they are written instead of restated here.
            entry: Some(address),
        });
    }
    items
}

/// The (kind, component) a typed removal deleted, when the command names it outright.
///
/// Only commands whose component is derivable from their own fields appear here. ZSetRemove's
/// component folds in the score it is deleting, and ListPop's is the sequence it just took --
/// neither survives the operation, so neither can be named from the command afterwards. Those
/// keep restating the whole object until the component is carried out of the arm.
fn command_removed_component(command: &Command) -> Option<(&'static str, Option<String>)> {
    match command {
        Command::SetRemove { member, .. } => Some(("set", Some(hex::encode(member)))),
        Command::HashDelete { field, .. } => Some(("hash", Some(field.clone()))),
        _ => None,
    }
}

fn collect_command_index_items(
    shard: &ShardState,
    command_keys: &[String],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<crate::index_log::IndexItem> {
    collect_command_index_items_for(shard, command_keys, start_routing_bucket, end_routing_bucket, None)
}

/// The same, restricted to one component when the command can name what it touched.
///
/// Without a filter this states EVERY block the object holds, and each item costs four string
/// allocations -- so a removal from a 3,200-member collection built 3,200 items to record that
/// one member went away, measured at ~2.5 MB per call. A command that can name its component
/// states that one block instead, which is what the add path already does through
/// `command_upsert_components`.
///
/// The block is still found: a typed removal marks its block deleted in the bucket index rather
/// than dropping it, so the item this emits carries `deleted: true` and the deletion is recorded
/// exactly as before -- the guarantee that keeps a replay from resurrecting it.
fn collect_command_index_items_for(
    shard: &ShardState,
    command_keys: &[String],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    only: Option<(&str, Option<&str>)>,
) -> Vec<crate::index_log::IndexItem> {
    use std::collections::BTreeSet;
    let keys: BTreeSet<&str> = command_keys.iter().map(String::as_str).collect();
    if keys.is_empty() {
        return Vec::new();
    }
    // NO SHARD, NO ITEMS. Each item states an object id derived from (shard, kind, key). A state
    // that never entered the engine has no shard to derive with and also no blocks to state, so an
    // empty list is the truthful answer rather than a list of ids belonging to shard zero.
    let Some(shard_id) = shard.shard_id() else {
        return Vec::new();
    };
    let buckets: BTreeSet<u32> = keys
        .iter()
        .map(|key| block_routing_bucket(key, start_routing_bucket, end_routing_bucket))
        .collect();
    // THE IDENTITY COMES FROM BESIDE THE PAGE, AND THE INVERSION IS HOISTED OUT OF THE WALK.
    //
    // Every row below states an object key and an object id derived from it. The entry is losing
    // its own copy, so the source is the object lookup inverted by position -- built ONCE here and
    // queried per page, never built per page. `shard` is held immutably throughout, so there is no
    // borrow to split.
    let identities = crate::engine::state::BlockKeysByPosition::from_lookup(
        &shard.bucket_index.object_block_lookup,
    );
    let mut items = Vec::new();
    for routing_bucket in buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        for (block_ref_key, block) in &bucket.block_index {
            // BOTH SOURCES, THROUGH THE ONE DOOR: the lookup names a live page by position, and
            // this bucket's own tombstone rows name a deleted one. A page neither names is
            // skipped, and the skip is refused outside the window that explains it -- a report is
            // not the durability path, where the same state is a refusal.
            let Some(beside) = identities.identity_of(bucket, *block_ref_key, block) else {
                debug_assert!(
                    identities.names_no_position(),
                    "bucket {} holds a page the index cannot name while the lookup DOES name \
                     positions, so this walk is dropping a row outside the one window that \
                     explains it",
                    bucket.routing_bucket
                );
                continue;
            };
            if !keys.contains(beside.object_key.as_ref()) {
                continue;
            }
            if let Some((kind, component)) = only {
                // NARROWED BY THE ENTRY'S OWN NAME, WHICH IT NO LONGER HAS. This read
                // `page.component.as_deref() != component`; every entry's name was already `None`,
                // so the surviving question is whether the CALLER asked for a named element at
                // all. It did not match a named element before this change and it does not now --
                // `component.is_some()` is the same predicate with the entry's constant folded in.
                if block.model_id.as_str() != kind || component.is_some() {
                    continue;
                }
            }
            items.push(crate::index_log::IndexItem {
                kind: crate::index_log::IndexItemKind::Block,
                routing_bucket,
                block_ref_key: block_ref_key.to_string(),
                // The resident entry already holds these as shared names, so the item
                // takes the pointer instead of copying the characters per item.
                object_key: Arc::clone(beside.object_key),
                model_id: block.model_id.clone().to_string(),
                // THE ROW'S SLOT 5 STAYS AND THE FIELD IS GONE. This emission was already writing
                // a constant `None` into it, so the bytes it produces do not move: the serializer
                // writes that same nil for every row now, and a row written by an older binary
                // still carries whatever name it was given.
                //
                // RESOLVED ACROSS TWO CHANGES TO ONE ARGUMENT LIST, and they are orthogonal. Main
                // retired the `component` field from the row entirely, so there is nothing to pass
                // for it. This branch changed what `object_id` TAKES -- the identity arrives as a
                // parameter because the entry is losing its own copy -- which is a change to the
                // call, not to the row. Keeping both means: no component term, and the key handed
                // in explicitly.
                object_id: block.object_id(shard_id, beside.object_key),
                deleted: block.deleted,
                // `in_log` here was `page.log_backed()` -- the accessor added when the stored
                // copy of this same fact was removed from the resident entry. One derivation,
                // two spellings, on a row. Now neither.
                entry: Some(block.address.clone()),
            });
        }
    }
    items
}

/// Capture the authoritative post-write state of the maps that a single block-index entry
/// cannot reconstruct on reload, for exactly the object keys a write touched (O(delta)):
///  - packed timestamped series (features + the context timestamped maps): one physical
///    block holds many timestamps, so an eviction that trims the in-memory membership leaves
///    the dropped timestamps physically in the block. Pinning the membership here stops
///    reconstruction-from-blocks resurrecting them.
///  - non-block maps that ride only on the serialized index (TTL expiry, control-state
///    change/sketch/selection, context nodes/entities/embeddings), which no block entry
///    encodes.
/// Each blob is `{"key": ..., "<map>": <value>}`, carrying ONLY the maps the key is actually
/// in. A field left out means the same thing an explicit null used to: the key is absent from
/// that map, a tombstone. `apply_key_state_field` cannot tell the two apart -- `None` and
/// `Some(null)` take the same `remove_entry` arm -- so the null was never read, and it cost its
/// own field name on every record for the life of the log.
///
/// That was most of the record. A write touching one ordinary key is in none of these twelve
/// maps, so every one was written as a null: measured on a 100,000-record store, the always-null
/// fields were 308 bytes of a 656-byte index-log record.
///
/// Opaque JSON so the index-log layer stays decoupled from the concrete `ShardState` field types.
/// How many entries the per-key maps hold for `key`, added up.
///
/// Counting, not capturing: twelve `len()` calls and no allocation, against serializing every
/// entry those maps hold for the key. Used to decide whether the capture below is needed at all.
fn key_membership_size(shard: &ShardState, key: &str) -> usize {
    shard.features().get(key).map_or(0, |v| v.len())
        + usize::from(shard.expires_at_ms().contains_key(key))
        + shard.control_state_changes.get(key).map_or(0, |v| v.len())
        + usize::from(shard.control_state_selection.contains_key(key))
        + shard.context_nodes().get(key).map_or(0, |_| 1)
        + shard.context_events().get(key).map_or(0, |v| v.len())
        + shard.context_indexes().get(key).map_or(0, |v| v.len())
        + shard.context_audits().get(key).map_or(0, |v| v.owned_element_count())
        + shard.context_children().get(key).map_or(0, |v| v.owned_element_count())
        + shard.context_summaries().get(key).map_or(0, |v| v.owned_element_count())
        + shard.context_compressions().get(key).map_or(0, |v| v.owned_element_count())
        + shard.context_entities().get(key).map_or(0, |v| v.len())
}

/// Did this write produce per-key state that reconstruction from physical blocks cannot redo?
///
/// TWO REASONS, AND THE SECOND ONE COVERS THE HALF THE FIRST CANNOT SEE.
///
/// The first is a MEMBERSHIP SHRINK: an entry was evicted or tombstoned, and rebuilding the
/// index from the blocks on disk would find it again and resurrect it. That is what this used
/// to ask on its own, and it is still asked first because it is the cheap half.
///
/// The second is a DEADLINE CHANGE. `key_membership_size` DOES count the deadline -- but only
/// as `expires_at_ms.contains_key(key)`, one unit of presence. That is enough to notice a
/// deadline being REMOVED (1 -> 0 is a shrink, so `CommonPersist` and `SET` without `KEEPTTL`
/// were always captured) and blind to the other two directions:
///
///   * ARMING a deadline where there was none is 0 -> 1, a GROWTH, and the gate only fires on
///     a shrink;
///   * MOVING a deadline to a different millisecond leaves `contains_key` true on both sides,
///     so the size does not move at all.
///
/// Either of those left `key_states` empty while the delta record it rode on still advanced
/// `applied_wal_sequence` to cover the WAL entry that set the deadline. On the legacy-recovery
/// load path the fold trusts that anchor and replays only the WAL tail BEYOND it, so the
/// command that armed the deadline is replayed by nobody, and the deadline is recovered from
/// neither the base, nor the record, nor the log. The anchor advance is what makes this a loss
/// rather than a slow path.
///
/// WHY THIS DOES NOT REINTRODUCE THE CAPTURE COST. Capturing serializes every entry the
/// per-key maps hold, which is why it is gated at all -- on a real corpus the unconditional
/// version wrote 3.53 GB of index log against 16 MB for the same 20,001 messages. That cost
/// came from APPENDS to long postings, and an append does not move a deadline, so it still
/// takes the cheap path. What newly captures is a write that armed or moved a key's deadline,
/// which is a deliberate and far rarer act than appending to a node.
///
/// The comparison is on `Option<u64>`, so a re-arm to the SAME millisecond correctly is not a
/// change and does not capture.
fn delta_key_state_change(
    shard: &ShardState,
    membership_before: &[(String, usize, Option<u64>)],
) -> bool {
    membership_before
        .iter()
        .any(|(key, size_before, deadline_before)| {
            key_membership_size(shard, key) < *size_before
                || shard.expires_at_ms().get(key.as_str()).copied() != *deadline_before
        })
}

/// One container element a write added or replaced, named by the identity its DURABLE map is keyed
/// by -- member bytes, list sequence, hash field -- and not by the component its block was filed
/// under.
///
/// PER ELEMENT, AND THAT IS THE WHOLE POINT. `apply_key_states` folds twelve maps and restores
/// every one of them per KEY, which is right for a map holding one value (or one series) per key.
/// The four container maps cannot be folded that way: their state is a COLLECTION of elements, so
/// a per-key rule would have to carry the whole collection and then assign it over the top. That
/// is the shape whose cost is already recorded on the key-state capture beside this -- 3.53 GB of
/// index log against 16 MB for the same 20,001 messages, because appending one entry serialized
/// every entry the key held. It is also the shape #1989 refused in the reconcile, for a second and
/// independent reason: taking a container map wholesale per key drops exactly the elements a fold
/// delivered. A write touches one element, so one element is what the record carries.
#[derive(Debug, Clone)]
enum TouchedContainerElement {
    Set { key: String, member: Vec<u8> },
    ZSet { key: String, member: Vec<u8> },
    List { key: String, sequence: i64 },
    Hash { key: String, field: String },
}

impl TouchedContainerElement {
    fn key(&self) -> &str {
        match self {
            TouchedContainerElement::Set { key, .. }
            | TouchedContainerElement::ZSet { key, .. }
            | TouchedContainerElement::List { key, .. }
            | TouchedContainerElement::Hash { key, .. } => key,
        }
    }
}

/// The container elements a command added or replaced, read from the command's own fields.
///
/// THE IDENTITY COMES FROM THE COMMAND, NOT FROM THE COMPONENT, and that is the difference between
/// this and `command_upsert_components` beside it. That function answers the same question in the
/// block index's vocabulary and spells each element as a component -- `hex::encode(member)` for a
/// set, sixteen hex digits of score followed by the member for a zset. Recovering the member back
/// out of one of those is what the reconcile does, and it is precisely the dependency this removes:
/// here the member is the `Vec<u8>` the caller asked to store.
///
/// A REMOVAL IS NOT HERE, and it does not need to be. An element is only ever restored if the
/// finished fold still holds a block at the address it was carried with, so a removal needs to say
/// nothing: taking the block away is already the whole answer. See
/// [`fold_carried_container_elements`].
fn command_touched_container_elements(
    command: &Command,
    shard: &ShardState,
) -> Vec<TouchedContainerElement> {
    match command {
        Command::SetAdd { key, member } => vec![TouchedContainerElement::Set {
            key: key.clone(),
            member: member.clone(),
        }],
        // The score is deliberately NOT taken from the command. It is read back out of the durable
        // map at capture time, so the record carries the score the map actually holds rather than a
        // second rendering of the `f64` the caller sent -- which is #1989's rule, applied at the
        // point the value is written down instead of at the point it is read back.
        Command::ZSetAdd { key, member, .. } => vec![TouchedContainerElement::ZSet {
            key: key.clone(),
            member: member.clone(),
        }],
        Command::HashSet { key, field, .. } | Command::HashIncrBy { key, field, .. } => {
            vec![TouchedContainerElement::Hash {
                key: key.clone(),
                field: field.clone(),
            }]
        }
        Command::HashMultiSet { key, entries } => entries
            .iter()
            .map(|(field, _)| TouchedContainerElement::Hash {
                key: key.clone(),
                field: field.clone(),
            })
            .collect(),
        // A push files its element under a sequence that is only knowable once the write has
        // landed, so this reads it back exactly as `command_upsert_components` does: post-apply the
        // pushed element is the list's FIRST entry for a left push and its LAST for a right one.
        Command::ListPush { key, left, .. } => shard
            .lists
            .get(key)
            .and_then(|list| {
                if *left {
                    list.keys().next().copied()
                } else {
                    list.keys().next_back().copied()
                }
            })
            .map(|sequence| {
                vec![TouchedContainerElement::List {
                    key: key.clone(),
                    sequence,
                }]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// The durable-map entries for the elements a write touched, one blob per key, in the shape
/// `apply_key_states` reads back.
///
/// ADDITIVE AND MATCHED BY NAME, which is the whole compatibility story. These are new fields in
/// the delta record's existing opaque per-key state channel, whose own documentation calls it "the
/// authoritative post-write value of the maps that are NOT reconstructable from a single
/// block-index entry" -- which, on the fold path, is exactly what the four container maps are. A
/// record written before this field existed simply does not carry it, `apply_key_states` finds it
/// absent and does nothing, and the fold behaves as it always has. So nothing positional moves,
/// no reader has to be upgraded first, and `SHARD_INDEX_FORMAT_VERSION` does not change.
