// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStoreSlabSummary, BlockStoreStats};
use crate::types::{BatchExecuteResponse, Command, ExecuteResponse};
use crate::types::{ShardId, Status};
use crate::wal::WriteAheadLogStats;
use matrixcache::CacheStats;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub version: u64,
    pub maxmemory_bytes: Option<u64>,
    #[serde(default)]
    pub tenant_name: Option<String>,
    pub read_qps: Option<u64>,
    pub write_qps: Option<u64>,
    #[serde(default)]
    pub table_read_qps: Option<u64>,
    #[serde(default)]
    pub table_write_qps: Option<u64>,
    #[serde(default)]
    pub tenant_read_qps: Option<u64>,
    #[serde(default)]
    pub tenant_write_qps: Option<u64>,
    #[serde(default)]
    pub extend_config: BTreeMap<String, String>,
    pub feature_max_size: usize,
    pub async_storage: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            maxmemory_bytes: None,
            tenant_name: None,
            read_qps: None,
            write_qps: None,
            table_read_qps: None,
            table_write_qps: None,
            tenant_read_qps: None,
            tenant_write_qps: None,
            extend_config: BTreeMap::new(),
            // Default retained points (and default read bound) per feature/sequence timeline.
            // Doubled from the historical 5000 to better serve long-sequence feature use cases
            // out of the box; still overridable per shard via set_config.
            feature_max_size: 10000,
            async_storage: false,
        }
    }
}

impl Config {
    /// Truthy check for an `extend_config` gate flag.
    pub fn flag(&self, name: &str) -> bool {
        self.flag_or(name, false)
    }

    /// `flag`, for a gate whose default is ON. An absent key takes `default`; a key that is
    /// present is read exactly as `flag` reads it, so an explicit false value opts out.
    pub fn flag_or(&self, name: &str, default: bool) -> bool {
        match self.extend_config.get(name) {
            None => default,
            Some(v) => matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes" | "enabled"
            ),
        }
    }

    /// Gate for the Control State rollup ladder (O(levels) sum-family window aggregates).
    /// Off by default so the live scan path is unchanged.
    pub fn control_rollup_enabled(&self) -> bool {
        self.flag("control_rollup")
    }

    /// Gate for coalesced Control State counter persistence: skip the per-write whole-series
    /// page rewrite and rely on the index snapshot + WAL replay (same durability model as
    /// control_state_changes/fol). Effective only with async_storage (WAL) on.
    ///
    /// DEFAULT ON. The rewrite it skips costs the length of the series on every increment --
    /// measured at 313,309 bytes per increment on a 3,200-point series against 2,459 coalesced,
    /// a 127x difference that grows without bound. What it relies on instead is covered by
    /// `control_state_coalesced_write_survives_restart_via_wal_replay`, which writes counters,
    /// reopens the engine and requires the counts to come back.
    ///
    /// Set the key to a false value to opt out; the async_storage gate still applies, so a
    /// deployment without a WAL keeps the per-write page rewrite either way.
    pub fn control_coalesce_persist_enabled(&self) -> bool {
        self.flag_or("control_coalesce_persist", true)
    }

    /// Gate for bounded distinct: convert oversized exact CHANGE sets to fixed-size HLL sketches
    /// (approximate distinct counts past the threshold). Off by default so distinct stays exact.
    pub fn control_distinct_sketch_enabled(&self) -> bool {
        self.flag("control_distinct_sketch")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub readonly: bool,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardRequest {
    pub shard_id: ShardId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetConfigRequest {
    pub shard_id: ShardId,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetConfigResponse {
    pub status: Status,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipUpdateRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub readonly: bool,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    #[serde(default = "default_membership_valid")]
    pub membership_valid: bool,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
    // True only while the shard's WAL is being replayed on load. keeps a partition in
    // PartitionLoadStage::LOADING (not serving) until the shard load and WAL replay
    // finishes; Rust must likewise refuse client commands during replay so a concurrent
    // write cannot interleave with replay and regress the WAL anchor (double-apply on the
    // next restart) or expose a stale mid-replay read. The replay thread itself bypasses
    // this gate via replaying_wal().
    #[serde(default)]
    pub recovering: bool,
}

fn default_membership_valid() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetInfoResponse {
    pub status: Status,
    pub info: Option<ShardInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectManagerStats {
    pub object_count: usize,
    pub block_ref_count: usize,
    pub dirty_object_count: usize,
    pub dirty_bucket_count: usize,
    pub routing_bucket_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatInfo {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardCanonicalStorageStats {
    pub page_index_entries: u64,
    /// The same live count as `page_index_entries` -- block and page are one concept under two
    /// names, and both are published while the rename is in flight. It is NOT a write counter;
    /// `block_writes` and `page_writes` are.
    pub block_index_entries: u64,
    pub object_index_entries: u64,
    /// The routing RANGE this shard covers -- the hash modulus, `end - start` -- and NOT a count
    /// of buckets that exist. A default shard covers the whole space, so this reads 4,294,967,295
    /// on an empty one. Read `bucket_index_resident_bytes_floor` below if you want a number that
    /// tracks what is actually resident.
    pub bucket_entries: u64,
    /// A FLOOR on what the bucket index costs in memory: one `BucketNode` per RESIDENT bucket,
    /// and nothing else.
    ///
    /// Published because every other memory number this engine emitted read the CACHE only -- the
    /// maintenance cycle's pressure gate and the `ShardLoad.memory_bytes` the metaserver balances
    /// on were both `cache.memory_bytes`. The bucket index was in neither, and it grows one entry
    /// per stored object, so "zero" was the one answer that is certainly wrong. Both of those now
    /// carry `bucket_index_resident_bytes` below as well; this floor stays node-only.
    ///
    /// A FLOOR, not the total: it counts the node structs and not the heap they point at (the
    /// shared object key, the map nodes). Measured over 96-byte values the true resident cost was
    /// roughly 3.5x this; `what_a_bucket_costs` prints both. Do not gate on it as though it were
    /// the whole figure -- it is published to make a growing index visible, not to size it.
    ///
    /// It also cannot move when a bucket is RELEASED, which is now a state a bucket can be in: a
    /// release frees the per-page entries and keeps the node, and this counts only nodes. The
    /// eviction gate uses `TemporalEngine::bucket_index_resident_bytes` instead, which counts both
    /// -- a gate reading this one would see a release free nothing and fire for ever.
    ///
    /// O(1): a count the stats path already holds, times a compile-time size.
    #[serde(default)]
    pub bucket_index_resident_bytes_floor: u64,
    /// What the resident bucket index costs RIGHT NOW: the nodes the floor above counts, PLUS one
    /// entry per page each bucket holds.
    ///
    /// Two numbers, deliberately, because they answer two different questions and one number
    /// cannot do both:
    ///
    ///  - `bucket_index_resident_bytes_floor` is STABLE under a release. It counts nodes, and a
    ///    release keeps every node, so it is the right thing to watch when you want "how big did
    ///    this index get" to be unaffected by whether maintenance has been through.
    ///  - this one MOVES under a release, because the per-page entries are exactly what a release
    ///    frees. That is what makes it usable as a pressure reading: acting on the pressure
    ///    reduces it, so the gate closes and the loop converges. A gate keyed on the floor would
    ///    see a release free nothing and ask again for ever.
    ///
    /// This is the same quantity `TemporalEngine::bucket_index_resident_bytes` returns and the
    /// same one `apply_storage_eviction` gates on; it is published here so the heartbeat and the
    /// metrics path can read it without taking the shard lock a second time.
    ///
    /// O(resident buckets): `block_index.len()` is O(1) per bucket, so this is one walk of the
    /// bucket map and no walk of the pages.
    #[serde(default)]
    pub bucket_index_resident_bytes: u64,
    /// How many buckets are actually resident -- `bucket_map.len()`.
    ///
    /// Distinct from `bucket_entries` above, which is the routing RANGE. The metric
    /// `slot_index_entry_count` was published from that range and therefore read
    /// 4,294,967,295 per shard no matter how large the index really was.
    #[serde(default)]
    pub bucket_index_resident_entries: u64,
    #[serde(alias = "storage_zone_count")]
    pub storage_slab_count: u64,
    #[serde(alias = "active_storage_zones")]
    pub active_storage_slabs: u64,
    #[serde(alias = "sealed_storage_zones")]
    pub sealed_storage_slabs: u64,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    #[serde(alias = "storage_zone_total_bytes")]
    pub storage_slab_total_bytes: u64,
    /// Physical bytes written per LOGICAL byte written, in PARTS PER MILLION (1.0x = 1_000_000).
    ///
    /// THE NAME ON THIS FIELD IS WRONG AND THE VALUE IS KEPT ANYWAY. `bytes_written /
    /// logical_bytes_written` is physical over logical, which is the STORAGE-write ratio. The
    /// logical-write ratio is logical bytes written over LIVE bytes -- a different numerator and a
    /// different denominator, so this is not an approximation of it. Read
    /// `storage_write_amplification_ppm` below, which carries the same value under the name that
    /// describes it, and prefer it in anything new.
    ///
    /// WHY THE WRONG NAME SURVIVES. This struct is `pub`, re-exported from `lib.rs`, and travels
    /// from a data node to the meta server inside `ShardStatInfo`. The field name is a serde key
    /// on that wire, so re-pointing it at a different quantity would leave an older peer's images
    /// decoding into the right shape with the wrong contents -- silently, which is the one failure
    /// mode worth spending a duplicated field to avoid. A `serde(alias)` cannot help here either:
    /// the old spelling and the correct quantity cannot both answer to this key.
    ///
    /// So the value here never changes. A reader that wants the ratio this name PROMISES wants
    /// `lifetime_write_amplification_ppm`, and the note on that field says why it is not spelled
    /// `logical_` anything.
    ///
    /// Zero when nothing logical has been written yet -- the convention the slab descriptor's own
    /// ratio already uses ("or 0 when the slab has no logical bytes"), so a fresh store reports 0
    /// rather than a division or a misleading 1.0.
    ///
    /// WHY IT IS PUBLISHED AT ALL. Amplification was measured in probes and described in prose and
    /// appeared in no report, so nothing watched it between measurements. A figure that is only
    /// ever produced by a probe is a figure nobody sees move.
    #[serde(default)]
    pub logical_write_amplification_ppm: u64,
    /// Physical bytes written per logical byte written, in PARTS PER MILLION -- THE CORRECT NAME
    /// for the quantity `logical_write_amplification_ppm` above has always carried.
    ///
    /// Byte-for-byte the same value as that field, deliberately: this is a renaming with the old
    /// spelling retained, not a second measurement. Below 1_000_000 means the store writes FEWER
    /// physical bytes than the workload handed it, which is what compression does -- a measured
    /// 136_718 ppm (0.137x) on a 2,000-key corpus of 256 B values.
    ///
    /// SO THIS RATIO HAS NO 1.0x FLOOR, and a gate that assumes one will fire on a healthy store.
    /// Its two halves are in different units on purpose: logical bytes are what a caller wrote and
    /// physical bytes are what landed, and the whole point of the ratio is the factor between
    /// them.
    #[serde(default)]
    pub storage_write_amplification_ppm: u64,
    /// Physical bytes written OVER THIS STORE'S LIFETIME per physical byte that is live NOW, in
    /// PARTS PER MILLION -- how many times over the store has written the data it is holding.
    ///
    /// `bytes_written` over the index's own live tally -- NOT over the slab summary's
    /// `live_physical_bytes`, which is slab-granular and cannot fall under rewrite. See the
    /// denominator note below; it is the whole reason this field is computable at all. Zero when
    /// the tally is not ready. Costs O(slabs), not O(objects), and no write-path accounting.
    ///
    /// WHAT IT IS FOR: rewrite pressure. A corpus written once reads 1_000_000 ppm however well it
    /// compresses; writing every key a second time reads 2_000_000. That is the question
    /// "amplification" is asked to answer, and neither write field above answers it --
    /// `storage_write_amplification_ppm` divides out by the compression factor, and
    /// `space_amplification_ppm` below has a numerator that is bytes RESIDENT rather than bytes
    /// written, so it cannot see a rewrite that reclaimed as it went.
    ///
    /// # THE DENOMINATOR IS BLOCK-GRANULAR, AND THE OBVIOUS ONE DOES NOT WORK
    ///
    /// `summarize_slabs` adds a slab's WHOLE `physical_bytes` to `live_physical_bytes` for every
    /// Active or Sealed slab, so "live" there means "in a slab not awaiting destruction". A slab
    /// counts fully live however many of its blocks are dead. That makes it track `bytes_written`
    /// to the byte under rewrite, and any ratio over it is PINNED AT 1.0x: measured at 2,000 keys
    /// overwritten five times, `bytes_written` and `live_physical_bytes` both read 2,680,000 B at
    /// every round while four fifths of those bytes were dead.
    ///
    /// `bucket_index.block_slab_live` sums `BlockAddress::length` over the blocks the INDEX still
    /// points at, and a block leaves the tally when its index entry does, so an overwrite retires
    /// the block it displaced. On that same corpus it holds at 536,000 B -- the live set's true
    /// size -- across all five rounds, and this ratio moves 1.000x -> 5.000x as it should.
    ///
    /// `the_live_denominator_must_fall_when_a_key_is_overwritten` holds both denominators side by
    /// side and fails if this field is ever re-pointed at the slab-granular one.
    ///
    /// # THIS RATIO HAS NO CEILING AND MUST NOT CARRY A FIXED ALERT THRESHOLD
    ///
    /// THE NUMERATOR IS A LIFETIME ACCUMULATOR AND THE DENOMINATOR IS A CURRENT READING. Nothing
    /// resets `bytes_written`: the only assignments to zero in the crate are local accumulators in
    /// `block_store/block_store_append.rs`, and no compaction, reclamation or dump path touches the counter. So
    /// on a store whose live set is roughly flat, this figure RISES WITHOUT LIMIT as a function of
    /// uptime, and a gate set at any multiple of 1.0x will eventually fire on a healthy store that
    /// has simply been up a long time. `lifetime_` is in the name for that reason.
    ///
    /// To make it gateable the numerator would have to be WINDOWED -- reset where the live set is
    /// rewritten wholesale, so the ratio means "written since the last compaction per byte live".
    /// That is a change to the write path's accounting, not a change to this arithmetic, and it is
    /// not made here. Read this field as a cumulative history, and compare it against ITSELF over
    /// time rather than against a constant.
    ///
    /// WHY IT IS NOT CALLED `logical_write_amplification_ppm`, which is the spelling the ratio
    /// logical-bytes-written over live-bytes would carry. That ratio needs both halves in the same
    /// unit to mean anything, and the live half we can produce today is COMPRESSED: an object
    /// record carries `address.length()`, the stored length, and no uncompressed size. Dividing
    /// uncompressed bytes written by compressed bytes live multiplies the real figure by the
    /// compression factor -- it reads 7.314x on a write-once corpus that has no write amplification
    /// at all, where this field reads 1.000x. Matching the units the other way, by adding an
    /// uncompressed size to the record, costs 4-8 B per object against a record the footprint
    /// campaign cut to 16, so it is a decision with a price rather than an oversight. Both halves
    /// here are stored bytes, so the compression factor cancels instead of dividing in.
    #[serde(default)]
    pub lifetime_write_amplification_ppm: u64,
    /// Physical bytes written SINCE THE LAST COMPACTION per physical byte that is live now, in
    /// PARTS PER MILLION -- the same ratio as `lifetime_write_amplification_ppm` over a window
    /// that closes whenever the live set is consolidated.
    ///
    /// `bytes_written_since_compaction / live_block_bytes`, both published beside it so the ratio
    /// is re-derivable. Zero when nothing is live.
    ///
    /// # THIS IS THE ONE OF THE TWO THAT CAN CARRY A THRESHOLD
    ///
    /// `lifetime_write_amplification_ppm` has a lifetime numerator and a current denominator, so on
    /// a store whose live set is flat it rises without limit as a function of uptime. A gate on it
    /// fires on a healthy store that has merely been up a long time, and its own note says so. This
    /// field is the same arithmetic with the history dropped at each consolidation, which is what
    /// makes a fixed trigger meaningful: it answers "how much has been written since the store last
    /// tidied itself, per byte it is holding", and a compaction brings it back down.
    ///
    /// # AND IT IS WHY COMPACTION AND WRITE AMPLIFICATION STOP FIGHTING
    ///
    /// Compaction is itself a writer. Measured on a three-slab store, one round wrote 3,697,200 B
    /// to make 1,232,400 B reclaimable. Charged against a lifetime numerator those bytes are pure
    /// amplification, so the operation that LOWERS space amplification RAISES the write figure and
    /// the two readings pull in opposite directions. Opening a window at the consolidation is what
    /// resolves that: the rewrite's cost is the last thing in the old window rather than the first
    /// thing in the new one.
    ///
    /// # WHAT IT DOES NOT PROMISE
    ///
    /// A BUDGETED OR DRAINED COMPACTION OPENS A WINDOW HAVING CONSOLIDATED ONLY PART OF THE STORE,
    /// so this reads optimistically after one. The compaction report carries
    /// `blocks_left_by_budget` and `blocks_left_off_drain_set` for exactly that question. And
    /// before the first compaction the window is the whole lifetime, so this equals
    /// `lifetime_write_amplification_ppm` -- which is correct rather than a special case.
    #[serde(default)]
    pub write_amplification_since_compaction_ppm: u64,
    /// Physical bytes written since the last compaction consolidated the live set.
    ///
    /// The numerator of `write_amplification_since_compaction_ppm`, published so that ratio is
    /// re-derivable from terms rather than trusted. Equal to `bytes_written` until the first
    /// compaction.
    #[serde(default)]
    pub bytes_written_since_compaction: u64,
    /// UNCOMPRESSED bytes written per uncompressed byte that is live, in PARTS PER MILLION -- how
    /// many times over the workload has written the data it is keeping, in the units the caller
    /// handed us rather than the units the codec produced.
    ///
    /// `logical_bytes_written / live_user_bytes`, both published beside it. Zero when nothing
    /// covered is live.
    ///
    /// # WHY THIS IS NOT A SPELLING OF `storage_write_amplification_ppm`
    ///
    /// That one is physical-over-logical and measures the CODEC: it reads 0.137x because values
    /// compress. This is user-over-user and measures REWRITE, so compression cancels out of it
    /// entirely. They move independently, and a store can have an excellent codec figure while
    /// rewriting its live set many times over.
    ///
    /// # ITS DENOMINATOR IS APPROXIMATE, AND BY A STATED AMOUNT
    ///
    /// No record holds an exact uncompressed size -- `page_size` is assigned from
    /// `address.length()` and is the STORED length. Each object instead carries a ONE-BYTE SIZE
    /// CLASS, which fits in the tail slack `ObjectRecord` had already pinned and therefore costs no
    /// width at all. The class is a four-bit exponent and four-bit mantissa, so it is within about
    /// 3% per object and measured at 1.5% over a corpus of lengths chosen to fall between classes.
    /// An exact `u32` would have taken the record from 16 bytes to 24.
    ///
    /// The alternative that needs no field -- dividing the stored tally by the store-wide
    /// compression ratio -- was measured at **67.6% out**, because it assumes the live set
    /// compresses like the history. It reported 1.013x for a live set written 1.698x over. See
    /// `what_a_live_user_bytes_denominator_would_cost`.
    ///
    /// # AND IT IS ONLY AS COMPLETE AS `live_objects_with_logical_size`
    ///
    /// Classes are installed by the string write path. An object written by another path, or
    /// decoded from an image predating the field, carries none and contributes nothing to either
    /// term. READ THE COVERAGE COUNT BEFORE THIS RATIO: a partial denominator makes this figure
    /// too HIGH, and nothing in the ppm value says so. The totals are also `serde(skip)`, so after
    /// a reload they are 0 until writes repopulate them -- the classes survive on the records, but
    /// nothing re-sums them on the load path.
    #[serde(default)]
    pub user_write_amplification_ppm: u64,
    /// Uncompressed bytes of the live objects that carry a size class.
    ///
    /// The denominator of `user_write_amplification_ppm`, published so the ratio is re-derivable
    /// rather than trusted.
    #[serde(default)]
    pub live_user_bytes: u64,
    /// How many live objects contributed to `live_user_bytes`.
    ///
    /// Coverage. Compare it against the live object count: equal means the figure above describes
    /// the whole live set, and anything less means it describes a subset and reads high.
    #[serde(default)]
    pub live_objects_with_logical_size: u64,
    /// Physical bytes the store occupies per physical byte that is live, in PARTS PER MILLION.
    ///
    /// `total_known_physical_bytes / live_physical_bytes` from the slab summary. What pushes it
    /// above 1_000_000 is reclaimable space a compaction has not taken yet, which is the thing this
    /// ratio exists to make visible. Zero when nothing is live.
    ///
    /// NOT THE SAME QUANTITY AS THE SLAB UTILISATION ALREADY ON A DESCRIPTOR, which is per slab and
    /// expressed in basis points; this is the whole store in ppm.
    ///
    /// # ITS DENOMINATOR IS SLAB-GRANULAR, SO IT CANNOT SEE A DEAD BLOCK INSIDE A LIVE SLAB
    ///
    /// This ratio computes what its name says, and its VALUE is nonetheless pinned near 1.0x for
    /// the ordinary shape of space amplification. `live_physical_bytes` counts an Active or Sealed
    /// slab's whole `physical_bytes`, so the figure can only move once an ENTIRE slab reaches
    /// delayed-destroy. Measured at 2,000 keys overwritten five times: 1.000x at every round, with
    /// `reclaimable_physical_bytes` never leaving zero, where the block-granular figure is 5.0x.
    ///
    /// IT CAN MOVE, BUT ONLY AFTER A COMPACTION, NOT AFTER THE REWRITE THAT CAUSED THE WASTE. The
    /// one state that shifts it is a whole slab reaching `DelayedDestroy`, and only the compaction
    /// phase produces that -- so the figure lags the condition it describes by however long it
    /// takes a collector to quarantine a slab, and then by `DELAYED_DESTROY_MIN_AGE_MS` before the
    /// space is actually returned.
    ///
    /// SO DO NOT PUT AN ALERT ON THIS EXPECTING IT TO CATCH RECLAIMABLE SPACE. It reports slab
    /// states, not how much of the data is dead. The block-granular quantity is
    /// `bucket_index.block_slab_live`, which `lifetime_write_amplification_ppm` above uses; moving
    /// THIS field onto it would change a published value that consumers already read, so it is
    /// recorded here rather than done silently.
    #[serde(default)]
    pub space_amplification_ppm: u64,
    /// Bytes still on disk per stored byte the INDEX still points at, in PARTS PER MILLION.
    ///
    /// `non_purged_physical_bytes / live_block_bytes`. This is the figure
    /// [`Self::space_amplification_ppm`] above cannot produce, published beside it rather than
    /// replacing it: both of that field's halves are blind to reclamation, and re-pointing a serde
    /// key that travels to the meta server would decode into the right shape with the wrong
    /// contents on an older peer.
    ///
    /// # BOTH HALVES MOVE IN THE DIRECTION THE RATIO CLAIMS TO MEASURE
    ///
    /// The numerator drops when a purge deletes a slab's files, because it excludes purged slabs.
    /// The denominator drops when an overwrite retires the block its index entry pointed at,
    /// because it is the index's own per-slab tally rather than a slab-file size. Measured over
    /// four rewrites of every key: **1.000x -> 5.000x**, where the pinned field held 1.000x at
    /// every round. See `a_space_ratio_whose_numerator_falls`.
    ///
    /// # ITS FLOOR IS EXACTLY 1.0x, AND BOTH TERMS ARE STORED BYTES
    ///
    /// AN EARLIER VERSION OF THIS NOTE SAID THE FLOOR WAS ABOVE 1.0x, on the reasoning that the
    /// numerator is file bytes and the denominator block payload bytes, so framing would sit in one
    /// half only. **The arms that measure it say otherwise**, and they said so in output that was
    /// already in front of the claim: `a_space_ratio_whose_numerator_falls` reports `after 1
    /// write(s) 268000 268000 1.000x`, and `the_live_and_stale_shares_are_a_partition` reports a
    /// stale share of exactly 0.00% on the same store.
    ///
    /// The terms agree because [`Self::live_block_bytes`] sums `BlockAddress::length` -- what was
    /// STORED, after compression and including the block envelope -- and
    /// `total_known_physical_bytes` is the file size of those same bytes. Both sides are stored
    /// bytes, so this ratio is unit-matched and reads exactly 1.000x on a store with nothing dead
    /// in it.
    ///
    /// FRAMING IS REAL BUT BELONGS TO A DIFFERENT DENOMINATOR: 536,000 B on disk for 512,000 B of
    /// USER bytes, 1.047x, which appears when stored bytes are divided by PRE-COMPRESSION bytes.
    /// That is the mixed-unit division these fields exist to avoid, and it does not arise between
    /// two stored-byte terms.
    ///
    /// Zero when the live tally has not been derived yet, which is the same condition under which
    /// [`Self::lifetime_write_amplification_ppm`] reports 0 ppm, so the two agree about when they
    /// are unavailable instead of one of them reading as a real figure.
    #[serde(default)]
    pub block_space_amplification_ppm: u64,
    /// # THE RECORDED-STALENESS GROUP, AND WHY IT IS NOT THE FIELD ABOVE
    ///
    /// Everything from here to `recorded_space_amplification_ppm` comes from a record the write
    /// path keeps: when an overwrite, a delete or an expiry sweep displaces a block's index entry,
    /// the byte range it retired is appended to a per-slab list in the same call that discharges
    /// the live tally. So these figures move at the moment the waste is created, where
    /// `space_amplification_ppm` above cannot move until a whole slab reaches delayed-destroy and
    /// `storage_slab_stale_bytes` below cannot move until a storage-manager cycle has published the
    /// union tally.
    ///
    /// # TWO FIGURES CAN SEE A DEAD BLOCK INSIDE A LIVE SLAB, AND THEY ARE NOT THE SAME FIGURE
    ///
    /// `space_amplification_ppm` is deliberately left where it is: re-pointing its denominator
