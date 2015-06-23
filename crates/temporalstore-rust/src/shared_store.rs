// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalstore_snapshot::object_store::{
    AppendBlobReceipt, FileObjectStore, MatrixObjectHttpStore, ObjectStore, ObjectStoreError,
};
use thiserror::Error;

use tokio::sync::oneshot;

use crate::block_store::{BlockStoreError, LazyCheckpointSlab, BlockStore, SharedSlabSource};
use crate::engine::TemporalEngine;
use crate::sdk::{self, v1};
use crate::types::{Command, ExecuteRequest, ShardId, Status};

#[derive(Debug, Error)]
pub enum SharedStoreReplicationError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error(
        "the published index for shard {shard_id} carries no routing range ({what}), so the \
         buckets it names cannot be accounted for. It was published before the range travelled \
         with the index; re-publish from a node running this build."
    )]
    RoutingRangeMissing { shard_id: ShardId, what: String },
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("no shared-store checkpoint found for shard {0}")]
    CheckpointNotFound(ShardId),
    #[error(
        "checkpoint for shard {shard_id} references live slab {block_slab_id} that was not uploaded; refusing to publish a manifest that would lose durable pages"
    )]
    CheckpointSlabNotDurable {
        shard_id: ShardId,
        block_slab_id: u64,
    },
    #[error("replicated command failed at WAL index {wal_index}: {status:?}")]
    ApplyFailed { wal_index: u64, status: Status },
    /// The durable single-writer fence rejected this operation: a newer owner (higher
    /// load_version) holds the shard lease, so this (stale) writer must ABORT rather than
    /// double-append to the shared WAL. Maps [`ObjectStoreError::ConditionFailed`].
    #[error("shared-store fence rejected write: {detail}")]
    StoreConditionFailed { detail: String },
    /// A lease acquisition was refused because the object store already holds a lease at an
    /// equal-or-higher load_version — this writer's ownership claim is stale.
    #[error(
        "shared-store lease for shard {shard_id} held at load_version {current} >= attempted {attempted}"
    )]
    StaleOwnership {
        shard_id: ShardId,
        current: u64,
        attempted: u64,
    },
    #[error("WAL replay gap: expected index {expected}, got {actual}")]
    ReplayGap { expected: u64, actual: u64 },
    #[error(
        "shared-store GC would remove WAL entry needed by replay cursor {cursor_wal_index} before retain {retain_from_wal_index}"
    )]
    GcBlockedByReplayCursor {
        cursor_wal_index: u64,
        retain_from_wal_index: u64,
    },
    #[error(
        "shared-store checkpoint GC would remove checkpoint {checkpoint_id} at WAL index {checkpoint_wal_index} needed by replay cursor {cursor_wal_index}"
    )]
    CheckpointGcBlockedByReplayCursor {
        cursor_wal_index: u64,
        checkpoint_wal_index: u64,
        checkpoint_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalEntry {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    /// The operation, for an entry that cannot say what it DID.
    ///
    /// Absent once the entry carries results, exactly as in the engine record it comes from. An
    /// entry published before results existed still carries one, and a successor still replays it
    /// by re-running it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Command>,
    /// The pages this write produced, carried beside the command that produced them.
    ///
    /// A command is not always enough to rebuild what it wrote. A page can be DERIVED state --
    /// a serialized counter series, a hash map -- and re-executing the command that bumped it
    /// reconstructs it only if every earlier write is also replayed, in order, from a state
    /// that still exists. Locally that is what the engine WAL record carries, for exactly this
    /// reason; a shared log that carried commands alone could hand a successor a shard it could
    /// not finish rebuilding.
    ///
    /// Empty for the overwhelming majority of writes, and `serde(default)` so an entry written
    /// before this field existed still loads.
    #[serde(default)]
    pub staged_blocks: Vec<crate::wal::StagedBlock>,
    /// What this write DID, so a successor can install results instead of re-running operations.
    ///
    /// Carrying pages was the same idea reached halfway: a page is derived state the command
    /// cannot rebuild, so the pages travelled beside the command. These finish the thought. A
    /// successor that installs them needs no clock of its own, no eviction config from the
    /// original node, and no assumption that re-executing here lands where it landed there.
    ///
    /// `serde(default)` and skipped when empty, so an entry written before this reads back
    /// unchanged and replays exactly as it used to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outcomes: Vec<crate::wal::WalOutcomeItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalObject {
    pub entry: SharedStoreWalEntry,
    pub entry_byte_size: u64,
    pub entry_sha256: String,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWalOffsetMetadata {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    /// WHICH blob the entry's frame is in.
    ///
    /// Always populated for a reader. It is not always WRITTEN: a sidecar frame whose key is the
    /// one its own shard derives omits it, and the decoder puts it back. See
    /// [`encode_wal_offset_metadata_frame`].
    pub wal_blob_key: String,
    pub wal_blob_start_offset: u64,
    pub wal_blob_end_offset: u64,
    pub wal_blob_bytes_written: u64,
    pub wal_blob_object_length: u64,
    #[serde(default)]
    pub wal_blob_physical_slab_count: u64,
    pub wal_blob_first_physical_offset: Option<u64>,
    pub command_byte_size: u64,
    pub command_encoding: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalIndexedRead {
    pub metadata: SharedStoreWalOffsetMetadata,
    pub entry: SharedStoreWalEntry,
    pub range_bytes_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreBlockSlab {
    #[serde(alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub key: String,
    pub byte_size: u64,
    pub sha256: String,
    // Per-slab SEALED-slab metadata carried so a lazy restore can install complete slab
    // descriptors (physical/logical bytes + page-id range) BEFORE the first on-demand slab
    // fetch, keeping GC/compaction accounting from under-counting sealed shared slabs between
    // restore and first fetch. All default so older manifests (without these fields) still load;
    // `byte_size` above is the slab's physical byte size.
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(rename = "first_block_id", alias = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreCheckpointManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub checkpoint_id: String,
    #[serde(rename = "checkpoint_wal_index")]
    pub checkpoint_wal_index: u64,
    pub created_at_ms: u64,
    pub index_key: String,
    pub index_byte_size: u64,
    pub index_sha256: String,
    pub block_slabs: Vec<SharedStoreBlockSlab>,
    /// Next free block page id at checkpoint time. A lazy restore advances the fresh
    /// owner's page-id counter past this floor so replayed/new writes never reuse a
    /// page id that a lazily-fetched checkpoint slab still carries. Defaults to 0 for
    /// manifests written before this field existed (backward compatible).
    #[serde(default)]
    pub next_block_id: u64,
    /// THE ROUTING RANGE THE CHECKPOINTED INDEX WAS BUILT ON.
    ///
    /// The index names buckets and a bucket is `start + hash(key) % (end - start + 1)`, so a
    /// checkpoint without this leaves a restoring node holding bucket numbers it cannot account
    /// for. `Option` so a manifest written before this field decodes and is REFUSED by name rather
    /// than failing to parse -- one shape, current or refused, no second reader.
    #[serde(default)]
    pub routing_range: Option<crate::engine::routing_range_stamp::RoutingRangeStamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreReplayCursor {
    pub shard_id: ShardId,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    pub last_replay_time_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SharedStoreStorageMode {
    Sync,
    Async,
}

impl Default for SharedStoreStorageMode {
    fn default() -> Self {
        Self::Async
    }
}

impl SharedStoreStorageMode {
    pub fn from_sync_flag(sync: bool) -> Self {
        if sync {
            Self::Sync
        } else {
            Self::Async
        }
    }

    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SharedStoreWalAppendMode {
    /// Explicit compatibility mode for stores that only support whole-object puts.
    JsonPerKey,
    /// Default binary WAL layout; requires a backend with real append support.
    ProtobufAppendBlob,
}

impl Default for SharedStoreWalAppendMode {
    fn default() -> Self {
        Self::ProtobufAppendBlob
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWriteReport {
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub published: bool,
    pub queued: bool,
    #[serde(default)]
    pub wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_bytes_written: Option<u64>,
    #[serde(default)]
    pub wal_blob_object_length: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreFlushReport {
    pub flushed: usize,
    pub remaining: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub last_wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_object_length: Option<u64>,
}

/// How long a consumer cursor may hold the retention point before it is abandoned.
///
/// Zero disables a rule. Both zero is the shape a replicator has when nothing configures a
/// policy, and it means a consumer holds the log for as long as its cursor exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreCursorAbandonPolicy {
    /// Maximum time since the consumer last recorded an advance. Only applied to a cursor that
    /// HAS recorded one -- see [`SharedStoreReplicator::abandon_stalled_replay_cursor`].
    pub max_cursor_age_ms: u64,
    /// Maximum number of log entries the consumer may sit behind the newest published index.
    pub max_cursor_lag_entries: u64,
}

/// A consumer cursor that was dropped because it held the retention point past its bound.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreAbandonedCursor {
    pub shard_id: ShardId,
    pub cursor_wal_index: u64,
    /// Zero when the cursor recorded no time, which is also when the age rule declines to judge
    /// it.
    pub cursor_age_ms: u64,
    pub cursor_lag_entries: u64,
    /// Which rule fired: `cursor_age_above_bound` or `cursor_lag_entries_above_bound`.
    pub reason: String,
}

/// What one pass of [`SharedStoreReplicator::collect_to_checkpoint`] reclaimed, and the two
/// positions it reasoned from.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreCollectionReport {
    pub shard_id: ShardId,
    /// The log position the newest checkpoint covers. Zero when there is no checkpoint, in which
    /// case nothing was collected and `blocked_reason` names why.
    pub checkpoint_wal_index: u64,
    /// The lowest log index kept; everything below it was deleted.
    pub retain_from_wal_index: u64,
    /// `None` when no consumer has ever written a cursor -- which is NOT the same as a consumer
    /// sitting at zero, and the two take different decisions here.
    pub cursor_wal_index: Option<u64>,
    /// True when a consumer held the retention point BELOW where the checkpoint alone would have
    /// put it.
    pub clamped_to_cursor: bool,
    pub deleted_wal_objects: usize,
    pub deleted_checkpoints: usize,
    pub deleted_checkpoint_objects: usize,
    /// Empty when a pass ran. Non-empty names why a pass declined to collect, which is a
    /// different thing from a pass that ran and found nothing to collect.
    pub blocked_reason: String,
    /// The position a consumer held when its cursor was dropped for holding the log past its
    /// bound, or `None` when no cursor was dropped.
    pub abandoned_cursor_wal_index: Option<u64>,
    /// Which abandon rule fired, empty when none did.
    pub abandoned_reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreGcReport {
    pub shard_id: ShardId,
    #[serde(rename = "deleted_wal_objects")]
    pub deleted_wal_objects: usize,
    pub deleted_checkpoints: usize,
    pub deleted_checkpoint_objects: usize,
    pub retained_checkpoint_ids: Vec<String>,
    #[serde(rename = "retained_for_cursor_wal_index", default)]
    pub retained_for_cursor_wal_index: Option<u64>,
    #[serde(default)]
    pub retained_for_cursor_checkpoint_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreRetryPolicy {
    pub max_attempts: usize,
    pub backoff_ms: u64,
}

impl Default for SharedStoreRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_ms: 0,
        }
    }
}

#[derive(Debug)]
pub struct SharedStoreReplicator<O> {
    cluster_id: String,
    object_store: Arc<O>,
    retry_policy: SharedStoreRetryPolicy,
    wal_append_mode: SharedStoreWalAppendMode,
    /// Which log frame format this writer emits. See [`WAL_FRAME_FORMAT_DEFAULT`] for why the
    /// default is the wide one. Reading is unaffected: a reader understands every format it
    /// knows regardless of what it writes.
    wal_frame_format: u32,
    /// Durable single-writer fence, configured by `with_fence` and applying whenever it is.
    /// Every WAL append and checkpoint publish then re-validates that this writer's
    /// `load_version` still owns the shard lease in the object store, aborting a superseded
    /// stale owner before it can double-append.
    ///
    /// `TS_SHARED_STORE_FENCE` used to gate this as well, so a caller could configure a fence
    /// and have it silently not apply. Nothing outside the R2 test configures one, so nothing
    /// production does changes by the gate going: with no fence there is nothing to enforce.
    fence: Option<ShardFenceConfig>,
    /// When a consumer cursor stops being treated as live. `None` means never, which is what a
    /// replicator does with no policy configured.
    cursor_abandon: Option<SharedStoreCursorAbandonPolicy>,
}

/// Ownership token carried by a fenced replicator: the `load_version` (monotonic ownership
/// epoch) this writer believes it holds, plus a human-readable owner tag for diagnostics.
#[derive(Debug, Clone)]
struct ShardFenceConfig {
    load_version: u64,
    /// Retained for diagnostics / future lease-holder attribution; the fence decision keys on
    /// `load_version` alone.
    #[allow(dead_code)]
    owner: String,
}

/// Durable lease object persisted in the shared object store, keyed by cluster+shard. The
/// `load_version` is the fence token: a writer may only install a lease strictly greater than
/// the currently stored one, and may only append while the stored value still equals its own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ShardLease {
    load_version: u64,
    owner: String,
}

/// TS_SHARED_STORE_MAX_PENDING: how many entries the async queue may hold before a write
/// stops being allowed to defer its own durability.
///
/// The async path acks a write once its entry is on an in-memory queue, so the queue depth IS
/// the size of what a non-graceful exit loses. Unbounded, a store that is merely slow turns
/// every ack into memory the process never gets back, and the eventual loss is the entire
/// backlog -- the failure gets worse the longer it goes unnoticed, which is backwards.
///
/// At the cap the next write publishes itself synchronously instead of queueing. The ack slows
/// to the store's own latency, which is precisely the signal a caller needs, and the backlog
/// stops growing. `0` restores the previous unbounded behaviour.
fn shared_store_max_pending() -> usize {
    std::env::var("TS_SHARED_STORE_MAX_PENDING")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(50_000)
}

impl<O> Clone for SharedStoreReplicator<O> {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            object_store: Arc::clone(&self.object_store),
            retry_policy: self.retry_policy,
            wal_append_mode: self.wal_append_mode,
            wal_frame_format: self.wal_frame_format,
            fence: self.fence.clone(),
            cursor_abandon: self.cursor_abandon,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreStagedBlockProto {
    #[prost(uint64, tag = "1")]
    object_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    bytes: Vec<u8>,
    /// Which element of the object this page holds.
    ///
    /// The same field `WalStagedBlock` carries in the local log, and for the same reason: a
    /// frame carries many pages and a reader picks one out by identity. A follower that
    /// received the pages without their elements would resolve the frame and then serve the
    /// FIRST page of the object for every element of it -- which is the failure this exists
    /// to remove, moved to the reader that has no other copy of the bytes.
    ///
    /// `optional`, so a frame written for a whole-object page omits it and stays
    /// byte-identical to one written before this field existed.
    #[prost(string, optional, tag = "3")]
    component: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalFrameProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    command_byte_size: u64,
    #[prost(string, tag = "5")]
    command_sha256: String,
    #[prost(uint32, tag = "6")]
    command_encoding: u32,
    /// Tag 7, added after the fact: an older reader ignores it and an older writer leaves it
    /// empty, so both directions stay readable across the change.
    #[prost(message, repeated, tag = "7")]
    staged_blocks: Vec<SharedStoreStagedBlockProto>,
    /// What the write DID, in the SAME message the engine log uses.
    ///
    /// Not a shared-store item type: the shared log needs a destination, not a schema. Carrying
    /// the engine's item means a successor installs exactly what the origin recorded, with no
    /// conversion in between to disagree about.
    ///
    /// Tag 8, same compatibility shape as tag 7. Without it this path would carry the command and
    /// silently drop the results, which is worse than not carrying them at all -- the successor
    /// would re-execute and look correct.
    #[prost(message, repeated, tag = "8")]
    items: Vec<crate::sdk::v1::EngineWalItem>,
    /// The command digest as its own bytes rather than as hex text. Populated only by
    /// [`WAL_FRAME_FORMAT_RAW_DIGEST`] frames; empty in a hex frame.
    ///
    /// Tag 9 has never been used by this message, so a frame carrying it is unambiguous.
    #[prost(bytes = "vec", tag = "9")]
    command_digest: Vec<u8>,
    /// WHICH DIGEST SHAPE THIS FRAME CARRIES. The frame is self-describing.
    ///
    /// Absent is zero is [`WAL_FRAME_FORMAT_HEX_DIGEST`], so every frame written before this
    /// field existed reads as the original shape with no conversion and no migration. A reader
    /// dispatches on it per frame, which is what lets one blob hold both shapes -- exactly the
    /// mixture a rolling restart leaves behind, and the same rule `log_framing::next_frame`
    /// follows when it picks a checksum by the record's own leading bytes.
    #[prost(uint32, tag = "10")]
    frame_format: u32,
    /// A CRC32C of the payload. Populated only by [`WAL_FRAME_FORMAT_CRC32C`] frames.
    ///
    /// `fixed32`, not `uint32`: a CRC's value is spread across all 32 bits, so a varint carrying
    /// one costs five bytes far more often than it costs one. Fixed is four, always.
    #[prost(fixed32, tag = "11")]
    command_crc32c: u32,
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalOffsetMetadataProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    /// Empty when it is the key the shard derives. Tag 3 stays a plain `string`, so a sidecar
    /// written before this field could be omitted still decodes with its key intact.
    #[prost(string, tag = "3")]
    wal_blob_key: String,
    #[prost(uint64, tag = "4")]
    wal_blob_start_offset: u64,
    #[prost(uint64, tag = "5")]
    wal_blob_end_offset: u64,
    /// How many bytes this entry's frame occupies. DERIVABLE AND DELIBERATELY KEPT.
    ///
    /// It is `wal_blob_end_offset - wal_blob_start_offset` in every receipt every object store in
    /// this crate produces, so dropping it and deriving it would round-trip exactly. It stays
    /// because `read_wal_entry_from_offset_metadata` compares the two, and that comparison is the
    /// ONLY INTEGRITY CHECK THIS RECORD HAS.
    ///
    /// It became the only one in #2319. That change retired the sidecar's `command_sha256` as an
    /// unread duplicate of the digest the log frame carries, which it was -- but the log frame's
    /// digest covers the log frame's PAYLOAD, not this message. So after #2319 nothing checksums
    /// the sidecar at all, and the agreement between this field and the offsets beside it is what
    /// is left: a bit-flip in one of those varints that still parses is caught here and nowhere
    /// else.
    ///
    /// That makes this field's redundancy load-bearing as a CONSEQUENCE of an earlier saving in
    /// the same effort, which is not a thing a reader could infer from either change alone. The
    /// guard is also uncovered by the default test gate -- see the comment on the check itself --
    /// so deleting this field would have cost the sidecar its last integrity check with nothing
    /// failing.
    #[prost(uint64, tag = "6")]
    wal_blob_bytes_written: u64,
    #[prost(uint64, tag = "7")]
    wal_blob_object_length: u64,
    #[prost(uint64, tag = "8")]
    command_byte_size: u64,
    // Tag 9 WAS `command_sha256`, a second hex copy of the digest the frame already carries.
    // Retired: see `encode_wal_offset_metadata_frame`. A sidecar written before the retirement
    // still carries tag 9, and prost skips it as an unknown field, so old sidecars keep decoding.
    #[prost(uint32, tag = "10")]
    command_encoding: u32,
    #[prost(uint64, tag = "11")]
    wal_blob_physical_slab_count: u64,
    #[prost(uint64, optional, tag = "12")]
    wal_blob_first_physical_offset: Option<u64>,
}

#[derive(Debug)]
pub struct SharedStoreStorageWriter<O> {
    replicator: SharedStoreReplicator<O>,
    mode: SharedStoreStorageMode,
    next_wal_index: AtomicU64,
    pending: Mutex<VecDeque<SharedStoreWalEntry>>,
    /// Depth at which the async path stops deferring. See [`shared_store_max_pending`].
    max_pending: usize,
    /// How many writes have been forced to publish themselves because the queue was full.
    /// Non-zero means the store is not keeping up and acks are paying for it.
    capacity_hits: AtomicU64,
    /// Timer-less queue-coalesced group commit for the SYNC path. When enabled, concurrent
    /// sync writers append their entry to `commit` and await a covering durable barrier instead
    /// of each publishing inline, amortizing N object-store appends onto ~1 per group. Ignored
    /// on the async path (that path is already off the ack critical path).
    group_commit: bool,
    /// Optional deliberate widening of the group-commit window under extreme load. 0 (default)
    /// keeps it purely timer-less — the group is exactly what accumulates during one append's
    /// in-flight duration.
    commit_delay: Duration,
    /// Group-commit staging buffer + leader flag. A writer pushes its `(entry, waker)` here; the
    /// first arrival becomes the flusher (leader) and every later arrival awaits its waker.
    commit: Mutex<GroupCommitBuffer>,
}

/// One sync writer waiting on the group-commit barrier: its entry (staged for the next flush)
/// and the channel the flusher wakes once the covering append has (or has not) reached the store.
#[derive(Debug)]
struct GroupCommitWaiter {
    entry: SharedStoreWalEntry,
    waker: oneshot::Sender<GroupCommitOutcome>,
}

#[derive(Debug, Default)]
struct GroupCommitBuffer {
    /// Entries appended by writers but not yet covered by a completed durable barrier.
    queue: VecDeque<GroupCommitWaiter>,
    /// A leader is currently running (or about to run) a flush round. Only the leader clears this,
    /// and only under the lock once the queue is drained empty — so no entry is ever stranded.
    flushing: bool,
}

/// Result the flusher hands each waiter. Errors are stringified so a single failed append can
/// fan out to every covered writer (the typed `SharedStoreReplicationError` is not `Clone`); a
/// stringified error is still an error — the durability contract only requires that a covered
/// writer whose barrier failed is NEVER told Ok.
#[derive(Debug, Clone)]
enum GroupCommitOutcome {
    Committed(SharedStoreWriteReport),
    Failed(String),
}

impl GroupCommitOutcome {
    fn into_result(self) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        match self {
            GroupCommitOutcome::Committed(report) => Ok(report),
            GroupCommitOutcome::Failed(message) => Err(SharedStoreReplicationError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, message),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub applied: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub offset_index_reads: usize,
    #[serde(default)]
    pub range_bytes_read: u64,
}

impl<O> SharedStoreReplicator<O>
where
    O: ObjectStore + 'static,
{
    pub fn new(cluster_id: impl Into<String>, object_store: Arc<O>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy::default(),
            wal_append_mode: SharedStoreWalAppendMode::default(),
            wal_frame_format: WAL_FRAME_FORMAT_DEFAULT,
            fence: None,
            cursor_abandon: None,
        }
    }

    pub fn with_retry_policy(
        cluster_id: impl Into<String>,
        object_store: Arc<O>,
        retry_policy: SharedStoreRetryPolicy,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy {
                max_attempts: retry_policy.max_attempts.max(1),
                backoff_ms: retry_policy.backoff_ms,
            },
            wal_append_mode: SharedStoreWalAppendMode::default(),
            wal_frame_format: WAL_FRAME_FORMAT_DEFAULT,
            fence: None,
            cursor_abandon: None,
        }
    }

    /// When a consumer cursor stops being treated as live. See
    /// [`Self::abandon_stalled_replay_cursor`].
    pub fn with_cursor_abandon_policy(mut self, policy: SharedStoreCursorAbandonPolicy) -> Self {
        self.cursor_abandon = Some(policy);
        self
    }

    pub fn with_wal_append_mode(mut self, mode: SharedStoreWalAppendMode) -> Self {
        self.wal_append_mode = mode;
        self
    }

    /// Choose between the narrowest log frame this build writes and the original wide one.
    ///
    /// `true` tracks [`WAL_FRAME_FORMAT_DEFAULT`] -- "as narrow as this build goes" rather than
    /// one named format, so a caller asking for narrow frames keeps getting them as the default
    /// moves. A caller that needs a SPECIFIC format wants [`Self::with_wal_frame_format`].
    ///
    /// `false` is the way back, and it is the reason this method is the backstop rather than a
    /// convenience: it returns the frame as it was before `frame_format` existed, byte for byte,
    /// with no rebuild. Asserted by
    /// `a_wide_frame_is_byte_identical_to_one_written_before_the_format_field_existed`.
    ///
    /// SAFE ONLY ONCE EVERY READER UNDERSTANDS WHAT IS WRITTEN. A reader that does not know a
    /// format refuses it -- loudly, and by name, but it refuses. See
    /// [`WAL_FRAME_FORMAT_DEFAULT`].
    ///
    /// Frames already written are untouched and stay readable: the format travels in the frame.
    pub fn with_narrow_wal_frames(mut self, narrow: bool) -> Self {
        self.wal_frame_format = if narrow {
            WAL_FRAME_FORMAT_DEFAULT
        } else {
            WAL_FRAME_FORMAT_HEX_DIGEST
        };
        self
    }

    /// Emit the log frame format `format`, for a caller that needs a specific one.
    ///
    /// The same rule as [`Self::with_narrow_wal_frames`] and for the same reason: a format is
    /// safe to WRITE only once every reader understands it, so this is not a knob to reach for
    /// without knowing what the rest of the cluster is running.
    ///
    /// An unknown format here is NOT rejected at the builder: the encoder falls back to the wide
    /// format rather than writing a frame no reader can verify. A frame is always one of the
    /// shapes this build can read back.
    pub fn with_wal_frame_format(mut self, format: u32) -> Self {
        self.wal_frame_format = format;
        self
    }

    /// Attach a durable single-writer fence: this replicator (and every storage writer it
    /// spawns) claims `load_version` for its shard leases, and re-validates that claim on every
    /// append and checkpoint. Combine with
    /// [`acquire_shard_lease`](Self::acquire_shard_lease) to install the lease.
    ///
    /// Attaching one IS enabling it. `TS_SHARED_STORE_FENCE` used to be a second condition, so a
    /// caller could ask for a fence and not get one.
    pub fn with_fence(mut self, load_version: u64, owner: impl Into<String>) -> Self {
        self.fence = Some(ShardFenceConfig {
            load_version,
            owner: owner.into(),
        });
        self
    }

    fn lease_key(&self, shard_id: ShardId) -> String {
        format!("leases/{}/shard_{}.lease", self.cluster_id, shard_id)
    }

    /// Durably claim ownership of `shard_id` at `load_version` via a store compare-and-set.
    /// Succeeds only if the currently persisted lease is absent or holds a strictly lower
    /// load_version; a stale writer (equal-or-lower load_version) is rejected with
    /// [`SharedStoreReplicationError::StaleOwnership`], and a lost CAS race with
    /// [`SharedStoreReplicationError::StoreConditionFailed`]. This is the ONLY way a writer
    /// becomes the fenced owner — it does not mutate in-memory state, so ownership is decided
    /// by the durable store, not a local view.
    pub async fn acquire_shard_lease(
        &self,
        shard_id: ShardId,
        load_version: u64,
        owner: impl Into<String>,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.lease_key(shard_id);
        let current_bytes = match self.object_store.get(&key).await {
            Ok(bytes) => Some(bytes),
            Err(ObjectStoreError::NotFound(_)) => None,
            Err(err) => return Err(err.into()),
        };
        if let Some(bytes) = &current_bytes {
            let current: ShardLease = serde_json::from_slice(bytes)?;
            if current.load_version >= load_version {
                return Err(SharedStoreReplicationError::StaleOwnership {
                    shard_id,
                    current: current.load_version,
                    attempted: load_version,
                });
            }
        }
        let lease = ShardLease {
            load_version,
            owner: owner.into(),
        };
        let new_bytes = Bytes::from(serde_json::to_vec(&lease)?);
        match self
            .object_store
            .compare_and_swap(&key, current_bytes, new_bytes)
            .await
        {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::ConditionFailed { detail, .. }) => {
                Err(SharedStoreReplicationError::StoreConditionFailed { detail })
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Re-read the durable lease and confirm it still records `load_version`. Any other value
    /// (a newer owner took over) or a missing lease is a fence breach → StoreConditionFailed.
    pub async fn validate_shard_lease(
        &self,
        shard_id: ShardId,
        load_version: u64,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.lease_key(shard_id);
        let bytes = match self.object_store.get(&key).await {
            Ok(bytes) => bytes,
            Err(ObjectStoreError::NotFound(_)) => {
                return Err(SharedStoreReplicationError::StoreConditionFailed {
                    detail: format!("shard {shard_id} lease missing; ownership not held"),
                });
            }
            Err(err) => return Err(err.into()),
        };
        let current: ShardLease = serde_json::from_slice(&bytes)?;
        if current.load_version != load_version {
            return Err(SharedStoreReplicationError::StoreConditionFailed {
                detail: format!(
                    "shard {shard_id} lease held by load_version {} (owner {}), not {}",
                    current.load_version, current.owner, load_version
                ),
            });
        }
        Ok(())
    }

    /// Fence check invoked on every WAL append + checkpoint publish. A no-op unless a fence is
    /// configured, which is a decision the caller makes by name, so the default write path is
    /// unchanged. When active it re-validates the durable lease, aborting a superseded writer.
    async fn enforce_fence(
        &self,
        shard_id: ShardId,
    ) -> Result<(), SharedStoreReplicationError> {
        let Some(fence) = &self.fence else {
            return Ok(());
        };
        self.validate_shard_lease(shard_id, fence.load_version).await
    }

    pub async fn publish_wal_entry(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        // R2 single-writer fence: re-validate ownership before appending to the shared WAL so
        // a partitioned-but-alive stale owner is rejected rather than double-appending.
        self.enforce_fence(entry.shard_id).await?;
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            return self.publish_wal_entry_protobuf_blob(entry).await;
        }
        let key = self.wal_key(entry.shard_id, entry.wal_index);
        let entry_bytes = serde_json::to_vec(&entry)?;
        let object = SharedStoreWalObject {
            entry,
            entry_byte_size: entry_bytes.len() as u64,
            entry_sha256: sha256_hex(&entry_bytes),
        };
        self.put_with_retry(&key, Bytes::from(serde_json::to_vec(&object)?))
            .await?;
        Ok(None)
    }

    async fn publish_wal_entry_protobuf_blob(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let key = self.wal_blob_key(entry.shard_id);
        let command_metadata = wal_command_metadata(entry.command.as_ref())?;
        let frame = encode_wal_proto_frame(&entry, self.wal_frame_format)?;
        let receipt = self
            .append_blob_with_retry(&key, Bytes::from(frame))
            .await?
            .expect("protobuf append blob must return a receipt");
        self.publish_wal_offset_metadata(&entry, &key, &receipt, command_metadata)
            .await?;
        Ok(Some(receipt))
    }

    async fn publish_wal_offset_metadata(
        &self,
        entry: &SharedStoreWalEntry,
        wal_blob_key: &str,
        receipt: &AppendBlobReceipt,
        command_metadata: WalCommandMetadata,
    ) -> Result<(), SharedStoreReplicationError> {
        let metadata = SharedStoreWalOffsetMetadata {
            shard_id: entry.shard_id,
            wal_index: entry.wal_index,
            wal_blob_key: wal_blob_key.to_string(),
            wal_blob_start_offset: receipt.start_offset,
            wal_blob_end_offset: receipt.end_offset,
            wal_blob_bytes_written: receipt.bytes_written,
            wal_blob_object_length: receipt.object_length,
            wal_blob_physical_slab_count: receipt.physical_slab_count as u64,
            wal_blob_first_physical_offset: receipt.first_physical_offset,
            command_byte_size: command_metadata.byte_size,
            command_encoding: command_metadata.encoding,
        };
        let frame = encode_wal_offset_metadata_frame(&metadata, wal_blob_key);
        self.append_blob_with_retry(
            &self.wal_offset_index_blob_key(entry.shard_id),
            Bytes::from(frame),
        )
        .await?;
        Ok(())
    }

    /// Group-commit barrier: make an entire batch of WAL entries durable with the FEWEST possible
    /// object-store appends, returning a per-entry [`SharedStoreWriteReport`] aligned with `entries`.
    ///
    /// In `ProtobufAppendBlob` mode the shared WAL is a single per-shard appendable log of
    /// length-prefixed frames, so the whole batch is concatenated and made durable in ONE
    /// `append_blob` (plus ONE append of the concatenated offset-index frames) — `N` entries share
    /// exactly `2` appends instead of `2N`. Per-entry logical offsets are derived from the single
    /// batch receipt (`start_offset` + cumulative frame lengths); `FileObjectStore::append_blob`
    /// appends contiguously from the prior object length, so each frame is individually
    /// range-readable exactly as a one-at-a-time publish would have written it.
    ///
    /// Every entry must belong to the same shard (the caller — a per-shard storage writer —
    /// guarantees this). The fence is re-validated ONCE for the batch. In `JsonPerKey` mode each
    /// index is its own object, so no single-object barrier exists; the batch degrades to per-entry
    /// publishes (still correct, just no fsync coalescing — the win is inherent to append-blob mode).
    ///
    /// Durability: this returns `Ok` only after the covering append(s) have reached the store, so a
    /// caller that fans `Ok` out to waiters can never ack a write before its barrier completed.
    pub async fn publish_wal_entries_batch(
        &self,
        entries: &[SharedStoreWalEntry],
    ) -> Result<Vec<SharedStoreWriteReport>, SharedStoreReplicationError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let shard_id = entries[0].shard_id;
        let single_shard = entries.iter().all(|entry| entry.shard_id == shard_id);
        let append_blob_mode = matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        );
        if !append_blob_mode || !single_shard {
            // No single-object barrier available (JsonPerKey, or a mixed-shard batch): publish each
            // entry on its own durable barrier. Correct, just uncoalesced.
            let mut reports = Vec::with_capacity(entries.len());
            for entry in entries {
                let receipt = self.publish_wal_entry(entry.clone()).await?;
                reports.push(write_report_from_receipt(entry.wal_index, receipt.as_ref()));
            }
            return Ok(reports);
        }

        // R2 single-writer fence: re-validate ownership once before the coalesced append.
        self.enforce_fence(shard_id).await?;
        let wal_blob_key = self.wal_blob_key(shard_id);

        // Concatenate every entry's length-prefixed frame into one append payload, remembering each
        // frame's length (and command metadata) so per-entry offsets can be derived from the single
        // receipt below.
        let mut wal_payload: Vec<u8> = Vec::new();
        let mut frame_lengths: Vec<u64> = Vec::with_capacity(entries.len());
        let mut command_metadata: Vec<WalCommandMetadata> = Vec::with_capacity(entries.len());
        for entry in entries {
            let frame = encode_wal_proto_frame(entry, self.wal_frame_format)?;
            frame_lengths.push(frame.len() as u64);
            command_metadata.push(wal_command_metadata(entry.command.as_ref())?);
            wal_payload.extend_from_slice(&frame);
        }
        let receipt = self
            .append_blob_with_retry(&wal_blob_key, Bytes::from(wal_payload))
            .await?
            .expect("protobuf append blob must return a receipt");

        // Derive each entry's [start, end) from the batch receipt and build the concatenated
        // offset-index payload, then make it durable in a single append.
        let mut offset_payload: Vec<u8> = Vec::new();
        let mut reports = Vec::with_capacity(entries.len());
        let mut cursor = receipt.start_offset;
        for ((entry, frame_len), metadata) in entries
            .iter()
            .zip(frame_lengths.iter().copied())
            .zip(command_metadata.into_iter())
        {
            let start = cursor;
            let end = start.saturating_add(frame_len);
            cursor = end;
            let offset_metadata = SharedStoreWalOffsetMetadata {
                shard_id: entry.shard_id,
                wal_index: entry.wal_index,
                wal_blob_key: wal_blob_key.clone(),
                wal_blob_start_offset: start,
                wal_blob_end_offset: end,
                wal_blob_bytes_written: frame_len,
                wal_blob_object_length: receipt.object_length,
                wal_blob_physical_slab_count: receipt.physical_slab_count as u64,
                wal_blob_first_physical_offset: receipt.first_physical_offset,
                command_byte_size: metadata.byte_size,
                command_encoding: metadata.encoding,
            };
            offset_payload.extend_from_slice(&encode_wal_offset_metadata_frame(
                &offset_metadata,
                &wal_blob_key,
            ));
            reports.push(SharedStoreWriteReport {
                wal_index: entry.wal_index,
                published: true,
                queued: false,
                wal_blob_start_offset: Some(start),
                wal_blob_end_offset: Some(end),
                wal_blob_bytes_written: Some(frame_len),
                wal_blob_object_length: Some(receipt.object_length),
            });
        }
        self.append_blob_with_retry(
            &self.wal_offset_index_blob_key(shard_id),
            Bytes::from(offset_payload),
        )
        .await?;
        Ok(reports)
    }

    pub async fn lookup_wal_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        Ok(self
            .load_wal_offset_metadata(shard_id)
            .await?
            .remove(&wal_index))
    }

    pub async fn read_wal_entry_by_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalIndexedRead>, SharedStoreReplicationError> {
        let Some(metadata) = self
            .lookup_wal_offset_metadata(shard_id, wal_index)
            .await?
        else {
            return Ok(None);
        };
        let (entry, range_bytes_read) = self
            .read_wal_entry_with_offset_metadata(shard_id, wal_index, &metadata)
            .await?;
        Ok(Some(SharedStoreWalIndexedRead {
            metadata,
            entry,
            range_bytes_read,
        }))
    }

    /// Read one entry given offset metadata the caller ALREADY HOLDS.
    ///
    /// # WHY THIS EXISTS SEPARATELY FROM THE FUNCTION ABOVE
    ///
    /// The offset index is one appendable blob for the whole shard, so looking one entry
    /// up means reading and decoding all of it. [`Self::read_wal_entry_by_offset_metadata`]
    /// does exactly that and then discards every record but one — correct for a single
    /// lookup, and quadratic when a replay calls it once per entry, which is what replay
    /// used to do while already holding the decoded map.
    ///
    /// MEASURED on a five-node shared-store sweep (one fenced writer, four replayers, 256
    /// byte values): `load_wal_offset_metadata` was 42.50% of the program's instructions —
    /// 35.56% reading the blob back through the object store and 6.93% decoding it — for
    /// records the replay loop had in hand. Per-entry replay cost grew with the corpus for
    /// that reason and no other: bytes read per entry were already flat, because the
    /// *range* read is correctly indexed. It was the LOOKUP that walked the log.
    ///
    /// Every guard from the single-lookup path is here rather than there, so neither
    /// caller can be the one without them.
    async fn read_wal_entry_with_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
        metadata: &SharedStoreWalOffsetMetadata,
    ) -> Result<(SharedStoreWalEntry, u64), SharedStoreReplicationError> {
        let length = metadata
            .wal_blob_end_offset
            .checked_sub(metadata.wal_blob_start_offset)
            .ok_or_else(|| SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid WAL offset metadata range for shard {shard_id} index {wal_index}"),
            )))?;
        // MEASURED, AND NOT COVERED. Removing this guard, or either half of the
        // shard/index cross-check further down, leaves the entire lib gate passing:
        // nothing drives this function's rejection paths. That is not a quiet corner --
        // this is the WAL replay fast path (`replay_wal_from_offset_metadata`), and the
        // cross-check below is what stops replay applying an entry that belongs to
        // another shard.
        //
        // Why the hole is here: the test that most exercises the offset-index machinery,
        // `shared_store_protobuf_append_blob_replays_matrixobject_blob`, sits behind
        // `#[cfg(feature = "matrixobject")]`, which is off by default and therefore not
        // compiled into the default gate at all.
        //
        // Covering it means writing a doctored offset-index object with
        // `encode_wal_offset_metadata_frame` -- one whose recorded byte range points at a
        // DIFFERENT entry than its own `wal_index` names, and one whose
        // `wal_blob_bytes_written` disagrees with its own start/end range -- then asserting
        // each guard rejects. Each arm has to leave the other conditions false, or one arm
        // satisfies several guards at once and passes with any one of them deleted.
        if length != metadata.wal_blob_bytes_written {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "WAL offset metadata bytes mismatch for shard {shard_id} index {wal_index}"
                ),
            )));
        }
        let bytes = self
            .object_store
            .get_range(
                &metadata.wal_blob_key,
                metadata.wal_blob_start_offset,
                length,
            )
            .await?;
        let entry = decode_wal_proto_frame_exact(&bytes, &metadata.wal_blob_key)?;
        if entry.shard_id != shard_id || entry.wal_index != wal_index {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("WAL offset metadata points to shard {} index {}, expected shard {shard_id} index {wal_index}", entry.shard_id, entry.wal_index),
            )));
        }
        Ok((entry, length))
    }

    pub fn storage_writer(
        &self,
        mode: SharedStoreStorageMode,
        next_wal_index: u64,
    ) -> SharedStoreStorageWriter<O> {
        SharedStoreStorageWriter {
            replicator: self.clone(),
            mode,
            next_wal_index: AtomicU64::new(next_wal_index.max(1)),
            pending: Mutex::default(),
            max_pending: shared_store_max_pending(),
            capacity_hits: AtomicU64::new(0),
            group_commit: false,
            commit_delay: Duration::ZERO,
            commit: Mutex::default(),
        }
    }

    pub fn default_storage_writer(&self, next_wal_index: u64) -> SharedStoreStorageWriter<O> {
        self.storage_writer(SharedStoreStorageMode::default(), next_wal_index)
    }

    pub async fn publish_index(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<(), SharedStoreReplicationError> {
        // THE RANGE GOES FIRST. A restore reads the index and refuses without the range, so the
        // order that can be interrupted safely is range-then-index: a range with no index is
        // nothing, an index with no range is refused.
        let (start_routing_bucket, end_routing_bucket) = engine.shard_routing_range(shard_id);
        self.object_store
            .put(
                &self.index_routing_range_key(shard_id),
                Bytes::from(serde_json::to_vec(
                    &crate::engine::routing_range_stamp::RoutingRangeStamp {
                        start_routing_bucket,
                        end_routing_bucket,
                    },
                )?),
            )
            .await?;
        self.object_store
            .put(
                &self.index_key(shard_id),
                Bytes::from(engine.export_index_bytes(shard_id)?),
            )
            .await?;
        Ok(())
    }

    pub async fn publish_block_slabs(
        &self,
        shard_id: ShardId,
        block_store: &BlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let mut published = Vec::new();
        for block_slab_id in block_store.slab_ids()? {
            self.object_store
                .put(
                    &self.block_slab_key(shard_id, block_slab_id),
                    Bytes::from(block_store.read_slab(block_slab_id)?),
                )
                .await?;
            published.push(block_slab_id);
        }
        Ok(published)
    }

    pub async fn publish_checkpoint(
        &self,
        shard_id: ShardId,
        checkpoint_wal_index: u64,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        // R2 single-writer fence: a checkpoint publish is a durable-frontier advance, so a
        // superseded stale owner must be rejected here just as on a WAL append.
        self.enforce_fence(shard_id).await?;
        // Durability barrier: fsync any bulk-deferred page bytes and persist the slab manifest
        // BEFORE capturing the slab set, mirroring the local dump path
        // (bucket_dump_manifest_methods) which fsyncs pages+WAL before recording slab ids. Without
        // this a relaxed (bulk) writer could enumerate a slab whose tail bytes are not yet on disk,
        // uploading a torn page or racing a not-yet-durable slab into the checkpoint.
        // Make the index PORTABLE before exporting it. A page that lives in a WAL record or in
        // memory is named by a slab that is not a file, resolvable only through this process's
        // registry -- so an index carrying one names a place the restoring node cannot reach, and
        // that read returns nothing with no error. Materialising them first means the index names
        // only slabs this checkpoint actually uploads.
        let materialised = engine.materialize_synthetic_blocks(shard_id);
        if materialised > 0 {
            tracing::info!(
                shard_id,
                blocks = materialised,
                "materialised in-process-only pages so the checkpoint index can travel"
            );
        }
        block_store.sync_durable()?;
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let prefix = self.checkpoint_prefix(shard_id, &checkpoint_id);
        let index_key = format!("{prefix}index/shard.index.json");
        let index = engine.export_index_bytes(shard_id)?;
        self.object_store
            .put(&index_key, Bytes::from(index.clone()))
            .await?;

        // Snapshot the local slab descriptors so each uploaded slab carries its sealed-slab
        // metadata (logical bytes + page-id range) into the manifest for S3 restore-time install.
        let slab_by_slab: BTreeMap<u64, _> = block_store
            .slab_descriptors()
            .into_iter()
            .map(|slab| (slab.block_slab_id, slab))
            .collect();
        let mut block_slabs = Vec::new();
        let mut uploaded_slab_ids = std::collections::BTreeSet::new();
        for block_slab_id in block_store.slab_ids()? {
            let bytes = block_store.read_slab(block_slab_id)?;
            let key = format!(
                "{prefix}block_segments/{}",
                block_slab_object_name(block_slab_id)
            );
            self.object_store
                .put(&key, Bytes::from(bytes.clone()))
                .await?;
            uploaded_slab_ids.insert(block_slab_id);
            let slab = slab_by_slab.get(&block_slab_id);
            block_slabs.push(SharedStoreBlockSlab {
                block_slab_id,
                key,
                byte_size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
                logical_bytes: slab.map(|slab| slab.logical_bytes).unwrap_or(0),
                first_block_id: slab.and_then(|slab| slab.first_block_id),
                last_block_id: slab.and_then(|slab| slab.last_block_id),
                created_unix_ms: slab.and_then(|slab| slab.created_unix_ms),
                updated_unix_ms: slab.and_then(|slab| slab.updated_unix_ms),
            });
        }

        // Completeness barrier: every slab the served index actually references must be covered by
        // an uploaded slab before the manifest is written, so a restore never resolves a live page
        // to a slab that is absent from the checkpoint. The synthetic in-memory hot-page slab
        // (u64::MAX) is folded into the exported index, not a durable slab, so it is excluded.
        const HOT_BLOCK_SLAB_ID: u64 = u64::MAX;
        for referenced in engine.live_block_slab_ids(shard_id) {
            if referenced == HOT_BLOCK_SLAB_ID {
                continue;
            }
            if !uploaded_slab_ids.contains(&referenced) {
                return Err(SharedStoreReplicationError::CheckpointSlabNotDurable {
                    shard_id,
                    block_slab_id: referenced,
                });
            }
        }

        let (start_routing_bucket, end_routing_bucket) = engine.shard_routing_range(shard_id);
        let manifest = SharedStoreCheckpointManifest {
            cluster_id: self.cluster_id.clone(),
            shard_id,
            checkpoint_id,
            checkpoint_wal_index,
            created_at_ms: now_ms(),
            // The range this node serves the shard on, which is the range the checkpointed index
            // was built on: one function decides both where a write is filed and what this says.
            routing_range: Some(crate::engine::routing_range_stamp::RoutingRangeStamp {
                start_routing_bucket,
                end_routing_bucket,
            }),
            index_key,
            index_byte_size: index.len() as u64,
            index_sha256: sha256_hex(&index),
            block_slabs,
            next_block_id: block_store.next_block_id(),
        };
        self.object_store
            .put(
                &self.checkpoint_manifest_key(shard_id, &manifest.checkpoint_id),
                Bytes::from(serde_json::to_vec_pretty(&manifest)?),
            )
            .await?;
        Ok(manifest)
    }

    pub async fn restore_index_and_blocks(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let index = self.object_store.get(&self.index_key(shard_id)).await?;
        // Refused rather than guessed: see `install_index_bytes`.
        let range_bytes = self
            .object_store
            .get(&self.index_routing_range_key(shard_id))
            .await
            .map_err(|_| SharedStoreReplicationError::RoutingRangeMissing {
                shard_id,
                what: "no routing-range object beside the published index".to_string(),
            })?;
        let routing_range: crate::engine::routing_range_stamp::RoutingRangeStamp =
            serde_json::from_slice(&range_bytes).map_err(|err| {
                SharedStoreReplicationError::RoutingRangeMissing {
                    shard_id,
                    what: format!("the routing-range object did not decode: {err}"),
                }
            })?;
        engine.install_index_bytes(shard_id, &index, routing_range)?;

        let prefix = self.block_slab_prefix(shard_id);
        let mut restored = Vec::new();
        for key in self.object_store.list(&prefix).await? {
            let Some(block_slab_id) = parse_block_slab_id(&key) else {
                continue;
            };
            let bytes = self.object_store.get(&key).await?;
            block_store.install_slab(block_slab_id, &bytes)?;
            restored.push(block_slab_id);
        }
        restored.sort_unstable();
        Ok(restored)
    }

    pub async fn list_checkpoints(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SharedStoreCheckpointManifest>, SharedStoreReplicationError> {
        let mut manifests = Vec::new();
        for key in self
            .object_store
            .list(&self.checkpoints_prefix(shard_id))
            .await?
        {
            if !key.ends_with("/manifest.json") {
                continue;
            }
            manifests.push(serde_json::from_slice(&self.object_store.get(&key).await?)?);
        }
        manifests.sort_by_key(|manifest: &SharedStoreCheckpointManifest| {
            (manifest.checkpoint_wal_index, manifest.created_at_ms)
        });
        Ok(manifests)
    }

    /// Publish this shard's bucket-dump manifests to the object store.
    ///
    /// A dump manifest is the durable reclaim watermark plus the recovery index for the data it
    /// covers. In shared mode that data outlives the node -- it is in the object store -- but
    /// the manifest describing it was only ever written to the node's local index dir, so
    /// losing the node lost the lineage for data that was still perfectly present. A manifest
    /// should be as durable as the data it describes; this is the half that was missing.
    ///
    /// Deliberately not on the write path. Manifests are produced at dump cadence, so this is
    /// one put per manifest at checkpoint time and nothing at all in the steady state -- unlike
    /// the WAL, which is the per-write barrier and stays node-local for exactly that reason.
    pub async fn publish_bucket_dump_manifests(
        &self,
        shard_id: ShardId,
        manifests: &[crate::BucketDumpManifest],
    ) -> Result<usize, SharedStoreReplicationError> {
        let mut published = 0usize;
        for manifest in manifests {
            if manifest.shard_id != shard_id {
                continue;
            }
            self.object_store
                .put(
                    &self.bucket_dump_manifest_key(shard_id, &manifest.manifest_id),
                    Bytes::from(serde_json::to_vec_pretty(manifest)?),
                )
                .await?;
            published += 1;
        }
        Ok(published)
    }

    /// Land every published bucket-dump manifest for this shard in the local index dir.
    ///
    /// The counterpart to [`publish_bucket_dump_manifests`](Self::publish_bucket_dump_manifests):
    /// a node taking over a shard inherits the dump lineage along with the data, instead of
    /// starting blind and treating every live generation as never dumped -- which is what
    /// blocks WAL reclaim until this node has dumped everything again itself.
    ///
    /// A manifest already present locally is left alone. A local manifest is at least as
    /// current as a published one, and rewriting it would churn the very file that authorizes
    /// WAL reclaim.
    pub async fn restore_bucket_dump_manifests(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<usize, SharedStoreReplicationError> {
        let existing: std::collections::BTreeSet<String> = engine
            .list_bucket_dump_manifests(shard_id)
            .into_iter()
            .map(|manifest| manifest.manifest_id)
            .collect();
        let mut restored = 0usize;
        for key in self
            .object_store
            .list(&self.bucket_dump_prefix(shard_id))
            .await?
        {
            if !key.ends_with(".json") {
                continue;
            }
            let manifest: crate::BucketDumpManifest =
                serde_json::from_slice(&self.object_store.get(&key).await?)?;
            // A manifest filed under another shard's prefix, or one this node already has,
            // is not ours to write.
            if manifest.shard_id != shard_id || existing.contains(&manifest.manifest_id) {
                continue;
            }
            engine.store_bucket_dump_manifest(&manifest)?;
            restored += 1;
        }
        Ok(restored)
    }

    pub async fn restore_checkpoint(
        &self,
        manifest: &SharedStoreCheckpointManifest,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<(), SharedStoreReplicationError> {
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        let routing_range = manifest.routing_range.ok_or_else(|| {
            SharedStoreReplicationError::RoutingRangeMissing {
                shard_id: manifest.shard_id,
                what: format!("checkpoint manifest {} records none", manifest.checkpoint_id),
            }
        })?;
        engine.install_index_bytes(manifest.shard_id, &index, routing_range)?;

        for slab in &manifest.block_slabs {
            let bytes = self.object_store.get(&slab.key).await?;
            verify_checksum(&slab.key, &bytes, slab.byte_size, &slab.sha256)?;
            block_store.install_slab(slab.block_slab_id, &bytes)?;
        }
        Ok(())
    }

    pub async fn restore_latest_checkpoint(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        self.restore_checkpoint(&manifest, engine, block_store)
            .await?;
        Ok(manifest)
    }

    /// Replay the shared WAL tail above `after_wal_index`, REFUSING a hole in it.
    ///
    /// `after_wal_index` is a position something else vouches for: a checkpoint's
    /// `checkpoint_wal_index`, or 0 for a log replayed from its start. Nothing covers what sits
    /// above it, so every index from `after_wal_index + 1` upwards must be present and
    /// contiguous. A missing one is a span of history this node cannot reconstruct, and the only
    /// honest answer is [`SharedStoreReplicationError::ReplayGap`] naming the position it wanted.
    ///
    /// THE CHECK IS IN THE DEFAULT, AND THAT IS THE POINT. It used to exist only in
    /// [`Self::replay_wal_strict`], while this -- the function all three production restore paths
    /// call -- had none at all. A collected prefix no checkpoint covered was therefore restored as
    /// a truncated history and REPORTED SUCCESS, with every surface saying fine. Putting the check
    /// behind a second name would leave the next caller to know to reach for it; there is now no
    /// unchecked loop in either layout for one to reach.
    pub async fn replay_wal(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            if let Some(report) = self
                .replay_wal_from_offset_metadata(shard_id, after_wal_index, engine)
                .await?
            {
                return Ok(report);
            }
        }
        let wal_entries = self.load_wal_entries(shard_id).await?;

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, entry) in wal_entries {
            if wal_index <= after_wal_index {
                continue;
            }
            if wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: wal_index,
                });
            }
            // Both command and outcome records advance the same sequence.
            expected = wal_index + 1;
            Self::apply_replayed_wal_entry(shard_id, wal_index, entry, engine)?;
            report.applied += 1;
            report.last_wal_index = wal_index;
        }
        Ok(report)
    }

    /// The older name for [`Self::replay_wal`], which now refuses a hole by itself.
    ///
    /// It delegates rather than keeping a second copy of the loop. The two copies DID diverge --
    /// one grew the hole check and the other, the one production reached, did not -- and that
    /// divergence is the whole of the finding above. One body cannot drift from itself.
    pub async fn replay_wal_strict(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        self.replay_wal(shard_id, after_wal_index, engine).await
    }

    fn apply_replayed_wal_entry(
        shard_id: ShardId,
        wal_index: u64,
        entry: SharedStoreWalEntry,
        engine: &TemporalEngine,
    ) -> Result<(), SharedStoreReplicationError> {
        if !entry.outcomes.is_empty() {
            if !engine.install_shared_outcomes_with_blocks(shard_id, &entry.outcomes, &entry.staged_blocks) {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: Status::error("shared_wal_outcome_refused", "a recorded outcome could not be installed"),
                });
            }
            return Ok(());
        }
        let Some(command) = entry.command else {
            return Err(SharedStoreReplicationError::ApplyFailed {
                wal_index,
                status: Status::error("shared_wal_entry_empty", "entry carries neither results nor an operation"),
            });
        };
        let response = engine.execute_with_carried_blocks(
            ExecuteRequest { shard_id, command }, entry.staged_blocks,
        );
        if !response.status.ok {
            return Err(SharedStoreReplicationError::ApplyFailed { wal_index, status: response.status });
        }
        Ok(())
    }

    async fn replay_wal_from_offset_metadata(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<Option<ReplayReport>, SharedStoreReplicationError> {
        let offset_metadata = self.load_wal_offset_metadata(shard_id).await?;
        if offset_metadata.is_empty() {
            return Ok(None);
        }

        // A compatibility writer can leave records before, between, or after indexed
        // records. Select the union reader before applying anything from the fast path.
        if self.object_store.list(&self.wal_prefix(shard_id)).await?
            .iter().filter_map(|key| parse_wal_index(key))
            .any(|index| index > after_wal_index)
        {
            return Ok(None);
        }

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        // ---- THE INDEX IS READ ONCE FOR THE WHOLE REPLAY, not once per entry.
        //
        // The loop binding used to discard the metadata (`for (wal_index, _)`) and then
        // ask `read_wal_entry_by_offset_metadata` for it, which re-read and re-decoded the
        // ENTIRE offset-index blob to answer each one. That made replay quadratic in the
        // log length while every per-entry BYTE figure stayed flat, because the bytes were
        // being counted on the range read -- which was always correctly indexed -- and not
        // on the lookup. Measured at 42.50% of the program's instructions before this, and
        // asserted below by `offset_index_reads`, which now counts the entries it served
        // from one load rather than the loads it performed.
        for (wal_index, metadata) in offset_metadata.range((after_wal_index + 1)..) {
            if *wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: *wal_index,
                });
            }
            let (entry, range_bytes_read) = self
                .read_wal_entry_with_offset_metadata(shard_id, *wal_index, metadata)
                .await?;
            Self::apply_replayed_wal_entry(shard_id, *wal_index, entry, engine)?;
            report.applied += 1;
            report.last_wal_index = *wal_index;
            report.offset_index_reads += 1;
            report.range_bytes_read += range_bytes_read;
            expected += 1;
        }
        Ok(Some(report))
    }

    /// The persisted consumer cursor for `shard_id`, or `None` when no consumer has ever written
    /// one.
    ///
    /// [`Self::load_replay_cursor`] answers a zero-valued cursor both for an ABSENT cursor object
    /// and for a consumer that has read NOTHING, and those two are opposites: the first is a shard
    /// with no reader to protect, the second is the reader with the most left to read. Every
    /// retention decision that asked `last_wal_index > 0` was really asking whether a consumer
    /// exists, got the wrong answer for the consumer stuck at zero, and collected the prefix that
    /// consumer had still to read. Presence is a different question from position, so it gets its
    /// own answer.
    pub async fn try_load_replay_cursor(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<SharedStoreReplayCursor>, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.replay_cursor_key(shard_id))
            .await
        {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(ObjectStoreError::NotFound(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn load_replay_cursor(
        &self,
        shard_id: ShardId,
    ) -> Result<SharedStoreReplayCursor, SharedStoreReplicationError> {
        Ok(self
            .try_load_replay_cursor(shard_id)
            .await?
            .unwrap_or(SharedStoreReplayCursor {
                shard_id,
                last_wal_index: 0,
                last_replay_time_ms: 0,
            }))
    }

    pub async fn save_replay_cursor(
        &self,
        cursor: &SharedStoreReplayCursor,
    ) -> Result<(), SharedStoreReplicationError> {
        self.object_store
            .put(
                &self.replay_cursor_key(cursor.shard_id),
                Bytes::from(serde_json::to_vec_pretty(cursor)?),
            )
            .await?;
        Ok(())
    }

    pub async fn replay_wal_strict_with_cursor(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut cursor = self.load_replay_cursor(shard_id).await?;
        let report = self
            .replay_wal_strict(shard_id, cursor.last_wal_index, engine)
            .await?;
        if report.last_wal_index > cursor.last_wal_index {
            cursor.last_wal_index = report.last_wal_index;
            cursor.last_replay_time_ms = now_ms();
            self.save_replay_cursor(&cursor).await?;
        }
        Ok(report)
    }

    pub async fn gc_wal_before(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let mut deleted_wal_objects = 0usize;
        for key in self.object_store.list(&self.wal_prefix(shard_id)).await? {
            let Some(wal_index) = parse_wal_index(&key) else {
                continue;
            };
            if wal_index < retain_from_wal_index {
                self.object_store.delete(&key).await?;
                deleted_wal_objects += 1;
            }
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_wal_objects,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_wal_before_cursor_safe(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        // PRESENCE, not position. A consumer that has read nothing still needs entry 1, so the
        // only retention point it can tolerate is 1 -- and it is the consumer with the most left
        // to lose. Reading its existence off `last_wal_index > 0` made it indistinguishable from
        // a shard with no consumer at all, which is the one case where collecting the whole
        // prefix is allowed, so the guard waved through exactly the deletion it existed to stop.
        let cursor = self.try_load_replay_cursor(shard_id).await?;
        if let Some(cursor) = &cursor {
            if retain_from_wal_index > cursor.last_wal_index.saturating_add(1) {
                return Err(SharedStoreReplicationError::GcBlockedByReplayCursor {
                    cursor_wal_index: cursor.last_wal_index,
                    retain_from_wal_index,
                });
            }
        }
        let mut report = self.gc_wal_before(shard_id, retain_from_wal_index).await?;
        report.retained_for_cursor_wal_index = cursor.map(|cursor| cursor.last_wal_index);
        Ok(report)
    }

    pub async fn gc_checkpoints(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        let delete_count = manifests.len().saturating_sub(keep_last);
        let retained_checkpoint_ids = manifests[delete_count..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoint_objects = 0usize;
        for manifest in manifests.iter().take(delete_count) {
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints: delete_count,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_checkpoints_cursor_safe(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        // Same presence question as the log collector. The retention SET does not move for a
        // consumer stuck at zero -- no generation sits at or below position 0, and `keep_last`
        // already holds a complete state -- but the report did, calling a present consumer absent.
        let cursor_wal_index = self
            .try_load_replay_cursor(shard_id)
            .await?
            .map(|cursor| cursor.last_wal_index);
        let retain_start = manifests.len().saturating_sub(keep_last);
        let cursor_anchor = if let Some(position) = cursor_wal_index {
            manifests
                .iter()
                .enumerate()
                .rev()
                .find(|(_, manifest)| manifest.checkpoint_wal_index <= position)
                .map(|(index, _)| index)
        } else {
            None
        };
        let mut retained_checkpoint_ids = manifests[retain_start..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoints = 0usize;
        let mut deleted_checkpoint_objects = 0usize;
        for (index, manifest) in manifests.iter().enumerate() {
            let retained_by_keep_last = index >= retain_start;
            let retained_by_cursor = cursor_anchor == Some(index);
            if retained_by_keep_last || retained_by_cursor {
                if retained_by_cursor
                    && !retained_checkpoint_ids
                        .iter()
                        .any(|id| id == &manifest.checkpoint_id)
                {
                    retained_checkpoint_ids.push(manifest.checkpoint_id.clone());
                }
                continue;
            }
            if cursor_wal_index.is_some_and(|position| {
                manifest.checkpoint_wal_index <= position && cursor_anchor.is_none()
            }) {
                return Err(
                    SharedStoreReplicationError::CheckpointGcBlockedByReplayCursor {
                        cursor_wal_index: cursor_wal_index.unwrap_or_default(),
                        checkpoint_wal_index: manifest.checkpoint_wal_index,
                        checkpoint_id: manifest.checkpoint_id.clone(),
                    },
                );
            }
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
            deleted_checkpoints += 1;
        }
        retained_checkpoint_ids.sort();
        retained_checkpoint_ids.dedup();
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            retained_for_cursor_wal_index: cursor_wal_index,
            retained_for_cursor_checkpoint_id: cursor_anchor
                .map(|index| manifests[index].checkpoint_id.clone()),
            ..SharedStoreGcReport::default()
        })
    }

    /// Decide whether a consumer cursor has held the retention point past its bound, and drop
    /// it if it has.
    ///
    /// CLAMPING TO A CONSUMER IS CORRECT AND IT IS ALSO UNBOUNDED. A consumer that never advances
    /// holds the retention point at its own position for as long as its cursor exists, and the
    /// log grows underneath it -- a disk-exhaustion failure in the shape of a retention policy.
    /// The only honest end to it is to stop treating that consumer as live.
    ///
    /// Abandoning it means DELETING the cursor, and that costs the consumer a resume rather than
    /// its data: the newest generation is a complete state, and restoring from it is the path a
    /// consumer already takes when it finds no cursor at all.
    ///
    /// Two rules, and the age one has a precondition. `last_replay_time_ms` is stamped only when
    /// a consumer ADVANCES, so a consumer that has never advanced can hold a cursor whose
    /// recorded time is zero -- which would read as infinitely old and be dropped on the first
    /// pass, before it had any chance to read anything. The age rule therefore declines to judge
    /// a cursor that recorded no time, and the distance rule is what ends that consumer's hold
    /// once the log moves past it. Before this, `last_replay_time_ms` was written by the resume
    /// path and read by nothing: the cursor recorded how long it had been stuck and no code asked.
    pub async fn abandon_stalled_replay_cursor(
        &self,
        shard_id: ShardId,
        now_ms: u64,
    ) -> Result<Option<SharedStoreAbandonedCursor>, SharedStoreReplicationError> {
        let Some(policy) = self.cursor_abandon else {
            return Ok(None);
        };
        let Some(cursor) = self.try_load_replay_cursor(shard_id).await? else {
            return Ok(None);
        };
        let frontier = self.latest_persisted_wal_index(shard_id).await?;
        let cursor_lag_entries = frontier.saturating_sub(cursor.last_wal_index);
        let cursor_age_ms = if cursor.last_replay_time_ms == 0 {
            0
        } else {
            now_ms.saturating_sub(cursor.last_replay_time_ms)
        };
        let reason = if policy.max_cursor_age_ms > 0
            && cursor.last_replay_time_ms > 0
            && cursor_age_ms > policy.max_cursor_age_ms
        {
            "cursor_age_above_bound"
        } else if policy.max_cursor_lag_entries > 0
            && cursor_lag_entries > policy.max_cursor_lag_entries
        {
            "cursor_lag_entries_above_bound"
        } else {
            return Ok(None);
        };
        self.object_store
            .delete(&self.replay_cursor_key(shard_id))
            .await?;
        Ok(Some(SharedStoreAbandonedCursor {
            shard_id,
            cursor_wal_index: cursor.last_wal_index,
            cursor_age_ms,
            cursor_lag_entries,
            reason: reason.to_string(),
        }))
    }

    /// Collect this shard's shared objects down to the position the newest checkpoint covers.
    ///
    /// A CHECKPOINT IS WHAT MAKES THE LOG FINITE. A checkpoint is a complete state AT a log
    /// position, so everything at or below that position is recoverable without the log and the
    /// whole prefix below it can go. The log is then bounded by CHECKPOINT CADENCE rather than by
    /// a retention span someone has to keep tuning, and that bound is the only one that holds
    /// when no consumer is present -- which is the ordinary case here, because no write path
    /// writes a consumer cursor.
    ///
    /// With no checkpoint this collects NOTHING and names that in `blocked_reason`. That is not a
    /// setting that could be loosened: with no complete state behind it there is no prefix that
    /// is safe to drop, so a shard that never checkpoints has an unbounded shared log by
    /// construction and no consumer policy can bound it.
    ///
    /// A consumer cursor is a SECOND and independent constraint, and it is applied by CLAMPING,
    /// not by refusing. [`Self::gc_wal_before_cursor_safe`] refuses, which is the right answer to
    /// a caller that hands it a retention point past a consumer -- that is a caller that got its
    /// arithmetic wrong and should hear about it. It is the wrong answer as a retention policy,
    /// because a refusal reclaims nothing at all while the log keeps growing. Clamping reclaims
    /// exactly the span every consumer has already passed. The local reclaim plan in
    /// `engine::storage_lifecycle_methods` settled on clamping for that recorded reason, and this
    /// is the same policy rather than a second one.
    pub async fn collect_to_checkpoint(
        &self,
        shard_id: ShardId,
        keep_last_checkpoints: usize,
    ) -> Result<SharedStoreCollectionReport, SharedStoreReplicationError> {
        // The abandon decision comes FIRST, including on the path that collects nothing: a
        // cursor that has held the log past its bound is not made live again by this shard
        // having no checkpoint to collect against.
        let abandoned = self
            .abandon_stalled_replay_cursor(shard_id, now_ms())
            .await?;
        let abandoned_cursor_wal_index = abandoned
            .as_ref()
            .map(|abandoned| abandoned.cursor_wal_index);
        let abandoned_reason = abandoned
            .as_ref()
            .map(|abandoned| abandoned.reason.clone())
            .unwrap_or_default();
        let manifests = self.list_checkpoints(shard_id).await?;
        let Some(newest) = manifests.last() else {
            return Ok(SharedStoreCollectionReport {
                shard_id,
                blocked_reason: "no_checkpoint_to_anchor_collection".to_string(),
                abandoned_cursor_wal_index,
                abandoned_reason,
                ..SharedStoreCollectionReport::default()
            });
        };
        let checkpoint_wal_index = newest.checkpoint_wal_index;
        let mut retain_from_wal_index = checkpoint_wal_index.saturating_add(1);
        let cursor_wal_index = self
            .try_load_replay_cursor(shard_id)
            .await?
            .map(|cursor| cursor.last_wal_index);
        let mut clamped_to_cursor = false;
        if let Some(position) = cursor_wal_index {
            let consumer_floor = position.saturating_add(1);
            if consumer_floor < retain_from_wal_index {
                retain_from_wal_index = consumer_floor;
                clamped_to_cursor = true;
            }
        }
        // Log first, then generations. The anchor is the NEWEST generation and `keep_last` is at
        // least one, so the generation this retention point was derived from survives the second
        // call -- doing it the other way round would reason from a position and then collect the
        // state that justified it.
        let wal = self
            .gc_wal_before_cursor_safe(shard_id, retain_from_wal_index)
            .await?;
        let checkpoints = self
            .gc_checkpoints_cursor_safe(shard_id, keep_last_checkpoints)
            .await?;
        Ok(SharedStoreCollectionReport {
            shard_id,
            checkpoint_wal_index,
            retain_from_wal_index,
            cursor_wal_index,
            clamped_to_cursor,
            deleted_wal_objects: wal.deleted_wal_objects,
            deleted_checkpoints: checkpoints.deleted_checkpoints,
            deleted_checkpoint_objects: checkpoints.deleted_checkpoint_objects,
            blocked_reason: String::new(),
            abandoned_cursor_wal_index,
            abandoned_reason,
        })
    }

    fn shard_prefix(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}/shared/", self.cluster_id, shard_id)
    }

    /// Where the published index's routing range lives: beside the index, under the same shard
    /// prefix, so a prefix copied wholesale carries it.
    fn index_routing_range_key(&self, shard_id: ShardId) -> String {
        format!("{}index/shard.routing-range.json", self.shard_prefix(shard_id))
    }

    fn index_key(&self, shard_id: ShardId) -> String {
        format!("{}index/shard.index.json", self.shard_prefix(shard_id))
    }

    fn block_slab_prefix(&self, shard_id: ShardId) -> String {
        format!("{}block_segments/", self.shard_prefix(shard_id))
    }

    fn block_slab_key(&self, shard_id: ShardId, block_slab_id: u64) -> String {
        format!(
            "{}{}",
            self.block_slab_prefix(shard_id),
            block_slab_object_name(block_slab_id)
        )
    }

    fn wal_prefix(&self, shard_id: ShardId) -> String {
        format!("{}wal/", self.shard_prefix(shard_id))
    }

    fn wal_key(&self, shard_id: ShardId, wal_index: u64) -> String {
        format!(
            "{}wal_{wal_index:020}.json",
            self.wal_prefix(shard_id)
        )
    }

    fn wal_blob_key(&self, shard_id: ShardId) -> String {
        format!("{}wal.protobuf.blob", self.wal_prefix(shard_id))
    }

    fn wal_offset_index_blob_key(&self, shard_id: ShardId) -> String {
        format!(
            "{}wal.offset_index.protobuf.blob",
            self.wal_prefix(shard_id)
        )
    }

    pub async fn load_wal_offset_metadata(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.wal_offset_index_blob_key(shard_id))
            .await
        {
            Ok(bytes) => {
                decode_wal_offset_metadata_blob(&bytes, &self.wal_blob_key(shard_id))
            }
            Err(ObjectStoreError::NotFound(_)) => Ok(BTreeMap::new()),
            Err(err) => Err(err.into()),
        }
    }

    fn replay_cursor_key(&self, shard_id: ShardId) -> String {
        format!("{}replay_cursor.json", self.shard_prefix(shard_id))
    }

    fn checkpoints_prefix(&self, shard_id: ShardId) -> String {
        format!("{}checkpoints/", self.shard_prefix(shard_id))
    }

    fn bucket_dump_prefix(&self, shard_id: ShardId) -> String {
        format!("{}slot_dumps/", self.shard_prefix(shard_id))
    }

    fn bucket_dump_manifest_key(&self, shard_id: ShardId, manifest_id: &str) -> String {
        format!("{}{manifest_id}.json", self.bucket_dump_prefix(shard_id))
    }

    fn checkpoint_prefix(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!("{}{checkpoint_id}/", self.checkpoints_prefix(shard_id))
    }

    fn checkpoint_manifest_key(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!(
            "{}manifest.json",
            self.checkpoint_prefix(shard_id, checkpoint_id)
        )
    }

    async fn read_wal_entry(
        &self,
        key: &str,
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        let bytes = self.object_store.get(key).await?;
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(&bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn parse_wal_entry_object(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    /// The highest WAL index currently persisted in shared storage for
    /// `shard_id`, or `0` when no WAL exists yet. A restarting node uses this to
    /// resume publishing WAL entries at `latest + 1` without clobbering
    /// already-persisted entries. Works for both WAL append encodings (the
    /// protobuf offset index and the per-key JSON objects).
    pub async fn latest_persisted_wal_index(
        &self,
        shard_id: ShardId,
    ) -> Result<u64, SharedStoreReplicationError> {
        let offset_metadata = self.load_wal_offset_metadata(shard_id).await?;
        if let Some(max) = offset_metadata.keys().max().copied() {
            let legacy_max = self.object_store.list(&self.wal_prefix(shard_id)).await?
                .iter().filter_map(|key| parse_wal_index(key)).max().unwrap_or(0);
            return Ok(max.max(legacy_max));
        }
        let entries = self.load_wal_entries(shard_id).await?;
        Ok(entries.keys().max().copied().unwrap_or(0))
    }

    async fn load_wal_entries(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalEntry>, SharedStoreReplicationError> {
        let mut entries = BTreeMap::new();
        let mut keys = self.object_store.list(&self.wal_prefix(shard_id)).await?;
        keys.sort();
        let indexed_keys = keys
            .into_iter()
            .filter_map(|key| parse_wal_index(&key).map(|wal_index| (wal_index, key)))
            .collect::<Vec<_>>();
        if !indexed_keys.is_empty() {
            let object_keys = indexed_keys
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let objects = self.object_store.get_many(&object_keys).await?;
            if objects.len() == indexed_keys.len()
                && objects
                    .iter()
                    .zip(&indexed_keys)
                    .all(|((actual, _), (_, expected))| actual == expected)
            {
                for ((_, bytes), (wal_index, key)) in objects.iter().zip(&indexed_keys) {
                    entries.insert(*wal_index, self.parse_wal_entry_object(key, bytes)?);
                }
            } else {
                for (wal_index, key) in &indexed_keys {
                    let entry = self.read_wal_entry(key).await?;
                    entries.insert(*wal_index, entry);
                }
            }
        }

        match self.object_store.get(&self.wal_blob_key(shard_id)).await {
            Ok(bytes) => {
                for entry in decode_wal_proto_blob(&bytes)? {
                    entries.insert(entry.wal_index, entry);
                }
            }
            Err(ObjectStoreError::NotFound(_)) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(entries)
    }

    async fn put_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.put(key, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.retry_policy.backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.retry_policy.backoff_ms))
                            .await;
                    }
                }
            }
        }
        Err(last_error
            .expect("retry loop must record failed object-store error")
            .into())
    }

    async fn append_blob_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.append_blob(key, bytes.clone()).await {
                Ok(receipt) => return Ok(Some(receipt)),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.retry_policy.backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.retry_policy.backoff_ms))
                            .await;
                    }
                }
            }
        }
        Err(last_error
            .expect("retry loop must record failed object-store error")
            .into())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize, SharedStoreReplicationError> {
        let keys = self.object_store.list(prefix).await?;
        let deleted = keys.len();
        for key in keys {
            self.object_store.delete(&key).await?;
        }
        Ok(deleted)
    }
}

/// One slab's location + integrity in a shared-storage checkpoint: the object key to
/// read it from and the checksum to verify it against before caching it locally.
#[derive(Debug, Clone)]
struct SharedSlabAddress {
    key: String,
    byte_size: u64,
    sha256: String,
}

/// Lazy read-through source backing conformance recovery on the shared-filesystem
/// (`FileObjectStore`) backend. Holds the checkpoint's slab address map (slab id ->
/// shared object key) with no slab bytes, and resolves each slab on demand to a
/// synchronous filesystem read of the shared store, verifying the checkpoint
/// checksum before returning the bytes. The block store installs (caches) the
/// returned bytes locally, so each shared slab is fetched at most once and only when
/// a read actually needs it — never eagerly at recovery time.
#[derive(Debug)]
pub struct SharedPathSlabSource {
    object_store: Arc<FileObjectStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

impl SharedPathSlabSource {
    fn new(object_store: Arc<FileObjectStore>, slabs: BTreeMap<u64, SharedSlabAddress>) -> Self {
        Self {
            object_store,
            slabs,
        }
    }

    /// Number of slabs this source can serve lazily (i.e. the checkpoint's slab
    /// count). Useful for asserting that recovery installed an address map, not bytes.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }
}

impl SharedSlabSource for SharedPathSlabSource {
    fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&block_slab_id) else {
            return Ok(None);
        };
        let path = self.object_store.object_path(&address.key).map_err(|err| {
            BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                err.to_string(),
            ))
        })?;
        let bytes = std::fs::read(&path)?;
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                block_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

/// Lazy read-through source backing conformance recovery on the *networked*
/// matrixobject (`MatrixObjectHttpStore`) backend. Same contract as
/// [`SharedPathSlabSource`], but resolves each slab to a synchronous networked
/// GET ([`MatrixObjectHttpStore::get_blocking`]) of the shared object instead of a
/// local filesystem read, verifying the checkpoint checksum before the block store
/// caches it. Old (pre-checkpoint) pages therefore *follow the shard across
/// nodes*: fetched on demand, over the network, at most once each, only when a
/// read actually needs them — never eagerly at recovery time.
#[derive(Debug)]
pub struct MatrixObjectSlabSource {
    object_store: Arc<MatrixObjectHttpStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

impl MatrixObjectSlabSource {
    fn new(object_store: Arc<MatrixObjectHttpStore>, slabs: BTreeMap<u64, SharedSlabAddress>) -> Self {
        Self {
            object_store,
            slabs,
        }
    }

    /// Number of slabs this source can serve lazily (the checkpoint's slab count).
    /// Useful for asserting that recovery installed an address map, not bytes.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }
}

impl SharedSlabSource for MatrixObjectSlabSource {
    fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&block_slab_id) else {
            return Ok(None);
        };
        let bytes = self
            .object_store
            .get_blocking(&address.key)
            .map_err(|err| {
                BlockStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    err.to_string(),
                ))
            })?
            .to_vec();
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                block_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

impl<O> SharedStoreReplicator<O>
where
    O: ObjectStore + 'static,
{
    /// Shared body of the conformance LAZY restore, parameterized over the
    /// slab-source constructor so every shared-storage backend reuses the identical
    /// index-install + address-map + lazy-range-reserve logic; only the per-slab
    /// fetch transport differs (a local filesystem read vs a networked GET). Install
    /// the served INDEX and a per-slab shared address map WITHOUT downloading any
    /// slab bytes; `make_source` builds the backend-specific lazy read-through the
    /// block store consults on the first read that misses a slab. The caller replays
    /// only the WAL tail after `manifest.checkpoint_wal_index`, so recovery cost is
    /// O(index + recent WAL), not O(full history + all slabs). Returns
    /// `CheckpointNotFound` when no checkpoint exists so the caller can fall back to
    /// a full WAL replay.
    async fn restore_index_and_block_addresses_with<F>(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
        make_source: F,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError>
    where
        F: FnOnce(Arc<O>, BTreeMap<u64, SharedSlabAddress>) -> Arc<dyn SharedSlabSource>,
    {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        let routing_range = manifest.routing_range.ok_or_else(|| {
            SharedStoreReplicationError::RoutingRangeMissing {
                shard_id: manifest.shard_id,
                what: format!("checkpoint manifest {} records none", manifest.checkpoint_id),
            }
        })?;
        engine.install_index_bytes(manifest.shard_id, &index, routing_range)?;

        let mut slabs = BTreeMap::new();
        let mut max_slab_id = 0u64;
        for slab in &manifest.block_slabs {
            max_slab_id = max_slab_id.max(slab.block_slab_id);
            slabs.insert(
                slab.block_slab_id,
                SharedSlabAddress {
                    key: slab.key.clone(),
                    byte_size: slab.byte_size,
                    sha256: slab.sha256.clone(),
                },
            );
        }
        let source = make_source(Arc::clone(&self.object_store), slabs);
        block_store.attach_shared_slab_source(source);
        // Roll local appends past the checkpoint's slab/page-id range so replayed WAL-tail
        // and new writes never overwrite a slab still served lazily from shared storage.
        if !manifest.block_slabs.is_empty() {
            block_store.reserve_lazy_checkpoint_range(max_slab_id, manifest.next_block_id)?;
            // S3: install SEALED slab descriptors for the lazily-backed checkpoint slabs so
            // GC/compaction accounting is complete immediately after restore, before the first
            // on-demand fetch materializes any slab locally. Runs AFTER the reserve so the freshly
            // reserved slab stays the active slab and every checkpoint slab is sealed.
            let lazy_slabs: Vec<LazyCheckpointSlab> = manifest
                .block_slabs
                .iter()
                .map(|slab| LazyCheckpointSlab {
                    block_slab_id: slab.block_slab_id,
                    physical_bytes: slab.byte_size,
                    logical_bytes: slab.logical_bytes,
                    first_block_id: slab.first_block_id,
                    last_block_id: slab.last_block_id,
                    created_unix_ms: slab.created_unix_ms,
                    updated_unix_ms: slab.updated_unix_ms,
                })
                .collect();
            block_store.install_lazy_checkpoint_slabs(&lazy_slabs)?;
        }
        Ok(manifest)
    }
}

impl SharedStoreReplicator<FileObjectStore> {
    /// Conformance LAZY restore for the shared-filesystem backend: install the served
    /// INDEX and a per-slab shared address map WITHOUT downloading any slab bytes.
    /// Old (pre-checkpoint) pages are then read lazily through [`SharedPathSlabSource`]
    /// on the first read that needs them. See
    /// [`restore_index_and_block_addresses_with`](Self::restore_index_and_block_addresses_with)
    /// for the shared logic.
    pub async fn restore_index_and_block_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_block_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(SharedPathSlabSource::new(store, slabs)) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

impl SharedStoreReplicator<MatrixObjectHttpStore> {
    /// Conformance LAZY restore for the *networked* matrixobject backend: install
    /// the served INDEX and a per-slab shared address map WITHOUT downloading any slab
    /// bytes. Old (pre-checkpoint) pages are then fetched lazily over the network
    /// through [`MatrixObjectSlabSource`] on the first read that needs them, so shard
    /// data follows the shard across nodes without an eager full download. See
    /// [`restore_index_and_block_addresses_with`](Self::restore_index_and_block_addresses_with)
    /// for the shared logic.
    pub async fn restore_index_and_block_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_block_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(MatrixObjectSlabSource::new(store, slabs)) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

/// Lazy read-through source for the NODE-LOCAL matrixobject backend
/// ([`crate::matrixobject_store::MatrixObjectObjectStore`]): the same contract as
/// [`MatrixObjectSlabSource`], resolved against the in-process store under its mutex
/// instead of a networked GET, verifying the checkpoint checksum before the block
/// store caches it.
#[cfg(feature = "matrixobject")]
#[derive(Debug)]
pub struct MatrixObjectLocalSlabSource {
    object_store: Arc<crate::matrixobject_store::MatrixObjectObjectStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

#[cfg(feature = "matrixobject")]
impl SharedSlabSource for MatrixObjectLocalSlabSource {
    fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&block_slab_id) else {
            return Ok(None);
        };
        let bytes = self
            .object_store
            .get_blocking(&address.key)
            .map_err(|err| {
                BlockStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    err.to_string(),
                ))
            })?
            .to_vec();
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                block_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

#[cfg(feature = "matrixobject")]
impl SharedStoreReplicator<crate::matrixobject_store::MatrixObjectObjectStore> {
    /// Conformance LAZY restore for the node-local matrixobject backend: install the
    /// served INDEX and a per-slab shared address map WITHOUT installing any slab
    /// bytes; old (pre-checkpoint) pages are read lazily out of the local store on the
    /// first read that needs them. The same flow the shared-filesystem and networked
    /// backends already run -- it is what lets this backend's recovery replay only the
    /// WAL tail after a checkpoint instead of all history.
    pub async fn restore_index_and_block_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &BlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_block_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(MatrixObjectLocalSlabSource {
                object_store: store,
                slabs,
            }) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

/// Build a per-entry [`SharedStoreWriteReport`] from an optional append receipt (JsonPerKey mode
/// has no receipt; append-blob mode carries the byte range).
fn write_report_from_receipt(
    wal_index: u64,
    receipt: Option<&AppendBlobReceipt>,
) -> SharedStoreWriteReport {
    SharedStoreWriteReport {
        wal_index,
        published: true,
        queued: false,
        wal_blob_start_offset: receipt.map(|receipt| receipt.start_offset),
        wal_blob_end_offset: receipt.map(|receipt| receipt.end_offset),
        wal_blob_bytes_written: receipt.map(|receipt| receipt.bytes_written),
        wal_blob_object_length: receipt.map(|receipt| receipt.object_length),
    }
}

impl<O> SharedStoreStorageWriter<O>
where
    O: ObjectStore + 'static,
{
    /// Enable timer-less queue-coalesced group commit on the SYNC path. `commit_delay` (default
    /// `ZERO`) optionally widens each batch under extreme load; `ZERO` keeps it purely timer-less.
    /// Override the async queue depth at which writes stop deferring. `0` is unbounded.
    pub fn with_max_pending(mut self, max_pending: usize) -> Self {
        self.max_pending = max_pending;
        self
    }

    /// How many writes have published themselves because the queue was at capacity.
    ///
    /// Zero is the healthy state. Non-zero means the async path is no longer absorbing the
    /// write rate, and the acks that hit it paid the store's latency rather than silently
    /// growing the amount a crash would lose.
    pub fn queue_capacity_hits(&self) -> u64 {
        self.capacity_hits.load(Ordering::Relaxed)
    }

    pub fn with_group_commit(mut self, enabled: bool, commit_delay: Duration) -> Self {
        self.group_commit = enabled;
        self.commit_delay = commit_delay;
        self
    }

    pub async fn write(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        let wal_index = self.next_wal_index.fetch_add(1, Ordering::Relaxed);
        let entry = SharedStoreWalEntry {
            shard_id,
            wal_index,
            command: Some(command),
        
                        staged_blocks: Vec::new(),
                                outcomes: Vec::new(),
        };
        match self.mode {
            SharedStoreStorageMode::Sync if self.group_commit => {
                self.group_commit_write(entry).await
            }
            SharedStoreStorageMode::Sync => {
                let receipt = self.replicator.publish_wal_entry(entry).await?;
                Ok(write_report_from_receipt(wal_index, receipt.as_ref()))
            }
            SharedStoreStorageMode::Async => {
                // Read the depth and release the lock before any await: holding it across the
                // publish below would serialize every writer behind one object-store round trip.
                let at_capacity = {
                    let pending = self
                        .pending
                        .lock()
                        .expect("shared-store async queue lock poisoned");
                    self.max_pending > 0 && pending.len() >= self.max_pending
                };
                if at_capacity {
                    // The queue is the loss window, so it is not allowed to grow without end.
                    // This write pays for its own durability instead of adding to what a
                    // non-graceful exit would drop. The report already tells the truth about
                    // which happened -- `published`, not `queued`.
                    self.capacity_hits.fetch_add(1, Ordering::Relaxed);
                    let receipt = self.replicator.publish_wal_entry(entry).await?;
                    return Ok(write_report_from_receipt(wal_index, receipt.as_ref()));
                }
                self.pending
                    .lock()
                    .expect("shared-store async queue lock poisoned")
                    .push_back(entry);
                Ok(SharedStoreWriteReport {
                    wal_index,
                    published: false,
                    queued: true,
                    wal_blob_start_offset: None,
                    wal_blob_end_offset: None,
                    wal_blob_bytes_written: None,
                    wal_blob_object_length: None,
                })
            }
        }
    }

    /// Timer-less queue-coalesced group commit (this design sync-closure group-commit model). The
    /// writer stages its entry and either becomes the flush LEADER (first arrival) or a FOLLOWER
    /// that awaits the leader's covering durable barrier. The leader issues ONE coalesced append
    /// per round covering every entry staged before that round began (the batch window is bounded
    /// naturally by the append's own in-flight duration), then wakes each covered waiter. A lone
    /// writer drains a one-entry batch immediately, adding no latency.
    ///
    /// Durability invariant: a waiter is told `Ok` STRICTLY AFTER the append covering its entry
    /// returned `Ok`; on failure every covered waiter (leader included) gets `Err`. A leader that
    /// vanishes without delivering (its `waker` dropped) surfaces as `Err` to the follower — never
    /// a false ack. Exactly amortizes N durable barriers onto ~1 per group.
    async fn group_commit_write(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        let wal_index = entry.wal_index;
        let (waker, waiter) = oneshot::channel();
        let is_leader = {
            let mut buffer = self.commit.lock().expect("group-commit buffer lock poisoned");
            buffer.queue.push_back(GroupCommitWaiter { entry, waker });
            if buffer.flushing {
                false
            } else {
                buffer.flushing = true;
                true
            }
        };

        if !is_leader {
            // Follower: block until the leader publishes our entry. A canceled channel means the
            // leader disappeared without delivering — report an error, never a false ack.
            return match waiter.await {
                Ok(outcome) => outcome.into_result(),
                Err(_canceled) => Err(SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "group-commit leader dropped before publishing WAL index {wal_index}"
                    ),
                ))),
            };
        }

        // Leader. Optionally widen the batch window under extreme load (default ZERO = timer-less).
        if !self.commit_delay.is_zero() {
            tokio::time::sleep(self.commit_delay).await;
        }

        let mut my_outcome: Option<GroupCommitOutcome> = None;
        loop {
            // Drain everything staged so far into this flush round. Entries that arrive DURING the
            // append below are the next round's natural group.
            let batch: Vec<GroupCommitWaiter> = {
                let mut buffer = self.commit.lock().expect("group-commit buffer lock poisoned");
                if buffer.queue.is_empty() {
                    // Nothing left to flush: hand leadership back. A writer that enqueues after this
                    // (under the same lock) will see `flushing == false` and become the next leader.
                    buffer.flushing = false;
                    break;
                }
                buffer.queue.drain(..).collect()
            };

            let entries: Vec<SharedStoreWalEntry> =
                batch.iter().map(|waiter| waiter.entry.clone()).collect();
            let result = self.replicator.publish_wal_entries_batch(&entries).await;
            match result {
                Ok(reports) => {
                    for (waiter, report) in batch.into_iter().zip(reports.into_iter()) {
                        let outcome = GroupCommitOutcome::Committed(report);
                        if waiter.entry.wal_index == wal_index {
                            my_outcome = Some(outcome);
                        } else {
                            // A dropped receiver just means that follower's future was canceled;
                            // its entry is already durable, so nothing to undo.
                            let _ = waiter.waker.send(outcome);
                        }
                    }
                }
                Err(err) => {
                    // The covering append failed: every entry in this round is NOT durable. Fan the
                    // error out to all of them (never a false ack). The stringified error is shared;
                    // the leader keeps a typed error for its own return.
                    let message = err.to_string();
                    for waiter in batch.into_iter() {
                        let outcome = GroupCommitOutcome::Failed(message.clone());
                        if waiter.entry.wal_index == wal_index {
                            my_outcome = Some(outcome);
                        } else {
                            let _ = waiter.waker.send(outcome);
                        }
                    }
                }
            }
        }

        // Our own entry was staged before the first round drained, so it was covered by round one.
        my_outcome
            .expect("group-commit leader must observe its own entry in a flush round")
            .into_result()
    }

    pub fn queued_len(&self) -> usize {
        self.pending
            .lock()
            .expect("shared-store async queue lock poisoned")
            .len()
    }

    pub async fn flush_pending(
        &self,
        max_entries: usize,
    ) -> Result<SharedStoreFlushReport, SharedStoreReplicationError> {
        let limit = max_entries.max(1);
        let mut drained = Vec::new();
        {
            let mut pending = self
                .pending
                .lock()
                .expect("shared-store async queue lock poisoned");
            for _ in 0..limit {
                let Some(entry) = pending.pop_front() else {
                    break;
                };
                drained.push(entry);
            }
        }

        let mut last_wal_index = 0;
        let mut last_receipt = None;
        for (index, entry) in drained.iter().cloned().enumerate() {
            last_wal_index = entry.wal_index;
            match self.replicator.publish_wal_entry(entry).await {
                Ok(receipt) => last_receipt = receipt,
                Err(err) => {
                    let mut pending = self
                        .pending
                        .lock()
                        .expect("shared-store async queue lock poisoned");
                    for entry in drained[index..].iter().rev().cloned() {
                        pending.push_front(entry);
                    }
                    return Err(err);
                }
            }
        }
        let remaining = self.queued_len();
        Ok(SharedStoreFlushReport {
            flushed: drained.len(),
            remaining,
            last_wal_index,
            last_wal_blob_start_offset: last_receipt.as_ref().map(|receipt| receipt.start_offset),
            last_wal_blob_end_offset: last_receipt.as_ref().map(|receipt| receipt.end_offset),
            last_wal_blob_object_length: last_receipt.as_ref().map(|receipt| receipt.object_length),
        })
    }
}

fn parse_block_slab_id(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("block_segment_")?
        .strip_suffix(".seg")?
        .parse()
        .ok()
}

fn parse_wal_index(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("wal_")?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

/// Verify a CRC32C frame, optionally against a payload length the frame declared.
///
/// `declared_size` is `None` for the format that does not carry one. That is not a weaker check
/// by much: the comparison it skips is between a field and the length protobuf already decoded
/// from the same message, so it only ever caught a bit-flip in that field itself. The CRC is what
/// catches a damaged PAYLOAD, in both formats.
///
/// The algorithm is NAMED in both halves of the error, because more than one checksum shape can
/// reach this message and `expected 9e0126c4, actual 1a2b3c4d` with no algorithm beside it is a
/// line nobody can act on.
fn verify_crc32c(
    key: &str,
    bytes: &[u8],
    declared_size: Option<u64>,
    expected: u32,
) -> Result<(), SharedStoreReplicationError> {
    let actual = crate::checksum::crc32c(bytes);
    let size_disagrees = declared_size.is_some_and(|size| bytes.len() as u64 != size);
    if size_disagrees || actual != expected {
        return Err(SharedStoreReplicationError::ChecksumMismatch {
            path: key.to_string(),
            expected: format!("crc32c:{expected:08x}"),
            actual: format!("crc32c:{actual:08x}"),
        });
    }
    Ok(())
}

fn verify_checksum(
    key: &str,
    bytes: &[u8],
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), SharedStoreReplicationError> {
    let actual = sha256_hex(bytes);
    if bytes.len() as u64 != expected_size || actual != expected_sha256 {
        return Err(SharedStoreReplicationError::ChecksumMismatch {
            path: key.to_string(),
            expected: expected_sha256.to_string(),
            actual,
        });
    }
    Ok(())
}

const WAL_COMMAND_ENCODING_JSON_SERDE: u32 = 0;
const WAL_COMMAND_ENCODING_SDK_PROTO: u32 = 1;

/// The original log frame: the command digest written as 64 hex characters in tag 5.
///
/// Zero on purpose. A frame written before `frame_format` existed omits the field, prost reads
/// the omission as zero, and the frame therefore declares itself to be exactly what it is. No
/// migration, no conversion, and no need for a reader to know when the field was added.
const WAL_FRAME_FORMAT_HEX_DIGEST: u32 = 0;

/// The narrow log frame: the SAME SHA-256, carried as its own 32 bytes in tag 9.
///
/// Not a weaker check -- byte for byte the same digest over the same payload. What goes away is
/// the transcription: 64 hex characters plus a tag and a length for a 32-byte value. The
/// `raft::wal_proto` encoder already stores this store's digests this way, and
/// `a_hex_digest_is_stored_as_bytes_not_text` is the test that keeps it honest there.
const WAL_FRAME_FORMAT_RAW_DIGEST: u32 = 2;

/// The narrowest log frame: a CRC32C of the payload, four bytes, in tag 11.
///
/// # THIS ONE CHANGES THE CHECK, SO IT OWES AN ARGUMENT ABOUT WHAT THE CHECK IS FOR
///
/// Formats 0 and 2 carry the same SHA-256 and differ only in how it is written, so neither
/// weakens anything. This one swaps the algorithm, and that is only defensible if the digest is
/// an INTEGRITY check rather than a content IDENTITY. It is, and here is the whole of the
/// evidence rather than a claim:
///
/// * There is exactly ONE production reader of it -- [`decode_wal_proto_frame_exact`], reached
///   from a single-entry range read and from the blob walk. Everything else that names the field
///   is this encoder, a test, or the conformance report's own copy of the message.
/// * That reader compares it against a digest RECOMPUTED FROM THE BYTES IT JUST READ. It never
///   compares one entry's digest against another's, never uses it as a map key, and nothing
///   keys replay or deduplication on it -- entries are addressed by `wal_index`, and the
///   idempotency this store relies on elsewhere keys on committed offsets, not on payload
///   digests.
/// * A mismatch propagates with `?` and refuses the read. It is never downgraded to a warning
///   and never used to choose between two candidate payloads.
///
/// So the question it answers is "did these bytes survive the trip", which is the question a CRC
/// answers. It is NOT a signature and nothing treats it as one: it is written by the same process
/// that writes the payload, before the durability barrier, so it cannot defend against a forged
/// record and never could.
///
/// `log_framing` reached this conclusion for this store's own local log at the same durability
/// tier, and wrote the reasoning down: a cryptographic digest is "simply the wrong tool here: it
/// is computed per record, synchronously, before the durability barrier." Its binary frame
/// carries a raw four-byte CRC32C for exactly this reason. This is that decision, applied to the
/// shared log.
///
/// IF THAT EVER STOPS BEING TRUE -- if some reader starts treating this field as an identity, for
/// deduplication or equality or an idempotency key -- then this format is wrong at any width and
/// should be retired rather than defaulted. The argument above is the thing to re-check first,
/// not the byte count.
const WAL_FRAME_FORMAT_CRC32C: u32 = 3;

/// The same CRC32C frame, without the payload length it was restating.
///
/// # WHY THIS FIELD AND NOT THE OTHER FOUR THAT LOOK LIKE IT
///
/// Four other fields in this path restate something a reader could derive, and all four STAY.
/// They are not redundant, they are the only thing CHECKING what they restate:
///
/// * the frame's `shard_id` is cross-checked against the shard the caller asked for, and the
///   comment on that check in `read_wal_entry_from_offset_metadata` says it "is what stops replay
///   applying an entry that belongs to another shard";
/// * the sidecar's `wal_blob_bytes_written` is cross-checked against its own start/end range, and
///   the SIDECAR HAS NO CHECKSUM OF ITS OWN -- so that one comparison is the whole of its
///   integrity checking (see the field's own doc for how it got that way);
/// * the sidecar's `shard_id` cannot use zero as an "absent, derive it" marker because shard zero
///   is a legitimate shard, so omitting it needs a format field the sidecar does not have, which
///   would cost the two bytes it saves;
/// * the 4-byte length prefix is OUTSIDE the protobuf, so `frame_format` -- which lives inside the
///   message that prefix delimits -- cannot negotiate it. It would need a second, out-of-band
///   marker byte, which costs back most of what a varint saves at these frame sizes.
///
/// `command_byte_size` is the one that really is redundant, and the test for that is what it
/// CATCHES. The CRC covers `command_payload`, so a payload cut short fails the checksum; a frame
/// cut short fails the outer length-prefix check before the protobuf is even parsed. The only
/// corruption `command_byte_size` detected on its own was a bit-flip in ITS OWN varint -- a field
/// nothing else reads, so the damage it was guarding against had no other effect.
///
/// Removing it does make the size comparison in the decoder tautological, which is why
/// `a_truncated_payload_is_still_refused_without_the_declared_size` exists and is proven by a
/// plant rather than by this paragraph.
const WAL_FRAME_FORMAT_NO_SIZE: u32 = 4;

/// WHICH FORMAT A WRITER EMITS BY DEFAULT. The narrowest this build can write.
///
/// # WHAT THE DEPLOY REQUIREMENT IS
///
/// A reader understands the formats its own build knows and REFUSES the rest by name, so a writer
/// may only emit a format every reader already understands. That is a property of the RELEASE and
/// not of this constant: the release carrying this default must also carry the decoder arm for it.
/// It does -- both are in this tree, with no build in between for a peer to be stranded on.
///
/// Said the other way round, because the distinction matters: this line does not make the ordering
/// safe and cannot. It records which format is written. A LATER change of it owes the same check
/// again, against whatever is deployed at that time, and the thing to examine then is the release
/// contents rather than this constant, which cannot know them.
///
/// # THE BACKSTOPS, WHICH ARE WHAT MAKE A DEFAULT ACCEPTABLE
///
/// * A reader that does not know a format REFUSES it and NAMES it, so a mis-ordered rollout fails
///   visibly instead of replaying unverified commands. Both directions are tested:
///   `an_old_reader_refuses_a_narrow_frame_rather_than_accepting_it` and
///   `a_reader_without_the_crc_format_refuses_a_crc_frame_by_name`, each with the frame that
///   reader DOES accept as its control.
/// * [`SharedStoreReplicator::with_narrow_wal_frames`] reverts to the original wide frame with no
///   rebuild, and that frame is asserted BYTE-IDENTICAL to the one written before `frame_format`
///   existed -- a way back has to produce the old bytes, not merely something acceptable.
/// * Frames already written are unaffected: the format travels in the record, and every format
///   this crate has ever written is still decoded. A log spanning all three replays end to end.
const WAL_FRAME_FORMAT_DEFAULT: u32 = WAL_FRAME_FORMAT_NO_SIZE;

/// A lowercase hex digest as its own bytes, or `None` when the text is not one.
///
/// The round trip is CHECKED rather than assumed: a digest only becomes bytes when re-encoding
/// those bytes reproduces the original text exactly. Anything else -- an empty digest, uppercase
/// hex, a non-digest string some other path put here -- is left alone and travels as text in the
/// wide field. Same rule, and for the same reason, as `raft::wal_proto::checksum_to_bytes`.
fn digest_text_to_bytes(digest: &str) -> Option<Vec<u8>> {
    if digest.is_empty() || digest.len() % 2 != 0 {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..digest.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&digest[at..at + 2], 16).ok())
        .collect();
    bytes.filter(|bytes| hex::encode(bytes) == digest)
}

struct WalCommandMetadata {
    byte_size: u64,
    encoding: u32,
}

/// Describe the operation an entry carries, for the offset sidecar.
///
/// An entry carrying results has no operation to describe, and this sidecar exists to say how the
/// operation was encoded. Empty is the honest answer rather than a fabricated one.
///
/// # THE DIGEST THIS NO LONGER COMPUTES
///
/// This struct carried a `sha256` field, and it was a full SHA-256 of the payload computed on
/// every append and thrown away. It became unread when the sidecar's duplicate digest was retired
/// (#2319): before that the two sidecar encoders read it, afterwards nothing in the crate did, and
/// the hash kept being computed because removing the writer is not the same as removing the field.
///
/// The frame's own encoder hashes the same payload for the digest it actually carries, so every
/// append hashed its payload TWICE and used one result. On the batch path that was once per entry.
///
/// It survived because of WHERE it was read: the only remaining readers were the tests that
/// measured it, and those compile under `cfg(test)`, where the field is live. `cargo test --lib`
/// -- the gate this path is normally held to -- therefore could not see it. `cargo check
/// --all-targets` compiles the plain lib as well and reports it as never read, which is how it
/// was found. A field kept alive by the tests that measure it is not a field anything uses.
fn wal_command_metadata(
    command: Option<&Command>,
) -> Result<WalCommandMetadata, SharedStoreReplicationError> {
    let Some(command) = command else {
        return Ok(WalCommandMetadata {
            byte_size: 0,
            encoding: WAL_COMMAND_ENCODING_JSON_SERDE,
        });
    };
    let (command_payload, command_encoding) = match command_to_sdk_proto(command) {
        Some(command) => (command.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
        None => (
            serde_json::to_vec(command)?,
            WAL_COMMAND_ENCODING_JSON_SERDE,
        ),
    };
    Ok(WalCommandMetadata {
        byte_size: command_payload.len() as u64,
        encoding: command_encoding,
    })
}

fn encode_wal_proto_frame(
    entry: &SharedStoreWalEntry,
    frame_format: u32,
) -> Result<Vec<u8>, SharedStoreReplicationError> {
    let (command_payload, command_encoding) = match entry.command.as_ref() {
        // No operation to carry: the entry states results instead.
        None => (Vec::new(), WAL_COMMAND_ENCODING_JSON_SERDE),
        Some(command) => match command_to_sdk_proto(command) {
            Some(encoded) => (encoded.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
            None => (serde_json::to_vec(command)?, WAL_COMMAND_ENCODING_JSON_SERDE),
        },
    };
    // The same rule the log frame follows: a carried block states the length its address
    // covers, so the address need not restate it.
    let implied_length = if entry.outcomes.len() == 1 && entry.staged_blocks.len() == 1 {
        let carried =
            entry.staged_blocks[0].bytes.len() + crate::block_store::BLOCK_RECORD_HEADER_LEN;
        Some(carried as u64)
    } else {
        None
    };
    // ONE digest, written in whichever shape this frame declares. The narrow shape falls back to
    // the wide one when the digest is not hex that round-trips, so the field that carries it is
    // never the field the format says is empty.
    // The CRC format computes no SHA-256 at all. The other two need one, and only one: this is
    // the single place the payload is hashed on the append path.
    let (command_sha256, command_digest, command_crc32c, frame_format) = match frame_format {
        WAL_FRAME_FORMAT_NO_SIZE => (
            String::new(),
            Vec::new(),
            crate::checksum::crc32c(&command_payload),
            WAL_FRAME_FORMAT_NO_SIZE,
        ),
        WAL_FRAME_FORMAT_CRC32C => (
            String::new(),
            Vec::new(),
            crate::checksum::crc32c(&command_payload),
            WAL_FRAME_FORMAT_CRC32C,
        ),
        WAL_FRAME_FORMAT_RAW_DIGEST => match digest_text_to_bytes(&sha256_hex(&command_payload)) {
            Some(raw) => (String::new(), raw, 0, WAL_FRAME_FORMAT_RAW_DIGEST),
            None => (
                sha256_hex(&command_payload),
                Vec::new(),
                0,
                WAL_FRAME_FORMAT_HEX_DIGEST,
            ),
        },
        _ => (
            sha256_hex(&command_payload),
            Vec::new(),
            0,
            WAL_FRAME_FORMAT_HEX_DIGEST,
        ),
    };
    // The payload's length is already on the wire as the `bytes` field's own length delimiter,
    // and the CRC covers the payload. Format 4 therefore does not restate it; every other format
    // does, because a reader of those formats compares against it.
    let command_byte_size = if frame_format == WAL_FRAME_FORMAT_NO_SIZE {
        0
    } else {
        command_payload.len() as u64
    };
    let frame = SharedStoreWalFrameProto {
        shard_id: entry.shard_id,
        wal_index: entry.wal_index,
        command_byte_size,
        command_sha256,
        command_digest,
        command_crc32c,
        frame_format,
        command_payload,
        command_encoding,
        staged_blocks: entry
            .staged_blocks
            .iter()
            .map(|block| SharedStoreStagedBlockProto {
                object_id: block.object_id,
                bytes: block.bytes.clone(),
                component: block.component.as_deref().map(str::to_string),
            })
            .collect(),
        items: entry
            .outcomes
            .iter()
            .map(|item| {
                crate::wal_proto::item_to_proto(
                    item,
                    entry.shard_id,
                    implied_length,
                    crate::wal_proto::command_object_key(entry.command.as_ref()).as_deref(),
                )
            })
            .collect(),
    };
    let mut encoded = frame.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    Ok(out)
}

/// One sidecar frame: WHERE in the log an entry is, so a reader can range-read it without
/// walking the log.
///
/// # WHAT THIS FRAME DELIBERATELY DOES NOT CARRY
///
/// Two of its fields used to be the whole cost of it. Measured on this encoder, a sidecar frame
/// for a 47-byte log key was 137 B: `command_sha256` 66 B, `wal_blob_key` 49 B, and 22 B for the
/// eleven other fields TOGETHER. Neither of the two was carrying information.
///
/// `command_sha256` was a second hex copy of the digest the log frame already carries. The frame's
/// copy is the one the read path verifies (`decode_wal_proto_frame_exact` -> `verify_checksum`);
/// the sidecar's was decoded into a struct field and never compared against anything. A 32-byte
/// digest written as 64 hex characters, twice per append, to be read once.
///
/// `wal_blob_key` is `derived_blob_key` for every entry of a shard, and the frame states its own
/// `shard_id`, so the key was the same string repeated in every frame of an append-only index. It
/// is written ONLY when it differs from the derived key, and
/// [`decode_wal_offset_metadata_blob`] puts the derived key back when the field is empty. A
/// reader therefore always sees a populated key, and a sidecar written before this change keeps
/// its explicit key and decodes unchanged.
///
/// What is left is offsets, and offsets are what a sidecar is for.
fn encode_wal_offset_metadata_frame(
    metadata: &SharedStoreWalOffsetMetadata,
    derived_blob_key: &str,
) -> Vec<u8> {
    let proto = SharedStoreWalOffsetMetadataProto {
        shard_id: metadata.shard_id,
        wal_index: metadata.wal_index,
        // The common case: the shard's own log. Omitted, and put back on decode.
        wal_blob_key: if metadata.wal_blob_key == derived_blob_key {
            String::new()
        } else {
            metadata.wal_blob_key.clone()
        },
        wal_blob_start_offset: metadata.wal_blob_start_offset,
        wal_blob_end_offset: metadata.wal_blob_end_offset,
        wal_blob_bytes_written: metadata.wal_blob_bytes_written,
        wal_blob_object_length: metadata.wal_blob_object_length,
        command_byte_size: metadata.command_byte_size,
        command_encoding: metadata.command_encoding,
        wal_blob_physical_slab_count: metadata.wal_blob_physical_slab_count,
        wal_blob_first_physical_offset: metadata.wal_blob_first_physical_offset,
    };
    let mut encoded = proto.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    out
}

fn decode_wal_proto_frame_exact(
    bytes: &[u8],
    checksum_source: &str,
) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
    if bytes.len() < 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated protobuf WAL frame length",
        )));
    }
    let len = u32::from_be_bytes(
        bytes[0..4]
            .try_into()
            .expect("length slice is exactly 4 bytes"),
    ) as usize;
    if bytes.len() != len + 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "protobuf WAL frame range does not match frame length",
        )));
    }
    let frame = SharedStoreWalFrameProto::decode(&bytes[4..])?;
    // DISPATCH PER FRAME, not per log. Which shape a frame carries is a property of the frame, so
    // a blob holding both -- what a rolling restart leaves -- walks end to end either way.
    match frame.frame_format {
        WAL_FRAME_FORMAT_HEX_DIGEST => verify_checksum(
            checksum_source,
            &frame.command_payload,
            frame.command_byte_size,
            &frame.command_sha256,
        )?,
        WAL_FRAME_FORMAT_RAW_DIGEST => verify_checksum(
            checksum_source,
            &frame.command_payload,
            frame.command_byte_size,
            &hex::encode(&frame.command_digest),
        )?,
        // Format 3 declares the payload length and is checked against it; format 4 does not
        // declare it, and the CRC is the whole of the check. Nothing else differs between them.
        WAL_FRAME_FORMAT_CRC32C => verify_crc32c(
            checksum_source,
            &frame.command_payload,
            Some(frame.command_byte_size),
            frame.command_crc32c,
        )?,
        WAL_FRAME_FORMAT_NO_SIZE => verify_crc32c(
            checksum_source,
            &frame.command_payload,
            None,
            frame.command_crc32c,
        )?,
        // REFUSED, not guessed. A format this build does not know carries its digest somewhere
        // this build cannot see, and verifying the field that happens to be empty would report a
        // checksum mismatch -- blaming the data for a version skew. Naming the format is the
        // difference between "this log is corrupt" and "this node is too old for this log".
        other => {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported shared-store WAL frame format {other} at {checksum_source}: \
                     this build understands {WAL_FRAME_FORMAT_HEX_DIGEST}, \
                     {WAL_FRAME_FORMAT_RAW_DIGEST}, {WAL_FRAME_FORMAT_CRC32C} and \
                     {WAL_FRAME_FORMAT_NO_SIZE}"
                ),
            )));
        }
    }
    let command = match frame.command_encoding {
        // Outcome-only records deliberately carry no command. Keep empty command records
        // invalid unless results are present, and still reject unknown encoding tags.
        WAL_COMMAND_ENCODING_JSON_SERDE
            if frame.command_payload.is_empty() && !frame.items.is_empty() => None,
        WAL_COMMAND_ENCODING_SDK_PROTO => {
            let command = v1::Command::decode(frame.command_payload.as_slice())?;
            Some(sdk::sdk_command_to_types(command).map_err(|status| {
                SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    status.to_string(),
                ))
            })?)
        }
        WAL_COMMAND_ENCODING_JSON_SERDE => Some(serde_json::from_slice(&frame.command_payload)?),
        other => {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported protobuf WAL command encoding {other}"),
            )));
        }
    };
    let implied_length = if frame.items.len() == 1 && frame.staged_blocks.len() == 1 {
        let carried =
            frame.staged_blocks[0].bytes.len() + crate::block_store::BLOCK_RECORD_HEADER_LEN;
        Some(carried as u64)
    } else {
        None
    };
    // Worked out before the command is moved into the record, and once rather than per item.
    let command_key = crate::wal_proto::command_object_key(command.as_ref());
    Ok(SharedStoreWalEntry {
        shard_id: frame.shard_id,
        wal_index: frame.wal_index,
        command,
        outcomes: frame
            .items
            .into_iter()
            .map(|item| {
                crate::wal_proto::item_from_proto(
                    item,
                    frame.shard_id,
                    implied_length,
                    command_key.as_deref(),
                )
            })
            .collect(),
        staged_blocks: frame
            .staged_blocks
            .into_iter()
            .map(|block| crate::wal::StagedBlock {
                object_id: block.object_id,
                // Never defaulted: an absent element MEANS a whole-object page, and
                // inventing one would make a string page claim to be an element.
                component: block.component.map(std::sync::Arc::from),
                bytes: block.bytes,
            })
            .collect(),
    })
}

fn decode_wal_proto_blob(
    bytes: &[u8],
) -> Result<Vec<SharedStoreWalEntry>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = Vec::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame payload",
            )));
        }
        let entry =
            decode_wal_proto_frame_exact(&bytes[cursor - 4..cursor + len], "protobuf-wal-blob")?;
        cursor += len;
        entries.push(entry);
    }
    Ok(entries)
}

/// Decode a whole sidecar blob, putting the derived log key back into every frame that omitted
/// it. `derived_blob_key` is the key the shard's log lives at; a frame carrying an explicit key
/// keeps it, which is what makes an older sidecar decode unchanged.
fn decode_wal_offset_metadata_blob(
    bytes: &[u8],
    derived_blob_key: &str,
) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = BTreeMap::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame payload",
            )));
        }
        let frame = SharedStoreWalOffsetMetadataProto::decode(&bytes[cursor..cursor + len])?;
        cursor += len;
        entries.insert(
            frame.wal_index,
            SharedStoreWalOffsetMetadata {
                shard_id: frame.shard_id,
                wal_index: frame.wal_index,
                wal_blob_key: if frame.wal_blob_key.is_empty() {
                    derived_blob_key.to_string()
                } else {
                    frame.wal_blob_key
                },
                wal_blob_start_offset: frame.wal_blob_start_offset,
                wal_blob_end_offset: frame.wal_blob_end_offset,
                wal_blob_bytes_written: frame.wal_blob_bytes_written,
                wal_blob_object_length: frame.wal_blob_object_length,
                wal_blob_physical_slab_count: frame.wal_blob_physical_slab_count,
                wal_blob_first_physical_offset: frame.wal_blob_first_physical_offset,
                command_byte_size: frame.command_byte_size,
                command_encoding: frame.command_encoding,
            },
        );
    }
    Ok(entries)
}

fn command_to_sdk_proto(command: &Command) -> Option<v1::Command> {
    let kind = match command {
        Command::CommonExpire { key, ttl_ms } => {
            v1::command::Kind::CommonExpire(v1::CommonExpire {
                key: key.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::CommonExists { key } => {
            v1::command::Kind::CommonExists(v1::CommonExists { key: key.clone() })
        }
        Command::StringSet { key, value } => v1::command::Kind::StringSet(v1::StringSet {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: 0,
        }),
        Command::StringSetEx { key, value, ttl_ms } => {
            v1::command::Kind::StringSet(v1::StringSet {
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::StringGet { key } => {
            v1::command::Kind::StringGet(v1::StringGet { key: key.clone() })
        }
        Command::StringDelete { key } => {
            v1::command::Kind::StringDelete(v1::StringDelete { key: key.clone() })
        }
        Command::HashSet { key, field, value } => v1::command::Kind::HashSet(v1::HashSet {
            key: key.clone(),
            field: field.clone(),
            value: value.clone(),
        }),
        Command::HashGet { key, field } => v1::command::Kind::HashGet(v1::HashGet {
            key: key.clone(),
            field: field.clone(),
        }),
        Command::HashMultiSet { key, entries } => {
            v1::command::Kind::HashMultiSet(v1::HashMultiSet {
                key: key.clone(),
                entries: entries
                    .iter()
                    .map(|(field, value)| v1::HashEntry {
                        field: field.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
        }
        Command::HashMultiGet { key, fields } => {
            v1::command::Kind::HashMultiGet(v1::HashMultiGet {
                key: key.clone(),
                fields: fields.clone(),
            })
        }
        Command::SetAdd { key, member } => v1::command::Kind::SetAdd(v1::SetAdd {
            key: key.clone(),
            member: member.clone(),
        }),
        Command::SetMembers { key } => {
            v1::command::Kind::SetMembers(v1::SetMembers { key: key.clone() })
        }
        Command::FeatureAppend { key, points } => {
            v1::command::Kind::FeatureAppend(v1::FeatureAppend {
                key: key.clone(),
                points: points
                    .iter()
                    .map(|point| v1::FeaturePoint {
                        timestamp_ms: point.timestamp_ms,
                        value: point.value.clone(),
                    })
                    .collect(),
            })
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => v1::command::Kind::FeatureQuery(v1::FeatureQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: count.unwrap_or(0).min(u32::MAX as usize) as u32,
        }),
        Command::SequenceAdd { key, rows } => {
            v1::command::Kind::SequenceAppend(v1::SequenceAppend {
                key: key.clone(),
                rows: rows
                    .iter()
                    .map(|row| v1::SequenceFeatureRow {
                        timestamp_ms: row.timestamp_ms,
                        gid: row.gid,
                        action_type: row.action_type,
                        duration: row.duration,
                        author_id: row.author_id,
                    })
                    .collect(),
            })
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } if filters.is_empty() => v1::command::Kind::SequenceQuery(v1::SequenceQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: (*count).min(u32::MAX as usize) as u32,
        }),
        Command::ControlStateIncrement {
            key,
            timestamp_ms,
            amount,
        } => v1::command::Kind::ControlStateIncrement(v1::ControlStateIncrement {
            key: key.clone(),
            family: String::new(),
            delta: *amount,
            timestamp_ms: *timestamp_ms,
        }),
        Command::ControlStateQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => v1::command::Kind::ControlStateQuery(v1::ControlStateQuery {
            key: key.clone(),
            family: aggregator.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
        }),
        _ => return None,
    };
    Some(v1::Command { kind: Some(kind) })
}

/// The one spelling of a block slab's object basename.
///
/// Composed in two places under two different prefixes -- `block_slab_key` for the
/// live shard layout and the checkpoint upload in `publish_checkpoint` -- and the
/// second of those used to spell it inline, where no test could reach it. A padding
/// change there survived the whole gate even once `block_slab_key` was pinned.
///
/// Two further spellings of this same format are still out of reach from here and
/// cannot be folded in: `parse_block_slab_id` below reads it back apart (and is
/// pinned against this one by a test), and `block_store::paths::slab_path` names the
/// same slab on local disk from inside a module this one cannot see.
fn block_slab_object_name(block_slab_id: u64) -> String {
    format!("block_segment_{block_slab_id:020}.seg")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

    use super::*;
    use crate::types::CommandResponse;


    #[tokio::test]
    async fn default_shared_wal_preserves_multiple_binary_outcomes_and_older_json() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
        let legacy = SharedStoreReplicator::new("default-binary", store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::JsonPerKey);
        legacy
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 1,
                command: Some(Command::StringSet {
                    key: "old".into(),
                    value: b"old-value".to_vec(),
                }),
                staged_blocks: Vec::new(),
                outcomes: Vec::new(),
            })
            .await
            .unwrap();
        let writer = SharedStoreReplicator::new("default-binary", store.clone());
        assert_eq!(
            writer.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        );
        let retry_writer = SharedStoreReplicator::with_retry_policy(
            "default-binary",
            store.clone(),
            SharedStoreRetryPolicy::default(),
        );
        assert_eq!(
            retry_writer.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        );
        let primary = test_engine(dir.path(), "default-writer");
        primary.load_shard(1);
        for index in 2..=3 {
            let key = format!("new-{index}");
            assert!(
                primary
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.clone(),
                            value: vec![0, 255, index as u8]
                        },
                    })
                    .status
                    .ok
            );
            let rows = primary
                .write_ahead_log_store()
                .scan(1, 0, u64::MAX, u64::MAX)
                .unwrap();
            let (_, payload) = rows.last().expect("a write produced a WAL record");
            assert_eq!(payload.first(), Some(&crate::log_framing::FRAME_MAGIC_V3));
            let protobuf = crate::log_framing::decode_line(payload).unwrap();
            assert!(matches!(
                protobuf.first(),
                Some(&crate::wal_proto::RAW_PAYLOAD_MARKER)
                    | Some(&crate::wal_proto::COMPRESSED_RAW_PAYLOAD_MARKER)
            ));
            let record = crate::wal::decode_wal_line(payload).unwrap();
            assert!(
                record.command.is_none(),
                "the default must record outcomes without a duplicate command"
            );
            assert_eq!(record.outcomes.len(), 1);
            let mut staged_blocks = record.staged_blocks;
            if staged_blocks.is_empty() {
                for item in &record.outcomes {
                    if let Some(address) = item.resolved_address() {
                        staged_blocks.push(crate::wal::StagedBlock {
                            object_id: item.object_id,
                            component: item.component.as_deref().map(Arc::from),
                            bytes: primary.block_store().read(&address).unwrap(),
                        });
                    }
                }
            }
            let entry = SharedStoreWalEntry {
                shard_id: 1,
                wal_index: index,
                command: record.command,
                staged_blocks,
                outcomes: record.outcomes,
            };
            assert!(writer.publish_wal_entry(entry).await.unwrap().is_some());
        }
        let bytes = store.get(&writer.wal_blob_key(1)).await.unwrap();
        let records = decode_wal_proto_blob(&bytes).unwrap();
        assert_eq!(
            records.len(),
            2,
            "append must not overwrite the first outcome"
        );
        for (record, index) in records.iter().zip(2..=3) {
            assert_eq!(record.wal_index, index);
            assert!(record.command.is_none());
            assert_eq!(record.outcomes[0].object_key, format!("new-{index}"));
        }
        let reader = SharedStoreReplicator::new("default-binary", store.clone());
        assert_eq!(reader.latest_persisted_wal_index(1).await.unwrap(), 3);
        let follower = test_engine(dir.path(), "default-reader");
        follower.load_shard(1);
        let replay = reader.replay_wal_strict(1, 0, &follower).await.unwrap();
        assert_eq!(replay.applied, 3);
        for (key, expected) in [
            ("old", b"old-value".to_vec()),
            ("new-2", vec![0, 255, 2]),
            ("new-3", vec![0, 255, 3]),
        ] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet { key: key.into() }
                    })
                    .response,
                CommandResponse::Bytes {
                    value: Some(expected)
                }
            );
        }
        store.delete(&legacy.wal_key(1, 1)).await.unwrap();
        let indexed = test_engine(dir.path(), "indexed-reader");
        indexed.load_shard(1);
        let replay = reader.replay_wal_strict(1, 1, &indexed).await.unwrap();
        assert_eq!(replay.applied, 2);
        assert_eq!(replay.offset_index_reads, 2);
        for index in 2..=3 {
            assert_eq!(indexed.execute(ExecuteRequest {
                shard_id: 1, command: Command::StringGet { key: format!("new-{index}") },
            }).response, CommandResponse::Bytes { value: Some(vec![0, 255, index as u8]) });
        }
        legacy.publish_wal_entry(SharedStoreWalEntry {
            shard_id: 1, wal_index: 4,
            command: Some(Command::StringSet { key: "compat-tail".into(), value: vec![4] }),
            outcomes: Vec::new(), staged_blocks: Vec::new(),
        }).await.unwrap();
        assert_eq!(reader.latest_persisted_wal_index(1).await.unwrap(), 4);
        assert_eq!(reader.replay_wal_strict(1, 3, &indexed).await.unwrap().applied, 1);
        assert_eq!(indexed.execute(ExecuteRequest {
            shard_id: 1, command: Command::StringGet { key: "compat-tail".into() },
        }).response, CommandResponse::Bytes { value: Some(vec![4]) });
    }

    #[test]
    fn outcome_only_binary_frames_recover_in_every_supported_format() {
        let key = "result-key";
        let mut entry = SharedStoreWalEntry {
            shard_id: 1,
            wal_index: 1,
            command: None,
            staged_blocks: Vec::new(),
            outcomes: vec![crate::wal::WalOutcomeItem {
                kind: "string".to_string(),
                object_key: key.to_string(),
                component: None,
                object_id: crate::engine::hashing::stable_block_object_id(1, "string", key),
                routing_bucket: 7,
                address: None,
                value: Some(vec![0, 255, 10, 128]),
                ttl: Some(123456),
                deleted: false,
                meta: false,
            }],
        };
        for format in [
            WAL_FRAME_FORMAT_HEX_DIGEST,
            WAL_FRAME_FORMAT_RAW_DIGEST,
            WAL_FRAME_FORMAT_CRC32C,
            WAL_FRAME_FORMAT_NO_SIZE,
        ] {
            let frame = encode_wal_proto_frame(&entry, format).unwrap();
            assert_eq!(decode_wal_proto_frame_exact(&frame, "test").unwrap(), entry);
            assert_eq!(decode_wal_proto_blob(&frame).unwrap(), vec![entry.clone()]);
        }
        // A carried command must still survive beside its results.
        entry.command = Some(Command::StringSet {
            key: key.to_string(),
            value: vec![0, 255],
        });
        let frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_NO_SIZE).unwrap();
        assert_eq!(decode_wal_proto_frame_exact(&frame, "test").unwrap(), entry);

        // The omission is specific to the writer's encoding, not permission to guess a codec.
        entry.command = None;
        let frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_NO_SIZE).unwrap();
        let mut message = SharedStoreWalFrameProto::decode(&frame[4..]).unwrap();
        message.command_encoding = 99;
        let payload = message.encode_to_vec();
        let mut unknown = (payload.len() as u32).to_be_bytes().to_vec();
        unknown.extend(payload);
        assert!(decode_wal_proto_frame_exact(&unknown, "test").is_err());

        entry.outcomes.clear();
        let empty = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_NO_SIZE).unwrap();
        assert!(decode_wal_proto_frame_exact(&empty, "test").is_err());
    }

    /// WHERE ONE APPEND'S FIXED ENVELOPE GOES, TERM BY TERM.
    ///
    /// A swept fit over the value size says one append in `ProtobufAppendBlob` mode costs
    /// `rate * payload + envelope`, where the rate is the payload written once and is at its
    /// floor. The envelope is not, and it is what these terms account for. It has come down in
    /// three steps, each by removing or narrowing a NAMED term rather than by tuning anything:
    ///
    /// ```text
    ///   242 B   the sidecar carried a duplicate hex digest and the log's object key
    ///   118 B   both retired: the digest was unread, the key was derivable
    ///    88 B   the log frame's digest carried as 32 bytes instead of 64 hex characters
    ///    59 B   that digest replaced by a CRC32C, which is what the check is FOR
    ///    56 B   the frame stops restating a payload length protobuf already carries
    /// ```
    ///
    /// Three of the five steps were the same term: the log frame's checksum, which began as 66 B
    /// of hex and is now 5 B.
    ///
    /// # WHAT IS LEFT, AND WHY IT IS STAYING
    ///
    /// Four more fields in these two frames restate something a reader could derive, and all four
    /// are KEPT -- not because deriving them is hard, but because each is the only thing checking
    /// what it restates. [`WAL_FRAME_FORMAT_NO_SIZE`] lists them with the guard behind each. The
    /// sidecar's case is the instructive one: its redundancy became load-bearing because #2319
    /// retired its checksum, so an earlier saving in this same effort is what made a later one
    /// unsafe.
    ///
    /// The figures are fitted by `one_append_amplification_report`, which is the only thing that
    /// can state them: a term measured here is a term of one frame, and the envelope is two
    /// frames plus what the object store does with them. These terms must SUM toward that figure,
    /// and the report's own ceiling is what fails if they stop doing so.
    ///
    /// # WHY A DELTA NAMES A FIELD HERE
    ///
    /// Both frames are plain protobuf with a 4-byte big-endian length prefix and NO compression,
    /// so re-encoding with one field cleared and subtracting attributes the difference to that
    /// field. On the served index -- `TSIDX\x01` + header + a ZSTD frame -- the same subtraction
    /// would name nothing, because a compressed delta is a fact about the compressor.
    ///
    /// # THE TERMS ARE ASSERTED TO SUM TO THE WHOLE
    ///
    /// Each term is measured independently and the residue is asserted to be what is left.
    /// Without that identity a term could be dropped from the table and the table would still
    /// look complete.
    mod one_append_envelope_terms {
        use super::*;

        const TERM_SHARD: u64 = 1;
        const TERM_INDEX: u64 = 1;

        /// The real production key, not a short stand-in: its LENGTH is the cost being priced, so
        /// a made-up short key would under-report the very term under test.
        const TERM_BLOB_KEY: &str = "cluster-a/shards/1/shared/wal/wal.protobuf.blob";

        fn offset_metadata(key: &str) -> SharedStoreWalOffsetMetadata {
            SharedStoreWalOffsetMetadata {
                shard_id: TERM_SHARD,
                wal_index: TERM_INDEX,
                wal_blob_key: key.to_string(),
                wal_blob_start_offset: 0,
                wal_blob_end_offset: 101,
                wal_blob_bytes_written: 101,
                wal_blob_object_length: 101,
                wal_blob_physical_slab_count: 1,
                wal_blob_first_physical_offset: Some(0),
                command_byte_size: 15,
                command_encoding: WAL_COMMAND_ENCODING_SDK_PROTO,
            }
        }

        /// The sidecar frame as it stands: offsets, and almost nothing else.
        ///
        /// Before the two retirements this frame was 137 B for this key -- `command_sha256` 66 B,
        /// `wal_blob_key` 49 B, and 22 B for the eleven other fields TOGETHER. Both of the big
        /// terms are now absent in the common case, and this asserts what is left.
        #[test]
        fn the_sidecar_frame_is_offsets_and_little_else() {
            let key = TERM_BLOB_KEY;
            // The common case: the key the shard derives, so the frame omits it.
            let derived = encode_wal_offset_metadata_frame(&offset_metadata(key), key).len();
            // The uncommon case: some OTHER blob, so the frame has to say which.
            let explicit =
                encode_wal_offset_metadata_frame(&offset_metadata(key), "somewhere/else").len();
            let key_term = explicit - derived;

            println!(
                "sidecar frame: {derived} B when the key is derived, {explicit} B when it is \
                 written out -- the key itself is {key_term} B"
            );

            assert_eq!(
                key_term,
                key.len() + 2,
                "writing the key out costs {key_term} B for a {} byte key, so the omission in \
                 the derived case is worth exactly that",
                key.len()
            );

            // THE HEADLINE. 22 B of offsets was the residue under the two strings; the frame is
            // now that residue. An exact figure, because a tolerance here would admit the very
            // regrowth this guard exists to catch.
            assert_eq!(
                derived, 22,
                "the sidecar frame is {derived} B, not 22. Something was added to it, or one of \
                 the two retired string terms came back. Re-price it field by field before \
                 changing this number"
            );

            // And the frame must still be big enough to be the offsets it claims to carry: a
            // frame that had collapsed to nothing would also satisfy the assertion above if the
            // number were edited, so the fields are checked by round-trip below rather than only
            // by size.
            let decoded = decode_wal_offset_metadata_blob(
                &encode_wal_offset_metadata_frame(&offset_metadata(key), key),
                key,
            )
            .expect("the frame decodes");
            assert_eq!(decoded.len(), 1, "one frame in, one frame out");
            assert_eq!(decoded[&TERM_INDEX], offset_metadata(key));
        }

        /// The derived key is OMITTED on the wire and PUT BACK on decode.
        ///
        /// Both halves matter. Omitting it without putting it back would hand the read path an
        /// empty key and it would range-read nothing; putting it back without omitting it would
        /// save no bytes. So the wire is checked for the absence and the decode for the presence.
        #[test]
        fn the_derived_key_is_omitted_on_the_wire_and_restored_on_decode() {
            let key = TERM_BLOB_KEY;
            let frame = encode_wal_offset_metadata_frame(&offset_metadata(key), key);

            // THE ABSENCE, asserted on the bytes rather than inferred from the length.
            assert!(
                !frame.windows(key.len()).any(|window| window == key.as_bytes()),
                "the derived key is still on the wire, so it was not omitted"
            );

            // THE PRESENCE, after decode.
            let decoded =
                decode_wal_offset_metadata_blob(&frame, key).expect("the frame decodes");
            assert_eq!(
                decoded[&TERM_INDEX].wal_blob_key, key,
                "the decoder did not put the derived key back, so a reader would range-read an \
                 empty key"
            );

            // A key that is NOT the derived one is written out and comes back as itself -- the
            // field still works when it has something to say.
            let other = "cluster-a/shards/1/shared/wal/wal.protobuf.blob.rotated";
            let rotated = encode_wal_offset_metadata_frame(&offset_metadata(other), key);
            assert!(
                rotated.windows(other.len()).any(|window| window == other.as_bytes()),
                "a key that differs from the derived one must be written out"
            );
            let decoded =
                decode_wal_offset_metadata_blob(&rotated, key).expect("the frame decodes");
            assert_eq!(
                decoded[&TERM_INDEX].wal_blob_key, other,
                "an explicit key must survive decode rather than being overwritten by the \
                 derived one"
            );
        }

        /// A SIDECAR WRITTEN BEFORE THE RETIREMENTS STILL DECODES.
        ///
        /// Retiring tag 9 and omitting tag 3 are both changes to a DURABLE format, so the only
        /// evidence that matters is an old frame going through the current decoder. This builds
        /// one in the retired shape -- explicit key in tag 3, hex digest in tag 9 -- and decodes
        /// it, rather than asserting from the comment that prost skips unknown fields.
        #[test]
        fn a_sidecar_written_before_the_retirements_still_decodes() {
            /// The sidecar as it was: tag 3 always written, tag 9 a hex digest.
            #[derive(Clone, PartialEq, Message)]
            struct RetiredSidecarProto {
                #[prost(uint64, tag = "1")]
                shard_id: u64,
                #[prost(uint64, tag = "2")]
                wal_index: u64,
                #[prost(string, tag = "3")]
                wal_blob_key: String,
                #[prost(uint64, tag = "4")]
                wal_blob_start_offset: u64,
                #[prost(uint64, tag = "5")]
                wal_blob_end_offset: u64,
                #[prost(uint64, tag = "6")]
                wal_blob_bytes_written: u64,
                #[prost(uint64, tag = "7")]
                wal_blob_object_length: u64,
                #[prost(uint64, tag = "8")]
                command_byte_size: u64,
                #[prost(string, tag = "9")]
                command_sha256: String,
                #[prost(uint32, tag = "10")]
                command_encoding: u32,
                #[prost(uint64, tag = "11")]
                wal_blob_physical_slab_count: u64,
                #[prost(uint64, optional, tag = "12")]
                wal_blob_first_physical_offset: Option<u64>,
            }

            let old = RetiredSidecarProto {
                shard_id: TERM_SHARD,
                wal_index: TERM_INDEX,
                wal_blob_key: TERM_BLOB_KEY.to_string(),
                wal_blob_start_offset: 0,
                wal_blob_end_offset: 101,
                wal_blob_bytes_written: 101,
                wal_blob_object_length: 101,
                command_byte_size: 15,
                command_sha256: sha256_hex(b"a command payload"),
                command_encoding: WAL_COMMAND_ENCODING_SDK_PROTO,
                wal_blob_physical_slab_count: 1,
                wal_blob_first_physical_offset: Some(0),
            };
            let encoded = old.encode_to_vec();
            let mut blob = Vec::with_capacity(4 + encoded.len());
            blob.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            blob.extend_from_slice(&encoded);

            // The old frame carried both retired terms, so it is MEASURABLY bigger. If this ever
            // stopped holding, the comparison below would be against a frame that is not the old
            // shape at all.
            let now = encode_wal_offset_metadata_frame(&offset_metadata(TERM_BLOB_KEY), TERM_BLOB_KEY);
            assert_eq!(
                blob.len(),
                137,
                "the retired shape should encode to 137 B for this key; it is {} B, so this \
                 fixture is not the shape that was retired",
                blob.len()
            );
            assert!(
                blob.len() > now.len(),
                "the retired frame ({} B) is not bigger than the current one ({} B)",
                blob.len(),
                now.len()
            );

            // AND IT STILL DECODES, with its explicit key intact and every offset where it was.
            let decoded = decode_wal_offset_metadata_blob(&blob, "a/different/derived/key")
                .expect("a sidecar written before the retirements must still decode");
            let entry = &decoded[&TERM_INDEX];
            assert_eq!(
                entry.wal_blob_key, TERM_BLOB_KEY,
                "the old frame's explicit key was lost; a replay would range-read the wrong blob"
            );
            assert_eq!(entry.wal_blob_start_offset, 0);
            assert_eq!(entry.wal_blob_end_offset, 101);
            assert_eq!(entry.wal_blob_bytes_written, 101);
            assert_eq!(entry.wal_blob_object_length, 101);
            assert_eq!(entry.command_byte_size, 15);
            assert_eq!(entry.command_encoding, WAL_COMMAND_ENCODING_SDK_PROTO);
            assert_eq!(entry.wal_blob_physical_slab_count, 1);
            assert_eq!(entry.wal_blob_first_physical_offset, Some(0));
        }

        /// THE LOG FRAME LEG, term by term, so the whole envelope is accounted and not just the
        /// sidecar's half of it.
        ///
        /// The frame's envelope over the user's bytes is what is left after the payload: a 4-byte
        /// length prefix, four small scalars, the protobuf wrapper around the payload, the format
        /// the frame declares, and the digest.
        ///
        /// # THE DIGEST IS NO LONGER THE LARGEST TERM, AND THAT IS THE RESULT
        ///
        /// It was 66 B of this leg when it was 64 hex characters, then 34 as its own 32 bytes, and
        /// it is 5 as a CRC32C. The framing and scalars have been 16 B throughout. So the leg that
        /// was "mostly its digest" is now mostly its FRAMING, and the assertion at the end of this
        /// test says so in that direction -- it is the honest statement of where the remaining
        /// bytes are, and the first thing anyone attacking this leg next should read.
        ///
        /// # THE TERMS ARE MEASURED, NOT COUNTED
        ///
        /// Each term is obtained by clearing its field and re-encoding, so the figure comes from
        /// the encoder rather than from arithmetic in a comment that can drift from it. That
        /// subtraction is only meaningful because both frames are plain protobuf behind a 4-byte
        /// length prefix with NO compression -- the module doc above says why the same move would
        /// name nothing on the zstd-framed served index.
        #[test]
        fn the_log_frame_leg_is_mostly_its_digest() {
            let entry = term_entry(&[b'v'; 512]);
            let frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_DEFAULT)
                .expect("the frame encodes");
            let metadata = wal_command_metadata(entry.command.as_ref()).expect("metadata");
            let decoded = SharedStoreWalFrameProto::decode(&frame[4..]).expect("decodes");

            // The terms below are the DEFAULT frame's terms. Said out loud so a flip of the
            // default cannot quietly re-label them: every number in this test belongs to the
            // format named here, and if the default moves the numbers have to move with it.
            assert_eq!(
                decoded.frame_format, WAL_FRAME_FORMAT_NO_SIZE,
                "this test prices the default frame, and the default is now format {}, so the \
                 term figures below are for a different frame than the one being written",
                decoded.frame_format
            );

            // The payload is INSIDE the envelope, not beside it.
            let payload = metadata.byte_size as usize;
            let envelope = frame.len() - payload;

            // CLEAR AND RE-ENCODE: the difference IS the field. WHICH field holds the checksum
            // depends on the format the frame declares, so this asks the frame instead of
            // assuming.
            //
            // It assumed once. While the default moved to the CRC format this probe was still
            // clearing the 32-byte digest field, which that frame does not populate -- so it
            // reported a checksum term of 0 B and the five real bytes were absorbed into the
            // residue, which then read 21 instead of 16. The accounting identity still held,
            // because the terms still summed to the frame; a decomposition can be wrong in every
            // term and still add up. Hence the non-zero assertion below.
            let digest_term = {
                let mut without = decoded.clone();
                match without.frame_format {
                    WAL_FRAME_FORMAT_HEX_DIGEST => without.command_sha256 = String::new(),
                    WAL_FRAME_FORMAT_RAW_DIGEST => without.command_digest = Vec::new(),
                    WAL_FRAME_FORMAT_CRC32C | WAL_FRAME_FORMAT_NO_SIZE => {
                        without.command_crc32c = 0
                    }
                    other => panic!(
                        "this probe does not know which field format {other} keeps its checksum \
                         in, so it cannot price it"
                    ),
                }
                (frame.len() - 4) - without.encoded_len()
            };
            assert!(
                digest_term > 0,
                "clearing the checksum field changed nothing, so this probe cleared a field this \
                 frame does not populate. The term it reports is not the checksum's, and the \
                 bytes are hiding in the residue"
            );
            let format_term = {
                let mut without = decoded.clone();
                without.frame_format = 0;
                (frame.len() - 4) - without.encoded_len()
            };
            let residue = envelope - digest_term - format_term;

            println!(
                "log frame {} B = payload {payload} B + digest {digest_term} B + format \
                 {format_term} B + framing and scalars {residue} B",
                frame.len()
            );

            // Four CRC bytes and a tag -- `fixed32`, so no length byte and no varint. Exact: a
            // tolerance here would admit the regrowth this guard exists to catch.
            assert_eq!(
                digest_term, 5,
                "the frame's digest costs {digest_term} B, not 5. It is four bytes plus a tag; \
                 re-price it before changing this number"
            );
            assert_eq!(
                format_term, 2,
                "the format the frame declares costs {format_term} B, not 2 -- a tag and a \
                 single-byte value"
            );

            // THE ACCOUNTING IDENTITY: the named terms and the residue must be the whole frame,
            // or this is not a decomposition of it.
            assert_eq!(
                payload + digest_term + format_term + residue,
                frame.len(),
                "payload {payload} + digest {digest_term} + format {format_term} + residue \
                 {residue} is not the frame ({} B)",
                frame.len()
            );

            // The residue is the 4-byte length prefix, the payload's own tag and length, and the
            // small scalars. It was 16 B through all three checksum narrowings -- the checksum
            // moved three times and nothing else moved at all -- and is 13 now that the frame no
            // longer restates the payload length it already carries.
            assert_eq!(
                residue, 13,
                "the frame's framing and scalars cost {residue} B, not 13. A field was added to \
                 the frame, or one of the scalars widened"
            );

            // AND THE HEADLINE HAS INVERTED, which is the result of the three narrowings rather
            // than a problem with them. The digest began as 66 B against 16 B of framing; it is
            // now 5 B against 16, so the LARGEST term in this leg is the framing and the scalars.
            //
            // Asserted in the new direction rather than deleted: it is the statement of where the
            // bytes now are, and the thing anyone attacking this leg next needs to know. The old
            // direction is kept as the historical claim in the sentence above it, not as a guard.
            assert!(
                residue > digest_term,
                "the digest is {digest_term} B against {residue} B of framing and scalars. The \
                 digest is the largest term again, which means a narrowing was undone -- check \
                 the default format before anything else"
            );
        }

        /// The frame carries ONE checksum, the read path verifies it, and by default it is now a
        /// four-byte CRC32C rather than a digest of any width.
        ///
        /// What the default frame carries has changed three times, so this test pins all of it on
        /// the WIRE: the CRC is the right CRC, and neither of the two retired digest spellings is
        /// anywhere in the bytes. An unused field left populated would cost its bytes just the
        /// same, so "retired" is asserted as absence rather than as disuse.
        #[test]
        fn the_frame_carries_the_one_digest_the_read_path_verifies() {
            let entry = term_entry(b"a value");
            let frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_DEFAULT)
                .expect("the frame encodes");
            let replayed = decode_wal_proto_frame_exact(&frame, "test").expect("it decodes");
            assert_eq!(replayed.wal_index, TERM_INDEX);

            let decoded = SharedStoreWalFrameProto::decode(&frame[4..]).expect("decodes");
            assert_eq!(
                decoded.frame_format, WAL_FRAME_FORMAT_NO_SIZE,
                "the default frame declares format {}, not the narrowest one",
                decoded.frame_format
            );

            // THE CRC IS THE RIGHT CRC. Recomputed here rather than taken from the encoder, so an
            // encoder that wrote the wrong checksum would fail this.
            assert_eq!(
                decoded.command_crc32c,
                crate::checksum::crc32c(&decoded.command_payload),
                "the frame carries a checksum that is not the CRC32C of its own payload"
            );

            // AND NEITHER RETIRED SPELLING IS ON THE WIRE. Both are checked, because the frame
            // has carried each of them in turn and either one left populated would cost its bytes
            // while nothing read it.
            let as_hex = sha256_hex(&decoded.command_payload);
            let as_bytes = digest_text_to_bytes(&as_hex).expect("a sha256 hex is hex");
            assert!(
                decoded.command_sha256.is_empty() && decoded.command_digest.is_empty(),
                "the crc frame is also populating a digest field, so it is paying for both"
            );
            assert!(
                !frame.windows(as_hex.len()).any(|w| w == as_hex.as_bytes()),
                "the hex transcription is still on the wire in the default frame"
            );
            assert!(
                !frame.windows(as_bytes.len()).any(|w| w == as_bytes.as_slice()),
                "the 32-byte digest is still on the wire in the default frame"
            );

            // And the sidecar still carries no checksum of its own to disagree with this one, in
            // any of the three spellings.
            let sidecar =
                encode_wal_offset_metadata_frame(&offset_metadata(TERM_BLOB_KEY), TERM_BLOB_KEY);
            assert!(
                !sidecar.windows(as_hex.len()).any(|w| w == as_hex.as_bytes()),
                "the sidecar is carrying a copy of the frame's digest again, as hex"
            );
            assert!(
                !sidecar.windows(as_bytes.len()).any(|w| w == as_bytes.as_slice()),
                "the sidecar is carrying a copy of the frame's digest again, as raw bytes"
            );
            assert!(
                !sidecar
                    .windows(4)
                    .any(|w| w == decoded.command_crc32c.to_le_bytes()),
                "the sidecar is carrying a copy of the frame's checksum again, as a crc"
            );
        }

        /// THE LOG FRAME AS IT WAS, for the two questions only an old shape can answer: whether
        /// the wide frame still weighs what it weighed, and whether a reader that predates the
        /// narrow format accepts one.
        ///
        /// Tags 1 through 8, and NOT tags 9 or 10 -- this is the message as it stood before the
        /// format field existed. Asserting from a comment that "prost omits a default" would be
        /// asserting the thing under test.
        #[derive(Clone, PartialEq, Message)]
        struct FrameBeforeTheFormatField {
            #[prost(uint64, tag = "1")]
            shard_id: u64,
            #[prost(uint64, tag = "2")]
            wal_index: u64,
            #[prost(bytes = "vec", tag = "3")]
            command_payload: Vec<u8>,
            #[prost(uint64, tag = "4")]
            command_byte_size: u64,
            #[prost(string, tag = "5")]
            command_sha256: String,
            #[prost(uint32, tag = "6")]
            command_encoding: u32,
            #[prost(message, repeated, tag = "7")]
            staged_blocks: Vec<SharedStoreStagedBlockProto>,
            #[prost(message, repeated, tag = "8")]
            items: Vec<crate::sdk::v1::EngineWalItem>,
        }

        fn term_entry(value: &[u8]) -> SharedStoreWalEntry {
            SharedStoreWalEntry {
                shard_id: TERM_SHARD,
                wal_index: TERM_INDEX,
                command: Some(Command::StringSet {
                    key: "amp-key".to_string(),
                    value: value.to_vec(),
                }),
                staged_blocks: Vec::new(),
                outcomes: Vec::new(),
            }
        }

        /// THE WIDE FORMAT IS STILL BYTE-IDENTICAL TO THE FRAME THAT PREDATES `frame_format`.
        ///
        /// This is what makes the wide format a way BACK. The default is now the narrow shape, so
        /// a cluster that cannot take the narrow format has to be able to ask for the old frame
        /// and get the old frame -- byte for byte, not merely something the same length that an
        /// older reader happens to accept. `with_narrow_wal_frames(false)` is that request and
        /// this is the assertion that it is honoured exactly.
        ///
        /// Pinned to [`WAL_FRAME_FORMAT_HEX_DIGEST`] explicitly rather than to the default: this
        /// test is about the wide shape, and following the default would have made it silently
        /// stop testing anything the moment the default moved.
        #[test]
        fn a_wide_frame_is_byte_identical_to_one_written_before_the_format_field_existed() {
            let entry = term_entry(&[b'v'; 512]);
            let now = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_HEX_DIGEST)
                .expect("the frame encodes");

            // The same content through the message as it stood before tags 9 and 10.
            let payload = match entry.command.as_ref() {
                Some(command) => command_to_sdk_proto(command)
                    .expect("a string set encodes as an sdk command")
                    .encode_to_vec(),
                None => Vec::new(),
            };
            let before = FrameBeforeTheFormatField {
                shard_id: TERM_SHARD,
                wal_index: TERM_INDEX,
                command_byte_size: payload.len() as u64,
                command_sha256: sha256_hex(&payload),
                command_payload: payload,
                command_encoding: WAL_COMMAND_ENCODING_SDK_PROTO,
                staged_blocks: Vec::new(),
                items: Vec::new(),
            };
            let encoded = before.encode_to_vec();
            let mut old = Vec::with_capacity(4 + encoded.len());
            old.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            old.extend_from_slice(&encoded);

            assert_eq!(
                now, old,
                "the default frame is no longer byte-identical to the frame written before the \
                 format field existed, so the two new tags are costing bytes on a path that is \
                 not using them"
            );
        }

        /// WHAT THE NARROW FRAME SAVES, measured on this encoder rather than counted by hand.
        ///
        /// The digest is the same SHA-256 over the same payload; only its transcription changes.
        /// Both figures are printed so the saving is a subtraction anyone can check, and the
        /// payload is held constant so the delta cannot be anything else.
        #[test]
        fn the_narrow_frame_saves_the_transcription_and_nothing_else() {
            let entry = term_entry(&[b'v'; 512]);
            let wide = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_HEX_DIGEST)
                .expect("the wide frame encodes");
            let narrow = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_RAW_DIGEST)
                .expect("the narrow frame encodes");
            let saved = wide.len() - narrow.len();

            println!(
                "log frame {} B wide -> {} B narrow, saving {saved} B per append",
                wide.len(),
                narrow.len()
            );

            // 64 hex characters with a tag and a length byte, against 32 raw bytes with a tag and
            // a length byte, less the two bytes the format field costs: 66 - 34 - 2.
            assert_eq!(
                saved, 30,
                "the narrow frame saves {saved} B, not 30. Re-price the digest term field by \
                 field before changing this number"
            );

            // AND IT IS THE SAME DIGEST. Not a weaker check -- the bytes in the narrow frame must
            // be exactly the hex the wide frame spells out.
            let decoded_narrow = SharedStoreWalFrameProto::decode(&narrow[4..]).expect("decodes");
            let decoded_wide = SharedStoreWalFrameProto::decode(&wide[4..]).expect("decodes");
            assert_eq!(decoded_narrow.frame_format, WAL_FRAME_FORMAT_RAW_DIGEST);
            assert_eq!(decoded_wide.frame_format, WAL_FRAME_FORMAT_HEX_DIGEST);
            assert_eq!(
                hex::encode(&decoded_narrow.command_digest),
                decoded_wide.command_sha256,
                "the narrow frame's digest is not the same digest the wide frame carries"
            );
            assert_eq!(
                decoded_narrow.command_digest.len(),
                32,
                "a SHA-256 is 32 bytes; the narrow frame carries {}",
                decoded_narrow.command_digest.len()
            );
            // The hex text must be ABSENT from the narrow frame's bytes, not merely unused.
            assert!(
                !narrow
                    .windows(decoded_wide.command_sha256.len())
                    .any(|window| window == decoded_wide.command_sha256.as_bytes()),
                "the narrow frame still has the hex transcription on the wire"
            );

            // And both verify through the real reader.
            for (label, frame) in [("wide", &wide), ("narrow", &narrow)] {
                let decoded = decode_wal_proto_frame_exact(frame, "test")
                    .unwrap_or_else(|err| panic!("the {label} frame must decode: {err}"));
                assert_eq!(decoded.wal_index, TERM_INDEX);
                assert_eq!(decoded.command, entry.command);
            }
        }

        /// ONE BLOB HOLDING BOTH SHAPES READS END TO END -- THE ROLLING RESTART.
        ///
        /// This is the case that makes the change risky, so it is the case that is tested. A
        /// cluster part-way through an upgrade has writers on both formats appending to the same
        /// per-shard log, which leaves the two interleaved in one object. A reader that could only
        /// handle a log written entirely in one format would be fine in every test where both
        /// sides are new and would fail exactly here.
        ///
        /// Interleaved in BOTH orders, because a reader that resynchronised on the first format it
        /// saw would pass a file that only ever changes shape once.
        #[test]
        fn one_log_holding_both_frame_shapes_replays_in_either_order() {
            // EVERY SHAPE, interleaved. A log can hold every format that has ever been written to
            // it, and three successive narrowings leave four.
            let formats = [
                WAL_FRAME_FORMAT_HEX_DIGEST,
                WAL_FRAME_FORMAT_RAW_DIGEST,
                WAL_FRAME_FORMAT_CRC32C,
                WAL_FRAME_FORMAT_NO_SIZE,
                WAL_FRAME_FORMAT_HEX_DIGEST,
                WAL_FRAME_FORMAT_NO_SIZE,
                WAL_FRAME_FORMAT_CRC32C,
                WAL_FRAME_FORMAT_RAW_DIGEST,
                WAL_FRAME_FORMAT_NO_SIZE,
                WAL_FRAME_FORMAT_HEX_DIGEST,
            ];
            let mut blob = Vec::new();
            let mut expected = Vec::new();
            for (at, format) in formats.iter().copied().enumerate() {
                let entry = SharedStoreWalEntry {
                    shard_id: TERM_SHARD,
                    wal_index: at as u64 + 1,
                    command: Some(Command::StringSet {
                        key: format!("mixed-{at}"),
                        value: vec![b'm'; at * 37],
                    }),
                    staged_blocks: Vec::new(),
                    outcomes: Vec::new(),
                };
                blob.extend_from_slice(
                    &encode_wal_proto_frame(&entry, format).expect("the frame encodes"),
                );
                expected.push(entry);
            }

            // ASSERT THE MIXTURE IS REALLY A MIXTURE, and that it holds EVERY shape. A fixture
            // missing one would satisfy everything below while testing nothing about it.
            for format in [
                WAL_FRAME_FORMAT_HEX_DIGEST,
                WAL_FRAME_FORMAT_RAW_DIGEST,
                WAL_FRAME_FORMAT_CRC32C,
                WAL_FRAME_FORMAT_NO_SIZE,
            ] {
                assert!(
                    formats.contains(&format),
                    "this fixture does not hold format {format}, so it is not the mixed case for \
                     it"
                );
            }

            let replayed = decode_wal_proto_blob(&blob).expect("a mixed log must replay whole");
            assert_eq!(
                replayed.len(),
                expected.len(),
                "a mixed log replayed {} of {} frames",
                replayed.len(),
                expected.len()
            );
            for (got, want) in replayed.iter().zip(expected.iter()) {
                assert_eq!(got.wal_index, want.wal_index);
                assert_eq!(got.command, want.command, "frame {} came back wrong", got.wal_index);
            }
        }

        /// AN OLD READER REFUSES A NARROW FRAME RATHER THAN ACCEPTING IT.
        ///
        /// This is the property that decides the deploy order, and it is the one a test where both
        /// sides are new cannot see. A reader built before the narrow format verifies tag 5
        /// unconditionally; handed a narrow frame it finds an EMPTY digest where it expects
        /// sixty-four characters.
        ///
        /// What matters is that it REFUSES. An old reader that accepted the frame -- by reading an
        /// absent digest as "nothing to check" -- would replay an unverified command and call it
        /// durable, and the skew would be invisible. So the refusal is asserted, and the wide
        /// frame is run through the same old reader as the control: without it, a reader that
        /// refused everything would pass.
        #[test]
        fn an_old_reader_refuses_a_narrow_frame_rather_than_accepting_it() {
            /// Exactly what the old read path did: decode, then compare tag 5 against the hex
            /// digest of the payload.
            fn old_reader_accepts(frame: &[u8]) -> bool {
                let Ok(decoded) = FrameBeforeTheFormatField::decode(&frame[4..]) else {
                    return false;
                };
                decoded.command_byte_size == decoded.command_payload.len() as u64
                    && decoded.command_sha256 == sha256_hex(&decoded.command_payload)
            }

            let entry = term_entry(b"a value");
            let wide = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_HEX_DIGEST)
                .expect("the wide frame encodes");
            let narrow = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_RAW_DIGEST)
                .expect("the narrow frame encodes");

            // THE CONTROL, first: the old reader still accepts what this build writes by default.
            // If this failed, the test below would be measuring a broken reader.
            assert!(
                old_reader_accepts(&wide),
                "a reader that predates the format field must still accept the frame this build \
                 writes by default, or the default is not backward compatible"
            );

            // THE PROPERTY: it refuses the narrow one.
            assert!(
                !old_reader_accepts(&narrow),
                "a reader that predates the narrow format ACCEPTED one. It therefore replayed a \
                 command whose digest it never checked, and a mis-ordered deploy would be silent \
                 rather than loud"
            );

            // And the current reader accepts both, which is what makes reader-first ordering work.
            assert!(decode_wal_proto_frame_exact(&wide, "test").is_ok());
            assert!(decode_wal_proto_frame_exact(&narrow, "test").is_ok());
        }

        /// A FORMAT THIS BUILD DOES NOT KNOW IS REFUSED BY NAME, not reported as corruption.
        ///
        /// The failure mode this prevents: a future format carries its digest in a field this
        /// build cannot see, so verifying the field that happens to be empty yields a checksum
        /// mismatch -- which blames the data for a version skew and sends whoever reads the error
        /// looking for a damaged log. The error must say the FORMAT is unsupported.
        #[test]
        fn a_frame_from_a_future_format_is_refused_by_name() {
            let entry = term_entry(b"a value");
            let mut frame = SharedStoreWalFrameProto::decode(
                &encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_RAW_DIGEST)
                    .expect("the frame encodes")[4..],
            )
            .expect("decodes");
            frame.frame_format = 77;
            let encoded = frame.encode_to_vec();
            let mut blob = Vec::with_capacity(4 + encoded.len());
            blob.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            blob.extend_from_slice(&encoded);

            let err = decode_wal_proto_frame_exact(&blob, "a-log-key")
                .expect_err("an unknown format must be refused");
            let message = err.to_string();
            assert!(
                message.contains("77") && message.contains("frame format"),
                "the error must name the unsupported format; it said: {message}"
            );
            assert!(
                !matches!(err, SharedStoreReplicationError::ChecksumMismatch { .. }),
                "an unknown format was reported as a checksum mismatch, which blames the data \
                 for a version skew"
            );
        }

        /// WHAT THE CRC FORMAT SAVES, and that it is still a check.
        ///
        /// The digest term goes from 34 B (32 bytes, a tag and a length) to 5 B (four bytes and a
        /// tag), so the frame loses 29 B more. The saving is the point, but the check surviving is
        /// the condition: a corrupted payload must still be refused, so that is asserted here
        /// rather than left to the format's reputation.
        #[test]
        fn the_crc_frame_is_narrower_still_and_still_refuses_a_corrupted_payload() {
            let entry = term_entry(&[b'v'; 512]);
            let raw = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_RAW_DIGEST)
                .expect("the raw-digest frame encodes");
            let crc = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_CRC32C)
                .expect("the crc frame encodes");
            let saved = raw.len() - crc.len();

            println!(
                "log frame {} B with a 32-byte digest -> {} B with a CRC32C, saving {saved} B \
                 more per append",
                raw.len(),
                crc.len()
            );

            assert_eq!(
                saved, 29,
                "the crc frame saves {saved} B against the raw-digest one, not 29. The digest \
                 term is 34 B against 5 B; re-price it before changing this number"
            );

            // IT IS STILL A CHECK. The frame round-trips, and a payload altered underneath a
            // frame that still declares the old checksum is REFUSED -- which is the only property
            // that makes a narrower checksum a saving rather than a removal.
            let replayed = decode_wal_proto_frame_exact(&crc, "test").expect("the crc frame decodes");
            assert_eq!(replayed.command, entry.command);

            let mut decoded = SharedStoreWalFrameProto::decode(&crc[4..]).expect("decodes");
            assert_eq!(decoded.frame_format, WAL_FRAME_FORMAT_CRC32C);
            assert!(
                decoded.command_sha256.is_empty() && decoded.command_digest.is_empty(),
                "the crc frame is also carrying a SHA-256, so it is paying for both"
            );
            // Flip one byte of the payload, leaving the declared length and checksum alone.
            let flip_at = decoded.command_payload.len() / 2;
            decoded.command_payload[flip_at] ^= 0x01;
            let tampered = {
                let body = decoded.encode_to_vec();
                let mut out = Vec::with_capacity(4 + body.len());
                out.extend_from_slice(&(body.len() as u32).to_be_bytes());
                out.extend_from_slice(&body);
                out
            };
            let err = decode_wal_proto_frame_exact(&tampered, "a-log-key")
                .expect_err("a payload that no longer matches its checksum must be refused");
            let message = err.to_string();
            assert!(
                message.contains("crc32c:"),
                "the mismatch must name the algorithm it checked, or the error is ambiguous now \
                 that more than one shape can reach it; it said: {message}"
            );
        }

        /// WHAT DROPPING THE DECLARED PAYLOAD LENGTH SAVES.
        #[test]
        fn the_frame_without_a_declared_size_is_narrower_still() {
            let entry = term_entry(&[b'v'; 512]);
            let with_size = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_CRC32C)
                .expect("the crc frame encodes");
            let without = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_NO_SIZE)
                .expect("the sizeless frame encodes");
            let saved = with_size.len() - without.len();

            println!(
                "log frame {} B declaring its payload length -> {} B without it, saving {saved} B",
                with_size.len(),
                without.len()
            );

            // A tag and a two-byte varint for a 527-byte payload.
            assert_eq!(
                saved, 3,
                "dropping the declared payload length saves {saved} B, not 3"
            );

            // THE FIELD IS ABSENT, not merely zero-by-accident: a reader must see nothing there.
            let decoded = SharedStoreWalFrameProto::decode(&without[4..]).expect("decodes");
            assert_eq!(decoded.frame_format, WAL_FRAME_FORMAT_NO_SIZE);
            assert_eq!(
                decoded.command_byte_size, 0,
                "the sizeless frame still declares a payload length of {}",
                decoded.command_byte_size
            );
            // And the payload still arrives whole, which is the thing the length was describing.
            let replayed = decode_wal_proto_frame_exact(&without, "test").expect("it decodes");
            assert_eq!(replayed.command, entry.command);
        }

        /// A TRUNCATED PAYLOAD IS STILL REFUSED WITHOUT THE DECLARED SIZE.
        ///
        /// This is the condition on dropping `command_byte_size` rather than a nicety. Removing it
        /// makes the decoder's size comparison `payload.len()` against itself, so something else
        /// has to catch a payload that is not all there. Two things do, and both are exercised
        /// here on frames that declare NO size:
        ///
        /// 1. a single flipped byte inside the payload, which the command still parses, fails the
        ///    CRC and nothing else;
        /// 2. a payload cut short with its delimiters rewritten consistently -- a frame that is
        ///    structurally perfect and simply holds less than it was given -- fails the CRC;
        /// 3. a frame cut short at the tail fails the outer length-prefix check before the
        ///    protobuf is parsed at all.
        ///
        /// ARM 1 IS FIRST ON PURPOSE. It is the only arm with no second line of defence, so a
        /// plant that disables the checksum fails there. Arm 2 is doubled -- the payload is an
        /// encoded command, so halving it breaks the inner protobuf as well, and with the CRC
        /// disabled arm 2 fails as a buffer underflow rather than as a checksum mismatch. That
        /// makes arm 2 a test of ORDERING (the checksum is consulted before the command is
        /// parsed), which is worth having, and arm 1 a test of the checksum itself. Ordered the
        /// other way round the plant would stop at arm 2 and never reach the arm that isolates.
        ///
        /// The CONTROL matters as much: an untouched sizeless frame must still decode, or a
        /// decoder that refused everything would satisfy every arm above.
        #[test]
        fn a_truncated_payload_is_still_refused_without_the_declared_size() {
            let entry = term_entry(&[b'v'; 512]);
            let frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_NO_SIZE)
                .expect("the sizeless frame encodes");

            // THE CONTROL, first.
            assert!(
                decode_wal_proto_frame_exact(&frame, "test").is_ok(),
                "an untouched sizeless frame must decode, or the refusals below prove nothing"
            );

            // (1) THE ARM WITH NO SECOND LINE OF DEFENCE, FIRST, so that a plant disabling the
            // checksum fails HERE -- on the case only the checksum can catch -- rather than on
            // one of the arms below that another layer would also have caught.
            //
            // A single flipped byte inside the value. The command still decodes, into a DIFFERENT
            // value, so nothing downstream has any reason to object: the checksum is the only
            // thing between this frame and a replay that applies the wrong bytes and calls them
            // durable.
            let mut flipped = SharedStoreWalFrameProto::decode(&frame[4..]).expect("decodes");
            let at = flipped.command_payload.len() - 1;
            flipped.command_payload[at] ^= 0x01;
            let flipped_frame = {
                let body = flipped.encode_to_vec();
                let mut out = Vec::with_capacity(4 + body.len());
                out.extend_from_slice(&(body.len() as u32).to_be_bytes());
                out.extend_from_slice(&body);
                out
            };

            // The flip must leave a payload that still DECODES, or this arm is arm (2) again.
            assert!(
                v1::Command::decode(flipped.command_payload.as_slice()).is_ok(),
                "the flipped payload no longer decodes, so this arm is testing the protobuf \
                 parser rather than the checksum -- move the flip to a byte inside the value"
            );

            let err = decode_wal_proto_frame_exact(&flipped_frame, "a-log-key").expect_err(
                "a single flipped byte in the payload must be refused: the command still parses, \
                 so nothing but the checksum can catch it",
            );
            assert!(
                matches!(err, SharedStoreReplicationError::ChecksumMismatch { .. }),
                "a value-preserving flip must fail the CHECKSUM; it failed as: {err}"
            );

            // (2) STRUCTURALLY PERFECT, PAYLOAD SHORT. Re-encode with a payload cut in half and
            // the ORIGINAL checksum left in place: every length on the wire agrees with every
            // other, and the only thing wrong is that bytes are missing.
            let mut shortened = SharedStoreWalFrameProto::decode(&frame[4..]).expect("decodes");
            let full_len = shortened.command_payload.len();
            shortened.command_payload.truncate(full_len / 2);
            assert_eq!(
                shortened.command_byte_size, 0,
                "this arm must run on a frame that declares no size, or it is testing the old \
                 check rather than what replaced it"
            );
            let reframed = {
                let body = shortened.encode_to_vec();
                let mut out = Vec::with_capacity(4 + body.len());
                out.extend_from_slice(&(body.len() as u32).to_be_bytes());
                out.extend_from_slice(&body);
                out
            };
            let err = decode_wal_proto_frame_exact(&reframed, "a-log-key").expect_err(
                "a payload cut short must be refused even though nothing on the wire declares \
                 how long it should have been",
            );
            assert!(
                matches!(err, SharedStoreReplicationError::ChecksumMismatch { .. }),
                "a short payload must fail the CHECKSUM, which is what now stands in for the \
                 declared length; it failed as: {err}"
            );

            // (3) CUT AT THE TAIL. The outer length prefix still claims the whole frame.
            for cut in [1usize, 17, full_len / 2, frame.len() - 4] {
                let torn = &frame[..frame.len() - cut];
                assert!(
                    decode_wal_proto_frame_exact(torn, "a-log-key").is_err(),
                    "a frame cut {cut} bytes short of what its length prefix declares must be \
                     refused"
                );
            }
        }

        /// A READER THAT KNOWS THE RAW-DIGEST FORMAT BUT NOT THE CRC ONE REFUSES A CRC FRAME.
        ///
        /// This is why the CRC format cannot ship as a default in the same release that teaches
        /// readers about it: the reader built one release ago understands formats 0 and 2, and a
        /// format 3 frame is unreadable to it. Same shape as the hazard formats 0 and 2 had, and
        /// it is tested the same way -- including the control, so a reader that refused
        /// everything could not pass for one that refuses the right thing.
        ///
        /// It refuses BY NAMING THE FORMAT, not as a checksum mismatch, which is the difference
        /// between an operator reading "this node is too old for this log" and "this log is
        /// corrupt".
        #[test]
        fn a_reader_without_the_crc_format_refuses_a_crc_frame_by_name() {
            /// The decoder as it stood before format 3: formats 0 and 2 understood, anything else
            /// refused by name.
            fn reader_before_crc(frame: &[u8]) -> Result<(), String> {
                let decoded = SharedStoreWalFrameProto::decode(&frame[4..])
                    .map_err(|err| err.to_string())?;
                match decoded.frame_format {
                    WAL_FRAME_FORMAT_HEX_DIGEST => {
                        if decoded.command_sha256 == sha256_hex(&decoded.command_payload) {
                            Ok(())
                        } else {
                            Err("checksum mismatch".to_string())
                        }
                    }
                    WAL_FRAME_FORMAT_RAW_DIGEST => {
                        if hex::encode(&decoded.command_digest)
                            == sha256_hex(&decoded.command_payload)
                        {
                            Ok(())
                        } else {
                            Err("checksum mismatch".to_string())
                        }
                    }
                    other => Err(format!("unsupported frame format {other}")),
                }
            }

            let entry = term_entry(b"a value");
            let raw = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_RAW_DIGEST)
                .expect("encodes");
            let crc = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_CRC32C).expect("encodes");

            // THE CONTROL: that reader accepts the formats it DOES know, so a reader that refused
            // everything could not pass for one that refuses the right thing. Note that the
            // default is no longer among them -- which is the point of the property below.
            assert!(
                reader_before_crc(&raw).is_ok(),
                "a reader from before the crc format must still accept a raw-digest frame, or \
                 this control is testing a broken reader rather than a format boundary"
            );

            // THE PROPERTY: it refuses the crc frame, AND it refuses what this build now writes by
            // default. This is the deploy requirement in test form -- the release that writes a
            // format must be the release that reads it, because the build before it does not.
            let err = reader_before_crc(&crc)
                .expect_err("a reader without the crc format must refuse a crc frame");
            assert!(
                err.contains("unsupported frame format 3"),
                "the refusal must name the format rather than blame the data; it said: {err}"
            );

            let default_frame = encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_DEFAULT)
                .expect("encodes");
            let err = reader_before_crc(&default_frame).expect_err(
                "a reader from before these formats must refuse what this build writes by default",
            );
            assert!(
                err.contains(&format!("unsupported frame format {WAL_FRAME_FORMAT_DEFAULT}")),
                "the refusal must name the default format; it said: {err}"
            );

            // And this build reads every one of them, which is what makes reader-first ordering
            // work at all.
            for (label, frame) in [
                ("hex", encode_wal_proto_frame(&entry, WAL_FRAME_FORMAT_HEX_DIGEST).unwrap()),
                ("raw", raw),
                ("crc", crc),
                ("sizeless", default_frame),
            ] {
                assert!(
                    decode_wal_proto_frame_exact(&frame, "test").is_ok(),
                    "this build must read the {label} frame"
                );
            }
        }

        /// THE DEFAULT REACHES THE OBJECT STORE, and the way back does too.
        ///
        /// Every other test here calls the encoder directly, which prices a frame but cannot say
        /// what a WRITER emits: the default constant could be flipped while the append path kept
        /// passing something else, and the whole saving would be a number in a test. So this one
        /// publishes through the replicator and reads the format out of the bytes that reached the
        /// store.
        ///
        /// Both directions, because the opt-out is what a cluster that cannot take the narrow
        /// format depends on. A test of the default alone would pass just as well if
        /// `with_narrow_wal_frames(false)` did nothing at all.
        #[tokio::test]
        async fn the_published_frame_carries_the_default_format_and_the_opt_out_reverses_it() {
            async fn published_format(dir: &Path, narrow: Option<bool>) -> (u32, usize) {
                let store = Arc::new(FileObjectStore::new(dir.join("objects")));
                let mut replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone())
                    .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
                if let Some(narrow) = narrow {
                    replicator = replicator.with_narrow_wal_frames(narrow);
                }
                replicator
                    .publish_wal_entry(SharedStoreWalEntry {
                        shard_id: TERM_SHARD,
                        wal_index: TERM_INDEX,
                        command: Some(Command::StringSet {
                            key: "amp-key".to_string(),
                            value: vec![b'v'; 512],
                        }),
                        staged_blocks: Vec::new(),
                        outcomes: Vec::new(),
                    })
                    .await
                    .expect("the append reaches the store");

                let key = format!(
                    "{TEST_CLUSTER_ID}/shards/{TERM_SHARD}/shared/wal/wal.protobuf.blob"
                );
                let bytes = store.get(&key).await.expect("the log object is there");
                // The object holds one length-prefixed frame.
                let len = u32::from_be_bytes(
                    bytes[0..4].try_into().expect("a 4-byte length prefix"),
                ) as usize;
                assert_eq!(
                    len + 4,
                    bytes.len(),
                    "the log object is not exactly one frame, so the format read below may not \
                     be the frame's"
                );
                let frame = SharedStoreWalFrameProto::decode(&bytes[4..]).expect("decodes");
                (frame.frame_format, bytes.len())
            }

            let dir = tempfile::tempdir().unwrap();
            let (default_format, default_bytes) =
                published_format(&dir.path().join("default"), None).await;
            let (narrow_format, narrow_bytes) =
                published_format(&dir.path().join("narrow"), Some(true)).await;
            let (wide_format, wide_bytes) =
                published_format(&dir.path().join("wide"), Some(false)).await;

            println!(
                "published log object: default {default_bytes} B (format {default_format}), \
                 narrow {narrow_bytes} B (format {narrow_format}), wide {wide_bytes} B (format \
                 {wide_format})"
            );

            assert_eq!(
                default_format, WAL_FRAME_FORMAT_NO_SIZE,
                "a published frame declares format {default_format}, so the flipped default is \
                 not reaching the append path"
            );
            assert_eq!(
                narrow_format, WAL_FRAME_FORMAT_NO_SIZE,
                "asking for narrow frames did not produce the narrowest this build writes"
            );
            assert_eq!(
                wide_format, WAL_FRAME_FORMAT_HEX_DIGEST,
                "the opt-out did not produce the wide frame, so a cluster that needs the old \
                 shape has no way back"
            );

            // AND THE SAVING IS IN THE BYTES THAT REACHED THE STORE, not only in the encoder.
            // 66 B of hex digest and 3 B of restated payload length, against 5 B of CRC and 2 B
            // of declared format: 62.
            assert_eq!(default_bytes, narrow_bytes);
            assert_eq!(
                wide_bytes - narrow_bytes,
                62,
                "the published objects differ by {} B, not the 62 B the narrowings save together",
                wide_bytes - narrow_bytes
            );
        }
    }


    const TEST_CLUSTER_ID: &str = "cluster-a";
    const TEST_CACHE_BYTES: usize = 1024;

    fn test_engine(root: &Path, role: &str) -> TemporalEngine {
        test_engine_with_cache(root, role, TEST_CACHE_BYTES)
    }

    fn test_engine_with_cache(root: &Path, role: &str, cache_bytes: usize) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            cache_bytes,
            root.join(format!("{role}-cache")),
            root.join(format!("{role}-blocks")),
            root.join(format!("{role}-index")),
        )
    }

    fn test_shared_store(
        root: &Path,
    ) -> (Arc<FileObjectStore>, SharedStoreReplicator<FileObjectStore>) {
        let store = Arc::new(FileObjectStore::new(root.join("objects")));
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::JsonPerKey);
        (store, replicator)
    }

    #[tokio::test]
    async fn shared_store_restores_index_blocks_and_replays_later_wal() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_block_slabs(1, &primary.block_store())
            .await
            .unwrap();
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
