// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
    Condvar,
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex, RwLock,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::http::{
    json_response, parse_json, post_json_with_options, HttpRequest, HttpRequestOptions,
};
use crate::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, DeleteTableRequest, GetShardResponse,
    GetTableTopologyRequest, ListNamespacesResponse, ListProxiesResponse, ListServersResponse,
    MetaMetricsReport, StateTally,
    ListShardsResponse,
    ListTablesResponse, LoadFinishRequest, MetaEntityState, MetaInfo, MetaMutation,
    MetaPreflightReport, MetaSnapshot, MetaStats, ProxyHeartbeatRequest, ProxyHeartbeatResponse,
    PublishShardSnapshotRequest, RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest,
    RegisterShardResponse, SafeModePolicy, SafeModeReport, ServerHeartbeatRequest,
    ServerHeartbeatResponse, ShardLocation, ShardSnapshotRef, SingleNodeMeta, StaleResourceReport,
    StaleServerReport, StateChangeRequest, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest, UpdateTableRequest,
};
use crate::rebalance::RaftPersistedSchedulerState;
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId, Status};

#[path = "raft/raft_matrixraft.rs"]
mod matrixraft;
#[path = "raft/raft_membership.rs"]
mod membership;
mod readiness;
mod cluster_snapshot;
mod production_runtime;
mod local_wal;
pub(crate) mod follower_pipeline;
pub(crate) mod wal_proto;
mod cluster_meta;
mod cluster_meta_inner;
mod cluster_inner;
mod cluster_inner_admin_report;
mod cluster_election;
mod cluster_membership;
mod cluster_replication;
mod cluster_read_status;
use bytes::Bytes;
pub use matrixraft::*;
pub use membership::*;
pub use readiness::*;
pub use production_runtime::*;
use temporalstore_snapshot::{ObjectStore, S3SnapshotStore, SnapshotRef, SnapshotStore};

pub type RaftNodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftReplicaRole {
    Voter,
    Learner,
    Witness,
}

impl RaftReplicaRole {
    fn participates_in_quorum(self) -> bool {
        matches!(self, Self::Voter | Self::Witness)
    }

    fn can_serve_data(self) -> bool {
        matches!(self, Self::Voter | Self::Learner)
    }

    fn can_be_leader(self) -> bool {
        matches!(self, Self::Voter)
    }
}

impl Default for RaftReplicaRole {
    fn default() -> Self {
        Self::Voter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub shard_id: ShardId,
    pub command: Command,
    /// The wall-clock instant, in milliseconds, at which the LEADER admitted this command.
    ///
    /// A relative deadline (SETEX, EXPIRE) is resolved to an absolute instant while the command
    /// executes, and on this path that execution happens once per replica, at whatever moment that
    /// replica applied. Resolving against the applying node's own clock therefore gives ONE
    /// committed entry as many different absolute deadlines as there are replicas, drifting by
    /// exactly how late each one applied -- a follower that was down for a minute stores a
    /// deadline a minute later than the leader's for the same key.
    ///
    /// Stamped once here, by the node that admits the write, and read back by the apply so every
    /// replica reproduces the leader's instant instead of minting its own. This is the same
    /// resolve-then-log rule the engine WAL already follows on recovery, where the record's
    /// metadata timestamp drives the replay clock.
    ///
    /// Zero means unstamped -- a record written before this field existed, or a synthetic entry --
    /// and the apply then falls back to the live clock exactly as it did before.
    #[serde(default)]
    pub leader_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftSnapshot {
    pub shard_id: ShardId,
    pub last_included_term: u64,
    pub last_included_index: u64,
    #[serde(default)]
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    pub entries: Vec<RaftLogEntry>,
    /// S2: opaque engine state image captured at `last_included_index`. When present, install
    /// reconstructs state from this image (O(state)) instead of replaying `entries`, and `entries`
    /// is empty. Absent (default) on the classic entry-carrying snapshot, so older peers and the
    /// gate-off path deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_image: Option<RaftSnapshotStateImage>,
    /// True when the image lives in its own file beside the log rather than inside the record.
    /// A record is written per persist and the image is large; embedding it re-serialized the
    /// whole image into every record that followed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub state_image_externalized: bool,
}

/// S2: an opaque engine STATE IMAGE — the exported served index plus every referenced page slab —
/// that reconstructs a shard's applied state without replaying the committed log. Mirrors the
/// metadata-checkpoint the shared-store recovery path builds (index bytes + slab bytes +
/// next-page-id), letting a snapshot install run in O(state) rather than O(total history).
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotStateImage {
    pub index_bytes: Vec<u8>,
    pub next_block_id: u64,
    pub slabs: Vec<RaftSnapshotStateImageSlab>,
    /// THE ROUTING RANGE THE CARRIED INDEX WAS BUILT ON.
    ///
    /// An index names buckets, and a bucket is `start + hash(key) % (end - start + 1)`. Ship the
    /// index without the range and the receiver has bucket numbers it cannot account for: it either
    /// guesses -- which files every later write in buckets the installed blocks are not in, readable
    /// and outside every per-bucket sweep -- or it refuses. This field is what lets it do neither.
    ///
    /// `Option` ON THE WIRE AND REQUIRED IN PRACTICE. An image encoded before this field existed
    /// decodes with `None`, and the install REFUSES it by name rather than failing to parse, so the
    /// operator is told which shape arrived instead of reading a decode error. There is no second
    /// reader and no migration: one shape, current or refused.
    #[serde(default)]
    pub routing_range: Option<crate::engine::routing_range_stamp::RoutingRangeStamp>,
}

impl RaftSnapshotStateImage {
    /// Index bytes plus every slab's bytes -- one copy of the shard, which is what this type
    /// costs to clone and what it costs to hold.
    pub fn payload_bytes(&self) -> usize {
        self.index_bytes.len() + self.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>()
    }
}

/// Hand-written so the copy can be CHARGED where it happens.
///
/// This type is the whole shard. Every other cost on the snapshot path is bounded by the corpus
/// once; a clone of this is bounded by the corpus again, and the derived `Clone` gave no way to
/// tell one apart from the other at any call site. Charging it inside the impl means a new caller
/// of `.clone()` anywhere in the tree is counted without anyone maintaining a list of callers.
///
/// Field-for-field equivalent to the derive it replaces -- `snapshot_cost` asserts a clone equals
/// its source, which is what a hand-written `Clone` needs a test for.
impl Clone for RaftSnapshotStateImage {
    fn clone(&self) -> Self {
        #[cfg(test)]
        crate::snapshot_probe::note_image_clone(self.payload_bytes() as u64);
        Self {
            index_bytes: self.index_bytes.clone(),
            next_block_id: self.next_block_id,
            slabs: self.slabs.clone(),
            // Field-for-field with the derive, which is what this impl's doc promises and what
            // `snapshot_cost` checks by comparing a clone against its source.
            routing_range: self.routing_range,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotStateImageSlab {
    pub block_slab_id: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTriggerReport {
    pub triggered: bool,
    pub reason: String,
    pub leader_id: RaftNodeId,
    pub applied_index: u64,
    pub last_snapshot_index: u64,
    pub applied_log_bytes: u64,
    pub max_applied_log_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftApplySnapshotFence {
    pub applied_index: u64,
    pub commit_index: u64,
    pub installed_snapshot_index: u64,
    pub first_retained_log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftStorageApplyFence {
    pub shard_id: ShardId,
    pub raft_term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub storage_epoch: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDataNodeAtomicDurabilityReport {
    pub node_id: RaftNodeId,
    pub shard_id: ShardId,
    pub commit_index: u64,
    pub applied_index: u64,
    pub wal_commit_index: u64,
    pub fence_committed_index: u64,
    pub fence_applied_index: u64,
    pub storage_epoch: u64,
    pub snapshot_id: Option<String>,
    pub storage_apply_fence_valid: bool,
    pub storage_mutation_atomic_commit_present: bool,
    pub snapshot_install_atomic_commit_present: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

pub const DATA_RAFT_LOG_MAGIC: u32 = 0x5453_5246; // "TSRF"
pub const DATA_RAFT_COMMAND_MAGIC: u32 = 0x5453_5243; // "TSRC"
pub const DATA_RAFT_CODEC_VERSION: u32 = 1;

const DATA_RAFT_LOG_HEADER_LEN: usize = 56;
const DATA_RAFT_COMMAND_HEADER_LEN: usize = 40;

// S2: snapshot the engine STATE IMAGE (exported index + page slabs) instead of every committed
// log entry. `create_snapshot` captures an opaque image at the leader's applied index and drops
// the replayable entries, so a far-behind follower installs in O(state) rather than replaying
// O(total history); install reconstructs state from the image.
//
// `TS_RAFT_SNAPSHOT_STATE_IMAGE` used to select entry-carrying snapshots instead, for every
// cluster in the process. It is `RaftClusterInner::snapshot_state_image` now, and only a test
// about entry chunking asks for entries, of the cluster it built.

// P1 (fsync coalescing): skip a node's WAL fdatasync when none of its DURABILITY-relevant state
// changed since the last persist. Driven purely by whether hard_state / log / membership /
// snapshot / fences changed, so it can never skip a persist that Raft safety requires -- only the
// volatile `pipeline_state` + `read_safety_state` (match/next index, inflight/queue depths,
// read-index accounting counters) are excluded from the change check.
//
// `TS_RAFT_WAL_COALESCE` used to decide this for every cluster in the process. It is
// `RaftClusterInner::wal_coalesce` now, and only a test measuring what the skip saves turns it
// off, of the one cluster it built.

/// Fingerprint of the DURABILITY-relevant subset of a WAL record. `pipeline_state` and
/// `read_safety_state` are cleared before hashing because they are volatile (reinitialised on
/// election / re-driven on restart) and pure metrics respectively -- excluding them is what lets
/// the read-index/tick/match-index storm coalesce to zero fsyncs while any change to term,
/// voted_for, commit_index, the log, membership, snapshots, or the apply/storage fences still
/// flips the fingerprint and forces a durable persist.
fn raft_durable_fingerprint(record: &RaftWalRecord) -> u64 {
    // Everything Raft must persist participates, but the log is SUMMARISED rather than
    // serialised. Hashing every entry made this check -- which runs on every persist, for
    // every node -- cost O(log length), the very cost incremental WAL records exist to
    // remove. A raft log only ever grows at the tail or has a suffix replaced, so its
    // length together with the first and last (index, term) changes whenever it does.
    let reduced = RaftWalRecord {
        hard_state: record.hard_state.clone(),
        membership: record.membership.clone(),
        replica_role: record.replica_role,
        joint_membership: record.joint_membership.clone(),
        latest_external_snapshot_ref: record.latest_external_snapshot_ref.clone(),
        installed_snapshot: record.installed_snapshot.clone(),
        apply_snapshot_fence: record.apply_snapshot_fence.clone(),
        storage_apply_fence: record.storage_apply_fence.clone(),
        pipeline_state: RaftPeerPipelineRuntimeState::default(),
        read_safety_state: RaftReadSafetyRuntimeState::default(),
        membership_evidence: record.membership_evidence.clone(),
        entries: Vec::new(),
    };
    let bytes = serde_json::to_vec(&reduced).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hasher::write(&mut hasher, &bytes);
    std::hash::Hasher::write_u64(&mut hasher, record.entries.len() as u64);
    for entry in [record.entries.first(), record.entries.last()].into_iter().flatten() {
        std::hash::Hasher::write_u64(&mut hasher, entry.index);
        std::hash::Hasher::write_u64(&mut hasher, entry.term);
    }
    std::hash::Hasher::finish(&hasher)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataRaftLogCodecEntry {
    pub shard_id: ShardId,
    pub raft_index: u64,
    pub log_id: u64,
    pub log_size: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataRaftCommandCodecEntry {
    pub shard_id: ShardId,
    pub raft_index: u64,
    pub request_id: u64,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftPeer {
    pub replica_id: RaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftConsensusOptions {
    pub shard_id: ShardId,
    pub replica_id: RaftNodeId,
    pub group_id: u64,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub wal_dir: Option<PathBuf>,
    pub snapshot_dir: Option<PathBuf>,
    pub wal_sync: bool,
    pub bootstrap_as_learner: bool,
    pub peers: Vec<DataRaftPeer>,
    pub initial_applied_index: u64,
}

impl Default for DataRaftConsensusOptions {
    fn default() -> Self {
        Self {
            shard_id: 0,
            replica_id: 0,
            group_id: 0,
            raft_addr: String::new(),
            snapshot_addr: String::new(),
            wal_dir: None,
            snapshot_dir: None,
            wal_sync: true,
            bootstrap_as_learner: false,
            peers: Vec::new(),
            initial_applied_index: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftStatus {
    pub running: bool,
    pub leader: bool,
    pub learner: bool,
    pub term: u64,
    pub leader_replica_id: RaftNodeId,
    pub committed_index: u64,
    pub applied_index: u64,
    pub first_index: u64,
    pub last_index: u64,
    pub pending_config_change_index: u64,
    pub voter_count: u64,
    pub learner_count: u64,
    pub fatal_event_count: u64,
    pub snapshot_creating: bool,
    pub snapshot_loading: bool,
}

pub trait DataRaftConsensusBackend {
    fn start(&mut self) -> Result<(), RaftError>;
    fn stop(&mut self);
    fn is_leader(&self) -> bool;
    fn status(&self) -> Result<DataRaftStatus, RaftError>;
    fn propose(&mut self, serialized_entry: Vec<u8>) -> Result<u64, RaftError>;
    fn wait_for_applied_index(&self, index: u64, timeout_ms: u64) -> Result<(), RaftError>;
    fn trigger_snapshot(&mut self) -> Result<u64, RaftError>;
    fn read_index(&self, timeout_ms: u64) -> Result<(), RaftError>;
    fn add_peer(&mut self, peer: DataRaftPeer) -> Result<(), RaftError>;
    fn add_learner(&mut self, peer: DataRaftPeer) -> Result<(), RaftError>;
    fn promote_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn remove_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn transfer_leader(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn campaign(&mut self, timeout_ms: u64, force: bool) -> Result<(), RaftError>;
    fn can_serve_bounded_stale_read(&self, max_stale_index_lag: u64) -> Result<(), RaftError>;
}

#[derive(Debug, Clone)]
pub struct UnavailableDataRaftConsensusBackend {
    options: DataRaftConsensusOptions,
}

impl UnavailableDataRaftConsensusBackend {
    pub fn new(options: DataRaftConsensusOptions) -> Self {
        Self { options }
    }

    fn unavailable(operation: &'static str) -> RaftError {
        RaftError::Transport(format!("data raft backend unavailable: {operation}"))
    }
}

impl DataRaftConsensusBackend for UnavailableDataRaftConsensusBackend {
    fn start(&mut self) -> Result<(), RaftError> {
        Err(Self::unavailable("start"))
    }

    fn stop(&mut self) {}

    fn is_leader(&self) -> bool {
        false
    }

    fn status(&self) -> Result<DataRaftStatus, RaftError> {
        Ok(DataRaftStatus {
            running: false,
            leader: false,
            learner: false,
            term: 0,
            leader_replica_id: 0,
            committed_index: 0,
            applied_index: self.options.initial_applied_index,
            first_index: 0,
            last_index: 0,
            pending_config_change_index: 0,
            voter_count: self.options.peers.len() as u64,
            learner_count: 0,
            fatal_event_count: 0,
            snapshot_creating: false,
            snapshot_loading: false,
        })
    }

    fn propose(&mut self, _serialized_entry: Vec<u8>) -> Result<u64, RaftError> {
        Err(Self::unavailable("propose"))
    }

    fn wait_for_applied_index(&self, _index: u64, _timeout_ms: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("wait_for_applied_index"))
    }

    fn trigger_snapshot(&mut self) -> Result<u64, RaftError> {
        Err(Self::unavailable("trigger_snapshot"))
    }

    fn read_index(&self, _timeout_ms: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("read_index"))
    }

    fn add_peer(&mut self, _peer: DataRaftPeer) -> Result<(), RaftError> {
        Err(Self::unavailable("add_peer"))
    }

    fn add_learner(&mut self, _peer: DataRaftPeer) -> Result<(), RaftError> {
        Err(Self::unavailable("add_learner"))
    }

    fn promote_peer(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("promote_peer"))
    }

    fn remove_peer(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("remove_peer"))
    }

    fn transfer_leader(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("transfer_leader"))
    }

    fn campaign(&mut self, _timeout_ms: u64, _force: bool) -> Result<(), RaftError> {
        Err(Self::unavailable("campaign"))
    }

    fn can_serve_bounded_stale_read(&self, _max_stale_index_lag: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("can_serve_bounded_stale_read"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataRaftCommittedLogApplier {
    shard_id: ShardId,
    applied_raft_index: u64,
    applied_wal_sequence: u64,
}

impl DataRaftCommittedLogApplier {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            applied_raft_index: 0,
            applied_wal_sequence: 0,
        }
    }

    pub fn applied_raft_index(&self) -> u64 {
        self.applied_raft_index
    }

    pub fn applied_wal_sequence(&self) -> u64 {
        self.applied_wal_sequence
    }

    pub fn apply(
        &mut self,
        raft_index: u64,
        committed_log: &[u8],
        engine: &TemporalEngine,
    ) -> Result<Option<CommandResponse>, RaftError> {
        if raft_index <= self.applied_raft_index {
            return Ok(None);
        }
        let entry = parse_data_raft_log(committed_log)?;
        if entry.shard_id != self.shard_id {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "shard mismatch: log={}, applier={}",
                entry.shard_id, self.shard_id
            )));
        }
        if entry.raft_index != raft_index {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "raft index mismatch: header={}, apply={}",
                entry.raft_index, raft_index
            )));
        }
        let response = engine.execute_raft_apply(ExecuteRequest {
            shard_id: entry.shard_id,
            command: entry.command,
        });
        if !response.status.ok {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "engine apply failed: {} {}",
                response.status.code, response.status.message
            )));
        }
        self.applied_raft_index = entry.raft_index;
        self.applied_wal_sequence = entry.wal_sequence;
        Ok(Some(response.response))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftExternalSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
}

pub const DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftSnapshotTransferMode {
    PeerStreaming,
    ExternalStore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTransferPolicy {
    pub external_threshold_bytes: u64,
    pub allow_peer_streaming: bool,
    pub allow_external_store: bool,
}

impl Default for RaftSnapshotTransferPolicy {
    fn default() -> Self {
        Self {
            external_threshold_bytes: DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES,
            allow_peer_streaming: true,
            allow_external_store: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTransferDecision {
    pub mode: RaftSnapshotTransferMode,
    pub snapshot_bytes: u64,
    pub threshold_bytes: u64,
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicaBootstrapPlan {
    pub shard_id: ShardId,
    pub target_id: RaftNodeId,
    pub transfer: RaftSnapshotTransferDecision,
    pub last_included_index: u64,
    pub catch_up_from_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotPublishReport {
    pub shard_id: ShardId,
    pub last_log_index: u64,
    pub raft_ref: RaftExternalSnapshotRef,
    pub meta_ref: ShardSnapshotRef,
}

/// Four times the compaction threshold: enough that an ordinary lagging follower is waited for,
/// while a peer that has stopped answering cannot hold the log open indefinitely.
fn default_max_retained_log_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeStatus {
    pub node_id: RaftNodeId,
    pub role: RaftRole,
    pub replica_role: RaftReplicaRole,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub alive: bool,
    pub lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftClusterStatus {
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    pub majority: usize,
    pub live_voters: usize,
    pub has_majority: bool,
    pub leader_lease_valid: bool,
    pub nodes: Vec<RaftNodeStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPeerPipelineState {
    pub peer_id: RaftNodeId,
    pub role: RaftRole,
    pub replica_role: RaftReplicaRole,
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    #[serde(default)]
    pub append_queue_limit: u64,
    #[serde(default)]
    pub inflight_bytes_limit: u64,
    #[serde(default)]
    pub apply_inflight_tasks: u64,
    #[serde(default)]
    pub apply_inflight_limit: u64,
    #[serde(default)]
    pub apply_queue_depth: u64,
    #[serde(default)]
    pub apply_queue_max_depth: u64,
    #[serde(default)]
    pub apply_batch_bytes_limit: u64,
    #[serde(default)]
    pub apply_backpressure_rejections: u64,
    #[serde(default)]
    pub memory_backpressure_rejections: u64,
    #[serde(default)]
    pub oversized_log_rejections: u64,
    #[serde(default)]
    pub append_queue_max_depth: u64,
    pub reorder_queue_depth: u64,
    #[serde(default)]
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_accepted: u64,
    pub reorder_entries_released: u64,
    pub reorder_entries_rejected: u64,
    /// Entries that left the reorder queue because the index they were waiting for arrived.
    ///
    /// Fed from `reorder_entries_released`, which is the same quantity derived differently: ours is
    /// a depth delta (`depth_before + received - depth_after`), theirs is counted once per entry as
    /// the drain removes it. A depth delta attributes EVERY departure to convergence, including a
    /// timeout or a dropped package, so this is an upper bound rather than a measurement. Counting
    /// it at the drain site is what would make the two agree.
    #[serde(default)]
    pub reorder_entries_converged: u64,
    #[serde(default)]
    pub reorder_entry_timeouts: u64,
    #[serde(default)]
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    /// Sends to this peer that failed outright, producing no response at all. Each one released a
    /// reservation that would otherwise have been held forever.
    #[serde(default)]
    pub append_send_failures: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_send_completed: u64,
    pub snapshot_send_failed: u64,
    pub snapshot_install_started: u64,
    pub snapshot_install_completed: u64,
    pub snapshot_install_rejected: u64,
    pub snapshot_install_rolled_back: u64,
    pub snapshot_install_received_chunks: u64,
    pub snapshot_install_total_chunks: u64,
    #[serde(default)]
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_retry_count: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    pub snapshot_backpressure_rejections: u64,
    #[serde(default)]
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_send_elapsed_ms: u64,
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub snapshot_during_membership_change: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_requests: u64,
    pub transfer_leader_accepted: u64,
    pub transfer_leader_rejected: u64,
    pub transfer_leader_completed: u64,
    pub transfer_leader_elapsed_ms: u64,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_elapsed_ms: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub auto_promoted_from_learner: bool,
}

impl MatrixRaftPeerPipelineState {
    fn to_matrixraft_peer_pipeline_status(&self) -> MatrixRaftPeerPipelineStatus {
        matrixraft_peer_pipeline_status_from_observed(&MatrixRaftObservedPeerPipeline {
            peer_id: self.peer_id,
            match_index: self.match_index,
            next_index: self.next_index,
            append_requests: self.append_requests,
            append_accepted: self.append_accepted,
            append_rejected: self.append_rejected,
            inflight_entries: self.inflight_entries,
            inflight_bytes: self.inflight_bytes,
            append_queue_depth: self.append_queue_depth,
            append_queue_limit: self.append_queue_limit,
            append_queue_max_depth: self.append_queue_max_depth,
            inflight_bytes_limit: self.inflight_bytes_limit,
            apply_inflight_tasks: self.apply_inflight_tasks,
            apply_inflight_limit: self.apply_inflight_limit,
            apply_queue_depth: self.apply_queue_depth,
            apply_queue_max_depth: self.apply_queue_max_depth,
            apply_batch_bytes_limit: self.apply_batch_bytes_limit,
            apply_backpressure_rejections: self.apply_backpressure_rejections,
            memory_backpressure_rejections: self.memory_backpressure_rejections,
            oversized_log_rejections: self.oversized_log_rejections,
            reorder_queue_depth: self.reorder_queue_depth,
            out_of_order_append_rejections: self.out_of_order_append_rejections,
            reorder_entries_rejected: self.reorder_entries_rejected,
            reorder_entries_converged: self.reorder_entries_converged,
            reorder_entry_timeouts: self.reorder_entry_timeouts,
            reorder_dropped_packages: self.reorder_dropped_packages,
            stale_term_rejections: self.stale_term_rejections,
            packet_loss_events: 0,
            network_error_probe_transitions: 0,
            snapshot_sending: self.snapshot_sending,
            snapshot_installing: self.snapshot_installing,
            snapshot_installed_index: self.snapshot_installed_index,
            snapshot_send_attempts: self.snapshot_send_attempts,
            snapshot_install_total_chunks: self.snapshot_install_total_chunks,
            snapshot_install_progress_per_mille: self.snapshot_install_progress_per_mille,
            snapshot_backpressure_rejections: self.snapshot_backpressure_rejections,
            snapshot_rate_limit_rejections: self.snapshot_rate_limit_rejections,
            snapshot_install_rolled_back: self.snapshot_install_rolled_back,
            snapshot_chunk_retry_count: self.snapshot_chunk_retry_count,
            snapshot_send_timeouts: self.snapshot_send_timeouts,
            snapshot_during_membership_change: self.snapshot_during_membership_change,
            snapshot_rejoin_after_compacted_log: self.snapshot_rejoin_after_compacted_log,
            transfer_leader_target: self.transfer_leader_target,
            transfer_leader_timeouts: self.transfer_leader_timeouts,
            pre_vote_rejections: self.pre_vote_rejections,
            election_rejections: self.election_rejections,
            offline_timeout_reached: self.offline_timeout_reached,
            offline_timeout_rejections: self.offline_timeout_rejections,
            auto_promoted_from_learner: self.auto_promoted_from_learner,
            witness_quorum_required: 0,
            witness_quorum_acked: 0,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCapabilityEvidence {
    pub capability: String,
    pub ready: bool,
    pub evidence_field: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRuntimeAdminReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub commit_index: u64,
    pub leader_lease_valid: bool,
    pub read_index_validated: bool,
    pub lease_read_validated: bool,
    pub stale_follower_read_rejected: bool,
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub stale_leader_lease_rejected: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_accepted: bool,
    #[serde(default)]
    pub bounded_stale_read_rejected: bool,
    #[serde(default)]
    pub minority_partition_rejected_reads: bool,
    #[serde(default)]
    pub minority_partition_rejected_writes: bool,
    #[serde(default)]
    pub healed_follower_caught_up: bool,
    #[serde(default)]
    pub witness_membership_present: bool,
    #[serde(default)]
    pub witness_role_behavior_present: bool,
    #[serde(default)]
    pub learner_auto_promote_present: bool,
    #[serde(default)]
    pub pending_joint_consensus_present: bool,
    #[serde(default)]
    pub learner_add_present: bool,
    #[serde(default)]
    pub learner_catchup_present: bool,
    #[serde(default)]
    pub learner_promote_present: bool,
    #[serde(default)]
    pub voter_remove_present: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_present: bool,
    #[serde(default)]
    pub pending_joint_consensus_restart_present: bool,
    #[serde(default)]
    pub membership_evidence: RaftMembershipRuntimeEvidence,
    pub peer_pipeline_states: Vec<MatrixRaftPeerPipelineState>,
    pub append_backpressure_enforced: bool,
    #[serde(default)]
    pub apply_backpressure_enforced: bool,
    #[serde(default)]
    pub memory_replicate_bytes_enforced: bool,
    #[serde(default)]
    pub oversized_log_rejection_present: bool,
    #[serde(default)]
    pub out_of_order_append_handling_present: bool,
    #[serde(default)]
    pub reorder_timeout_drop_present: bool,
    #[serde(default)]
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
    pub snapshot_sender_lifecycle_present: bool,
    pub snapshot_downloader_lifecycle_present: bool,
    pub snapshot_retry_backpressure_present: bool,
    #[serde(default)]
    pub snapshot_chunk_retry_present: bool,
    #[serde(default)]
    pub snapshot_send_timeout_present: bool,
    #[serde(default)]
    pub snapshot_rate_limit_present: bool,
    #[serde(default)]
    pub snapshot_install_progress_present: bool,
    #[serde(default)]
    pub snapshot_install_rollback_present: bool,
    #[serde(default)]
    pub snapshot_membership_change_present: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log_present: bool,
    pub wal_segment_lifecycle_present: bool,
    pub wal_segment_count: u64,
    pub wal_active_segment_id: u64,
    pub wal_first_retained_segment_id: u64,
    pub wal_last_retained_segment_id: u64,
    pub wal_total_bytes: u64,
    pub wal_active_segment_bytes: u64,
    pub wal_total_records: u64,
    pub wal_first_sequence: u64,
    pub wal_last_sequence: u64,
    #[serde(default)]
    pub wal_first_log_index: u64,
    #[serde(default)]
    pub wal_last_log_index: u64,
    #[serde(default)]
    pub wal_released_segment_count: u64,
    #[serde(default)]
    pub wal_slow_fsync_backpressure_observed: bool,
    pub pre_vote_enforced: bool,
    pub election_controls_enforced: bool,
    #[serde(default)]
    pub pre_vote_process_evidence_observed: bool,
    #[serde(default)]
    pub election_prohibition_observed: bool,
    #[serde(default)]
    pub offline_timeout_observed: bool,
    #[serde(default)]
    pub transfer_timeout_observed: bool,
    pub read_index_requests: u64,
    pub read_index_accepted: u64,
    pub read_index_rejected: u64,
    pub lease_read_requests: u64,
    pub lease_read_accepted: u64,
    pub lease_read_rejected: u64,
    #[serde(default)]
    pub stale_leader_lease_rejection_count: u64,
    #[serde(default)]
    pub lagging_follower_read_rejection_count: u64,
    #[serde(default)]
    pub bounded_stale_read_requests: u64,
    #[serde(default)]
    pub bounded_stale_read_accepted_count: u64,
    #[serde(default)]
    pub bounded_stale_read_rejected_count: u64,
    #[serde(default)]
    pub minority_partition_read_rejection_count: u64,
    #[serde(default)]
    pub minority_partition_write_rejection_count: u64,
    #[serde(default)]
    pub stale_follower_write_rejection_count: u64,
    #[serde(default)]
    pub healed_follower_catchup_count: u64,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    #[serde(default)]
    pub quorum_peer_progress_observed: bool,
    #[serde(default)]
    pub peer_pipeline_runtime_activity_observed: bool,
    #[serde(default)]
    pub peer_pipeline_limits_observed: bool,
    pub admin_status_surface_complete: bool,
    #[serde(default)]
    pub capability_matrix: Vec<MatrixRaftCapabilityEvidence>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLeaderElectionParityReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    pub leader_election_ready: bool,
    pub pre_vote_ready: bool,
    pub leader_failover_observed: bool,
    pub learner_add_ready: bool,
    pub learner_catchup_ready: bool,
    pub learner_promote_ready: bool,
    pub learner_auto_promote_ready: bool,
    pub membership_ready: bool,
    pub leader_transfer_exact_once_ready: bool,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    pub learner_add_count: u64,
    pub learner_catchup_count: u64,
    pub learner_promote_count: u64,
    pub auto_promote_count: u64,
    pub leader_transfer_exact_once_commit_count: u64,
    pub evidence_fields: Vec<String>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLocalPeerStatus {
    pub status: RaftNodeStatus,
    pub pipeline_state: MatrixRaftPeerPipelineState,
    pub participates_in_quorum: bool,
    pub can_serve_data: bool,
    pub can_be_leader: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLocalStatusReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    #[serde(default)]
    pub wal_first_log_index: u64,
    #[serde(default)]
    pub wal_last_log_index: u64,
    pub has_majority: bool,
    pub leader_lease_valid: bool,
    pub pending_joint_consensus: Option<JointConsensusMembership>,
    pub witness_membership_present: bool,
    pub learner_membership_present: bool,
    pub learner_auto_promote_present: bool,
    pub peers: Vec<MatrixRaftLocalPeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicaLag {
    pub node_id: RaftNodeId,
    pub lag: u64,
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftApplyLag {
    pub node_id: RaftNodeId,
    pub commit_index: u64,
    pub applied_index: u64,
    pub apply_lag: u64,
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftApplyHealth {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_allowed_apply_lag: u64,
    pub max_apply_lag: u64,
    pub fully_applied_nodes: Vec<RaftNodeId>,
    pub slow_appliers: Vec<RaftApplyLag>,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicationHealth {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_allowed_lag: u64,
    pub max_lag: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub lagging_voters: Vec<RaftReplicaLag>,
    pub live_voters: usize,
    pub majority: usize,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftHardState {
    pub current_term: u64,
    pub voted_for: Option<RaftNodeId>,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembership {
    pub shard_id: ShardId,
    pub voters: Vec<RaftNodeId>,
    pub leader_id: RaftNodeId,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipRuntimeEvidence {
    #[serde(default)]
    pub learner_add_count: u64,
    #[serde(default)]
    pub learner_catchup_count: u64,
    #[serde(default)]
    pub learner_promote_count: u64,
    #[serde(default)]
    pub voter_remove_count: u64,
    #[serde(default)]
    pub witness_add_count: u64,
    #[serde(default)]
    pub auto_promote_count: u64,
    #[serde(default)]
    pub leader_transfer_write_count: u64,
    #[serde(default)]
    pub leader_transfer_exact_once_commit_count: u64,
    #[serde(default)]
    pub leader_transfer_exact_once_commit_ids: Vec<u64>,
    #[serde(default)]
    pub pending_joint_consensus_persist_count: u64,
    #[serde(default)]
    pub pending_joint_consensus_restore_count: u64,
}

impl RaftReplicaRole {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    fn is_default(&self) -> bool {
        *self == RaftReplicaRole::default()
    }
}

impl RaftApplySnapshotFence {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    fn is_default(&self) -> bool {
        *self == RaftApplySnapshotFence::default()
    }
}

impl RaftMembershipRuntimeEvidence {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    /// This block is a dozen counters that are zero on almost every record, and spelling them
    /// out cost more than the entry the record exists to carry.
    fn is_default(&self) -> bool {
        *self == RaftMembershipRuntimeEvidence::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftWalRecord {
    pub hard_state: RaftHardState,
    pub membership: RaftMembership,
    #[serde(default, skip_serializing_if = "RaftReplicaRole::is_default")]
    pub replica_role: RaftReplicaRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub joint_membership: Option<JointConsensusMembership>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_snapshot: Option<RaftSnapshot>,
    #[serde(default, skip_serializing_if = "RaftApplySnapshotFence::is_default")]
    pub apply_snapshot_fence: RaftApplySnapshotFence,
    #[serde(default)]
    pub storage_apply_fence: RaftStorageApplyFence,
    // Volatile runtime telemetry: excluded from `raft_durable_fingerprint` because it is
    // not durability-relevant, and omitted from the encoding when it is at its default so
    // an incremental record does not spend 2.4 KB restating counters.
    #[serde(default, skip_serializing_if = "RaftPeerPipelineRuntimeState::is_default")]
    pub pipeline_state: RaftPeerPipelineRuntimeState,
    #[serde(default, skip_serializing_if = "RaftReadSafetyRuntimeState::is_default")]
    pub read_safety_state: RaftReadSafetyRuntimeState,
    #[serde(default, skip_serializing_if = "RaftMembershipRuntimeEvidence::is_default")]
    pub membership_evidence: RaftMembershipRuntimeEvidence,
    pub entries: Vec<RaftLogEntry>,
}

impl RaftPeerPipelineRuntimeState {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl RaftReadSafetyRuntimeState {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftPeerPipelineRuntimeState {
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    /// Appends refused because they carried a DIFFERENT term for an index at or below the commit
    /// point. Overwriting a committed entry is forbidden, so a non-zero count here is a bug in
    /// the SENDER -- it is not a condition this node may absorb by truncating its own log.
    #[serde(default)]
    pub committed_overwrite_rejections: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    /// Cluster clock reading when this node last ACCEPTED an append. Not when one last
    /// arrived: a follower stuck rejecting every entry still receives them, and counting
    /// arrivals is how a stalled follower comes to look healthy.
    #[serde(default)]
    pub last_accepted_append_ms: u64,
    /// The commit index the leader last reported here. It comes from the leader itself rather
    /// than from this process's shadow of it, which is what makes it trustworthy: a shadow does
    /// not move when a peer rejects, so it cannot tell "caught up" from "stuck".
    #[serde(default)]
    pub leader_reported_commit_index: u64,
    #[serde(default)]
    pub apply_inflight_tasks: u64,
    #[serde(default)]
    pub apply_queue_depth: u64,
    #[serde(default)]
    pub apply_queue_max_depth: u64,
    #[serde(default)]
    pub apply_backpressure_rejections: u64,
    #[serde(default)]
    pub memory_backpressure_rejections: u64,
    #[serde(default)]
    pub oversized_log_rejections: u64,
    #[serde(default)]
    pub append_queue_max_depth: u64,
    pub reorder_queue_depth: u64,
    #[serde(default)]
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_accepted: u64,
    pub reorder_entries_released: u64,
    pub reorder_entries_rejected: u64,
    #[serde(default)]
    pub reorder_entry_timeouts: u64,
    #[serde(default)]
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    /// Sends to this peer that failed outright, producing no response. Each released a reservation
    /// that would otherwise have been held forever.
    #[serde(default)]
    pub append_send_failures: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_send_completed: u64,
    pub snapshot_send_failed: u64,
    pub snapshot_install_started: u64,
    pub snapshot_install_completed: u64,
    pub snapshot_install_rejected: u64,
    pub snapshot_install_rolled_back: u64,
    pub snapshot_install_received_chunks: u64,
    /// Chunk count as of the last stall check. The send counts as progressing while this
    /// keeps changing, however long the transfer takes overall.
    pub snapshot_send_progress_mark: u64,
    pub snapshot_install_total_chunks: u64,
    #[serde(default)]
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_retry_count: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    pub snapshot_backpressure_rejections: u64,
    #[serde(default)]
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_send_started_ms: Option<u64>,
    pub snapshot_send_elapsed_ms: u64,
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub snapshot_during_membership_change: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_requests: u64,
    pub transfer_leader_accepted: u64,
    pub transfer_leader_rejected: u64,
    pub transfer_leader_completed: u64,
    pub transfer_leader_started_ms: Option<u64>,
    pub transfer_leader_elapsed_ms: u64,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_since_ms: Option<u64>,
    pub offline_elapsed_ms: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub auto_promoted_from_learner: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReadSafetyRuntimeState {
    pub read_index_requests: u64,
    pub read_index_accepted: u64,
    pub read_index_rejected: u64,
    pub lease_read_requests: u64,
    pub lease_read_accepted: u64,
    pub lease_read_rejected: u64,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    #[serde(default)]
    pub bounded_stale_read_requests: u64,
    #[serde(default)]
    pub bounded_stale_read_accepted: u64,
    #[serde(default)]
    pub bounded_stale_read_rejected: u64,
    #[serde(default)]
    pub stale_leader_lease_rejected: u64,
    #[serde(default)]
    pub lagging_follower_read_rejected: u64,
    #[serde(default)]
    pub stale_follower_write_rejected: u64,
    #[serde(default)]
    pub minority_partition_read_rejected: u64,
    #[serde(default)]
    pub minority_partition_write_rejected: u64,
    #[serde(default)]
    pub healed_follower_catchup_observed: u64,
}

impl RaftReadSafetyRuntimeState {
    fn record_matrixraft_runtime_decision(
        &mut self,
        decision: &MatrixRaftReadSafetyRuntimeDecision,
    ) {
        if decision.stale_leader_lease_rejected {
            self.stale_leader_lease_rejected = self.stale_leader_lease_rejected.saturating_add(1);
        }
        if decision.lagging_follower_read_rejected {
            self.lagging_follower_read_rejected =
                self.lagging_follower_read_rejected.saturating_add(1);
        }
        if decision.stale_follower_write_rejected {
            self.stale_follower_write_rejected =
                self.stale_follower_write_rejected.saturating_add(1);
        }
        if decision.minority_partition_read_rejected {
            self.minority_partition_read_rejected =
                self.minority_partition_read_rejected.saturating_add(1);
        }
        if decision.minority_partition_write_rejected {
            self.minority_partition_write_rejected =
                self.minority_partition_write_rejected.saturating_add(1);
        }
        if decision.healed_follower_catchup_observed {
            self.healed_follower_catchup_observed =
                self.healed_follower_catchup_observed.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RaftWalEnvelope {
    sequence: u64,
    checksum: String,
    record: RaftWalRecord,
    /// Present on an incremental record: `record.entries` then carries ONLY the log
    /// entries appended since `from_index`, and recovery folds it onto the most recent
    /// full ("base") record. Absent on a base record, so a WAL written before this
    /// field existed still reads back as a sequence of full snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delta: Option<RaftWalEntryDelta>,
}

/// Describes how to fold an incremental WAL record onto the running base.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct RaftWalEntryDelta {
    /// The record's `entries` hold only indexes strictly greater than this.
    from_index: u64,
    /// First index the node's full log holds after this record (0 when the log is empty).
    /// Anything below it was compacted away and must be dropped while folding.
    log_first_index: u64,
    /// Last index the node's full log holds after this record (0 when the log is empty).
    log_last_index: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RaftWalSegmentRuntimeState {
    #[serde(default)]
    released_segment_count: u64,
    #[serde(default)]
    last_fsync_elapsed_ms: u64,
    #[serde(default)]
    slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RaftWalRecovery {
    pub record: Option<RaftWalRecord>,
    pub valid_records: usize,
    pub truncated_bytes: u64,
    pub corrupt_tail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentInfo {
    pub segment_id: u64,
    pub path: String,
    pub bytes: u64,
    pub record_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    #[serde(default)]
    pub first_log_index: u64,
    #[serde(default)]
    pub last_log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentReport {
    pub active_segment_id: u64,
    pub segments: Vec<RaftWalSegmentInfo>,
    #[serde(default)]
    pub released_segment_count: u64,
    #[serde(default)]
    pub first_retained_log_index: u64,
    #[serde(default)]
    pub last_retained_log_index: u64,
    #[serde(default)]
    pub last_fsync_elapsed_ms: u64,
    #[serde(default)]
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Default)]
struct NodeWalCursor {
    /// Snapshot boundary of the last marker this node persisted, so an incremental record can
    /// omit an unchanged marker the way it omits already-persisted entries.
    persisted_marker_index: Option<u64>,
    next_sequence: u64,
    segments: Vec<RaftWalSegmentInfo>,
    released_segment_count: u64,
    last_fsync_elapsed_ms: u64,
    slow_fsync_backpressure_observed: bool,
    /// Encoded size of the full record that opened the active segment. Rotation waits
    /// until the segment is a multiple of it, so the one O(log) base write per segment
    /// stays a bounded fraction of the bytes that segment costs.
    base_bytes: u64,
    /// Last log index already written into the active segment, and the term at that
    /// index. An incremental record is only sound while the in-memory log still agrees
    /// with both; a conflict overwrite or a snapshot compaction re-bases instead.
    persisted_last_index: u64,
    persisted_last_term: u64,
    /// False until a full base record has been written into the active segment.
    has_base: bool,
}

/// A segment is never rotated below the size of the full record that opens it: a
/// segment that cannot hold even one base record would re-base on every append, which
/// is the O(log length) cost incremental records exist to remove. Above that floor the
/// configured `max_segment_bytes` decides rotation, so retention keeps its meaning and
/// the base cost is amortised over `max_segment_bytes / delta_size` appends.

/// How far behind a node must be, with nothing accepted, before it says so.
const RAFT_STALL_WARN_MS: u64 = 30_000;
/// How often it may repeat that. A node that is stuck stays stuck, and one line per tick would
/// bury everything else in the log.
const RAFT_STALL_REPORT_INTERVAL_MS: u64 = 60_000;
/// Consecutive failed AppendEntries before a leader marks a peer down.
const RAFT_PEER_FAILURE_THRESHOLD: u32 = 3;
/// Ticks of leader silence a follower tolerates before standing for election.
const RAFT_ELECTION_TIMEOUT_BASE_TICKS: u64 = 8;
/// Width of the random spread added to the election timeout.
const RAFT_ELECTION_TIMEOUT_SPREAD_TICKS: u64 = 12;

/// Draw a fresh election timeout. It must be re-drawn on every attempt: a timeout that is
/// merely a fixed function of the node id puts two survivors a constant distance apart, so
/// each one's campaign supersedes the other's brand-new leadership before its first
/// heartbeat can land, and they trade the term back and forth indefinitely.
pub(crate) fn randomized_election_timeout_ticks(node_id: RaftNodeId) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(node_id);
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default(),
    );
    RAFT_ELECTION_TIMEOUT_BASE_TICKS + (hasher.finish() % RAFT_ELECTION_TIMEOUT_SPREAD_TICKS)
}

#[derive(Debug, Clone)]
pub struct LocalRaftWal {
    root: PathBuf,
    cursors: Arc<Mutex<BTreeMap<(ShardId, RaftNodeId), NodeWalCursor>>>,
    /// One barrier gate per node log, so writers that arrive while an fsync is in flight ride it
    /// instead of queueing to take an identical one. Shared by `clone`, like the cursors: two
    /// handles to the same file must share a gate or each would take its own barrier again.
    flush_gates: Arc<crate::flush_gate::FlushRegistry>,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub candidate_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// Asks whether the vote WOULD be granted, without anyone acting on the question.
    ///
    /// A node that cannot reach the cluster would otherwise campaign on a timer and raise its term
    /// every time, and a leader that later hears that term must step down -- so one unreachable
    /// node drags a healthy cluster through repeated elections. Answering a pre-vote changes no
    /// state, so an unreachable node never moves its term.
    #[serde(default)]
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    #[serde(default)]
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub snapshot: RaftSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub success: bool,
    pub last_included_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotInstallReport {
    pub shard_id: ShardId,
    pub node_id: RaftNodeId,
    pub snapshot_index: u64,
    pub before_commit_index: u64,
    pub after_commit_index: u64,
    pub freeze_started: bool,
    pub flush_completed: bool,
    pub manifest_verified: bool,
    pub checksum_verified: bool,
    pub install_completed: bool,
    pub tail_replay_completed: bool,
    pub rollback_performed: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotChunkRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub snapshot_id: String,
    pub last_included_term: u64,
    pub last_included_index: u64,
    pub chunk_index: u64,
    pub chunk_count: u64,
    pub entries: Vec<RaftLogEntry>,
    /// S2: opaque engine state image, carried on chunk 0 of an image-based snapshot (whose
    /// `entries` are empty, so it is a single chunk). Default `None` keeps entry-carrying chunks
    /// and older peers byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_image: Option<RaftSnapshotStateImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotChunkResponse {
    pub term: u64,
    pub success: bool,
    pub snapshot_complete: bool,
    pub received_chunks: u64,
    pub last_included_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcMetadata {
    pub auth_token: Option<String>,
    pub deadline_ms: Option<u64>,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftTickOutcome {
    LeaderAlive {
        leader_id: RaftNodeId,
    },
    ElectionPending {
        elapsed_tick: u64,
        timeout_tick: u64,
    },
    PreVoteRejected {
        candidate_id: RaftNodeId,
    },
    LeaderElected {
        leader_id: RaftNodeId,
        term: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSchedulerOptions {
    pub heartbeat_interval_tick: u64,
    pub election_timeout_min_tick: u64,
    pub election_timeout_max_tick: u64,
    pub random_seed: u64,
}

impl Default for RaftSchedulerOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval_tick: 1,
            election_timeout_min_tick: 3,
            election_timeout_max_tick: 6,
            random_seed: 0x5eed,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSchedulerTick {
    pub heartbeat_due: bool,
    pub election_due: bool,
    pub elapsed_heartbeat_tick: u64,
    pub elapsed_election_tick: u64,
    pub election_timeout_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftScheduler {
    options: RaftSchedulerOptions,
    elapsed_heartbeat_tick: u64,
    elapsed_election_tick: u64,
    election_timeout_tick: u64,
    rng_state: u64,
}

impl RaftScheduler {
    pub fn new(options: RaftSchedulerOptions) -> Self {
        let mut scheduler = Self {
            options: RaftSchedulerOptions {
                heartbeat_interval_tick: options.heartbeat_interval_tick.max(1),
                election_timeout_min_tick: options.election_timeout_min_tick.max(1),
                election_timeout_max_tick: options
                    .election_timeout_max_tick
                    .max(options.election_timeout_min_tick.max(1)),
                random_seed: options.random_seed,
            },
            elapsed_heartbeat_tick: 0,
            elapsed_election_tick: 0,
            election_timeout_tick: 0,
            rng_state: options.random_seed,
        };
        scheduler.election_timeout_tick = scheduler.next_election_timeout();
        scheduler
    }

    pub fn tick(&mut self, leader_alive: bool) -> RaftSchedulerTick {
        self.elapsed_heartbeat_tick += 1;
        if leader_alive {
            self.elapsed_election_tick = 0;
            self.election_timeout_tick = self.next_election_timeout();
        } else {
            self.elapsed_election_tick += 1;
        }

        let heartbeat_due =
            self.elapsed_heartbeat_tick >= self.options.heartbeat_interval_tick && leader_alive;
        if heartbeat_due {
            self.elapsed_heartbeat_tick = 0;
        }
        let election_due =
            !leader_alive && self.elapsed_election_tick >= self.election_timeout_tick;
        if election_due {
            self.elapsed_election_tick = 0;
            self.election_timeout_tick = self.next_election_timeout();
        }
        RaftSchedulerTick {
            heartbeat_due,
            election_due,
            elapsed_heartbeat_tick: self.elapsed_heartbeat_tick,
            elapsed_election_tick: self.elapsed_election_tick,
            election_timeout_tick: self.election_timeout_tick,
        }
    }

    fn next_election_timeout(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let min = self.options.election_timeout_min_tick;
        let max = self.options.election_timeout_max_tick;
        let span = max.saturating_sub(min).saturating_add(1);
        min + (self.rng_state % span)
    }
}

pub trait RaftTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError>;

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError>;

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError>;

    fn install_snapshot_chunk(
        &self,
        _request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        Err(RaftError::Transport(
            "snapshot chunk transport is not implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcRuntimeOptions {
    pub max_inflight: usize,
    pub max_retries: usize,
    pub retry_backoff_ms: u64,
    pub deadline_ms: u64,
    pub auth_token_required: bool,
}

impl Default for RaftRpcRuntimeOptions {
    fn default() -> Self {
        Self {
            max_inflight: 128,
            max_retries: 2,
            retry_backoff_ms: 10,
            deadline_ms: 1_000,
            auth_token_required: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcRuntimeMetrics {
    pub inflight: usize,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub retries: u64,
    pub backpressure_rejections: u64,
}

#[derive(Debug, Clone)]
pub struct RaftRpcRuntime<T> {
    transport: T,
    options: RaftRpcRuntimeOptions,
    auth_token: Option<String>,
    inflight: Arc<Mutex<usize>>,
    metrics: Arc<Mutex<RaftRpcRuntimeMetrics>>,
    request_counter: Arc<Mutex<u64>>,
}

impl<T> RaftRpcRuntime<T> {
    pub fn new(transport: T, options: RaftRpcRuntimeOptions) -> Self {
        Self::with_auth_token(transport, options, None)
    }

    pub fn with_auth_token(
        transport: T,
        options: RaftRpcRuntimeOptions,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            transport,
            options: RaftRpcRuntimeOptions {
                max_inflight: options.max_inflight.max(1),
                deadline_ms: options.deadline_ms.max(1),
                ..options
            },
            auth_token,
            inflight: Arc::default(),
            metrics: Arc::default(),
            request_counter: Arc::default(),
        }
    }

    pub fn inflight(&self) -> usize {
        *self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned")
    }

    pub fn metrics(&self) -> RaftRpcRuntimeMetrics {
        let mut metrics = *self.metrics.lock().expect("raft rpc metrics lock poisoned");
        metrics.inflight = self.inflight();
        metrics
    }

    fn record_metrics(&self, update: impl FnOnce(&mut RaftRpcRuntimeMetrics)) {
        let mut metrics = self.metrics.lock().expect("raft rpc metrics lock poisoned");
        update(&mut metrics);
    }

    fn acquire(&self) -> Result<RaftRpcPermit, RaftError> {
        let mut inflight = self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned");
        if *inflight >= self.options.max_inflight {
            self.record_metrics(|metrics| metrics.backpressure_rejections += 1);
            return Err(RaftError::Transport("raft rpc backpressure".to_string()));
        }
        *inflight += 1;
        Ok(RaftRpcPermit {
            inflight: Arc::clone(&self.inflight),
        })
    }

    fn retry<R>(&self, mut call: impl FnMut() -> Result<R, RaftError>) -> Result<R, RaftError> {
        let _permit = self.acquire()?;
        let attempts = self.options.max_retries.saturating_add(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            self.record_metrics(|metrics| metrics.attempts += 1);
            match call() {
                Ok(response) => {
                    self.record_metrics(|metrics| metrics.successes += 1);
                    return Ok(response);
                }
                Err(err @ RaftError::Transport(_)) => {
                    if attempt + 1 < attempts {
                        self.record_metrics(|metrics| metrics.retries += 1);
                    }
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.options.retry_backoff_ms > 0 {
                        thread::sleep(Duration::from_millis(self.options.retry_backoff_ms));
                    }
                }
                Err(err) => {
                    self.record_metrics(|metrics| metrics.failures += 1);
                    return Err(err);
                }
            }
        }
        self.record_metrics(|metrics| metrics.failures += 1);
        Err(last_error.unwrap_or_else(|| RaftError::Transport("raft rpc failed".to_string())))
    }

    fn next_metadata(&self) -> RaftRpcMetadata {
        let mut counter = self
            .request_counter
            .lock()
            .expect("raft rpc request counter lock poisoned");
        *counter += 1;
        RaftRpcMetadata {
            auth_token: self.auth_token.clone(),
            deadline_ms: Some(self.options.deadline_ms),
            request_id: format!("raft-rpc-{}", *counter),
        }
    }
}

#[derive(Debug)]
struct RaftRpcPermit {
    inflight: Arc<Mutex<usize>>,
}

impl Drop for RaftRpcPermit {
    fn drop(&mut self) {
        let mut inflight = self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned");
        *inflight = inflight.saturating_sub(1);
    }
}

impl<T> RaftTransport for RaftRpcRuntime<T>
where
    T: RaftTransport,
{
    fn append_entries(
        &self,
        mut request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.append_entries(request.clone()))
    }

    fn request_vote(&self, mut request: VoteRequest) -> Result<VoteResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.request_vote(request.clone()))
    }

    fn install_snapshot(
        &self,
        mut request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.install_snapshot(request.clone()))
    }

    fn install_snapshot_chunk(
        &self,
        mut request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.install_snapshot_chunk(request.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedRaftTransport<T> {
    inner: T,
    required_token: String,
}

impl<T> AuthenticatedRaftTransport<T> {
    pub fn new(inner: T, required_token: impl Into<String>) -> Self {
        Self {
            inner,
            required_token: required_token.into(),
        }
    }

    fn check(&self, rpc: &Option<RaftRpcMetadata>) -> Result<(), RaftError> {
        validate_raft_rpc_metadata(rpc, &self.required_token)
    }
}

impl<T> RaftTransport for AuthenticatedRaftTransport<T>
where
    T: RaftTransport,
{
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.request_vote(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.install_snapshot_chunk(request)
    }
}

#[derive(Debug, Clone)]
pub struct HttpRaftTransport {
    peers: BTreeMap<RaftNodeId, String>,
    options: HttpRequestOptions,
    /// Whether replicated batches go out in the binary encoding.
    ///
    /// True everywhere: a receiver sniffs the body and accepts either, so this is the sender's
    /// half of a decision the reader already handles. A test sends JSON to keep the receiver's
    /// other branch exercised.
    binary_replication: bool,
}

impl HttpRaftTransport {
    pub fn new(peers: BTreeMap<RaftNodeId, String>) -> Self {
        Self {
            peers,
            options: HttpRequestOptions::default(),
            binary_replication: true,
        }
    }

    pub fn with_options(peers: BTreeMap<RaftNodeId, String>, options: HttpRequestOptions) -> Self {
        Self {
            peers,
            options,
