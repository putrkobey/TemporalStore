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
            binary_replication: true,
        }
    }

    /// Send append-entries as JSON, for the test that keeps the receiver's JSON branch covered.
    #[cfg(test)]
    pub(crate) fn send_json_append_entries_for_test(&mut self) {
        self.binary_replication = false;
    }

    fn peer_addr(&self, node_id: RaftNodeId) -> Result<&str, RaftError> {
        self.peers
            .get(&node_id)
            .map(String::as_str)
            .ok_or(RaftError::NodeNotFound(node_id))
    }
}

impl RaftTransport for HttpRaftTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let addr = self.peer_addr(request.target_id)?.to_string();
        if self.binary_replication {
            let body = wal_proto::encode_append_entries(&request)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            // The response is a handful of integers either way, so it stays as it was; the size
            // that matters is the entries travelling out.
            let raw = crate::http::request_bytes_with_options(
                &addr,
                "POST",
                "/raft/append_entries",
                &body,
                "application/x-protobuf",
                self.options,
            )
            .map_err(|err| RaftError::Transport(err.to_string()))?;
            return serde_json::from_slice(&raw)
                .map_err(|err| RaftError::Transport(err.to_string()));
        }
        Ok(post_json_with_options(
            &addr,
            "/raft/append_entries",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/request_vote",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/install_snapshot",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/install_snapshot_chunk",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }
}

/// Whether a request body carries the binary encoding rather than text.
pub fn is_binary_rpc(body: &[u8]) -> bool {
    wal_proto::is_binary_rpc(body)
}

/// Decode a binary replicated batch. Callers that only need a field from the header still have to
/// decode, since a binary body has no fields to read out by name.
pub fn decode_append_entries(body: &[u8]) -> std::io::Result<AppendEntriesRequest> {
    wal_proto::decode_append_entries(body)
}

pub fn handle_raft_http(cluster: &RaftCluster, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        // Accept either encoding: a body starting with the magic byte is binary, and a text
        // body always starts with `{`.
        ("POST", "/raft/append_entries") if wal_proto::is_binary_rpc(&request.body) => {
            match wal_proto::decode_append_entries(&request.body) {
                Ok(req) => match cluster.receive_append_entries(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        ("POST", "/raft/append_entries") => match parse_json::<AppendEntriesRequest>(&request.body)
        {
            Ok(req) => match cluster.receive_append_entries(req) {
                Ok(response) => json_response(200, &response),
                Err(err) => json_response(500, &err.to_string()),
            },
            Err(err) => json_response(400, &err.to_string()),
        },
        ("POST", "/raft/request_vote") => match parse_json::<VoteRequest>(&request.body) {
            Ok(req) => match cluster.receive_vote_request(req) {
                Ok(response) => json_response(200, &response),
                Err(err) => json_response(500, &err.to_string()),
            },
            Err(err) => json_response(400, &err.to_string()),
        },
        ("POST", "/raft/install_snapshot") => {
            match parse_json::<InstallSnapshotRequest>(&request.body) {
                Ok(req) => match cluster.receive_install_snapshot(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        ("POST", "/raft/install_snapshot_chunk") => {
            match parse_json::<InstallSnapshotChunkRequest>(&request.body) {
                Ok(req) => match cluster.receive_install_snapshot_chunk(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        _ => json_response(404, &"unknown raft route"),
    }
}

pub fn handle_authenticated_raft_http(
    cluster: &RaftCluster,
    request: HttpRequest,
    required_token: &str,
) -> (u16, Vec<u8>) {
    let auth = match raft_rpc_metadata_for_http_request(&request) {
        Ok(Some(rpc)) => validate_raft_rpc_metadata(&Some(rpc), required_token),
        Ok(None) => Ok(()),
        Err(err) => Err(err),
    };
    match auth {
        Ok(()) => handle_raft_http(cluster, request),
        Err(err) => json_response(403, &err.to_string()),
    }
}

fn raft_rpc_metadata_for_http_request(
    request: &HttpRequest,
) -> Result<Option<RaftRpcMetadata>, RaftError> {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/raft/append_entries") => {
            // A binary body carries its rpc metadata inside the message, not as a JSON field.
            // Parsing it as JSON here fails, and that failure surfaced as a 403 on EVERY binary
            // append -- the authenticated wrapper choked before dispatch ever saw the request.
            if wal_proto::is_binary_rpc(&request.body) {
                let req = wal_proto::decode_append_entries(&request.body)
                    .map_err(|err| RaftError::Transport(err.to_string()))?;
                return Ok(req.rpc);
            }
            let req = parse_json::<AppendEntriesRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/request_vote") => {
            let req = parse_json::<VoteRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/install_snapshot") => {
            let req = parse_json::<InstallSnapshotRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/install_snapshot_chunk") => {
            let req = parse_json::<InstallSnapshotChunkRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        _ => Ok(None),
    }
}

/// Which peer a replication or vote request came FROM, for a caller that filters by sender.
///
/// The sender is a field of the message, and a binary body has no fields to read out by name, so
/// it has to be decoded. A caller that reaches for `serde_json` here gets `None` on every binary
/// append -- and `None` reads as "no peer to filter", which is the permissive answer. That is how
/// the chaos peer block stopped covering replication the day appends became binary: the filter
/// did not error, it just stopped matching. `raft_rpc_metadata_for_http_request` already has to
/// decode for the same reason; this is the same requirement for the sender id.
pub fn replication_peer_id_for_http_request(request: &HttpRequest) -> Option<RaftNodeId> {
    match request.path.as_str() {
        "/raft/append_entries" => {
            if wal_proto::is_binary_rpc(&request.body) {
                return wal_proto::decode_append_entries(&request.body)
                    .ok()
                    .map(|req| req.leader_id);
            }
            json_field_u64(&request.body, "leader_id")
        }
        "/raft/install_snapshot" | "/raft/install_snapshot_chunk" => {
            json_field_u64(&request.body, "leader_id")
        }
        "/raft/request_vote" => json_field_u64(&request.body, "candidate_id"),
        _ => None,
    }
}

fn json_field_u64(body: &[u8], field: &str) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get(field)?
        .as_u64()
}

fn validate_raft_rpc_metadata(
    rpc: &Option<RaftRpcMetadata>,
    required_token: &str,
) -> Result<(), RaftError> {
    let Some(rpc) = rpc else {
        return Err(RaftError::Transport(
            "missing raft rpc metadata".to_string(),
        ));
    };
    if rpc.auth_token.as_deref() != Some(required_token) {
        return Err(RaftError::Transport("raft rpc auth failed".to_string()));
    }
    if rpc.deadline_ms.unwrap_or_default() == 0 {
        return Err(RaftError::Transport(
            "raft rpc deadline missing".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftProposeRequest {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftReadRequest {
    pub node_id: RaftNodeId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftControlLeadershipRequest {
    pub node_id: RaftNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftCommandResponse {
    pub status: Status,
    pub response: CommandResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRaftEngineKind {
    TemporalRaft,
    RaftRs,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRaftSecurityMode {
    Mtls,
    PlaintextForLocalChaos,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftSecurity {
    pub mode: ProductionRaftSecurityMode,
    pub auth_token: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_cert_path: Option<String>,
}

impl ProductionRaftSecurity {
    pub fn mtls(
        auth_token: impl Into<String>,
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
        ca_cert_path: impl Into<String>,
    ) -> Self {
        Self {
            mode: ProductionRaftSecurityMode::Mtls,
            auth_token: Some(auth_token.into()),
            cert_path: Some(cert_path.into()),
            key_path: Some(key_path.into()),
            ca_cert_path: Some(ca_cert_path.into()),
        }
    }

    pub fn plaintext_for_local_chaos(auth_token: impl Into<String>) -> Self {
        Self {
            mode: ProductionRaftSecurityMode::PlaintextForLocalChaos,
            auth_token: Some(auth_token.into()),
            cert_path: None,
            key_path: None,
            ca_cert_path: None,
        }
    }

    pub fn validate(&self, allow_plaintext_for_local_chaos: bool) -> Result<(), RaftError> {
        if self.auth_token.as_deref().unwrap_or_default().is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires an auth token".to_string(),
            ));
        }
        match self.mode {
            ProductionRaftSecurityMode::Mtls => {
                validate_nonempty_file(self.cert_path.as_deref().unwrap_or_default(), "cert_path")?;
                validate_nonempty_file(self.key_path.as_deref().unwrap_or_default(), "key_path")?;
                validate_nonempty_file(
                    self.ca_cert_path.as_deref().unwrap_or_default(),
                    "ca_cert_path",
                )?;
            }
            ProductionRaftSecurityMode::PlaintextForLocalChaos
                if !allow_plaintext_for_local_chaos =>
            {
                return Err(RaftError::InvalidConfig(
                    "plaintext raft transport is allowed only for local chaos tests".to_string(),
                ));
            }
            ProductionRaftSecurityMode::PlaintextForLocalChaos => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftSecurityEnv {
    pub security: ProductionRaftSecurity,
    pub allow_plaintext_for_local_chaos: bool,
}

pub fn production_raft_security_from_env(
    default_auth_token: impl Into<String>,
    default_allow_plaintext_for_local_chaos: bool,
) -> ProductionRaftSecurityEnv {
    production_raft_security_from_lookup(
        default_auth_token,
        default_allow_plaintext_for_local_chaos,
        |key| std::env::var(key).ok(),
    )
}

pub fn production_raft_security_from_lookup<F>(
    default_auth_token: impl Into<String>,
    default_allow_plaintext_for_local_chaos: bool,
    mut lookup: F,
) -> ProductionRaftSecurityEnv
where
    F: FnMut(&str) -> Option<String>,
{
    let auth_token = lookup("TS_RAFT_AUTH_TOKEN").unwrap_or_else(|| default_auth_token.into());
    let mode = lookup("TS_RAFT_SECURITY_MODE")
        .or_else(|| lookup("TS_RAFT_TRANSPORT_SECURITY"))
        .unwrap_or_else(|| "plaintext_for_local_chaos".to_string());
    let allow_plaintext_for_local_chaos = lookup("TS_RAFT_ALLOW_PLAINTEXT")
        .map(|raw| parse_env_bool(&raw, default_allow_plaintext_for_local_chaos))
        .unwrap_or(default_allow_plaintext_for_local_chaos);
    let security = match mode.trim().to_ascii_lowercase().as_str() {
        "mtls" | "mutual_tls" | "mutual-tls" => ProductionRaftSecurity::mtls(
            auth_token,
            lookup("TS_RAFT_CERT_PATH").unwrap_or_default(),
            lookup("TS_RAFT_KEY_PATH").unwrap_or_default(),
            lookup("TS_RAFT_CA_CERT_PATH")
                .or_else(|| lookup("TS_RAFT_CA_PATH"))
                .unwrap_or_default(),
        ),
        "plaintext" | "plaintext_for_local_chaos" | "local_chaos" => {
            ProductionRaftSecurity::plaintext_for_local_chaos(auth_token)
        }
        other => ProductionRaftSecurity::mtls(
            auth_token,
            format!("invalid-security-mode-{other}"),
            "",
            "",
        ),
    };
    ProductionRaftSecurityEnv {
        security,
        allow_plaintext_for_local_chaos,
    }
}

fn parse_env_bool(raw: &str, default: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => true,
        "0" | "false" | "no" | "n" | "off" => false,
        _ => default,
    }
}

fn validate_nonempty_file(path: &str, label: &str) -> Result<(), RaftError> {
    let path = path.trim();
    if path.is_empty() {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS requires {label}"
        )));
    }
    let metadata = fs::metadata(path).map_err(|err| {
        RaftError::InvalidConfig(format!(
            "production raft mTLS {label} is not readable at {path}: {err}"
        ))
    })?;
    if !metadata.is_file() {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS {label} must point to a file: {path}"
        )));
    }
    if metadata.len() == 0 {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS {label} must not be empty: {path}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftNode {
    pub node_id: RaftNodeId,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftRuntimeOptions {
    pub engine: ProductionRaftEngineKind,
    pub shard_id: ShardId,
    pub local_node_id: RaftNodeId,
    pub nodes: Vec<ProductionRaftNode>,
    pub wal_dir: String,
    pub config: RaftConfig,
    pub rpc: RaftRpcRuntimeOptions,
    pub security: ProductionRaftSecurity,
    pub heartbeat_interval_ms: u64,
    pub election_tick_ms: u64,
    pub max_catchup_entries_per_heartbeat: u64,
    pub allow_plaintext_for_local_chaos: bool,
    /// How often the timer loop asks whether the applied raft log has grown past
    /// [`RaftConfig::max_applied_log_bytes`] and should be compacted into a
    /// snapshot. Zero disables the check.
    ///
    /// Nothing used to ask. `maybe_trigger_snapshot` was implemented, and the
    /// config already said to compact at a threshold, but no loop ever called
    /// it -- so the log grew for the life of the process.
    pub snapshot_check_interval_ms: u64,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftProcessSpec {
    pub node_id: RaftNodeId,
    pub addr: String,
    pub wal_dir: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftChaosPlan {
    pub shard_id: ShardId,
    pub nodes: Vec<ProductionRaftProcessSpec>,
    pub partition_pairs: Vec<(RaftNodeId, RaftNodeId)>,
    pub crash_nodes: Vec<RaftNodeId>,
    pub restart_nodes: Vec<RaftNodeId>,
}

impl ProductionRaftChaosPlan {
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.nodes.len() < 3 {
            return Err(RaftError::InvalidConfig(
                "multi-process chaos plan requires at least three raft nodes".to_string(),
            ));
        }
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        for node_id in self
            .crash_nodes
            .iter()
            .chain(self.restart_nodes.iter())
            .copied()
        {
            if !node_ids.contains(&node_id) {
                return Err(RaftError::NodeNotFound(node_id));
            }
        }
        for (left, right) in &self.partition_pairs {
            if !node_ids.contains(left) || !node_ids.contains(right) {
                return Err(RaftError::InvalidConfig(
                    "partition pair references an unknown raft node".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct InstantCompat(std::time::Instant);

impl InstantCompat {
    fn now() -> Self {
        Self(std::time::Instant::now())
    }

    fn elapsed(self) -> Duration {
        self.0.elapsed()
    }
}

#[cfg(feature = "temporal-raft-engine")]
pub mod temporal_raft_integration {
    use super::*;
    use std::collections::BTreeSet;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftConfig;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftLogId {
        pub term: u64,
        pub node_id: RaftNodeId,
        pub index: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum TemporalRaftEntryPayload {
        Blank,
        Normal(Command),
        Membership(TemporalRaftMembership),
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftEntry {
        pub log_id: TemporalRaftLogId,
        pub payload: TemporalRaftEntryPayload,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftNode {
        pub addr: String,
    }

    impl TemporalRaftNode {
        pub fn new(addr: String) -> Self {
            Self { addr }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftMembership {
        pub voter_sets: Vec<BTreeSet<RaftNodeId>>,
        pub nodes: BTreeMap<RaftNodeId, TemporalRaftNode>,
    }

    impl TemporalRaftMembership {
        pub fn new(
            voter_sets: Vec<BTreeSet<RaftNodeId>>,
            nodes: BTreeMap<RaftNodeId, TemporalRaftNode>,
        ) -> Self {
            Self { voter_sets, nodes }
        }

        pub fn voter_ids(&self) -> impl Iterator<Item = RaftNodeId> + '_ {
            self.voter_sets
                .first()
                .into_iter()
                .flat_map(|voters| voters.iter().copied())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftStoredMembership {
        pub last_log_id: Option<TemporalRaftLogId>,
        pub membership: TemporalRaftMembership,
    }

    impl TemporalRaftStoredMembership {
        pub fn new(
            last_log_id: Option<TemporalRaftLogId>,
            membership: TemporalRaftMembership,
        ) -> Self {
            Self {
                last_log_id,
                membership,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftSnapshotMeta {
        pub last_log_id: Option<TemporalRaftLogId>,
        pub last_membership: TemporalRaftStoredMembership,
        pub snapshot_id: String,
    }

    pub trait ExternalRaftEngine {
        fn propose_command(&self, command: Command) -> Result<CommandResponse, RaftError>;
        fn install_snapshot_chunks(
            &self,
            chunks: Vec<InstallSnapshotChunkRequest>,
        ) -> Result<InstallSnapshotChunkResponse, RaftError>;
        fn joint_change(
            &self,
            new_voters: Vec<RaftNodeId>,
        ) -> Result<JointConsensusMembership, RaftError>;
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub enum TemporalRaftRuntimeKind {
        DataNode,
        Metaserver,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftDurableLogRecord {
        pub log_id: u64,
        pub term: u64,
        pub leader_id: RaftNodeId,
        pub index: u64,
        pub shard_id: ShardId,
        pub command: Option<Command>,
        pub membership: Option<Vec<RaftNodeId>>,
        pub checksum: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftDurableSnapshot {
        pub snapshot_id: String,
        pub last_log_index: u64,
        pub last_log_term: u64,
        pub last_leader_id: RaftNodeId,
        pub applied_index: u64,
        pub membership: Vec<RaftNodeId>,
        pub payload_checksum: String,
        pub payload_bytes: Vec<u8>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TemporalRaftDurableState {
        version: u32,
        kind: TemporalRaftRuntimeKind,
        shard_id: ShardId,
        local_node_id: RaftNodeId,
        current_term: u64,
        leader_id: RaftNodeId,
        committed_index: u64,
        applied_index: u64,
        first_index: u64,
        last_index: u64,
        voters: Vec<RaftNodeId>,
        learners: Vec<RaftNodeId>,
        log: Vec<TemporalRaftDurableLogRecord>,
        snapshot: Option<TemporalRaftDurableSnapshot>,
        #[serde(default)]
        storage_apply_fence: RaftStorageApplyFence,
    }

    impl TemporalRaftDurableState {
        fn new(
            kind: TemporalRaftRuntimeKind,
            options: &DataRaftConsensusOptions,
            voters: Vec<RaftNodeId>,
            learners: Vec<RaftNodeId>,
        ) -> Self {
            let mut voters = voters;
            if voters.is_empty() {
                voters = options
                    .peers
                    .iter()
                    .map(|peer| peer.replica_id)
                    .chain(std::iter::once(options.replica_id))
                    .collect();
            }
            voters.sort_unstable();
            voters.dedup();
            let leader_id = voters.first().copied().unwrap_or(options.replica_id);
            let mut state = Self {
                version: 1,
                kind,
                shard_id: options.shard_id,
                local_node_id: options.replica_id,
                current_term: 1,
                leader_id,
                committed_index: options.initial_applied_index,
                applied_index: options.initial_applied_index,
                first_index: options.initial_applied_index.saturating_add(1),
                last_index: options.initial_applied_index,
                voters,
                learners,
                log: Vec::new(),
                snapshot: None,
                storage_apply_fence: RaftStorageApplyFence::default(),
            };
            state.refresh_storage_apply_fence();
            state
        }

        fn snapshot_id(&self) -> Option<&str> {
            self.snapshot
                .as_ref()
                .map(|snapshot| snapshot.snapshot_id.as_str())
        }

        fn storage_epoch(&self) -> u64 {
            self.applied_index.max(
                self.snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.applied_index)
                    .unwrap_or_default(),
            )
        }

        fn refresh_storage_apply_fence(&mut self) {
            let snapshot_id = self.snapshot_id().map(str::to_string);
            let storage_epoch = self.storage_epoch();
            let checksum = raft_storage_apply_fence_checksum(
                self.shard_id,
                self.current_term,
                self.committed_index,
                self.applied_index,
                snapshot_id.as_deref(),
                storage_epoch,
            );
            self.storage_apply_fence = RaftStorageApplyFence {
                shard_id: self.shard_id,
                raft_term: self.current_term,
                committed_index: self.committed_index,
                applied_index: self.applied_index,
                snapshot_id,
                storage_epoch,
                checksum,
            };
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftBackendReport {
        pub kind: TemporalRaftRuntimeKind,
        pub shard_id: ShardId,
        pub local_node_id: RaftNodeId,
        pub durable_log_records: usize,
        pub snapshot_installed: bool,
        pub storage_apply_fence: RaftStorageApplyFence,
        pub storage_apply_fence_valid: bool,
        pub read_index_supported: bool,
        pub membership_change_supported: bool,
        pub leader_transfer_supported: bool,
        pub campaign_supported: bool,
        pub learner_bootstrap_supported: bool,
        pub temporal_raft_entry_boundary: String,
    }

    #[derive(Debug, Clone)]
    pub struct TemporalRaftConsensusBackend {
        options: DataRaftConsensusOptions,
        kind: TemporalRaftRuntimeKind,
        state: TemporalRaftDurableState,
        engine: Option<TemporalEngine>,
        state_path: Option<PathBuf>,
        running: bool,
    }

    impl TemporalRaftConsensusBackend {
        pub fn new_data_node(options: DataRaftConsensusOptions, engine: TemporalEngine) -> Self {
            Self::new(options, TemporalRaftRuntimeKind::DataNode, Some(engine))
        }

        pub fn new_metaserver(options: DataRaftConsensusOptions) -> Self {
            Self::new(options, TemporalRaftRuntimeKind::Metaserver, None)
        }

        fn new(
            options: DataRaftConsensusOptions,
            kind: TemporalRaftRuntimeKind,
            engine: Option<TemporalEngine>,
        ) -> Self {
            let state_path = options.wal_dir.as_ref().map(|dir| {
                dir.join(format!(
                    "temporalraft-{}-{}.json",
                    options.shard_id, options.replica_id
                ))
            });
            let mut voters = options
                .peers
                .iter()
                .filter(|peer| peer.replica_id != options.replica_id)
                .map(|peer| peer.replica_id)
                .collect::<Vec<_>>();
            let learners = if options.bootstrap_as_learner {
                vec![options.replica_id]
            } else {
                voters.push(options.replica_id);
                Vec::new()
            };
            let state = match state_path.as_ref() {
                Some(path) => Self::load_state(path)
                    .unwrap_or_else(|err| {
                        panic!("failed to load durable TemporalRaft state from {path:?}: {err}")
                    })
                    .unwrap_or_else(|| {
                        TemporalRaftDurableState::new(kind, &options, voters, learners)
                    }),
                None => TemporalRaftDurableState::new(kind, &options, voters, learners),
            };
            Self {
                options,
                kind,
                state,
                engine,
                state_path,
                running: false,
            }
        }

        pub fn report(&self) -> TemporalRaftBackendReport {
            TemporalRaftBackendReport {
                kind: self.kind,
                shard_id: self.state.shard_id,
                local_node_id: self.state.local_node_id,
                durable_log_records: self.state.log.len(),
                snapshot_installed: self.state.snapshot.is_some(),
                storage_apply_fence: self.state.storage_apply_fence.clone(),
                storage_apply_fence_valid: validate_temporal_raft_storage_apply_fence(&self.state)
                    .is_ok(),
                read_index_supported: true,
                membership_change_supported: true,
                leader_transfer_supported: true,
                campaign_supported: true,
                learner_bootstrap_supported: true,
                temporal_raft_entry_boundary: std::any::type_name::<TemporalRaftEntry>()
                    .to_string(),
            }
        }

        pub fn temporal_raft_membership(&self) -> TemporalRaftMembership {
            let voter_set = self.state.voters.iter().copied().collect::<BTreeSet<_>>();
            let mut nodes = BTreeMap::new();
            for node_id in self
                .state
                .voters
                .iter()
                .chain(self.state.learners.iter())
                .copied()
            {
                let addr = self
                    .options
                    .peers
                    .iter()
                    .find(|peer| peer.replica_id == node_id)
                    .map(|peer| peer.raft_addr.clone())
                    .unwrap_or_default();
                nodes.insert(node_id, TemporalRaftNode::new(addr));
            }
            TemporalRaftMembership::new(vec![voter_set], nodes)
        }

        pub fn temporal_raft_membership_compat(&self) -> TemporalRaftMembership {
            self.temporal_raft_membership()
        }

        pub fn build_temporal_raft_snapshot_meta(&self) -> TemporalRaftSnapshotMeta {
            let last_log_id = if self.state.last_index == 0 {
                None
            } else {
                Some(temporal_raft_log_id(
                    self.state.current_term,
                    self.state.leader_id,
                    self.state.last_index,
                ))
            };
            let membership = self.temporal_raft_membership();
            TemporalRaftSnapshotMeta {
                last_log_id: last_log_id.clone(),
                last_membership: TemporalRaftStoredMembership::new(last_log_id, membership),
                snapshot_id: self
                    .state
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.snapshot_id.clone())
                    .unwrap_or_else(|| {
                        format!("{}-{}", self.state.shard_id, self.state.last_index)
                    }),
            }
        }

        pub fn build_temporal_raft_snapshot_meta_compat(&self) -> TemporalRaftSnapshotMeta {
            self.build_temporal_raft_snapshot_meta()
        }

        fn load_state(path: &Path) -> Result<Option<TemporalRaftDurableState>, RaftError> {
            if !path.exists() {
                return Ok(None);
            }
            let bytes = fs::read(path).map_err(|err| RaftError::Wal(err.to_string()))?;
            let state = serde_json::from_slice(&bytes).map_err(|err| {
                RaftError::Wal(format!("temporal raft state decode failed: {err}"))
            })?;
            validate_temporal_raft_storage_apply_fence(&state)?;
            Ok(Some(state))
        }

        fn persist_state(&self) -> Result<(), RaftError> {
            let Some(path) = &self.state_path else {
                return Ok(());
            };
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|err| RaftError::Wal(err.to_string()))?;
            }
            let tmp = path.with_extension("json.tmp");
            let bytes = serde_json::to_vec_pretty(&self.state)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
            {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
                use std::io::Write as _;
                file.write_all(&bytes)
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
                file.sync_all()
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
            }
            fs::rename(&tmp, path).map_err(|err| RaftError::Wal(err.to_string()))?;
            if let Some(parent) = path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        }

        fn append_record(
            &mut self,
            command: Option<Command>,
            membership: Option<Vec<RaftNodeId>>,
        ) -> Result<u64, RaftError> {
            self.state.last_index = self.state.last_index.saturating_add(1);
            let index = self.state.last_index;
            let log_id = temporal_raft_log_id(self.state.current_term, self.state.leader_id, index);
            let temporal_raft_entry = match (&command, &membership) {
                (Some(command), _) => TemporalRaftEntry {
                    log_id,
                    payload: TemporalRaftEntryPayload::Normal(command.clone()),
                },
                (None, Some(voters)) => {
                    let membership = membership_from_voters(voters, &self.options.peers);
                    TemporalRaftEntry {
                        log_id,
                        payload: TemporalRaftEntryPayload::Membership(membership),
                    }
                }
                (None, None) => TemporalRaftEntry {
                    log_id,
                    payload: TemporalRaftEntryPayload::Blank,
                },
            };
            let checksum = checksum_temporal_raft_record(
                index,
                self.state.current_term,
                self.state.leader_id,
                &command,
                membership.as_deref(),
            )?;
            let record = TemporalRaftDurableLogRecord {
                log_id: temporal_raft_entry.log_id.index,
                term: self.state.current_term,
                leader_id: self.state.leader_id,
                index,
                shard_id: self.state.shard_id,
                command,
                membership,
                checksum,
            };
            self.state.log.push(record);
            self.state.committed_index = index;
            self.apply_committed_record(index)?;
            self.persist_state()?;
            Ok(index)
        }

        fn apply_committed_record(&mut self, index: u64) -> Result<(), RaftError> {
            let Some(record) = self.state.log.iter().find(|record| record.index == index) else {
                return Ok(());
            };
            if let Some(voters) = &record.membership {
                let mut voters = voters.clone();
                voters.sort_unstable();
                voters.dedup();
                if voters.is_empty() {
                    return Err(RaftError::CannotRemoveLastNode);
                }
                self.state.voters = voters;
            }
            if let (Some(engine), Some(command)) = (&self.engine, &record.command) {
                let response = engine.execute_raft_apply(ExecuteRequest {
                    shard_id: self.state.shard_id,
                    command: command.clone(),
                });
                if !response.status.ok {
                    return Err(RaftError::InvalidDataRaftLog(format!(
                        "temporal raft state-machine apply failed: {} {}",
                        response.status.code, response.status.message
                    )));
                }
            }
            self.state.applied_index = self.state.applied_index.max(index);
            self.state.refresh_storage_apply_fence();
            Ok(())
        }
    }

    impl DataRaftConsensusBackend for TemporalRaftConsensusBackend {
        fn start(&mut self) -> Result<(), RaftError> {
            self.running = true;
            self.persist_state()
        }

        fn stop(&mut self) {
            self.running = false;
            let _ = self.persist_state();
        }

        fn is_leader(&self) -> bool {
            self.running && self.state.leader_id == self.state.local_node_id
        }

        fn status(&self) -> Result<DataRaftStatus, RaftError> {
            Ok(DataRaftStatus {
                running: self.running,
                leader: self.is_leader(),
                learner: self.state.learners.contains(&self.state.local_node_id),
                term: self.state.current_term,
                leader_replica_id: self.state.leader_id,
                committed_index: self.state.committed_index,
                applied_index: self.state.applied_index,
                first_index: self.state.first_index,
                last_index: self.state.last_index,
                pending_config_change_index: 0,
                voter_count: self.state.voters.len() as u64,
                learner_count: self.state.learners.len() as u64,
                fatal_event_count: 0,
                snapshot_creating: false,
                snapshot_loading: false,
            })
        }

        fn propose(&mut self, serialized_entry: Vec<u8>) -> Result<u64, RaftError> {
            if !self.is_leader() {
                return Err(RaftError::NotLeader {
                    node_id: self.state.local_node_id,
                });
            }
            let entry = parse_data_raft_log(&serialized_entry)?;
            if entry.shard_id != self.state.shard_id {
                return Err(RaftError::InvalidDataRaftLog(format!(
                    "shard mismatch: entry={}, backend={}",
                    entry.shard_id, self.state.shard_id
                )));
            }
            self.append_record(Some(entry.command), None)
        }

        fn wait_for_applied_index(&self, index: u64, _timeout_ms: u64) -> Result<(), RaftError> {
            if self.state.applied_index >= index {
                Ok(())
            } else {
                Err(RaftError::AppliedIndexTimeout {
                    node_id: self.state.local_node_id,
                    applied_index: self.state.applied_index,
                    target_index: index,
                    timeout_ms: 0,
                })
            }
        }

        fn trigger_snapshot(&mut self) -> Result<u64, RaftError> {
            let payload_bytes = serde_json::to_vec(&self.state.log)
                .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
            let payload_checksum = hex::encode(Sha256::digest(&payload_bytes));
            let snapshot = TemporalRaftDurableSnapshot {
                snapshot_id: format!(
                    "temporalraft-{}-{}-{}",
                    self.state.shard_id, self.state.current_term, self.state.applied_index
                ),
                last_log_index: self.state.last_index,
                last_log_term: self.state.current_term,
                last_leader_id: self.state.leader_id,
                applied_index: self.state.applied_index,
                membership: self.state.voters.clone(),
                payload_checksum,
                payload_bytes,
            };
            self.state.snapshot = Some(snapshot);
            self.state.first_index = self.state.applied_index.saturating_add(1);
            self.state
                .log
                .retain(|record| record.index >= self.state.first_index);
            self.state.refresh_storage_apply_fence();
            self.persist_state()?;
            Ok(self.state.applied_index)
        }

        fn read_index(&self, _timeout_ms: u64) -> Result<(), RaftError> {
            if !self.running {
                return Err(RaftError::LeaderUnavailable);
            }
            if self.state.committed_index > self.state.applied_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                });
            }
            Ok(())
        }

        fn add_peer(&mut self, peer: DataRaftPeer) -> Result<(), RaftError> {
            if self.state.voters.contains(&peer.replica_id) {
                return Err(RaftError::NodeAlreadyExists(peer.replica_id));
            }
            let mut voters = self.state.voters.clone();
            voters.push(peer.replica_id);
            voters.sort_unstable();
            voters.dedup();
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn add_learner(&mut self, peer: DataRaftPeer) -> Result<(), RaftError> {
            if self.state.voters.contains(&peer.replica_id)
                || self.state.learners.contains(&peer.replica_id)
            {
                return Err(RaftError::NodeAlreadyExists(peer.replica_id));
            }
            self.state.learners.push(peer.replica_id);
            self.state.learners.sort_unstable();
            self.state.learners.dedup();
            self.persist_state()?;
            if peer.auto_promote {
                self.promote_peer(peer.replica_id)
            } else {
                Ok(())
            }
        }

        fn promote_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.learners.contains(&replica_id) {
                return Err(RaftError::NodeNotFound(replica_id));
            }
            let mut voters = self.state.voters.clone();
            voters.push(replica_id);
            self.state.learners.retain(|learner| *learner != replica_id);
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn remove_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.voters.contains(&replica_id) {
                self.state.learners.retain(|learner| *learner != replica_id);
                return self.persist_state();
            }
            if self.state.voters.len() == 1 {
                return Err(RaftError::CannotRemoveLastNode);
            }
            let mut voters = self.state.voters.clone();
            voters.retain(|voter| *voter != replica_id);
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn transfer_leader(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.voters.contains(&replica_id) {
                return Err(RaftError::NodeNotFound(replica_id));
            }
            self.state.leader_id = replica_id;
            self.state.current_term = self.state.current_term.saturating_add(1);
            self.state.refresh_storage_apply_fence();
            self.persist_state()
        }

        fn campaign(&mut self, _timeout_ms: u64, force: bool) -> Result<(), RaftError> {
            if !self.running {
                return Err(RaftError::LeaderUnavailable);
            }
            if !self.state.voters.contains(&self.state.local_node_id) {
                return Err(RaftError::NodeNotFound(self.state.local_node_id));
            }
            if !force && self.state.committed_index > self.state.applied_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                });
            }
            self.state.leader_id = self.state.local_node_id;
            self.state.current_term = self.state.current_term.saturating_add(1);
            self.state.refresh_storage_apply_fence();
            self.persist_state()
        }

        fn can_serve_bounded_stale_read(&self, max_stale_index_lag: u64) -> Result<(), RaftError> {
            let lag = self
                .state
                .committed_index
                .saturating_sub(self.state.applied_index);
            if lag <= max_stale_index_lag {
                Ok(())
            } else {
                Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                })
            }
        }
    }

    pub fn new_temporal_raft_data_node_backend(
        options: DataRaftConsensusOptions,
        engine: TemporalEngine,
    ) -> TemporalRaftConsensusBackend {
        TemporalRaftConsensusBackend::new_data_node(options, engine)
    }

    pub fn new_temporal_raft_metaserver_backend(
        options: DataRaftConsensusOptions,
    ) -> TemporalRaftConsensusBackend {
        TemporalRaftConsensusBackend::new_metaserver(options)
    }

    fn temporal_raft_log_id(term: u64, node_id: RaftNodeId, index: u64) -> TemporalRaftLogId {
        TemporalRaftLogId {
            term,
            node_id,
            index,
        }
    }

    fn membership_from_voters(
        voters: &[RaftNodeId],
        peers: &[DataRaftPeer],
    ) -> TemporalRaftMembership {
        let voter_set = voters.iter().copied().collect::<BTreeSet<_>>();
        let mut nodes = BTreeMap::new();
        for voter in voters {
            let addr = peers
                .iter()
                .find(|peer| peer.replica_id == *voter)
                .map(|peer| peer.raft_addr.clone())
                .unwrap_or_default();
            nodes.insert(*voter, TemporalRaftNode::new(addr));
        }
        TemporalRaftMembership::new(vec![voter_set], nodes)
    }

    fn validate_temporal_raft_storage_apply_fence(
        state: &TemporalRaftDurableState,
    ) -> Result<(), RaftError> {
        let fence = &state.storage_apply_fence;
        if fence == &RaftStorageApplyFence::default()
            && (state.committed_index > 0 || state.applied_index > 0 || state.snapshot.is_some())
        {
            return Err(RaftError::ApplySnapshotFence(
                "missing temporal raft storage apply fence".to_string(),
            ));
        }
        if fence == &RaftStorageApplyFence::default() {
            return Ok(());
        }
        if fence.shard_id != state.shard_id {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence shard {} does not match state shard {}",
                fence.shard_id, state.shard_id
            )));
        }
        if fence.raft_term != state.current_term {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence term {} does not match current term {}",
                fence.raft_term, state.current_term
            )));
        }
        if fence.committed_index != state.committed_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence committed index {} does not match committed index {}",
                fence.committed_index, state.committed_index
            )));
        }
        if fence.applied_index != state.applied_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence applied index {} does not match applied index {}",
                fence.applied_index, state.applied_index
            )));
        }
        if fence.applied_index > fence.committed_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence applied index {} is ahead of committed index {}",
                fence.applied_index, fence.committed_index
            )));
        }
        let snapshot_id = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot_id.clone());
        if fence.snapshot_id != snapshot_id {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence snapshot id {:?} does not match durable snapshot id {:?}",
                fence.snapshot_id, snapshot_id
            )));
        }
        let snapshot_index = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.applied_index)
            .unwrap_or_default();
        if fence.storage_epoch < fence.applied_index || fence.storage_epoch < snapshot_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence epoch {} is behind applied index {} or snapshot index {}",
                fence.storage_epoch, fence.applied_index, snapshot_index
            )));
        }
        let expected = raft_storage_apply_fence_checksum(
            fence.shard_id,
            fence.raft_term,
            fence.committed_index,
            fence.applied_index,
            fence.snapshot_id.as_deref(),
            fence.storage_epoch,
        );
        if fence.checksum != expected {
            return Err(RaftError::ApplySnapshotFence(
                "temporal raft storage fence checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }

    fn checksum_temporal_raft_record(
        index: u64,
        term: u64,
        leader_id: RaftNodeId,
        command: &Option<Command>,
        membership: Option<&[RaftNodeId]>,
    ) -> Result<String, RaftError> {
        let payload = serde_json::to_vec(&(index, term, leader_id, command, membership))
            .map_err(|err| RaftError::Wal(err.to_string()))?;
        Ok(hex::encode(Sha256::digest(payload)))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftReadStrategy {
    RelaxRead,
    LeaseRead,
    ReadIndex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReadOptions {
    pub strategy: RaftReadStrategy,
    pub enable_read_from_follower: bool,
    pub fill_cache: bool,
    pub ignore_write_intent: bool,
    pub wait_millis: u64,
}

impl Default for RaftReadOptions {
    fn default() -> Self {
        Self {
            strategy: RaftReadStrategy::RelaxRead,
            enable_read_from_follower: false,
            fill_cache: true,
            ignore_write_intent: false,
            wait_millis: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataRaftReadMode {
    Leader,
    Linearizable,
    BoundedStale,
    UnsafeAnyReplica,
}

impl FromStr for DataRaftReadMode {
    type Err = RaftError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "leader" => Ok(Self::Leader),
            "linearizable" => Ok(Self::Linearizable),
            "bounded_stale" => Ok(Self::BoundedStale),
            "unsafe_any_replica" => Ok(Self::UnsafeAnyReplica),
            _ => Err(RaftError::InvalidConfig(format!(
                "invalid data_raft_read_mode: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftReadPolicy {
    pub mode: DataRaftReadMode,
    pub bounded_stale_max_index_lag: u64,
    pub read_index_timeout_ms: u64,
}

impl Default for DataRaftReadPolicy {
    fn default() -> Self {
        Self {
            mode: DataRaftReadMode::Leader,
            bounded_stale_max_index_lag: 0,
            read_index_timeout_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftConfig {
    pub election_cycle_tick: u32,
    pub transfer_timeout_tick: u64,
    pub offline_timeout_tick: u64,
    pub lease_duration_ms: u64,
    pub last_lease_duration_ms: u64,
    pub assume_lease_when_start: bool,
    pub max_memory_replicate_log_bytes: u64,
    pub max_disk_replicate_log_num: u64,
    pub max_cache_memory_bytes: u64,
    pub max_apply_batch_bytes: u64,
    pub enable_reorder_queue: bool,
    pub reorder_timeout_us: u64,
    pub reorder_window_size: u64,
    pub max_inflights_apply_task: u64,
    pub max_inflights_replicate: u64,
    pub enable_pre_vote: bool,
    pub prohibits_election: bool,
    pub ignore_witness: bool,
    pub send_snapshot_timeout_ms: u64,
    pub raft_transport_timeout_ms: u64,
    pub wal_sync: bool,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    pub can_trigger_snapshot: bool,
    pub max_applied_log_bytes: u64,
    /// Ceiling on the log kept for a follower that is behind, in bytes.
    ///
    /// Compaction is held while a live follower still needs the entries, so that catching it up
    /// stays a matter of sending entries rather than installing a snapshot. Past this it compacts
    /// anyway: a peer that is this far behind is cheaper to catch up with a snapshot, and one
    /// that never returns must not pin the log open. Zero disables the hold entirely.
    #[serde(default = "default_max_retained_log_bytes")]
    pub max_retained_log_bytes: u64,
    /// P2: how long `propose_distributed_one` waits for the replication quorum before returning
    /// `NoMajority`. Defaults to 5000 ms (the legacy hardcoded deadline); a config that omits the
    /// field also resolves to 5000 so behavior stays byte-identical. Lower it (e.g. 500) so a
    /// lagging/rejecting follower no longer freezes the proposer for a full 5 s.
    #[serde(default = "default_replication_deadline_ms")]
    pub replication_deadline_ms: u64,
    /// How many bytes of state image a routine snapshot may READ for each byte of applied log it
    /// DISCARDS.
    ///
    /// `max_applied_log_bytes` above is admission control on the LOG, and what a snapshot reads is
    /// the STORE: the served index plus every live slab. Against a constant log threshold those
    /// two are unrelated, so the bytes a snapshot reads per byte it discards is linear in the
    /// shard. This bounds that ratio: the configured threshold becomes a FLOOR, raised to
    /// `last_image_bytes / divisor` so a snapshot never reads more than `divisor` bytes of image
    /// per byte of log it frees. It can only ever make a snapshot wait for MORE to discard, never
    /// fire earlier, so a shard below the crossover keeps the cadence it has.
    ///
    /// Zero turns the relative term off and restores the constant threshold exactly.
    #[serde(default = "default_snapshot_image_fraction_divisor")]
    pub snapshot_image_fraction_divisor: u64,
}

fn default_replication_deadline_ms() -> u64 {
    5000
}

fn default_snapshot_image_fraction_divisor() -> u64 {
    cluster_snapshot::SNAPSHOT_IMAGE_FRACTION_DIVISOR
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_cycle_tick: 3,
            transfer_timeout_tick: 3,
            offline_timeout_tick: 10,
            lease_duration_ms: 0,
            last_lease_duration_ms: 0,
            assume_lease_when_start: true,
            max_memory_replicate_log_bytes: 32 * 1024,
            max_disk_replicate_log_num: 64,
            max_cache_memory_bytes: 32 * 1024 * 1024,
            max_apply_batch_bytes: 64 * 1024,
            enable_reorder_queue: true,
            reorder_timeout_us: 3_000,
            reorder_window_size: 128,
            max_inflights_apply_task: 5,
            max_inflights_replicate: 128,
            // On by default: every env-driven entry point in this crate -- the metaserver
            // meta raft, the datanode data raft and the standalone raft node -- builds its
            // config from this default and never names this field, so the default is what
            // they all run. Without pre-vote a node that was partitioned and rejoins bumps
            // its term and forces an election, unseating a leader that was serving fine; a
            // pre-vote candidate first checks it COULD win, and a candidate that could not
            // is rejected without its term ever moving. The check is in-process, not an
            // extra network round. Set it to false explicitly to opt back out.
            enable_pre_vote: true,
            prohibits_election: false,
            ignore_witness: false,
            // A stall budget, not a transfer budget: the clock resets on every chunk, so
            // this is how long a send may make NO progress before the peer is released. A
            // minute of complete silence is a dead transfer, and holding the peer that long
            // rejects every proposal to it for the same minute.
            send_snapshot_timeout_ms: 10_000,
            raft_transport_timeout_ms: 1_000,
            wal_sync: false,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            can_trigger_snapshot: true,
            max_applied_log_bytes: 1024 * 1024 * 1024,
            max_retained_log_bytes: default_max_retained_log_bytes(),
            replication_deadline_ms: default_replication_deadline_ms(),
            snapshot_image_fraction_divisor: default_snapshot_image_fraction_divisor(),
        }
    }
}

impl RaftConfig {
    pub fn validate(&self) -> Result<(), RaftConfigError> {
        if self.election_cycle_tick == 0 {
            return Err(RaftConfigError::InvalidValue("election_cycle_tick"));
        }
        if self.max_memory_replicate_log_bytes == 0 {
            return Err(RaftConfigError::InvalidValue(
                "max_memory_replicate_log_bytes",
            ));
        }
        if self.max_disk_replicate_log_num == 0 {
            return Err(RaftConfigError::InvalidValue("max_disk_replicate_log_num"));
        }
        if self.max_apply_batch_bytes == 0 {
            return Err(RaftConfigError::InvalidValue("max_apply_batch_bytes"));
        }
        if self.max_inflights_replicate == 0 {
            return Err(RaftConfigError::InvalidValue("max_inflights_replicate"));
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftConfigError::InvalidValue("max_segment_bytes"));
        }
        if self.min_keep_segment_num == 0 {
            return Err(RaftConfigError::InvalidValue("min_keep_segment_num"));
        }
        Ok(())
    }
}

fn initial_leader_lease_deadline_ms(config: &RaftConfig) -> u64 {
    if config.lease_duration_ms == 0 {
        u64::MAX
    } else if config.assume_lease_when_start {
        config.lease_duration_ms
    } else {
        0
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftConfigError {
    #[error("invalid raft config value: {0}")]
    InvalidValue(&'static str),
}

#[derive(Debug)]
struct RaftNode {
    id: RaftNodeId,
    role: RaftRole,
    replica_role: RaftReplicaRole,
    current_term: u64,
    voted_for: Option<RaftNodeId>,
    commit_index: u64,
    alive: bool,
    log: Vec<RaftLogEntry>,
    installed_snapshot: Option<RaftSnapshot>,
    applied_index: u64,
    applied: BTreeSet<u64>,
    // Monotonic exactly-once floor: the highest raft index ever applied. It is NEVER
    // lowered by a log truncation, so an entry at or below it is never re-executed even
    // if `applied`/`applied_index` were rewound.
    max_applied_index: u64,
    engine: TemporalEngine,
    pipeline_state: RaftPeerPipelineRuntimeState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JointConsensusMembership {
    pub old_voters: Vec<RaftNodeId>,
    pub new_voters: Vec<RaftNodeId>,
}

#[derive(Debug)]
struct PendingSnapshotChunks {
    shard_id: ShardId,
    last_included_term: u64,
    last_included_index: u64,
    chunks: Vec<Option<Vec<RaftLogEntry>>>,
    // S2: opaque engine state image reassembled from the chunk stream (carried on chunk 0 of an
    // image-based snapshot). `None` for the classic entry-carrying stream.
    state_image: Option<RaftSnapshotStateImage>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftError {
    #[error("invalid raft config: {0}")]
    InvalidConfig(String),
    #[error("leader is not available")]
    LeaderUnavailable,
    #[error("not enough live replicas for majority: live={live}, required={required}")]
    NoMajority { live: usize, required: usize },
    #[error("node not found: {0}")]
    NodeNotFound(RaftNodeId),
    #[error("node already exists: {0}")]
    NodeAlreadyExists(RaftNodeId),
    #[error("cannot remove the last node")]
    CannotRemoveLastNode,
    #[error("replica {replica_id} is behind leader commit index: replica={replica_commit_index}, leader={leader_commit_index}")]
    ReplicaLagging {
        replica_id: RaftNodeId,
        replica_commit_index: u64,
        leader_commit_index: u64,
    },
    #[error("node {node_id} did not apply raft index {target_index} within {timeout_ms}ms: applied={applied_index}")]
    AppliedIndexTimeout {
        node_id: RaftNodeId,
        applied_index: u64,
        target_index: u64,
        timeout_ms: u64,
    },
    #[error("replica {replica_id} has committed but not applied up to the read frontier: applied={replica_applied_index}, required={required_index}")]
    ReplicaApplyLagging {
        replica_id: RaftNodeId,
        replica_applied_index: u64,
        required_index: u64,
    },
    #[error("snapshot shard mismatch: snapshot={snapshot_shard_id}, cluster={cluster_shard_id}")]
    SnapshotShardMismatch {
        snapshot_shard_id: ShardId,
        cluster_shard_id: ShardId,
    },
    #[error("stale snapshot cannot overwrite newer local raft state: snapshot_index={snapshot_index}, local_commit_index={local_commit_index}")]
    StaleSnapshot {
        snapshot_index: u64,
        local_commit_index: u64,
    },
    #[error("raft apply/snapshot fence is inconsistent: {0}")]
    ApplySnapshotFence(String),
    #[error("raft log entry too large: bytes={bytes}, limit={limit}")]
    LogEntryTooLarge { bytes: u64, limit: u64 },
    #[error("raft append pipeline backpressure for node {node_id}: entry_bytes={entry_bytes} does not fit in what is left of byte_limit={byte_limit} (inflight_entries={inflight_entries}, inflight_bytes={inflight_bytes}, entry_limit={entry_limit})")]
    AppendBackpressure {
        node_id: RaftNodeId,
        /// What the pipeline is actually holding. These are the peer's live window, not the
        /// entry that was refused -- reporting the entry's size as `inflight_bytes` said the
        /// window held bytes it did not, and the refusal reads as an accounting bug that is
        /// not there.
        inflight_entries: u64,
        inflight_bytes: u64,
        /// The single entry that did not fit in what remained. This is the number that
        /// explains the refusal, and it has nowhere else to be reported.
        entry_bytes: u64,
        entry_limit: u64,
        byte_limit: u64,
    },
    #[error("raft snapshot sender backpressure for node {node_id}: snapshot transfer already in progress")]
    SnapshotBackpressure { node_id: RaftNodeId },
    #[error("node {node_id} is not leader")]
    NotLeader { node_id: RaftNodeId },
    #[error("election is prohibited by raft config")]
    ElectionProhibited,
    #[error("raft wal error: {0}")]
    Wal(String),
    #[error("raft transport error: {0}")]
    Transport(String),
    #[error("joint consensus is already in progress")]
    JointConsensusInProgress,
    #[error("no joint consensus is in progress")]
    NoJointConsensus,
    #[error("invalid snapshot chunk: {0}")]
    InvalidSnapshotChunk(String),
    #[error("external snapshot reference is required: snapshot_bytes={snapshot_bytes}, threshold_bytes={threshold_bytes}")]
    ExternalSnapshotRequired {
        snapshot_bytes: u64,
        threshold_bytes: u64,
    },
    #[error("snapshot serialization failed: {0}")]
    SnapshotEncoding(String),
    #[error("snapshot store error: {0}")]
    SnapshotStore(String),
    #[error("invalid data raft log: {0}")]
    InvalidDataRaftLog(String),
    #[error("invalid data raft command: {0}")]
    InvalidDataRaftCommand(String),
}

pub fn serialize_data_raft_log(entry: &DataRaftLogCodecEntry) -> Result<Vec<u8>, RaftError> {
    if entry.wal_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "wal sequence must be nonzero".to_string(),
        ));
    }
    let payload = serde_json::to_vec(&entry.command)
        .map_err(|err| RaftError::InvalidDataRaftLog(err.to_string()))?;
    if payload.is_empty() {
        return Err(RaftError::InvalidDataRaftLog(
            "command payload is empty".to_string(),
        ));
    }
    let log_size = if entry.log_size == 0 {
        payload.len() as u64
    } else {
        entry.log_size
    };
    let mut bytes = Vec::with_capacity(DATA_RAFT_LOG_HEADER_LEN + payload.len());
    push_u32_le(&mut bytes, DATA_RAFT_LOG_MAGIC);
    push_u32_le(&mut bytes, DATA_RAFT_CODEC_VERSION);
    push_u64_le(&mut bytes, entry.shard_id);
    push_u64_le(&mut bytes, entry.raft_index);
    push_u64_le(&mut bytes, entry.log_id);
    push_u64_le(&mut bytes, log_size);
    push_u64_le(&mut bytes, entry.wal_sequence);
    push_u64_le(&mut bytes, payload.len() as u64);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn parse_data_raft_log(bytes: &[u8]) -> Result<DataRaftLogCodecEntry, RaftError> {
    if bytes.len() < DATA_RAFT_LOG_HEADER_LEN {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "truncated header: bytes={}, need={}",
            bytes.len(),
            DATA_RAFT_LOG_HEADER_LEN
        )));
    }
    let magic = read_u32_le(bytes, 0)?;
    if magic != DATA_RAFT_LOG_MAGIC {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "bad magic: {magic:#x}"
        )));
    }
    let version = read_u32_le(bytes, 4)?;
    if version != DATA_RAFT_CODEC_VERSION {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "unsupported version: {version}"
        )));
    }
    let shard_id = read_u64_le(bytes, 8)?;
    let raft_index = read_u64_le(bytes, 16)?;
    let log_id = read_u64_le(bytes, 24)?;
    let log_size = read_u64_le(bytes, 32)?;
    let wal_sequence = read_u64_le(bytes, 40)?;
    let payload_size = read_u64_le(bytes, 48)?;
    if wal_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "wal sequence must be nonzero".to_string(),
        ));
    }
    if payload_size > u32::MAX as u64 {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "payload too large: {payload_size}"
        )));
    }
    let payload_size_usize = payload_size as usize;
    if bytes.len() - DATA_RAFT_LOG_HEADER_LEN != payload_size_usize {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "payload size mismatch: header={}, actual={}",
            payload_size,
            bytes.len() - DATA_RAFT_LOG_HEADER_LEN
        )));
    }
    let command = serde_json::from_slice(&bytes[DATA_RAFT_LOG_HEADER_LEN..])
        .map_err(|err| RaftError::InvalidDataRaftLog(err.to_string()))?;
    Ok(DataRaftLogCodecEntry {
        shard_id,
        raft_index,
        log_id,
        log_size,
        wal_sequence,
        command,
    })
}

pub fn serialize_data_raft_command(
    entry: &DataRaftCommandCodecEntry,
) -> Result<Vec<u8>, RaftError> {
    if entry.shard_id == 0 {
        return Err(RaftError::InvalidDataRaftCommand(
            "shard id must be nonzero".to_string(),
        ));
    }
    if entry.commands.is_empty() {
        return Err(RaftError::InvalidDataRaftCommand(
            "command batch is empty".to_string(),
        ));
    }
    let payload = serde_json::to_vec(&entry.commands)
        .map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if payload.is_empty() {
        return Err(RaftError::InvalidDataRaftCommand(
            "command payload is empty".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(DATA_RAFT_COMMAND_HEADER_LEN + payload.len());
    push_u32_le(&mut bytes, DATA_RAFT_COMMAND_MAGIC);
    push_u32_le(&mut bytes, DATA_RAFT_CODEC_VERSION);
    push_u64_le(&mut bytes, entry.shard_id);
    push_u64_le(&mut bytes, entry.raft_index);
    push_u64_le(&mut bytes, entry.request_id);
    push_u64_le(&mut bytes, payload.len() as u64);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn parse_data_raft_command(bytes: &[u8]) -> Result<DataRaftCommandCodecEntry, RaftError> {
    if bytes.len() < DATA_RAFT_COMMAND_HEADER_LEN {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "truncated header: bytes={}, need={}",
            bytes.len(),
            DATA_RAFT_COMMAND_HEADER_LEN
        )));
    }
    let magic =
        read_u32_le(bytes, 0).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if magic != DATA_RAFT_COMMAND_MAGIC {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "bad magic: {magic:#x}"
        )));
    }
    let version =
        read_u32_le(bytes, 4).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if version != DATA_RAFT_CODEC_VERSION {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "unsupported version: {version}"
        )));
    }
    let shard_id =
        read_u64_le(bytes, 8).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let raft_index =
        read_u64_le(bytes, 16).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let request_id =
        read_u64_le(bytes, 24).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let payload_size =
        read_u64_le(bytes, 32).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if shard_id == 0 {
        return Err(RaftError::InvalidDataRaftCommand(
            "shard id must be nonzero".to_string(),
        ));
    }
    let payload_size_usize = usize::try_from(payload_size).map_err(|_| {
        RaftError::InvalidDataRaftCommand(format!("payload too large: {payload_size}"))
    })?;
    if bytes.len() - DATA_RAFT_COMMAND_HEADER_LEN != payload_size_usize {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "payload size mismatch: header={}, actual={}",
            payload_size,
            bytes.len() - DATA_RAFT_COMMAND_HEADER_LEN
        )));
    }
    let commands = serde_json::from_slice(&bytes[DATA_RAFT_COMMAND_HEADER_LEN..])
        .map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    Ok(DataRaftCommandCodecEntry {
        shard_id,
        raft_index,
        request_id,
        commands,
    })
}

fn push_u32_le(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64_le(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32, RaftError> {
    let end = offset.saturating_add(4);
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| RaftError::InvalidDataRaftLog("truncated u32".to_string()))?;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("u32 slice length"),
    ))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64, RaftError> {
    let end = offset.saturating_add(8);
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| RaftError::InvalidDataRaftLog("truncated u64".to_string()))?;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("u64 slice length"),
    ))
}

pub fn estimate_snapshot_bytes(snapshot: &RaftSnapshot) -> Result<u64, RaftError> {
    serde_json::to_vec(snapshot)
        .map(|bytes| bytes.len() as u64)
        .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))
}

pub fn decide_snapshot_transfer(
    snapshot: &RaftSnapshot,
    policy: RaftSnapshotTransferPolicy,
    external_snapshot_ref: Option<RaftExternalSnapshotRef>,
) -> Result<RaftSnapshotTransferDecision, RaftError> {
    let threshold = policy.external_threshold_bytes.max(1);
    let snapshot_bytes = estimate_snapshot_bytes(snapshot)?;
    if snapshot_bytes <= threshold && policy.allow_peer_streaming {
        return Ok(RaftSnapshotTransferDecision {
            mode: RaftSnapshotTransferMode::PeerStreaming,
            snapshot_bytes,
            threshold_bytes: threshold,
            external_snapshot_ref: None,
        });
    }
    if policy.allow_external_store {
        let Some(snapshot_ref) = external_snapshot_ref else {
            return Err(RaftError::ExternalSnapshotRequired {
                snapshot_bytes,
                threshold_bytes: threshold,
            });
        };
        return Ok(RaftSnapshotTransferDecision {
            mode: RaftSnapshotTransferMode::ExternalStore,
            snapshot_bytes,
            threshold_bytes: threshold,
            external_snapshot_ref: Some(snapshot_ref),
        });
    }
    Err(RaftError::ExternalSnapshotRequired {
        snapshot_bytes,
        threshold_bytes: threshold,
    })
}

pub fn raft_external_ref_from_snapshot_ref(snapshot_ref: &SnapshotRef) -> RaftExternalSnapshotRef {
    RaftExternalSnapshotRef {
        uri: snapshot_ref.uri.clone(),
        checksum: snapshot_ref.checksum.clone(),
        byte_size: snapshot_ref.byte_size,
    }
}

pub fn shard_snapshot_ref_from_snapshot_ref(snapshot_ref: &SnapshotRef) -> ShardSnapshotRef {
    ShardSnapshotRef {
        uri: snapshot_ref.uri.clone(),
        checksum: snapshot_ref.checksum.clone(),
        byte_size: snapshot_ref.byte_size,
        last_log_index: snapshot_ref.last_log_index,
        created_at_ms: snapshot_ref.created_at.timestamp_millis().max(0) as u64,
    }
}

fn validate_downloaded_snapshot_ref(
    expected_shard_id: ShardId,
    snapshot_ref: &ShardSnapshotRef,
    manifest: &temporalstore_snapshot::SnapshotManifest,
    index_bytes: &[u8],
) -> Result<(), RaftError> {
    if manifest.shard_id != expected_shard_id {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot shard mismatch: manifest={}, ref={}",
            manifest.shard_id, expected_shard_id
        )));
    }
    if manifest.last_log_index != snapshot_ref.last_log_index {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot log index mismatch: manifest={}, ref={}",
            manifest.last_log_index, snapshot_ref.last_log_index
