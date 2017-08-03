// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{ShardStatInfo, ShardCanonicalStorageStats};
use crate::types::{ShardId, Status};

/// TS_META_ADMIN_TOKEN: shared secret for the metaserver's HTTP surface.
///
/// When the metaserver starts with this set, every route except the liveness
/// probes (`/health`, `/readiness`, `/metrics` and its alias) requires
/// `Authorization: Bearer <token>`; when unset, the surface stays open, which
/// is the previous behavior. The same variable is what the metaserver's
/// clients (proxy, datanode, SDK topology sync) read to attach the credential,
/// so one value configures a whole deployment.
pub fn admin_auth_token() -> Option<String> {
    std::env::var("TS_META_ADMIN_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

/// The ready-to-append header line for metaserver-bound requests, from
/// TS_META_ADMIN_TOKEN. Empty when unset, so callers can attach it
/// unconditionally.
pub fn admin_auth_header() -> String {
    admin_auth_token()
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default()
}
#[path = "meta/meta_partitioning.rs"]
mod partitioning;
#[path = "meta/meta_lifecycle.rs"]
mod lifecycle;
mod snapshot_ops;
mod node_reports;
mod state_setters;
#[path = "meta/meta_registration.rs"]
mod registration;
mod table_ops;
mod topology_helpers;
mod auto_rebalance;
mod failure_detector;
#[path = "meta/meta_location.rs"]
mod location;
mod placement_rebalance;
mod raft_failover;
mod shard_check;
#[path = "meta/meta_retention.rs"]
mod retention;
mod proxy_groups;
mod subsystem_metrics;
mod event_bus;
use self::partitioning::*;
use self::topology_helpers::*;
pub use self::auto_rebalance::{
    compute_auto_rebalance, AutoRebalanceOptions, ShardReassignment, ShardReassignmentReason,
};
pub use self::failure_detector::{
    plan_conviction, AdaptiveConvictionReport, ConvictionCandidate, ConvictionPlan,
    ConvictionPolicy, DamageSeverity, Diagnosis, FailureDetectorOptions, LocationDamage,
    MetaFailureDetector,
};
pub use self::placement_rebalance::{
    compute_placement_aware_rebalance, PlacementTarget, ShardPlacement,
};
pub use self::location::{separated_from, separation_ladder, Location};
pub use self::raft_failover::{compute_raft_failover_triggers, RaftFailoverTrigger};
pub use self::shard_check::{
    ShardCheckOptions, ShardCheckReport, ShardChecker, ShardDivergence,
};
pub use self::proxy_groups::{
    plan_proxy_calibration, DropProxyGroupRequest, ListProxyGroupsResponse, ProxyAttachment,
    ProxyCalibrationOptions, ProxyCalibrationPlan, ProxyCalibrationReport, ProxyGroupInfo,
    ProxyGroupShortfall, PutProxyGroupRequest,
};
pub use self::subsystem_metrics::{SubsystemMetrics, TIER_PROXY, TIER_SERVER};
pub use self::event_bus::{TopologyNotice, TopologySubscription, SUBSCRIBER_QUEUE_DEPTH};
pub use self::retention::{
    plan_freeze_aging, plan_meta_retention, FreezeAgingOptions, FreezeAgingPlan,
    FreezeAgingReport, MetaRetentionOptions, MetaRetentionPlan, MetaRetentionReport,
    RetentionCandidate,
};

/// Why a resource was frozen.
///
/// Freezing recorded no reason at all, so the metaserver could not tell an
/// operator taking a node out for maintenance from the failure detector
/// convicting one that stopped answering — even though the two want opposite
/// recovery behaviour. A maintenance freeze ends when the operator says so; a
/// conviction should not end merely because the convicted node asked.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FreezeReason {
    /// No reason was recorded. Either the resource is not frozen, or it was
    /// frozen before reasons existed.
    #[default]
    Unspecified,
    /// An operator froze it explicitly, typically for maintenance.
    Operator,
    /// The failure detector convicted it for going silent.
    Unresponsive,
    /// The metaserver observed it restart in place, so it no longer holds what
    /// the metaserver believes it holds.
    Restarted,
    /// It announced its own shutdown, so it was taken out of service before the
    /// failure detector could notice the silence.
    Stopping,
}

impl FreezeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Operator => "operator",
            Self::Unresponsive => "unresponsive",
            Self::Restarted => "restarted",
            Self::Stopping => "stopping",
        }
    }

    /// True when the metaserver, not an operator, decided this resource was
    /// unhealthy. These are the freezes a resource must not clear for itself.
    pub fn is_conviction(self) -> bool {
        // `Stopping` is deliberately absent. A node that announced its own
        // shutdown is expected back, and locking it out of re-registration
        // would turn every clean restart into an operator ticket.
        matches!(self, Self::Unresponsive | Self::Restarted)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetaEntityState {
    Normal,
    Frozen,
    Dropped,
}

impl Default for MetaEntityState {
    fn default() -> Self {
        Self::Normal
    }
}

impl MetaEntityState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Frozen => "frozen",
            Self::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLocation {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// Whether this shard is being served.
    ///
    /// There was previously no way to take one shard out of service: the only
    /// lever was freezing its whole table, which stops every other shard in it
    /// too. A `Frozen` shard keeps its owner entry so it can be returned to
    /// service, but stops being routed to, and the planners leave it alone --
    /// an operator who froze a shard does not want rebalancing to move it or
    /// the divergence check to "repair" it.
    ///
    /// Defaults to `Normal`, so shard entries written before this existed load
    /// as serving.
    #[serde(default)]
    pub state: MetaEntityState,
    #[serde(default)]
    pub latest_snapshot: Option<ShardSnapshotRef>,
    /// When this shard was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
    /// Where this shard in particular should live.
    ///
    /// A table can express a preferred location, and every shard in it inherits
    /// that. There was no way to say anything about one shard: a single hot
    /// shard that wants its own hardware, or one shard whose data has to stay
    /// somewhere its siblings need not, had to be expressed by pinning the
    /// whole table or not at all.
    ///
    /// Empty means "whatever the table says", which is what every existing
    /// shard means.
    #[serde(default)]
    pub preferred_location: String,
}

/// Take one shard out of service, or return it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStateRequest {
    pub shard_id: ShardId,
}

/// Where a single shard should live. An empty location releases the pin and
/// returns the shard to whatever its table prefers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardPinRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardRequest {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
    pub last_log_index: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishShardSnapshotRequest {
    pub shard_id: ShardId,
    pub snapshot: ShardSnapshotRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetShardResponse {
    pub status: Status,
    pub location: Option<ShardLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckResponse {
    pub status: Status,
}

/// One memory/CPU domain of a datanode's hardware.
///
/// A server is not uniform: it is some number of domains, each with its own
/// cores and its own memory. The metaserver had no way to know that, so it
/// could not tell a machine that is one large domain from a machine that is
/// four smaller ones -- which is the difference between one big placement
/// target and four that fail independently for memory pressure.
///
/// Recorded here, reported by `/servers`, and carried in snapshots. Placement
/// still treats a server as one target; this is what a placement that wanted to
/// be finer-grained would have to read.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NumaNode {
    /// Index within the reporting server, as the server numbers them.
    pub id: u64,
    /// The cores in this domain, in the form the server reports them
    /// (comma or space separated).
    #[serde(default)]
    pub cpu_list: String,
    #[serde(default)]
    pub memory_size_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterServerRequest {
    pub server_addr: String,
    #[serde(default)]
    pub node_id: u64,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub binary_version: String,
    /// The memory/CPU domains this server is made of. Empty from a server that
    /// does not report them, which is every server built before this existed.
    #[serde(default)]
    pub numa_nodes: Vec<NumaNode>,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatRequest {
    pub server_addr: String,
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub binary_version: String,
    #[serde(default)]
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatResponse {
    pub status: Status,
    pub forbid_auto_register: bool,
    #[serde(default)]
    pub topology_version: u64,
    #[serde(default)]
    pub server_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    pub key_count: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatLoad {
    pub shard_id: ShardId,
    #[serde(alias = "partition_info")]
    pub shard_stat_info: ShardStatInfo,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerRuntimeLoad {
    pub queue_depth: usize,
    pub background_queue_depth: usize,
    pub queued_shard_count: usize,
    pub running_shard_count: usize,
    pub dirty_object_count: usize,
    pub dirty_shard_count: usize,
    pub rejected_total: u64,
    pub rejected_background_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
    pub dump_runs: u64,
    pub compaction_runs: u64,
    pub gc_runs: u64,
    pub storage_lifecycle_runs: u64,
    #[serde(default)]
    pub last_meta_topology_version: u64,
    #[serde(default)]
    pub meta_heartbeat_consecutive_failures: u64,
    #[serde(default)]
    pub meta_forbid_auto_register: bool,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerShardServingState {
    pub shard_id: ShardId,
    pub serving_state: String,
    pub worker_index: usize,
    pub worker_threads: usize,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub cache_memory_bytes: u64,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    pub block_store_bytes_written: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub dirty_object_count: u64,
    pub dirty_bucket_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMetaInfo {
    pub server_addr: String,
    pub node_id: u64,
    pub location: String,
    /// The hardware shape this server declared when it registered.
    #[serde(default)]
    pub numa_nodes: Vec<NumaNode>,
    pub state: MetaEntityState,
    pub last_heartbeat_ms: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    /// Why this server was frozen, or `Unspecified` when it is not.
    #[serde(default)]
    pub freeze_reason: FreezeReason,
    pub boot_time_ms: u64,
    /// The boot time the metaserver anchored on for this server: the first
    /// non-zero boot time it heartbeated after registering. `boot_time_ms`
    /// tracks whatever the latest heartbeat claimed; this one does not move, so
    /// the two disagreeing is the signal that the process restarted.
    #[serde(default)]
    pub reported_boot_time_ms: u64,
    /// Set once the server heartbeats a boot time different from the anchored
    /// one, meaning the process restarted without re-registering. Sticky until
    /// the server registers again, because a restarted datanode has lost the
    /// shards the metaserver still believes it is serving.
    #[serde(default)]
    pub reboot_detected: bool,
    /// Set once this server has been seen reporting `shard_states` at all.
    /// Until then an empty report is indistinguishable from an old build that
    /// does not send them, so the shard-divergence check declines to judge it.
    #[serde(default)]
    pub reports_shard_states: bool,
    /// When this server was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
    /// What `shard_loads` and `shard_states` add up to, summarised when the
    /// server reports them.
    ///
    /// Placement needs three numbers out of those two lists: the total keys,
    /// the total memory, and the worst serving state. It used to add them up
    /// from scratch inside every topology request -- walking every shard of
    /// every live server, twice for the loads and once for the states, to
    /// re-derive figures that only change when a heartbeat arrives. A hundred
    /// servers of fifty shards is fifteen thousand iterations per request, for
    /// an answer identical to the last request's.
    #[serde(default)]
    pub load_key_count: u64,
    #[serde(default)]
    pub load_memory_bytes: u64,
    #[serde(default)]
    pub worst_shard_state_penalty: u8,
    /// What this server's shards add up to, from the states it reports.
    ///
    /// Every heartbeat carries `total_records` and `storage_bytes` for each
    /// shard the server holds, and nothing read either. The metaserver knew how
    /// much data sat on every node and had no way to say so -- `/metrics`
    /// counted shards and never their size.
    ///
    /// Summed here rather than at scrape time, like the placement figures
    /// beside them: a scrape should not walk every shard of every server.
    #[serde(default)]
    pub reported_record_count: u64,
    #[serde(default)]
    pub reported_storage_bytes: u64,
    pub binary_version: String,
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleServerReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleResourceReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
    pub frozen_proxies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FreezeStaleServersRequest {
    pub stale_after_ms: u64,
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModePolicy {
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

/// Names held back from creation.
///
/// A name that is reserved cannot be used to create a namespace or a table.
/// Existing ones are left alone: reserving a name is a statement about what may
/// be created from now on, not a way to delete something already serving.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservedNames {
    /// Namespaces nobody may create.
    #[serde(default)]
    pub namespaces: BTreeSet<String>,
    /// Table names nobody may create, in any namespace.
    #[serde(default)]
    pub tables: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservedNamesResponse {
    pub status: Status,
    pub reserved: ReservedNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModeReport {
    pub status: Status,
    pub blocked_servers: Vec<String>,
    pub blocked_proxies: Vec<String>,
    pub server_count: usize,
    pub proxy_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterProxyRequest {
    pub proxy_addr: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
    /// When the metaserver first saw this, stamped by the metaserver on the way
    /// in and carried here so a replayed registration keeps the original time
    /// rather than restamping itself to the moment of the replay. Anything a
    /// caller puts here is overwritten.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatRequest {
    pub proxy_addr: String,
    /// Milliseconds-since-epoch when this proxy process started. Lets the
    /// metaserver see a RESTART: the address is unchanged and the heartbeats
    /// never stop, so without this an in-place reboot is invisible and a proxy
    /// that came back with an empty route cache and a reset config looks
    /// identical to one that has been up for days. Datanodes already report it
    /// (`ServerHeartbeatRequest::boot_time_ms`); proxies were the gap.
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatResponse {
    pub status: Status,
    pub config_changed: bool,
    pub namespace: String,
    pub config_version: u64,
    #[serde(default)]
    pub serving_mode: String,
    /// None means the metaserver has no opinion, which today is always: there is no
    /// per-proxy drop_percent in the meta model at all -- the one that exists is a TABLE
    /// serving option. As a bare `u8` this field could not say that, so it said 0, and the
    /// proxy applied it and lifted whatever drain an operator had put in force.
    #[serde(default)]
    pub drop_percent: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetaInfo {
    pub proxy_addr: String,
    pub namespace: String,
    /// The proxy group this proxy is attached to, or empty when it is idle.
    ///
    /// Assigned by the metaserver rather than declared by the proxy, so the
    /// cluster owns the assignment and can repair it. `namespace` above follows
    /// from the group while attached.
    #[serde(default)]
    pub group: String,
    pub location: String,
    pub state: MetaEntityState,
    pub config_version: u64,
    pub last_heartbeat_ms: u64,
    /// How many heartbeats this proxy has sent since it registered.
    ///
    /// Separate from `last_heartbeat_ms` because registration stamps that clock, so it cannot
    /// answer "has this proxy ever reported in" -- every registered proxy reads non-zero from
    /// the moment it appears. The staleness checks want the timestamp; the question of whether
    /// a proxy is capacity or merely a registration wants this.
    #[serde(default)]
    pub heartbeats_total: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    /// Why this proxy was frozen, or `Unspecified` when it is not.
    #[serde(default)]
    pub freeze_reason: FreezeReason,
    pub binary_version: String,
    /// Boot time last reported by this proxy; `0` when it has never reported one.
    /// When this proxy was first registered.
    ///
    /// `last_heartbeat_ms` is reset every time it registers again, and
    /// `boot_time_ms` is when its process started -- neither answers how long
    /// it has been part of the cluster. Registering again keeps the original:
    /// a node that restarted has not newly joined.
    #[serde(default)]
    pub registered_at_ms: u64,
    /// Mirrors `ServerMetaInfo::boot_time_ms`.
    #[serde(default)]
    pub boot_time_ms: u64,
    /// How many times this proxy has come back with a different boot time while
    /// still registered -- i.e. observed in-place restarts.
    #[serde(default)]
    pub restart_count: u64,
}

/// Which node leads the metaserver's raft group, and the address it was
/// configured with.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MetaRaftLeaderResponse {
    pub status: Status,
    pub leader_id: crate::raft::RaftNodeId,
    /// Empty when there is no leader, or none with a configured address.
    pub addr: String,
    /// Whether this process is that node, so a caller can tell it has arrived.
    pub is_local: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddNamespaceRequest {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceMetaInfo {
    pub namespace: String,
    pub table_count: usize,
    pub state: MetaEntityState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddTableRequest {
    pub namespace: String,
    pub table_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    #[serde(default = "default_replica_count")]
    pub replica_count: u64,
    #[serde(default)]
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteTableRequest {
    pub namespace: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub shard_count: Option<u64>,
    #[serde(default)]
    pub replica_count: Option<u64>,
    #[serde(default)]
    pub first_shard_id: Option<ShardId>,
    #[serde(default)]
    pub partition_version: Option<u32>,
    #[serde(default)]
    pub serving_options: Option<TableServingOptionsPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptions {
    #[serde(default = "default_pin_primary")]
    pub pin_primary: bool,
    #[serde(default = "default_replica_read_policy")]
    pub replica_read_policy: String,
    #[serde(default)]
    pub preferred_location: String,
    #[serde(default)]
    pub drop_percent: u8,
    #[serde(default = "default_max_read_retries")]
    pub max_read_retries: u32,
    #[serde(default)]
    pub max_write_retries: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_continuous_failed_time_ms")]
    pub continuous_failed_time_ms: u64,
    #[serde(default = "default_table_io_timeout_ms")]
    pub io_timeout_ms: u64,
    #[serde(default = "default_table_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// The fields this table set for itself, by name.
    ///
    /// Which fields a table has spoken for is not recoverable from the values: the
    /// patch that carries them knows, and flattening it into this struct used to
    /// throw that away. Records written before this field carry none, and are read
    /// exactly as they were before -- see `table_decides`.
    #[serde(default)]
    pub set_fields: BTreeSet<String>,
}

/// A field of `TableServingOptions`, for naming one without spelling it.
///
/// The names go on the wire as plain strings, so a reader that meets a field it does
/// not know simply carries it; but every name a caller can ask for comes from here,
/// so a misspelling is a compile error rather than a silent "the table did not set
/// this".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableServingField {
    PinPrimary,
    ReplicaReadPolicy,
    PreferredLocation,
    DropPercent,
    MaxReadRetries,
    MaxWriteRetries,
    RetryBackoffMs,
    ContinuousFailedTimeMs,
    IoTimeoutMs,
    ConnectTimeoutMs,
}

impl TableServingField {
    pub fn name(self) -> &'static str {
        match self {
            Self::PinPrimary => "pin_primary",
            Self::ReplicaReadPolicy => "replica_read_policy",
            Self::PreferredLocation => "preferred_location",
            Self::DropPercent => "drop_percent",
            Self::MaxReadRetries => "max_read_retries",
            Self::MaxWriteRetries => "max_write_retries",
            Self::RetryBackoffMs => "retry_backoff_ms",
            Self::ContinuousFailedTimeMs => "continuous_failed_time_ms",
            Self::IoTimeoutMs => "io_timeout_ms",
            Self::ConnectTimeoutMs => "connect_timeout_ms",
        }
    }
}

impl TableServingOptions {
    /// Whether this table means to decide `field` itself, rather than leaving it to
    /// whatever the calling client was configured with.
    ///
    /// A table that set the field decides it. Records written before `set_fields`
    /// existed say nothing about what was set, so for those the only signal left is
    /// that the value differs from the default -- which is what every caller used to
    /// rely on, and which cannot express a table deliberately choosing a default
    /// value. `drop_percent: 0` means "never shed this table" and
    /// `max_write_retries: 0` means "never retry a write here"; both equal the
    /// default, so both were quietly replaced by the client's own setting. The two
    /// options whose whole purpose is to hold something back were the two that could
    /// not be said.
    pub fn table_decides(&self, field: TableServingField) -> bool {
        if self.set_fields.contains(field.name()) {
            return true;
        }
        let defaults = Self::default();
        match field {
            TableServingField::PinPrimary => self.pin_primary != defaults.pin_primary,
            TableServingField::ReplicaReadPolicy => {
                self.replica_read_policy != defaults.replica_read_policy
            }
            TableServingField::PreferredLocation => {
                self.preferred_location != defaults.preferred_location
            }
            TableServingField::DropPercent => self.drop_percent != defaults.drop_percent,
            TableServingField::MaxReadRetries => self.max_read_retries != defaults.max_read_retries,
            TableServingField::MaxWriteRetries => {
                self.max_write_retries != defaults.max_write_retries
            }
            TableServingField::RetryBackoffMs => self.retry_backoff_ms != defaults.retry_backoff_ms,
            TableServingField::ContinuousFailedTimeMs => {
                self.continuous_failed_time_ms != defaults.continuous_failed_time_ms
            }
            TableServingField::IoTimeoutMs => self.io_timeout_ms != defaults.io_timeout_ms,
            TableServingField::ConnectTimeoutMs => {
                self.connect_timeout_ms != defaults.connect_timeout_ms
            }
        }
    }
}

impl Default for TableServingOptions {
    fn default() -> Self {
        Self {
            pin_primary: default_pin_primary(),
            replica_read_policy: default_replica_read_policy(),
            preferred_location: String::new(),
            drop_percent: 0,
            max_read_retries: default_max_read_retries(),
            max_write_retries: 0,
            retry_backoff_ms: default_retry_backoff_ms(),
            continuous_failed_time_ms: default_continuous_failed_time_ms(),
            io_timeout_ms: default_table_io_timeout_ms(),
            connect_timeout_ms: default_table_connect_timeout_ms(),
            set_fields: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptionsPatch {
    #[serde(default)]
    pub pin_primary: Option<bool>,
    #[serde(default)]
    pub replica_read_policy: Option<String>,
    #[serde(default)]
    pub preferred_location: Option<String>,
    #[serde(default)]
    pub drop_percent: Option<u8>,
    #[serde(default)]
    pub max_read_retries: Option<u32>,
    #[serde(default)]
    pub max_write_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    #[serde(default)]
    pub continuous_failed_time_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
}

/// How many resources there are, and how many are in each state.
///
/// A scrape reports exactly this about tables, namespaces and proxy groups and
/// nothing else, but it was cloning every one of them -- names and serving
/// options included -- to call `.len()` and filter on the state field.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateTally {
    pub normal: u64,
    pub frozen: u64,
    pub dropped: u64,
}

impl StateTally {
    fn record(&mut self, state: MetaEntityState) {
        match state {
            MetaEntityState::Normal => self.normal += 1,
            MetaEntityState::Frozen => self.frozen += 1,
            MetaEntityState::Dropped => self.dropped += 1,
        }
    }

    /// Every resource counted, whatever state it is in.
    ///
    /// The three states are the whole of `MetaEntityState`, so this is the
    /// count the listing's `.len()` used to give.
    pub fn total(&self) -> u64 {
        self.normal + self.frozen + self.dropped
    }

    /// How many are in the state of this name, named as
    /// [`MetaEntityState::as_str`] names it.
    pub fn in_state(&self, state: &str) -> u64 {
        match state {
            "normal" => self.normal,
            "frozen" => self.frozen,
            "dropped" => self.dropped,
            _ => 0,
        }
    }
}

/// One server, reduced to what a scrape reports about it.
///
/// A server record carries its shard loads, its stat loads and its shard
/// serving states -- one entry per shard it holds, and the serving states carry
/// strings. A scrape reads none of them, so none of them are here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerScrapeRow {
    pub server_addr: String,
    pub state: MetaEntityState,
    pub reported_record_count: u64,
    pub reported_storage_bytes: u64,
    pub last_meta_topology_version: u64,
    pub rejected_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
}

/// One proxy, reduced the same way.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyScrapeRow {
    pub proxy_addr: String,
    pub state: MetaEntityState,
    pub restart_count: u64,
}

/// Everything a scrape needs, taken in one pass under one read lock.
///
/// The scrape used to make five separate listing calls, each taking the lock
/// again, so its numbers described five consecutive moments rather than one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaMetricsReport {
    pub status: Status,
    pub tables: StateTally,
    pub namespaces: StateTally,
    pub proxy_groups: StateTally,
    pub servers: Vec<ServerScrapeRow>,
    pub proxies: Vec<ProxyScrapeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTableTopologyRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub old_topology_version: u64,
    /// Where the caller is, in the same hierarchical form as a server's
    /// location. Used only to order each shard's replicas nearest-first; an
    /// empty value leaves the order exactly as it was.
    #[serde(default)]
    pub client_location: String,
}

impl GetTableTopologyRequest {
    /// Ask only whether a table can be served, and at what version.
    ///
    /// A caller that wants the version or just the status does not need the
    /// shard list, and building one is the whole cost of the answer. Measured
    /// through the open-table route, the cost was the size of the table --
    /// 28.9us at 50 shards, 114.6us at 200, 434.9us at 800 -- and asking this
    /// way is 0.5us at every one of them.
    ///
    /// This is the ordinary request with the version set past anything a table
    /// can hold, which is the existing answer for a caller that is already
    /// current: every missing, dropped and frozen check still runs and still
    /// returns exactly what it returned before, and the answer stops before the
    /// shard list. Written this way so the status cannot drift from the status
    /// the full answer gives -- it is the same code producing it.
    pub fn status_only(namespace: String, table_name: String) -> Self {
        Self {
            namespace,
            table_name,
            old_topology_version: u64::MAX,
            client_location: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMetaInfo {
    pub table_id: u64,
    pub namespace: String,
    pub table_name: String,
    pub state: MetaEntityState,
    pub topology_version: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub replica_count: u64,
    #[serde(default)]
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableShard {
    pub shard_id: ShardId,
    pub start_bucket: u64,
    pub end_bucket: u64,
    pub primary: Option<String>,
    pub replicas: Vec<String>,
    #[serde(default)]
    pub primary_endpoint: Option<ServerEndpoint>,
    #[serde(default)]
    pub replica_endpoints: Vec<ServerEndpoint>,
    /// The fencing token for this shard's placement: the `load_version` its PRIMARY reports for
    /// it. A write carrying this is refused by a node that no longer holds that version, which is
    /// the window a stale route otherwise writes into.
    ///
    /// 0 means NOT KNOWN, and is the safe direction: a client without a version keeps the
    /// unfenced behaviour rather than being locked out. A topology written before this field
    /// existed reads as 0 for the same reason.
    #[serde(default)]
    pub load_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerEndpoint {
    pub server_addr: String,
    #[serde(default)]
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableTopologyResponse {
    pub status: Status,
    pub table: Option<TableMetaInfo>,
    #[serde(alias = "partitions")]
    pub shards: Vec<TableShard>,
    pub unchanged: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionRequest {
    #[serde(default)]
    pub old_topology_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionReport {
    pub status: Status,
    pub current_topology_version: u64,
    pub old_topology_version: u64,
    pub unchanged: bool,
    pub server_count: usize,
    pub proxy_count: usize,
    pub table_count: usize,
    pub shard_route_count: usize,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub dropped_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_proxies: usize,
    pub normal_tables: usize,
    pub frozen_tables: usize,
    pub dropped_tables: usize,
    pub changed_tables: Vec<TableMetaInfo>,
    #[serde(default)]
    pub events: Vec<TopologyChangeEvent>,
    #[serde(default)]
    pub event_history_truncated: bool,
}

/// Which slice of the change history to return.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyEventsRequest {
    /// Return changes newer than this topology version. Zero returns whatever
    /// the ring still holds.
    #[serde(default)]
    pub after_version: u64,
    /// Most changes to return. Zero means the ring's own limit.
    #[serde(default)]
    pub limit: usize,
}

/// What the metaserver recorded, and whether the caller missed anything.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyEventsResponse {
    pub status: Status,
    pub events: Vec<TopologyChangeEvent>,
    /// The oldest change still held. The history is a bounded ring, so anything
    /// older than this has been overwritten and cannot be asked for.
    pub oldest_retained_version: u64,
    /// Set when the caller asked to resume from a point the ring no longer
    /// holds, so a gap in what they receive is not mistaken for quiet.
    pub missed_events: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyChangeEvent {
    pub topology_version: u64,
    pub timestamp_ms: u64,
    pub kind: String,
    pub resource: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    pub status: Status,
    pub namespaces: Vec<NamespaceMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTablesResponse {
    pub status: Status,
    pub tables: Vec<TableMetaInfo>,
}

/// Which shards to list, and how many.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListShardsRequest {
    /// Only shards owned by this server. Empty lists every server's.
    #[serde(default)]
    pub server_addr: String,
    /// Resume after this shard id, exclusive. Zero starts from the beginning.
    #[serde(default)]
    pub after_shard_id: ShardId,
    /// Most shards to return. Zero means the default cap.
    #[serde(default)]
    pub limit: usize,
}

/// One shard's placement, as the metaserver has it recorded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardListEntry {
    pub shard_id: ShardId,
    pub server_addr: String,
    /// The table this shard's id falls inside, when one claims it. A registered
    /// shard whose table was dropped, or that was registered before its table
    /// existed, belongs to nothing and reports empty here -- which is worth
    /// seeing rather than hiding.
    pub namespace: String,
    pub table_name: String,
    pub latest_snapshot: Option<ShardSnapshotRef>,
    /// Whether the metaserver considers this shard served.
    ///
    /// A shard can be taken out of service on its own, and the listing had no
    /// way to show it: you could freeze a shard and then not see, here, that it
    /// was frozen.
    #[serde(default)]
    pub state: MetaEntityState,
    /// Whether the owning server still reports this shard as loaded.
    ///
    /// Freezing a shard is a decision the metaserver records immediately, but
    /// the datanode holding it has work to do before it has really let go. Until
    /// then the shard is frozen in the metadata and still resident on the node,
    /// and nothing said which. For a shard that is still serving this reads the
    /// other way: an owner that is not holding it is a divergence.
    ///
    /// `None` means the owner does not report shard states at all, which is not
    /// the same as reporting that it holds nothing -- the same distinction the
    /// divergence check makes before it is willing to judge a server.
    #[serde(default)]
    pub owner_reports_loaded: Option<bool>,
    /// Where this shard in particular was pinned, empty when it follows
    /// whatever its table prefers.
    ///
    /// A pin that can only be read one shard at a time cannot answer the
    /// question it exists for -- which shards are pinned, and where -- without a
    /// request for every shard in the cluster.
    #[serde(default)]
    pub preferred_location: String,
    /// When this shard first joined.
    ///
    /// Recorded so a shard that has been here for a week can be told from one
    /// that arrived during the incident, which is a comparison across shards,
    /// so it belongs where shards are listed.
    #[serde(default)]
    pub registered_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListShardsResponse {
    pub status: Status,
    pub shards: Vec<ShardListEntry>,
    /// Set only when the cap cut the page short. Pass it back as
    /// `after_shard_id` to continue; absent means this was the last page.
    pub next_after_shard_id: Option<ShardId>,
}

/// Default and hard cap on one page of shards.
///
/// Every other list endpoint returns everything, which is fine for servers,
/// proxies and tables -- there are tens of those. Shards are the one entity
/// there can be hundreds of thousands of, so this one paginates rather than
/// handing back the whole placement table in a single response.
pub const LIST_SHARDS_DEFAULT_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListServersResponse {
    pub status: Status,
    pub servers: Vec<ServerMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListProxiesResponse {
    pub status: Status,
    pub proxies: Vec<ProxyMetaInfo>,
}

/// Relabel a registered server's location without disturbing anything else
/// about it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateServerRequest {
    pub server_addr: String,
    /// The new location. May be empty, which means "unlabelled, place anywhere".
    #[serde(default)]
    pub location: String,
}

/// A resource announcing that it is shutting down.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotifyStopRequest {
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateChangeRequest {
    /// Why the resource is being frozen. Absent from the wire it reads as
    /// `Unspecified`; the admin routes set `Operator` explicitly, since a
    /// request arriving there is operator-driven by definition.
    #[serde(default)]
    pub reason: FreezeReason,
    pub endpoint: String,
    #[serde(default)]
    pub freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadFinishRequest {
    pub server_addr: String,
    pub shard_id: ShardId,
    pub load_version: u64,
    pub status: Status,
    #[serde(default)]
    pub scheduler_task_id: Option<u64>,
    #[serde(default)]
    pub scheduler_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaControlPlaneParityReport {
    pub status: Status,
    pub table_topology_ready: bool,
    pub transitional_state_model_ready: bool,
    pub topology_history_ready: bool,
    pub scheduler_owned_finish_load_ready: bool,
    pub scheduler_generation_check_ready: bool,
    pub durable_replay_ready: bool,
    pub real_data_node_coordination_ready: bool,
    pub scheduler_finish_generation_count: usize,
    pub topology_event_count: usize,
    pub topology_version: u64,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaStats {
    pub register_shard_total: u64,
    pub get_shard_total: u64,
    pub server_register_total: u64,
    pub server_heartbeat_total: u64,
    pub proxy_register_total: u64,
    pub proxy_heartbeat_total: u64,
    pub namespace_create_total: u64,
    pub table_create_total: u64,
    pub topology_query_total: u64,
    pub load_finish_total: u64,
    pub topology_version: u64,
    pub server_count: usize,
    pub proxy_count: usize,
    pub namespace_count: usize,
    pub table_count: usize,
    pub shard_count: usize,
    /// How many of those shards are not serving.
    ///
    /// A shard can be taken out of service on its own, and nothing counted it:
    /// an operator could freeze one and see no change on any dashboard. Counted
    /// here rather than kept as a running total, because a tally maintained
    /// beside the shards is a second thing to keep in step with them.
    #[serde(default)]
    pub frozen_shard_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInfo {
    pub status: Status,
    pub stats: MetaStats,
    pub boot_time_ms: u64,
    pub durable_mutation_log: bool,
    /// Whether recorded metadata mutations are currently refused.
    #[serde(default)]
    pub meta_change_muted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaPreflightReport {
    pub status: Status,
    pub stats: MetaStats,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_tables: usize,
    pub shard_routes: usize,
    pub degraded_reasons: Vec<String>,
}

impl MetaMutation {
    /// Whether this change may still be made while metadata change is muted.
    ///
    /// Only the lever itself and the retention purge. The lever, because muting
    /// must not be a one-way door -- refusing it would leave no way back. The
    /// purge, because the single-node path has always allowed it: its one
    /// caller is the retention loop, which the mute stops separately, and the
    /// two backends have to agree on what the mute means rather than each
    /// inventing an answer.
    pub(crate) fn allowed_while_muted(&self) -> bool {
        matches!(self, Self::SetMetaChangeMuted(_) | Self::PurgeMeta(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum MetaMutation {
    RegisterShard(RegisterShardRequest),
    PublishShardSnapshot(PublishShardSnapshotRequest),
    RegisterServer(RegisterServerRequest),
    RegisterProxy(RegisterProxyRequest),
    AddNamespace(AddNamespaceRequest),
    AddTable(AddTableRequest),
    DeleteTable(DeleteTableRequest),
    UpdateTable(UpdateTableRequest),
    FreezeTable(DeleteTableRequest),
    UnfreezeTable(DeleteTableRequest),
    FinishLoad(LoadFinishRequest),
    UpdateServer(UpdateServerRequest),
    FreezeServer(StateChangeRequest),
    UnfreezeServer(StateChangeRequest),
    DropServer(StateChangeRequest),
    FreezeProxy(StateChangeRequest),
    UnfreezeProxy(StateChangeRequest),
    DropProxy(StateChangeRequest),
    SetShardState(ShardStateRequest, MetaEntityState),
    DropShard(ShardStateRequest),
    PutProxyGroup(PutProxyGroupRequest),
    DropProxyGroup(DropProxyGroupRequest),
    SetProxyGroup(ProxyAttachment),
    /// One applied retention round. The *outcome* is recorded rather than the
    /// intent to run a round, because retention is computed from the wall clock
    /// and re-planning it during replay would purge a different set. Replaying
    /// the concrete list forgets exactly what the live round forgot.
    PurgeMeta(MetaRetentionPlan),
    /// Freeze, unfreeze or drop a whole namespace.
    SetNamespaceState(AddNamespaceRequest, MetaEntityState),
    /// Replace the set of names held back from creation.
    SetReservedNames(ReservedNames),
    /// Pin one shard to a location, or release it with an empty one.
    SetShardPreferredLocation(ShardPinRequest),
    /// Mute or resume metadata change. Recorded like any other mutation so it
    /// replays in order and reaches raft peers; the guard is deliberately not
    /// applied during replay, because the log only ever contains mutations that
    /// were accepted live, and a mute entry flips the flag at exactly the point
    /// the original sequence did.
    SetMetaChangeMuted(bool),
    /// A snapshot installed over the whole state.
    ///
    /// Carries the snapshot because replay has to reach the same state the
    /// install did. Every record before it is superseded by it, which is what
    /// makes the restore hold across a restart.
    InstallSnapshot(Box<MetaSnapshot>),
}

/// One line of the mutation log: a change, and when the metaserver accepted it.
///
/// The time has to travel with the change. Everything that ages -- retention,
/// freeze aging -- was stamped with `now_ms()` inside the apply path, which
/// replay also runs, so every clock restarted whenever the metaserver did and
/// nothing was ever old enough to act on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaMutationRecord {
    /// Zero on a line written before this field existed. Replay falls back to
    /// the current clock for those, which is what they have always been given.
    #[serde(default)]
    pub at_ms: u64,
    #[serde(flatten)]
    pub mutation: MetaMutation,
}

#[derive(Debug, Clone)]
pub struct LocalMetaMutationLog {
    path: PathBuf,
    /// The handle records are appended through, opened once.
    ///
    /// Opening the file per record cost more than it looked like: with the
    /// barrier split out so writers can share one, a record was opening it
    /// twice.
    write_lock: Arc<Mutex<Option<File>>>,
    /// A second handle for the barrier, so it can run without the write lock
    /// and let the next writer get its bytes down meanwhile. Syncing is per
    /// file, not per handle, so this covers what the other one wrote.
    sync_file: Arc<Mutex<Option<File>>>,
    /// How many records have been written to the file.
    ///
    /// Written under `write_lock`, so it counts the records whose bytes have
    /// reached the file in the order the lock granted.
    written: Arc<AtomicU64>,
    /// How many records a completed sync has covered.
    ///
    /// Guarded by its own lock rather than `write_lock`, so a writer waiting for
    /// durability is not holding up the writer behind it.
    synced: Arc<Mutex<u64>>,
}

impl LocalMetaMutationLog {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            write_lock: Arc::default(),
            sync_file: Arc::default(),
            written: Arc::default(),
            synced: Arc::default(),
        })
    }

    pub fn append(&self, mutation: &MetaMutation, at_ms: u64) -> io::Result<()> {
        // The bytes reach the file in the order the write lock grants, and the
        // record is numbered by that order.
        let mine = {
            let mut handle = self
                .write_lock
                .lock()
                .expect("meta mutation log lock poisoned");
            let file = match handle.as_mut() {
                Some(file) => file,
                None => handle.insert(
                    OpenOptions::new().create(true).append(true).open(&self.path)?,
                ),
            };
            let record = MetaMutationRecord {
                at_ms,
                mutation: mutation.clone(),
            };
            serde_json::to_writer(&mut *file, &record).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
            self.written.fetch_add(1, Ordering::SeqCst) + 1
        };
        self.sync_through(mine)
    }

    /// Return once a sync that began after record `mine` was written has
    /// completed.
    ///
    /// The barrier a writer needs is not its own: any sync beginning after its
    /// bytes reached the file covers them, because the file is append-only and
    /// the writes are ordered. So a writer that finds its record already covered
    /// has nothing to do, and one that does not takes the barrier for everything
    /// written so far -- including the writers still queued behind it.
    fn sync_through(&self, mine: u64) -> io::Result<()> {
        let mut synced = self.synced.lock().expect("meta mutation log sync poisoned");
        if *synced >= mine {
            // Somebody else's barrier already covered this record.
            return Ok(());
        }
        // Read before the barrier, published after it. The other order would
        // tell a writer its bytes were durable while the sync was still running.
        let covered = self.written.load(Ordering::SeqCst);
        let mut handle = self
            .sync_file
            .lock()
            .expect("meta mutation log sync handle poisoned");
        let file = match handle.as_mut() {
            Some(file) => file,
            None => handle.insert(
                OpenOptions::new().create(true).append(true).open(&self.path)?,
            ),
        };
        crate::durability_metrics::record_barrier("meta_log_append");
        file.sync_data()?;
        *synced = (*synced).max(covered);
        Ok(())
    }

    /// Read the log back.
    ///
    /// A partial record at the END is what a crash partway through an append
    /// leaves behind, and it is dropped rather than refused. That record was
    /// never acknowledged: a writer only returns once a sync covering its bytes
    /// has completed, so nothing was promised to anybody about it. Refusing the
    /// whole file for it meant a metaserver whose metadata was entirely durable
    /// except for a fraction of one record would not start.
    ///
    /// A line that does not parse with records AFTER it is a different thing.
    /// Those later records WERE acknowledged, and stopping there would discard
    /// them silently, so that is still an error. The distinction is the whole
    /// point: the tail is expected, the middle is not.
    /// Read the log back, repairing a torn tail if there is one.
    ///
    /// A partial record at the END is what a crash partway through an append
    /// leaves behind. It is dropped rather than refused: that record was never
    /// acknowledged, because a writer only returns once a sync covering its
    /// bytes has completed, so nothing was promised to anybody about it.
    /// Refusing the whole file for it meant a metaserver whose metadata was
    /// entirely durable except for a fraction of one record would not start.
    ///
    /// The fragment is also REMOVED, not merely skipped. Appends open the file
    /// for append and write at the end; left in place, the fragment would be
    /// spliced onto the front of the next record and stop that line parsing.
    /// That line would then be the last one, so the following restart would
    /// drop it as a torn tail -- silently losing a write that WAS acknowledged.
    /// Truncating to the end of the last record that parsed means the next
    /// append starts on a record boundary.
    ///
    /// A line that does not parse with records AFTER it is a different thing.
    /// Those later records were acknowledged, and stopping there would discard
    /// them silently, so that is still an error. The tail is expected; the
    /// middle is not.
    pub fn load(&self) -> io::Result<Vec<MetaMutationRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(&self.path)?;
        let mut mutations = Vec::new();
        // Where the last record that parsed ends, including its newline. This is
        // the only point the file can be safely cut back to.
        let mut good_end = 0usize;
        let mut offset = 0usize;
        // Held rather than returned: a line that does not parse is only a torn
        // tail if nothing follows it. Anything following turns it into an error.
        let mut unparsed: Option<(usize, String)> = None;
        for (number, line) in text.split('\n').enumerate() {
            let line_end = (offset + line.len() + 1).min(text.len());
            offset += line.len() + 1;
            if line.trim().is_empty() {
                continue;
            }
            if let Some((bad_number, message)) = unparsed.take() {
                return Err(io::Error::other(format!(
                    "meta mutation log {}: line {} does not parse and more lines follow it, \
                     so it is not a torn tail: {}",
                    self.path.display(),
                    bad_number + 1,
                    message
                )));
            }
            match serde_json::from_str::<MetaMutationRecord>(line) {
                Ok(record) => {
                    mutations.push(record);
                    good_end = line_end;
                }
                Err(err) => unparsed = Some((number, err.to_string())),
            }
        }
        if let Some((number, message)) = unparsed {
            tracing::warn!(
                path = %self.path.display(),
                line = number + 1,
                recovered = mutations.len(),
                truncated_to = good_end,
                error = %message,
                "metadata log ends in a partial record; it was never acknowledged, so it is \
                 dropped and the file is cut back to the last whole record"
            );
            let file = OpenOptions::new().write(true).open(&self.path)?;
            file.set_len(good_end as u64)?;
            // The cut has to survive the crash that follows it, or the fragment
            // comes back and the next append splices onto it again.
            crate::durability_metrics::record_barrier("meta_log_truncate");
            file.sync_all()?;
        }
        Ok(mutations)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Service counters, held outside the metadata lock.
///
/// These used to live inside `MetaState`, which meant the two hottest *read*
/// paths -- resolving a table's topology and reading one shard's location --
/// had to take the **exclusive** lock, purely to add one to a number. Every
/// topology lookup therefore serialised against every other lookup and against
/// every metadata write, on a path that every client and every proxy calls.
///
/// Counting is the one thing here that does not need to agree with anything
/// else, so `Relaxed` is the right ordering: the total is a monotonic tally for
/// `/metrics`, never a decision input.
#[derive(Debug, Default)]
struct MetaCounters {
    register_shard_total: AtomicU64,
    get_shard_total: AtomicU64,
    server_register_total: AtomicU64,
    server_heartbeat_total: AtomicU64,
    proxy_register_total: AtomicU64,
    proxy_heartbeat_total: AtomicU64,
    namespace_create_total: AtomicU64,
    table_create_total: AtomicU64,
    topology_query_total: AtomicU64,
    load_finish_total: AtomicU64,
}

impl MetaCounters {
    /// Restore the tallies carried by an installed snapshot, so a peer that
    /// installs one does not report its counters starting from zero.
    fn install_from(&self, stats: &MetaStats) {
        for (slot, value) in [
            (&self.register_shard_total, stats.register_shard_total),
            (&self.get_shard_total, stats.get_shard_total),
            (&self.server_register_total, stats.server_register_total),
            (&self.server_heartbeat_total, stats.server_heartbeat_total),
            (&self.proxy_register_total, stats.proxy_register_total),
            (&self.proxy_heartbeat_total, stats.proxy_heartbeat_total),
            (&self.namespace_create_total, stats.namespace_create_total),
            (&self.table_create_total, stats.table_create_total),
            (&self.topology_query_total, stats.topology_query_total),
            (&self.load_finish_total, stats.load_finish_total),
        ] {
            slot.store(value, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableRecord {
    info: TableMetaInfo,
}

impl MetaState {
    /// Record where a table's shards start, and notice if its range overlaps
    /// one already there.
    ///
    /// Called wherever a table is created or its shard count changes, which are
    /// the only ways a range comes into being or grows.
    pub(crate) fn note_table_range(&mut self, key: &str, info: &TableMetaInfo) {
        let first = info.first_shard_id;
        let end = first.saturating_add(info.shard_count);
        let overlaps_below = self
            .table_starts
            .range(..first)
            .next_back()
            .and_then(|(_, other)| self.tables.get(other))
            .is_some_and(|other| {
                other
                    .info
                    .first_shard_id
                    .saturating_add(other.info.shard_count)
                    > first
            });
        // Every other table starting inside this range overlaps it, not just
        // the nearest one -- and the nearest entry at or above  is this
        // table itself once it has been recorded.
        let overlaps_above = self
            .table_starts
            .range(first..end)
            .any(|(_, other)| other != key);
        if overlaps_below || overlaps_above {
            self.table_ranges_overlap = true;
        }
        self.table_starts.insert(first, key.to_string());
    }

    /// Forget a table's start when the table itself is forgotten.
    pub(crate) fn forget_table_range(&mut self, key: &str, info: &TableMetaInfo) {
        if self
            .table_starts
            .get(&info.first_shard_id)
            .is_some_and(|owner| owner == key)
        {
            self.table_starts.remove(&info.first_shard_id);
        }
    }

    /// Rebuild the index from the tables, for a state that arrived whole.
    pub(crate) fn reindex_table_ranges(&mut self) {
        self.table_starts.clear();
        self.table_ranges_overlap = false;
        let entries = self
            .tables
            .iter()
            .map(|(key, record)| (key.clone(), record.info.clone()))
            .collect::<Vec<_>>();
        for (key, info) in entries {
            self.note_table_range(&key, &info);
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetaState {
    /// Kept in shard order so a page of them is a range rather than a scan
    /// of everything above the cursor.
    shards: BTreeMap<ShardId, ShardLocation>,
    servers: BTreeMap<String, ServerMetaInfo>,
    proxies: BTreeMap<String, ProxyMetaInfo>,
    proxy_groups: BTreeMap<String, ProxyGroupInfo>,
    namespaces: BTreeMap<String, MetaEntityState>,
    tables: BTreeMap<String, TableRecord>,
    /// Where each table's shards start, keyed by that start.
    ///
    /// Finding which table owns a shard is done on the shard-load path, under
    /// the write lock, and doing it by walking every table made every other
    /// call wait behind a walk that grew with the cluster. `first_shard_id` is
    /// pinned when a table is created, so where tables start is something that
    /// can be looked up.
    #[serde(default)]
    table_starts: BTreeMap<ShardId, String>,
    /// Whether two tables have ever claimed overlapping shards.
    ///
    /// The walk answered an overlap with the first table in map order, which is
    /// not necessarily the one starting nearest below the shard. Once that has
    /// happened the index cannot reproduce the walk's answer, so the walk
    /// decides from then on.
    #[serde(default)]
    table_ranges_overlap: bool,
    next_table_id: u64,
    topology_version: u64,
    topology_events: VecDeque<TopologyChangeEvent>,
    scheduler_finish_generations: BTreeMap<String, u64>,
    /// When each dropped resource was dropped, keyed `server:<addr>`,
    /// `proxy:<addr>` or `table:<namespace.table>`. Dropping previously recorded
    /// no timestamp at all, so "dropped long enough ago to forget" was not
    /// expressible; retention ages against this. Kept beside the resources
    /// rather than inside them so the wire types are unchanged.
    dropped_since_ms: BTreeMap<String, u64>,
    /// While set, the metaserver refuses every recorded metadata mutation.
    ///
    /// This is an incident lever: every automatic subsystem is otherwise gated
    /// by an environment variable, so the only way to stop the metaserver making
    /// changes was to restart it with different configuration -- during exactly
    /// the incident where a restart is least welcome.
    meta_change_muted: bool,
    /// Names held back from creation. Durable, like every other decision an
    /// operator makes here.
    reserved_names: ReservedNames,
    /// When each frozen table was frozen, keyed `table:<namespace.table>`.
    /// Servers and proxies carry `frozen_since_ms` on the resource itself;
    /// tables do not, and adding a field there would touch every
    /// `TableMetaInfo` literal in the tree, so the metaserver keeps it beside
    /// them the same way it keeps drop times.
    frozen_since_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshot {
    pub format_version: u32,
    pub created_at_ms: u64,
    pub shards: HashMap<ShardId, ShardLocation>,
    pub servers: BTreeMap<String, ServerMetaInfo>,
    pub proxies: BTreeMap<String, ProxyMetaInfo>,
    /// Declared proxy capacity. Carried so a peer installing this snapshot
    /// knows what the routing tier is supposed to look like, not just what it
    /// currently is.
    #[serde(default)]
    pub proxy_groups: BTreeMap<String, ProxyGroupInfo>,
    pub namespaces: BTreeMap<String, MetaEntityState>,
    pub tables: Vec<TableMetaInfo>,
    pub stats: MetaStats,
    pub next_table_id: u64,
    pub topology_version: u64,
    #[serde(default)]
    pub scheduler_finish_generations: BTreeMap<String, u64>,
    /// Drop timestamps for the tombstones in this snapshot. Carried so a peer
    /// that installs the snapshot can keep ageing them instead of restarting
    /// every tombstone's clock.
    #[serde(default)]
    pub dropped_since_ms: BTreeMap<String, u64>,
    /// Whether metadata change was muted when this snapshot was taken. Carried
    /// so a peer installing it does not silently resume a mute an operator put
    /// in place.
    #[serde(default)]
    pub meta_change_muted: bool,
    /// Freeze timestamps for the frozen tables in this snapshot, so a peer that
    /// installs it keeps ageing them instead of restarting their clocks.
    #[serde(default)]
    pub frozen_since_ms: BTreeMap<String, u64>,
    /// The recorded change history, oldest first.
    ///
    /// The topology version travelled while the history it belongs to did not,
    /// so a peer installing a snapshot inherited a version with nothing behind
    /// it -- and then reported its own control plane as blocked on evidence it
    /// had a moment earlier. Bounded to the same ring the metaserver keeps, so
    /// this adds a fixed, small amount to a snapshot rather than growing with
    /// the cluster.
    #[serde(default)]
    pub topology_events: VecDeque<TopologyChangeEvent>,
    /// Names held back from creation. Carried so a peer that installs this
    /// snapshot keeps refusing them rather than quietly allowing what the
    /// operator reserved.
    #[serde(default)]
    pub reserved_names: ReservedNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotResponse {
    pub status: Status,
    pub snapshot: Option<MetaSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileResponse {
    pub status: Status,
    pub path: String,
    pub snapshot: Option<MetaSnapshot>,
}

#[derive(Debug, Clone)]
pub struct SingleNodeMeta {
    inner: Arc<RwLock<MetaState>>,
    boot_time_ms: u64,
    mutation_log: Option<LocalMetaMutationLog>,
    /// When set, a resource the metaserver convicted cannot register its way
    /// back into service; only an explicit unfreeze returns it. Off by default
    /// because today's automatic recovery is load-bearing for deployments
    /// running with a zero freeze cooldown.
    forbid_self_clearing_conviction: bool,
    /// Where the background subsystems record what each round did, so `/metrics`
    /// can report it. Shared by clone, so every handle to this meta writes to
    /// and reads from the same recorder.
    metrics: SubsystemMetrics,
    /// Fan-out for metadata change, deliberately outside `inner` so a
    /// subscriber's cost never lands inside the metadata lock. Shared by clone,
    /// like the metadata itself.
    events: Arc<event_bus::MetaEventBus>,
    /// Service counters, deliberately outside `inner` -- see [`MetaCounters`].
    /// Shared by clone, like the metadata itself, so every handle counts into
    /// the same tallies.
    counters: Arc<MetaCounters>,
}

impl Default for SingleNodeMeta {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetaState {
                next_table_id: 1,
                ..MetaState::default()
            })),
            boot_time_ms: now_ms(),
            mutation_log: None,
            forbid_self_clearing_conviction: false,
            metrics: SubsystemMetrics::new(),
            events: Arc::new(event_bus::MetaEventBus::default()),
            counters: Arc::new(MetaCounters::default()),
        }
    }
}

impl SingleNodeMeta {
    /// Set the conviction lock on a meta that already exists.
    ///
    /// The builder cannot reach a raft node's meta: those are constructed inside
    /// the cluster, long after the process read its configuration.
    pub fn set_conviction_lock(&mut self, forbid: bool) {
        self.forbid_self_clearing_conviction = forbid;
    }

    /// Refuse to let a resource the metaserver convicted register its way back
    /// into service; only an explicit unfreeze returns it. Off by default.
    pub fn with_conviction_lock(mut self, forbid: bool) -> Self {
        self.forbid_self_clearing_conviction = forbid;
        self
    }

    /// True when a convicted resource cannot clear its own freeze.
    pub fn conviction_lock_enabled(&self) -> bool {
        self.forbid_self_clearing_conviction
    }

    /// The recorder the background subsystems write their round outcomes into.
    pub fn subsystem_metrics(&self) -> &SubsystemMetrics {
        &self.metrics
    }

    pub fn register(&self, mut request: RegisterShardRequest) -> RegisterShardResponse {
        if let Some(status) = self.meta_change_refusal() {
            return RegisterShardResponse { status };
        }
        // Stamped here, so the recorded mutation carries the metaserver's clock
        // and a replay keeps the original rather than restamping itself.
        request.registered_at_ms = now_ms();
        self.record_mutation(MetaMutation::RegisterShard(request.clone()));
        self.apply_register(request)
    }

    fn apply_register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.register_shard_total.fetch_add(1, Ordering::Relaxed);
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        // Registering a shard again -- to move it, or after a restart -- does
        // not make it a new shard.
        let registered_at_ms = state
            .shards
            .get(&request.shard_id)
            .map(|location| location.registered_at_ms)
            .filter(|first| *first != 0)
            .unwrap_or(request.registered_at_ms);
        // Where an operator put this shard, kept for the same reason the join
        // time above is: registering again is a datanode saying it still holds
        // the shard, not an operator saying they no longer care where it lives.
        // A node restarting re-registers everything it holds, so writing this
        // away here meant a pin lasted until the next restart.
        let preferred_location = state
            .shards
            .get(&request.shard_id)
            .map(|location| location.preferred_location.clone())
            .unwrap_or_default();
        state.shards.insert(
            request.shard_id,
            ShardLocation {
                registered_at_ms,
                preferred_location,
                state: MetaEntityState::Normal,
                shard_id: request.shard_id,
                server_addr: request.server_addr.clone(),
                latest_snapshot,
            },
        );
        ensure_server(&mut state, &request.server_addr);
        record_topology_event(
            &mut state,
            "register_shard",
            format!("shard:{}", request.shard_id),
            format!("server_addr={}", request.server_addr),
        );
        RegisterShardResponse {
            status: Status::ok(),
        }
    }

    /// Stop serving one shard, keeping its owner entry so it can come back.
    pub fn freeze_shard(&self, request: ShardStateRequest) -> AckResponse {
        self.set_shard_state(request, MetaEntityState::Frozen)
    }

    /// Return a frozen shard to service.
    pub fn unfreeze_shard(&self, request: ShardStateRequest) -> AckResponse {
        self.set_shard_state(request, MetaEntityState::Normal)
    }

    fn set_shard_state(&self, request: ShardStateRequest, next: MetaEntityState) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetShardState(request.clone(), next));
        self.apply_set_shard_state(request, next)
    }

    pub(crate) fn apply_set_shard_state(
        &self,
        request: ShardStateRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(shard) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if shard.state == next {
            return AckResponse {
                status: Status::error("not_modified", "shard state is unchanged"),
            };
        }
        shard.state = next;
        // Topology is derived on read, so the version bump is what makes clients
        // stop (or resume) resolving to this shard.
        record_topology_event(
            &mut state,
            "shard_state",
            format!("shard:{}", request.shard_id),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Forget a shard entirely. The route is removed rather than tombstoned:
    /// a shard has no identity beyond where it is served from.
    pub fn drop_shard(&self, request: ShardStateRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::DropShard(request.clone()));
        self.apply_drop_shard(request)
    }

    pub(crate) fn apply_drop_shard(&self, request: ShardStateRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        if state.shards.remove(&request.shard_id).is_none() {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        }
        record_topology_event(
            &mut state,
            "drop_shard",
            format!("shard:{}", request.shard_id),
            "state=dropped",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        self.counters.get_shard_total.fetch_add(1, Ordering::Relaxed);
        // Reading one shard's location only reads, for the same reason.
        let state = self.inner.read().expect("meta lock poisoned");
        let location = state.shards.get(&shard_id).cloned();
        GetShardResponse {
            status: if location.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not registered")
            },
            location,
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::PublishShardSnapshot(request.clone()));
        self.apply_publish_shard_snapshot(request)
    }

    fn apply_publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(location) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if location.latest_snapshot.as_ref().map_or(false, |existing| {
            existing.last_log_index > request.snapshot.last_log_index
        }) {
            return AckResponse {
                status: Status::error(
                    "stale_snapshot",
                    "snapshot is older than the latest metaserver record",
                ),
            };
        }
        location.latest_snapshot = Some(request.snapshot);
        record_topology_event(
            &mut state,
            "publish_shard_snapshot",
            format!("shard:{}", request.shard_id),
            "latest_snapshot_updated",
        );
        AckResponse {
            status: Status::ok(),
        }
    }



    /// True while recorded metadata mutations are being refused.
    /// Whether metadata change is muted, phrased so both backends answer the
    /// same shape. A single-node metaserver always knows; a raft-backed one may
    /// not, and `None` is the difference between "not muted" and "cannot say".
    pub fn meta_change_muted(&self) -> Option<bool> {
        Some(self.is_meta_change_muted())
    }

    pub fn is_meta_change_muted(&self) -> bool {
        self.inner
            .read()
            .expect("meta lock poisoned")
            .meta_change_muted
    }

    /// The refusal every guarded entry point returns while muted, or `None` when
    /// metadata change is flowing normally.
    /// The refusal a muted metaserver answers with, so both backends phrase it
    /// identically.
    pub(crate) fn muted_status() -> Status {
        Status::error(
            "meta_change_muted",
            "metadata change is muted; resume it before making changes",
        )
    }

    fn meta_change_refusal(&self) -> Option<Status> {
        self.is_meta_change_muted().then(Self::muted_status)
    }

    /// The names currently held back from creation.
    pub fn reserved_names(&self) -> ReservedNamesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ReservedNamesResponse {
            status: Status::ok(),
            reserved: state.reserved_names.clone(),
        }
    }

    /// Replace the reserved set. Guarded by the mute like any other change, and
    /// recorded so it survives restart and reaches raft peers.
    pub fn set_reserved_names(&self, reserved: ReservedNames) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetReservedNames(reserved.clone()));
        self.apply_set_reserved_names(reserved)
    }

    pub(crate) fn apply_set_reserved_names(&self, reserved: ReservedNames) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.reserved_names = reserved;
        let detail = format!(
            "namespaces={},tables={}",
            state.reserved_names.namespaces.len(),
            state.reserved_names.tables.len()
        );
        record_topology_event(&mut state, "reserved_names", "cluster".to_string(), detail);
        AckResponse {
            status: Status::ok(),
        }
    }

    /// The refusal a guarded creation gives, or `None` when the name is free.
    ///
    /// Checked on the way in rather than inside `apply_*`, exactly like the
    /// mute: the mutation log then holds only creations that were accepted, so
    /// replay does not have to re-decide against a reserved set that has since
    /// changed.
    /// Why this change must not be admitted, judged against current state.
    ///
    /// These guards lived only in the public methods. `apply_mutation` dispatches
    /// straight to the `apply_*` functions, so the raft propose path went around
    /// every one of them: a reserved namespace could be created, and a namespace
    /// could be dropped out from under a table that was still live, stranding it.
    ///
    /// Judged before proposing and never while applying. Replay has to reapply
    /// what was already accepted -- a name reserved today must not invalidate a
    /// namespace legitimately created before it.
    pub(crate) fn admission_refusal(&self, mutation: &MetaMutation) -> Option<Status> {
        let state = self.inner.read().expect("meta lock poisoned");
        Self::admission_refusal_in(&state, mutation)
    }

    /// The same judgement, for a caller that already holds the lock.
    ///
    /// A caller that has to decide and act without letting go -- the namespace
    /// state change does, or a table created in the gap is stranded -- holds
    /// the write lock across the whole thing. Calling the `&self` form there
    /// would acquire a read inside that write, on the same lock and the same
    /// thread, and never return.
    pub(crate) fn admission_refusal_in(
        state: &MetaState,
        mutation: &MetaMutation,
    ) -> Option<Status> {
        match mutation {
            MetaMutation::AddNamespace(request) => {
                Self::reserved_name_refusal_in(state, &request.namespace, None)
            }
            MetaMutation::AddTable(request) => {
                Self::reserved_name_refusal_in(state, &request.namespace, Some(&request.table_name))
            }
            MetaMutation::SetNamespaceState(request, MetaEntityState::Dropped) => {
                Self::namespace_not_empty_in(state, &request.namespace).or_else(|| {
                    Self::namespace_still_routed_in(state, &request.namespace)
                })
            }
            MetaMutation::PutProxyGroup(request) => Self::proxy_group_name_refusal(request),
            _ => None,
        }
    }

    /// Refuses dropping a namespace a proxy group still routes to.
    ///
    /// A proxy group is the routing tier's standing promise to serve the
    /// namespace, so it is a live dependent exactly as a table is. Dropping the
    /// namespace under one used to succeed and leave the contradiction in
    /// place: the group stayed Normal, its proxies stayed attached, and the
    /// next heartbeat still handed them the dropped namespace to serve.
    /// Calibration does not clean that up -- it keys off the group's own state,
    /// which nothing changed -- and no report flags it. Refusing keeps the
    /// order of operations the code already implies: drop the group, then drop
    /// the namespace, and `drop_proxy_group` releases the proxies as it goes.
    fn namespace_still_routed_in(state: &MetaState, namespace: &str) -> Option<Status> {
        state
            .proxy_groups
            .values()
            .any(|group| {
                group.namespace == namespace && group.state != MetaEntityState::Dropped
            })
            .then(|| {
                Status::error(
                    "namespace_still_routed",
                    "namespace still has a proxy group routing to it",
                )
            })
    }

    /// Refuses a proxy group that names neither itself nor a namespace.
    ///
    /// The group name is the key it is stored under, and an empty one is also
    /// the value an unattached proxy carries -- so a nameless group is indexed
    /// by "no group at all" and reads as though every idle proxy belongs to it.
    /// The public method has always refused this; the propose path dispatched
    /// straight to `apply_put_proxy_group`, which does not check, and committed
    /// it into replicated metadata.
    ///
    /// Takes no lock: it judges the request alone, not the state around it.
    fn proxy_group_name_refusal(request: &PutProxyGroupRequest) -> Option<Status> {
        (request.group.is_empty() || request.namespace.is_empty())
            .then(|| Status::error("bad_request", "group and namespace are required"))
    }

    /// Refuses dropping a namespace that still holds a table which is not itself
    /// dropped, so dropping a namespace cannot strand one.
    fn namespace_not_empty_in(state: &MetaState, namespace: &str) -> Option<Status> {
        state
            .tables
            .values()
            .any(|table| {
                table.info.namespace == namespace
                    && table.info.state != MetaEntityState::Dropped
            })
            .then(|| {
                Status::error(
                    "namespace_not_empty",
                    "namespace still holds a table that is not dropped",
                )
            })
    }

    fn reserved_name_refusal_in(
        state: &MetaState,
        namespace: &str,
        table: Option<&str>,
    ) -> Option<Status> {
        if state.reserved_names.namespaces.contains(namespace) {
            return Some(Status::error(
                "name_reserved",
                format!("namespace {namespace} is reserved"),
            ));
        }
        let table = table?;
        state
            .reserved_names
            .tables
            .contains(table)
            .then(|| Status::error("name_reserved", format!("table name {table} is reserved")))
    }

    /// Pin one shard to a location, overriding what its table prefers. An
    /// empty location releases the pin.
    pub fn pin_shard(&self, request: ShardPinRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::SetShardPreferredLocation(request.clone()));
        self.apply_pin_shard(request)
    }

    pub(crate) fn apply_pin_shard(&self, request: ShardPinRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(shard) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if shard.preferred_location == request.location {
            return AckResponse {
                status: Status::error("not_modified", "shard location preference is unchanged"),
            };
        }
        shard.preferred_location = request.location.clone();
        // Placement is derived on read, so the version bump is what makes the
        // planners reconsider where this shard belongs.
        record_topology_event(
            &mut state,
            "shard_preferred_location",
            format!("shard:{}", request.shard_id),
            if request.location.is_empty() {
                "released".to_string()
            } else {
                format!("location={}", request.location)
            },
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// The recorded change history, oldest first.
    ///
    /// Every metadata change records one of these, and the only way to see them
    /// was to subscribe and wait: an operator looking at an incident that had
    /// already happened could not ask what changed.
    pub fn topology_events(&self, request: TopologyEventsRequest) -> TopologyEventsResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let oldest_retained_version = state
            .topology_events
            .front()
            .map(|event| event.topology_version)
            .unwrap_or_default();
        // Resuming from a version the ring has already overwritten means there
        // are changes the caller will never see. Saying so is the difference
        // between a gap and a quiet period.
        let missed_events = request.after_version > 0
            && oldest_retained_version > request.after_version.saturating_add(1);
        let limit = if request.limit == 0 {
            TOPOLOGY_EVENT_HISTORY_LIMIT
        } else {
            request.limit.min(TOPOLOGY_EVENT_HISTORY_LIMIT)
        };
        let events = state
            .topology_events
            .iter()
            .filter(|event| event.topology_version > request.after_version)
            .take(limit)
            .cloned()
            .collect();
        TopologyEventsResponse {
            status: Status::ok(),
            events,
            oldest_retained_version,
            missed_events,
        }
    }

    /// Mute or resume metadata change.
    ///
    /// Never guarded by the mute itself -- an operator must always be able to
    /// resume -- and recorded so it survives restart and reaches raft peers.
    pub fn set_meta_change_muted(&self, muted: bool) -> AckResponse {
        self.record_mutation(MetaMutation::SetMetaChangeMuted(muted));
        self.apply_set_meta_change_muted(muted)
    }

    pub(crate) fn apply_set_meta_change_muted(&self, muted: bool) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.meta_change_muted = muted;
        record_topology_event(
            &mut state,
            "meta_change_mute",
            "cluster".to_string(),
            format!("muted={muted}"),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Record a change and return the time it was accepted, which the apply
            /// path must stamp with so replay reproduces it rather than the
            /// clock of whenever the metaserver was last restarted.
    fn record_mutation(&self, mutation: MetaMutation) -> u64 {
        let at_ms = now_ms();
        if let Some(log) = &self.mutation_log {
            log.append(&mutation, at_ms)
                .expect("failed to append metaserver mutation log");
        }
        at_ms
    }

    pub(crate) fn apply_mutation(&self, mutation: MetaMutation) -> Status {
        self.apply_mutation_at(mutation, now_ms())
    }

    pub(crate) fn apply_mutation_at(&self, mutation: MetaMutation, at_ms: u64) -> Status {
        let status = self.apply_mutation_at_inner(mutation, at_ms);
        if !status.ok {
            // Worth counting wherever it happens, but it is replay this is for:
            // there, a mutation that will not apply means the recorded history
            // and the state it should have produced have parted company, and
            // nothing else would say so.
            self.metrics.record_mutation_apply_failed();
        }
        status
    }

    fn apply_mutation_at_inner(&self, mutation: MetaMutation, at_ms: u64) -> Status {
        match mutation {
            MetaMutation::RegisterShard(request) => self.apply_register(request).status,
            MetaMutation::PublishShardSnapshot(request) => {
                self.apply_publish_shard_snapshot(request).status
            }
            MetaMutation::RegisterServer(request) => self.apply_register_server(request).status,
            MetaMutation::RegisterProxy(request) => self.apply_register_proxy(request).status,
            MetaMutation::AddNamespace(request) => self.apply_add_namespace(request).status,
            MetaMutation::AddTable(request) => self.apply_add_table(request).status,
            MetaMutation::DeleteTable(request) => self.apply_delete_table(request, at_ms).status,
            MetaMutation::UpdateTable(request) => self.apply_update_table(request).status,
            MetaMutation::FreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::UnfreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::FinishLoad(request) => self.apply_finish_load(request).status,
            MetaMutation::UnfreezeServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::UnfreezeProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Normal, at_ms)
                    .status
            }
            MetaMutation::UpdateServer(request) => self.apply_update_server(request).status,
            MetaMutation::SetNamespaceState(request, next) => {
                self.apply_set_namespace_state(request, next, at_ms).status
            }
            MetaMutation::SetReservedNames(reserved) => {
                self.apply_set_reserved_names(reserved).status
            }
            MetaMutation::SetShardPreferredLocation(request) => {
                self.apply_pin_shard(request).status
            }
            MetaMutation::SetMetaChangeMuted(muted) => {
                self.apply_set_meta_change_muted(muted).status
            }
            MetaMutation::PutProxyGroup(request) => {
                self.apply_put_proxy_group(request).status
            }
            MetaMutation::DropProxyGroup(request) => {
                self.apply_drop_proxy_group(request).status
            }
            MetaMutation::SetProxyGroup(request) => {
                self.apply_set_proxy_group(request).status
            }
            MetaMutation::SetShardState(request, state) => {
                self.apply_set_shard_state(request, state).status
            }
            MetaMutation::DropShard(request) => self.apply_drop_shard(request).status,
            MetaMutation::FreezeServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::DropServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Dropped, at_ms)
                    .status
            }
            MetaMutation::FreezeProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Frozen, at_ms)
                    .status
            }
            MetaMutation::DropProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Dropped, at_ms)
                    .status
            }
            MetaMutation::PurgeMeta(plan) => {
                self.apply_meta_purge(&plan);
                Status::ok()
            }
            MetaMutation::InstallSnapshot(snapshot) => {
                self.apply_install_snapshot(*snapshot).status
            }
        }
    }
}

fn scheduler_finish_generation_key(shard_id: ShardId, server_addr: &str) -> String {
    format!("{shard_id}@{server_addr}")
}

fn table_key(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn default_replica_count() -> u64 {
    1
}

fn default_pin_primary() -> bool {
    true
}

fn default_replica_read_policy() -> String {
    "pin_primary".to_string()
}

fn default_max_read_retries() -> u32 {
    1
}

fn default_retry_backoff_ms() -> u64 {
    2
}

fn default_continuous_failed_time_ms() -> u64 {
    10_000
}

fn default_table_io_timeout_ms() -> u64 {
    200
}

fn default_table_connect_timeout_ms() -> u64 {
    200
}

fn validate_serving_options(options: &TableServingOptions) -> Result<(), String> {
    match options.replica_read_policy.as_str() {
        "pin_primary" | "first_replica" | "round_robin_replica" => {}
        _ => {
            return Err(format!(
                "unsupported replica_read_policy {:?}",
                options.replica_read_policy
            ));
        }
    }
    if options.drop_percent > 100 {
        return Err("drop_percent must be <= 100".to_string());
    }
    if options.io_timeout_ms == 0 || options.connect_timeout_ms == 0 {
        return Err("io/connect timeout must be > 0".to_string());
    }
    Ok(())
}

fn proxy_serving_mode_for_state(state: MetaEntityState) -> &'static str {
    match state {
        MetaEntityState::Normal => "serving",
        MetaEntityState::Frozen | MetaEntityState::Dropped => "not_serving",
    }
}

impl TableServingOptionsPatch {
    /// Apply this patch onto `base`, recording which fields it set.
    ///
    /// This is the only place the merge is written. It used to exist twice -- once
    /// here and once in the metaserver's create handler, which rebuilt the same
    /// field-by-field merge by hand and, having no reason to, did not carry over
    /// which fields the caller had actually sent. That is how a table created with
    /// an explicit setting arrived carrying no record of it.
    pub fn onto(&self, base: TableServingOptions) -> TableServingOptions {
        let patch = self;
        let mut options = base;
    let set = |field: TableServingField, options: &mut TableServingOptions| {
        options.set_fields.insert(field.name().to_string());
    };
    if let Some(pin_primary) = patch.pin_primary {
        options.pin_primary = pin_primary;
        set(TableServingField::PinPrimary, &mut options);
    }
    if let Some(replica_read_policy) = &patch.replica_read_policy {
        options.replica_read_policy = replica_read_policy.clone();
        set(TableServingField::ReplicaReadPolicy, &mut options);
    }
    if let Some(preferred_location) = &patch.preferred_location {
        options.preferred_location = preferred_location.clone();
        set(TableServingField::PreferredLocation, &mut options);
    }
    if let Some(drop_percent) = patch.drop_percent {
        options.drop_percent = drop_percent;
        set(TableServingField::DropPercent, &mut options);
    }
    if let Some(max_read_retries) = patch.max_read_retries {
        options.max_read_retries = max_read_retries;
        set(TableServingField::MaxReadRetries, &mut options);
    }
    if let Some(max_write_retries) = patch.max_write_retries {
        options.max_write_retries = max_write_retries;
        set(TableServingField::MaxWriteRetries, &mut options);
    }
    if let Some(retry_backoff_ms) = patch.retry_backoff_ms {
        options.retry_backoff_ms = retry_backoff_ms;
        set(TableServingField::RetryBackoffMs, &mut options);
    }
    if let Some(continuous_failed_time_ms) = patch.continuous_failed_time_ms {
        options.continuous_failed_time_ms = continuous_failed_time_ms;
        set(TableServingField::ContinuousFailedTimeMs, &mut options);
    }
    if let Some(io_timeout_ms) = patch.io_timeout_ms {
        options.io_timeout_ms = io_timeout_ms;
        set(TableServingField::IoTimeoutMs, &mut options);
    }
    if let Some(connect_timeout_ms) = patch.connect_timeout_ms {
        options.connect_timeout_ms = connect_timeout_ms;
        set(TableServingField::ConnectTimeoutMs, &mut options);
    }
    options
    }
}

fn apply_serving_options_patch(
    options: TableServingOptions,
    patch: &TableServingOptionsPatch,
) -> TableServingOptions {
    patch.onto(options)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heartbeat_summarises_every_list_it_carries() {
        // These five are folded before the metaserver's write lock is taken, so
        // the reader waiting on that lock is not also waiting on a walk of one
        // datanode's whole shard list. Only two of them had any coverage, and
        // the fold is only safe if all five still say what the lists say.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);

        let state_for = |shard_id: u64, serving_state: &str, records: u64, bytes: u64| {
            ServerShardServingState {
                shard_id,
                serving_state: serving_state.to_string(),
                total_records: records as usize,
                storage_bytes: bytes,
                ..ServerShardServingState::default()
            }
        };

        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: vec![
                    ShardLoad {
                        shard_id: 1,
                        key_count: 10,
                        memory_bytes: 100,
                    },
                    ShardLoad {
                        shard_id: 2,
                        key_count: 7,
                        memory_bytes: 250,
                    },
                ],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                // "serving" scores 0, "loading" 1 and "failed" 3. The worst is
                // 3 and the total is 4, which is what makes this tell a max from
                // a sum -- the one of the five that would not survive being
                // folded the wrong way. Two states scoring 0 and 3 could not:
                // both give 3, and the assertion passed either way.
                shard_states: vec![
                    state_for(1, "serving", 40, 4_000),
                    state_for(2, "failed", 2, 500),
                    state_for(3, "loading", 1, 100),
                ],
            })
            .status
            .ok);

        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.load_key_count, 17);
        assert_eq!(server.load_memory_bytes, 350);
        assert_eq!(server.reported_record_count, 43);
        assert_eq!(server.reported_storage_bytes, 4_600);
        assert_eq!(server.worst_shard_state_penalty, 3);

        // And a later heartbeat replaces them rather than accumulating.
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count: 1,
                    memory_bytes: 2,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: vec![state_for(1, "serving", 3, 4)],
            })
            .status
            .ok);
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.load_key_count, 1);
        assert_eq!(server.load_memory_bytes, 2);
        assert_eq!(server.reported_record_count, 3);
        assert_eq!(server.reported_storage_bytes, 4);
        assert_eq!(server.worst_shard_state_penalty, 0);
    }

    #[test]
    fn metaserver_tracks_servers_heartbeats_and_shard_routes() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "127.0.0.1:18001".to_string(),
                node_id: 1,
                location: "zone-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 7,
                server_addr: "127.0.0.1:18001".to_string(),
            })
            .status
            .ok
        );
        assert_eq!(meta.get(7).location.unwrap().server_addr, "127.0.0.1:18001");
        let heartbeat = meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "127.0.0.1:18001".to_string(),
            boot_time_ms: 11,
            binary_version: "v1".to_string(),
            shard_loads: vec![ShardLoad {
                shard_id: 7,
                key_count: 10,
                memory_bytes: 100,
            }],
            shard_stat_loads: vec![ShardStatLoad {
                shard_id: 7,
                shard_stat_info: crate::control::ShardStatInfo {
                    shard_id: 7,
                    loaded: true,
                    readonly: false,
                    load_version: 11,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_bucket: 1,
                    end_routing_bucket: 2,
                    total_records: 10,
                    storage_bytes: 100,
                    object_manager: crate::control::ObjectManagerStats {
                        object_count: 10,
                        block_ref_count: 10,
                        dirty_object_count: 1,
                        dirty_bucket_count: 1,
                        routing_bucket_count: 2,
                    },
                },
            }],
            runtime_load: ServerRuntimeLoad {
                queue_depth: 2,
                background_queue_depth: 1,
                queued_shard_count: 1,
                running_shard_count: 1,
                dirty_object_count: 3,
                dirty_shard_count: 1,
                rejected_total: 4,
                rejected_background_total: 1,
                dump_runs: 5,
                compaction_runs: 6,
                gc_runs: 7,
                storage_lifecycle_runs: 8,
                last_meta_topology_version: 12,
                meta_heartbeat_consecutive_failures: 2,
                degraded_reasons: vec!["background_queue_full".to_string()],
                ..ServerRuntimeLoad::default()
            },
            shard_states: vec![ServerShardServingState {
                shard_id: 7,
                serving_state: "serving".to_string(),
                worker_index: 3,
                worker_threads: 4,
                loaded: true,
                readonly: false,
                load_version: 11,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 1,
                end_routing_bucket: 2,
                total_records: 10,
                storage_bytes: 100,
                cache_memory_bytes: 64,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 100,
                wal_sequence: 9,
                dirty_object_count: 1,
                dirty_bucket_count: 1,
            }],
        });
        assert!(heartbeat.status.ok);
        assert_eq!(heartbeat.topology_version, meta.stats().topology_version);
        assert_eq!(heartbeat.server_state, "normal");
        let server = meta.list_servers().servers.remove(0);
        assert_eq!(server.shard_loads[0].key_count, 10);
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.table_name, "tbl");
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.storage_bytes, 100);
        assert_eq!(server.runtime_load.queue_depth, 2);
        assert_eq!(server.runtime_load.storage_lifecycle_runs, 8);
        assert_eq!(server.runtime_load.last_meta_topology_version, 12);
        assert_eq!(server.runtime_load.meta_heartbeat_consecutive_failures, 2);
        assert_eq!(
            server.runtime_load.degraded_reasons,
            vec!["background_queue_full"]
        );
        assert_eq!(server.shard_states[0].serving_state, "serving");
        assert_eq!(server.shard_states[0].wal_sequence, 9);
        assert_eq!(meta.stats().server_heartbeat_total, 1);
    }

    #[test]
    fn a_proxy_that_only_registered_is_not_treated_as_capacity() {
        // `is_idle_candidate` is written to keep a proxy that has never reported in out of a
        // group -- "a registration, not capacity". It tested `last_heartbeat_ms != 0`, and
        // registration stamps that clock, so every registered proxy passed and the rule was
        // never in force. A proxy that registered and died was attached to a group and counted
        // as serving capacity until the failure detector caught up.
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "silent".to_string(),
                namespace: "ns".to_string(),
                location: "zone-a".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.put_proxy_group(PutProxyGroupRequest {
                drop_percent: 0,
                group: "front".to_string(),
                namespace: "ns".to_string(),
                location: String::new(),
                instance_num: 1,
            })
            .status
            .ok
        );

        // It has registered and nothing more, so it is not capacity yet.
        let plan = meta.plan_proxy_calibration_now(ProxyCalibrationOptions::default());
        assert!(
            !plan.attach.iter().any(|item| item.proxy_addr == "silent"),
            "a proxy that has only registered must not be attached, got {:?}",
            plan.attach
        );

        // One heartbeat is the difference between a registration and capacity.
        assert!(
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                boot_time_ms: 1,
                proxy_addr: "silent".to_string(),
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        let plan = meta.plan_proxy_calibration_now(ProxyCalibrationOptions::default());
        assert!(
            plan.attach.iter().any(|item| item.proxy_addr == "silent"),
            "once it has reported in it is capacity and the group should claim it, got {:?}",
            plan.attach
        );
    }

    #[test]
    fn metaserver_preflight_reports_inventory_and_frozen_resources() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "server-a".to_string(),
                node_id: 7,
                location: "zone-a".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "zone-a".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                registered_at_ms: 0,
                shard_id: 1,
                server_addr: "server-a".to_string(),
            })
            .status
            .ok
        );

        let healthy = meta.preflight_report();
        assert!(healthy.status.ok);
        assert_eq!(healthy.normal_servers, 1);
        assert_eq!(healthy.normal_proxies, 1);
        assert_eq!(healthy.shard_routes, 1);
        assert!(healthy.degraded_reasons.is_empty());

        assert!(
            meta.freeze_server(StateChangeRequest {
                reason: FreezeReason::Unspecified,
                endpoint: "server-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                reason: FreezeReason::Unspecified,
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        let degraded = meta.preflight_report();
        assert!(!degraded.status.ok);
        assert_eq!(degraded.frozen_servers, 1);
        assert_eq!(degraded.frozen_proxies, 1);
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_servers".to_string()));
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_proxies".to_string()));
    }

    #[test]
    fn metaserver_freezes_stale_servers_from_heartbeat_age() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "stale".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let report = meta.freeze_stale_servers(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["stale".to_string()]);
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_failure_detector_loop_freezes_stale_servers_and_proxies() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "stale-server".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "stale-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "z".to_string(),
            config_version: 1,
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let _detector = meta.start_failure_detector_loop(0, 10);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if meta.list_servers().servers[0].state == MetaEntityState::Frozen
                && meta.list_proxies().proxies[0].state == MetaEntityState::Frozen
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("failure detector loop did not freeze stale resources");
    }

    #[test]
    fn metaserver_detects_a_datanode_that_restarted_in_place() {
        // The heartbeats never stop, so nothing in the stale-timeout path notices
        // this. But the restarted node has dropped every shard the metaserver
        // still believes it serves, and reads routed there will all miss.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });

        let heartbeat = |boot_time_ms: u64| ServerHeartbeatRequest {
            server_addr: "node-a".to_string(),
            boot_time_ms,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        };
        let server_state = || {
            meta.list_servers()
                .servers
                .into_iter()
                .find(|server| server.server_addr == "node-a")
                .expect("registered")
        };

        // The first heartbeat anchors the boot time; repeats of it are normal.
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(!server_state().reboot_detected);
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert!(!server_state().reboot_detected);

        // A different boot time means the process restarted underneath us.
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);
        // The anchor does not follow the new value, so the verdict is sticky
        // rather than resetting itself on the next beat.
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);

        // Re-registering is how a datanode says it is ready to be trusted again.
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(!server_state().reboot_detected);
        assert_eq!(server_state().reported_boot_time_ms, 0);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 2_000);
        assert!(!server_state().reboot_detected);
    }

    #[test]
    fn a_datanode_that_never_reports_a_boot_time_is_not_flagged() {
        // Older datanodes send 0. Treating that as a changed boot time would
        // convict the entire fleet on upgrade.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        for boot_time_ms in [0, 0, 0] {
            assert!(meta
                .server_heartbeat(ServerHeartbeatRequest {
                    server_addr: "node-a".to_string(),
                    boot_time_ms,
                    binary_version: "v1".to_string(),
                    shard_loads: Vec::new(),
                    shard_stat_loads: Vec::new(),
                    runtime_load: ServerRuntimeLoad::default(),
                    shard_states: Vec::new(),
                })
                .status
                .ok);
        }
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert!(!server.reboot_detected);
    }

    fn register(meta: &SingleNodeMeta, addr: &str) -> AckResponse {
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: addr.to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        })
    }

    fn server_state(meta: &SingleNodeMeta, addr: &str) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
    }

    fn joined(meta: &SingleNodeMeta) -> Vec<String> {
        let state = meta.inner.read().expect("meta lock poisoned");
        state
            .topology_events
            .iter()
            .map(|event| format!("{}:{}", event.kind, event.resource))
            .collect()
    }

    #[test]
    fn a_proxy_joining_is_recorded() {
        // A server registering has always been recorded; a proxy registering
        // was not, so a proxy could enter the cluster and leave no trace in the
        // history at all.
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-1:9000".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        assert!(
            joined(&meta).contains(&"register_proxy:proxy:proxy-1:9000".to_string()),
            "a proxy joined and the history did not notice: {:?}",
            joined(&meta)
        );
    }

    #[test]
    fn a_namespace_being_created_is_recorded() {
        // Freezing and dropping a namespace were recorded, but creating one was
        // not -- so the history could show a namespace being frozen with no
        // record of it ever having existed.
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "tenant-a".to_string(),
        });
        assert!(
            joined(&meta).contains(&"add_namespace:namespace:tenant-a".to_string()),
            "a namespace was created and the history did not notice: {:?}",
            joined(&meta)
        );
    }

    #[test]
    fn creating_the_same_namespace_twice_records_once() {
        // The call is idempotent. An event per repeat would advance the
        // topology version for a change that did not happen, and a caller
        // polling the history would see movement where there was none.
        let meta = SingleNodeMeta::default();
        for _ in 0..3 {
            meta.add_namespace(AddNamespaceRequest {
                namespace: "tenant-a".to_string(),
            });
        }
        let recorded = joined(&meta)
            .into_iter()
            .filter(|entry| entry == "add_namespace:namespace:tenant-a")
            .count();
        assert_eq!(recorded, 1, "an idempotent call recorded more than once");
    }

    #[test]
    fn a_proxy_turned_away_is_not_recorded_as_having_joined() {
        // The refusal paths return before the recording. A proxy under
        // conviction that is refused must not appear in the history as having
        // joined, or the history contradicts the state.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        let register_proxy = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
        };
        let joins = || {
            joined(&meta)
                .into_iter()
                .filter(|entry| entry.starts_with("register_proxy:"))
                .count()
        };
        assert!(register_proxy().status.ok);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let before = joins();
        assert_eq!(register_proxy().status.code, "conviction_requires_unfreeze");
        assert_eq!(joins(), before, "a refused registration was recorded anyway");
    }

    #[test]
    fn a_freeze_records_why_it_happened() {
        let meta = SingleNodeMeta::default();
        register(&meta, "node-a");
        register(&meta, "node-b");
        std::thread::sleep(std::time::Duration::from_millis(2));

        // The detector's own freeze is a conviction.
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            server_state(&meta, "node-a").freeze_reason,
            FreezeReason::Unresponsive
        );
        assert!(server_state(&meta, "node-a").freeze_reason.is_conviction());

        // An operator freeze is not.
        assert!(meta.unfreeze_server(unfreeze_request("node-b")).status.ok);
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-b".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
        assert_eq!(
            server_state(&meta, "node-b").freeze_reason,
            FreezeReason::Operator
        );
        assert!(!server_state(&meta, "node-b").freeze_reason.is_conviction());
    }

    fn unfreeze_request(addr: &str) -> StateChangeRequest {
        StateChangeRequest {
            endpoint: addr.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        }
    }

    #[test]
    fn the_default_detector_reports_what_it_freezes() {
        // temporalstore_meta_convicted_total is exported unconditionally, and
        // only the adaptive detector was recording into it. The adaptive one is
        // off unless asked for, so on a default metaserver the counter sat at
        // zero while this detector froze servers and proxies -- a confident
        // wrong number, which reads worse than an absent series.
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        std::thread::sleep(std::time::Duration::from_millis(2));

        let report = meta.freeze_stale_resources(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers.len(), 1, "{report:?}");
        assert_eq!(report.frozen_proxies.len(), 1, "{report:?}");

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"server\"} 1"),
            "the detector froze a server and the counter did not move:\n{exported}"
        );
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"proxy\"} 1"),
            "the detector froze a proxy and the counter did not move:\n{exported}"
        );
    }

    #[test]
    fn each_tier_counts_only_its_own_freezes() {
        // `record_conviction` sums both lists, so one call carrying servers and
        // proxies together would count every freeze under both tiers. The
        // counts are deliberately asymmetric -- two servers, one proxy -- so
        // any cross-contamination shows up as a wrong number rather than a
        // coincidentally equal one.
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        register(&meta, "server-b");
        assert!(meta
            .register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let exported = meta.subsystem_metrics().prometheus();
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"server\"} 2"),
            "two servers frozen must count two under server:
{exported}"
        );
        assert!(
            exported.contains("temporalstore_meta_convicted_total{tier=\"proxy\"} 1"),
            "one proxy frozen must count one under proxy:
{exported}"
        );
    }

    #[test]
    fn a_convicted_server_cannot_register_its_way_back_into_service() {
        // Without this the freeze cooldown is the only guard, and it defaults to
        // zero -- so the convicted node clears the metaserver's decision simply
        // by asking, and conviction is advisory.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);

        let rejected = register(&meta, "node-a");
        assert!(!rejected.status.ok);
        assert_eq!(rejected.status.code, "conviction_requires_unfreeze");
        assert_eq!(
            server_state(&meta, "node-a").state,
            MetaEntityState::Frozen
        );

        // An operator unfreeze is the way back, and afterwards the node
        // registers normally again.
        assert!(meta.unfreeze_server(unfreeze_request("node-a")).status.ok);
        assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
        assert_eq!(
            server_state(&meta, "node-a").freeze_reason,
            FreezeReason::Unspecified
        );
        assert!(register(&meta, "node-a").status.ok);
    }

    #[test]
    fn without_the_lock_a_convicted_server_still_recovers_by_registering() {
        // The default path is unchanged: this is the automatic recovery that
        // deployments running a zero freeze cooldown depend on.
        let meta = SingleNodeMeta::default();
        register(&meta, "node-a");
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert!(register(&meta, "node-a").status.ok);
        assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
    }

    #[test]
    fn an_operator_freeze_is_not_locked_against_re_registration() {
        // The lock is about the metaserver's own verdicts. A maintenance freeze
        // already has an operator in the loop, and the freeze cooldown is the
        // knob for holding a node out.
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
        assert!(register(&meta, "node-a").status.ok);
    }

    #[test]
    fn a_restarted_server_is_locked_out_under_its_own_reason() {
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        register(&meta, "node-a");
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Restarted,
            })
            .status
            .ok);
        let rejected = register(&meta, "node-a");
        assert_eq!(rejected.status.code, "conviction_requires_unfreeze");
        assert!(rejected.status.message.contains("restarted"));
    }

    #[test]
    fn a_convicted_proxy_is_locked_out_too() {
        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        let register_proxy = || {
            meta.register_proxy(RegisterProxyRequest {
                registered_at_ms: 0,
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "rack-1".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
        };
        register_proxy();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            register_proxy().status.code,
            "conviction_requires_unfreeze"
        );
        assert!(meta.unfreeze_proxy(unfreeze_request("proxy-a")).status.ok);
        assert!(register_proxy().status.ok);
    }

    #[test]
    fn an_unfreeze_survives_mutation_log_replay() {
        // Unfreeze was previously the one state change that recorded no
        // mutation, so recovery would have silently re-frozen the resource.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("unfreeze-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            register(&meta, "node-a");
            std::thread::sleep(std::time::Duration::from_millis(2));
            assert!(meta.freeze_stale_resources(0).status.ok);
            assert!(meta.unfreeze_server(unfreeze_request("node-a")).status.ok);
            assert_eq!(server_state(&meta, "node-a").state, MetaEntityState::Normal);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            server_state(&recovered, "node-a").state,
            MetaEntityState::Normal
        );
        assert_eq!(
            server_state(&recovered, "node-a").freeze_reason,
            FreezeReason::Unspecified
        );
    }

    fn one_rack_cluster(meta: &SingleNodeMeta, servers: &[&str], replica_count: u64) -> Vec<String> {
        for (index, addr) in servers.iter().enumerate() {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: index as u64 + 1,
                    // One rack: the ladder can separate nothing, so every
                    // replica after the first comes from the fallback fill.
                    location: "us-east/dc1/az1/rack1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "t".to_string(),
                first_shard_id: 700,
                shard_count: 1,
                replica_count,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);
        meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "t".to_string(),
            old_topology_version: 0,
            client_location: String::new(),
        })
        .shards
        .into_iter()
        .next()
        .expect("one shard")
        .replicas
    }

    #[test]
    fn a_shard_does_not_stack_its_replicas_on_one_host() {
        // Four datanodes, two per physical host, all in one rack -- an ordinary
        // small deployment. Both replicas landed on host-a while host-b sat
        // idle: the shard reported two replicas and lost both to one host.
        let meta = SingleNodeMeta::default();
        let replicas = one_rack_cluster(
            &meta,
            &["host-a:1001", "host-a:1002", "host-b:1001", "host-b:1002"],
            2,
        );
        let hosts: BTreeSet<String> = replicas
            .iter()
            .map(|addr| super::topology_helpers::server_host(addr))
            .collect();
        assert_eq!(replicas.len(), 2, "{replicas:?}");
        assert_eq!(
            hosts.len(),
            2,
            "both replicas share a host while another was free: {replicas:?}"
        );
    }

    #[test]
    fn a_host_is_reused_only_when_there_is_no_other() {
        // The point is to spread, not to refuse. With one host there is nothing
        // to spread across, and the shard must still reach its replica count
        // rather than come back short.
        let meta = SingleNodeMeta::default();
        let replicas = one_rack_cluster(&meta, &["host-a:1001", "host-a:1002"], 2);
        assert_eq!(
            replicas.len(),
            2,
            "spreading turned a fill into a shortfall: {replicas:?}"
        );
    }

    #[test]
    fn the_topology_carries_the_load_version_the_primary_reports() {
        // The fencing token only reaches a client if the topology carries it. Without it a client
        // has no version to send, so the checked execute path cannot be used at all and a write
        // routed on a stale topology is accepted by a node that no longer owns the shard.
        let meta = SingleNodeMeta::default();
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "us-east/dc1/az1/rack1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 1,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);

        let topology = || {
            meta.get_table_topology(GetTableTopologyRequest {
                client_location: String::new(),
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
            })
        };

        // CONTROL: nothing reported yet. An unknown must read as 0 -- a token invented here would
        // fence out every client the moment this field started being sent.
        let before = topology();
        assert!(before.status.ok);
        assert_eq!(before.shards.len(), 1);
        assert_eq!(
            before.shards[0].load_version, 0,
            "an unreported shard must carry 0, not a token that would fence a client out"
        );
        let primary = before.shards[0]
            .primary
            .clone()
            .expect("the shard should have a primary");

        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: primary,
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: Vec::new(),
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: vec![ServerShardServingState {
                    shard_id: 1,
                    loaded: true,
                    load_version: 7,
                    ..ServerShardServingState::default()
                }],
            })
            .status
            .ok);

        let after = topology();
        assert!(after.status.ok);
        assert_eq!(after.shards.len(), 1);
        assert_eq!(
            after.shards[0].load_version, 7,
            "the topology must carry the load_version its primary reports"
        );
    }

    #[test]
    fn replicas_spread_across_availability_units_not_just_racks() {
        // Four servers, two availability units, two racks each. Comparing whole
        // location strings makes rack1 and rack2 of az1 look like "different
        // locations", so both replicas of a shard land inside az1 and losing
        // that unit loses both. Placement must reach for az2 instead.
        let meta = SingleNodeMeta::default();
        for (addr, location) in [
            ("a1", "us-east/dc1/az1/rack1"),
            ("a2", "us-east/dc1/az1/rack2"),
            ("b1", "us-east/dc1/az2/rack1"),
            ("b2", "us-east/dc1/az2/rack2"),
        ] {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: 1,
                    location: location.to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 2,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.shards.len(), 1);
        let replicas = &topology.shards[0].replicas;
        assert_eq!(replicas.len(), 2, "expected two replicas, got {replicas:?}");

        let units = replicas
            .iter()
            .map(|addr| {
                let server = meta
                    .list_servers()
                    .servers
                    .into_iter()
                    .find(|server| &server.server_addr == addr)
                    .expect("replica is a registered server");
                Location::parse(&server.location).ancestor(3).to_path()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            units.len(),
            2,
            "both replicas landed in the same availability unit: {units:?}"
        );
    }

    fn relabel_meta(servers: &[(&str, &str)]) -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (addr, location) in servers {
            assert!(meta
                .register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: addr.to_string(),
                    node_id: 1,
                    location: location.to_string(),
                    binary_version: "v1".to_string(),
                })
                .status
                .ok);
        }
        meta
    }

    fn located(meta: &SingleNodeMeta, addr: &str) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == addr)
            .expect("registered")
    }

    #[test]
    fn relabelling_changes_the_location_and_nothing_else() {
        // The whole reason this exists rather than re-registering: a
        // re-registration resets heartbeat state, reported shards and runtime
        // load, which is a disruptive way to fix a label.
        let meta = relabel_meta(&[("node-a", "us-east/dc1/az1/rack1")]);
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 4_242,
                binary_version: "v9".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: 7,
                    key_count: 11,
                    memory_bytes: 22,
                }],
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad {
                    queue_depth: 3,
                    ..ServerRuntimeLoad::default()
                },
                shard_states: Vec::new(),
            })
            .status
            .ok);
        let before = located(&meta, "node-a");

        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "us-east/dc1/az2/rack9".to_string(),
            })
            .status
            .ok);

        let after = located(&meta, "node-a");
        assert_eq!(after.location, "us-east/dc1/az2/rack9");
        assert_eq!(after.last_heartbeat_ms, before.last_heartbeat_ms);
        assert_eq!(after.boot_time_ms, 4_242);
        assert_eq!(after.binary_version, "v9");
        assert_eq!(after.shard_loads, before.shard_loads);
        assert_eq!(after.runtime_load.queue_depth, 3);
        assert_eq!(after.state, MetaEntityState::Normal);
    }

    #[test]
    fn relabelling_bumps_the_topology_version() {
        // Placement is derived from location on every topology read, so without
        // this bump clients keep serving from the placement the old label implied.
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        let before = meta.stats().topology_version;
        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .ok);
        assert!(meta.stats().topology_version > before);
    }

    #[test]
    fn relabelling_moves_where_replicas_land() {
        // The payoff. Four servers crammed into one availability unit can only
        // spread replicas across racks; relabelling one into a second unit lets
        // placement reach for the wider split.
        let meta = relabel_meta(&[
            ("a1", "us-east/dc1/az1/rack1"),
            ("a2", "us-east/dc1/az1/rack2"),
            ("a3", "us-east/dc1/az1/rack3"),
            ("a4", "us-east/dc1/az1/rack4"),
        ]);
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta
            .add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                first_shard_id: 1,
                shard_count: 1,
                replica_count: 2,
                partition_version: 0,
                serving_options: TableServingOptions::default(),
            })
            .status
            .ok);

        let units = |meta: &SingleNodeMeta| {
            let topology = meta.get_table_topology(GetTableTopologyRequest {
                client_location: String::new(),
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
                old_topology_version: 0,
            });
            topology.shards[0]
                .replicas
                .iter()
                .map(|addr| Location::parse(&located(meta, addr).location).ancestor(3).to_path())
                .collect::<std::collections::BTreeSet<_>>()
        };

        // Everything is in az1, so both replicas must share it.
        assert_eq!(units(&meta).len(), 1);

        assert!(meta
            .update_server(UpdateServerRequest {
                server_addr: "a2".to_string(),
                location: "us-east/dc1/az2/rack1".to_string(),
            })
            .status
            .ok);

        // Now the shard can span two units, and does.
        assert_eq!(units(&meta).len(), 2);
    }

    #[test]
    fn relabelling_rejects_an_unknown_or_frozen_server() {
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "ghost".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .code,
            "server_not_found"
        );

        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(meta.freeze_stale_resources(0).status.ok);
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-2".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );
    }

    #[test]
    fn relabelling_to_the_same_location_is_not_modified() {
        // So a config-reconciler running this on a loop does not bump the
        // topology version on every pass and invalidate every client's cache.
        let meta = relabel_meta(&[("node-a", "rack-1")]);
        let before = meta.stats().topology_version;
        assert_eq!(
            meta.update_server(UpdateServerRequest {
                server_addr: "node-a".to_string(),
                location: "rack-1".to_string(),
            })
            .status
            .code,
            "not_modified"
        );
        assert_eq!(meta.stats().topology_version, before);
    }

    #[test]
    fn a_relabel_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("relabel-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta
                .update_server(UpdateServerRequest {
                    server_addr: "node-a".to_string(),
                    location: "rack-9".to_string(),
                })
                .status
                .ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(located(&recovered, "node-a").location, "rack-9");
    }

    fn stop_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        meta
    }

    fn stop(endpoint: &str) -> NotifyStopRequest {
        NotifyStopRequest {
            endpoint: endpoint.to_string(),
        }
    }

    fn stopped_server(meta: &SingleNodeMeta) -> ServerMetaInfo {
        meta.list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered")
    }

    #[test]
    fn announcing_a_stop_beats_the_failure_detector() {
        // The point of the whole thing. A clean shutdown is otherwise
        // indistinguishable from a crash: the node stops heartbeating and the
        // metaserver waits out the detection window, routing reads to a process
        // that has already exited.
        let meta = stop_meta();

        // The detector is not going to help: the heartbeat is fresh, so a sweep
        // with a realistic window leaves the node serving.
        assert!(meta.freeze_stale_resources(30_000).status.ok);
        assert_eq!(stopped_server(&meta).state, MetaEntityState::Normal);

        // Announcing the stop takes it out immediately.
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        let server = stopped_server(&meta);
        assert_eq!(server.state, MetaEntityState::Frozen);
        assert_eq!(server.freeze_reason, FreezeReason::Stopping);
    }

    #[test]
    fn a_stopping_server_is_not_a_conviction_and_can_come_back() {
        // A node that announced its own shutdown is expected back. Treating the
        // freeze as a conviction would turn every clean restart into an operator
        // ticket under the self-clearing lock.
        assert!(!FreezeReason::Stopping.is_conviction());

        let meta = SingleNodeMeta::default().with_conviction_lock(true);
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);

        // It restarts and registers again; the lock does not stand in its way.
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v2".to_string(),
            })
            .status
            .ok);
        assert_eq!(stopped_server(&meta).state, MetaEntityState::Normal);
    }

    #[test]
    fn announcing_a_stop_drops_a_proxy_rather_than_freezing_it() {
        // A proxy holds no data, so there is nothing to preserve by keeping a
        // frozen tombstone -- and a frozen proxy stays in the damage accounting
        // the conviction gate reads.
        let meta = stop_meta();
        assert!(meta.notify_proxy_stop(stop("proxy-a")).status.ok);
        let proxy = meta
            .list_proxies()
            .proxies
            .into_iter()
            .find(|proxy| proxy.proxy_addr == "proxy-a")
            .expect("registered");
        assert_eq!(proxy.state, MetaEntityState::Dropped);
    }

    #[test]
    fn announcing_a_stop_twice_is_not_an_error() {
        // A shutdown hook may retry, or race the failure detector. Neither
        // should produce a spurious failure in a node's last log line.
        let meta = stop_meta();
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        assert_eq!(
            meta.notify_server_stop(stop("node-a")).status.code,
            "not_modified"
        );
        assert!(meta.notify_proxy_stop(stop("proxy-a")).status.ok);
        assert_eq!(
            meta.notify_proxy_stop(stop("proxy-a")).status.code,
            "not_modified"
        );
    }

    #[test]
    fn announcing_a_stop_for_an_unknown_endpoint_is_rejected() {
        let meta = stop_meta();
        assert_eq!(
            meta.notify_server_stop(stop("ghost")).status.code,
            "server_not_found"
        );
        assert_eq!(
            meta.notify_proxy_stop(stop("ghost")).status.code,
            "proxy_not_found"
        );
    }

    #[test]
    fn a_stop_leaves_no_freeze_cooldown_behind() {
        // The node is coming back; a cooldown would delay its return for no
        // reason, since nothing about it was judged unhealthy.
        let meta = stop_meta();
        assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        // A zero cooldown is stored as "expires now" rather than as zero, so
        // assert the behaviour that matters: the window is not in the future,
        // and the node can register again the moment it comes back.
        assert!(stopped_server(&meta).freeze_cooldown_until_ms <= now_ms());
        assert!(meta
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v2".to_string(),
            })
            .status
            .ok);
    }

    #[test]
    fn a_stop_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("stop-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta.notify_server_stop(stop("node-a")).status.ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        let server = recovered
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert_eq!(server.state, MetaEntityState::Frozen);
        assert_eq!(server.freeze_reason, FreezeReason::Stopping);
    }

    fn muted_meta_with_a_server() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(meta.set_meta_change_muted(true).status.ok);
        meta
    }

    #[test]
    fn muting_refuses_every_recorded_mutation() {
        let meta = muted_meta_with_a_server();
        assert!(meta.is_meta_change_muted());

        for (what, status) in [
            (
                "freeze_server",
                meta.freeze_server(StateChangeRequest {
                    endpoint: "node-a".to_string(),
                    freeze_cooldown_ms: 0,
                    reason: FreezeReason::Operator,
                })
                .status,
            ),
            (
                "add_table",
                meta.add_table(AddTableRequest {
                    namespace: "ns".to_string(),
                    table_name: "orders".to_string(),
                    first_shard_id: 1,
                    shard_count: 1,
                    replica_count: 1,
                    partition_version: 0,
                    serving_options: TableServingOptions::default(),
                })
                .status,
            ),
            (
                "register_server",
                meta.register_server(RegisterServerRequest {
                    registered_at_ms: 0,
                    numa_nodes: Vec::new(),
                    server_addr: "node-b".to_string(),
                    node_id: 2,
                    location: "rack-1".to_string(),
                    binary_version: "v1".to_string(),
                })
                .status,
            ),
            (
                "reassign_shard",
                meta.reassign_shard(1, "node-a").status,
            ),
        ] {
            assert!(!status.ok, "{what} should have been refused");
            assert_eq!(status.code, "meta_change_muted", "{what}");
        }

        // Nothing landed: the fleet is exactly as it was.
        assert_eq!(meta.list_servers().servers.len(), 1);
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Normal
        );
        assert!(meta.list_tables().tables.is_empty());
    }

    #[test]
    fn muting_does_not_touch_reads_or_heartbeats() {
        // The mute gates recorded mutations. A heartbeat records none -- it is
        // liveness, not a metadata change -- and muting it would blind the
        // failure detector to the very fleet the operator is inspecting.
        let meta = muted_meta_with_a_server();
        assert!(meta
            .server_heartbeat(ServerHeartbeatRequest {
                server_addr: "node-a".to_string(),
                boot_time_ms: 1,
                binary_version: "v1".to_string(),
                shard_loads: Vec::new(),
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            })
            .status
            .ok);
        assert!(meta.list_servers().status.ok);
        assert!(meta.list_namespaces().status.ok);
        assert!(meta.stats().shard_count == 0);
    }

    #[test]
    fn a_muted_metaserver_will_not_admit_a_restarting_datanode() {
        // The sharp edge, asserted so it is a decision rather than a surprise:
        // registration is a recorded mutation, so while muted a node that
        // restarts cannot rejoin until an operator resumes.
        let meta = muted_meta_with_a_server();
        let rejoin = meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v2".to_string(),
        });
        assert_eq!(rejoin.status.code, "meta_change_muted");
    }

    #[test]
    fn resume_is_never_itself_muted() {
        // An operator must always be able to undo this, or the lever is a trap.
        let meta = muted_meta_with_a_server();
        assert!(meta.set_meta_change_muted(false).status.ok);
        assert!(!meta.is_meta_change_muted());
        assert!(meta
            .freeze_server(StateChangeRequest {
                endpoint: "node-a".to_string(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Operator,
            })
            .status
            .ok);
    }

    #[test]
    fn meta_info_reports_whether_change_is_muted() {
        let meta = SingleNodeMeta::default();
        assert!(!meta.info().meta_change_muted);
        meta.set_meta_change_muted(true);
        assert!(meta.info().meta_change_muted);
    }

    #[test]
    fn a_mute_survives_mutation_log_replay() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("mute-mutations.jsonl");
        {
            let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            });
            assert!(meta.set_meta_change_muted(true).status.ok);
        }
        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(recovered.is_meta_change_muted());
        // The registration recorded before the mute still replayed: the guard
        // applies to live calls, not to replaying a log of accepted changes.
        assert_eq!(recovered.list_servers().servers.len(), 1);
    }

    fn meta_with_history() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        register(&meta, "server-a");
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: "ns".to_string()
            })
            .status
            .ok);
        assert!(
            !meta
                .inner
                .read()
                .expect("meta lock poisoned")
                .topology_events
                .is_empty(),
            "the fixture recorded no history to lose"
        );
        meta
    }

    #[test]
    fn the_change_history_survives_a_snapshot_install() {
        // The topology version travelled and the history it belongs to did not,
        // so a peer inherited a version with nothing behind it.
        let meta = meta_with_history();
        let expected = meta
            .inner
            .read()
            .expect("meta lock poisoned")
            .topology_events
            .clone();

        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);

        let carried = peer
            .inner
            .read()
            .expect("meta lock poisoned")
            .topology_events
            .clone();
        assert_eq!(carried, expected, "the change history was dropped on install");
    }

    #[test]
    fn a_peer_that_installs_a_snapshot_is_not_reported_as_missing_its_history() {
        // The readiness report treats the history as evidence, so dropping it on
        // install made a healthy peer report its own control plane as blocked on
        // evidence it had a moment earlier.
        let meta = meta_with_history();
        assert!(meta.control_plane_parity_report().topology_history_ready);

        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(meta.export_snapshot()).status.ok);

        let report = peer.control_plane_parity_report();
        assert!(
            report.topology_history_ready,
            "a peer reported its history missing right after inheriting it"
        );
        assert!(
            !report
                .blockers
                .iter()
                .any(|blocker| blocker.contains("topology history")),
            "{:?}",
            report.blockers
        );
    }

    #[test]
    fn a_snapshot_written_before_this_field_still_installs() {
        // The field is defaulted, so an older snapshot on disk -- or from a peer
        // that has not been upgraded -- must still load rather than be rejected
        // as malformed.
        let meta = meta_with_history();
        let mut encoded: serde_json::Value =
            serde_json::to_value(meta.export_snapshot()).expect("snapshot encodes");
        encoded
            .as_object_mut()
            .expect("snapshot is an object")
            .remove("topology_events")
            .expect("the field was there to remove");

        let older: MetaSnapshot = serde_json::from_value(encoded).expect("an older snapshot loads");
        assert!(older.topology_events.is_empty());
        let peer = SingleNodeMeta::default();
        assert!(peer.install_snapshot(older).status.ok);
    }

    #[test]
    fn a_mute_survives_a_snapshot_round_trip() {
        // Otherwise a peer installing the snapshot quietly resumes the change an
        // operator stopped.
        let meta = SingleNodeMeta::default();
        meta.set_meta_change_muted(true);
        let snapshot = meta.export_snapshot();
        assert!(snapshot.meta_change_muted);

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        assert!(restored.is_meta_change_muted());
    }

    /// One table, one shard, registered to node-a, with node-b also live.
    fn owned_shard_meta() -> SingleNodeMeta {
        let meta = SingleNodeMeta::default();
        for (addr, node_id, location) in [("node-a", 1, "rack-1"), ("node-b", 2, "rack-2")] {
            meta.register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: addr.to_string(),
                node_id,
                location: location.to_string(),
                binary_version: "v1".to_string(),
            });
        }
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: "node-a".to_string(),
        });
        meta
    }

    fn routed_shard(meta: &SingleNodeMeta) -> TableShard {
        meta.get_table_topology(GetTableTopologyRequest {
            client_location: String::new(),
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            old_topology_version: 0,
        })
        .shards
        .into_iter()
        .next()
        .expect("one shard")
    }

    fn state_change(endpoint: &str) -> StateChangeRequest {
        StateChangeRequest {
            endpoint: endpoint.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        }
    }

    #[test]
    fn a_shard_is_not_routed_to_a_frozen_owner() {
        // Placement already refuses to pick a server that is not Normal. The
        // recorded owner is the one entry that reaches the topology without
        // passing that filter, so freezing a server left every shard it owns
        // pointing straight at it.
        let meta = owned_shard_meta();
        assert_eq!(routed_shard(&meta).primary, Some("node-a".to_string()));

        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        let shard = routed_shard(&meta);
        assert_eq!(shard.primary, None, "a frozen server is still being routed to");
        assert!(
            !shard.replicas.contains(&"node-a".to_string()),
            "a frozen server is still listed as a replica"
        );
    }

    #[test]
    fn a_shard_is_not_routed_to_a_dropped_owner() {
        // A dropped server is not coming back, so naming it is simply false.
        let meta = owned_shard_meta();
        assert!(meta.drop_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, None);
    }

    #[test]
    fn unfreezing_the_owner_puts_the_shard_back() {
        let meta = owned_shard_meta();
        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, None);
        assert!(meta.unfreeze_server(state_change("node-a")).status.ok);
        assert_eq!(routed_shard(&meta).primary, Some("node-a".to_string()));
    }

    #[test]
    fn a_stale_route_does_not_get_a_stand_in_primary() {
        // The candidate scan would happily nominate node-b, which has never
        // loaded this shard: a client that followed the nomination would read
        // an empty shard and believe it.
        let meta = owned_shard_meta();
        assert!(meta.freeze_server(state_change("node-a")).status.ok);
        let shard = routed_shard(&meta);
        assert_ne!(shard.primary, Some("node-b".to_string()));
        assert_eq!(shard.primary, None);
    }

    #[test]
    fn a_shard_with_no_owner_yet_still_gets_a_proposed_placement() {
        // Guards the other direction: a table that nothing has registered
        // against must still be told where its shards should go.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
