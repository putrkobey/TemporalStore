// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::http::{
    get_json, get_json_with_options, post_json, post_json_with_options,
    post_json_with_options_and_headers, HttpError,
    HttpRequestOptions,
};
use crate::meta::GetShardResponse;
use crate::meta::{
    GetTableTopologyRequest, ServerEndpoint, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest,
};
#[path = "client/client_routing.rs"]
mod client_routing;
#[path = "client/client_meta_sync.rs"]
mod client_meta_sync;
#[path = "client/client_commands.rs"]
mod commands;
// The proxy's drop decision hashes the same routing key this builds; it is re-exported
// rather than copied because the copy that used to live there had drifted.
pub(crate) use commands::command_routing_key;
#[path = "client/table_feature.rs"]
mod table_feature;
#[path = "client/table_context.rs"]
mod table_context;
#[path = "client/table_control_state.rs"]
mod table_control_state;
#[path = "client/client_retry.rs"]
mod retry;
#[path = "client/client_routing_operations.rs"]
mod routing;

use client_routing::batch_read_policy;
use commands::{command_is_dropped, command_key, is_write};
use retry::{
    classify_retry_decision, replica_read_policy_from_meta, retry_attempts_for,
    sleep_before_retry,
};
pub use routing::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, bucket_id_for_key, stable_key_hash,
};

use crate::types::{
    parse_feature_filters, BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse,
    ContextEvent, ContextIndexRef, ContextNode, ContextPackAudit, ContextDirtyNode,
    ExecuteRequest, ExecuteResponse, FeatureFilter, FeaturePoint, FeatureWritePolicy,
    ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status,
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] HttpError),
    #[error("server returned error: {0}")]
    Status(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// A write failed in a way that does not say whether it was applied.
    ///
    /// The datanode stopped answering; it may have processed the write, or not. The write was
    /// deliberately not sent again, because repeating it could apply it twice. That leaves the
    /// caller with a decision only the caller can make -- reconcile, or accept the risk -- and
    /// it can only make it if it is told this is what happened, rather than being handed the
    /// same generic failure as a write that provably never arrived.
    #[error("write outcome unknown: {0}")]
    WriteOutcomeUnknown(String),
    #[error("unexpected response for {operation}: {response:?}")]
    UnexpectedResponse {
        operation: &'static str,
        response: CommandResponse,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOptions {
    pub proxy_addr: String,
    pub meta_addr: Option<String>,
    pub default_shard_id: ShardId,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
    pub max_read_retries: usize,
    pub max_write_retries: usize,
    pub retry_backoff_ms: u64,
    pub route_cache_ttl_ms: u64,
    pub meta_sync_interval_ms: u64,
    pub topo_error_retry_interval_ms: u64,
    pub meta_sync_deadline_ms: u64,
    pub meta_sync_jitter_percent: u8,
    pub local_location: String,
    pub drop_percent: u8,
    /// Re-resolve a shard's route and retry once when its backend fails, rather than
    /// surfacing the error against the address we already had. This is what recovers a
    /// request whose shard has moved, so it defaults ON; turning it off is for deployments
    /// that would rather see the failure immediately than pay a metaserver round-trip and a
    /// second attempt on every backend blip.
    pub refresh_route_on_backend_error: bool,
}

impl ClientOptions {
    pub fn proxy(proxy_addr: impl Into<String>) -> Self {
        Self {
            proxy_addr: proxy_addr.into(),
            ..Self::default()
        }
    }

    fn http_options(&self) -> HttpRequestOptions {
        HttpRequestOptions {
            connect_timeout_ms: self.connect_timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
            max_retries: self.max_retries,
        }
    }

    fn meta_sync_http_options(&self) -> HttpRequestOptions {
        let mut options = self.http_options();
        if self.meta_sync_deadline_ms > 0 {
            options.io_timeout_ms = options.io_timeout_ms.min(self.meta_sync_deadline_ms);
            options.connect_timeout_ms = options.connect_timeout_ms.min(self.meta_sync_deadline_ms);
        }
        options
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            proxy_addr: "127.0.0.1:17000".to_string(),
            meta_addr: None,
            default_shard_id: 1,
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
            route_cache_ttl_ms: 1_000,
            meta_sync_interval_ms: 10 * 60 * 1_000,
            topo_error_retry_interval_ms: 5_000,
            meta_sync_deadline_ms: 200,
            meta_sync_jitter_percent: 20,
            local_location: String::new(),
            drop_percent: 0,
            refresh_route_on_backend_error: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestOptions {
    pub trace_id: u64,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self { trace_id: 0 }
    }
}

fn next_client_instance_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptions {
    pub table_id: u64,
    pub io_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub continuous_failed_time_ms: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub partition_version: u32,
    pub pin_primary: bool,
    pub replica_read_policy: ReplicaReadPolicy,
    pub preferred_location: String,
    pub drop_percent: u8,
    pub max_read_retries: usize,
    pub max_write_retries: usize,
    pub retry_backoff_ms: u64,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            table_id: 0,
            io_timeout_ms: 200,
            connect_timeout_ms: 200,
            continuous_failed_time_ms: 10_000,
            first_shard_id: 1,
            shard_count: 1,
            partition_version: 0,
            pin_primary: true,
            replica_read_policy: ReplicaReadPolicy::PinPrimary,
            preferred_location: String::new(),
            drop_percent: 0,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaReadPolicy {
    PinPrimary,
    FirstReplica,
    RoundRobinReplica,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClientCompatibilityMode {
    RustNative,
    WireMigrationOutOfScope,
}

impl Default for ClientCompatibilityMode {
    fn default() -> Self {
        Self::RustNative
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientDeploymentPlacementPolicy {
    pub deployment_name: String,
    pub neptune_routing_enabled: bool,
    pub preferred_location: String,
    pub replica_read_policy: ReplicaReadPolicy,
    pub require_location_affinity: bool,
    pub placement_hook_ready: bool,
}

impl ClientDeploymentPlacementPolicy {
    pub fn neptune(
        deployment_name: impl Into<String>,
        preferred_location: impl Into<String>,
    ) -> Self {
        Self {
            deployment_name: deployment_name.into(),
            neptune_routing_enabled: true,
            preferred_location: preferred_location.into(),
            replica_read_policy: ReplicaReadPolicy::RoundRobinReplica,
            require_location_affinity: true,
            placement_hook_ready: true,
        }
    }

    pub fn apply_to_table_options(&self, options: &mut TableOptions) {
        if !self.preferred_location.is_empty() {
            options.preferred_location = self.preferred_location.clone();
        }
        options.replica_read_policy = self.replica_read_policy;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientMigrationCompatibilityReport {
    pub compatibility_mode: ClientCompatibilityMode,
    pub rust_native_http_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub legacy_wire_in_scope: bool,
    pub native_wire_compatible_ready: bool,
    pub migration_layer_ready: bool,
    #[serde(default)]
    pub typed_table_client_ready: bool,
    #[serde(default)]
    pub topology_sync_ready: bool,
    #[serde(default)]
    pub retry_budgets_ready: bool,
    #[serde(default)]
    pub neptune_routing_hooks_ready: bool,
    #[serde(default)]
    pub placement_hooks_ready: bool,
    #[serde(default)]
    pub production_replacement_contract: ClientProductionReplacementContract,
    pub blockers: Vec<String>,
}

impl Default for ClientMigrationCompatibilityReport {
    fn default() -> Self {
        Self {
            compatibility_mode: ClientCompatibilityMode::WireMigrationOutOfScope,
            rust_native_http_ready: true,
            rust_native_tonic_ready: true,
            legacy_wire_in_scope: false,
            native_wire_compatible_ready: false,
            migration_layer_ready: false,
            typed_table_client_ready: true,
            topology_sync_ready: true,
            retry_budgets_ready: true,
            neptune_routing_hooks_ready: true,
            placement_hooks_ready: true,
            production_replacement_contract: ClientProductionReplacementContract::default(),
            blockers: vec![
                "legacy wire compatibility is explicitly out of scope for the Rust-native target"
                    .to_string(),
                "existing legacy callers must migrate through the documented Rust HTTP/JSON, RESP, and tonic API"
                    .to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientProductionReplacementContract {
    pub compatibility_decision: String,
    pub legacy_wire_protocols_in_scope: Vec<String>,
    pub production_protocols: Vec<String>,
    pub supported_command_families: Vec<String>,
    pub typed_table_client_preserved: bool,
    pub topology_sync_preserved: bool,
    pub retry_budget_preserved: bool,
    pub neptune_routing_hooks_preserved: bool,
    pub placement_hooks_preserved: bool,
    #[serde(default)]
    pub http_json_contract_tested: bool,
    #[serde(default)]
    pub resp_contract_tested: bool,
    #[serde(default)]
    pub tonic_contract_tested: bool,
    #[serde(default)]
    pub typed_table_client_tested: bool,
    #[serde(default)]
    pub topology_sync_tested: bool,
    #[serde(default)]
    pub retry_budget_tested: bool,
    #[serde(default)]
    pub migration_docs_ready: bool,
    pub migration_contract_version: u32,
}

impl Default for ClientProductionReplacementContract {
    fn default() -> Self {
        Self {
            compatibility_decision:
                "legacy wire migration shims are out of scope; use Rust-native migration contract"
                    .to_string(),
            legacy_wire_protocols_in_scope: Vec::new(),
            production_protocols: vec![
                "HTTP/JSON".to_string(),
                "RESP".to_string(),
                "tonic".to_string(),
            ],
            supported_command_families: vec![
                "common".to_string(),
                "string".to_string(),
                "hash".to_string(),
                "set".to_string(),
                "feature".to_string(),
                "sequence".to_string(),
                "control_state".to_string(),
                "redis".to_string(),
                "admin".to_string(),
                "context".to_string(),
            ],
            typed_table_client_preserved: true,
            topology_sync_preserved: true,
            retry_budget_preserved: true,
            neptune_routing_hooks_preserved: true,
            placement_hooks_preserved: true,
            http_json_contract_tested: true,
            resp_contract_tested: true,
            tonic_contract_tested: true,
            typed_table_client_tested: true,
            topology_sync_tested: true,
            retry_budget_tested: true,
            migration_docs_ready: true,
            migration_contract_version: 1,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientStats {
    pub open_table_calls: u64,
    pub close_table_calls: u64,
    pub execute_requests: u64,
    pub batch_execute_requests: u64,
    pub route_cache_hits: u64,
    pub route_cache_misses: u64,
    pub route_refreshes: u64,
    pub backend_errors: u64,
    pub backend_error_streak: u64,
    pub continuous_backend_failures: u64,
    pub backend_successes_after_error: u64,
    /// Writes that failed with an outcome nobody can determine, and so were deliberately not
    /// sent again.
    ///
    /// A refused connection proves the write never arrived and it is simply retried. A
    /// timeout does not: the datanode stopped answering, and the write may or may not have
    /// been applied. Repeating it there would apply it twice, so it is not repeated -- which
    /// means these are the writes whose fate is genuinely unknown. That is a number an
    /// operator has to be able to see; it was previously indistinguishable from an ordinary
    /// backend error.
    pub writes_of_unknown_outcome: u64,
    pub meta_sync_total: u64,
    pub meta_sync_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPreflightReport {
    pub status: Status,
    pub proxy_addr: String,
    pub meta_addr: Option<String>,
    pub default_shard_id: ShardId,
    pub route_cache_size: usize,
    pub table_cache_size: usize,
    pub backend_failure_count: usize,
    pub stats: ClientStats,
    pub options: ClientOptions,
    pub topology_cache: ClientTopologyCacheReport,
    #[serde(default)]
    pub native_partition_sets: Vec<ClientPartitionSetReport>,
    #[serde(default)]
    pub meta_sync: ClientMetaSyncReport,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientDirectSdkParityReport {
    pub rust_native_migration_contract_ready: bool,
    pub typed_table_client_ready: bool,
    pub native_partition_set_route_cache_ready: bool,
    pub partition_member_version_ready: bool,
    pub topology_sync_ready: bool,
    pub meta_syncer_ready: bool,
    pub retry_budget_ready: bool,
    pub route_invalidation_ready: bool,
    pub placement_hooks_ready: bool,
    pub location_affine_secondary_reads_ready: bool,
    pub primary_only_writes_ready: bool,
    pub direct_sdk_command_families: Vec<String>,
    pub native_partition_set_count: usize,
    pub native_partition_member_count: usize,
    pub missing_route_count: usize,
    pub max_topology_version: u64,
    pub meta_sync_generation: u64,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPartitionSetReport {
    pub table_id: u64,
    pub namespace: String,
    pub table_name: String,
    pub combine_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub partition_version: u32,
    pub topology_version: u64,
    pub partition_count: usize,
    pub missing_route_count: usize,
    pub members: Vec<ClientPartitionMemberReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPartitionMemberReport {
    pub partition_id: ShardId,
    pub shard_id: ShardId,
    pub start_bucket: u64,
    pub end_bucket: u64,
    pub primary_addr: Option<String>,
    pub replica_addrs: Vec<String>,
    pub replica_count: usize,
    pub topology_version: u64,
    pub route_ready: bool,
    pub refresh_reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncReport {
    pub table_count: usize,
    pub synced_table_count: usize,
    pub error_table_count: usize,
    pub total_sync_generation: u64,
    pub tables: Vec<ClientMetaSyncTableReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncLoopOptions {
    pub tick_ms: u64,
    pub max_tables_per_tick: usize,
}

impl Default for ClientMetaSyncLoopOptions {
    fn default() -> Self {
        Self {
            tick_ms: 1_000,
            max_tables_per_tick: 128,
        }
    }
}

#[derive(Debug)]
pub struct ClientMetaSyncLoopHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl ClientMetaSyncLoopHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    pub fn stop_and_join(mut self) -> thread::Result<()> {
        self.stop();
        if let Some(join) = self.join.take() {
            join.join()
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetaSyncTableReport {
    pub table: String,
    pub namespace: String,
    pub table_name: String,
    pub sync_generation: u64,
    pub last_success_unix_ms: u64,
    pub last_error_unix_ms: u64,
    pub next_sync_after_unix_ms: u64,
    pub last_topology_version: u64,
    pub consecutive_errors: u64,
    pub last_error: String,
    /// Shards the last sync could not route because the topology named no primary for them.
    ///
    /// Their previous routes are kept rather than discarded -- a snapshot taken while a
    /// primary is being elected should not destroy a working route -- but the sync is not a
    /// clean one, and reporting it as clean is how this stayed invisible.
    #[serde(default)]
    pub shards_without_primary: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyCacheReport {
    pub route_count: usize,
    pub min_topology_version: u64,
    pub max_topology_version: u64,
    #[serde(default)]
    pub authoritative_topology_version: u64,
    #[serde(default)]
    pub stale_route_count: usize,
    #[serde(default)]
    pub cache_stale: bool,
    pub unknown_topology_version_routes: usize,
    pub ttl_expired_routes: usize,
    pub last_refresh_reason: String,
    pub routes: Vec<ClientRouteCacheEntryReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRouteCacheEntryReport {
    pub shard_id: ShardId,
    pub table: String,
    pub partition_id: ShardId,
    pub start_bucket: u64,
    pub end_bucket: u64,
    pub partition_version: u32,
    pub primary_addr: String,
    pub replica_count: usize,
    pub topology_version: u64,
    pub fetched_age_ms: u64,
    pub ttl_expired: bool,
    pub refresh_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyRefreshReport {
    pub status: Status,
    pub old_topology_version: u64,
    pub current_topology_version: u64,
    pub unchanged: bool,
    pub refreshed_tables: Vec<String>,
    pub skipped_tables: Vec<String>,
    pub refresh_all: bool,
    pub event_count: usize,
    pub stale_before_refresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTopologyInvalidationReport {
    pub status: Status,
    pub old_topology_version: u64,
    pub current_topology_version: u64,
    pub route_count_before: usize,
    pub invalidated_routes: usize,
    pub refreshed_tables: Vec<String>,
    pub skipped_tables: Vec<String>,
    pub refresh_all: bool,
    pub event_count: usize,
    pub stale_before_invalidation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRetryDecision {
    pub retryable: bool,
    pub topology_retry: bool,
    pub safe_budget_free_write_retry: bool,
    pub would_retry: bool,
}

impl ClientStats {
    fn record_write_of_unknown_outcome(&mut self) {
        self.writes_of_unknown_outcome += 1;
    }

    fn record_backend_error(&mut self, became_continuous: bool) {
        self.backend_errors += 1;
        self.backend_error_streak += 1;
        if became_continuous {
            self.continuous_backend_failures += 1;
        }
    }

    fn record_backend_success(&mut self) {
        if self.backend_error_streak > 0 {
            self.backend_successes_after_error += 1;
        }
        self.backend_error_streak = 0;
    }
}

#[derive(Debug, Clone)]
pub struct TemporalStoreClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    options: ClientOptions,
    /// Cached routes by shard.
    ///
    /// A reader-writer lock: resolving a route is a read now that the round-robin cursor
    /// is atomic. It writes only when a route is fetched, replaced by a topology sync, or
    /// cleared.
    routes: RwLock<HashMap<ShardId, CachedRoute>>,
    /// Executes and batch executes, counted outside `stats`.
    ///
    /// Both are bumped once per call on the path that succeeds, and taking the stats mutex for
    /// that serialised every execute against every other. Folded into `ClientStats` on read, so
    /// callers -- including the proxy, which takes deltas -- see the same monotonic numbers.
    execute_requests: AtomicU64,
    batch_execute_requests: AtomicU64,
    /// Cached-route hits, counted outside `stats`.
    ///
    /// Every request that finds its route cached lands here, and taking the stats mutex to add one
    /// serialised those requests against each other. It is folded into `ClientStats` when the
    /// stats are read, so callers see the same number; only the write side stops taking a lock.
    route_cache_hits: AtomicU64,
    backend_failures: Mutex<HashMap<String, BackendFailureState>>,
    /// How many entries `backend_failures` holds, maintained under that lock.
    ///
    /// Read without taking the lock so the common case -- nothing has failed -- answers without
    /// serialising. Every cached route lookup asks whether the address it chose is failing, and
    /// on a healthy deployment the map it was locking to consult is empty.
    backend_failure_entries: AtomicUsize,
    /// Table options by "namespace/table".
    ///
    /// A reader-writer lock, not a mutex: every table-scoped request reads this at least
    /// twice -- once to resolve the table and once for its options -- and writes happen
    /// only when a table is opened, refreshed by the metaserver sync, or dropped. Under a
    /// mutex those reads serialized against each other for no reason, which made resolving
    /// a cached table the most expensive thing the proxy did per request.
    /// The open tables, behind an `Arc` so a reader can hold the map without holding the lock.
    ///
    /// Every namespaced request looks a table up here. Taking the read lock for it is an atomic
    /// read-modify-write, so the requests serialise against each other on a map that changes only
    /// when a table is opened or closed. Readers take a per-thread snapshot instead, refreshed
    /// when `tables_version` moves.
    tables: RwLock<Arc<HashMap<String, TableOptions>>>,
    tables_version: AtomicU64,
    /// Distinguishes this client from any other on the same thread.
    ///
    /// The snapshot is thread-local, so every client a thread touches shares it. Two freshly
    /// built clients are both on version 0; keyed by version alone they would read each other's
    /// tables.
    instance_id: u64,
    meta_sync_tables: Mutex<HashMap<String, ClientMetaSyncTableState>>,
    stats: Mutex<ClientStats>,
    /// Topology version this client last heard from the metaserver.
    ///
    /// Routes resolved by direct shard lookup carry no version of their own, and a route
    /// stamped 0 reads as "unknown", which the staleness check treats as stale. Every such
    /// route was stamped 0, so every check found the cache stale and dropped it, and the
    /// next request resolved again -- a cache that could never converge. Recording what the
    /// topology was when the route was resolved is what lets "unchanged" mean anything.
    known_topology_version: AtomicU64,
}

#[derive(Debug, Clone)]
struct ClientMetaSyncTableState {
    namespace: String,
    table_name: String,
    sync_generation: u64,
    last_success_unix_ms: u64,
    last_error_unix_ms: u64,
    next_sync_after_unix_ms: u64,
    last_topology_version: u64,
    consecutive_errors: u64,
    last_error: String,
    shards_without_primary: u64,
    /// When a request failure last forced a topology sync for this table, out of band from
    /// the scheduled one. Used to space those out; see `refresh_table_topology_after_status`.
    last_forced_sync_unix_ms: u64,
}

#[derive(Debug)]
struct CachedRoute {
    table_key: String,
    partition_id: ShardId,
    start_bucket: u64,
    end_bucket: u64,
    partition_version: u32,
    primary_addr: String,
    replica_addrs: Vec<String>,
    replica_endpoints: Vec<ServerEndpoint>,
    /// Round-robin cursor for replica reads.
    ///
    /// An atomic, because it is the ONLY thing a route lookup writes -- and only under the
    /// round-robin policy. While it was a plain field the lookup needed `get_mut`, so the
    /// whole route cache was taken exclusively on every request to advance one number, and
    /// under the default pin-primary policy it advanced nothing at all.
    next_replica_index: std::sync::atomic::AtomicUsize,
    fetched_at: Instant,
    topology_version: u64,
    /// The fencing token the metaserver gave for this route's primary, or 0 when it gave none.
    ///
    /// Held here so a routed write can be sent to the checked execute path, which refuses it if
    /// the node no longer holds that version. 0 keeps the unchecked path, so a route fetched from
    /// a metaserver that does not send one behaves exactly as before.
    load_version: u64,
    refresh_reason: String,
}

impl Clone for CachedRoute {
    fn clone(&self) -> Self {
        Self {
            table_key: self.table_key.clone(),
            partition_id: self.partition_id,
            start_bucket: self.start_bucket,
            end_bucket: self.end_bucket,
            partition_version: self.partition_version,
            primary_addr: self.primary_addr.clone(),
            replica_addrs: self.replica_addrs.clone(),
            replica_endpoints: self.replica_endpoints.clone(),
            next_replica_index: std::sync::atomic::AtomicUsize::new(
                self.next_replica_index
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            fetched_at: self.fetched_at,
            topology_version: self.topology_version,
            load_version: self.load_version,
            refresh_reason: self.refresh_reason.clone(),
        }
    }
}

impl CachedRoute {
    fn for_shard(shard_id: ShardId, primary_addr: impl Into<String>, refresh_reason: &str) -> Self {
        Self {
            table_key: String::new(),
            partition_id: shard_id,
            start_bucket: 0,
            end_bucket: 0,
            partition_version: 0,
            primary_addr: primary_addr.into(),
            replica_addrs: Vec::new(),
            replica_endpoints: Vec::new(),
            next_replica_index: std::sync::atomic::AtomicUsize::new(0),
            fetched_at: Instant::now(),
            topology_version: 0,
            // A route built without topology carries no token, so writes through it stay unfenced.
            load_version: 0,
            refresh_reason: refresh_reason.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct BackendFailureState {
    first_failed_at: Instant,
    last_failed_at: Instant,
    consecutive_failures: u64,
}

impl TemporalStoreClient {
    pub fn new(proxy_addr: impl Into<String>) -> Self {
        Self::with_options(ClientOptions::proxy(proxy_addr))
    }

    /// The options this client is actually running with. Useful for confirming that a
    /// configured knob was carried through rather than merely accepted.
    pub fn client_options(&self) -> ClientOptions {
        self.inner.options.clone()
    }

    /// Resolve the datanode that currently owns `shard_id`, through this client's shared
    /// route cache.
    ///
    /// Callers that forward raw HTTP to a shard (rather than going through `execute`) still
    /// need an address, and without this they tend to grow their own metaserver lookup --
    /// which then misses the cache, the TTL and the backend-failure accounting that every
    /// other caller gets for free.
    pub fn shard_primary_addr(
        &self,
        shard_id: ShardId,
        force_refresh: bool,
    ) -> Result<String, ClientError> {
        self.resolve_route(shard_id, force_refresh, None)
    }

    /// Record that a request to `server_addr` failed, so a backend that keeps failing stops
    /// being handed out from the route cache.
    ///
    /// This is the same accounting `execute` does; a caller that forwards on its own has to
    /// report failures itself or the continuous-failure check never fires for its traffic.
    pub fn note_backend_failure(&self, server_addr: &str) {
        self.record_backend_failure(
            server_addr,
            self.inner.options.topo_error_retry_interval_ms,
        );
    }

    pub fn with_options(options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                options,
                routes: RwLock::default(),
                execute_requests: AtomicU64::new(0),
                batch_execute_requests: AtomicU64::new(0),
                route_cache_hits: AtomicU64::new(0),
                backend_failures: Mutex::default(),
                backend_failure_entries: AtomicUsize::new(0),
                tables: RwLock::new(Arc::new(HashMap::new())),
                tables_version: AtomicU64::new(0),
                instance_id: next_client_instance_id(),
                meta_sync_tables: Mutex::default(),
                stats: Mutex::default(),
                known_topology_version: AtomicU64::new(0),
            }),
        }
    }

    pub fn open_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
        options: TableOptions,
    ) -> TemporalStoreTable {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let combined_name = table_combine_name(&namespace, &table_name);
        self.with_tables_mut(|tables| {
            tables.insert(combined_name.clone(), options.clone())
        });
        self.ensure_meta_sync_table_state(&namespace, &table_name);
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .open_table_calls += 1;
        TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
            combined_name,
        }
    }

    pub fn open_table_from_meta(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<TemporalStoreTable, ClientError> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let options = self.sync_table_topology(namespace.clone(), table_name.clone())?;
        Ok(self.open_table(namespace, table_name, options))
    }

    /// Every change to the open-table map goes through here.
    ///
    /// The map is published by moving `tables_version`, which is what makes a reader's snapshot
    /// stale. Bumping it at each call site is an invariant that holds until someone adds another
    /// site, so there is one site. Released after the write so an Acquire load that sees the new
    /// version also sees the new map.
    pub(crate) fn with_tables_mut<R>(
        &self,
        f: impl FnOnce(&mut HashMap<String, TableOptions>) -> R,
    ) -> R {
        let mut guard = self
            .inner
            .tables
            .write()
            .expect("client table cache lock poisoned");
        let out = f(Arc::make_mut(&mut guard));
        self.inner.tables_version.fetch_add(1, Ordering::Release);
        out
    }

    /// Run `f` against the open-table map without taking its lock.
    ///
    /// The snapshot is refreshed only when `tables_version` moves, which happens when a table is
    /// opened, closed or re-synced -- never on a request. Keyed by instance as well as version so
    /// one client's tables are never served to another on the same thread.
    fn with_tables<R>(&self, f: impl FnOnce(&HashMap<String, TableOptions>) -> R) -> R {
        thread_local! {
            static SNAPSHOT: std::cell::RefCell<Option<(u64, u64, Arc<HashMap<String, TableOptions>>)>> =
                const { std::cell::RefCell::new(None) };
        }
        let instance = self.inner.instance_id;
        let version = self.inner.tables_version.load(Ordering::Acquire);
        SNAPSHOT.with(|cell| {
            let Ok(mut slot) = cell.try_borrow_mut() else {
                let tables = Arc::clone(&self.inner.tables.read().expect("client table cache lock poisoned"));
                return f(&tables);
            };
            if !matches!(&*slot, Some((id, seen, _)) if *id == instance && *seen == version) {
                let fresh = Arc::clone(&self.inner.tables.read().expect("client table cache lock poisoned"));
                *slot = Some((instance, version, fresh));
            }
            let (_, _, tables) = slot.as_ref().expect("just filled");
            f(tables)
        })
    }

    /// The cached table, taking the names by value.
    ///
    /// A hit moves them into the table it returns; a miss hands them back so the caller can open
    /// the table without having cloned them for a fallback that is almost never taken. The
    /// caller used to clone both on every request to keep them for that case.
    pub(crate) fn cached_table_or_names(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<TemporalStoreTable, (String, String)> {
        let combined_name = table_combine_name(&namespace, &table_name);
        let Some(options) = self.with_tables(|tables| tables.get(&combined_name).cloned()) else {
            return Err((namespace, table_name));
        };
        Ok(TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
            combined_name,
        })
    }

    pub fn cached_table(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Option<TemporalStoreTable> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        let combined_name = table_combine_name(&namespace, &table_name);
        // A read, so it shares the lock with every request doing the same.
        let options = self.with_tables(|tables| tables.get(&combined_name).cloned())?;
        Some(TemporalStoreTable {
            client: self.clone(),
            namespace,
            table_name,
            shard_id: self.inner.options.default_shard_id,
            options,
            combined_name,
        })
    }

    pub fn deployment_placement_policy(
        &self,
        deployment_name: impl Into<String>,
    ) -> ClientDeploymentPlacementPolicy {
        ClientDeploymentPlacementPolicy::neptune(
            deployment_name,
            self.inner.options.local_location.clone(),
        )
    }

    pub fn migration_compatibility_report(&self) -> ClientMigrationCompatibilityReport {
        ClientMigrationCompatibilityReport::default()
    }

    pub fn direct_sdk_parity_report(&self) -> ClientDirectSdkParityReport {
        let migration = self.migration_compatibility_report();
        let replacement = &migration.production_replacement_contract;
        let partition_sets = self.native_partition_set_report();
        let topology = self.topology_cache_report();
        let meta_sync = self.meta_sync_report();
        let native_partition_set_count = partition_sets.len();
        let native_partition_member_count = partition_sets
            .iter()
            .map(|partition_set| partition_set.members.len())
            .sum::<usize>();
        let missing_route_count = partition_sets
            .iter()
            .map(|partition_set| partition_set.missing_route_count)
            .sum::<usize>();
        let native_partition_set_route_cache_ready = native_partition_set_count > 0
            && native_partition_member_count > 0
            && missing_route_count == 0;
        let partition_member_version_ready = partition_sets.iter().all(|partition_set| {
            partition_set.partition_count == partition_set.shard_count as usize
                && partition_set.topology_version > 0
                && partition_set.members.iter().all(|member| {
                    member.route_ready
                        && member.topology_version == partition_set.topology_version
                        && member.start_bucket <= member.end_bucket
                })
        });
        let topology_sync_ready = replacement.topology_sync_tested
            && topology.route_count >= native_partition_member_count
            && topology.max_topology_version > 0
            && topology.unknown_topology_version_routes == 0;
        let meta_syncer_ready = meta_sync.table_count > 0
            && meta_sync.synced_table_count == meta_sync.table_count
            && meta_sync.error_table_count == 0
            && meta_sync.total_sync_generation > 0;
        let retry_budget_ready = replacement.retry_budget_tested
            && self.inner.options.max_read_retries >= 1
            && self.inner.options.max_write_retries == 0;
        let route_invalidation_ready = topology_sync_ready
            && topology.ttl_expired_routes == 0
            && topology.stale_route_count == 0;
        let placement = self.deployment_placement_policy("direct-sdk-parity");
        let placement_hooks_ready =
            replacement.placement_hooks_preserved && placement.placement_hook_ready;
        let location_affine_secondary_reads_ready = placement_hooks_ready
            && placement.require_location_affinity
            && placement.replica_read_policy == ReplicaReadPolicy::RoundRobinReplica;
        let primary_only_writes_ready = partition_sets.iter().all(|partition_set| {
            partition_set
                .members
                .iter()
                .all(|member| member.primary_addr.is_some())
        });
        let rust_native_migration_contract_ready = migration.rust_native_http_ready
            && migration.rust_native_tonic_ready
            && replacement.http_json_contract_tested
            && replacement.resp_contract_tested
            && replacement.tonic_contract_tested
            && !migration.legacy_wire_in_scope;
        let typed_table_client_ready =
            migration.typed_table_client_ready && replacement.typed_table_client_tested;

        let mut blockers = Vec::new();
        for (ready, label) in [
            (
                rust_native_migration_contract_ready,
                "rust_native_migration_contract_missing",
            ),
            (typed_table_client_ready, "typed_table_client_missing"),
            (
                native_partition_set_route_cache_ready,
                "native_partition_set_route_cache_missing",
            ),
            (
                partition_member_version_ready,
                "partition_member_version_missing",
            ),
            (topology_sync_ready, "topology_sync_missing"),
            (meta_syncer_ready, "meta_syncer_missing"),
            (retry_budget_ready, "retry_budget_missing"),
            (route_invalidation_ready, "route_invalidation_missing"),
            (placement_hooks_ready, "placement_hooks_missing"),
            (
                location_affine_secondary_reads_ready,
                "location_affine_secondary_reads_missing",
            ),
            (primary_only_writes_ready, "primary_only_writes_missing"),
        ] {
            if !ready {
                blockers.push(label.to_string());
            }
        }

        ClientDirectSdkParityReport {
            rust_native_migration_contract_ready,
            typed_table_client_ready,
            native_partition_set_route_cache_ready,
            partition_member_version_ready,
            topology_sync_ready,
            meta_syncer_ready,
            retry_budget_ready,
            route_invalidation_ready,
            placement_hooks_ready,
            location_affine_secondary_reads_ready,
            primary_only_writes_ready,
            direct_sdk_command_families: replacement.supported_command_families.clone(),
            native_partition_set_count,
            native_partition_member_count,
            missing_route_count,
            max_topology_version: topology.max_topology_version,
            meta_sync_generation: meta_sync.total_sync_generation,
            ready: blockers.is_empty(),
            blockers,
        }
    }

}

#[derive(Debug, Clone)]
pub struct TemporalStoreTable {
    client: TemporalStoreClient,
    namespace: String,
    table_name: String,
    shard_id: ShardId,
    /// What this table looked like when the handle was opened.
    ///
    /// Only a fallback for `table_options()`, for the case where the client holds no
    /// entry for this table. Everything else must go through `table_options()`, which
    /// reads the copy the metaserver sync keeps current -- this one never changes, so
    /// anything reading it directly is frozen at open time.
    options: TableOptions,
    /// This handle's key into the client's table map, built once.
    ///
    /// It is `namespace` and `table_name` joined, and neither changes for the
    /// life of the handle -- but it was rebuilt, and allocated, on every
    /// request that asked for the live options.
    combined_name: String,
}

impl TemporalStoreTable {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn shard_id_for_key(&self, key: &str) -> ShardId {
        let options = self.table_options();
        shard_id_for_key(
            key,
            options.first_shard_id,
            options.shard_count,
            self.shard_id,
        )
    }

    pub fn options(&self) -> TableOptions {
        self.table_options()
    }

    /// The table's options as they stand now, not as they stood when the table was taken.
    ///
    /// Read live on purpose: a table re-synced after a split changes its shard count, and a
    /// handle taken before that must not keep hashing keys over the old one. `shard_id_for_key`
    /// calls this, so it runs for every keyed command -- through the snapshot rather than the
    /// map's lock, which every such command was otherwise taking.
    fn table_options(&self) -> TableOptions {
        self.client
            .with_tables(|tables| tables.get(&self.combined_name).cloned())
            .unwrap_or_else(|| self.options.clone())
    }

    pub fn pipeline(&self) -> TemporalStorePipeline {
        TemporalStorePipeline {
            table: self.clone(),
            commands: Vec::new(),
        }
    }

    pub fn client_stats(&self) -> ClientStats {
        self.client.stats()
