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
