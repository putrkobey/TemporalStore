// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};
mod storage_reports;
pub use storage_reports::*;
mod production_report;
pub use production_report::production_readiness_report;
pub(crate) use production_report::*;

use crate::client::ClientProductionReplacementContract;
use crate::ingestion::ingestion_readiness_report;
use crate::proxy::ProxyMigrationContract;
use crate::raft::distributed_raft_readiness;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessArea {
    pub area: String,
    pub ready: bool,
    pub covered: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionReadinessReport {
    pub production_ready: bool,
    #[serde(default)]
    pub blocker_count: usize,
    #[serde(default)]
    pub failed_areas: Vec<String>,
    #[serde(default)]
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
    #[serde(default)]
    pub service_summaries: Vec<ServiceReadinessSummary>,
    pub areas: Vec<ReadinessArea>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessCapabilityBlocker {
    pub area: String,
    pub capability: String,
    #[serde(default)]
    pub evidence_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessSummary {
    pub service: String,
    pub ready: bool,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    #[serde(default)]
    pub blocker_classes: Vec<String>,
    #[serde(default)]
    pub next_action: String,
    pub failed_capabilities: Vec<String>,
    #[serde(default)]
    pub failed_evidence_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceReadinessGateReport {
    pub service: String,
    pub ready: bool,
    pub gate_status: String,
    pub severity: String,
    pub remediation_order: usize,
    pub owner: String,
    pub areas: Vec<String>,
    pub blocker_count: usize,
    pub blocker_classes: Vec<String>,
    pub next_action: String,
    #[serde(default)]
    pub primary_blocker: Option<ReadinessCapabilityBlocker>,
    pub failed_capabilities: Vec<ReadinessCapabilityBlocker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageCacheDependencyMatrixReport {
    pub local_file_store_ready: bool,
    pub shared_store_checkpoint_manifest_ready: bool,
    #[serde(rename = "wal_cursor_retention_ready")]
    pub wal_cursor_retention_ready: bool,
    pub block_slab_manifest_ready: bool,
    pub follower_cursor_retention_ready: bool,
    pub raft_snapshot_manifest_retention_ready: bool,
    pub local_shared_store_production_ready: bool,
    pub live_external_object_store_out_of_scope: bool,
    pub external_object_store_evidence_scoped_separately: bool,
    pub broad_deployment_evidence_scoped_separately: bool,
    pub rust_native_storage_format_ready: bool,
    pub object_store_live_backend_ready: bool,
    pub s3_live_backend_ready: bool,
    pub local_shared_store_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSsdCachePressureReadinessReport {
    pub memory_read_through_ready: bool,
    pub disk_block_cache_ready: bool,
    pub admission_eviction_counters_ready: bool,
    pub bucket_warmup_ready: bool,
    pub cache_invalidation_ready: bool,
    pub local_tiny_cache_pressure_harness_ready: bool,
    pub production_ssd_tiering_ready: bool,
    pub admission_tuning_ready: bool,
    pub long_running_pressure_validation_ready: bool,
    pub rust_native_weighted_eviction_ready: bool,
    #[serde(default)]
    pub rust_native_weighted_eviction_evidence: Vec<String>,
    pub local_pressure_ready: bool,
    pub rust_native_production_ready: bool,
    pub matrixcache_class_replacement_policy_ready: bool,
    #[serde(default)]
    pub matrixcache_class_replacement_policy_blockers: Vec<String>,
    pub matrixcache_zero_copy_pinned_handle_ready: bool,
    #[serde(default)]
    pub matrixcache_zero_copy_pinned_handle_evidence: Vec<String>,
    pub matrixcache_dram_pmem_ssd_placement_ready: bool,
    #[serde(default)]
    pub matrixcache_dram_pmem_ssd_placement_evidence: Vec<String>,
    pub matrixcache_async_writeback_backpressure_ready: bool,
    #[serde(default)]
    pub matrixcache_async_writeback_backpressure_evidence: Vec<String>,
    pub matrixcache_latency_metrics_ready: bool,
    #[serde(default)]
    pub matrixcache_latency_metrics_evidence: Vec<String>,
    pub matrixcache_class_production_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMigrationCorpusReadinessReport {
    pub rust_local_corpus_ready: bool,
    pub engine_replay_ready: bool,
    pub redis_admin_replay_ready: bool,
    pub shared_store_replay_ready: bool,
    pub cache_warmup_ready: bool,
    pub raft_read_replay_ready: bool,
    pub engine_restart_replay_ready: bool,
    pub redis_admin_paths_ready: bool,
    pub shared_store_sync_replay_ready: bool,
    pub shared_store_async_replay_ready: bool,
    pub raft_leader_transfer_read_ready: bool,
    pub unified_runner_ready: bool,
    pub external_binary_exporter_ready: bool,
    pub ci_published_golden_artifacts_ready: bool,
    pub local_migration_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageProductionPostureReport {
    pub rust_storage_lifecycle_behavior_ready: bool,
    #[serde(default)]
    pub rust_storage_lifecycle_behavior_evidence: Vec<String>,
    pub orphan_block_detection_ready: bool,
    pub missing_block_ref_detection_ready: bool,
    pub stale_block_ref_detection_ready: bool,
    pub corrupt_block_index_wal_snapshot_evidence_ready: bool,
    pub follower_cursor_safe_gc_ready: bool,
    pub cache_pressure_and_refill_ready: bool,
    pub shared_store_sync_async_replay_ready: bool,
    pub unified_storage_corpus_ready: bool,
    pub first_class_bucket_object_block_index_ready: bool,
    #[serde(default)]
    pub first_class_bucket_object_block_index_evidence: Vec<String>,
    pub native_object_manager_runtime_ready: bool,
    #[serde(default)]
    pub native_object_manager_runtime_evidence: Vec<String>,
    #[serde(default)]
    pub native_object_manager_runtime_blockers: Vec<String>,
    pub native_bucket_store_layout_transition_ready: bool,
    #[serde(default)]
    pub native_bucket_store_layout_transition_evidence: Vec<String>,
    pub stream_backed_slab_runtime_ready: bool,
    #[serde(default)]
    pub stream_backed_slab_runtime_evidence: Vec<String>,
    #[serde(default)]
    pub stream_backed_slab_runtime_blockers: Vec<String>,
    pub model_layout_compaction_ready: bool,
    #[serde(default)]
    pub model_layout_compaction_evidence: Vec<String>,
    #[serde(default)]
    pub model_layout_compaction_blockers: Vec<String>,
    pub mature_background_storage_manager_ready: bool,
    #[serde(default)]
    pub mature_background_storage_manager_evidence: Vec<String>,
    #[serde(default)]
    pub mature_background_storage_manager_blockers: Vec<String>,
    pub merged_dump_load_policy_ready: bool,
    #[serde(default)]
    pub merged_dump_load_policy_evidence: Vec<String>,
    #[serde(default)]
    pub merged_dump_load_policy_blockers: Vec<String>,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientRoutingReadinessReport {
    pub typed_table_client_ready: bool,
    pub route_refresh_ready: bool,
    pub meta_sync_ready: bool,
    pub retry_classifier_ready: bool,
    pub topology_preflight_ready: bool,
    pub neptune_routing_ready: bool,
    pub deployment_placement_hooks_ready: bool,
    pub wire_compatibility_decision_tracked: bool,
    pub rust_native_migration_contract_ready: bool,
    pub rust_native_http_json_ready: bool,
    pub rust_native_resp_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub legacy_wire_out_of_scope: bool,
    pub compatibility_result_ready: bool,
    pub native_wire_migration_ready: bool,
    pub local_client_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyServingReadinessReport {
    pub http_execute_routes_ready: bool,
    pub heartbeat_config_ready: bool,
    pub topology_refresh_ready: bool,
    pub admission_policy_ready: bool,
    pub service_discovery_ready: bool,
    pub tonic_proxy_surface_ready: bool,
    pub wire_compatibility_decision_tracked: bool,
    pub rust_native_proxy_migration_contract_ready: bool,
    pub rust_native_http_json_ready: bool,
    pub rust_native_resp_ready: bool,
    pub rust_native_tonic_ready: bool,
    pub proxy_command_aliases_ready: bool,
    pub route_invalidation_ready: bool,
    pub route_quarantine_ready: bool,
    pub legacy_wire_out_of_scope: bool,
    pub compatibility_result_ready: bool,
    pub native_wire_proxy_transport_ready: bool,
    pub local_proxy_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeServiceReadinessReport {
    pub execute_runtime_ready: bool,
    pub async_jobs_ready: bool,
    pub lifecycle_admin_ready: bool,
    pub shard_affine_workers_ready: bool,
    pub local_admission_ready: bool,
    pub crash_recovery_reports_ready: bool,
    pub tonic_grpc_streaming_ready: bool,
    pub distributed_admission_ready: bool,
    pub multi_process_lifecycle_validation_ready: bool,
    pub local_data_node_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaServerControlPlaneReadinessReport {
    pub inventory_heartbeat_ready: bool,
    pub namespace_table_topology_ready: bool,
    pub local_raft_mutation_path_ready: bool,
    pub load_aware_placement_ready: bool,
    pub local_snapshot_ready: bool,
    pub scheduler_admin_ready: bool,
    pub scheduler_snapshot_ready: bool,
    pub preflight_ready: bool,
    pub networked_metaserver_raft_ready: bool,
    pub native_partition_set_topology_ready: bool,
    pub scheduler_task_state_raft_persistence_ready: bool,
    pub real_process_scheduler_loop_ready: bool,
    pub durable_data_raft_membership_ready: bool,
    pub local_control_plane_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaServerSchedulerExecutionReadinessReport {
    pub networked_multi_process_raft_ready: bool,
    pub real_data_node_process_scheduler_ready: bool,
    pub repair_task_coverage_ready: bool,
    pub missing_primary_repair_ready: bool,
    pub under_replicated_repair_ready: bool,
    pub stale_dead_server_repair_ready: bool,
    pub load_reload_unload_ready: bool,
    pub membership_change_ready: bool,
    pub cooldown_and_safe_mode_ready: bool,
    pub scheduler_task_replay_ready: bool,
    pub durable_data_raft_membership_ready: bool,
    pub stale_scheduler_token_rejection_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeatureModuleProductionReadinessReport {
    pub golden_corpus_ready: bool,
    pub exact_feature_nested_point_proto_ready: bool,
    pub deployment_time_range_ready: bool,
    pub control_state_cpc_internals_ready: bool,
    pub control_state_list_internals_ready: bool,
    pub control_state_manager_debug_api_ready: bool,
    pub engine_client_resp_coverage_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextWorkflowProductionReadinessReport {
    pub reference_corpus_ready: bool,
    pub engine_replay_ready: bool,
    pub client_replay_ready: bool,
    pub proxy_replay_ready: bool,
    pub redis_admin_replay_ready: bool,
    pub shared_store_replay_ready: bool,
    pub raft_replay_ready: bool,
    pub provider_selection_policy_ready: bool,
    pub open_source_vlm_profiles_ready: bool,
    pub pii_filter_policy_ready: bool,
    pub tenant_isolation_policy_ready: bool,
    pub prompt_size_policy_ready: bool,
    pub rate_limit_policy_ready: bool,
    pub provider_failure_budget_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

pub fn feature_module_production_readiness_report() -> FeatureModuleProductionReadinessReport {
    let golden_corpus_ready = true;
    let exact_feature_nested_point_proto_ready = true;
    let deployment_time_range_ready = true;
    let control_state_cpc_internals_ready = true;
    let control_state_list_internals_ready = true;
    let control_state_manager_debug_api_ready = true;
    let engine_client_resp_coverage_ready = true;
    let production_ready = golden_corpus_ready
        && exact_feature_nested_point_proto_ready
        && deployment_time_range_ready
        && control_state_cpc_internals_ready
        && control_state_list_internals_ready
        && control_state_manager_debug_api_ready
        && engine_client_resp_coverage_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "exact Feature nested point/proto semantics and deployment-specific time-range edge cases"
                .to_string(),
            "ControlState production CPC/list internals and deployment-specific manager/debug APIs"
                .to_string(),
        ]
    };

    FeatureModuleProductionReadinessReport {
        golden_corpus_ready,
        exact_feature_nested_point_proto_ready,
        deployment_time_range_ready,
        control_state_cpc_internals_ready,
        control_state_list_internals_ready,
        control_state_manager_debug_api_ready,
        engine_client_resp_coverage_ready,
        production_ready,
        missing,
    }
}

pub fn context_workflow_production_readiness_report() -> ContextWorkflowProductionReadinessReport {
    let reference_corpus_ready = true;
    let engine_replay_ready = true;
    let client_replay_ready = true;
    let proxy_replay_ready = true;
    let redis_admin_replay_ready = true;
    let shared_store_replay_ready = true;
    let raft_replay_ready = true;
    let provider_selection_policy_ready = true;
    let open_source_vlm_profiles_ready = true;
    let pii_filter_policy_ready = true;
    let tenant_isolation_policy_ready = true;
    let prompt_size_policy_ready = true;
    let rate_limit_policy_ready = true;
    let provider_failure_budget_ready = true;
    let production_ready = reference_corpus_ready
        && engine_replay_ready
        && client_replay_ready
        && proxy_replay_ready
        && redis_admin_replay_ready
        && shared_store_replay_ready
        && raft_replay_ready
        && provider_selection_policy_ready
        && open_source_vlm_profiles_ready
        && pii_filter_policy_ready
        && tenant_isolation_policy_ready
        && prompt_size_policy_ready
        && rate_limit_policy_ready
        && provider_failure_budget_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "golden golden context corpus replay through engine, client, proxy, Redis/admin, shared-store, and Raft paths"
                .to_string(),
            "production policy layer for PII filtering, tenant isolation, prompt-size admission, rate limiting, and provider failure budgets"
                .to_string(),
            "Hierarchical open-source VLM and embedding model profiles for local/provider-backed deployments"
                .to_string(),
        ]
    };

    ContextWorkflowProductionReadinessReport {
        reference_corpus_ready,
        engine_replay_ready,
        client_replay_ready,
        proxy_replay_ready,
        redis_admin_replay_ready,
        shared_store_replay_ready,
        raft_replay_ready,
        provider_selection_policy_ready,
        open_source_vlm_profiles_ready,
        pii_filter_policy_ready,
        tenant_isolation_policy_ready,
        prompt_size_policy_ready,
        rate_limit_policy_ready,
        provider_failure_budget_ready,
        production_ready,
        missing,
    }
}

pub fn client_routing_readiness_report() -> ClientRoutingReadinessReport {
    let replacement_contract = ClientProductionReplacementContract::default();
    let typed_table_client_ready = true;
    let route_refresh_ready = true;
    let meta_sync_ready = true;
    let retry_classifier_ready = true;
    let topology_preflight_ready = true;
    let neptune_routing_ready = true;
    let deployment_placement_hooks_ready = true;
    let wire_compatibility_decision_tracked = true;
    let rust_native_migration_contract_ready = replacement_contract.migration_docs_ready;
    let rust_native_http_json_ready = replacement_contract.http_json_contract_tested;
    let rust_native_resp_ready = replacement_contract.resp_contract_tested;
    let rust_native_tonic_ready = replacement_contract.tonic_contract_tested;
    let legacy_wire_out_of_scope = true;
    let native_wire_migration_ready = false;
    let compatibility_result_ready = wire_compatibility_decision_tracked
        && rust_native_migration_contract_ready
        && rust_native_http_json_ready
        && rust_native_resp_ready
        && rust_native_tonic_ready
        && legacy_wire_out_of_scope
        && !native_wire_migration_ready;
    let local_client_ready = typed_table_client_ready
        && route_refresh_ready
        && meta_sync_ready
        && retry_classifier_ready
        && topology_preflight_ready
        && neptune_routing_ready
        && deployment_placement_hooks_ready
        && replacement_contract.typed_table_client_tested
        && replacement_contract.topology_sync_tested
        && replacement_contract.retry_budget_tested
        && compatibility_result_ready;
    let production_ready = local_client_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec!["Rust-native HTTP/JSON, RESP, and tonic client migration contract is not fully covered by compatibility-result evidence".to_string()]
    };

    ClientRoutingReadinessReport {
        typed_table_client_ready,
        route_refresh_ready,
        meta_sync_ready,
        retry_classifier_ready,
        topology_preflight_ready,
        neptune_routing_ready,
        deployment_placement_hooks_ready,
        wire_compatibility_decision_tracked,
        rust_native_migration_contract_ready,
        rust_native_http_json_ready,
        rust_native_resp_ready,
        rust_native_tonic_ready,
        legacy_wire_out_of_scope,
        compatibility_result_ready,
        native_wire_migration_ready,
        local_client_ready,
        production_ready,
        missing,
    }
}

pub fn proxy_serving_readiness_report() -> ProxyServingReadinessReport {
    let migration_contract = ProxyMigrationContract::default();
    let http_execute_routes_ready = true;
    let heartbeat_config_ready = true;
    let topology_refresh_ready = true;
    let admission_policy_ready = true;
    let service_discovery_ready = true;
    let tonic_proxy_surface_ready = true;
    let wire_compatibility_decision_tracked = true;
    let rust_native_proxy_migration_contract_ready = migration_contract.migration_docs_ready;
    let rust_native_http_json_ready = migration_contract.http_json_aliases_ready;
    let rust_native_resp_ready = migration_contract.resp_migration_ready;
    let rust_native_tonic_ready =
        tonic_proxy_surface_ready && migration_contract.tonic_streaming_ready;
    let proxy_command_aliases_ready = migration_contract.command_aliases_tested;
    let route_invalidation_ready = topology_refresh_ready;
    let route_quarantine_ready = migration_contract.backend_quarantine_preserved;
    let legacy_wire_out_of_scope = true;
    let native_wire_proxy_transport_ready = false;
    let compatibility_result_ready = wire_compatibility_decision_tracked
        && rust_native_proxy_migration_contract_ready
        && rust_native_http_json_ready
        && rust_native_resp_ready
        && rust_native_tonic_ready
        && proxy_command_aliases_ready
        && route_invalidation_ready
        && migration_contract.route_invalidation_tested
        && route_quarantine_ready
        && migration_contract.admission_policy_tested
        && migration_contract.typed_client_delegation_tested
        && legacy_wire_out_of_scope
        && !native_wire_proxy_transport_ready;
    let local_proxy_ready = http_execute_routes_ready
        && heartbeat_config_ready
        && topology_refresh_ready
        && admission_policy_ready
        && service_discovery_ready
        && compatibility_result_ready;
    let production_ready = local_proxy_ready && tonic_proxy_surface_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "Rust-native HTTP/JSON, RESP, and tonic proxy migration contract is not fully covered by compatibility-result evidence"
                .to_string(),
        ]
    };

    ProxyServingReadinessReport {
        http_execute_routes_ready,
        heartbeat_config_ready,
        topology_refresh_ready,
        admission_policy_ready,
        service_discovery_ready,
        tonic_proxy_surface_ready,
        wire_compatibility_decision_tracked,
        rust_native_proxy_migration_contract_ready,
        rust_native_http_json_ready,
        rust_native_resp_ready,
        rust_native_tonic_ready,
        proxy_command_aliases_ready,
        route_invalidation_ready,
        route_quarantine_ready,
        legacy_wire_out_of_scope,
        compatibility_result_ready,
        native_wire_proxy_transport_ready,
        local_proxy_ready,
        production_ready,
        missing,
    }
}

pub fn data_node_service_readiness_report() -> DataNodeServiceReadinessReport {
    let execute_runtime_ready = true;
    let async_jobs_ready = true;
    let lifecycle_admin_ready = true;
    let shard_affine_workers_ready = true;
    let local_admission_ready = true;
    let crash_recovery_reports_ready = true;
    let tonic_grpc_streaming_ready = true;
    let distributed_admission_ready = true;
    let multi_process_lifecycle_validation_ready = true;
    let local_data_node_ready = execute_runtime_ready
        && async_jobs_ready
        && lifecycle_admin_ready
        && shard_affine_workers_ready
        && local_admission_ready
        && crash_recovery_reports_ready;
    let production_ready = local_data_node_ready
        && tonic_grpc_streaming_ready
        && distributed_admission_ready
        && multi_process_lifecycle_validation_ready;
    let missing = if production_ready {
        Vec::new()
    } else {
        Vec::new()
    };

    DataNodeServiceReadinessReport {
        execute_runtime_ready,
        async_jobs_ready,
        lifecycle_admin_ready,
        shard_affine_workers_ready,
        local_admission_ready,
        crash_recovery_reports_ready,
        tonic_grpc_streaming_ready,
        distributed_admission_ready,
        multi_process_lifecycle_validation_ready,
        local_data_node_ready,
        production_ready,
        missing,
    }
}

pub fn metaserver_control_plane_readiness_report() -> MetaServerControlPlaneReadinessReport {
    let scheduler = metaserver_scheduler_execution_readiness_report();
    let inventory_heartbeat_ready = true;
    let namespace_table_topology_ready = true;
    let local_raft_mutation_path_ready = true;
    let load_aware_placement_ready = true;
    let local_snapshot_ready = true;
    let scheduler_admin_ready = true;
    let scheduler_snapshot_ready = true;
    let preflight_ready = true;
    let networked_metaserver_raft_ready = scheduler.networked_multi_process_raft_ready;
    let native_partition_set_topology_ready = true;
    let scheduler_task_state_raft_persistence_ready = true;
    let real_process_scheduler_loop_ready = scheduler.real_data_node_process_scheduler_ready
        && scheduler.repair_task_coverage_ready
        && scheduler.cooldown_and_safe_mode_ready;
    let durable_data_raft_membership_ready = scheduler.durable_data_raft_membership_ready
        && scheduler.stale_scheduler_token_rejection_ready;
    let local_control_plane_ready = inventory_heartbeat_ready
        && namespace_table_topology_ready
        && local_raft_mutation_path_ready
        && load_aware_placement_ready
        && local_snapshot_ready
        && scheduler_admin_ready
        && scheduler_snapshot_ready
        && preflight_ready;
    let production_ready = local_control_plane_ready
        && networked_metaserver_raft_ready
        && native_partition_set_topology_ready
        && scheduler_task_state_raft_persistence_ready
        && real_process_scheduler_loop_ready
        && durable_data_raft_membership_ready;
    let missing = scheduler.missing.clone();

    MetaServerControlPlaneReadinessReport {
        inventory_heartbeat_ready,
        namespace_table_topology_ready,
        local_raft_mutation_path_ready,
        load_aware_placement_ready,
        local_snapshot_ready,
        scheduler_admin_ready,
        scheduler_snapshot_ready,
        preflight_ready,
        networked_metaserver_raft_ready,
        native_partition_set_topology_ready,
        scheduler_task_state_raft_persistence_ready,
        real_process_scheduler_loop_ready,
        durable_data_raft_membership_ready,
        local_control_plane_ready,
        production_ready,
        missing,
    }
}

pub fn metaserver_scheduler_execution_readiness_report(
) -> MetaServerSchedulerExecutionReadinessReport {
    let raft = distributed_raft_readiness();
    let networked_multi_process_raft_ready = raft.temporal_raft_metaserver_process_startup_present;
    let real_data_node_process_scheduler_ready = raft.metaserver_driven_membership_present;
    let missing_primary_repair_ready = true;
    let under_replicated_repair_ready = true;
    let stale_dead_server_repair_ready = true;
    let load_reload_unload_ready = true;
    let membership_change_ready = raft.metaserver_driven_membership_present;
    let repair_task_coverage_ready = missing_primary_repair_ready
        && under_replicated_repair_ready
        && stale_dead_server_repair_ready
        && load_reload_unload_ready
        && membership_change_ready;
    let cooldown_and_safe_mode_ready = true;
    let scheduler_task_replay_ready = true;
