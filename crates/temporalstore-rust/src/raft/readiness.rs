// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub type RaftReadinessEvidenceBlocker = MatrixRaftProcessReadinessBlocker;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDistributedReadiness {
    pub complete: bool,
    pub production_ready: bool,
    pub mode: RaftDeploymentMode,
    pub local_model_tested: bool,
    pub temporal_raft_engine_adapter_present: bool,
    pub temporal_raft_data_node_process_startup_present: bool,
    pub temporal_raft_metaserver_process_startup_present: bool,
    pub transport_contracts_present: bool,
    pub http_transport_tested: bool,
    pub rpc_runtime_observability_present: bool,
    pub external_snapshot_refs_present: bool,
    pub timer_election_tested: bool,
    pub matrixraft_leader_write_authority_present: bool,
    pub matrixraft_operator_observability_present: bool,
    pub matrixraft_rpc_transport_contract_present: bool,
    pub matrixraft_log_retention_snapshot_trigger_present: bool,
    pub matrixraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub matrixraft_snapshot_floor_log_matching_present: bool,
    pub matrixraft_snapshot_tail_catchup_present: bool,
    pub matrixraft_compacted_entry_rejection_present: bool,
    pub matrixraft_metaserver_snapshot_floor_election_present: bool,
    pub durable_apply_index_snapshot_integrated: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
    pub metaserver_driven_membership_present: bool,
    pub production_mtls_transport_present: bool,
    pub external_chaos_validation_present: bool,
    #[serde(default)]
    pub missing_evidence_fields: Vec<RaftReadinessEvidenceBlocker>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftTransportSecurityReadiness {
    /// Which transport this process is actually configured for.
    ///
    /// The `*_present` flags below say what the build implements, which is the
    /// same answer on every deployment. This one says what is running here, so
    /// a reader can tell "we can do mTLS" from "this node is doing mTLS".
    /// Defaults to plaintext, so its absence from a report is not a claim.
    #[serde(default)]
    pub configured_transport_mode: String,
    pub auth_token_validation_present: bool,
    pub mtls_cert_key_ca_validation_present: bool,
    pub authenticated_http_transport_present: bool,
    pub plaintext_local_chaos_guard_present: bool,
    pub service_process_mtls_enforcement_present: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftExternalChaosReadiness {
    pub local_os_process_restart_failover_present: bool,
    pub stale_read_partition_heal_present: bool,
    pub lagging_follower_catchup_present: bool,
    pub networked_membership_snapshot_present: bool,
    pub storage_replay_gate_present: bool,
    pub external_packet_loss_present: bool,
    pub external_disk_pressure_present: bool,
    pub external_process_chaos_present: bool,
    pub local_chaos_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftAtomicApplyReadiness {
    pub storage_apply_fence_present: bool,
    pub wal_fence_recovery_validation_present: bool,
    pub snapshot_lifecycle_report_present: bool,
    pub storage_mutation_atomic_commit_present: bool,
    pub snapshot_install_atomic_commit_present: bool,
    pub real_data_node_process_integration_present: bool,
    pub local_contract_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMetaserverMembershipReadiness {
    pub topology_membership_plan_present: bool,
    pub data_raft_membership_apply_present: bool,
    pub meta_owned_workflow_report_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub leader_transfer_voter_remove_present: bool,
    pub networked_scheduler_transport_present: bool,
    pub persisted_scheduler_task_state_present: bool,
    pub real_data_node_group_execution_present: bool,
    pub local_workflow_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftTemporalRaftRolloutReadiness {
    pub adapter_present: bool,
    pub data_node_process_startup_selects_temporal_raft: bool,
    pub metaserver_process_startup_selects_temporal_raft: bool,
    pub durable_log_state_present: bool,
    pub data_node_real_process_rollout_validated: bool,
    pub metaserver_real_process_rollout_validated: bool,
    pub multi_process_log_store_validation_present: bool,
    pub local_rollout_ready: bool,
    pub production_ready: bool,
    #[serde(default)]
    pub missing_evidence_fields: Vec<RaftReadinessEvidenceBlocker>,
    pub missing: Vec<String>,
}

pub type RaftProcessPathReadinessReport = MatrixRaftCrossPlaneProcessReadinessBlockerReport;

pub fn raft_temporal_raft_rollout_readiness() -> RaftTemporalRaftRolloutReadiness {
    raft_temporal_raft_rollout_readiness_from_reports(None, None)
}

pub fn raft_process_path_readiness_report_from_reports(
    data_node_report: &TemporalRaftDataNodeProcessRolloutReport,
    metaserver_report: &TemporalRaftMetaProcessRolloutReport,
) -> RaftProcessPathReadinessReport {
    matrixraft_cross_plane_process_readiness_blocker_report(data_node_report, metaserver_report)
}

fn raft_readiness_blocker_from_matrixraft_blocker(
    blocker: MatrixRaftProcessReadinessBlocker,
) -> RaftReadinessEvidenceBlocker {
    blocker
}

pub fn raft_temporal_raft_rollout_readiness_from_reports(
    data_node_report: Option<&TemporalRaftDataNodeProcessRolloutReport>,
    metaserver_report: Option<&TemporalRaftMetaProcessRolloutReport>,
) -> RaftTemporalRaftRolloutReadiness {
    let adapter_present = cfg!(feature = "temporal-raft-engine");
    let data_node_process_startup_selects_temporal_raft = true;
    let metaserver_process_startup_selects_temporal_raft = true;
    let durable_log_state_present = true;
    let data_node_real_process_rollout_validated = data_node_report
        .map(matrixraft_data_node_strict_process_rollout_validated)
        .unwrap_or(false);
    let metaserver_real_process_rollout_validated = metaserver_report
        .map(matrixraft_meta_strict_process_rollout_validated)
        .unwrap_or(false);
    let multi_process_log_store_validation_present = data_node_report
        .map(|report| report.multi_process_log_store_validated)
        .unwrap_or(false)
        && metaserver_report
            .map(|report| report.multi_process_log_store_validated)
            .unwrap_or(false);
    let local_rollout_ready = adapter_present
        && data_node_process_startup_selects_temporal_raft
        && metaserver_process_startup_selects_temporal_raft
        && durable_log_state_present;
    let production_ready = local_rollout_ready
        && data_node_real_process_rollout_validated
        && metaserver_real_process_rollout_validated
        && multi_process_log_store_validation_present;
    let mut missing = Vec::new();
    let mut missing_evidence_fields = Vec::new();
    if !adapter_present {
        missing.push(
            "enable the TemporalRaft production engine adapter feature for readiness-eligible process rollout"
                .to_string(),
        );
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "temporal_raft_engine_adapter_missing".to_string(),
            evidence_field: "cfg(feature = \"temporal-raft-engine\")".to_string(),
            detail: "Readiness-eligible Raft must be built with the production TemporalRaft/OpenRaft adapter, not the local fixture path.".to_string(),
        });
    }
