// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shared test helpers/fixtures, split from tests.rs.
#![allow(dead_code)]
use super::*;

#[derive(Debug, Clone)]
pub(super) struct FlakyTransport {
    pub(super) cluster: RaftCluster,
    pub(super) failures_left: Arc<Mutex<usize>>,
}

impl RaftTransport for FlakyTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let mut failures_left = self
            .failures_left
            .lock()
            .expect("flaky transport lock poisoned");
        if *failures_left > 0 {
            *failures_left -= 1;
            return Err(RaftError::Transport("injected retry".to_string()));
        }
        drop(failures_left);
        self.cluster.receive_append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.cluster.receive_install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        let mut failures_left = self
            .failures_left
            .lock()
            .expect("flaky transport lock poisoned");
        if *failures_left > 0 {
            *failures_left -= 1;
            return Err(RaftError::Transport("injected retry".to_string()));
        }
        drop(failures_left);
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

#[derive(Debug, Clone)]
pub(super) struct OneFailedFollowerTransport {
    pub(super) cluster: RaftCluster,
    pub(super) failed_target: RaftNodeId,
    pub(super) snapshot_attempts: Arc<Mutex<usize>>,
}

impl RaftTransport for OneFailedFollowerTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        if request.target_id == self.failed_target {
            return Err(RaftError::Transport("target unavailable".to_string()));
        }
        self.cluster.receive_append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        *self
            .snapshot_attempts
            .lock()
            .expect("snapshot attempts lock poisoned") += 1;
        self.cluster.receive_install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

pub(super) fn ready_temporal_raft_process_node(node_id: RaftNodeId) -> TemporalRaftProcessNodeEvidence {
    TemporalRaftProcessNodeEvidence {
        node_id,
        addr: format!("127.0.0.1:{}", 19000 + node_id),
        wal_dir: format!("/tmp/temporalstore-temporal-raft-test/node-{node_id}"),
        snapshot_dir: format!("/tmp/temporalstore-temporal-raft-test/node-{node_id}/snapshot"),
        commit_index: 11,
        applied_index: 11,
        snapshot_id: Some("snapshot-11".to_string()),
        restarted: true,
        log_store_validated: true,
    }
}

pub(super) fn ready_temporal_raft_operational_semantics() -> TemporalRaftProcessOperationalSemanticsEvidence {
    TemporalRaftProcessOperationalSemanticsEvidence {
        api_presence_only_rejected: true,
        process_path_validated: true,
        read_index_validated: true,
        leader_lease_validated: true,
        stale_leader_lease_rejection_observed: true,
        follower_lease_expiration_observed: true,
        lagging_follower_read_rejected: true,
        bounded_stale_read_acceptance_observed: true,
        bounded_stale_read_rejection_observed: true,
        minority_partition_read_rejection_observed: true,
        healed_follower_catchup_observed: true,
        stale_follower_write_rejected: true,
        leader_transfer_exact_once_validated: true,
        leader_transfer_under_load_validated: true,
        snapshot_bootstrap_validated: true,
        snapshot_install_restart_validated: true,
        membership_rescale_validated: true,
        membership_add_promote_remove_validated: true,
        follower_rejoin_after_compaction_validated: true,
        secondary_read_eligibility_validated: true,
        apply_pipeline_converged: true,
        wal_persistence_observed: true,
        fsm_apply_idempotent_replay_observed: true,
        storage_mutation_wal_fence_atomicity_observed: true,
        snapshot_install_apply_fence_atomicity_observed: true,
        process_restart_after_apply_crash_recovered: true,
        ready: true,
        blockers: Vec::new(),
    }
}

pub(super) fn ready_data_node_temporal_raft_rollout_report() -> TemporalRaftDataNodeProcessRolloutReport {
    TemporalRaftDataNodeProcessRolloutReport {
        shard_id: 7,
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        nodes: vec![
            ready_temporal_raft_process_node(1),
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
        ],
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 6,
        read_index_responses_observed: 2,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        write_proposed_through_process_api: true,
        leader_transfer_validated: true,
        failover_validated: true,
        membership_change_validated: true,
        secondary_lag_observed: true,
        lagging_follower_read_rejection_observed: true,
        stale_follower_write_rejection_observed: true,
        catchup_read_eligibility_observed: true,
        minority_partition_rejection_observed: true,
        bounded_stale_read_eligibility_observed: true,
        healed_follower_catchup_observed: true,
        lagging_follower_observed_lag: 1,
        follower_lag_validated: true,
        secondary_read_validated: true,
        recovered_after_restart: true,
        restart_recovery_validated: true,
        snapshot_install_validated: true,
        applied_fence_validated: true,
        crash_after_storage_mutation_recovered: true,
        crash_after_wal_persist_recovered: true,
        crash_during_snapshot_install_recovered: true,
        apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_temporal_raft_operational_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

pub(super) fn ready_meta_temporal_raft_rollout_report() -> TemporalRaftMetaProcessRolloutReport {
    TemporalRaftMetaProcessRolloutReport {
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        nodes: vec![
            ready_temporal_raft_process_node(1),
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
        ],
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 6,
        read_index_responses_observed: 2,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        mutation_proposed_through_process_api: true,
        applied_raft_mutations: 4,
        generated_scheduler_tasks: 2,
        scheduler_retries: 1,
        stale_scheduler_token_rejected: true,
        data_node_membership_results_ready: true,
        scheduler_mutations_proposed_through_process_api: true,
        scheduler_task_replay_from_raft_log_observed: true,
        membership_mutations_proposed_through_process_api: true,
        data_node_membership_workflow_report_attached: true,
        data_node_raft_group_results_observed: true,
        failover_validated: true,
        membership_change_validated: true,
        follower_lag_validated: true,
        secondary_read_validated: true,
        read_index_validated: true,
        snapshot_install_validated: true,
        recovered_after_restart: true,
        scheduler_task_replay_validated: true,
        crash_after_meta_mutation_recovered: true,
        crash_after_meta_wal_persist_recovered: true,
        crash_during_meta_snapshot_install_recovered: true,
        meta_apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_temporal_raft_operational_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

pub(super) fn wait_for_replica_value(
    runtime: &ProductionRaftRuntime,
    node_id: u64,
    key: &str,
    expected: &[u8],
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = runtime
