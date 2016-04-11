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
