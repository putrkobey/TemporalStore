// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Production Raft/Meta-Raft runtime + timer-handle types and impls, split from raft.rs.
use super::*;

impl ProductionRaftRuntimeOptions {
    pub fn validate(&self) -> Result<(), RaftError> {
        self.config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        if self.nodes.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires at least one node".to_string(),
            ));
        }
        let mut node_ids = BTreeSet::new();
        let mut node_addrs = BTreeSet::new();
        for node in &self.nodes {
            if node.node_id == 0 {
                return Err(RaftError::InvalidConfig(
                    "production raft node_id must be non-zero".to_string(),
                ));
            }
            if node.addr.trim().is_empty() {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node {} requires a non-empty addr",
                    node.node_id
                )));
            }
            if !node_ids.insert(node.node_id) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node_id {} is duplicated",
                    node.node_id
                )));
            }
            if !node_addrs.insert(node.addr.trim().to_string()) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft addr {} is duplicated",
                    node.addr
                )));
            }
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production raft nodes".to_string(),
            ));
        }
        if self.wal_dir.trim().is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires wal_dir".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0 || self.election_tick_ms == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft heartbeat/election intervals must be non-zero".to_string(),
            ));
        }
        if self.max_catchup_entries_per_heartbeat == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft max_catchup_entries_per_heartbeat must be non-zero".to_string(),
            ));
        }
        self.security
            .validate(self.allow_plaintext_for_local_chaos)?;
        Ok(())
    }

    pub(super) fn peer_map(&self) -> BTreeMap<RaftNodeId, String> {
        self.nodes
            .iter()
            .filter(|node| node.node_id != self.local_node_id)
            .map(|node| (node.node_id, node.addr.clone()))
            .collect()
    }

    pub(super) fn node_addr(&self, node_id: RaftNodeId) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.addr.as_str())
    }

    pub(super) fn voter_ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }
}

#[derive(Debug, Clone)]
pub struct ProductionRaftRuntime {
    options: ProductionRaftRuntimeOptions,
    cluster: RaftCluster,
}

impl ProductionRaftRuntime {
    pub fn start(options: ProductionRaftRuntimeOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let cluster = RaftCluster::restore_single_shard_from_wal(
            &options.wal_dir,
            options.shard_id,
            options.voter_ids(),
            options.config.clone(),
        )?;
        // One node per process here, so the WAL persist and the committed-entry apply can both be
        // scoped to our own node instead of covering every peer's shadow copy.
        cluster.set_local_node_id(options.local_node_id);
        Ok(Self { options, cluster })
    }

    pub fn cluster(&self) -> RaftCluster {
        self.cluster.clone()
    }

    pub fn peer_auth_token(&self) -> Option<&str> {
        self.options.security.auth_token.as_deref()
    }

    /// Advertised addresses of every raft node except this one. These are the same
    /// addresses the datanode serves its HTTP tier on (raft, `/blob`, etc. share the
    /// advertised server addr), so they double as the cross-peer blob-fetch target
    /// list. Empty for a single-node cluster.
    pub fn peer_addrs(&self) -> Vec<String> {
        self.options.peer_map().into_values().collect()
    }

    pub fn local_node_id(&self) -> RaftNodeId {
        self.options.local_node_id
    }

    pub fn local_apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        self.cluster
            .observer_apply_health(self.options.local_node_id, max_allowed_apply_lag)
    }

    pub fn data_node_atomic_durability_report(&self) -> RaftDataNodeAtomicDurabilityReport {
        let status = self.cluster.status();
        let local_status = status
            .nodes
            .iter()
            .find(|node| node.node_id == self.options.local_node_id);
        let wal_record = self
            .cluster
            .wal_records()
            .into_iter()
            .find(|(node_id, _)| *node_id == self.options.local_node_id)
            .map(|(_, record)| record);

        let mut blockers = Vec::new();
        let mut commit_index = 0;
        let mut applied_index = 0;
        if let Some(local_status) = local_status {
            commit_index = local_status.commit_index;
            applied_index = local_status.applied_index;
            if local_status.applied_index < local_status.commit_index {
                blockers.push("local_applied_index_lags_commit_index".to_string());
            }
        } else {
            blockers.push("local_node_status_missing".to_string());
        }

        let mut wal_commit_index = 0;
        let mut fence = RaftStorageApplyFence::default();
        let mut storage_apply_fence_valid = false;
        if let Some(record) = wal_record {
            wal_commit_index = record.hard_state.commit_index;
            fence = record.storage_apply_fence.clone();
            match validate_raft_storage_apply_fence(&record) {
                Ok(()) => storage_apply_fence_valid = true,
                Err(err) => blockers.push(format!("storage_apply_fence_invalid:{err}")),
            }
            if record.hard_state.commit_index != commit_index {
                blockers.push("wal_commit_index_mismatch".to_string());
            }
            if record.storage_apply_fence.applied_index != applied_index {
                blockers.push("storage_fence_applied_index_mismatch".to_string());
            }
        } else {
            blockers.push("local_wal_record_missing".to_string());
        }

        let storage_mutation_atomic_commit_present = storage_apply_fence_valid
            && wal_commit_index == commit_index
            && fence.applied_index == applied_index;
        let snapshot_install_atomic_commit_present =
            storage_apply_fence_valid && fence.storage_epoch >= fence.applied_index;
        if !storage_mutation_atomic_commit_present {
            blockers.push("storage_mutation_atomic_commit_missing".to_string());
        }
        if !snapshot_install_atomic_commit_present {
            blockers.push("snapshot_install_atomic_commit_missing".to_string());
        }

        RaftDataNodeAtomicDurabilityReport {
            node_id: self.options.local_node_id,
            shard_id: self.options.shard_id,
            commit_index,
            applied_index,
            wal_commit_index,
            fence_committed_index: fence.committed_index,
            fence_applied_index: fence.applied_index,
            storage_epoch: fence.storage_epoch,
            snapshot_id: fence.snapshot_id,
            storage_apply_fence_valid,
            storage_mutation_atomic_commit_present,
            snapshot_install_atomic_commit_present,
            ready: blockers.is_empty(),
            blockers,
        }
    }

    pub fn transport(&self) -> RaftRpcRuntime<AuthenticatedRaftTransport<HttpRaftTransport>> {
        let http_options = HttpRequestOptions {
            connect_timeout_ms: self.options.rpc.deadline_ms,
            io_timeout_ms: self.options.rpc.deadline_ms,
            max_retries: self.options.rpc.max_retries,
        };
        let http = HttpRaftTransport::with_options(self.options.peer_map(), http_options);
        let auth = AuthenticatedRaftTransport::new(
            http,
            self.options
                .security
                .auth_token
                .clone()
                .expect("validated production raft auth token"),
        );
        RaftRpcRuntime::with_auth_token(
            auth,
            self.options.rpc,
            self.options.security.auth_token.clone(),
        )
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        if self.cluster.leader_id() != self.options.local_node_id {
            return Err(RaftError::NotLeader {
                node_id: self.options.local_node_id,
            });
        }
        let transport = self.transport();
        self.cluster.propose_distributed(command, &transport)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn transfer_leader(&self, target_id: RaftNodeId) -> Result<(), RaftError> {
        if self.cluster.leader_id() == target_id {
            return Ok(());
