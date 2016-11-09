// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster read/read-index/status/report methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn read_local(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        Ok(node
            .engine
            .execute(ExecuteRequest {
                shard_id: inner.shard_id,
                command,
            })
            .response)
    }

    pub fn read_from_replica(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if node.commit_index < leader_commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: node.commit_index,
                leader_commit_index,
            });
        }
        // R3: committed is not the right freshness bar — a replica can hold
        // commit_index == leader_commit while its state machine has only applied a prefix of
        // that (applied_index < commit_index), which would serve HALF-APPLIED state as fresh.
        // Gate the read on applied_index reaching the leader's committed frontier.
        if node.applied_index < leader_commit_index {
            return Err(RaftError::ReplicaApplyLagging {
                replica_id: node_id,
                replica_applied_index: node.applied_index,
                required_index: leader_commit_index,
            });
        }
        Ok(node
            .engine
            .execute(ExecuteRequest {
                shard_id: inner.shard_id,
                command,
            })
            .response)
    }

    pub fn leader_id(&self) -> RaftNodeId {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .leader_id
    }

    pub fn advance_time_ms(&self, elapsed_ms: u64) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.logical_time_ms = inner.logical_time_ms.saturating_add(elapsed_ms);
        inner.refresh_offline_timeout_states();
        inner.refresh_snapshot_send_timeouts();
        inner.refresh_leader_transfer_timeouts();
        let _ = inner.persist_configured_wal();
    }

    /// How long this node has been behind the leader without accepting anything, in
    /// milliseconds of cluster clock. Zero when it is not behind.
    ///
    /// Staleness used to be derivable only by comparing log indices against this process's own
    /// shadow of the peer. That shadow does not move when the peer rejects, so a follower that
    /// had stopped converging reported a lag of ZERO while falling further behind -- the failure
    /// mode reporting itself as health. This cannot: the comparison is against the commit index
    /// the LEADER sent, and the clock advances whether or not appends are landing.
    pub fn replication_stall_ms(&self, node_id: RaftNodeId) -> u64 {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let Some(node) = inner.nodes.get(&node_id) else {
            return 0;
        };
        let behind = node.pipeline_state.leader_reported_commit_index
            > node_last_log_or_snapshot_index(node);
        if !behind {
            // Caught up with everything the leader has admitted to committing. An idle cluster
            // must not look stalled.
            return 0;
        }
        inner
            .logical_time_ms
            .saturating_sub(node.pipeline_state.last_accepted_append_ms)
    }

    pub fn shard_id(&self) -> ShardId {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .shard_id
    }

    pub fn latest_external_snapshot_ref(&self) -> Option<RaftExternalSnapshotRef> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .latest_external_snapshot_ref
            .clone()
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_accounted(node_id, false)
    }

    pub(super) fn read_index_accounted(
        &self,
        node_id: RaftNodeId,
        lease_read: bool,
    ) -> Result<ReadIndexResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.read_safety_state.read_index_requests = inner
            .read_safety_state
            .read_index_requests
            .saturating_add(1);
        if lease_read {
            inner.read_safety_state.lease_read_requests = inner
                .read_safety_state
                .lease_read_requests
                .saturating_add(1);
        }
        // R4: withhold read-index/lease reads on a leader that has not yet committed an entry in
        // its own term. A leader freshly elected (or one that has just superseded a partitioned
        // predecessor) has not established its commit point until it commits in-term, so serving
        // now risks returning stale linearizable state.
        if inner.leader_ready_barrier
            && node_id == inner.leader_id
            && !inner.leader_has_committed_current_term()
        {
            inner.read_safety_state.read_index_rejected = inner
                .read_safety_state
                .read_index_rejected
                .saturating_add(1);
            if lease_read {
                inner.read_safety_state.lease_read_rejected = inner
                    .read_safety_state
                    .lease_read_rejected
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            return Err(RaftError::LeaderUnavailable);
        }
        let status = inner.status();
        let Some(node) = inner.nodes.get(&node_id) else {
            inner.read_safety_state.read_index_rejected = inner
                .read_safety_state
                .read_index_rejected
                .saturating_add(1);
            if lease_read {
                inner.read_safety_state.lease_read_rejected = inner
                    .read_safety_state
                    .lease_read_rejected
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        };
        let decision = matrixraft_read_safety_runtime_decision(MatrixRaftReadSafetyRuntimeInput {
            operation: if lease_read {
                MatrixRaftReadSafetyOperation::LeaseRead
            } else {
                MatrixRaftReadSafetyOperation::ReadIndex
            },
            node_id,
            leader_id: inner.leader_id,
            node_alive: node.alive,
            role_can_serve_data: node.replica_role.can_serve_data(),
            leader_lease_valid: status.leader_lease_valid,
            has_majority: status.has_majority,
            node_commit_index: node.commit_index,
            leader_commit_index: status.commit_index,
            max_stale_index_lag: 0,
        });
        // The external read-safety decision only flags a stale leader lease for the
        // LeaseRead operation; a plain ReadIndex issued after the lease has expired
        // (majority still present) is otherwise allowed. Linearizable reads must
        // block until the next heartbeat renews the lease, mirroring the write path
        // (propose_one rejects on !leader_lease_valid), so enforce and count it here.
        // leader_lease_valid() is `lease_duration_ms == 0 || time <= deadline`, so
        // `!leader_lease_valid` already means the lease is enabled and expired.
        let stale_leader_lease = !status.leader_lease_valid;
        if !decision.allowed || stale_leader_lease {
            let replica_commit_index = node.commit_index;
            inner
                .read_safety_state
                .record_matrixraft_runtime_decision(&decision);
            if stale_leader_lease && !decision.stale_leader_lease_rejected {
                inner.read_safety_state.stale_leader_lease_rejected = inner
                    .read_safety_state
                    .stale_leader_lease_rejected
                    .saturating_add(1);
            }
            inner.read_safety_state.read_index_rejected = inner
                .read_safety_state
                .read_index_rejected
                .saturating_add(1);
            if lease_read {
                inner.read_safety_state.lease_read_rejected = inner
                    .read_safety_state
                    .lease_read_rejected
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            if stale_leader_lease {
                return Err(RaftError::LeaderUnavailable);
            }
            return match decision.reason.as_str() {
                "stale_leader_lease" | "minority_partition" => Err(RaftError::LeaderUnavailable),
                "replica_lagging" => Err(RaftError::ReplicaLagging {
                    replica_id: node_id,
                    replica_commit_index,
                    leader_commit_index: status.commit_index,
                }),
                _ => Err(RaftError::NodeNotFound(node_id)),
            };
        }
        if decision.healed_follower_catchup_observed {
            inner
                .read_safety_state
                .record_matrixraft_runtime_decision(&decision);
        }
        inner.read_safety_state.read_index_accepted = inner
            .read_safety_state
            .read_index_accepted
            .saturating_add(1);
        if lease_read {
            inner.read_safety_state.lease_read_accepted = inner
                .read_safety_state
                .lease_read_accepted
                .saturating_add(1);
        }
        inner.persist_configured_wal()?;
        Ok(ReadIndexResponse {
            leader_id: inner.leader_id,
            node_id,
            term: status.current_term,
            read_index: status.commit_index,
        })
    }

    pub fn read_safety_runtime_state(&self) -> RaftReadSafetyRuntimeState {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .read_safety_state
            .clone()
    }

    pub fn check_read(
        &self,
        node_id: RaftNodeId,
        options: RaftReadOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !options.enable_read_from_follower && node_id != inner.leader_id {
            return Err(RaftError::NotLeader { node_id });
        }
        drop(inner);
        match options.strategy {
            RaftReadStrategy::RelaxRead => {
                let status = self.status();
                Ok(ReadIndexResponse {
                    leader_id: status.leader_id,
                    node_id,
                    term: status.current_term,
                    read_index: self.commit_index(node_id)?,
                })
            }
            RaftReadStrategy::LeaseRead => self.read_index_accounted(node_id, true),
            RaftReadStrategy::ReadIndex => self.read_index(node_id),
        }
    }

    pub fn check_data_raft_read_policy(
        &self,
        node_id: RaftNodeId,
        policy: DataRaftReadPolicy,
    ) -> Result<ReadIndexResponse, RaftError> {
        match policy.mode {
            DataRaftReadMode::Leader => self.check_read(node_id, RaftReadOptions::default()),
            DataRaftReadMode::Linearizable => {
                if node_id != self.leader_id() {
                    return Err(RaftError::NotLeader { node_id });
                }
                let _timeout_ms = policy.read_index_timeout_ms;
                self.check_read(
                    node_id,
                    RaftReadOptions {
                        strategy: RaftReadStrategy::ReadIndex,
                        ..RaftReadOptions::default()
                    },
                )
            }
            DataRaftReadMode::BoundedStale => {
                let mut inner = self.inner.write().expect("raft cluster lock poisoned");
                inner.read_safety_state.bounded_stale_read_requests = inner
                    .read_safety_state
                    .bounded_stale_read_requests
                    .saturating_add(1);
                let status = inner.status();
                let Some((node_alive, can_serve_data, node_commit_index)) =
                    inner.nodes.get(&node_id).map(|node| {
                        (
                            node.alive,
                            node.replica_role.can_serve_data(),
                            node.commit_index,
                        )
                    })
                else {
                    inner.read_safety_state.bounded_stale_read_rejected = inner
                        .read_safety_state
                        .bounded_stale_read_rejected
                        .saturating_add(1);
                    inner.persist_configured_wal()?;
