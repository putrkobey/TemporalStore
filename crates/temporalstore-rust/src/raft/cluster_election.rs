// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster leader election / transfer / failover methods, split from raft.rs.
use super::*;

impl RaftCluster {
    /// Monotonic counter of AppendEntries accepted from a leader. A follower that sees
    /// it stall for an election timeout concludes the leader is gone.
    pub fn leader_contact_epoch(&self) -> u64 {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .leader_contact_epoch
    }

    /// True only when this node both believes it is the leader AND still holds the
    /// leader role. A restarted node initialises `leader_id` to the lowest node id, so
    /// `leader_id == me` on its own is not evidence that this node still leads.
    pub fn is_local_leader(&self, node_id: RaftNodeId) -> bool {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        inner.leader_id == node_id
            && inner
                .nodes
                .get(&node_id)
                .map(|node| node.role == RaftRole::Leader)
                .unwrap_or(false)
    }

    /// Give up leadership locally. Used when a leader can no longer reach a quorum, or
    /// learns a peer has moved to a newer term: it must stop acting as leader before it
    /// can serve another write.
    pub fn step_down_local(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let stepped = {
            let node = inner
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if node.role == RaftRole::Leader {
                node.role = RaftRole::Follower;
                true
            } else {
                false
            }
        };
        if stepped {
            inner.leader_lease_deadline_ms = 0;
            inner.persist_configured_wal()?;
        }
        Ok(())
    }

    /// Production runtimes clear this: leadership there is decided by real RequestVote
    /// RPCs, so `tick_election` must not promote a node from local shadow state.
    pub fn set_local_shadow_election(&self, enabled: bool) {
        self.inner
            .write()
            .expect("raft cluster lock poisoned")
            .local_shadow_election = enabled;
    }

    /// Step one of a networked election: bump this node's term, record its self-vote,
    /// and get both durable BEFORE any vote is requested. Returns the RequestVote to
    /// send to each peer (`target_id` is filled in per peer by the caller).
    /// Build the question: would you vote for me, at the term I would use?
    ///
    /// Read-only on purpose. The term is not raised and no vote is recorded, so asking costs
    /// nothing and losing costs nothing. Only [`Self::prepare_campaign`] commits to an election.
    pub fn prepare_pre_vote(&self, candidate_id: RaftNodeId) -> Result<VoteRequest, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        if inner.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        if !inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.alive && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(candidate_id));
        }
        let candidate = inner
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let last_log_index = node_last_log_or_snapshot_index(candidate);
        let last_log_term =
            node_term_at_log_or_snapshot_index(candidate, last_log_index).unwrap_or_default();
        Ok(VoteRequest {
            rpc: None,
            shard_id: inner.shard_id,
            term: inner
                .nodes
                .values()
                .map(|node| node.current_term)
                .max()
                .unwrap_or_default()
                .saturating_add(1),
            candidate_id,
            target_id: candidate_id,
            last_log_index,
            last_log_term,
            pre_vote: true,
        })
    }

    pub fn prepare_campaign(&self, candidate_id: RaftNodeId) -> Result<VoteRequest, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        if !inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.alive && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(candidate_id));
        }
        let shard_id = inner.shard_id;
        let term = inner
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        let (last_log_index, last_log_term) = {
            let candidate = inner
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            let index = node_last_log_or_snapshot_index(candidate);
            (
                index,
                node_term_at_log_or_snapshot_index(candidate, index).unwrap_or_default(),
            )
        };
        {
            let candidate = inner
