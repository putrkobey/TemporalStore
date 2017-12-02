// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster membership-change / joint-consensus / catch-up methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn add_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Voter)
    }

    pub fn add_node_with_role(
        &self,
        node_id: RaftNodeId,
        replica_role: RaftReplicaRole,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeAlreadyExists(node_id));
        }
        inner.ensure_live_leader()?;
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut node = new_node(node_id, RaftRole::Follower, inner.shard_id);
        node.replica_role = replica_role;
        node.current_term = leader.current_term;
        install_leader_snapshot_tail(
            &mut node,
            leader.installed_snapshot.clone(),
            leader.log.clone(),
            leader.commit_index,
        );
        inner.nodes.insert(node_id, node);
        match replica_role {
            RaftReplicaRole::Learner => {
                inner.membership_evidence.learner_add_count = inner
                    .membership_evidence
                    .learner_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Witness => {
                inner.membership_evidence.witness_add_count = inner
                    .membership_evidence
                    .witness_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Voter => {}
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn add_learner_with_auto_promote(
        &self,
        node_id: RaftNodeId,
        auto_promote: bool,
    ) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Learner)?;
        if auto_promote {
            self.catch_up(node_id)?;
            self.promote_learner_to_voter(node_id)?;
        }
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
    }

    pub fn promote_learner_to_voter(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        match node.replica_role {
            RaftReplicaRole::Learner => {
                node.pipeline_state.auto_promoted_from_learner = true;
                node.replica_role = RaftReplicaRole::Voter;
                inner.membership_evidence.learner_promote_count = inner
                    .membership_evidence
                    .learner_promote_count
                    .saturating_add(1);
                inner.membership_evidence.auto_promote_count = inner
                    .membership_evidence
                    .auto_promote_count
                    .saturating_add(1);
                inner.persist_configured_wal()
            }
            RaftReplicaRole::Voter => Ok(()),
            RaftReplicaRole::Witness => Err(RaftError::InvalidConfig(
                "witness replicas cannot be promoted to voter through learner promotion"
                    .to_string(),
            )),
        }
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.voting_node_ids().len() == 1
            && inner
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        inner
            .nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        inner.membership_evidence.voter_remove_count = inner
            .membership_evidence
            .voter_remove_count
            .saturating_add(1);
        if inner.leader_id == node_id {
            inner.promote_best_live_follower()?;
        }
        inner.persist_configured_wal()?;
        Ok(())
    }
