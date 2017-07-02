// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftClusterInner core helpers (timeouts, candidates, election, membership, WAL, status), split from raft.rs.
use super::*;

impl RaftClusterInner {
    pub(super) fn refresh_offline_timeout_states(&mut self) {
        for node in self.nodes.values_mut() {
            if node.alive {
                node.pipeline_state.offline_since_ms = None;
                node.pipeline_state.offline_elapsed_ms = 0;
                node.pipeline_state.offline_timeout_reached = false;
                continue;
            }
            let offline_since_ms = node
                .pipeline_state
                .offline_since_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.offline_elapsed_ms =
                self.logical_time_ms.saturating_sub(*offline_since_ms);
            let reached = self.config.offline_timeout_tick > 0
                && node.pipeline_state.offline_elapsed_ms >= self.config.offline_timeout_tick;
            if reached && !node.pipeline_state.offline_timeout_reached {
                node.pipeline_state.offline_timeout_rejections = node
                    .pipeline_state
                    .offline_timeout_rejections
                    .saturating_add(1);
            }
            node.pipeline_state.offline_timeout_reached = reached;
        }
    }

    pub(super) fn refresh_snapshot_send_timeouts(&mut self) {
        if self.config.send_snapshot_timeout_ms == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            // Either flag makes the guard reject a proposal to this peer, so either has to be
            // able to time out. Watching only the sending flag left a peer stuck installing to
            // reject proposals forever: the refresh skipped it and reset its clock every pass.
            let in_transfer = node.pipeline_state.snapshot_sending
                || node.pipeline_state.snapshot_installing;
            if !in_transfer {
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                node.pipeline_state.snapshot_send_progress_mark = 0;
                continue;
            }
            // Progress resets the clock. Measuring from the start of the send instead would
            // abort a large transfer that is moving along perfectly well, which is the reason
            // the timeout had to be set long enough to be useless against a real stall.
            let progressed = node.pipeline_state.snapshot_install_received_chunks
                != node.pipeline_state.snapshot_send_progress_mark;
            if progressed {
                node.pipeline_state.snapshot_send_progress_mark =
                    node.pipeline_state.snapshot_install_received_chunks;
                node.pipeline_state.snapshot_send_started_ms = Some(self.logical_time_ms);
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .snapshot_send_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.snapshot_send_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.snapshot_send_elapsed_ms >= self.config.send_snapshot_timeout_ms
            {
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_progress_mark = 0;
                node.pipeline_state.snapshot_send_timeouts =
                    node.pipeline_state.snapshot_send_timeouts.saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                node.pipeline_state.snapshot_backpressure_rejections = node
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
            }
        }
    }

    pub(super) fn refresh_leader_transfer_timeouts(&mut self) {
        if self.config.transfer_timeout_tick == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            if !node.pipeline_state.transfer_leader_target {
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .transfer_leader_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.transfer_leader_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.transfer_leader_elapsed_ms >= self.config.transfer_timeout_tick {
                node.pipeline_state.transfer_leader_target = false;
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_timeouts = node
                    .pipeline_state
                    .transfer_leader_timeouts
                    .saturating_add(1);
                node.pipeline_state.transfer_leader_rejected = node
                    .pipeline_state
                    .transfer_leader_rejected
                    .saturating_add(1);
            }
        }
    }

    pub(super) fn ensure_live_leader(&mut self) -> Result<(), RaftError> {
        if self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.promote_best_live_follower()
    }

    pub(super) fn promote_best_live_follower(&mut self) -> Result<(), RaftError> {
        let candidate = self.best_live_candidate()?;
        self.elect_leader(candidate)
    }

    pub(super) fn best_live_candidate(&self) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| node.alive && node.replica_role.can_be_leader())
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    pub(super) fn best_live_candidate_in(
        &self,
        allowed: &BTreeSet<RaftNodeId>,
    ) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| {
                allowed.contains(&node.id) && node.alive && node.replica_role.can_be_leader()
            })
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    pub(super) fn catch_up_live_followers(&mut self) -> Result<Vec<RaftNodeId>, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut caught_up = Vec::new();
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index < leader_commit_index
                || node_last_log_or_snapshot_index(node)
                    < leader_log
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or_default()
            {
                install_leader_snapshot_tail(
                    node,
                    leader_snapshot.clone(),
                    leader_log.clone(),
                    leader_commit_index,
                );
            }
            if node.commit_index >= leader_commit_index {
                caught_up.push(node.id);
            }
        }
        Ok(caught_up)
    }

    pub(super) fn catch_up_live_followers_bounded(
        &mut self,
        max_entries_per_follower: u64,
    ) -> Result<u64, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut replayed_log_entries = 0u64;
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index >= leader_commit_index {
                continue;
            }
            let before = node.commit_index;
            let snapshot_floor = leader_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            let target_commit_index = leader_commit_index
                .min(node.commit_index + max_entries_per_follower)
                .max(snapshot_floor.min(leader_commit_index));
            if let Some(snapshot) = leader_snapshot.clone() {
                if node_last_log_or_snapshot_index(node) < snapshot.last_included_index {
                    install_snapshot_state_for_role(node, snapshot);
                }
            }
            node.log = leader_log
                .iter()
                .filter(|entry| entry.index <= target_commit_index)
                .cloned()
                .collect();
            node.commit_index = target_commit_index;
            if node.replica_role.can_serve_data() {
                apply_committed(node);
            }
            replayed_log_entries += node.commit_index.saturating_sub(before);
        }
        Ok(replayed_log_entries)
    }

    pub(super) fn remove_node_safely(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.voting_node_ids().len() == 1
            && self
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        let remaining = self
            .voting_node_ids()
            .into_iter()
            .filter(|id| *id != node_id)
            .collect::<BTreeSet<_>>();
        let required_after = majority(remaining.len());
        let live_after = remaining
            .iter()
            .filter(|id| self.nodes.get(id).map(|node| node.alive).unwrap_or(false))
            .count();
        if live_after < required_after {
            return Err(RaftError::NoMajority {
                live: live_after,
                required: required_after,
            });
        }

        if self.leader_id == node_id {
            let leader_commit_index = self.leader_commit_index();
            let candidate_id = self.best_live_candidate_in(&remaining)?;
            let candidate = self
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            if candidate.commit_index < leader_commit_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: candidate_id,
                    replica_commit_index: candidate.commit_index,
                    leader_commit_index,
                });
            }
            self.nodes.remove(&node_id);
            self.elect_leader(candidate_id)?;
        } else {
            self.nodes.remove(&node_id);
        }
        Ok(())
    }

    pub(super) fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        if self.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        if !self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Err(RaftError::LeaderUnavailable);
        }
        let old_voters = self.voting_node_ids().into_iter().collect::<BTreeSet<_>>();
        let new_voters = new_voters.into_iter().collect::<BTreeSet<_>>();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if old_voters == new_voters {
            return Err(RaftError::InvalidConfig(
                "membership change must add or remove at least one voter".to_string(),
            ));
        }

        let add_voters = new_voters
            .difference(&old_voters)
            .copied()
            .collect::<Vec<_>>();
        let remove_voters = old_voters
            .difference(&new_voters)
            .copied()
            .collect::<Vec<_>>();
        let kind = match (add_voters.is_empty(), remove_voters.is_empty()) {
            (false, true) => RaftMembershipChangeKind::AddVoter,
            (true, false) => RaftMembershipChangeKind::RemoveVoter,
            (false, false) => RaftMembershipChangeKind::ReplaceVoter,
            (true, true) => unreachable!("old_voters != new_voters was checked"),
        };

        let live_new_voters = new_voters
            .iter()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.alive)
                    .unwrap_or(true)
            })
            .count();
        let required_new_majority = majority(new_voters.len());
        if live_new_voters < required_new_majority {
            return Err(RaftError::NoMajority {
                live: live_new_voters,
                required: required_new_majority,
            });
        }

        Ok(RaftMembershipChangePlan {
            shard_id: self.shard_id,
            kind,
            old_voters: old_voters.into_iter().collect(),
            new_voters: new_voters.into_iter().collect(),
            add_voters,
            remove_voters,
        })
    }

    pub(super) fn scale_change_report(&self) -> RaftScaleChangeReport {
        let status = self.status();
        RaftScaleChangeReport {
            leader_id: status.leader_id,
            voters: self.voting_node_ids(),
            live_voters: status.live_voters,
            majority: status.majority,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn failover_report(&self, old_leader_id: RaftNodeId) -> RaftFailoverReport {
        let status = self.status();
        let term = status.current_term;
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    pub(super) fn pre_vote_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        self.candidate_log_would_win(candidate_id)
    }

    pub(super) fn candidate_log_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        // Snapshot-aware tail: after log compaction the last committed entry lives in the
        // installed snapshot, not `log`. Comparing raw `log.last()` (which is None -> (0,0)
        // for a fully-snapshotted node) would let a lagging candidate look "up to date"
        // against a caught-up replica (safety) and stop the healthiest node from winning
        // (liveness). Mirrors the meta-raft election path.
        let candidate_last_index = node_last_log_or_snapshot_index(candidate);
        let candidate_last_term =
            node_term_at_log_or_snapshot_index(candidate, candidate_last_index).unwrap_or_default();
        let votes = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node_last_log_or_snapshot_index(node);
                let local_last_term =
                    node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        if let Some(membership) = &self.joint_membership {
            let old_votes = self.up_to_date_votes_in(candidate_id, &membership.old_voters)?;
            let new_votes = self.up_to_date_votes_in(candidate_id, &membership.new_voters)?;
            return Ok(old_votes >= majority(membership.old_voters.len())
                && new_votes >= majority(membership.new_voters.len()));
        }
        Ok(votes >= majority(self.voting_node_ids().len()))
    }

    pub(super) fn up_to_date_votes_in(
        &self,
        candidate_id: RaftNodeId,
        voters: &[RaftNodeId],
    ) -> Result<usize, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        // Snapshot-aware tail: after log compaction the last committed entry lives in the
        // installed snapshot, not `log`. Comparing raw `log.last()` (which is None -> (0,0)
        // for a fully-snapshotted node) would let a lagging candidate look "up to date"
        // against a caught-up replica (safety) and stop the healthiest node from winning
        // (liveness). Mirrors the meta-raft election path.
        let candidate_last_index = node_last_log_or_snapshot_index(candidate);
        let candidate_last_term =
            node_term_at_log_or_snapshot_index(candidate, candidate_last_index).unwrap_or_default();
        Ok(voters
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node_last_log_or_snapshot_index(node);
                let local_last_term =
                    node_term_at_log_or_snapshot_index(node, local_last_index).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count())
    }

    /// R5: promote `node_id` only after a DURABLE per-term quorum RequestVote round grants it a
    /// majority. This mirrors the receiver rules
    /// already implemented in `receive_vote_request` (single vote per term via `voted_for`,
    /// leader-completeness log-freshness), but drives them across every voter and gates
    /// promotion on the collected grant count. A partitioned candidate whose granting voters
    /// fall short of majority cannot elect, so two disjoint partitions can never each promote a
    /// leader for the same term.
    pub(super) fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            for node in self.nodes.values_mut() {
                node.pipeline_state.election_rejections =
                    node.pipeline_state.election_rejections.saturating_add(1);
            }
            return Err(RaftError::ElectionProhibited);
        }
        if let Some((live, required)) = self.joint_majority_failure() {
