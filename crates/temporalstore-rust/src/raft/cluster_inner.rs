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
