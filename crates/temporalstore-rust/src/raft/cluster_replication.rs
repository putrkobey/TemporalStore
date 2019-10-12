// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster append-entries / vote / WAL replication methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn wait_for_applied_index(
        &self,
        node_id: RaftNodeId,
        index: u64,
        timeout_ms: u64,
    ) -> Result<(), RaftError> {
        let deadline = InstantCompat::now();
        loop {
            let applied_index = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                inner
                    .nodes
                    .get(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))?
                    .applied_index
            };
            if applied_index >= index {
                return Ok(());
            }
            if deadline.elapsed() >= Duration::from_millis(timeout_ms) {
                return Err(RaftError::AppliedIndexTimeout {
                    node_id,
                    applied_index,
                    target_index: index,
                    timeout_ms,
                });
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn hard_state(&self, node_id: RaftNodeId) -> Result<RaftHardState, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        Ok(RaftHardState {
            current_term: node.current_term,
            voted_for: node.voted_for,
            commit_index: node.commit_index,
        })
    }

    pub fn membership(&self) -> RaftMembership {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        }
    }

    pub fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        };
        inner
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    RaftWalRecord {
                        hard_state: RaftHardState {
                            current_term: node.current_term,
                            voted_for: node.voted_for,
                            commit_index: node.commit_index,
                        },
                        membership: membership.clone(),
                        replica_role: node.replica_role,
                        joint_membership: inner.joint_membership.clone(),
                        latest_external_snapshot_ref: inner.latest_external_snapshot_ref.clone(),
                        installed_snapshot: node.installed_snapshot.clone(),
                        apply_snapshot_fence: raft_apply_snapshot_fence(node),
                        storage_apply_fence: raft_storage_apply_fence(inner.shard_id, node),
                        pipeline_state: node.pipeline_state.clone(),
                        read_safety_state: inner.read_safety_state.clone(),
                        membership_evidence: inner.membership_evidence.clone(),
                        entries: node.log.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn persist_wal(&self, root: impl AsRef<Path>) -> io::Result<()> {
        let wal = LocalRaftWal::new(root.as_ref().to_path_buf());
        let records = self.wal_records();
        for (node_id, record) in records {
            wal.persist_node(record.membership.shard_id, node_id, &record)?;
        }
        Ok(())
    }

    pub fn build_append_entries_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<AppendEntriesRequest, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let entry_limit = inner.config.max_inflights_replicate.max(1);
        let byte_limit = inner.config.max_memory_replicate_log_bytes.max(1);
        let mut current_inflight_entries = 0;
        let mut current_inflight_bytes = 0;
        let mut probe_only = false;
        let leader_commit_for_drain = inner
            .nodes
            .get(&inner.leader_id)
            .map(|leader| leader.commit_index)
            .unwrap_or_default();
        let leader_last_index = inner
            .nodes
            .get(&inner.leader_id)
            .map(node_last_log_or_snapshot_index)
            .unwrap_or_default();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            if target.commit_index >= leader_commit_for_drain
                && (target.pipeline_state.inflight_entries > 0
                    || target.pipeline_state.inflight_bytes > 0)
            {
                target.pipeline_state.inflight_entries = 0;
                target.pipeline_state.inflight_bytes = 0;
                target.pipeline_state.append_queue_depth = 0;
            }
            target.pipeline_state.append_requests =
                target.pipeline_state.append_requests.saturating_add(1);
            current_inflight_entries = target.pipeline_state.inflight_entries;
            current_inflight_bytes = target.pipeline_state.inflight_bytes;
            // A request with nothing to send buffers nothing, so the in-flight limits
            // do not apply to it. This is the heartbeat -- and the probe a newly promoted
            // leader sends to find out where a follower really is. Blocking it leaves the
            // new leader unable to contact anyone, which reads as a leaderless shard.
            let carries_entries = target.pipeline_state.next_index <= leader_last_index;
            if carries_entries
                && (target.pipeline_state.inflight_entries >= entry_limit
                    || target.pipeline_state.inflight_bytes >= byte_limit)
            {
                // A charged-up window used to REFUSE the append outright. For a peer that is
                // behind, that is a deadlock, not backpressure: the refusal blocks the very
                // entries whose acknowledgements would drain the window, the lag-derived
                // refresh keeps re-charging it while the leader commits with the remaining
                // quorum, and the peer is cut off for good. Measured on a 30k-write corpus:
                // one election put a follower past the 128-entry window, it was never sent
                // another entry, the second follower followed at the next election, writes
                // froze on NoMajority, and the log grew unbounded because compaction held for
                // a "catching up" follower that was never allowed to catch up. Degrade to a
                // PROBE instead -- one entry, whatever its size. Its acknowledgement moves the
                // match forward, the refresh re-derives a smaller charge, and the window
                // reopens by itself. The rejection counters still record the pressure.
                target.pipeline_state.append_rejected =
                    target.pipeline_state.append_rejected.saturating_add(1);
                target.pipeline_state.memory_backpressure_rejections = target
                    .pipeline_state
                    .memory_backpressure_rejections
                    .saturating_add(u64::from(
                        target.pipeline_state.inflight_bytes >= byte_limit,
                    ));
                probe_only = true;
            }
        }
        // Never build a batch larger than a receiver will accept in one request: a follower
        // refuses anything over its apply limit, so a bigger batch is not throughput, it is a
        // request that can only be rejected.
        let apply_limit = inner.config.max_inflights_apply_task.max(1);
        let (available_entries, available_bytes) = if probe_only {
            // One entry regardless of the charge: the probe is what drains the window.
            (1, byte_limit)
        } else {
            (
                entry_limit
                    .saturating_sub(current_inflight_entries)
                    .min(apply_limit),
                byte_limit.saturating_sub(current_inflight_bytes),
            )
        };
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_id = inner.leader_id;
        let leader_term = leader.current_term;
        let shard_id = inner.shard_id;
        let leader_commit = leader.commit_index;
        let target = inner
            .nodes
            .get(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        let enable_reorder_queue = inner.config.enable_reorder_queue;
        let leader_last_index = node_last_log_or_snapshot_index(leader);
        // Where to resume from. `next_index` is the peer's own answer -- lowered by every
        // rejection, set from the peer's match index on success -- so it is what makes a mismatch
        // converge. Falling back to the local shadow of the peer is only for a peer that has not
        // answered yet, because the shadow does not move when a peer rejects, and asking about the
        // same index forever is exactly how a diverged follower stays stuck.
        let target_next_index = target.pipeline_state.next_index;
        let prev_log_index = if target_next_index > 0 {
            target_next_index.saturating_sub(1).min(leader_last_index)
        } else {
            node_last_log_or_snapshot_index(target).min(leader_last_index)
        };
        let prev_log_term =
            node_term_at_log_or_snapshot_index(leader, prev_log_index).unwrap_or_default();
        let mut entries = Vec::new();
        let mut inflight_bytes = 0u64;
        // A raft log is in ascending index order, so everything this peer still needs is a
        // SUFFIX of it. Scanning from the front and filtering costs the whole log on every
        // AppendEntries to every peer, which makes a write more expensive the more history sits
        // in front of it -- appending one entry should cost the same at index 10 and 10,000.
        let first_unsent = leader
            .log
            .partition_point(|entry| entry.index <= prev_log_index);
        crate::durability_metrics::record_scan(
            "replication_entries_examined",
            (leader.log.len() - first_unsent) as u64,
        );
        for entry in leader.log[first_unsent..].iter() {
            if entries.len() as u64 >= available_entries {
                break;
            }
            let entry_bytes = command_size_bytes(&entry.command);
            if !entries.is_empty() && inflight_bytes.saturating_add(entry_bytes) > available_bytes {
                break;
            }
            if entries.is_empty()
                && entry_bytes > available_bytes
                && current_inflight_bytes > 0
                && !probe_only
            {
                if let Some(target) = inner.nodes.get_mut(&target_id) {
                    target.pipeline_state.append_rejected =
                        target.pipeline_state.append_rejected.saturating_add(1);
                    target.pipeline_state.memory_backpressure_rejections = target
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::AppendBackpressure {
                    node_id: target_id,
                    inflight_entries: current_inflight_entries,
                    inflight_bytes: current_inflight_bytes,
                    entry_bytes,
                    entry_limit,
                    byte_limit,
                });
            }
            inflight_bytes = inflight_bytes.saturating_add(entry_bytes);
            entries.push(entry.clone());
        }
        let inflight_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            target.pipeline_state.append_accepted =
                target.pipeline_state.append_accepted.saturating_add(1);
            target.pipeline_state.match_index = target.commit_index;
            target.pipeline_state.next_index = prev_log_index.saturating_add(1);
            target.pipeline_state.inflight_entries =
                current_inflight_entries.saturating_add(entries.len() as u64);
            target.pipeline_state.inflight_bytes =
                current_inflight_bytes.saturating_add(inflight_bytes);
            target.pipeline_state.append_queue_depth = target.pipeline_state.inflight_entries;
            target.pipeline_state.append_queue_max_depth = target
                .pipeline_state
                .append_queue_max_depth
                .max(target.pipeline_state.append_queue_depth);
            if enable_reorder_queue {
                target.pipeline_state.reorder_queue_depth =
                    target.commit_index.saturating_sub(target.applied_index);
            }
        }
        inner.persist_configured_wal()?;
        Ok(AppendEntriesRequest {
            rpc: None,
            shard_id,
            term: leader_term,
            leader_id,
