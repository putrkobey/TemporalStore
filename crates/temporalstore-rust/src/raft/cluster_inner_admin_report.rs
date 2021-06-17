// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftClusterInner::matrixraft_runtime_admin_report, split from raft.rs.
use super::*;

impl RaftClusterInner {
    pub(super) fn matrixraft_runtime_admin_report(&self) -> MatrixRaftRuntimeAdminReport {
        let status = self.status();
        let leader_log = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.log.as_slice())
            .unwrap_or_default();
        let peer_pipeline_states = self
            .nodes
            .values()
            .map(|node| {
                let mut pipeline = node.pipeline_state.clone();
                pipeline.snapshot_installing = pipeline.snapshot_installing
                    || self
                        .pending_snapshots
                        .keys()
                        .any(|(target_id, _)| *target_id == node.id);
                if pipeline.next_index == 0 {
                    pipeline.next_index = node_next_log_index(node);
                }
                if pipeline.inflight_entries == 0 && node.commit_index < status.commit_index {
                    pipeline.inflight_entries =
                        status.commit_index.saturating_sub(node.commit_index);
                    pipeline.inflight_bytes = leader_log
                        .iter()
                        .filter(|entry| entry.index > node.commit_index)
                        .map(|entry| command_size_bytes(&entry.command))
                        .sum();
                    pipeline.append_queue_depth = pipeline.inflight_entries;
                }
                if node.alive {
                    pipeline.offline_since_ms = None;
                    pipeline.offline_elapsed_ms = 0;
                    pipeline.offline_timeout_reached = false;
                } else if let Some(offline_since_ms) = pipeline.offline_since_ms {
                    pipeline.offline_elapsed_ms =
                        self.logical_time_ms.saturating_sub(offline_since_ms);
                    let reached = self.config.offline_timeout_tick > 0
                        && pipeline.offline_elapsed_ms >= self.config.offline_timeout_tick;
                    pipeline.offline_timeout_reached = reached;
                }
                if pipeline.transfer_leader_target {
                    if let Some(started_ms) = pipeline.transfer_leader_started_ms {
                        pipeline.transfer_leader_elapsed_ms =
                            self.logical_time_ms.saturating_sub(started_ms);
                    }
                } else {
                    pipeline.transfer_leader_elapsed_ms = 0;
                }
                MatrixRaftPeerPipelineState {
                    peer_id: node.id,
                    role: node.role,
                    replica_role: node.replica_role,
                    match_index: pipeline.match_index,
                    next_index: pipeline.next_index,
                    append_requests: pipeline.append_requests,
                    append_accepted: pipeline.append_accepted,
                    append_rejected: pipeline.append_rejected,
                    append_send_failures: pipeline.append_send_failures,
                    inflight_entries: pipeline.inflight_entries,
                    inflight_bytes: pipeline.inflight_bytes,
                    append_queue_depth: pipeline.append_queue_depth,
                    append_queue_limit: self.config.max_inflights_replicate,
                    inflight_bytes_limit: self.config.max_memory_replicate_log_bytes,
                    apply_inflight_tasks: pipeline.apply_inflight_tasks,
                    apply_inflight_limit: self.config.max_inflights_apply_task,
                    apply_queue_depth: pipeline.apply_queue_depth,
                    apply_queue_max_depth: pipeline.apply_queue_max_depth,
                    apply_batch_bytes_limit: self.config.max_apply_batch_bytes,
                    apply_backpressure_rejections: pipeline.apply_backpressure_rejections,
                    memory_backpressure_rejections: pipeline.memory_backpressure_rejections,
                    oversized_log_rejections: pipeline.oversized_log_rejections,
                    append_queue_max_depth: pipeline.append_queue_max_depth,
                    reorder_queue_depth: pipeline.reorder_queue_depth,
                    out_of_order_append_rejections: pipeline.out_of_order_append_rejections,
                    reorder_entries_accepted: pipeline.reorder_entries_accepted,
                    reorder_entries_released: pipeline.reorder_entries_released,
                    // The same quantity under their name; see the field's doc for where the two
                    // can disagree.
                    reorder_entries_converged: pipeline.reorder_entries_released,
                    reorder_entries_rejected: pipeline.reorder_entries_rejected,
                    reorder_entry_timeouts: pipeline.reorder_entry_timeouts,
                    reorder_dropped_packages: pipeline.reorder_dropped_packages,
                    stale_term_rejections: pipeline.stale_term_rejections,
                    snapshot_sending: pipeline.snapshot_sending,
                    snapshot_installing: pipeline.snapshot_installing,
                    snapshot_installed_index: pipeline.snapshot_installed_index,
                    snapshot_send_attempts: pipeline.snapshot_send_attempts,
                    snapshot_send_completed: pipeline.snapshot_send_completed,
                    snapshot_send_failed: pipeline.snapshot_send_failed,
                    snapshot_install_started: pipeline.snapshot_install_started,
                    snapshot_install_completed: pipeline.snapshot_install_completed,
                    snapshot_install_rejected: pipeline.snapshot_install_rejected,
                    snapshot_install_rolled_back: pipeline.snapshot_install_rolled_back,
                    snapshot_install_received_chunks: pipeline.snapshot_install_received_chunks,
                    snapshot_install_total_chunks: pipeline.snapshot_install_total_chunks,
                    snapshot_install_progress_per_mille: pipeline
                        .snapshot_install_progress_per_mille,
                    snapshot_retry_count: pipeline.snapshot_retry_count,
                    snapshot_chunk_retry_count: pipeline.snapshot_chunk_retry_count,
                    snapshot_backpressure_rejections: pipeline.snapshot_backpressure_rejections,
                    snapshot_rate_limit_rejections: pipeline.snapshot_rate_limit_rejections,
                    snapshot_send_elapsed_ms: pipeline.snapshot_send_elapsed_ms,
                    snapshot_send_timeouts: pipeline.snapshot_send_timeouts,
                    snapshot_during_membership_change: pipeline.snapshot_during_membership_change,
                    snapshot_rejoin_after_compacted_log: pipeline
                        .snapshot_rejoin_after_compacted_log,
                    transfer_leader_target: pipeline.transfer_leader_target,
                    transfer_leader_requests: pipeline.transfer_leader_requests,
                    transfer_leader_accepted: pipeline.transfer_leader_accepted,
                    transfer_leader_rejected: pipeline.transfer_leader_rejected,
                    transfer_leader_completed: pipeline.transfer_leader_completed,
                    transfer_leader_elapsed_ms: pipeline.transfer_leader_elapsed_ms,
                    transfer_leader_timeouts: pipeline.transfer_leader_timeouts,
                    pre_vote_rejections: pipeline.pre_vote_rejections,
                    election_rejections: pipeline.election_rejections,
                    offline_elapsed_ms: pipeline.offline_elapsed_ms,
                    offline_timeout_reached: pipeline.offline_timeout_reached,
                    offline_timeout_rejections: pipeline.offline_timeout_rejections,
                    auto_promoted_from_learner: pipeline.auto_promoted_from_learner,
                }
            })
            .collect::<Vec<_>>();

        let stale_follower_read_rejected = status.nodes.iter().any(|node| {
            node.node_id != self.leader_id
                && node.replica_role.can_serve_data()
                && node.lag > 0
                && node.alive
        }) || self.read_safety_state.read_index_rejected > 0;
        let stale_follower_write_rejected = status
            .nodes
            .iter()
            .any(|node| node.node_id != self.leader_id && node.alive)
            && self.read_safety_state.stale_follower_write_rejected > 0;
        let stale_leader_lease_rejected = self.read_safety_state.stale_leader_lease_rejected > 0;
        let lagging_follower_read_rejected =
            self.read_safety_state.lagging_follower_read_rejected > 0;
        let bounded_stale_read_accepted = self.read_safety_state.bounded_stale_read_accepted > 0;
        let bounded_stale_read_rejected = self.read_safety_state.bounded_stale_read_rejected > 0;
        let minority_partition_rejected_reads =
            self.read_safety_state.minority_partition_read_rejected > 0;
        let minority_partition_rejected_writes =
            self.read_safety_state.minority_partition_write_rejected > 0;
        let healed_follower_caught_up = self.read_safety_state.healed_follower_catchup_observed > 0;
        let witness_membership_present = self
            .nodes
            .values()
            .any(|node| matches!(node.replica_role, RaftReplicaRole::Witness));
        let witness_role_behavior_present = self.nodes.values().any(|node| {
            matches!(node.replica_role, RaftReplicaRole::Witness)
                && node.replica_role.participates_in_quorum()
                && !node.replica_role.can_serve_data()
                && !node.replica_role.can_be_leader()
        });
        let learner_auto_promote_present = self
            .nodes
            .values()
            .any(|node| node.pipeline_state.auto_promoted_from_learner);
        let pending_joint_consensus_present = self.joint_membership.is_some();
        let membership_evidence = self.membership_evidence.clone();
        let learner_add_present = membership_evidence.learner_add_count > 0;
        let learner_catchup_present = membership_evidence.learner_catchup_count > 0;
        let learner_promote_present = membership_evidence.learner_promote_count > 0;
        let voter_remove_present = membership_evidence.voter_remove_count > 0;
        let unique_leader_transfer_commit_ids = membership_evidence
            .leader_transfer_exact_once_commit_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let leader_transfer_exact_once_present = membership_evidence.leader_transfer_write_count
            > 0
            && membership_evidence.leader_transfer_exact_once_commit_count
                >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids
                == membership_evidence
                    .leader_transfer_exact_once_commit_ids
                    .len() as u64;
        let pending_joint_consensus_restart_present =
            membership_evidence.pending_joint_consensus_persist_count > 0
                && membership_evidence.pending_joint_consensus_restore_count > 0;
        let read_index_validated = status.leader_lease_valid && status.has_majority;
        let lease_read_validated =
            self.config.lease_duration_ms > 0 || self.config.assume_lease_when_start;
        let matrixraft_peer_pipeline_states = peer_pipeline_states
            .iter()
            .map(MatrixRaftPeerPipelineState::to_matrixraft_peer_pipeline_status)
            .collect::<Vec<_>>();
        let matrixraft_pipeline_evidence = matrixraft_pipeline_evidence(
            &matrixraft_peer_pipeline_states,
            MatrixRaftPipelineLimits {
                max_inflights_replicate: self.config.max_inflights_replicate,
                max_memory_replicate_log_bytes: self.config.max_memory_replicate_log_bytes,
                max_inflights_apply_task: self.config.max_inflights_apply_task,
                max_apply_batch_bytes: self.config.max_apply_batch_bytes,
                enable_reorder_queue: self.config.enable_reorder_queue,
                reorder_window_size: self.config.reorder_window_size,
                reorder_timeout_us: self.config.reorder_timeout_us,
            },
        );
        let matrixraft_snapshot_evidence = matrixraft_snapshot_lifecycle_evidence(
            &matrixraft_peer_pipeline_states,
            self.config.send_snapshot_timeout_ms,
            self.config.max_inflights_replicate,
        );
        let append_backpressure_enforced =
            matrixraft_pipeline_evidence.append_backpressure_enforced;
        let apply_backpressure_enforced = matrixraft_pipeline_evidence.apply_backpressure_enforced;
        let memory_replicate_bytes_enforced =
            matrixraft_pipeline_evidence.memory_replicate_bytes_enforced;
        let oversized_log_rejection_present =
            matrixraft_pipeline_evidence.oversized_log_rejection_present;
        let out_of_order_append_handling_present =
            matrixraft_pipeline_evidence.out_of_order_append_handling_present;
        let reorder_timeout_drop_present =
            matrixraft_pipeline_evidence.reorder_timeout_drop_present;
        let stale_term_rejection_present =
            matrixraft_pipeline_evidence.stale_term_rejection_present;
        let reorder_queue_enabled = matrixraft_pipeline_evidence.reorder_queue_enabled;
        let snapshot_sender_lifecycle_present =
            matrixraft_snapshot_evidence.sender_lifecycle_present;
        let snapshot_downloader_lifecycle_present =
            matrixraft_snapshot_evidence.downloader_lifecycle_present;
        let snapshot_retry_backpressure_present =
            matrixraft_snapshot_evidence.retry_backpressure_present;
        let snapshot_chunk_retry_present = matrixraft_snapshot_evidence.chunk_retry_present;
        let snapshot_send_timeout_present = matrixraft_snapshot_evidence.send_timeout_present;
        let snapshot_rate_limit_present = matrixraft_snapshot_evidence.rate_limit_present;
        let snapshot_install_progress_present =
            matrixraft_snapshot_evidence.install_progress_present;
        let snapshot_install_rollback_present =
            matrixraft_snapshot_evidence.install_rollback_present;
        let snapshot_membership_change_present =
            matrixraft_snapshot_evidence.membership_change_present;
        let snapshot_rejoin_after_compacted_log_present =
            matrixraft_snapshot_evidence.rejoin_after_compacted_log_present;
        let (
            wal_segment_count,
            wal_active_segment_id,
            wal_first_retained_segment_id,
            wal_last_retained_segment_id,
            wal_total_bytes,
            wal_active_segment_bytes,
            wal_total_records,
            wal_first_sequence,
            wal_last_sequence,
