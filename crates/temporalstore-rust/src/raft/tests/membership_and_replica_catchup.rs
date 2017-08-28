// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn raft_config_matches_defaults_and_validates_required_limits() {
    let config = RaftConfig::default();
    assert_eq!(config.election_cycle_tick, 3);
    assert_eq!(config.max_apply_batch_bytes, 64 * 1024);
    assert_eq!(config.max_cache_memory_bytes, 32 * 1024 * 1024);
    assert_eq!(config.raft_transport_timeout_ms, 1_000);
    assert_eq!(config.max_segment_bytes, 64 * 1024 * 1024);
    assert!(!config.wal_sync);
    assert!(config.assume_lease_when_start);
    assert!(config.can_trigger_snapshot);
    assert!(config.validate().is_ok());

    let mut invalid = config;
    invalid.max_memory_replicate_log_bytes = 0;
    assert_eq!(
        invalid.validate(),
        Err(RaftConfigError::InvalidValue(
            "max_memory_replicate_log_bytes"
        ))
    );
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_election_controls
#[test]
fn raft_config_rejects_oversized_log_entries_and_prohibited_elections() {
    let mut config = RaftConfig {
        max_memory_replicate_log_bytes: 16,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config.clone()).unwrap();
    let err = cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: vec![b'x'; 128],
        })
        .unwrap_err();
    assert!(matches!(err, RaftError::LogEntryTooLarge { .. }));

    config.max_memory_replicate_log_bytes = 1024;
    config.prohibits_election = true;
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
    assert_eq!(cluster.elect_leader(2), Err(RaftError::ElectionProhibited));
}

#[test]
fn raft_chunks_large_sequence_add_under_default_entry_limit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let rows = long_sequence_rows(5_000);

    cluster
        .propose(Command::SequenceAdd {
            key: "long-sequence".to_string(),
            rows,
        })
        .unwrap();

    assert!(cluster.commit_index(1).unwrap() > 1);
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                2,
                Command::SequenceQuery {
                    key: "long-sequence".to_string(),
                    start_ms: 1_700_000_000_000,
                    end_ms: 1_700_000_999_999,
                    count: 5_000,
                    filters: vec![FeatureFilter {
                        field: "action_type".to_string(),
                        op: FeatureFilterOp::GreaterThan,
                        value: 2,
                    }],
                },
            )
            .unwrap(),
        CommandResponse::SequenceRows {
            rows: long_sequence_rows(5_000)
                .into_iter()
                .filter(|row| row.action_type > 2)
                .collect()
        }
    );
}

#[test]
fn distributed_raft_chunks_large_sequence_add_under_default_entry_limit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let rows = long_sequence_rows(5_000);

    cluster
        .propose_distributed(
            Command::SequenceAdd {
                key: "distributed-long-sequence".to_string(),
                rows,
            },
            &cluster,
        )
        .unwrap();

    assert!(cluster.commit_index(1).unwrap() > 1);
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::SequenceQuery {
                    key: "distributed-long-sequence".to_string(),
                    start_ms: 1_700_000_000_000,
                    end_ms: 1_700_000_999_999,
                    count: 5_000,
                    filters: vec![FeatureFilter {
                        field: "duration".to_string(),
                        op: FeatureFilterOp::LessThan,
                        value: 10,
                    }],
                },
            )
            .unwrap(),
        CommandResponse::SequenceRows {
            rows: long_sequence_rows(5_000)
                .into_iter()
                .filter(|row| row.duration < 10)
                .collect()
        }
    );
}

#[test]
fn raft_read_options_enforce_leader_and_follower_read_paths() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    assert_eq!(
        cluster.check_read(2, RaftReadOptions::default()),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert!(cluster
        .check_read(
            2,
            RaftReadOptions {
                enable_read_from_follower: true,
                strategy: RaftReadStrategy::ReadIndex,
                ..RaftReadOptions::default()
            },
        )
        .is_ok());
    assert!(cluster
        .check_read(
            1,
            RaftReadOptions {
                strategy: RaftReadStrategy::LeaseRead,
                ..RaftReadOptions::default()
            },
        )
        .is_ok());
}

#[test]
fn data_raft_read_policy_matches_partition_manager_modes() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    assert_eq!("leader".parse(), Ok(DataRaftReadMode::Leader));
    assert_eq!("linearizable".parse(), Ok(DataRaftReadMode::Linearizable));
    assert_eq!("bounded_stale".parse(), Ok(DataRaftReadMode::BoundedStale));
    assert_eq!(
        "unsafe_any_replica".parse(),
        Ok(DataRaftReadMode::UnsafeAnyReplica)
    );
    assert!("bad-mode".parse::<DataRaftReadMode>().is_err());

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "policy".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert_eq!(
        cluster.check_data_raft_read_policy(2, DataRaftReadPolicy::default()),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert_eq!(
        cluster.check_data_raft_read_policy(
            2,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::Linearizable,
                ..DataRaftReadPolicy::default()
            },
        ),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert!(cluster
        .check_data_raft_read_policy(
            1,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::Linearizable,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
    assert_eq!(
        cluster.check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 0,
                ..DataRaftReadPolicy::default()
            },
        ),
        Err(RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        })
    );
    assert!(cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 1,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
    assert!(cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::UnsafeAnyReplica,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
}

// shared-corpus: raft_matrixraft_read_safety_policy
// shared-corpus: raft_matrixraft_read_lease_fault_matrix
#[test]
fn raft_read_index_and_transfer_reject_lagging_replica() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert_eq!(
        cluster.read_index(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );
    assert_eq!(
        cluster.transfer_leader(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );
    cluster.catch_up(3).unwrap();
    assert!(cluster.read_index(3).is_ok());
    assert!(cluster.transfer_leader(3).is_ok());
}

#[test]
fn raft_wait_for_applied_index_matches_backend_contract() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "wait-applied".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert!(cluster.wait_for_applied_index(1, 1, 0).is_ok());
    assert_eq!(
        cluster.wait_for_applied_index(3, 1, 1),
        Err(RaftError::AppliedIndexTimeout {
            node_id: 3,
            applied_index: 0,
            target_index: 1,
            timeout_ms: 1,
        })
    );

    cluster.catch_up(3).unwrap();
    assert!(cluster.wait_for_applied_index(3, 1, 0).is_ok());
}

#[test]
fn raft_apply_health_reports_commit_to_apply_lag() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "apply-health".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    {
        let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
        let node = inner.nodes.get_mut(&2).unwrap();
        node.applied_index = 0;
        node.applied.clear();
    }

    let health = cluster.apply_health(0);
    assert!(!health.healthy);
    assert_eq!(health.leader_commit_index, 1);
    assert_eq!(health.max_apply_lag, 1);
    assert_eq!(health.fully_applied_nodes, vec![1, 3]);
    assert_eq!(
        health.slow_appliers,
        vec![RaftApplyLag {
            node_id: 2,
            commit_index: 1,
            applied_index: 0,
            apply_lag: 1,
            alive: true,
        }]
    );
    assert!(cluster.prometheus_metrics().contains(
        "temporalstore_raft_node_apply_lag{kind=\"data\",node_id=\"2\",role=\"follower\",replica_role=\"voter\"} 1"
    ));

    cluster.catch_up(2).unwrap();
    let healthy = cluster.apply_health(0);
    assert!(healthy.healthy);
    assert_eq!(healthy.max_apply_lag, 0);
}

#[test]
fn raft_apply_health_excludes_dead_replicas_behind_leader_commit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "before-dead".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "after-dead".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    let health = cluster.apply_health(0);
    assert!(health.healthy);
    assert_eq!(health.leader_commit_index, 2);
    assert_eq!(health.max_apply_lag, 0);
    assert_eq!(health.fully_applied_nodes, vec![1, 2]);
    assert!(health.slow_appliers.is_empty());
}

#[test]
fn secondary_is_promoted_automatically_when_primary_is_down() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(1, false).unwrap();
    let promoted = cluster.promote_if_leader_down().unwrap();
    assert_eq!(promoted, 2);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"after-promotion".to_vec(),
        })
        .unwrap();
    let response = cluster
        .read_local(
            2,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"after-promotion".to_vec())
        }
    );
}

#[test]
fn scale_up_adds_caught_up_replica() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"before-scale-up".to_vec(),
        })
        .unwrap();
    cluster.add_node(4).unwrap();
    assert_eq!(cluster.commit_index(4).unwrap(), 1);
    let response = cluster
        .read_local(
            4,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"before-scale-up".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn learner_and_witness_roles_match_membership_shape() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();

    let status = cluster.status();
    assert_eq!(status.majority, 3);
    assert_eq!(status.live_voters, 4);
    assert_eq!(
        cluster.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Learner
    );
    assert_eq!(
        cluster.local_status(5).unwrap().replica_role,
        RaftReplicaRole::Witness
    );
    assert_eq!(cluster.membership().voters, vec![1, 2, 3, 5]);

    cluster
        .propose(Command::StringSet {
            key: "role-k".to_string(),
            value: b"role-v".to_vec(),
        })
        .unwrap();
    assert_eq!(
        cluster.read_from_replica(
            4,
            Command::StringGet {
                key: "role-k".to_string()
            }
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"role-v".to_vec())
        })
    );
    assert_eq!(
        cluster.read_from_replica(
            5,
            Command::StringGet {
                key: "role-k".to_string()
            }
        ),
        Err(RaftError::NodeNotFound(5))
    );
    assert!(matches!(
        cluster.elect_leader(4),
        Err(RaftError::NodeNotFound(4))
    ));
    assert!(matches!(
        cluster.elect_leader(5),
        Err(RaftError::NodeNotFound(5))
    ));
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn learner_does_not_count_for_majority_but_witness_does() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2]);
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster.set_alive(2, false).unwrap();
    assert_eq!(
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    );

    cluster.set_alive(2, true).unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();
    cluster.set_alive(2, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.status().live_voters, 2);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn replica_roles_survive_wal_restore() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3, 4, 5],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(
        restored.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Learner
    );
    assert_eq!(
        restored.local_status(5).unwrap().replica_role,
        RaftReplicaRole::Witness
    );
    assert_eq!(restored.membership().voters, vec![1, 2, 3, 5]);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn matrixraft_admin_reports_witness_auto_promote_and_pending_joint_consensus() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();
    cluster.add_learner_with_auto_promote(4, true).unwrap();
    cluster.add_node(6).unwrap();
    cluster.remove_node(6).unwrap();
    cluster.begin_leader_transfer(2).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "membership-transfer-exact-once".to_string(),
            value: b"committed-once".to_vec(),
        })
        .unwrap();
    cluster.begin_joint_consensus([1, 2, 3, 4, 5]).unwrap();
    let cluster = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3, 4, 5],
        RaftConfig {
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();

    let admin = cluster.matrixraft_runtime_admin_report();
    assert!(admin.witness_membership_present);
    assert!(admin.witness_role_behavior_present);
    assert!(admin.learner_add_present);
    assert!(admin.learner_catchup_present);
    assert!(admin.learner_promote_present);
    assert!(admin.voter_remove_present);
    assert!(admin.learner_auto_promote_present);
    assert!(admin.leader_transfer_exact_once_present);
    assert!(admin.pending_joint_consensus_present);
    assert!(admin.pending_joint_consensus_restart_present);
    assert_eq!(admin.membership_evidence.learner_add_count, 1);
    assert_eq!(admin.membership_evidence.learner_promote_count, 1);
    assert_eq!(admin.membership_evidence.voter_remove_count, 1);
    assert_eq!(admin.membership_evidence.leader_transfer_write_count, 1);
    assert_eq!(
        admin
            .membership_evidence
            .leader_transfer_exact_once_commit_count,
        1
    );
    assert_eq!(
        admin
            .membership_evidence
            .leader_transfer_exact_once_commit_ids,
