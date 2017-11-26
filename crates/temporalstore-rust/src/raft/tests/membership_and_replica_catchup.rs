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
