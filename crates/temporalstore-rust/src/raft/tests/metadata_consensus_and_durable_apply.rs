// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 4, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn a_proxy_group_needs_a_name_on_the_raft_path_too() {
    use crate::meta::{PutProxyGroupRequest, SingleNodeMeta};

    // put_proxy_group validates in the public method, and the propose path
    // dispatches straight to apply_put_proxy_group, which does not. So a raft
    // metaserver committed a group with no name and no namespace into
    // replicated metadata, where the single-node one answered bad_request.
    //
    // Judged before proposing, never while applying: replay has to reapply what
    // was already accepted.
    let empty = || PutProxyGroupRequest {
        drop_percent: 0,
        group: String::new(),
        namespace: String::new(),
        location: "rack-1".to_string(),
        instance_num: 1,
    };

    let single = SingleNodeMeta::default();
    let single_ack = single.put_proxy_group(empty());
    assert_eq!(single_ack.status.code, "bad_request");
    assert!(single.list_proxy_groups().groups.is_empty());

    let meta = MetaRaftCluster::new([10, 11, 12]);
    let raft_ack = meta.put_proxy_group(empty());
    assert_eq!(
        raft_ack.status.code, "bad_request",
        "the raft path accepted a proxy group with no name"
    );
    assert!(
        meta.list_proxy_groups().groups.is_empty(),
        "a nameless proxy group reached replicated metadata"
    );

    // A named group still goes through, so the guard is not simply refusing.
    let good = meta.put_proxy_group(PutProxyGroupRequest {
        drop_percent: 0,
        group: "orders".to_string(),
        namespace: "ns".to_string(),
        location: "rack-1".to_string(),
        instance_num: 1,
    });
    assert!(good.status.ok, "a valid group was refused: {good:?}");
    assert_eq!(meta.list_proxy_groups().groups.len(), 1);
}

#[test]
fn add_node_after_leader_snapshot_installs_snapshot_and_tail() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshotted-key".to_string(),
            value: b"base".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 1);
    cluster
        .propose(Command::StringSet {
            key: "tail-key".to_string(),
            value: b"tail".to_vec(),
        })
        .unwrap();

    cluster.add_node(4).unwrap();
    assert_eq!(cluster.commit_index(4).unwrap(), 2);
    assert_eq!(cluster.local_status(4).unwrap().last_log_index, 2);
    assert_eq!(
        cluster
            .read_from_replica(
                4,
                Command::StringGet {
                    key: "snapshotted-key".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"base".to_vec())
        }
    );
    assert_eq!(
        cluster
            .read_from_replica(
                4,
                Command::StringGet {
                    key: "tail-key".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"tail".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_leader_transfer_high_write_fault_harness
#[test]
fn matrixraft_leader_transfer_under_high_write_load_commits_once() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            transfer_timeout_tick: 50,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    let mut accepted = Vec::new();

    for index in 0..24 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("before-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    cluster.begin_leader_transfer(2).unwrap();
    for index in 24..48 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("during-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    cluster.transfer_leader(2).unwrap();
    assert_eq!(cluster.leader_id(), 2);

    for index in 48..72 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("after-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    let mut commit_indexes = accepted
        .iter()
        .map(|(_, _, index)| *index)
        .collect::<Vec<_>>();
    commit_indexes.sort_unstable();
    assert_eq!(
        commit_indexes,
        (1..=accepted.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(cluster.status().commit_index, accepted.len() as u64);
    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.commit_index(replica_id).unwrap(),
            accepted.len() as u64
        );
    }

    for replica_id in [1, 2, 3] {
        for (key, value, _) in &accepted {
            assert_eq!(
                cluster
                    .read_from_replica(replica_id, Command::StringGet { key: key.clone() },)
                    .unwrap(),
                CommandResponse::Bytes {
                    value: Some(value.clone())
                }
            );
        }
    }

    let admin = cluster.matrixraft_runtime_admin_report();
    let transferred_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 2)
        .expect("new leader peer status");
    assert_eq!(transferred_peer.transfer_leader_requests, 2);
    assert_eq!(transferred_peer.transfer_leader_accepted, 2);
    assert_eq!(transferred_peer.transfer_leader_completed, 1);
    assert_eq!(transferred_peer.transfer_leader_rejected, 0);
    assert_eq!(transferred_peer.transfer_leader_timeouts, 0);
    assert!(admin.admin_status_surface_complete);
    assert!(admin.wal_segment_lifecycle_present);
    assert!(admin.wal_last_log_index >= admin.commit_index);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "admin_status_surface" && item.ready));
}

// shared-corpus: raft_matrixraft_admin_status_surface
#[test]
fn matrixraft_admin_status_surface_requires_wal_and_peer_pipeline_fields() {
    let local_fixture = RaftCluster::new_single_shard(1, [1, 2, 3]);
    local_fixture
        .propose(Command::StringSet {
            key: "admin-without-wal".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let local_admin = local_fixture.matrixraft_runtime_admin_report();
    assert!(!local_admin.admin_status_surface_complete);
    assert!(!local_admin.wal_segment_lifecycle_present);
    assert!(local_admin.quorum_peer_progress_observed);
    assert!(local_admin.peer_pipeline_runtime_activity_observed);
    assert!(local_admin.peer_pipeline_limits_observed);
    assert!(local_admin
        .blockers
        .contains(&"admin_status_surface_incomplete".to_string()));

    let dir = tempfile::tempdir().unwrap();
    let durable =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    durable
        .propose(Command::StringSet {
            key: "admin-with-wal".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let durable_admin = durable.matrixraft_runtime_admin_report();
    assert!(durable_admin.wal_segment_lifecycle_present);
    assert!(durable_admin.quorum_peer_progress_observed);
    assert!(durable_admin.peer_pipeline_runtime_activity_observed);
    assert!(durable_admin.peer_pipeline_limits_observed);
    assert!(durable_admin.admin_status_surface_complete);
    assert!(durable_admin.wal_first_log_index > 0);
    assert!(durable_admin.wal_last_log_index >= durable_admin.commit_index);
    assert!(durable_admin.peer_pipeline_states.iter().all(|peer| {
        peer.next_index > 0
            && peer.append_queue_limit > 0
            && peer.inflight_bytes_limit > 0
            && peer.apply_inflight_limit > 0
            && peer.apply_batch_bytes_limit > 0
    }));
    assert!(durable_admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "admin_status_surface" && item.ready));
}

#[test]
fn append_entries_ignores_entries_at_or_below_snapshot_floor() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted".to_string(),
            value: b"snapshot".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted-tail".to_string(),
            value: b"before".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 2);

    let request = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 2,
        prev_log_term: 1,
        entries: vec![
            RaftLogEntry {
                leader_time_ms: 0,
                term: 1,
                index: 2,
                shard_id: 1,
                command: Command::StringSet {
                    key: "compacted-tail".to_string(),
                    value: b"stale-should-not-replay".to_vec(),
                },
            },
            RaftLogEntry {
                leader_time_ms: 0,
                term: 1,
                index: 3,
                shard_id: 1,
                command: Command::StringSet {
                    key: "compacted-tail".to_string(),
                    value: b"after".to_vec(),
                },
            },
        ],
        leader_commit: 3,
    };
    let response = cluster.receive_append_entries(request).unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 3);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "compacted-tail".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"after".to_vec())
        }
    );
}

#[test]
fn data_raft_snapshot_trigger_compacts_applied_log_bytes() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "trigger-data-snapshot".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(report.triggered);
    assert_eq!(report.reason, "applied_log_bytes_threshold");
    assert_eq!(report.applied_index, 1);
    assert!(report.applied_log_bytes >= 1);
    assert_eq!(cluster.local_status(1).unwrap().last_log_index, 1);

    let second = cluster.maybe_trigger_snapshot().unwrap();
    assert!(!second.triggered);
    assert_eq!(second.reason, "no_new_applied_logs");
    assert_eq!(second.last_snapshot_index, 1);
}

#[test]
fn raft_snapshot_cannot_overwrite_newer_data_state() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    assert_eq!(
        cluster.install_snapshot(2, snapshot).unwrap_err(),
        RaftError::StaleSnapshot {
            snapshot_index: 1,
            local_commit_index: 2,
        }
    );
}

#[test]
fn raft_election_does_not_depend_on_snapshot_availability() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "before".to_string(),
            value: b"leader-local".to_vec(),
        })
        .unwrap();
    cluster.set_alive(1, false).unwrap();
    assert_eq!(cluster.promote_if_leader_down().unwrap(), 2);
    cluster
        .propose(Command::StringSet {
            key: "after".to_string(),
            value: b"new-leader".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.commit_index(2).unwrap(), 2);
}

#[test]
fn metaserver_raft_replicates_shard_location_metadata() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let location = ShardLocation {
        registered_at_ms: 0,
        preferred_location: String::new(),
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 1,
        server_addr: "127.0.0.1:17002".to_string(),
        latest_snapshot: None,
    };
    meta.propose(MetaCommand::PutShardLocation(location.clone()))
        .unwrap();
    for node_id in [10, 11, 12] {
        assert_eq!(
            meta.get_shard_location(node_id, 1).unwrap(),
            Some(location.clone())
        );
    }
}

#[test]
fn metaserver_raft_replays_scheduler_state_and_partition_set_topology() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let mut scheduler = DeterministicTaskScheduler::default();
    let task = scheduler.submit(
        0,
        100,
        SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
            shard_id: 42,
            replica_id: 2,
            node_id: 11,
            load_version: 9,
        }),
    );
    scheduler
        .run_next(
            200,
            SchedulerTaskResult::RetryLater,
            TaskSchedulerOptions::default(),
        )
        .unwrap();
    let topology = PartitionSetTopology::from_replicas(
        "ns",
        "tbl",
        7,
        700,
        42,
        12,
        5,
        3,
        &BTreeMap::from([
            (10, "127.0.0.1:18010".to_string()),
            (11, "127.0.0.1:18011".to_string()),
        ]),
        [
            ShardReplica {
                shard_id: 42,
                replica_id: 1,
                node_id: 10,
                role: ShardRole::Primary,
                state: ShardReplicaState::Normal,
                load_version: 8,
            },
            ShardReplica {
                shard_id: 42,
                replica_id: 2,
                node_id: 11,
                role: ShardRole::Secondary,
                state: ShardReplicaState::Normal,
                load_version: 9,
            },
        ],
    )
    .unwrap();
    let persisted = RaftPersistedSchedulerState {
        scheduler: scheduler.export_snapshot(),
        topology,
        executions: vec![NetworkSchedulerTaskExecution {
            task_id: task.id,
            target_node_id: 11,
            target_addr: "127.0.0.1:18011".to_string(),
            result: SchedulerTaskResult::RetryLater,
            retry_times: 1,
            next_run_time_ms: Some(1200),
            status: Status::error("node_request_failed", "timeout"),
            lifecycle_token: task.lifecycle_token(),
        }],
        raft_generation: 44,
    };

    meta.propose(MetaCommand::PersistSchedulerState(persisted.clone()))
        .unwrap();
    let snapshot = meta.create_snapshot().unwrap();
    assert_eq!(snapshot.state.scheduler_state.as_ref().unwrap(), &persisted);
    for node_id in [10, 11, 12] {
        assert_eq!(
            meta.status()
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .unwrap()
                .commit_index,
            snapshot.last_included_index
        );
    }
}

#[test]
fn metaserver_raft_replicates_full_metadata_mutation_api() {
    let meta = MetaRaftCluster::new([10, 11, 12]);

    assert!(
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "az-a".to_string(),
            binary_version: "test".to_string(),
        })
        .status
        .ok
    );
    assert!(
        meta.add_namespace(AddNamespaceRequest {
            namespace: "feature".to_string(),
        })
        .status
        .ok
    );
    assert!(
        meta.add_table(AddTableRequest {
            namespace: "feature".to_string(),
            table_name: "user_seq".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        })
        .status
        .ok
    );
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 100,
            server_addr: "server-a".to_string(),
        })
        .status
        .ok
    );

    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 4);
    }

    let topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(topology.status.ok);
    assert_eq!(topology.table.unwrap().shard_count, 2);
    assert_eq!(topology.shards.len(), 2);
    assert_eq!(topology.shards[0].primary.as_deref(), Some("server-a"));

    // A replica-count change, because shard_count is pinned once a shard is
    // registered against the table -- and this test is about UpdateTable
    // reaching every peer, which either change demonstrates.
    let updated = meta.update_table(UpdateTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        shard_count: None,
        replica_count: Some(2),
        first_shard_id: None,
        partition_version: None,
        serving_options: None,
    });
    assert!(updated.status.ok, "{updated:?}");
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 5);
    }
    let updated_topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(updated_topology.status.ok, "{updated_topology:?}");
    let updated_table = updated_topology.table.unwrap();
    assert_eq!(updated_table.shard_count, 2);
    assert_eq!(updated_table.replica_count, 2);
    assert_eq!(updated_topology.shards.len(), 2);

    let duplicate = meta.add_table(AddTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        first_shard_id: 100,
        shard_count: 2,
        replica_count: 1,
        partition_version: 0,
        serving_options: crate::meta::TableServingOptions::default(),
    });
    assert!(!duplicate.status.ok);
    assert_eq!(duplicate.status.code, "already_exists");

    let deleted = meta.delete_table(DeleteTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
    });
    assert!(deleted.status.ok, "{deleted:?}");
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 7);
    }
    let dropped_topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert_eq!(dropped_topology.status.code, "table_not_found");
    assert_eq!(
        dropped_topology.table.unwrap().state,
        MetaEntityState::Dropped
    );
}

#[test]
fn metaserver_raft_freeze_stale_server_is_replicated_mutation() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: "server-stale".to_string(),
        node_id: 1,
        location: "az-a".to_string(),
        binary_version: "test".to_string(),
    });
    thread::sleep(Duration::from_millis(2));

    let report = meta.freeze_stale_servers(0);
    assert!(report.status.ok);
    assert_eq!(report.frozen_servers, vec!["server-stale".to_string()]);

    let servers = meta.list_servers();
    assert!(servers.status.ok);
    assert_eq!(servers.servers[0].state, MetaEntityState::Frozen);
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 2);
    }
}

#[test]
fn production_meta_raft_runtime_ticks_failover_and_failure_detection() {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:17101".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:17102".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:17103".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 5,
        election_tick_ms: 2,
        failure_detector_interval_ms: 5,
        stale_server_after_ms: 1,
    })
    .unwrap();
    assert!(runtime.validate_ready().is_ok());
    assert!(
        runtime
            .cluster()
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "server-stale".to_string(),
                node_id: 1,
                location: "az-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
    );
    runtime.cluster().set_alive(10, false).unwrap();
    let timer = runtime.start_timer_loop();
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        let status = runtime.status();
        let servers = runtime.cluster().list_servers();
        let server_frozen = servers
            .servers
            .first()
            .map(|server| server.state == MetaEntityState::Frozen)
            .unwrap_or(false);
        if status.leader_id != 10 && server_frozen {
            timer.stop();
            assert!(status.has_majority);
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    timer.stop();
    panic!("production metaserver raft runtime did not fail over and freeze stale server");
}

#[test]
fn production_meta_raft_runtime_matches_multinode_control_and_fault_contract() {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:17101".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:17102".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:17103".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 5,
        election_tick_ms: 2,
        failure_detector_interval_ms: 5,
        stale_server_after_ms: 1_000,
    })
    .unwrap();

    assert_eq!(runtime.list_membership(), vec![10, 11, 12]);
    assert!(runtime.validate_ready().is_ok());
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-control".to_string(),
            },
        )))
        .unwrap();
    assert_eq!(runtime.wait_for_log_applied().unwrap().read_index, 1);

    let add_report = runtime.add_node(13, RaftReplicaRole::Voter).unwrap();
    assert_eq!(add_report.live_voters, 4);
    assert_eq!(runtime.list_membership(), vec![10, 11, 12, 13]);
    assert!(matches!(
        runtime.add_node(14, RaftReplicaRole::Learner),
        Err(RaftError::InvalidConfig(message)) if message.contains("voter membership only")
    ));

    let membership = runtime.apply_membership([10, 11, 13]).unwrap();
    assert_eq!(membership.committed_membership.voters, vec![10, 11, 13]);
    assert_eq!(runtime.list_membership(), vec![10, 11, 13]);

    let snapshot = runtime.trigger_snapshot().unwrap();
    assert!(snapshot.last_included_index >= 1);
    runtime.transfer_leader(11).unwrap();
    assert_eq!(runtime.status().leader_id, 11);
    assert_eq!(runtime.read_index(10).unwrap().leader_id, 11);

    runtime.cluster().set_alive(11, false).unwrap();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-after-failover".to_string(),
            },
        )))
        .unwrap();
    let status = runtime.status();
    assert_ne!(status.leader_id, 11);
    assert!(status.has_majority);
    assert!(runtime
        .cluster()
        .list_namespaces()
        .namespaces
        .iter()
        .any(|namespace| namespace.namespace == "meta-raft-after-failover"));
}

#[test]
fn metaserver_owns_data_raft_membership_workflow() {
    let meta = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:19110".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:19111".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:19112".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    let data = RaftCluster::new_single_shard(7, [1, 2, 3]);
    data.propose(Command::StringSet {
        key: "membership-workflow".to_string(),
        value: b"before".to_vec(),
    })
    .unwrap();

    let report = meta
        .drive_data_raft_membership_workflow(&data, 4, Some(4), Some(1))
        .unwrap();
    assert_eq!(report.shard_id, 7);
    assert_eq!(report.learner_id, 4);
    assert_eq!(report.removed_voter_id, Some(1));
    assert_eq!(report.requested_leader_id, Some(4));
    assert_eq!(report.initial_voters, vec![1, 2, 3]);
    assert!(report.learner_added);
    assert!(report.catch_up_verified);
    assert_eq!(
        report.learner_catch_up_index,
        report.required_catch_up_index
    );
    assert!(report.promoted_to_voter);
    assert!(report.membership_committed);
    assert_eq!(report.voters_after_promote, vec![1, 2, 3, 4]);
    assert!(report.leader_transferred);
    assert!(report.voter_removed);
    assert_eq!(report.final_leader_id, 4);
    assert_eq!(report.final_voters, vec![2, 3, 4]);
    assert_eq!(data.membership().voters, vec![2, 3, 4]);
    assert_eq!(
        data.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Voter
    );
    assert_eq!(
        data.read_from_replica(
            4,
            Command::StringGet {
                key: "membership-workflow".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"before".to_vec())
        })
    );
}

#[cfg(feature = "temporal-raft-engine")]
#[test]
fn production_raft_readiness_requires_temporal_raft_process_and_meta_owned_membership_evidence() {
    let rollout = raft_temporal_raft_rollout_readiness();
    assert!(rollout.adapter_present);
    assert!(!rollout.data_node_real_process_rollout_validated);
    assert!(!rollout.metaserver_real_process_rollout_validated);
    assert!(!rollout.multi_process_log_store_validation_present);
    assert!(!rollout.production_ready);

    let data_report = ready_data_node_temporal_raft_rollout_report();
    let meta_report = ready_meta_temporal_raft_rollout_report();
    let rollout_with_evidence =
        raft_temporal_raft_rollout_readiness_from_reports(Some(&data_report), Some(&meta_report));
    assert!(rollout_with_evidence.data_node_real_process_rollout_validated);
    assert!(rollout_with_evidence.metaserver_real_process_rollout_validated);
    assert!(rollout_with_evidence.multi_process_log_store_validation_present);
    assert!(rollout_with_evidence.production_ready);

    let membership = raft_metaserver_membership_readiness();
    assert!(membership.networked_scheduler_transport_present);
    assert!(membership.persisted_scheduler_task_state_present);
    assert!(membership.real_data_node_group_execution_present);
    assert!(membership.production_ready);

    let readiness = distributed_raft_readiness();
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(!readiness.production_ready);
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("multi-process rollout evidence")));
    assert!(matches!(
        validate_raft_deployment_mode(RaftDeploymentMode::LocalModel),
        Err(RaftProductionReadinessError { message, .. })
            if message.contains("local Raft deployment mode is disabled")
    ));
}

#[test]
fn meta_owned_membership_report_covers_networked_scheduler_contract() {
    let workflow = MetaDataRaftMembershipWorkflowReport {
        shard_id: 7,
        learner_id: 4,
        removed_voter_id: Some(1),
        requested_leader_id: Some(4),
        initial_voters: vec![1, 2, 3],
        learner_added: true,
        catch_up_verified: true,
        learner_catch_up_index: 9,
        required_catch_up_index: 9,
        promoted_to_voter: true,
        membership_committed: true,
        voters_after_promote: vec![1, 2, 3, 4],
        leader_transferred: true,
        voter_removed: true,
        final_leader_id: 4,
        final_voters: vec![2, 3, 4],
        commit_index: 9,
    };
    let report = MetaOwnedDataRaftMembershipReport {
        scheduler_task_id: 99,
        scheduler_generation: 9,
        stale_scheduler_token_rejected: true,
        workflow,
        executed_steps: vec![
            "learner_add".to_string(),
            "catch_up_verify".to_string(),
            "promote_to_voter".to_string(),
            "leader_transfer".to_string(),
            "voter_remove".to_string(),
        ],
        final_node_evidence: vec![
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
            ready_temporal_raft_process_node(4),
        ],
        final_secondary_replica_lag: 0,
        follower_lag_validated: true,
        failover_validated: true,
        scale_up_validated: true,
        scale_down_validated: true,
        secondary_replication_validated: true,
        networked_process_api_used: true,
        scheduler_process_api_calls_observed: 5,
        data_node_membership_apply_process_api_calls_observed: 5,
        data_node_raft_group_process_nodes_observed: 3,
        data_node_raft_group_commit_indexes_observed: vec![9, 9, 9],
        learner_add_process_api_observed: true,
        catchup_verification_process_api_observed: true,
        promote_process_api_observed: true,
        leader_transfer_process_api_observed: true,
        voter_remove_process_api_observed: true,
        scheduler_generation_token_coupling_observed: true,
        stale_generation_rejection_observed: true,
        membership_generation_replayed_from_meta_raft: true,
        persisted_through_meta_raft_replay: true,
        ready: true,
        blockers: Vec::new(),
    };
    assert!(report.ready);
    assert!(report.networked_process_api_used);
    assert!(report.scheduler_generation_token_coupling_observed);
    assert!(report.stale_generation_rejection_observed);
    assert!(report.membership_generation_replayed_from_meta_raft);
    assert!(report.persisted_through_meta_raft_replay);
    assert!(report.stale_scheduler_token_rejected);
    assert_eq!(report.workflow.final_voters, vec![2, 3, 4]);
    assert_eq!(report.final_secondary_replica_lag, 0);
    assert!(report
        .executed_steps
        .iter()
        .any(|step| step == "leader_transfer"));
    assert!(report
        .final_node_evidence
        .iter()
        .all(|node| node.log_store_validated));
}

#[test]
fn metaserver_membership_workflow_requires_meta_majority() {
    let meta = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:19210".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:19211".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:19212".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    meta.cluster().set_alive(11, false).unwrap();
    meta.cluster().set_alive(12, false).unwrap();
    let data = RaftCluster::new_single_shard(8, [1, 2, 3]);

    let error = meta
        .drive_data_raft_membership_workflow(&data, 4, Some(4), Some(1))
        .unwrap_err();
    assert!(matches!(
        error,
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    ));
    assert!(data.local_status(4).is_err());
}

#[test]
fn metaserver_raft_mutation_api_rejects_without_majority() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_alive(11, false).unwrap();
    meta.set_alive(12, false).unwrap();

    let response = meta.add_namespace(AddNamespaceRequest {
        namespace: "feature".to_string(),
    });
    assert!(!response.status.ok);
    assert_eq!(response.status.code, "raft_error");
    assert!(response.status.message.contains("majority"));
}

fn cluster_with_an_installed_snapshot() -> MetaRaftCluster {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "before".to_string()
        })
        .status
        .ok);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    meta
}

#[test]
fn a_change_after_a_snapshot_install_actually_takes_effect() {
    // Installing a meta snapshot truncates the log and marks everything up to
    // the snapshot applied. The next index came from the log alone, so
    // numbering restarted at 1 -- indices the node had already applied. Every
    // proposal after an install was skipped as a duplicate and reported ok, so
    // the cluster accepted metadata changes and silently discarded them.
    let meta = cluster_with_an_installed_snapshot();

    let added = meta.add_namespace(AddNamespaceRequest {
        namespace: "after".to_string(),
    });
    assert!(added.status.ok, "{:?}", added.status);

    let namespaces = meta
        .read_meta()
        .expect("a readable replica")
        .list_namespaces()
        .namespaces;
    assert!(
        namespaces.iter().any(|namespace| namespace.namespace == "after"),
        "a change reported as accepted never took effect: {namespaces:?}"
    );
    // What the snapshot carried is still there too.
    assert!(namespaces.iter().any(|namespace| namespace.namespace == "before"));
}

#[test]
fn changes_keep_taking_effect_after_a_snapshot_install() {
    // One change could succeed by luck if the numbering only collided once.
    let meta = cluster_with_an_installed_snapshot();
    for round in 0..5u64 {
        let name = format!("ns-{round}");
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: name.clone()
            })
            .status
            .ok);
        let namespaces = meta
            .read_meta()
            .expect("a readable replica")
            .list_namespaces()
            .namespaces;
        assert!(
            namespaces.iter().any(|namespace| namespace.namespace == name),
            "change {round} was accepted and discarded: {namespaces:?}"
        );
    }
}

#[test]
fn a_snapshot_install_does_not_rewind_the_commit_index() {
    // The reused index was also written straight into commit_index, walking it
    // backwards past entries the cluster had already committed.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    for round in 0..3u64 {
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: format!("ns-{round}")
            })
            .status
            .ok);
    }
    let before = meta.status().commit_index;
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "after".to_string()
        })
        .status
        .ok);
    assert!(
        meta.status().commit_index >= before,
        "commit index went backwards: {} then {}",
        before,
        meta.status().commit_index
    );
}

fn convicted_proxy_cluster(forbid: bool) -> MetaRaftCluster {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_conviction_lock(forbid);
    assert!(meta
        .register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        })
        .status
        .ok);
    // Frozen for a reason that counts as conviction, which is what the lock is
    // about: a resource the metaserver took out, not one an operator did.
    assert!(meta
        .freeze_proxy(StateChangeRequest {
            endpoint: "proxy-a".to_string(),
            reason: crate::meta::FreezeReason::Unresponsive,
            freeze_cooldown_ms: 0,
        })
        .status
        .ok);
    meta
}

fn rejoin(meta: &MetaRaftCluster) -> Status {
    meta.register_proxy(RegisterProxyRequest {
        registered_at_ms: 0,
        proxy_addr: "proxy-a".to_string(),
        namespace: "ns".to_string(),
        location: "rack-1".to_string(),
        config_version: 1,
        binary_version: "v1".to_string(),
    })
    .status
}

#[test]
fn the_conviction_lock_holds_on_a_raft_backed_metaserver() {
    // The setting is read after `from_env` has already returned the raft
    // backend, so it reached the single-node metaserver and nothing else. The
    // check that consults it runs on these nodes -- against a flag that was
    // always false, which let a convicted resource register its way back in.
    let meta = convicted_proxy_cluster(true);
    let refused = rejoin(&meta);
    assert!(!refused.ok, "a convicted proxy registered its way back in");
    assert_eq!(refused.code, "conviction_requires_unfreeze");

    // An explicit unfreeze is the way back, or the lock would be a dead end.
    assert!(meta
        .unfreeze_proxy(StateChangeRequest {
            endpoint: "proxy-a".to_string(),
            reason: crate::meta::FreezeReason::Unspecified,
            freeze_cooldown_ms: 0,
        })
        .status
        .ok);
    assert!(rejoin(&meta).ok, "an unfrozen proxy could not rejoin");
}

#[test]
fn the_runtime_carries_the_conviction_lock_to_its_nodes() {
    // The setter alone proves nothing about production: what was broken is that
    // the option never reached the nodes, because the flag is read on a path the
    // raft backend returns before.
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: true,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 1,
        nodes: vec![ProductionRaftNode {
            node_id: 1,
            addr: "127.0.0.1:18147".to_string(),
        }],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    assert_eq!(
        runtime
            .cluster()
            .read_meta()
            .expect("a readable replica")
            .conviction_lock_enabled(),
        true,
        "the runtime did not carry the setting to its nodes"
    );
}

#[test]
fn the_conviction_lock_stays_off_when_it_is_not_asked_for() {
    // Off by default on purpose: the automatic recovery it removes is
    // load-bearing wherever the freeze cooldown is left at zero.
    let meta = convicted_proxy_cluster(false);
    assert!(
        rejoin(&meta).ok,
        "a setting nobody asked for locked a proxy out"
    );
}

#[test]
fn installing_a_snapshot_does_not_clear_the_conviction_lock() {
    // Each node's metadata used to be rebuilt from `Default` on install, which
    // discarded everything configured on it -- the lock along with the event
    // bus, the metrics recorder and the counters.
    let meta = convicted_proxy_cluster(true);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");

    let refused = rejoin(&meta);
    assert!(
        !refused.ok,
        "a snapshot install turned the conviction lock off"
    );
    assert_eq!(refused.code, "conviction_requires_unfreeze");
}

#[test]
fn an_ordinary_change_still_reports_success() {
    // The guard on "committed but applied nothing" must not catch the normal
    // path: every real change still has to come back ok, before and after a
    // snapshot install.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "before".to_string()
        })
        .status
        .ok);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    let after = meta.add_namespace(AddNamespaceRequest {
        namespace: "after".to_string(),
    });
    assert!(after.status.ok, "{:?}", after.status);
    assert_ne!(after.status.code, "mutation_not_applied");
}

#[test]
fn muting_metadata_change_refuses_changes_on_the_raft_path_too() {
    // The mute is the incident lever: while it is set the metaserver is meant
    // to refuse every recorded metadata mutation. That check lived only in
    // SingleNodeMeta's public methods, and the raft backend proposes straight
    // past them -- so on a raft-backed metaserver, which is what a real
    // deployment runs, setting the mute changed nothing.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta.set_meta_change_muted(true).status.ok);

    let muted = meta.add_namespace(AddNamespaceRequest {
        namespace: "during-an-incident".to_string(),
    });
    assert!(
        !muted.status.ok,
        "the cluster was muted and the change went through anyway"
    );
    assert_eq!(muted.status.code, "meta_change_muted");

    // And the lever has to be releasable, or muting would be a one-way door.
    assert!(meta.set_meta_change_muted(false).status.ok);
    assert!(
        meta.add_namespace(AddNamespaceRequest {
            namespace: "after-the-incident".to_string(),
        })
        .status
        .ok,
        "unmuting did not restore metadata change"
    );
}

fn table_in(meta: &MetaRaftCluster, namespace: &str, table_name: &str) -> Status {
    meta.add_table(AddTableRequest {
        namespace: namespace.to_string(),
        table_name: table_name.to_string(),
        first_shard_id: 500,
        shard_count: 1,
        replica_count: 1,
        partition_version: 0,
        serving_options: crate::meta::TableServingOptions::default(),
    })
    .status
}

#[test]
fn a_reserved_name_cannot_be_taken_on_the_raft_path() {
    // Reserved names exist to hold a name back from creation. The check lived
    // in the public method, and the raft path proposes past it, so on a
    // raft-backed metaserver the reservation held nothing back at all.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let mut reserved = crate::meta::ReservedNames::default();
    reserved.namespaces.insert("system".to_string());
    reserved.tables.insert("internal".to_string());
    assert!(meta.set_reserved_names(reserved).status.ok);

    let taken = meta.add_namespace(AddNamespaceRequest {
        namespace: "system".to_string(),
    });
    assert!(!taken.status.ok, "a reserved namespace was created anyway");
    assert_eq!(taken.status.code, "name_reserved");

    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string()
        })
        .status
        .ok);
    let table = table_in(&meta, "tenant", "internal");
    assert!(!table.ok, "a reserved table name was created anyway");
    assert_eq!(table.code, "name_reserved");

    // And an unreserved name is still allowed, so the guard is not a blanket no.
    assert!(table_in(&meta, "tenant", "orders").ok);
}

#[test]
fn dropping_a_namespace_cannot_strand_a_live_table_on_the_raft_path() {
    // The single-node path refuses this precisely so a drop cannot leave tables
    // behind in a namespace that no longer exists. The raft path did not.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string()
        })
        .status
        .ok);
    assert!(table_in(&meta, "tenant", "orders").ok);

    let stranding = meta.drop_namespace(AddNamespaceRequest {
        namespace: "tenant".to_string(),
    });
    assert!(
        !stranding.status.ok,
        "a namespace was dropped out from under a live table"
    );
    assert_eq!(stranding.status.code, "namespace_not_empty");

    // Once the table is gone the namespace can be dropped, or the guard would
    // make a namespace undroppable rather than merely safe to drop.
