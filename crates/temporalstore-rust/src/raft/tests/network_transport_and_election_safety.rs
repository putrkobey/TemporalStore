// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

// shared-corpus: raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn http_raft_transport_sends_append_vote_and_snapshot_over_tcp() {
    append_vote_and_snapshot_over_tcp(11, Wire::Binary);
}

// The receiver sniffs the body and accepts either wire, so the JSON sender stays covered rather
// than merely reachable.
#[test]
fn http_raft_transport_sends_append_entries_as_json_over_tcp() {
    append_vote_and_snapshot_over_tcp(12, Wire::Json);
}

#[derive(Clone, Copy)]
enum Wire {
    Binary,
    Json,
}

fn append_vote_and_snapshot_over_tcp(shard: ShardId, wire: Wire) {
    let cluster = RaftCluster::new_single_shard(shard, [1, 2, 3]);
    let addr = free_local_addr();
    std::thread::spawn({
        let cluster = cluster.clone();
        let addr = addr.clone();
        move || {
            crate::http::test_ports::serve_reserved(&addr, move |request| {
                handle_raft_http(&cluster, request)
            })
            .unwrap()
        }
    });
    wait_for_http(&addr);

    let mut peers = BTreeMap::new();
    peers.insert(2, addr.clone());
    peers.insert(3, addr.clone());
    let mut transport = HttpRaftTransport::with_options(
        peers,
        HttpRequestOptions {
            connect_timeout_ms: 200,
            io_timeout_ms: 500,
            max_retries: 1,
        },
    );
    if matches!(wire, Wire::Json) {
        transport.send_json_append_entries_for_test();
    }

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "network".to_string(),
            value: b"append".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let append = cluster.build_append_entries_request(3).unwrap();
    let append_response = transport.append_entries(append).unwrap();
    assert!(append_response.success);
    assert_eq!(append_response.match_index, 1);
    assert_eq!(
        cluster.read_local(
            3,
            Command::StringGet {
                key: "network".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"append".to_vec())
        })
    );

    let vote = cluster.build_vote_request(2, 3).unwrap();
    let vote_response = transport.request_vote(vote).unwrap();
    assert!(vote_response.vote_granted);

    cluster.elect_leader(2).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot".to_string(),
            value: b"installed".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.build_install_snapshot_request(3).unwrap();
    let snapshot_response = transport.install_snapshot(snapshot).unwrap();
    assert!(snapshot_response.success);
    assert_eq!(snapshot_response.last_included_index, 2);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn streaming_snapshot_chunks_install_only_after_all_chunks_arrive() {
    let cluster = RaftCluster::new_single_shard(21, [1, 2, 3]);
    // This test is about ENTRY chunking; a state image is one small unit and completes in a
    // single chunk, which is not what these assertions probe.
    cluster.snapshot_carries_entries_for_test();
    cluster.set_alive(3, false).unwrap();
    for index in 0..5 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    cluster.set_alive(3, true).unwrap();
    let chunks = cluster.build_install_snapshot_chunks(3, 2).unwrap();
    assert_eq!(chunks.len(), 3);

    let first = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);

    let second = cluster
        .receive_install_snapshot_chunk(chunks[1].clone())
        .unwrap();
    assert!(second.success);
    assert!(!second.snapshot_complete);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);

    let final_chunk = cluster
        .receive_install_snapshot_chunk(chunks[2].clone())
        .unwrap();
    assert!(final_chunk.snapshot_complete);
    assert_eq!(final_chunk.last_included_index, 5);
    assert_eq!(cluster.commit_index(3).unwrap(), 5);
    assert_eq!(
        cluster
            .read_local(
                3,
                Command::StringGet {
                    key: "k4".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(vec![4])
        }
    );
}

// shared-corpus: raft_matrixraft_snapshot_lifecycle_depth
// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn matrixraft_snapshot_chunk_retry_releases_backpressure_and_installs_chunk() {
    let transport = FlakyTransport {
        cluster: RaftCluster::new_single_shard(213, [1, 2, 3]),
        failures_left: Arc::new(Mutex::new(1)),
    };
    // This test is about ENTRY chunking; a state image is one small unit and completes in a
    // single chunk, which is not what these assertions probe.
    transport.cluster.snapshot_carries_entries_for_test();
    for index in 0..3 {
        transport
            .cluster
            .propose(Command::StringSet {
                key: format!("snapshot-retry-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    let chunks = transport
        .cluster
        .build_install_snapshot_chunks(3, 2)
        .unwrap();
    let runtime = RaftRpcRuntime::new(
        transport.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 2,
            retry_backoff_ms: 0,
            deadline_ms: 100,
            auth_token_required: false,
        },
    );

    let first = runtime.install_snapshot_chunk(chunks[0].clone()).unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    assert_eq!(first.received_chunks, 1);
    let metrics = runtime.metrics();
    assert_eq!(metrics.attempts, 2);
    assert_eq!(metrics.retries, 1);
    assert_eq!(metrics.successes, 1);
    assert_eq!(metrics.inflight, 0);

    let _permit = runtime.acquire().unwrap();
    assert!(matches!(
        runtime.install_snapshot_chunk(chunks[1].clone()).unwrap_err(),
        RaftError::Transport(message) if message.contains("backpressure")
    ));
    assert_eq!(runtime.metrics().backpressure_rejections, 1);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn matrixraft_snapshot_lifecycle_reports_timeout_rate_limit_rollback_membership_and_rejoin() {
    let cluster = RaftCluster::new_single_shard_with_config(
        214,
        [1, 2, 3],
        RaftConfig {
            max_inflights_replicate: 1,
            max_applied_log_bytes: 1,
            send_snapshot_timeout_ms: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    // This test is about ENTRY chunking; a state image is one small unit and completes in a
    // single chunk, which is not what these assertions probe.
    cluster.snapshot_carries_entries_for_test();
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("snapshot-lifecycle-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();
    cluster.set_alive(3, true).unwrap();

    let chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
    assert!(chunks.len() > 1);
    cluster.advance_time_ms(2);
    let first = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    let duplicate = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(duplicate.success);
    assert!(!duplicate.snapshot_complete);
    for chunk in chunks.iter().skip(1) {
        cluster
            .receive_install_snapshot_chunk(chunk.clone())
            .unwrap();
    }
    assert_eq!(cluster.commit_index(3).unwrap(), 3);

    let rollback_chunks = cluster.build_install_snapshot_chunks(2, 1).unwrap();
    cluster
        .receive_install_snapshot_chunk(rollback_chunks[0].clone())
        .unwrap();
    let mut changed_metadata = rollback_chunks[1].clone();
    changed_metadata.last_included_index = changed_metadata.last_included_index.saturating_add(1);
    assert!(matches!(
        cluster.receive_install_snapshot_chunk(changed_metadata).unwrap_err(),
        RaftError::InvalidSnapshotChunk(message) if message.contains("metadata changed")
    ));

    let admin = cluster.matrixraft_runtime_admin_report();
    assert!(admin.snapshot_retry_backpressure_present);
    assert!(admin.snapshot_chunk_retry_present);
    assert!(admin.snapshot_send_timeout_present);
    assert!(admin.snapshot_rate_limit_present);
    assert!(admin.snapshot_install_progress_present);
    assert!(admin.snapshot_install_rollback_present);
    assert!(admin.snapshot_membership_change_present);
    assert!(admin.snapshot_rejoin_after_compacted_log_present);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "snapshot_sender_downloader_lifecycle" && item.ready));
    let lagging_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .unwrap();
    assert!(lagging_peer.snapshot_send_timeouts > 0);
    assert!(lagging_peer.snapshot_rate_limit_rejections > 0);
    assert!(lagging_peer.snapshot_chunk_retry_count > 0);
    assert_eq!(lagging_peer.snapshot_install_progress_per_mille, 1_000);
    assert!(lagging_peer.snapshot_during_membership_change);
    assert!(lagging_peer.snapshot_rejoin_after_compacted_log);
    let rollback_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 2)
        .unwrap();
    assert!(rollback_peer.snapshot_install_rolled_back > 0);

    let metrics = cluster.prometheus_metrics();
    for metric in [
        "temporalstore_raft_matrixraft_snapshot_chunk_retry_present",
        "temporalstore_raft_matrixraft_snapshot_send_timeout_present",
        "temporalstore_raft_matrixraft_snapshot_rate_limit_present",
        "temporalstore_raft_matrixraft_snapshot_install_progress_present",
        "temporalstore_raft_matrixraft_snapshot_install_rollback_present",
        "temporalstore_raft_matrixraft_snapshot_membership_change_present",
        "temporalstore_raft_matrixraft_snapshot_rejoin_after_compacted_log_present",
        "temporalstore_raft_matrixraft_peer_snapshot_send_timeouts",
        "temporalstore_raft_matrixraft_peer_snapshot_rate_limit_rejections",
        "temporalstore_raft_matrixraft_peer_snapshot_install_progress_per_mille",
        "temporalstore_raft_matrixraft_peer_snapshot_install_rolled_back",
        "temporalstore_raft_matrixraft_peer_snapshot_during_membership_change",
        "temporalstore_raft_matrixraft_peer_snapshot_rejoin_after_compacted_log",
    ] {
        assert!(metrics.contains(metric), "missing snapshot metric {metric}");
    }
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix
#[test]
fn raft_snapshot_transport_rejects_stale_term_before_install() {
    let cluster = RaftCluster::new_single_shard(211, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "snapshot-term".to_string(),
            value: b"leader-value".to_vec(),
        })
        .unwrap();
    cluster.elect_leader(2).unwrap();
    let mut request = cluster.build_install_snapshot_request(3).unwrap();
    request.term = 1;

    let response = cluster.receive_install_snapshot(request).unwrap();
    assert!(!response.success);
    assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
    assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn raft_snapshot_chunk_transport_rejects_stale_term_before_buffering() {
    let cluster = RaftCluster::new_single_shard(212, [1, 2, 3]);
    // This test is about ENTRY chunking; a state image is one small unit and completes in a
    // single chunk, which is not what these assertions probe.
    cluster.snapshot_carries_entries_for_test();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("snapshot-chunk-term-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    cluster.elect_leader(2).unwrap();
    let mut chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
    chunks[0].term = 1;

    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(!response.success);
    assert!(!response.snapshot_complete);
    assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
    assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);

    chunks[0].term = 2;
    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(response.success);
    assert!(!response.snapshot_complete);
    assert_eq!(response.received_chunks, 1);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix raft_matrixraft_rolling_restart_joint_consensus_fault_harness
#[test]
fn joint_consensus_requires_old_and_new_majorities_before_commit_or_write() {
    let cluster = RaftCluster::new_single_shard(22, [1, 2, 3]);
    let membership = cluster
        .begin_joint_consensus([1, 2, 3, 4, 5, 6, 7])
        .unwrap();
    assert_eq!(membership.old_voters, vec![1, 2, 3]);
    assert_eq!(membership.new_voters, vec![1, 2, 3, 4, 5, 6, 7]);
    for node_id in [4, 5, 6, 7] {
        cluster.set_alive(node_id, false).unwrap();
    }
    assert_eq!(
        cluster
            .propose(Command::StringSet {
                key: "blocked".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 3,
            required: 4,
        }
    );
    assert_eq!(
        cluster.commit_joint_consensus().unwrap_err(),
        RaftError::NoMajority {
            live: 3,
            required: 4,
        }
    );

    cluster.set_alive(4, true).unwrap();
    cluster.commit_joint_consensus().unwrap();
    assert_eq!(cluster.membership().voters, vec![1, 2, 3, 4, 5, 6, 7]);
    cluster
        .propose(Command::StringSet {
            key: "after".to_string(),
            value: b"ok".to_vec(),
        })
        .unwrap();
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix raft_matrixraft_rolling_restart_joint_consensus_fault_harness
#[test]
fn joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(222, [1, 2, 3]);
    cluster.begin_joint_consensus([1, 2, 3, 4, 5]).unwrap();
    cluster.persist_wal(dir.path()).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        222,
        [1, 2, 3, 4, 5],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(
        restored.joint_membership(),
        Some(JointConsensusMembership {
            old_voters: vec![1, 2, 3],
            new_voters: vec![1, 2, 3, 4, 5],
        })
    );

    restored.set_alive(2, false).unwrap();
    restored.set_alive(4, false).unwrap();
    restored.set_alive(5, false).unwrap();
    assert_eq!(
        restored
            .propose(Command::StringSet {
                key: "blocked-after-restore".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 2,
            required: 3,
        }
    );

    restored.set_alive(4, true).unwrap();
    restored.commit_joint_consensus().unwrap();
    assert_eq!(restored.membership().voters, vec![1, 2, 3, 4, 5]);
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn raft_rpc_runtime_retries_transport_errors_and_releases_inflight() {
    let transport = FlakyTransport {
        cluster: RaftCluster::new_single_shard(23, [1, 2, 3]),
        failures_left: Arc::new(Mutex::new(1)),
    };
    let runtime = RaftRpcRuntime::new(
        transport.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 2,
            retry_backoff_ms: 0,
            deadline_ms: 100,
            auth_token_required: false,
        },
    );
    let response = runtime
        .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
        .unwrap();
    assert!(response.success);
    assert_eq!(runtime.inflight(), 0);
    let metrics = runtime.metrics();
    assert_eq!(metrics.attempts, 2);
    assert_eq!(metrics.retries, 1);
    assert_eq!(metrics.successes, 1);
    assert_eq!(metrics.failures, 0);
    assert_eq!(metrics.inflight, 0);

    let _permit = runtime.acquire().unwrap();
    assert!(matches!(
        runtime
            .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
            .unwrap_err(),
        RaftError::Transport(message) if message.contains("backpressure")
    ));
    assert_eq!(runtime.metrics().backpressure_rejections, 1);
}

// shared-corpus: raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn raft_rpc_runtime_attaches_auth_and_deadline_metadata() {
    let cluster = RaftCluster::new_single_shard(25, [1, 2, 3]);
    let authenticated = AuthenticatedRaftTransport::new(cluster.clone(), "secret");
    let unauthenticated_runtime = RaftRpcRuntime::new(
        authenticated.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 0,
            retry_backoff_ms: 0,
            deadline_ms: 250,
            auth_token_required: true,
        },
    );
    assert!(matches!(
        unauthenticated_runtime
            .append_entries(cluster.build_append_entries_request(2).unwrap())
            .unwrap_err(),
        RaftError::Transport(message) if message.contains("auth")
    ));

    let authenticated_runtime = RaftRpcRuntime::with_auth_token(
        authenticated,
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 0,
            retry_backoff_ms: 0,
            deadline_ms: 250,
            auth_token_required: true,
        },
        Some("secret".to_string()),
    );
    let response = authenticated_runtime
        .append_entries(cluster.build_append_entries_request(2).unwrap())
        .unwrap();
    assert!(response.success);
}

#[test]
fn raft_scheduler_randomizes_election_timeout_and_emits_heartbeats() {
    let mut scheduler = RaftScheduler::new(RaftSchedulerOptions {
        heartbeat_interval_tick: 2,
        election_timeout_min_tick: 3,
        election_timeout_max_tick: 5,
        random_seed: 7,
    });
    assert!(!scheduler.tick(true).heartbeat_due);
    assert!(scheduler.tick(true).heartbeat_due);

    let mut election_due_at = None;
    for tick in 1..=8 {
        let event = scheduler.tick(false);
        assert!((3..=5).contains(&event.election_timeout_tick));
        if event.election_due {
            election_due_at = Some(tick);
            break;
        }
    }
    assert!((3..=5).contains(&election_due_at.expect("election should become due")));
}

// shared-corpus: raft_matrixraft_read_lease_fault_matrix raft_matrixraft_packet_loss_fault_harness
#[test]
fn partition_chaos_majority_side_continues_and_healed_replica_catches_up() {
    let cluster = RaftCluster::new_single_shard(24, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("majority-{index}"),
                value: vec![index],
            })
            .unwrap();
    }
    assert_eq!(
        cluster.read_from_replica(
            3,
            Command::StringGet {
                key: "majority-2".to_string(),
            },
        ),
        Err(RaftError::NodeNotFound(3))
    );

    cluster.set_alive(3, true).unwrap();
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster.commit_index(3).unwrap(),
        cluster.status().commit_index
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "majority-2".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(vec![2])
        }
    );
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_packet_loss_fault_harness
#[test]
fn distributed_propose_does_not_wait_for_failed_followers_after_quorum() {
    let cluster =
        RaftCluster::new_single_shard_with_config(7, vec![1, 2, 3], RaftConfig::default()).unwrap();
    let snapshot_attempts = Arc::new(Mutex::new(0usize));
    let transport = OneFailedFollowerTransport {
        cluster: cluster.clone(),
        failed_target: 3,
        snapshot_attempts: Arc::clone(&snapshot_attempts),
    };

    let response = cluster
        .propose_distributed(
            Command::StringSet {
                key: "quorum-fast-path".to_string(),
                value: b"ok".to_vec(),
            },
            &transport,
        )
        .unwrap();
    assert_eq!(response, CommandResponse::Empty);
    assert_eq!(*snapshot_attempts.lock().unwrap(), 0);
    assert_eq!(cluster.commit_index(1).unwrap(), 1);
    assert_eq!(cluster.commit_index(2).unwrap(), 1);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);
}

#[test]
fn distributed_raft_readiness_reports_remaining_production_blockers() {
    let readiness = distributed_raft_readiness();
    assert!(!readiness.complete);
    assert!(!readiness.production_ready);
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.local_model_tested);
    assert!(readiness.transport_contracts_present);
    assert!(readiness.rpc_runtime_observability_present);
    assert!(readiness.external_snapshot_refs_present);
    assert_eq!(
        readiness.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.temporal_raft_data_node_process_startup_present);
    assert!(readiness.temporal_raft_metaserver_process_startup_present);
    assert!(readiness.matrixraft_leader_write_authority_present);
    assert!(readiness.matrixraft_operator_observability_present);
    assert!(readiness.matrixraft_rpc_transport_contract_present);
    assert!(readiness.matrixraft_log_retention_snapshot_trigger_present);
    assert!(readiness.matrixraft_apply_snapshot_fence_present);
    assert!(readiness.raft_storage_apply_fence_present);
    assert!(readiness.matrixraft_snapshot_floor_log_matching_present);
    assert!(readiness.matrixraft_snapshot_tail_catchup_present);
    assert!(readiness.matrixraft_compacted_entry_rejection_present);
    assert!(readiness.matrixraft_metaserver_snapshot_floor_election_present);
    assert!(readiness.learner_catchup_promotion_present);
    assert!(readiness.metaserver_membership_workflow_present);
    assert!(readiness.durable_apply_index_snapshot_integrated);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(readiness.production_mtls_transport_present);
    assert!(readiness.external_chaos_validation_present);
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("applied Raft index")));
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("learner add")));
    assert!(!readiness.missing.iter().any(|item| item.contains("mTLS")));
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("process startup")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("metaserver multi-process rollout evidence")));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "data_node_report_missing" && item.evidence_field == "data_node_report"
    }));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "metaserver_report_missing" && item.evidence_field == "metaserver_report"
    }));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn raft_temporal_raft_rollout_readiness_fails_closed_without_process_rollout_evidence() {
    let readiness = raft_temporal_raft_rollout_readiness();
    assert_eq!(
        readiness.adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.data_node_process_startup_selects_temporal_raft);
    assert!(readiness.metaserver_process_startup_selects_temporal_raft);
    assert!(readiness.durable_log_state_present);
    assert_eq!(
        readiness.local_rollout_ready,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(!readiness.data_node_real_process_rollout_validated);
    assert!(!readiness.metaserver_real_process_rollout_validated);
    assert!(!readiness.multi_process_log_store_validation_present);
    assert!(!readiness.production_ready);
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("metaserver multi-process rollout evidence")));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "data_node_report_missing" && item.evidence_field == "data_node_report"
    }));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "metaserver_report_missing" && item.evidence_field == "metaserver_report"
    }));
    if !cfg!(feature = "temporal-raft-engine") {
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("TemporalRaft production engine adapter")));
    }

    let distributed = distributed_raft_readiness();
    assert_eq!(
        distributed.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(distributed.temporal_raft_data_node_process_startup_present);
    assert!(distributed.temporal_raft_metaserver_process_startup_present);
    assert!(distributed
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(distributed
        .missing_evidence_fields
        .iter()
        .any(|item| item.evidence_field == "data_node_report"));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn raft_temporal_raft_rollout_readiness_accepts_only_multi_process_reports() {
    let data_report = ready_data_node_temporal_raft_rollout_report();
    let meta_report = ready_meta_temporal_raft_rollout_report();
    let readiness =
        raft_temporal_raft_rollout_readiness_from_reports(Some(&data_report), Some(&meta_report));

    assert_eq!(
        readiness.adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.data_node_real_process_rollout_validated);
    assert!(readiness.metaserver_real_process_rollout_validated);
    assert!(readiness.multi_process_log_store_validation_present);
    assert_eq!(
        readiness.production_ready,
        cfg!(feature = "temporal-raft-engine")
    );
    if cfg!(feature = "temporal-raft-engine") {
        assert!(readiness.missing.is_empty());
    } else {
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("TemporalRaft production engine adapter")));
    }
    let distributed_with_evidence =
        distributed_raft_readiness_from_temporal_raft_reports(&data_report, &meta_report);
    assert_eq!(
        distributed_with_evidence.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(distributed_with_evidence.metaserver_driven_membership_present);
    assert_eq!(
        distributed_with_evidence.production_ready,
        cfg!(feature = "temporal-raft-engine")
    );

    let mut local_fixture_like_data = data_report;
    local_fixture_like_data.nodes.truncate(1);
    let rejected = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&local_fixture_like_data),
        Some(&meta_report),
    );
    assert!(!rejected.data_node_real_process_rollout_validated);
    assert!(!rejected.production_ready);
    assert!(rejected
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));

    let mut missing_process_path_proof = ready_data_node_temporal_raft_rollout_report();
    missing_process_path_proof.independent_snapshot_dirs = false;
    missing_process_path_proof.observed_process_requests = 0;
    missing_process_path_proof.read_index_responses_observed = 0;
    missing_process_path_proof.per_node_log_store_inspection_count = 1;
    let rejected_process_path = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_process_path_proof),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_process_path.data_node_real_process_rollout_validated);
    assert!(!rejected_process_path.production_ready);
    assert!(rejected_process_path
        .missing
        .iter()
        .any(|item| item.contains("independent WAL/snapshot dirs")));

    let mut duplicate_process_identity = ready_data_node_temporal_raft_rollout_report();
    duplicate_process_identity.nodes[1].addr = duplicate_process_identity.nodes[0].addr.clone();
    duplicate_process_identity.nodes[1].wal_dir =
        duplicate_process_identity.nodes[0].wal_dir.clone();
    duplicate_process_identity.nodes[1].snapshot_dir =
        duplicate_process_identity.nodes[0].snapshot_dir.clone();
    let rejected_duplicate_identity = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&duplicate_process_identity),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_duplicate_identity.data_node_real_process_rollout_validated);
    assert!(!rejected_duplicate_identity.production_ready);

    let mut missing_api_write = ready_data_node_temporal_raft_rollout_report();
    missing_api_write.write_proposed_through_process_api = false;
    missing_api_write.restart_recovery_validated = false;
    let rejected_missing_api_write = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_api_write),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_missing_api_write.data_node_real_process_rollout_validated);
    assert!(!rejected_missing_api_write.production_ready);

    let mut missing_meta_api_mutation = ready_meta_temporal_raft_rollout_report();
    missing_meta_api_mutation.mutation_proposed_through_process_api = false;
    missing_meta_api_mutation.read_index_validated = false;
    missing_meta_api_mutation.per_node_log_store_inspection_count = 1;
    let rejected_missing_meta_api_mutation = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_api_mutation),
    );
    assert!(!rejected_missing_meta_api_mutation.metaserver_real_process_rollout_validated);
    assert!(!rejected_missing_meta_api_mutation.production_ready);

    let mut missing_behavior_evidence = ready_data_node_temporal_raft_rollout_report();
    missing_behavior_evidence.failover_validated = false;
    missing_behavior_evidence.membership_change_validated = false;
    missing_behavior_evidence.follower_lag_validated = false;
    missing_behavior_evidence.secondary_read_validated = false;
    let rejected_behavior = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_behavior_evidence),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_behavior.data_node_real_process_rollout_validated);
    assert!(!rejected_behavior.production_ready);
    assert!(rejected_behavior
        .missing
        .iter()
        .any(|item| item.contains("failover")));

    let mut missing_meta_behavior = ready_meta_temporal_raft_rollout_report();
    missing_meta_behavior.secondary_read_validated = false;
    let rejected_meta_behavior = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_behavior),
    );
    assert!(!rejected_meta_behavior.metaserver_real_process_rollout_validated);
    assert!(!rejected_meta_behavior.production_ready);
    assert!(rejected_meta_behavior
        .missing
        .iter()
        .any(|item| item.contains("secondary reads")));

    let mut api_only_data = ready_data_node_temporal_raft_rollout_report();
    api_only_data.operational_semantics =
        TemporalRaftProcessOperationalSemanticsEvidence::default();
    let rejected_api_only = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&api_only_data),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_api_only.data_node_real_process_rollout_validated);
    assert!(!rejected_api_only.production_ready);
    assert!(rejected_api_only
        .missing
        .iter()
        .any(|item| item.contains("operational semantics evidence")));
    assert!(rejected_api_only
        .missing
        .iter()
        .any(|item| item.contains("process_path_validated")));

    let mut missing_read_safety = ready_data_node_temporal_raft_rollout_report();
    missing_read_safety
        .operational_semantics
        .read_index_validated = false;
    missing_read_safety
        .operational_semantics
        .leader_lease_validated = false;
    missing_read_safety
        .operational_semantics
        .stale_leader_lease_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .follower_lease_expiration_observed = false;
    missing_read_safety
        .operational_semantics
        .bounded_stale_read_acceptance_observed = false;
    missing_read_safety
        .operational_semantics
        .bounded_stale_read_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .minority_partition_read_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .healed_follower_catchup_observed = false;
    missing_read_safety
        .operational_semantics
        .stale_follower_write_rejected = false;
    missing_read_safety
        .operational_semantics
        .leader_transfer_exact_once_validated = false;
    missing_read_safety
        .operational_semantics
        .snapshot_bootstrap_validated = false;
    missing_read_safety
        .operational_semantics
        .membership_rescale_validated = false;
    missing_read_safety
        .operational_semantics
        .apply_pipeline_converged = false;
    missing_read_safety
        .operational_semantics
        .wal_persistence_observed = false;
    missing_read_safety
        .operational_semantics
        .fsm_apply_idempotent_replay_observed = false;
    missing_read_safety
        .operational_semantics
        .storage_mutation_wal_fence_atomicity_observed = false;
    missing_read_safety
        .operational_semantics
        .snapshot_install_apply_fence_atomicity_observed = false;
    missing_read_safety
        .operational_semantics
        .process_restart_after_apply_crash_recovered = false;
    let rejected_read_safety = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_read_safety),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_read_safety.data_node_real_process_rollout_validated);
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("read_index_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("leader_lease_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("stale_leader_lease_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("bounded_stale_read_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("minority_partition_read_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("healed_follower_catchup_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("stale_follower_write_rejected")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("leader_transfer_exact_once_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("snapshot_bootstrap_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("membership_rescale_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("apply_pipeline_converged")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("wal_persistence_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("fsm_apply_idempotent_replay_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("storage_mutation_wal_fence_atomicity_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("snapshot_install_apply_fence_atomicity_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("process_restart_after_apply_crash_recovered")));

    let mut missing_data_crash_windows = ready_data_node_temporal_raft_rollout_report();
    missing_data_crash_windows.crash_after_storage_mutation_recovered = false;
    missing_data_crash_windows.crash_after_wal_persist_recovered = false;
    missing_data_crash_windows.crash_during_snapshot_install_recovered = false;
    missing_data_crash_windows.apply_fence_recovered_after_restart = false;
    let rejected_data_crash_windows = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_data_crash_windows),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_data_crash_windows.data_node_real_process_rollout_validated);
    assert!(
        rejected_data_crash_windows
            .missing
            .iter()
            .any(|item| item
                .contains("storage mutation/WAL persistence/snapshot install/apply fence"))
    );

    let mut missing_meta_crash_windows = ready_meta_temporal_raft_rollout_report();
    missing_meta_crash_windows.crash_after_meta_mutation_recovered = false;
    missing_meta_crash_windows.crash_after_meta_wal_persist_recovered = false;
    missing_meta_crash_windows.crash_during_meta_snapshot_install_recovered = false;
    missing_meta_crash_windows.meta_apply_fence_recovered_after_restart = false;
    let rejected_meta_crash_windows = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_crash_windows),
    );
    assert!(!rejected_meta_crash_windows.metaserver_real_process_rollout_validated);
    assert!(rejected_meta_crash_windows
        .missing
        .iter()
        .any(|item| item.contains("meta mutation/WAL persistence/snapshot install/apply fence")));
}

