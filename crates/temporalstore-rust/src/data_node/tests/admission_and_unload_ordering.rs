// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn distributed_admission_policy_aggregates_peer_counters() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let peers = vec![
        DistributedAdmissionPeerSnapshot {
            node_id: "node-a".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 5,
            write_count: 3,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-b".to_string(),
            shard_id: 7,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 4,
            write_count: 6,
        },
        DistributedAdmissionPeerSnapshot {
            node_id: "node-c".to_string(),
            shard_id: 8,
            topology_version: 12,
            window_start_ms: 1000,
            read_count: 100,
            write_count: 100,
        },
    ];

    let allowed = runtime.distributed_admission_decision(7, &peers, 10, 10, 12);
    assert!(allowed.status.ok, "{allowed:?}");
    assert_eq!(allowed.participating_nodes, 2);
    assert_eq!(allowed.aggregate_read_count, 9);
    assert_eq!(allowed.aggregate_write_count, 9);
    assert!(allowed.read_allowed);
    assert!(allowed.write_allowed);

    let rejected = runtime.distributed_admission_decision(7, &peers, 9, 9, 12);
    assert_eq!(rejected.status.code, "distributed_admission_rejected");
    assert!(!rejected.read_allowed);
    assert!(!rejected.write_allowed);
    assert!(rejected
        .reasons
        .contains(&"distributed_read_budget_exceeded".to_string()));
    assert!(rejected
        .reasons
        .contains(&"distributed_write_budget_exceeded".to_string()));

    let stale = runtime.distributed_admission_decision(7, &peers, 10, 10, 13);
    assert!(stale
        .reasons
        .contains(&"stale_distributed_admission_topology".to_string()));
}

#[test]
fn multi_process_lifecycle_validation_requires_load_reload_unload_and_restart() {
    let reports = vec![
        DataNodeLifecycleReport {
            loaded_shard_count: 1,
            serving_count: 1,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 42,
            shards: Vec::new(),
            transitions: vec![
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "serving".to_string(),
                    operation: "load".to_string(),
                    load_version: 42,
                    updated_at_ms: 1,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(1),
                    scheduler_generation: Some(10),
                },
                DataNodeShardLifecycleState {
                    shard_id: 7,
                    state: "readonly".to_string(),
                    operation: "reload".to_string(),
                    load_version: 43,
                    updated_at_ms: 2,
                    last_status: Some(Status::ok()),
                    scheduler_task_id: Some(2),
                    scheduler_generation: Some(11),
                },
            ],
            ..DataNodeLifecycleReport::default()
        },
        DataNodeLifecycleReport {
            loaded_shard_count: 0,
            serving_count: 0,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 43,
            shards: Vec::new(),
            transitions: vec![DataNodeShardLifecycleState {
                shard_id: 7,
                state: "unloaded".to_string(),
                operation: "unload".to_string(),
                load_version: 43,
                updated_at_ms: 3,
                last_status: Some(Status::ok()),
                scheduler_task_id: Some(3),
                scheduler_generation: Some(12),
            }],
            ..DataNodeLifecycleReport::default()
        },
    ];
    let persistence = vec![
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
        DataNodeLifecyclePersistenceReport {
            enabled: true,
            last_restore_status: Some(Status::ok()),
            restore_success_total: 1,
            ..DataNodeLifecyclePersistenceReport::default()
        },
    ];

    let validated = validate_multi_process_lifecycle_reports(&reports, &persistence);
    assert!(validated.passed, "{validated:?}");
    assert_eq!(validated.node_count, 2);
    assert!(validated.load_validated);
    assert!(validated.reload_validated);
    assert!(validated.unload_validated);
    assert!(validated.restart_restore_validated);
    assert!(validated.all_nodes_have_persistence);

    let missing_restart = validate_multi_process_lifecycle_reports(&reports, &[]);
    assert!(!missing_restart.passed);
    assert!(missing_restart
        .blockers
        .contains(&"restart_restore_not_validated".to_string()));
}

#[test]
fn runtime_direct_unload_rejects_busy_shard_without_unloading() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 4,
        },
    );

    let queued = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 7,
            selected_routing_buckets: Vec::new(),
