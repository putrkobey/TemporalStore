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
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(queued.status.ok, "{queued:?}");

    let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
    assert_eq!(unload.status.code, "shard_busy");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.failed_count, 1);
    assert_eq!(lifecycle.transitions[0].state, "failed");
    assert_eq!(lifecycle.transitions[0].operation, "unload");
    assert_eq!(
        lifecycle.transitions[0].last_status.as_ref().unwrap().code,
        "shard_busy"
    );
}

#[test]
fn runtime_queued_unload_waits_for_prior_shard_work() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let write = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "before-unload".to_string(),
                value: b"value".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let unload = runtime.submit_unload(
        crate::control::UnloadShardRequest { shard_id: 7 },
        RequestController { timeout_ms: 1000 },
    );
    assert!(write.status.ok, "{write:?}");
    assert!(unload.status.ok, "{unload:?}");

    let first = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("first shard task should be ready");
    assert_eq!(first.job_id, write.job_id);
    let output = execute_task(&runtime.inner, &first);
    let DataNodeTaskOutput::Execute(response) = output else {
        panic!("expected execute output");
    };
    assert!(response.status.ok, "{response:?}");
    runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .finish_shard(7);

    let second = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("queued unload should be ready after prior work");
    assert_eq!(second.job_id, unload.job_id);
    let output = execute_task(&runtime.inner, &second);
    let DataNodeTaskOutput::Unload(response) = output else {
        panic!("expected unload output");
    };
    assert!(response.status.ok, "{response:?}");
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}


/// THE RUNTIME'S MUTEXES HAVE ONE GLOBAL ORDER, AND HERE IS THE NUMBER.
///
/// `DataNodeRuntimeInner` declares thirteen mutexes. Nothing stops two of them being held at
/// once, and several paths do hold two or three: `cancel_job` holds `jobs` then `queue` and
/// charges `stats` inside both, `submit` holds `queue` across the two rejection paths that
/// charge `stats`, and `stats()` holds three at once so its report is one cut rather than three
/// readings taken at three instants.
///
/// Two of those paths taking the same pair in OPPOSITE orders is a deadlock with no symptom
/// until it happens and no recovery when it does -- and it is invisible to every test that
/// exercises one path at a time, which is every test there was. `stats()` took
/// stats -> dirty -> queue while `submit` took queue -> stats, so a reporting thread holding
/// `stats` waited for `queue` while a submitting thread holding `queue` waited for `stats`.
/// Both threads exist in a running data node and both calls sit on request paths.
///
/// So this walks the source and asserts a PROPERTY rather than an example: the directed graph of
/// "lock A was held when lock B was taken" edges has no cycle. A cycle of length two is the
/// deadlock above; a longer cycle is the same deadlock with more threads in it. Acyclic means
/// some global order exists that every site respects, which is the only thing that makes holding
/// two of these at once safe.
///
/// NO RANKING IS HAND-ASSIGNED HERE, deliberately. A test that declared "queue outranks stats"
/// would assert the fix rather than the property, and would need editing every time a lock is
/// added -- at which point it asserts whatever the editor believed that day. Cycle-freedom needs
/// no list to maintain and fails for a lock that does not exist yet.
///
/// WHAT THIS CANNOT SEE, stated so a pass is not read as more than it is: it reads the three
/// files where `self.inner.<field>.lock()` / `runtime.inner.<field>.lock()` appears, so a path
/// reaching one of these mutexes through a helper that takes `&DataNodeRuntimeInner` is not
/// followed, and neither is a lock held across a channel hand-off. The denominators below say
/// how many acquisitions and edges were actually seen, so a scan that silently stopped matching
/// fails here instead of reading exactly like a clean run.
#[test]
fn the_data_node_runtime_locks_have_one_global_order() {
    const SOURCES: [(&str, &str); 3] = [
        ("data_node.rs", include_str!("../../data_node.rs")),
        (
            "data_node/runtime_reports.rs",
            include_str!("../runtime_reports.rs"),
        ),
        (
            "data_node/runtime_storage.rs",
            include_str!("../runtime_storage.rs"),
        ),
    ];

    // One held guard. `depth` is the brace depth it was taken at, so leaving that block drops it;
    // `name` is what it is bound to, so an explicit `drop(name)` ends it early.
    struct Held {
        field: String,
        line: usize,
        depth: i64,
        name: String,
    }

    // (outer field, inner field, file, line the outer was taken, line the inner was taken)
    let mut edges: Vec<(String, String, &str, usize, usize)> = Vec::new();
    let mut acquisitions = 0usize;

    for (file, source) in SOURCES {
        // Collapse a chain written one method per line onto the line it starts on, so
        // `self\n    .inner\n    .queue\n    .lock()` matches as one acquisition. Without this
        // step the scan matches almost nothing in this crate -- and a scan that matched nothing
        // reads exactly like a clean one, which is what the denominator assertions below exist
        // to catch.
        let raw: Vec<&str> = source.lines().collect();
        let mut joined: Vec<String> = raw.iter().map(|line| (*line).to_string()).collect();
        let mut owner: Vec<usize> = (0..raw.len()).collect();
        for index in 1..raw.len() {
            if raw[index].trim_start().starts_with('.') {
                let target = owner[index - 1];
                let continuation = raw[index].trim_start().to_string();
                joined[target].push_str(&continuation);
                joined[index].clear();
                owner[index] = target;
            }
        }

        let mut held: Vec<Held> = Vec::new();
        let mut depth: i64 = 0;
        for (index, line) in joined.iter().enumerate() {
            let trimmed = line.trim_start();

            // A new function body resets the stack. Brace depth alone does not: every method of
            // an `impl` block sits at the same depth as its siblings.
            if trimmed.starts_with("fn ")
                || trimmed.starts_with("pub fn ")
                || trimmed.starts_with("pub(crate) fn ")
                || trimmed.starts_with("pub(super) fn ")
                || trimmed.starts_with("async fn ")
            {
                held.clear();
            }
