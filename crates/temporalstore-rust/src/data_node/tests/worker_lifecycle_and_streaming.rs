// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn runtime_reports_style_shard_worker_ownership() {
    let runtime = DataNodeRuntime::new(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 4,
            max_queue_depth: 16,
            max_background_queue_depth: 8,
        },
    );

    let stats = runtime.stats();
    assert_eq!(stats.worker_threads, 4);
    assert_eq!(stats.max_queue_depth, 16);
    assert_eq!(stats.max_background_queue_depth, 8);
    assert_eq!(
        runtime.shard_worker_info(7),
        ShardWorkerInfo {
            shard_id: 7,
            worker_index: 3,
            worker_threads: 4,
        }
    );
}

#[test]
fn runtime_lifecycle_report_counts_loaded_readonly_and_preflight() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 19,
                readonly: true,
                load_version: 42,
                local_node_id: Some(3),
            })
            .status
            .ok
    );
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 1);
    assert_eq!(lifecycle.serving_count, 0);
    assert_eq!(lifecycle.readonly_count, 1);
    assert_eq!(lifecycle.queued_count, 0);
    assert_eq!(lifecycle.running_count, 0);
    assert_eq!(lifecycle.unloading_count, 0);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.max_load_version, 42);
    assert_eq!(lifecycle.shards.len(), 1);
    assert_eq!(lifecycle.shards[0].shard_id, 7);
    assert_eq!(lifecycle.shards[0].serving_state, "readonly");
    assert!(lifecycle.shards[0].readonly);
    assert_eq!(lifecycle.shards[0].table_name, "tbl");
    assert_eq!(lifecycle.transitions.len(), 0);

    let preflight = runtime.preflight_report();
    assert_eq!(preflight.lifecycle, lifecycle);
}

#[test]
fn runtime_load_reload_unload_records_lifecycle_transitions() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.serving_count, 1);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions.len(), 1);
    assert_eq!(lifecycle.transitions[0].state, "serving");
    assert_eq!(lifecycle.transitions[0].operation, "load");
    assert_eq!(lifecycle.transitions[0].load_version, 42);

    let stale = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "stale".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 20,
        end_routing_bucket: 29,
