// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn client_meta_sync_report_tracks_success_and_table_errors() {
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                    if req.table_name == "bad" {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::error("not_found", "missing table"),
                                table: None,
                                shards: Vec::new(),
                                unchanged: false,
                            },
                        );
                    }
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 11,
                                namespace: req.namespace,
                                table_name: req.table_name,
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 9,
                                first_shard_id: 3,
                                shard_count: 2,
                                replica_count: 1,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            shards: Vec::new(),
                            unchanged: false,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 25,
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert_eq!(table.options().first_shard_id, 3);
    let err = client.sync_table_topology("ns", "bad").unwrap_err();
    assert!(err.to_string().contains("missing table"));

    let report = client.meta_sync_report();
    assert_eq!(report.table_count, 2);
    assert_eq!(report.synced_table_count, 1);
    assert_eq!(report.error_table_count, 1);
    assert_eq!(report.total_sync_generation, 2);
    let good = report
        .tables
        .iter()
        .find(|table| table.table == "ns/tbl")
        .unwrap();
    assert_eq!(good.sync_generation, 1);
    assert_eq!(good.last_topology_version, 9);
    assert_eq!(good.consecutive_errors, 0);
    assert!(good.last_success_unix_ms > 0);
    assert!(good.next_sync_after_unix_ms >= good.last_success_unix_ms);
    let bad = report
        .tables
        .iter()
        .find(|table| table.table == "ns/bad")
        .unwrap();
    assert_eq!(bad.sync_generation, 1);
    assert_eq!(bad.consecutive_errors, 1);
    assert_eq!(bad.last_error, "missing table");
    assert!(bad.last_error_unix_ms > 0);
    assert!(bad.next_sync_after_unix_ms > bad.last_error_unix_ms);

    let preflight = client.preflight_report();
    assert_eq!(preflight.meta_sync.error_table_count, 1);
    assert!(preflight
        .degraded_reasons
        .contains(&"meta_sync_table_errors".to_string()));
}

// shared-corpus: control_client_metasync_outage_churn_stress
#[test]
fn client_metasync_backoff_deadline_and_topology_refresh_survive_outage_churn() {
    let meta_addr = free_local_addr();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let meta_addr_for_listener = meta_addr.clone();
    let attempts_for_listener = std::sync::Arc::clone(&attempts);
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &crate::meta::TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 40,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 1,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 1,
                        normal_servers: 1,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                ("POST", "/tables/topology") => {
                    let attempt =
                        attempts_for_listener.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt < 2 {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::error("metaserver_unavailable", "outage"),
                                table: None,
                                shards: Vec::new(),
                                unchanged: false,
                            },
                        );
                    }
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(TableMetaInfo {
                                table_id: 91,
                                namespace: "ns".to_string(),
                                table_name: "churn".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: 40,
                                first_shard_id: 40,
                                shard_count: 1,
                                replica_count: 1,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            shards: vec![TableShard {
                                load_version: 0,
                                shard_id: 40,
                                start_bucket: 0,
                                end_bucket: 1_073_741_823,
                                primary: Some("127.0.0.1:27440".to_string()),
                                replicas: vec!["127.0.0.1:27440".to_string()],
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 50,
        topo_error_retry_interval_ms: 5,
        meta_sync_deadline_ms: 7,
        meta_sync_jitter_percent: 50,
        ..ClientOptions::default()
    });
    client.open_table(
        "ns",
        "churn",
        TableOptions {
            first_shard_id: 10,
            shard_count: 1,
            ..TableOptions::default()
        },
    );

    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let first_error = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(first_error.consecutive_errors, 1);
    assert_eq!(first_error.last_error, "outage");
    let first_delay = first_error
        .next_sync_after_unix_ms
        .saturating_sub(first_error.last_error_unix_ms);
    assert!((5..=8).contains(&first_delay));

    std::thread::sleep(Duration::from_millis(12));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let second_error = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(second_error.consecutive_errors, 2);
    let second_delay = second_error
        .next_sync_after_unix_ms
        .saturating_sub(second_error.last_error_unix_ms);
    assert!((10..=15).contains(&second_delay));

    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        client.run_due_meta_sync_once(ClientMetaSyncLoopOptions {
            tick_ms: 1,
            max_tables_per_tick: 1,
        }),
        1
    );
    let success = client
        .meta_sync_report()
        .tables
        .into_iter()
        .find(|table| table.table == "ns/churn")
        .unwrap();
    assert_eq!(success.consecutive_errors, 0);
    assert_eq!(success.last_topology_version, 40);
    assert!(success.next_sync_after_unix_ms > success.last_success_unix_ms);
    let table = client.cached_table("ns", "churn").unwrap();
    assert_eq!(table.options().first_shard_id, 40);
    assert_eq!(client.topology_cache_report().max_topology_version, 40);
}

#[test]
fn failure_driven_topology_syncs_are_spaced_out() {
    // A request that fails in a way suggesting the cached topology is wrong triggers a
    // re-sync. The guard on that is per REQUEST, which says nothing about the other requests
    // in flight -- and a shard moving fails many at once. Unthrottled, each one is its own
    // metaserver round-trip: a sync storm aimed at the metaserver exactly while it is working
    // through the topology change that caused the failures. One failure is enough to learn
    // what all of them need.
    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some("127.0.0.1:1".to_string()),
        topo_error_retry_interval_ms: 5_000,
        ..ClientOptions::default()
    });

    assert!(
        client.forced_sync_is_due("ns", "tbl"),
        "the first failure should learn the topology"
    );
    for attempt in 0..8 {
        assert!(
            !client.forced_sync_is_due("ns", "tbl"),
            "concurrent failure {attempt} must not each fire their own sync"
        );
    }

    // Per table, not global: another table's failure is its own question.
    assert!(
        client.forced_sync_is_due("ns", "other"),
        "a different table must not be throttled by this one"
    );

    // Zero disables the spacing, which is the older behaviour, kept reachable.
    let eager = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some("127.0.0.1:1".to_string()),
        topo_error_retry_interval_ms: 0,
        ..ClientOptions::default()
    });
    for attempt in 0..4 {
        assert!(
            eager.forced_sync_is_due("ns", "tbl"),
            "with spacing off, attempt {attempt} should still sync"
        );
    }
}

#[test]
fn a_second_sync_asks_only_for_what_changed_and_keeps_its_routes() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // The request has always carried old_topology_version and the metaserver has always
    // answered "unchanged" when it is current -- with the shard list omitted, because there is
    // no point rebuilding what the caller already has. The client hardcoded 0, so that answer
    // could never be given: every sync of every table rebuilt and shipped the whole shard list.
    //
    // It could not simply start sending the version either. An unchanged reply carries no
    // shards, and the route surgery would have read that as "this table has no shards" and
    // deleted them all. Both halves are needed, in that order.
    let seen_versions = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let builds = std::sync::Arc::new(AtomicUsize::new(0));
    let seen_version_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let versions = seen_versions.clone();
    let built = builds.clone();
    let version_calls = seen_version_calls.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    let req = parse_json::<crate::meta::GetTableTopologyRequest>(&request.body)
                        .unwrap();
                    versions.lock().unwrap().push(req.old_topology_version);
