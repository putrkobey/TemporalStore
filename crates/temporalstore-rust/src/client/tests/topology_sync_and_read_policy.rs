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
                    let table = crate::meta::TableMetaInfo {
                        table_id: 1,
                        namespace: "ns".to_string(),
                        table_name: "tbl".to_string(),
                        state: crate::meta::MetaEntityState::Normal,
                        topology_version: 7,
                        first_shard_id: 1,
                        shard_count: 1,
                        replica_count: 1,
                        partition_version: 0,
                        serving_options: crate::meta::TableServingOptions::default(),
                    };
                    // Exactly what the metaserver does: current version in, no shards back.
                    if req.old_topology_version >= table.topology_version {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(table),
                                shards: Vec::new(),
                                unchanged: true,
                            },
                        );
                    }
                    built.fetch_add(1, Ordering::SeqCst);
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(table),
                            shards: vec![TableShard {
                                load_version: 0,
                                shard_id: 1,
                                start_bucket: 0,
                                end_bucket: u64::MAX,
                                primary: Some("127.0.0.1:29101".to_string()),
                                replicas: Vec::new(),
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                // Only the CALL is interesting here. What comes back does not matter: an
                // unparsed answer just falls back to the table's own version, and what this
                // test measures is whether the call is made at all.
                ("POST", "/meta/topology_version") => {
                    version_calls.fetch_add(1, Ordering::SeqCst);
                    json_response(503, &Status::error("unavailable", "not part of this test"))
                }
                _ => json_response(404, &Status::error("not_found", "no route")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: meta_addr.clone(),
        meta_addr: Some(meta_addr.clone()),
        ..ClientOptions::default()
    });

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("first sync succeeds");
    assert_eq!(client.topology_cache_report().route_count, 1);
    let version_calls_after_first = seen_version_calls.load(Ordering::SeqCst);

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("second sync succeeds");

    let versions = seen_versions.lock().unwrap().clone();
    assert_eq!(
        versions.first().copied(),
        Some(0),
        "the first sync has nothing to declare"
    );
    assert_eq!(
        versions.get(1).copied(),
        Some(7),
        "the second sync must declare what it already has, or the metaserver cannot skip work"
    );
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "the shard list should be built once, not once per sync"
    );
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "an unchanged reply carries no shards and must not be read as having none"
    );
    assert_eq!(
        seen_version_calls.load(Ordering::SeqCst),
        version_calls_after_first,
        "a sync that changed nothing should cost one round-trip, not two -- the \
         cluster topology version is only needed to stamp routes, and an \
         unchanged reply installs none"
    );
}

#[test]
fn a_topology_missing_a_primary_keeps_the_route_it_cannot_replace() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A topology snapshot taken while a primary is being elected names the shard but gives it
    // no primary. That entry used to be dropped, AND the purge that precedes the insert
    // removed the shard's previous route -- so a working route was destroyed on the strength
    // of an incomplete snapshot, and the sync reported success. The route is now kept, and
    // the sync says how many shards it could not route.
    let round = std::sync::Arc::new(AtomicUsize::new(0));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let r = round.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    // First sync: a healthy topology. Second: the same shard, no primary.
                    let healthy = r.fetch_add(1, Ordering::SeqCst) == 0;
                    json_response(
                        200,
                        &TableTopologyResponse {
                            status: Status::ok(),
                            table: Some(crate::meta::TableMetaInfo {
                                table_id: 1,
                                namespace: "ns".to_string(),
                                table_name: "tbl".to_string(),
                                state: crate::meta::MetaEntityState::Normal,
                                topology_version: if healthy { 7 } else { 8 },
                                first_shard_id: 1,
                                shard_count: 1,
                                replica_count: 1,
                                partition_version: 0,
                                serving_options: crate::meta::TableServingOptions::default(),
                            }),
                            shards: vec![TableShard {
                                load_version: 0,
                                shard_id: 1,
                                start_bucket: 0,
                                end_bucket: u64::MAX,
                                primary: if healthy {
                                    Some("127.0.0.1:29001".to_string())
                                } else {
                                    None
                                },
                                replicas: Vec::new(),
                                primary_endpoint: None,
                                replica_endpoints: Vec::new(),
                            }],
                            unchanged: false,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "no route")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: meta_addr.clone(),
        meta_addr: Some(meta_addr.clone()),
        ..ClientOptions::default()
    });

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("first sync succeeds");
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "the healthy topology should install a route"
    );

    client
        .sync_table_topology("ns".to_string(), "tbl".to_string())
        .expect("second sync still succeeds");
    assert_eq!(
        client.topology_cache_report().route_count,
        1,
        "a topology with no primary must not delete the route it cannot replace"
    );

    let synced = client.meta_sync_report();
    let table = synced
        .tables
        .iter()
        .find(|table| table.table_name == "tbl")
        .expect("the table is reported");
    assert_eq!(
        table.shards_without_primary, 1,
        "an incomplete sync must say so rather than reporting a clean success"
    );
}

#[test]
fn a_tables_timeouts_reach_the_wire_after_they_change() {
    // The table handle keeps a copy of the options it was opened with, and the client
    // keeps the live ones, refreshed on every sync. Routing already reads the live
    // copy; the request timeouts did not. So a table whose timeouts changed reported
    // the new ones through `options()` while still putting the old ones on the wire,
    // for the whole life of the handle.
    use std::sync::atomic::{AtomicU64, Ordering};
    let io_ms = std::sync::Arc::new(AtomicU64::new(1111));
    let connect_ms = std::sync::Arc::new(AtomicU64::new(2222));
    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let io_for_server = io_ms.clone();
    let connect_for_server = connect_ms.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(crate::meta::TableMetaInfo {
                            table_id: 1,
                            namespace: "ns".to_string(),
                            table_name: "tbl".to_string(),
                            state: crate::meta::MetaEntityState::Normal,
                            topology_version: 7,
                            first_shard_id: 10,
                            shard_count: 1,
                            replica_count: 1,
                            partition_version: 0,
                            serving_options: crate::meta::TableServingOptions {
                                io_timeout_ms: io_for_server.load(Ordering::Relaxed),
                                connect_timeout_ms: connect_for_server.load(Ordering::Relaxed),
                                ..crate::meta::TableServingOptions::default()
                            },
                        }),
                        shards: Vec::new(),
                        unchanged: false,
                    },
                ),
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert_eq!(table.http_options_for_test().io_timeout_ms, 1111);
    assert_eq!(table.http_options_for_test().connect_timeout_ms, 2222);

    io_ms.store(3333, Ordering::Relaxed);
    connect_ms.store(4444, Ordering::Relaxed);
    client.sync_table_topology("ns", "tbl").unwrap();

    assert_eq!(
        table.options().io_timeout_ms,
        3333,
        "the handle reports the refreshed timeout"
    );
    assert_eq!(
        table.http_options_for_test().io_timeout_ms,
        3333,
        "and must actually send it: reporting one timeout while using another is the bug"
    );
    assert_eq!(table.http_options_for_test().connect_timeout_ms, 4444);
}

#[test]
fn a_batch_is_split_across_shards_the_table_gained_after_it_was_opened() {
    // Whether a batch is grouped by shard was decided from the shard count the handle
    // was opened with. A table that gains shards afterwards kept sending every command
    // to the one shard the handle started on -- while `shard_id_for_key`, on the same
    // handle, already knew the right shard for each key. The handle worked out the
    // correct answer and then declined to use it.
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    let shard_count = std::sync::Arc::new(AtomicU64::new(1));
    let seen: std::sync::Arc<Mutex<Vec<ShardId>>> = std::sync::Arc::new(Mutex::new(Vec::new()));

    let backend_addr = free_local_addr();
    let backend_for_listener = backend_addr.clone();
    let seen_for_server = seen.clone();
    std::thread::spawn(move || {
        serve(&backend_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/batch_execute") => {
                    let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                    seen_for_server.lock().unwrap().push(req.shard_id);
                    json_response(
                        200,
                        &BatchExecuteResponse {
                            status: Status::ok(),
                            responses: req
                                .commands
                                .iter()
                                .map(|_| ExecuteResponse {
                                    status: Status::ok(),
                                    response: CommandResponse::Empty,
                                })
                                .collect(),
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&backend_addr);

    let meta_addr = free_local_addr();
    let meta_addr_for_listener = meta_addr.clone();
    let count_for_server = shard_count.clone();
    let backend_for_meta = backend_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => {
                    let count = count_for_server.load(Ordering::Relaxed);
                    let shards = (0..count)
                        .map(|offset| crate::meta::TableShard {
                            load_version: 0,
                            shard_id: 10 + offset,
                            start_bucket: offset * 4096,
                            end_bucket: (offset + 1) * 4096 - 1,
                            primary: Some(backend_for_meta.clone()),
                            replicas: Vec::new(),
                            primary_endpoint: None,
                            replica_endpoints: Vec::new(),
                        })
                        .collect();
                    json_response(
                        200,
                        &TableTopologyResponse {
