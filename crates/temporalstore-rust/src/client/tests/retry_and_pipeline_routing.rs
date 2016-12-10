// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn table_write_refreshes_due_topology_before_network() {
    let data_addr = free_local_addr();
    let observed_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    std::thread::spawn({
        let data_addr = data_addr.clone();
        let observed_shard = std::sync::Arc::clone(&observed_shard);
        move || {
            serve(&data_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/execute") => {
                        let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                        observed_shard.store(req.shard_id, std::sync::atomic::Ordering::Relaxed);
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::ok(),
                                response: CommandResponse::Empty,
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        }
    });
    wait_for_http(&data_addr);

    let meta_addr = free_local_addr();
    let first_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(10));
    std::thread::spawn({
        let meta_addr = meta_addr.clone();
        let data_addr = data_addr.clone();
        let first_shard = std::sync::Arc::clone(&first_shard);
        move || {
            serve(&meta_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => {
                        let first_shard_id = first_shard.load(std::sync::atomic::Ordering::Relaxed);
                        json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(crate::meta::TableMetaInfo {
                                    table_id: 1,
                                    namespace: "ns".to_string(),
                                    table_name: "tbl".to_string(),
                                    state: crate::meta::MetaEntityState::Normal,
                                    topology_version: first_shard_id,
                                    first_shard_id,
                                    shard_count: 1,
                                    replica_count: 1,
                                    partition_version: 0,
                                    serving_options: crate::meta::TableServingOptions::default(),
                                }),
                                shards: vec![crate::meta::TableShard {
                                    load_version: 0,
                                    shard_id: first_shard_id,
                                    start_bucket: 0,
                                    end_bucket: u64::MAX,
                                    primary: Some(data_addr.clone()),
                                    replicas: vec![data_addr.clone()],
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
        }
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 1,
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert_eq!(table.options().first_shard_id, 10);
    first_shard.store(20, std::sync::atomic::Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(5));

    table.set("stale-write", b"value".to_vec()).unwrap();
    assert_eq!(
        observed_shard.load(std::sync::atomic::Ordering::Relaxed),
        20
    );
    assert_eq!(table.options().first_shard_id, 20);
}

#[test]
fn client_drop_percent_sampler_is_deterministic_and_bounded() {
    assert!(!key_is_dropped_by_percent("k", 0));
    assert!(key_is_dropped_by_percent("k", 100));
    assert_eq!(
        key_is_dropped_by_percent("stable-key", 17),
        key_is_dropped_by_percent("stable-key", 17)
    );
    assert_eq!(
        key_is_dropped_by_percent("k", 255),
        key_is_dropped_by_percent("k", 100)
    );
}

#[test]
fn client_retries_retryable_read_status_before_returning() {
    let proxy_addr = free_local_addr();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_server = attempts.clone();
    let proxy_addr_for_listener = proxy_addr.clone();
    std::thread::spawn(move || {
        serve(&proxy_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let attempt =
                        attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::error("retry_later", "loading"),
                                response: CommandResponse::Empty,
                            },
                        )
                    } else {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::ok(),
                                response: CommandResponse::Bytes {
                                    value: Some(b"ok".to_vec()),
                                },
                            },
                        )
                    }
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&proxy_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: proxy_addr.clone(),
        ..ClientOptions::default()
    });
    let table = client.open_table(
        "ns",
        "tbl",
        TableOptions {
            retry_backoff_ms: 0,
            ..TableOptions::default()
        },
    );

    assert_eq!(table.get("retry-key").unwrap(), Some(b"ok".to_vec()));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn client_does_not_retry_write_status_without_write_retry_budget() {
    let proxy_addr = free_local_addr();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_server = attempts.clone();
    let proxy_addr_for_listener = proxy_addr.clone();
    std::thread::spawn(move || {
        serve(&proxy_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    json_response(
                        200,
                        &ExecuteResponse {
                            status: Status::error("retry_later", "write loading"),
                            response: CommandResponse::Empty,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&proxy_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: proxy_addr.clone(),
        ..ClientOptions::default()
    });
    let table = client.open_table("ns", "tbl", TableOptions::default());

    let err = table.set("retry-write", b"v".to_vec()).unwrap_err();
    assert!(err.to_string().contains("write loading"));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn a_write_refused_while_its_shard_is_loading_is_worth_asking_again() {
    // The data node refuses a foreground write while its shard is loading, reloading or
    // unloading and answers lifecycle_write_blocked. Each of those states ends on its own --
    // the shard finishes and serves -- and the write is refused BEFORE it executes, so asking
    // again cannot duplicate anything. The client treated it as fatal, so a rebalance, a
    // restart or a dump/load cycle surfaced to the caller as failed writes, while the sibling
    // conditions it already retries (partition_loading, shard_not_loaded) say the same thing.
    let blocked = classify_retry_decision(
        &Status::error("lifecycle_write_blocked", "shard 1 is reloading for dump"),
        true,
        0,
        2,
        false,
    );
    assert!(
        blocked.retryable,
        "a shard that is loading will finish loading"
    );
    assert!(
        blocked.would_retry,
        "with retry budget in hand the client should ask again"
    );
    assert!(
        !blocked.topology_retry,
        "the shard has not moved, so re-resolving the route would name the same node"
    );

    // Budget still governs it. The budget-free retry is only for rejections that prove the
    // write never reached the shard, and this one is not routed through that path.
    let no_budget = classify_retry_decision(
        &Status::error("lifecycle_write_blocked", "shard 1 is reloading for dump"),
        true,
        0,
        1,
        false,
    );
    assert!(
        !no_budget.would_retry,
        "a write with no budget left must not repeat itself"
    );
}

#[test]
fn client_retry_classifier_separates_safe_topology_retry_from_unsafe_write_retry() {
    let unsafe_write_retry = classify_retry_decision(
        &Status::error("retry_later", "possibly applied"),
        true,
        0,
        1,
        false,
    );
    assert!(unsafe_write_retry.retryable);
    assert!(!unsafe_write_retry.topology_retry);
    assert!(!unsafe_write_retry.safe_budget_free_write_retry);
    assert!(
        !unsafe_write_retry.would_retry,
        "write retry without budget must not duplicate a possibly applied write"
    );

    let safe_topology_retry = classify_retry_decision(
        &Status::error("meta_changed", "not applied on stale route"),
        true,
        0,
        1,
        false,
    );
    assert!(safe_topology_retry.retryable);
    assert!(safe_topology_retry.topology_retry);
    assert!(safe_topology_retry.safe_budget_free_write_retry);
    assert!(
        safe_topology_retry.would_retry,
        "stale topology rejection may refresh and retry once even with no write retry budget"
    );

    let duplicate_topology_retry = classify_retry_decision(
        &Status::error("meta_changed", "still stale"),
        true,
        1,
        1,
        true,
    );
    assert!(!duplicate_topology_retry.safe_budget_free_write_retry);
    assert!(
        !duplicate_topology_retry.would_retry,
        "budget-free topology retry is intentionally single-shot"
    );
}

#[test]
fn client_background_meta_sync_updates_existing_table_handle() {
    let meta_addr = free_local_addr();
    let first_shard = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(10));
    std::thread::spawn({
        let first_shard = std::sync::Arc::clone(&first_shard);
        let meta_addr = meta_addr.clone();
        move || {
            serve(&meta_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/tables/topology") => {
                        let first_shard_id = first_shard.load(std::sync::atomic::Ordering::Relaxed);
                        json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(crate::meta::TableMetaInfo {
                                    table_id: 1,
                                    namespace: "ns".to_string(),
                                    table_name: "tbl".to_string(),
                                    state: crate::meta::MetaEntityState::Normal,
                                    topology_version: first_shard_id,
                                    first_shard_id,
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
        }
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        meta_addr: Some(meta_addr),
        meta_sync_interval_ms: 10,
        topo_error_retry_interval_ms: 5,
        ..ClientOptions::default()
    });
    let table = client.open_table_from_meta("ns", "tbl").unwrap();
    assert!((10..12).contains(&table.shard_id_for_key("k")));
    first_shard.store(20, std::sync::atomic::Ordering::Relaxed);
    let syncer = client.start_meta_sync_loop_handle(ClientMetaSyncLoopOptions {
        tick_ms: 5,
        max_tables_per_tick: 1,
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let shard = table.shard_id_for_key("k");
        if (20..22).contains(&shard) {
            assert!(client.stats().meta_sync_total >= 2);
            syncer.stop_and_join().unwrap();
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    syncer.stop_and_join().unwrap();
    panic!("client meta sync loop did not refresh table options");
}

#[test]
fn table_routes_keys_to_shards_and_pipeline_splits_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.load_shard(2);
    let server_addr = free_local_addr();
