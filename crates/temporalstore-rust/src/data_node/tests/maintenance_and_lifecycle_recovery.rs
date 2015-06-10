// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

/// Every maintenance phase is on under the options a server actually starts with.
///
/// The server passes `StorageManagerOptions::default()` and prints a banner naming seven phases.
/// The banner is a string; this checks the report.
///
/// Asserted on the SUFFIX rather than against a list of names, because the failure this guards
/// against is a default going quiet, and the worst version of that is a phase added later whose
/// default is off -- which a fixed list would not mention. Any stage the runtime records as
/// `<name>_disabled` fails this, including one that does not exist yet.
///
/// The second half is the other way to be wrong: a phase that stops reporting at all. A stage
/// missing from both lists is not "off", it is absent, and the first assertion cannot see it.
#[test]
fn every_maintenance_phase_is_enabled_under_the_shipped_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Real content, so a phase that declines for want of work is distinguishable from one that
    // declines because it is switched off.
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("phase-{index:03}"),
                value: vec![b'v'; 64],
            },
        });
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());

    let disabled = report
        .skipped_stages
        .iter()
        .filter(|stage| stage.ends_with("_disabled"))
        .cloned()
        .collect::<Vec<_>>();
    // NINE OF NINE STAGES NOW SHIP ON, and this list is EMPTY. It used to be
    // `vec!["evict_disabled"]`, and its comment read "so does `evict` being quietly switched on" --
    // which is the tooth that fired when the ninth stage was defaulted on, exactly as intended.
    // The assertion is restated rather than relaxed, and it keeps both teeth: any stage going off
    // still fails here, and so would a tenth stage arriving switched off.
    assert!(
        disabled.is_empty(),
        "phases switched off under the options the server ships with: {disabled:?}. All nine \
         stages ship enabled -- `evict` was the last, defaulted on together with the \
         `index_resident_limit_bytes` bound that keeps its gate shut on a shard under the bound \
         (executed: {:?}, skipped: {:?})",
        report.executed_stages,
        report.skipped_stages
    );
    // AND THE NINTH STAGE RAN BOUNDED, not merely "ran". A stage that is enabled and unbounded is
    // the thing the bound exists to prevent, and `executed_stages` alone cannot tell the two
    // apart -- it says "evict" either way. So read the report: the gate that opened must be the
    // bound, it must have declined on this 64-record shard, and it must have released nothing.
    let eviction = report
        .eviction
        .as_ref()
        .expect("the ninth stage ships on, so a shipped round must report it");
    assert_eq!(
        eviction.mode, "index_resident_bound",
        "the shipped round opened the WIDE eviction gate rather than the bound: mode={} \
         threshold={}. At the shipped threshold of 0 the wide gate cannot close, so this is an \
         always-open release on every loaded shard",
        eviction.mode, eviction.memory_pressure_threshold
    );
    assert_eq!(
        eviction.skipped_reason, "index_resident_below_bound",
        "a 64-record shard was not under the shipped bound: reason={:?} pressure_before={} \
         threshold={}",
        eviction.skipped_reason, eviction.pressure_before, eviction.memory_pressure_threshold
    );
    assert_eq!(
        eviction.bucket_index_buckets_released, 0,
        "the shipped round released {} buckets from a 64-record shard",
        eviction.bucket_index_buckets_released
    );

    // Every phase must have reported SOMETHING -- ran, or declined for a reason that is not
    // "disabled". A phase in neither list has gone missing from the cycle.
    for phase in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
        "evict",
    ] {
        let ran = report.executed_stages.iter().any(|stage| stage == phase);
        let declined = report
            .skipped_stages
            .iter()
            .any(|stage| stage.starts_with(phase));
        assert!(
            ran || declined,
            "{phase} appears in neither list, so the cycle no longer reports it \
             (executed: {:?}, skipped: {:?})",
            report.executed_stages,
            report.skipped_stages
        );
    }
}

/// A dump actually happens under the shipped default, once enough has accumulated to be worth
/// one.
///
/// Being ENABLED is not being REACHED. The default holds a dump back until
/// `min_undumped_wal_records` = 1000 records are undumped, so a handful of writes produces no
/// dump at all -- correctly, because the delay exists to let repeated writes to the same bucket
/// coalesce into one dumped generation. A guard that only checked the phase flags would pass on
/// a store that never dumps, which is the state this whole line of work started from.
///
/// So: write past the threshold, run one round with the options a server ships with, and require
/// that buckets were actually selected and a manifest written.
#[test]
fn a_dump_fires_under_the_shipped_default_once_the_threshold_is_crossed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Every default except the dump cap, which this fixture pins on purpose.
    //
    // A data-node round calls `apply_storage_lifecycle` more than once, and only the first one
    // that finds undumped records dumps -- the rest correctly see a threshold the dump reset.
    // So WHICH apply carries `dump_manifest` depends on how many buckets the first one covered,
    // and with the cap off the first one covers them all and the reported apply has nothing
    // left to do. Pinning the cap keeps this a test of the RECORD THRESHOLD, which is what its
    // name is about; `the_shipped_dump_cap_still_lets_the_log_be_reclaimed` is where the cap's
    // own behaviour is pinned.
    let options = StorageManagerOptions {
        max_dump_buckets_per_round: 64,
        ..StorageManagerOptions::default()
    };
    // Past the coalescing delay, and not by one: the threshold counts UNDUMPED records, so a
    // round that dumps resets it, and a fixture sitting exactly on the line would be deciding
    // the test on an off-by-one in the counter rather than on whether a dump happens.
    let writes = options.min_undumped_wal_records as usize + 256;
    for index in 0..writes {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dumped-{index:05}"),
                value: vec![b'v'; 32],
            },
        });
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let report = runtime.run_storage_manager_once(1, options);

    assert!(
        !report.lifecycle_plan.dump_delayed,
        "past {writes} writes the dump is still being delayed: {:?}",
        report.lifecycle_plan.undumped_wal_records
    );
    assert!(
        !report.lifecycle_plan.selected_dump_buckets.is_empty(),
        "the round selected no buckets to dump, so nothing was going to be written"
    );
    let lifecycle = report
        .lifecycle_report
        .as_ref()
        .expect("a round that selected buckets must report what it did with them");
    let manifest = lifecycle
        .dump_manifest
        .as_ref()
        .expect("selected buckets must produce a dump manifest");
    assert!(
        !manifest.bucket_ids.is_empty(),
        "the manifest names no buckets, so the dump captured nothing"
    );
}

/// A big log dumps on its SIZE, while the record count is still saying wait.
///
/// The record threshold cannot bound a log. A thousand hundred-byte records is a hundred
/// kilobytes and a thousand megabyte records is a gigabyte, and neither reaches 1000 records any
/// sooner than the other -- so a workload with large values holds the dump off across an
/// arbitrarily large log, and reclaim only follows a dump.
///
/// The fixture is the case the record count is blind to: FAR fewer records than the record
/// threshold, but past the byte threshold. If the byte threshold did nothing, the record count
/// alone would still be delaying, which is what the first assertion states.
#[test]
fn a_large_log_dumps_on_bytes_while_the_record_count_still_says_wait() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A byte threshold small enough to reach in a test, and a record threshold far out of reach,
    // so only the byte one can release the dump.
    //
    // 2 KiB, not the 64 KiB this asked for originally. The threshold now counts the bytes the
    // log has TAKEN since its last dump; it used to read `persistent_bytes`, which on a
    // preallocated segment is 262,144 from the very first record and never moves. So the old
    // 64 KiB was cleared by the preallocation, not by anything written -- the fixture below
    // holds about 3.7 KiB of records, and would have passed with no writes at all.
    let options = StorageManagerOptions {
        min_undumped_wal_records: 1_000_000,
        min_undumped_wal_bytes: 2 * 1024,
        ..StorageManagerOptions::default()
    };

    let records = 48;
    for index in 0..records {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("fat-{index:03}"),
                value: vec![b'v'; 4 * 1024],
            },
        });
    }
    assert!(
        (records as u64) < options.min_undumped_wal_records,
        "the fixture must stay far below the record threshold, or it proves nothing"
    );

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let report = runtime.run_storage_manager_once(1, options.clone());

    assert!(
        !report.lifecycle_plan.dump_delayed,
        "the log is past {} bytes and still delayed, so the byte threshold did nothing",
        options.min_undumped_wal_bytes
    );
    assert!(
        !report.lifecycle_plan.selected_dump_buckets.is_empty(),
        "released, but no bucket was selected, so nothing would be written"
    );

    // The control: the same store, the same records, with only the byte threshold taken away.
    // Now nothing can release the dump and it must be delayed -- which is what proves the first
    // assertion was the BYTE threshold's doing and not the record count quietly being satisfied.
    let without_bytes = StorageManagerOptions {
        min_undumped_wal_bytes: 0,
        ..options.clone()
    };
    let control = runtime.run_storage_manager_once(1, without_bytes);
    assert!(
        control.lifecycle_plan.dump_delayed,
        "with no byte threshold the record count alone must still be delaying this dump"
    );

    // And the half that was missing: the threshold must track HOW MUCH, not merely that a log
    // exists. Set it above what this fixture wrote and the dump has to go back to being delayed.
    // Without this the test passes on any reading that is large for an unrelated reason -- which
    // is exactly how the preallocated segment size passed it before.
    let above_what_was_written = StorageManagerOptions {
        min_undumped_wal_bytes: 64 * 1024,
        ..options.clone()
    };
    let still_delayed = runtime.run_storage_manager_once(1, above_what_was_written);
    assert!(
        still_delayed.lifecycle_plan.dump_delayed,
        "a threshold above what the log has taken must delay the dump; releasing here means the \
         byte reading is not measuring this shard's log growth"
    );
}

/// The log's size threshold is set in the options a server actually starts with.
///
/// The test that proves the byte threshold WORKS passes its own options, so it cannot see the
/// shipped default at all -- zeroing that default left it green. This is the half that watches
/// production: a threshold nothing sets is a threshold that does not exist, which is the exact
/// shape of the seven round bounds that were implemented, enforced, tested and left at zero.
#[test]
fn the_shipped_default_bounds_the_log_by_size() {
    let options = StorageManagerOptions::default();
    assert_eq!(options.min_undumped_wal_bytes, 96 * 1024 * 1024);
    assert!(
        options.min_undumped_wal_bytes > 0,
        "with no byte threshold the record count alone decides, and a record count does not \
         bound a file"
    );
    // Both thresholds are set, because each bounds something the other cannot: records bound how
    // much replay a restart faces, bytes bound the file.
    assert!(options.min_undumped_wal_records > 0);
}

/// Switching WAL reclaim off stops the reclaim, however large the log is.
///
/// The two thresholds are alternatives for WHEN to dump, not for WHETHER to reclaim. With the
/// byte threshold crossed and reclaim disabled, no WAL reclaim may run.
///
/// Each arm gets its OWN store, because a round changes what the next round sees. Sharing one
/// failed twice, in opposite directions: with the disabled arm second it inherited an
/// already-reclaimed log and declined for want of work, so deleting the flag check changed
/// nothing; with it first, its own prepare consumed the dump pressure -- the dump is not gated
/// by this flag on this path -- and the control then had nothing left to reclaim. Two
/// experiments, two fixtures.
#[test]
fn switching_reclaim_off_stops_the_reclaim_however_large_the_log() {
    fn store_past_the_byte_threshold(
        dir: &tempfile::TempDir,
    ) -> DataNodeRuntime {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..48 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("suppressed-{index:03}"),
                    value: vec![b'v'; 4 * 1024],
                },
            });
        }
        DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        )
    }

    // Past the byte threshold and far short of the record one, so only the byte threshold can
    // release the dump that reclaim follows.
    // 2 KiB for the same reason as the test above: the byte threshold counts what the log has
    // taken since its last dump, and this fixture writes about 3.7 KiB of records. The 64 KiB
    // this asked for originally was cleared by the segment's preallocated size, not by writes.
    let reclaiming = StorageManagerOptions {
        min_undumped_wal_records: 1_000_000,
        min_undumped_wal_bytes: 2 * 1024,
        ..StorageManagerOptions::default()
    };
    let not_reclaiming = StorageManagerOptions {
        enable_wal_reclaim: false,
        ..reclaiming.clone()
    };

    let control_dir = tempfile::tempdir().unwrap();
    let control = store_past_the_byte_threshold(&control_dir)
        .run_storage_manager_once(1, reclaiming);
    assert!(
        !control.lifecycle_plan.dump_delayed,
        "the byte threshold must release this dump, or nothing below is demonstrated"
    );
    assert!(
        control.executed_stages.iter().any(|stage| stage == "reclaim_wal"),
        "the control must actually reclaim: {:?}",
        control.executed_stages
    );

    let suppressed_dir = tempfile::tempdir().unwrap();
    let suppressed = store_past_the_byte_threshold(&suppressed_dir)
        .run_storage_manager_once(1, not_reclaiming);
    assert!(
        !suppressed.executed_stages.iter().any(|stage| stage == "reclaim_wal"),
        "reclaim is off, so no WAL reclaim may run however large the log: {:?}",
        suppressed.executed_stages
    );
    assert!(
        suppressed
            .skipped_stages
            .iter()
            .any(|stage| stage == "reclaim_wal_disabled"),
        "and it must say it was disabled rather than silently doing nothing: {:?}",
        suppressed.skipped_stages
    );
}

/// The RUNNING scheduler reaches every maintenance phase, not just one round of it.
///
/// The guard beside this one proves a single round enables every phase. That is not the same
/// claim: a loop that ran once, or only ever visited its first shard, or died on the first error,
/// would satisfy it. This starts the scheduler the server starts, lets it tick, and requires that
/// every per-phase counter moved.
///
/// Asserted as "at least once each" rather than "once per loop". Measured over four loops:
/// prepare 4, reclaim_wal 1, reclaim_memory 3, expire 4, reclaim_page 4, compact 4, index_gc 4 --
/// reclaim_wal ran once because after it reclaimed there was nothing left, and reclaim_memory is
/// pressure-dependent. Pinning either to the loop count would pin this fixture's luck.
#[test]
fn the_running_scheduler_reaches_every_phase() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let options = StorageManagerOptions::default();
    // Pressure of several kinds, so a phase that declines for want of work is distinguishable
    // from one the loop never reaches: volume for the dump threshold, overwrites for stale
    // pages, deletes for tombstones, and a small cache so memory pressure is reachable.
    let writes = options.min_undumped_wal_records as usize + 256;
    for index in 0..writes {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("periodic-{index:05}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for index in 0..(writes / 2) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("periodic-{index:05}"),
                value: vec![b'w'; 96],
            },
        });
    }
    for index in 0..(writes / 4) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("periodic-{index:05}") },
        });
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let scheduler = runtime.start_storage_manager_scheduler_for_all_shards(
        std::time::Duration::from_millis(5),
        options,
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if runtime.stats().storage_manager_loops >= 4 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let stats = runtime.stats();
    drop(scheduler);

    assert!(
        stats.storage_manager_loops >= 4,
        "the scheduler only completed {} rounds, so nothing below is about periodic behaviour",
        stats.storage_manager_loops
    );
    for (phase, runs) in [
        ("prepare", stats.storage_manager_prepare_runs),
        ("reclaim_wal", stats.storage_manager_reclaim_wal_runs),
        ("reclaim_memory", stats.storage_manager_reclaim_memory_runs),
        ("expire", stats.storage_manager_expire_runs),
        ("reclaim_page", stats.storage_manager_reclaim_block_runs),
        ("compact", stats.storage_manager_compact_runs),
        ("index_gc", stats.storage_manager_index_gc_runs),
    ] {
        assert!(
            runs > 0,
            "{phase} never ran across {} scheduler rounds",
            stats.storage_manager_loops
        );
    }
}

#[test]
fn runtime_enforces_authorized_lifecycle_token_when_installed() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 12,
        shard_id: 7,
        operation: "load".to_string(),
        load_version: 43,
        generation: 900,
    });

    let stale = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert_eq!(stale.status.code, "lifecycle_token_mismatch");
    let failed = runtime.lifecycle_report();
    assert_eq!(failed.failed_count, 1);
    assert_eq!(failed.transitions[0].scheduler_task_id, Some(12));
    assert_eq!(failed.transitions[0].scheduler_generation, Some(900));

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions[0].state, "serving");
    assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(12));
    assert_eq!(lifecycle.transitions[0].scheduler_generation, Some(900));
}

#[test]
fn runtime_lifecycle_snapshot_restores_transitions_and_tokens() {
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 21,
        shard_id: 7,
        operation: "load".to_string(),
        load_version: 42,
        generation: 700,
    });
    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 22,
        shard_id: 7,
        operation: "reload".to_string(),
        load_version: 43,
        generation: 701,
    });
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
    let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl".to_string(),
        shard_uri: "local://tbl/7".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(reload.status.ok, "{reload:?}");

    let snapshot = runtime.lifecycle_snapshot();
    assert_eq!(snapshot.format_version, 1);
    assert_eq!(snapshot.tokens.len(), 2);
    assert_eq!(snapshot.transitions.len(), 1);
    assert_eq!(snapshot.transitions[0].operation, "reload");
    assert_eq!(snapshot.transitions[0].state, "readonly");
    assert_eq!(snapshot.transitions[0].scheduler_task_id, Some(22));

    let restored = DataNodeRuntime::new_without_workers_with_options(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    assert!(restored.restore_lifecycle_snapshot(snapshot.clone()).ok);
    assert_eq!(restored.lifecycle_snapshot(), snapshot);
    assert_eq!(restored.lifecycle_tokens(), snapshot.tokens);
    let lifecycle = restored.lifecycle_report();
    assert_eq!(lifecycle.loaded_shard_count, 0);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.max_load_version, 43);
    assert_eq!(lifecycle.transitions[0].operation, "reload");
    assert_eq!(lifecycle.transitions[0].state, "readonly");

    let stale_reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "stale".to_string(),
        shard_uri: "local://tbl/stale".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert_eq!(stale_reload.status.code, "lifecycle_token_mismatch");

    let good_reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 7,
        table_name: "tbl-restored".to_string(),
        shard_uri: "local://tbl/restored".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(good_reload.status.ok, "{good_reload:?}");
    let lifecycle = restored.lifecycle_report();
    assert_eq!(lifecycle.readonly_count, 1);
    assert_eq!(lifecycle.failed_count, 0);
    assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(22));
}

#[test]
fn runtime_auto_persists_lifecycle_snapshot_across_transitions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("data-node-lifecycle.json");
    let runtime = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );

    runtime.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 31,
        shard_id: 8,
        operation: "load".to_string(),
        load_version: 42,
        generation: 800,
    });
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.tokens.len(), 1);
    assert!(saved.transitions.is_empty());
    let report = runtime.lifecycle_persistence_report();
    assert!(report.enabled);
    assert_eq!(report.path.as_deref(), Some(path.to_str().unwrap()));
    assert_eq!(report.persist_success_total, 1);
    assert_eq!(report.persist_failure_total, 0);
    assert_eq!(report.last_persist_status.as_ref().unwrap().code, "ok");

    let load = runtime.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 8,
        table_name: "storage_lifecycle".to_string(),
        shard_uri: "local://storage-lifecycle/8".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: false,
        load_version: 42,
        local_node_id: Some(3),
    });
    assert!(load.status.ok, "{load:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.transitions[0].operation, "load");
    assert_eq!(saved.transitions[0].state, "serving");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(31));
    assert!(runtime.lifecycle_persistence_report().persist_success_total >= 3);

    let restored = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );
    assert_eq!(restored.lifecycle_snapshot(), saved);
    assert_eq!(restored.lifecycle_report().transitions[0].operation, "load");
    let report = restored.lifecycle_persistence_report();
    assert_eq!(report.restore_success_total, 1);
    assert_eq!(report.restore_failure_total, 0);
    assert_eq!(report.last_restore_status.as_ref().unwrap().code, "ok");

    restored.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 32,
        shard_id: 8,
        operation: "reload".to_string(),
        load_version: 43,
        generation: 801,
    });
    let reload = restored.reload_shard_with(crate::control::LoadShardRequest {
        shard_id: 8,
        table_name: "storage_lifecycle_reloaded".to_string(),
        shard_uri: "local://storage-lifecycle/8-reload".to_string(),
        start_routing_bucket: 10,
        end_routing_bucket: 19,
        readonly: true,
        load_version: 43,
        local_node_id: Some(3),
    });
    assert!(reload.status.ok, "{reload:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.transitions[0].operation, "reload");
    assert_eq!(saved.transitions[0].state, "readonly");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(32));

    restored.require_lifecycle_token(SchedulerLifecycleToken {
        task_id: 33,
        shard_id: 8,
        operation: "unload".to_string(),
        load_version: 43,
        generation: 802,
    });
    let unload = restored.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 8 });
    assert!(unload.status.ok, "{unload:?}");
    let saved: DataNodeLifecycleSnapshot =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved.tokens.len(), 3);
    assert_eq!(saved.transitions[0].operation, "unload");
    assert_eq!(saved.transitions[0].state, "unloaded");
    assert_eq!(saved.transitions[0].scheduler_task_id, Some(33));
}

#[test]
fn runtime_reports_bad_lifecycle_snapshot_restore_in_preflight() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad-data-node-lifecycle.json");
    fs::write(&path, b"{not json").unwrap();

    let runtime = DataNodeRuntime::new_without_workers_with_options_and_lifecycle_snapshot_path(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
        Some(path.clone()),
    );
    let report = runtime.lifecycle_persistence_report();
    assert!(report.enabled);
    assert_eq!(report.restore_success_total, 0);
    assert_eq!(report.restore_failure_total, 1);
    assert_eq!(
        report.last_restore_status.as_ref().unwrap().code,
        "bad_lifecycle_snapshot"
    );

    let preflight = runtime.preflight_report();
    assert_eq!(preflight.status.code, "degraded");
    assert!(preflight
        .degraded_reasons
        .contains(&"lifecycle_snapshot_restore_failed".to_string()));
    assert_eq!(preflight.lifecycle_persistence, report);
}

#[test]
fn runtime_lifecycle_snapshot_rejects_unknown_format() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    let status = runtime.restore_lifecycle_snapshot(DataNodeLifecycleSnapshot {
        format_version: 99,
        transitions: Vec::new(),
        tokens: Vec::new(),
    });
    assert_eq!(status.code, "bad_lifecycle_snapshot");
}

#[test]
fn runtime_async_lifecycle_jobs_report_progress_and_outputs() {
    let runtime = DataNodeRuntime::new(
        TemporalEngine::default(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 4,
        },
    );

    let submitted = runtime.submit_load(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");
    assert_eq!(submitted.kind, DataNodeTaskKind::Load);
    assert!(submitted.finished_at_ms.is_none());

    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok, "{finished:?}");
    let Some(DataNodeTaskOutput::Load(output)) = finished.output else {
        panic!("expected load output");
    };
    assert!(output.status.ok, "{output:?}");
    let lifecycle = runtime.lifecycle_report();
    assert_eq!(lifecycle.serving_count, 1);
    assert_eq!(lifecycle.transitions[0].operation, "load");
    assert_eq!(lifecycle.transitions[0].state, "serving");

    let reloaded = runtime.submit_reload(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl-new".to_string(),
            shard_uri: "local://tbl/7-new".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: true,
            load_version: 43,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    let reloaded = wait_for_job(&runtime, reloaded.job_id);
    let Some(DataNodeTaskOutput::Reload(output)) = reloaded.output else {
        panic!("expected reload output");
    };
    assert!(output.status.ok, "{output:?}");
    assert_eq!(runtime.lifecycle_report().readonly_count, 1);

    let unloaded = runtime.submit_unload(
        crate::control::UnloadShardRequest { shard_id: 7 },
        RequestController { timeout_ms: 1000 },
    );
    let unloaded = wait_for_job(&runtime, unloaded.job_id);
    let Some(DataNodeTaskOutput::Unload(output)) = unloaded.output else {
        panic!("expected unload output");
    };
    assert!(output.status.ok, "{output:?}");
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}

#[test]
fn runtime_rejects_foreground_writes_during_lifecycle_transition() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
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
    let seed = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(seed.status.ok, "{seed:?}");

    record_lifecycle_state_inner(&runtime.inner, 7, "reloading", "reload", 43, None);

    let write = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"blocked".to_vec(),
        },
    });
    assert_eq!(write.status.code, "lifecycle_write_blocked");
    let checked = runtime.execute_checked(CheckedExecuteRequest {
        shard_id: 7,
        load_version: 42,
        command: Command::StringDelete {
            key: "k".to_string(),
        },
    });
    assert_eq!(checked.status.code, "lifecycle_write_blocked");
    let batch = runtime.batch_execute(BatchExecuteRequest {
        shard_id: 7,
        commands: vec![Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        }],
    });
    assert_eq!(batch.status.code, "lifecycle_write_blocked");

    let read = runtime.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(read.status.ok, "{read:?}");
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
}

#[test]
fn runtime_rejects_queued_foreground_write_during_lifecycle_transition() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
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
    record_lifecycle_state_inner(&runtime.inner, 7, "unloading", "unload", 42, None);
    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let task = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("queued write should be ready");
    assert_eq!(task.job_id, submitted.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Execute(response) = output else {
        panic!("expected execute output");
    };
    assert_eq!(response.status.code, "lifecycle_write_blocked");
}

#[test]
fn runtime_executes_async_tracks_dirty_and_dump_clears_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, job.job_id);
    assert!(finished.status.ok);
    assert_eq!(runtime.dirty_objects().len(), 1);

    let dump = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, dump.job_id);
    let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
        panic!("expected dump output");
    };
    assert_eq!(output.dirty_objects_flushed, 1);
    assert!(runtime.dirty_objects().is_empty());
}

#[test]
fn runtime_dump_can_flush_only_selected_dirty_buckets() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut key_a = String::new();
    let mut key_b = String::new();
    let mut bucket_a = 0;
    for index in 0..128 {
        let key = format!("slot-key-{index}");
        let bucket = engine.routing_bucket_for_key(1, &key);
        if key_a.is_empty() {
            key_a = key;
            bucket_a = bucket;
        } else if bucket != bucket_a {
            key_b = key;
            break;
        }
    }
    assert!(!key_b.is_empty(), "test needs two distinct routing slots");
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    for key in [&key_a, &key_b] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }
    assert_eq!(runtime.dirty_objects().len(), 2);

    let dump = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: vec![bucket_a],
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, dump.job_id);
    let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
        panic!("expected dump output");
    };
    assert!(output.status.ok);
    assert_eq!(output.dirty_objects_flushed, 1);
    assert_eq!(
        output.bucket_dump_manifest.as_ref().unwrap().bucket_ids,
        vec![bucket_a]
    );
    let remaining = runtime.dirty_objects();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, key_b);
}

#[test]
fn previously_misclassified_writes_mark_shard_dirty() {
    // The data_node write classifier delegates to the engine's authoritative one, so writes it
    // used to omit (context / control-state change+fol / conditional
    // string) now correctly mark the shard dirty (and hit the lifecycle write gate). Regression:
    // a ControlStateSelectionSet -- previously classified READ here -- must mark the shard dirty.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateSelectionSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
                occur_time_ms: 1,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::First,
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, job.job_id).status.ok);
    assert_eq!(
        runtime.dirty_shards(),
        vec![1],
        "a control-state FOL write must be classified as a write and mark the shard dirty"
    );
}

#[test]
fn gc_does_not_clear_the_dirty_scheduling_tracker() {
    // GC (block/index reclaim) never touches the dirty-bucket set -- a bucket leaves it
    // only via a completed dump/replay that clears its dirty flag. GC must not drop the re-dump
    // scheduling state, or schedule_dirty_shard_dumps would stop scheduling those objects.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let write = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, write.job_id).status.ok);
    assert_eq!(runtime.dirty_shards(), vec![1]);

    let gc = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: None,
            block_gc_delayed_destroy: false,
            block_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, gc.job_id).status.ok);
    assert_eq!(
        runtime.dirty_shards(),
        vec![1],
        "GC must not clear the dirty-scheduling tracker (only a completed dump does)"
    );
}

#[test]
fn runtime_schedules_dumps_for_dirty_shards() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 2,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );

    for (shard_id, key) in [(1, "alpha"), (2, "beta")] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }

    assert_eq!(runtime.dirty_shards(), vec![1, 2]);
    let dumps = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
    assert_eq!(dumps.len(), 2);
    for dump in dumps {
        let finished = wait_for_job(&runtime, dump.job_id);
        let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
            panic!("expected dump output");
        };
        assert!(output.status.ok);
        assert_eq!(output.dirty_objects_flushed, 1);
    }
    assert!(runtime.dirty_objects().is_empty());
    assert!(runtime.dirty_shards().is_empty());
}

#[test]
fn runtime_preflight_reports_dirty_backlog_and_queue_degradation() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 1,
            max_background_queue_depth: 1,
        },
    );
    mark_dirty(&runtime.inner.dirty, 1, Some("dirty-key"));
    let first = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(first.status.ok);
    let rejected = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "queued".to_string(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert_eq!(rejected.status.code, "queue_full");

    let preflight = runtime.preflight_report();

    assert!(!preflight.status.ok);
    assert!(preflight
        .degraded_reasons
        .contains(&"foreground_queue_full".to_string()));
    assert!(preflight
        .degraded_reasons
        .contains(&"rejected_requests".to_string()));
    assert_eq!(preflight.stats.queue_depth, 1);
    assert_eq!(preflight.queued_workers.len(), 1);
    assert_eq!(preflight.dirty_shards, vec![1]);
    assert_eq!(preflight.dirty_objects.len(), 1);
}

#[test]
fn runtime_builds_style_server_load_report() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 19,
                readonly: false,
                load_version: 42,
                local_node_id: Some(3),
            })
            .status
            .ok
    );
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 4,
            max_queue_depth: 2,
            max_background_queue_depth: 1,
        },
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 7,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    mark_dirty(&runtime.inner.dirty, 7, Some("k"));
    let queued = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(queued.status.ok);

    let load = runtime.server_runtime_load();
    assert_eq!(load.queue_depth, 1);
    assert_eq!(load.queued_shard_count, 1);
    assert_eq!(load.dirty_object_count, 1);
    assert_eq!(load.last_meta_topology_version, 0);
    runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
        status: Status::error("resource_frozen", "server frozen"),
        forbid_auto_register: true,
        topology_version: 99,
        server_state: "frozen".to_string(),
    });
    let preflight = runtime.preflight_report();
    assert_eq!(preflight.metaserver.last_topology_version, 99);
    assert_eq!(preflight.metaserver.last_server_state, "frozen");
    assert_eq!(preflight.metaserver.consecutive_failures, 1);
    assert!(preflight
        .degraded_reasons
        .contains(&"metaserver_heartbeat_failed".to_string()));
    assert!(preflight
        .degraded_reasons
        .contains(&"metaserver_forbid_auto_register".to_string()));
    let load = runtime.server_runtime_load();
    assert_eq!(load.last_meta_topology_version, 99);
    assert_eq!(load.meta_heartbeat_consecutive_failures, 1);
    assert!(load.meta_forbid_auto_register);
    runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
        status: Status::ok(),
        forbid_auto_register: false,
        topology_version: 100,
        server_state: "normal".to_string(),
    });
    let recovered = runtime.preflight_report();
    assert_eq!(recovered.metaserver.consecutive_failures, 0);
    assert_eq!(recovered.metaserver.last_topology_version, 100);
    assert_eq!(
        recovered.topology_validation.last_meta_topology_version,
        100
    );
    assert_eq!(recovered.topology_validation.loaded_shards, vec![7]);
    assert!(recovered.topology_validation.validation_limited);
    let topology = TableTopologyResponse {
        status: Status::ok(),
        table: Some(crate::meta::TableMetaInfo {
            table_id: 1,
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            state: crate::meta::MetaEntityState::Normal,
            topology_version: 100,
            first_shard_id: 7,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        }),
        shards: vec![crate::meta::TableShard {
            load_version: 0,
            shard_id: 7,
            start_bucket: 10,
            end_bucket: 19,
            primary: Some("server-a".to_string()),
            replicas: vec!["server-a".to_string()],
            primary_endpoint: None,
            replica_endpoints: Vec::new(),
        }],
        unchanged: false,
    };
    let validated = runtime.validate_topology_against_metaserver("server-a", &[topology]);
    assert!(validated.validated_against_metaserver);
    assert!(!validated.validation_limited);
    assert_eq!(validated.authoritative_topology_version, 100);
    assert_eq!(validated.mismatch_count, 0);
    let mismatch_topology = TableTopologyResponse {
        status: Status::ok(),
        table: None,
        shards: vec![crate::meta::TableShard {
            load_version: 0,
            shard_id: 7,
            start_bucket: 0,
            end_bucket: 9,
            primary: Some("server-b".to_string()),
            replicas: vec!["server-b".to_string()],
            primary_endpoint: None,
            replica_endpoints: Vec::new(),
        }],
        unchanged: false,
    };
    let mismatched = runtime.validate_topology_against_metaserver("server-a", &[mismatch_topology]);
    assert!(mismatched.mismatch_count >= 2);
    let shard_states = runtime.shard_serving_states();
    assert_eq!(shard_states.len(), 1);
    let shard = &shard_states[0];
    assert_eq!(shard.shard_id, 7);
    assert_eq!(shard.serving_state, "queued");
    assert_eq!(shard.worker_index, 0);
    assert_eq!(shard.worker_threads, 1);
    assert!(!shard.readonly);
    assert_eq!(shard.load_version, 42);
    assert_eq!(shard.table_name, "tbl");
    assert_eq!(shard.dirty_object_count, 1);
    assert_eq!(shard.wal_sequence, 1);
}

#[test]
fn runtime_dirty_dump_scheduler_periodically_flushes_dirty_shards() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.load_shard(2);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 2,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let scheduler = runtime.start_dirty_dump_scheduler(
        Duration::from_millis(5),
        RequestController { timeout_ms: 1000 },
    );

    for (shard_id, key) in [(1, "periodic-a"), (2, "periodic-b")] {
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
    }

    wait_until(Duration::from_secs(1), || {
        runtime.dirty_objects().is_empty() && runtime.stats().dump_runs >= 2
    });
    scheduler.stop();
    assert!(runtime.dirty_objects().is_empty());
    assert_eq!(runtime.dirty_shards(), Vec::<ShardId>::new());
}

#[test]
fn runtime_dirty_dump_scheduler_skips_already_queued_dump_for_shard() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    mark_dirty(&runtime.inner.dirty, 1, Some("queued"));
    let first = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
    let second = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });

    assert_eq!(first.len(), 1);
    assert!(first[0].status.ok);
    assert!(second.is_empty());
    assert_eq!(runtime.stats().background_queue_depth, 1);
}

#[test]
fn runtime_storage_lifecycle_scheduler_runs_periodically() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let job = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "lifecycle-scheduler".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(wait_for_job(&runtime, job.job_id).status.ok);
    let scheduler = runtime.start_storage_lifecycle_scheduler(
        Duration::from_millis(5),
        StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            purge_delayed_destroy: false,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            block_gc_shared_store_cursors: Vec::new(),
            block_gc_raft_snapshot_refs: Vec::new(),
            block_gc_checkpoint_floor_slab_id: None,
            block_gc_raft_install_floor_slab_id: None,
            block_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: true,
        },
    );
    wait_until(Duration::from_secs(1), || {
        runtime.stats().storage_lifecycle_runs >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_lifecycle_runs >= 1);
}

#[test]
// shared-corpus: storage_dump_load_recovery storage_cache_refill;
fn runtime_storage_manager_loop_runs_style_pressure_stages() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);

    for (key, value) in [
        ("manager-a", b"old".to_vec()),
        ("manager-a", b"new".to_vec()),
        ("manager-b", b"two".to_vec()),
    ] {
        let response = runtime.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let ttl = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetEx {
            key: "manager-ttl".to_string(),
            value: b"gone".to_vec(),
            ttl_ms: 1,
        },
    });
    assert!(ttl.status.ok, "{ttl:?}");
    let cached = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "manager-a".to_string(),
        },
    });
    assert!(cached.status.ok, "{cached:?}");
    std::thread::sleep(Duration::from_millis(2));

    let report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            max_dump_buckets_per_round: 16,
            min_undumped_wal_records: 1,
            dirty_bucket_pressure: 1,
            stale_block_slab_pressure: 1,
            reclaimable_physical_bytes_pressure: 1,
            cache_memory_bytes_pressure: 1,
            cache_disk_bytes_pressure: 1,
            ..StorageManagerOptions::default()
        },
    );

    assert!(report.status.ok, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ] {
        assert!(
            report
                .executed_stages
                .iter()
                .any(|executed| executed == stage),
            "missing stage {stage}: {report:?}"
        );
    }
    assert!(report.pressure.dirty_bucket_count >= 1);
    assert!(report.pressure.undumped_wal_records >= 1);
    // Nine since the evict stage was wired: eight that ship on, plus evict, which reports a
    // decision every round whether or not it is enabled.
    assert_eq!(report.pressure_decisions.len(), 9, "{report:?}");
    for stage in [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ] {
        let decision = report
            .pressure_decisions
            .iter()
            .find(|decision| decision.stage == stage)
            .unwrap_or_else(|| panic!("missing pressure decision {stage}: {report:?}"));
        assert!(decision.enabled, "{decision:?}");
        assert!(decision.executed, "{decision:?}");
        assert!(!decision.signals.is_empty(), "{decision:?}");
        assert!(decision.skip_reason.is_none(), "{decision:?}");
    }
    let reclaim_wal = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_wal")
        .unwrap();
    assert!(reclaim_wal.pressure_active, "{reclaim_wal:?}");
    assert!(reclaim_wal
        .signals
        .iter()
        .any(|signal| signal.name == "dirty_slot_count" && signal.over_threshold));
    assert!(reclaim_wal
        .signals
        .iter()
        .any(|signal| signal.name == "undumped_wal_records" && signal.over_threshold));
    assert!(reclaim_wal
        .trigger_reasons
        .iter()
        .any(|reason| reason == "dirty_slot_pressure" || reason == "undumped_wal_pressure"));
    let reclaim_memory = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_memory")
        .unwrap();
    assert!(reclaim_memory
        .signals
        .iter()
        .any(|signal| signal.name == "cache_memory_bytes"));
    assert!(reclaim_memory
        .signals
        .iter()
        .any(|signal| signal.name == "cache_disk_bytes"));
    let compact_blocks = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "compact_pages")
        .unwrap();
    assert!(compact_blocks
        .signals
        .iter()
        .any(|signal| signal.name == "reclaimable_physical_bytes"));
    let reclaim_index = report
        .pressure_decisions
        .iter()
        .find(|decision| decision.stage == "reclaim_index")
        .unwrap();
    assert!(reclaim_index
        .signals
        .iter()
        .any(|signal| signal.name == "manifest_prune_reasons"));
    assert!(reclaim_index
        .signals
        .iter()
        .any(|signal| signal.name == "install_roll_forward_reasons"));
    assert!(!report.lifecycle_plan.reclaim_candidates.is_empty());
    assert!(report.lifecycle_report.is_some());
    assert!(report.compaction_report.as_ref().unwrap().status.ok);
    assert!(report.gc_report.as_ref().unwrap().status.ok);
    assert!(report.expired_records_removed >= 1);
    assert!(runtime.dirty_objects().is_empty());

    let stats = runtime.stats();
    assert_eq!(stats.storage_manager_loops, 1);
    assert_eq!(stats.storage_manager_prepare_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_wal_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_memory_runs, 1);
    assert_eq!(stats.storage_manager_expire_runs, 1);
    assert_eq!(stats.storage_manager_reclaim_block_runs, 1);
    assert_eq!(stats.storage_manager_compact_runs, 1);
    assert_eq!(stats.storage_manager_index_gc_runs, 1);
}

#[test]
// shared-corpus: storage_manager_pressure_scale_evidence;
fn runtime_storage_manager_scale_repeats_style_pressure_stages() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        512,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(17);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);
    let mut reports = Vec::new();

    for round in 0..4 {
        for key_index in 0..16 {
            let key = format!("manager-scale-{round}-{key_index}");
            let first = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringSet {
                    key: key.clone(),
                    value: vec![round as u8; 128 + key_index],
                },
            });
            assert!(first.status.ok, "{first:?}");
            let second = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringSet {
                    key: key.clone(),
                    value: vec![(round + 1) as u8; 192 + key_index],
                },
            });
            assert!(second.status.ok, "{second:?}");
            let cached = runtime.execute(ExecuteRequest {
                shard_id: 17,
                command: Command::StringGet { key },
            });
            assert!(cached.status.ok, "{cached:?}");
        }
        let ttl_key = format!("manager-scale-ttl-{round}");
        let ttl = runtime.execute(ExecuteRequest {
            shard_id: 17,
            command: Command::StringSetEx {
                key: ttl_key,
                value: b"expire-me".repeat(16),
                ttl_ms: 1,
            },
        });
        assert!(ttl.status.ok, "{ttl:?}");
        std::thread::sleep(Duration::from_millis(3));

        let report = runtime.run_storage_manager_once(
            17,
            StorageManagerOptions {
                max_dump_buckets_per_round: 64,
                min_undumped_wal_records: 1,
                dirty_bucket_pressure: 1,
                stale_block_slab_pressure: 1,
                reclaimable_physical_bytes_pressure: 1,
                cache_memory_bytes_pressure: 1,
                cache_disk_bytes_pressure: 1,
                ..StorageManagerOptions::default()
            },
        );
        assert!(report.status.ok, "{report:?}");
        reports.push(report);
    }

    let required_stages = [
        "prepare",
        "reclaim_wal",
        "reclaim_memory",
        "expire",
        "reclaim_page",
        "compact_pages",
        "reclaim_index",
        "reap_metrics",
    ];
    for report in &reports {
        for stage in required_stages {
            assert!(
                report
                    .pressure_decisions
                    .iter()
                    .any(|decision| decision.stage == stage
                        && decision.enabled
                        && decision.executed
                        && !decision.signals.is_empty()),
                "missing executed pressure decision {stage}: {report:?}"
            );
        }
        assert!(report.pressure.dirty_bucket_count >= 1, "{report:?}");
        assert!(report.pressure.undumped_wal_records >= 1, "{report:?}");
        assert!(report.lifecycle_report.is_some(), "{report:?}");
        assert!(
            report.gc_report.as_ref().is_some_and(|gc| gc.status.ok),
            "{report:?}"
        );
        assert!(
            report
                .compaction_report
                .as_ref()
                .is_some_and(|compaction| compaction.status.ok),
            "{report:?}"
        );
    }

    assert!(reports
        .iter()
        .any(|report| report.expired_records_removed >= 1));
    assert!(reports.iter().any(|report| {
        report
            .pressure_decisions
            .iter()
            .any(|decision| decision.stage == "reclaim_memory" && decision.pressure_active)
    }));
    assert!(reports.iter().any(|report| {
        report
            .pressure_decisions
            .iter()
            .any(|decision| decision.stage == "compact_pages" && decision.pressure_active)
    }));
    assert!(runtime.dirty_objects().is_empty());

    let stats = runtime.stats();
    assert_eq!(stats.storage_manager_loops, reports.len() as u64);
    assert_eq!(stats.storage_manager_prepare_runs, reports.len() as u64);
    assert_eq!(
        stats.storage_manager_reclaim_wal_runs,
        reports.len() as u64
    );
    assert_eq!(
        stats.storage_manager_reclaim_memory_runs,
        reports.len() as u64
    );
    assert_eq!(stats.storage_manager_expire_runs, reports.len() as u64);
    assert_eq!(
        stats.storage_manager_reclaim_block_runs,
        reports.len() as u64
    );
    assert_eq!(stats.storage_manager_compact_runs, reports.len() as u64);
    assert_eq!(stats.storage_manager_index_gc_runs, reports.len() as u64);
}

/// The all-shards loop visits EVERY loaded shard, and picks up one loaded after it started.
///
/// The per-shard scheduler beside this one has to be told which shard it serves, which is why
/// nothing in the server ever started one -- a server does not know its shards up front. This
/// asks the engine each tick, so the thing worth asserting is not that it ran, but that it
/// reached a shard nobody named when it was started.
///
/// Asserted through the SHARD ID the last round recorded, not through a loop counter. The first
/// version of this counted `storage_manager_loops` and waited for it to climb, which at a 5 ms
/// interval it does whatever the loop visits -- so it measured the clock, and BOTH mutations
/// (visit only the first shard; snapshot the shard list at startup) passed it. A count per tick
/// cannot see which shard a tick chose. The report's shard id can.
#[test]
fn the_all_shards_scheduler_reaches_a_shard_loaded_after_it_started() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let response = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "all-shards-1".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    let scheduler = runtime.start_storage_manager_scheduler_for_all_shards(
        Duration::from_millis(5),
        StorageManagerOptions::default(),
    );
    wait_until(Duration::from_secs(2), || {
        runtime.stats().storage_manager_last_shard_id == Some(1)
    });
    assert_eq!(
        runtime.stats().storage_manager_last_shard_id,
        Some(1),
        "the loop never ran for the shard it started with"
    );

    // A shard the scheduler was never told about, loaded while it is already running.
    engine.load_shard(2);
    let response = runtime.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "all-shards-2".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");
    assert!(
        engine.loaded_shard_ids().contains(&2),
        "the engine must be holding the second shard for this to prove anything"
    );

    // The report has to name shard 2 at some point, which only happens if a tick chose it.
    wait_until(Duration::from_secs(5), || {
        runtime.stats().storage_manager_last_shard_id == Some(2)
    });
    let reached = runtime.stats().storage_manager_last_shard_id;
    scheduler.stop();
    assert_eq!(
        reached,
        Some(2),
        "the loop never reached the shard loaded after it started; last report was for {reached:?}"
    );
}

#[test]
// rust-internal: validates periodic runtime scheduling for the storage-manager loop
fn runtime_storage_manager_scheduler_runs_continuous_loop() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let response = runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "manager-scheduler".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    let scheduler = runtime.start_storage_manager_scheduler(
        Duration::from_millis(5),
        1,
        StorageManagerOptions::default(),
    );
    wait_until(Duration::from_secs(1), || {
        runtime.stats().storage_manager_loops >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_manager_loops >= 1);
}

#[test]
fn runtime_expiry_sweep_scheduler_removes_expired_records() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "ttl".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    let scheduler = runtime.start_expiry_sweep_scheduler(Duration::from_millis(5));
    wait_until(Duration::from_secs(1), || {
        runtime.stats().expired_records_removed >= 1
    });
    scheduler.stop();
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "ttl".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert!(runtime.stats().expiry_sweeps >= 1);
}

#[test]
fn runtime_rejects_when_queue_is_full() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 1,
            max_background_queue_depth: 1,
        },
    );
    let _first = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let rejected = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "b".to_string(),
                value: b"2".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    if !rejected.status.ok {
        assert_eq!(rejected.status.code, "queue_full");
    }
}

#[test]
fn runtime_cancel_reports_not_found_and_already_finished_jobs() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(engine, DataNodeRuntimeOptions::default());

    assert_eq!(runtime.cancel_job(42).status.code, "job_not_found");

    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok);
    assert_eq!(
        runtime.cancel_job(submitted.job_id).status.code,
        "job_already_finished"
    );
}

#[test]
fn runtime_compaction_rewrites_live_blocks_and_reports_stale_slabs() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for (key, value) in [
        ("a", b"old".to_vec()),
        ("a", b"one".to_vec()),
        ("b", b"two".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok);
    }
    assert_eq!(engine.live_block_slab_ids(1), vec![0]);

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let submitted = runtime.submit_compaction(
        CompactionRequest { shard_id: 1 },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Compact(output)) = finished.output else {
        panic!("expected compaction output");
    };
    assert!(output.status.ok);
    assert_eq!(output.compacted_objects, 2);
    assert_eq!(output.previous_block_slab_id, 0);
    assert_eq!(output.compacted_block_slab_id, 1);
    assert_eq!(output.stale_block_slab_ids, vec![0]);
    assert_eq!(output.before.total_block_count, 3);
    assert_eq!(output.before.live_block_refs, 2);
    assert_eq!(output.before.stale_block_estimate, 1);
    assert_eq!(output.before.live_ref_density_basis_points, 6_666);
    assert_eq!(output.after.total_block_count, 2);
    assert_eq!(output.after.live_block_refs, 2);
    assert_eq!(output.after.stale_block_estimate, 0);
    assert_eq!(output.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_block_slab_ids(1), vec![1]);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "a".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"one".to_vec())
        }
    );
}

#[test]
fn runtime_gc_reclaims_log_tails_and_reports_counts() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for key in ["a", "b", "c"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: key.as_bytes().to_vec(),
            },
        });
        assert!(response.status.ok);
    }
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "a".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"a".to_vec())
        }
    );
    engine.block_store().install_slab(1, b"old").unwrap();
    engine.block_store().install_slab(2, b"new").unwrap();

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    let submitted = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: Some(3),
            retain_index_log_from_sequence: Some(2),
            retain_block_slabs_from_id: Some(2),
            block_gc_delayed_destroy: false,
            block_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok);
    // ONE CACHE ENTRY PER READ KEY, where this was two. The single `StringGet` above used to
    // populate BOTH cache layers for the block it read -- the command response under
    // `CacheKey::string(shard, key)` and the block frame under `CacheKey::page_with_slot(...)` --
    // and #2291 stopped writing the response copy, because it was memory-only with a single reader
    // while the frame is the tiered, persistent copy. So an invalidating GC finds one entry for
    // that block, not two.
    //
    // KEPT EXACT. The count is what this test is for; a `>=` here would stop noticing a layer
    // appearing or the invalidation missing one.
    assert_eq!(output.cache_entries_removed, 1);
    assert!(output.cache_disk_bytes_removed > 0);
    assert_eq!(output.wal_records_removed, 2);
    assert_eq!(output.index_log_records_removed, 1);
    assert_eq!(output.block_slabs_removed, 1);
    assert!(output.block_slabs_removed_physical_bytes > 0);
    assert!(output.block_slabs_retained_physical_bytes > 0);
    assert_eq!(output.block_slabs_retained_live, 1);
    assert!(output.block_slabs_retained_live_physical_bytes > 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 3);
    assert_eq!(engine.block_store().slab_ids().unwrap(), vec![0, 2]);
}

#[test]
fn operator_gc_retains_slabs_referenced_by_dump_manifest() {
    // The /gc operator RPC must not delete a page slab a durable bucket-dump manifest still
    // references, even when retain_block_slabs_from_id would sweep it and it is no longer in
    // the resident live set. Deleting it makes the manifest uninstallable and loses data on
    // a lagging follower's replay / snapshot-install. The gated storage-manager cycle blocks
    // this via storage_block_gc_dependency_plan; the operator path must mirror the guard.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v1".to_vec(),
        },
    });
    // Dump: the manifest captures the slab (id 0) holding v1.
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(
        !manifest.block_slab_ids.is_empty(),
        "dump manifest should reference the slab holding v1"
    );
    // Roll to a new slab and overwrite: slab 0 becomes stale (not live) but is still named
    // by the manifest.
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let live = engine.live_block_slab_ids(1);
    assert!(
        !manifest.block_slab_ids.iter().any(|s| live.contains(s)),
        "manifest slab must be stale (not live) to exercise the guard; live={live:?}"
    );

    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    // Aggressive operator sweep that would delete the stale manifest slab.
    let submitted = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: Some(u64::MAX),
            block_gc_delayed_destroy: false,
            block_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    let finished = wait_for_job(&runtime, submitted.job_id);
    let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
        panic!("expected gc output");
    };
    assert!(output.status.ok, "{:?}", output.status);
    let remaining = engine.block_store().slab_ids().unwrap();
    for slab in &manifest.block_slab_ids {
        assert!(
            remaining.contains(slab),
            "operator /gc deleted slab {slab} still referenced by a dump manifest \
             (remaining={remaining:?})"
        );
    }
}

#[test]
fn runtime_cancels_queued_job_before_worker_executes_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let submitted = runtime.submit_execute(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "queued".to_string(),
                value: b"v".to_vec(),
            },
        },
        RequestController { timeout_ms: 1000 },
    );
    let canceled = runtime.cancel_job(submitted.job_id);
    assert_eq!(canceled.status.code, "job_canceled");
    assert_eq!(runtime.stats().canceled_total, 1);
    assert_eq!(runtime.stats().queue_depth, 0);
}

#[test]
fn runtime_cancels_queued_lifecycle_job_before_execution() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);

    let submitted = runtime.submit_load(
        crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_bucket: 10,
            end_routing_bucket: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");

    let canceled = runtime.cancel_job(submitted.job_id);
    assert_eq!(canceled.status.code, "job_canceled");
    assert_eq!(canceled.kind, DataNodeTaskKind::Load);
    assert_eq!(runtime.stats().canceled_total, 1);
    assert_eq!(runtime.stats().queue_depth, 0);
    assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
}

#[test]
fn runtime_marks_inflight_cancellation_requested_before_worker_finishes() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

    let submitted = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    let task = runtime
        .inner
        .queue
        .lock()
        .expect("runtime queue lock poisoned")
        .pop_ready()
        .expect("task should be marked running");
    assert_eq!(task.job_id, submitted.job_id);

    let cancel_requested = runtime.cancel_job(submitted.job_id);
    assert_eq!(cancel_requested.status.code, "job_cancel_requested");
    assert_eq!(
        runtime.job_status(submitted.job_id).unwrap().status.code,
        "job_cancel_requested"
    );
    assert_eq!(runtime.stats().canceled_total, 0);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Dump(response) = output else {
        panic!("expected dump output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert!(take_canceled(&runtime.inner, submitted.job_id));
}

#[test]
fn runtime_honors_inflight_cancellation_before_dump_side_effects() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "dirty".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(response.status.ok);
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);
    mark_dirty(&runtime.inner.dirty, 1, Some("dirty"));
    let task = QueuedTask {
        job_id: 99,
        kind: DataNodeTaskKind::Dump,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Dump(DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        }),
    };
    runtime
        .inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .insert(task.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Dump(response) = output else {
        panic!("expected dump output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert_eq!(response.shard_id, 1);
    assert_eq!(runtime.dirty_objects().len(), 1);
}

#[test]
fn runtime_honors_inflight_cancellation_before_gc_side_effects() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for key in ["a", "b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: key.as_bytes().to_vec(),
            },
        });
        assert!(response.status.ok);
    }
    let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);
    mark_dirty(&runtime.inner.dirty, 1, Some("a"));
    let task = QueuedTask {
        job_id: 100,
        kind: DataNodeTaskKind::Gc,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Gc(GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: Some(2),
            retain_index_log_from_sequence: Some(2),
            retain_block_slabs_from_id: None,
            block_gc_delayed_destroy: false,
            block_gc_invalidate_removed_slabs_only: false,
        }),
    };
    runtime
        .inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .insert(task.job_id);

    let output = execute_task(&runtime.inner, &task);
    let DataNodeTaskOutput::Gc(response) = output else {
        panic!("expected gc output");
    };
    assert_eq!(response.status.code, "job_canceled");
    assert_eq!(response.shard_id, 1);
    assert_eq!(runtime.dirty_objects().len(), 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).last_sequence, 2);
    assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
    assert_eq!(
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn runtime_queues_are_shard_affine_and_parallel_across_shards() {
    let mut queues = RuntimeQueues::default();
    queues.push(queued_string_set(1, 1, "a"));
    queues.push(queued_string_set(2, 1, "b"));
    queues.push(queued_string_set(3, 2, "c"));

    let first = queues.pop_ready().expect("shard 1 should be ready");
    assert_eq!(first.job_id, 1);
    assert_eq!(queues.queued_total, 2);
    assert!(queues.running_shards.contains(&1));

    let second = queues
        .pop_ready()
        .expect("shard 2 should run while shard 1 is busy");
    assert_eq!(second.job_id, 3);
    assert_eq!(second.request.shard_id(), 2);
    assert!(queues.pop_ready().is_none());

    queues.finish_shard(1);
    let third = queues
        .pop_ready()
        .expect("next shard 1 task should run after lane release");
    assert_eq!(third.job_id, 2);
    queues.finish_shard(2);
    queues.finish_shard(1);
    assert_eq!(queues.queued_total, 0);
    assert!(queues.by_shard.is_empty());
    assert!(queues.running_shards.is_empty());
}

#[test]
fn runtime_scheduler_prioritizes_foreground_over_background() {
    let mut queues = RuntimeQueues::default();
    queues.push(queued_dump(1, 1));
    queues.push(queued_string_set(2, 1, "foreground"));
    queues.push(queued_dump(3, 2));

    let first = queues.pop_ready().expect("foreground work should be ready");
    assert_eq!(first.job_id, 2);
    assert_eq!(first.request.priority(), TaskPriority::Foreground);
    queues.finish_shard(1);

    let second = queues.pop_ready().expect("background shard should run");
    assert_eq!(second.request.priority(), TaskPriority::Background);
    assert_eq!(queues.background_queued_total, 1);
}

#[test]
fn runtime_rejects_background_work_when_background_queue_is_full() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 8,
            max_background_queue_depth: 1,
        },
    );

    let accepted = runtime.submit_dump(
        DumpShardRequest {
            shard_id: 1,
            selected_routing_buckets: Vec::new(),
        },
        RequestController { timeout_ms: 1000 },
    );
    assert!(accepted.status.ok);
    let rejected = runtime.submit_gc(
        GcRequest {
            shard_id: 1,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: None,
            block_gc_delayed_destroy: false,
            block_gc_invalidate_removed_slabs_only: false,
        },
        RequestController { timeout_ms: 1000 },
    );
    assert_eq!(rejected.status.code, "background_queue_full");
    assert_eq!(runtime.stats().rejected_background_total, 1);
    assert_eq!(runtime.stats().background_queue_depth, 1);
}

// shared-corpus: storage_matrixraft_dump_load_atomicity storage_matrixraft_cache_refill_pressure
#[test]
/// A stage that reports itself as executed has to have executed something.
///
/// The reap used to push "reap_metrics" onto the executed list and stop there, so a cycle said
/// the stage ran while nothing was gathered and nothing published. The WAL counters are the
/// clearest case: incremented on every append and barrier, and read by nothing outside the
/// module that owns them.
#[test]
fn a_metrics_reap_collects_the_counters_it_says_it_did() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 8,
        },
    );
    for key in ["reap-a", "reap-b", "reap-c"] {
        assert!(
            runtime
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
    }

    let report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_metrics_reap: true,
            ..StorageManagerOptions::default()
        },
    );
    assert!(
        report
            .executed_stages
            .iter()
            .any(|stage| stage == "reap_metrics"),
        "the stage should report itself executed: {:?}",
        report.executed_stages
    );
    let reaped = report
        .metrics_reap
        .expect("the stage ran, so the report carries what it read");
    assert!(
        reaped.wal.last_sequence >= 3,
        "the WAL counters have to be THIS shard's, and three writes went in: {reaped:?}"
    );
    assert_eq!(
        reaped.durability_barriers_total,
        reaped.durability_barriers.values().copied().sum::<u64>(),
        "the total has to be the sum of what was attributed"
    );

    // Disabled: nothing collected, and the cycle says which it was.
    let off = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_metrics_reap: false,
            ..StorageManagerOptions::default()
        },
    );
    assert!(off.metrics_reap.is_none());
    assert!(
        off.skipped_stages
            .iter()
            .any(|stage| stage == "reap_metrics_disabled"),
        "{:?}",
        off.skipped_stages
    );
}

#[test]
fn storage_manager_cycle_runs_as_bounded_background_data_node_task() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "storage-manager".to_string(),
                shard_uri: "local://storage-manager/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    assert!(
        runtime
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "storage-manager-background".to_string(),
                    value: b"value".to_vec(),
                },
            })
            .status
            .ok
    );

    let submitted = runtime.submit_storage_manager_cycle(
        StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 8,
            warm_cache: true,
            ..StorageManagerCycleRequest::default()
        },
        RequestController { timeout_ms: 30_000 },
    );
    assert!(submitted.status.ok, "{submitted:?}");

    let finished = wait_for_job(&runtime, submitted.job_id);
    assert!(finished.status.ok, "{finished:?}");
    let Some(DataNodeTaskOutput::StorageManager(response)) = finished.output else {
        panic!("expected storage manager output: {finished:?}");
    };
    assert!(response.status.ok, "{response:?}");
    assert!(response.report.completed, "{:#?}", response.report);
    assert!(
        response.report.production_parity_slice,
        "{:#?}",
        response.report
    );
    assert_eq!(
        response.report.native_stage_order,
        vec![
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
    );
    assert!(response
        .report
        .stages
        .iter()
        .any(|stage| stage.stage == "reclaim_wal" && stage.dumped_bucket_count >= 1));
    assert_eq!(runtime.stats().storage_manager_runs, 1);
    assert_eq!(runtime.stats().background_queue_depth, 0);
}

// shared-corpus: storage_matrixraft_dump_load_atomicity storage_matrixraft_cache_refill_pressure
#[test]
fn storage_manager_scheduler_submits_deduplicated_background_cycles() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        512,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 1,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "storage-manager-scheduler".to_string(),
                shard_uri: "local://storage-manager-scheduler/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    runtime.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "storage-manager-scheduler".to_string(),
            value: b"value".to_vec(),
        },
    });

    let scheduler = runtime.start_storage_manager_cycle_scheduler(
        Duration::from_millis(5),
        StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 8,
            warm_cache: true,
            ..StorageManagerCycleRequest::default()
        },
        RequestController { timeout_ms: 30_000 },
    );

    wait_until(Duration::from_secs(5), || {
        runtime.stats().storage_manager_runs >= 1
    });
    scheduler.stop();
    assert!(runtime.stats().storage_manager_runs >= 1);
    assert_eq!(runtime.stats().rejected_background_total, 0);
}

// shared-corpus: storage_manager_continuous_background_runtime
#[test]
fn storage_manager_runtime_collects_the_report_of_a_cycle_that_outlived_its_wait() {
    // A completed cycle's report must reach the runtime report even when the cycle takes longer
    // than the short in-loop wait.
    //
    // It did not. The loop tracked the outstanding cycle in a single slot: the top-of-tick poll
    // saw the cycle still running, the cycle finished later in that same tick, `has_pending` then
    // went false so a new cycle was submitted, and submitting OVERWROTE the slot before anything
    // polled the finished job again. Every cycle was abandoned exactly one tick before it
    // completed. Measured on the unfixed code: 432 collection attempts across 45 cycles, 47 of
    // which finished with a valid report, and not one was ever collected.
    //
    // The visible cost is that `last_completed_cycle` stays None forever, and with it every
    // derived figure -- bytes reclaimed, pressure after, the WAL and index-log floors -- reads
    // zero while the storage manager is doing real work. That is the operator's evidence that
    // reclaim ran, and reclaim is the only thing that recovers the disk growth from repeated
    // dumps.
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 9,
                table_name: "storage-manager-collect".to_string(),
                shard_uri: "local://storage-manager-collect/9".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..32 {
        runtime.execute(ExecuteRequest {
            shard_id: 9,
            command: Command::StringSet {
                key: format!("collect-{index}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let manager = runtime.start_storage_manager_runtime(StorageManagerRuntimeOptions {
        // A 5ms interval against cycles that take far longer is the case that broke: the wait
        // budget is tied to the interval, so essentially every cycle outlives it.
        interval_ms: 5,
        jitter_percent: 50,
        initial_backoff_ms: 3,
        max_backoff_ms: 40,
        request: StorageManagerCycleRequest {
            shard_id: 9,
            max_dump_buckets_per_round: 3,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_evict: true,
            enable_block_reclaim: true,
            enable_index_gc: true,
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 30_000 },
    });

    wait_until(Duration::from_secs(15), || {
        manager.report().last_completed_cycle.is_some()
    });
    let report = manager.report();
    manager.stop();

    assert!(
        report.rounds_submitted >= 1,
        "expected at least one submitted cycle, got {report:?}"
    );
    assert!(
        report.last_completed_cycle.is_some(),
        "a finished cycle's report was never collected: {report:?}"
    );
    assert_eq!(
        report.submit_failures, 0,
        "cycles should submit cleanly: {report:?}"
    );
}

#[test]
fn storage_manager_runtime_supports_stop_pause_resume_jitter_backoff_and_phase_flags() {
    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let runtime = DataNodeRuntime::new(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 1,
            max_queue_depth: 8,
            max_background_queue_depth: 2,
        },
    );
    assert!(
        runtime
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 8,
                table_name: "storage-manager-runtime".to_string(),
                shard_uri: "local://storage-manager-runtime/8".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live".to_string(),
            value: b"value".to_vec(),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live".to_string(),
            value: b"value-newer".repeat(32),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringGet {
            key: "runtime-live".to_string(),
        },
    });
    // `runtime-live` is 352 bytes and this engine's memory tier is 256, so reading it back
    // spills straight to disk and leaves the memory cache empty -- the pressure snapshot then
    // reports cache_memory_bytes = 0 no matter how warm the shard actually is. Touch a value
    // that fits, so the assertion below measures a warm memory tier rather than the tier size.
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-small".to_string(),
            value: b"s".to_vec(),
        },
    });
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringGet {
            key: "runtime-small".to_string(),
        },
    });

    let manager = runtime.start_storage_manager_runtime(StorageManagerRuntimeOptions {
        interval_ms: 5,
        jitter_percent: 50,
        initial_backoff_ms: 3,
        max_backoff_ms: 40,
        request: StorageManagerCycleRequest {
            shard_id: 8,
            max_dump_buckets_per_round: 3,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_expire: false,
            enable_evict: true,
            enable_block_reclaim: true,
            enable_block_compaction: false,
            enable_index_gc: true,
            warm_cache: true,
            follower_replay_cursors: vec![crate::engine::reports::BucketDumpFollowerReplayCursor {
                follower_id: "follower-lagging-runtime".to_string(),
                shard_id: 8,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            raft_snapshot_refs: vec![crate::engine::reports::BucketDumpRaftSnapshotRef {
                snapshot_id: "raft-snapshot-runtime".to_string(),
                shard_id: 8,
                last_included_index: 1,
                last_included_term: 1,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            block_gc_raft_install_floor_slab_id: Some(1),
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 30_000 },
    });

    // Wait for a cycle that saw the state these assertions describe, instead of whichever
    // snapshot happened to be first.
    //
    // Two of the conditions pull against each other. The pressure plan is computed at the TOP of
    // a cycle, so the first cycle sees no dump manifest and a replay cursor has nothing to anchor
    // on -- zero retention blockers is the correct answer there, not a defect. But by the time a
    // manifest exists, the writes made before the runtime started have been dumped and nothing is
    // dirty. A shard with a lagging follower AND outstanding work is a shard still being written
    // to, so keep writing while waiting.
    //
    // The predicate SELECTS a cycle; it does not stand in for the assertions. It looks at two
    // fields, and the block below verifies the whole snapshot.
    let mut churn = 0u64;
    wait_until(Duration::from_secs(15), || {
        churn += 1;
        runtime.execute(ExecuteRequest {
            shard_id: 8,
            command: Command::StringSet {
                key: format!("runtime-churn-{churn}"),
                value: b"c".to_vec(),
            },
        });
        let report = manager.report();
        report.rounds_submitted >= 2
            && runtime.stats().storage_manager_runs >= 2
            && report.last_completed_cycle.is_some()
            && report.last_pressure_snapshot.as_ref().is_some_and(|pressure| {
                pressure.dirty_bucket_count >= 1
                    && pressure.follower_cursor_retention_blockers >= 1
            })
    });
    let running = manager.report();
    assert!(running.running);
    assert!(!running.paused);
    assert!(!running.stopped);
    assert_eq!(running.interval_ms, 5);
    assert_eq!(running.jitter_percent, 50);
    assert!(running.last_delay_ms >= 5);
    assert!(running.last_delay_ms <= 7);
    assert_eq!(running.bounded_max_dump_buckets_per_round, 3);
    assert!(running.phase_prepare_enabled);
    assert!(running.phase_wal_reclaim_enabled);
    assert!(!running.phase_expire_enabled);
    assert!(running.phase_evict_enabled);
    assert!(running.phase_block_gc_enabled);
    assert!(!running.phase_compaction_enabled);
    assert!(running.phase_index_gc_enabled);
    assert_eq!(running.configured_follower_cursor_count, 1);
    assert_eq!(running.configured_raft_snapshot_ref_count, 1);
    assert_eq!(
        running.configured_block_gc_raft_install_floor_slab_id,
        Some(1)
    );
    assert!(running.last_job_id.is_some());
    assert!(running.last_status.as_ref().is_some_and(|status| status.ok));
    assert!(running.last_completed_cycle.is_some());
    assert!(running.last_pressure_snapshot.is_some());
    let pressure = running.last_pressure_snapshot.as_ref().unwrap();
    assert!(pressure.dirty_bucket_count >= 1, "{pressure:?}");
    assert!(pressure.undumped_wal_records >= 1, "{pressure:?}");
    assert!(pressure.wal_bytes >= 1, "{pressure:?}");
    assert!(pressure.index_log_bytes >= 1, "{pressure:?}");
    assert!(pressure.cache_memory_bytes >= 1, "{pressure:?}");
    assert!(pressure.memory_cache_pressure_score >= pressure.cache_memory_bytes);
    assert!(pressure.follower_cursor_retention_blockers >= 1, "{pressure:?}");
    assert!(pressure.raft_snapshot_retention_blockers >= 1, "{pressure:?}");
    assert!(pressure.total_pressure_score >= pressure.wal_bytes);
    assert!(running.last_pressure_before >= running.last_pressure_after);
    assert!(!running.last_selected_buckets.is_empty());
    assert!(running.last_bytes_reclaimed >= pressure.cache_disk_bytes);
    // This scenario configures a follower pinned at sequence 1 and a raft snapshot at sequence 1.
    // What a retention cursor imposes is a BOUND on how far reclaim may go, not a refusal; the
    // note below the binding says why, and what is asserted instead.
    let reclaim_wal = running
        .last_phase_reports
        .iter()
        .find(|stage| stage.stage == "reclaim_wal")
        .expect("the cycle ran a reclaim_wal stage");
    // The refusal this asserted was deliberately replaced by a CLAMP. A cursor at sequence 1
    // means everything at or below 1 is behind every reader, so reclaim may take exactly that
    // span and no more; refusing outright let one lagging follower pin the whole log for as long
    // as it lagged, and the log grew without bound underneath it (storage_lifecycle_methods.rs).
    // So the property worth asserting is the BOUND, not a refusal message -- and the bound is the
    // one a retention cursor exists to impose.
    //
    // Both cursors sit at sequence 1, and the wait above already required a retention blocker,
    // which needs 1 < durable_frontier. The floor over the cursors is therefore 1 and the plan
    // must retain from exactly 2. The exact value is the point: >= would also pass if the clamp
    // were dropped and reclaim ran all the way to the durable frontier, which is the regression
    // being guarded here.
    assert!(!reclaim_wal.skipped, "{reclaim_wal:?}");
    assert_eq!(
        reclaim_wal.retain_from_wal_sequence, 2,
        "reclaim must not advance past the slowest cursor: {reclaim_wal:?}"
    );
    assert_eq!(
        reclaim_wal.retain_from_index_log_sequence, 2,
        "reclaim must not advance past the slowest cursor: {reclaim_wal:?}"
    );
    // Clamping instead of refusing did not stop the cursors being counted, nor the pressure
    // signal naming what is holding the logs.
    assert_eq!(reclaim_wal.retention_blockers, 2, "{reclaim_wal:?}");
    assert!(
        reclaim_wal
            .pressure_signal
            .contains("follower_snapshot_retention"),
        "{reclaim_wal:?}"
    );
    assert_eq!(
        running.last_wal_floor_sequence, reclaim_wal.wal_floor_sequence,
        "the manager must report the floor the stage computed: {:?}",
        running.last_skipped_reasons
    );
    assert_eq!(
        running.last_index_log_floor_sequence,
        reclaim_wal.index_log_floor_sequence
    );
    assert!(running.last_retention_blockers >= 1);
    assert!(running
        .last_phase_blockers
        .iter()
        .any(|blocker| blocker.contains("retention_blockers")));
    assert!(running
        .last_phase_reports
        .iter()
        .any(|stage| stage.stage == "prepare"
            && stage.pressure_signal.contains("dirty_slots")
            && stage.pressure_before >= stage.pressure_after));
    // The floor is the CLAMPED frontier, not zero: reclaim ran, and stopped at the slowest cursor.
    // The refusal was asserted TWICE in this test, in two blocks about sixty lines apart, so
    // correcting the first one only moved the failure down here. Both now say the same thing.
    assert!(reclaim_wal.retention_blockers >= 1, "{reclaim_wal:?}");
    assert_eq!(reclaim_wal.wal_floor_sequence, 2, "{reclaim_wal:?}");
    assert_eq!(reclaim_wal.index_log_floor_sequence, 2, "{reclaim_wal:?}");
    assert!(running.last_phase_reports.iter().any(|stage| {
        stage.stage == "reclaim_page"
            && (stage
                .skipped_reason
                .contains("retained dependencies remain")
                || stage.retention_blockers >= 1)
    }));
    assert!(running
        .last_phase_reports
        .iter()
        .any(|stage| stage.pressure_signal.contains("stale_density")
            || stage.pressure_signal.contains("cache_pressure")));
    assert!(
        running
            .last_phase_reports
            .iter()
            .any(|stage| stage.stage == "reclaim_wal"
                && stage.pressure_signal.contains("wal_bytes"))
    );
    assert!(!running.last_phase_reports.is_empty());

    manager.pause();
    let paused_before = manager.report().rounds_submitted;
    // Wait for a skipped round instead of sleeping 25ms and hoping one fits inside it. A round is
    // a delay PLUS a cycle, and a cycle on a busy machine outlasts that sleep -- so the assertion
    // measured how loaded the box was rather than whether pausing skips rounds. The properties
    // being checked are unchanged: while paused, rounds are skipped and none are submitted.
    wait_until(Duration::from_secs(5), || {
        manager.report().rounds_skipped_paused >= 1
    });
    let paused = manager.report();
    assert!(paused.paused);
    assert_eq!(paused.rounds_submitted, paused_before);
    assert!(paused.rounds_skipped_paused >= 1);

    manager.resume();
    runtime.execute(ExecuteRequest {
        shard_id: 8,
        command: Command::StringSet {
            key: "runtime-live-2".to_string(),
            value: b"value-2".to_vec(),
        },
    });
    wait_until(Duration::from_secs(5), || {
        manager.report().rounds_submitted > paused_before
    });
    assert!(!manager.report().paused);

    let stopped = manager.stop();
    assert!(!stopped.running);
    assert!(stopped.stopped);
    assert!(stopped.rounds_submitted >= 2);
    assert_eq!(stopped.submit_failures, 0);
}

// shared-corpus: storage_manager_continuous_background_runtime
#[test]
fn storage_manager_runtime_jitter_and_backoff_are_bounded() {
    let options = StorageManagerRuntimeOptions {
        interval_ms: 10,
        jitter_percent: 50,
        initial_backoff_ms: 4,
        max_backoff_ms: 16,
        request: StorageManagerCycleRequest {
            shard_id: 404,
            max_dump_buckets_per_round: 1,
            ..StorageManagerCycleRequest::default()
        },
        controller: RequestController { timeout_ms: 10 },
    };

    let first_delay = storage_manager_runtime_delay_ms(&options, 1, 4);
    assert!((10..=15).contains(&first_delay));
    assert_eq!(storage_manager_runtime_next_backoff_ms(0, 4, 16), 4);
    assert_eq!(storage_manager_runtime_next_backoff_ms(4, 4, 16), 8);
    assert_eq!(storage_manager_runtime_next_backoff_ms(8, 4, 16), 16);
    assert_eq!(storage_manager_runtime_next_backoff_ms(16, 4, 16), 16);

    let report = storage_manager_runtime_initial_report(&options);
    assert!(report.running);
    assert_eq!(report.current_backoff_ms, 4);
    assert_eq!(report.bounded_max_dump_buckets_per_round, 1);
    assert!(report.phase_prepare_enabled);
    assert!(report.phase_wal_reclaim_enabled);
    assert!(report.phase_expire_enabled);
    assert!(report.phase_evict_enabled);
    assert!(report.phase_block_gc_enabled);
    assert!(report.phase_compaction_enabled);
    assert!(report.phase_index_gc_enabled);
}

fn queued_string_set(job_id: u64, shard_id: ShardId, key: &str) -> QueuedTask {
    QueuedTask {
        job_id,
        kind: DataNodeTaskKind::Execute,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            },
        }),
    }
}

fn queued_dump(job_id: u64, shard_id: ShardId) -> QueuedTask {
    QueuedTask {
        job_id,
        kind: DataNodeTaskKind::Dump,
        deadline: Instant::now() + Duration::from_secs(60),
        submitted_at_ms: now_ms(),
        request: TaskRequest::Dump(DumpShardRequest {
            shard_id,
            selected_routing_buckets: Vec::new(),
        }),
    }
}

fn wait_for_job(runtime: &DataNodeRuntime, job_id: u64) -> DataNodeTaskStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(status) = runtime.job_status(job_id) {
            if status.finished_at_ms.is_some() {
                return status;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("job {job_id} did not finish");
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        predicate(),
        "condition did not become true within {timeout:?}"
    );
}

#[test]
fn the_slot_index_metric_counts_resident_buckets_not_the_routing_range() {
    // `slot_index_entry_count` was built from `storage.bucket_entries`, which is the routing
    // RANGE. On a default shard that is u32::MAX, and `max(u32::MAX, anything)` is u32::MAX, so
    // the metric exported 4,294,967,295 per shard whatever the index held -- and these are SUMMED
    // across shards. Nothing asserted it, which is why it stayed that way.
    let shard = crate::meta::ServerShardServingState {
        shard_id: 1,
        dirty_bucket_count: 3,
        storage: crate::control::ShardCanonicalStorageStats {
            // the routing range a default shard reports
            bucket_entries: u32::MAX as u64,
            bucket_index_resident_entries: 42,
            ..crate::control::ShardCanonicalStorageStats::default()
        },
        ..crate::meta::ServerShardServingState::default()
    };

    let mut metrics = std::collections::BTreeMap::new();
    super::super::apply_shard_storage_metrics(&mut metrics, std::slice::from_ref(&shard));

    assert_eq!(
        metrics.get("slot_index_entry_count").copied(),
        Some(42),
        "the metric must report resident buckets"
    );
    // The control: the field it used to read is still u32::MAX right here, so a regression to it
    // cannot pass by the numbers happening to agree.
    assert_eq!(shard.storage.bucket_entries, u32::MAX as u64);
}

/// What each maintenance stage costs on a realistic shard, by ablation.
///
///   cargo test --release -p temporalstore-rust --lib what_each_maintenance_stage_costs -- --ignored --nocapture --test-threads=1
///
/// The cycle is timed with everything on, then once per stage with that stage off. The difference
/// is that stage's cost. Ablation rather than instrumentation, so no stage has to be edited to be
/// measured, and a stage that declines for want of pressure shows up as ~0 rather than as absent.
///
/// Compaction was already measured directly at seconds per round
/// (`what_a_whole_shard_compaction_costs`); this is the rest of them, which had no number at all.
#[test]
#[ignore]
fn what_each_maintenance_stage_costs() {
    fn build(objects: usize) -> (tempfile::TempDir, DataNodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("stage-cost-{index:06}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        (dir, runtime)
    }

    fn time_once(objects: usize, options: StorageManagerOptions) -> f64 {
        let (_dir, runtime) = build(objects);
        let started = std::time::Instant::now();
        let report = runtime.run_storage_manager_once(1, options);
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let _ = report;
        elapsed
    }

    for objects in [5_000usize, 20_000] {
        let all_on = time_once(objects, StorageManagerOptions::default());
        eprintln!("  [stages] {objects:>6} objects, everything on -> {all_on:>9.1} ms");

        for (name, off) in [
            ("prepare", StorageManagerOptions { enable_prepare: false, ..Default::default() }),
            ("reclaim_wal", StorageManagerOptions { enable_wal_reclaim: false, ..Default::default() }),
            ("reclaim_memory", StorageManagerOptions { enable_memory_reclaim: false, ..Default::default() }),
            ("expire", StorageManagerOptions { enable_expire: false, ..Default::default() }),
            ("reclaim_page", StorageManagerOptions { enable_block_gc: false, ..Default::default() }),
            ("compact_pages", StorageManagerOptions { enable_block_compaction: false, ..Default::default() }),
            ("reclaim_index", StorageManagerOptions { enable_index_gc: false, ..Default::default() }),
            ("reap_metrics", StorageManagerOptions { enable_metrics_reap: false, ..Default::default() }),
        ] {
            let without = time_once(objects, off);
            eprintln!(
                "  [stages] {objects:>6}   without {name:<15} {without:>9.1} ms   (stage costs {:>9.1} ms)",
                all_on - without,
            );
        }
    }
}

#[test]
fn the_periodic_loop_can_evict_when_the_operator_asks() {
    // Until this stage existed the periodic loop -- the one the server actually starts -- relieved
    // memory by invalidating cached pages and nothing else. `apply_storage_eviction`, with
    // dump-before-evict, delete-drop, a batch limit and a pressure threshold, was reachable ONLY
    // from the on-demand cycle, so eviction could not happen on the running loop at ANY setting.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("evict-{index:04}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    // CONTROL, RESTATED. This asserted `evict_disabled` and read "the shipped default must not
    // evict ... the change is opt-in". The stage now ships ON, so the control is no longer "it
    // does not run" -- it is "it runs and RELEASES NOTHING", because the shipped
    // `index_resident_limit_bytes` is the higher bar and this 64-record shard is far under it.
    // That is the half that says defaulting the stage on did not silently change how every
    // deployment relieves memory.
    let default_report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    let default_eviction = default_report
        .eviction
        .as_ref()
        .expect("the stage ships on, so a default round reports it");
    assert_eq!(
        default_eviction.mode, "index_resident_bound",
        "the shipped default opened the wide gate: mode={} threshold={}",
        default_eviction.mode, default_eviction.memory_pressure_threshold
    );
    assert_eq!(
        default_eviction.skipped_reason, "index_resident_below_bound",
        "the shipped default acted on a 64-record shard: reason={:?} pressure_before={}",
        default_eviction.skipped_reason, default_eviction.pressure_before
    );
    assert_eq!(
        default_eviction.bucket_index_buckets_released, 0,
        "the shipped default released {} buckets from a 64-record shard",
        default_eviction.bucket_index_buckets_released
    );

    // And with the operator lowering the bound out of the way, the wide stage runs and acts.
    let enabled_report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_evict: true,
            eviction_dump_before_evict: true,
            // The one field that differs from the control above.
            index_resident_limit_bytes: 0,
            ..StorageManagerOptions::default()
        },
    );
    assert!(
        enabled_report.executed_stages.iter().any(|stage| stage == "evict"),
        "enabling eviction must reach the stage, got executed={:?} skipped={:?}",
        enabled_report.executed_stages,
        enabled_report.skipped_stages
    );
    // AND IT TOOK THE WIDE ARM, which is what makes the control's `index_resident_bound` a
    // statement about the BOUND rather than about this fixture.
    let enabled_eviction = enabled_report
        .eviction
        .as_ref()
        .expect("the stage reports when it is enabled");
    assert_eq!(
        enabled_eviction.mode, "evict_cache",
        "removing the bound did not fall back on the wide gate: mode={}",
        enabled_eviction.mode
    );
    assert_eq!(
        enabled_eviction.skipped_reason, "",
        "the wide gate declined at a threshold of 0: reason={:?}",
        enabled_eviction.skipped_reason
    );
    // Two rounds have run and BOTH reached the stage: the default one bounded, this one wide.
    assert_eq!(
        runtime.stats().storage_manager_evict_runs,
        2,
        "the stage must be counted once per round that reached it"
    );
}

#[test]
fn the_periodic_expire_stage_takes_a_bounded_window_each_round() {
    // The periodic loop passed `ShardExpirySweepRequest::default()`, whose limits are 0 -- and the
    // window code says plainly that "zero limits mean no limit". So every tick walked the WHOLE
    // deadline map, hot and cold, for every loaded shard, while the on-demand cycle passed 128 and
    // carried a cursor. Measured by ablation, that stage was 201.5 ms at 20k objects.
    //
    // This test was originally built around a CURSOR, and that mechanism is gone.
    //
    // The sweep used to walk the deadlines in KEY order, so a window from the start landed on
    // whatever sorted first -- live or not -- and only a resuming cursor let later rounds reach
    // the expired tail. The fixture below still carries a live prefix sorted ahead of the expired
    // keys because of that.
    //
    // Since the deadlines gained a deadline-ordered view, the due keys ARE the front: the live
    // prefix is never examined, the walk stops at the first deadline in the future, and progress
    // needs no cursor because removing a due key shrinks the prefix. The live prefix is kept as a
    // control -- it must still be there afterwards, untouched -- rather than as the obstacle it
    // used to be.
    //
    // What remains worth asserting is the BOUND: one round must take at most its window, and
    // successive rounds must finish the job.
    const LIVE_PREFIX: usize = 20;
    const EXPIRED: usize = 20;
    const WINDOW: usize = 4;

    fn runtime_with_a_live_prefix() -> DataNodeRuntime {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        // Sorted before the expired ones, so they are what a window from the start lands on.
        for index in 0..LIVE_PREFIX {
            let key = format!("aaa-live-{index:04}");
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet { key: key.clone(), value: vec![b'v'; 32] },
                })
                .status
                .ok);
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire { key, ttl_ms: 10 * 60 * 1000 },
                })
                .status
                .ok);
        }
        for index in 0..EXPIRED {
            let key = format!("zzz-dead-{index:04}");
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet { key: key.clone(), value: vec![b'v'; 32] },
                })
                .status
                .ok);
            assert!(engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire { key, ttl_ms: 1 },
                })
                .status
                .ok);
        }
        std::thread::sleep(std::time::Duration::from_millis(30));
        DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        )
    }

    // CONTROL: the shipped default clears this fixture in one round -- because its bound is
    // LARGER than the fixture, not because it is unbounded.
    //
    // This wording is deliberate. When this test was written the default WAS 0, meaning no bound,
    // and the assertion below read as "the default is unbounded". It is not any more: the default
    // is now the cycle's 128 hot / 8 cold. The assertion still passes, but for a different reason
    // than it used to, and a control that passes for a reason it does not state is worth nothing.
    //
    // What it pins now is that the shipped bound comfortably exceeds a small store, so ordinary
    // expiry is not slowed by it -- which is the property that actually matters to a deployment,
    // and which a too-small default would break loudly here.
    let default_bound = StorageManagerOptions::default().max_expire_hot_buckets_per_round;
    assert!(
        default_bound > EXPIRED,
        "this control only means something while the default bound ({default_bound}) exceeds \
         the fixture ({EXPIRED}); raise the fixture or rewrite the control"
    );
    let unbounded = runtime_with_a_live_prefix();
    let cleared = unbounded.run_storage_manager_once(1, StorageManagerOptions::default());
    assert!(
        cleared.executed_stages.iter().any(|stage| stage == "expire"),
        "expire must run: {:?}",
        cleared.executed_stages
    );
    assert_eq!(
        unbounded.stats().expired_records_removed,
        EXPIRED as u64,
        "the shipped default must clear a store smaller than its own per-round bound"
    );

    // Bounded, ONE round: the window is what limits it.
    //
    // The old form of this assertion ran many rounds and required that they had NOT finished,
    // which only held because each round wasted its window on live keys it could not remove.
    // Now a round spends its whole window on due keys, so "not finished yet" is no longer a
    // statement about the bound -- the bound is what ONE round takes.
    let bounded = runtime_with_a_live_prefix();
    let options = StorageManagerOptions {
        max_expire_hot_buckets_per_round: WINDOW,
        max_expire_cold_buckets_per_round: WINDOW,
        ..StorageManagerOptions::default()
    };
    bounded.run_storage_manager_once(1, options.clone());
    let after_one = bounded.stats().expired_records_removed;
    assert!(
        after_one > 0,
        "a bounded round removed nothing, so the window is not reaching the expired keys at all"
    );
    assert!(
        after_one <= WINDOW as u64,
        "one bounded round removed {after_one} with a window of {WINDOW} -- the bound is not \
         binding"
    );

    // And successive rounds finish the job, so the bound paces the work without stalling it.
    let rounds = EXPIRED / WINDOW + 3;
    for _ in 0..rounds {
        bounded.run_storage_manager_once(1, options.clone());
    }
    let removed = bounded.stats().expired_records_removed;
    assert_eq!(
        removed, EXPIRED as u64,
        "after {rounds} further bounded rounds the sweep should have cleared all {EXPIRED} \
         expired keys, but removed {removed}"
    );

    // That equality is also the control on the live prefix: those {LIVE_PREFIX} keys carry no
    // deadline at all, so removing one would be counted here and push the total ABOVE
    // {EXPIRED}. Exactly {EXPIRED} means the sweep took the due keys and nothing else.
}

/// Does the PERIODIC loop -- the one `bin/server.rs` starts -- actually reclaim the logs?
///
///   cargo test -p temporalstore-rust --lib what_the_periodic_loop_reclaims -- --ignored --nocapture
///
/// `bin/server.rs` says the scheduler runs "Dump, WAL and index-log reclaim ... the same phases
/// the cycle endpoint runs", and its banner prints `phases=prepare,reclaim,...`. Reading the call
/// graph says otherwise: `gc_before_sequence` is the only thing that truncates either log, and
/// nothing the periodic loop calls reaches it. This measures rather than argues, and carries the
/// on-demand cycle as a POSITIVE CONTROL so "nothing moved" cannot be mistaken for "the fixture
/// had nothing to reclaim".
#[test]
#[ignore]
fn what_the_periodic_loop_reclaims() {
    fn first_record_offset(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> u64 {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .first()
            .map(|(offset, _)| *offset)
            .unwrap_or(0)
    }
    fn record_count(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }

    fn build(records: usize) -> TemporalEngine {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("reclaim-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 512;

    // ARM A: the periodic loop, run enough times that no threshold can be the explanation.
    let periodic_engine = build(records);
    let before_low = first_record_offset(&periodic_engine, 1);
    let before_count = record_count(&periodic_engine, 1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        periodic_engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    for _ in 0..6 {
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }
    let periodic_engine = runtime.engine();
    let after_low = first_record_offset(&periodic_engine, 1);
    let after_count = record_count(&periodic_engine, 1);

    // ARM B, the POSITIVE CONTROL: the same fixture through the on-demand cycle.
    let cycle_engine = build(records);
    let control_before = record_count(&cycle_engine, 1);
    for _ in 0..6 {
        cycle_engine.run_storage_manager_cycle(crate::engine::reports::StorageManagerCycleRequest {
            shard_id: 1,
            ..crate::engine::reports::StorageManagerCycleRequest::default()
        });
    }
    let control_after = record_count(&cycle_engine, 1);
    let control_low = first_record_offset(&cycle_engine, 1);

    eprintln!("  [periodic]  {before_count:>6} records (first offset {before_low}) -> {after_count:>6} (first offset {after_low})");
    eprintln!("  [cycle   ]  {control_before:>6} records -> {control_after:>6} (first offset {control_low})");
    eprintln!(
        "  periodic reclaimed {} records; the cycle reclaimed {}",
        before_count.saturating_sub(after_count),
        control_before.saturating_sub(control_after)
    );
}

#[test]
fn the_periodic_loop_actually_reclaims_the_write_ahead_log() {
    // The stage was named `reclaim_wal` and reclaimed nothing. `gc_before_sequence` is the only
    // thing that truncates the log, and its production callers were the cycle endpoint, an
    // explicit /gc request, and the embedded proxy's own thread -- none of them this loop. So a
    // server started as shipped grew its log for ever unless something outside asked.
    //
    // Measured before the fix, six rounds on 1,512 records: 0 reclaimed. Six rounds of the
    // on-demand cycle on the same fixture: 1,511.
    fn record_count(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }
    fn build(records: usize) -> TemporalEngine {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("reclaim-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
    }
    fn run(engine: TemporalEngine, options: StorageManagerOptions) -> TemporalEngine {
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        for _ in 0..4 {
            runtime.run_storage_manager_once(1, options.clone());
        }
        runtime.engine()
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 256;

    let engine = build(records);
    let before = record_count(&engine);
    let engine = run(engine, StorageManagerOptions::default());
    let after = record_count(&engine);
    assert!(
        after < before,
        "the periodic loop must reclaim the log it dumps: {before} records before, {after} after"
    );

    // CONTROL: the same rounds with the stage switched off must reclaim NOTHING. Without this,
    // the assertion above would also pass on any unrelated path that happened to shrink the log,
    // and it is the stage under test that has to be responsible.
    let control = build(records);
    let control_before = record_count(&control);
    let control = run(
        control,
        StorageManagerOptions { enable_wal_reclaim: false, ..StorageManagerOptions::default() },
    );
    let control_after = record_count(&control);
    assert_eq!(
        control_after, control_before,
        "with reclaim_wal disabled nothing may shrink the log: {control_before} -> {control_after}"
    );
}

/// A retention cursor handed to the loop a server actually starts must reach all three decisions.
///
/// The engine has honoured these cursors for some time, but only as ARGUMENTS.
/// `StorageManagerCycleRequest` carries the two lists and is reachable over the wire
/// (`POST /storage_manager/cycle`); `StorageManagerOptions` carried neither, and
/// `start_storage_manager_scheduler_for_all_shards` -- the loop server.rs starts unconditionally
/// every 30 s -- takes `StorageManagerOptions`. So the default entry point was the one that could
/// not be told about a reader, and all four of its lifecycle requests plus both of its plan calls
/// passed `Vec::new()` because there was nothing else to pass.
///
/// THE THREE HALVES ARE ASSERTED SEPARATELY, each behind its own denominator, because they are
/// three decisions reached in three files and they are not equally reversible. The write-ahead log
/// floor (`storage_wal_reclaim_plan` -> `gc_before_sequence`) drops records; the index-log floor
/// (`apply_periodic_index_gc` -> `gc_before_sequence_limited`) drops index-log records; the prune
/// (`bucket_dump_manifest_prune_plan_at`) deletes DUMPS, and a deleted dump is the thing a node
/// rebuilt through `POST /server/storage/dumps/install` would have restored from -- unlike a
/// reclaimed log record, nothing regenerates it.
///
/// A count across them reads full on any one, which is exactly the shape that let this sit unfed:
/// the mechanism was whole and well covered at the engine's own entry. The third half is here
/// because the mutation run PROVED it was needed -- with two halves this guard passed with
/// `apply_periodic_index_gc` reverted, one fix live and one silently unguarded.
///
/// Each half carries a CONTROL arm with no cursor, so "the cursor held it" cannot be satisfied by
/// a round that simply did no work.
#[test]
fn a_retention_cursor_reaches_the_loop_a_server_actually_starts() {
    fn build(dir: &std::path::Path, records: usize) -> TemporalEngine {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.join("cache"),
            dir.join("blocks"),
            dir.join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("retention-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        engine
    }
    fn run(engine: TemporalEngine, options: StorageManagerOptions) -> TemporalEngine {
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        for _ in 0..4 {
            runtime.run_storage_manager_once(1, options.clone());
        }
        runtime.engine()
    }
    // Sequence 1 in both logs: behind everything this fixture writes, and still above zero, so the
    // plan clamps to it rather than refusing outright (a zero floor reclaims nothing and would
    // make this pass for the wrong reason).
    fn pinned() -> StorageManagerOptions {
        StorageManagerOptions {
            follower_replay_cursors: vec![crate::engine::reports::BucketDumpFollowerReplayCursor {
                follower_id: "reader-held-behind".to_string(),
                shard_id: 1,
                wal_sequence: 1,
                index_log_sequence: 1,
            }],
            ..StorageManagerOptions::default()
        }
    }

    let records = StorageManagerOptions::default().min_undumped_wal_records as usize + 256;

    // ---- HALF ONE: the write-ahead log floor. Its own denominator and its own control. ----
    fn wal_records(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("scan")
            .len()
    }

    let wal_control_dir = tempfile::tempdir().unwrap();
    let wal_control = build(wal_control_dir.path(), records);
    let wal_before = wal_records(&wal_control);
    assert!(
        wal_before > 0,
        "DENOMINATOR: the fixture must leave write-ahead log records to reclaim, got {wal_before}"
    );
    let wal_control = run(wal_control, StorageManagerOptions::default());
    let wal_control_after = wal_records(&wal_control);
    assert!(
        wal_control_after < wal_before,
        "CONTROL: with no cursor the loop must reclaim this log, or the treatment below proves \
         nothing: {wal_before} records before, {wal_control_after} after"
    );

    let wal_pinned_dir = tempfile::tempdir().unwrap();
    let wal_pinned = build(wal_pinned_dir.path(), records);
    let wal_pinned_before = wal_records(&wal_pinned);
    let wal_pinned = run(wal_pinned, pinned());
    let wal_pinned_after = wal_records(&wal_pinned);
    assert!(
        wal_pinned_after > wal_control_after,
        "a cursor at sequence 1 must hold the log the uncursored arm freed: cursored kept \
         {wal_pinned_after} of {wal_pinned_before}, uncursored kept {wal_control_after} of \
         {wal_before}"
    );

    // ---- HALF TWO: the bucket-dump manifest prune. Its own denominator and its own control. ----
    //
    // This half is the one the log counts above cannot see. It is reached through a DIFFERENT
    // request field on a DIFFERENT stage (`reclaim_index`, the only one of the four carrying
    // `prune_bucket_dump_manifests: true`), so half one can pass with this still unfed.
    fn manifest_count(engine: &TemporalEngine) -> usize {
        engine.list_bucket_dump_manifests(1).len()
    }
    fn build_with_dumps(dir: &std::path::Path) -> TemporalEngine {
        let engine = build(dir, 64);
        // Several generations, so there are older dumps for a prune to take. Each write dirties
        // the same bucket again, so each manifest sits above the last.
        for generation in 0..3 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "retention-anchor".to_string(),
                    value: format!("generation-{generation}").into_bytes(),
                },
            });
            engine
                .create_bucket_dump_manifest(1, Vec::new())
                .expect("fixture dump manifest");
        }
        engine
    }

    let prune_control_dir = tempfile::tempdir().unwrap();
    let prune_control = build_with_dumps(prune_control_dir.path());
    let prune_before = manifest_count(&prune_control);
    assert!(
        prune_before >= 2,
        "DENOMINATOR: pruning is vacuous without older dumps to take, got {prune_before}"
    );
    let prune_control = run(prune_control, StorageManagerOptions::default());
    let prune_control_after = manifest_count(&prune_control);
    assert!(
        prune_control_after < prune_before,
        "CONTROL: with no cursor the loop must prune older dumps, or the treatment below proves \
         nothing: {prune_before} manifests before, {prune_control_after} after"
    );

    let prune_pinned_dir = tempfile::tempdir().unwrap();
    let prune_pinned = build_with_dumps(prune_pinned_dir.path());
    let prune_pinned_before = manifest_count(&prune_pinned);
    let prune_pinned = run(prune_pinned, pinned());
    let prune_pinned_after = manifest_count(&prune_pinned);
    assert!(
        prune_pinned_after > prune_control_after,
        "a cursor behind every dump must keep the one it would restore from: cursored kept \
         {prune_pinned_after} of {prune_pinned_before}, uncursored kept {prune_control_after} of \
         {prune_before}"
    );

    // ---- HALF THREE: the index-log floor, which neither count above can see. ----
    //
    // `apply_periodic_index_gc` is handed a `StorageLifecycleRequest` that CARRIES both lists and
    // built its reclaim plan with an empty pair anyway, so a caller that supplied a cursor had it
    // honoured by the prune in the same request and dropped for the truncation. One request, two
    // answers, disagreeing about who is still reading.
    //
    // Asserted on the FLOOR the report carries rather than on bytes removed. The floor is the
    // decision -- `storage_index_gc_report` hands it straight to `gc_before_sequence_limited` --
    // and it is reported even when the shipped 768 KiB byte gate declines, so this needs no
    // 16,000-record fixture to be meaningful. Halves one and two both passed with this reverted,
    // which is why it needs its own arm rather than a shared count.
    let floor_dir = tempfile::tempdir().unwrap();
    let floor_engine = build_with_dumps(floor_dir.path());
    fn index_gc_request(
        cursors: Vec<crate::engine::reports::BucketDumpFollowerReplayCursor>,
    ) -> crate::engine::reports::StorageLifecycleRequest {
        crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            purge_delayed_destroy: true,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            follower_replay_cursors: cursors,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        }
    }
    let uncursored_floor = floor_engine
        .apply_periodic_index_gc(
            index_gc_request(Vec::new()),
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        )
        .retain_from_index_log_sequence;
    assert!(
        uncursored_floor > 2,
        "DENOMINATOR: with no cursor the floor must sit above the sequence the cursor below names, \
         or clamping to it cannot show: {uncursored_floor}"
    );
    let cursored_floor = floor_engine
        .apply_periodic_index_gc(
            index_gc_request(vec![
                crate::engine::reports::BucketDumpFollowerReplayCursor {
                    follower_id: "reader-held-behind".to_string(),
                    shard_id: 1,
                    wal_sequence: 1,
                    index_log_sequence: 1,
                },
            ]),
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        )
        .retain_from_index_log_sequence;
    assert!(
        cursored_floor < uncursored_floor,
        "the cursor on the request must clamp the index-log floor it is handed: cursored floor \
         {cursored_floor}, uncursored floor {uncursored_floor}"
    );
}

#[test]
fn the_periodic_loop_actually_reclaims_the_index_log() {
    // #1470 gave this loop its write-ahead log reclaim and left the index log open, because
    // `storage_index_gc_report` is engine-internal and takes a cycle request for its thresholds.
    // Until this, the index log was truncated only by the cycle endpoint, an explicit /gc request,
    // or the embedded proxy's own thread -- so a server started as shipped grew it for ever, the
    // same way it grew the write-ahead log.
    //
    // The fixture is large on purpose. The shipped gate needs BOTH triggers, ANDed: at least
    // `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` (768 KiB) of index log AND at least 40%
    // removable. Measured, an index-log record is about 59 B, so 8,000 records is 477 KB and the
    // byte gate declines -- correctly. 16,000 reaches 943 KB and it fires. Testing at a tuned-down
    // threshold would exercise a gate no deployment runs.
    fn index_log_records(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .index_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    fn runtime_with_records(records: usize) -> (DataNodeRuntime, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("index-gc-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let before = index_log_records(&engine, 1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        (runtime, before)
    }

    const RECORDS: usize = 16_000;

    let (runtime, before) = runtime_with_records(RECORDS);
    assert!(before > 0, "the fixture must write index-log records, got {before}");
    for _ in 0..3 {
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }
    let runtime_engine = runtime.engine();
    let after = index_log_records(&runtime_engine, 1);
    assert!(
        after < before,
        "the periodic loop must reclaim index-log records: {before} before, {after} after"
    );

    // CONTROL: the same rounds with the stage switched off must reclaim NOTHING. Without it,
    // "the log shrank" could be the dump, the prune or the roll-forward, and the assertion above
    // would pass while proving nothing about index GC.
    let (control, control_before) = runtime_with_records(RECORDS);
    for _ in 0..3 {
        control.run_storage_manager_once(
            1,
            StorageManagerOptions {
                enable_index_gc: false,
                ..StorageManagerOptions::default()
            },
        );
    }
    let control_engine = control.engine();
    let control_after = index_log_records(&control_engine, 1);
    assert_eq!(
        control_after, control_before,
        "with index GC off nothing may truncate the index log: {control_before} before, \
         {control_after} after -- if this moved, the stage under test is not the one responsible"
    );
}


/// Why does the index-log reclaim decline? Prints the gate.
///
///   cargo test -p temporalstore-rust --lib what_the_index_gc_gate_says -- --ignored --nocapture
#[test]
#[ignore]
fn what_the_index_gc_gate_says() {
    for records in [8_000usize, 16_000, 32_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        // A round first, so the dump has run and the plan reflects it.
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let engine = runtime.engine();
        let wal_plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {records:>6} records -> wal_safe={} blockers={:?}",
            wal_plan.safe_to_reclaim, wal_plan.blocker_reasons
        );
        let report = engine.apply_periodic_index_gc(
            crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                purge_delayed_destroy: true,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                ..crate::engine::reports::StorageLifecycleRequest::default()
            },
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        );
        eprintln!(
            "  {records:>6} records -> enabled={} applied={} safe_dirty={} bytes_before={} \
             threshold={} usage={}bp trigger={}bp retain_from={} before={} after={}",
            report.enabled,
            report.applied,
            report.dirty_buckets_committed_before_truncate,
            report.bytes_before,
            report.bytes_threshold,
            report.usage_ratio_basis_points,
            report.usage_ratio_trigger_basis_points,
            report.retain_from_index_log_sequence,
            report.records_before,
            report.records_after,
        );
    }
}

/// What does the index-GC GATE cost when it cannot possibly fire? Prints.
///
///   cargo test --release -p temporalstore-rust --lib what_the_index_gc_gate_costs -- --ignored --nocapture
///
/// The gate needs BOTH triggers, ANDed: at least 768 KiB of index log AND at least 40% removable.
/// It establishes the second by scanning the WHOLE log and decoding every record -- and only then
/// checks the first, which is a file length. So every round below the byte threshold pays a full
/// scan to learn it was never eligible.
#[test]
#[ignore]
fn what_the_index_gc_gate_costs() {
    for records in [4_000usize, 8_000, 16_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-cost-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let engine = runtime.engine();
        let log_bytes = engine.index_log_store().log_len_bytes(1);

        let lifecycle_request = || crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            purge_delayed_destroy: true,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        };

        // Attribute the three pieces separately. Timing only the enclosing call cannot say which
        // of them costs, and the first guess -- the index-log scan -- turned out to be wrong.
        let started = std::time::Instant::now();
        for _ in 0..5 {
            let _ = engine.storage_lifecycle_plan(lifecycle_request());
        }
        let plan_ms = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        let started = std::time::Instant::now();
        for _ in 0..5 {
            let _ = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        }
        let wal_plan_ms = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        let started = std::time::Instant::now();
        let mut applied_any = false;
        for _ in 0..5 {
            let report = engine.apply_periodic_index_gc(lifecycle_request(), None, crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND);
            applied_any |= report.applied;
        }
        let per_call = started.elapsed().as_micros() as f64 / 5.0 / 1000.0;

        eprintln!(
            "  [gate] {records:>6} records, log {log_bytes:>8} B -> whole {per_call:>7.1} ms = \
             plan {plan_ms:>7.1} + wal_plan {wal_plan_ms:>6.1} + rest \
             {:>6.1}, applied={applied_any}",
            per_call - plan_ms - wal_plan_ms
        );
    }
}

/// How many times does ONE maintenance round materialize EVERY live page? Prints.
///
///   cargo test --release -p temporalstore-rust --lib how_many_times_a_round_walks_every_live_block -- --ignored --nocapture
///
/// `collect_live_block_entries` builds a fresh `Vec<LiveBlockEntry>` holding every live page in the
/// shard. It has roughly twenty call sites, and a single periodic round reaches many of them:
/// `storage_wal_reclaim_plan` (via `bucket_storage_summaries`), `storage_lifecycle_plan`, the
/// compaction preamble, eviction victim selection, dump-manifest creation. None of them shares a
/// result with the next.
///
/// This is why the stage timings look the way they do. `what_the_index_gc_gate_costs` attributes
/// the index-GC stage as roughly 60% `storage_wal_reclaim_plan` and 30% `storage_lifecycle_plan`
/// at every corpus size, both scaling linearly with the record count -- 234 ms and 120 ms
/// respectively at 16,000 records, on a loop whose period is 30 s. Neither number is bounded by
/// anything the round was asked to do.
///
/// The multiplier below is the thing to fix, and it is the honest way to size it: not "this stage
/// is slow" but "the round walks the whole shard N times, and N is a property of how the stages
/// are wired rather than of how much work the round was asked to do".
#[test]
#[ignore]
fn how_many_times_a_round_walks_every_live_block() {
    for records in [2_000usize, 8_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..records {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("walks-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }

        // Measured BEFORE the runtime takes the engine, and before the counter is reset, so this
        // report's own walk is not counted against the round.
        let live_blocks: u64 = engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.block_ref_count as u64)
            .sum();

        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        crate::engine::reset_live_block_scan_entries();
        let started = std::time::Instant::now();
        let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let round_ms = started.elapsed().as_micros() as f64 / 1000.0;
        let scanned = crate::engine::live_block_scan_entries();

        // Denominators. A shard with no live pages, or a round that walked nothing, would make
        // the ratio below meaningless rather than zero.
        assert!(live_blocks > 0, "fixture stored no live pages");
        assert!(
            scanned > 0,
            "the round materialized no live-page entries, so it never reached the stages this measures",
        );

        eprintln!(
            "  [walks] {records:>6} records, {live_blocks:>6} live pages -> round {round_ms:>8.1} ms \
materialized {scanned:>8} live-page entries = {:>5.1}x the shard, stages {:?}",
            scanned as f64 / live_blocks as f64,
            report.executed_stages,
        );
    }
}

/// WHICH stages do the walking? Attributes the round's live-page scan volume per stage. Prints.
///
///   cargo test --release -p temporalstore-rust --lib which_stages_walk_every_live_block -- --ignored --nocapture
///
/// `how_many_times_a_round_walks_every_live_block` establishes that one round materializes every
/// live page ~35x. That number says a fix is worth doing but not where to apply it. This names the
/// stages, by toggling each one off and diffing the scan counter -- the same subtract-one-stage
/// shape `what_each_maintenance_stage_costs` uses for time, so the two can be read together.
///
/// Read the DELTA (all-on minus without-this-stage), not the without-column. A stage that shares
/// its walks with another stage will under-report here, because switching it off leaves the other
/// one still walking -- so the deltas are a lower bound per stage and need not sum to the total.
/// That is a property worth seeing rather than hiding: where the deltas fall well short of the
/// total, the walking is SHARED, and hoisting one materialization helps more than removing any
/// single stage would.
#[test]
#[ignore]
fn which_stages_walk_every_live_block() {
    fn build(objects: usize) -> (tempfile::TempDir, DataNodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("stage-scan-{index:06}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        (dir, runtime)
    }

    fn scan_once(objects: usize, options: StorageManagerOptions) -> (u64, Vec<String>) {
        let (_dir, runtime) = build(objects);
        crate::engine::reset_live_block_scan_entries();
        let report = runtime.run_storage_manager_once(1, options);
        (crate::engine::live_block_scan_entries(), report.executed_stages)
    }

    for objects in [4_000usize] {
        let (all_on, stages) = scan_once(objects, StorageManagerOptions::default());
        assert!(
            all_on > 0,
            "the round materialized nothing, so this attributes nothing",
        );

        // WHY the two stages below walk so much: they rebuild the same plans. Both counters
        // already exist for exactly this question, and a COUNT is immune to whatever else is
        // running on the box, where a timing would not be.
        let (_dir, runtime) = build(objects);
        crate::engine::reset_storage_plan_build_counts();
        let _ = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let (lifecycle_builds, wal_builds) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  [stage-scan] {objects:>6}   ONE round builds the lifecycle plan {lifecycle_builds} \
time(s) and the WAL reclaim plan {wal_builds} time(s); each walks the shard"
        );
        eprintln!(
            "  [stage-scan] {objects:>6} objects, everything on -> {all_on:>9} live-page entries \
= {:>5.1}x the shard, stages {stages:?}",
            all_on as f64 / objects as f64,
        );

        for (name, off) in [
            ("prepare", StorageManagerOptions { enable_prepare: false, ..Default::default() }),
            ("reclaim_wal", StorageManagerOptions { enable_wal_reclaim: false, ..Default::default() }),
            ("reclaim_memory", StorageManagerOptions { enable_memory_reclaim: false, ..Default::default() }),
            ("expire", StorageManagerOptions { enable_expire: false, ..Default::default() }),
            ("reclaim_page", StorageManagerOptions { enable_block_gc: false, ..Default::default() }),
            ("compact_pages", StorageManagerOptions { enable_block_compaction: false, ..Default::default() }),
            ("reclaim_index", StorageManagerOptions { enable_index_gc: false, ..Default::default() }),
            ("reap_metrics", StorageManagerOptions { enable_metrics_reap: false, ..Default::default() }),
        ] {
            // A stage that did not RUN in the all-on round contributes 0 here, and that zero
            // reads exactly like "this stage does no walking" while meaning "this row measured
            // nothing". Say which it is. `compact_pages` is the one that matters: it does not run
            // on this fixture, yet its preamble is six whole-shard passes when it does.
            let ran = stages.iter().any(|stage| stage == name);
            let (without, _) = scan_once(objects, off);
            eprintln!(
                "  [stage-scan] {objects:>6}   without {name:<15} {without:>9} \
(stage accounts for {:>9} entries, {:>5.1}x the shard){}",
                all_on.saturating_sub(without),
                all_on.saturating_sub(without) as f64 / objects as f64,
                if ran { "" } else { "   <- DID NOT RUN on this fixture; the zero measures nothing" },
            );
        }

        // `enable_evict` used to be the ONE stage flag that defaulted to false, so a subtract-one
        // row for it would have toggled nothing. It now defaults ON, and what is special about it
        // instead is its BOUND: the shipped `index_resident_limit_bytes` is the higher bar, so the
        // default round declines under it and the row above measures a refusal. This arm pins the
        // bound away, so it still measures what an ACTING evict stage walks.
        let (with_evict, evict_stages) = scan_once(
            objects,
            StorageManagerOptions {
                enable_evict: true,
                index_resident_limit_bytes: 0,
                ..Default::default()
            },
        );
        eprintln!(
            "  [stage-scan] {objects:>6}   evict ships ON but bounded; unbounded -> {with_evict:>9} \
(adds {:>9} entries, {:>5.1}x the shard), stages {evict_stages:?}",
            with_evict.saturating_sub(all_on),
            with_evict.saturating_sub(all_on) as f64 / objects as f64,
        );
    }
}

/// FOOTPRINT CADENCE: does every subsystem reach a steady state, or keep growing? Prints.
///
///   cargo test --release -p temporalstore-rust --lib the_footprint_cadence_over_many_rounds -- --ignored --nocapture
///
/// This is the shared measurement spine for the per-subsystem scale work. One fixture, many
/// maintenance rounds, and after EACH round every subsystem's footprint is recorded on one line,
/// so the question "is the cadence good" is answered by reading a column rather than by running
/// ten separate experiments that cannot be compared to each other.
///
/// WHAT A GOOD COLUMN LOOKS LIKE: it rises while the shard fills and then FLATTENS. A column that
/// climbs every round on a fixture that stops writing is the #1565 shape -- an idle shard grew a
/// slab every thirty seconds for ever, eleven after ten rounds, because a `min` over a set with an
/// immovable member froze the reclaim floor. That defect was invisible in any single round and
/// obvious in a column.
///
/// The write phase stops before the rounds begin ON PURPOSE. Growth under continuing writes is
/// expected and says nothing; growth with no writer is the signal.
///
/// Columns, and which subsystem each belongs to:
///
///   wal_bytes / wal_seq   WAL              -- persistent bytes and the sequence, so reclaim shows
///   idx_bytes             index log        -- file length; the collector should flatten it
///   slabs / slab_bytes    page/block store -- slab count is the #1565 signal
///   cache_mem             eviction         -- the only memory the evict gate can see
///   bkt_idx               (memory)         -- bucket index entries: never evicted, expect FLAT
///   manifests             dump             -- retained manifests; the prune policy should bound
///   dirty                 store manager    -- dirty buckets; should fall to 0 with no writer
#[test]
#[ignore]
fn the_footprint_cadence_over_many_rounds() {
    const RECORDS: usize = 8_000;
    const ROUNDS: usize = 12;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Overwrite a third, so there is genuine garbage for the collectors to reclaim. Without this
    // every collector correctly does nothing and a flat column proves only that nothing happened.
    for index in 0..RECORDS / 3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'w'; 192],
            },
        });
    }

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let engine = runtime.engine();

    eprintln!(
        "  [cadence] {RECORDS} records written, then NO further writes -- any column that keeps \
climbing below is growth with no writer"
    );
    eprintln!(
        "  [cadence] {:>5} {:>11} {:>9} {:>6} {:>11} {:>10} {:>8} {:>9} {:>6}  stages",
        "round", "wal_bytes", "idx_bytes", "slabs", "slab_bytes", "cache_mem", "bkt_idx",
        "manifests", "dirty",
    );

    let mut first = None;
    let mut last = None;
    for round in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());

        let wal = engine.write_ahead_log_store().stats(1);
        let idx_bytes = engine.index_log_store().log_len_bytes(1);
        let slab_ids = engine.block_store().slab_ids().unwrap_or_default();
        let slab_bytes: u64 = engine
            .block_store()
            .slab_block_counts()
            .unwrap_or_default()
            .iter()
            .map(|(_id, physical_bytes, _count)| *physical_bytes)
            .sum();
        let cache_mem = engine.cache().stats().memory_bytes;
        let summaries = engine.bucket_storage_summaries(1);
        let bkt_idx = summaries.len() as u64;
        let dirty = summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .count();
        let manifests = engine.list_bucket_dump_manifests(1).len();

        let row = (
            wal.persistent_bytes,
            idx_bytes,
            slab_ids.len() as u64,
            slab_bytes,
            cache_mem,
            bkt_idx,
            manifests as u64,
        );
        if round == 0 {
            first = Some(row);
        }
        last = Some(row);

        eprintln!(
            "  [cadence] {round:>5} {:>11} {idx_bytes:>9} {:>6} {slab_bytes:>11} {cache_mem:>10} \
{bkt_idx:>8} {manifests:>9} {dirty:>6}  {:?}",
            wal.persistent_bytes,
            slab_ids.len(),
            report.executed_stages,
        );
    }

    let (first, last) = (first.expect("a round ran"), last.expect("a round ran"));
    // Denominator: a fixture where nothing was stored would print zeros in every column and every
    // "did not grow" assertion below would hold vacuously.
    assert!(
        last.5 > 0,
        "the shard holds no buckets, so every column below is trivially flat",
    );

    eprintln!(
        "  [cadence] first -> last:  wal {} -> {}   idx {} -> {}   slabs {} -> {}   cache {} -> {}",
        first.0, last.0, first.1, last.1, first.2, last.2, first.4, last.4,
    );
    eprintln!(
        "  [cadence] READ THE COLUMNS, not this line: a subsystem whose column climbs to the last \
round with no writer is the one to open a thread on."
    );

    // WHICH STAGE GROWS THE INDEX LOG? The column above climbs every round on a shard nobody is
    // writing to. `compact_pages` also runs every round on that shard, and compaction relocates
    // pages and persists the resulting index -- so the obvious suspect is that the index log is
    // growing because compaction keeps rewriting it, not because anything changed.
    //
    // Obvious is not measured. A second fixture, identical except compaction is OFF, settles it:
    // if the growth persists with compaction disabled the suspect is wrong.
    let quiet = TemporalEngine::default();
    quiet.load_shard(1);
    for index in 0..RECORDS {
        quiet.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'v'; 128],
            },
        });
    }
    for index in 0..RECORDS / 3 {
        quiet.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cadence-{index:07}"),
                value: vec![b'w'; 192],
            },
        });
    }
    let quiet_runtime = DataNodeRuntime::new_without_workers_with_options(
        quiet,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let quiet_engine = quiet_runtime.engine();
    let no_compaction = StorageManagerOptions {
        enable_block_compaction: false,
        ..StorageManagerOptions::default()
    };
    let mut quiet_first = 0u64;
    let mut quiet_last = 0u64;
    for round in 0..ROUNDS {
        let _ = quiet_runtime.run_storage_manager_once(1, no_compaction.clone());
        let idx = quiet_engine.index_log_store().log_len_bytes(1);
        if round == 0 {
            quiet_first = idx;
        }
        quiet_last = idx;
    }

    let with_compaction_growth = last.1.saturating_sub(first.1);
    let without_compaction_growth = quiet_last.saturating_sub(quiet_first);
    eprintln!(
        "  [cadence] index-log growth over {} rounds with NO writer: compaction ON {} bytes, \
compaction OFF {} bytes",
        ROUNDS - 1,
        with_compaction_growth,
        without_compaction_growth,
    );

    // Denominator: if the compaction-on arm did not grow either, this comparison is measuring
    // nothing and the attribution below would be read off two zeros.
    assert!(
        with_compaction_growth > 0,
        "the index log did not grow even with compaction on, so this arm attributes nothing",
    );
}

#[test]
fn the_dump_cap_bounds_a_stage_but_not_a_round() {
    // Two facts, and the second is the surprising one.
    //
    // `reclaim_wal` honours `max_dump_buckets_per_round` (64). `reclaim_index` does NOT, and that
    // is deliberate: `wal_plan.safe_to_reclaim` needs a durable manifest for every live
    // generation, and its whole-dirty-set dump is what produces one. Capping it was tried and
    // stopped index-log reclaim dead -- 16,000 records before a round, 16,000 after.
    //
    // So the option bounds a STAGE, not a round, and a default round still dumps everything. If
    // you came here because you capped that stage and this test failed, that is the reason, and
    // `the_periodic_loop_actually_reclaims_the_index_log` is what breaks next.
    let cap = StorageManagerOptions::default().max_dump_buckets_per_round;
    assert!(cap > 0, "this test is about a non-zero cap");
    let keys = cap * 20;

    fn dirty_after_one_round(keys: usize, options: StorageManagerOptions) -> (usize, usize) {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cap-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let first = runtime.run_storage_manager_once(1, options.clone());
        let second = runtime.run_storage_manager_once(1, options);
        (
            first.pressure.dirty_bucket_count,
            second.pressure.dirty_bucket_count,
        )
    }

    // The stage that honours the cap dumps exactly the cap.
    let (before, after) = dirty_after_one_round(
        keys,
        StorageManagerOptions {
            enable_memory_reclaim: false,
            enable_index_gc: false,
            ..StorageManagerOptions::default()
        },
    );
    assert_eq!(
        before.saturating_sub(after),
        cap,
        "reclaim_wal alone must dump exactly its cap: {before} -> {after}"
    );

    // A whole round does not, because reclaim_index dumps the rest on purpose.
    let (before, after) = dirty_after_one_round(keys, StorageManagerOptions::default());
    assert_eq!(
        after, 0,
        "a default round dumps the whole dirty set, because index GC needs the coverage: \
         {before} -> {after}"
    );
}

/// How many O(shard) plans does ONE round build? Prints.
///
///   cargo test -p temporalstore-rust --lib what_one_round_rebuilds -- --ignored --nocapture
///
/// #1496 attributed the index-GC stage: 95% of its 365 ms at 16k records is rebuilding a
/// lifecycle plan and a WAL reclaim plan, not the index-log work it exists to do. This asks the
/// wider question -- how many such plans a whole round builds -- because the answer decides
/// whether reuse is worth the correctness risk.
///
/// COUNTED, not timed. A count does not move when another build is running on the box, and the
/// question is how much duplicated work happens, which is a count. #1496's timings were taken on
/// a quiet box; these do not need one.
#[test]
#[ignore]
fn what_one_round_rebuilds() {
    for keys in [2_000usize, 8_000] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("plan-{index:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        crate::engine::reset_storage_plan_build_counts();
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let (lifecycle, wal) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  {keys:>6} keys -> one round built {lifecycle} lifecycle plans and {wal} wal \
             reclaim plans"
        );

        // And the same for the stage #1496 measured, on its own.
        crate::engine::reset_storage_plan_build_counts();
        let engine = runtime.engine();
        let _ = engine.apply_periodic_index_gc(
            crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                ..crate::engine::reports::StorageLifecycleRequest::default()
            },
            None,
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        );
        let (lifecycle, wal) = crate::engine::storage_plan_build_counts();
        eprintln!(
            "  {keys:>6} keys -> apply_periodic_index_gc alone built {lifecycle} lifecycle and \
             {wal} wal reclaim plans"
        );
    }
}

/// WHICH periodic stage truncates the logs, and what gates it? Prints.
///
///   cargo test -p temporalstore-rust --lib what_gates_the_periodic_truncation -- --ignored --nocapture
///
/// #1470 said the periodic scheduler reached none of `gc_before_sequence`'s production callers.
/// That was too strong, and this is the measurement that says so. `run_gc_inner` truncates BOTH
/// logs and the page-GC stage calls it -- but only when `stale_block_pressure` holds. #1470's
/// probe wrote 1,512 keys and never deleted, so there were no stale slabs, the gate was false,
/// and the stage never ran. "Six rounds reclaimed 0" was true for a WRITE-ONLY workload and I
/// generalised it into "the loop never truncates".
///
/// Two arms, identical except for the deletes, so the gate is the only difference between them.
#[test]
#[ignore]
fn what_gates_the_periodic_truncation() {
    fn wal_records(engine: &TemporalEngine, shard_id: crate::types::ShardId) -> usize {
        engine
            .write_ahead_log_store()
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    // A 2x2 on the two variables that differed between #1470's fixture and the first attempt
    // here -- record COUNT and value SIZE -- because changing both at once cannot say which one
    // decides. Plus the delete arm, which fails for a different and already-identified reason.
    for (label, delete_every, keys, value_bytes) in [
        ("1256 x 64", 0usize, 1_256usize, 64usize),
        ("1256 x 96", 0, 1_256, 96),
        ("2000 x 64", 0, 2_000, 64),
        ("2000 x 96", 0, 2_000, 96),
        ("2000 x 96 del", 2, 2_000, 96),
    ] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("gate-{index:06}"),
                    value: vec![b'v'; value_bytes],
                },
            });
        }
        if delete_every > 0 {
            for index in (0..keys).step_by(delete_every) {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete {
                        key: format!("gate-{index:06}"),
                    },
                });
            }
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let engine_before = runtime.engine();
        let before = wal_records(&engine_before, 1);
        let mut report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let mut block_gc_rounds = 0;
        if report.executed_stages.iter().any(|stage| stage == "reclaim_page") {
            block_gc_rounds += 1;
        }
        for _ in 0..5 {
            report = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
            if report.executed_stages.iter().any(|stage| stage == "reclaim_page") {
                block_gc_rounds += 1;
            }
        }
        let _ = block_gc_rounds;
        let engine_after = runtime.engine();
        let after = wal_records(&engine_after, 1);
        // Why did (or did not) it reclaim? The plan is the thing that decides.
        let plan = engine_after.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {label:>13}  plan: safe={} covered={} uncovered={} retain_from={} current={} \
             blockers={:?}",
            plan.safe_to_reclaim,
            plan.covered_bucket_count,
            plan.uncovered_bucket_count,
            plan.retain_from_wal_sequence,
            plan.current_wal_sequence,
            plan.blocker_reasons,
        );
        eprintln!(
            "  {label:>13}  log-resident pages still registered: {}",
            engine_after.wal_resident_block_count(1)
        );
        // The clamp `gc_before_sequence` actually applies. A permissive plan still cannot drop
        // anything above this, so it is the field that explains a safe plan reclaiming nothing.
        eprintln!(
            "  {label:>13}  durable frontier: wal={} index_log={}",
            plan.durable_bucket_generation_frontier_wal_sequence,
            plan.durable_bucket_generation_frontier_index_log_sequence,
        );
        eprintln!(
            "  {label:>12}: stale_slabs={} reclaim_candidates={} reclaimable_bytes={} \
             page_gc_rounds={block_gc_rounds}/6 wal {before} -> {after}",
            report.pressure.stale_block_slab_count,
            report.pressure.reclaim_candidate_count,
            report.pressure.reclaimable_physical_bytes,
        );
    }
}

/// Do BOTH logs stay bounded while ingestion keeps going? Prints a per-round table.
///
///   cargo test -p temporalstore-rust --lib do_both_logs_stay_bounded_under_ingestion \
///       -- --ignored --nocapture --test-threads=1
///
/// Everything this session wired -- #1470 (WAL reclaim on the periodic loop), #1490 (index-log
/// reclaim), #1500 (the dump cap bounds a stage not a round), #1503 (the cycle's round bounds) --
/// was verified one stage at a time on a STATIC store: write everything, then run rounds. That is
/// not how a server runs. The question this answers is the one that actually matters: with writes
/// arriving continuously, do the logs reach a steady state, or do they grow for ever anyway?
///
/// The shape is deliberate. Each round writes a batch and THEN runs one maintenance round, which
/// is the real interleaving -- the dump, the reclaim and the next batch all racing the same shard
/// lock. A test that writes everything first cannot see a reclaim that is always one round behind
/// its own ingest.
#[test]
#[ignore]
fn do_both_logs_stay_bounded_under_ingestion() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    fn wal_records(engine: &TemporalEngine) -> usize {
        engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }
    fn index_records(engine: &TemporalEngine) -> usize {
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    const BATCH: usize = 2_000;
    const ROUNDS: usize = 12;

    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );

    eprintln!("  round   written      wal   index    wal_peak  index_peak");
    let mut written = 0usize;
    let mut wal_peak = 0usize;
    let mut index_peak = 0usize;
    for round in 1..=ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            let key = format!("soak-{:08}", written + index);
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key, value: vec![b'v'; 96] },
            });
            assert!(response.status.ok, "write failed: {:?}", response.status);
        }
        written += BATCH;

        // One maintenance round, exactly as the scheduler runs it.
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());

        let engine = runtime.engine();
        let wal = wal_records(&engine);
        let index = index_records(&engine);
        wal_peak = wal_peak.max(wal);
        index_peak = index_peak.max(index);
        // WHY, per round. A log that never shrinks under ingestion is either being declined by
        // the plan or never reaching a threshold, and those want different fixes.
        let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        eprintln!(
            "  {round:>5}   {written:>7}   {wal:>6}  {index:>6}   safe={} cov={} uncov={} \
             retain_wal={} retain_idx={} blockers={:?}",
            plan.safe_to_reclaim,
            plan.covered_bucket_count,
            plan.uncovered_bucket_count,
            plan.retain_from_wal_sequence,
            plan.retain_from_index_log_sequence,
            plan.blocker_reasons,
        );
    }

    let engine = runtime.engine();
    let wal = wal_records(&engine);
    let index = index_records(&engine);
    eprintln!(
        "  ingested {written}, wal ends at {wal} (peak {wal_peak}), index ends at {index} (peak {index_peak})"
    );
    // Deliberately no assertion yet: this prints first so the steady state can be READ off a real
    // interleaving before anything is pinned to a number picked from a static fixture.
}

/// How large must the index-GC round cap be to KEEP UP with ingestion? Prints.
///
///   cargo test -p temporalstore-rust --lib what_index_gc_cap_keeps_up \
///       -- --ignored --nocapture --test-threads=1
///
/// #1516 unfroze the reclaim floor and both logs started reclaiming. The WAL reaches a steady
/// state; the index log does not, because `index_gc_max_entries_per_round` removes 256 records a
/// round while ingest adds 2,000. This sweeps the cap on the same interleaved fixture -- write a
/// batch, run one maintenance round -- and reports whether the log converges or diverges.
///
/// 256 is the CYCLE's default, chosen for an on-demand call. A periodic loop racing live ingest
/// is a different workload, and the question is what value makes the log stop growing rather than
/// what value is tidy.
///
/// MEASURED -- and the answer is that no value does:
///
///      cap   ingested    index_log   verdict
///      256      16000        15488   diverging
///     1024      16000        13952   diverging
///     2048      16000        11904   diverging
///     4096      16000        11904   diverging
///
/// Removal rises with the cap up to 2,048 and then stops dead: 4,096 leaves the log at exactly the
/// same size, so on this workload, above 2,048 the cap is not what binds -- 512 records a round
/// against 2,000 arriving, and raising the constant does not reach it.
///
/// Two limits on how far to read that. This fixture writes 16,000 DISTINCT keys, so almost every
/// index record is the current version of a live key and there is legitimately little to reclaim:
/// "diverging" here is not by itself evidence of a defect. And `what_the_stage_order_buys`, whose
/// fixture overwrites a small key space so records actually become garbage, reclaims 1,765 records
/// a round rather than 64 on the same batch size. So what binds at 512 is specific to insert-only
/// traffic and is NOT identified here -- do not read this as a general ceiling upstream of the
/// knob. What it does establish is that this knob is not the lever on that shape of load, and a
/// future round of tuning should re-run the sweep rather than assume the cap was left too low.
#[test]
#[ignore]
fn what_index_gc_cap_keeps_up() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 8;

    fn index_records(engine: &TemporalEngine) -> usize {
        engine
            .index_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or(0)
    }

    eprintln!("     cap   ingested    index_log   verdict");
    for cap in [256usize, 1_024, 2_048, 4_096] {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let options = StorageManagerOptions {
            index_gc_max_entries_per_round: cap,
            ..StorageManagerOptions::default()
        };
        let mut written = 0usize;
        for _ in 0..ROUNDS {
            let engine = runtime.engine();
            for index in 0..BATCH {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("cap-{:08}", written + index),
                        value: vec![b'v'; 96],
                    },
                });
            }
            written += BATCH;
            runtime.run_storage_manager_once(1, options.clone());
        }
        let engine = runtime.engine();
        let index = index_records(&engine);
        // Converging means the log is a bounded multiple of one batch rather than of the total.
        let verdict = if index < BATCH * 2 {
            "bounded"
        } else if index < written / 2 {
            "lagging"
        } else {
            "diverging"
        };
        eprintln!("  {cap:>6}   {written:>8}   {index:>10}   {verdict}");
    }
}

/// Does reclaiming the index log before rewriting blocks trim more of it per round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_the_stage_order_buys \
///       -- --ignored --nocapture --test-threads=1
///
/// The round-cap sweep established that index-log removal saturates at 512 records a round no
/// matter how large the round cap is -- 2,048 and 4,096 leave the log at exactly the same size --
/// so the cap is not what binds. One candidate for what does: compaction rewrites live records
/// into fresh blocks and appends an index record for each, and it ran BEFORE the reclaim. Those
/// records sit above the floor the reclaim computed, so no round cap can touch them.
///
/// The fixture OVERWRITES a fixed key space rather than appending fresh keys. That matters: an
/// insert-only load leaves no stale pages, so compaction never fires, and a first version of this
/// probe measured a run where `compact_pages` executed zero times out of eight -- a number that
/// looked like a clean result and was actually a measurement of nothing. The assertion below is
/// there so that can never pass silently again.
///
/// MEASURED, both arrangements, compaction firing in all eight rounds of each:
///
///     compact then reclaim (before)   ingested=16000  index_log=1873  per_round=1765
///     reclaim then compact (after)    ingested=16000  index_log=1873  per_round=1765
///
/// Identical. The candidate above is REFUTED: compaction's fresh records are above the floor the
/// reclaim computed, and a record above the floor is protected whichever order the stages run in,
/// so moving the reclaim earlier cannot collect them sooner. The stage order was changed to match
/// the intended sequence, not to buy throughput, and this probe exists so a later change that
/// claims otherwise has to produce a different pair of numbers.
///
/// Note the per-round figure against the round-cap sweep's 64: the difference is the WORKLOAD,
/// not the cap. Here records genuinely become garbage; there, 16,000 distinct keys meant almost
/// every record was the live version of a key and there was little to reclaim.
#[test]
#[ignore]
fn what_the_stage_order_buys() {
    const BATCH: usize = 2_000;
    const ROUNDS: usize = 8;
    // Smaller than BATCH, so every round rewrites keys earlier rounds wrote and the blocks
    // holding the old versions become garbage -- which is what compaction needs to trigger.
    const KEYSPACE: usize = 500;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    let mut compacted_rounds = 0usize;
    let mut reclaimed_rounds = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("order-{:08}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        let report = runtime.run_storage_manager_once(1, options.clone());
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "compact_pages")
        {
            compacted_rounds += 1;
        }
        if report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_index")
        {
            reclaimed_rounds += 1;
        }
    }
    let engine = runtime.engine();
    let index = engine
        .index_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .map(|records| records.len())
        .unwrap_or(0);
    let removed = written.saturating_sub(index);
    eprintln!("  ingested={written} index_log={index} removed={removed} per_round={}",
        removed / ROUNDS);
    eprintln!("  rounds={ROUNDS} compact_pages_ran={compacted_rounds} reclaim_index_ran={reclaimed_rounds}");
    // The positive control. Where compaction never runs, the two stage orders are the same
    // program and the size above says nothing about either.
    assert!(
        compacted_rounds > 0,
        "compaction never ran, so this measures nothing about the stage order"
    );
}


/// WHAT THE MAINTAINED TALLY REMOVES, AND WHAT IT COSTS. Counts first.
///
///   cargo test -p temporalstore-rust --lib what_the_maintained_slab_tally_removes_and_costs \
///       -- --nocapture --test-threads=1
///
/// THE COST REMOVED is a whole-shard live-page walk per maintenance round per shard.
/// `storage_reclaim_slab_reports` builds the per-slab live/stale tally the reclaim planner reads,
/// and it built it by materializing every live page in the shard -- on a loop that ticks every
/// thirty seconds per shard for the life of the process. Both arms are measured HERE, in ONE
/// process against ONE corpus, by withholding the tally and asking the same question again:
/// anything else would be comparing two corpora.
///
/// THE COST ADDED is one map update per page charged or discharged, on the write path. Counted by
/// `BLOCK_SLAB_LIVE_CHARGES` rather than timed, because a count is what survives a busy box.
///
/// Durations are deliberately NOT asserted. This box cannot resolve a 15% effect and routinely
/// runs at a load average where a round-number timeout means saturation, not slowness. The counts
/// below are the measurement; a run with `--nocapture` prints them.
#[test]
fn what_the_maintained_slab_tally_removes_and_costs() {
    const RECORDS: usize = 8_000;
    const KEYSPACE: usize = 2_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    crate::engine::reset_block_slab_live_charges();
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("scale-{:06}", index % KEYSPACE),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let charges = crate::engine::block_slab_live_charges();
    let live_blocks = engine.live_block_count_for_test(1) as u64;
    let slabs = engine
        .block_slab_live_tallies(1)
        .expect("a derived tally")
        .len() as u64;

    // THE DENOMINATORS, before any ratio is taken.
    assert!(live_blocks > 0, "no live pages, so nothing below measures anything");
    assert!(slabs > 0, "no slabs in the tally: {slabs}");

    // ARM A: the tally withheld, so the report falls back to the walk.
    engine.forget_block_slab_live_for_test(1);
    crate::engine::reset_live_block_scan_entries();
    let walked_reports = engine.storage_reclaim_slab_reports_for_test(1);
    let walked_entries = crate::engine::live_block_scan_entries();

    // ARM B: the tally restored, same shard, same question.
    let drift = engine.block_slab_live_drift_check(1);
    assert!(
        drift.is_clean(),
        "the tally had drifted before the measurement, so arm B is not the same answer: {drift:?}"
    );
    crate::engine::reset_live_block_scan_entries();
    let tallied_reports = engine.storage_reclaim_slab_reports_for_test(1);
    let tallied_entries = crate::engine::live_block_scan_entries();

    eprintln!("  records={RECORDS} keyspace={KEYSPACE} live_blocks={live_blocks} slabs={slabs}");
    eprintln!("  REMOVED: live page entries materialized by one reclaim report");
    eprintln!("    walked  = {walked_entries}");
    eprintln!("    tallied = {tallied_entries}");
    eprintln!("  ADDED: tally charges over the whole ingest");
    eprintln!(
        "    charges = {charges}  ({:.2} per record, {:.2} per live page)",
        charges as f64 / RECORDS as f64,
        charges as f64 / live_blocks as f64
    );

    // THE SAME ANSWER. A cheaper round that reports something else is not a saving.
    let walked_live: Vec<(u64, u64, u64)> = walked_reports
        .iter()
        .filter(|report| report.live_block_refs > 0 || report.live_physical_bytes > 0)
        .map(|report| {
            (
                report.block_slab_id,
                report.live_block_refs,
                report.live_physical_bytes,
            )
        })
        .collect();
    let tallied_live: Vec<(u64, u64, u64)> = tallied_reports
        .iter()
        .filter(|report| report.live_block_refs > 0 || report.live_physical_bytes > 0)
        .map(|report| {
            (
                report.block_slab_id,
                report.live_block_refs,
                report.live_physical_bytes,
            )
        })
        .collect();
    assert!(
        !tallied_live.is_empty(),
        "the tallied arm reported no live slabs at all, so the comparison is vacuous"
    );
    assert_eq!(
        walked_live, tallied_live,
        "the tally and the walk disagree about the shard's live slabs"
    );

    // AND IT IS THE WALK THAT WENT. This is the whole claim, and it is the one that can regress
    // silently: a fallback that fires on every round costs the old cost and changes nothing.
    assert_eq!(
        tallied_entries, 0,
        "the tallied arm still walked {tallied_entries} live page entries -- the fallback fired"
    );
    assert!(
        walked_entries >= live_blocks,
        "the walked arm materialized {walked_entries} entries for {live_blocks} live pages, which \
         is fewer than one walk: this arm is not measuring the walk"
    );
    // The charge is per page written, not per page in the shard: a bounded cost on the write
    // path, against an unbounded one on the maintenance loop.
    assert!(
        charges <= (RECORDS as u64).saturating_mul(4),
        "{charges} charges for {RECORDS} records is more than four per write, which is not a \
         constant-per-write cost any more"
    );
}

/// Can the page-GC garbage floor ever exclude a slab IN A RUNNING STORE? It still cannot -- and
/// the reason has moved, which is the whole content of this update.
///
///   cargo test -p temporalstore-rust --lib can_the_block_gc_garbage_floor_bind \
///       -- --nocapture --test-threads=1
///
/// THE OLD REASON, now gone. `used_bytes` summed the file sizes of the slabs grouped under a
/// candidate's stored id that are NOT collectable. A stored id names exactly one slab and the
/// candidate filter is the exact negation of that test, so a candidate could not contribute to
/// its own used bytes. Every candidate read zero because two filters contradicted each other, not
/// because anything had been counted.
///
/// THE NEW REASON, which is a measurement. `used_bytes` is now the live page bytes on the slab
/// itself, from the tally the index maintains on its own mutation path and publishes into the
/// block store. A collector candidate is a slab that NO LIVE PAGE POINTS AT -- `is_live` gates
/// candidacy, and gates removal again independently -- so its maintained live bytes are genuinely
/// zero. The figure is real, it is really zero, and the floor still excludes nothing.
///
/// SO THE OBSTACLE IS THE CANDIDATE PREDICATE, NOT THE ARITHMETIC. Nothing offers this floor a
/// partially-live slab, because a slab holding one live page is not a candidate at all. That is
/// the same all-or-nothing rule that lets one live page pin a whole slab, and widening it means
/// relocating the survivors first -- a compaction decision with its own measurement.
/// `a_published_live_tally_makes_used_bytes_mean_live_block_bytes` (block_store.rs) is the other
/// half: given a 90%-live candidate, the shipped 4,000 bp floor DOES exclude it. The knob is real
/// machinery now; what it is waiting for is a caller that presents it with a partially-live slab.
///
/// AND `utility_basis_points` IS NO LONGER UNIFORMLY ZERO ACROSS THE STORE, which is the change a
/// reader of the old note would most want to know. It is uniformly zero across the CANDIDATES,
/// necessarily. `slab_live_fractions` asks the same question of every slab, and the table below
/// prints the answer; the assertion under it is that the figure discriminates.
#[test]
fn can_the_block_gc_garbage_floor_bind() {
    const BATCH: usize = 400;
    const ROUNDS: usize = 6;
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("floor-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        runtime.run_storage_manager_once(1, options.clone());
    }

    let engine = runtime.engine();
    // Hand the store the tally before asking it anything. The maintenance rounds above publish on
    // their own; this makes the test say so rather than depend on it.
    let ready_shards = engine.publish_block_slab_live_bytes();
    assert_eq!(
        ready_shards, 1,
        "the shard's tally was never derived, so `used_bytes` would fall back to the old figure \
         and this test would measure the thing it used to measure"
    );
    let live = engine.live_block_slab_ids_all_shards();
    let plan = engine
        .block_store()
        .gc_policy_plan(
            u64::MAX,
            live.iter().copied(),
            &crate::block_store::BlockStoreGcPolicy::with_slab_garbage_floor(
                crate::engine::reports::DEFAULT_BLOCK_GC_MIN_SLAB_GARBAGE_BASIS_POINTS,
                None,
            ),
        )
        .expect("plan");

    eprintln!(
        "  candidates={} selected={} skipped_by_policy={}",
        plan.candidate_count,
        plan.selected_block_slab_ids.len(),
        plan.skipped_by_policy_count
    );
    eprintln!("     slab   total_b    used_b   utility_bp   garbage_bp   floor_keeps_it");
    for candidate in plan.candidates.iter() {
        let garbage = 10_000u64.saturating_sub(candidate.utility_basis_points);
        let kept = garbage < crate::engine::reports::DEFAULT_BLOCK_GC_MIN_SLAB_GARBAGE_BASIS_POINTS;
        eprintln!(
            "  {:>7}  {:>8}  {:>8}   {:>10}   {:>10}   {}",
            candidate.block_slab_id,
            candidate.total_bytes,
            candidate.used_bytes,
            candidate.utility_basis_points,
            garbage,
            kept
        );
    }
    eprintln!(
        "  VERDICT: the floor excluded {} of {} candidates",
        plan.skipped_by_policy_count, plan.candidate_count
    );

    // THE DENOMINATOR FIRST. A floor that excluded nothing because there was nothing to exclude
    // would satisfy every assertion below while saying nothing at all, and that is the failure
    // mode this test spent its whole life in: ignored, so zero candidates and zero exclusions
    // were indistinguishable from a working floor.
    assert!(
        plan.candidate_count > 0,
        "no candidates, so this measures nothing about the floor: {plan:?}"
    );
    assert!(
        crate::engine::reports::DEFAULT_BLOCK_GC_MIN_SLAB_GARBAGE_BASIS_POINTS > 0,
        "a floor of zero excludes nothing by definition and would make this vacuous"
    );

    for candidate in plan.candidates.iter() {
        assert_eq!(
            candidate.used_bytes, 0,
            "a collector candidate is a slab no live page points at, so its MAINTAINED live bytes \
             are zero -- counted, not inferred from two filters contradicting each other: \
             {candidate:?}"
        );
        assert_eq!(
            candidate.utility_basis_points, 0,
            "so its live fraction is zero: {candidate:?}"
        );
    }
    assert_eq!(
        plan.skipped_by_policy_count, 0,
        "every candidate is 10,000 bp garbage, so the shipped floor excludes none of them. If \
         this now fails, something has started offering the collector a PARTIALLY LIVE slab, and \
         the per-round budget question (PR #1719) is open again: {plan:?}"
    );
    assert_eq!(
        plan.selected_block_slab_ids.len(),
        plan.candidate_count,
        "and every candidate is selected: {plan:?}"
    );

    // THE OTHER HALF: the same question asked of EVERY slab, not just the collectable ones.
    let fractions = engine.block_store().slab_live_fractions().expect("fractions");
    eprintln!("     slab   physical   logical      live_b   live_bp   garbage_bp");
    for fraction in fractions.iter() {
        eprintln!(
            "  {:>7}  {:>9}  {:>8}  {:>10}  {:>8}  {:>11}",
            fraction.block_slab_id,
            fraction.physical_bytes,
            fraction.logical_bytes,
            fraction.live_bytes,
            fraction.live_basis_points,
            fraction.garbage_basis_points
        );
    }
    // DENOMINATOR FIRST, again. One slab cannot show a spread.
    assert!(
        fractions.len() > 1,
        "one slab in the store, so nothing here could discriminate: {fractions:?}"
    );
    let distinct = fractions
        .iter()
        .map(|fraction| fraction.live_basis_points)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        distinct.iter().any(|points| *points > 0),
        "every slab in the store reads zero live bytes, which cannot be true of a store that is \
         serving reads -- the tally is not reaching the block store: {fractions:?}"
    );
    assert!(
        distinct.len() > 1,
        "the live fraction is the same for every slab, so it still carries no information; it is \
         supposed to separate the slabs holding live pages from the ones that do not: \
         {fractions:?}"
    );
    eprintln!(
        "  VERDICT: the floor excluded {} of {} CANDIDATES, and the store holds {} distinct live \
         fractions over {} slabs",
        plan.skipped_by_policy_count,
        plan.candidate_count,
        distinct.len(),
        fractions.len()
    );
}

/// The reclaim_page stage quarantines the slabs it reclaims instead of unlinking them.
///
/// The stage reached `gc_slabs_before_with_live_refs`, which destroys immediately. The
/// storage-manager cycle has always reached the delayed-destroy entry, so a slab it reclaimed
/// stayed on disk until a later purge and could be recovered if the retain floor turned out to
/// have been computed too generously. The periodic loop had no such second chance.
///
/// Quarantined slabs are still collected -- `DELAYED_DESTROY_MIN_AGE_MS` is an hour, and the
/// prepare stage passes `purge_delayed_destroy` whenever page GC is on -- so this trades prompt
/// space for recoverability, it does not leak.
#[test]
fn the_periodic_reclaim_quarantines_what_it_collects() {
    const BATCH: usize = 400;
    const ROUNDS: usize = 6;
    // Smaller than BATCH so later rounds overwrite earlier keys and whole slabs fall out of use.
    // With insert-only traffic every slab stays live, nothing is reclaimed at all, and the
    // assertions below would be measuring an empty run.
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();
    let mut written = 0usize;
    let mut removed = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("quarantine-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        let report = runtime.run_storage_manager_once(1, options.clone());
        if let Some(gc) = report.gc_report.as_ref() {
            removed = removed.saturating_add(gc.block_slabs_removed);
        }
    }

    // The denominator. Nothing below means anything if the stage reclaimed nothing.
    assert!(
        removed > 0,
        "the stage reclaimed no slabs, so this measures nothing about how it disposes of them"
    );

    // Purging with a zero minimum age reports exactly what the stage left in quarantine. The
    // loop's own purge cannot have taken them: it uses the one-hour minimum.
    let quarantined = runtime
        .engine()
        .block_store()
        .purge_delayed_destroy_slabs_older_than(0)
        .map(|report| report.purged_block_slab_ids.len())
        .unwrap_or(0);
    assert!(
        quarantined > 0,
        "reclaimed {removed} slabs and quarantined none of them -- the stage unlinked them outright"
    );
}

/// The maintenance round runs its stages in the intended order.
///
/// Every other assertion about stages in this suite asks whether one RAN -- `.any(|s| s == ..)` --
/// and none of them asks what ran before what, so the round could be reshuffled without a single
/// test noticing. Two stages have already been in the wrong place: the index reclaim ran after
/// compaction, and evict ran last of all, after the metrics reap.
///
/// Evict's position was the more interesting of the two. Expire drops what has died of old age and
/// evict drops what is merely cold; they are two halves of relieving memory and belong together,
/// ahead of the stages that rewrite pages. Running evict last meant it chose victims from a state
/// compaction had just churned, and the memory it freed arrived too late to spare any of those
/// stages work.
///
/// This pins the sequence itself. It asserts relative order and not an exact list, so a new stage
/// can be added without editing it -- only MOVING one of these breaks it.
#[test]
fn the_maintenance_round_runs_its_stages_in_order() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    for index in 0..600usize {
        let engine = runtime.engine();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("order-guard-{:06}", index % 100),
                value: vec![b'v'; 96],
            },
        });
    }
    // Every stage on, so the round has something to order. Eviction now ships ENABLED, so this
    // restates the shipped value rather than overriding it -- but the shipped BOUND is pinned
    // away, because at 64 MiB this fixture's evict stage declines on the bound and the order this
    // test is about is the order of stages that ran.
    let options = StorageManagerOptions {
        enable_evict: true,
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);
    let stages = report.executed_stages.clone();

    // The order the design intends. Some of these are pressure-gated and legitimately skip a
    // round -- `reclaim_memory` does exactly that on this fixture -- so the check is on the
    // stages that RAN, in the order they ran. Requiring all nine made the guard fail for a
    // reason that had nothing to do with ordering.
    let expected = [
        "prepare",
        "reclaim_wal",
        // Expire and evict come BEFORE the blanket cache invalidation, not after it: the
        // invalidation zeroes the very number eviction's gate reads, so running it first meant
        // eviction never opened its gate at all. See
        // `eviction_opens_its_gate_when_the_cache_is_still_there`.
        "expire",
        "evict",
        "reclaim_memory",
        "reclaim_page",
        "reclaim_index",
        "compact_pages",
        "reap_metrics",
    ];
    let mut previous: Option<(&str, usize)> = None;
    for name in expected {
        let Some(position) = stages.iter().position(|stage| stage == name) else {
            continue;
        };
        if let Some((earlier, earlier_position)) = previous {
            assert!(
                earlier_position < position,
                "{earlier} must run before {name}, but the round executed {stages:?}"
            );
        }
        previous = Some((name, position));
    }

    // The denominator. Skipping absent stages is what makes the loop above robust, and it is
    // also what would let it pass a round that ran nothing worth ordering -- so require the
    // three this change is actually about.
    for required in ["expire", "evict", "reclaim_page"] {
        assert!(
            stages.iter().any(|stage| stage == required),
            "{required} did not run, so the order assertion above proved nothing: {stages:?}"
        );
    }
}


/// Who actually drops the shard cache during a maintenance round? Prints.
///
///   cargo test -p temporalstore-rust --lib what_drops_the_shard_cache \
///       -- --ignored --nocapture --test-threads=1
///
/// `run_gc_inner` opens by calling `invalidate_shard`, which empties the whole shard. But
/// `reclaim_memory` ALSO passes `invalidate_cache: true` and runs earlier in the round, so on any
/// round where that stage fires the cache is already gone by the time page GC looks at it. This
/// prints, per round, which stages ran, how warm the cache was, and what each invalidation
/// actually removed -- so the size of the page-GC drop is measured rather than assumed.
#[test]
#[ignore]
fn what_drops_the_shard_cache() {
    const BATCH: usize = 300;
    const ROUNDS: usize = 8;
    const KEYSPACE: usize = 100;

    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions::default();

    eprintln!("  round  warm_bytes  mem_ran  gc_ran  gc_cache_removed  slabs_removed");
    let mut written = 0usize;
    for round in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("whodrops-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        for key_index in 0..KEYSPACE {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("whodrops-{:06}", key_index),
                },
            });
        }
        let warm = engine.cache().stats().memory_bytes;
        let report = runtime.run_storage_manager_once(1, options.clone());
        let mem_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_memory");
        let gc_ran = report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_page");
        let gc_removed = report
            .gc_report
            .as_ref()
            .map(|gc| gc.cache_entries_removed)
            .unwrap_or(0);
        let slabs = report
            .gc_report
            .as_ref()
            .map(|gc| gc.block_slabs_removed)
            .unwrap_or(0);
        eprintln!(
            "  {round:>5}  {warm:>10}  {mem_ran:>7}  {gc_ran:>6}  {gc_removed:>16}  {slabs:>13}"
        );
    }

    // Scenario two: reclaim_memory SKIPPED.
    //
    // That stage is pressure-gated. When it fires it invalidates the whole shard itself, which is
    // why page GC's own drop removes nothing above. The question this scenario answers is what
    // page GC does on a round where the earlier stage did NOT run and the cache is still warm.
    eprintln!("  -- with reclaim_memory disabled --");
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let options = StorageManagerOptions {
        enable_memory_reclaim: false,
