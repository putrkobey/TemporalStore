// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 1 of engine tests, split from engine/engine_regression_tests.rs.
#![allow(clippy::all)]
use super::*;
// FOR `points()` ON A SLOT: `super::*` re-exports the engine's items but not this trait,
// and two summary readers here walk a migrated series. Anonymous, because only the method
// is wanted.

// shared-corpus: dynamic_event_replication_mode_selection
/// Is the per-object memory permanent, or does a storage cycle give some of it back?
///
/// A write that carries its block leaves an entry in two side maps -- the log-resident registry and
/// the shard's resident-block map -- and both are released when the storage manager materialises
/// that block into the block store. The memory probe beside this one never runs a cycle, so it
/// measures a store that has been written to and never maintained. That is a real state, but it is
/// the WORST one, and reporting it as "what an object costs" would overstate the steady state.
///
/// This runs the cycle and asks what comes back. The answer is: nothing, and it asks for a great
/// deal more.
///
/// Measured at twenty thousand objects, debug build:
///
///        resident      58,404 KB before
///                      75,472 KB after writing        (873 B per object)
///                     345,012 KB after one cycle
///                     345,016 KB after two cycles
///
///        first cycle took 269,540 KB, second took 4 KB
///
/// So the cycle needs about 263 MB of working memory for a twenty-thousand-object shard -- roughly
/// thirteen kilobytes per object, wanted all at once -- and it is a PEAK, not a leak: the second
/// cycle reuses every byte of it. That distinction is the whole point of running the cycle twice.
/// One reading cannot make it, because resident memory does not fall when an allocation goes back
/// to the allocator, and "the cycle leaks 263 MB" and "the cycle needs 263 MB" call for completely
/// different work.
///
/// What it means: maintenance on a large shard needs headroom proportional to the shard, because
/// the phases are whole-shard. The counterpart in the design this follows bounds each phase per
/// round, so its working set is a function of the round limit instead. The compaction and dump
/// probes above measure the time side of that same difference.
#[test]
#[ignore]
fn does_a_storage_cycle_give_the_memory_back() {
    fn resident_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|rest| rest.split_whitespace().next().map(str::to_string))
                })
            })
            .and_then(|kb| kb.parse().ok())
            .unwrap_or(0)
    }

    const OBJECTS: usize = 20_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // A warm-up shard, so the allocator has taken its arena before anything is measured -- see the
    // note on the probe above about what the first pass otherwise charges to whoever went first.
    for index in 0..OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("warmup-{index:07}"),
                value: vec![b'v'; 96],
            },
        });
    }

    let before = resident_kb();
    for index in 0..OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("cycle-cost-{index:07}"),
                value: vec![b'v'; 96],
            },
        });
    }
    let written = resident_kb();
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let settled = resident_kb();

    println!(
        "  [cycle] {OBJECTS} objects: {:>5} B each after writing, {:>5} B each after a cycle ({} stages)",
        (written.saturating_sub(before) * 1024) / OBJECTS as u64,
        (settled.saturating_sub(before) * 1024) / OBJECTS as u64,
        cycle.stages.len(),
    );
    // A second cycle, to separate what the cycle TAKES from what it KEEPS. Resident memory does
    // not fall when an allocation is returned to the allocator, so one reading cannot tell a
    // permanent cost from a peak that is now free for the next caller. If the second cycle adds
    // little, the first one's memory is being reused and the cost is a peak; if it adds as much
    // again, the cycle is retaining it.
    let _ = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let twice = resident_kb();
    println!(
        "  [cycle] resident: {} KB before, {} KB after writing, {} KB after one cycle, {} KB after two",
        before, written, settled, twice
    );
    println!(
        "  [cycle] first cycle took {} KB, second took {} KB",
        settled.saturating_sub(written),
        twice.saturating_sub(settled),
    );
}

/// What a stored object costs in memory, and whether the cost follows the value or the object.
///
/// A block header in the design this follows spells its ids as 32-bit integers; ours holds `u64`s
/// and `String`s per item, and an address is 64 bytes on its own. Whether that is what the memory
/// goes on is a question, not an assumption, so this asks it the only way that answers: hold the
/// object count fixed and vary the value size.
///
/// A cost that tracks the VALUE is data being retained -- caches, buffers, the blocks themselves.
/// A cost that is flat in the value size is per-object structure: keys, addresses, map nodes. The
/// two want completely different fixes, and the number alone cannot tell them apart.
///
/// Measured at twenty thousand objects, debug build:
///
///        value    32 B      272 B per object
///        value   256 B      272 B per object
///        value 1,024 B      272 B per object
///
/// Flat, so it is structure and not data -- which the tiny cache above already implies and this
/// confirms. Two hundred and seventy-two bytes to hold a fourteen-byte key and where its block
/// lives: the key as a `String`, a `BlockAddress` that is 64 bytes on its own, a map entry for
/// each, and an entry in each of the two side maps a carried block needs.
///
/// The sizes are deliberately measured in more than one POSITION, and that is not fussiness. Run
/// in one order, the first size reads about 1,233 bytes per object and every later one reads 272 --
/// and the outlier follows the position, not the size. It is the allocator taking its arena, once,
/// and charging it to whoever went first. Run a size again at the end and it reads about 1 byte
/// per object, because the arena is already there. A single-pass version of this test would have
/// reported whichever number it happened to sample and looked entirely credible.
#[test]
#[ignore]
fn what_a_stored_object_costs_in_memory() {
    fn resident_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|rest| rest.split_whitespace().next().map(str::to_string))
                })
            })
            .and_then(|kb| kb.parse().ok())
            .unwrap_or(0)
    }

    const OBJECTS: usize = 20_000;
    for value_len in [1024usize, 256, 32, 1024, 32] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            // A cache this small holds nothing, so what grows is the shard's own state.
            256,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let before = resident_kb();
        for index in 0..OBJECTS {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("memory-cost-{index:07}"),
                    value: vec![b'v'; value_len],
                },
            });
        }
        let after = resident_kb();
        let grew = after.saturating_sub(before) * 1024;
        println!(
            "  [memory] value {value_len:>5} B x {OBJECTS} objects -> {:>6} KB resident, {:>5} B per object",
            grew / 1024,
            grew / OBJECTS as u64,
        );
    }
}

/// What a dump costs as the shard grows, and whether it costs the shard or the change.
///
/// `flush_shard_index` writes the index out. A bounded dump takes a capped set of dirty buckets
/// per round, so the work of one dump is a function of that cap and not of how much the shard
/// holds. Ours has no such cap, and this measures what that means: if the time grows with the
/// shard rather than with what changed since the last dump, then a store pays more to checkpoint
/// the larger it gets, forever.
///
/// One write between dumps, so what CHANGED is constant and only the shard size varies.
///
/// Measured, debug build, so a floor rather than a ceiling:
///
///        1,000 objects         77 ms
///        5,000 objects        333 ms
///       20,000 objects      1,375 ms
///
/// Twenty times the objects, eighteen times the time, for the same one-write delta. The dump
/// costs the SHARD, not the change -- so a store pays more to checkpoint the larger it grows, and
/// pays it again every time the cadence fires. The cadence is driven by undumped bytes, so a big
/// shard does not dump less often to compensate; it dumps just as often and each one costs more.
///
/// This records the cost; it is not a fix. The shape of one is visible in the design this
/// follows: bound the dirty slots per round, dump that set, advance the dumped-log id, and let
/// the next round take the next set.
#[test]
#[ignore]
fn what_a_dump_costs_as_the_shard_grows() {
    for objects in [1_000usize, 5_000, 20_000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("dump-cost-{index:07}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        // A first dump so the second one has a known, tiny delta to write.
        engine.flush_shard_index(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "dump-cost-one-more".to_string(),
                value: vec![b'v'; 96],
            },
        });
        let started = std::time::Instant::now();
        engine.flush_shard_index(1);
        let elapsed = started.elapsed();
        println!(
            "  [dump] {objects:>6} objects, ONE write since the last dump -> {:>8.1} ms",
            elapsed.as_secs_f64() * 1000.0,
        );
    }
}

/// What one compaction costs, and for how long it holds the shard.
///
/// `compact_shard_blocks` takes the shard write lock for the whole relocation, so its duration IS
/// the stall every reader and writer of the shard sees.
///
/// It is no longer unbounded: the round relocates at most `COMPACTION_ROUND_BYTES` and resumes
/// onto the slab it was filling rather than rolling a fresh one. But that budget is 256 MiB and
/// a store this size has a live set of about two, so the bound does not bind and the cost below
/// is still the whole shard. `what_the_compaction_budget_buys` above measures where it starts to.
///
/// Measured here, debug build, so a floor rather than a ceiling:
///
///        1,000 objects        413 ms       413 us per block ref
///        5,000 objects      1,876 ms       375 us per block ref
///       20,000 objects     19,664 ms       983 us per block ref
///
/// Twenty seconds at twenty thousand objects, and the per-ref cost RISES with the shard -- 2.6x
/// between five and twenty thousand -- so it is worse than linear in the thing it is unbounded in.
///
/// This records the cost rather than choosing a smaller budget, because the value is a production
/// decision and these are debug numbers. The mechanism for a smaller one is already in place --
/// a round resumes onto the slab it was filling, so stopping early no longer trades a stall for
/// slab proliferation.
/// What the compaction ROUND BUDGET actually buys, at one shard size.
///
///   cargo test -p temporalstore-rust --lib what_the_compaction_budget_buys -- --ignored --nocapture --test-threads=1
///
/// `COMPACTION_ROUND_BYTES` is 256 MiB, chosen so every store the suite builds still compacts in
/// ONE round. This asks what the first round costs at smaller budgets, because the stall it was
/// built to bound is only bounded once the budget is smaller than the live set.
#[test]
#[ignore]
fn what_the_compaction_budget_buys() {
    for budget in [
        256u64 * 1024 * 1024,
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        1024 * 1024,
        256 * 1024,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..20_000usize {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("compact-budget-{index:07}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let started = std::time::Instant::now();
        let report = engine
            .compact_shard_blocks_with_budget(1, budget)
            .expect("compaction should run");
        let elapsed = started.elapsed();
        eprintln!(
            "  [budget] {:>9} KiB -> first round {:>9.1} ms, {:>6} page refs moved",
            budget / 1024,
            elapsed.as_secs_f64() * 1000.0,
            report.rewritten_block_refs,
        );
    }
}

#[test]
#[ignore]
fn what_a_whole_shard_compaction_costs() {
    for objects in [1_000usize, 5_000, 20_000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("compaction-cost-{index:07}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let started = std::time::Instant::now();
        let report = engine.compact_shard_blocks(1).expect("compaction runs");
        let elapsed = started.elapsed();
        println!(
            "  [compaction] {objects:>6} objects -> {:>7.1} ms holding the shard, {} page refs rewritten ({:.1} us each)",
            elapsed.as_secs_f64() * 1000.0,
            report.rewritten_block_refs,
            elapsed.as_secs_f64() * 1_000_000.0 / report.rewritten_block_refs.max(1) as f64,
        );
    }
}

#[test]
fn stale_shard_index_is_refused_rather_than_decoded_with_the_wrong_key_meaning() {
    // A pre-rekey index decodes CLEANLY -- context_events is still
    // HashMap<String, BTreeMap<u64, BlockAddress>> -- while the u64 changed meaning from
    // timeline_key to event_id_hash. None of that is type-visible, so without a version stamp
    // the engine would serve an index whose events are unaddressable and whose time-windowed
    // reads return nothing, raising no error at all.
    //
    // Built from a REAL serialized shard rather than hand-written JSON, so the fixture cannot
    // drift from the struct and quietly stop exercising the decode path.
    let shard = super::super::state::ShardState::default();
    let stamped = super::super::stamp_index_format_version(&shard);
    assert_eq!(
        stamped.get("index_format_version").and_then(|v| v.as_u64()),
        Some(u64::from(super::super::SHARD_INDEX_FORMAT_VERSION)),
        "every write path must stamp the current format version"
    );

    let current = serde_json::from_value::<super::super::state::ShardState>(stamped.clone())
        .expect("a stamped index must decode");
    assert_eq!(
        current.index_format_version,
        super::super::SHARD_INDEX_FORMAT_VERSION
    );

    // Exactly what a file written before the stamp existed looks like.
    let mut legacy = stamped;
    legacy
        .as_object_mut()
        .expect("index is a json object")
        .remove("index_format_version");
    let stale = serde_json::from_value::<super::super::state::ShardState>(legacy)
        .expect("a stale index still DECODES -- that is the whole problem");
    assert_eq!(
        stale.index_format_version, 0,
        "an index written before the stamp must read as version 0"
    );
    assert!(
        stale.index_format_version < super::super::SHARD_INDEX_FORMAT_VERSION,
        "the load guard must classify a pre-stamp index as stale and refuse it"
    );
}

#[test]
fn replicated_execute_selects_sync_async_or_raft_without_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let request = ReplicatedBatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "sync-event".to_string(),
                    value: b"sync".to_vec(),
                },
                replication_mode: EventReplicationMode::SyncStorage,
            },
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "async-event".to_string(),
                    value: b"async".to_vec(),
                },
                replication_mode: EventReplicationMode::AsyncStorage,
            },
            ReplicatedCommand {
                command: Command::StringSet {
                    key: "raft-event".to_string(),
                    value: b"raft".to_vec(),
                },
                replication_mode: EventReplicationMode::Raft,
            },
        ],
    };

    let response = engine.batch_execute_replicated(request);
    assert!(response.status.ok);
    assert!(response.responses.iter().all(|response| response.status.ok));
    assert_eq!(
        response
            .replication
            .iter()
            .map(|report| report.effective_mode)
            .collect::<Vec<_>>(),
        vec![
            EventReplicationMode::SyncStorage,
            EventReplicationMode::AsyncStorage,
            EventReplicationMode::Raft,
        ]
    );
    assert!(response
        .replication
        .iter()
        .all(|report| report.accepted && !report.restart_required));
}

// shared-corpus: context_events_slabs_entities_child_refs context_event_index_audit_dirty_models
#[test]
fn context_models_match_keys_timeline_blocks_and_filters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let node = ContextNode {
        node_hash: 42,
        parent_hash: 7,
        kind: 3,
        canonical_name: "checkout".to_string(),
        l0: "service".to_string(),
        status: 1,
        last_event_time_ms: 1_000,
        l1_ref: "l1://summary".to_string(),
        raw_metadata_ref: "raw://node".to_string(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    };
    let native_node = ContextNode {
        status: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
        ..node.clone()
    };
    let upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 11,
            node: Box::new(node.clone()),
        },
    });
    assert!(upsert.status.ok);
    assert!(matches!(
        upsert.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:node:11:42"
    ));

    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 11,
            node_hash: 42,
        },
    });
    assert!(matches!(
        get.response,
        CommandResponse::ContextNode { node: Some(ref stored), .. } if stored == &native_node
    ));
    let meta = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "ctx:node:11:42".to_string(),
            field: CONTEXT_NODE_FIELD.to_string(),
        },
    });
    assert!(matches!(
        meta.response,
        CommandResponse::Bytes { value: Some(ref bytes) }
            if ContextNode::decode_context_value(bytes).as_ref() == Some(&native_node)
    ));

    let entity = ContextEntity {
        entity_hash: 7001,
        node_hash: 42,
        entity_type: 1,
        name: "gpu_purchase_request".to_string(),
        value: "approved".to_string(),
        updated_at_ms: 1_000,
        valid_from_ms: 1_000,
        confidence: 0.97,
        source_event_hashes: vec![5],
        vector: Vec::new(),
    };
    let entity_upsert = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertEntity {
            tenant_hash: 11,
            entity: entity.clone(),
        },
    });
    assert!(entity_upsert.status.ok);
    // The collection key, matching the event contract asserted below ("ctx:event:11:42").
    // Entities are grouped by node in shard state now, so the object key a command reports has
    // to be the key that state is stored under -- every key-driven mechanism in engine.rs
    // (key-state capture/apply, tombstone removal, membership) looks it up directly. The
    // per-entity key "ctx:entity:11:42:7001" still exists where it must: as the block object id,
    // so two entities of one node cannot share a block, and as the persisted entry key, which is
    // what keeps the on-disk format identical across this change.
    assert!(matches!(
        entity_upsert.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:entity:11:42"
    ));
    let entity_get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetEntity {
            tenant_hash: 11,
            node_hash: 42,
            entity_hash: 7001,
        },
    });
    assert!(matches!(
        entity_get.response,
        CommandResponse::ContextEntity { entity: Some(ref stored), .. } if stored == &entity
    ));
    let entity_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEntities {
            tenant_hash: 11,
            node_hash: 42,
            entity_hashes: vec![7001, 8888],
            limit: Some(10),
        },
    });
    assert!(matches!(
        entity_query.response,
        CommandResponse::ContextEntities { ref entities, .. }
            if entities == &vec![entity.clone()]
    ));

    let event_a = ContextEvent {
        event_id_hash: 5,
        event_time_ms: 1_000,
        ingestion_time_ms: 1_000,
        kind: 9,
        event_type: 2,
        actor_hash: 77,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.9,
        importance: 0.7,
        text: "first".to_string(),
        source_ref: "src://a".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: vec![1, 2, 3],
        vector: Vec::new(),
    };
    let mut event_b = event_a.clone();
    event_b.event_id_hash = 6;
    event_b.text = "second".to_string();

    for event in [event_a.clone(), event_b.clone()] {
        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: Box::new(event),
                first_write_only: true,
                cold_storage: false,
            },
        });
        assert!(write.status.ok);
        assert!(matches!(
            write.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:event:11:42"
        ));
    }
    let duplicate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteEvent {
            tenant_hash: 11,
            node_hash: 42,
            event: Box::new(ContextEvent {
                text: "ignored".to_string(),
                ..event_a.clone()
            }),
            first_write_only: true,
            cold_storage: false,
        },
    });
    assert!(duplicate.status.ok);

    let queried = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 999,
            end_time_ms: 1_001,
            limit: Some(10),
            max_scan: None,
            current_valid_only: true,
            as_of_ms: 0,
            kinds: vec![2],
            statuses: Vec::new(),
            min_confidence: 0.8,
            min_importance: 0.6,
        },
    });
    // Newest-first: the timeline scan is deliberately `.rev()`ed so retrieval reaches
    // recent, serving-relevant context without walking cold history first. Both events
    // share event_time_ms, so the tie breaks on event_id_hash -- 6 ("second") then
    // 5 ("first").
    assert!(matches!(
        queried.response,
        CommandResponse::ContextEvents { ref object_key, ref events }
            if object_key == "ctx:event:11:42"
                && events.iter().map(|event| event.text.as_str()).collect::<Vec<_>>()
                    == vec!["second", "first"]
    ));

    let index_ref = ContextIndexRef {
        primary_node_hash: 42,
        primary_event_time_ms: 1_000,
        event_id_hash: 5,
    };
    let index_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteIndexRef {
            tenant_hash: 11,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            event_time_ms: 1_000,
            index_ref: index_ref.clone(),
        },
    });
    assert!(matches!(
        index_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctxidx:11:actor:77:3"
    ));
    let index_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryIndex {
            tenant_hash: 11,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            start_time_ms: 999,
            end_time_ms: 1_001,
            limit: None,
        },
    });
    assert!(matches!(
        index_query.response,
        CommandResponse::ContextIndexRefs { refs, .. } if refs == vec![index_ref]
    ));

    let extracted_event = ContextEvent {
        event_id_hash: 445,
        event_time_ms: 1_781_500_000_000,
        ingestion_time_ms: 1_781_500_000_000,
        kind: 7,
        event_type: 7,
        actor_hash: 0,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.96,
        importance: 0.88,
        text: "Finance confirmed the Project 1 GPU purchase approval.".to_string(),
        source_ref: "cursor://701".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: Vec::new(),
        vector: Vec::new(),
    };
    let extracted = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: 11,
            node_hash: 42,
            event: Box::new(extracted_event.clone()),
            indexes: ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: vec![501, 502],
                status_hash: 601,
                source_hash: 701,
                event_time_bucket_ms: 1_781_500_000_000,
                disabled_indexes: Vec::new(),
            },
            first_write_only: true,
            cold_storage: false,
        },
    });
    assert!(matches!(
        extracted.response,
        CommandResponse::ContextExtractedEventWrite {
            ref event_object_key,
            written_index_count: 6,
            ref index_object_keys,
        } if event_object_key == "ctx:event:11:42" && index_object_keys.len() == 6
    ));
    for (index_name, value_hash, start_time_ms, end_time_ms) in [
        ("event_kind", 7, 1_781_499_999_999, 1_781_500_000_001),
        ("entity", 501, 1_781_499_999_999, 1_781_500_000_001),
        ("entity", 502, 1_781_499_999_999, 1_781_500_000_001),
        ("status", 601, 1_781_499_999_999, 1_781_500_000_001),
        ("source", 701, 1_781_499_999_999, 1_781_500_000_001),
        (
            "event_time_bucket",
            1_781_500_000_000,
            1_781_499_999_999,
            1_781_500_000_001,
        ),
    ] {
        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryIndex {
                tenant_hash: 11,
                index_name: index_name.to_string(),
                index_value_hash: value_hash,
                scope_hash: 3001,
                start_time_ms,
                end_time_ms,
                limit: Some(10),
            },
        });
        assert!(matches!(
            query.response,
            CommandResponse::ContextIndexRefs { refs, .. }
                if refs.len() == 1
                    && refs[0].primary_node_hash == 42
                    && refs[0].primary_event_time_ms == extracted_event.event_time_ms
                    && refs[0].event_id_hash == extracted_event.event_id_hash
        ));
    }

    let disabled_source = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteExtractedEvent {
            tenant_hash: 11,
            node_hash: 43,
            event: Box::new(ContextEvent {
                event_id_hash: 446,
                event_time_ms: 1_781_500_000_010,
                ingestion_time_ms: 1_781_500_000_010,
                kind: 8,
                event_type: 8,
                actor_hash: 0,
                status: 1,
                valid_until_ms: 0,
                confidence: 0.9,
                importance: 0.8,
                text: "A low-noise event that should not be source-indexed.".to_string(),
                source_ref: "cursor://701".to_string(),
                related_node_hashes: vec![43],
                compact_attrs: Vec::new(),
                vector: Vec::new(),
            }),
            indexes: ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: Vec::new(),
                status_hash: 602,
                source_hash: 701,
                event_time_bucket_ms: 1_781_500_000_000,
                disabled_indexes: vec![InternalContextIndex::Source],
            },
            first_write_only: false,
            cold_storage: false,
        },
    });
    assert!(matches!(
        disabled_source.response,
        CommandResponse::ContextExtractedEventWrite {
            written_index_count: 3,
            ..
        }
    ));
    let disabled_source_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryIndex {
            tenant_hash: 11,
            index_name: "source".to_string(),
            index_value_hash: 701,
            scope_hash: 3001,
            start_time_ms: 1_781_500_000_009,
            end_time_ms: 1_781_500_000_011,
            limit: Some(10),
        },
    });
    assert!(matches!(
        disabled_source_query.response,
        CommandResponse::ContextIndexRefs { refs, .. } if refs.is_empty()
    ));

    let audit = ContextPackAudit {
        query_id: "q1".to_string(),
        session_hash: 99,
        request_time_ms: 2_000,
        query_hash: 123,
        max_prompt_tokens: 4096,
        selected_tokens: 128,
        selected_refs: vec![ContextAuditRef {
            node_hash: 42,
            event_time_ms: 1_000,
            reason: "ranked".to_string(),
        }],
        blocked_refs: Vec::new(),
    };
    let audit_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWritePackAudit {
            tenant_hash: 11,
            audit: audit.clone(),
        },
    });
    assert!(matches!(
        audit_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:audit:11:99"
    ));
    let audit_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryPackAudit {
            tenant_hash: 11,
            session_hash: 99,
            start_time_ms: 1_999,
            end_time_ms: 2_001,
            limit: None,
        },
    });
    assert!(matches!(
        audit_query.response,
        CommandResponse::ContextPackAudits { audits, .. } if audits == vec![audit]
    ));

    let dirty = ContextDirtyNode {
        node_hash: 42,
        first_event_time_ms: 3_000,
        last_event_time_ms: 3_000,
        reason: 4,
        propagate_depth: 2,
        mark_count: 1,
    };
    let dirty_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextMarkSummaryDirty {
            tenant_hash: 11,
            node_hash: dirty.node_hash,
            event_time_ms: dirty.last_event_time_ms,
            reason: dirty.reason,
            propagate_depth: dirty.propagate_depth,
        },
    });
    assert!(matches!(
        dirty_write.response,
        CommandResponse::ContextObjectKey { ref object_key }
            if object_key == "ctx:dirty:11:42"
    ));
    let dirty_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaryDirty {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 2_999,
            end_time_ms: 3_001,
            limit: None,
        },
    });
    assert!(matches!(
        dirty_query.response,
        CommandResponse::ContextSummaryDirtyNodes { nodes, .. } if nodes == vec![dirty]
    ));

    assert!(
        engine
            .bucket_storage_summaries(1)
            .iter()
            .map(|summary| summary.block_ref_count)
            .sum::<u64>()
            >= 5
    );
    let recovery = engine.storage_recovery_report(1);
    assert!(
        recovery.total_block_refs >= 5,
        "context pages should be visible to recovery accounting"
    );
}

#[test]
fn context_query_events_applies_limit_after_filter_like_native() {
    // QueryEvents filters within the (bounded) scan and applies `limit` AFTER filtering.
    // Rust previously took `limit` off the raw timeline first, so matching events beyond the
    // first `limit` timeline entries were silently dropped -- e.g. many earlier
    // low-confidence events hid the high-confidence matches on the live retrieval path.
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let base = ContextEvent {
        event_id_hash: 0,
        event_time_ms: 0,
        ingestion_time_ms: 0,
        kind: 9,
        event_type: 2,
        actor_hash: 77,
        status: 1,
        valid_until_ms: 0,
        confidence: 0.1,
        importance: 0.7,
        text: String::new(),
        source_ref: "s".to_string(),
        related_node_hashes: vec![42],
        compact_attrs: vec![],
        vector: Vec::new(),
    };
    // 100 low-confidence events at earlier timestamps, then 5 high-confidence ones later.
    for i in 0..100u64 {
        let mut event = base.clone();
        event.event_id_hash = i + 1;
        event.event_time_ms = 1_000 + i;
        event.ingestion_time_ms = 1_000 + i;
        event.confidence = 0.1;
        event.text = format!("low{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: Box::new(event),
                first_write_only: false,
                cold_storage: false,
            },
        });
    }
    for i in 0..5u64 {
        let mut event = base.clone();
        event.event_id_hash = 1_000 + i;
        event.event_time_ms = 1_200 + i;
        event.ingestion_time_ms = 1_200 + i;
        event.confidence = 0.9;
        event.text = format!("high{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: Box::new(event),
                first_write_only: false,
                cold_storage: false,
            },
        });
    }
    let queried = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: 11,
            node_hash: 42,
            start_time_ms: 0,
            end_time_ms: 100_000,
            limit: Some(100),
            max_scan: None,
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.6,
            min_importance: 0.0,
        },
    });
    let CommandResponse::ContextEvents { events, .. } = queried.response else {
        panic!("expected ContextEvents");
    };
    assert_eq!(
        events.len(),
        5,
        "the 5 high-confidence events must survive limit-after-filter, not be hidden by the \
         100 earlier low-confidence events"
    );
}

// shared-corpus: context_tree_embedding_summary_compression
#[test]
fn context_tree_embedding_summary_and_compression_match_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 1001;
    const ROOT: u64 = 10;
    const GPU: u64 = 20;
    const COST: u64 = 30;
    const EVENT_TIME: u64 = 1_781_500_000_000;

    for node in [
        ContextNode {
            node_hash: ROOT,
            parent_hash: 0,
            kind: 1,
            canonical_name: "company_a".to_string(),
            l0: "Company A context root.".to_string(),
            status: 0,
            last_event_time_ms: 0,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        },
        ContextNode {
            node_hash: GPU,
            parent_hash: ROOT,
            kind: 2,
            canonical_name: "gpu_purchase".to_string(),
            l0: "GPU purchase leaf node.".to_string(),
            status: 0,
            last_event_time_ms: 0,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(node),
            },
        });
        assert!(response.status.ok);
    }

    let child_gpu = ContextChildRef {
        parent_hash: ROOT,
        child_hash: GPU,
        updated_at_ms: EVENT_TIME,
    };
    for (child_ref, created) in [
        (child_gpu.clone(), true),
        (
            ContextChildRef {
                parent_hash: ROOT,
                child_hash: COST,
                updated_at_ms: EVENT_TIME,
            },
            true,
        ),
        (child_gpu.clone(), false),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertChildRef {
                tenant_hash: TENANT,
                child_ref,
            },
        });
        assert!(matches!(
            response.response,
            CommandResponse::ContextChildRefs {
                ref object_key,
                created: Some(actual_created),
                ..
            } if object_key == "ctx:child:1001:10"
                && actual_created == created
        ));
    }
    let children = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryChildren {
            tenant_hash: TENANT,
            parent_hash: ROOT,
            limit: Some(10),
        },
    });
    assert!(matches!(
        children.response,
        CommandResponse::ContextChildRefs { refs, .. }
            if refs.len() == 2 && refs[0].child_hash == GPU
    ));

    // Store these the way PRODUCTION writers do: on the node itself, addressed by the node.
    for (node_hash, first, second) in [(GPU, 1.0, 0.0), (COST, 0.0, 1.0)] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash,
                model_hash: 1,
                vector: vec![first, second],
                updated_at_ms: EVENT_TIME,
            },
        });
        assert!(response.status.ok);
    }
    let traversal = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextTraverseTree {
            tenant_hash: TENANT,
            start_node_hash: ROOT,
            query_vector: vec![1.0, 0.0],
            max_depth: Some(2),
            top_k_per_depth: Some(1),
            max_children_scored_per_parent: Some(10),
            max_candidate_nodes: Some(4),
            leaf_only: true,
        },
    });
    assert!(matches!(
        traversal.response,
        CommandResponse::ContextTraversedNodes { ref nodes, .. }
            if nodes.len() == 1 && nodes[0].node_hash == GPU && nodes[0].score > 0.99
    ));

    for (text, valid_from_ms) in [
        ("L0 GPU purchase summary.", EVENT_TIME),
        ("Latest overall GPU purchase summary.", EVENT_TIME + 5),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: ContextSummary {
                    node_hash: GPU,
                    level: 1,
                    text: text.to_string(),
                    valid_from_ms,
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                },
            },
        });
        assert!(response.status.ok);
    }
    let summaries = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaries {
            tenant_hash: TENANT,
            node_hash: GPU,
            level: 1,
            as_of_ms: EVENT_TIME + 1,
            limit: Some(10),
        },
    });
    assert!(matches!(
        summaries.response,
        CommandResponse::ContextSummaries { ref summaries, .. }
            if summaries.len() == 1 && summaries[0].text == "L0 GPU purchase summary."
    ));

    let compression = ContextCompressionEvent {
        compression_id_hash: 5001,
        node_hash: GPU,
        source_start_ms: EVENT_TIME - 1000,
        source_end_ms: EVENT_TIME,
        compressed_time_ms: EVENT_TIME,
        summary: "Older GPU purchase timeline compressed into one summary.".to_string(),
    };
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextWriteCompressionEvent {
            tenant_hash: TENANT,
            event: compression.clone(),
        },
    });
    assert!(response.status.ok);
    let compression_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryCompressionEvents {
            tenant_hash: TENANT,
            node_hashes: vec![GPU],
            start_time_ms: EVENT_TIME - 2000,
            end_time_ms: EVENT_TIME + 1,
            limit: Some(10),
        },
    });
    assert!(matches!(
        compression_query.response,
        CommandResponse::ContextCompressionEvents { ref events, .. }
            if events == &vec![compression.clone()]
    ));

    let node_context = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryNodeContext {
            tenant_hash: TENANT,
            node_hash: GPU,
            summary_level: Some(1),
            as_of_ms: EVENT_TIME + 10,
            cold_start_time_ms: EVENT_TIME - 2000,
            cold_end_time_ms: EVENT_TIME + 1,
            compression_limit: Some(10),
        },
    });
    assert!(matches!(
        node_context.response,
        CommandResponse::ContextNodeContext {
            node_exists: true,
            overall_summary_exists: true,
            overall_summary: Some(ref summary),
            ref cold_window_summaries,
            ..
        } if summary.text == "Latest overall GPU purchase summary."
            && cold_window_summaries.len() == 1
            && cold_window_summaries[0].summary == compression.summary
    ));
}

// shared-corpus: context_temporal_compression_replayable_summary
#[test]
fn context_temporal_compression_builds_replayable_summary_without_deleting_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3003;
    const NODE: u64 = 9100;
    const START: u64 = 1_781_400_000_000;
    const COMPRESSED_AT: u64 = 1_781_500_000_000;

    for (offset_ms, event_id, text) in [
        (0, 7001, "Week-old approval was created."),
        (10, 7002, "Week-old approval was reviewed by finance."),
        (20, 7003, "Week-old approval was confirmed by infra."),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: Box::new(ContextEvent {
                    event_id_hash: event_id,
                    event_time_ms: START + offset_ms,
                    ingestion_time_ms: START + offset_ms,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.96,
                    importance: 0.82,
                    text: text.to_string(),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                first_write_only: false,
                cold_storage: false,
            },
        });
        assert!(response.status.ok);
    }

    let compressed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextCompressEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            source_start_ms: START,
            source_end_ms: START + 20,
            compressed_time_ms: COMPRESSED_AT,
            max_source_events: Some(2),
            min_confidence: 0.9,
            min_importance: 0.8,
        },
    });
    assert!(matches!(
        compressed.response,
        CommandResponse::ContextCompressionEvents {
            ref object_key,
            ref events,
            source_event_count: Some(2),
            truncated_source_events: Some(true),
        } if object_key == "ctx:compress:3003:9100"
            && events.len() == 1
            && events[0].summary.contains("Temporal compression window")
            && events[0].summary.contains("Week-old approval was created")
    ));

    let raw_events = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: START,
            end_time_ms: START + 20,
            limit: Some(10),
            max_scan: None,
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    assert!(matches!(
        raw_events.response,
        CommandResponse::ContextEvents { ref events, .. } if events.len() == 3
    ));
}

// shared-corpus: context_temporal_compression_cold_scan context_raw_backfill_cold_ingest
#[test]
fn context_temporal_compression_and_raw_backfill_use_cold_storage_without_cache_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3010;
    const NODE: u64 = 9200;
    const START: u64 = 1_782_400_000_000;

    let cache_puts_before = engine.cache().stats().puts;
    let block_writes_before = engine.block_store().stats().writes;
    for idx in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: Box::new(ContextEvent {
                    event_id_hash: 8000 + idx,
                    event_time_ms: START + idx * 10,
                    ingestion_time_ms: START + idx * 10,
                    kind: 7,
                    event_type: 7,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.95,
                    importance: 0.85,
                    text: format!("Cold backfill event {idx}"),
                    source_ref: "backfill://raw-query".to_string(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    vector: Vec::new(),
                }),
                first_write_only: false,
                cold_storage: true,
            },
        });
        assert!(response.status.ok);
    }
    assert!(engine.block_store().stats().writes > block_writes_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before);

    let block_reads_before = engine.block_store().stats().reads;
    let cache_puts_before_compress = engine.cache().stats().puts;
    let compressed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextCompressEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            source_start_ms: START,
            source_end_ms: START + 20,
            compressed_time_ms: START + 1_000,
            max_source_events: Some(3),
            min_confidence: 0.9,
            min_importance: 0.8,
        },
    });
    assert!(compressed.status.ok);
    assert!(matches!(
        compressed.response,
        CommandResponse::ContextCompressionEvents {
            ref events,
            source_event_count: Some(3),
            ..
        } if events.len() == 1
    ));
    assert!(engine.block_store().stats().reads > block_reads_before);
    assert_eq!(engine.cache().stats().puts, cache_puts_before_compress);
}

#[test]
fn live_block_slab_ids_scan_all_index_backed_data_models() {
    let mut shard = ShardState::default();
    shard.set_string_address(
        "string".into(), &ElementEntry::from_parts(7, 0, 1, None, None),
    );
    shard.hashes.insert_element_for_test(
        "hash",
        std::sync::Arc::from("field"),
        ElementEntry::from_parts(8, 0, 1, None, None),
    );
    shard.sets.insert_element_for_test(
        "set",
        b"member".to_vec(),
        ElementEntry::from_parts(9, 0, 1, None, None),
    );
    shard
        .features_mut()
        .entry("feature".into())
        .or_default()
        .set(
            10,
            ElementEntry::from_parts(10, 0, 1, None, None),
        );
    shard
        .features_mut()
        .entry("sequence".into())
        .or_default()
        .set(
            11,
            ElementEntry::from_parts(11, 0, 1, None, None),
        );
    shard
        .control_state_mut()
        .entry(std::sync::Arc::from("control_state"))
        .or_default()
        .insert(14, 1);

    let ids = collect_live_block_slab_ids(&shard)
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![7, 8, 9, 10, 11]);
}

#[test]
fn block_compaction_rewrites_live_addresses_and_allows_old_slab_gc() {
    let block_dir = unique_temp_path("compact-blocks");
    let index_dir = unique_temp_path("compact-index");
    let block_store = BlockStore::new(&block_dir);
    let engine = TemporalEngine::with_cache_block_store_and_index_dir(
        MultiLayerCache::default(),
        block_store.clone(),
        &index_dir,
    );
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v1".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v2".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );
    assert_eq!(engine.live_block_slab_ids(1), vec![0]);

    let report = engine.compact_shard_blocks(1).unwrap();
    assert_eq!(report.previous_block_slab_id, 0);
    assert_eq!(report.compacted_block_slab_id, 1);
    assert_eq!(report.rewritten_block_refs, 2);
    assert_eq!(report.stale_block_slab_ids, vec![0]);
    assert_eq!(report.before.live_block_slab_count, 1);
    assert_eq!(report.before.total_block_count, 3);
    assert_eq!(report.before.live_block_refs, 2);
    assert_eq!(report.before.stale_block_estimate, 1);
    assert_eq!(report.before.live_ref_density_basis_points, 6_666);
    assert_eq!(report.after.live_block_slab_count, 1);
    assert_eq!(report.after.total_block_count, 2);
    assert_eq!(report.after.live_block_refs, 2);
    assert_eq!(report.after.stale_block_estimate, 0);
    assert_eq!(report.after.live_ref_density_basis_points, 10_000);
    assert_eq!(engine.live_block_slab_ids(1), vec![1]);
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let string_address = shard.string_address("k").expect("string address");
        let hash_address = shard
            .hashes
            .get("h")
            .and_then(|fields| fields.get("f"))
            .expect("hash address");
        // The address used to carry the id the engine computes. It carries none now, and the
        // property moved to the ENTRY, which derives it -- `shard_carried_identity` is where that
        // is asserted against the ids the write path stored.
        // WHERE THE BLOCK IS FILED, not what the address claims -- the address carries no bucket.
        // The bucket map's key is the container's answer, and it has to be the key's own bucket.
        // WHICH BUCKET HOLDS A PAGE OF THIS OBJECT, asked of the structures beside the pages.
        let placed = super::blocks_with_identity(&shard.bucket_index);
        let buckets_holding = |object: &str| -> Vec<u32> {
            let mut found: Vec<u32> = placed
                .iter()
                .filter(|(_, _, named, _)| named.as_ref() == object)
                .map(|(routing_bucket, ..)| *routing_bucket)
                .collect();
            found.dedup();
            found
        };
        assert_eq!(buckets_holding("k"), vec![block_routing_bucket("k", 0, u32::MAX)]);
        // As above: the id is derived from the entry's terms, not read off the address.
        assert_eq!(buckets_holding("h"), vec![block_routing_bucket("h", 0, u32::MAX)]);
    }

    let gc = block_store
        .gc_slabs_before_with_live_refs(1, engine.live_block_slab_ids(1))
        .unwrap();
    assert_eq!(gc.removed_block_slab_ids, vec![0]);
    assert_eq!(block_store.slab_ids().unwrap(), vec![1]);

    let restarted = TemporalEngine::with_cache_block_store_and_index_dir(
        MultiLayerCache::default(),
        block_store,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"hv".to_vec())
        }
    );
}

#[test]
// shared-corpus: storage_dump_load_recovery storage_cache_refill storage_tombstone_compaction;
fn block_compaction_reports_model_layouts_delete_markers_object_blocks_and_density() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    for command in [
        Command::StringSet {
            key: "compact-string".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "compact-string".to_string(),
            value: b"new".to_vec(),
        },
        Command::HashSet {
            key: "compact-hash".to_string(),
            field: "field".to_string(),
            value: b"hash-value".to_vec(),
        },
        Command::SetAdd {
            key: "compact-set".to_string(),
            member: b"member".to_vec(),
        },
        Command::FeatureAppend {
            key: "compact-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::SequenceAdd {
            key: "compact-sequence".to_string(),
            rows: vec![
                SequenceFeatureRow {
                    timestamp_ms: 22,
                    gid: 1,
                    action_type: 2,
                    duration: 3,
                    author_id: 4,
                },
                SequenceFeatureRow {
                    timestamp_ms: 23,
                    gid: 2,
                    action_type: 3,
                    duration: 4,
                    author_id: 5,
                },
            ],
        },
        Command::ControlStateSet {
            family: ControlStateFamily::Distinct,
            key: "compact-control_state".to_string(),
            timestamp_ms: 45,
            amount: 3,
        },
        Command::ContextWriteEvent {
            tenant_hash: 7,
            node_hash: 9,
            event: Box::new(ContextEvent {
                event_id_hash: 50,
                event_time_ms: 50,
                ingestion_time_ms: 50,
                kind: 0,
                event_type: 1,
                actor_hash: 0,
                status: 0,
                valid_until_ms: 0,
                confidence: 1.0,
                importance: 0.5,
                text: "context event".to_string(),
                source_ref: String::new(),
                related_node_hashes: Vec::new(),
                compact_attrs: Vec::new(),
                vector: Vec::new(),
            }),
            first_write_only: false,
            cold_storage: false,
        },
        Command::ContextUpsertSummary {
            tenant_hash: 7,
            summary: ContextSummary {
                node_hash: 9,
                level: 1,
                text: "compact summary".to_string(),
                valid_from_ms: 52,
                vector: Vec::new(),
                embedding_model_hash: 0,
            },
        },
        Command::CommonDelete {
            key: "compact-set".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    let async_response = engine.execute_replicated(
        ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "compact-hot-object".to_string(),
                value: b"hot".to_vec(),
            },
        }
        .with_async_storage(),
    );
    assert!(async_response.status.ok, "{async_response:?}");

    let before = engine.storage_recovery_report(1);
    assert!(before.object_lifecycle.delete_marked_object_ids >= 1);
    assert!(before.object_lifecycle.stale_object_ids >= 1);

    let report = engine.compact_shard_blocks(1).unwrap();
    assert!(report.model_layout_compaction_ready, "{report:?}");
    assert!(report.model_layout_compaction_blockers.is_empty());
    assert!(report
        .model_layout_compaction_evidence
        .iter()
        .any(|item| item.contains("rewrites live refs by model layout")));
    assert!(report
        .model_layout_compaction_evidence
        .iter()
        .any(|item| item.contains("tombstone object ids are preserved")));
    assert_eq!(report.rewritten_object_blocks, report.rewritten_block_refs);
    assert!(report.rewritten_object_blocks >= 5);
    assert!(report.reclaimable_stale_block_slab_count >= 1);
    assert_eq!(
        report.reclaimable_stale_block_slab_count,
        report.stale_block_slab_ids.len()
    );
    assert!(report.model_policy_family_count >= 6);
    assert!(report.delete_marker_policy_model_count >= 1);
    assert!(report.stale_density_policy_model_count >= 1);
    assert!(report.layout_aware_policy_model_count >= 5);
    assert!(report.before.stale_block_estimate >= 1);
    assert_eq!(report.after.stale_block_estimate, 0);
    assert!(
        report.before.live_ref_density_basis_points < report.after.live_ref_density_basis_points
    );
    assert_eq!(
        report.delete_marked_object_ids_before,
        report.delete_marked_object_ids_after
    );
    assert!(report.delete_marked_object_ids_after >= 1);

    let layout = |kind: &str| {
        report
            .model_layouts
            .iter()
            .find(|layout| layout.kind == kind)
            .unwrap_or_else(|| panic!("missing layout for {kind}: {:?}", report.model_layouts))
    };
    assert_eq!(layout("string").unique_block_refs, 2);
    assert_eq!(layout("hash").unique_block_refs, 1);
    // Sequence folds into the feature family (shared timestamped-KV storage), so the
    // feature layout now aggregates both the feature series and the formerly separate
    // sequence series; there is no distinct "sequence" layout.
    assert_eq!(layout("feature").index_refs, 4);
    assert_eq!(layout("feature").unique_block_refs, 2);
    assert_eq!(layout("feature").packed_timestamped_blocks, 2);
    assert_eq!(layout("context_event").index_refs, 1);
    assert_eq!(layout("context_summary").index_refs, 1);

    let policy = |model_id: &str| {
        report
            .before
            .model_policies
            .iter()
            .find(|policy| policy.model_id == model_id)
            .unwrap_or_else(|| {
                panic!(
                    "missing model policy for {model_id}: {:?}",
                    report.before.model_policies
                )
            })
    };
    assert!(policy("string").stale_density_triggered);
    assert!(policy("hash").layout_aware_rewrite_required);
    assert!(policy("feature").layout_aware_rewrite_required);
    assert!(policy("control_state").layout_aware_rewrite_required);
    assert!(policy("context_event").layout_aware_rewrite_required);
    assert!(policy("context_summary").layout_aware_rewrite_required);
    assert!(policy("hash").object_block_packing_enabled);
    assert!(policy("feature").cold_block_rewrite_eligible_refs >= 1);

    let rewrite_policy = |model_id: &str| {
        report
            .model_rewrite_policies
            .iter()
            .find(|policy| policy.model_id == model_id)
            .unwrap_or_else(|| {
                panic!(
                    "missing rewrite policy for {model_id}: {:?}",
                    report.model_rewrite_policies
                )
            })
    };
    for model_id in [
        "string",
        "hash",
        "feature",
        "control_state",
        "context_event",
        "context_summary",
    ] {
        assert!(
            rewrite_policy(model_id).rewritten_block_refs >= 1,
            "expected rewrite evidence for {model_id}"
        );
    }

    let after = engine.storage_recovery_report(1);
    assert_eq!(after.object_lifecycle.owner_mismatch_block_refs, 0);
    assert_eq!(after.object_lifecycle.missing_owner_block_refs, 0);
    assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
    assert_eq!(
        after.object_lifecycle.live_block_refs,
        report.after.live_block_refs
    );
    assert!(after
        .object_lifecycle
        .delete_marked_object_keys
        .iter()
        .any(|key| key == "compact-set"));
    let object_runtime = engine.object_manager_runtime_report(1);
    assert!(object_runtime.object_block_count >= 1);
    assert!(
        engine
            .bucket_storage_summaries(1)
            .iter()
            .flat_map(|summary| summary.block_slab_ids.iter().copied())
            .all(|slab_id| slab_id == report.compacted_block_slab_id),
        "all index summaries should move to compacted segment: {:?}",
        engine.bucket_storage_summaries(1)
    );
}

// shared-corpus: storage_manager_background_loop;
#[test]
fn storage_manager_cycle_runs_prepare_reclaim_evict_expire_compact_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"new".to_vec(),
        },
        Command::FeatureAppend {
            key: "manager-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::StringSet {
            key: "manager-expire".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonExpire {
            key: "manager-expire".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 16,
        min_undumped_wal_records: 0,
        warm_cache: true,
        ..StorageManagerCycleRequest::default()
    });

    assert!(report.completed, "{report:?}");
    for phase in [
        "prepare",
        "reclaim_wal",
        "evict",
        "expire",
        "compact",
        "index_gc",
    ] {
        assert!(
            report
                .stages
                .iter()
                .any(|entry| entry.stage == phase && entry.enabled),
            "missing attempted phase {phase}: {report:?}"
        );
    }
    assert!(report.lifecycle_report.is_some());
    assert!(report
        .expiry_report
        .as_ref()
        .is_some_and(|expiry| expiry.expired_records_removed >= 1));
    assert!(report
        .compaction_report
        .as_ref()
        .is_some_and(|compaction| compaction.model_layout_compaction_ready));
    assert!(
        report
            .stages
            .iter()
            .find(|phase| phase.stage == "prepare")
            .unwrap()
            .dirty_bucket_count
            >= 1
    );
    assert_eq!(
        report.native_stage_order,
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
}

// shared-corpus: storage_data_structure_api_parity
#[test]
fn storage_data_structure_api_parity_report_covers_stream_block_and_manager_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for (key, value) in [("parity-a", b"one".to_vec()), ("parity-b", b"two".to_vec())] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    engine.block_store().roll_slab().unwrap();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "parity-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"feature".to_vec(),
            }],
        },
    });
    assert!(response.status.ok, "{response:?}");

    let report = engine.storage_data_structure_api_parity_report(1);
    assert!(report.ready, "{report:?}");
    assert!(report.bucket_object_block_authority_ready);
    assert!(report.bucket_store_layout_api_ready);
    assert!(report.object_manager_runtime_api_ready);
    assert!(report.block_address_api_ready);
    assert!(report.block_store_slab_api_ready);
    assert!(report.stream_backed_slab_api_ready);
    assert!(report.legacy_block_slab_aliases_ready);
    assert!(report.storage_manager_phase_api_ready);
    assert!(report.storage_manager_pressure_api_ready);
    assert!(report.storage_manager_merged_dump_load_api_ready);
    assert!(report.bucket_count >= 1);
    assert!(report.page_index_count >= 1);
    assert!(report.block_index_count >= 1);
    assert!(report.stream_slab_count >= 2);
    assert!(report.stream_record_count >= 3);
    assert_eq!(
        report.storage_manager_stage_order,
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
}

// shared-corpus: storage_manager_background_loop;
#[test]
fn storage_manager_loop_runs_prepare_reclaim_evict_expire_compact_and_index_gc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"old".to_vec(),
        },
        Command::StringSet {
            key: "manager-live".to_string(),
            value: b"new".to_vec(),
        },
        Command::FeatureAppend {
            key: "manager-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ],
        },
        Command::StringSet {
            key: "manager-expire".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonExpire {
            key: "manager-expire".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.run_storage_manager_loop(StorageManagerLoopRequest {
        shard_id: 1,
        apply: true,
        expire_records: true,
        compact_blocks: true,
        lifecycle: StorageLifecycleRequest {
            shard_id: 1,
            max_dump_buckets_per_round: 16,
            min_undumped_wal_records: 0,
            purge_delayed_destroy: true,
            prune_bucket_dump_manifests: true,
            roll_forward_bucket_dump_installs: true,
            invalidate_cache: true,
            warm_cache: true,
            ..StorageLifecycleRequest::default()
        },
    });

    assert!(report.loop_ready, "{report:?}");
    for phase in [
        "prepare", "reclaim", "evict", "expire", "compact", "index_gc",
    ] {
        assert!(
            report
                .phases
                .iter()
                .any(|entry| entry.phase == phase && entry.attempted),
            "missing attempted phase {phase}: {report:?}"
        );
    }
    assert!(report.lifecycle.dump_manifest.is_some());
    assert!(report.expiry_sweep.expired_records_removed >= 1);
    assert!(report
        .compaction
        .as_ref()
        .is_some_and(|compaction| compaction.model_layout_compaction_ready));
    assert!(report
        .phases
        .iter()
        .find(|phase| phase.phase == "prepare")
        .unwrap()
        .evidence
        .iter()
        .any(|item| item.contains("dirty slots")));
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("prepare/reclaim/evict/expire/compact/index-GC")));
}

/// RETIRED: THE MISMATCH THIS PLANTED CANNOT BE PLANTED ANY MORE.
///
/// It set a wrong object id on a page's address and asserted that
/// `storage_recovery_report` reported one `owner_mismatch_block_ref` and refused compaction. Both
/// halves rested on an address carrying an id that could disagree with the entry's. An address
/// carries none now, so there is no value to make wrong -- and the detection went with it, because
/// a comparison between a derived id and the same derivation cannot fail. That loss is deliberate
/// and is recorded here rather than left as a test asserting zero.
///
/// WHAT STILL FIRES in its place: `validate_bucket_ownership_index_from_entries` keeps its FILING
/// comparison, which is the container's key against the key's own hash -- two independent answers --
/// and its `!bucket_block_present` term. `storage_reporting` keeps a reuse check, restated over
/// distinct (kind, key) pairs reaching one derived id, which is a real collision rather than a
/// planted one.
#[test]
#[ignore = "retired: an address carries no object id that could mismatch the entry's"]
fn recovery_reports_owner_mismatch_and_compaction_refuses_it() {
    println!("retired: the owner-mismatch detection had no second copy left to compare against");
}


/// RETIRED: A REUSED ID CANNOT BE PLANTED BY ASSIGNMENT ANY MORE.
///
/// It copied one page's object id onto another page's address and asserted the recovery report
/// named the reuse. The id is derived from (shard, kind, key) now, so the only way two pages share
/// one is a genuine hash collision, which this fixture cannot manufacture.
///
/// The check itself is NOT gone: `object_lifecycle_report_from_entries` still reports
/// `reused_object_ids`, restated as "one derived id reached by more than one distinct (kind, key)".
/// That fires on a real collision and could not fire on a planted one, which is the right way round.
#[test]
#[ignore = "retired: an id derived from the key cannot be reassigned to another key"]
fn recovery_reports_reused_object_id_conflicts() {
    println!("retired: reuse is now a hash collision, not an assignable field");
}


#[test]
fn crash_recovery_report_covers_wal_index_block_and_slab_manifest() {
    let cache_dir = unique_temp_path("recovery-cache");
    let block_dir = unique_temp_path("recovery-blocks");
    let index_dir = unique_temp_path("recovery-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v1".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_slab().unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );

    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    recovered.load_shard(1);
    let report = recovered.storage_recovery_report(1);

    // Base-only single-barrier recovery re-derives block layout from WAL replay (the out-of-band
    // roll_slab() is not a WAL command), so the detailed physical report -- slab ids, slab
    // descriptors, per-slab density -- differs from the delta-fold path. It still recovers every
    // acked write (asserted by the reads below) with all live blocks readable and integral.
    if crate::engine::wal_single_barrier() {
        assert!(report.all_live_blocks_readable);
        assert!(report.slab_integrity.integrity_ok);
        assert_eq!(report.slab_integrity.unreadable_block_ref_count, 0);
    } else {
    assert!(report.index_bytes > 0);
    assert!(report.index_write_atomic);
    assert_eq!(report.wal_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert_eq!(report.active_block_slab_ids, vec![0, 1]);
    assert_eq!(report.live_block_slab_ids, vec![0, 1]);
    assert_eq!(report.total_block_refs, 2);
    assert_eq!(report.readable_block_refs, 2);
    assert!(report.all_live_blocks_readable);
    assert!(report.slab_integrity.integrity_ok);
    assert!(!report.slab_integrity.reclaim_required);
    assert_eq!(report.slab_integrity.indexed_block_slab_count, 2);
    assert_eq!(report.slab_integrity.discovered_block_slab_count, 2);
    assert_eq!(report.slab_integrity.live_block_slab_count, 2);
    assert_eq!(report.slab_integrity.unreadable_block_ref_count, 0);
    assert_eq!(report.slab_descriptors.len(), 2);
    assert_eq!(
        report.slab_descriptors[0].state,
        BlockStoreSlabState::Sealed
    );
    assert_eq!(
        report.slab_descriptors[1].state,
        BlockStoreSlabState::Active
    );
    assert_eq!(report.slab_summary.sealed_slabs, 1);
    assert_eq!(report.slab_summary.active_slabs, 1);
    assert_eq!(report.slab_summary.delayed_destroy_slabs, 0);
    assert_eq!(
        report.slab_summary.sealed_physical_bytes,
        report.slab_descriptors[0].physical_bytes
    );
    assert_eq!(
        report.slab_summary.active_physical_bytes,
        report.slab_descriptors[1].physical_bytes
    );
    assert_eq!(
        report.slab_summary.live_physical_bytes,
        report.slab_descriptors[0].physical_bytes + report.slab_descriptors[1].physical_bytes
    );
    assert_eq!(report.block_slab_live_reports.len(), 2);
    assert_eq!(report.block_slab_live_reports[0].block_slab_id, 0);
    assert_eq!(report.block_slab_live_reports[0].block_count, 1);
    assert_eq!(report.block_slab_live_reports[0].live_block_refs, 1);
    assert_eq!(
        report.block_slab_live_reports[0].readable_live_block_refs,
        1
    );
    assert_eq!(
        report.block_slab_live_reports[0].unreadable_live_block_refs,
        0
    );
    assert_eq!(report.block_slab_live_reports[0].stale_block_estimate, 0);
    assert_eq!(
        report.block_slab_live_reports[0].live_ref_density_basis_points,
        10_000
    );
    assert_eq!(report.block_slab_live_reports[0].live_object_count, 1);
    assert_eq!(
        report.block_slab_live_reports[0].live_routing_bucket_count,
        1
    );
    assert_eq!(report.block_slab_live_reports[0].live_logical_bytes, 2);
    assert!(report.block_slab_live_reports[0].live_physical_bytes > 0);
    }

    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"hv".to_vec())
        }
    );
}

#[test]
fn crash_recovery_report_marks_stale_slab_density_after_overwrite() {
    let cache_dir = unique_temp_path("recovery-density-cache");
    let block_dir = unique_temp_path("recovery-density-blocks");
    let index_dir = unique_temp_path("recovery-density-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "hot".to_string(),
                    value: b"old".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "hot".to_string(),
                    value: b"new".to_vec(),
                },
            })
            .status
            .ok
    );

    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    recovered.load_shard(1);
    // The overwrite keeps exactly one live object ("hot"="new", 3 bytes) under any recovery mode.
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "hot".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"new".to_vec())
        }
    );
    let report = recovered.storage_recovery_report(1);
    let slab = report
        .block_slab_live_reports
        .iter()
        .find(|slab| slab.block_slab_id == 0)
        .expect("segment 0 live-density report");

    // The single live object is exactly the same regardless of recovery mode.
    assert_eq!(slab.live_block_refs, 1);
    assert_eq!(slab.readable_live_block_refs, 1);
    assert_eq!(slab.live_logical_bytes, 3);
    assert_eq!(slab.live_object_count, 1);
    assert_eq!(slab.live_routing_bucket_count, 1);
    if !crate::engine::wal_single_barrier() {
        // Default path: the delta fold reconstructs exactly the two physical blocks (stale old +
        // live new) -> 50% live density. Base-only single-barrier recovery replays the two writes
        // on top of the blocks that happened to survive a clean in-process reload, so the slab
        // physically holds extra stale blocks (same single live object, reclaimed by GC). On a real
        // power cut the un-synced blocks are gone and replay rebuilds them cleanly. Physical density
        // is therefore not asserted under the flag.
        assert_eq!(slab.block_count, 2);
        assert_eq!(slab.stale_block_estimate, 1);
        assert_eq!(slab.live_ref_density_basis_points, 5_000);
    }
}

#[test]
// shared-corpus: storage_cache_refill storage_matrixraft_cache_refill_pressure;
fn cold_index_block_address_reads_from_disk_cache_or_block_store_and_refills_memory() {
    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("cache");
    let block_dir = root.path().join("blocks");
    let index_dir = root.path().join("index");
    let engine = TemporalEngine::with_local_dirs(128, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "cold-key".to_string(),
                    value: b"cold-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let block_key = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let address = shard.string_address("cold-key").expect("indexed page address");
        assert_ne!(address.block_slab_id(), HOT_BLOCK_SLAB_ID);
        // THE SAME DERIVATION THE READ PATH USES: the page cache key's slot is the KEY's bucket
        // over the range the shard is stamped with, not a field on the address.
        let (start, end) = shard.routing_range();
        CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(block_routing_bucket("cold-key", start, end)),
        )
    };

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, 1);
    // ONE KEY PER POINT READ, NOT TWO. These were `>= 2` because a string get looked up and filled
    // both the command response under `CacheKey::string` and the block frame under
    // `CacheKey::page_with_slot`. The response copy is no longer cached -- it was memory-only with
    // a single reader, while the frame is the tiered, persistent copy this test goes on to exercise
    // -- so one miss and one put is the floor now.
    //
    // Every assertion below about that tier is unchanged and still passes, which is the point:
    // `get_memory(&block_key)` holds the frame, and the read after `clear_memory_for_test` is
    // served from disk with no block-store read.
    assert!(engine.cache().stats().misses >= 1);
    assert!(engine.cache().stats().puts >= 1);
    assert_eq!(
        engine.cache().get_memory(&block_key),
        Some(b"cold-value".to_vec())
    );

    // THIS INVALIDATION IS NOW A NO-OP and is kept deliberately: nothing caches
    // `CacheKey::string` for a string any more, so there is nothing under it to invalidate. The
    // step that does the work here is `clear_memory_for_test` below. Removing it belongs to a sweep
    // of every site that invalidates that key, not to this change, where dropping one by hand risks
    // missing another.
    let _ = engine.cache().invalidate(&CacheKey::string(1, "cold-key"));
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&block_key), None);
    let reads_before_disk_cache = engine.block_store().stats().reads;
    let disk_cache_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        disk_cache_read.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, reads_before_disk_cache);
    assert!(engine.cache().stats().disk_hits >= 1);
    assert_eq!(
        engine.cache().get_memory(&block_key),
        Some(b"cold-value".to_vec())
    );

    let cold_restart = TemporalEngine::with_local_dirs(
        128,
        root.path().join("fresh-cache"),
        &block_dir,
        &index_dir,
    );
    cold_restart.load_shard(1);
    let restart_read = cold_restart.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "cold-key".to_string(),
        },
    });
    assert_eq!(
        restart_read.response,
        CommandResponse::Bytes {
            value: Some(b"cold-value".to_vec())
        }
    );
    assert_eq!(cold_restart.block_store().stats().reads, 1);
    // One key per point read here too -- see the note on the first read above.
    assert!(cold_restart.cache().stats().misses >= 1);
    assert!(cold_restart.cache().stats().puts >= 1);
}

#[test]
fn crash_recovery_rebuilds_missing_slab_manifest_from_block_stream() {
    let cache_dir = unique_temp_path("recovery-rebuild-cache");
    let block_dir = unique_temp_path("recovery-rebuild-blocks");
    let index_dir = unique_temp_path("recovery-rebuild-index");
    let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "before".to_string(),
                    value: b"before".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_slab().unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"after".to_vec(),
                },
            })
            .status
            .ok
    );

    fs::remove_file(block_dir.join("block_extent_manifest.json")).unwrap();
    let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &block_dir, &index_dir);
    recovered.load_shard(1);
    let report = recovered.storage_recovery_report(1);

    assert_eq!(report.wal_records, 2);
    assert!(report.all_live_blocks_readable);
    assert!(report.slab_summary.live_physical_bytes > 0);
    // The slab manifest was rebuilt (from the block stream on the default path; from WAL-replayed
    // blocks under the single barrier). Recovery of both acked writes is asserted by the reads below.
    assert!(block_dir.join("block_extent_manifest.json").exists());
    if !crate::engine::wal_single_barrier() {
        // Default path: the delta fold reconstructs the exact on-disk block layout at the original
        // addresses, so the sealed(slab 0)+active(slab 1) split from the out-of-band roll_slab()
        // survives. Base-only single-barrier recovery re-derives layout by replaying the WAL (the
        // roll_slab() is not a WAL command, so both writes replay into the active slab) -- a
        // different but valid physical layout that preserves the same logical state.
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_block_slab_ids, vec![0, 1]);
        assert_eq!(report.live_block_slab_ids, vec![0, 1]);
        assert_eq!(report.total_block_refs, 2);
        assert_eq!(report.slab_descriptors.len(), 2);
        assert_eq!(
            report.slab_descriptors[0].state,
            BlockStoreSlabState::Sealed
        );
        assert_eq!(
            report.slab_descriptors[1].state,
            BlockStoreSlabState::Active
        );
        assert_eq!(report.slab_summary.sealed_slabs, 1);
        assert_eq!(report.slab_summary.active_slabs, 1);
    }
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "before".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"before".to_vec())
        }
    );
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "after".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"after".to_vec())
        }
    );
}

#[test]
fn durable_writes_stamp_stable_object_ids_on_block_addresses() {
    let engine = TemporalEngine::default();
    assert!(
        engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                table_name: "table".to_string(),
                shard_uri: "local://1".to_string(),
                start_routing_bucket: 10,
                end_routing_bucket: 20,
                readonly: false,
                load_version: 1,
                local_node_id: None,
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "h".to_string(),
                    field: "f".to_string(),
                    value: b"hv".to_vec(),
                },
            })
            .status
            .ok
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");
    let string_address = shard.string_address("k").expect("string address");
    let hash_address = shard
        .hashes
        .get("h")
        .and_then(|fields| fields.get("f"))
        .expect("hash address");

    // See the note in `block_compaction_rewrites_live_addresses_and_allows_old_slab_gc`: a durable
    // write no longer stamps an id onto the address, so what this asserted is now a property of the
    // entry. The FILING assertions below are the part of this test that still reads the address.
    // The FILING, on a shard loaded 10..20 -- see the note at the string arm above.
    let placed = super::blocks_with_identity(&shard.bucket_index);
    let buckets_holding = |object: &str| -> Vec<u32> {
        let mut found: Vec<u32> = placed
            .iter()
            .filter(|(_, _, named, _)| named.as_ref() == object)
            .map(|(routing_bucket, ..)| *routing_bucket)
            .collect();
        found.dedup();
        found
    };
    assert_eq!(buckets_holding("k"), vec![block_routing_bucket("k", 10, 20)]);
    assert_eq!(
        string_address.slab_id(),
        Some(string_address.block_slab_id())
    );
    // As above.
    assert_eq!(buckets_holding("h"), vec![block_routing_bucket("h", 10, 20)]);
    assert_eq!(hash_address.slab_id(), Some(hash_address.block_slab_id()));
    // TWO KEYS STILL GET TWO IDS, which is the claim this line was making; it is now stated over
    // the derivation rather than over two fields that no longer exist.
    assert_ne!(
        stable_block_object_id(1, "string", "k"),
        stable_block_object_id(1, "hash", "h")
    );
}

#[test]
fn string_setex_sets_value_and_ttl() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                    ttl_ms: 60_000,
                },
            })
            .status
            .ok
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    let ttl = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonTtl {
            key: "k".to_string(),
        },
    });
    let CommandResponse::Integer { value } = ttl.response else {
        panic!("expected ttl integer response");
    };
    assert!(value > 0);
}

/// A sink that just remembers what it was told, so a test can ask whether a write reached it.
#[derive(Debug, Default)]
struct RecordingWalSink {
    seen: std::sync::Mutex<Vec<(ShardId, Command)>>,
}

impl crate::data_node::SharedWalSink for RecordingWalSink {
    fn record_write(&self, shard_id: ShardId, command: &Command) {
        self.seen
            .lock()
            .expect("recording sink lock poisoned")
            .push((shard_id, command.clone()));
    }
}

/// An expiry deletion has to reach the mirror, not only this node's log.
///
/// Expiry appends its tombstone straight to the WAL, outside the request path, so nothing at
/// the data-node layer ever sees it. In shared mode that meant the deletion reached the local
/// log and no other: a successor replaying the shared log never observed it, reapplied the
/// earlier write, and the key came back -- which is the very failure the tombstone was added
/// to prevent, reappearing one level up.
#[test]
fn an_expiry_deletion_reaches_the_maintenance_mirror() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let sink = std::sync::Arc::new(RecordingWalSink::default());
    engine.set_maintenance_wal_mirror(sink.clone());

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "expire-me".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);

    let seen = sink
        .seen
        .lock()
        .expect("recording sink lock poisoned")
        .clone();
    assert_eq!(
        seen,
        vec![(
            1u64,
            Command::CommonDelete {
                key: "expire-me".to_string()
            }
        )],
        "the expiry tombstone must reach the mirror, not just the local log"
    );
}

/// The mirror sink is looked up ONCE per round, not once per expired key.
///
/// The lookup takes a lock and clones an `Arc`, and the loop that used to do it per key runs
/// inside the shard-table WRITE guard -- the one lock that excludes every reader and writer on
/// the shard. N uncontended acquisitions in that position are invisible to wall-clock on a
/// loaded box, and exactly as invisible once they are gone, so this counts them instead.
///
/// The POSITIVE CONTROL is the per-call form itself: the same counter is driven `RECORDS` times
/// through `mirror_maintenance_write` and has to reach `RECORDS`, so a zero from the sweep below
/// cannot be a counter that stopped counting.
#[test]
fn the_maintenance_mirror_is_looked_up_once_per_round_not_once_per_key() {
    const RECORDS: usize = 200;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let sink = std::sync::Arc::new(RecordingWalSink::default());
    engine.set_maintenance_wal_mirror(sink.clone());

    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: format!("mirror-{index:06}"),
                value: b"gone".to_vec(),
                ttl_ms: 1,
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));

    crate::engine::reset_maintenance_mirror_sink_lookups();
    let report = engine
        .sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id: 1,
            load_cold_buckets: true,
            max_hot_buckets_per_round: RECORDS,
            max_cold_buckets_per_round: RECORDS,
            ..ShardExpirySweepRequest::default()
        })
        .expect("sweep");
    let sweep_lookups = crate::engine::maintenance_mirror_sink_lookups();
    let mirrored = sink
        .seen
        .lock()
        .expect("recording sink lock poisoned")
        .len();

    eprintln!(
        "[mirror lookup] {sweep_lookups} lookup(s) for {} expired record(s), {mirrored} \
command(s) mirrored",
        report.expired_records_removed,
    );

    // DENOMINATORS FIRST. A sweep that expired nothing takes no lookups at all and would satisfy
    // "one lookup" by never reaching the loop.
    assert_eq!(
        report.expired_records_removed, RECORDS,
        "the sweep removed {} of {RECORDS} records, so the mirror loop did not run over the set \
this is measuring",
        report.expired_records_removed,
    );
    assert_eq!(
        mirrored, RECORDS,
        "the mirror received {mirrored} of {RECORDS} tombstones, so the hoisted lookup is not \
delivering what the per-key one did",
    );

    // POSITIVE CONTROL: the counter can reach RECORDS, because here it does.
    crate::engine::reset_maintenance_mirror_sink_lookups();
    for index in 0..RECORDS {
        engine.mirror_maintenance_write(
            1,
            &Command::CommonDelete {
                key: format!("control-{index:06}"),
            },
        );
    }
    let control_lookups = crate::engine::maintenance_mirror_sink_lookups();
    assert_eq!(
        control_lookups, RECORDS as u64,
        "the per-call form was driven {RECORDS} times and the counter saw {control_lookups}. The \
measurement is broken, not the code under it: the assertion below would pass against a counter \
that had stopped counting entirely",
    );

    // THE ASSERTION THE HOIST MADE.
    assert_eq!(
        sweep_lookups, 1,
        "the expiry sweep took the maintenance-mirror lock {sweep_lookups} time(s) to delete \
{RECORDS} keys, while holding the shard-table write guard. Look the sink up ONCE before the loop \
and reuse it: besides the lock, a sink swapped mid-loop would split one round's tombstones \
across two mirrors and leave neither with the whole deletion",
    );
}

/// With no mirror attached the sweep behaves exactly as it did before one existed.
#[test]
fn an_expiry_sweep_without_a_mirror_is_unchanged() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "expire-me".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);
}

#[test]
fn expiry_sweep_removes_expired_records_without_lazy_read() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: "expire-me".to_string(),
                    value: b"gone".to_vec(),
                    ttl_ms: 1,
                },
            })
            .status
            .ok
    );
    std::thread::sleep(std::time::Duration::from_millis(5));

    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "expire-me".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );
    assert_eq!(
        engine
            .sweep_expired_records(1)
            .unwrap()
            .expired_records_removed,
        0
    );
}

#[test]
fn string_get_uses_memory_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024, dir.path());
    let engine = TemporalEngine::new(cache.clone());
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    let stats = cache.stats();
    assert_eq!(stats.misses, 2);
    assert!(stats.memory_hits >= 1);
    assert!(stats.puts >= 2);
}

// shared-corpus: context_dirty_coalescing_is_visible
#[test]
fn repeated_marks_coalesce_and_the_query_says_so() {
    // Dirty tracking is a coalescing map, not a log of records. The record type that used to
    // carry a mark could hold only one timestamp, so a query had to flatten first and last into
    // a single event_time_ms and drop the mark count entirely -- the caller could not tell one
    // edit from fifty, nor how much time the dirty span covered.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3300;
    const NODE: u64 = 88;

    for event_time_ms in [5_000_u64, 9_000, 7_000] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                event_time_ms,
                reason: 2,
                propagate_depth: 1,
            },
        });
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQuerySummaryDirty {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: 0,
            end_time_ms: 100_000,
            limit: None,
        },
    });
    let CommandResponse::ContextSummaryDirtyNodes { nodes, .. } = response.response else {
        panic!("expected ContextSummaryDirtyNodes");
    };

    // Three marks, one entry -- that is the coalescing, and it is why nothing is appended per
    // edit. Before, a caller could only have learned this by counting records that no longer
    // exist.
    assert_eq!(1, nodes.len(), "repeated marks must not accumulate entries");
    let node = &nodes[0];
    assert_eq!(3, node.mark_count, "the query must report how many marks arrived");
    assert_eq!(5_000, node.first_event_time_ms, "earliest event that made it dirty");
    assert_eq!(
        9_000, node.last_event_time_ms,
        "latest event, and NOT the last mark to arrive -- 7_000 came in last"
    );
    assert_eq!(2, node.reason);
    assert_eq!(1, node.propagate_depth);
}

// shared-corpus: context_dirty_window_uses_the_whole_span
#[test]
fn a_query_window_is_matched_against_the_whole_dirty_span() {
    // The span matters for more than reporting: a window that overlaps only the earliest event
    // must still find the node. Flattened to one timestamp, the early half of the span was
    // invisible and a drain scoped to it would skip work it owed.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 3301;
    const NODE: u64 = 89;

    for event_time_ms in [1_000_u64, 50_000] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                event_time_ms,
                reason: 0,
                propagate_depth: 0,
            },
        });
    }

    let found = |start: u64, end: u64| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                start_time_ms: start,
                end_time_ms: end,
                limit: None,
            },
        });
        match response.response {
            CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => !nodes.is_empty(),
            _ => false,
        }
    };

    assert!(found(0, 2_000), "a window over the earliest event must match");
    assert!(found(40_000, 60_000), "a window over the latest event must match");
    assert!(found(10_000, 20_000), "a window inside the span must match");
    assert!(!found(60_000, 70_000), "a window past the span must not match");
}

// shared-corpus: context_node_inline_embedding
#[test]
fn a_node_embedding_lands_on_the_node_itself() {
    // Addressing the write by the node is the whole point: the old ref hash of
    // (tenant, owner, level) is one-way, so a vector written under it could only ever be
    // found again by recomputing that hash from an owner someone already had in hand.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 4242;
    const NODE: u64 = 77;

    let node = ContextNode {
        node_hash: NODE,
        parent_hash: 0,
        kind: 1,
        canonical_name: "session/with-a-vector".to_string(),
        l0: "the text this node is about".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    };
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: Box::new(node),
        },
    });

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: TENANT,
            node_hash: NODE,
            model_hash: 909,
            vector: vec![0.25, 0.5, -0.75],
            updated_at_ms: 1_781_000_000_000,
        },
    });

    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: TENANT,
            node_hash: NODE,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    let node = node.expect("the node must still be there");
    assert_eq!(vec![0.25, 0.5, -0.75], node.vector, "vector did not reach the node");
    assert_eq!(909, node.embedding_model_hash);
    assert_eq!(1_781_000_000_000, node.embedding_updated_at_ms);
    // The rest of the node must survive a write that only meant to attach a vector.
    assert_eq!("session/with-a-vector", node.canonical_name);
    assert_eq!("the text this node is about", node.l0);
    assert_eq!(1, node.kind);
}

// shared-corpus: context_node_inline_embedding_replaces
#[test]
fn re_embedding_a_node_replaces_the_vector_rather_than_accumulating() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 4243;
    const NODE: u64 = 78;

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: Box::new(ContextNode {
                node_hash: NODE,
                parent_hash: 0,
                kind: 1,
                canonical_name: "n".to_string(),
                l0: "text".to_string(),
                status: 0,
                last_event_time_ms: 0,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: Vec::new(),
                embedding_model_hash: 0,
                embedding_updated_at_ms: 0,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            }),
        },
    });
    for (model, vector, at) in [
        (1_u64, vec![1.0_f32, 0.0], 1_781_000_000_000_u64),
        (2, vec![0.0, 1.0], 1_781_000_000_500),
    ] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash: NODE,
                model_hash: model,
                vector,
                updated_at_ms: at,
            },
        });
    }
    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: TENANT,
            node_hash: NODE,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    let node = node.expect("node must exist");
    assert_eq!(vec![0.0, 1.0], node.vector, "the newer vector must win outright");
    assert_eq!(2, node.embedding_model_hash);
    assert_eq!(1_781_000_000_500, node.embedding_updated_at_ms);
}

// shared-corpus: context_node_inline_embedding_no_node
#[test]
fn an_embedding_for_a_node_that_does_not_exist_is_refused_not_invented() {
    // Writing a placeholder node here would put a node into the tree that ingest never
    // created -- a node with a vector, no name and no text, which retrieval would then score.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: 4244,
            node_hash: 999,
            model_hash: 1,
            vector: vec![1.0],
            updated_at_ms: 1_781_000_000_000,
        },
    });
    let CommandResponse::ContextObjectKey { object_key } = response.response else {
        panic!("expected ContextObjectKey");
    };
    assert!(object_key.is_empty(), "no node means nothing was written");

    let fetched = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash: 4244,
            node_hash: 999,
        },
    });
    let CommandResponse::ContextNode { node, .. } = fetched.response else {
        panic!("expected ContextNode");
    };
    assert!(node.is_none(), "a node must not be conjured by embedding it");
}

// shared-corpus: context_node_inline_embedding_query
#[test]
fn node_vectors_can_be_asked_for_by_owner() {
    // The read that the separate record could never offer. ContextQueryEmbeddings is keyed by a
    // hash of (tenant, owner, level), so a caller must already hold every owner just to build
    // the keys, and the reply cannot say which owner each vector came from -- the caller has to
    // rebuild that mapping itself. Asking by node returns the pairing directly.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 5150;

    for (node_hash, vector) in [(1_u64, vec![1.0_f32, 0.0]), (2, vec![0.0, 1.0]), (3, vec![])] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("n{node_hash}"),
                    l0: format!("text {node_hash}"),
                    status: 0,
                    last_event_time_ms: 0,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        if !vector.is_empty() {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextSetNodeEmbedding {
                    tenant_hash: TENANT,
                    node_hash,
                    model_hash: 5,
                    vector,
                    updated_at_ms: 1_781_000_000_000,
                },
            });
        }
    }

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextQueryNodeEmbeddings {
            tenant_hash: TENANT,
            node_hashes: vec![1, 2, 3, 4],
        },
    });
    let CommandResponse::ContextNodeEmbeddings { embeddings } = response.response else {
        panic!("expected ContextNodeEmbeddings");
    };

    // Node 3 exists but was never embedded, and node 4 does not exist at all. Both are omitted
    // rather than returned as an empty vector, so "not embedded yet" stays distinguishable from
    // "embedded to zeros" -- a caller that cannot tell them apart scores an un-embedded node as
    // maximally dissimilar instead of falling back to lexical matching.
    assert_eq!(2, embeddings.len(), "only embedded nodes may come back");
    let by_node: std::collections::BTreeMap<u64, Vec<f32>> = embeddings.into_iter().collect();
    assert_eq!(Some(&vec![1.0, 0.0]), by_node.get(&1));
    assert_eq!(Some(&vec![0.0, 1.0]), by_node.get(&2));
    assert!(!by_node.contains_key(&3), "an un-embedded node must not appear");
    assert!(!by_node.contains_key(&4), "a missing node must not appear");
}

// shared-corpus: context_traversal_scores_from_inline_vector
#[test]
fn traversal_scores_a_child_whose_only_vector_is_on_the_node() {
    // The retirement-readiness assertion: NO separate embedding record exists here. If the
    // traversal still reaches for one, dropping those records would silently turn every
    // tree query into an empty answer -- so this test must hold BEFORE they can go.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    const TENANT: u64 = 5001;
    const ROOT: u64 = 1;
    const NEAR: u64 = 2;
    const FAR: u64 = 3;
    const EVENT_TIME: u64 = 1_781_600_000_000;

    for (node_hash, name) in [(ROOT, "root"), (NEAR, "near"), (FAR, "far")] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash,
                    parent_hash: if node_hash == ROOT { 0 } else { ROOT },
                    kind: 1,
                    canonical_name: name.to_string(),
                    l0: format!("{name} text"),
                    status: 0,
                    last_event_time_ms: EVENT_TIME,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        });
        assert!(response.status.ok);
    }
    for child_hash in [NEAR, FAR] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertChildRef {
                tenant_hash: TENANT,
                child_ref: ContextChildRef {
                    parent_hash: ROOT,
                    child_hash,
                    updated_at_ms: EVENT_TIME,
                },
            },
        });
        assert!(response.status.ok);
    }
    // Vectors arrive ONLY through the node-addressed write -- no ContextUpsertEmbedding at all.
    for (node_hash, first, second) in [(NEAR, 1.0, 0.0), (FAR, 0.0, 1.0)] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash: TENANT,
                node_hash,
                model_hash: 7,
                vector: vec![first, second],
                updated_at_ms: EVENT_TIME,
            },
        });
        assert!(response.status.ok);
    }

    let traversal = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextTraverseTree {
            tenant_hash: TENANT,
            start_node_hash: ROOT,
            query_vector: vec![1.0, 0.0],
            max_depth: Some(2),
            top_k_per_depth: Some(1),
            max_children_scored_per_parent: Some(10),
            max_candidate_nodes: Some(4),
            leaf_only: true,
        },
    });
    assert!(
        matches!(
            traversal.response,
            CommandResponse::ContextTraversedNodes { ref nodes, .. }
                if nodes.len() == 1 && nodes[0].node_hash == NEAR && nodes[0].score > 0.99
        ),
        "a child whose only vector lives on the node must be scored"
    );
}

// shared-corpus: context_extracted_event_time_query
#[test]
fn an_extracted_event_is_visible_to_time_ranged_queries() {
    // The event rekey moved the primary map to EVENT ID keys with a separate time index; this
    // arm was missed, kept inserting timeline keys into the id-keyed map and never fed the time
    // index -- so every extracted event wrote successfully and was invisible to time queries.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: serde_json::from_str(r#"{"kind":"context_write_extracted_event","tenant_hash":99,"node_hash":700,"event":{"event_id_hash":701,"event_time_ms":510,"ingestion_time_ms":515,"type":4,"confidence":0.9,"importance":0.8,"text":"probe event"},"indexes":{"entity_hashes":[9001],"status_hash":77,"source_hash":88,"event_time_bucket_ms":500}}"#).unwrap(),
    });
    assert!(write.status.ok);
    assert!(matches!(
        write.response,
        CommandResponse::ContextExtractedEventWrite { ref event_object_key, .. }
            if event_object_key == "ctx:event:99:700"
    ));
    let q = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: serde_json::from_str(r#"{"kind":"context_query_events","tenant_hash":99,"node_hash":700,"start_time_ms":500,"end_time_ms":520,"limit":null}"#).unwrap(),
    });
    assert!(
        matches!(
            q.response,
            CommandResponse::ContextEvents { ref events, .. }
                if events.len() == 1 && events[0].event_id_hash == 701
        ),
        "a successfully written extracted event must be visible to a time-ranged query"
    );
}

#[test]
fn scratch_engine_index_dir_dies_with_the_last_engine_clone() {
    let engine = TemporalEngine::default();
    let index_dir = engine.index_dir.clone();
    assert!(index_dir.exists(), "a scratch engine must create its index dir");
    let clone = engine.clone();
    drop(engine);
    assert!(index_dir.exists(), "a live engine clone must keep the scratch dir");
    drop(clone);
    assert!(
        !index_dir.exists(),
        "the last engine clone must remove the scratch index dir on drop"
    );
}

#[test]
fn served_index_container_round_trips_and_still_reads_plain_json() {
    use crate::engine::{decode_index_bytes, encode_index_bytes, encode_index_bytes_as_plain_json};

    let mut shard = ShardState::default();
    shard.set_string_address(
        "container-probe".into(), &ElementEntry::from_parts(7, 11, 13, None, None),
    );

    // Raw JSON, which is what an older binary wrote. Nothing in production produces this any
    // more, so the test names the shape it wants rather than setting a variable to select it.
    let plain = encode_index_bytes_as_plain_json(&shard);
    assert_eq!(plain.first(), Some(&b'{'), "container off must write JSON");
    let decoded = decode_index_bytes(&plain).expect("json index decodes");
    assert!(decoded.strings().contains_key("container-probe"));

    // And what production writes: a magic-prefixed payload that decodes back to the same state.
    let wrapped = encode_index_bytes(&shard);
    assert!(wrapped.starts_with(b"TSIDX\x01"), "container on must write the container");
    assert_ne!(wrapped.first(), Some(&b'{'));
    let decoded = decode_index_bytes(&wrapped).expect("container index decodes");
    assert!(decoded.strings().contains_key("container-probe"));

    // ...and reading is unconditional: a container decodes with the flag off, which is what
    // makes the write flag safe to turn on and off independently of any reader.
    let decoded_again = decode_index_bytes(&wrapped).expect("container decodes with flag off");
    assert!(decoded_again.strings().contains_key("container-probe"));

    // An unknown payload codec must be refused, never guessed at: a mis-parsed index serves
    // wrong data with no error anywhere, which is the failure mode this container exists to stop.
    let mut future = wrapped.clone();
    future[6] = 0xEE;
    let refused = decode_index_bytes(&future).expect_err("unknown codec must refuse");
    assert!(refused.contains("cannot read"), "unhelpful refusal: {refused}");
}

#[test]
fn binary_index_payload_round_trips_and_refuses_a_shape_it_cannot_read() {
    use crate::engine::{decode_index_bytes, encode_index_bytes, encode_index_bytes_as_plain_json};

    let mut shard = ShardState::default();
    shard.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    for i in 0..64u64 {
        shard.set_string_address(
            std::sync::Arc::from(format!("object-{i}").as_str()), &ElementEntry::from_parts(i, i * 7, i + 1, Some(i), Some(i * 3)),
        );
    }
    shard.hashes.insert_element_for_test(
        "hash-object",
        std::sync::Arc::from("component"),
        ElementEntry::from_parts(9, 1, 2, None, None),
    );
    shard.applied_wal_sequence = Some(4242);

    std::env::set_var("TS_INDEX_CODEC", "msgpack");
    let binary = encode_index_bytes(&shard);
    std::env::remove_var("TS_INDEX_CODEC");

    assert!(binary.starts_with(b"TSIDX\x01"), "binary payload must ride the container");
    assert_eq!(binary[6], 2, "payload codec id");

    // Exact round-trip: the durable image has to come back as the same state, not merely a
    // plausible one -- so compare the maps, not just that it decoded.
    let decoded = decode_index_bytes(&binary).expect("binary index decodes");
    assert_eq!(decoded.strings().len(), shard.strings().len());
    for (key, slot) in shard.strings() {
        assert_eq!(decoded.string_address(key), Some(slot.address()), "address changed for {key}");
    }
    // `hashes` RIDES THE PAYLOAD NOW, and this assertion is back to the exact round-trip it
    // originally made. The history is the point: it asserted equality, was weakened to assert the
    // DROP when `hashes` became `#[serde(default, skip_serializing)]`, and the drop is what stopped
    // being true when the field became durable again. So the guard is retargeted a second time, in
    // the direction that makes it stronger rather than weaker -- equality over the field map, not
    // merely that something arrived.
    //
    // Exact, per field, for the reason the `strings` loop above is exact: a durable image has to
    // come back as the SAME state and not a plausible one, and a length check alone would pass an
    // image whose addresses had all moved.
    assert_eq!(
        decoded.hashes.len(),
        shard.hashes.len(),
        "the decoded image holds a different number of hash keys"
    );
    for (key, fields) in &shard.hashes {
        let got = decoded
            .hashes
            .get(key)
            .unwrap_or_else(|| panic!("hash key {key} did not survive the round trip"));
        assert_eq!(
            got.len(),
            fields.len(),
            "hash key {key} came back with a different field count"
        );
        for (field, address) in fields.iter() {
            assert_eq!(
                got.get(field),
                Some(address),
                "hash field {key}/{field} changed across the round trip"
            );
        }
    }
    // THE DENOMINATOR. Every assertion above is satisfied by two empty maps.
    assert!(
        !shard.hashes.is_empty(),
        "the shard under test must actually have hashes, or the assertions above prove nothing"
    );
    assert_eq!(decoded.applied_wal_sequence, shard.applied_wal_sequence);
    assert_eq!(decoded.index_format_version, shard.index_format_version);

    // The payload is addressed by field ORDER, so a struct of a different shape must be refused
    // rather than decoded into something plausible and wrong. The version stamp sits outside the
    // payload precisely so it can be checked before any of it is parsed.
    let mut wrong_version = binary.clone();
    wrong_version[7..11].copy_from_slice(&99u32.to_be_bytes());
    let refused = decode_index_bytes(&wrong_version).expect_err("a foreign struct version refuses");
    assert!(refused.contains("struct version"), "unhelpful refusal: {refused}");

    // And every older on-disk shape still loads: plain JSON, and the compressed-JSON payload.
    let plain = encode_index_bytes_as_plain_json(&shard);
    assert_eq!(plain.first(), Some(&b'{'), "container off still writes JSON");
    assert_eq!(
        decode_index_bytes(&plain).expect("json decodes").strings().len(),
        shard.strings().len()
    );

    std::env::set_var("TS_INDEX_CODEC", "zstd-json");
    let compressed_json = encode_index_bytes(&shard);
    std::env::remove_var("TS_INDEX_CODEC");
    assert_eq!(compressed_json[6], 1, "zstd-json keeps codec id 1");
    assert_eq!(
        decode_index_bytes(&compressed_json).expect("zstd-json decodes").strings().len(),
        shard.strings().len()
    );

    // The binary payload is the smaller one -- that is the whole point of adding it.
    assert!(
        binary.len() < plain.len(),
        "binary {} not smaller than json {}",
        binary.len(),
        plain.len()
    );
}


/// Where does the cost of reading ONE summary go: fetching the bytes, or decoding them?
///
/// The command-level measurement says 253 allocations and 289 KB per candidate for a 4 KB summary
/// -- a 70x amplification -- and two guesses about why have already been wrong, including one that
/// made it worse. So split the read in half and count each half, and print how many points the
/// extent holds, which settles whether siblings are being decoded at all.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_reading_one_summary_actually_costs -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_reading_one_summary_actually_costs() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  text   points/extent   extent bytes   read allocs   read bytes   decode allocs   decode bytes
"
    );
    for text_len in [16usize, 512, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let text = "s".repeat(text_len);
        for node_hash in 1..=120u64 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertSummary {
                    tenant_hash: 7201,
                    summary: crate::types::ContextSummary {
                        node_hash,
                        level: 2,
                        text: text.clone(),
                        valid_from_ms: 1_000,
                        vector: vec![0.25_f32; 16],
                        embedding_model_hash: 0,
                    },
                },
            });
            assert!(response.status.ok, "{:?}", response.status);
        }

        let (address, object_key, routing_bucket) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let key = super::context::context_summary_key(7201, 60, 2);
            let series = shard
                .context_summaries()
                .get(&key[..])
                .expect("the summary series must exist, or this measures nothing");
            let address = series
                .points()
                .next()
                .map(|(_, address)| address.clone())
                .expect("one entry");
            let (start, end) = shard.routing_range();
            (
                address,
                key.clone(),
                Some(super::hashing::block_routing_bucket(&key, start, end)),
            )
        };

        // Warm, so neither half is charged for filling a cache that a steady-state read finds warm.
        let _ = super::read_block_bytes(&engine.cache, &engine.block_store, 1, &address, crate::engine::hashing::BlockIdentity::of(1, "context_summary", &object_key, None), routing_bucket);

        // THE BUCKET IS HOISTED OUT OF THE PROBE, and that is not hiding a cost. It is one FNV-1a
        // pass over a borrowed `&str` returning a `u32`: no allocation, so it cannot move an
        // allocation count either way. What it would move is the number of ROUNDS the derivation
        // ran, and this probe counts allocations per read, not instructions --
        // `engine::tests::address_footprint::what_consulting_the_object_index_costs_against_computing_the_hash`
        // is where the derivation itself is counted.
        let read_probe = crate::alloc_probe::Probe::start();
        let mut bytes = Vec::new();
        for _ in 0..5 {
            bytes = super::read_block_bytes(&engine.cache, &engine.block_store, 1, &address, crate::engine::hashing::BlockIdentity::of(1, "context_summary", &object_key, None), routing_bucket)
                .expect("the page must read, or the split below is measuring a None");
        }
        let read = read_probe.stop();

        let decode_probe = crate::alloc_probe::Probe::start();
        let mut points = 0usize;
        for _ in 0..5 {
            points = match super::packed_blocks::decode_feature_block_strict(&bytes) {
                super::state::PackedFeatureBlockDecode::Packed(p) => p.len(),
                super::state::PackedFeatureBlockDecode::Legacy => 1,
                super::state::PackedFeatureBlockDecode::Corrupt(_) => 0,
            };
        }
        let decode = decode_probe.stop();

        println!(
            "  {text_len:>4}   {points:>13}   {:>12}   {:>11}   {:>10}   {:>13}   {:>12}",
            bytes.len(),
            read.allocs / 5,
            read.alloc_bytes / 5,
            decode.allocs / 5,
            decode.alloc_bytes / 5,
        );

        // Cold walk over every address, which is what a batch read actually does: 120 distinct
        // extents rather than one warm one. Warming a single address measures the best case and
        // would report it as the cost.
        // Each block paired with the bucket its own key routes to, derived OUTSIDE the probe for the
        // reason given at the first probe in this test: the derivation allocates nothing, so it
        // cannot move an allocation count, and hoisting it keeps the numbers comparable with the
        // runs taken before the bucket became an argument.
        let addresses_with_buckets: Vec<(u32, String, crate::block_store::ElementEntry)> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let (start, end) = shard.routing_range();
            (1..=120u64)
                .filter_map(|node_hash| {
                    let key = super::context::context_summary_key(7201, node_hash, 2);
                    shard
                        .context_summaries()
                        .get(&key[..])
                        .and_then(|series| series.points().next())
                        .map(|(_, address)| {
                            (
                                super::hashing::block_routing_bucket(&key, start, end),
                                key.clone(),
                                address.clone(),
                            )
                        })
                })
                .collect()
        };
        let addresses: Vec<crate::block_store::ElementEntry> = addresses_with_buckets
            .iter()
            .map(|(_, _, address)| address.clone())
            .collect();
        assert_eq!(addresses.len(), 120, "every summary must be addressable");
        let wal_resident = addresses
            .iter()
            .filter(|a| crate::wal_record::is_wal_resident(a.block_slab_id()))
            .count();
        let with_block_id = addresses.iter().filter(|a| a.block_id().is_some()).count();
        let distinct_slabs: std::collections::BTreeSet<u64> =
            addresses.iter().map(|a| a.block_slab_id()).collect();
        println!(
            "         {wal_resident}/120 wal_resident addresses, {with_block_id} carry a page_id, {} distinct slabs",
            distinct_slabs.len(),
        );
        let extent_total: u64 = addresses.iter().map(|a| a.length()).sum();

        let walk_probe = crate::alloc_probe::Probe::start();
        let mut decoded = 0usize;
        for (routing_bucket, object_key, address) in &addresses_with_buckets {
            if let Some(block) = super::read_block_bytes(
                &engine.cache,
                &engine.block_store,
                1,
                address,
                crate::engine::hashing::BlockIdentity::of(1, "context_summary", object_key, None),
                Some(*routing_bucket),
            ) {
                if let super::state::PackedFeatureBlockDecode::Packed(points) =
                    super::packed_blocks::decode_feature_block_strict(&block)
                {
                    decoded += points.len();
                }
            }
        }
        let walk = walk_probe.stop();

        // The block-store read alone, over the same addresses, on a store whose cache is already
        // warm from the walk above -- so this is purely file seek + read_exact + decode.
        let read_only_probe = crate::alloc_probe::Probe::start();
        let mut read_bytes_total = 0usize;
        for address in &addresses {
            if let Ok(b) = engine.block_store.read(address) {
                read_bytes_total += b.len();
            }
        }
        let read_only = read_only_probe.stop();
        println!(
            "         block-store read alone: {} allocs/addr, {} bytes/addr ({} payload bytes returned in total)",
            read_only.allocs / 120,
            read_only.alloc_bytes / 120,
            read_bytes_total,
        );
        println!(
            "         cold walk over every address: {} allocs/addr, {} bytes/addr, {} extents totalling {} bytes, {decoded} points decoded",
            walk.allocs / 120,
            walk.alloc_bytes / 120,
            addresses.len(),
            extent_total,
        );
    }
    println!(
        "
  points/extent > 1 means siblings ARE decoded for every read, and a page-granular cache pays.
  points/extent == 1 means each record has its own extent and the cost is inside one record.
"
    );
}


/// How many distinct blocks do the nodes a retrieve scores actually span?
///
/// The node fetch is now nearly all of a retrieve's per-candidate cost, and with the discarded
/// text measured at 1 allocation of 16 it is essentially one block read per candidate. Clustering
/// the nodes a traversal co-selects would only help if they currently sit on DIFFERENT blocks.
///
/// candidates-per-extent is the ceiling: 1.0 means every candidate costs its own block read and
/// clustering could remove nearly all of them; a high number means they already share and there is
/// nothing here worth a storage-layout change.
///
///   cargo test -p temporalstore-rust --lib how_many_blocks_do_a_retrieves_candidates_span -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn how_many_blocks_do_a_retrieves_candidates_span() {
    println!(
        "
  nodes   distinct extents   nodes/extent   distinct slabs   bytes addressed
"
    );
    for count in [40_u64, 120, 360] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let tenant = 7401_u64;
        for node_hash in 1..=count {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode {
                    tenant_hash: tenant,
                    node: Box::new(crate::types::ContextNode {
                        node_hash,
                        parent_hash: 0,
                        kind: 1,
                        canonical_name: format!("session/node-{node_hash}"),
                        l0: "a summary of roughly the length a real extract produces for one turn"
                            .to_string(),
                        status: 0,
                        last_event_time_ms: 1_781_700_000_000,
                        l1_ref: String::new(),
                        raw_metadata_ref: String::new(),
                        vector: vec![0.25_f32; 16],
                        embedding_model_hash: 7,
                        embedding_updated_at_ms: 1,
                        summary_vector: vec![0.5_f32; 16],
                        summary_vector_valid_from_ms: 1_781_700_000_000,
                        summary_vector_model_hash: 7,
                    }),
                },
            });
            assert!(response.status.ok, "upsert {node_hash}: {:?}", response.status);
        }

        // The addresses the scoring pass would read, straight from the shard's own map.
        let (extents, slabs, bytes) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let mut extents: std::collections::BTreeSet<(u64, u64, u64)> =
                std::collections::BTreeSet::new();
            let mut slabs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            let mut bytes = 0_u64;
            for node_hash in 1..=count {
                let key = super::context::context_node_key(tenant, node_hash);
                let address = shard
                    .hashes
                    .get(&key)
                    .and_then(|fields| fields.values().next())
                    .or_else(|| shard.context_nodes().get(key.as_str()));
                if let Some(address) = address {
                    if extents.insert((address.block_slab_id(), address.offset(), address.length())) {
                        bytes += address.length();
                    }
                    slabs.insert(address.block_slab_id());
                }
            }
            (extents.len(), slabs.len(), bytes)
        };
        assert!(extents > 0, "no node addresses resolved -- this measures nothing");
        println!(
            "  {count:>5}   {extents:>16}   {:>12.2}   {slabs:>14}   {bytes:>15}",
            count as f64 / extents as f64,
        );

        // Span versus payload: if these extents are adjacent, `read_range` can fetch many nodes in
        // one read and slice them apart, which needs no durable format change. If they are spread
        // through the slab, only a real re-layout helps.
        let (span, largest_gap, ordered) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let mut ranges: Vec<(u64, u64)> = Vec::new();
            for node_hash in 1..=count {
                let key = super::context::context_node_key(tenant, node_hash);
                if let Some(address) = shard
                    .hashes
                    .get(&key)
                    .and_then(|fields| fields.values().next())
                    .or_else(|| shard.context_nodes().get(key.as_str()))
                {
                    ranges.push((address.offset(), address.length()));
                }
            }
            ranges.sort_unstable();
            let ordered = ranges.len();
            let span = match (ranges.first(), ranges.last()) {
                (Some(first), Some(last)) => (last.0 + last.1).saturating_sub(first.0),
                _ => 0,
            };
            let mut largest_gap = 0_u64;
            for pair in ranges.windows(2) {
                let end_of_first = pair[0].0 + pair[0].1;
                largest_gap = largest_gap.max(pair[1].0.saturating_sub(end_of_first));
            }
            (span, largest_gap, ordered)
        };
        println!(
            "          span {span} bytes over {ordered} extents holding {bytes} bytes  \
({:.2}x the payload), largest gap {largest_gap}",
            span as f64 / bytes.max(1) as f64,
        );
    }
    println!(
        "
  nodes/extent near 1.0  => every candidate costs its own page read; clustering is the lever.
  nodes/extent high      => candidates already share pages and a layout change wins nothing.
"
    );
}


/// The same contiguity question, but after a REAL ingest rather than a node-only fixture.
///
/// Writing only nodes packs them perfectly (span == payload, zero gaps) and that is not the shape
/// a deployment has: an ingest writes events, summaries, entities and index rows in between, so the
/// nodes a retrieve scores are separated by records it does not want.
///
/// Reports the contiguous RUNS the node extents form. Reads per retrieve fall from one-per-candidate
/// to one-per-run if the fetch coalesces, and the foreign bytes column says what that costs.
///
///   cargo test -p temporalstore-rust --lib how_scattered_are_node_extents_after_a_real_ingest -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn how_scattered_are_node_extents_after_a_real_ingest() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    println!(
        "
  adds   node extents   runs   extents/run   node bytes   span bytes   foreign bytes dragged
"
    );
    for adds in [40_usize, 120] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let tenant = 7501_u64;
        let mut node_hashes: Vec<u64> = Vec::new();
        for index in 0..adds {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: tenant,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: tenant,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("LOC-{index:06}"),
                        title: format!("locality {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} about the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "ingest {index}: {:?}", report.status);
            node_hashes.extend(report.node_hashes.iter().copied());
        }
        node_hashes.sort_unstable();
        node_hashes.dedup();
        assert!(!node_hashes.is_empty(), "no nodes ingested -- nothing to measure");

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut ranges: Vec<(u64, u64, u64)> = Vec::new(); // (slab, offset, length)
        for node_hash in &node_hashes {
            let key = super::context::context_node_key(tenant, *node_hash);
            if let Some(address) = shard
                .hashes
                .get(&key)
                .and_then(|fields| fields.values().next())
                .or_else(|| shard.context_nodes().get(key.as_str()))
            {
                ranges.push((address.block_slab_id(), address.offset(), address.length()));
            }
        }
        ranges.sort_unstable();
        let node_bytes: u64 = ranges.iter().map(|r| r.2).sum();

        // A run is a maximal group of extents that a single range read could cover: same slab, and
        // each starting where the previous ended.
        let mut runs = 0_usize;
        let mut span = 0_u64;
        let mut previous: Option<(u64, u64)> = None; // (slab, end offset)
        let mut run_start: Option<(u64, u64)> = None;
        for (slab, offset, length) in &ranges {
            match previous {
                Some((prev_slab, prev_end)) if prev_slab == *slab && prev_end == *offset => {}
                _ => {
                    if let (Some((_, start)), Some((_, end))) = (run_start, previous) {
                        span += end.saturating_sub(start);
                    }
                    runs += 1;
                    run_start = Some((*slab, *offset));
                }
            }
            previous = Some((*slab, offset + length));
        }
        if let (Some((_, start)), Some((_, end))) = (run_start, previous) {
            span += end.saturating_sub(start);
        }

        println!(
            "  {adds:>4}   {:>12}   {runs:>4}   {:>11.2}   {node_bytes:>10}   {span:>10}   {:>21}",
            ranges.len(),
            ranges.len() as f64 / runs.max(1) as f64,
            span.saturating_sub(node_bytes),
        );
    }
    println!(
        "
  extents/run near 1  => nodes are isolated between other records; only a re-layout helps.
  extents/run high    => a coalescing fetch cuts reads by that factor with no format change.
"
    );
}


/// Does the per-ingest bucket-index reconstruct change anything, or recompute what is already right?
///
/// It costs 94% of an add's allocations at 320 memories and grows with the store. Since a context
/// write now maintains the index rather than asking for a rebuild, the rebuild at the end of each
/// ingest may be pure overhead -- and if it is, skipping it removes almost all of an add's
/// allocation.
///
/// Snapshots every bucket before and after, and reports the buckets that differ. A difference is
/// not a failure of this test: it names precisely what a write still fails to maintain, which is
/// the next thing to fix.
///
///   cargo test -p temporalstore-rust --lib does_the_per_ingest_reconstruct_change_anything -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn does_the_per_ingest_reconstruct_change_anything() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    /// (routing bucket, blocks, live object ids, tombstoned ids, layout, deleted, in_memory)
    type Shape = (u32, usize, usize, usize, String, bool, bool);

    fn snapshot(engine: &TemporalEngine) -> Vec<Shape> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let mut out: Vec<Shape> = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| {
                (
                    *routing_bucket,
                    bucket.block_index.len(),
                    bucket.object_index.object_count(),
                    bucket.deleted_object_index.object_count(),
                    format!("{:?}", bucket.layout),
                    bucket.deleted(),
                    bucket.in_memory(),
                )
            })
            .collect();
        out.sort_unstable();
        out
    }

    for rung in [40_usize, 160] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 7901,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 7901,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("REDUN-{index:06}"),
                        title: format!("redundancy {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        // The ingest already ran a reconstruct at its end. Snapshot, run another, compare: a
        // reconstruct that changes nothing on an already-reconstructed store is idempotent, which
        // is necessary but not sufficient. The interesting case is the one below it.
        let before_idempotent = snapshot(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after_idempotent = snapshot(&engine);

        // The real question: does a reconstruct change anything after ONE MORE plain write, which
        // is what the ingest's own final reconstruct is there to absorb?
        let node = crate::types::ContextNode {
            node_hash: 8_500_000 + rung as u64,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("session/redun-{rung}"),
            l0: "one more write, then reconstruct".to_string(),
            status: 0,
            last_event_time_ms: 1_781_700_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vec![0.25_f32; 16],
            embedding_model_hash: 7,
            embedding_updated_at_ms: 1,
            summary_vector: vec![0.5_f32; 16],
            summary_vector_valid_from_ms: 1_781_700_000_000,
            summary_vector_model_hash: 7,
        };
        let wrote = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: 7901, node: Box::new(node) },
        });
        assert!(wrote.status.ok, "{:?}", wrote.status);
        let before_write = snapshot(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after_write = snapshot(&engine);

        let idempotent_differs = before_idempotent
            .iter()
            .zip(after_idempotent.iter())
            .filter(|(a, b)| a != b)
            .count()
            + before_idempotent.len().abs_diff(after_idempotent.len());
        let write_differs = before_write
            .iter()
            .zip(after_write.iter())
            .filter(|(a, b)| a != b)
            .count()
            + before_write.len().abs_diff(after_write.len());

        println!(
            "  corpus {rung:>4}: buckets {:>4}   reconstruct-after-reconstruct differs in {idempotent_differs}   reconstruct-after-one-write differs in {write_differs}",
            before_idempotent.len(),
        );
        if write_differs > 0 {
            for (a, b) in before_write.iter().zip(after_write.iter()).filter(|(a, b)| a != b).take(3)
            {
                println!("      before {a:?}");
                println!("      after  {b:?}");
            }
        }
    }
    println!(
        "
  both columns 0 => the reconstruct recomputes an index the writes already maintain, and the
                    ingest's final call is removable (94% of an add's allocations at 320 memories).
  non-zero      => that difference is what a write still fails to maintain; fix that first.
"
    );
}


/// Full-contents version of the reconstruct-redundancy question.
///
/// The shape comparison said a reconstruct changes nothing, but it compared counts after a bare
/// node write. Removing a call that keeps a durable index correct deserves the strong form: every
/// block entry's identity, address and flags, captured after a COMPLETE ingest rather than one
/// write.
///
///   cargo test -p temporalstore-rust --lib deep_compare_the_index_a_reconstruct_produces -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn deep_compare_the_index_a_reconstruct_produces() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    /// Every field of every block entry, ordered, so two indexes compare exactly.
    fn deep(engine: &TemporalEngine) -> Vec<String> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        // THE IDENTITY, TAKEN ONCE AND KEYED BY POSITION. The walk itself has to stay per
        // bucket because it also emits a row per object-index and per deleted-object-index
        // member, which belong to the bucket and not to any page.
        let named: std::collections::HashMap<(u32, u64), std::sync::Arc<str>> =
            super::blocks_with_identity(&shard.bucket_index)
                .into_iter()
                .map(|(routing_bucket, handle, object_key, _block)| {
                    ((routing_bucket, handle), object_key)
                })
                .collect();
        let mut rows: Vec<String> = Vec::new();
        for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
            for (field_key, block) in bucket.block_index.iter() {
                let object_key = named
                    .get(&(*routing_bucket, *field_key))
                    .expect("every page walked above was named beside it");
                rows.push(format!(
                    "{routing_bucket}|{field_key}|{}|{}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}",
                    object_key,
                    block.model_id,
                    None::<std::sync::Arc<str>>,
                    block.object_id(1, object_key),
                    block.address.block_slab_id(),
                    block.address.offset(),
                    block.address.length(),
                    block.dirty,
                    block.deleted,
                    block.log_backed(),
                    bucket.layout as u8 as u32,
                ));
            }
            for object_id in bucket.object_index.iter() {
                rows.push(format!("{routing_bucket}|obj|{object_id}"));
            }
            for object_id in bucket.deleted_object_index.iter() {
                rows.push(format!("{routing_bucket}|tomb|{object_id}"));
            }
        }
        rows.sort_unstable();
        rows
    }

    for rung in [30_usize, 120] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let mut ingest = |index: usize| {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 8101,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 8101,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("DEEP-{index:06}"),
                        title: format!("deep {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "ingest {index}: {:?}", report.status);
        };

        for index in 0..rung {
            ingest(index);
        }

        // A COMPLETE ingest, then compare the index before and after a reconstruct. The ingest ran
        // its own reconstruct at the end, so this asks whether a further one would find anything to
        // change -- which is exactly what removing the call would rely on.
        ingest(rung);
        let before = deep(&engine);
        engine.reconstruct_bucket_index_now(1);
        let after = deep(&engine);

        let only_before = before.iter().filter(|row| !after.contains(row)).count();
        let only_after = after.iter().filter(|row| !before.contains(row)).count();
        println!(
            "  corpus {rung:>4}: {} page/index rows   only-before {only_before}   only-after {only_after}",
            before.len(),
        );
        for row in before.iter().filter(|row| !after.contains(row)).take(2) {
            println!("      lost by reconstruct: {row}");
        }
        for row in after.iter().filter(|row| !before.contains(row)).take(2) {
            println!("      added by reconstruct: {row}");
        }
    }
    println!(
        "
  both 0 => the reconstruct reproduces the index the writes already built, entry for entry, and
            the ingest's final call can go (94% of an add's allocations at 320 memories).
  non-0  => those rows are what a write fails to maintain, and are the thing to fix instead.
"
    );
}


/// Series append or block-store append: which one grows with the store?
///
/// `ContextUpsertSummary` costs 1,553 allocations at 40 memories and 10,667 at 320, while
/// `ContextUpsertNode` is flat at ~194. They differ in their write primitive, so measure the
/// primitives directly on grown stores with FRESH keys -- nothing merges into an existing series,
/// so anything that grows is responding to the store rather than to the key.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib which_write_primitive_grows_with_the_store -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn which_write_primitive_grows_with_the_store() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };
    use crate::types::FeaturePoint;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  primitive                        corpus 40   corpus 320   growth
"
    );

    let mut series_small = 0_u64;
    let mut series_large = 0_u64;
    let mut value_small = 0_u64;
    let mut value_large = 0_u64;

    for (rung, series_out, value_out) in [
        (40_usize, &mut series_small, &mut value_small),
        (320_usize, &mut series_large, &mut value_large),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: 8301,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: 8301,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("PRIM-{index:06}"),
                        title: format!("prim {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        let payload = vec![7_u8; 512];
        let shards = engine.shards.read().expect("engine lock poisoned");
        let _ = shards.get(&1).expect("loaded shard");
        drop(shards);

        // The series primitive, on a fresh key.
        let warm_key = format!("probe:series:warm:{rung}");
        let _ = super::packed_blocks::append_timestamped_kv_blocks(
            &engine.cache,
            &engine.block_store,
            1,
            "context_summary",
            &warm_key,
            vec![FeaturePoint { timestamp_ms: 1, value: payload.clone() }],
            0,
            false,
            // promote_sync_writes: every caller in the engine passes true, and this probe
            // exists to characterise the primitive as the engine uses it.
            true,
            // A fresh object here, so its first block is block 0.
            0,
        );
        let key = format!("probe:series:{rung}");
        let probe = crate::alloc_probe::Probe::start();
        let _ = super::packed_blocks::append_timestamped_kv_blocks(
            &engine.cache,
            &engine.block_store,
            1,
            "context_summary",
            &key,
            vec![FeaturePoint { timestamp_ms: 1, value: payload.clone() }],
            0,
            false,
            // promote_sync_writes: every caller in the engine passes true, and this probe
            // exists to characterise the primitive as the engine uses it.
            true,
            // A fresh object here, so its first block is block 0.
            0,
        );
        *series_out = probe.stop().allocs;

        // The plain value append, for contrast.
        let _ = super::append_value(
            &engine.cache,
            &engine.block_store,
            1,
            &payload,
            Some(9_900_000 + rung as u64),
            None,
            Some(0),
            false,
        );
        let probe = crate::alloc_probe::Probe::start();
        let _ = super::append_value(
            &engine.cache,
            &engine.block_store,
            1,
            &payload,
            Some(9_950_000 + rung as u64),
            None,
            Some(0),
            false,
        );
        *value_out = probe.stop().allocs;
    }

    println!(
        "  append_timestamped_kv_pages    {series_small:>9}   {series_large:>10}   {:>6.2}x",
        series_large as f64 / series_small.max(1) as f64,
    );
    println!(
        "  append_value                   {value_small:>9}   {value_large:>10}   {:>6.2}x",
        value_large as f64 / value_small.max(1) as f64,
    );
    println!(
        "
  series grows, value flat => the series primitive is the cost.
  both grow                => the block-store append beneath them is.
  neither grows            => the cost is elsewhere in the command arm.
"
    );
}


/// What does post-write index maintenance cost, per key kind?
///
/// A summary write costs 1,554 allocations at 40 memories and 10,671 at 320; a node write is flat
/// at ~197. The append primitives underneath both are flat, no rebuild fires, and every counted
/// bucket walk is flat -- so the cost is in the maintenance that succeeds, not in the write and not
/// in a fallback. The two arms differ in which branch of `sync_context_blocks_for_object` they take:
/// a node's block is filed under `shard.hashes` and goes through `upsert_bucket_index_block_with`, a
/// summary is a timestamped series and goes through `sync_bucket_index_object_blocks`.
///
/// Calling maintenance directly, on the same grown stores, one key of each kind, decides it.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_post_write_maintenance_costs_per_key_kind -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_post_write_maintenance_costs_per_key_kind() {
    use crate::context_workflow::{
        ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
        ContextModelProviderConfig, ContextSourceKind,
    };

    const TENANT: u64 = 8807;

    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed -- rerun with `--features alloc-probe`"
    );
    drop(sink);

    println!(
        "
  corpus   maintained key kind      allocs   covered
"
    );

    for rung in [40_usize, 320] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..rung {
            let report = ingest_extract_context(
                &engine,
                ContextIngestExtractRequest {
                    shard_id: 1,
                    tenant_hash: TENANT,
                    sources: vec![ContextExtractRequest {
                        shard_id: 1,
                        tenant_hash: TENANT,
                        source_kind: ContextSourceKind::Incident,
                        source_id: format!("MAINT-{index:06}"),
                        title: format!("maint {index}"),
                        body: format!(
                            "{}{}",
                            format!("entry {index} covering the depot rota "),
                            "context payload sentence. ".repeat(40)
                        ),
                        timestamp_ms: 1_000 + index as u64,
                        provider: ContextModelProviderConfig::default(),
                    }],
                    provider: ContextModelProviderConfig::default(),
                    start_time_ms: 0,
                    end_time_ms: 0,
                    max_events: 0,
                    query: String::new(),
                },
            );
            assert!(report.status.ok, "grow {index}: {:?}", report.status);
        }

        // Real keys, written through the real arms, so maintenance sees exactly what it sees in
        // production rather than a key that happens to be absent.
        let node_hash = 9_800_000 + rung as u64;
        let node_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(crate::types::ContextNode {
                    node_hash,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("session/maint-{node_hash}"),
                    l0: "probe node".to_string(),
                    status: 0,
                    last_event_time_ms: 1_781_700_000_000,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: vec![0.25_f32; 16],
                    embedding_model_hash: 7,
                    embedding_updated_at_ms: 1,
                    summary_vector: vec![0.5_f32; 16],
                    summary_vector_valid_from_ms: 1_781_700_000_000,
                    summary_vector_model_hash: 7,
                }),
            },
        });
        assert!(node_write.status.ok, "{:?}", node_write.status);
        let summary_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertSummary {
                tenant_hash: TENANT,
                summary: crate::types::ContextSummary {
                    node_hash,
                    level: 2,
                    text: "a probe summary of ordinary length for one turn".to_string(),
                    valid_from_ms: 1_781_700_000_000,
                    vector: vec![0.5_f32; 16],
                    embedding_model_hash: 7,
                },
            },
        });
        assert!(summary_write.status.ok, "{:?}", summary_write.status);

        let node_key = super::context::context_node_key(TENANT, node_hash);
        let summary_key = super::context::context_summary_key(TENANT, node_hash, 2);

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("loaded shard");
        for (label, key) in [("context node   ", &node_key), ("context summary", &summary_key)] {
            // Warm once: the first maintenance of a key does one-off work.
            let _ = super::storage_bucket_internals::sync_context_blocks_for_object(
                shard, 1, key,
            );
            let probe = crate::alloc_probe::Probe::start();
            let covered = super::storage_bucket_internals::sync_context_blocks_for_object(
                shard, 1, key,
            );
            let allocs = probe.stop().allocs;
            println!("  {rung:>6}   {label}      {allocs:>9}   {covered}");
        }
    }

    println!(
        "
  summary rising while node stays flat => maintenance is the cost, and the fix belongs in the
  series branch of it. Both flat => maintenance is not the cost either, and what remains is the
  wrapper around the arm.
"
    );
}


// THREE ORACLE ARMS ARE RETIRED HERE, AND THE ORACLE IS WHAT THEY WERE.
//
// `deleting_leaves_the_lookup_a_rebuild_would_have_built` and
// `a_reload_rebuilds_the_lookup_the_index_no_longer_writes` each took the MAINTAINED object
// lookup, ran `rebuild_object_block_lookup()`, and asserted the two agree position for position.
// `part1::the_maintained_block_lookup_matches_a_rebuilt_one` did the same over a whole ingest.
// Twenty-one defects were caught by that shape across this change.
//
// THE REBUILD WALKED THE PAGES AND READ EACH ONE'S KEY, so with the identity off the entry there
// is nothing to rebuild FROM, and the oracle has one side. `rebuild_object_block_lookup` is
// deleted in `engine_storage_state.rs` with the reasoning; these three go with it rather than being rewritten to
// compare the maintained lookup with itself.
//
// WHAT THE ORACLE BOUGHT, so it outlives the assertions: it is the whole of the evidence that
// filing-as-you-go leaves the lookup exactly where a whole-shard rebuild would have, which is
// what licensed removing two rebuild calls from the load path and one from the delete path. The
// obligation is at the producers now -- every site that places a page files it, every site that
// drops one un-files it, and `fold_delta_block_items` carries the note that those are ONE
// obligation.
//
// AND THE RULE THEY WERE AN INSTANCE OF IS THE PART TO KEEP: when replacing a source, keep the
// old one alive and assert agreement until the last reader converts. These three are what that
// rule looks like when it is spent -- the old source is gone because the last reader converted,
// so the agreement is unrepeatable and the assertions come out in the same commit that removes
// the source. A tie kept past that point compares one source with itself.

/// Which structures does one record actually land in?
///
/// A record occupies about six index entries' worth of memory
/// (`how_many_map_entries_is_one_record`), so the capacity ceiling moves by holding it in fewer
/// places. That is only actionable if the places are named. This writes a known number of
/// records and counts what each structure ends up holding.
///
/// Counts, not bytes: a structure holding one entry per record is a candidate whatever its entry
/// costs, and a structure holding none is not.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib where_a_record_is_kept -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn where_a_record_is_kept() {
    const RECORDS: usize = 5_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("kept-{index:08}"),
                value: vec![b'v'; 512],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("loaded shard");

    let rows: Vec<(&str, usize)> = vec![
        ("strings", shard.strings().len()),
        ("hashes", shard.hashes.len()),
        ("sets", shard.sets.len()),
        ("expires_at_ms", shard.expires_at_ms().len()),
        ("seen", shard.seen.len()),
        ("wal_resident_pages", shard.wal_resident_blocks.len()),
        ("buckets_pending_flag_refresh", shard.buckets_pending_flag_refresh.len()),
    ];

    println!();
    println!("  {RECORDS} plain string writes land in:");
    println!("  structure                        entries   per record");
    let mut per_record_total = 0.0;
    for (name, count) in &rows {
        let per = *count as f64 / RECORDS as f64;
        per_record_total += per;
        println!("  {name:32} {count:>7} {per:>12.2}");
    }
    println!("  ------------------------------------------------------");
    println!("  {:32} {:>7} {per_record_total:>12.2}", "counted here", "");
    println!();
    println!("  A structure at 1.00 per record holds every record; one at 0.00 holds none of");
    println!("  them and is not where the memory goes. Anything the six entries' worth is made");
    println!("  of that does NOT appear above lives outside ShardState -- the bucket index and");
    println!("  the page-address maps beneath it are the next place to look.");

    assert_eq!(shard.strings().len(), RECORDS, "every write should be in strings");
}

/// How many map entries' worth of memory does one record actually occupy?
///
/// A record keeps ~1,040 bytes resident whatever kind it is
/// (`what_a_records_kind_costs_resident`), and that number sets a node's ceiling at roughly 20M
/// records on a 32 GB box. A record's primary home is one entry in `strings: HashMap<String,
/// BlockAddress>`, so this measures what ONE such entry costs on its own and divides.
///
/// A ratio near 1 would mean the entry is simply expensive and the only fix is a smaller entry.
/// A ratio well above 1 means the record is held in several places, and the fix is to hold it in
/// fewer.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib how_many_map_entries_is_one_record -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn how_many_map_entries_is_one_record() {
    use std::collections::HashMap;

    const RECORDS: u64 = 20_000;

    // A real address, produced by a real write, so the entry measured is the one the store keeps.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..RECORDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("floor-{index:08}"),
                value: vec![b'v'; 1536],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let address = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        shard.strings().values().next().map(|slot| slot.address()).expect("a written page")
    };

    // The floor: the same key and the same address, in a map of their own.
    let probe = crate::alloc_probe::Probe::start();
    let mut bare: HashMap<String, crate::block_store::ElementEntry> =
        HashMap::with_capacity(RECORDS as usize);
    for index in 0..RECORDS {
        bare.insert(format!("floor-{index:08}"), address.clone());
    }
    let counts = probe.stop();
    let bare_live = counts.alloc_bytes.saturating_sub(counts.free_bytes);
    std::hint::black_box(&bare);

    let per_entry = bare_live as f64 / RECORDS as f64;
    const RECORD_RESIDENT: f64 = 1040.0;   // measured by what_a_records_kind_costs_resident

    println!();
    println!("  one HashMap<String, ElementEntry> entry   {per_entry:>8.0} bytes");
    println!("  a record resident in the engine           {RECORD_RESIDENT:>8.0} bytes");
    println!("  ratio                                     {:>8.1}x",
             RECORD_RESIDENT / per_entry);
    println!();
    if RECORD_RESIDENT / per_entry > 1.6 {
        println!("  A record occupies several entries' worth. It is not that the entry is");
        println!("  expensive -- the record is held in more than one place, and the ceiling moves");
        println!("  by holding it in fewer rather than by shrinking any single structure.");
    } else {
        println!("  A record is about one entry. The entry itself is the cost, so the ceiling");
        println!("  moves only by making the key or the address smaller.");
    }

    assert!(bare_live > 0, "measured nothing");
}

/// How much of a record's resident cost is the core index entry, and how much is its kind?
///
/// A context node keeps ~1.34 kB resident and none of it is the payload
/// (`what_a_resident_record_is_made_of`) -- it is fixed index structure, which is the only thing
/// left that could move a node's ~20M-record ceiling. This asks which structure: a plain string
/// lands in the fewest maps a record can land in, so what a context node costs ABOVE it is what
/// the context indexes add.
///
/// Both write the same number of records with a payload of the same size, so the difference is
/// the indexing and not the data.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_records_kind_costs_resident -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_records_kind_costs_resident() {
    const RECORDS: u64 = 30_000;
    const PAYLOAD: usize = 1536;

    let engine_for = || {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        (dir, engine)
    };

    // --- a plain string: the fewest maps a record can land in ---------------------------
    let (_dir_a, engine_a) = engine_for();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let response = engine_a.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("kind-{index:08}"),
                value: vec![b'v'; PAYLOAD],
            },
        });
        assert!(response.status.ok, "string write {index}: {:?}", response.status);
    }
    let string_counts = probe.stop();
    let string_live = string_counts.alloc_bytes.saturating_sub(string_counts.free_bytes);

    // --- a context node: the same bytes, more indexes -----------------------------------
    let (_dir_b, engine_b) = engine_for();
    let vector: Vec<f32> = (0..PAYLOAD / 4).map(|i| (i as f32) * 0.001).collect();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let node = crate::types::ContextNode {
            node_hash: 1_000_000 + index,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("kind-{index:08}"),
            l0: String::new(),
            status: 0,
            last_event_time_ms: 1_780_000_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vector.clone(),
            embedding_model_hash: 909,
            embedding_updated_at_ms: 1_780_000_000_000,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let response = engine_b.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode { tenant_hash: 7, node: Box::new(node) },
        });
        assert!(response.status.ok, "node write {index}: {:?}", response.status);
    }
    let node_counts = probe.stop();
    let node_live = node_counts.alloc_bytes.saturating_sub(node_counts.free_bytes);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  record kind                 live/record   allocated/record   freed %");
    println!("  string ({PAYLOAD} B value)      {:>8.0} B  {:>15.0} B  {:>6.1}%",
             per(string_live), per(string_counts.alloc_bytes),
             100.0 * string_counts.free_bytes as f64 / string_counts.alloc_bytes as f64);
    println!("  context node ({PAYLOAD} B vec)  {:>8.0} B  {:>15.0} B  {:>6.1}%",
             per(node_live), per(node_counts.alloc_bytes),
             100.0 * node_counts.free_bytes as f64 / node_counts.alloc_bytes as f64);
    println!();
    let extra = per(node_live) - per(string_live);
    println!("  a context node keeps {extra:+.0} bytes more than a string carrying the same bytes");
    if extra > per(string_live) {
        println!("  -- the context indexes cost MORE than the core entry, so they are where the");
        println!("     ceiling is. Which of them is the next thing to split out.");
    } else {
        println!("  -- the core index entry dominates, so every record kind pays about the same");
        println!("     and the ceiling is not specific to context nodes.");
    }

    assert!(string_live > 0 && node_live > 0, "measured nothing");
}

/// What the 1.3 kB a record keeps resident is actually made of.
///
/// A node keeps 1.302 kB of live memory per record and RSS grows 1.586 kB
/// (`how_much_of_rss_per_record_is_live`), which is what limits a node to roughly 20M records on
/// a 32 GB box. Notably that is LESS than the 1,536-byte vector each record carries, so the
/// payload is not what is retained -- it goes to a block and the block is not held. What stays is
/// index structure, and this attributes it by writing the same corpus with one thing removed at
/// a time.
///
/// Differences, not absolutes: the baseline includes fixed costs that do not scale, and only
/// what separates two runs is per-record.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_a_resident_record_is_made_of -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_a_resident_record_is_made_of() {
    const RECORDS: u64 = 30_000;

    let live_for = |label: &str, dims: usize, name_len: usize, text_len: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let vector: Vec<f32> = (0..dims).map(|i| (i as f32) * 0.001).collect();

        let probe = crate::alloc_probe::Probe::start();
        for index in 0..RECORDS {
            let node = crate::types::ContextNode {
                node_hash: 1_000_000 + index,
                parent_hash: 0,
                kind: 1,
                canonical_name: format!("{:width$}", index, width = name_len),
                l0: "t".repeat(text_len),
                status: 0,
                last_event_time_ms: 1_780_000_000_000,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: vector.clone(),
                embedding_model_hash: 909,
                embedding_updated_at_ms: 1_780_000_000_000,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            };
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode { tenant_hash: 7, node: Box::new(node) },
            });
            assert!(response.status.ok, "{label} write {index}: {:?}", response.status);
        }
        let counts = probe.stop();
        let live = counts.alloc_bytes.saturating_sub(counts.free_bytes);
        println!("  {label:34} {:>9.3} kB/record live",
                 live as f64 / RECORDS as f64 / 1024.0);
        live
    };

    println!();
    println!("  variant                             live memory kept per record");
    let base = live_for("baseline: 384 dims, 40-char name", 384, 40, 42);
    let no_vector = live_for("no vector at all", 0, 40, 42);
    let short_name = live_for("4-char canonical name", 384, 4, 42);
    let no_text = live_for("no l0 text", 384, 40, 0);

    let per = |v: u64| v as f64 / RECORDS as f64;
    println!();
    println!("  removing            saves per record");
    println!("    the vector        {:>8.1} bytes", per(base) - per(no_vector));
    println!("    36 name chars     {:>8.1} bytes", per(base) - per(short_name));
    println!("    the l0 text       {:>8.1} bytes", per(base) - per(no_text));
    println!("    ---------------------------------");
    println!("    unattributed      {:>8.1} bytes",
             per(no_vector).min(per(short_name)).min(per(no_text)));
    println!();
    println!("  A vector costs 1,536 bytes on the wire. If removing it saves far less than that,");
    println!("  the resident cost is the INDEX ENTRY for the record, not its payload -- and the");
    println!("  capacity ceiling moves only by making that entry smaller.");

    assert!(base > 0, "measured nothing");
}

/// Of the RSS a node holds per record, how much is LIVE and how much is the allocator's?
///
/// A soak measured a marginal 1.561 kB of RSS per record, linear, which puts a 32 GB node out of
/// memory near 20M records. That figure only bounds capacity if it is real data. Process RSS
/// conflates three things -- memory allocated and still held, memory freed but retained by the
/// allocator, and memory that never came from the heap -- and a proxy measurement in this project
/// once found 71% of RSS was retention rather than live data.
///
/// The counting allocator knows the difference: allocated bytes minus freed bytes is what the
/// process is actually still holding. Comparing that against RSS says whether the capacity limit
/// is the data or the allocator, and those have completely different fixes.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib how_much_of_rss_per_record_is_live -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn how_much_of_rss_per_record_is_live() {
    fn rss_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("VmRSS:")
                        .and_then(|rest| rest.trim().split_whitespace().next()?.parse().ok())
                })
            })
            .unwrap_or(0)
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    const RECORDS: u64 = 60_000;
    let vector: Vec<f32> = (0..384).map(|i| (i as f32) * 0.001).collect();

    let rss_before = rss_kb();
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..RECORDS {
        let node = crate::types::ContextNode {
            node_hash: 1_000_000 + index,
            parent_hash: 0,
            kind: 1,
            canonical_name: format!("session/live-{index}"),
            l0: format!("memory {index}: the user prefers aisle seats"),
            status: 0,
            last_event_time_ms: 1_780_000_000_000,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: vector.clone(),
            embedding_model_hash: 909,
            embedding_updated_at_ms: 1_780_000_000_000,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: 7,
                node: Box::new(node),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let counts = probe.stop();
    let rss_after = rss_kb();

    let live_bytes = counts.alloc_bytes.saturating_sub(counts.free_bytes);
    let rss_bytes = (rss_after.saturating_sub(rss_before)) * 1024;
    let per = |v: u64| v as f64 / RECORDS as f64;

    println!();
    println!("  over {RECORDS} records:");
    println!("    allocated        {:>12} bytes   ({:.3} kB/record)",
             counts.alloc_bytes, per(counts.alloc_bytes) / 1024.0);
    println!("    freed            {:>12} bytes", counts.free_bytes);
    println!("    STILL LIVE       {:>12} bytes   ({:.3} kB/record)",
             live_bytes, per(live_bytes) / 1024.0);
    println!("    RSS grew by      {:>12} bytes   ({:.3} kB/record)",
             rss_bytes, per(rss_bytes) / 1024.0);
    println!();
    if rss_bytes > 0 {
        let retained = rss_bytes.saturating_sub(live_bytes);
        println!("    live is {:.0}% of the RSS growth; the other {:.0}% ({:.3} kB/record) is",
                 100.0 * live_bytes as f64 / rss_bytes as f64,
                 100.0 * retained as f64 / rss_bytes as f64,
                 per(retained) / 1024.0);
        println!("    memory the allocator kept rather than data the node holds.");
        println!();
        if retained > live_bytes {
            println!("    RETENTION DOMINATES: the capacity limit is the allocator, and an arena or a");
            println!("    periodic trim would move it further than shrinking any structure would.");
        } else {
            println!("    LIVE DATA DOMINATES: the per-record structures are the capacity limit, so");
            println!("    the only thing that moves it is holding less per record.");
        }
    }

    assert!(counts.alloc_bytes > 0, "measured nothing");
}

/// Is a put expensive because it STORES, or because it EVICTS?
///
/// Storing a 1 KB block measured 51 allocations and 43.6 KB
/// (`what_the_block_cache_costs_per_call`), which is a 43x amplification and too large to be the
/// copy. That measurement used a cache too small to hold the working set, so every put also
/// evicted. This separates the two: the same put into a cache with room to spare cannot be
/// evicting.
///
/// It matters which it is. If storing is expensive, every write to the cache costs it. If
/// EVICTING is expensive, only a cache under pressure pays -- which is every cache on a corpus
/// larger than it, but the fix is an admission policy rather than a cheaper store.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib is_a_put_expensive_to_store_or_to_evict -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn is_a_put_expensive_to_store_or_to_evict() {
    use matrixcache::CacheKey;

    const ROUNDS: u64 = 200;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    let measure = |cache_bytes: usize, label: &str| {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            cache_bytes,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let cache = &engine.cache;

        // Keys and buffers built outside the window; `put` takes both by value.
        let mut pending: Vec<(CacheKey, Vec<u8>)> = (0..ROUNDS)
            .map(|round| {
                (
                    CacheKey::page_with_slot(1, 2, round * 4096, 1024, Some(3)),
                    vec![b'p'; 1024],
                )
            })
            .collect();
        // Warm: the first put touches one-off structures.
        if let Some((key, value)) = pending.pop() {
            let _ = cache.put(key, value);
        }

        let mut allocs = 0u64;
        let mut bytes = 0u64;
        let mut n = 0u64;
        for (key, value) in pending.drain(..) {
            let probe = crate::alloc_probe::Probe::start();
            let stored = cache.put(key, value);
            let counts = probe.stop();
            assert!(stored.is_ok(), "{label}: the put must succeed");
            allocs += counts.allocs;
            bytes += counts.alloc_bytes;
            n += 1;
        }
        let per_n = |v: u64| v as f64 / n as f64;
        println!("  {label:38} {:>7.2} {:>9.0}", per_n(allocs), per_n(bytes));
        (per_n(allocs), per_n(bytes))
    };

    println!();
    println!("  cache size                              allocs     bytes   (per 1 KB put)");
    // 200 blocks of 1 KB need ~200 KB; 64 MB has room for all of them, 8 KB for none.
    let (roomy_allocs, roomy_bytes) = measure(64 * 1024 * 1024, "64 MB (room to spare, no eviction)");
    let (tight_allocs, tight_bytes) = measure(8 * 1024, "8 KB (evicts on every put)");

    println!();
    println!("  eviction adds {:+.2} allocations and {:+.0} bytes per put",
             tight_allocs - roomy_allocs, tight_bytes - roomy_bytes);
    if roomy_allocs > tight_allocs - roomy_allocs {
        println!("  STORING dominates: the cost is paid by every put, pressure or not.");
    } else {
        println!("  EVICTING dominates: a cache with room is cheap and one under pressure is not,");
        println!("  so the fix is to stop admitting pages that will be evicted before they are");
        println!("  read again, rather than to make the store itself cheaper.");
    }

    assert!(roomy_allocs > 0.0 && tight_allocs > 0.0, "measured nothing");
    let _ = (roomy_bytes, tight_bytes);
}

/// Does a missed lookup pay for tiers that hold nothing?
///
/// A cache miss costs 18 allocations (`what_the_block_cache_costs_per_call`), which contradicts
/// `get_with_tier`'s own claim that a miss "releases the lock having touched nothing". One
/// candidate: the lookup walks memory, then pmem, then SSD, and the SSD tier builds its key as a
/// String before asking -- so a tier with no capacity still costs something to consult.
///
/// `disk_cache_tier: false` zeroes the pmem and SSD capacities, which is what a one-box or raft
/// node runs with. If the miss gets cheaper, walking dead tiers is part of the 18.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib does_a_miss_pay_for_empty_tiers -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn does_a_miss_pay_for_empty_tiers() {
    use matrixcache::CacheKey;

    const ROUNDS: u64 = 200;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    let measure = |disk_cache_tier: bool, label: &str| {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
            8 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
            Default::default(),
            disk_cache_tier,
        );
        engine.load_shard(1);
        let cache = &engine.cache;

        // Keys built outside the window: this measures the lookup, not the key.
        let absent: Vec<CacheKey> = (0..ROUNDS)
            .map(|round| CacheKey::page_with_slot(1, 3, 5_000_000 + round * 4096, 1024, Some(4)))
            .collect();
        // Warm whatever one-off structures the first lookup touches.
        let _ = cache.get(&absent[0]);

        let mut allocs = 0u64;
        let mut bytes = 0u64;
        for key in &absent {
            let probe = crate::alloc_probe::Probe::start();
            let got = cache.get(key);
            let counts = probe.stop();
            assert!(matches!(got, Ok(None)), "{label}: this key must be absent");
            allocs += counts.allocs;
            bytes += counts.alloc_bytes;
        }
        println!("  {label:34} {:>7.2} {:>8.0}", per(allocs), per(bytes));
        (allocs, bytes)
    };

    println!();
    println!("  cache shape                       allocs    bytes   (per missed lookup)");
    let (with_disk, _) = measure(true, "memory + pmem + ssd tiers");
    let (memory_only, _) = measure(false, "memory only (one-box / raft)");

    println!();
    let saved = per(with_disk) - per(memory_only);
    if saved > 0.5 {
        println!("  Walking the empty disk tiers costs {saved:.2} allocations per missed lookup.");
        println!("  A one-box or raft node has those tiers switched off already, so it does not");
        println!("  pay this -- but a shared-storage node consults them on every miss.");
    } else {
        println!("  The disk tiers are not the cost: a miss allocates about the same either way,");
        println!("  so the {:.0} allocations are in the memory-tier lookup itself.", per(memory_only));
    }

    assert!(with_disk > 0, "measured nothing");
    assert!(memory_only > 0, "measured nothing");
}

/// What the CACHE costs, split into the three calls a block-read miss makes.
///
/// A miss spends ~60 allocations and ~86 KB inside the cache against 13 allocations and 1.5 KB
/// actually reading the block (`where_a_block_read_miss_allocates`). That is the read path's real
/// memory cost, and 60 is too many to guess at: this splits it into building the key, the lookup
/// that misses, and the put that stores the block.
///
/// Keys and buffers are built BEFORE the measured window -- `put` takes both by value, and
/// counting their construction would attribute the caller's work to the cache.
///
///   cargo test --features alloc-probe -p temporalstore-rust --lib what_the_block_cache_costs_per_call -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_the_block_cache_costs_per_call() {
    use matrixcache::CacheKey;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let cache = &engine.cache;

    const ROUNDS: u64 = 100;
    let per = |n: u64| n as f64 / ROUNDS as f64;

    // --- building the key -----------------------------------------------------------------
    let mut key_allocs = 0u64;
    let mut key_bytes = 0u64;
    for round in 0..ROUNDS {
        let probe = crate::alloc_probe::Probe::start();
        let key = CacheKey::page_with_slot(1, 0, round * 4096, 1024, Some(7));
        let counts = probe.stop();
        std::hint::black_box(&key);
        key_allocs += counts.allocs;
        key_bytes += counts.alloc_bytes;
    }

    // --- a lookup that misses -------------------------------------------------------------
    let absent: Vec<CacheKey> = (0..ROUNDS)
        .map(|round| CacheKey::page_with_slot(1, 9, 1_000_000 + round * 4096, 1024, Some(7)))
        .collect();
    let mut miss_allocs = 0u64;
    let mut miss_bytes = 0u64;
    for key in &absent {
        let probe = crate::alloc_probe::Probe::start();
        let got = cache.get(key);
        let counts = probe.stop();
        assert!(matches!(got, Ok(None)), "this key must not be present");
        miss_allocs += counts.allocs;
        miss_bytes += counts.alloc_bytes;
