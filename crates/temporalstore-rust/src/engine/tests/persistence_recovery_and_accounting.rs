// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 4 of engine tests, split from engine/engine_regression_tests.rs.
#![allow(clippy::all)]
use super::*;

/// What one read allocates, on a cache HIT and on a MISS.
///
/// The append path has been measured and cut three times this week -- 31 allocations to 4. The
/// read path has never been measured. It goes through `cached_response(cache,
/// CacheKey::string(shard_id, &key), ..)`, so every read builds a cache key whether or not the
/// answer is cached, and a hit still pays for whatever the key costs.
///
/// Separates the two because they have different fixes: a hit that allocates is the key and the
/// response, a miss is the block read underneath.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn what_one_read_allocates() {
    for value_len in [64usize, 1024, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let keys = 200usize;
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tenant/7/object/{index:09}"),
                    value: vec![118u8; value_len],
                },
            });
        }

        // WARM: read every key once so the cache holds them, then measure re-reads.
        for index in 0..keys {
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: format!("tenant/7/object/{index:09}") },
            });
        }
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..keys {
            let answer = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: format!("tenant/7/object/{index:09}") },
            });
            std::hint::black_box(&answer);
        }
        let hit = probe.stop();

        println!(
            "  READ value {value_len:>5}B | warm hit {:>6.1} allocs {:>8.0} B per read ({:>5.2}x the value)",
            hit.allocs as f64 / keys as f64,
            hit.alloc_bytes as f64 / keys as f64,
            hit.alloc_bytes as f64 / keys as f64 / value_len as f64,
        );

        // A read that allocated nothing would make every figure above read as free.
        assert!(hit.allocs > 0, "the probe must observe the reads at {value_len}B");
    }
}

/// Does the read cache actually hit?
///
/// A warm re-read measured 15 allocations and 2,788 bytes at a 64 byte value, and 19 and 38,308
/// at a kilobyte -- 37 times the value, to serve something already cached. That is not the shape
/// of a hit, so the first question is whether it IS one.
///
/// Compares the FIRST read of a key against the SECOND. If the cache serves the second, the two
/// differ sharply. If they do not, the cache is not answering and the "warm" figure above is
/// really the cost of a miss.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn does_the_read_cache_actually_hit() {
    for value_len in [64usize, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let keys = 200usize;
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tenant/7/object/{index:09}"),
                    value: vec![118u8; value_len],
                },
            });
        }

        let read_once = |engine: &TemporalEngine, index: usize| {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: format!("tenant/7/object/{index:09}") },
            })
        };

        // FIRST touch of each key.
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..keys {
            std::hint::black_box(read_once(&engine, index));
        }
        let first = probe.stop();

        // SECOND touch of the same keys, in the same order.
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..keys {
            std::hint::black_box(read_once(&engine, index));
        }
        let second = probe.stop();

        // And a THIRD, to see whether it is still falling or has settled.
        let probe = crate::alloc_probe::Probe::start();
        for index in 0..keys {
            std::hint::black_box(read_once(&engine, index));
        }
        let third = probe.stop();

        println!(
            "  HITMISS value {value_len:>5}B | 1st {:>5.1} allocs {:>8.0} B | 2nd {:>5.1} {:>8.0} B | 3rd {:>5.1} {:>8.0} B",
            first.allocs as f64 / keys as f64, first.alloc_bytes as f64 / keys as f64,
            second.allocs as f64 / keys as f64, second.alloc_bytes as f64 / keys as f64,
            third.allocs as f64 / keys as f64, third.alloc_bytes as f64 / keys as f64,
        );
        let saved = 100.0 * (first.alloc_bytes as f64 - second.alloc_bytes as f64)
            / first.alloc_bytes.max(1) as f64;
        println!("      the second pass costs {saved:.1}% fewer bytes than the first");

        assert!(first.allocs > 0, "the probe must observe the first pass");
        assert!(second.allocs > 0, "the probe must observe the second pass");
    }
}

/// What the read cache's codec costs, and whether a binary one can replace it.
///
/// `cached_response` stores each answer as JSON and re-parses it on every hit. `Vec<u8>` in JSON
/// is an ARRAY OF DECIMAL NUMBERS -- about four characters a byte -- so a cached kilobyte becomes
/// roughly four, and a hit pays to parse it back. That is the constant ~37x a warm read allocates:
/// it does not scale with the value, it scales with the encoding.
///
/// The entries are `put_memory_only`, so nothing on disk depends on the choice. The question is
/// whether msgpack can carry `CommandResponse` at all: it is an INTERNALLY TAGGED enum
/// (`#[serde(tag = "kind")]`), and that representation needs a self-describing deserialiser.
/// Answer it here rather than in a refactor.
#[test]
#[ignore]
fn what_the_read_cache_codec_costs() {
    for value_len in [64usize, 1024, 4096] {
        let response = crate::types::CommandResponse::Bytes {
            value: Some(vec![118u8; value_len]),
        };

        let as_json = serde_json::to_vec(&response).expect("json encode");
        let json_round: crate::types::CommandResponse =
            serde_json::from_slice(&as_json).expect("json decode");
        assert!(matches!(json_round, crate::types::CommandResponse::Bytes { .. }));

        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        let packable = serde::Serialize::serialize(&response, &mut serializer).is_ok();

        let decodes = if packable {
            rmp_serde::from_slice::<crate::types::CommandResponse>(&packed).is_ok()
        } else {
            false
        };

        println!(
            "  CODEC value {value_len:>5}B | json {:>8} B ({:>5.1}x the value) | msgpack {:>8} B ({:>5.1}x) | encodes {packable} decodes {decodes}",
            as_json.len(),
            as_json.len() as f64 / value_len as f64,
            if packable { packed.len() } else { 0 },
            if packable { packed.len() as f64 / value_len as f64 } else { 0.0 },
        );

        assert!(!as_json.is_empty(), "the probe must encode something at {value_len}B");
    }
}

/// Where a warm read's remaining allocations go.
///
/// Caching the answer packed rather than as text took a warm read from 15 allocations to 8. The
/// convenient story about the rest is that it all lives inside the cache crate -- convenient
/// because that crate is a pinned dependency. Worth splitting rather than assuming: anything on
/// this side of the boundary is ours.
///
/// Decodes a stored answer directly, which is the same call a hit makes, and compares that against
/// the whole read. What is left over is the plumbing around the codec.
#[test]
#[ignore]
#[cfg(feature = "alloc-probe")]
fn where_a_warm_reads_allocations_go() {
    for value_len in [64usize, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let keys = 200usize;
        let names: Vec<String> =
            (0..keys).map(|index| format!("tenant/7/object/{index:09}")).collect();
        for name in &names {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: name.clone(), value: vec![118u8; value_len] },
            });
        }
        // Warm the cache, so what follows measures hits.
        for name in &names {
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: name.clone() },
            });
        }

        // What one stored answer costs to decode: the same shape the cache stores.
        let stored = crate::engine::command_validation::encode_cached_response(
            &crate::types::CommandResponse::Bytes { value: Some(vec![118u8; value_len]) },
        )
        .expect("encode");

        let probe = crate::alloc_probe::Probe::start();
        for _ in 0..keys {
            std::hint::black_box(crate::engine::command_validation::decode_cached_response(
                &stored,
            ));
        }
        let decoding = probe.stop();

        // The whole read, key building and cache lookup included.
        let probe = crate::alloc_probe::Probe::start();
        for name in &names {
            std::hint::black_box(engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: name.clone() },
            }));
        }
        let whole = probe.stop();

        let split = |counts: &crate::alloc_probe::AllocCounts| {
            (counts.allocs as f64 / keys as f64, counts.alloc_bytes as f64 / keys as f64)
        };
        let (da, db) = split(&decoding);
        let (wa, wb) = split(&whole);
        println!(
            "  SPLITREAD value {value_len:>5}B | stored {:>6} B | decode {da:>5.1} allocs {db:>8.0} B | whole read {wa:>5.1} allocs {wb:>8.0} B | everything else {:>5.1} allocs {:>8.0} B ({:>4.0}% of allocs)",
            stored.len(),
            wa - da,
            wb - db,
            if wa > 0.0 { (wa - da) / wa * 100.0 } else { 0.0 },
        );

        assert!(whole.allocs > 0, "the probe must observe the reads at {value_len}B");
    }
}

/// A cached answer reads back as itself, whichever shape it is.
///
/// The read cache writes the three payload-carrying shapes as their bytes and everything else
/// through msgpack. Both halves are exercised here, along with the shapes that have an easy
/// off-by-one in them -- an absent value, an empty payload, an empty collection, and a `Values`
/// run that mixes present and absent -- because the framing is hand-written.
#[test]
fn a_cached_answer_reads_back_as_itself() {
    use crate::engine::command_validation::{decode_cached_response, encode_cached_response};
    use crate::types::CommandResponse;

    let cases = vec![
        ("bytes absent", CommandResponse::Bytes { value: None }),
        ("bytes empty", CommandResponse::Bytes { value: Some(Vec::new()) }),
        ("bytes small", CommandResponse::Bytes { value: Some(b"a value".to_vec()) }),
        ("bytes large", CommandResponse::Bytes { value: Some(vec![118u8; 8192]) }),
        ("bytes with a tag byte in it", CommandResponse::Bytes { value: Some(vec![0, 1, 2, 3, 4]) }),
        ("members none", CommandResponse::Members { members: Vec::new() }),
        (
            "members several",
            CommandResponse::Members {
                members: vec![Vec::new(), b"one".to_vec(), vec![7u8; 4096]],
            },
        ),
        ("values none", CommandResponse::Values { values: Vec::new() }),
        (
            "values mixed",
            CommandResponse::Values {
                values: vec![None, Some(Vec::new()), Some(b"three".to_vec()), None],
            },
        ),
        ("empty", CommandResponse::Empty),
        ("integer", CommandResponse::Integer { value: -9_007_199_254_740_993 }),
        (
            "hash entries, through the packed fallback",
            CommandResponse::HashEntries {
                entries: vec![("field".to_string(), vec![9u8; 32])],
            },
        ),
    ];

    for (label, response) in cases {
        let encoded = encode_cached_response(&response).unwrap_or_else(|| {
            panic!("{label} must encode");
        });
        let decoded = decode_cached_response(&encoded)
            .unwrap_or_else(|| panic!("{label} must decode back"));
        assert_eq!(
            format!("{decoded:?}"),
            format!("{response:?}"),
            "{label} did not survive the cache codec"
        );

        // A truncated entry is a miss, not a panic and not a different answer. The cache drops
        // whatever fails to decode and recomputes, so this is the path an interrupted entry takes.
        for cut in [0usize, 1, encoded.len().saturating_sub(1)] {
            if cut < encoded.len() {
                let short = decode_cached_response(&encoded[..cut]);
                if let Some(short) = short {
                    assert_ne!(
                        format!("{short:?}"),
                        format!("{response:?}"),
                        "{label} cut to {cut} bytes must not read back as the whole answer"
                    );
                }
            }
        }
    }

    // Bytes that are not a stored answer read as a miss rather than as a wrong answer.
    assert!(decode_cached_response(&[]).is_none(), "an empty entry must not decode");
    assert!(decode_cached_response(&[0xc1, 0xff, 0x00]).is_none(), "junk must not decode");

    // What is STORED must not drift. The cache admits and evicts these entries by size, so an
    // encoding change is a behaviour change: it decides which answers survive pressure, not just
    // how they are written. Four tests around eviction and refill turn on exactly that.
    let response = CommandResponse::Bytes { value: Some(b"a value".to_vec()) };
    let packed_directly = {
        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(&response, &mut serializer).expect("pack");
        packed
    };
    assert_eq!(
        encode_cached_response(&response).expect("encode"),
        packed_directly,
        "a stored answer must stay the bytes it has always been"
    );
}

/// A batch is written as ONE record, and that is worth pinning.
///
/// The log can take a batch two ways: N records, one per command, or one record carrying every
/// item. The second amortises the envelope, measured at 16.9% for eight items and 19.0% for
/// sixty-four with compression off, so the figure is the envelope rather than the compressor.
///
/// `batch_execute` already chooses the one-record form when the items can be found again after a
/// crash, and nothing asserted it. That is how this kind of saving disappears: the condition
/// (`wal_data_only_enabled() && !staged_outcomes.is_empty() && recoverable`) has three terms, any
/// of which can quietly become false -- a flag default flips, a path stops staging outcomes -- and
/// the batch goes back to N records with every test still green.
///
/// Counted through the log rather than by reading the file: records are length-framed, so counting
/// newlines counts nothing -- a first attempt at this reported 7 for both forms -- and
/// `info().length_bytes` is the preallocated file length rather than what the records occupy.
#[test]
fn a_batch_is_written_as_one_record() {
    let items = 16usize;
    let commands = |prefix: &str| -> Vec<Command> {
        (0..items)
            .map(|index| Command::StringSet {
                key: format!("{prefix}/{index:06}"),
                value: vec![118u8; 256],
            })
            .collect()
    };
    let records_in = |dir: &std::path::Path| -> usize {
        crate::wal::LocalWriteAheadLogStore::new(dir.join("indexes").join("wals"))
            .scan(1, 0, u64::MAX, u64::MAX)
            .map(|rows| rows.len())
            .unwrap_or(0)
    };
    let engine_in = |dir: &std::path::Path| {
        let engine = TemporalEngine::with_local_dirs(
            32 * 1024 * 1024,
            dir.join("cache"),
            dir.join("blocks"),
            dir.join("indexes"),
        );
        engine.load_shard(1);
        engine
    };

    let single_dir = tempfile::tempdir().unwrap();
    let engine = engine_in(single_dir.path());
    for command in commands("single") {
        engine.execute(ExecuteRequest { shard_id: 1, command });
    }
    let one_at_a_time = records_in(single_dir.path());

    let batch_dir = tempfile::tempdir().unwrap();
    let engine = engine_in(batch_dir.path());
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: commands("batch"),
    });
    assert!(response.status.ok, "the batch must succeed: {:?}", response.status);
    let as_a_batch = records_in(batch_dir.path());

    println!(
        "  BATCHFORM {items} commands | one at a time: {one_at_a_time} record(s) | as a batch: {as_a_batch} record(s)"
    );

    // The positive control: written separately they must be a record each, or "one record for the
    // batch" would hold for an uninteresting reason.
    assert_eq!(
        one_at_a_time, items,
        "written one at a time, each command should be its own record"
    );
    assert_eq!(
        as_a_batch, 1,
        "a batch should be one record carrying every item, not {items} of them"
    );
}

#[test]
fn object_manager_runtime_report_tracks_residency_layout_and_delete_markers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });

    for command in [
        Command::StringSet {
            key: "object-a".to_string(),
            value: b"a".to_vec(),
        },
        Command::HashSet {
            key: "hash-a".to_string(),
            field: "field-a".to_string(),
            value: b"field".to_vec(),
        },
        Command::FeatureReplace {
            key: "feature-a".to_string(),
            start_ms: 0,
            end_ms: 100,
            points: vec![FeaturePoint {
                timestamp_ms: 42,
                value: b"42".to_vec(),
            }],
        },
        Command::StringSet {
            key: "object-delete".to_string(),
            value: b"delete".to_vec(),
        },
        Command::CommonDelete {
            key: "object-delete".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }

    let report = engine.object_manager_runtime_report(1);
    assert!(report.runtime_ready, "{report:?}");
    assert!(report.routing_bucket_count >= 1);
    assert!(report.object_count >= 4);
    assert!(report.block_ref_count >= 3);
    // Freshly written, materialized, in-memory blocks are hot (not log-backed), so
    // their objects are hot; cold residency only applies to reloaded-from-disk blocks.
    assert!(report.hot_object_count >= 3);
    assert!(report.delete_marker_object_count >= 1);
    assert!(report.dirty_object_count >= 4);
    assert!(report.dirty_bucket_count >= 1);
    assert!(report.max_dirty_generation >= 1);
    assert!(report.object_block_count >= 2);
    assert_eq!(report.missing_owner_block_ref_count, 0);
    assert_eq!(report.owner_mismatch_block_ref_count, 0);
    assert_eq!(report.reused_object_id_conflict_count, 0);
    assert!(report.blockers.is_empty());
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("hot/cold/tombstone object state")));

    let compaction = engine.compact_shard_blocks(1).unwrap();
    assert!(compaction.model_layout_compaction_ready, "{compaction:?}");
    let after_compaction = engine.object_manager_runtime_report(1);
    assert!(after_compaction.runtime_ready, "{after_compaction:?}");
    assert!(after_compaction.object_count >= report.object_count.saturating_sub(1));
    assert_eq!(after_compaction.missing_owner_block_ref_count, 0);
    assert_eq!(after_compaction.owner_mismatch_block_ref_count, 0);
    assert!(after_compaction.object_block_count >= 1);

    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("slot dump manifest should persist");
    engine
        .install_bucket_dump_manifest(&manifest)
        .expect("slot dump manifest should install");
    let after_dump_load = engine.object_manager_runtime_report(1);
    assert!(after_dump_load.runtime_ready, "{after_dump_load:?}");
    assert_eq!(after_dump_load.object_count, after_compaction.object_count);
    assert_eq!(
        after_dump_load.delete_marker_object_count,
        after_compaction.delete_marker_object_count
    );

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    let reloaded = engine.object_manager_runtime_report(1);
    assert!(reloaded.runtime_ready, "{reloaded:?}");
    assert_eq!(reloaded.object_count, after_dump_load.object_count);
    assert_eq!(reloaded.block_ref_count, after_dump_load.block_ref_count);
    assert_eq!(
        reloaded.delete_marker_object_count,
        after_dump_load.delete_marker_object_count
    );
    assert_eq!(
        reloaded.object_block_count,
        after_dump_load.object_block_count
    );
}

// shared-corpus: native_storage_object_page_bucket_parity_surfaces;
#[test]
fn object_manager_runtime_report_tracks_residency_layout_and_delete_markers_parity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });

    for command in [
        Command::StringSet {
            key: "object-a".to_string(),
            value: b"a".to_vec(),
        },
        Command::HashSet {
            key: "hash-a".to_string(),
            field: "field-a".to_string(),
            value: b"field".to_vec(),
        },
        Command::FeatureReplace {
            key: "feature-a".to_string(),
            start_ms: 0,
            end_ms: 100,
            points: vec![FeaturePoint {
                timestamp_ms: 42,
                value: b"42".to_vec(),
            }],
        },
        Command::StringSet {
            key: "object-delete".to_string(),
            value: b"delete".to_vec(),
        },
        Command::CommonDelete {
            key: "object-delete".to_string(),
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }

    let report = engine.object_manager_runtime_report(1);
    assert!(report.runtime_ready, "{report:?}");
    assert!(report.routing_bucket_count >= 1);
    assert!(report.object_count >= 4);
    assert!(report.block_ref_count >= 3);
    // Freshly written, materialized, in-memory blocks are hot (not log-backed), so
    // their objects are hot; cold residency only applies to reloaded-from-disk blocks.
    assert!(report.hot_object_count >= 3);
    assert!(report.delete_marker_object_count >= 1);
    assert!(report.dirty_object_count >= 4);
    assert!(report.dirty_bucket_count >= 1);
    assert!(report.max_dirty_generation >= 1);
    assert!(report.object_block_count >= 2);
    assert!(report.packed_timestamped_block_count >= 1);
    assert_eq!(report.missing_owner_block_ref_count, 0);
    assert_eq!(report.owner_mismatch_block_ref_count, 0);
    assert_eq!(report.reused_object_id_conflict_count, 0);
    assert!(report.blockers.is_empty());
    assert!(report
        .evidence
        .iter()
        .any(|item| item.contains("hot/cold/tombstone object state")));

    engine.unload_shard(1);
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 10,
        end_routing_bucket: 12,
        readonly: false,
        table_name: String::new(),
    });
    let reloaded = engine.object_manager_runtime_report(1);
    assert!(reloaded.runtime_ready, "{reloaded:?}");
    assert_eq!(reloaded.object_count, report.object_count);
    assert_eq!(reloaded.block_ref_count, report.block_ref_count);
    assert_eq!(
        reloaded.delete_marker_object_count,
        report.delete_marker_object_count
    );
    assert_eq!(
        reloaded.packed_timestamped_block_count,
        report.packed_timestamped_block_count
    );
}

#[test]
fn bucket_dump_manifest_validation_rejects_checksum_and_missing_slabs() {
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
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let mut manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    manifest.logical_bytes = manifest.logical_bytes.saturating_add(1);
    assert!(
        !engine
            .validate_bucket_dump_manifest(&manifest)
            .unwrap_err()
            .ok
    );

    let mut missing = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    missing.block_slab_ids.push(999_999);
    missing.checksum = bucket_dump_manifest_checksum(&missing).unwrap();
    let missing_preflight = engine.bucket_dump_install_preflight_report(&missing);
    assert!(!missing_preflight.install_safe);
    assert_eq!(missing_preflight.missing_block_slab_ids, vec![999_999]);
    assert!(missing_preflight
        .blockers
        .contains(&"missing_block_segments".to_string()));
    assert!(!engine.validate_bucket_dump_manifest(&missing).unwrap_err().ok);

    let mut incomplete = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    incomplete.block_slab_ids.clear();
    incomplete.checksum = bucket_dump_manifest_checksum(&incomplete).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&incomplete)
            .unwrap_err()
            .code,
        "slot_dump_block_segment_mismatch"
    );

    let corrupt = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    let slab_id = corrupt.block_slab_ids[0];
    let mut slab = engine.block_store().read_slab(slab_id).unwrap();
    *slab.last_mut().unwrap() ^= 0xff;
    let _ = engine.block_store().install_slab(slab_id, &slab);
    let corrupt_preflight = engine.bucket_dump_install_preflight_report(&corrupt);
    assert!(!corrupt_preflight.install_safe);
    assert!(corrupt_preflight
        .corrupt_block_slab_ids
        .contains(&slab_id));
    assert!(corrupt_preflight.unreadable_block_ref_count > 0);
    assert!(corrupt_preflight.unreadable_block_bytes > 0);
    assert!(corrupt_preflight
        .blockers
        .contains(&"unreadable_page_refs".to_string()));
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&corrupt)
            .unwrap_err()
            .code,
        "slot_dump_unreadable_block_refs"
    );
}

#[test]
fn bucket_dump_manifest_watermark_tracks_embedded_index_not_wal_tail() {
    // Regression for silent data loss. create_bucket_dump_manifest embeds the on-disk index, but
    // under MATRIXARK_BULK_INGEST that index lags the WAL tail (per-command index persist is a
    // no-op in bulk mode). It used to stamp the manifest's wal_sequence from the LIVE WAL tail,
    // so on reload install set replay_watermark past records the embedded index never captured
    // and WAL replay skipped them -> gone. couples the two: Load replays from the DumpedLogId
    // stored inside the dumped index. We reproduce the same tail-ahead-of-index
    // divergence deterministically (no bulk env) by appending straight to the WAL.
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(1);
    // Applied + persisted normally: on-disk index anchored to WAL seq 1, containing k1.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    // Advance the WAL tail past the persisted index anchor WITHOUT touching the on-disk index --
    // exactly the state bulk mode produces (deferred index persist, live WAL).
    engine
        .write_ahead_log_store()
        .append_with_sync(
            1,
            Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
            true,
        )
        .expect("direct wal append");
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    assert_eq!(
        manifest.wal_sequence, 1,
        "manifest watermark must track the embedded index anchor (1), not the live WAL tail (2)"
    );

    // Fresh engine over the SAME pages+index dirs: load installs the manifest + replays the WAL.
    let reloaded =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    reloaded.load_shard(1);
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k2".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "a WAL record past the embedded index anchor must be replayed on reload, not skipped"
    );
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "k1".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        },
    );
}

#[test]
fn reversed_range_bounds_return_empty_not_a_lock_poisoning_panic() {
    // BTreeMap::range PANICS when start > end, and range queries run under the shard write lock,
    // so a reversed-bounds query would poison the lock and take the whole shard down (every later
    // lock().expect() panics). RangeGet returns an empty result with OK for min > max. Assert
    // reversed bounds return empty AND leave the engine usable.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 20,
                value: b"x".to_vec(),
            }],
        },
    });
    let feat = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "f".to_string(),
            start_ms: 100,
            end_ms: 50,
            count: None,
        },
    });
    assert_eq!(
        feat.response,
        CommandResponse::FeaturePoints { points: vec![] },
        "a reversed-bounds feature range must return empty, not panic"
    );
    // The shard lock must not be poisoned: the engine stays usable.
    let ok = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"1".to_vec(),
        },
    });
    assert!(
        ok.status.ok,
        "engine must stay usable after a reversed-bounds query (lock not poisoned): {:?}",
        ok.status
    );
}

#[test]
fn batch_execute_on_unloaded_shard_returns_a_batch_level_topology_error() {
    // returns a batch-level topology error with ZERO response entries when the partition is
    // not loaded, so the topology-retryable client refreshes + retries.
    // Rust previously returned an OK batch full of per-command shard_not_loaded errors, leaving the
    // batch-level status ok, so the client (which keys retry on the batch status) never refreshed.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    // Shard 1 is deliberately NOT loaded.
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"2".to_vec(),
            },
        ],
    });
    assert_eq!(
        response.status.code, "shard_not_loaded",
        "an unloaded-shard batch must fail at the batch level, got {:?}",
        response.status
    );
    assert!(
        response.responses.is_empty(),
        "a batch-level topology failure must carry zero per-command responses, got {}",
        response.responses.len()
    );
}

#[test]
fn dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id() {
    // The WAL-reclaim routine dumps dirty buckets oldest-first (by first-dirty-log-id).
    // Rust selected dirty buckets by ascending routing_bucket id then
    // truncated to the per-round cap, so a high-id bucket dirtied once was starved forever by
    // low-id buckets re-dirtied every round. The fix orders by `first_dirty_wal_sequence` -- the
    // bucket's oldest undumped write, which is what actually holds the log -- with the SUMMARY's
    // `last_dump_sequence` as the tiebreaker behind it. That summary field is filled from the
    // newest dump manifest (`merge_last_dump_sequence`), not from the bucket node, which carries no
    // such figure; 0 means "the newest dump does not cover this bucket". Here we dump the LOW-id
    // bucket (so the newest manifest names it) then re-dirty it; a cap-1 plan must now pick the
    // never-dumped HIGH-id bucket, not the low-id one.
    use std::collections::BTreeMap;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Find one key in each of two distinct routing buckets.
    let mut key_by_bucket: BTreeMap<u32, String> = BTreeMap::new();
    for i in 0..200 {
        let key = format!("k{i}");
        let bucket = engine.routing_bucket_for_key(1, &key);
        key_by_bucket.entry(bucket).or_insert(key);
        if key_by_bucket.len() >= 2 {
            break;
        }
    }
    assert!(key_by_bucket.len() >= 2, "need two distinct routing buckets");
    let mut buckets = key_by_bucket.into_iter();
    let (low_bucket, low_key) = buckets.next().unwrap();
    let (high_bucket, high_key) = buckets.next().unwrap();

    for key in [&low_key, &high_key] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: b"v1".to_vec(),
            },
        });
    }
    // Dump ONLY the low-id bucket: the newest manifest then names it, which is what the summary's
    // last_dump_sequence reports, and its dirty flag is cleared.
    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![low_bucket],
        ..Default::default()
    });
    // Re-dirty the low-id bucket so both buckets are dirty, but low was just dumped.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: low_key.clone(),
            value: b"v2".to_vec(),
        },
    });

    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 1,
        ..Default::default()
    });
    assert_eq!(
        plan.selected_dump_buckets,
        vec![high_bucket],
        "the overdue never-dumped bucket ({high_bucket}) must be selected before the just-dumped \
         low-id bucket ({low_bucket}) under the per-round cap"
    );
}

#[test]
fn delete_drop_eviction_emits_a_wal_delete_marker_and_does_not_resurrect() {
    // A delete_drop eviction is a logical delete and must emit a WAL tombstone + advance the
    // replay anchor like the expiry sweep, or the deletion is unreplicated and (under bulk mode)
    // resurrects on reload. Assert the eviction appends a CommonDelete to the WAL (observable as a
    // WAL sequence advance) and that a reload -- which replays the WAL past the anchor -- does not
    // bring the key back.
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "evict-me".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let wal_before = engine.write_ahead_log_store().stats(1).last_sequence;
    // memory_pressure_threshold = 0 forces the eviction to run; delete_drop = true.
    let report = engine.apply_storage_eviction(1, 0, 1024, false, true);
    assert!(
        report.dropped_object_count >= 1,
        "delete_drop eviction should have dropped the record: {report:?}"
    );
    let wal_after = engine.write_ahead_log_store().stats(1).last_sequence;
    assert!(
        wal_after > wal_before,
        "delete_drop must emit a WAL tombstone (sequence must advance): {wal_before} -> {wal_after}"
    );

    // Reload over the same dirs: WAL replay past the anchor must NOT resurrect the deleted key.
    let reloaded =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &blocks, &indexes);
    reloaded.load_shard(1);
    assert_eq!(
        reloaded
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "evict-me".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes { value: None },
        "a delete_drop-evicted key must stay deleted after reload, not resurrect"
    );
}

#[test]
fn partial_compaction_failure_durably_persists_the_consistent_partial_index() {
    // CP4 regression. A mid-compaction read failure used to return with the in-memory index
    // half-advanced (relocated blocks point at the fresh slab) but UNPERSISTED, so it diverged from
    // the on-disk index -- and the independent reclaim path could then physically purge a
    // fully-vacated old slab the durable index still referenced -> silent data loss on reload. The
    // fix durably commits the consistent partial state before propagating the error. avoids the
    // desync structurally (the compactor leaves the index unchanged on failure + one atomic commit).
    //
    // Setup: k1 (string, relocated FIRST by compact_shard_blocks) lives in an earlier slab; k2 (hash,
    // relocated after) lives in a later slab. We corrupt ONLY the latest slab (k2's), so compaction
    // relocates k1 (vacating its old slab) and then FAILS reading k2. A tiny cache forces the reads
    // to hit disk. Assert the on-disk index CHANGED -- i.e. the relocated partial state was durably
    // persisted (it stays byte-identical without the fix).
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(16, dir.path().join("cache"), &blocks, &indexes);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    // Force k2 into a later slab than k1 so corrupting it cannot touch k1's slab.
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v2".to_vec(),
        },
    });

    // On the delta served-index path the per-write base rewrite is deferred, so the base file
    // may not exist yet before the first compaction (the served index is the live shard; a
    // compaction is what materializes the base). Tolerate an absent base here: the assertion
    // below still holds -- compaction must durably write the relocated partial index, so
    // `index_after` differs from an absent/empty `index_before` exactly as it differs from a
    // per-write-persisted one on the default path.
    let index_before = fs::read(engine.index_path(1)).unwrap_or_default();

    // Corrupt only the newest block slab (k2's): truncate it to empty so k2's block cannot be read.
    let mut slabs = fs::read_dir(&blocks)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("block_segment_") && name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    slabs.sort();
    // A non-zero denominator. The filter above names a file ON DISK; if that name moves it
    // matches nothing, and everything below would run over an empty set.
    assert!(
        slabs.len() >= 2,
        "expected at least two slab files under {} after the roll, found {}",
        blocks.display(),
        slabs.len()
    );
    let k2_slab = slabs.last().expect("at least two page slabs after the roll").clone();
    fs::write(&k2_slab, b"").unwrap();

    let result = engine.compact_shard_blocks(1);
    assert!(
        result.is_err(),
        "compaction must fail when a live page's slab is unreadable, got {result:?}"
    );
    let index_after = fs::read(engine.index_path(1)).unwrap();
    assert_ne!(
        index_before, index_after,
        "a partial-failure compaction must durably persist the relocated partial index (CP4), \
         so the volatile and on-disk indexes cannot diverge and let reclaim purge a needed slab"
    );
}

#[test]
fn sync_write_surfaces_wal_commit_failure_instead_of_acking_ok() {
    // Durability conformance: a synchronous write whose durable WAL commit fails must NOT be acked ok.
    // The WAL is the recovery source of truth, so a swallowed append error would tell the client a
    // write that is gone after a crash succeeded. surfaces the wal Commit failure
    // We inject the failure by replacing the WAL file with a
    // directory so the next durable append cannot open it for writing (EISDIR fails even for
    // root, unlike a chmod which root bypasses).
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        &indexes,
    );
    engine.load_shard(1);
    let baseline = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    assert!(
        baseline.status.ok,
        "baseline write should succeed: {:?}",
        baseline.status
    );
    let wal_path = indexes.join("wals").join("shard-1.wal.bin");
    fs::remove_file(&wal_path).unwrap();
    fs::create_dir(&wal_path).unwrap();
    let failed = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        },
    });
    assert_eq!(
        failed.status.code, "wal_commit_failed",
        "a sync write whose durable WAL commit failed must return an error, got {:?}",
        failed.status
    );
}

#[test]
fn bucket_dump_manifest_install_restores_index_and_rejects_partial_or_stale() {
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
            key: "restore-me".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("blocks"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    let safe_preflight = restore_engine.bucket_dump_install_preflight_report(&manifest);
    assert!(safe_preflight.install_safe, "{safe_preflight:?}");
    assert!(safe_preflight.blockers.is_empty());
    assert_eq!(
        safe_preflight.manifest_index_log_sequence,
        manifest.index_log_sequence
    );
    restore_engine
        .install_bucket_dump_manifest(&manifest)
        .expect("manifest should install");
    assert!(
        fs::read_dir(dir.path().join("restore-indexes"))
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp-")),
        "slot dump install should not leave atomic index temp files"
    );
    assert!(restore_engine.interrupted_bucket_dump_installs(1).is_empty());
    let markers = list_bucket_dump_install_markers_at(&restore_engine.index_dir, 1).unwrap();
    assert!(markers.iter().any(|marker| marker.phase == "prepare"));
    assert!(markers.iter().any(|marker| marker.phase == "install"));
    assert!(markers.iter().any(|marker| marker.phase == "commit"));
    let response = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "restore-me".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );

    let mut partial = manifest.clone();
    partial.index_bytes.clear();
    partial.checksum = bucket_dump_manifest_checksum(&partial).unwrap();
    assert_eq!(
        restore_engine
            .install_bucket_dump_manifest(&partial)
            .unwrap_err()
            .code,
        "slot_dump_partial_manifest"
    );

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "newer".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.bucket_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe);
    assert!(stale_preflight.stale_manifest);
    assert!(stale_preflight
        .blockers
        .contains(&"stale_manifest_sequence".to_string()));
    assert_eq!(
        engine
            .install_bucket_dump_manifest(&manifest)
            .unwrap_err()
            .code,
        "slot_dump_stale_manifest"
    );
}

// shared-corpus: storage_merged_dump_load_policy
#[test]
fn storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc() {
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
            key: "merged-a".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "merged-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"seven".to_vec(),
            }],
        },
    });

    let report =
        engine.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
            lifecycle: StorageLifecycleRequest {
                shard_id: 1,
                max_dump_buckets_per_round: 16,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                invalidate_cache: true,
                warm_cache: true,
                ..StorageLifecycleRequest::default()
            },
            create_dump_manifest: true,
            install_dump_manifest: false,
        });
    assert!(report.policy_ready, "{report:?}");
    assert!(report.dump_manifest_created);
    assert!(report.load_preflight_safe);
    assert!(report.replay_boundary_safe);
    assert!(report.manifest_chain_valid);
    assert!(report.follower_retention_safe);
    assert!(report.index_gc_ready);
    assert!(report.manifest_id.is_some());
    assert!(!report.manifest_bucket_ids.is_empty());
    // The merged dump/load policy report was restructured: the granular
    // `*_validated` booleans and conflict/interruption counters are now
    // expressed through `blockers` (empty == all validations passed) and the
    // recovery `boundary`. On this clean path there are no blockers, no
    // interrupted installs, and no roll-forward recoveries.
    assert!(report.blockers.is_empty(), "{report:?}");
    assert!(report.boundary.interrupted_bucket_dump_installs.is_empty());
    assert!(report.install_roll_forward_reports.is_empty());

    let manifest = latest_bucket_dump_manifest_at(&engine.index_dir, 1).unwrap();
    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("blocks"),
        dir.path().join("restore-indexes"),
    );
    assert!(
        restore_engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 0,
                local_node_id: Some(10),
                shard_uri: "local://restore/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                table_name: "restore".to_string(),
            })
            .status
            .ok
    );
    let merged_manifest = engine
        .create_merged_bucket_dump_manifest(
            1,
            manifest.bucket_ids.clone(),
            vec![manifest.manifest_id.clone()],
            Some(1),
        )
        .expect("merged manifest with load-version handoff");
    let install_report = restore_engine.install_merged_bucket_dump_manifest(&merged_manifest);
    assert!(install_report.installed, "{install_report:?}");
    assert!(install_report.rollback_marker_written);
    assert!(install_report.prepare_marker_written);
    assert!(install_report.install_marker_written);
    assert!(install_report.commit_marker_written);
    assert_eq!(install_report.source_manifest_count, 1);
    assert_eq!(install_report.stale_object_conflict_count, 0);
    assert_eq!(install_report.stale_block_conflict_count, 0);
    assert!(install_report
        .load_version_handoff
        .as_ref()
        .is_some_and(|handoff| handoff.previous_load_version == 0
            && handoff.next_load_version == 1
            && handoff.applied));
    assert_eq!(restore_engine.get_info(1).info.unwrap().load_version, 1);
    let get = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "merged-a".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    let feature = restore_engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "merged-feature".to_string(),
            start_ms: 0,
            end_ms: 20,
            count: None,
        },
    });
    assert!(matches!(
        feature.response,
        CommandResponse::FeaturePoints { ref points } if points.len() == 1
    ));

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-a".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let stale_preflight = engine.bucket_dump_install_preflight_report(&manifest);
    assert!(!stale_preflight.install_safe, "{stale_preflight:?}");
    assert!(stale_preflight
        .blockers
        .contains(&"stale_block_conflicts".to_string()));
    assert!(stale_preflight.stale_block_conflict_count > 0);
    assert_eq!(stale_preflight.stale_object_conflict_count, 0);
    assert!(!engine
        .install_bucket_dump_manifest(&manifest)
        .unwrap_err()
        .code
        .is_empty());

    // The interrupted-install roll-forward phase below spins up a SECOND engine (`restarted`) on
    // the SAME index dir (WAL) AND pages dir as `engine`, then keeps writing on `engine`. On the
    // default path the second engine's load folds the served-index delta and reuses the existing
    // block addresses, so the shared block store is not mutated. Base-only single-barrier recovery
    // re-derives blocks by REPLAYING the shared WAL, which rewrites blocks into the shared slab files
    // and desynchronizes the two engines' independent block-store write offsets -- corrupting this
    // two-instances-on-one-storage construction. That is a split-brain scenario, not single-engine
    // crash recovery (whose zero-loss guarantee is proven exhaustively by the subprocess crash
    // harness in tests/wal_single_barrier_recovery.rs, including data-block loss after a dump and
    // exactly-once counter replay). This phase is therefore exercised on the default path only.
    if !crate::engine::wal_single_barrier() {
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "install".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();
    let restarted = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache-restarted"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(1);
    assert_eq!(restarted.interrupted_bucket_dump_installs(1).len(), 1);
    let restart_boundary = restarted.storage_recovery_boundary_report(1);
    assert_eq!(restart_boundary.interrupted_bucket_dump_installs.len(), 1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "merged-c".to_string(),
            value: b"v3".to_vec(),
        },
    });
    let recovered =
        engine.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
            lifecycle: StorageLifecycleRequest {
                shard_id: 1,
                max_dump_buckets_per_round: 16,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                invalidate_cache: true,
                warm_cache: true,
                ..StorageLifecycleRequest::default()
            },
            create_dump_manifest: true,
            install_dump_manifest: false,
        });
    assert!(recovered.policy_ready, "{recovered:?}");
    // Recovery path: the interrupted install has been rolled forward, so no
    // interrupted installs remain in the boundary and at least one roll-forward
    // report was produced (was: interrupted_install_count==0,
    // roll_forward_recovery_count>=1, rollback_marker_count>=1).
    assert!(recovered.boundary.interrupted_bucket_dump_installs.is_empty());
    assert!(!recovered.install_roll_forward_reports.is_empty());
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());

    let mismatch_restore = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("mismatch-cache"),
        dir.path().join("blocks"),
        dir.path().join("mismatch-indexes"),
    );
    assert!(
        mismatch_restore
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 2,
                local_node_id: Some(11),
                shard_uri: "local://mismatch/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 16_383,
                readonly: false,
                table_name: "mismatch".to_string(),
            })
            .status
            .ok
    );
    let mismatch = mismatch_restore.bucket_dump_install_preflight_report(&merged_manifest);
    assert!(!mismatch.install_safe, "{mismatch:?}");
    assert!(mismatch
        .blockers
        .contains(&"load_version_handoff_mismatch".to_string()));
    }
}

#[test]
fn bucket_dump_install_markers_report_interrupted_prepare() {
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
            key: "marker".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: "interrupted".to_string(),
            phase: "prepare".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let interrupted = engine.interrupted_bucket_dump_installs(1);
    assert_eq!(interrupted.len(), 1);
    assert_eq!(interrupted[0].phase, "prepare");
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.interrupted_bucket_dump_installs, interrupted);
    assert_eq!(boundary.prepared_bucket_dump_install_count, 1);
    assert_eq!(boundary.installed_bucket_dump_install_count, 0);
    assert_eq!(boundary.unknown_bucket_dump_install_count, 0);
    let readiness = engine.storage_production_readiness_report(1);
    assert_eq!(readiness.interrupted_bucket_dump_install_count, 1);
    assert_eq!(readiness.prepared_bucket_dump_install_count, 1);
    assert_eq!(readiness.installed_bucket_dump_install_count, 0);
    assert_eq!(readiness.unknown_bucket_dump_install_count, 0);
}

#[test]
fn bucket_dump_install_roll_forward_completes_safe_installed_marker() {
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
            key: "roll".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "install".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.bucket_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "commit_ready");

    let applied = engine.roll_forward_bucket_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());
    let marker_files =
        bucket_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn bucket_dump_install_roll_forward_retries_safe_prepare_marker() {
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
            key: "retry-prepare".to_string(),
            value: b"value".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            phase: "prepare".to_string(),
            wal_sequence: manifest.wal_sequence,
            index_log_sequence: manifest.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let dry_run = engine.bucket_dump_install_roll_forward_reports(1);
    assert_eq!(dry_run.len(), 1);
    assert!(dry_run[0].can_retry_install);
    assert!(!dry_run[0].can_roll_forward);
    assert_eq!(dry_run[0].reason, "install_retry_ready");

    let applied = engine.roll_forward_bucket_dump_installs(1);
    assert_eq!(applied.len(), 1);
    assert!(applied[0].completed_install);
    assert!(applied[0].completed_commit);
    assert!(applied[0].obsolete_marker_files_removed > 0);
    assert!(engine.interrupted_bucket_dump_installs(1).is_empty());
    let marker_files =
        bucket_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
    assert!(marker_files
        .iter()
        .all(|(marker, _)| marker.phase == "commit"));
}

#[test]
fn bucket_dump_recovery_reports_broken_manifest_parent_chain() {
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
            key: "chain".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("parent manifest should persist");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "chain".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("child manifest should persist");
    assert_eq!(child.parent_manifest_id, Some(parent.manifest_id.clone()));

    fs::remove_file(bucket_dump_manifest_path(
        &engine.index_dir,
        1,
        &parent.manifest_id,
    ))
    .unwrap();
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.manifest_chain_issues.len(), 1);
    assert_eq!(
        boundary.manifest_chain_issues[0].manifest_id,
        child.manifest_id
    );
    assert_eq!(
        boundary.manifest_chain_issues[0].reason,
        "missing_parent_manifest"
    );
}

#[test]
fn bucket_dump_manifest_prune_keeps_latest_parent_chain_and_removes_obsolete_fork() {
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
            key: "prune".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "prune".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();
    write_bucket_dump_install_marker(
        &engine.index_dir,
        &BucketDumpInstallMarker {
            shard_id: 1,
            manifest_id: fork.manifest_id.clone(),
            phase: "commit".to_string(),
            wal_sequence: fork.wal_sequence,
            index_log_sequence: fork.index_log_sequence,
            created_unix_ms: now_ms(),
        },
    )
    .unwrap();

    let plan = engine.bucket_dump_manifest_prune_plan(1);
    // Retention keeps the NEWEST manifest and nothing else. The parent is a complete,
    // self-contained index that nothing recovers through, so with no cursor pinning it, it is
    // prunable just like the off-chain fork.
    assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
    assert!(!plan.retained_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(plan.prunable_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(
        plan.prunable_marker_manifest_ids,
        vec![fork.manifest_id.clone()]
    );

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: true,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = lifecycle
        .manifest_prune_report
        .expect("lifecycle should apply manifest prune");
    // Retention keeps the newest manifest only, so the obsolete fork AND the older parent are
    // both removed -- with no follower cursor or snapshot ref supplied, nothing pins them.
    assert!(report.removed_manifest_ids.contains(&fork.manifest_id));
    assert!(report.removed_manifest_ids.contains(&parent.manifest_id));
    assert_eq!(report.removed_marker_files, 1);
    assert!(!bucket_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
    assert!(!bucket_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
    let surviving = list_bucket_dump_manifests_at(&engine.index_dir, 1).unwrap();
    assert_eq!(
        surviving.len(),
        1,
        "only the newest manifest survives, got {surviving:?}"
    );
    // Pruning detaches the survivor from its removed parent, so a dangling parent link keeps
    // meaning corruption rather than ordinary history.
    assert!(surviving[0].parent_manifest_id.is_none());
}

#[test]
fn torn_manifest_does_not_hide_valid_manifests() {
    // Crash-consistency: a torn/corrupt manifest .json (from a crash mid-write, now
    // prevented by the atomic write) must not fail the whole manifest listing and
    // silently drop all dumped state on load. list_bucket_dump_manifests skips the bad
    // file and keeps the valid one.
    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        &index_dir,
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::<u32>::new())
        .unwrap();
    let manifest_dir = index_dir.join("slot-dumps").join("shard-1");
    std::fs::write(manifest_dir.join("torn-9999.json"), b"{ not valid json").unwrap();
    let listed = engine.list_bucket_dump_manifests(1);
    assert!(
        listed
            .iter()
            .any(|entry| entry.manifest_id == manifest.manifest_id),
        "a valid manifest must survive a torn sibling file: {listed:?}"
    );
}

#[test]
fn dump_clears_dumped_buckets_so_they_are_not_redumped() {
    // Once a dirty bucket is dumped its dirty flag is cleared, so the storage cycle does not
    // re-select and re-dump the same buckets (and re-export the whole index) forever.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..4 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            },
        });
    }
    let dirty_before: u64 = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.dirty_object_count)
        .sum();
    assert!(dirty_before > 0, "writes should leave dirty objects to dump");

    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        ..StorageLifecycleRequest::default()
    });
    assert!(
        !engine.list_bucket_dump_manifests(1).is_empty(),
        "a dump manifest should have been produced for the dirty buckets"
    );

    let dirty_after: u64 = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|summary| summary.dirty_object_count)
        .sum();
    assert_eq!(
        dirty_after, 0,
        "dumped buckets must be cleared, not left dirty to be re-dumped every cycle"
    );
}

/// A follower behind EVERY dump must be reported, not passed over in silence.
///
/// A cursor pins the newest dump at or below it. One that sits below all of them matched nothing
/// and fell straight through, so the case where NO retained dump can serve a follower -- the one
/// that most needs saying -- produced no signal at all, and pruning went ahead and threw away the
/// only dump that could ever have helped. The oldest is kept instead, and reported with a reason
/// that separates "pins an older dump" from "is behind everything we kept".
#[test]
fn a_follower_behind_every_dump_is_reported_and_keeps_the_oldest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for value in ["v1", "v2", "v3"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "behind".to_string(),
                value: value.as_bytes().to_vec(),
            },
        });
        engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    }
    let plan_before = engine.bucket_dump_manifest_prune_plan(1);
    let oldest = plan_before
        .prunable_manifest_ids
        .first()
        .cloned()
        .expect("older dumps are prunable when nothing pins them");

    // A follower older than every dump: sequence zero precedes them all.
    let plan = engine.bucket_dump_manifest_prune_plan_with_follower_cursors(
        1,
        vec![BucketDumpFollowerReplayCursor {
            follower_id: "follower-behind-everything".to_string(),
            shard_id: 1,
            wal_sequence: 0,
            index_log_sequence: 0,
        }],
    );

    assert_eq!(
        plan.follower_blocks.len(),
        1,
        "a follower no retained dump can serve must be reported, not skipped: {plan:?}"
    );
    assert_eq!(
        plan.follower_blocks[0].reason,
        "follower_cursor_precedes_every_manifest"
    );
    assert!(
        plan.retained_manifest_ids.contains(&plan.follower_blocks[0].manifest_id),
        "the dump reported as blocking must actually be kept"
    );
    assert!(
        !plan.prunable_manifest_ids.contains(&oldest),
        "the oldest dump is this follower's only chance and must not be pruned"
    );
}

#[test]
fn bucket_dump_manifest_prune_is_blocked_by_lagging_follower_cursor() {
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
            key: "cursor".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "cursor".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-follower-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();

    let no_cursor = engine.bucket_dump_manifest_prune_plan(1);
    // Without a cursor only the newest manifest is retained, so both the older parent and the
    // off-chain fork are prunable.
    assert!(no_cursor.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(no_cursor.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!no_cursor.prunable_manifest_ids.contains(&child.manifest_id));

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-a".to_string(),
        shard_id: 1,
        wal_sequence: fork.wal_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked =
        engine.bucket_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
    // The lagging cursor pins the manifest it would replay from, so that manifest is retained
    // and reported as the thing blocking the prune.
    assert_eq!(blocked.follower_blocks.len(), 1);
    assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
    let anchored = blocked.follower_blocks[0].manifest_id.clone();
    assert!(blocked.retained_manifest_ids.contains(&anchored));
    assert!(!blocked.prunable_manifest_ids.contains(&anchored));
    assert!(blocked
        .reasons
        .contains(&"follower_cursor_blocks_prune".to_string()));

    let caught_up = engine.bucket_dump_manifest_prune_plan_with_follower_cursors(
        1,
        vec![BucketDumpFollowerReplayCursor {
            wal_sequence: child.wal_sequence,
            index_log_sequence: child.index_log_sequence,
            ..lagging_cursor
        }],
    );
    // Once the cursor catches up to the newest manifest it pins nothing older, so every older
    // manifest -- the parent and the off-chain fork -- becomes prunable again.
    assert!(caught_up.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(caught_up.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!caught_up.prunable_manifest_ids.contains(&child.manifest_id));
}

#[test]
fn bucket_dump_manifest_prune_is_blocked_by_raft_snapshot_reference() {
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
            key: "snapshot".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let mut fork = parent.clone();
    fork.manifest_id = format!("{}-snapshot-anchor", fork.manifest_id);
    fork.parent_manifest_id = None;
    fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    engine.persist_bucket_dump_manifest(&fork).unwrap();

    let no_snapshot = engine.bucket_dump_manifest_prune_plan(1);
    // Same as the follower case: only the newest manifest is retained without a pin.
    assert!(no_snapshot.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(no_snapshot.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!no_snapshot.prunable_manifest_ids.contains(&child.manifest_id));

    let snapshot_ref = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-0007".to_string(),
        shard_id: 1,
        last_included_index: 7,
        last_included_term: 2,
        wal_sequence: fork.wal_sequence,
        index_log_sequence: fork.index_log_sequence,
    };
    let blocked = engine.bucket_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<BucketDumpFollowerReplayCursor>::new(),
        vec![snapshot_ref.clone()],
    );
    // The snapshot ref pins the manifest it would install from; anything else older is still
    // prunable, since only the newest manifest is retained unconditionally.
    assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
    assert!(!blocked.prunable_manifest_ids.contains(&fork.manifest_id));
    assert_eq!(blocked.raft_snapshot_blocks.len(), 1);
    assert_eq!(
        blocked.raft_snapshot_blocks[0].snapshot_id,
        "raft-snapshot-0007"
    );
    assert_eq!(
        blocked.raft_snapshot_blocks[0].manifest_id,
        fork.manifest_id
    );
    assert!(blocked
        .reasons
        .contains(&"raft_snapshot_blocks_prune".to_string()));

    let advanced = engine.bucket_dump_manifest_prune_plan_with_retention_refs(
        1,
        Vec::<BucketDumpFollowerReplayCursor>::new(),
        vec![BucketDumpRaftSnapshotRef {
            wal_sequence: child.wal_sequence,
            index_log_sequence: child.index_log_sequence,
            ..snapshot_ref
        }],
    );
    // Once the snapshot ref advances to the newest manifest it pins nothing older, so both the
    // parent and the off-chain fork become prunable again.
    assert!(advanced.prunable_manifest_ids.contains(&fork.manifest_id));
    assert!(advanced.prunable_manifest_ids.contains(&parent.manifest_id));
    assert!(!advanced.prunable_manifest_ids.contains(&child.manifest_id));
}

// shared-corpus: storage_wal_index_gc_generation_retention
/// Each manifest must be judged against ITS OWN fingerprints.
///
/// The plan decodes every manifest's index once, up front, and indexes into that by position
/// while walking buckets -- so a mis-paired index would judge one manifest's buckets against
/// another's fingerprints and anchor reclaim on the wrong dump. THREE manifests, because an
/// off-by-one can coincidentally pick the right one out of two.
///
/// This guards the shape of the lookup, not the old behaviour: before, the decode happened
/// inside the loop and could not be mispaired -- it was merely quadratic, taking 100s at 4k
/// records and not finishing at all at 40k.
#[test]
fn the_reclaim_plan_pairs_each_manifest_with_its_own_fingerprints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let mut manifests = Vec::new();
    for round in 0..3 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "reclaim-slot".to_string(),
                        value: format!("v{round}").into_bytes(),
                    },
                })
                .status
                .ok
        );
        manifests.push(engine.create_bucket_dump_manifest(1, Vec::new()).unwrap());
    }

    // Strictly increasing, or the assertion below cannot tell the three apart.
    assert!(manifests[1].wal_sequence > manifests[0].wal_sequence);
    assert!(manifests[2].wal_sequence > manifests[1].wal_sequence);

    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());

    // The newest manifest is the only one whose fingerprints still match the live state, so it
    // is the one the frontier must anchor on. Pairing manifest N with manifest N-1's
    // fingerprints would anchor on an older dump and reclaim a span that dump does not cover.
    assert_eq!(
        plan.durable_bucket_generation_frontier_wal_sequence, manifests[2].wal_sequence,
        "the frontier must anchor on the newest matching manifest: {plan:?}"
    );
    assert!(
        plan.retained_manifest_ids
            .contains(&manifests[2].manifest_id),
        "the newest manifest must be retained: {plan:?}"
    );
    assert!(
        plan.missing_bucket_generations.is_empty(),
        "every live bucket is covered by the newest dump: {plan:?}"
    );
}

#[test]
fn storage_wal_index_gc_reclaim_requires_durable_generation_and_retention_release() {
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
            key: "reclaim-slot".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "reclaim-slot".to_string(),
            value: b"v2".to_vec(),
        },
    });
    let child = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(child.wal_sequence > parent.wal_sequence);
    assert!(child.index_log_sequence > parent.index_log_sequence);

    let lagging_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-lagging".to_string(),
        shard_id: 1,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let lagging_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-lagging".to_string(),
        shard_id: 1,
        last_included_index: 11,
        last_included_term: 2,
        wal_sequence: parent.wal_sequence,
        index_log_sequence: parent.index_log_sequence,
    };
    let blocked = engine.storage_wal_reclaim_plan(
        1,
        vec![lagging_cursor.clone()],
        vec![lagging_snapshot.clone()],
    );
    // Both cursors sit at the parent manifest, so the frontier clamps down to the parent
    // instead of refusing at the child. They are still counted and named as retaining logs --
    // the ones above them -- which is what an operator needs in order to know why the log stops
    // shrinking where it does.
    assert!(blocked.safe_to_reclaim, "{blocked:?}");
    assert_eq!(blocked.follower_cursor_block_count, 1);
    assert_eq!(blocked.raft_snapshot_block_count, 1);
    assert_eq!(
        blocked.durable_bucket_generation_frontier_wal_sequence,
        child.wal_sequence
    );
    assert_eq!(
        blocked.durable_bucket_generation_frontier_index_log_sequence,
        child.index_log_sequence
    );
    assert_eq!(
        blocked.retain_from_wal_sequence,
        parent.wal_sequence.saturating_add(1),
        "clamped to the slowest cursor, not refused at the frontier: {blocked:?}"
    );
    assert_eq!(
        blocked.retain_from_index_log_sequence,
        parent.index_log_sequence.saturating_add(1)
    );
    assert!(blocked
        .blocker_reasons
        .contains(&"follower_cursor_retains_logs:follower-lagging".to_string()));
    assert!(blocked
        .blocker_reasons
        .contains(&"raft_snapshot_retains_logs:raft-snapshot-lagging".to_string()));

    let blocked_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![lagging_cursor],
        raft_snapshot_refs: vec![lagging_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 8,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let blocked_wal = blocked_cycle.wal_reclaim_report.as_ref().unwrap();
    // Clamped rather than refused. Both cursors sit at the parent manifest, so the cycle reclaims
    // the span they have already consumed and keeps everything above them. Asserting that nothing
    // was reclaimed was asserting the refusal itself, which is the behaviour that let one lagging
    // follower pin the whole log.
    assert!(blocked_wal.applied, "{blocked_wal:?}");
    assert!(
        blocked_wal.wal_records_removed > 0,
        "the span below the slowest cursor should go: {blocked_wal:?}"
    );
    assert_eq!(
        blocked_wal.plan.retain_from_wal_sequence,
        parent.wal_sequence.saturating_add(1),
        "and nothing above it: {blocked_wal:?}"
    );
    assert_eq!(
        blocked_wal.plan.retain_from_index_log_sequence,
        parent.index_log_sequence.saturating_add(1),
        "{blocked_wal:?}"
    );
    // Index GC follows the same frontier, so whatever it decides must agree with the plan rather
    // than be asserted independently -- the two disagreeing is the defect worth catching here.
    let blocked_index_gc = blocked_cycle.index_gc_report.as_ref().unwrap();
    assert_eq!(
        blocked_index_gc.applied,
        blocked_wal.plan.safe_to_reclaim,
        "index GC and WAL reclaim must reach the same verdict on one frontier: \
         {blocked_index_gc:?} against {:?}",
        blocked_wal.plan
    );
    assert!(
        blocked_cycle
            .stages
            .iter()
            .find(|stage| stage.stage == "reclaim_wal")
            .unwrap()
            .retention_blockers
            >= 2
    );

    let released_anchor = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    assert!(released_anchor.wal_sequence >= child.wal_sequence);
    assert!(released_anchor.index_log_sequence >= child.index_log_sequence);
    let released_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-caught-up".to_string(),
        shard_id: 1,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-caught-up".to_string(),
        shard_id: 1,
        last_included_index: 12,
        last_included_term: 2,
        wal_sequence: released_anchor.wal_sequence,
        index_log_sequence: released_anchor.index_log_sequence,
    };
    let released = engine.storage_wal_reclaim_plan(
        1,
        vec![released_cursor.clone()],
        vec![released_snapshot.clone()],
    );
    assert!(released.safe_to_reclaim, "{released:?}");
    assert_eq!(released.follower_cursor_block_count, 0);
    assert_eq!(released.raft_snapshot_block_count, 0);
    assert_eq!(
        released.retain_from_wal_sequence,
        released_anchor.wal_sequence.saturating_add(1)
    );
    assert_eq!(
        released.retain_from_index_log_sequence,
        released_anchor.index_log_sequence.saturating_add(1)
    );

    let threshold_blocked_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![released_cursor.clone()],
        raft_snapshot_refs: vec![released_snapshot.clone()],
        index_gc_index_log_bytes_threshold: u64::MAX,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 1,
        min_undumped_wal_records: 0,
        enable_wal_reclaim: false,
        ..StorageManagerCycleRequest::default()
    });
    let threshold_blocked_index_gc = threshold_blocked_cycle.index_gc_report.as_ref().unwrap();
    assert!(!threshold_blocked_index_gc.applied);
    assert_eq!(
        threshold_blocked_index_gc.skipped_reason,
        "index-log byte threshold not reached"
    );

    let final_anchor = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    let final_cursor = BucketDumpFollowerReplayCursor {
        follower_id: "follower-final".to_string(),
        shard_id: 1,
        wal_sequence: final_anchor.wal_sequence,
        index_log_sequence: final_anchor.index_log_sequence,
    };
    let final_snapshot = BucketDumpRaftSnapshotRef {
        snapshot_id: "raft-snapshot-final".to_string(),
        shard_id: 1,
        last_included_index: 13,
        last_included_term: 2,
        wal_sequence: final_anchor.wal_sequence,
        index_log_sequence: final_anchor.index_log_sequence,
    };
    let released_cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        follower_replay_cursors: vec![final_cursor],
        raft_snapshot_refs: vec![final_snapshot],
        index_gc_index_log_bytes_threshold: 0,
        index_gc_usage_ratio_trigger_basis_points: 0,
        index_gc_max_entries_per_round: 1,
        min_undumped_wal_records: 0,
        ..StorageManagerCycleRequest::default()
    });
    let released_wal = released_cycle.wal_reclaim_report.as_ref().unwrap();
    assert!(released_wal.plan.safe_to_reclaim, "{released_wal:?}");
    assert!(released_wal.applied, "{released_wal:?}");
    // There may be nothing left to remove, because the clamped cycle above already released the
    // span both cursors had consumed. That is the point of clamping: the work happens as the
    // cursor advances rather than all at once when it finally reaches the frontier. What must
    // hold is that the frontier itself has moved up to the final anchor.
    assert_eq!(
        released_wal.plan.retain_from_wal_sequence,
        final_anchor.wal_sequence.saturating_add(1),
        "{released_wal:?}"
    );
    let released_index_gc = released_cycle.index_gc_report.as_ref().unwrap();
    assert!(released_index_gc.safe_to_truncate, "{released_index_gc:?}");
    assert!(released_index_gc.applied, "{released_index_gc:?}");
    assert_eq!(released_index_gc.records_removed, 1);
    assert!(released_index_gc.budget_exhausted);

    let restarted = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restart-cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    restarted.load_shard(1);
    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "reclaim-slot".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    let restart_boundary = restarted.storage_recovery_boundary_report(1);
    assert!(restart_boundary.latest_safe_index_log_sequence >= final_anchor.index_log_sequence);
    assert!(restart_boundary.stale_index_block_refs.is_empty());
    assert_eq!(restart_boundary.missing_owner_block_refs, 0);
}

/// The lifecycle round's purge re-checks liveness, and the re-check REACHES it.
///
/// The block-store guards prove the re-check works when it is handed a live set. This proves the
/// scheduled round actually hands it one -- the failure mode a re-check is most likely to have is
/// not being wrong but being unreachable, declared on a method nothing in production calls with a
/// non-empty set.
///
/// The quarantine here is deliberately WRONG: the collector is handed an empty live set, so it
/// sets aside a slab holding a live block. That is the shape the re-check exists for, and forcing
/// it is the only way to reach the branch without an upstream bug to wait for.
///
/// Asserted as three separate facts, because a combined one hides all three: the slab was
/// restored rather than purged, the purge still reported it under `restored` rather than staying
/// silent, and -- the only assertion that is about the data rather than the bookkeeping -- the
/// key whose block lives in that slab still READS BACK.
#[test]
fn the_lifecycle_purge_re_checks_liveness_and_returns_a_live_slab() {
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
            key: "still-needed".to_string(),
            value: b"payload-in-slab-zero".to_vec(),
        },
    });
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "written-later".to_string(),
            value: b"payload-in-slab-one".to_vec(),
        },
    });

    // DENOMINATOR ONE: slab 0 really holds a live block.
    assert!(
        engine.live_block_slab_ids(1).contains(&0),
        "slab 0 must really be live for this to test anything: {:?}",
        engine.live_block_slab_ids(1)
    );

    // The wrong quarantine: an empty live set, so the collector sets aside a live slab.
    let quarantined = engine
        .block_store()
        .gc_slabs_before_with_live_refs_delayed_destroy(1, Vec::<u64>::new())
        .unwrap();
    // DENOMINATOR TWO: it really was quarantined, and really left the store.
    assert_eq!(
        quarantined.delayed_destroy_block_slab_ids,
        vec![0],
        "the slab was really quarantined"
    );
    assert!(
        !engine.block_store().slab_ids().unwrap().contains(&0),
        "and really left the store, so a reader cannot reach it by path"
    );

    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        purge_delayed_destroy: true,
        purge_delayed_destroy_slab_ids: None,
        ..Default::default()
    });

    assert_eq!(
        lifecycle.delayed_destroy_restored_slabs,
        vec![0],
        "the round's purge re-checked liveness and put the slab back"
    );
    assert!(
        lifecycle.delayed_destroy_purged_slabs.is_empty(),
        "and did not destroy it: {:?}",
        lifecycle.delayed_destroy_purged_slabs
    );
    assert!(
        engine.block_store().slab_ids().unwrap().contains(&0),
        "the slab is readable by path again"
    );

    // The payoff: the data survived.
    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "still-needed".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"payload-in-slab-zero".to_vec())
        },
        "the key whose page lives in the restored slab still reads back"
    );
}

/// One pinned slab no longer stops every other candidate being collected.
///
/// `safe_to_reclaim` is a single store-wide boolean, false whenever ANY candidate is blocked, and
/// both reclaim stages consulted it while `reclaimable_block_slab_ids` -- the per-slab answer the
/// same plan already computed -- went unread. A shard with one slab behind a lagging follower's
/// replay cursor therefore collected nothing at all, for as long as that follower stayed behind,
/// however many other slabs were free.
///
/// THE DENOMINATOR IS THE POINT OF THIS TEST: it asserts FIRST that the plan really does have a
/// blocked candidate and a free one. A plan that blocked nothing would make "the free slab was
/// collected" true for the wrong reason, and a plan that blocked everything would make "the
/// pinned slab survived" true for the wrong reason. Both halves are then asserted separately.
#[test]
fn a_single_pinned_slab_does_not_suppress_the_other_candidates() {
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
            key: "k0".to_string(),
            value: b"v0".to_vec(),
        },
    });
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.block_store().roll_slab().unwrap();
    // Overwrite k0 so its live block moves OFF slab 0. Slab 0 is now genuinely dead -- without
    // this it still holds a live block and is blocked on its own account, which would make the
    // test's "free" candidate not free and prove nothing about the gate.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k0".to_string(),
            value: b"v0-rewritten".to_vec(),
        },
    });

    // One shared-store cursor pins slab 1 and everything above it; slab 0 is free.
    let plan = engine.storage_block_gc_dependency_plan(
        1,
        vec![0, 1],
        vec![StorageBlockGcReplayCursor {
            cursor_id: "lagging-follower".to_string(),
            shard_id: 1,
            retain_from_block_slab_id: 1,
            reason: "follower has not replayed past slab one".to_string(),
        }],
        Vec::<BucketDumpRaftSnapshotRef>::new(),
        None,
        None,
        0,
    );

    // DENOMINATOR, both sides.
    assert_eq!(
        plan.blocked_block_slab_ids,
        vec![1],
        "the plan must really have a BLOCKED candidate: {plan:?}"
    );
    assert_eq!(
        plan.reclaimable_block_slab_ids,
        vec![0],
        "and really have a FREE one: {plan:?}"
    );
    assert!(
        !plan.safe_to_reclaim,
        "the store-wide boolean is false -- which is exactly what used to suppress slab 0: {plan:?}"
    );

    // The free subset is non-empty, so a round may proceed; the blocked slab is not in it.
    assert!(
        !plan.reclaimable_block_slab_ids.is_empty(),
        "a round gated on the free subset runs, where one gated on safe_to_reclaim would not"
    );
    assert!(
        !plan
            .reclaimable_block_slab_ids
            .contains(&1),
        "and the pinned slab is excluded from what that round may touch"
    );
}

// shared-corpus: storage_gc_dependency_retention_matrix
#[test]
fn storage_block_gc_blocks_all_retention_dependencies_before_reclaim() {
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
    let parent = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "gc-key".to_string(),
            value: b"v2".to_vec(),
        },
    });
    assert_eq!(engine.live_block_slab_ids(1), vec![1]);
    let delayed = engine
        .block_store()
        .gc_slabs_before_with_live_refs_delayed_destroy(1, engine.live_block_slab_ids(1))
        .unwrap();
    assert_eq!(delayed.delayed_destroy_block_slab_ids, vec![0]);

    let matrix = engine.storage_block_gc_dependency_plan(
        1,
        vec![0, 1],
        vec![StorageBlockGcReplayCursor {
            cursor_id: "shared-follower-a".to_string(),
            shard_id: 1,
            retain_from_block_slab_id: 0,
            reason: "shared-store follower is behind segment zero".to_string(),
        }],
        vec![BucketDumpRaftSnapshotRef {
            snapshot_id: "raft-snapshot-a".to_string(),
            shard_id: 1,
            last_included_index: 7,
            last_included_term: 2,
            wal_sequence: parent.wal_sequence,
            index_log_sequence: 0,
        }],
        Some(0),
        Some(0),
        60_000,
    );
    assert!(!matrix.safe_to_reclaim, "{matrix:?}");
    assert_eq!(matrix.candidate_block_slab_ids, vec![0, 1]);
    assert_eq!(matrix.live_ref_block_count, 1);
    assert_eq!(matrix.bucket_dump_manifest_block_count, 1);
    assert_eq!(matrix.shared_store_cursor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_ref_block_count, 2);
    assert_eq!(matrix.checkpoint_snapshot_floor_block_count, 2);
    assert_eq!(matrix.raft_snapshot_install_floor_block_count, 2);
    assert_eq!(matrix.delayed_destroy_grace_block_count, 1);
    for expected in [
        "live_block_ref",
        "slot_dump_manifest",
        "shared_store_replay_cursor",
        "raft_snapshot_ref",
        "checkpoint_snapshot_floor",
        "raft_snapshot_install_floor",
        "delayed_destroy_grace_period",
    ] {
        assert!(
            matrix.blocker_reasons.contains(&expected.to_string()),
            "{matrix:?}"
        );
    }

    let released = engine.storage_block_gc_dependency_plan(
        1,
        vec![0],
        Vec::<StorageBlockGcReplayCursor>::new(),
        Vec::<BucketDumpRaftSnapshotRef>::new(),
        None,
        None,
        0,
    );
    assert!(!released.safe_to_reclaim, "{released:?}");
    assert_eq!(released.bucket_dump_manifest_block_count, 1);
    assert_eq!(released.delayed_destroy_grace_block_count, 0);
    assert!(released
        .blocker_reasons
        .contains(&"slot_dump_manifest".to_string()));
}

#[test]
fn bucket_dump_manifest_rejects_generation_mismatch_and_conflicts() {
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
            key: "generation".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    assert_eq!(manifest.version, 3);
    assert!(!manifest.dump_generation_id.is_empty());
    assert_eq!(manifest.object_lifecycle.live_object_ids, 1);
    assert_eq!(manifest.object_lifecycle.live_block_refs, 1);

    let mut legacy_v2 = manifest.clone();
    legacy_v2.version = 2;
    legacy_v2.object_lifecycle = StorageObjectLifecycleReport::default();
    let legacy_generation_id = bucket_dump_generation_id(&legacy_v2);
    legacy_v2.object_lifecycle.live_object_ids = 99;
    assert_eq!(bucket_dump_generation_id(&legacy_v2), legacy_generation_id);

    let mut mismatched = manifest.clone();
    mismatched.dump_generation_id = "wrong-generation".to_string();
    mismatched.checksum = bucket_dump_manifest_checksum(&mismatched).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&mismatched)
            .unwrap_err()
            .code,
        "slot_dump_generation_mismatch"
    );

    let restore_engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("restore-cache"),
        dir.path().join("blocks"),
        dir.path().join("restore-indexes"),
    );
    restore_engine.load_shard(1);
    restore_engine
        .install_bucket_dump_manifest(&manifest)
        .expect("first generation should install");

    let mut fork = manifest.clone();
    let extra_bucket = fork
        .bucket_ids
        .iter()
        .copied()
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    fork.bucket_ids.push(extra_bucket);
    fork.dump_generation_id = bucket_dump_generation_id(&fork);
    fork.manifest_id = format!("{}-fork", fork.manifest_id);
    fork.checksum = bucket_dump_manifest_checksum(&fork).unwrap();
    assert_eq!(
        restore_engine
            .install_bucket_dump_manifest(&fork)
            .unwrap_err()
            .code,
        "slot_dump_generation_conflict"
    );
}

#[test]
fn bucket_dump_manifest_rejects_object_lifecycle_mismatch() {
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
            key: "lifecycle".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_lifecycle = manifest.clone();
    stale_lifecycle.object_lifecycle.live_object_ids = stale_lifecycle
        .object_lifecycle
        .live_object_ids
        .saturating_add(1);
    stale_lifecycle.dump_generation_id = bucket_dump_generation_id(&stale_lifecycle);
    stale_lifecycle.checksum = bucket_dump_manifest_checksum(&stale_lifecycle).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_lifecycle)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );

    let mut reused_owner = manifest.clone();
    {
        // A manifest carries the index in the format it was written in; decode and re-encode it
        // through the funnel rather than assuming JSON on either side.
        let mut restored = crate::engine::decode_index_bytes(&reused_owner.index_bytes)
            .expect("manifest index should decode");
        // THE MISMATCH IS PLANTED IN THE KEY, NOT IN A STORED ID. An object id is derived from
        // (shard, kind, key), so there is no field on the address to make wrong -- but renaming the
        // key the manifest's index holds changes the id the install DERIVES for it, which is the
        // same disagreement this test has always been about, reached through a mechanism that still
        // exists. Without this the install gate would be tested by planting a value nothing reads.
        let address = restored
            .take_string_address("lifecycle")
            .expect("manifest string address");
        restored
            .set_string_address("lifecycle-renamed-to-move-its-derived-id".into(), &address);
        reused_owner.index_bytes = crate::engine::encode_index_bytes(&restored);
        reused_owner.index_sha256 = sha256_hex_bytes(&reused_owner.index_bytes);
        reused_owner.dump_generation_id = bucket_dump_generation_id(&reused_owner);
        reused_owner.checksum = bucket_dump_manifest_checksum(&reused_owner).unwrap();
    }
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&reused_owner)
            .unwrap_err()
            .code,
        "slot_dump_object_lifecycle_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_bucket_summary_mismatch() {
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
            key: "slot-summary".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_summary = manifest.clone();
    let summary = stale_summary
        .bucket_summaries
        .first_mut()
        .expect("slot summary should exist");
    summary.block_ref_count = summary.block_ref_count.saturating_add(1);
    stale_summary.dump_generation_id = bucket_dump_generation_id(&stale_summary);
    stale_summary.checksum = bucket_dump_manifest_checksum(&stale_summary).unwrap();

    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_summary)
            .unwrap_err()
            .code,
        "slot_dump_slot_summary_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_byte_accounting_mismatch() {
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
            key: "byte-accounting".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut stale_bytes = manifest.clone();
    stale_bytes.logical_bytes = stale_bytes.logical_bytes.saturating_add(1);
    stale_bytes.checksum = bucket_dump_manifest_checksum(&stale_bytes).unwrap();

    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&stale_bytes)
            .unwrap_err()
            .code,
        "slot_dump_byte_accounting_mismatch"
    );
}

#[test]
fn bucket_dump_manifest_rejects_non_canonical_bucket_and_block_slab_ids() {
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
            key: "canonical".to_string(),
            value: b"v1".to_vec(),
        },
    });
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("manifest should persist");
    engine
        .validate_bucket_dump_manifest(&manifest)
        .expect("fresh manifest should validate");

    let mut duplicate_bucket = manifest.clone();
    duplicate_bucket.bucket_ids.push(
        duplicate_bucket
            .bucket_ids
            .first()
            .copied()
            .expect("slot id should exist"),
    );
    duplicate_bucket.dump_generation_id = bucket_dump_generation_id(&duplicate_bucket);
    duplicate_bucket.checksum = bucket_dump_manifest_checksum(&duplicate_bucket).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&duplicate_bucket)
            .unwrap_err()
            .code,
        "slot_dump_slot_ids_not_canonical"
    );

    let mut duplicate_block_slab = manifest.clone();
    duplicate_block_slab.block_slab_ids.push(
        duplicate_block_slab
            .block_slab_ids
            .first()
            .copied()
            .expect("page segment id should exist"),
    );
    duplicate_block_slab.dump_generation_id = bucket_dump_generation_id(&duplicate_block_slab);
    duplicate_block_slab.checksum = bucket_dump_manifest_checksum(&duplicate_block_slab).unwrap();
    assert_eq!(
        engine
            .validate_bucket_dump_manifest(&duplicate_block_slab)
            .unwrap_err()
            .code,
        "slot_dump_block_segment_ids_not_canonical"
    );
}

#[test]
fn storage_lifecycle_plan_and_boundary_report_cover_dirty_and_orphan_slabs() {
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
            key: "k".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.block_store().roll_slab().unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        },
    });

    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!plan.dirty_buckets.is_empty());
    assert_eq!(plan.selected_dump_buckets, plan.dirty_buckets);
    assert!(plan.reasons.contains(&"dirty_slot_dump".to_string()));
    assert!(plan.stale_block_slab_ids.contains(&0));
    assert!(plan
        .reasons
        .contains(&"ranked_reclaim_candidates".to_string()));
    assert!(!plan.reclaim_candidates.is_empty());
    assert_eq!(plan.reclaim_candidates[0].block_slab_id, 0);
    assert_eq!(plan.reclaim_candidates[0].reason, "orphan_segment");
    assert!(plan.reclaim_candidates[0].stale_physical_bytes > 0);
    assert!(plan.reclaim_candidates[0].reclaim_score > 0);

    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: plan.selected_dump_buckets.clone(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: true,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.dump_manifest.is_some());
    assert_eq!(report.object_lifecycle.live_object_ids, 1);
    assert_eq!(report.object_lifecycle.live_block_refs, 1);
    assert_eq!(report.object_lifecycle.stale_object_ids, 1);
    let boundary = engine.storage_recovery_boundary_report(1);
    assert_eq!(boundary.latest_safe_wal_sequence, 2);
    assert_eq!(boundary.latest_dump_wal_sequence, 2);
    assert!(boundary.orphan_block_slab_ids.contains(&0));
}

#[test]
fn storage_production_readiness_reports_warnings_without_blocking_dirty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "ready-key".to_string(),
                    value: b"ready-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report(1);
    assert!(report.production_ready, "{report:?}");
    assert!(report.blockers.is_empty());
    assert_eq!(report.dirty_bucket_count, 1);
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report.slab_integrity.integrity_ok);
    assert_eq!(report.slab_integrity.unreadable_block_ref_count, 0);
    assert_eq!(report.unreadable_block_ref_count, 0);
    assert_eq!(report.owner_mismatch_block_ref_count, 0);
    assert!(report.log_compatibility.rust_native_replay_safe);
    assert!(!report.log_compatibility.native_binary_compatible);
    assert_eq!(
        report.log_compatibility.wal_format,
        "rust-protobuf-wal-v1"
    );
    assert_eq!(
        report.log_compatibility.index_log_format,
        "rust-jsonl-shard-index-v1"
    );
    assert_eq!(
        report.log_compatibility.compatibility_mode,
        "rust_native_migration_only"
    );
    assert!(report.log_compatibility.migration_required);
    assert!(report.log_compatibility.golden_conversion_required);
    assert!(!report.log_compatibility.native_reader_supported);
    assert!(!report.log_compatibility.native_writer_supported);
    assert!(report.block_format_compatibility.rust_native_read_safe);
    assert!(!report.block_format_compatibility.native_block_header_compatible);
    assert_eq!(
        report.block_format_compatibility.block_format,
        "rust-page-envelope-v6"
    );
    assert_eq!(
        report.block_format_compatibility.compatibility_mode,
        "rust_envelope_migration_only"
    );
    assert!(report.block_format_compatibility.migration_required);
    assert!(report.block_format_compatibility.golden_conversion_required);
    assert!(
        !report
            .block_format_compatibility
            .native_block_header_reader_supported
    );
    assert!(
        !report
            .block_format_compatibility
            .native_block_header_writer_supported
    );
    assert!(report.block_format_compatibility.checksum_protected);
    assert!(report.block_format_compatibility.object_ids_embedded);
    assert!(report.block_store_bytes_written > 0);
}

#[test]
fn storage_log_compatibility_report_counts_jsonl_sequences_and_native_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..2 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("log-key-{index}"),
                        value: format!("log-value-{index}").into_bytes(),
                    },
                })
                .status
                .ok
        );
    }

    let report = engine.storage_log_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.compatibility_mode, "rust_native_migration_only");
    assert!(report.migration_required);
    assert!(!report.native_reader_supported);
    assert!(!report.native_writer_supported);
    assert!(report.golden_conversion_required);
    assert_eq!(report.wal_last_sequence, 2);
    assert_eq!(report.index_log_last_sequence, 2);
    assert_eq!(report.wal_records, 2);
    assert_eq!(report.index_log_records, 2);
    assert!(report.wal_bytes > 0);
    assert!(report.index_log_bytes > 0);
    assert!(report.rust_native_replay_safe);
    assert!(!report.native_binary_compatible);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| { gap.contains("migration-only") && gap.contains("binary log") }));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("binary/protobuf wal")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden log conversion/replay")));
}

#[test]
fn storage_block_format_compatibility_report_counts_zones_and_header_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "page-format-key".to_string(),
                    value: vec![11; 512],
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_slab().unwrap();

    let report = engine.storage_block_format_compatibility_report(1);
    assert_eq!(report.shard_id, 1);
    assert_eq!(report.block_format, "rust-page-envelope-v6");
    assert_eq!(report.rust_envelope_version, 6);
    assert_eq!(report.compatibility_mode, "rust_envelope_migration_only");
    assert!(report.migration_required);
    assert!(!report.native_block_header_reader_supported);
    assert!(!report.native_block_header_writer_supported);
    assert!(report.golden_conversion_required);
    assert!(report.rust_native_read_safe);
    assert!(!report.native_block_header_compatible);
    assert!(report.checksum_protected);
    assert!(report.object_ids_embedded);
    assert!(report.routing_buckets_embedded);
    assert!(report.compression_supported);
    assert_eq!(report.sealed_slabs, 1);
    assert_eq!(report.active_slabs, 1);
    assert!(report.live_physical_bytes > 0);
    assert!(report.block_store_writes > 0);
    assert!(report.block_store_bytes_written > 0);
    assert!(report.logical_bytes_written >= 512);
    assert!(report.compressed_records_written > 0);
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("migration-only") && gap.contains("page-header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("binary protobuf page header")));
    assert!(report
        .compatibility_gaps
        .iter()
        .any(|gap| gap.contains("golden page conversion/replay")));
}

#[test]
fn storage_production_readiness_policy_can_block_dirty_dump_lag_and_missing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "policy-key".to_string(),
                    value: b"policy-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            max_dirty_buckets: Some(0),
            max_undumped_wal_records: Some(0),
            require_bucket_dump_manifest: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert_eq!(report.policy.max_dirty_buckets, Some(0));
    assert_eq!(report.dirty_bucket_count, 1);
    assert!(report.undumped_wal_records > 0);
    assert!(report
        .blockers
        .contains(&"dirty_slots_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"undumped_wal_records_exceed_policy".to_string()));
    assert!(report
        .blockers
        .contains(&"slot_dump_manifest_required".to_string()));
}

#[test]
fn storage_production_readiness_policy_can_promote_warnings_to_blockers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "warn-key".to_string(),
                    value: b"warn-value".to_vec(),
                },
            })
            .status
            .ok
    );

    let report = engine.storage_production_readiness_report_with_policy(
        1,
        StorageProductionReadinessPolicy {
            block_on_warnings: true,
            ..StorageProductionReadinessPolicy::default()
        },
    );

    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .warnings
        .contains(&"dirty_slots_pending_dump".to_string()));
    assert!(report
        .blockers
        .contains(&"warnings_exceed_policy".to_string()));
}

#[test]
fn storage_production_readiness_blocks_corrupt_live_block_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "corrupt-key".to_string(),
                    value: b"corrupt-value".to_vec(),
                },
            })
            .status
            .ok
    );
    let slab_id = engine.live_block_slab_ids(1)[0];
    let mut bytes = engine.block_store().read_slab(slab_id).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    engine
        .block_store()
        .install_slab(slab_id, &bytes)
        .unwrap();

    let report = engine.storage_production_readiness_report(1);
    assert!(!report.production_ready, "{report:?}");
    assert!(report
        .blockers
        .contains(&"corrupt_block_segments".to_string()));
    assert!(report
        .blockers
        .contains(&"unreadable_live_block_refs".to_string()));
    assert!(report
        .blockers
        .contains(&"storage_segment_integrity_failed".to_string()));
    assert!(!report.slab_integrity.integrity_ok);
    assert!(report.slab_integrity.corrupt_block_slab_count > 0);
    assert!(report.slab_integrity.unreadable_block_ref_count > 0);
    assert!(report.corrupt_block_slab_count > 0);
    assert!(report.unreadable_block_ref_count > 0);
}

#[test]
fn storage_lifecycle_apply_warms_cache_from_block_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm-me".to_string(),
            value: vec![7; 128],
        },
    });
    engine.cache().invalidate_shard(1).unwrap();
    let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: plan.selected_dump_buckets,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: true,
        ..StorageLifecycleRequest::default()
    });
    assert!(report.cache_warmup_block_refs >= 1);
    assert_eq!(
        report.cache_warmup.warmed_block_refs,
        report.cache_warmup_block_refs
    );
    assert!(report.cache_warmup.considered_block_refs >= 1);
    assert!(report.cache_warmup.block_store_reads >= 1);
    assert!(report.cache_warmup.warmed_bytes >= 128);
    assert_eq!(report.cache_warmup.failed_block_refs, 0);
    assert!(engine.cache().stats().puts >= 1);
}

#[test]
fn storage_cache_warmup_report_filters_buckets_and_counts_cache_hits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let first_key = "warm-slot-a";
    let first_bucket = engine.routing_bucket_for_key(1, first_key);
    let second_key = (0..100)
        .map(|index| format!("warm-slot-b-{index}"))
        .find(|key| engine.routing_bucket_for_key(1, key) != first_bucket)
        .expect("test should find a key in another slot");
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: first_key.to_string(),
            value: b"value-a".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: second_key,
            value: b"value-b".to_vec(),
        },
    });
    engine.cache().invalidate_shard(1).unwrap();

    let bucket = first_bucket;
    let first = engine.storage_cache_warmup_report(1, [bucket]);
    assert_eq!(first.selected_buckets, vec![bucket]);
    assert_eq!(first.considered_block_refs, 1);
    assert_eq!(first.skipped_block_refs, 1);
    assert_eq!(first.block_store_reads, 1);
    assert_eq!(first.already_cached_block_refs, 0);
    assert_eq!(first.failed_block_refs, 0);
    assert!(first.warmed_bytes > 0);

    let hits_before_second = engine.cache().stats().memory_hits
        + engine.cache().stats().pmem_hits
        + engine.cache().stats().disk_hits;
    let second = engine.storage_cache_warmup_report(1, [bucket]);
    let hits_after_second = engine.cache().stats().memory_hits
        + engine.cache().stats().pmem_hits
        + engine.cache().stats().disk_hits;
    assert_eq!(second.considered_block_refs, 1);
    assert_eq!(second.skipped_block_refs, 1);
    assert_eq!(second.block_store_reads, 0);
    assert_eq!(second.already_cached_block_refs, 1);
    assert_eq!(second.warmed_block_refs, 1);
    assert_eq!(hits_after_second, hits_before_second);
}

#[test]
fn storage_cache_inspection_reports_bucket_entries_and_invalidates_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let key = "slot-cache-key";
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"slot-cache-value".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.cache().invalidate_shard(1).unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            })
            .status
            .ok
    );

    let bucket = engine.routing_bucket_for_key(1, key);
    let report = engine.storage_cache_inspection_report(1);
    assert!(report.stats.disk_fills >= 1);
    assert!(report
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{bucket}:"))));
    assert!(report
        .bucket_summaries
        .iter()
        .any(|summary| summary.routing_bucket == bucket && summary.entry_count >= 1));

    let invalidated = engine
        .invalidate_storage_cache_bucket(StorageCacheInvalidateBucketRequest {
            shard_id: 1,
            routing_bucket: bucket,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let after = engine.storage_cache_inspection_report(1);
    assert!(!after
        .entries
        .iter()
        .any(|entry| entry.selector.starts_with(&format!("slot-{bucket}:"))));
}

#[test]
fn tiny_cache_dump_load_restart_refills_from_disk_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let restore_index_dir = dir.path().join("restore-indexes");
    let engine = TemporalEngine::with_local_dirs(32, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);
    let target_value = b"dump-load-target-1234".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("churn-a", b"cache-churn-a-1234".to_vec()),
        ("churn-b", b"cache-churn-b-1234".to_vec()),
    ] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }
    let target_block_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).unwrap();
        let address = shard.string_address("target").unwrap().clone();
        let (start, end) = shard.routing_range();
        CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(crate::engine::hashing::block_routing_bucket("target", start, end)),
        )
    };

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "target".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    for key in ["churn-a", "churn-b"] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );
    }
    assert!(engine.cache().stats().memory_evictions > 0);
    assert!(engine.cache().stats().disk_bytes > 0);
    assert_eq!(engine.cache().get_memory(&target_block_key), None);
    let manifest = engine
        .create_bucket_dump_manifest(1, Vec::new())
        .expect("slot dump manifest should persist");
    engine.validate_bucket_dump_manifest(&manifest).unwrap();

    let restored = TemporalEngine::with_local_dirs(32, &cache_dir, &block_dir, &restore_index_dir);
    restored.load_shard(1);
    restored
        .install_bucket_dump_manifest(&manifest)
        .expect("slot dump should install after restart");
    let block_reads_before = restored.block_store().stats().reads;
    let disk_hits_before = restored.cache().stats().disk_hits;
    let response = restored.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(target_value)
        }
    );
    assert_eq!(
        restored.block_store().stats().reads,
        block_reads_before,
        "restored engine should refill from disk block cache before block store"
    );
    assert!(restored.cache().stats().disk_hits > disk_hits_before);

    let bucket = restored.routing_bucket_for_key(1, "target");
    let cache_report = restored.storage_cache_inspection_report(1);
    assert!(cache_report
        .bucket_summaries
        .iter()
        .any(|summary| summary.routing_bucket == bucket && summary.entry_count >= 1));
    let invalidated = restored
        .invalidate_storage_cache_bucket(StorageCacheInvalidateBucketRequest {
            shard_id: 1,
            routing_bucket: bucket,
        })
        .unwrap();
    assert!(invalidated.memory_entries_removed >= 1);
    let readiness = restored.storage_production_readiness_report(1);
    assert!(readiness.production_ready, "{readiness:?}");
    assert_eq!(readiness.unreadable_block_ref_count, 0);
    assert_eq!(readiness.corrupt_block_slab_count, 0);
}

#[test]
fn storage_lifecycle_plan_matches_delayed_and_limited_dirty_bucket_dump_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..128 {
        let key = format!("slot-{i}");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: key.as_bytes().to_vec(),
            },
        });
        let observed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            purge_delayed_destroy: false,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
            ..StorageLifecycleRequest::default()
        });
        if observed.dirty_buckets.len() >= 3 {
            break;
        }
    }

    let delayed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 99,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(delayed.dump_delayed);
    assert!(delayed.selected_dump_buckets.is_empty());
    assert!(delayed
        .reasons
        .contains(&"dirty_slot_dump_delayed".to_string()));

    let limited = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 2,
        min_undumped_wal_records: 1,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!limited.dump_delayed);
    assert!(limited.undumped_wal_records >= 3);
    assert_eq!(limited.selected_dump_buckets.len(), 2);
    assert!(limited.dirty_buckets.len() >= limited.selected_dump_buckets.len());

    let explicit = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![delayed.dirty_buckets[0]],
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 99,
        purge_delayed_destroy: false,
        purge_delayed_destroy_slab_ids: None,
        prune_bucket_dump_manifests: false,
        roll_forward_bucket_dump_installs: false,
        follower_replay_cursors: Vec::new(),
        invalidate_cache: false,
        warm_cache: false,
        ..StorageLifecycleRequest::default()
    });
    assert!(!explicit.dump_delayed);
    assert_eq!(explicit.selected_dump_buckets, vec![delayed.dirty_buckets[0]]);
}

// E1 regression: one engine hosts many shards over a SINGLE page_store with a global slab
// cursor, so two shards' blocks can share a slab. Block reclaim used to compute the live set from
// only the shard whose cycle was running, so a slab holding another shard's committed blocks
// looked like an orphan and was deleted -- silent cross-shard data loss.
//
// This test builds exactly that: shard B's block lands in slab 0, the slab is sealed by a roll,
// shard A's block lands in slab 1, then shard A's storage-manager cycle runs block reclaim. Slab 0
// is absent from shard A's live set, below the retention floor, and not the current slab, so the
// legacy per-shard live set deletes it. The fix unions live slab ids across ALL loaded shards, so
// slab 0 (live in shard B) is retained.
//
// FAIL-before / PASS-after is demonstrated with the TS_CROSS_SHARD_RECLAIM_GUARD kill-switch:
// running this test with TS_CROSS_SHARD_RECLAIM_GUARD=0 reproduces the legacy per-shard behavior
// and the assertions below fail (slab 0 is moved out of the read path); the default (guard on)
// passes.
#[test]
fn cross_shard_block_reclaim_retains_another_shards_live_slab() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    // Tiny cache so reads hit the slab on disk rather than an in-memory copy.
    let engine =
        TemporalEngine::with_local_dirs(16, dir.path().join("cache"), &blocks, &indexes);
    // Two shards hosted by the SAME engine => the SAME page_store + global slab cursor.
    engine.load_shard(1); // shard A -- runs the reclaim cycle
    engine.load_shard(2); // shard B -- owns the shared slab's live page

    // Shard B's committed block lands in the current slab (slab 0).
    let response = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSet {
            key: "shard-b-key".to_string(),
            value: b"shard-b-committed-value".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    // Identify the slab file backing shard B's block (the only .seg under the pages root).
    let shard_b_slabs = fs::read_dir(&blocks)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("block_segment_") && name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    // The comment above promises exactly one, so count it rather than taking the first that
    // happens to match: a filter over an on-disk name that has moved finds none, and `find`
    // would report that as "shard B never wrote" instead of "the name moved".
    assert_eq!(
        shard_b_slabs.len(),
        1,
        "expected exactly one slab file under {}, found {:?}",
        blocks.display(),
        shard_b_slabs
    );
    let shard_b_slab_path = shard_b_slabs.into_iter().next().unwrap();

    // Seal slab 0 so future appends go to a fresh slab, then write shard A into slab 1. Slab 0 is
    // now a non-current slab that is live ONLY in shard B.
    engine.block_store().roll_slab().unwrap();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "shard-a-key".to_string(),
            value: b"shard-a-committed-value".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");

    // Sanity: from shard A's own viewpoint, slab 0 is NOT live (it holds no shard-A blocks), which
    // is precisely why the legacy per-shard reclaim would delete it.
    assert!(
        !engine.live_block_slab_ids(1).contains(&0),
        "slab 0 must be absent from shard A's per-shard live set"
    );
    assert!(
        engine.live_block_slab_ids_all_shards().contains(&0),
        "slab 0 must be present in the cross-shard union live set"
    );

    // Run shard A's storage-manager cycle with block reclaim (only). Other stages are off so the
    // test isolates the reclaim decision.
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: false,
        enable_evict: false,
        enable_expire: false,
        enable_block_reclaim: true,
        enable_block_compaction: false,
        enable_index_gc: false,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        cycle.errors.is_empty(),
        "reclaim cycle should not error: {:?}",
        cycle.errors
    );

    // Decisive, cache-independent check: shard B's slab file must still be in the read path (the
    // legacy per-shard reclaim moves it into the delayed-destroy trash, vanishing shard B's data).
    assert!(
        shard_b_slab_path.exists(),
        "shard B's live slab was reclaimed by shard A's cycle -> cross-shard data loss"
    );

    // Behavioral check: shard B's committed value is still readable after shard A's cycle.
    let read_back = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringGet {
            key: "shard-b-key".to_string(),
        },
    });
    assert_eq!(
        read_back.response,
        CommandResponse::Bytes {
            value: Some(b"shard-b-committed-value".to_vec()),
        },
        "shard B's committed value must survive shard A's reclaim cycle"
    );
}

// E5 regression: FeatureQuery / FeatureQueryFiltered used to skip lazy expiry, so a key past its
// deadline but not yet swept read live points from FeatureQuery while FeatureAggQuery (which
// applies remove_if_expired) read empty -- an inconsistency between two reads of the same key.
// All feature reads must agree: an expired-but-unswept key reads empty everywhere.
#[test]
fn expired_feature_key_reads_empty_consistently_across_feature_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for command in [
        Command::FeatureAppend {
            key: "expiring-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            }],
        },
        Command::CommonExpire {
            key: "expiring-feature".to_string(),
            ttl_ms: 1,
        },
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "{response:?}");
    }
    // Cross the deadline but run NO sweep: this is the window where lazy expiry must fire.
    std::thread::sleep(std::time::Duration::from_millis(5));

    let feature_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            count: None,
        },
    });
    assert_eq!(
        feature_query.response,
        CommandResponse::FeaturePoints { points: Vec::new() },
        "FeatureQuery must apply lazy expiry and read empty for an expired-but-unswept key"
    );

    let filtered_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            count: None,
            filters: Vec::new(),
        },
    });
    assert_eq!(
        filtered_query.response,
        CommandResponse::FeaturePoints { points: Vec::new() },
        "FeatureQueryFiltered must apply lazy expiry and read empty for an expired-but-unswept key"
    );

    let agg_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "expiring-feature".to_string(),
            start_ms: 0,
            end_ms: 1000,
            aggregator: "count".to_string(),
            count: None,
        },
    });
    assert_eq!(
        agg_query.response,
        CommandResponse::Aggregate { value: 0 },
        "FeatureAggQuery reads empty for an expired key -- the other feature reads must match"
    );
}

// RN2: a storage-manager cycle must be an inert no-op while the shard is RECOVERING (WAL
// replay in progress). A GC/compaction/reclaim round interleaved with an in-flight replay
// would observe a half-reconstructed bucket index and could mis-reclaim a still-live block.
#[test]
fn storage_manager_cycle_is_a_noop_while_shard_is_recovering() {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &block_dir,
        &index_dir,
    );
    engine.load_shard(1);
    for i in 0..4 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    drop(engine);

    // Restart and park the shard in the recovery window (recovering=true, replay not yet run).
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    let watermark = restarted.test_publish_recovering_shard(1);
    let wal_before = restarted.write_ahead_log_store().stats(1).last_sequence;

    let report = restarted.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        max_dump_buckets_per_round: 16,
        min_undumped_wal_records: 0,
        warm_cache: true,
        enable_wal_reclaim: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        !report.completed,
        "a storage-manager cycle must not complete while the shard is recovering: {report:?}"
    );
    assert!(
        report.stages.is_empty(),
        "a recovering cycle must build no stages (it returns early before any mutation)"
    );
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.contains("recovering") || error.contains("recovery")),
        "the skip reason must name recovery: {:?}",
        report.errors
    );
    // The WAL tail is untouched (nothing reclaimed), and recovery still completes normally.
    assert_eq!(
        restarted.write_ahead_log_store().stats(1).last_sequence,
        wal_before,
        "no WAL record may be reclaimed during recovery"
    );
    restarted.test_finish_recovery(1, watermark);
    let after = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k0".to_string(),
        },
    });
    assert!(after.status.ok, "the shard serves once recovery completes");
}

// F4: a corrupt / unreadable WAL scan on load is DATA LOSS, not "nothing to replay". The load
// must refuse rather than silently serving the stale base index (dropping the committed WAL
// tail). A value-preserving bit-flip in a committed, newline-terminated record exercises this.
#[test]
fn corrupt_wal_scan_refuses_load_rather_than_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &block_dir,
        &index_dir,
    );
    engine.load_shard(1);
    for key in ["keepme", "corruptme"] {
        assert!(
            engine
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
    drop(engine);

    let wal_path = index_dir.join("wals").join("shard-1.wal.bin");
    let mut bytes = std::fs::read(&wal_path).unwrap();
    let position = bytes
        .windows(9)
        .position(|window| window == b"corruptme")
        .expect("the second WAL record is present");
    bytes[position + 1] = b'0'; // "corruptme" -> "c0rruptme": still valid JSON, wrong digest
    std::fs::write(&wal_path, &bytes).unwrap();

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    let response = restarted.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        !response.status.ok,
        "a corrupt WAL scan must refuse the load, not serve a truncated prefix: {:?}",
        response.status
    );
}

// F3: a corrupt INTERIOR served-index delta record must abort the load, not be silently
// skipped (a skipped delta loses a removal/eviction recorded only there -> resurrection or
// dangling ref).
#[test]
fn corrupt_index_log_delta_refuses_load_rather_than_silently_skipping() {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &block_dir,
        &index_dir,
    );
    engine.load_shard(1);
    for key in ["alpha", "beta", "gamma"] {
        assert!(
            engine
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
    drop(engine);

    let index_log_path = index_dir
        .join("indexlogs")
        .join("shard-1.indexlog.bin");
    let contents = std::fs::read(&index_log_path).unwrap();
    // Walk frames, not newlines: a record's payload may hold a 0x0A, so splitting on one
    // yields fragments rather than records and the corruption below would land nowhere.
    let mut lines: Vec<Vec<u8>> = Vec::new();
    let mut at = 0usize;
    while at < contents.len() {
        match crate::log_framing::next_frame(&contents[at..]) {
            Ok(Some((consumed, _))) => {
                lines.push(contents[at..at + consumed].to_vec());
                at += consumed;
            }
            _ => break,
        }
    }
    assert!(
        lines.len() >= 2,
        "the write path must have appended index-log delta records"
    );
    // Corrupt an interior record (not the tail) so it is committed corruption, not a torn tail.
    // Framed the way the writer frames, so what fails is the decode rather than the envelope.
    lines[0] = crate::log_framing::encode_record(b"corrupt-not-a-record");
    let mut rebuilt = Vec::new();
    for line in &lines {
        rebuilt.extend_from_slice(line);
    }
    std::fs::write(&index_log_path, &rebuilt).unwrap();

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    let response = restarted.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    if crate::engine::wal_single_barrier() {
        // Base-only single-barrier recovery (the DEFAULT) never folds the served-index delta:
        // it re-derives state from the durable base checkpoint + WAL replay (+ config-log),
        // which is a COMPLETE source of truth (evictions/removals are WAL/config-log
        // re-derivable, not delta-only). A corrupt delta is therefore never parsed and cannot
        // cause a silent skip of a delta-only removal -- so the load must SUCCEED and reconstruct
        // the EXACT logical state. We prove no silent loss by reading every acked key back at its
        // written value (a strictly stronger guarantee than the original binary refuse/accept),
        // so this generalization preserves the test's protection rather than weakening it. The
        // delta-fold refuse-on-corruption path is still asserted below under the legacy escape
        // hatch (and covered by the subprocess crash harness's drop-indexlog cases).
        assert!(
            response.status.ok,
            "base-only recovery ignores the un-folded delta and must load: {:?}",
            response.status
        );
        for key in ["alpha", "beta", "gamma"] {
            let got = restarted.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert!(
                got.status.ok,
                "read of {key} must succeed after base-only recovery: {:?}",
                got.status
            );
            assert_eq!(
                got.response,
                CommandResponse::Bytes {
                    value: Some(b"v".to_vec())
                },
                "base-only recovery must preserve the exact value for {key} (no silent loss)"
            );
        }
    } else {
        assert!(
            !response.status.ok,
            "a corrupt interior index-log delta must refuse the load, not be silently skipped: {:?}",
            response.status
        );
    }
}

/// The same, for the batch path -- which is the one bulk ingest actually runs.
///
/// `batch_execute` swept every bucket once per batch rather than once per write. That is a much
/// smaller constant than the per-write sweep, but the same `O(total pages)` shape, so a bulk
/// import still paid for the whole corpus on every batch. Measured per BATCH, and the flags are
/// compared against a full sweep for the same reason as the single-write case: a bucket left
/// stale-false in `dirty` never gets flushed.
#[test]
fn batch_bucket_maintenance_does_not_grow_with_the_store() {
    const BATCH: usize = 32;
    const MEASURED_BATCHES: usize = 5;

    fn visits_per_batch(object_count: usize) -> (f64, usize) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("batch-fill-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        crate::engine::reset_bucket_block_index_visits();
        for batch in 0..MEASURED_BATCHES {
            let commands = (0..BATCH)
                .map(|item| Command::StringSet {
                    key: format!("batch-measured-{batch}-{item}"),
                    value: vec![b'v'; 64],
                })
                .collect();
            engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands,
            });
        }
        let visited = crate::engine::bucket_block_index_visits();

        // Same equivalence requirement as the single-write path: sweeping afterwards must not
        // move anything the targeted refresh already settled.
        let snapshot = |engine: &TemporalEngine| {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let shard = shards.get(&1).expect("shard 1 loaded");
            shard
                .bucket_index
                .bucket_map
                .iter()
                .map(|(id, bucket)| {
                    (
                        *id,
                        (
                            bucket.in_memory(),
                            bucket.deleted(),
                            bucket.dirty(),
                            format!("{:?}", bucket.layout),
                            bucket.object_index.iter().copied().collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before_sweep = snapshot(&engine);
        {
            let mut shards = engine.shards.write().expect("shards lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1 loaded");
            crate::engine::storage_bucket_internals::refresh_bucket_runtime_flags(shard);
        }
        let after_sweep = snapshot(&engine);
        assert_eq!(
            before_sweep, after_sweep,
            "batch targeted refresh disagreed with a full sweep at {object_count} objects"
        );

        (visited as f64 / MEASURED_BATCHES as f64, before_sweep.len())
    }

    let (small, small_buckets) = visits_per_batch(200);
    let (large, large_buckets) = visits_per_batch(800);
    println!(
        "
  200 objects -> {small:>9.1} page-index visits per {BATCH}-command batch ({small_buckets} buckets)
           800 objects -> {large:>9.1} page-index visits per {BATCH}-command batch ({large_buckets} buckets)
           growth: {:.2}x cost for 4x the corpus
",
        if small > 0.0 { large / small } else { 0.0 }
    );

    assert!(
        large <= small * 1.5 + 1.0,
        "per-batch bucket maintenance grew with the store: {small:.1} -> {large:.1}          visits/batch for 200 -> 800 objects"
    );
}

/// The per-write targeted refresh must leave the same flags as sweeping the whole shard.
///
/// The write path refreshes only the buckets it recorded as touched. That is only correct if a
/// bucket nobody recorded genuinely cannot have changed, which is an argument about every site
/// that mutates a bucket, `dirty_objects` or `expires_at_ms`. Rather than rest on the argument,
/// this runs a mixed workload and then sweeps every bucket: if the targeted path ever misses one,
/// the sweep moves it and the comparison fails, naming the bucket and the field.
///
/// `ttl_ms` is a countdown recomputed from the clock, so it is compared as present/absent rather
/// than by value; every other flag is exact.
#[test]
fn bucket_runtime_flags_match_full_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A mix, so the comparison covers inserts, overwrites, hash fields, expiries and deletes
    // rather than one shape of write.
    for index in 0..120 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("flags-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("flags-hash-{}", index % 17),
                field: format!("field-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 5 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("flags-str-{}", index / 2),
                    value: vec![b'w'; 64],
                },
            });
        }
        if index % 7 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("flags-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 11 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("flags-str-{}", index / 3),
                },
            });
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct BucketFlags {
        in_memory: bool,
        deleted: bool,
        dirty: bool,
        has_ttl: bool,
        layout: String,
        object_index: Vec<u64>,
        block_count: usize,
    }

    let capture = |engine: &TemporalEngine| -> std::collections::BTreeMap<u32, BucketFlags> {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| {
                (
                    *routing_bucket,
                    BucketFlags {
                        in_memory: bucket.in_memory(),
                        deleted: bucket.deleted(),
                        dirty: bucket.dirty(),
                        has_ttl: bucket.ttl_ms.is_some(),
                        layout: format!("{:?}", bucket.layout),
                        object_index: bucket.object_index.iter().copied().collect(),
                        block_count: bucket.block_index.len(),
                    },
                )
            })
            .collect()
    };

    let after_targeted = capture(&engine);
    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        crate::engine::storage_bucket_internals::refresh_bucket_runtime_flags(shard);
    }
    let after_full_sweep = capture(&engine);

    assert!(
        !after_targeted.is_empty(),
        "workload produced no buckets, so the comparison would be vacuous"
    );

    let mut differences = Vec::new();
    for (routing_bucket, targeted) in &after_targeted {
        let swept = after_full_sweep
            .get(routing_bucket)
            .expect("the sweep cannot add or drop buckets");
        if targeted != swept {
            differences.push(format!(
                "  bucket {routing_bucket}: targeted {targeted:?} != swept {swept:?}"
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "the per-write targeted refresh left {} bucket(s) in a different state than a full sweep,          so a mutation site is not recording the bucket it touched:
{}",
        differences.len(),
        differences.join("
")
    );
    println!(
        "
  {} buckets compared; targeted refresh matches a full sweep on every flag
",
        after_targeted.len()
    );
}

/// Does `dirty_objects` say anything the blocks' own `dirty` flags do not?
///
/// It holds a `String` per record -- 19 B of key at this key length, plus a String header and a
/// BTreeSet node each -- and nothing drains it during a pure ingest: only the publish path or a
/// storage-manager cycle clears it. At 4.7M records that is several hundred MB held in a set the
/// ingest never releases. It is also the set whose per-entry iteration made the heartbeat
/// shard-sized.
///
/// Each `BlockIndex` already carries `dirty`. If the two agree, the set is derivable from the
/// blocks and its per-record String is duplicated state. If they disagree, it is carrying
/// something the flags cannot express, and this test says exactly what -- which is the part worth
/// knowing before anyone tries to remove it.
///
/// # THEY DISAGREE, AND THE SET IS NOT REMOVABLE
///
///     dirty_objects: 160
///     pages with dirty=true: 80
///              in the set but no page flagged dirty: 80
///              a page flagged dirty but not in the set: 0
///              in the set with NO live page at all: 80
///
/// Read the last two rows together and the answer is exact. **Nothing is dirty without being in the
/// set** -- zero the other way -- so the flags are a strict subset and the set is not merely a
/// cache of them. And **half the set has no live page at all**: this workload's 100 string writes
/// mark both the data object and its `control_state:cpc:*` record, and a control-state record is
/// not page-backed, so there is no `dirty` flag anywhere that could carry its dirtiness.
///
/// So the set holds exactly what the flags cannot express: dirty objects with no page. Removing it
/// would silently lose them, which is a wrong dump rather than a slower one.
///
/// WHAT THE SET STILL COSTS, because "not removable" is not "free". The `String` per record this
/// doc opens with is no longer the shape: both marking sites read `any_shared_object_key` first and
/// hand the page index's handle to `insert_shared`, so a key the page index already holds costs a
/// pointer rather than a second copy of the characters. An ORDERING still defeats that on an
/// object's first write -- the mark runs before the page is filed, so there is no handle to hand
/// over -- which `DirtyObjectIndex::adopt_shared_key` exists to repair afterwards, and which was
/// measured at 24.0 B/key across 40,040 keys with ZERO of this index's allocations shared while it
/// went unrepaired. The per-entry cost that remains is the `DirtyKeySet` arm, which is why
/// `bucket_arms` reports (Empty, One, Many) rather than a length.
///
/// A report, not a threshold: it prints the comparison and asserts only that the workload
/// produced something to compare.
#[test]
fn dirty_objects_versus_the_blocks_own_dirty_flags() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "dirty-duplication".to_string(),
                shard_uri: "local://dirty-duplication/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..100 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dd-{index}"),
                value: vec![b'v'; 48],
            },
        });
        if index % 5 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("dd-{}", index / 3),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let blocks_marked_dirty: std::collections::BTreeSet<String> =
        super::blocks_with_identity(&shard.bucket_index)
            .into_iter()
            .filter(|(_, _, _, block)| block.dirty)
            .map(|(_, _, object_key, _)| object_key.to_string())
            .collect();
    let any_block_at_all: std::collections::BTreeSet<String> =
        super::blocks_with_identity(&shard.bucket_index)
            .into_iter()
            .map(|(_, _, object_key, _)| object_key.to_string())
            .collect();

    let in_set_not_flagged: Vec<&str> = shard
        .dirty_objects
        .iter()
        .filter(|k| !blocks_marked_dirty.contains(*k))
        .collect();
    let flagged_not_in_set: Vec<&String> = blocks_marked_dirty
        .iter()
        .filter(|k| !shard.dirty_objects.contains(*k))
        .collect();
    let in_set_with_no_block: usize = shard
        .dirty_objects
        .iter()
        .filter(|k| !any_block_at_all.contains(*k))
        .count();

    assert!(
        !shard.dirty_objects.is_empty(),
        "workload left nothing in dirty_objects to compare"
    );
    println!(
        "
  dirty_objects: {}
  pages with dirty=true: {}
           in the set but no page flagged dirty: {}
           a page flagged dirty but not in the set: {}
           in the set with NO live page at all: {}
",
        shard.dirty_objects.len(),
        blocks_marked_dirty.len(),
        in_set_not_flagged.len(),
        flagged_not_in_set.len(),
        in_set_with_no_block,
    );
    if let Some(sample) = in_set_not_flagged.first() {
        println!("  e.g. in the set, no dirty page: {sample}");
    }
}

// `components_of_one_object_stay_separate` WAS HERE, AND IT WAS ALREADY RED BEFORE THIS COMMIT.
//
// It asserted that a six-field hash yields six separately-addressable, ordered component entries,
// that an unwritten field is absent rather than empty, that a hash object has no componentless
// entry, and that a string object's one entry is reachable through `refs_for(None)`.
//
// TWO OF THOSE WERE FALSE ON THE BASE COMMIT, 75a25910e, and in OPPOSITE directions -- which is
// why it is recorded here rather than quietly rewritten. `refs_for(Some("field-0")).is_some()`
// could not hold: the element name came off the page entry one commit earlier, so every page of
// every object filed under one nameless slot and a named query had nothing to find. And
// `grouped.refs_for(None).is_none()` -- "a hash object has no componentless entry" -- was wrong
// the other way, because the nameless slot is now exactly where a hash object's pages live.
//
// WHY IT IS NOT REWRITTEN. The only claim that survives is "six written fields produce six block
// refs", and that is a claim about `BlockRefs` accumulating, not about per-component grouping --
// `the_insert_accumulates_an_objects_blocks_into_the_one_slot` in `engine_storage_state.rs` holds it directly,
// against the producer. The `Some`-misses half would restate `names_the_only_slot`, which
// `a_named_component_resolves_nothing_while_the_unnamed_one_resolves_everything` already asserts
// in both directions in one arm so that it cannot pass one-sided.
//
// ONE CLAIM FROM THE ELEMENT-NAME BRANCH'S REWRITE IS CARRIED RATHER THAN DROPPED. That branch kept
// this arm alive as `one_objects_pages_are_one_lookup_row_holding_every_ref` and moved the order
// claim -- "removal binary-searches this vector, so its order is load-bearing" -- down from
// `by_component` onto the refs. The rewrite as a whole cannot come across, because it reads
// `grouped.by_component` and `refs_for(..)` and neither exists. The claim it moved IS live:
// `BlockRefs::insert` keeps the refs sorted and `BlockRefs::remove` binary-searches them, so an
// `insert` rewritten to append would make `remove` miss a ref that is present. Nothing else in the
// tree asserted it. It is restated against the producer, beside
// `the_insert_accumulates_an_objects_blocks_into_the_one_slot` in `engine_storage_state.rs`.
//
// AND RESTATED ON THE WHOLE REF, NOT RE-GOLDENED. That branch's version sorted a PROJECTION --
// `refs.iter().map(|r| r.block_ref_key)` -- while `BlockLookupRef` derives `Ord` over
// `(routing_bucket, block_ref_key)`. Those are different orders, agreeing only while the routing
// buckets happen to ascend with the keys, so the projection could pass over a vector `remove`
// could not search. The restatement compares the refs themselves, over a fixture inserted
// DESCENDING so that an append and a sorted insert cannot produce the same vector.

/// The common case takes the inline arm, and the spilled arm still behaves.
///
/// Measuring that 100% of components hold one ref only justifies the shape; it does not show the
/// shape is being used. A structure that silently spilled every time would measure identically
/// from the outside and cost an allocation each, so the arm is asserted directly.
#[test]
fn single_block_components_are_held_inline() {
    use crate::engine::state::{BlockLookupRef, BlockRefs};

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("inline-{index:04}"),
                value: vec![b'v'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut inline = 0usize;
    let mut spilled = 0usize;
    for entry in shard.bucket_index.object_block_lookup.values() {
        // ONE SLOT PER OBJECT NOW, so this is a census of the object's own ref list rather than of
        // each component's. `BlockRefs` is unchanged and the question is the same one: does a
        // single-block object spill its refs to a vector, or stay inline.
        match entry {
            BlockRefs::One(_) => inline += 1,
            BlockRefs::Many(_) => spilled += 1,
        }
    }
    assert!(inline + spilled > 0, "no object slots were recorded; nothing was measured");
    assert_eq!(
        spilled, 0,
        "{spilled} of {} object slots allocated a vector for a single ref",
        inline + spilled
    );

    // The spilled arm has to keep working: sorted, deduplicated, and reporting what it added.
    let first = BlockLookupRef { routing_bucket: 7, block_ref_key: 2 };
    let second = BlockLookupRef { routing_bucket: 7, block_ref_key: 1 };
    let mut refs = BlockRefs::One(first.clone());
    assert!(!refs.insert(first.clone()), "re-inserting the same ref adds nothing");
    assert_eq!(refs.len(), 1);
    assert!(refs.insert(second.clone()), "a second ref is an addition");
    assert_eq!(refs.len(), 2);
    assert_eq!(
        refs.as_slice(),
        &[second, first],
        "promotion must land sorted -- removal binary-searches these"
    );
}

/// The index on disk does not change shape.
///
/// This is held inline in memory but has to serialize as the sequence it always was, or an index
/// written before this stops loading. Both directions are checked, including a spilled arm read
/// back as an inline one.
#[test]
fn block_refs_serialize_as_a_sequence() {
    use crate::engine::state::{BlockLookupRef, BlockRefs};

    let single = BlockRefs::One(BlockLookupRef {
        routing_bucket: 3,
        block_ref_key: 1,
    });
    let json = serde_json::to_value(&single).unwrap();
    assert!(json.is_array(), "must encode as a sequence, got {json}");
    assert_eq!(json.as_array().unwrap().len(), 1);
    assert_eq!(json[0]["routing_slot"], 3);

    // A one-element sequence written by the previous shape comes back inline, not spilled.
    let restored: BlockRefs = serde_json::from_value(json).unwrap();
    assert!(
        matches!(restored, BlockRefs::One(_)),
        "a one-element sequence should load into the inline arm"
    );
    assert_eq!(restored, single);

    let mut pair = single.clone();
    pair.insert(BlockLookupRef {
        routing_bucket: 4,
        block_ref_key: 2,
    });
    let round_tripped: BlockRefs = serde_json::from_value(serde_json::to_value(&pair).unwrap()).unwrap();
    assert_eq!(round_tripped, pair);
    assert_eq!(round_tripped.len(), 2);
}

/// Blocks of the same kind hold ONE BYTE for it and no string at all.
///
/// THIS TEST USED TO ASSERT SOMETHING WEAKER, and the difference is the point. The field was an
/// `Arc<str>` interned through a per-shard pool, so the strongest thing available was "every block
/// after the first of its kind points at that first allocation" -- true, but it still spent a
/// sixteen-byte fat pointer per block and a heap allocation per shard, and a block that had built
/// its own copy would have looked identical from outside. The field is now the one-byte spelling
/// off `model_kind_registry`, so there is no allocation to share: every block of a kind reads the
/// same `&'static str`, and it reads it out of one byte of the entry.
///
/// Still compares POINTERS, for the reason it always did -- contents cannot tell one shared
/// spelling from two equal ones -- and still refuses to score on a corpus where no kind repeats.
#[test]
fn blocks_of_one_kind_spend_one_byte_and_share_one_static_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..200 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("shared-kind-{index:05}"),
                value: vec![b'v'; 32],
            },
        });
    }
    for index in 0..40 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("shared-kind-hash-{index:05}"),
                field: "f".to_string(),
                value: vec![b'h'; 32],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut first_of_kind: std::collections::HashMap<String, *const u8> =
        std::collections::HashMap::new();
    let mut blocks = 0usize;
    let mut shared = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, block) in bucket.block_index.iter() {
            blocks += 1;
            let spelling = block.model_id.as_str().as_ptr();
            match first_of_kind.get(block.model_id.as_str()) {
                None => {
                    first_of_kind.insert(block.model_id.to_string(), spelling);
                }
                Some(first) => {
                    if *first == spelling {
                        shared += 1;
                    }
                }
            }
        }
    }
    // The width is half the claim and it is the half a pointer census cannot see.
    assert_eq!(
        1,
        std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>(),
        "the entry's model spelling is supposed to be one byte",
    );

    // Anti-vacuity first: with no blocks, or one block per kind, "everything is shared" is true for
    // free and proves nothing.
    assert!(blocks > 0, "no pages were recorded; nothing was measured");
    assert!(
        blocks > first_of_kind.len(),
        "only {} pages for {} kinds -- no kind repeats, so sharing is untested",
        blocks,
        first_of_kind.len()
    );
    assert_eq!(
        shared,
        blocks - first_of_kind.len(),
        "every page after the first of its kind should read the same static spelling; \
         {} kinds over {} pages, {shared} shared",
        first_of_kind.len(),
        blocks
    );
    assert!(
        first_of_kind.len() >= 2,
        "corpus produced one kind; a second is needed to show the pool distinguishes them"
    );
}

/// The observed range of every numeric field in a block address.
///
/// A field is only narrowable if something real bounds it. Slab geometry bounds a slab id, an
/// offset within a slab and a block length; a hash bounds nothing. This reports the maximum each
/// field actually reaches so the distinction is measured rather than assumed -- a field that looks
/// small in one corpus because the corpus is small would otherwise read as narrowable.
#[test]
fn what_each_address_field_actually_ranges_over() {
    const BLOCKS: usize = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        2 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..BLOCKS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("range-{index:07}"),
                value: vec![b'v'; 96],
            },
        });
    }
    for object in 0..120 {
        for field in 0..6 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("range-hash-{object:06}"),
                    field: format!("f{field}"),
                    value: vec![b'h'; 96],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut blocks = 0usize;
    let (mut slab, mut offset, mut length) = (0u64, 0u64, 0u64);
    let (mut block_id, mut object_id, mut generation, mut derived_slab) = (0u64, 0u64, 0u64, 0u64);
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_key, block) in bucket.block_index.iter() {
            blocks += 1;
            let a = &block.address;
            slab = slab.max(a.block_slab_id());
            offset = offset.max(a.offset());
            length = length.max(a.length());
            block_id = block_id.max(a.block_id().unwrap_or(0));
            generation = generation.max(a.generation().unwrap_or(0));
            derived_slab = derived_slab.max(a.slab_id().unwrap_or(0));
        }
    }
    assert!(blocks > 0, "no pages were recorded; nothing was measured");

    let bits = |v: u64| if v == 0 { 0 } else { 64 - v.leading_zeros() };
    println!(
        "
  address field ranges over {blocks} pages (max observed, and bits to hold it):
    block_slab_id  {slab:>22}  {:>2} bits   bounded by slab count
    offset        {offset:>22}  {:>2} bits   bounded by slab size
    length        {length:>22}  {:>2} bits   bounded by page size
    page_id       {block_id:>22}  {:>2} bits
    object_id     {object_id:>22}  {:>2} bits   a hash -- bounded by nothing
    generation    {generation:>22}  {:>2} bits
    slab_id       {derived_slab:>22}  {:>2} bits

    the routing bucket is no longer a field of the address at all: a page's
    bucket is `block_routing_bucket(object_key, ..)` over the range the store
    is stamped with, so the container answers for it

    a maximum observed here is NOT a bound: it says a field is a candidate,
    not that it is safe. Narrowing one needs the bound asserted where the
    value is produced, so a violation fails loudly instead of truncating.
",
        bits(slab), bits(offset), bits(length), bits(block_id),
        bits(object_id), bits(generation), bits(derived_slab),
    );
}

/// How many separate allocations one object's key text occupies across the whole shard.
///
/// The per-structure censuses each answer a smaller question than this one. Sharing is worth doing
/// where the same bytes are held many times, and that only shows up when the structures are
/// counted together.
#[test]
fn how_many_times_one_object_key_is_stored() {
    use std::collections::HashSet;

    const OBJECTS: usize = 800;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("crossref-object-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    // Pick one object and find every place its text is stored, by pointer.
    let sample = shard
        .strings()
        .keys()
        .next()
        .expect("the workload must produce at least one string object")
        .clone();

    let mut allocations: HashSet<*const u8> = HashSet::new();
    let mut holders = 0usize;

    for key in shard.strings().keys() {
        if *key == sample {
            allocations.insert(key.as_ptr());
            holders += 1;
        }
    }
    for key in shard.expires_at_ms().keys() {
        if **key == *sample {
            allocations.insert(key.as_ptr());
            holders += 1;
        }
    }
    for (_model, object, _refs) in shard.bucket_index.object_block_lookup.iter() {
        if object.as_ref() == &*sample {
            allocations.insert(object.as_ptr());
            holders += 1;
        }
    }
    // THE PAGE INDEX IS NO LONGER A HOLDER, AND THAT IS THE FINDING RATHER THAN A GAP IN THE
    // CENSUS. This walk added one holder and one allocation per PAGE of the sampled object. A
    // page entry has no key, so there is nothing to insert and nothing to count -- and the
    // allocation those pages pointed at was the lookup's, which is counted one loop above.
    //
    // IT IS REMOVED RATHER THAN LEFT COUNTING THE LOOKUP'S COPY THROUGH THE PAGES. Reaching the
    // lookup's `Arc` by walking pages and charging it once per page would report the cost as if
    // nothing had changed, which is exactly the mistake the `model_id` comment in
    // `blocks_of_one_kind_spend_one_byte_and_share_one_static_spelling` names.
    assert!(holders > 0, "the sampled key was found nowhere; nothing was measured");
    // AND THE PAGE INDEX IS NOT EMPTY, so the holder above being absent cannot be read as "this
    // store has no pages". Without this the removal above and an empty fixture look identical.
    assert!(
        shard
            .bucket_index
            .bucket_map
            .values()
            .any(|bucket| !bucket.block_index.is_empty()),
        "the store holds no page at all, so the page index's absence from the holder list above          says nothing about where this key is held"
    );

    let key_bytes: usize = shard.strings().keys().map(|key| key.len()).sum();
    // STRUCTURALLY ZERO, AND KEPT AS A ROW RATHER THAN DELETED. The page index held one `Arc<str>`
    // per entry; it holds none, so its contribution to the key text of this store is nothing at
    // all. The row stays in the table because the table is read as "where the key copies live",
    // and a row that has gone to zero is the answer to that question rather than an absence from
    // it. The per-object cost it used to duplicate is the `object_page_lookup keys` row below.
    let block_key_bytes: usize = 0;
    // No concatenation any more: what the lookup holds per object is the object key itself.
    let lookup_key_bytes: usize = shard
        .bucket_index
        .object_block_lookup
        .iter()
        .map(|(_model, object, _refs)| object.len())
        .sum();
    let expiry_key_bytes: usize = shard.expires_at_ms().keys().map(|key| key.len()).sum();
    let total = key_bytes + block_key_bytes + lookup_key_bytes + expiry_key_bytes;

    println!(
        "
  one object key ({} B of text), across the shard:
    structures holding it        {holders}
    distinct allocations         {}   <- what sharing would collapse to 1

  and in total over {OBJECTS} objects:
    strings keys              {key_bytes:>8} B
    page_index object_key     {block_key_bytes:>8} B
    object_page_lookup keys   {lookup_key_bytes:>8} B   (its own allocation of the key)
    expires_at_ms keys        {expiry_key_bytes:>8} B
    TOTAL                     {total:>8} B  = {:.1} B per object
",
        sample.len(),
        allocations.len(),
        total as f64 / OBJECTS as f64,
    );
}

/// The lookup is nested in memory and flat on disk.
///
/// This is the property the change rests on: an index written before the nesting has to load, and
/// one written now has to stay readable by anything expecting the old shape. Both directions are
/// checked, and the composite is compared literally rather than by round-tripping alone -- a
/// round-trip through a consistently wrong format would pass.
#[test]
fn the_nested_lookup_still_serializes_as_the_flat_composite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "nested-key".to_string(),
            value: vec![b'v'; 32],
        },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let lookup = &shard.bucket_index.object_block_lookup;
    assert!(!lookup.is_empty(), "the write must produce a lookup entry");

    let json = serde_json::to_value(lookup).unwrap();
    let object = json.as_object().expect("the wire form is a flat map");

    // Length-prefixed parts: model, then object key.
    let expected = format!("{}:{}|{}:{}|", "string".len(), "string", "nested-key".len(), "nested-key");
    assert!(
        object.contains_key(&expected),
        "the serialized key must be the flat composite {expected:?}, got {:?}",
        object.keys().collect::<Vec<_>>()
    );

    // And a document in that shape loads back into the nested form.
    let restored: crate::engine::state::ObjectBlockLookup =
        serde_json::from_value(json).unwrap();
    assert!(
        restored.get("string", "nested-key").is_some(),
        "a flat document must load into the nested map"
    );
    assert_eq!(restored.len(), lookup.len());
}

/// The block entry and the lookup hold the SAME allocation of an object's key.
///
/// Compared by pointer, not by contents. Changing the field's type shares nothing on its own --
/// the lookup previously called `Arc::from` and built a second allocation of text the block already
/// owned, which is byte-for-byte identical from the outside and costs exactly as much as before.
/// Only pointer identity can tell those apart.
#[test]
fn the_block_and_the_lookup_point_at_one_object_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("shared-object-{index:04}"),
                value: vec![b'v'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    // THE CLAIM MOVED FROM THE ENTRY'S COPY TO THE ONE THE READ PATH HANDS OUT, and it is the
    // same claim about the same allocation.
    //
    // WHAT IT ASSERTED: the `Arc` on the page and the `Arc` in the lookup are ONE allocation, not
    // two copies of one string. The entry has no `Arc` now -- so taken literally the claim is
    // true of nothing, and a test that asserted it would pass for free.
    //
    // WHAT IT ASSERTS INSTEAD: the identity a reader is HANDED for a page is the lookup's own
    // allocation. Every production reader reaches a page's object through `BlockKeysByPosition`,
    // which is built by inverting the lookup, and `block_identities_in` hands back
    // `Arc::clone` of what it found. A clone shares the buffer, so `as_ptr` must equal
    // `key_ptr`'s -- and if the inversion ever started copying the text instead, this reddens.
    // That is a load-bearing property rather than a curiosity: forty walks in this module tree
    // take a key per page that way, and a copy per page would put the duplication this change
    // removed straight back, one level out.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut checked = 0usize;
    let mut shared = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (ref_key, block) in bucket.block_index.iter() {
            let handed_out = named.at(bucket.routing_bucket, *ref_key);
            let Some(refs) = shard
                .bucket_index
                .object_block_lookup
                .key_ptr(block.model_id.as_str(), handed_out.as_ref())
            else {
                continue;
            };
            checked += 1;
            if std::ptr::eq(refs, handed_out.as_ptr()) {
                shared += 1;
            }
        }
    }

    // Anti-vacuity: with nothing checked, "all shared" is true for free.
    assert!(checked > 0, "no page was matched to a lookup entry; nothing was measured");
    assert_eq!(
        shared, checked,
        "{shared} of {checked} pages are handed an object key that is the lookup's own          allocation; for the rest the read path made a COPY of the text, which is the duplication          this change removed reappearing one level out"
    );
}

/// A block filed with an address that carries no object id still knows which object it belongs to.
///
/// This is the one way removing the entry's own copy could lose information. The id is computed as
/// `address.object_id().unwrap_or_else(stable_block_object_id)`, so before this change the entry
/// could hold a fallback the address did not have. Reading through the address would then answer
/// zero for exactly those blocks. The write path now puts the computed id into the address; this
/// asserts it, because the census that motivated the removal cannot see the case at all -- every
/// address in it already carried an id.
#[test]
fn a_block_whose_address_carries_no_object_id_still_reports_one() {
    use crate::block_store::ElementEntry;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        // Deliberately no object id, and no routing slot either -- which is now EVERY address,
        // since the field is gone. What is under test is that the ENTRY still reports one.
        let address = ElementEntry::from_parts(0, 0, 16, Some(7), None);
        crate::engine::storage_bucket_internals::upsert_bucket_index_block(
            shard,
            1,
            "string",
            "orphan-key",
            None,
            address,
            false,
        );
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut seen = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (ref_key, block) in bucket.block_index.iter() {
            let object_key = named.at(bucket.routing_bucket, *ref_key);
            if object_key.as_ref() != "orphan-key" {
                continue;
            }
            seen += 1;
            assert_ne!(
                block.object_id(1, object_key),
                0,
                "a page filed from an address with no object id must still report the computed one"
            );
            // Cross-checked against what the write path recorded, rather than by recomputing
            // the same function and comparing it with itself: the bucket's object index was
            // populated from the id the write path actually used.
            assert!(
                bucket.object_index.contains(&block.object_id(1, object_key)),
                "the id the page reports must be the one the write path filed it under"
            );
        }
    }
    assert_eq!(seen, 1, "the page must be in the index, or nothing was tested");
}

/// Deleting an object costs a bounded number of allocations, not one per block examined.
///
/// The scan compared each block's key against the wanted one by building a fresh `Arc<str>` from
/// the wanted key inside the loop -- a heap allocation and a copy of the key per block, to compare
/// and immediately drop. Twelve sites did this. Borrowing compares the same bytes with no
/// allocation at all.
///
/// Measured as allocations rather than time because the allocation IS the cost here, and counted
/// with `allocs` rather than `outstanding`: this pattern frees everything it takes, so a live-heap
/// measurement reports zero while the work still happens.
#[test]
#[cfg(feature = "alloc-probe")]
fn deleting_an_object_does_not_allocate_per_block_scanned() {
    let delete_from_a_hash_of = |fields: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for field in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "wide".to_string(),
                    field: format!("f{field}"),
                    value: vec![b'v'; 16],
                },
            });
        }

        let probe = crate::alloc_probe::Probe::start();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "wide".to_string(),
            },
        });
        probe.stop().allocs
    };

    let narrow = delete_from_a_hash_of(20);
    let wide = delete_from_a_hash_of(200);

    println!("delete allocations: {narrow} at 20 fields, {wide} at 200 fields");

    // Guard the guard: an upper bound passes hardest at zero, so prove the probe saw the work.
    assert!(narrow > 0, "the probe must observe the delete: {narrow}");

    // 180 more blocks to scan. Per-block allocation would put ~180 extra allocations here.
    let growth = wide.saturating_sub(narrow);
    assert!(
        growth < 180,
        "deleting scanned 180 more pages and allocated {growth} more times          ({narrow} at 20 fields, {wide} at 200) -- that is per-page allocation"
    );
}

/// What happens to the index when the same logical object is written twice.
///
/// Reported, not asserted into a particular answer: the point is to find out whether the map
/// deduplicates by key collision or through the lookup, because that decides whether the key can
/// become an opaque assigned id.
#[test]
fn rewriting_a_block_does_not_reuse_its_index_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "rewritten-object";
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'a'; 64] },
    });
    let after_first: Vec<String> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        super::blocks_with_identity(&shard.bucket_index)
            .into_iter()
            .filter(|(_, _, object_key, _)| object_key.as_ref() == key)
            .map(|(_, handle, _, _)| handle.to_string())
            .collect()
    };

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'b'; 64] },
    });
    let after_second: Vec<String> = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        super::blocks_with_identity(&shard.bucket_index)
            .into_iter()
            .filter(|(_, _, object_key, _)| object_key.as_ref() == key)
            .map(|(_, handle, _, _)| handle.to_string())
            .collect()
    };

    assert_eq!(after_first.len(), 1, "the first write must produce exactly one entry");
    println!(
        "
  rewriting one object:
    entries after the first write   {}
    entries after the second        {}
    key reused?                     {}
    first  key: {}
    second key: {}
",
        after_first.len(),
        after_second.len(),
        after_second == after_first,
        after_first.first().map(String::as_str).unwrap_or("-"),
        after_second.first().map(String::as_str).unwrap_or("-"),
    );

    // The property that must hold either way: one live entry per object, however that is achieved.
    assert_eq!(
        after_second.len(),
        1,
        "a rewrite must leave one entry, not accumulate them"
    );
}

/// The same block, installed twice, is one entry with one handle.
///
/// This is the property the rendered string key provided for free: it WAS the key, so installing
/// a block whose identity already appeared replaced it. A handle from a counter compiles, dumps
/// and reloads perfectly and still breaks this -- each install takes a fresh slot, so a rebuild
/// accumulates entries and the object counts drift apart. Three tests caught that as a wrong
/// number; this one states the reason.
#[test]
fn installing_the_same_block_twice_replaces_it() {
    let block = || crate::engine::state::BlockIndex {
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        address: ElementEntry::from_parts(1, 0, 4, Some(1), Some(30)),
        dirty: false,
        deleted: false,
    };

    let mut map = crate::engine::state::BlockIndexMap::default();
    let mut live = crate::engine::state::BlockSlabLiveIndex::default();
    let object_key: Arc<str> = Arc::from("twice");
    let first = map.insert(&object_key, block(), &mut live);
    let second = map.insert(&object_key, block(), &mut live);

    assert_eq!(first, second, "the same page must land on the same handle");
    assert_eq!(map.len(), 1, "installing it twice must not add a second entry");

    // A block differing in one identity field is a different block and keeps its own slot.
    let mut moved = block();
    moved.address = ElementEntry::from_parts(1, 64, 4, Some(1), Some(30));
    let third = map.insert(&object_key, moved, &mut live);
    assert_ne!(first, third, "a page at another offset is not the same page");
    assert_eq!(map.len(), 2);

    // The tally follows the same rule the map does. Installing the same block twice charges it
    // once, because the second install discharges the address it displaced; a block at another
    // offset is a second block and is charged as one.
    assert_eq!(
        live.tally(1),
        crate::engine::state::SlabLiveTally {
            block_refs: 2,
            bytes: 8
        },
        "two distinct 4-byte pages on slab 1"
    );
}

/// Writing the block index does not build a second copy of it first.
///
/// `#[serde(into = "...")]` is defined as `T::from(self.clone()).serialize(..)`, so it duplicates
/// the whole map -- once for the clone, once for the converted map -- before a byte is written.
/// The `Arc`s inside are refcount bumps rather than allocations, so what this actually counts is
/// the duplicated map structure, which is why it is measured rather than asserted.
#[test]
#[cfg(feature = "alloc-probe")]
fn dumping_the_block_index_does_not_copy_it_first() {
    let dump_a_shard_of = |blocks: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for field in 0..blocks {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "dumped".to_string(),
                    field: format!("f{field}"),
                    value: vec![b'v'; 16],
                },
            });
        }
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|b| !b.block_index.is_empty())
            .expect("the writes must produce pages");

        let probe = crate::alloc_probe::Probe::start();
        let json = serde_json::to_string(&crate::engine::state::BlockIndexNamedBy(&bucket.block_index, &["dumped"]))
            .expect("the index serializes");
        let counts = probe.stop();
        assert!(!json.is_empty());
        counts.allocs
    };

    let small = dump_a_shard_of(20);
    let large = dump_a_shard_of(200);
    println!("dump allocations: {small} at 20 pages, {large} at 200 pages");

    // An upper bound passes most easily when nothing was measured, so prove the probe saw work.
    assert!(small > 0, "the probe must observe the dump: {small}");

    // Writing a block costs its key and little else. Copying the index first, or building that key
    // with `format!` (which allocates its own buffer and then allocates again to return it), put
    // this near four.
    let per_block = (large.saturating_sub(small)) as f64 / 180.0;
    assert!(
        per_block < 2.0,
        "dumping cost {per_block:.2} allocations per page ({small} at 20, {large} at 200)"
    );
}

/// The dump lists blocks in written-key order.
///
/// Worth pinning because the in-memory key stopped being the written one. The index used to be
/// serialized by converting it into a map keyed by the rendered string, which emitted entries in
/// string order for free. This map is keyed by a handle and iterates in hash order, so anything
/// writing it has to restore that order deliberately -- otherwise the bytes on disk change even
/// though every entry is still present and correct.
#[test]
fn the_block_index_dump_is_ordered_by_its_written_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for field in 0..24 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "ordered".to_string(),
                field: format!("f{field:02}"),
                value: vec![b'v'; 16],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .values()
        .find(|b| b.block_index.len() > 4)
        .expect("the writes must produce several pages in one bucket");

    let json = serde_json::to_string(&crate::engine::state::BlockIndexNamedBy(&bucket.block_index, &["ordered"]))
        .expect("the index serializes");
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
    let keys: Vec<String> = value
        .as_object()
        .expect("a map of rendered keys")
        .keys()
        .cloned()
        .collect();

    assert!(keys.len() > 4, "need several keys to see an order: {keys:?}");

    // THE ORDER IS READ OFF THE EMITTED BYTES, NOT OFF THE PARSED VALUE.
    //
    // `serde_json::Value` holds an object in a map that sorts its keys, so `.keys()` on a parsed
    // value comes back in sorted order whatever the dump actually emitted. Comparing that against
    // its own sorted copy is an identity: it passes for every possible dump order, including a
    // reversed one. This test was written that way and could not fail; a mutation that reversed
    // the sort inside `BlockIndexMap::serialize` survived it until the order was taken from the
    // string. The parsed value is still used, but only for the SET of keys.
    let mut placed: Vec<(usize, String)> = Vec::with_capacity(keys.len());
    for key in &keys {
        let needle = format!("\"{key}\":");
        let at = json
            .find(&needle)
            .unwrap_or_else(|| panic!("the emitted dump does not contain the key {key:?}"));
        placed.push((at, key.clone()));
    }
    assert_eq!(
        keys.len(),
        placed.len(),
        "every key must have been located in the emitted bytes"
    );
    placed.sort_by_key(|(at, _)| *at);
    let emitted: Vec<String> = placed.into_iter().map(|(_, key)| key).collect();

    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(
        emitted, sorted,
        "the dump must be ordered by written key; it emitted {emitted:?}"
    );

    // AND THE CONTROL: the emitted order must be something this comparison could have found
    // wrong. If the keys were already all equal, or there were one of them, the assertion above
    // would hold for any implementation at all.
    assert!(
        emitted.first() != emitted.last(),
        "the emitted keys are not distinguishable, so the order assertion is vacuous"
    );
}

/// A command does not allocate to discover that nothing has expired.
///
/// Lazy expiry runs on every command -- 51 call sites -- and used to build four owned keys per
/// call to look them up in a map that, on a store with no TTLs, is empty. One of those keys came
/// from `format!`. All four were dropped immediately after the lookup.
///
/// Counted as allocations rather than time because the allocation is the whole cost: the work
/// this does on a store without expiries is a single `is_empty` check.
#[test]
#[cfg(feature = "alloc-probe")]
fn a_command_does_not_allocate_to_check_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let reads = |n: usize| -> f64 {
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..n {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("absent{i:04}"),
                },
            });
        }
        probe.stop().allocs as f64 / n as f64
    };

    let per_read = reads(200);
    println!("miss costs {per_read:.1} allocations");

    // An upper bound passes most easily when nothing happened, so prove work was observed.
    assert!(per_read > 1.0, "the probe must observe the reads: {per_read}");

    // The expiry check accounted for five of these. The rest is the lookup itself, which this
    // bound deliberately leaves room for -- it is here to catch the per-command allocation
    // coming back, not to freeze the whole read path.
    assert!(
        per_read < 38.0,
        "a read cost {per_read:.1} allocations; lazy expiry is allocating again"
    );
}

/// The block index is keyed by a number in memory and by the old string on disk.
///
/// This is what makes the numeric key an in-memory change rather than a format change. Asserted on
/// the literal key rather than by round-tripping alone: a round trip through a consistently wrong
/// spelling would pass, and an index written now has to be readable by something expecting the old
/// one.
#[test]
fn the_block_index_still_writes_string_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: "dumped".to_string(), value: vec![b'v'; 32] },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .values()
        .find(|bucket| !bucket.block_index.is_empty())
        .expect("the write must produce a page");

    // In memory: a handle, not text.
    let (handle, block) = bucket.block_index.iter().next().expect("one page");
    assert!(*handle > 0, "the map assigns a handle");

    // On the wire: the same rendered key it always wrote, rebuilt from the block AND the identity
    // beside it -- which is where the renderer takes it from now.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let json = serde_json::to_value(&crate::engine::state::BlockIndexNamedBy(&bucket.block_index, &["dumped"])).unwrap();
    let written = json.as_object().expect("the wire form is a map of string keys");
    let expected = crate::engine::state::block_index_written_key(
        named.at(bucket.routing_bucket, *handle),
        block,
    );
    assert!(
        written.contains_key(&expected),
        "the dump must carry the rendered key {expected:?}, got {:?}",
        written.keys().collect::<Vec<_>>()
    );
    assert!(
        expected.contains("dumped"),
        "and that key names the object: {expected}"
    );

    // And a document in that shape loads back, with handles assigned afresh.
    let restored: crate::engine::state::BlockIndexMap = serde_json::from_value(json).unwrap();
    assert_eq!(restored.len(), bucket.block_index.len());
    assert!(
        restored.iter().all(|(handle, _)| *handle > 0),
        "every loaded page gets a handle"
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

/// One delete costs the same whether the store holds 200 keys or 3,200.
///
/// It did not. Deleting an object rebuilt the object-to-block lookup for the entire shard, which
/// clears it, clones every block in every bucket into a vector and re-inserts them -- so a delete
/// allocated in proportion to the whole store, and deleting a store cost the square of its size.
/// Measured over 400 deletes: 610 allocations each at 200 resident keys and 4,053 at 3,200.
///
/// Counted rather than timed because the count is the cost, and compared as a ratio across store
/// sizes rather than against a fixed number, since what matters is that it stopped scaling.
#[test]
#[cfg(feature = "alloc-probe")]
fn does_delete_scale_with_the_store() {
    let cost_at = |resident: usize| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("resident{i:06}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        // Keys to delete, beyond the resident set.
        for i in 0..400 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("victim{i:04}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..400 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("victim{i:04}"),
                },
            });
        }
        let c = probe.stop();
        (c.allocs as f64 / 400.0, c.alloc_bytes as f64 / 400.0)
    };

    let (small, small_bytes) = cost_at(200);
    let (large, large_bytes) = cost_at(3200);
    println!("delete at 200: {small:.1} allocs / {small_bytes:.0} B, at 3200: {large:.1} / {large_bytes:.0} B");

    // A ratio test passes trivially if neither side did anything.
    assert!(small > 1.0, "the probe must observe deletes: {small}");

    // Sixteen times the store. Rebuilding put this at 6.6x; the establishing rebuild that still
    // happens once per shard leaves a little slope, which 400 deletes amortise to nearly nothing.
    let growth = large / small;
    assert!(
        growth < 1.5,
        "a delete cost {small:.1} allocations at 200 resident keys and {large:.1} at 3,200          ({growth:.1}x) -- it is scaling with the store again"
    );
    let byte_growth = large_bytes / small_bytes;
    assert!(
        byte_growth < 1.6,
        "a delete allocated {small_bytes:.0} bytes at 200 keys and {large_bytes:.0} at 3,200          ({byte_growth:.1}x)"
    );
}

/// Exploratory: one block per field, or one block rewritten per write?
#[test]
fn how_many_blocks_does_a_wide_hash_hold() {
    for fields in [10usize, 100, 400] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("f{i:06}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let blocks: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum();
        let buckets = shard.bucket_index.bucket_map.len();
        let lookup_refs: usize = shard
            .bucket_index
            .object_block_lookup
            .iter()
            .map(|(_m, _o, refs)| refs.len())
            .sum();
        println!("fields {fields:5}  pages {blocks:6}  buckets {buckets:4}  lookup refs {lookup_refs:6}");
    }
}

/// Every field of a hash is filed in the bucket index after it is written.
///
/// The per-write sync used to re-file every field of the object each time, which made this true
/// by brute force. It now files only the fields that are missing, so the property has to be
/// checked rather than assumed: skipping one leaves the index quietly short of a block, and a
/// missing block is a read that returns nothing rather than an error.
#[test]
fn every_field_of_a_hash_is_filed_in_the_bucket_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..64 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{i:03}"),
                value: vec![b'v'; 32],
            },
        });
    }
    // Overwrite some, so a field's address changes and the old filing is stale.
    for i in (0..64).step_by(5) {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{i:03}"),
                value: vec![b'w'; 48],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let fields = shard.hashes.get("wide").expect("the hash exists");
    assert_eq!(fields.len(), 64, "all fields written");

    // THE LOOKUP TAKES NO ELEMENT NAME NOW, AND THE ADDRESS IS STILL WHAT IS CHECKED.
    //
    // This passed `Some(field.as_str())`. `contains_object_block_address` ends with
    // `&& component.is_none()` -- the old `page.component.as_deref() == component` with the
    // entry's own constant folded in -- so a `Some` can never match and every field read as
    // UNFILED: "field f000 holds an address the bucket index does not have filed", for a field
    // that was filed. The object's pages are all under its single componentless row, and what is
    // asserted is unchanged: each field's address is among them, including the overwritten ones
    // whose old filing is stale.
    for (field, address) in fields.iter() {
        assert!(
            shard
                .bucket_index
                .contains_object_block_address("hash", "wide", None, address),
            "field {field} holds an address the bucket index does not have filed"
        );
        // AND THE ELEMENT-NAME AXIS IS ASSERTED GONE, so the line above cannot quietly become
        // redundant. If the lookup starts answering by field name again, the name is back on the
        // stored entry and this arm says so rather than passing on the componentless row alone.
        assert!(
            !shard.bucket_index.contains_object_block_address(
                "hash",
                "wide",
                Some(&field[..]),
                address
            ),
            "the lookup answered for field {field} BY NAME. An entry carries no element name, so \
             a named lookup must resolve nothing -- if it does, the name has returned to the entry"
        );
    }
}

/// Exploratory: which bucket-visiting site grows with a wide object?
#[test]
#[cfg(feature = "alloc-probe")]
fn which_site_visits_the_blocks() {
    use crate::engine::storage_bucket_internals::bucket_visit_sites;
    for fields in [100usize, 400, 1600] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..fields {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("f{i:06}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        bucket_visit_sites::reset();
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..100 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "one".to_string(),
                    field: format!("probe{i:04}"),
                    value: vec![b'v'; 32],
                },
            });
        }
        let allocs = probe.stop().allocs as f64 / 100.0;
        let (layout, clear_dirty, refresh_flags, remove_all) = bucket_visit_sites::snapshot();
        println!(
            "fields {fields:5}  allocs/write {allocs:8.1}  visits/write: layout {:7.1} clear_dirty {:7.1} refresh {:7.1} remove_all {:7.1}",
            layout as f64 / 100.0,
            clear_dirty as f64 / 100.0,
            refresh_flags as f64 / 100.0,
            remove_all as f64 / 100.0
        );
    }
}

/// Exploratory: do writes cost more as the store grows?
#[test]
#[cfg(feature = "alloc-probe")]
fn does_writing_scale_with_the_store() {
    let cost_at = |resident: usize, wide_object: bool| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            if wide_object {
                // Everything under ONE object key: the object's own ref list grows.
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "one".to_string(),
                        field: format!("f{i:06}"),
                        value: vec![b'v'; 32],
                    },
                });
            } else {
                // Spread across many object keys: the store grows, each object stays small.
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("h{i:06}"),
                        field: "f".to_string(),
                        value: vec![b'v'; 32],
                    },
                });
            }
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..100 {
            if wide_object {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "one".to_string(),
                        field: format!("probe{i:04}"),
                        value: vec![b'v'; 32],
                    },
                });
            } else {
                engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("probe{i:04}"),
                        field: "f".to_string(),
                        value: vec![b'v'; 32],
                    },
                });
            }
        }
        let c = probe.stop();
        (c.allocs as f64 / 100.0, c.alloc_bytes as f64 / 100.0)
    };

    let (narrow_small, _) = cost_at(100, false);
    let (narrow_large, _) = cost_at(1600, false);
    let (wide_small, _) = cost_at(100, true);
    let (wide_large, _) = cost_at(1600, true);
    println!(
        "many objects {narrow_small:.1} -> {narrow_large:.1}   one wide object {wide_small:.1} -> {wide_large:.1}"
    );

    // A ratio test proves nothing if neither side did any work.
    assert!(wide_small > 1.0, "the probe must observe writes: {wide_small}");

    // Writing to a store with 16x the objects already cost the same; this is the guard on it.
    assert!(
        narrow_large / narrow_small < 1.3,
        "a write cost {narrow_small:.1} allocations at 100 objects and {narrow_large:.1} at 1,600"
    );

    // Writing a field to an object that already has 1,600 of them cost 8,388 allocations against
    // 800 at 100 fields, because the per-write sync re-filed every field the object had.
    assert!(
        wide_large / wide_small < 1.3,
        "a write cost {wide_small:.1} allocations at 100 fields and {wide_large:.1} at 1,600          ({:.1}x) -- the write is scaling with the object again",
        wide_large / wide_small
    );
}

/// Every key the shard index writes, listed.
///
/// A guard for renaming Rust identifiers without moving the format under the data. Most of these
/// fields carry no `serde` attribute, so their Rust name IS their wire name -- renaming one
/// changes what is written, and an index written by the new code would not be read by the old.
/// Listing them makes that visible in a diff instead of silent.
///
/// The derived object-to-block lookup is deliberately absent. It duplicates block refs the bucket
/// index already carries, so it is `skip_serializing` and rebuilt on load -- which is only
/// safe while loading actually rebuilds it. `a_reload_rebuilds_the_lookup_the_index_no_longer_writes`
/// holds that half; this one holds that the format stays as small as that change made it.
#[test]
fn the_block_index_entry_count_falls_when_entries_go() {
    // `block_index_entries` was fed `page_store.writes`, a counter that only ever increases, so
    // the published entry count could never fall -- after a delete it still reported every write
    // the shard had ever taken. An "entry count" that cannot go down is not a count.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    for index in 0..40usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("block-entry-{index:04}"),
                value: vec![b'v'; 16],
            },
        });
    }
    let filled = engine.get_stats(1).stats.expect("stats for a loaded shard");
    let entries_before = filled.storage.block_index_entries;
    let writes_before = filled.storage.block_writes;
    assert!(entries_before > 0, "40 writes should produce entries");

    for index in 0..40usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringDelete {
                key: format!("block-entry-{index:04}"),
            },
        });
    }
    let emptied = engine.get_stats(1).stats.expect("stats for a loaded shard");

    assert!(
        emptied.storage.block_index_entries < entries_before,
        "deleting every object must lower the entry count: {} -> {}",
        entries_before,
        emptied.storage.block_index_entries
    );
    // The control: writes DID keep climbing over the same span, which is what the entry count
    // used to be reporting. If the two moved together this test would prove nothing.
    assert!(
        emptied.storage.block_writes >= writes_before,
        "the write counter is monotonic and is a separate number"
    );
}

/// Is the lifecycle PLAN the fixed cost, or is it the rest of the apply?
///
///   cargo test --release -p temporalstore-rust --lib what_the_lifecycle_plan_costs_alone -- --ignored --nocapture --test-threads=1
///
/// `apply_storage_lifecycle` costs ~635 ms at 20k objects with every optional flag off, and the
/// first thing it does unconditionally is `storage_lifecycle_plan`. The plan was already the
/// subject of one large cut. This times the plan on its own against the whole call, so the
/// remaining fixed cost is attributed to one side or the other rather than guessed at.
#[test]
#[ignore]
fn what_the_lifecycle_plan_costs_alone() {
    fn bare_request(shard_id: ShardId) -> crate::engine::reports::StorageLifecycleRequest {
        crate::engine::reports::StorageLifecycleRequest {
            shard_id,
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
            warm_cache: false,
        }
    }

    fn build(objects: usize) -> (tempfile::TempDir, TemporalEngine) {
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
                    key: format!("plan-cost-{index:06}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        (dir, engine)
    }

    for objects in [5_000usize, 20_000] {
        let (_d1, engine) = build(objects);
        let started = std::time::Instant::now();
        let plan = engine.storage_lifecycle_plan(bare_request(1));
        let plan_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = plan;

        let (_d2, engine2) = build(objects);
        let started = std::time::Instant::now();
        let response = engine2.apply_storage_lifecycle(bare_request(1));
        let apply_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = response;

        // The SAME engine again. A cycle calls this three times, so if the second call is cheap
        // the cost is first-call initialisation and not per-call work -- which is also the only
        // way three calls fit inside a cycle shorter than three times this number.
        let started = std::time::Instant::now();
        let second = engine2.apply_storage_lifecycle(bare_request(1));
        let second_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = second;
        let started = std::time::Instant::now();
        let third = engine2.apply_storage_lifecycle(bare_request(1));
        let third_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = third;
        eprintln!(
            "  [plan] {objects:>6}   apply again on the SAME engine: 2nd {second_ms:>8.1} ms | 3rd {third_ms:>8.1} ms",
        );

        eprintln!(
            "  [plan] {objects:>6} objects: plan alone {plan_ms:>8.1} ms | whole apply {apply_ms:>8.1} ms | plan is {:>5.1}% of it",
            if apply_ms > 0.0 { plan_ms / apply_ms * 100.0 } else { 0.0 },
        );

        // The three calls the apply makes UNCONDITIONALLY after the plan. Note the manifest
        // prune PLAN is computed even when pruning is switched off -- only the apply of it is
        // gated -- so it is paid on every round regardless.
        let (_d3, engine3) = build(objects);
        let started = std::time::Instant::now();
        let lifecycle = engine3.storage_object_lifecycle_snapshot_for_test(1);
        let lifecycle_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = lifecycle;

        let started = std::time::Instant::now();
        let roll_forward = engine3.bucket_dump_install_roll_forward_reports(1);
        let roll_forward_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = roll_forward;

        let started = std::time::Instant::now();
        let prune_plan =
            engine3.bucket_dump_manifest_prune_plan_with_follower_cursors(1, Vec::new());
        let prune_plan_ms = started.elapsed().as_secs_f64() * 1000.0;
        let _ = prune_plan;

        eprintln!(
            "  [plan] {objects:>6}   of which: object_lifecycle_snapshot {lifecycle_ms:>8.1} ms | roll_forward_reports {roll_forward_ms:>7.1} ms | manifest_prune_PLAN {prune_plan_ms:>7.1} ms",
        );

        // The four sample builders the apply runs unconditionally at the end, purely to fill in
        // REPORT fields. Timed under the same read lock the apply takes.
        {
            let shards = engine3.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");

            let started = std::time::Instant::now();
            let index_snapshot = crate::engine::storage_bucket_internals::
                storage_index_snapshot_with_samples(1, shard, Default::default());
            let index_ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = index_snapshot;

            let started = std::time::Instant::now();
            let watermark = crate::engine::storage_bucket_internals::
                storage_watermark_snapshot_with_samples(1, shard, Default::default());
            let watermark_ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = watermark;

            let started = std::time::Instant::now();
            let gc = crate::engine::storage_bucket_internals::
                storage_gc_snapshot_with_samples(1, shard, Default::default());
            let gc_ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = gc;

            let started = std::time::Instant::now();
            let topology = crate::engine::storage_bucket_internals::
                storage_topology_snapshot_with_samples(1, shard, Default::default());
            let topology_ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = topology;

            eprintln!(
                "  [plan] {objects:>6}   SAMPLE BUILDERS: index {index_ms:>7.1} | watermark {watermark_ms:>7.1} | gc {gc_ms:>7.1} | topology {topology_ms:>7.1}  (sum {:>7.1} ms)",
                index_ms + watermark_ms + gc_ms + topology_ms,
            );
        }
    }
}

/// Where the reclaim_index stage's time actually goes.
///
///   cargo test --release -p temporalstore-rust --lib where_the_index_reclaim_time_goes -- --ignored --nocapture --test-threads=1
///
/// The stage is the most expensive one in a maintenance cycle -- 750 ms of a 1,336 ms cycle at
/// 20k objects, and roughly linear in store size. It turns on three things at once
/// (`purge_delayed_destroy`, `prune_bucket_dump_manifests`, `roll_forward_bucket_dump_installs`),
/// and nothing said which of them costs. Same ablation as the stage-level probe, one level down:
/// all three on, then each turned off in turn.
#[test]
#[ignore]
fn where_the_index_reclaim_time_goes() {
    fn request(
        purge: bool,
        prune: bool,
        roll_forward: bool,
    ) -> crate::engine::reports::StorageLifecycleRequest {
        crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            purge_delayed_destroy: purge,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: prune,
            roll_forward_bucket_dump_installs: roll_forward,
            follower_replay_cursors: Vec::new(),
            block_gc_shared_store_cursors: Vec::new(),
            block_gc_raft_snapshot_refs: Vec::new(),
            block_gc_checkpoint_floor_slab_id: None,
            block_gc_raft_install_floor_slab_id: None,
            block_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        }
    }

    fn time_once(objects: usize, req: crate::engine::reports::StorageLifecycleRequest) -> f64 {
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
                    key: format!("index-gc-{index:06}"),
                    value: vec![b'v'; 96],
                },
            });
        }
        let started = std::time::Instant::now();
        let response = engine.apply_storage_lifecycle(req);
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let _ = response;
        elapsed
    }

    for objects in [5_000usize, 20_000] {
        let all_on = time_once(objects, request(true, true, true));
        eprintln!("  [index_gc] {objects:>6} objects, all three on -> {all_on:>9.1} ms");
        for (name, req) in [
            ("purge_delayed_destroy", request(false, true, true)),
            ("prune_manifests", request(true, false, true)),
            ("roll_forward_installs", request(true, true, false)),
            ("ALL THREE OFF", request(false, false, false)),
        ] {
            let without = time_once(objects, req);
            eprintln!(
                "  [index_gc] {objects:>6}   without {name:<22} {without:>9.1} ms   (costs {:>8.1} ms)",
                all_on - without,
            );
        }
    }
}

#[test]
fn the_resident_bucket_count_is_not_the_routing_range() {
    // `slot_index_entry_count` was published from `bucket_entries`, which is the routing RANGE --
    // u32::MAX on a default shard -- so the metric read 4,294,967,295 per shard whatever the
    // index actually held, and the per-shard values are SUMMED. A count that cannot move is not
    // a measurement.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let empty = engine.get_stats(1).stats.expect("stats for a loaded shard");
    assert_eq!(empty.storage.bucket_index_resident_entries, 0);
    // The control: the old source reads u32::MAX on this very same empty shard.
    assert_eq!(empty.storage.bucket_entries, u32::MAX as u64);

    for index in 0..300usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("resident-{index:05}"),
                value: vec![b'v'; 16],
            },
        });
    }

    let filled = engine.get_stats(1).stats.expect("stats for a loaded shard");
    assert!(
        filled.storage.bucket_index_resident_entries > 0
            && filled.storage.bucket_index_resident_entries <= 300,
        "300 objects must give a resident count in range, got {}",
        filled.storage.bucket_index_resident_entries
    );
    // And the floor stays consistent with it -- one node per resident bucket.
    assert_eq!(
        filled.storage.bucket_index_resident_bytes_floor,
        filled.storage.bucket_index_resident_entries
            * std::mem::size_of::<crate::engine::state::BucketNode>() as u64
    );
}

#[test]
fn the_stats_report_a_floor_on_what_the_bucket_index_costs() {
    // Every other memory number this engine emits is the CACHE's. The bucket index grows one
    // entry per stored object and is never evicted, so a shard can hold gigabytes of it while
    // every published signal reads near zero. This is not a gate -- it is the number being
    // visible at all.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // CONTROL: an empty shard has no buckets, so the floor is genuinely zero here -- which is
    // what makes a non-zero reading below mean something.
    let empty = engine.get_stats(1).stats.expect("stats for a loaded shard");
    assert_eq!(empty.storage.bucket_index_resident_bytes_floor, 0);
    // And NOT from , which is the routing RANGE: it reads u32::MAX here, so a
    // floor derived from it would report ~927 GB for an empty shard. That is how this was first
    // written, and this control is what caught it.
    assert_eq!(empty.storage.bucket_entries, u32::MAX as u64);

    for index in 0..500usize {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("index-cost-{index:05}"),
                value: vec![b'v'; 32],
            },
        });
    }

    let filled = engine.get_stats(1).stats.expect("stats for a loaded shard");
    assert!(
        filled.storage.bucket_entries > 0,
        "the fixture should have produced buckets"
    );
    assert!(
        filled.storage.bucket_index_resident_bytes_floor > 0,
        "500 stored objects must show a non-zero index cost"
    );
    assert_eq!(
        filled.storage.bucket_index_resident_bytes_floor
            % std::mem::size_of::<crate::engine::state::BucketNode>() as u64,
        0,
        "the floor is a whole number of nodes"
    );
    // And it tracks the index rather than the cache: the cache can be dropped without the index
    // going anywhere, which is the whole reason this number exists.
    let before = filled.storage.bucket_index_resident_bytes_floor;
    let _ = engine.cache().invalidate_shard(1);
    let after = engine.get_stats(1).stats.expect("stats for a loaded shard");
    assert_eq!(
        after.storage.bucket_index_resident_bytes_floor, before,
        "dropping the cache must not change what the INDEX costs"
    );
}

/// THE KEYS THE INDEX WRITES, LISTED, so a rename or a serde attribute cannot reach the format
/// quietly.
///
/// `last_dump_sequence` IS DELIBERATELY GONE FROM THIS LIST, and this guard is the one that caught
/// it going in: #1440 tried to persist a transient claim and this test refused the added key. It is
/// refusing a REMOVED key now, and the removal is intended. The node does not hold a
/// `last_dump_sequence` any more -- it was read into two reports and nothing else, and both take
/// the figure from the newest dump manifest -- so there is no value for the index to write.
///
/// What that costs, stated rather than discovered: an index this engine writes will not load in an
/// engine built before this change, which refuses a node that omits the key. The other direction
/// still works, and has its own guards: the key is still recognised, still decoded as a `u64` and
/// still refused twice on the way in, so every index ever written still loads here.
#[test]
fn the_index_wire_keys_are_what_they_were() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: "wire".to_string(), value: vec![b'v'; 32] },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "wire-hash".to_string(),
            field: "f".to_string(),
            value: vec![b'v'; 32],
        },
    });

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let value = serde_json::to_value(&shard.bucket_index).expect("the index serializes");

    // Every object key that appears anywhere in the document, deduplicated.
    fn collect(value: &serde_json::Value, into: &mut std::collections::BTreeSet<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    into.insert(key.clone());
                    collect(child, into);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    collect(item, into);
                }
            }
            _ => {}
        }
    }
    let mut keys = std::collections::BTreeSet::new();
    collect(&value, &mut keys);

    // The rendered block keys are data, not field names: they are built from the object's own
    // identity, so they vary with the test's keys rather than with the format.
    keys.retain(|key| !key.contains(':') && !key.chars().all(|c| c.is_ascii_digit()));

    let listed: Vec<&str> = keys.iter().map(String::as_str).collect();
    assert_eq!(
        listed,
        vec![
            // The MERGED ADDRESS WORD, which replaced "ps" and "o". One key where there were two:
            // the slab id is its high 32 bits and the offset its low 32. The old pair is NOT an
            // alias of this -- an index that spells them separately is refused at load, because
            // read as a word a slab id of 1 beside an offset of 2 is slab 0 offset 1, a well-formed
            // address for a different block.
            "a",
            "address",
            "bucket_map",
            // "b", the older grouping id, is gone: a slab IS the unit, so an address derives it from
            // the address word rather than carrying it. An index written before this still has the
            // key and still loads -- the wire struct does not deny unknown fields, so the stored
            // value is read and ignored.
            // "component" is GONE, and it is the key this change removes. `BlockIndex` has no
            // element-name field: all four container kinds file one entry a page, so an entry
            // names a page and has nothing to say about which element is on it. The entry went 56
            // bytes of field to 40 with it.
            //
            // AND IT OWES NO FORMAT STAMP, which is the part a reader of this list will want and
            // is argued in full beside `SHARD_INDEX_FORMAT_VERSION`. The short form: the load
            // refuses anything stamped below 12 and 12 was stamped BY the commit that made hash
            // and zset page-named, so every index this binary accepts was written by a binary that
            // filed no element name. The key this list stops expecting is a key no accepted index
            // contains; one that really carries it is stamped 10 or lower and is rebuilt from the
            // WAL.
            //
            // RESTATED RATHER THAN RE-GOLDENED, which for a list means the removed key is named
            // here rather than quietly absent -- the same treatment "log_backed", "h", "rs", "o",
            // "ps", "b" and "last_dump_sequence" already get below. A key that simply disappeared
            // from this vector would leave the next reader unable to tell a removal from an
            // oversight, which is the whole job of the list.
            "deleted",
            "deleted_object_index",
            "dirty",
            "dirty_generation",
            // "g" is the LAST optional slot on an address. It is written on every one of them,
            // because a row is read by position in the index log and a field that vanishes when
            // empty moves every field behind it -- and because its PRESENCE is the meaning:
            // `ADDRESS_HAS_GENERATION` comes from whether this key is there.
            "g",
            // "h", the digest, is GONE, and so is "rs" further down. Both were written as nil for
            // the positional reason above, and neither was ever read. A key that carries no
            // meaning does not need the slot held open for it: `shorter_struct_against_an_existing_row`
            // measures that a positional row longer than the struct is refused by LENGTH rather
            // than reinterpreted, so retiring a dead slot cannot shift anything silently.
            // An index already carrying "h" or "rs" still loads: the wire struct does not deny
            // unknown fields.
            "in_memory",
            "l",
            // "last_dump_sequence" is gone on purpose: see this test's own note above.
            "layout",
            "loading",
            // "log_backed" is GONE, and it is the only key this change removes. The entry shed the
            // flag nothing maintained -- the log-resident fact is derived from the address by an
            // accessor -- so the stored form is one key lighter.
            //
            // AND NOTHING ARRIVED IN ITS PLACE, which is the half worth asserting: the entry also
            // GAINED two locating fields absorbed from the index-log row, and both are skipped on
            // the named side. A resident entry is always a page, and the node already records its
            // own bucket. So neither appears in this list, and this list is what would have caught
            // them if they did.
            "meta_loaded",
            "model_id",
            // "o" and "ps" are gone -- merged into "a" at the top of this list.
            "object_index",
            "object_key",
            "oi",
            "page_index",
            "pi",
            // "routing_slot" here is the BUCKET NODE's own key, not the address's retired "rs" --
            // two different things that read alike. The node still records which bucket it is.
            "routing_slot",
            "ttl_ms",
        ],
        "the index writes different keys than it did; a rename or a serde attribute reached the format"
    );
}


/// A bucket holding one object id holds it inline, and goes back to inline when it can.
///
/// Keys route one to a bucket, so a bucket holds a single object unless its key is a COLLECTION:
/// the object id is hashed over `shard:kind:key:component` and the routing bucket over the key
/// alone, so a hash with eight fields files eight ids into one bucket. Measured, that is 86.98%
/// of buckets holding exactly one on a store of strings and series and 46.57% on a store with
/// collections in it -- this test drives the first case, and the inline arm is what makes it
/// free. A single-member collection behind the pointer costs 128 live bytes to carry eight of
/// id, which is what the inline arm is not spending.
///
/// The demotion is checked too. A bucket that briefly held two objects would otherwise keep its
/// node for the rest of its life, which is the cost being removed and shows up nowhere else.
#[test]
fn a_bucket_holding_one_object_holds_no_node() {
    use crate::engine::state::ObjectIndex;

    let mut index = ObjectIndex::default();
    assert!(index.is_empty());

    assert!(index.insert(7));
    assert!(matches!(index, ObjectIndex::One(7)), "one id is held inline");
    assert!(!index.insert(7), "the same id twice is one entry");
    assert_eq!(index.object_count(), 1);
    assert!(index.contains(&7));

    assert!(index.insert(9));
    assert!(matches!(index, ObjectIndex::Many(_)), "two ids need a set");
    assert_eq!(index.object_count(), 2);

    assert!(index.remove(&9));
    assert!(
        matches!(index, ObjectIndex::One(7)),
        "a set that drops back to one id must give up its node"
    );

    assert!(index.remove(&7));
    assert!(index.is_empty(), "and to nothing at all");
    assert!(!index.remove(&7), "removing what is not there changes nothing");

    // WHAT IT ITERATES AND WHAT IT WRITES ARE NOW THE SAME ORDER AGAIN, and that is the change.
    //
    // The multi arm is a SLOT ARRAY, so `iter` yields the order the ids were FILED -- a slot has to
    // mean the same object for as long as the object is in the bucket, which it cannot if an insert
    // reorders. This comment read "what is still ASCENDING is the written form, because that is the
    // order already on disk and the Serialize impl sorts to keep it there", and that was true until
    // a slot had to survive a load: a sorted spelling cannot carry a position. The written form is
    // the slot array now, nullable so a placeholder is a stored position, and the format stamp pays
    // for the bytes that moved. REVERSED here rather than re-goldened.
    //
    // `sorted_ids` is still ascending and still has exactly one caller that wants it -- the
    // TOMBSTONE side, which nothing names by slot and whose bytes therefore did not move.
    let mut many = ObjectIndex::default();
    many.extend([5u64, 1, 3]);
    let ids: Vec<u64> = many.iter().copied().collect();
    assert_eq!(ids, vec![5, 1, 3], "ids come out in the slots they were filed into");
    assert_eq!(many.sorted_ids(), vec![1, 3, 5], "and sorted on request");
    assert_eq!(
        serde_json::to_string(&many).expect("serializes"),
        "[5,1,3]",
        "and are WRITTEN in the slots they were filed into, which is what makes a stored slot \
         name one object across a load"
    );
    // AND THE CONTROL FOR THAT: the fixture's slot order is not its ascending order, so an
    // ascending Serialize impl could not pass the assertion above.
    assert_ne!(
        many.sorted_ids(),
        ids,
        "the fixture files its ids in ascending order, so the assertion above would hold for an \
         ascending written form too and says nothing about positions"
    );
    // A PLACEHOLDER IS A STORED POSITION, which is the other half of what moved on the wire.
    assert!(many.remove(&1), "the middle id is filed");
    assert_eq!(
        serde_json::to_string(&many).expect("serializes"),
        "[5,null,3]",
        "a hole is not written, so every slot above it closes up on the next load"
    );
    assert!(many.insert(1), "refilling the hole");
    assert_eq!(
        serde_json::to_string(&many).expect("serializes"),
        "[5,1,3]",
        "the first free slot was not the hole, so a refill appended instead"
    );

    // A single id writes the same shape, and reads back inline rather than as a set.
    let one: ObjectIndex = serde_json::from_str("[4]").expect("deserializes");
    assert!(matches!(one, ObjectIndex::One(4)), "a loaded single id is held inline");
    assert_eq!(serde_json::to_string(&one).expect("serializes"), "[4]");
}


/// Writing a message does not cost the length of its node's history.
///
/// The per-write index sync files every block its object has. Each upsert drops the entry's
/// existing refs before inserting, so filing a list leaves only its last element -- the rest are
/// removed again on the way past. A node holding 850 events therefore re-filed 850 blocks to add
/// its 851st, and filling a node cost the square of its length: 2,072 allocations per message at
/// 50 events, 23,822 at 800.
///
/// Bounded on the absolute cost at 800 rather than on a ratio, because a ratio would still pass
/// comfortably -- this is not flat yet. What remains is in the WAL append, measured at 8,700 of
/// the 8,800 allocations a write costs there and untouched by this.
#[test]
#[cfg(feature = "alloc-probe")]
fn writing_a_message_does_not_cost_its_nodes_history() {
    // A message as a caller would write one: some text, and an embedding.
    let dims = 384usize;
    let write = |engine: &TemporalEngine, i: usize, with_vector: bool| {
        let vector: Vec<f32> = if with_vector {
            (0..dims).map(|d| (d as f32) * 0.001 + i as f32).collect()
        } else {
            Vec::new()
        };
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 1,
                node_hash: 42,
                first_write_only: false,
                cold_storage: false,
                event: Box::new(ContextEvent {
                    event_id_hash: 1_000 + i as u64,
                    event_time_ms: 1_700_000_000_000 + i as u64,
                    ingestion_time_ms: 1_700_000_000_000,
                    kind: 1,
                    event_type: 2,
                    actor_hash: 7,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.5,
                    text: format!(
                        "message {i}: the quick brown fox jumps over the lazy dog, twice over"
                    ),
                    source_ref: format!("src/{i}"),
                    related_node_hashes: vec![42],
                    compact_attrs: Vec::new(),
                    vector,
                }),
            },
        });
    };

    let cost_at = |resident: usize| -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..resident {
            write(&engine, i, true);
        }
        let probe = crate::alloc_probe::Probe::start();
        for i in resident..resident + 50 {
            write(&engine, i, true);
        }
        probe.stop().allocs as f64 / 50.0
    };

    let narrow = cost_at(50);
    let wide = cost_at(800);
    println!("message write: {narrow:.0} allocations at 50 events, {wide:.0} at 800");

    // A bound passes most easily when nothing was measured.
    assert!(narrow > 1.0, "the probe must observe the writes: {narrow}");

    // Was 23,822 when the sync filed every block of the series.
    assert!(
        wide < 12_000.0,
        "a message cost {wide:.0} allocations on a node holding 800 events; the index sync is \
         filing the whole series again"
    );
}


/// What a key costs the index in live heap.
///
/// Measured as allocated-minus-freed rather than as a struct size, because the cost this bounds
/// was never in the struct: a bucket held its single block in a `BTreeMap`, whose node is sized
/// for eleven entries and cost 1,496 live bytes to carry 120 bytes of block. Holding one block
/// inline took a key from 2,336 live bytes to 1,085.
#[test]
#[cfg(feature = "alloc-probe")]
fn what_a_key_costs_the_index_in_live_heap() {
    let cost_at = |keys: usize| -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let probe = crate::alloc_probe::Probe::start();
        for i in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("k{i:07}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let counts = probe.stop();
        (counts.alloc_bytes as i64 - counts.free_bytes as i64) as f64 / keys as f64
    };

    let per_key = cost_at(2000);
    println!("live heap: {per_key:.0} bytes per key");

    // A bound passes most easily when nothing was measured.
    assert!(per_key > 100.0, "the probe must observe the writes: {per_key}");

    assert!(
        per_key < 1060.0,
        "a key costs {per_key:.0} live bytes; a bucket is holding a node for a single entry again"
    );
}

/// A bucket holding one block holds no NODE and no list buffer, and goes back to that when it can.
///
/// THE COST THIS TEST WAS WRITTEN AGAINST IS STILL AVOIDED, BY A DIFFERENT MEANS. It was written
/// when the multi-block arm was a `BTreeMap`, which cost 1,496 live bytes to carry a 120-byte block
/// because its node is sized for eleven -- making the containers about 70% of the index's live heap.
/// #1963 replaced that tree with a flat list, and the single-block arm then held its entry INLINE, so
/// a single-block bucket allocated nothing at all.
///
/// WHAT CHANGED IS WHERE THE SINGLE BLOCK LIVES, NOT WHETHER IT HAS AN ARM. The inline entry made
/// every bucket in the map pay the width of a whole entry, which at the shipped routing range almost
/// none could use, so the arm now holds a POINTER: one small allocation for the entry instead of 72
/// bytes in every node. `inline_arm_trade.rs` measures the trade at both ranges.
///
/// SO WHAT THIS DRIVES is both collapses -- to the single-block arm at one block and to nothing at
/// zero. They matter as much as the growth does, for the same reason: a bucket that briefly held
/// several blocks would otherwise keep their buffer for the rest of its life, and it would not show
/// up as a failure anywhere else. The per-key ceiling above is what fails if either stops happening.
///
/// AND A PER-ELEMENT REMOVAL NOW DEFERS THE FIRST COLLAPSE, which is why the middle of this test
/// reads differently than it used to. A removal keeps an entry pointing at the block that records it,
/// so deleting two of three fields leaves one live entry and two tombstones -- three entries, and the
/// arm correctly follows the count. The collapse is still driven, twice: once by writing the fields
/// back, which clears their tombstones through the upsert's own `retain` and is what bounds the cost
/// of the change at one entry per DISTINCT element removed; and once by the whole-object delete, which
/// keeps no tombstone because an object with no membership left has nothing for a block to keep true.
#[test]
fn a_bucket_holding_one_block_holds_no_node() {
    use crate::engine::state::BlockIndexMap;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // One block under an object: inline.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "solo".to_string(),
            value: vec![b'v'; 32],
        },
    });
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| bucket.block_index.len() == 1)
            .expect("the write must produce a bucket holding one page");
        // THE SINGLE-BLOCK ARM, WHICH NOW HOLDS ITS ENTRY BEHIND A POINTER. The arm survives; what
        // it costs the node does not. It used to hold the whole entry inline, so every bucket in
        // the map paid the width of an entry whether or not it held exactly one block -- and at the
        // shipped routing range almost none does.
        assert!(
            matches!(bucket.block_index, BlockIndexMap::One(..)),
            "a bucket holding one page must take the single-page arm; it holds {} page(s)",
            bucket.block_index.len()
        );
    }

    // Several components of one object: a map, which is what a map is for.
    for field in ["a", "b", "c"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: field.to_string(),
                value: vec![b'v'; 32],
            },
        });
    }
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| bucket.block_index.len() > 1)
            .expect("three fields must share a bucket");
        assert!(
            matches!(bucket.block_index, BlockIndexMap::Many(_)),
            "several pages belong in a map"
        );
    }

    // Down to one again: back to inline, not a map with one entry left in it.
    for field in ["a", "b"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashDelete {
                key: "wide".to_string(),
                field: field.to_string(),
            },
        });
    }
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| {
                bucket
                    .block_index
                    .iter()
                    .any(|(handle, _)| &**named.at(bucket.routing_bucket, *handle) == "wide")
            })
            .expect("the remaining field must still be filed");
        // THE DEMOTION IS NOW DEFERRED, NOT DEAD, AND THIS IS WHAT DEFERS IT.
        //
        // WHAT THIS ASSERTED BEFORE: `len() == 1` and the single-block arm, because deleting two of
        // three fields left one entry. `mark_bucket_index_block_deleted_with` was a `retain`
        // returning false, so a delete removed the entry and the map drained.
        //
        // A per-element removal now KEEPS an entry -- pointing at the block that records the removal,
        // so a membership derived from the blocks does not resurrect the element. So the map holds
        // ONE LIVE entry and TWO TOMBSTONES, three entries, and the arm follows the entry count,
        // which is correct: the bucket really is carrying three entries.
        //
        // THE COST IS REAL AND IS NOT A COUNT CHANGE. The collapse this test exists for trades a list
        // buffer sized for a whole growth step for one sized for a single entry, and a bucket with
        // tombstones does not get it until something collects them. Nothing collects them yet --
        // `compact_container_blocks_batched` reads the resident maps, which hold no removed elements --
        // so for such a bucket the buffer is held until a re-add clears the tombstones or a later
        // stage collects them. That is stated here rather than left to be rediscovered from the
        // per-key ceiling above, which is the assertion that would eventually fail for it.
        let live = bucket.block_index.values().filter(|block| !block.deleted).count();
        let tombstoned = bucket.block_index.values().filter(|block| block.deleted).count();
        let resident = shard.hashes.get("wide").map_or(0, |fields| fields.len());
        // RESTATED: THE LIVE ENTRY IS RETAINED TOO, NOT ONLY A TOMBSTONE ADDED.
        //
        // The paragraph above said "the map holds ONE LIVE entry and TWO TOMBSTONES, three
        // entries". It holds FIVE. A removal reaches the index through a door whose `retain`
        // matches `component.is_none()`, which a page-named entry cannot satisfy, so it keeps the
        // live entry over the page its element vacated AND appends a tombstone beside it. Two
        // removals therefore leave THREE live entries and two tombstones.
        //
        // The membership claim is asked of the resident map, which is the authority for which
        // fields exist and is what `HashLen` and `HashGetAll` answer from. The live entry count is
        // asserted beside it as the retention, so the cost is recorded where it is read rather
        // than folded into a number that looks like membership.
        assert_eq!(
            1, resident,
            "two of three fields were removed and the resident map holds {resident} field(s)"
        );
        assert_eq!(
            3, live,
            "two of three fields were removed and {live} live entries remain. Three is the \
             expected number and the retention described above -- the survivor plus one entry per \
             removal. If this is 1 the retirement of a vacated page's entry has landed, and this \
             assertion is the one to come and restate"
        );
        assert_eq!(
            2, tombstoned,
            "two fields were removed and {tombstoned} tombstone entries remain; one per removal is \
             what keeps the removals readable from the pages, and zero would mean a page-derived \
             membership resurrects both fields"
        );
        assert_eq!(
            live + tombstoned,
            bucket.block_index.len(),
            "the map holds {} entries where the live and tombstoned counts above sum to {}; the \
             two walks and `len()` must see the same set",
            bucket.block_index.len(),
            live + tombstoned
        );
        assert_eq!(
            5,
            bucket.block_index.len(),
            "the map holds {} entries where three live and two tombstones is five. This read \
             `3 == len()` under the belief that a removal replaced the live entry with a \
             tombstone; it ADDS one beside it, so two removals leave five entries rather than \
             three",
            bucket.block_index.len()
        );
        // AND THE ARM FOLLOWS THE ENTRY COUNT, which is the invariant this test is really about: the
        // arm must never disagree with `len()`, whatever the entries are.
        assert!(
            matches!(bucket.block_index, BlockIndexMap::Many(_)),
            "a page index holding five entries must be in the multi-entry arm"
        );
    }

    // AND THE COLLAPSE ITSELF STILL WORKS, driven where nothing defers it: writing the two fields
    // BACK clears their tombstones through the upsert's retain, and then removing them from a kind
    // that keeps no tombstone drains the map as it always did.
    //
    // WITHOUT THIS THE INVERSION ABOVE WOULD HAVE DELETED THE TEST'S SUBJECT. The demotion is the
    // thing the module cares about; asserting only that it is deferred would leave nothing driving
    // that it happens at all.
    for field in ["a", "b"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: field.to_string(),
                value: vec![b'v'; 32],
            },
        });
    }
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        let bucket = shard
            .bucket_index
            .bucket_map
            .values()
            .find(|bucket| {
                bucket
                    .block_index
                    .iter()
                    .any(|(handle, _)| &**named.at(bucket.routing_bucket, *handle) == "wide")
            })
            .expect("the object must still be filed");
        let live = bucket.block_index.values().filter(|block| !block.deleted).count();
        let tombstoned = bucket.block_index.values().filter(|block| block.deleted).count();
        let resident = shard.hashes.get("wide").map_or(0, |fields| fields.len());
        // THE TOMBSTONE CLAIM HELD AND WAS CHECKED RATHER THAN ADJUSTED. The message below is the
        // one this arm shipped with, and the measurement still agrees with it: writing a field back
        // clears its tombstone through the upsert's retain, so the tombstone cost is bounded at one
        // entry per DISTINCT element removed rather than one per removal.
        assert_eq!(
            0, tombstoned,
            "writing a field back did not clear its tombstone: {tombstoned} remain. The upsert's \
             `retain` matches on the component, so a re-add takes the tombstone with it -- which is \
             what bounds the cost of this change at one entry per DISTINCT element removed rather \
             than one per removal."
        );
        // THE LIVE COUNT IS FIVE, AND THE RE-ADD DOES NOT BOUND THAT ONE. Three fields are
        // resident, and the two entries the removals left over the pages their elements vacated are
        // STILL THERE -- the re-add wrote each field to a NEW address, so it superseded nothing at
        // the old one. This asserted 3 on the belief that a removal replaced its entry; it adds one
        // beside it, and a remove-then-re-add leaves that one behind for good.
        //
        // The membership is asked of the resident map, which is the authority, so the five is read
        // as the retention it is rather than as a field count.
        assert_eq!(
            3, resident,
            "writing the two fields back left the resident map holding {resident} field(s), not 3"
        );
        assert_eq!(
            5, live,
            "writing the two fields back left {live} live entries. Five is the expected number: \
             three resident fields plus the two entries the removals left over the vacated pages, \
             which a re-add at a NEW address does not supersede. If this is 3 the retirement of a \
             vacated page's entry has landed, and this assertion is the one to come and restate"
        );
    }
    // Now the whole object, through the path that keeps no tombstone, and the map must DRAIN.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonDelete {
            key: "wide".to_string(),
        },
    });
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        let held: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| {
                bucket
                    .block_index
                    .iter()
                    .filter(|(handle, _)| {
                        &**named.at(bucket.routing_bucket, **handle) == "wide"
                    })
                    .count()
            })
            .sum();
        assert_eq!(
            0, held,
            "a whole-object delete left {held} entries for the object, tombstones included -- there \
             is no membership left to keep true once the object is gone, so an entry retained here \
             is leaked for the life of the store"
        );
    }
}










/// An `ObjectIndex` is two words, so the multi-entry arm cannot widen the bucket that is not on
/// it.
///
/// Held inline, the collection made the field as wide as the arm most buckets were not using.
/// The number here is what stops that from silently coming back: a future arm holding anything
/// larger than a pointer widens every bucket in the shard.
///
/// `BucketNode` used to carry two of these. The tombstone side is now `DeletedObjectIndex`,
/// which is one nullable pointer, because the case IT is almost always in is the empty one --
/// a different distribution wanting a different free tier. `how_many_objects_a_bucket_holds_at
/// _two_corpus_sizes_and_two_shape_mixes` reports both.
#[test]
fn an_object_index_costs_a_word_and_a_pointer() {
    use crate::engine::state::ObjectIndex;
    assert_eq!(
        std::mem::size_of::<ObjectIndex>(),
        2 * std::mem::size_of::<usize>(),
        "ObjectIndex should be a discriminant and one pointer-sized payload"
    );

    // And the shape it takes is still the shape the bucket needs: one id inline, two in a set.
    let mut index = ObjectIndex::default();
    assert!(index.is_empty());
    assert!(index.insert(7));
    assert_eq!(index.object_count(), 1);
    assert!(index.contains(&7));
    assert!(index.insert(9));
    assert_eq!(index.object_count(), 2);
    assert_eq!(index.iter().copied().collect::<Vec<_>>(), vec![7, 9]);
    // BACK DOWN TO ONE ID, AND WHETHER THE ALLOCATION GOES BACK NOW DEPENDS ON WHICH SLOT IS LEFT.
    //
    // 7 is in slot 0 and 9 in slot 1. Removing 7 leaves the survivor in SLOT 1, and collapsing to
    // `One` would renumber it to slot 0 and retire slot 1 -- which is the collapse #2023 drove as
    // "the collapse retired ordinal 1". So the arm STAYS, placeholder and all.
    assert!(index.remove(&7));
    assert!(
        matches!(index, ObjectIndex::Many(_)),
        "the arm collapsed with a live id in slot 1, renumbering the survivor"
    );
    assert_eq!(index.object_count(), 1, "one id is left");
    assert_eq!(index.id_at(0), None, "slot 0 is a placeholder");
    assert_eq!(index.id_at(1), Some(9), "slot 1 still names 9");

    // AND THE OTHER DIRECTION DOES GIVE IT BACK, because nothing is renumbered: removing the id in
    // the LAST slot leaves the survivor in slot 0 with nothing above it, so `One` and a one-slot
    // array are indistinguishable through `id_at`.
    let mut other = ObjectIndex::default();
    assert!(other.insert(7));
    assert!(other.insert(9));
    assert!(other.remove(&9));
    assert!(
        matches!(other, ObjectIndex::One(7)),
        "an arm whose survivor is in slot 0 must give the allocation back; it is {other:?}"
    );
}

// `one_component_is_held_without_a_vector` WAS HERE.
//
// It drove `ComponentList`'s promotion and demotion ladder -- `Empty` to `One` to `Many` and back
// down -- and its doc was right that the DEMOTION was the half worth a test: an object that
// briefly held two components would otherwise keep the vector for the rest of its life, and that
// would not have shown up as a failure anywhere else.
//
// There is no ladder left. One slot per object is an `Option`, so promotion is assignment and
// demotion is `Option::take`; a rewrite of this arm onto the new shape asserts that an `Option` is
// `None` before you assign to it, which is `Option`'s definition and not a property of this
// crate. Its third claim -- that `binary_search_by` places `None` at position 0 and a named
// component after it -- has no analogue either, because there are no positions.
//
// The measurement in its doc, "the measured average is 1.0 components per object", is what paid
// for the deletion. It is recorded beside `ObjectBlockRefs` in `engine_storage_state.rs`, where the shape it
// justifies now lives.

/// A bucket summary reads BOTH spellings, and now writes the short one.
///
/// This was the first half of a two-step change, and the second half has been taken. A dump
/// manifest crosses engines during install and these fields carry no serde default, so a writer
/// that switched before every reader knew the short names would hand over a document the reader
/// cannot parse at all. Readers learned first; the writer switched once an operator confirmed the
/// fleet, which is the one fact this code cannot check for itself.
///
/// The property to pin is now: short names parse, long names parse, what gets written is short,
/// and the switch still writes long when asked. The first two matter most -- they are what makes
/// a directory holding manifests written BOTH ways readable end to end, which is exactly what
/// flipping the default leaves behind.
#[test]
fn a_bucket_summary_reads_short_field_names_too() {
    use crate::engine::reports::BucketStorageSummary;

    let long = r#"{"routing_slot":7,"object_count":1,"block_ref_count":2,"logical_bytes":3,
        "physical_bytes":4,"dirty_object_count":5,"dirty_generation":6,"last_dump_sequence":8,
        "block_slab_ids":[9]}"#;
    let short = r#"{"rs":7,"oc":1,"prc":2,"lb":3,"pb":4,"doc":5,"dg":6,"lds":8,"psi":[9]}"#;

    let from_long: BucketStorageSummary =
        serde_json::from_str(long).expect("the long names must still parse");
    let from_short: BucketStorageSummary =
        serde_json::from_str(short).expect("the short names must parse");
    assert_eq!(from_long, from_short, "both spellings must mean the same summary");
    assert_eq!(from_long.routing_bucket, 7);
    assert_eq!(from_long.block_slab_ids, vec![9]);

    // And writing is now short by default -- the saving this whole change exists for.
    let written = serde_json::to_string(&from_long).expect("serialize");
    assert!(
        written.contains("\"oc\""),
        "the default write should use the short names: {written}"
    );
    assert!(
        !written.contains("\"object_count\""),
        "the default write should not carry the long names: {written}"
    );

    // The escape hatch still writes long, for a fleet that holds an older reader. Named
    // explicitly rather than through the process-wide switch, which is shared state several
    // hundred sites in this crate already race over.
    let long_written =
        serde_json::to_string(&crate::engine::reports::SummaryNamed(&from_long, false))
            .expect("serialize long");
    assert!(
        long_written.contains("\"object_count\""),
        "the long spelling must still be reachable: {long_written}"
    );
    let round_trip: BucketStorageSummary =
        serde_json::from_str(&long_written).expect("the long spelling must parse back");
    assert_eq!(round_trip, from_long, "the long spelling must round-trip");

    // What the second step would be worth, on this one summary.
    println!(
        "  ALIASES long {} B, short {} B ({:.2}x smaller if writers ever switch)",
        written.len(),
        short.len(),
        written.len() as f64 / short.len() as f64
    );
}

/// A dump manifest written in the old array shape still loads.
///
/// `index_bytes` carries the index image and is written encoded now, because the array shape costs
/// three to four characters per byte and this field holds the whole image. That is only safe
/// because the reader takes either shape -- which is a claim in a doc comment until something
/// feeds it an array-shaped document and checks the bytes come back.
#[test]
fn a_manifest_written_as_an_array_of_numbers_still_loads() {
    use crate::engine::reports::BucketDumpManifest;

    let image: Vec<u8> = (0..=255u8).collect();
    let as_numbers = image
        .iter()
        .map(|byte| byte.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let document = format!(
        r#"{{"version":1,"shard_id":1,"manifest_id":"m-1","created_unix_ms":0,"bucket_ids":[7],"block_slab_ids":[],"wal_sequence":3,"index_log_sequence":4,"live_block_refs":0,"logical_bytes":0,"physical_bytes":0,"bucket_summaries":[],"index_bytes":[{as_numbers}],"index_sha256":"","checksum":""}}"#
    );

    let loaded: BucketDumpManifest =
        serde_json::from_str(&document).expect("an array-shaped manifest must still deserialize");
    assert_eq!(loaded.index_bytes, image, "every byte must survive the old shape");
    assert_eq!(loaded.bucket_ids, vec![7], "the rest of the document must load too");

    // And what it writes now is the compact shape, which the reader also takes.
    let rewritten = serde_json::to_string(&loaded).expect("serialize");
    assert!(
        !rewritten.contains("\"index_bytes\":[1,2,3"),
        "the encoded shape must not be an array of numbers: {}",
        &rewritten[..rewritten.len().min(200)]
    );
    let round_tripped: BucketDumpManifest =
        serde_json::from_str(&rewritten).expect("the shape it writes must be readable");
    assert_eq!(round_tripped.index_bytes, image, "round trip must preserve the image");

    // The encoded document must actually be smaller, or the change bought nothing.
    assert!(
        rewritten.len() < document.len(),
        "encoded {} should be smaller than array {}",
        rewritten.len(),
        document.len()
    );
    println!(
        "  MANIFEST array {} B -> encoded {} B ({:.2}x smaller)",
        document.len(),
        rewritten.len(),
        document.len() as f64 / rewritten.len() as f64
    );
}

/// End-to-end write amplification: every durable byte a write causes, over the bytes it carries.
///
/// **Two numbers, and they are not interchangeable.** `AMP` is what has been WRITTEN when the
/// ingest finishes; `AMP-STEADY` is what is still RETAINED after maintenance has run. Write
/// amplification conventionally means the first. The second is smaller and flatters the system,
/// because a maintenance pass reclaims the write-ahead log to a hundred-odd bytes and the index
/// log to zero -- so the index log, which every single write appends to, disappears from it
/// entirely at about 233 bytes a record (see `what_an_index_log_record_costs`).
///
/// Reporting only the retained figure was an error made here once. Both are printed now, and
/// which one is meant should be said out loud whenever either is quoted.
///
/// The WAL record format is field-for-field the operation-log message, and the block geometry is
/// the same 128 KiB block with a 128 B footer, so the log itself should already be at the size the
/// design intends. This asks the question the log alone cannot: what does a write cost across the
/// log, the blocks and the index together.
///
/// Traps this avoids, each of which produced a confident wrong number before:
///   * the log preallocates in 256 KiB steps, so file size is not bytes written -- ingest enough
///     that the step is amortised, and report it separately
///   * pieces and subdirectories both hold bytes, so walk the tree rather than stat one file
///   * a payload of one repeated byte compresses to nothing and flatters every ratio
#[test]
#[ignore]
fn what_a_write_costs_across_every_subsystem() {
    fn tree_bytes(root: &std::path::Path) -> u64 {
        let mut total = 0;
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    stack.push(entry.path());
                } else {
                    total += meta.len();
                }
            }
        }
        total
    }

    // Deterministic but genuinely incompressible. An arithmetic sequence mod 256 has a period of
    // 256 bytes, so a 4 KiB value would be sixteen copies of one block and compress to nothing --
    // the same trap as a repeated byte, just less obvious. This is xorshift64*, seeded per record.
    let spread = |len: usize, salt: usize| -> Vec<u8> {
        let mut state = 0x9E3779B97F4A7C15u64 ^ (salt as u64).wrapping_mul(0xD1B54A32D192ED03);
        if state == 0 {
            state = 0x1234_5678_9ABC_DEF0;
        }
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 33) as u8
            })
            .collect()
    };

    for value_len in [64usize, 256, 1024, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let blocks_dir = dir.path().join("blocks");
        let index_dir = dir.path().join("indexes");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            blocks_dir.clone(),
            index_dir.clone(),
        );
        engine.load_shard(1);

        let records = 4_000usize;
        let mut user_bytes = 0u64;
        for index in 0..records {
            let key = format!("tenant/7/object/{index:09}");
            let value = spread(value_len, index);
            user_bytes += (key.len() + value.len()) as u64;
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key, value },
            });
        }
        engine.flush_shard_index(1);

        let blocks = tree_bytes(&blocks_dir);
        let indexes = tree_bytes(&index_dir);
        let total = blocks + indexes;

        // What is on disk before maintenance runs is the transient shape: the index log has every
        // append in it and nothing has been reclaimed. Steady state is after a cycle, which is
        // what the design this is measured against reaches by truncating its log behind each dump.
        // One cycle is not steady state: a dump writes a fresh base, and the old log can only be
        // reclaimed once that dump is durable, which is a later cycle's job. Watch it converge
        // rather than reading one pass as the answer.
        let mut trail = Vec::new();
        for _ in 0..4 {
            engine.run_storage_manager_cycle(StorageManagerCycleRequest {
                shard_id: 1,
                index_gc_index_log_bytes_threshold: 0,
                index_gc_usage_ratio_trigger_basis_points: 0,
                index_gc_max_entries_per_round: usize::MAX,
                min_undumped_wal_records: 0,
                ..StorageManagerCycleRequest::default()
            });
            trail.push(tree_bytes(&index_dir));
        }
        let settled = *trail.last().expect("four cycles ran");

        // Which files hold it: a snapshot that got bigger and a log that was reclaimed is a very
        // different story from a log that was never reclaimed at all.
        let mut listing: Vec<(String, u64)> = Vec::new();
        let mut stack = vec![index_dir.clone()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&d) else { continue };
            for entry in entries.flatten() {
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    stack.push(entry.path());
                } else {
                    let rel = entry
                        .path()
                        .strip_prefix(&index_dir)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| entry.path().display().to_string());
                    listing.push((rel, meta.len()));
                }
            }
        }
        listing.sort_by(|a, b| b.1.cmp(&a.1));
        for (name, size) in listing.iter().take(6) {
            println!("    INDEXFILE {value_len:>5}B  {size:>9} B  {name}");
        }
        println!(
            "  AMP-STEADY value {value_len:>5}B | index before {indexes:>9} -> {trail:?}              | settled {settled:>9} ({:>5.2}x) | pages {:>9} ({:>5.2}x)",
            settled as f64 / user_bytes as f64,
            tree_bytes(&blocks_dir),
            tree_bytes(&blocks_dir) as f64 / user_bytes as f64,
        );

        println!(
            "  AMP value {value_len:>5}B | user {user_bytes:>9} B | pages {blocks:>9} B ({:>5.2}x) \
             | index {indexes:>9} B ({:>5.2}x) | total {total:>9} B ({:>5.2}x)",
            blocks as f64 / user_bytes as f64,
            indexes as f64 / user_bytes as f64,
            total as f64 / user_bytes as f64,
        );

        // A bound passes most easily when nothing was measured.
        assert!(user_bytes > 0 && total > 0, "the probe must observe writes");
    }
}

/// What an index-log record costs, BEFORE anything reclaims it.
///
/// The steady-state footprint measurement misses this entirely: a maintenance pass reclaims the
/// index log to zero bytes, so it never appears in what is retained. Write amplification counts
/// bytes WRITTEN, and every write puts a record here whether or not it survives the hour.
#[test]
#[ignore]
fn what_an_index_log_record_costs() {
    for records in [1_000usize, 4_000] {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        for index in 0..records {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tenant/7/object/{index:09}"),
                    value: vec![118u8; 256],
                },
            });
        }
        // Deliberately NO maintenance: this is the written size, not the retained one.
        let log = index_dir.join("indexlogs").join("shard-1.indexlog.bin");
        let bytes = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
        let raw = std::fs::read(&log).unwrap_or_default();
        let printable = raw
            .iter()
            .filter(|byte| {
                let value = **byte;
                value == 10 || value == 9 || (0x20u8..0x7f).contains(&value)
            })
            .count();
        println!(
            "  ILOG {records:>5} records -> {bytes:>9} B = {:>6.1} B/record | printable {:.1}%",
            bytes as f64 / records as f64,
            if raw.is_empty() { 0.0 } else { 100.0 * printable as f64 / raw.len() as f64 },
        );
        assert!(bytes > 0, "the probe must observe an index log");
    }
}

/// How many distinct identity strings the block index holds, against how many copies of each.
///
/// Interning pays only where cardinality is low relative to the number of holders. The object key
/// is unique per object, so sharing it saves copies but never collapses them -- established
/// already, and not what this measures. The question here is the other two: `model_id` should be
/// drawn from a small fixed set of model types, and `component` from a per-object field name.
///
/// Reported per field rather than as one identity-strings total, because the answer differs by
/// field and a combined number would hide that.
#[test]
fn block_index_identity_string_cardinality() {
    use std::collections::HashSet;

    const STRING_OBJECTS: usize = 1_500;
    const HASH_OBJECTS: usize = 150;
    const FIELDS_PER_HASH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..STRING_OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("identity-string-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for object in 0..HASH_OBJECTS {
        for field in 0..FIELDS_PER_HASH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("identity-hash-{object:08}"),
                    field: format!("field-{field}"),
                    value: vec![b'h'; 64],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");

    let mut blocks = 0usize;
    let mut distinct_models: HashSet<&str> = HashSet::new();
    // Distinct ALLOCATIONS, not distinct values. Once the kind is shared, counting holders would
    // report the cost as if nothing had changed -- every block still holds one, it just points at
    // a string it does not own.
    let mut model_allocations: HashSet<*const u8> = HashSet::new();
    // THE OBJECT-KEY HALF IS COUNTED OVER THE LOOKUP AND ITS DENOMINATOR IS OBJECTS, NOT PAGES.
    // It summed one `len` per PAGE off the entry's own copy. The entry has none; the text lives
    // once per (kind, object) in the lookup, so that is what is walked and `objects` is what it
    // is divided by. Both denominators are named at the point each ratio is printed, because a
    // per-page figure and a per-object figure over the same bytes differ by the pages-per-object
    // of whatever fixture is running.
    let mut distinct_keys: HashSet<&str> = HashSet::new();
    let mut objects = 0usize;
    let mut model_bytes = 0usize;
    let mut key_bytes = 0usize;
    for (_model, object, _refs) in shard.bucket_index.object_block_lookup.iter() {
        objects += 1;
        distinct_keys.insert(object.as_ref());
        key_bytes += object.len();
    }
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_ref_key, block) in bucket.block_index.iter() {
            blocks += 1;
            distinct_models.insert(block.model_id.as_str());
            // One address per spelling for the whole process, and not an allocation: the entry
            // holds one byte and reads the registry's `&'static str` through it.
            model_allocations.insert(block.model_id.as_str().as_ptr());
            model_bytes += block.model_id.as_str().len();
            // THE COMPONENT HALF OF THIS CENSUS HAS NO SUBJECT LEFT.
            //
            // It measured whether an entry's element name was a SHARED allocation -- distinct
            // names against distinct `Arc` pointers -- which only means anything while an entry
            // carries one. `BlockIndex` has none, so there is nothing to count, nothing to share,
            // and no bytes to attribute.
            //
            // AND ITS ANTI-VACUITY FLOOR IS WHAT FORCED THIS TO BE REMOVED RATHER THAN RELAXED.
            // The assertion below read `components_present > 0`, with the stated reason that a
            // corpus with no components "would decide the component question by construction".
            // That is now true of EVERY corpus: the floor cannot be satisfied by any fixture, so
            // keeping it and lowering it would have been the exact move its own comment forbids.
            // The two halves that still have a subject -- `model_id` sharing and `object_key`
            // copies -- are unchanged and still measured.
        }
    }

    // Anti-vacuity first: an empty index would make every ratio below true for free.
    //
    // THE SECOND FLOOR IS DELETED, NOT LOWERED. It read `components_present > 0` because "a corpus
    // with no components would decide the component question by construction" -- and that is now
    // true of every possible corpus, since an entry has no element name. A floor no fixture can
    // satisfy is not a floor; lowering it to zero would have been the move its own comment forbids.
    assert!(blocks > 0, "the page index is empty; nothing was measured");
    // AND THE LOOKUP NAMES SOMETHING, or the object-key row below is a row of zeroes that reads
    // as "no key text in this store" instead of "the lookup was not walked".
    assert!(
        objects > 0,
        "the object lookup names no object, so the key row below measures nothing"
    );

    let share = |distinct: usize, copies: usize| {
        if distinct == 0 { 0.0 } else { copies as f64 / distinct as f64 }
    };
    println!(
        "
  page index identity strings:
    over {blocks} PAGES
      model_id    {:>5} distinct, {:>6} holders ({:>7.1} each), {:>4} allocations behind {:>6} B of referenced text
    over {objects} OBJECTS, in the lookup, which is where the key text now lives
      object_key  {:>5} distinct, {:>6} slots   ({:>7.1} slots each, {:>6} B held)

    BlockIndex is {} B before its heap strings, and holds no key text at all
",
        distinct_models.len(), blocks, share(distinct_models.len(), blocks),
        model_allocations.len(), model_bytes,
        distinct_keys.len(), objects, share(distinct_keys.len(), objects), key_bytes,
        std::mem::size_of::<crate::engine::state::BlockIndex>(),
    );
}

/// How many block refs an object's slot actually holds.
///
/// # WHAT THIS USED TO ASK, AND WHY THE QUESTION MOVED
///
/// It was `object_block_lookup_occupancy_census`, and it asked how many COMPONENTS an object has,
/// to decide whether an inline single-component shape was worth having. That question is answered
/// and gone: an object has exactly one slot, the two-level shape it was sizing up is deleted, and
/// its own anti-vacuity guard had already said so. The guard asserted `single_component > 0 &&
/// multi_component > 0`, and once the element name came off the page entry every object had one
/// component -- so `multi_component` was 0 and this arm was RED on 75a25910e, before the collapse
/// touched it. Its failure message named the consequence exactly: the occupancy question "would be
/// decided by construction, not measurement".
///
/// # THE AXIS THAT SURVIVES, AND WHY THE MIXED CORPUS STILL EARNS ITS KEEP
///
/// Refs per OBJECT is a real distribution and the corpus still has two sides of it. A string
/// object holds one page, so its slot holds one ref and takes `BlockRefs::One` inline. An
/// eight-field hash object holds eight pages that all file under the same slot, so its slot spills
/// to `BlockRefs::Many`. Both are asserted present before any ratio is read, which is what the
/// 2,000/200 mix is for -- a corpus of one kind would decide this by construction too.
#[test]
fn object_block_lookup_refs_per_object_census() {
    const STRING_OBJECTS: usize = 2_000;
    const HASH_OBJECTS: usize = 200;
    const FIELDS_PER_HASH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..STRING_OBJECTS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("occupancy-string-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }
    for object in 0..HASH_OBJECTS {
        for field in 0..FIELDS_PER_HASH {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: format!("occupancy-hash-{object:08}"),
                    field: format!("field-{field}"),
                    value: vec![b'h'; 64],
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let lookup = &shard.bucket_index.object_block_lookup;

    let mut single_ref_object = 0usize;
    let mut multi_ref_object = 0usize;
    let mut refs_total = 0usize;
    for entry in lookup.values() {
        let held = entry.len();
        if held == 1 {
            single_ref_object += 1;
        } else {
            multi_ref_object += 1;
        }
        refs_total += held;
    }
    let objects = lookup.len();

    // Anti-vacuity, asserted before any ratio is read: an empty or one-sided corpus would make
    // every claim below true for free.
    assert!(objects > 0, "the lookup is empty; nothing was measured");
    assert!(refs_total > 0, "no refs were recorded; nothing was measured");
    // THE TWO-SIDED GATE MOVED FROM THE COMPONENT AXIS TO THE REFS AXIS, BECAUSE THE COMPONENT
    // AXIS COLLAPSED TO ONE SHAPE AND THEN THE AXIS ITSELF CAME OFF.
    //
    // It read `single_component > 0 && multi_component > 0` and refused with "corpus is one-sided
    // (2200 single, 0 multi) -- the occupancy question would be decided by construction, not
    // measurement". It was right to refuse: once a page entry carried no element name an object
    // held exactly ONE componentless lookup row whatever its kind, and a mixed corpus could not
    // produce a second shape on that axis.
    //
    // THE REPLACEMENT GATE THE ELEMENT-NAME BRANCH WROTE IS NOT CARRIED, AND THAT IS A MERGE
    // DECISION RATHER THAN AN OMISSION. It asserted `objects == single_component` -- "every object
    // must hold exactly one componentless row". That counts ROWS, and there is no row level left
    // to count: `ObjectBlockRefs`, `ComponentList` and `ComponentBlocks` are deleted and the
    // lookup's per-object value IS a `BlockRefs`. Rewritten onto this shape it would read
    // `objects == objects` and pass for every corpus including an empty one. Its claim -- that a
    // component can no longer be filed under a row of its own -- is kept where it is still
    // falsifiable: `a_named_component_resolves_nothing_while_the_unnamed_one_resolves_everything`
    // in `engine_storage_state.rs` asserts `names_the_only_slot` in both directions in one arm.
    //
    // THE MULTIPLICITY DID NOT GO -- IT MOVED INSIDE THE SLOT. A hash of eight fields is one slot
    // holding EIGHT refs, where it used to be eight rows of one. So the question this census
    // exists for -- does the inline single-ref shape earn its place -- is asked of the refs
    // vector, and that axis is genuinely two-sided in this corpus: 2,000 strings at one ref and
    // 200 hashes at eight.
    assert!(
        single_ref_object > 0 && multi_ref_object > 0,
        "corpus is one-sided ({single_ref_object} single-ref, {multi_ref_object} multi-ref) -- \
         the occupancy question would be decided by construction, not measurement. The \
         {STRING_OBJECTS} string objects are meant to supply the first and the {HASH_OBJECTS} \
         hash objects of {FIELDS_PER_HASH} fields the second"
    );

    let pct = |n: usize, d: usize| 100.0 * n as f64 / d as f64;
    println!(
        "
  object page lookup occupancy ({objects} objects, {refs_total} refs):
    objects whose slot holds one ref    {single_ref_object:>6}  ({:>5.1}%)
    objects whose slot holds more       {multi_ref_object:>6}  ({:>5.1}%)

    sizes: the lookup's per-object value is BlockRefs {:>3} B over BlockLookupRef {:>3} B
    a one-ref object is {:>3} B with the ref riding inside the value rather than in an
    allocation of its own, and the three wrappers that used to sit between them are gone
",
        pct(single_ref_object, objects),
        pct(multi_ref_object, objects),
        std::mem::size_of::<crate::engine::state::BlockRefs>(),
        std::mem::size_of::<crate::engine::state::BlockLookupRef>(),
        std::mem::size_of::<crate::engine::state::BlockRefs>(),
    );
}

/// Which structures hold an entry per record, and how many.
///
/// Resident memory is ~2.8-3.9 KB per record depending on key length, and a key byte costs 14.1
/// bytes of it -- the same string is held by `strings`, by the composite keys of
/// `object_page_lookup` and `object_component_lookup`, and again inside each `BlockIndex`.
/// Extrapolating to a zero-length key still leaves ~2581 B per record, so most of the bill is
/// fixed per-record structure rather than the key.
///
/// This counts the entries so the fixed part can be attributed rather than guessed at. It is a
/// report, not a threshold: it prints and asserts only the invariants that must hold, so it
/// cannot fail spuriously as unrelated work changes the numbers.
#[test]
fn per_record_structure_census() {
    const RECORDS: usize = 4_000;
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "census".to_string(),
                shard_uri: "local://census/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 1023,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..RECORDS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("census-key-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let buckets = shard.bucket_index.bucket_map.len();
    let page_index_entries: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .sum();
    let object_index_entries: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.object_index.object_count())
        .sum();
    // The map is keyed by object now, so its length is the object count and the per-component
    // map it used to be compared against is gone. Both numbers are still reported, because the
    // interesting quantity is entries PER RECORD and one of them stopped existing.
    let block_lookup_keys = shard.bucket_index.object_block_lookup.len();
    let block_lookup_refs: usize = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(crate::engine::state::BlockRefs::len)
        .sum();
    // `component_lookup_keys` WAS HERE AND IT WAS A SECOND COPY OF THE ROW ABOVE. It summed
    // `by_component.len()` per object, which has been exactly 1 per object since the element name
    // came off the page entry -- so it equalled `block_lookup_keys` by construction and printed
    // 1.00/record beside it. The level it counted is gone; the row goes with it rather than being
    // rewritten into a third spelling of the same number.
    let strings = shard.strings().len();
    let wal_resident = shard.wal_resident_blocks.len();
    let dirty_objects = shard.dirty_objects.len();

    let per = |n: usize| n as f64 / RECORDS as f64;
    println!(
        "
  per-record entries at {RECORDS} records, 1024 routing slots:
             strings                    {:>6.2}
             bucket page_index          {:>6.2}
             bucket object_index        {:>6.2}
             object_page_lookup keys    {:>6.2}
             object_page_lookup refs    {:>6.2}
             dirty_objects              {:>6.2}
             wal_resident_pages         {:>6.2}
             (buckets: {buckets}, not per record)
",
        per(strings), per(page_index_entries), per(object_index_entries),
        per(block_lookup_keys), per(block_lookup_refs),
        per(dirty_objects), per(wal_resident),
    );

    // String bytes each structure actually holds. RSS is ~2.8 KB/record and a key byte costs
    // 14.1 B of it, so knowing WHERE the key copies live says whether interning would pay and
    // which structure to attack first.
    let key_bytes: usize = shard.strings().keys().map(|key| key.len()).sum();
    // ZERO, AND THE ROW IS KEPT BECAUSE THE TABLE IS A LIST OF PLACES. The page index held one
    // `Arc<str>` of key text per entry. It holds none: no key, a one-byte kind spelled by a
    // `&'static str`, and no element name. So this structure contributes no heap text, and the
    // whole of its former contribution is the `block_lookup_bytes` row below -- once per object
    // rather than once per page, which is the saving this change is.
    //
    // THE LOOKUP ROW IS FLOORED JUST BELOW, so a zero here cannot be read as "this fixture wrote
    // no keys".
    let block_index_bytes: usize = 0;
    // Outer keys plus the block-ref key each entry holds.
    let block_lookup_bytes: usize = shard
        .bucket_index
        .object_block_lookup
        .iter()
        .map(|(_model, object, entry)| {
            // The model is stored once for all its objects now, so only the object key is a
            // per-object cost here.
            object.len()
                + entry
                    .iter()
                    .map(|_block_ref| 0usize)
                    .sum::<usize>()
        })
        .sum();
    // `component_lookup_bytes` WAS HERE AND IT WAS IDENTICALLY ZERO. It summed the length of each
    // entry's component NAME, and every name had been `None` since the element name came off the
    // page entry -- so the row printed 0.0 into a byte table that ends in "= {:.1}x the key". The
    // name field is now deleted outright, so there is nothing left to sum. Leaving a hard zero in
    // a measurement table is worse than removing the row.
    let dirty_bytes: usize = shard.dirty_objects.iter().map(str::len).sum();
    // THE FLOOR THE PAGE-INDEX ROW NEEDS. That row is a structural zero now, and a structural
    // zero beside an unfloored lookup row is indistinguishable from a fixture that wrote no keys
    // at all -- the table would read "no key text anywhere" and both halves of that would be
    // wrong. This says the text IS held, and says it of the structure that now holds it.
    assert!(
        block_lookup_bytes > 0,
        "the object lookup holds no key text over {RECORDS} records, so the zero in the page-index          row below cannot be read as a saving"
    );
    let total_string_bytes =
        key_bytes + block_index_bytes + block_lookup_bytes + dirty_bytes;
    let sample_key_len = shard.strings().keys().next().map_or(0, |name| name.len());
    let perb = |n: usize| n as f64 / RECORDS as f64;
    println!(
        "  string bytes held per record (sample key is {sample_key_len} B):
             strings keys               {:>7.1}
             bucket page_index          {:>7.1}
             object_page_lookup         {:>7.1}
             dirty_objects              {:>7.1}
             TOTAL                      {:>7.1}  = {:.1}x the key
",
        perb(key_bytes), perb(block_index_bytes), perb(block_lookup_bytes),
        perb(dirty_bytes), perb(total_string_bytes),
        perb(total_string_bytes) / sample_key_len.max(1) as f64,
    );

    // Only the invariants: every record must be reachable by key and by block.
    assert_eq!(strings, RECORDS, "every record should have a string entry");
    assert_eq!(
        page_index_entries, RECORDS,
        "every record should have exactly one live page ref"
    );
    assert!(
        buckets <= 1024,
        "routing range should cap the bucket count, got {buckets}"
    );
}

/// The dirty-bucket count must equal the answer the unshortened scan would give.
///
/// The stats path counts dirty buckets by collecting the buckets already marked dirty and then
/// asking, per dirty object, which buckets hold its blocks. `dirty_objects` grows by one per
/// record ingested and each pass builds a composite lookup key, so that loop was shard-sized work
/// on the heartbeat timer, under the read lock writers need.
///
/// It now stops once every bucket is in the set, because the answer is a set of bucket ids and
/// cannot grow past the bucket count. That is exact rather than approximate -- but "exact by
/// argument" is what this test exists to check, by recomputing the count the long way and
/// requiring equality.
///
/// Measured, 200k records in five equal phases at 1023 slots, growth of the last phase over the
/// first with the heartbeat at 1s: 3.42/2.17 before, 1.14/0.93 after, against 0.84-0.94 with the
/// heartbeat switched off entirely.
#[test]
fn dirty_bucket_count_matches_the_unshortened_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "dirty-bucket-count".to_string(),
                shard_uri: "local://dirty-bucket-count/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    // Mixed enough that not every bucket ends up dirty: the short-circuit must not fire in the
    // case where the loop still has something to contribute.
    for index in 0..90 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dbc-{index}"),
                value: vec![b'v'; 48],
            },
        });
        if index % 4 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("dbc-{}", index / 3),
                },
            });
        }
    }

    let reported = engine
        .shard_stats(1)
        .expect("shard 1 loaded")
        .object_manager
        .dirty_bucket_count;

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    // The same computation with no early exit.
    let mut expected: std::collections::BTreeSet<u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(bucket_id, bucket)| bucket.dirty().then_some(*bucket_id))
        .collect();
    for object_key in shard.dirty_objects.iter() {
        expected.extend(crate::engine::bucket_index_target_buckets_for_object_key(
            shard, object_key,
        ));
    }
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "workload produced no buckets"
    );
    assert_eq!(
        reported,
        expected.len(),
        "dirty bucket count diverged from the unshortened scan: {reported} vs {}",
        expected.len()
    );
    println!("
  dirty buckets: reported {reported} == unshortened {}
", expected.len());
}

/// The maintained block-ref total must equal the walk it replaced.
///
/// The stats path reports that number and used to derive it by summing every set in
/// `object_component_lookup` -- a walk over every object in the shard, run on the heartbeat timer
/// while holding the shard read lock writers need. Measured on a 200k-record ingest in five equal
/// phases: heartbeat at 1s, the last phase cost 3.0x the first and the datanode's own CPU grew
/// 3.3-3.7x; heartbeat off, 0.84-0.94x. Turning the timer off removed the growth entirely, which
/// is what identified the walk rather than the write path.
///
/// It is now kept as a running total, so it can drift instead of merely being slow. This drives
/// inserts, superseding overwrites that remove block refs, hash fields with components, expiries
/// and deletes, then compares the maintained value against the sum it is meant to equal.
#[test]
fn maintained_component_block_ref_total_matches_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "page-ref-total".to_string(),
                shard_uri: "local://page-ref-total/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..140 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("tot-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("tot-hash-{}", index % 9),
                field: format!("f-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 3 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tot-str-{}", index / 2),
                    value: vec![b'w'; 96],
                },
            });
        }
        if index % 6 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("tot-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 8 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("tot-str-{}", index / 4),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let walked: usize = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(crate::engine::state::BlockRefs::len)
        .sum();
    let maintained = shard
        .bucket_index
        .object_component_block_refs
        .expect("the total should be established once the lookup has been built");
    assert!(walked > 0, "workload produced no component page refs to compare");
    assert_eq!(
        maintained, walked,
        "maintained component page-ref total drifted from the walk it replaces:          {maintained} vs {walked}"
    );
    println!("
  component page refs: maintained {maintained} == walked {walked}
");
}

/// Every bucket's `object_index` must already equal a from-scratch recompute.
///
/// `update_bucket_layout` rebuilds that set by scanning all of a bucket's blocks, with no
/// short-circuit -- it is the remaining guaranteed full pass in bucket maintenance, and the whole
/// of what still scales with the corpus once the TTL pass is skipped.
///
/// The rebuild looks redundant: the mutation sites already maintain the set, inserting an
/// object id when a block is added and removing it when the last live block for it goes. If that
/// invariant genuinely holds, refreshing a bucket never needs the scan and can classify from the
/// two lengths it already has.
///
/// This is the evidence for that "if". It runs a workload that exercises inserts, overwrites that
/// supersede a block, hash fields, expiries and deletes, then recomputes the live-object set from
/// each bucket's blocks and requires it to match what is stored. A mutation site that fails to
/// maintain the set shows up here as a named mismatch -- which is what makes dropping the scan
/// safe rather than hopeful.
#[test]
fn bucket_object_index_already_matches_a_from_scratch_recompute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "object-index-invariant".to_string(),
                shard_uri: "local://object-index-invariant/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..150 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("inv-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: format!("inv-hash-{}", index % 11),
                field: format!("f-{index}"),
                value: vec![b'h'; 32],
            },
        });
        if index % 3 == 0 {
            // Supersede an existing block: this is the path that removes block refs.
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("inv-str-{}", index / 2),
                    value: vec![b'w'; 96],
                },
            });
        }
        if index % 7 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("inv-str-{index}"),
                    ttl_ms: 60_000,
                },
            });
        }
        if index % 9 == 0 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonDelete {
                    key: format!("inv-str-{}", index / 4),
                },
            });
        }
    }

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut checked = 0usize;
    let mut mismatches = Vec::new();
    for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
        let recomputed: std::collections::BTreeSet<u64> = bucket
            .block_index
            .iter()
            .filter(|(_, block)| !block.deleted)
            .map(|(handle, block)| block.object_id(1, named.at(*routing_bucket, *handle)))
            .collect();
        // Mirrors update_bucket_layout: an empty live set over an empty block index leaves the
        // stored set untouched, so only compare where the rebuild would actually assign.
        if recomputed.is_empty() && bucket.block_index.is_empty() {
            continue;
        }
        checked += 1;
        let expected: crate::engine::state::ObjectIndex = if recomputed.is_empty() {
            crate::engine::state::ObjectIndex::default()
        } else {
            recomputed.into()
        };
        // THE SET, not the slots. `ObjectIndex::Many` is a slot array, so `==` also compares
        // which slot each id sits in -- and the ids collected ascending above take different slots
        // from the ids this bucket filed as it was written. The claim under test is that the stored
        // SET equals a rebuild, which is what `sorted_ids` compares.
        if bucket.object_index.sorted_ids() != expected.sorted_ids() {
            mismatches.push(format!(
                "  bucket {routing_bucket}: stored {:?} != recomputed {:?}",
                bucket.object_index.sorted_ids(),
                expected.sorted_ids()
            ));
        }
    }
    // --- AND THE PASS AN ID-SET EQUALITY CANNOT DO: THE REBUILD MUST NOT RENUMBER A SLOT. ---
    //
    // The comparison above is blind to exactly the failure that matters once a slot is stored. It
    // compares `sorted_ids`, so a rebuild that kept every id and moved every one of them to a
    // different slot passes it -- which is what the old whole-array assignment from a `BTreeSet`
    // did on every call, eleven production callers deep.
    //
    // Driven on a CLONE, so the shard under test is not mutated and the before/after is exact, and
    // compared slot for slot INCLUDING PLACEHOLDERS: `id_at` answers `None` for a hole, and a hole
    // closing up is a renumber of everything above it.
    let mut renumbered = Vec::new();
    let mut slot_arrays_compared = 0usize;
    let mut placeholders_seen = 0usize;
    for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
        let slots_of = |index: &crate::engine::state::ObjectIndex| -> Vec<Option<u64>> {
            (0..index.slot_count()).map(|slot| index.id_at(slot)).collect()
        };
        let before = slots_of(&bucket.object_index);
        if before.is_empty() {
            continue;
        }
        slot_arrays_compared += 1;
        placeholders_seen += before.iter().filter(|slot| slot.is_none()).count();
        let mut rebuilt = bucket.clone();
        // THE IDENTITIES COME FROM THE SHARD, because the rebuild no longer reads them off the
        // pages. The bucket under test is a CLONE, so the shard is still only borrowed immutably
        // here and `block_identities` -- which borrows the whole index -- is available; the
        // production sweeps have to split the two field borrows by hand instead.
        let block_keys = shard.bucket_index.block_identities();
        crate::engine::storage_bucket_internals::update_bucket_layout(
            1,
            &mut rebuilt,
            &block_keys,
        );
        let after = slots_of(&rebuilt.object_index);
        if before != after {
            renumbered.push(format!("  bucket {routing_bucket}: {before:?} -> {after:?}"));
        }
    }

    assert!(checked > 0, "workload produced no buckets to check");
    assert!(
        slot_arrays_compared > 0,
        "no bucket carried a slot array, so the renumber pass examined nothing. A zero here reads \
         as agreement and would be the absence the comparison is satisfied by"
    );
    assert!(
        renumbered.is_empty(),
        "the layout rebuild RENUMBERED {} of {slot_arrays_compared} bucket(s). A stored slot names \
         one object for life only if the rebuild leaves it where it is:\n{}",
        renumbered.len(),
        renumbered.join("\n")
    );
    println!(
        "\n  {slot_arrays_compared} slot arrays survived a rebuild unmoved, {placeholders_seen} \
         placeholders among them"
    );
    assert!(
        mismatches.is_empty(),
        "object_index is NOT maintained incrementally by {} of {checked} bucket(s), so the          layout rebuild is load-bearing and cannot be dropped:
{}",
        mismatches.len(),
        mismatches.join("
")
    );
    println!("
  {checked} buckets: stored object_index matches a from-scratch recompute
");
}

/// Per-write cost must be flat at a NARROW routing range too, not only a wide one.
///
/// This is the limitation the sibling tests cannot see, because they run at the default routing
/// range where every key lands in its own bucket: a write touches one bucket of many, the
/// targeted refresh skips the rest, and cost is flat.
///
/// Narrow the range -- `TS_SHARD_END_ROUTING_SLOT=1023`, the setting that cuts resident memory
/// 45% and the one to run in production -- and there are only 1024 buckets. A batch of any size
/// hashes across essentially all of them, so there is nothing to skip: the targeted path visits
/// the same blocks as the sweep and the guard correctly hands the work back to the sweep, which is
/// `O(total pages)` per batch. Bucket maintenance is therefore STILL linear per write there.
///
/// Measured end to end at 1023 slots, five equal 40k-record phases: 6.3s -> 14.9s while every
/// byte written stayed flat (index log 1.02x, WAL 1.01x). At the default range the same run grows
/// 1.64x. The remedy is not to choose a path but to stop rescanning: keep each bucket's live-block
/// count, dirty-block count and minimum TTL incrementally, so refreshing a bucket is O(1) and the
/// range stops mattering.
///
/// Counted at 64 slots as each pass came off: 12.6 -> 39.0 (3.10x), then 8.4 -> 26.0 after
/// skipping the TTL pass when nothing expires, then 4.2 -> 13.0 once the refresh classifies
/// instead of rescanning, then flat once the upsert does too. Only the last changed the SHAPE,
/// because rebuilding the live-object set is the one pass with no short-circuit: `deleted`
/// stops at the first live block and `dirty` at the first dirty one, but that rebuild reads
/// every block every time.
#[test]
fn bucket_maintenance_is_flat_at_a_narrow_routing_range() {
    fn visits_per_write(object_count: usize) -> f64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        // The range travels on the load request, NOT the environment: the datanode binary reads
        // TS_SHARD_*_ROUTING_SLOT and puts it here. Setting those vars around a bare
        // `load_shard` leaves the shard on the u32::MAX default, and this test then silently
        // measures the wide range and agrees with its sibling -- which is exactly what the first
        // version of it did.
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 1,
                    table_name: "narrow-routing".to_string(),
                    shard_uri: "local://narrow-routing/1".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: 63,
                    readonly: false,
                    load_version: 1,
                    local_node_id: Some(1),
                })
                .status
                .ok
        );
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("narrow-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        const MEASURED_WRITES: usize = 20;
        crate::engine::reset_bucket_block_index_visits();
        for index in 0..MEASURED_WRITES {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("narrow-measured-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let visited = crate::engine::bucket_block_index_visits();
        visited as f64 / MEASURED_WRITES as f64
    }

    let small = visits_per_write(200);
    let large = visits_per_write(800);
    println!(
        "
  64 routing slots:
    200 objects -> {small:>8.1} page-index visits per write
    800 objects -> {large:>8.1} page-index visits per write
"
    );

    // An absolute bound, not a ratio: the work is meant to be independent of the corpus, and a
    // ratio is undefined once it reaches zero. 4x the objects must not buy more than a constant.
    assert!(
        large <= 2.0,
        "per-write bucket maintenance should not scan pages at a narrow routing range, got {large:.1} visits/write at 800 objects (200 objects: {small:.1})"
    );
    assert!(
        large <= small + 2.0,
        "per-write bucket maintenance grew with the corpus at 64 routing slots: {small:.1} -> {large:.1} visits/write for 200 -> 800 objects"
    );
}

/// Bucket maintenance must not make a write cost more as the store grows.
///
/// `update_bucket_layout` rebuilds a bucket's whole object set from `block_index`, and the
/// per-object dirty-state clear walks every bucket in the shard. Both are `O(pages)` and both sit
/// inside loops, so the write path can be quadratic in the corpus while every individual function
/// still looks cheap.
///
/// Counted, not timed: the number is work done, so the test cannot pass by running on a fast
/// machine, and it reports per-write cost so growth is visible rather than implied.
#[test]
fn bucket_maintenance_per_write_does_not_grow_with_the_store() {
    fn visits_per_write(object_count: usize) -> (u64, f64, (u64, u64, u64, u64)) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        // Fill to the target corpus size first; only the writes AFTER the reset are measured.
        for index in 0..object_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("bucket-maint-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        const MEASURED_WRITES: usize = 20;
        crate::engine::reset_bucket_block_index_visits();
        crate::engine::bucket_visit_sites::reset();
        for index in 0..MEASURED_WRITES {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("bucket-maint-measured-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        let visited = crate::engine::bucket_block_index_visits();
        let sites = crate::engine::bucket_visit_sites::snapshot();
        (visited, visited as f64 / MEASURED_WRITES as f64, sites)
    }

    let (small_total, small_each, small_sites) = visits_per_write(200);
    let (large_total, large_each, large_sites) = visits_per_write(800);

    let per = |value: u64| value as f64 / 20.0;
    println!(
        "
  200 objects -> {small_total:>9} visits for 20 writes ({small_each:>9.1} per write)
           800 objects -> {large_total:>9} visits for 20 writes ({large_each:>9.1} per write)
           growth: {:.2}x cost for 4x the corpus

           per-write attribution      200 objects    800 objects
             update_bucket_layout     {:>9.1}      {:>9.1}
             clear_dirty (all bkts)   {:>9.1}      {:>9.1}
             refresh_runtime_flags    {:>9.1}      {:>9.1}
             remove (all bkts)        {:>9.1}      {:>9.1}
",
        if small_each > 0.0 { large_each / small_each } else { 0.0 },
        per(small_sites.0), per(large_sites.0),
        per(small_sites.1), per(large_sites.1),
        per(small_sites.2), per(large_sites.2),
        per(small_sites.3), per(large_sites.3),
    );

    // 4x the corpus must not cost materially more PER WRITE. Allowed a little slack for
    // bucket-fill effects; a linear-in-corpus path shows up here as ~4x and fails loudly.
    assert!(
        large_each <= small_each * 1.5 + 1.0,
        "per-write bucket maintenance grew with the store: {small_each:.1} -> {large_each:.1}          visits/write for 200 -> 800 objects"
    );
}

/// Eviction cost must track how many victims are wanted, not how much the shard holds.
///
/// Measured with the live-block scan counter rather than a clock, so the number is the work done
/// rather than the speed of the machine, and the test cannot pass by happening to run fast.
///
/// Serialized against other tests in this file only by the fact that it reads a process-wide
/// counter around its own call; it resets immediately before measuring.
#[test]
fn sampled_eviction_scan_volume_does_not_grow_with_the_store() {
    fn scan_volume_for(block_count: usize, sampled: bool) -> (u64, usize) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..block_count {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("scaling-key-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }

        // Both sides say what they mean. The default moved to the sampled path, so the
        // full-scan arm has to ask for the exhaustive scan -- if it did not, both arms would
        // sample and the comparison would compare one path against itself.
        if sampled {
            engine.use_sampled_eviction_for_test();
        } else {
            engine.use_full_scan_eviction_for_test();
        }
        crate::engine::reset_live_block_scan_entries();
        // Threshold 0 so the pressure gate always admits and the selection path actually runs.
        let report = engine.apply_storage_eviction(1, 0, 4, false, false);
        let scanned = crate::engine::live_block_scan_entries();
        (scanned, report.selected_victims.len())
    }

    let (small_full, _) = scan_volume_for(200, false);
    let (large_full, _) = scan_volume_for(1600, false);
    let (small_sampled, small_victims) = scan_volume_for(200, true);
    let (large_sampled, large_victims) = scan_volume_for(1600, true);

    println!(
        "\n  full-scan   200 pages -> {small_full:>6} live-page entries scanned\n  \
         full-scan  1600 pages -> {large_full:>6} live-page entries scanned\n  \
         sampled     200 pages -> {small_sampled:>6} live-page entries scanned ({small_victims} victims)\n  \
         sampled    1600 pages -> {large_sampled:>6} live-page entries scanned ({large_victims} victims)\n"
    );

    // The default path pays for the whole store: 8x the blocks costs materially more.
    assert!(
        large_full > small_full * 4,
        "full scan should grow with the store, got {small_full} -> {large_full}"
    );

    // The sampled path must not. It may still read the blocks of the buckets it picked, which is
    // bounded by batch_limit, so this is a ceiling rather than an equality.
    assert!(
        large_sampled < large_full / 4,
        "sampled scan should be far below the full scan, got {large_sampled} vs {large_full}"
    );
    assert!(
        large_sampled <= small_sampled.saturating_mul(2).max(64),
        "sampled scan should not grow with the store, got {small_sampled} -> {large_sampled}"
    );
}

/// An async write whose cache entry is dropped before any dump must still read back.
///
/// Without this its only durable copy is the WAL record, at an address naming no file, so the
/// read returns MISSING for a write that was acked -- the hole the spill workaround was added
/// to paper over. Here the record itself serves the value.
#[test]
fn an_evicted_async_write_is_served_from_its_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    // The spill path would otherwise mask what is being tested by copying the value to a real
    // slab on eviction.
    // Said to THIS engine. `TS_HOT_PAGE_SPILL=0` was how this asked, and it could not
    // work: the handler is installed during construction, which already happened above.
    engine.disable_hot_block_spill_for_test();

    let key = "block-in-wal-key";
    let value = b"block-in-wal-value".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.clone(),
        },
    });
    assert!(write.status.ok, "the write must be acked");

    // Drop every cached copy: the value now exists only in its WAL record.
    engine.cache().invalidate_shard(1).unwrap();

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(value)
        },
        "an acked write must not read back as missing once its cache entry is gone"
    );
}

/// A block that is DERIVED state must also survive its cache entry being dropped.
///
/// A hash field set stores a serialized map, so unlike a plain string the block cannot be
/// reconstructed from the command that wrote it. Serving it back therefore requires the record
/// to carry the block itself, which is what staging does.
#[test]
fn an_evicted_derived_block_is_served_from_its_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    // Said to THIS engine. `TS_HOT_PAGE_SPILL=0` was how this asked, and it could not
    // work: the handler is installed during construction, which already happened above.
    engine.disable_hot_block_spill_for_test();

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "derived-page-key".to_string(),
            field: "f1".to_string(),
            value: b"derived-page-value".to_vec(),
        },
    });
    assert!(write.status.ok, "the write must be acked: {write:?}");

    // Drop every cached copy: the block now exists only inside its WAL record.
    engine.cache().invalidate_shard(1).unwrap();

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: "derived-page-key".to_string(),
            field: "f1".to_string(),
        },
    });

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"derived-page-value".to_vec())
        },
        "a derived page must not read back as missing once its cache entry is gone"
    );
}


/// A write handed its blocks must log THOSE blocks, not the ones it would have derived.
///
/// This is what lets a replayed write reproduce the bytes that were acked somewhere else
/// rather than this node's reconstruction of them.
#[test]
fn a_carried_block_is_what_reaches_the_log_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    let carried = vec![crate::wal::StagedBlock {
        object_id: 4242,
        component: None,
        bytes: b"pages-from-somewhere-else".to_vec(),
    }];
    let write = engine.execute_with_carried_blocks(
        ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "carried".to_string(),
                field: "f".to_string(),
                value: b"local-derivation".to_vec(),
            },
        },
        carried.clone(),
    );
    assert!(write.status.ok, "the write must be acked: {write:?}");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let logged = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .find(|record| !record.staged_blocks.is_empty())
        .expect("the record must carry pages");
    assert_eq!(
        logged.staged_blocks, carried,
        "the carried pages belong on the record, not this node's re-derivation"
    );
}

// ---------------------------------------------------------------------------------------
// Interleavings: the shard does not hold still between a decision and the act it authorizes.
//
// Most of this suite is sequential -- set up, act, assert -- and that shape found none of the
// concurrency-adjacent defects in the reclaim sweep. These exercise the seam this codebase
// actually has: plan and apply are separate calls, so anything that happens in between is a
// real interleaving, reproducible without threads or timing.
// ---------------------------------------------------------------------------------------

/// A write that lands between planning a reclaim and applying it must survive.
///
/// The plan is computed, handed back -- over an RPC, or through a cycle stage -- and applied
/// later. The shard keeps taking writes the whole time. A plan authorizes dropping what the
/// durable index could replace WHEN IT WAS MADE, and a record that did not exist then was
/// never covered by that proof.
#[test]
fn a_reclaim_plan_does_not_authorize_dropping_writes_that_landed_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "before".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());

    // The interleaving: a write lands after the plan was made, before it is applied.
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "after".to_string(),
                    value: b"v2".to_vec(),
                },
            })
            .status
            .ok
    );
    let after_sequence = engine.write_ahead_log_store().stats(1).last_sequence;

    let _report = engine.apply_storage_wal_reclaim(plan);

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let highest = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .map(|record| record.sequence)
        .max()
        .unwrap_or(0);
    assert!(
        highest >= after_sequence,
        "the later write's record was reclaimed by a plan made before it existed \
         (highest kept {highest}, the write was at {after_sequence})"
    );
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "after".to_string()
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "and it must still read back"
    );
}

/// A dump taken between planning a manifest prune and applying it must not be pruned.
///
/// The prune decides which manifests are redundant. A dump that completes after that decision
/// is the freshest thing the shard has, and pruning it would throw away the only manifest
/// covering the current generation.
#[test]
fn a_manifest_created_after_a_prune_plan_is_not_pruned_by_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        },
    });
    engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let _plan = engine.bucket_dump_manifest_prune_plan(1);

    // The interleaving: a third dump completes after the prune was planned.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v3".to_vec(),
        },
    });
    let newest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();

    let _report = engine.apply_bucket_dump_manifest_prune(1);

    let remaining = engine.list_bucket_dump_manifests(1);
    assert!(
        remaining
            .iter()
            .any(|manifest| manifest.manifest_id == newest.manifest_id),
        "the dump taken after the prune was planned is the freshest manifest and must survive; \
         remaining: {:?}",
        remaining
            .iter()
            .map(|manifest| &manifest.manifest_id)
            .collect::<Vec<_>>()
    );
}

/// The same block must still read back after the shard is RELOADED.
///
/// Registrations are live-path state: they are dropped when a shard unloads, and the standing
/// claim is that reload replays the WAL and re-derives every block. That only holds if replay
/// revisits the record carrying the block -- and replay starts ABOVE the persisted watermark,
/// which this very write advanced past its own record. If nothing rebuilds the mapping, the
/// served index is left pointing at a synthetic address naming no file, and an acked write
/// reads back as MISSING after a restart: exactly the hole staging was added to close,
/// reopened by a reload.
#[test]
fn a_block_in_wal_block_still_reads_back_after_a_shard_reload() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });

    // Said to THIS engine. `TS_HOT_PAGE_SPILL=0` was how this asked, and it could not
    // work: the handler is installed during construction, which already happened above.
    engine.disable_hot_block_spill_for_test();

    let key = "reload-block-key";
    let value = b"reload-block-value".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.clone(),
        },
    });
    assert!(write.status.ok, "the write must be acked: {write:?}");

    // Drop every cached copy, then take the shard down and bring it back. This is the restart,
    // and it is what clears the registrations.
    engine.cache().invalidate_shard(1).unwrap();
    engine.unload_shard(1);
    engine.load_shard(1);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });

    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(value)
        },
        "an acked write must not read back as missing after a reload"
    );
}

/// A block that lives only in a WAL record must have its location written down where the index
/// keeps it, not only in a table this process happens to hold.
///
/// The address in the served index is synthetic -- a counter, not a position -- so on its own it
/// cannot be turned back into bytes. The resolver's table can, but it starts empty after a
/// restart, which is why the location has to travel with the index that depends on it.
#[test]
fn a_wal_resident_block_records_where_it_lives_in_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.set_config(SetConfigRequest {
        shard_id: 1,
        config: Config {
            version: 2,
            async_storage: true,
            ..Config::default()
        },
    });
    // Said to THIS engine. `TS_HOT_PAGE_SPILL=0` was how this asked, and it could not
    // work: the handler is installed during construction, which already happened above.
    engine.disable_hot_block_spill_for_test();

    assert_eq!(
        engine.wal_resident_block_count(1),
        0,
        "nothing is in the log yet"
    );

    let value = b"only-durable-copy-is-the-record".to_vec();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "logged".to_string(),
                    value: value.clone(),
                },
            })
            .status
            .ok
    );

    assert!(
        engine.wal_resident_block_count(1) > 0,
        "the index must carry where the page went"
    );

    // Take the shard down and back up: the resolver's table is dropped, so what comes back has
    // to come from the index.
    engine.cache().invalidate_shard(1).unwrap();
    engine.unload_shard(1);
    engine.load_shard(1);

    assert!(
        engine.wal_resident_block_count(1) > 0,
        "the location has to survive the reload, or it was never durable"
    );
    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "logged".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes { value: Some(value) },
        "an acked write must read back after the reload"
    );
}

/// An asynchronous write whose block is not spilled records WHERE that block lives.
///
/// This was once the control half of a test asserting the opposite for a store that put nothing
/// in the log. That store no longer exists -- a write always carries its block -- so the control
/// is the whole test: an async write with the spill handler off leaves its block in its record
/// and nowhere else, and the location it records is the only thing that can serve it back.
/// A zero here means the write staged nothing, which is the acked-write-reads-back-as-MISSING
/// case the carry was added to close.
#[test]
fn an_async_write_records_where_its_block_lives() {
    fn resident_blocks_after_a_staging_write() -> usize {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        // Asynchronous storage is what leaves a block somewhere other than the block store, and
        // the spill handler is what would then catch it. Without both, nothing stages and the
        // count is zero for reasons that have nothing to do with the setting.
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        engine.disable_hot_block_spill_for_test();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "plain".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.wal_resident_block_count(1)
    }

    assert!(
        resident_blocks_after_a_staging_write() > 0,
        "the write has to stage something, or nothing can serve its block back from the record"
    );
}

/// A synchronous write puts its block INSIDE its own record.
///
/// The single barrier acks on the WAL fsync and DEFERS the block fsync, so at the moment a write
/// is acknowledged the block store holds the block in buffers and nowhere else. The copy carried
/// in the record is therefore the only durable one until a dump moves it into a slab, which is
/// why the write carries it at all.
///
/// Two things have to happen for the count below to be non-zero: the write stages its bytes, and
/// the append puts what was staged into the record rather than dropping it. Both were arms of a
/// predicate once; stop doing either and an acked write's block is gone if the process dies before
/// the deferred block fsync. `a_group_commit_write_keeps_the_block_it_staged` prints this number
/// on the same path -- this is the test that requires it.
#[test]
fn a_synchronous_write_puts_its_block_in_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // No `async_storage`: this is the synchronous path, and it is the default.
    let key = "carried-sync-key";
    let value = b"carried-sync-value".to_vec();
    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.clone(),
        },
    });
    assert!(write.status.ok, "the write must be acked: {write:?}");

    let carried: Vec<crate::wal::StagedBlock> = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .flat_map(|record| record.staged_blocks)
        .collect();
    assert_eq!(
        carried.len(),
        1,
        "the acked write's record must carry its block: at ack time the block store has it in \
         buffers and nowhere else"
    );
    assert_eq!(
        carried[0].bytes, value,
        "the record has to carry the bytes that were written, not a reconstruction of them"
    );

    // Drop every cached copy, then take the shard down and bring it back: the value has to come
    // back from something durable rather than from memory.
    engine.cache().invalidate_shard(1).unwrap();
    engine.unload_shard(1);
    engine.load_shard(1);
    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes { value: Some(value) },
        "an acked write must read back after the reload"
    );
}

/// A record can state what the write DID, and that statement has to match what the command
/// actually produced.
///
/// This is the obligation that has to be discharged before replay can install outcomes instead
/// of re-running commands. If an item disagreed with the index entry the command built, then
/// switching replay over would silently rebuild a different shard.
#[test]
fn a_recorded_outcome_matches_the_index_entry_the_command_produced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "outcome-key".to_string(),
                    value: b"outcome-value".to_vec(),
                },
            })
            .status
            .ok
    );
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let record = records
        .iter()
        .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
        .find(|record| !record.outcomes.is_empty())
        .expect("the record must state what the write did");

    let item = record
        .outcomes
        .iter()
        .find(|item| item.object_key == "outcome-key")
        .expect("the object it touched must be named");
    assert_eq!(item.kind, "string");
    assert!(!item.deleted);
    // The item states the id the TERMS derive. It used to be compared against a copy on the
    // address; the address carries none, so the comparison is against the derivation itself -- which
    // is not circular here, because `item.object_id` was written by the engine's write path and this
    // side is computed in the test.
    assert_eq!(
        item.object_id,
        crate::engine::hashing::stable_block_object_id(1, "string", "outcome-key"),
        "the outcome states the id the write path derived for the key it names"
    );

    // The claim has to equal what the index actually holds. This is the whole point.
    let indexed = engine
        .string_block_address(1, "outcome-key")
        .expect("the index holds an address for the key");
    // Through the accessor, not the raw field: a recorded address omits the routing bucket the
    // item already carries, and putting it back is what `resolved_address` is for. Comparing the
    // raw field against an index entry compares a trimmed address with a whole one.
    assert_eq!(
        item.resolved_address().as_ref(),
        Some(&indexed),
        "the recorded outcome disagrees with the index entry the command built"
    );
}

/// With the gate off a record carries no outcomes, so it is byte-identical to before.
#[test]
fn a_record_carries_no_outcomes_unless_asked() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "0");
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "plain".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .status
            .ok
    );
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    assert!(
        records
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .all(|record| record.outcomes.is_empty()),
        "no record should carry outcomes with the gate off"
    );
}

// ---------------------------------------------------------------------------------------
// Multiple engines in ONE process. The embedded path (the gateway proxy) does exactly this,
// and every embedded engine serves shard 1 -- so anything keyed on (shard, object) alone is
// shared between engines that have nothing to do with each other.
// ---------------------------------------------------------------------------------------

/// Two engines, same shard id, same object key: each must read back its OWN value.
///
/// The block resolver is a process-wide table keyed on (shard, object id), and a block's object
/// id is derived from kind + key -- not from which engine wrote it. Two embedded engines
/// therefore collide on every key they happen to share, and the only thing separating them is
/// that a registration also remembers WHICH log it points into.
#[test]
fn two_engines_in_one_process_do_not_read_each_others_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str| {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join(format!("{name}-cache")),
            dir.path().join(format!("{name}-blocks")),
            dir.path().join(format!("{name}-index")),
        );
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        // Said to EACH engine this closure builds. The variable worked here -- it is set before
        // construction, unlike the four above -- but it said it to the process, and this test
        // builds two engines to prove they do not share a spill map.
        engine.disable_hot_block_spill_for_test();
        engine
    };


    let first = make("first");
    let second = make("second");

    // Same shard, same key, different values, different engines.
    for (engine, value) in [(&first, b"from-first".to_vec()), (&second, b"from-second".to_vec())] {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "shared-key".to_string(),
                        value,
                    },
                })
                .status
                .ok
        );
    }

    // Drop every cached copy so the read has to go through the resolver, which is the shared
    // thing. If it resolved by (shard, object) alone, one engine would serve the other's bytes.
    first.cache().invalidate_shard(1).unwrap();
    second.cache().invalidate_shard(1).unwrap();

    let read = |engine: &TemporalEngine| {
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "shared-key".to_string(),
                },
            })
            .response
    };
    let first_read = read(&first);
    let second_read = read(&second);

    assert_eq!(
        first_read,
        CommandResponse::Bytes {
            value: Some(b"from-first".to_vec())
        },
        "the first engine read the wrong engine's page"
    );
    assert_eq!(
        second_read,
        CommandResponse::Bytes {
            value: Some(b"from-second".to_vec())
        },
        "the second engine read the wrong engine's page"
    );
}

/// One engine's log-resident blocks must not pin another engine's reclaim floor.
///
/// The retention floor is the lowest sequence any live registration still depends on. Computed
/// across the whole process it would be pinned by every OTHER engine's writes forever, and a
/// shard that had dumped everything would never be able to reclaim.
#[test]
fn one_engines_retention_floor_ignores_another_engines_registrations() {
    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str| {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join(format!("{name}-cache")),
            dir.path().join(format!("{name}-blocks")),
            dir.path().join(format!("{name}-index")),
        );
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                async_storage: true,
                ..Config::default()
            },
        });
        engine
    };

    let busy = make("busy");
    let quiet = make("quiet");

    // The busy engine registers blocks; the quiet one writes nothing at all.
    for index in 0..8 {
        assert!(
            busy.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("busy-{index}"),
                    value: vec![b'v'; 64],
                },
            })
            .status
            .ok
        );
    }

    // The quiet engine's log has nothing registered against it, so nothing holds its floor.
    assert_eq!(
        quiet.write_ahead_log_store().block_retention_floor(1),
        None,
        "another engine's registrations pinned this engine's reclaim floor"
    );
}

/// Every command that changes stored state has to record what it did, or replay cannot be
/// switched from re-running commands to installing outcomes without silently losing that state.
///
/// This is a COVERAGE probe, not a spot check: it drives a spread of write commands and reports
/// every one whose record carries no outcome, so the gap list comes from the engine rather than
/// from reading call sites and hoping the list is complete.
#[test]
fn every_mutating_command_records_what_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    let commands = vec![
        Command::StringSet {
            key: "probe-string".to_string(),
            value: b"v".to_vec(),
        },
        Command::StringSetEx {
            key: "probe-setex".to_string(),
            value: b"v".to_vec(),
            ttl_ms: 60_000,
        },
        Command::HashSet {
            key: "probe-hash".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
        Command::HashIncrBy {
            key: "probe-hash".to_string(),
            field: "counter".to_string(),
            increment: 3,
        },
        Command::SetAdd {
            key: "probe-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetAdd {
            key: "probe-zset".to_string(),
            member: b"m".to_vec(),
            score: 1.5,
        },
        Command::ListPush {
            key: "probe-list".to_string(),
            member: b"m".to_vec(),
            left: true,
        },
        Command::SeenCheck {
            key: "probe-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 60_000,
        },
        Command::BucketTake {
            key: "probe-bucket".to_string(),
            tokens: 1.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::ControlStateIncrement {
            key: "probe-control".to_string(),
            timestamp_ms: 1_787_270_070_000,
            amount: 2,
        },
        Command::CommonExpire {
            key: "probe-string".to_string(),
            ttl_ms: 30_000,
        },
        // Removals go through mark_bucket_index_block_deleted, not the upsert -- a different
        // path, so a different chance to record nothing.
        Command::HashDelete {
            key: "probe-hash".to_string(),
            field: "f".to_string(),
        },
        Command::SetRemove {
            key: "probe-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetRemove {
            key: "probe-zset".to_string(),
            member: b"m".to_vec(),
        },
        Command::ListPop {
            key: "probe-list".to_string(),
            left: true,
        },
        Command::StringDelete {
            key: "probe-setex".to_string(),
        },
        // Feature series have their own map again.
        Command::FeatureAppend {
            key: "probe-feature".to_string(),
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_070_000,
                value: b"fv".to_vec(),
            }],
        },
        Command::CommonDelete {
            key: "probe-string".to_string(),
        },
    ];

    let mut attempted = Vec::new();
    for command in commands {
        let label = format!("{command:?}");
        let label = label
            .split_once(' ')
            .map(|(head, _)| head.to_string())
            .unwrap_or(label);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        // Only commands the engine ACCEPTED are evidence about outcome coverage.
        if response.status.ok {
            attempted.push(label);
        }
    }
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");

    // Which of the accepted writes left a record that says nothing about what changed?
    let records = engine
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let mut silent = Vec::new();
    for (_, line) in records {
        let Ok(record) = crate::wal::decode_wal_line(&line) else {
            continue;
        };
        if record.outcomes.is_empty() {
            let label = format!("{:?}", record.command);
            let label = label
                .split_once(' ')
                .map(|(head, _)| head.to_string())
                .unwrap_or(label);
            silent.push(label);
        }
    }
    silent.sort();
    silent.dedup();

    assert!(
        silent.is_empty(),
        "these accepted writes recorded nothing about what they changed, so replay could not \
         install them: {silent:?} (attempted: {attempted:?})"
    );
}

/// THE GATE. A shard rebuilt by installing recorded outcomes must equal one built by running
/// the commands.
///
/// Until this holds, replay cannot be switched over: an apply that drops or garbles a kind
/// produces a shard that is subtly wrong rather than one that fails, and nothing downstream
/// would notice. Kinds are brought over one at a time and this test grows with them.
#[test]
fn a_shard_rebuilt_from_outcomes_equals_one_rebuilt_from_commands() {
    let dir = tempfile::tempdir().unwrap();
    let ran = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("ran-cache"),
        dir.path().join("ran-blocks"),
        dir.path().join("ran-index"),
    );
    ran.load_shard(1);
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");

    // A bound tight enough that the workload below overflows it. Only this engine is told --
    // which is the whole point: the shard rebuilt from records has to arrive at the same three
    // points without ever learning what the bound was.
    assert!(
        ran.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                feature_max_size: 3,
                ..Config::default()
            },
        })
        .ok
    );

    // The kinds the apply path claims to handle so far.
    let workload = vec![
        Command::StringSet {
            key: "eq-a".to_string(),
            value: b"first".to_vec(),
        },
        Command::StringSet {
            key: "eq-b".to_string(),
            value: b"second".to_vec(),
        },
        Command::StringSet {
            key: "eq-a".to_string(),
            value: b"first-overwritten".to_vec(),
        },
        Command::SeenCheck {
            key: "eq-seen".to_string(),
            member: b"m1".to_vec(),
            window_ms: 60_000,
        },
        Command::BucketTake {
            key: "eq-bucket".to_string(),
            tokens: 2.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::HashSet {
            key: "eq-hash".to_string(),
            field: "f1".to_string(),
            value: b"hv".to_vec(),
        },
        Command::HashSet {
            key: "eq-hash".to_string(),
            field: "f2".to_string(),
            value: b"hv2".to_vec(),
        },
        Command::SetAdd {
            key: "eq-set".to_string(),
            member: b"member-one".to_vec(),
        },
        Command::ZSetAdd {
            key: "eq-zset".to_string(),
            member: b"zm".to_vec(),
            score: 2.5,
        },
        Command::ListPush {
            key: "eq-list".to_string(),
            member: b"lm".to_vec(),
            left: true,
        },
        Command::ListPush {
            key: "eq-list".to_string(),
            member: b"lm2".to_vec(),
            left: false,
        },
        Command::CommonExpire {
            key: "eq-b".to_string(),
            ttl_ms: 60_000,
        },
        Command::StringSet {
            key: "eq-doomed".to_string(),
            value: b"gone".to_vec(),
        },
        Command::CommonDelete {
            key: "eq-doomed".to_string(),
        },
        // Five points into a series bounded at three. The two oldest are dropped by config the
        // command never mentions -- so unless the trim itself is recorded, the rebuilt shard
        // keeps points this one has thrown away.
        Command::FeatureAppend {
            key: "eq-feature".to_string(),
            points: (0..5)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("point-{index}").into_bytes(),
                })
                .collect(),
        },
        // The one kind whose index entry is NOT keyed by the stored timestamp: the block packs by
        // time, the map keys by event id, and the time index maps one to the other. All three
        // have to come back, which is why the record carries both keys.
        Command::ContextWriteExtractedEvent {
            tenant_hash: 41,
            node_hash: 42,
            event: Box::new(crate::types::ContextEvent {
                event_id_hash: 445,
                event_time_ms: 1_787_270_075_000,
                ingestion_time_ms: 1_787_270_075_000,
                kind: 7,
                event_type: 7,
                actor_hash: 0,
                status: 1,
                valid_until_ms: 0,
                confidence: 0.96,
                importance: 0.88,
                text: "an extracted event".to_string(),
                source_ref: String::new(),
                related_node_hashes: vec![42],
                compact_attrs: Vec::new(),
                vector: Vec::new(),
            }),
            indexes: crate::types::ContextExtractedEventIndexes {
                scope_hash: 3001,
                entity_hashes: vec![501],
                status_hash: 601,
                source_hash: 701,
                event_time_bucket_ms: 1_787_270_000_000,
                disabled_indexes: Vec::new(),
            },
            first_write_only: false,
            cold_storage: false,
        },
        // The context maps are the same shape as a feature series -- stored key to block -- so
        // they are installed by the same arm. Which is exactly why they belong in here: one arm
        // covering six kinds is one place for five of them to go silently uninstalled.
        Command::ContextWriteIndexRef {
            tenant_hash: 41,
            index_name: "actor".to_string(),
            index_value_hash: 77,
            scope_hash: 3,
            event_time_ms: 1_787_270_070_000,
            index_ref: crate::types::ContextIndexRef {
                primary_node_hash: 9,
                primary_event_time_ms: 1_787_270_070_000,
                event_id_hash: 1234,
            },
        },
        Command::ContextWritePackAudit {
            tenant_hash: 41,
            audit: crate::types::ContextPackAudit {
                query_id: "q-1".to_string(),
                session_hash: 5,
                request_time_ms: 1_787_270_071_000,
                query_hash: 6,
                max_prompt_tokens: 100,
                selected_tokens: 40,
                selected_refs: Vec::new(),
                blocked_refs: Vec::new(),
            },
        },
        Command::ContextUpsertChildRef {
            tenant_hash: 41,
            child_ref: crate::types::ContextChildRef {
                parent_hash: 9,
                child_hash: 10,
                updated_at_ms: 1_787_270_072_000,
            },
        },
        Command::ContextUpsertSummary {
            tenant_hash: 41,
            summary: crate::types::ContextSummary {
                node_hash: 9,
                level: 1,
                text: "a summary".to_string(),
                valid_from_ms: 1_787_270_073_000,
                vector: Vec::new(),
                embedding_model_hash: 0,
            },
        },
        Command::ContextWriteCompressionEvent {
            tenant_hash: 41,
            event: crate::types::ContextCompressionEvent {
                compression_id_hash: 21,
                node_hash: 9,
                source_start_ms: 1_787_270_070_000,
                source_end_ms: 1_787_270_073_000,
                compressed_time_ms: 1_787_270_074_000,
                summary: "compressed".to_string(),
            },
        },
        // A replace drops a range and writes over it: removals and inserts in one command.
        Command::FeatureReplace {
            key: "eq-feature".to_string(),
            start_ms: 1_787_270_073_000,
            end_ms: 1_787_270_074_000,
            points: vec![crate::types::FeaturePoint {
                timestamp_ms: 1_787_270_073_500,
                value: b"replacement".to_vec(),
            }],
        },
    ];
    for command in workload {
        let response = ran.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(response.status.ok, "workload write failed: {response:?}");
    }
    // Two empty maps compare equal, so a workload that quietly wrote nothing would pass. Assert
    // each kind is actually present before comparing anything.
    let ran_shape = ran.index_shape_for_test(1);
    for kind in [
        "context_event",
        "context_timeline",
        "context_index",
        "context_audit",
        "context_child",
        "context_summary",
        "context_compression",
    ] {
        assert!(
            ran_shape.lines().any(|line| line.starts_with(&format!("{kind} "))),
            "the workload wrote no {kind}, so installing it proves nothing"
        );
    }

    // If this stops being true the trim is no longer under test and the gate has gone quiet.
    // Five points, bounded to the newest three, then a replace drops two of those and writes one
    // back: the oldest two must be gone by the trim, and the series must be down to two.
    let ran_feature = ran
        .index_shape_for_test(1)
        .lines()
        .filter(|line| line.starts_with("feature eq-feature "))
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        ran_feature.len(),
        2,
        "the workload was supposed to leave a trimmed series, got {ran_feature:?}"
    );
    assert!(
        !ran_feature
            .iter()
            .any(|line| line.contains("at=1787270070000") || line.contains("at=1787270071000")),
        "the two oldest points should have been trimmed, got {ran_feature:?}"
    );

    // Take the outcomes off the log, in the order they were written.
    let records = ran
        .write_ahead_log_store()
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap();
    let mut outcomes = Vec::new();
    for (_, line) in records {
        let Ok(record) = crate::wal::decode_wal_line(&line) else {
            continue;
        };
        outcomes.push((record.sequence, record.outcomes));
    }
    outcomes.sort_by_key(|(sequence, _)| *sequence);
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
    assert!(
        outcomes.iter().any(|(_, items)| !items.is_empty()),
        "the workload recorded no outcomes at all"
    );

    // A second shard that never sees a command -- only what the first one did.
    let installed = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("inst-cache"),
        dir.path().join("inst-blocks"),
        dir.path().join("inst-index"),
    );
    installed.load_shard(1);
    let mut refused = Vec::new();
    for (sequence, items) in &outcomes {
        for item in items {
            if !installed.apply_outcome_item(1, item, &[]) {
                refused.push(format!("seq={sequence} kind={}", item.kind));
            }
        }
    }
    assert!(
        refused.is_empty(),
        "the apply path refused outcomes it should understand: {refused:?}"
    );

    assert_eq!(
        installed.index_shape_for_test(1),
        ran.index_shape_for_test(1),
        "installing the outcomes did not reproduce the shard the commands built"
    );

    // The typed maps are not the whole shard. The bucket index is durable state the read path
    // consults, so compare it too -- separately, so a failure says WHICH half diverged.
    assert_eq!(
        installed.bucket_index_shape_for_test(1),
        ran.bucket_index_shape_for_test(1),
        "the typed maps matched but the bucket index did not"
    );
}

/// What waiting before a dump saves, and what the wait costs.
///
/// A bucket is dumped, dirtied again by the very next write, and dumped again. Letting the log
/// accumulate first lets those writes merge into one dump. The knob for that exists and is off by
/// default, so today every cycle dumps every dirty bucket.
///
/// Off is a defensible choice -- the dumps saved are paid for with a longer log to replay after a
/// restart -- so this reports both sides rather than asserting one is right. The dumps are taken,
/// not merely planned: planning alone never advances the dumped watermark, and then the backlog
/// never falls and the counts mean nothing.
#[test]
fn what_delaying_a_dump_saves_and_costs() {
    for threshold in [0u64, 10, 50] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let writes = 100u64;
        let mut dumps = 0u64;
        let mut worst_backlog = 0u64;
        for index in 0..writes {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    // One key, written over and over: the case where merging matters.
                    key: "hot".to_string(),
                    value: format!("v{index}").into_bytes(),
                },
            });
            let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1,
                min_undumped_wal_records: threshold,
                max_dump_buckets_per_round: 16,
                ..Default::default()
            });
            worst_backlog = worst_backlog.max(plan.undumped_wal_records);
            if !plan.selected_dump_buckets.is_empty() {
                // Take the dump, so the watermark moves and the next backlog is real.
                if engine
                    .create_bucket_dump_manifest(1, plan.selected_dump_buckets.clone())
                    .is_ok()
                {
                    dumps += 1;
                }
            }
        }
        println!(
            "  threshold {threshold:>3} records: {dumps:>4} dumps for {writes} writes to one bucket, \
             worst replay backlog {worst_backlog} records"
        );
    }
}

/// One expiry round, as the number of keys carrying a deadline grows.
///
/// A round only ever acts on a bounded window. Choosing that window copies and sorts every key
/// that has a deadline, so the cost tracks the whole set instead of the window -- and it is paid on
/// every round, forever.
#[test]
fn what_one_expiry_round_costs_as_deadlines_accumulate() {
    for keys in [1_000usize, 5_000, 20_000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1 << 24,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..keys {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSetEx {
                    key: format!("k{index:08}"),
                    value: b"v".to_vec(),
                    // Far enough out that nothing expires during the measurement.
                    ttl_ms: 3_600_000,
                },
            });
        }

        let rounds = 20;
        let started = std::time::Instant::now();
        for _ in 0..rounds {
            let report = engine.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                // A small window, which is the point: the round should cost the window.
                max_hot_buckets_per_round: 16,
                max_cold_buckets_per_round: 16,
                ..Default::default()
            });
            assert!(report.is_ok(), "the sweep should run");
        }
        let per_round = started.elapsed().as_secs_f64() * 1e6 / rounds as f64;
        println!(
            "  {keys:>6} keys with a deadline: {per_round:>9.0} us per round \
             ({:.2} us per 1k keys) for a window of 16",
            per_round / (keys as f64 / 1000.0)
        );
    }
}

/// Every per-round bound on the background cycle is set, and none of them is zero.
///
/// Zero is not a small bound in any of these, it is NO bound: the expiry window walks the whole
/// deadline map, the eviction sampler keeps every victim it finds, the index-log sweep removes
/// every removable record, and both index-GC triggers fire whatever the log looks like. They were
/// all zero, with the enforcement written and tested around them, so a cycle did as much work as
/// there was work to do.
///
/// Asserted as exact values AND as non-zero. The exact values keep the numbers deliberate; the
/// non-zero assertions say what the test is really defending, which is that none of these
/// silently goes back to meaning "unbounded" -- the state they were all in.
#[test]
fn every_maintenance_round_is_bounded_by_default() {
    let request = StorageManagerCycleRequest::default();

    assert_eq!(request.index_gc_max_entries_per_round, 256);
    assert_eq!(request.index_gc_index_log_bytes_threshold, 768 * 1024);
    assert_eq!(request.index_gc_usage_ratio_trigger_basis_points, 4_000);
    assert_eq!(request.max_expire_hot_buckets_per_round, 128);
    assert_eq!(request.max_expire_cold_buckets_per_round, 8);
    assert_eq!(request.eviction_batch_limit, 16);
    assert_eq!(request.block_gc_min_slab_garbage_basis_points, 4_000);

    for (name, bound) in [
        ("index_gc_max_entries_per_round", request.index_gc_max_entries_per_round as u64),
        ("index_gc_index_log_bytes_threshold", request.index_gc_index_log_bytes_threshold),
        (
            "index_gc_usage_ratio_trigger_basis_points",
            request.index_gc_usage_ratio_trigger_basis_points,
        ),
        ("max_expire_hot_buckets_per_round", request.max_expire_hot_buckets_per_round as u64),
        ("max_expire_cold_buckets_per_round", request.max_expire_cold_buckets_per_round as u64),
        ("eviction_batch_limit", request.eviction_batch_limit as u64),
        (
            "block_gc_min_band_garbage_basis_points",
            request.block_gc_min_slab_garbage_basis_points,
        ),
    ] {
        assert!(bound > 0, "{name} is zero, which is not a bound at all");
    }

    // Cold is deliberately much smaller than hot: a cold bucket has to be LOADED before its
    // records can be expired, so a cold round costs I/O a hot one does not.
    assert!(
        request.max_expire_cold_buckets_per_round < request.max_expire_hot_buckets_per_round,
        "a cold expiry round must stay smaller than a hot one"
    );
}

/// The default hot-expiry bound actually truncates a round, and the round after it resumes.
///
/// Pinning the constant proves the number; this proves the number reaches the mechanism. A bound
/// that is set but never read is the state this whole change is about, so asserting the value
/// alone would assert exactly the thing that was already true.
#[test]
fn the_default_expiry_bound_truncates_a_round_and_the_next_one_resumes() {
    use std::collections::BTreeMap;

    let request = StorageManagerCycleRequest::default();
    let limit = request.max_expire_hot_buckets_per_round;
    let mut deadlines: BTreeMap<std::sync::Arc<str>, u64> = BTreeMap::new();
    for index in 0..(limit as u64 * 3) {
        deadlines.insert(format!("key-{index:05}").into(), index);
    }

    let (first, cursor) = crate::engine::expiry_window(
        &deadlines,
        None,
        limit,
        limit.saturating_mul(8).max(64),
        |_| true,
    );
    assert_eq!(first.len(), limit, "the round must stop at the bound");
    assert!(
        first.len() < deadlines.len(),
        "and the bound must be smaller than the work, or this proves nothing"
    );
    let cursor = cursor.expect("an unfinished round must hand back where to resume");

    let (second, _) = crate::engine::expiry_window(
        &deadlines,
        Some(cursor.as_str()),
        limit,
        limit.saturating_mul(8).max(64),
        |_| true,
    );
    assert_eq!(second.len(), limit, "the next round takes another full window");
    assert!(
        second[0].0 > first[first.len() - 1].0,
        "and it resumes past the first round rather than re-walking it"
    );
}

/// Paging by cursor reaches every deadline exactly once.
///
/// The window is read from an ordered set, so paging only works if the cursor advances past
/// everything examined. If it advanced only past what was taken, a run of keys the round rejected
/// would be re-examined forever and the sweep would never reach what lies beyond them.
#[test]
fn paging_the_expiry_window_reaches_every_deadline_once() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<std::sync::Arc<str>, u64> = BTreeMap::new();
    for index in 0..250u64 {
        deadlines.insert(format!("key-{index:04}").into(), index);
    }

    for window in [1usize, 7, 64, 250, 400] {
        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..2_000 {
            let (selected, next) = crate::engine::expiry_window(
                &deadlines,
                cursor.as_deref(),
                window,
                window.saturating_mul(8).max(64),
                |_| true,
            );
            assert!(
                selected.len() <= window,
                "a window of {window} returned {} keys",
                selected.len()
            );
            let mut sorted = selected.clone();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(selected, sorted, "the window should come back in key order");
            seen.extend(selected.iter().map(|(key, _)| key.clone()));
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let mut unique = seen.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), seen.len(), "window {window} repeated a key");
        assert_eq!(
            unique.len(),
            deadlines.len(),
            "window {window} never reached every deadline"
        );
    }
}

/// A long run of keys the round rejects does not stall the sweep.
///
/// This is the case the scan budget exists for: looking for the few keys in one category when the
/// set is almost entirely the other. The round stops early, but the cursor has moved, so the next
/// one resumes past what was examined and the sweep still finishes.
#[test]
fn a_run_of_rejected_keys_does_not_stall_the_sweep() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<std::sync::Arc<str>, u64> = BTreeMap::new();
    for index in 0..1_000u64 {
        deadlines.insert(format!("key-{index:04}").into(), index);
    }
    // Only the last handful are wanted; everything before them is examined and rejected.
    let wanted = |key: &str| key >= "key-0990";

    let mut found: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut rounds = 0;
    loop {
        let (selected, next) =
            crate::engine::expiry_window(&deadlines, cursor.as_deref(), 8, 64, wanted);
        found.extend(selected.iter().map(|(key, _)| key.clone()));
        rounds += 1;
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(rounds < 200, "the sweep is not making progress");
    }
    assert_eq!(found.len(), 10, "every wanted key should be reached");
    assert!(
        rounds > 1,
        "this should take several bounded rounds, or the budget is not bounding anything"
    );
}

/// No limit means every match after the cursor, and nothing to resume from.
#[test]
fn an_unlimited_expiry_window_returns_every_match() {
    use std::collections::BTreeMap;

    let mut deadlines: BTreeMap<std::sync::Arc<str>, u64> = BTreeMap::new();
    for index in 0..40u64 {
        deadlines.insert(format!("key-{index:04}").into(), index);
    }
    let (selected, next) = crate::engine::expiry_window(&deadlines, None, 0, 0, |_| true);
    assert_eq!(selected.len(), 40);
    assert!(next.is_none(), "there is nothing left to resume from");

    let (after, _) =
        crate::engine::expiry_window(&deadlines, Some("key-0019"), 0, 0, |_| true);
    assert_eq!(after.len(), 20, "the cursor is exclusive");
    assert_eq!(after.first().map(|(key, _)| key.as_str()), Some("key-0020"));
}


/// A cycle request that does not mention the ordering guard must still get it.
///
/// Index-log records may only be discarded once the buckets they describe have been dumped;
/// truncating first throws away the record of state that is not durable anywhere else. The guard
/// that enforces that is a field on the request, and the request is parsed from an HTTP body --
/// so what an OMITTED field decodes to is the behaviour every caller gets who does not name it.
/// `#[serde(default)]` on a bool decodes to `false`, which is the unsafe order, even though the
/// type's own `Default` says true.
#[test]
fn omitting_the_commit_before_truncate_guard_still_commits_before_truncating() {
    // Take a well-formed body and remove ONLY the guard, so this tests what silence means rather
    // than whether every other field happens to be optional.
    let mut body: serde_json::Value =
        serde_json::to_value(StorageManagerCycleRequest::default()).unwrap();
    let removed = body
        .as_object_mut()
        .unwrap()
        .remove("index_gc_commit_dirty_buckets_before_truncation");
    assert!(removed.is_some(), "the guard should be present in a serialised request");

    let silent: StorageManagerCycleRequest = serde_json::from_value(body).unwrap();
    assert!(
        silent.index_gc_commit_dirty_buckets_before_truncation,
        "a request that does not mention the guard decoded to the unsafe order: index-log records \
         would be discarded before the buckets they describe had been dumped"
    );

    // Naming it false is still allowed -- this is about what silence means, not about removing
    // the choice.
    let mut off: serde_json::Value =
        serde_json::to_value(StorageManagerCycleRequest::default()).unwrap();
    off.as_object_mut().unwrap().insert(
        "index_gc_commit_dirty_buckets_before_truncation".to_string(),
        serde_json::Value::Bool(false),
    );
    let explicit: StorageManagerCycleRequest = serde_json::from_value(off).unwrap();
    assert!(!explicit.index_gc_commit_dirty_buckets_before_truncation);
}

/// Every stage records how long it took, instead of every stage reporting zero.
///
/// `duration_ms` is published as `temporalstore_storage_manager_phase_duration_ms`, and nothing
/// ever set it -- so the metric read zero for every phase of every shard, always. A missing series
/// is obviously missing; a series that reads zero says the cycle is instantaneous, which is a claim
/// and a false one.
///
/// Asserted as a property rather than a threshold, because a fast stage legitimately rounds to
/// zero milliseconds and a threshold would be flaky: the stages must TILE the cycle -- their total
/// cannot exceed the wall time of the call that produced them.
#[test]
fn the_storage_cycle_records_how_long_its_stages_take() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // Enough to make the cycle do real work, and no more: an earlier version used 4000 records
    // and took ten minutes on a shared machine, which is not a cost a suite should carry.
    for index in 0..400 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{index:06}"),
                value: vec![118u8; 128],
            },
        });
    }

    let started = std::time::Instant::now();
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let wall_ms = started.elapsed().as_millis() as u64;

    assert!(!cycle.stages.is_empty(), "the cycle should have run stages");
    let total: u64 = cycle.stages.iter().map(|stage| stage.duration_ms).sum();
    assert!(
        total <= wall_ms + 50,
        "stage durations total {total} ms but the whole call took {wall_ms} ms -- they should tile          the cycle, not overlap it"
    );
    assert!(
        cycle.duration_ms >= total,
        "the round total {} ms should cover the stages that tile it ({total} ms)",
        cycle.duration_ms
    );
    // The tiling assertion above is the one that matters, and it is what caught the first
    // implementation: a closure capturing the clock by value restarted a COPY, so every stage
    // reported the time since the cycle began and the total came to eight times the wall clock.
    // Deliberately NOT asserting some stage exceeds zero -- a quick cycle rounds every stage to
    // zero milliseconds, and that assertion would fail on a fast machine for no reason.
}

/// A COLD RELOAD, not a synthetic apply loop: a fresh engine rebuilds the shard from disk.
///
/// The equivalence gate feeds recorded outcomes to an engine by hand, which proves the apply path
/// understands them. It does not prove RECOVERY uses them -- replay could ignore every outcome and
/// re-execute every command and that gate would stay green.
///
/// Neither does comparing shard shapes across a restart, which is worth stating because the first
/// version of this test did exactly that and passed while recovery installed NOTHING: unloading a
/// shard flushes its index, so the reload had no tail left to replay and never entered the path
/// under test. The install counter is what caught it, and asserting on it is what keeps this test
/// honest -- so the engine is dropped without unloading, the way a crash leaves it.
#[test]
fn a_cold_reload_rebuilds_the_shard_from_what_the_writes_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");

    let workload = vec![
        Command::StringSet {
            key: "rs-a".to_string(),
            value: b"alpha".to_vec(),
        },
        Command::StringSetEx {
            key: "rs-ttl".to_string(),
            value: b"expiring".to_vec(),
            ttl_ms: 600_000,
        },
        Command::HashSet {
            key: "rs-hash".to_string(),
            field: "f".to_string(),
            value: b"hv".to_vec(),
        },
        Command::SetAdd {
            key: "rs-set".to_string(),
            member: b"m".to_vec(),
        },
        Command::ZSetAdd {
            key: "rs-zset".to_string(),
            member: b"zm".to_vec(),
            score: 3.5,
        },
        Command::ListPush {
            key: "rs-list".to_string(),
            member: b"lm".to_vec(),
            left: true,
        },
        Command::SeenCheck {
            key: "rs-seen".to_string(),
            member: b"m".to_vec(),
            window_ms: 600_000,
        },
        Command::BucketTake {
            key: "rs-bucket".to_string(),
            tokens: 2.0,
            capacity: 10.0,
            refill_per_sec: 1.0,
        },
        Command::FeatureAppend {
            key: "rs-feature".to_string(),
            points: (0..3)
                .map(|index| crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index * 1_000,
                    value: format!("p{index}").into_bytes(),
                })
                .collect(),
        },
    ];

    let (before, before_index, recorded) = {
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            cache.clone(),
            blocks.clone(),
            indexes.clone(),
        );
        engine.load_shard(1);
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");
        for command in workload {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "workload write failed: {response:?}");
        }
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        let shape = engine.index_shape_for_test(1);
        assert!(
            shape.contains("string rs-a") && shape.contains("feature rs-feature"),
            "the workload did not build the shard it was supposed to: {shape}"
        );
        let records = engine
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap();
        let recorded = records
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .filter(|record| !record.outcomes.is_empty())
            .count();
        assert!(
            recorded >= 8,
            "expected the workload to leave records carrying outcomes, got {recorded}"
        );
        (shape, engine.bucket_index_shape_for_test(1), recorded)
        // dropped WITHOUT unload: the index is not flushed, so the tail must be replayed.
    };

    // The gate stays OFF for the reload. Recording is what it gates; a record that already says
    // what it did is installed on its own evidence.
    let recovered = TemporalEngine::with_local_dirs(1024 * 1024, cache, blocks, indexes);
    recovered.load_shard(1);

    // The shapes below would match even if recovery ignored every outcome and re-executed the
    // commands, so assert WHICH path ran before asserting the result.
    let installed = recovered.replay_installs_for_test();
    assert!(
        installed >= recorded as u64,
        "recovery installed {installed} outcomes for {recorded} records that carried them, so it          fell back to replaying commands and this test proves nothing"
    );

    assert_eq!(
        recovered.index_shape_for_test(1),
        before,
        "a cold reload did not rebuild the shard the writes described"
    );
    assert_eq!(
        recovered.bucket_index_shape_for_test(1),
        before_index,
        "a cold reload rebuilt the maps but not the index"
    );
}

/// PER COMMAND: does what a write recorded describe EVERYTHING it changed, or just something?
///
/// The coverage probe asks whether a record said anything at all, which is a much weaker question
/// than it looks. StringSetEx writes a value and a deadline; recording only the value passes the
/// probe, passes the equivalence gate as long as no deadline is in the workload, and produces a
/// recovered key that never expires. That defect was real and this is the test that names it.
///
/// Each command runs alone against a fresh pair of shards -- one built by running it, one built by
/// installing only what it recorded -- so a failure names the command rather than the workload.
#[test]
fn what_each_command_recorded_describes_everything_it_changed() {
    // (label, what must already exist, the command under test). A command that only fires
    // against existing state -- a conditional write, a persist -- cannot be probed on an empty
    // shard, and leaving it out is how StringSetConditional kept a real defect through four
    // rounds of this.
    let commands: Vec<(&str, Vec<Command>, Command)> = vec![
        (
            "StringSet",
            Vec::new(),
            Command::StringSet {
                key: "pc-string".to_string(),
                value: b"v".to_vec(),
            },
        ),
        (
            "StringSetEx",
            Vec::new(),
            Command::StringSetEx {
                key: "pc-setex".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 600_000,
            },
        ),
        (
            "HashSet",
            Vec::new(),
            Command::HashSet {
                key: "pc-hash".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
        ),
        (
            "HashIncrBy",
            Vec::new(),
            Command::HashIncrBy {
                key: "pc-hash".to_string(),
                field: "counter".to_string(),
                increment: 3,
            },
        ),
        (
            "SetAdd",
            Vec::new(),
            Command::SetAdd {
                key: "pc-set".to_string(),
                member: b"m".to_vec(),
            },
        ),
        (
            "ZSetAdd",
            Vec::new(),
            Command::ZSetAdd {
                key: "pc-zset".to_string(),
                member: b"m".to_vec(),
                score: 1.5,
            },
        ),
        (
            "ListPush",
            Vec::new(),
            Command::ListPush {
                key: "pc-list".to_string(),
                member: b"m".to_vec(),
                left: true,
            },
        ),
        (
            "SeenCheck",
            Vec::new(),
            Command::SeenCheck {
                key: "pc-seen".to_string(),
                member: b"m".to_vec(),
                window_ms: 600_000,
            },
        ),
        (
            "BucketTake",
            Vec::new(),
            Command::BucketTake {
                key: "pc-bucket".to_string(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
        ),
        (
            "FeatureAppend",
            Vec::new(),
            Command::FeatureAppend {
                key: "pc-feature".to_string(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000,
                    value: b"fv".to_vec(),
                }],
            },
        ),
        (
            "StringSetConditional-refresh",
            vec![Command::StringSetEx {
                key: "pc-cond".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 120,
            }],
            Command::StringSetConditional {
                key: "pc-cond".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: Some(600_000),
                condition: crate::types::StringSetCondition::IfExists,
                return_old: false,
                keep_ttl: false,
            },
        ),
        (
            "StringSetConditional-clears-deadline",
            vec![Command::StringSetEx {
                key: "pc-cond2".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 600_000,
            }],
            Command::StringSetConditional {
                key: "pc-cond2".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: None,
                condition: crate::types::StringSetCondition::IfExists,
                return_old: false,
                keep_ttl: false,
            },
        ),
        (
            "CommonPersist",
            vec![Command::StringSetEx {
                key: "pc-persist".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 600_000,
            }],
            Command::CommonPersist {
                key: "pc-persist".to_string(),
            },
        ),
        (
            "CommonExpire",
            vec![Command::StringSet {
                key: "pc-expire".to_string(),
                value: b"v".to_vec(),
            }],
            Command::CommonExpire {
                key: "pc-expire".to_string(),
                ttl_ms: 600_000,
            },
        ),
        (
            "FeatureDelete",
            vec![Command::FeatureAppend {
                key: "pc-featdel".to_string(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000,
                    value: b"fv".to_vec(),
                }],
            }],
            Command::FeatureDelete {
                key: "pc-featdel".to_string(),
            },
        ),
        (
            "ControlStateIncrement",
            Vec::new(),
            Command::ControlStateIncrement {
                key: "pc-ctr".to_string(),
                timestamp_ms: 1_787_270_070_000,
                amount: 5,
            },
        ),
        (
            "ContextUpsertNode",
            Vec::new(),
            Command::ContextUpsertNode {
                tenant_hash: 41,
                node: Box::new(crate::types::ContextNode {
                    node_hash: 9,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: "probe-node".to_string(),
                    status: 1,
                    last_event_time_ms: 1_787_270_070_000,
                    raw_metadata_ref: String::new(),
                    l0: String::new(),
                    l1_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
                    summary_vector: Vec::new(),
                    summary_vector_valid_from_ms: 0,
                    summary_vector_model_hash: 0,
                }),
            },
        ),
    ];

    let mut incomplete = Vec::new();
    for (label, prelude, command) in commands {
        let dir = tempfile::tempdir().unwrap();
        let ran = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("ran-cache"),
            dir.path().join("ran-blocks"),
            dir.path().join("ran-index"),
        );
        ran.load_shard(1);
        // The prelude runs with the gate OFF so the only outcomes on the log belong to the
        // command under test, and the installed shard starts from the same prelude state.
        for setup in &prelude {
            let response = ran.execute(ExecuteRequest {
                shard_id: 1,
                command: setup.clone(),
            });
            assert!(response.status.ok, "{label}: prelude failed: {response:?}");
        }
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", "1");
        let response = ran.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
        if !response.status.ok {
            continue;
        }
        // THE PAGES ARE KEPT ALONGSIDE THE OUTCOMES, which they were not before.
        //
        // A record records two things: what the write DID (its outcomes) and the PAGES it produced.
        // This harness took the first and dropped the second, which was invisible while every
        // outcome carried its own copy of the element name. It is not invisible now:
        // `apply_outcome_item`'s container arms read the element off the page, so an install handed
        // outcomes with no pages is handed strictly less than the replay loop has -- and it refused
        // every hash outcome, which is this harness reporting its own missing half as the product
        // recording nothing.
        //
        // Production is the comparison that settles which side to fix: the WAL replay loop passes
        // `&record.staged_blocks`, and `block_in_wal::stage` is unconditional, so a record always
        // has them. The harness now passes them too.
        let replayed = ran
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .collect::<Vec<_>>();
        let outcomes = replayed
            .iter()
            .flat_map(|record| record.outcomes.iter().cloned())
            .collect::<Vec<_>>();
        let carried = replayed
            .iter()
            .flat_map(|record| record.staged_blocks.iter().cloned())
            .collect::<Vec<_>>();

        let installed = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("inst-cache"),
            dir.path().join("inst-blocks"),
            dir.path().join("inst-index"),
        );
        installed.load_shard(1);
        for setup in &prelude {
            let response = installed.execute(ExecuteRequest {
                shard_id: 1,
                command: setup.clone(),
            });
            assert!(response.status.ok, "{label}: prelude failed: {response:?}");
        }
        for item in &outcomes {
            if !installed.apply_outcome_item(1, item, &carried) {
                incomplete.push(format!("{label}: apply refused a {} outcome", item.kind));
            }
        }
        let expected = ran.index_shape_for_test(1);
        let actual = installed.index_shape_for_test(1);
        if expected != actual {
            let missing = expected
                .lines()
                .filter(|line| !actual.lines().any(|other| other == *line))
                .collect::<Vec<_>>();
            let extra = actual
                .lines()
                .filter(|line| !expected.lines().any(|other| other == *line))
                .collect::<Vec<_>>();
            incomplete.push(format!(
                "{label}: recorded outcomes do not describe everything it changed -- missing {missing:?}, extra {extra:?}"
            ));
        }
    }

    assert!(
        incomplete.is_empty(),
        "these commands recorded SOMETHING but not EVERYTHING, so a recovered shard would be          quietly wrong rather than obviously broken: {incomplete:#?}"
    );
}

/// What a record COSTS, before and after -- measured through the engine, not the log.
///
/// The reclaim harness appends straight to the log and never stages a result, so its records are
/// byte-identical either way; its base offsets matched to the byte across both runs, which is how
/// that was caught rather than reported as a saving. Anything measuring this has to go through
/// execute().
///
/// BEFORE is what shipped: the operation, in text. AFTER is what ships now: results, in protobuf,
/// with no operation. Both arms set every flag explicitly, because the defaults are the thing
/// under test and inheriting them would compare a configuration against itself -- which this test
/// did, silently, the moment the defaults flipped.
#[test]
fn a_record_carrying_results_is_smaller_than_one_carrying_the_operation() {
    fn wal_bytes(binary: &str, record_results: &str, data_only: &str, writes: usize) -> (u64, u64) {
        std::env::set_var("TS_WAL_OUTCOME_ITEMS", record_results);
        std::env::set_var("TS_WAL_DATA_ONLY", data_only);
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
        );
