// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Part 2 of engine tests, split from engine/engine_regression_tests.rs.
#![allow(clippy::all)]
use super::*;

#[test]
fn memory_miss_reads_local_block_file_using_index_address() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(block_store.stats().writes, 1);

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    let second = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        second.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().memory_hits, 1);

    cache.clear_memory_for_test();
    let third = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        third.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    assert_eq!(cache.stats().disk_hits, 1);
}

#[test]
fn three_layer_cache_reads_memory_then_block_cache_then_local_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 1);
    // ONE PUT PER POINT READ, where this was two. A string get used to cache the payload twice --
    // the command response under `CacheKey::string` and the block frame under
    // `CacheKey::page_with_slot` -- and the response copy was memory-only with a single reader,
    // while the frame is the tiered, persistent copy this very test goes on to exercise. The
    // response copy is no longer written.
    //
    // KEPT EXACT RATHER THAN WIDENED TO A RANGE. The count is the subject here, and a `>=` would
    // stop noticing either a layer returning or one going away.
    assert_eq!(cache.stats().puts, 1);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let memory = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        memory.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert!(cache.stats().memory_hits >= 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.clear_memory_for_test();
    let block_cache = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        block_cache.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(cache.stats().disk_hits, 1);
    assert_eq!(block_store.stats().reads, 1);

    cache.invalidate_shard(1).unwrap();
    let local_file = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        local_file.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
    assert_eq!(block_store.stats().reads, 2);
    // EXACTLY TWO PUTS ACROSS THE THREE TIERS, where this asserted `>= 4`. The payload is cached
    // once per fill now, not twice, and the four reads above account for both puts: the cold read
    // fills the frame, the second read is a memory hit, the read after `clear_memory_for_test` is a
    // disk hit that does NOT promote, and the read after `invalidate_shard` fills it again from the
    // local file.
    //
    // PINNED EXACTLY RATHER THAN LOWERED. Measured at 2 on three consecutive runs. A threshold
    // walked down every time the number moves stops being a guard; an exact count with its
    // derivation written out fails in both directions.
    assert_eq!(cache.stats().puts, 2);
    assert!(cache.stats().memory_bytes > 0);
    assert!(cache.stats().disk_bytes > 0);

    let observation = engine.rust_storage_observation(1).unwrap();
    assert!(observation.observed_memory_hit);
    assert!(observation.observed_block_cache_hit);
    assert!(observation.observed_local_file_read);
    assert!(observation.observed_cache_invalidation);
    assert!(observation.cache_memory_bytes > 0);
    assert!(observation.cache_disk_bytes > 0);
    assert!(observation.local_block_bytes_written > 0);
    assert!(observation.local_block_bytes_read > 0);
}

#[test]
fn tiny_memory_cache_eviction_refills_from_persistence_then_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let cache = engine.cache();
    let block_store = engine.block_store();
    engine.load_shard(1);

    let target_value = b"target-value-0123456789".to_vec();
    for (key, value) in [
        ("target", target_value.clone()),
        ("evict-a", b"eviction-value-a-0123456789".to_vec()),
        ("evict-b", b"eviction-value-b-0123456789".to_vec()),
    ] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }
    let first_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(block_store.stats().reads, 1);

    for key in ["evict-a", "evict-b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        });
        assert!(
            response.status.ok,
            "eviction pressure read should pass: {response:?}"
        );
    }
    assert!(
        cache.stats().memory_evictions > 0,
        "reading multiple persisted blocks through a tiny memory cache should evict older blocks"
    );
    assert!(
        cache.stats().disk_bytes > 0,
        "persistent page read should populate block-cache files"
    );

    let target_block_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard should exist");
        let address = shard.string_address("target").expect("target address should exist");
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
        cache.get_memory(&target_block_key),
        None,
        "target page block should have been evicted from memory"
    );

    let disk_hits_before = cache.stats().disk_hits;
    let file_reads_before_block_hit = block_store.stats().reads;
    let second_read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        block_store.stats().reads,
        file_reads_before_block_hit,
        "memory miss should hit disk block cache instead of rereading block store"
    );
    assert!(
        cache.stats().disk_hits > disk_hits_before,
        "block cache should serve the read and promote it to memory"
    );
    assert_eq!(
        cache.get_memory(&target_block_key),
        Some(target_value),
        "disk block hit should promote the page block into memory"
    );
}

#[test]
// shared-corpus: storage_cache_replacement_policy_soak;
fn cache_replacement_policy_soak() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        32,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let target_value = b"target-value-0123456789".to_vec();
    for round in 0..4 {
        for item in 0..8 {
            let key = format!("soak-{round}-{item}");
            let value = format!("soak-value-{round}-{item}").into_bytes();
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key, value },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    let target_write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "soak-target".to_string(),
            value: target_value.clone(),
        },
    });
    assert!(target_write.status.ok, "{target_write:?}");

    for round in 0..4 {
        for item in 0..8 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("soak-{round}-{item}"),
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }
    let warm_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        warm_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );

    assert!(
        engine.cache().stats().memory_evictions > 0,
        "tiny memory cache should perform replacement during the pressure pass"
    );
    assert!(
        engine.cache().stats().disk_bytes > 0,
        "pressure pass should leave a disk-cache tier for cold read refill"
    );

    let target_block_key = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let address = shard
            .strings()
            .get("soak-target")
            .expect("target page address")
            .clone();
        let (start, end) = shard.routing_range();
        CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(crate::engine::hashing::block_routing_bucket("soak-target", start, end)),
        )
    };

    let evict_report = engine.apply_storage_eviction(1, 1, 4, true, false);
    assert_eq!(evict_report.mode, "evict_cache");
    assert!(evict_report.pressure_gate_open, "{evict_report:?}");
    assert!(evict_report.pressure_before > 0, "{evict_report:?}");
    assert!(
        !evict_report.selected_victims.is_empty(),
        "{evict_report:?}"
    );
    assert!(
        !evict_report.dump_manifest_ids.is_empty(),
        "dump-before-evict should durably dump dirty victim slots: {evict_report:?}"
    );
    assert!(
        evict_report.cache_entries_removed > 0 || evict_report.cache_disk_bytes_removed > 0,
        "eviction should remove at least one cache entry or disk-cache block: {evict_report:?}"
    );

    let _ = engine.cache().invalidate(&target_block_key);
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&target_block_key), None);
    let block_reads_before = engine.block_store().stats().reads;
    let disk_hits_before = engine.cache().stats().disk_hits;
    let cold_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        cold_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        engine.cache().get_memory(&target_block_key),
        Some(target_value.clone()),
        "cold read should refill memory from disk cache or page store"
    );
    assert!(
        engine.cache().stats().disk_hits > disk_hits_before
            || engine.block_store().stats().reads > block_reads_before,
        "cold read should be backed by disk cache or persistent page store"
    );
    engine.cache().clear_memory_for_test();
    assert_eq!(engine.cache().get_memory(&target_block_key), None);
    let refill_samples_before = engine.cache().stats().refill_latency_samples;
    let disk_refill_target = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "soak-target".to_string(),
        },
    });
    assert_eq!(
        disk_refill_target.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert!(
        engine.cache().stats().refill_latency_samples > refill_samples_before,
        "second cold read should hit disk cache and record refill latency"
    );
    engine.cache().set_async_writeback_queue_limit_for_test(1);
    engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(1, 50_000, 0, 8, Some(99)),
            b"writeback".to_vec(),
        )
        .unwrap();
    assert!(engine
        .cache()
        .enqueue_async_writeback(
            CacheKey::page_with_slot(1, 50_001, 0, 8, Some(99)),
            b"overflow".to_vec(),
        )
        .is_err());
    let drained = engine.cache().drain_async_writeback(8).unwrap();
    assert_eq!(drained.drained, 1);
    engine.cache().record_compaction_latency_micros(750);
    let stats = engine.cache().stats();
    assert!(stats.async_writeback_backpressure_rejections > 0);
    assert!(stats.async_writeback_max_queue_depth > 0);
    assert!(stats.async_writeback_max_queue_bytes > 0);
    assert_cache_latency_histograms_observed(stats);

    let drop_dir = tempfile::tempdir().unwrap();
    let drop_engine = TemporalEngine::with_local_dirs(
        64,
        drop_dir.path().join("cache"),
        drop_dir.path().join("blocks"),
        drop_dir.path().join("indexes"),
    );
    drop_engine.load_shard(7);
    for item in 0..6 {
        let key = format!("drop-{item}");
        let response = drop_engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: key.clone(),
                value: format!("drop-value-{item}").repeat(10).into_bytes(),
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert!(
            drop_engine
                .execute(ExecuteRequest {
                    shard_id: 7,
                    command: Command::StringGet { key },
                })
                .status
                .ok
        );
    }
    let drop_report = drop_engine.apply_storage_eviction(7, 1, 2, true, true);
    assert_eq!(drop_report.mode, "delete_drop");
    assert!(drop_report.pressure_gate_open, "{drop_report:?}");
    assert!(!drop_report.selected_victims.is_empty(), "{drop_report:?}");
    assert!(
        drop_report.dropped_object_count > 0,
        "delete/drop eviction mode should drop selected cold objects: {drop_report:?}"
    );
}

#[test]
// shared-corpus: storage_cache_replacement_policy_soak;
fn cache_dram_pmem_ssd_tiers_admit_refill_and_evict() {
    let dir = tempfile::tempdir().unwrap();
    let cache = matrixcache::MultiLayerCache::with_tiering_policy(
        dir.path(),
        matrixcache::CacheTieringPolicy {
            memory_capacity_bytes: 16,
            pmem_capacity_bytes: 24,
            ssd_capacity_bytes: 4096,
            data_placement: matrixcache::CacheDataPlacement::Tiered,
            data_placement_threshold_bytes: 0,
            memory_hotness_threshold: 10,
            pmem_admit_hotness_threshold: 5,
            ssd_admit_hotness_threshold: 2,
            max_memory_block_bytes: 32,
            max_pmem_block_bytes: 32,
            max_ssd_block_bytes: 128,
            ssd_write_through: true,
        },
        matrixcache::CacheBlockOptions::default(),
    );
    let key = CacheKey::page_with_slot(9, 1, 0, 16, Some(11));
    let request = matrixcache::CacheAdmissionRequest {
        block_kind: matrixcache::CacheBlockKind::Page,
        shard_id: 9,
        routing_slot: Some(11),
        block_bytes: 16,
        hotness: 6,
        pinned: false,
    };

    cache
        .put_with_admission(key.clone(), b"pmem-tier-value!".to_vec(), request)
        .unwrap();
    let stats = cache.stats();
    assert_eq!(stats.memory_bytes, 0, "PMEM admission should not warm DRAM");
    assert!(
        stats.pmem_bytes > 0,
        "PMEM tier should hold the admitted block"
    );
    assert!(
        stats.disk_bytes > 0,
        "SSD write-through should persist the block"
    );
    assert_eq!(stats.pmem_admission_accepted, 1);
    assert_eq!(stats.ssd_admission_accepted, 1);

    assert_eq!(
        cache.get(&key).unwrap(),
        Some(b"pmem-tier-value!".to_vec()),
        "PMEM hit should serve before SSD and promote to DRAM"
    );
    let stats = cache.stats();
    assert!(stats.pmem_hits > 0);
    assert!(stats.memory_bytes > 0);
    assert!(stats.refill_latency_samples > 0);

    for idx in 0..4 {
        let cold_key = CacheKey::page_with_slot(9, 2 + idx, 0, 16, Some(11));
        cache
            .put_with_admission(
                cold_key,
                vec![b'a' + idx as u8; 16],
                matrixcache::CacheAdmissionRequest {
                    block_kind: matrixcache::CacheBlockKind::Page,
                    shard_id: 9,
                    routing_slot: Some(11),
                    block_bytes: 16,
                    hotness: 6,
                    pinned: false,
                },
            )
            .unwrap();
    }
    let eviction = cache.eviction_report();
    assert!(
        eviction.pmem_evictions > 0,
        "PMEM capacity pressure should evict through the middle tier: {eviction:?}"
    );
    assert!(
        cache.stats().disk_bytes > 0,
        "SSD tier should retain refill copies after PMEM eviction"
    );
}

#[test]
fn restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let original =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-a"), &block_dir, &index_dir);
    original.load_shard(1);
    let target_value = b"restart-target-value-0123456789".to_vec();
    let write = original.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "target".to_string(),
            value: target_value.clone(),
        },
    });
    assert!(write.status.ok, "{write:?}");
    assert_eq!(original.block_store().stats().writes, 1);

    let restarted =
        TemporalEngine::with_local_dirs(32, dir.path().join("cache-b"), &block_dir, &index_dir);
    restarted.load_shard(1);
    let restarted_cache = restarted.cache();
    let restarted_block_store = restarted.block_store();
    let target_block_key = {
        let shards = restarted.shards.read().expect("shards lock poisoned");
        let shard = shards
            .get(&1)
            .expect("shard should exist after index replay");
        let address = shard
            .strings()
            .get("target")
            .expect("target address should be restored from index")
            .clone();
        let (start, end) = shard.routing_range();
        CacheKey::page_with_slot(
            1,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            Some(crate::engine::hashing::block_routing_bucket("target", start, end)),
        )
    };

    let first_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        first_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        1,
        "restart should miss memory and load the persisted page once"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_block_key),
        Some(target_value.clone()),
        "persistent page read should refill the memory cache"
    );
    assert!(
        restarted_cache.stats().disk_bytes > 0,
        "persistent page read should also write the disk block cache"
    );

    restarted_cache.clear_memory_for_test();
    assert_eq!(restarted_cache.get_memory(&target_block_key), None);
    let disk_hits_before = restarted_cache.stats().disk_hits;
    let block_reads_before = restarted_block_store.stats().reads;
    let second_read = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        second_read.response,
        CommandResponse::Bytes {
            value: Some(target_value.clone())
        }
    );
    assert_eq!(
        restarted_block_store.stats().reads,
        block_reads_before,
        "memory miss after restart should use the disk block cache"
    );
    assert!(
        restarted_cache.stats().disk_hits > disk_hits_before,
        "disk block cache should serve the second read"
    );
    assert_eq!(
        restarted_cache.get_memory(&target_block_key),
        Some(target_value),
        "disk block hit should promote the page block back into memory"
    );
}

#[test]
fn block_reads_fill_compressed_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::with_block_options(
        1024 * 1024,
        dir.path().join("cache"),
        matrixcache::CacheBlockOptions {
            compression: matrixcache::CacheCompression::Zstd { level: 1 },
            min_compress_bytes: 16,
        },
    );
    let engine = TemporalEngine::with_cache_block_store_and_index_dir(
        cache.clone(),
        BlockStore::new(dir.path().join("blocks")),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let value = vec![b'x'; 4096];
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large".to_string(),
            value: value.clone(),
        },
    });

    let first = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert_eq!(
        first.response,
        CommandResponse::Bytes { value: Some(value) }
    );
    assert!(cache.stats().compressed_puts >= 1);
    assert!(cache.stats().compression_bytes_saved > 0);

    cache.clear_memory_for_test();
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large".to_string(),
        },
    });
    assert!(cache.stats().compressed_hits >= 1);
}

#[test]
fn local_dirs_constructor_applies_block_store_compression_options() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
        BlockStoreOptions {
            compression_enabled: false,
            ..BlockStoreOptions::default()
        },
    );
    engine.load_shard(1);
    let value = b"engine-page-policy-".repeat(80);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "large-policy".to_string(),
            value: value.clone(),
        },
    });

    let block_store = engine.block_store();
    let stats = block_store.stats();
    assert_eq!(stats.writes, 1);
    assert_eq!(stats.compressed_records_written, 0);
    assert_eq!(stats.compression_bytes_saved, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "large-policy".to_string(),
        },
    });
    assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
}

#[test]
fn write_invalidates_cached_string() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MultiLayerCache::new(1024, dir.path());
    let engine = TemporalEngine::new(cache.clone());
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"old".to_vec(),
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
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"new".to_vec(),
        },
    });
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"new".to_vec())
        }
    );
    assert!(cache.stats().invalidations >= 2);
}

#[test]
fn async_storage_string_write_stays_on_hot_memory_path() {
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
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    let write = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "hot".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    // The write records
    // a WAL entry even in async mode; async only means the commit does not
    // block (no fsync -> syncs == 0), and page/index materialization is deferred.
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).syncs, 0);
    assert_eq!(engine.index_log_store().stats(1).writes, 0);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "hot".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
    assert_eq!(engine.block_store().stats().reads, 0);
    assert!(engine.cache().stats().memory_hits >= 1);
}

#[test]
fn async_hot_block_survives_memory_eviction_via_spill() {
    // Regression for the log-backed hot-page read-as-missing bug (gap P1): under async_storage a
    // write lives ONLY in the memory tier at a synthetic hot-page address. If the memory-only
    // entry is evicted before the next dump, a read used to miss the cache, hit a non-existent
    // slab file, and return value:None for an ACKED write. The eviction handler now spills the
    // evicted hot page to a real slab and a hot-address cache miss resolves the redirect, so the
    // value survives eviction on the live path (no reload needed).
    let dir = tempfile::tempdir().unwrap();
    // Deliberately tiny memory tier so the flood below forces capacity eviction of the target.
    let engine = TemporalEngine::with_local_dirs(
        2048,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    // Async write -> memory-only hot page, nothing in the block store yet.
    let target_value = b"durable-async-value-that-must-survive-eviction".to_vec();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "target".to_string(),
                    value: target_value.clone(),
                },
            })
            .status
            .ok
    );
    assert_eq!(engine.block_store().stats().writes, 0);

    // Flood the memory tier with far more async hot pages than it can hold, forcing the target's
    // page to be evicted (and spilled to a real slab by the eviction handler).
    for i in 0..400 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("filler-{i}"),
                value: vec![b'x'; 96],
            },
        });
    }

    // At least one hot page must have spilled to a durable slab (proves the handler fired).
    assert!(
        engine.block_store().stats().writes > 0,
        "evicted hot pages must spill to a durable slab"
    );

    // Drop EVERY cache tier for the shard so the only surviving copy of the target is the durable
    // spilled slab (plus the WAL). This defeats the cache's own cross-tier retention and forces
    // the read to resolve through the hot-page spill redirect -- the path this fix adds.
    let _ = engine.cache().invalidate_shard(1);

    let reads_before = engine.block_store().stats().reads;
    // The acked write must still read back as its value -- resolved via the spill redirect + a
    // durable slab read, NOT as value:None.
    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "target".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(target_value)
        },
        "an acked async write must not read as missing after its hot page is evicted"
    );
    // The value came from the durable spilled slab, confirming the redirect fallback was used.
    assert!(
        engine.block_store().stats().reads > reads_before,
        "the evicted target must be served from the durable spilled slab"
    );
}

fn set_async_config(engine: &TemporalEngine, shard_id: ShardId) {
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
}

#[test]
fn atomic_batch_survives_restart_when_complete() {
    // Positive control for gap E3: a fully-persisted atomic batch replays in full on restart.
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-a"), &block_dir, &index_dir);
    engine.load_shard(1);
    set_async_config(&engine, 1);
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet { key: "b0".to_string(), value: b"v0".to_vec() },
            Command::StringSet { key: "b1".to_string(), value: b"v1".to_vec() },
            Command::StringSet { key: "b2".to_string(), value: b"v2".to_vec() },
        ],
    });
    assert!(batch.status.ok);
    drop(engine);

    let restarted =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-b"), &block_dir, &index_dir);
    restarted.load_shard(1);
    for (key, value) in [("b0", "v0"), ("b1", "v1"), ("b2", "v2")] {
        let read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.to_string() },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes { value: Some(value.as_bytes().to_vec()) },
            "complete batch key {key} must survive restart"
        );
    }
}

#[test]
fn atomic_batch_is_all_or_nothing_when_commit_marker_lost() {
    // Gap E3: a crash between the batch's buffered appends and its single durability barrier can
    // leave a partial suffix on disk. Simulate that by dropping the batch's final (commit-marker)
    // WAL line; on restart the WHOLE batch must be discarded -- never a partially-applied prefix
    // that a retry would double-apply.
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-a"), &block_dir, &index_dir);
    engine.load_shard(1);
    set_async_config(&engine, 1);
    // A standalone durable write before the batch, to prove only the batch is dropped.
    assert!(engine
        .execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: "keep".to_string(), value: b"kept".to_vec() },
        })
        .status
        .ok);
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet { key: "b0".to_string(), value: b"v0".to_vec() },
            Command::StringSet { key: "b1".to_string(), value: b"v1".to_vec() },
            Command::StringSet { key: "b2".to_string(), value: b"v2".to_vec() },
        ],
    });
    assert!(batch.status.ok);
    drop(engine);

    // Simulate the lost commit marker: drop the last WAL line (batch_index == batch_size).
    let wal_path = index_dir.join("wals").join("shard-1.wal.bin");
    // Read as BYTES and walk the records by their FRAMES, not by newlines. A record is only a
    // "line" while the frame ends with one; once it declares its own length the payload carries
    // 0x0A freely, and splitting on that byte cuts records in half -- which makes this test
    // build a log no writer would ever produce and then assert on how recovery handles it.
    // Asking the frame where each record ends works whichever frame wrote it.
    let contents = std::fs::read(&wal_path).expect("wal file should exist");
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut at = 0usize;
    while at < contents.len() {
        // The preallocated zeros reservation trails the records and is not one of them.
        if contents[at] == 0 {
            break;
        }
        match crate::log_framing::next_frame(&contents[at..]) {
            Ok(Some((consumed, _))) if consumed > 0 => {
                spans.push((at, at + consumed));
                at += consumed;
            }
            _ => break,
        }
    }
    // however many records the batch became. Written as N records sharing a batch id the last
    // one is the commit marker and dropping it strands the rest; written as ONE record carrying
    // every item, dropping it removes the batch outright. Both are the same test -- cut the tail
    // and require that nothing of the batch survives -- and neither depends on the count, so this
    // asserts the shape it needs rather than the shape one format happens to produce.
    assert!(
        spans.len() >= 2,
        "expected the standalone write plus at least one batch record, got {}",
        spans.len()
    );
    spans.pop(); // the commit marker, or the whole batch when it is one record
    let mut truncated = Vec::new();
    for (start, end) in spans {
        truncated.extend_from_slice(&contents[start..end]);
    }
    std::fs::write(&wal_path, truncated).expect("rewrite wal");

    let restarted =
        TemporalEngine::with_local_dirs(1024 * 1024, dir.path().join("cache-b"), &block_dir, &index_dir);
    restarted.load_shard(1);

    // The pre-batch durable write survives.
    assert_eq!(
        restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: "keep".to_string() },
            })
            .response,
        CommandResponse::Bytes { value: Some(b"kept".to_vec()) }
    );
    // No member of the incomplete batch is applied -- not even the durable prefix (b0, b1).
    for key in ["b0", "b1", "b2"] {
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: key.to_string() },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "incomplete-batch key {key} must NOT be applied"
        );
    }
}

#[test]
fn durable_execute_overrides_async_storage_for_raft_local_file_path() {
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
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    let write = engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "raft".to_string(),
            value: b"value".to_vec(),
        },
    });
    assert!(write.status.ok);
    assert_eq!(engine.block_store().stats().writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    assert_eq!(engine.index_log_store().stats(1).writes, 1);

    let read = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "raft".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"value".to_vec())
        }
    );
}

#[test]
fn durable_index_survives_restart_and_points_to_block_file() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache-a");
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &block_dir, &index_dir);
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"persisted".to_vec(),
        },
    });

    let restarted =
        TemporalEngine::with_local_dirs(1024, dir.path().join("cache-b"), &block_dir, &index_dir);
    restarted.load_shard(1);
    let response = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"persisted".to_vec())
        }
    );
    assert_eq!(restarted.block_store().stats().reads, 1);
}

#[test]
fn async_write_survives_restart_via_wal_replay_like_native() {
    // conformance: an async_storage write records a WAL entry but defers page/
    // index materialization to the background dump. If the crash beats the dump,
    // Startup load replays the wal and recovers the write. Rust must replay
    // its WAL on shard load the same way, or the async write is silently lost.
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
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"async-durable".to_vec(),
        },
    });
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let response = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"async-durable".to_vec())
        },
        "async write must be recovered by replaying the WAL on shard load"
    );
}

// Readiness race regression: after a restart with WAL entries not yet reflected in the served
// index, the shard is published but NOT serving until `replay_wal_into_shard` completes
// (ShardInfo.recovering gates it). A read that arrives inside that window must NOT observe a
// false null from the half-reconstructed state -- it must be rejected with a retryable
// `shard_not_loaded`. Once replay completes the same read must return the recovered value.
// (The fix commit gated this deliberately without a deterministic test; this pins it.)
#[test]
fn read_during_wal_replay_recovery_returns_retryable_not_false_null() {
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
    // async_storage: the write lands in the WAL but page/index materialization is deferred, so
    // it is absent from the served index until the WAL is replayed on the next load.
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"recovered-value".to_vec(),
        },
    });
    assert_eq!(engine.block_store().stats().writes, 0);
    assert_eq!(engine.write_ahead_log_store().stats(1).writes, 1);
    drop(engine);

    // Restart, and park the shard in the recovery window: base index loaded (the async write is
    // NOT in it yet), recovering=true, WAL not yet replayed.
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    let watermark = restarted.test_publish_recovering_shard(1);

    // A read inside the recovery window must be rejected retryably, NOT return a false null.
    let during = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(
        !during.status.ok,
        "read during WAL-replay recovery must be rejected, not served from half-built state"
    );
    assert_eq!(during.status.code, "shard_not_loaded");
    assert_eq!(
        during.response,
        CommandResponse::Empty,
        "recovering read must return a retryable error, never a false null (Bytes {{ None }})"
    );

    // After replay completes and the gate clears, the same read returns the recovered value.
    restarted.test_finish_recovery(1, watermark);
    let after = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert!(after.status.ok, "shard must serve once recovery completes");
    assert_eq!(
        after.response,
        CommandResponse::Bytes {
            value: Some(b"recovered-value".to_vec())
        },
        "the WAL-recovered value must be served after recovery completes"
    );
}

// Coalesced control-state persistence skips the per-write whole-series page rewrite
// (write-amplification) and relies on the index snapshot + WAL replay for durability, the
// same model control_state_changes/fol already use. This MUST stay crash-safe: increments
// recorded only in the WAL (no page materialization, no flush) must be recovered on reload.
#[test]
fn control_state_coalesced_write_survives_restart_via_wal_replay() {
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
    let mut config = Config {
        version: 2,
        async_storage: true,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_coalesce_persist".to_string(), "on".to_string());
    assert!(engine.set_config(SetConfigRequest { shard_id: 1, config }).ok);

    for (timestamp_ms, amount) in [(10u64, 1i64), (20, 2), (30, 4), (40, 8)] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateIncrement {
                key: "cap".to_string(),
                timestamp_ms,
                amount,
            },
        });
    }
    // Coalesced: the per-write control_state page rewrite is skipped; the WAL carries durability.
    assert_eq!(
        engine.block_store().stats().writes,
        0,
        "coalesced control-state writes must not materialize a per-write page"
    );
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 4,
        "each coalesced increment must still be recorded in the WAL"
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let response = restarted
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateCount {
                key: "cap".to_string(),
                start_ms: 0,
                end_ms: 100,
            },
        })
        .response;
    assert_eq!(
        response,
        CommandResponse::Integer { value: 15 },
        "coalesced control-state counter must be recovered by WAL replay on reload"
    );
}

#[test]
fn async_storage_batch_write_records_wal_without_sync_or_index() {
    // Batch analog of the single-command async conformance: the batch path (used by
    // ingestion) must also record a WAL entry per async write, unfsynced, with page/
    // index materialization deferred.
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
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    let batch = engine.batch_execute(BatchExecuteRequest {
        shard_id: 1,
        commands: vec![
            Command::StringSet {
                key: "a".to_string(),
                value: b"a".to_vec(),
            },
            Command::StringSet {
                key: "b".to_string(),
                value: b"b".to_vec(),
            },
        ],
    });
    assert!(batch.status.ok);
    assert_eq!(engine.block_store().stats().writes, 0);
    // logged at all, not logged once per command. Whether a batch of two becomes two records
    // sharing a batch id or one record carrying both items is a property of the log format, and
    // this test is about what an ASYNCHRONOUS batch does and does not touch: the log yes, the
    // barrier no, the block store no, the index no. Pinning the count made it a test of the
    // format instead, which is how it came to fail on a change that took nothing away from it.
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 1,
        "the batch must reach the log"
    );
    assert_eq!(engine.write_ahead_log_store().stats(1).syncs, 0);
    assert_eq!(engine.index_log_store().stats(1).writes, 0);
}

#[test]
fn eviction_ranks_victims_least_recently_used_first_like_native() {
    // Eviction victims are ordered least-recently-used first, so a
    // repeatedly-read hot bucket is evicted last. Rust stamps per-bucket recency on
    // every read/write and sorts victims oldest-first.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for key in ["cold-a", "cold-b", "cold-c", "cold-d", "hot"] {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: b"value".to_vec(),
            },
        });
    }
    std::thread::sleep(std::time::Duration::from_millis(3));
    for _ in 0..3 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "hot".to_string(),
            },
        });
    }
    let report = engine.apply_storage_eviction(1, 0, 0, false, false);
    assert!(
        report.selected_victims.len() >= 2,
        "expected multiple victim buckets: {report:?}"
    );
    let recencies: Vec<u64> = report
        .selected_victims
        .iter()
        .map(|victim| victim.last_touched_ms)
        .collect();
    assert!(
        recencies.windows(2).all(|pair| pair[0] <= pair[1]),
        "victims must be ordered least-recently-used first: {report:?}"
    );
    assert!(
        recencies.last() > recencies.first(),
        "the hot bucket must have the newest recency and sort last: {report:?}"
    );
}

#[test]
fn wal_replay_gap_refuses_load_like_dataloss() {
    // WAL replay returns DataLoss and aborts load on a hole in the
    // retained wal. Rust must likewise refuse the load (not-loaded) rather than
    // silently serve a truncated prefix.
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    // Seed the WAL with a sequence hole {1,2,4} (missing 3) before loading the shard;
    // there is no served index, so recovery replays from watermark 0.
    let wal = engine.write_ahead_log_store();
    for seq in [1u64, 2, 4] {
        wal.append_replayed_record(WriteAheadLogRecord {
            shard_id: 1,
            sequence: seq,
            command: Some(Command::StringSet {
                key: format!("k{seq}"),
                value: b"v".to_vec(),
            }),
            metadata: None,
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        })
        .unwrap();
    }
    let response = engine.load_shard_with(LoadShardRequest {
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
        "load must refuse on a WAL sequence gap: {response:?}"
    );
    assert_eq!(response.status.code, "wal_replay_sequence_gap");
    // The shard must be left NOT loaded (unwound).
    let get = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k1".to_string(),
        },
    });
    assert!(
        !get.status.ok,
        "a refused load must leave the shard not-loaded: {get:?}"
    );
}

#[test]
fn expiry_sweep_emits_wal_delete_marker_like_native() {
    // Active expiry is a logged, replicated delete. Rust's sweep must append a
    // WAL tombstone per expired key so followers / WAL replay observe the deletion,
    // instead of removing only in-memory + index.
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
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "k".to_string(),
            ttl_ms: 1,
        },
    });
    std::thread::sleep(std::time::Duration::from_millis(5));
    let wal_before_sweep = engine.write_ahead_log_store().stats(1).writes;
    let report = engine.sweep_expired_records(1).unwrap();
    assert_eq!(report.expired_records_removed, 1);
    let wal_after_sweep = engine.write_ahead_log_store().stats(1).writes;
    assert_eq!(
        wal_after_sweep - wal_before_sweep,
        1,
        "the expiry sweep must append one WAL delete (tombstone) per expired key"
    );
}


/// Pins the two clocks these recovery tests depend on: the timestamp stamped onto each WAL
/// record (which replay reads back as the leader clock) and the clock the engine computes
/// deadlines with. Both are restored on drop, including on panic, so a pinned clock can
/// never leak into another test on this thread.
static PINNED_LEADER_CLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct PinnedLeaderClock(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl PinnedLeaderClock {
    fn at(leader_now_ms: u64) -> Self {
        // The record clock is process-wide, so two tests pinning it at once would stamp each
        // other's records. Held for the few writes that need it, released on drop.
        let guard = PINNED_LEADER_CLOCK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        crate::wal::set_test_record_clock_ms(Some(leader_now_ms));
        crate::engine::set_replay_clock_ms(Some(leader_now_ms));
        PinnedLeaderClock(guard)
    }

    fn real_now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_millis() as u64
    }
}

impl Drop for PinnedLeaderClock {
    fn drop(&mut self) {
        crate::wal::set_test_record_clock_ms(None);
        crate::engine::set_replay_clock_ms(None);
    }
}

#[test]
fn wal_replay_uses_leader_timestamp_for_ttl_deadline_like_native() {
    // resolves a TTL to an ABSOLUTE deadline on the leader before logging it
    // (deadline = request.ttl_ms + current time at log time); replay uses that
    // value. Rust must resolve replayed TTLs against the leader's logged timestamp, not
    // the (later) restart clock -- otherwise crash recovery resurrects a key past its
    // deadline with a fresh restart-time+ttl lifetime.
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
    // async so the SETEX lives only in the WAL, forcing a replay on reload.
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSetEx {
            key: "k".to_string(),
            value: b"v".to_vec(),
            ttl_ms: 40,
        },
    });
    // Sleep well past the 40ms TTL so the leader deadline is in the past at reload.
    std::thread::sleep(std::time::Duration::from_millis(160));
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes { value: None },
        "a key past its leader TTL deadline must not be resurrected with a replay-time deadline"
    );
}

/// A WAL record carrying NO leader timestamp must replay against the LIVE clock.
///
/// The rule is already written down for the raft-apply path, on `execute_raft_apply_at`: "`None`
/// (or a zero stamp, from an entry written before the log carried one) keeps the live clock".
/// `ReplayClockGuard::enter` enforces it with `.filter(|stamp| *stamp > 0)`. The WAL replay loop
/// sets the same thread-local, but set it straight from `metadata.timestamp_ms` -- so an
/// unstamped record pinned the replay clock to 0 and every relative deadline resolved during it
/// landed in 1970. A durably acknowledged write then comes back from recovery already expired,
/// which is the quietest way a store can lose one.
///
/// The zero is reachable rather than hypothetical: protobuf is the only WAL encoder, and its
/// decoder copies this field verbatim, so an absent `timestamp_ms` decodes as 0 -- while the
/// field immediately beside it, `version`, IS normalized for precisely this case.
///
/// BOTH HALVES ARE ASSERTED SEPARATELY, and they fail the same way for opposite reasons: a
/// stamped record must still resolve against its stamp (the behaviour this must not break), and
/// an unstamped one must not resolve against a zero. Asserting only one of them would pass on a
/// replay that got the other exactly backwards.
#[test]
fn wal_replay_reads_an_unstamped_record_against_the_live_clock() {
    use crate::wal::{WriteAheadLogRecord, WriteAheadLogRecordMetadata};

    const ONE_MINUTE_MS: u64 = 60 * 1000;

    // Records built by hand and written straight into the log, because the stamp is the whole
    // subject: no writer API will produce a zero on request (`set_test_record_clock_ms` reads
    // its own zero as "not pinned"), so the only way to state one is to write the record.
    fn record_at(sequence: u64, key: &str, stamp_ms: u64) -> WriteAheadLogRecord {
        WriteAheadLogRecord {
            shard_id: 1,
            sequence,
            command: Some(Command::StringSetEx {
                key: key.to_string(),
                value: b"v".to_vec(),
                ttl_ms: ONE_MINUTE_MS,
            }),
            metadata: Some(WriteAheadLogRecordMetadata {
                version: crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION,
                timestamp_ms: stamp_ms,
                items: Vec::new(),
                batch_id: None,
                batch_size: None,
                batch_index: None,
            }),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let live_now_ms = PinnedLeaderClock::real_now_ms();

    {
        let writer = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache-a"),
            &block_dir,
            &index_dir,
        );
        let log = writer.write_ahead_log_store();
        // Sequence 1: STAMPED an hour ago, one-minute TTL. Its deadline is long past.
        log.append_replayed_record(record_at(1, "stamped", live_now_ms - 60 * ONE_MINUTE_MS))
            .unwrap();
        // Sequence 2: UNSTAMPED, same one-minute TTL. Nothing in the record says when the
        // leader took it, so the only honest reading of its deadline is "a minute from now".
        log.append_replayed_record(record_at(2, "unstamped", 0))
            .unwrap();
    }

    // No index file was ever written, so the replay watermark is 0 and both records replay.
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    restarted.load_shard(1);

    let ttl_of = |key: &str| {
        match restarted
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonTtl {
                    key: key.to_string(),
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("TTL for {key} answered {other:?}"),
        }
    };

    // HALF A, the control. The stamped record still resolves against its own stamp, so its
    // deadline sits 59 minutes in the past and the key is gone. -2 is "no such key". This half
    // passes before the fix as well as after; it is here so a change that repaired half B by
    // ignoring every stamp could not pass.
    assert_eq!(
        ttl_of("stamped"),
        -2,
        "a record carrying a real leader stamp must resolve its deadline against that stamp, \
         not against the restart clock"
    );

    // HALF B, the finding. Read against a zero clock the unstamped record's deadline is 60000 --
    // an absolute instant in 1970 -- so it replays already-expired and answers -2, the same
    // number as half A for the opposite reason.
    let remaining = ttl_of("unstamped");
    assert!(
        remaining > 0 && remaining <= ONE_MINUTE_MS as i64,
        "an unstamped record must resolve its one-minute TTL against the live clock, leaving \
         at most {ONE_MINUTE_MS}ms and more than 0; got {remaining} (-2 means the replay clock \
         was pinned to 1970 and a durable write came back from recovery already expired)"
    );
}

#[test]
fn wal_replay_conditional_write_uses_leader_clock_for_lazy_expiry_like_native() {
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
    // async so both records live only in the WAL, forcing a replay on reload.
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    // An hour of downtime, STATED rather than slept for. Sleeping for it was the defect in
    // this test: the two writes below have to land while k is still live, so a real deadline
    // made that a race against whatever else the machine was doing, and any stall longer than
    // the TTL failed the test on a property it is not about.
    let leader_now_ms = PinnedLeaderClock::real_now_ms() - 60 * 60 * 1000;
    {
        let _clock = PinnedLeaderClock::at(leader_now_ms);
        // Record 1: create k with a one-minute deadline, an hour before restart.
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: "k".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: 60 * 1000,
            },
        });
        // Record 2: a SET XX refreshing k=v2 with a deadline that OUTLASTS the downtime. On
        // the leader this fires because k is live at leader time, and it now fires
        // unconditionally, because the deadline and the check read the same pinned clock.
        let cond = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: Some(2 * 60 * 60 * 1000),
                condition: StringSetCondition::IfExists,
                return_old: false,
                keep_ttl: false,
            },
        });
        assert_eq!(
            cond.response,
            CommandResponse::Integer { value: 1 },
            "SET XX must fire on the leader while k is live"
        );
    }
    // These writes are asynchronous: the commit does not block, so a returned write does not
    // mean the record has reached the file. The engine has no Drop that flushes, so dropping
    // it is not a barrier either -- the successor below can open the log before the last
    // record lands in it, and a replay test whose log is missing its last record fails
    // looking exactly like a recovery defect. Flush, then check there is something to replay.
    let flushed = engine
        .write_ahead_log_store()
        .flush(1)
        .expect("flush the log before the engine goes away");
    let _ = flushed;
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 2,
        "premise: the log must hold the writes this test is about to replay, got {}",
        engine.write_ahead_log_store().stats(1).writes
    );
    drop(engine);

    // Restart on the REAL clock. Record 1's deadline lapsed 59 minutes ago; the deadline the
    // SET XX installed is still an hour out. Replay installs record 1's outcome (an absolute
    // leader-time deadline) and re-executes record 2, whose lazy-expiry check picks the
    // branch: against the leader clock k is live and the write survives; against the restart
    // clock k reads expired and a durably-committed write is silently lost.
    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    // Assert the LOAD, not just what is readable after it. `load_shard` throws its response
    // away, so a shard that refused to recover looks identical to one that recovered empty --
    // every assertion below then reports a missing value and blames replay for dropping it,
    // which is the wrong end of the failure and is exactly how this was read for several
    // rounds.
    let loaded = restarted.load_shard_with(LoadShardRequest {
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
        loaded.status.ok,
        "the shard refused the load: {:?}",
        loaded.status
    );

    let get = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        get.response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        },
        "replay must reproduce the leader's SET XX branch (k live at leader time), not drop \
         the committed write because the original deadline lapsed during downtime"
    );
}

#[test]
fn wal_replay_rearmed_expire_does_not_abort_recovery_like_native() {
    // A committed EXPIRE that re-arms a key's TTL must replay cleanly even if the key's
    // ORIGINAL (pre-re-arm) deadline has lapsed by restart time. Command preconditions are
    // a LEADER-time gate; re-running them during replay against the restart clock made a
    // replayed EXPIRE fail "not_found" and abort the WHOLE shard load (data unavailability
    // for every key on the shard). ReplayWal re-applies logged effects without
    // re-checking preconditions. The canary below proves the shard actually recovered.
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
    // async so every record lives only in the WAL, forcing a full replay on reload.
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );

    // An hour of downtime, stated rather than slept for, as in the sibling test. What this
    // test needs is that k's deadline has LAPSED by restart -- a premise that now holds by
    // construction. It previously held only if the sleep outlasted the TTL, and widening the
    // TTL to steady the race quietly inverted it: 250ms of downtime against a 1500ms
    // deadline left the key live at restart, so the test passed without ever building the
    // situation it exists to check.
    let leader_now_ms = PinnedLeaderClock::real_now_ms() - 60 * 60 * 1000;
    {
        let _clock = PinnedLeaderClock::at(leader_now_ms);
        // Durable canary that survives iff replay runs to completion (not aborted mid-way).
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "canary".to_string(),
                value: b"present".to_vec(),
            },
        });
        // Create k with a one-minute deadline, then re-arm it via EXPIRE. Both logged, both
        // an hour before restart, so both deadlines are in the past by the time replay runs.
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: "k".to_string(),
                value: b"v".to_vec(),
                ttl_ms: 60 * 1000,
            },
        });
        let rearmed = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "k".to_string(),
                ttl_ms: 60 * 1000,
            },
        });
        assert!(
            rearmed.status.ok,
            "EXPIRE on a live key should succeed on the leader"
        );
    }
    // These writes are asynchronous: the commit does not block, so a returned write does not
    // mean the record has reached the file. The engine has no Drop that flushes, so dropping
    // it is not a barrier either -- the successor below can open the log before the last
    // record lands in it, and a replay test whose log is missing its last record fails
    // looking exactly like a recovery defect. Flush, then check there is something to replay.
    let flushed = engine
        .write_ahead_log_store()
        .flush(1)
        .expect("flush the log before the engine goes away");
    let _ = flushed;
    assert!(
        engine.write_ahead_log_store().stats(1).writes >= 3,
        "premise: the log must hold the writes this test is about to replay, got {}",
        engine.write_ahead_log_store().stats(1).writes
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    // Assert the LOAD, not just what is readable after it. `load_shard` throws its response
    // away, so a shard that refused to recover looks identical to one that recovered empty --
    // every assertion below then reports a missing value and blames replay for dropping it,
    // which is the wrong end of the failure and is exactly how this was read for several
    // rounds.
    let loaded = restarted.load_shard_with(LoadShardRequest {
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
        loaded.status.ok,
        "the shard refused the load: {:?}",
        loaded.status
    );

    // THE PREMISE, ASSERTED. This test is only meaningful if k's deadline actually lapsed
    // while the engine was down; if it did not, the situation it exists to check was never
    // built and the canary below passes for free. That is not hypothetical -- widening this
    // test's TTL to steady a race once left 250ms of downtime against a 1500ms deadline, and
    // it kept passing while checking nothing. So: k must be GONE at restart.
    let lapsed = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        lapsed.response,
        CommandResponse::Bytes { value: None },
        "premise: k's re-armed deadline must have lapsed during the downtime, or this test \
         is not exercising a replayed EXPIRE whose deadline is in the past"
    );
    let canary = restarted.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "canary".to_string(),
        },
    });
    assert_eq!(
        canary.response,
        CommandResponse::Bytes {
            value: Some(b"present".to_vec())
        },
        "shard must recover: a replayed re-arming EXPIRE whose original deadline lapsed must \
         not abort the whole WAL replay"
    );
}

#[test]
fn hdel_last_field_removes_key_like_native() {
    // hash2::Del removes the whole object once the last field is deleted; Rust left an
    // empty field map behind, so the key still reported as existing (EXISTS=1, TYPE=hash).
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashDelete {
            key: "h".to_string(),
            field: "f".to_string(),
        },
    });
    let exists = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExists {
            key: "h".to_string(),
        },
    });
    assert_eq!(
        exists.response,
        CommandResponse::Integer { value: 0 },
        "an emptied hash must not leave a phantom key (deletes the object on last HDEL)"
    );
}

#[test]
fn timestamped_models_survive_unload_reload_round_trip() {
    // Regression net for the membership-authoritative reconcile + serde round-trip: write a
    // string plus the reconcile page-reading timestamped kinds (feature + sequence), then
    // unload + reload and assert every value comes back intact (no loss, no resurrection).
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
        command: Command::StringSet {
            key: "s".to_string(),
            value: b"string-value".to_vec(),
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"a".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"b".to_vec(),
                },
            ],
        },
    });
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "seq".to_string(),
            rows: vec![SequenceFeatureRow {
                timestamp_ms: 5,
                gid: 1,
                action_type: 2,
                duration: 3,
                author_id: 4,
            }],
        },
    });

    let feature_timestamps = |engine: &TemporalEngine| -> Vec<u64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|p| p.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    let sequence_len = |engine: &TemporalEngine| -> usize {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: "seq".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: 100,
                    filters: Vec::new(),
                },
            })
            .response
        {
            CommandResponse::SequenceRows { rows } => rows.len(),
            other => panic!("expected SequenceRows, got {other:?}"),
        }
    };
    let features_before = feature_timestamps(&engine);
    assert_eq!(features_before, vec![10, 20]);
    assert_eq!(sequence_len(&engine), 1);

    engine.unload_shard(1);
    engine.load_shard(1);

    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "s".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"string-value".to_vec()),
        },
        "string must survive reload"
    );
    assert_eq!(
        feature_timestamps(&engine),
        features_before,
        "feature series must survive reload unchanged"
    );
    assert_eq!(
        sequence_len(&engine),
        1,
        "sequence row must survive reload"
    );
}

#[test]
fn reconcile_does_not_resurrect_evicted_feature_points_on_reload() {
    // A single FeatureAppend packs all timestamps into one page; the feature_max_size trim
    // drops the oldest from the in-memory series, but the page still physically holds them.
    // reconcile-from-pages must keep the persisted (trimmed) membership -- the evicted points
    // must NOT resurrect on reload (writes per-timestamp deleted tombstones for this).
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &block_dir,
        &index_dir,
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: 3,
                    ..Config::default()
                },
            })
            .ok
    );
    // One append of 5 points; the max_size=3 trim keeps the newest 3 in the series.
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f".to_string(),
            points: (1..=5)
                .map(|i| FeaturePoint {
                    timestamp_ms: i * 10,
                    value: vec![i as u8],
                })
                .collect(),
        },
    });
    let live_timestamps = |engine: &TemporalEngine| -> Vec<u64> {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureQuery {
                    key: "f".to_string(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: Some(100),
                },
            })
            .response
        {
            CommandResponse::FeaturePoints { points } => {
                points.iter().map(|point| point.timestamp_ms).collect()
            }
            other => panic!("expected FeaturePoints, got {other:?}"),
        }
    };
    let before = live_timestamps(&engine);
    assert!(
        before.contains(&50) && !before.contains(&10),
        "trim should keep the newest points and drop the oldest: {before:?}"
    );
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &block_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        live_timestamps(&restarted),
        before,
        "reload must not resurrect the evicted feature points that still physically live in the page"
    );
