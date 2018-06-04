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
