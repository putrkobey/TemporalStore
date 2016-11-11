// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE THE NINE GO.
//!
//! #1952 measured a warm serving read at 10.00 allocations and split it once: exactly one of the
//! ten is inside the cache, and nine are this engine's. It named those nine as the largest
//! remaining item on the read path and deliberately did not start on them. This module takes
//! them apart, one named site at a time, until the residual is ZERO -- not small, zero -- and
//! then removes the two that were buying nothing.
//!
//! MEASURED WHOLE, THEN DECOMPOSED, NEVER SUMMED. The figure that carries every claim here is
//! the SPAN total over 512 serving reads with real stored data, exactly as #1952 took it. The
//! per-site rows are an attribution of that total, charged inside the operation while it runs,
//! and the check that the attribution is complete is the residual reading zero. A decomposition
//! is a hypothesis about where a measured total went; per-statement measurements added up are
//! not a total at all, and MatrixCache #92 reported seven allocations for a line that costs nine
//! by doing exactly that.
//!
//! THE TEN, DECOMPOSED. Store path held at fifteen characters, key width twenty-five, 512
//! records, 512 serving reads, `residual 0 allocs 0 B`:
//!
//! ```text
//!   site                              allocs/read   B/read   whose
//!   the cache's own lookup                   1.00     1045   the cache crate's
//!   the request's key, built by the caller   1.00       32   the caller's
//!   command.clone() into execute_on_shard    1.00       25   ours
//!   the LRU recency stamp                    2.00       49   ours
//!   CacheKey::string                         2.00       30   the cache crate's
//!   decoding the cached answer, the tag      1.00        5   ours
//!   decoding the cached answer, the value    1.00     1024   ours
//!   Status::ok()'s "ok"                      1.00        2   ours
//!   ----------------------------------------------------------------
//!   total                                   10.00     2212
//! ```
//!
//! SO "NINE ARE OURS" IS TWO CLAIMS, AND ONLY ONE OF THEM HOLDS. One of the nine is the
//! caller's: `Command::StringGet` owns its key, the fixture builds that `String` inside the
//! measured span, and a production request builds it in the wire decoder instead. Two more are
//! the cache crate's: `CacheKey::string` is `MultiLayerCache`'s own key constructor and is no
//! more ours than the lookup it is handed to. The engine's own share of a warm read is SIX
//! allocations, not nine.
//!
//! AND THE TEN IS A FLOOR. `TemporalEngine::load_shard` -- what every arm in #1952 used --
//! writes `table_name: String::new()` and `shard_uri: String::new()`, and an empty `String`
//! clones without allocating. A shard loaded the way a metaserver loads one carries both, and
//! `execute_with_storage_override` clones the whole `ShardInfo` on EVERY command to read four
//! fields, three of which are `Copy`. Measured on the same fixture with the two names filled in:
//! **12.00 allocations, not 10.00**. That row is `shard_info_clone 2.00`, it is the single
//! largest avoidable item on this path, and no fixture in #1952 could see it.
//!
//! WHAT THIS CHANGES, AND WHAT IT PRICES AND LEAVES. The recency stamp -- 2.00 of the ten, 49
//! bytes -- built a `Vec<String>` and cloned each key into it so that a routing bucket could be
//! derived and a `u32 -> u64` inserted. It visits the keys now. Everything else is priced in the
//! pull request with the reason it stands, and the largest of them, the `ShardInfo` clone, is
//! named rather than taken: removing it means holding the `infos` read guard across the
//! admission check, and `admission_limits` reads `table_name` out of an owned
//! `&Option<ShardInfo>`.
//!
//! COUNTS CARRY EVERY CLAIM. The byte columns wobble between otherwise identical runs and the
//! counts do not -- #1952 observed that and so does this. The store path length is asserted
//! equal across arms rather than assumed, because allocation bytes move with it at about six
//! bytes per character.

// Unconditional: the guards at the foot of this file run in an ORDINARY build, where the
// counting allocator is not installed. The measurement apparatus above them is gated item by
// item instead.
#[allow(unused_imports)]
use super::*;

/// Records written in the small arm.
#[cfg(feature = "alloc-probe")]
const SMALL: usize = 512;
/// Records written in the large arm. Eight times the small one, so a per-read cost that grew
/// with the corpus would show as a ratio near eight rather than near one.
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 4_096;
/// Serving reads measured in EVERY arm.
#[cfg(feature = "alloc-probe")]
const READS: usize = 512;
/// The stored value's width, in bytes.
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;
/// What a warm serving read cost before this change, on the `load_shard` fixture. The band below
/// EXCLUDES it: the assertion is strictly less than this, so no value can satisfy both.
#[cfg(feature = "alloc-probe")]
const ALLOCS_BEFORE: f64 = 10.0;
/// What the per-command `ShardInfo` clone costs a shard that carries the two names a metaserver
/// gives it: one allocation for the table name and one for the shard URI. This is the whole of
/// the difference between the two fixtures below, and it is asserted as an equality.
#[cfg(feature = "alloc-probe")]
const SHARD_INFO_CLONE_ALLOCS: f64 = 2.0;

/// The key for a record, twenty-five characters wide at every index this module uses -- the same
/// width #1952 held, so the two modules' figures are comparable.
#[cfg(feature = "alloc-probe")]
fn record_key(index: usize) -> String {
    format!("tenant/7/object/{index:09}")
}

/// What one arm observed, and what the cache itself says it did.
#[cfg(feature = "alloc-probe")]
struct WarmArm {
    counted: crate::alloc_probe::ClassifiedCounts,
    memory_hits: u64,
    disk_hits: u64,
    misses: u64,
}

#[cfg(feature = "alloc-probe")]
impl WarmArm {
    fn allocs_per_read(&self) -> f64 {
        self.counted.total.allocs as f64 / READS as f64
    }
    fn bytes_per_read(&self) -> f64 {
        self.counted.total.alloc_bytes as f64 / READS as f64
    }
    fn per_read(&self, class: crate::alloc_probe::AllocClass) -> f64 {
        self.counted.classes.row(class).allocs as f64 / READS as f64
    }
}

/// The `load_shard` fixture: the one every arm in #1952 used. Its `ShardInfo` carries two EMPTY
/// names, which clone without allocating, so every figure taken through it is a floor.
#[cfg(feature = "alloc-probe")]
fn engine_at(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    (engine, dir.to_string_lossy().len())
}

/// The same engine loaded the way a metaserver loads one: a named table, a shard URI and a node.
/// Identical in every other respect -- same capacity, same directories, same path length -- so
/// the arms differ in the two names and in nothing else.
#[cfg(feature = "alloc-probe")]
fn engine_at_named(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: Some(7),
        shard_uri: "ts://tenant-7/table-orders/shard-00001".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: "tenant_7_orders".to_string(),
    });
    assert!(
        response.status.ok,
        "the named shard must load, or this arm is measuring an unloaded shard: {:?}",
        response.status
    );
    (engine, dir.to_string_lossy().len())
}

/// Write `records` records, warm the command-answer cache, then measure `READS` warm reads.
#[cfg(feature = "alloc-probe")]
fn measure_warm_reads(engine: &TemporalEngine, records: usize) -> WarmArm {
    for index in 0..records {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: record_key(index),
                value: vec![118u8; VALUE_BYTES],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    for offset in 0..READS {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: record_key(offset % records),
            },
        });
    }

    let before = engine.cache().stats();
    let span = crate::alloc_probe::ClassSpan::open();
    for offset in 0..READS {
        std::hint::black_box(engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: record_key(offset % records),
            },
        }));
    }
    let counted = span.close().expect("the counting allocator must be installed");
