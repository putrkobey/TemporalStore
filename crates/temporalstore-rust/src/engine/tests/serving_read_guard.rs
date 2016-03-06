// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the serving read fast path does while it holds the shard-table read guard.
#![allow(clippy::all)]
use super::*;

/// Fields on the one hash the measurement reads. Chosen larger than any plausible per-call
/// constant so a hold that scales with the value's WIDTH cannot be mistaken for a fixed one.
const FIELDS: usize = 64;

/// A shard holding one wide hash whose pages are all OUT of cache.
fn cold_wide_hash(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..FIELDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "wide".to_string(),
                field: format!("f{index:04}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // Without this every page is already resident and the read below never reaches the block
    // store -- the count would be zero and the claim would hold for the wrong reason.
    let _ = engine.cache.invalidate_shard(1);
    engine
}

/// One `HashGetAll` down the durable serving route, with the page reads counted.
///
/// Returns the tally and the FIELD COUNT the response actually carried. Those are two different
/// claims -- "N reads happened under the guard" and "the value had N fields" -- and a single
/// total passes when two errors cancel, so they are asserted apart below.
fn measured_hash_get_all(
    engine: &TemporalEngine,
) -> (crate::engine::MaintenanceBlockReadCounts, usize) {
    crate::engine::reset_maintenance_block_read_counts();
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll {
            key: "wide".to_string(),
        },
    });
    assert!(response.status.ok, "read: {:?}", response.status);
    let fields = match response.response {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("expected HashEntries, got {other:?}"),
    };
    (crate::engine::maintenance_block_read_counts(), fields)
}

/// The serving read fast path reads its pages AFTER the shard-table read guard drops.
///
/// A read guard is the easy one to leave in place, because admitting other readers does not look
/// like exclusion. It is: it excludes every WRITER on the shard for as long as it is held, and
/// this path held it across one block-store read PER FIELD. The window therefore scaled with the
/// WIDTH of the value being served -- a 64-field hash stopped all writes on the shard for 64
/// reads -- which is a property of the request, not of any per-round budget.
///
/// TWO arms, ONE process, the same fixture:
///   * UNDER THE GUARD -- where the reads used to be. The positive control: the counter has to
///     produce a non-zero number inside the region, or the zero below is a broken counter rather
///     than a shortened hold.
///   * SHIPPED -- the addresses are chosen under the guard, the pages are read after it drops.
/// Both arms must read the same pages, which is what stops this from being satisfied by a read
/// that found everything cached or served nothing.
#[test]
fn the_serving_fast_path_reads_blocks_after_the_shard_guard_drops() {
    let control_dir = tempfile::tempdir().unwrap();
    let control_engine = cold_wide_hash(control_dir.path());
    crate::engine::shard_write_guard::serve_fast_path_under_shard_guard_for_test(true);
    let (control, control_fields) = measured_hash_get_all(&control_engine);
    crate::engine::shard_write_guard::serve_fast_path_under_shard_guard_for_test(false);

    let shipped_dir = tempfile::tempdir().unwrap();
    let shipped_engine = cold_wide_hash(shipped_dir.path());
    let (shipped, shipped_fields) = measured_hash_get_all(&shipped_engine);

    eprintln!(
        "[serving read] under the guard: {} of {} page read(s) under the guard, \
{control_fields} field(s) served",
        control.block_reads_under_guard, control.block_reads_total,
