// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a crash in the middle of a bucket-dump manifest prune leaves on disk.
//!
//! The prune does two things: it detaches the survivors whose parent it is about to remove, and
//! it unlinks the pruned manifests. Both orders finish in the same place, so the order looks
//! free. It is not -- it decides what a crash BETWEEN them leaves behind.
//!
//! Unlink first, and the window contains manifests that are gone with survivors still pointing
//! at them. `bucket_dump_manifest_chain_issues` reads that as `missing_parent_manifest` and
//! `storage_production_readiness_report` raises a `broken_slot_dump_manifest_chain` blocker --
//! the state the comment in `apply_bucket_dump_manifest_prune_with_retention_refs` itself calls
//! indistinguishable from real corruption. No data is lost either way (each manifest embeds a
//! complete index, and installing one never walks the chain), so the whole cost is a shard
//! reported as corrupt for a reason that never happened.
//!
//! Detach first and the window contains survivors with no parent link while their parent file is
//! still on disk -- which is exactly what a prune that ran to completion looks like, and
//! consistent either way.
//!
//! The two halves are asserted separately and against the same fixture, because they are two
//! different claims: that the old order's window really is the corrupt-looking state (it is not
//! a hypothetical), and that the new order's window is clean. Neither half is evidence for the
//! other. The crash point itself is not reachable from production code on demand, so the second
//! half drives `stop_bucket_dump_prune_after_first_phase_for_test`, the `cfg(test)` seam that
//! stops the prune between the phases.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

/// Three manifests, each parented on the last: `first <- second <- third`.
///
/// The retention rule keeps the NEWEST manifest and nothing else, so this prunes two and leaves
/// one survivor whose parent is among the pruned. Three rather than two so that the pruned count
/// is not 1, which would let an off-by-one in either direction read as correct.
fn engine_with_three_chained_manifests(dir: &std::path::Path) -> (TemporalEngine, Vec<String>) {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    let mut ids = Vec::new();
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v1".to_vec(),
        },
    });
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("first manifest should persist")
            .manifest_id,
    );
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v2".to_vec(),
        },
    });
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("second manifest should persist")
            .manifest_id,
    );
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "window".to_string(),
            value: b"v3".to_vec(),
        },
    });
