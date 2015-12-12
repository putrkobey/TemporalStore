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
    ids.push(
        engine
            .create_bucket_dump_manifest(SHARD, Vec::new())
            .expect("third manifest should persist")
            .manifest_id,
    );
    (engine, ids)
}

fn manifests_on_disk(engine: &TemporalEngine) -> Vec<BucketDumpManifest> {
    list_bucket_dump_manifests_at(&engine.index_dir, SHARD).expect("manifest dir should list")
}

/// Manifests still on disk that carry a parent link, and how many of those links point at a
/// manifest that is NOT on disk. The second number is the defect: it is what
/// `bucket_dump_manifest_chain_issues` turns into `missing_parent_manifest`.
fn parent_link_counts(engine: &TemporalEngine) -> (usize, usize) {
    let present = manifests_on_disk(engine);
    let ids = present
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let with_link = present
        .iter()
        .filter(|manifest| manifest.parent_manifest_id.is_some())
        .count();
    let dangling = present
        .iter()
        .filter(|manifest| {
            manifest
                .parent_manifest_id
                .as_ref()
                .is_some_and(|parent| !ids.contains(parent))
        })
        .count();
    (with_link, dangling)
}

/// HALF ONE: the order the prune used to run in, stopped where a crash would stop it.
///
/// Reproduces the old first phase directly -- unlink every manifest the plan names, detach
/// nothing -- because that state is the claim under test and building it by hand is the only way
/// to assert it once the production order no longer produces it. What a load sees is then
/// asserted twice over: the chain-issue reason, and the readiness blocker that reason raises.
#[test]
fn the_old_prune_order_leaves_invented_corruption_in_its_crash_window() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, created) = engine_with_three_chained_manifests(dir.path());
    assert_eq!(created.len(), 3, "fixture should create three manifests");

    let plan = engine.bucket_dump_manifest_prune_plan(SHARD);
    let pruned = plan.prunable_manifest_ids.len();
    let (with_link_before, dangling_before) = parent_link_counts(&engine);
    let on_disk_before = manifests_on_disk(&engine).len();
    println!(
        "OLD ORDER  manifests_on_disk={on_disk_before} pruned={pruned} \
         parent_links_before={with_link_before} dangling_before={dangling_before}"
    );
    // Vacuity floor. A prune that names nothing, or a fixture whose manifests carry no parent
    // links, would sail through every assertion below while testing nothing at all.
    assert!(
        pruned >= 2,
        "vacuous: the plan must name at least two manifests to prune, named {pruned}"
    );
    assert!(
        with_link_before >= 1,
        "vacuous: at least one manifest must carry a parent link, {with_link_before} do"
    );
    assert_eq!(
        dangling_before, 0,
        "fixture must start with a whole chain, {dangling_before} links already dangle"
    );

    // The old first phase, and only it: unlink, detach nothing. This is the process dying in the
    // window between the two loops.
    let mut unlinked = 0usize;
    for manifest_id in &plan.prunable_manifest_ids {
        if fs::remove_file(bucket_dump_manifest_path(&engine.index_dir, SHARD, manifest_id)).is_ok()
        {
            unlinked += 1;
