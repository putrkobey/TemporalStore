// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT a per-shard snapshot carries, and WHERE the install does its work.
//!
//! Two properties, measured rather than read:
//!
//! 1. A snapshot for shard A must carry shard A's slabs. `BlockStore` has no shard concept --
//!    `slab_ids()` lists a directory -- so the walk that built the image answered for the whole
//!    NODE. The tests below build one engine hosting several shards and compare the image for
//!    one shard against the image for another. Before the scope, the two were the same set: the
//!    image was sized by the node's shard count, and the receiving replica installed slabs for
//!    shards it does not host.
//!
//! 2. The install must not do corpus-sized work under the cluster WRITE guard, which excludes
//!    readers as well as writers. The engine an install publishes is private until the moment
//!    it is published, so the slab installs and the applied-set fill belong outside the guard.
//!    That is asserted by COUNTING the work attributed to the guarded span, not by timing it:
//!    this box's load moves between runs and a count does not.
//!
//! Every counted claim here carries its own positive control, because a probe that never fires
//! and a subject that does no work are otherwise the same measurement.

use super::*;
use crate::raft::cluster_snapshot::{build_state_image, install_probe};
use std::collections::BTreeSet;

/// 256-byte records, so a slab holds a countable number of them rather than thousands.
fn write_records(engine: &TemporalEngine, shard_id: ShardId, tag: &str, count: usize) {
    let mut index = 0usize;
    while index < count {
        engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: format!("{tag}-{index:05}"),
                value: vec![b'v'; 256],
            },
        });
        index += 1;
    }
}

fn slabs_on_the_node(engine: &TemporalEngine) -> BTreeSet<u64> {
    engine
        .block_store()
        .slab_ids()
        .expect("the block store must list its slabs")
        .into_iter()
        .collect()
}

fn carried_slabs(engine: &TemporalEngine, shard_id: ShardId) -> BTreeSet<u64> {
    build_state_image(engine, shard_id)
        .expect("the engine must be able to serve a state image for a loaded shard")
        .slabs
        .iter()
        .map(|slab| slab.block_slab_id)
        .collect()
}

/// One engine hosting `shards` shards, each shard's writes separated by a slab roll so the
/// store ends up holding several shards' slabs at once. Shard 2 is deliberately given a roll
/// mid-window, so it owns a DIFFERENT NUMBER of slabs from every other shard -- that is the
/// control: a scoped image answers with a different count per shard, an unscoped one cannot.
fn engine_hosting(shards: &[ShardId]) -> TemporalEngine {
    let engine = TemporalEngine::default();
    for shard_id in shards {
        engine.load_shard(*shard_id);
    }
    for shard_id in shards {
        let tag = format!("shard{shard_id}");
        if *shard_id == 2 {
            write_records(&engine, *shard_id, &tag, 40);
            engine
                .block_store()
                .roll_slab()
                .expect("rolling the active slab must succeed");
            write_records(&engine, *shard_id, &format!("{tag}b"), 40);
        } else {
            write_records(&engine, *shard_id, &tag, 40);
        }
        engine
            .block_store()
            .roll_slab()
            .expect("rolling the active slab must succeed");
    }
    engine
}

/// The denominators, printed, plus the floors that stop any of this being vacuous.
fn report_scope(label: &str, engine: &TemporalEngine, shards: &[ShardId]) -> (usize, usize) {
    let present = slabs_on_the_node(engine);
    let first = carried_slabs(engine, shards[0]);
    println!("--- {label} ---");
    println!("shards_hosted        = {}", shards.len());
    println!("slabs_on_the_node    = {}", present.len());
    println!("slabs_carried_shard1 = {}", first.len());

    assert!(
        present.len() >= shards.len(),
        "fixture did not produce a slab per shard: the node holds {} slabs for {} shards, so \
         there is nothing for an unscoped walk to over-carry and this measures nothing",
        present.len(),
