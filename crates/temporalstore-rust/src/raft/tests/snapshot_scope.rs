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
        shards.len()
    );
    assert!(
        !first.is_empty(),
        "the image for shard {} carries NO slabs, so every comparison below is vacuous",
        shards[0]
    );
    (present.len(), first.len())
}

/// FINDING 1, at two shards. The image for shard 1 must not be the image for shard 2.
#[test]
fn a_shard_snapshot_carries_only_its_own_slabs_at_two_shards() {
    let shards = [1 as ShardId, 2 as ShardId];
    let engine = engine_hosting(&shards);
    let (present, carried_one) = report_scope("two shards", &engine, &shards);

    let first = carried_slabs(&engine, 1);
    let second = carried_slabs(&engine, 2);
    println!("slabs_carried_shard2 = {}", second.len());
    println!(
        "over_carry_ratio     = {:.2}x",
        present as f64 / carried_one as f64
    );

    // THE CONTROL, and it is the whole refutation of "the walk is implicitly shard-scoped":
    // two shards of one engine must not be answered with the same slab set. Shard 2 was given
    // an extra roll, so a scoped answer differs in COUNT and not only in membership.
    assert_ne!(
        first, second,
        "shard 1 and shard 2 of the same engine were handed the IDENTICAL slab set, so the walk \
         is not shard-scoped: {first:?}"
    );
    assert_ne!(
        first.len(),
        second.len(),
        "the two shards were handed the same NUMBER of slabs ({}), so this fixture cannot tell \
         a scoped walk from an unscoped one",
        first.len()
    );

    // THE CLAIM: shard 1's image is a strict subset of the node's slabs, i.e. the other shard's
    // bytes were actually dropped rather than merely reordered.
    assert!(
        carried_one < present,
        "shard 1's image carries all {present} slabs on the node, so it is still carrying shard \
         2's bytes"
    );
    assert!(
        !first.contains(second.iter().next().expect("shard 2 owns a slab")),
        "shard 1's image carries a slab that shard 2's image also claims exclusively"
    );
}

/// FINDING 1, at four shards. Same property; the point of the second size is the RATIO, which
/// grows with the shard count because the over-carry was the whole store every time.
#[test]
fn a_shard_snapshot_carries_only_its_own_slabs_at_four_shards() {
    let shards = [1 as ShardId, 2 as ShardId, 3 as ShardId, 4 as ShardId];
    let engine = engine_hosting(&shards);
    let (present, carried_one) = report_scope("four shards", &engine, &shards);
    println!(
        "over_carry_ratio     = {:.2}x",
        present as f64 / carried_one as f64
    );

    let first = carried_slabs(&engine, 1);
    let fourth = carried_slabs(&engine, 4);
    assert_ne!(
        first, fourth,
        "shard 1 and shard 4 of the same engine were handed the IDENTICAL slab set"
    );
    assert!(
        carried_one < present,
        "shard 1's image carries all {present} slabs on the node at four shards"
    );

    // The over-carry the scope removes grows with the shard count: at four shards the node
    // holds strictly more slabs than at two, while shard 1 still owns what it owned.
    let two_shard_present = slabs_on_the_node(&engine_hosting(&[1 as ShardId, 2 as ShardId])).len();
    println!("slabs_on_the_node@2  = {two_shard_present}");
    assert!(
        present > two_shard_present,
        "four shards did not put more slabs on the node than two ({present} vs \
         {two_shard_present}), so the multiplier claim is untested"
    );
}

/// The scope must not drop a slab the installed index can still address. This is the property
/// that matters and it is checked END TO END rather than against the same formula the scope
/// uses: install shard 1's image into a fresh engine and read every record back.
#[test]
fn a_scoped_image_still_serves_every_record_the_shard_held() {
    let shards = [1 as ShardId, 2 as ShardId, 3 as ShardId, 4 as ShardId];
    let engine = engine_hosting(&shards);
    let image = build_state_image(&engine, 1).expect("shard 1 must serve a state image");
    println!("slabs_carried_shard1 = {}", image.slabs.len());
    assert!(
        !image.slabs.is_empty(),
        "nothing was carried, so serving the records back proves nothing"
    );

    // Exactly what the install does with an image, on a fresh private engine.
