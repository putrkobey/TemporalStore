// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the snapshot path COSTS, rather than what it returns.
//!
//! The snapshot carries a state image: the served index plus every slab the engine holds,
//! materialised as owned byte vectors. That payload is sized by the STORE, so anything on this
//! path that copies it rather than moving it costs a whole second shard in resident memory --
//! and one of those copies ran while the cluster write lock was held, which is the half that
//! also costs availability.
//!
//! Measured on this box before the move, with a 128-byte value per record:
//!
//!   records   image bytes   cutting the single install chunk
//!     8,000     1,618,593                            106 ms
//!    80,000    16,482,599                          1,289 ms
//!
//! Cutting one chunk is bookkeeping and should be free. That time was the copy, and it tracked
//! the corpus 12x for 10x.
//!
//! The guard below counts BYTES ALLOCATED rather than reading RSS. On this box RSS is mostly
//! allocator retention and it moves with machine load; an allocation count does neither. It is
//! also written as a DELTA against the same call's own legitimate cost, measured in the same
//! test, so it does not depend on the fixture being large enough to dwarf a fixed baseline.

use super::*;

/// Slab bytes plus index bytes -- what one copy of the image actually costs.
#[cfg(feature = "alloc-probe")]
fn image_bytes(image: &crate::raft::RaftSnapshotStateImage) -> usize {
    image.index_bytes.len() + image.slabs.iter().map(|slab| slab.bytes.len()).sum::<usize>()
}

#[cfg(feature = "alloc-probe")]
fn cluster_with_an_image(records: usize) -> RaftCluster {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < records {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
        index += 1;
    }
    cluster
}

/// Cutting the install chunks must not leave a SECOND copy of the state image behind.
///
/// `build_install_snapshot_chunks` legitimately builds one image -- it calls `create_snapshot`
/// itself -- so the question is not whether it allocates, but whether it allocates an image MORE
/// than building that snapshot already costs. That baseline is measured here rather than assumed,
/// so the bound holds at any fixture size: it is a difference between two measurements of the
/// same machine in the same test, not a constant someone has to keep up to date.
///
/// Pre-fix the image was cloned into the single chunk, so the subject cost baseline + one whole
/// image. Post-fix it is moved, so the subject costs about the baseline. The threshold sits
/// halfway between, giving half an image of slack in each direction.
#[cfg(feature = "alloc-probe")]
#[test]
fn cutting_the_install_chunks_does_not_copy_the_state_image() {
    // The probe reads process-global counters, so nothing else may be allocating.
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed despite the alloc-probe feature being on"
    );
    drop(sink);

    let cluster = cluster_with_an_image(400);

    // Warm whatever is lazily built on the first snapshot, so the baseline below measures the
    // steady-state cost rather than one-time setup.
    drop(cluster.create_snapshot().expect("create_snapshot must succeed"));

    // BASELINE: building the snapshot, which is where the one legitimate image comes from.
    let baseline_probe = crate::alloc_probe::Probe::start();
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let baseline_bytes = baseline_probe.stop().alloc_bytes;

    let image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path must be the one under test");
    let one_image = image_bytes(image);

    // DENOMINATORS, printed. The floor is a resolution floor: below it an image-sized copy is
    // not distinguishable from allocator bookkeeping and the assertion proves nothing either way.
    println!("one_image_bytes  = {one_image}");
    println!("baseline_bytes   = {baseline_bytes}");
