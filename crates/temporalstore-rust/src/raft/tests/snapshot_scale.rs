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
