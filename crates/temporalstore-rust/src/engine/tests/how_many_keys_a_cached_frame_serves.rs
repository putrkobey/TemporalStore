// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY KEYS ONE CACHED FRAME SERVES, which is what prices the payload cache's 2.0x.
//!
//! # THE CLAIM THIS CHECKS IS MY OWN, AND IT WAS UNVERIFIED
//!
//! `the_payload_cache_per_user_byte` established that a point read populates TWO cache layers and
//! both hold the payload — the command response under `CacheKey::string(shard, key)` and the block
//! frame under `CacheKey::page_with_slot(shard, slab, offset, length, bucket)` — at exactly 2.00
//! fills per key with zero evictions, costing `2 * value + 21` bytes an entry.
//!
//! Its doc then says the duplication "is not a defect" because "the frame entry earns its place on
//! multi-element blocks, where one frame answers several keys". **That was asserted, not measured.**
//! If blocks hold one element each in practice, the frame entry answers exactly one key. The
//! inference then drawn from that -- that it therefore duplicates the response entry entirely and
//! the 2.0x is waste -- is the one the next section shows does NOT hold.
//!
//! # WHAT THIS ARM DOES NOT ANSWER, AND WHAT WAS WRONGLY READ OFF IT
//!
//! It answers *do two KEYS share a frame*. It does **not** answer *can anything else reach this
//! frame*, and the conclusion drawn from it originally — that the frame entry duplicates the
//! response entry entirely and the 2.0x is waste — used it for the second question.
//!
//! The frame key is read by the 23 callers of `read_block_bytes` and the two of
//! `read_block_frame_bytes`, and it is the key of the persistent block cache that six `part2` arms
//! assert refills a tiny memory cache without a block-store read. One key per frame, 25 call paths
//! per key. Removing the frame put on the string read path broke all six of those arms, which is
//! how the overreach was caught.
//!
//! So read the number below as what it is: a statement about packing, which bears on whether a
//! frame can AMORTISE across keys — not on whether the entry has readers.
//!
//! # ANSWERABLE FROM THE INDEX ALONE
//!
//! Two keys share a frame if and only if they share `(slab, offset, length)` — those are the
//! components of the frame's cache key that come from the block. So grouping the live object rows by
//! their address triple and counting the keys per group answers it, with no cache internals
//! involved.
//!
//! # ACROSS VALUE SIZES, BECAUSE THE ANSWER MAY DEPEND ON THEM
//!
//! Small values may pack several to a block, in which case the frame cache earns its place for them
//! and my claim holds in a REGIME rather than universally. That is a more useful answer than either
//! "always" or "never", and it is the shape a reader can act on.
//!
//! Values are incompressible: a run of one byte compresses to nothing and changes how much fits in a
//! block, which is the variable under test.
//!
//! rust-internal: measures this crate's own index, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib how_many_keys_a_cached_frame_serves -- --ignored --nocapture

use std::collections::BTreeMap;

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 500;

fn noisy_value(index: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x1234_5678_9ABC_DEF0)
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// (value size, distinct frames, keys, largest group, groups serving more than one key)
fn measure(value: usize) -> (usize, usize, usize, usize, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
