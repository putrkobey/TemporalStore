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
