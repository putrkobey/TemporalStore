// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE LOGICAL AND PHYSICAL CACHE KEYS ARE, printed side by side for one real key.
//!
//! # WHY THIS EXISTS
//!
//! "The logical copy" and "the physical copy" have carried a lot of weight: which one a point read
//! fills, which one has readers, which one survives a restart, which one the payload byte law is
//! written against. All of that was argued in prose, and prose is exactly where a distinction stops
//! being true without anything failing.
//!
//! So this prints both keys for a key that was just written and read, with their actual field
//! values, and asserts the ways they differ.
//!
//! # THEY ARE NOT TWO VIEWS OF ONE KEY
//!
//! `CacheKey` is four fields, and the two constructors agree on exactly one of them:
//!
//! ```text
//!                shard_id   namespace   record_key                     selector
//!   logical      1          "string"    the CALLER's key               "value"
//!   physical     1          "page"      "segment-000...00<slab>"       <bucket/offset/length>
//! ```
//!
//! Only `shard_id` matches. The logical key is addressed by **what was asked for**; the physical
//! key is addressed by **where the bytes landed** — slab, offset, length, routing bucket. That is
//! the whole reason one of them can be shared by other readers and the other cannot: 25 call paths
//! reach a block by its address, and exactly one reaches it by the caller's string.
//!
//! # THE PHYSICAL KEY IS DENOMINATED IN STORED BYTES
//!
//! The selector's three components are routing bucket, offset and **length** — and that length is
//! the block's *stored* size, not the caller's. For a 256-byte run of one character it reads 35.
//!
//! This is not a detail about cache keys. It is the reason a user-denominated amplification ratio
//! cannot be computed from the index as it stands: every length on the path — the address, the
//! element entry — records what was written to storage after compression, and the size the caller
//! offered is not retained anywhere. The arm shows it by writing two keys of the **same** user
//! size, one compressible and one not, and letting their addresses disagree.
//!
//! # AND IT IS A GUARD, NOT ONLY A DEMONSTRATION
//!
//! If the two key spaces ever collapse — a namespace renamed to match, a physical key rebuilt from
//! the caller's string — the "exactly one copy" arms next door would keep passing while meaning
//! something else entirely, because they ask two keys that had become the same key. This fails
//! first and says which field stopped distinguishing them.
//!
//! rust-internal: reads the engine's own cache keys, no product behaviour

use crate::types::{Command, CommandResponse, ExecuteRequest};

#[test]
fn what_the_two_cache_keys_are() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "user:42:profile";
    let value = vec![b'v'; 256];

    let write = engine.execute(ExecuteRequest {
