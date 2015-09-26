// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CACHE KEY COSTS, against the information it actually carries.
//!
//! # THE QUESTION
//!
//! `CacheKey` is four fields — `shard_id`, `record_key`, `namespace`, `selector`. The question was
//! whether all four are needed. **The fields are not the problem.** Shard, which namespace, which
//! record, and which part of that record are four real distinctions, and dropping any one merges
//! keys that must not merge: `what_the_two_cache_keys_are` shows the logical and physical keys for
//! one string agreeing on `shard_id` alone and differing in the other three.
//!
//! The **representation** is, and only for the page namespace, where every part of the key is an
//! integer carried as formatted decimal text:
//!
//! ```text
//!   record_key   "segment-00000000000000000000"   a u64 slab id, zero-padded to 20 digits
//!   selector     "slot-970485159:0:35"            a u32 bucket and two u64s, colon-joined
//! ```
//!
//! The zero-padding is load-bearing — it makes lexicographic key order agree with numeric order —
//! so the record key cannot simply be shortened.
//!
//! # THIS ARM WAS A TRIPWIRE ON THE DEPENDENCY PIN, AND IT FIRED
//!
//! It previously measured a flat 64-byte selector reservation against a 19-byte selector, and a
//! maximal generation selector that reallocated past it. Both were real at the revision then
//! pinned, and both were **already fixed upstream** — the pin was 105 commits behind the fix.
//!
//! Building against the dependency tip turned this arm into twelve compile errors rather than
//! twelve failed assertions, because the types changed:
//!
//! | field | before | now |
//! |---|---|---|
//! | `record_key` | `String` | `Arc<str>` — one allocation, shared on clone |
//! | `selector` | `String` | `Cow<'static, str>` — **borrowed** for literal selectors |
//!
//! The second is the one that pays on the hot path. `"value"`, `"members"` and the other literal
//! selectors now cost no heap at all, where every key built a `String` for them before. The page
//! selector still has to be computed, so it stays owned — with the exact capacity rather than a
//! flat 64.
//!
//! **A compile error is a better tripwire than a failed assertion**, because it cannot be read as
//! a flake and cannot be skipped.
//!
//! # WHAT SURVIVES, AND WHAT THIS DOES NOT CLAIM
//!
//! The slack is gone; the **rendering** is not. A page key still spends far more than the 32 bytes
//! of integers it names, and closing that means changing `CacheKey` itself — another crate's type —
//! rather than its capacities. This arm prices that and does not attempt it.
//!
//! rust-internal: measures the engine's own cache key, no product behaviour

use crate::types::{Command, ExecuteRequest};
use std::borrow::Cow;

/// Heap bytes and allocations behind one key, by inspecting what each field actually is.
///
/// `Arc<str>` is one allocation holding the bytes plus two reference counts; a `Cow` is an
/// allocation only when it is `Owned`. Neither exposes `capacity()`, which is what the old
/// measurement used and why this arm had to be rewritten rather than retuned.
fn heap_and_allocs(key: &matrixcache::CacheKey) -> (usize, usize) {
    const ARC_COUNTS: usize = 2 * std::mem::size_of::<usize>();
    let record_heap = ARC_COUNTS + key.record_key.len();
    let (selector_heap, selector_allocs) = match &key.selector {
        Cow::Borrowed(_) => (0, 0),
        Cow::Owned(owned) => (owned.capacity(), 1),
    };
    let (namespace_heap, namespace_allocs) = match &key.namespace {
        Cow::Borrowed(_) => (0, 0),
        Cow::Owned(owned) => (owned.capacity(), 1),
    };
    (
        record_heap + selector_heap + namespace_heap,
        1 + selector_allocs + namespace_allocs,
    )
}

fn selector_kind(key: &matrixcache::CacheKey) -> &'static str {
    match &key.selector {
        Cow::Borrowed(_) => "borrowed",
        Cow::Owned(_) => "OWNED",
    }
}

#[test]
fn what_a_cache_key_costs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let key = "user:42:profile";
    let wrote = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: key.to_string(), value: vec![b'v'; 256] },
    });
    assert!(wrote.status.ok, "write: {:?}", wrote.status);

    let logical = matrixcache::CacheKey::string(1, key);
    let physical = engine
        .string_block_cache_key_for_test(1, key)
