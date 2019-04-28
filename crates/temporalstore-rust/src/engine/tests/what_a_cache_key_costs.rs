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
