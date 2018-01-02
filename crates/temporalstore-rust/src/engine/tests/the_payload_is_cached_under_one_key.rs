// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAYLOAD IS CACHED UNDER EXACTLY ONE KEY, which is what "remove the duplication" meant.
//!
//! # THE ITEM THIS CLOSES
//!
//! A point read used to leave the payload in the cache **twice**: the encoded command response under
//! `CacheKey::string(shard, key)`, and the block frame under `CacheKey::page_with_slot(...)`. That
//! cost `2 * value + envelope` bytes an entry at exactly 2.00 fills per key, and removing it was the
//! open work.
//!
//! The logical copy is gone, and two existing arms *imply* this — `B/entry == value` exactly, and
//! one fill per key. But implied by a byte law is not the same as asserted, so here it is in the
//! form the item was written in: for a key just read, **exactly one** of the two cache keys that
//! could hold its payload does.
//!
//! # TOGETHER WITH ITS NEIGHBOUR IT PINS BOTH HALVES
//!
//! `a_point_read_fills_the_physical_tier` asserts the **physical** copy exists, because removing it
//! would leave string reads with no cache at all — the measured regression that reverted the
//! original design. This asserts the **logical** copy does not, because that is the duplication the
//! item set out to remove.
//!
//! **Either alone permits the wrong state:** the first is satisfied by two copies, the second by
//! none. Only the pair says "exactly one, and which one."
//!
//! # WHY NOT BY COUNTING CACHE ENTRIES
