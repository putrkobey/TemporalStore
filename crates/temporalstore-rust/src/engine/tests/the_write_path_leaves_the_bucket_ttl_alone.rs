// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WRITE PATH NO LONGER RECOMPUTES A BUCKET'S TTL MINIMUM, AND THE SWEEP STILL DOES.
//!
//! # WHY THE TERM CAME OFF THE WRITE PATH
//!
//! The minimum is one map lookup per page in the bucket, keyed by the page's object key, and it is
//! the last per-page pass in bucket maintenance that does not short-circuit. Every reader of the
//! field it produces is a REPORT or the serializer; real expiry runs off the shard's expiry map and
//! its deadline-ordered mirror instead, and no expiry, eviction or serving decision consults a
//! bucket's TTL.
//!
//! # WHAT BOUNDS THE STALENESS, which is the claim a later reader needs
//!
//! The index flush runs the FULL sweep immediately before it serializes, so THE PERSISTED VALUE IS
//! NEVER STALE -- it is recomputed right before being written. What can lag is the in-memory value
//! the reports read, and it lags by at most one index flush. A load recomputes it as well.
//!
//! # WHY THE EXISTING EQUIVALENCE TEST DOES NOT COVER THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted path against the sweep and does
//! include the TTL -- but as `ttl_ms.is_some()`, a BOOLEAN. A value that is stale and still present
//! compares equal to a fresh one, so that test is insensitive to exactly the difference this change
//! introduces, and it passes either way. MEASURED: it passes with this change in place. That is not
//! evidence the change is correct; it is evidence the test cannot see it. Hence this module, which
//! asserts the VALUE.
//!
//! # THE CONTROL IS THE SECOND HALF OF EACH PAIR
//!
//! "The write path left it alone" is also what a broken computation looks like, so each case asserts
//! the sweep then COMPUTES it on the same shard. Without that second half, deleting the computation
//! outright would pass.
//!
//! Both write-path arms are covered, because the write path is not always the targeted traversal:
//! the pending refresh hands its work to the sweep once the touched set reaches half the buckets,
//! and at the routing range production runs it essentially always does. Gating the TTL on the
//! traversal would therefore have left the cost where it was on the configuration that matters,
//! which is why it rides its own parameter -- and why both arms are asserted here.

use crate::engine::state::{BlockIndex, BlockSlabLiveIndex, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    note_bucket_flags_stale, refresh_bucket_runtime_flags, refresh_pending_bucket_runtime_flags,
};

/// Far enough ahead that the remaining time is non-zero after the subtraction.
///
/// It has to be: the field is `Option<NonZeroU64>`, so a remaining time of zero is ABSENT and a
/// fixture whose expiry has already passed would assert nothing at all.
const FAR_FUTURE_MS: u64 = u64::MAX / 2;

fn a_bucket_holding_one_block(routing_bucket: u32, object_key: &str) -> BucketNode {
    let mut bucket = BucketNode::default();
    bucket.routing_bucket = routing_bucket;
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_block(&object_key, 
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            dirty: false,
            deleted: false,
        },
        &mut live,
