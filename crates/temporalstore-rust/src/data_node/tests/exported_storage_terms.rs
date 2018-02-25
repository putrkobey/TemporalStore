// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE EXPORTED STORAGE TERMS: they sum across shards, and no ratio is exported at all.
//!
//! # WHY THE ABSENCE IS THE HARD HALF
//!
//! `apply_shard_storage_metrics` aggregates with `saturating_add`, so it SUMS ACROSS SHARDS. A byte
//! total sums correctly. A parts-per-million ratio does not: ten shards at 1.000x would export
//! 10,000,000 ppm, which is not a ratio of anything.
//!
//! That is not hypothetical. Three lines from where the storage terms are added sits this, about a
//! key that shipped broken:
//!
//! ```text
//! // NOT `storage.bucket_entries`: that is the routing RANGE, so this metric reported
//! // u32::MAX per shard -- summed across shards -- regardless of the real index size.
//! ```
//!
//! and the note on its fix adds **"Nothing asserted it, which is why it stayed that way."** This is
//! that assertion.
//!
//! # ASSERTED BY NAME PATTERN, NOT BY A LIST
//!
//! Any exported key ending `_ppm`, or containing `amplification` or `ratio`, fails here. A list of
//! today's ratio names would pass the moment someone adds tomorrow's — which is exactly how the
//! `bucket_entries` key survived.
//!
//! # AND THE SUM IS CHECKED OVER TWO SHARDS WITH DIFFERENT VALUES
//!
//! One shard cannot tell "summed" from "overwritten by the last shard": the two agree at n=1. That
//! is the shape of a per-shard figure that looks right until a second shard exists.
//!
//! rust-internal: drives this crate's own metric exporter, no product behaviour

use std::collections::BTreeMap;

fn shard_with(shard_id: crate::types::ShardId, scale: u64) -> crate::meta::ServerShardServingState {
    crate::meta::ServerShardServingState {
        shard_id,
        storage: crate::control::ShardCanonicalStorageStats {
            live_block_bytes: 100 * scale,
            non_purged_physical_bytes: 500 * scale,
            logical_bytes_written: 700 * scale,
