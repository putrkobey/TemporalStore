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
            stale_bytes_recorded: 400 * scale,
            stale_bytes_discarded: 150 * scale,
            pending_discard_bytes: 250 * scale,
            pending_discard_block_refs: 7 * scale,
            stale_bytes_by_overwrite: 300 * scale,
            stale_bytes_by_removal: 70 * scale,
            stale_bytes_by_sweep: 30 * scale,
            discard_candidate_bytes: 90 * scale,
            discard_candidate_slabs: 2 * scale,
            compaction_candidate_bytes: 60 * scale,
            compaction_candidate_slabs: 3 * scale,
            compaction_candidate_range_count: 11 * scale,
            merged_discard_range_count: 13 * scale,
            fragmented_range_count: 5 * scale,
            // SELECTIVE, so the node-level count below can tell "counts the qualifying
            // shards" from "counts every shard". 4 and 0 also keeps the byte-sum
            // assertion exact: 4 + 0 is the same 4 that 1 x scale summed to.
            dropped_discard_range_count: if scale == 1 { 4 } else { 0 },
            stale_slabs_tracked: 4 * scale,
            stale_slabs_pruned: 1 * scale,
            // THE NODE-LEVEL CONDITIONS, set so exactly ONE of the two shards qualifies on each.
            // A fixture where both qualify cannot tell "counts the qualifying shards" from "counts
            // every shard" -- the two agree at 2.
            compaction_recommended: scale == 1,
            // The ratios are populated too, precisely so that exporting one would be POSSIBLE here
            // and the absence below is a choice this arm can observe rather than a vacuous pass.
            block_space_amplification_ppm: 5_000_000,
            live_bytes_ppm: 200_000,
            stale_bytes_ppm: 800_000,
            discard_backlog_ppm: 625_000,
            recorded_space_amplification_ppm: 3_500_000,
            space_amplification_ppm: 1_000_000,
            ..crate::control::ShardCanonicalStorageStats::default()
        },
        ..crate::meta::ServerShardServingState::default()
    }
}

fn exported(shards: &[crate::meta::ServerShardServingState]) -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::new();
    super::super::apply_shard_storage_metrics(&mut metrics, shards);
    metrics
}

