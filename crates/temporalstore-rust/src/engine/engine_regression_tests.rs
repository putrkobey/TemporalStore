// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::BlockStoreSlabState;
use crate::engine::golden::{
    native_api_golden_corpus_report, native_feature_sequence_golden_corpus_report,
};
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent,
    ContextExtractedEventIndexes, ContextSummary, ContextWire, FeatureFilter, FeatureFilterOp,
    ReplicatedCommand,
};
use crate::{ElementEntry, BlockStoreOptions, BlockStore};

/// Holds the one-entry-a-page gate OFF for as long as it lives, putting back whatever was there --
/// on a normal drop AND during unwinding.
///
/// # WHY A SHARED GUARD APPEARED WHEN THE GATE'S DEFAULT MOVED
///
/// Eleven tests in six modules went red when that default became ON, and not one of them mentions
/// the gate. They were driving the per-element projection THROUGH THE DEFAULT -- so the default was
/// load-bearing on coverage nobody had declared, and moving it silently changed what they measured.
/// They fail loudly, which is the good version of this problem; the twenty `remove_var` sites are
/// the silent version of the same thing, and six of those were arms that meant "off" and stopped
/// meaning it.
///
/// Each user of this guard states the path it drives and names where the COLLAPSED behaviour it is
/// not asserting is asserted instead, so holding the gate off here cannot quietly drop coverage of
/// what a deployment now runs.
///
/// A guard rather than a `set_var` and a matching `remove_var`: `Drop` runs while unwinding, so a
/// FAILING test cannot leak the gate into every test after it in the process. That contamination
/// has already happened once in this series, and it is ordered and silent rather than a flake.



/// THE OBJECT OF EVERY PAGE IN ONE INDEX, KEYED BY THE POSITION THE PAGE IS FILED AT.
///
/// # WHY THIS AND NOT ONLY [`blocks_with_identity`]
///
/// Half the walks that have to be converted cannot give up their own nesting. They need the
/// BUCKET as well as the page -- to read a tombstone row out of it, to count its object index, to
/// `retain` over it -- and a flat list of pages does not carry the bucket. This composes with
/// whatever walk is already there: take it once, then ask it per page.
///
/// IT OWNS ITS KEYS AND BORROWS NOTHING, which is what lets a `retain` use it. `retain` holds the
/// bucket map mutably and the keys are drawn from a SIBLING field of the same index, so a map that
/// borrowed would be refused; the `Arc` clones cost one refcount bump each and the borrow ends
/// when this returns.
pub(super) struct BlockObjects {
    by_position: std::collections::HashMap<(u32, u64), std::sync::Arc<str>>,
}

impl BlockObjects {
    /// Taken in ONE pass over the index, from the same two sources [`blocks_with_identity`] reads.
    pub(super) fn of(index: &crate::engine::state::CoreIndex) -> Self {
        Self {
            by_position: blocks_with_identity(index)
                .into_iter()
                .map(|(routing_bucket, handle, object_key, _block)| {
                    ((routing_bucket, handle), object_key)
                })
                .collect(),
        }
    }

    /// The object the page at this position belongs to.
    ///
    /// A PANIC AND NOT AN `Option`, for [`blocks_with_identity`]'s reason: a page that nothing
    /// beside it names is a state the producer refuses, and a caller handed `None` would reach
    /// for a skip. Every caller here is walking the same index this was taken from, so the only
    /// way to miss is to ask at a position that does not exist.
    pub(super) fn at(&self, routing_bucket: u32, handle: u64) -> &std::sync::Arc<str> {
        self.by_position.get(&(routing_bucket, handle)).unwrap_or_else(|| {
            panic!(
                "no page is filed at bucket {routing_bucket} handle {handle} in the index this                  was taken from"
            )
        })
    }
}

/// EVERY PAGE OF ONE CORE INDEX, WITH THE OBJECT THAT THE STRUCTURES BESIDE IT NAME.
///
/// # WHY THIS IS SHARED AND NOT A WALK PER TEST
///
/// `BlockIndex` carries no object key. Every test that walked a real store's buckets and asked a
/// page which object it belonged to now has to ask the bucket instead, and there are some forty
/// such walks across this module tree. Written out one by one they are forty chances to pick the
/// WRONG DOOR, and the wrong door fails silently rather than loudly:
///
///   * [`BlockKeysByPosition::live_identity`] answers from the object lookup alone, and
///     `insert_object_block_lookup` returns early on a deleted page -- so it names NO tombstone.
///     A walk that reports on deleted pages and asks that door drops every one of them and comes
///     back with a smaller, entirely plausible number.
///   * [`BlockKeysByPosition::identity_of`] knows about both sources: the lookup for a live page,
///     and the bucket's own tombstone row for a deleted one. That is what this asks, so a caller
///     cannot get the tombstone case wrong by omitting to think about it.
///
/// # A LIVE PAGE NAMED BY NOTHING IS A PANIC; AN UNNAMED TOMBSTONE IS A SKIP
///
/// THIS WAS A FLAT PANIC OVER BOTH STATES AND THAT WAS WRONG, AND THE SUITE IS WHAT SAID SO. The
/// paragraph here read "A PAGE NAMED BY NOTHING IS A PANIC, NOT A SKIP ... Both are states a test
/// walking a store should fail on, loudly", naming the window between a decode and its rebuild and
/// a removal that named no element.
///
/// THE SECOND IS NOT A DEFECT, AND `identity_of`'s OWN DOC SAYS SO: a deleted page whose removal
/// named no element "is the one page in this store whose object nothing beside it states".
/// Production's `block_identities_in` skips it through a `filter_map`, with the note that in every
/// caller it means "this page is not the one I am matching".
///
/// `hash_read_union_divergence::an_index_entry_marked_deleted_loses_no_field_and_is_not_counted`
/// plants exactly that page -- it marks an entry deleted in place, with no tombstone row -- and
/// the flat panic fired while this walk was still BUILDING its list, before any caller's
/// `!page.deleted` filter could short-circuit. A test whose whole subject is that page could not
/// run.
///
/// SO THE TWO STATES ARE SPLIT, and the assertion says which is which:
///
///   * a LIVE page the lookup does not name PANICS with the handle. That is the decode-before-
///     rebuild window, which the decoder closed by filing as it decodes, so it is a defect.
///   * a DELETED page no tombstone row names is SKIPPED. Its object is genuinely unstated and
///     there is nothing for a walk to attribute it to.
///
/// A skip is still the dangerous direction, which is why only the second case gets one: a skip on
/// the first would be a count quietly short, the denominator mistake this campaign keeps
/// re-finding in a new costume.
///
/// # WHAT THIS IS NOT FOR
///
/// A test whose SUBJECT is a page filed under a bucket its key does not route to. This asks by
/// POSITION, and the lookup names such a page at the position it was filed under, not the one it
/// now sits in -- so this would panic on the very state that test is driving.
/// [`BlockKeysByPosition::identity_anywhere`] is the door for that, and
/// `bucket_fill`'s mis-filing walk uses it directly and says so.
pub(super) fn blocks_with_identity(
    index: &crate::engine::state::CoreIndex,
) -> Vec<(u32, u64, std::sync::Arc<str>, &crate::engine::state::BlockIndex)> {
    let keys = crate::engine::state::BlockKeysByPosition::from_lookup(&index.object_block_lookup);
    let mut out = Vec::new();
    for bucket in index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            let Some(beside) = keys.identity_of(bucket, *handle, block) else {
                assert!(
                    block.deleted,
                    "the LIVE page at handle {handle} in bucket {} is named by nothing beside \n                     it, and a live page is named by the object lookup unless the index is in \n                     the one window the decoder closed by filing as it decodes",
                    bucket.routing_bucket
                );
                continue;
            };
            out.push((
                bucket.routing_bucket,
                *handle,
                std::sync::Arc::clone(beside.object_key),
                block,
            ));
        }
    }
    out
}

fn wait_for_fresh_admission_second() {
    loop {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch");
        if elapsed.subsec_millis() < 100 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn assert_cache_latency_histograms_observed(stats: matrixcache::CacheStats) {
    assert!(stats.read_through_latency_samples > 0);
    assert!(stats.refill_latency_samples > 0);
    assert!(stats.writeback_latency_samples > 0);
    assert!(stats.eviction_latency_samples > 0);
    assert!(stats.compaction_latency_samples > 0);
    assert_eq!(
        stats.read_through_latency_samples,
        stats.read_through_latency_le_10us
            + stats.read_through_latency_le_100us
            + stats.read_through_latency_le_1ms
            + stats.read_through_latency_le_10ms
            + stats.read_through_latency_gt_10ms
    );
    assert_eq!(
        stats.refill_latency_samples,
        stats.refill_latency_le_10us
            + stats.refill_latency_le_100us
            + stats.refill_latency_le_1ms
            + stats.refill_latency_le_10ms
            + stats.refill_latency_gt_10ms
    );
    assert_eq!(
        stats.writeback_latency_samples,
        stats.writeback_latency_le_10us
            + stats.writeback_latency_le_100us
            + stats.writeback_latency_le_1ms
            + stats.writeback_latency_le_10ms
            + stats.writeback_latency_gt_10ms
    );
    assert_eq!(
        stats.eviction_latency_samples,
        stats.eviction_latency_le_10us
            + stats.eviction_latency_le_100us
            + stats.eviction_latency_le_1ms
            + stats.eviction_latency_le_10ms
            + stats.eviction_latency_gt_10ms
    );
    assert_eq!(
        stats.compaction_latency_samples,
        stats.compaction_latency_le_10us
            + stats.compaction_latency_le_100us
            + stats.compaction_latency_le_1ms
            + stats.compaction_latency_le_10ms
            + stats.compaction_latency_gt_10ms
    );
}


#[path = "tests/engine_tests_conformance.rs"]
mod conformance;
#[path = "tests/concurrent_commit.rs"]
mod concurrent_commit;
#[path = "tests/raft_apply_coalesce.rs"]
mod raft_apply_coalesce;
#[path = "tests/phase1_flat.rs"]
mod phase1_flat;
#[path = "tests/storage_maintenance_and_compaction.rs"]
mod part1;
#[path = "tests/cache_tiers_and_command_execution.rs"]
mod part2;
#[path = "tests/engine_tests_quota.rs"]
mod quota;
#[path = "tests/stream_reads_and_shard_controls.rs"]
mod part3;
#[path = "tests/persistence_recovery_and_accounting.rs"]
mod part4;
#[path = "tests/expiry_scale.rs"]
mod expiry_scale;
#[path = "tests/reclaim_dump.rs"]
mod reclaim_dump;
#[path = "tests/claim_stamp_hold.rs"]
mod claim_stamp_hold;
#[path = "tests/command_surface.rs"]
mod command_surface;
#[path = "tests/expiry_command_boundaries.rs"]
mod expiry_command_boundaries;
#[path = "tests/address_footprint.rs"]
mod address_footprint;
#[path = "tests/address_word_footprint.rs"]
mod address_word_footprint;
#[path = "tests/serving_read_guard.rs"]
mod serving_read_guard;
#[path = "tests/poison_shard_guard.rs"]
mod poison_shard_guard;
#[path = "tests/restore_serving_gate.rs"]
mod restore_serving_gate;
#[path = "tests/page_and_block_counter_ratio.rs"]
mod page_and_block_counter_ratio;
#[path = "tests/page_and_block_name_one_tier.rs"]
mod page_and_block_name_one_tier;
#[path = "tests/manifest_restore_model_maps.rs"]
mod manifest_restore_model_maps;
#[path = "tests/prune_crash_window.rs"]
mod prune_crash_window;
#[path = "tests/install_on_load_anchor.rs"]
mod install_on_load_anchor;
#[path = "tests/index_log_replay_reach.rs"]
mod index_log_replay_reach;
#[path = "tests/round_scale.rs"]
mod round_scale;
#[path = "tests/round_stage_cost.rs"]
mod round_stage_cost;
#[path = "tests/round_backlog_cost.rs"]
mod round_backlog_cost;
#[path = "tests/round_walk_scope.rs"]
mod round_walk_scope;
#[path = "tests/bucket_filing_range.rs"]
mod bucket_filing_range;
#[path = "tests/inner_range.rs"]
mod inner_range;
#[path = "tests/shard_carried_range.rs"]
mod shard_carried_range;
#[path = "tests/shard_carried_identity.rs"]
mod shard_carried_identity;
#[path = "tests/index_bytes_per_key.rs"]
mod index_bytes_per_key;
#[path = "tests/sibling_handles.rs"]
mod sibling_handles;
#[path = "tests/level_two_value.rs"]
mod level_two_value;
#[path = "tests/log_resident_is_derived.rs"]
mod log_resident_is_derived;
#[path = "tests/fold_hash_map_completeness.rs"]
mod fold_hash_map_completeness;
#[path = "tests/recorded_map_invariant.rs"]
mod recorded_map_invariant;
#[path = "tests/set_install_census.rs"]
mod set_install_census;
#[path = "tests/model_number_agreement.rs"]
mod model_number_agreement;
#[path = "tests/model_slot_is_a_position.rs"]
mod model_slot_is_a_position;
#[path = "tests/dump_scale.rs"]
mod dump_scale;
#[path = "tests/dump_release_scale.rs"]
mod dump_release_scale;
#[path = "tests/index_write_amplification.rs"]
mod index_write_amplification;
#[path = "tests/restore_scale.rs"]
mod restore_scale;
#[path = "tests/restore_large_scale.rs"]
mod restore_large_scale;
#[path = "tests/promote_precondition.rs"]
mod promote_precondition;
#[path = "tests/alloc_class_scale.rs"]
mod alloc_class_scale;
#[path = "tests/evict_scale.rs"]
mod evict_scale;
#[path = "tests/reclaiming_space_is_its_own_pass.rs"]
mod reclaiming_space_is_its_own_pass;
#[path = "tests/cache_invalidation_cost.rs"]
mod cache_invalidation_cost;
#[path = "tests/cache_read_cost.rs"]
mod cache_read_cost;
#[path = "tests/warm_read_decomposition.rs"]
mod warm_read_decomposition;
#[path = "tests/wal_reclaim_frame_boundary.rs"]
mod wal_reclaim_frame_boundary;
#[path = "tests/blocks_per_bucket.rs"]
mod blocks_per_bucket;
#[path = "tests/bucket_fill.rs"]
mod bucket_fill;
#[path = "tests/routing_range_default.rs"]
mod routing_range_default;
#[path = "tests/context_node_survives_reload.rs"]
mod context_node_survives_reload;
#[path = "tests/durable_outranks_derived.rs"]
mod durable_outranks_derived;
#[path = "tests/per_item_byte_budget.rs"]
mod per_item_byte_budget;
#[path = "tests/flat_block_list.rs"]
