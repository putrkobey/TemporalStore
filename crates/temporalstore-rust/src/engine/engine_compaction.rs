// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction utility/policy/layout reporting helpers, split from engine.rs.
use crate::engine::timestamped_series::TimestampedSeries as _;
use super::*;

/// How many bytes one compaction round may relocate.
///
/// Compaction used to relocate EVERY live page of every model in one pass, holding the shard
/// write lock throughout, so the stall grew with the store. A bound makes the round's cost a
/// property of the constant rather than of the corpus.
///
/// Large enough that a store whose live pages fit inside it still compacts in a single round --
/// which is every store the suite builds, so the existing assertions about what one compaction
/// leaves behind are unchanged. A store bigger than this takes several rounds, and each one
/// relocates pages that have not moved yet rather than re-moving the last round's work.
pub(super) const COMPACTION_ROUND_BYTES: u64 = 256 * 1024 * 1024;

/// How many page refs one compaction round may relocate.
///
/// The stall this bounds tracks the NUMBER of refs moved, not their size: measured in release,
/// a relocation costs ~200 us per ref almost regardless of page size. So a byte budget bounds it
/// only through average page size, and the same byte constant gives wildly different stalls --
/// 256 KiB is ~2,400 refs of 100-byte values but ~64 refs of 4 KiB ones. A ref budget bounds the
/// stall directly.
///
/// 2,048 puts a round near 0.4 s of shard write lock at the measured per-ref cost. It is chosen
/// to sit ABOVE every fixture the suite builds -- which are tens of objects, so they still
/// compact in one round and their single-round assumptions hold -- and BELOW production scale,
/// where 20k refs in one round measured 3.1 s. That is the difference from
/// `COMPACTION_ROUND_BYTES`, which was sized only to clear the suite and so never binds anywhere.
///
/// A round that stops here stays open and the next one resumes onto the same slab, so bounding
/// costs round count, not progress.
pub(super) const COMPACTION_ROUND_BLOCK_REFS: usize = 2_048;

/// Blocks per slab, counted by header walk.
///
/// Both callers below read `block_count` and nothing else off a slab report, and `slab_reports()`
/// reaches that by calling `decode_block_record` on every record in the store -- a CRC32C verify
/// and a decompress each. The compaction phase builds a utility report and a model-layout report
/// BEFORE and AFTER the relocation, so that was four whole-store decodes per round to populate a
/// before/after figure. `count_slab_blocks` walks headers instead: measured 35x cheaper at 32,000
/// records, with identical counts.
///
/// The relocation itself was never the problem -- it has a 256 MiB budget and resumes. This is
/// the survey around it.
fn slab_block_counts_by_slab(block_store: &BlockStore) -> BTreeMap<u64, u64> {
    block_store
        .slab_block_counts()
        .unwrap_or_default()
        .into_iter()
        .map(|(block_slab_id, _physical_bytes, block_count)| (block_slab_id, block_count))
        .collect()
}

pub(super) fn compaction_utility_report(
    block_store: &BlockStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    compaction_utility_report_from_entries(block_store, shard, &collect_live_block_entries(shard))
}

/// The same report, from live-page entries the caller ALREADY has.
///
/// `collect_live_block_entries` materializes every live page in the shard. The compaction preamble
/// builds several reports from that same set, so one walk can serve them all; the wrapper above
/// keeps the old signature for callers with nothing to share.
pub(super) fn compaction_utility_report_from_entries(
    block_store: &BlockStore,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
) -> ShardCompactionUtilityReport {
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_block_slab_ids = addresses
        .iter()
        .map(|address| address.block_slab_id())
        .collect::<BTreeSet<_>>();
    let slab_block_counts = slab_block_counts_by_slab(block_store);
    let total_block_count = live_block_slab_ids
        .iter()
        .map(|block_slab_id| {
            slab_block_counts
                .get(block_slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_block_refs = addresses.len() as u64;
    let stale_block_estimate = total_block_count.saturating_sub(live_block_refs);
    let live_ref_density_basis_points = if total_block_count == 0 {
        0
    } else {
        live_block_refs.saturating_mul(10_000) / total_block_count
    };
    ShardCompactionUtilityReport {
        live_block_slab_count: live_block_slab_ids.len(),
        total_block_count,
        live_block_refs,
        stale_block_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(shard, &entries, &slab_block_counts),
    }
}

pub(super) fn model_compaction_policy_reports(
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    slab_block_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_block_refs: u64,
        deleted_block_refs: u64,
        slab_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone().to_string()).or_default();
        if entry.deleted {
            stats.deleted_block_refs = stats.deleted_block_refs.saturating_add(1);
        } else {
            stats.live_block_refs = stats.live_block_refs.saturating_add(1);
            stats.slab_ids.insert(entry.address.block_slab_id());
        }
    }
    for key in shard
        .dirty_objects
        .iter()
        .filter(|key| !record_exists(shard, key))
    {
        let model_id = if shard.hashes.contains_key(key) {
            "hash"
        } else if shard.sets.contains_key(key) {
            "set"
        } else if shard.lists.contains_key(key) {
            "list"
        } else if shard.zsets.contains_key(key) {
            "zset"
        } else if shard.features().contains_key(key) {
            "feature"
        } else if shard.control_state_blocks().contains_key(key) {
            "control_state"
        } else if shard.context_nodes().contains_key(key) {
            "context_node"
        } else if split_context_entity_key(key)
            .map(|(collection_key, entity_hash)| {
                shard
                    .context_entities()
                    .get(&collection_key[..])
                    .is_some_and(|series| series.get(entity_hash).is_some())
            })
            .unwrap_or(false)
        {
            "context_entity"
        } else {
            "string"
        };
        let stats = by_model.entry(model_id.to_string()).or_default();
        stats.deleted_block_refs = stats.deleted_block_refs.saturating_add(1);
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_slab_blocks = stats
                .slab_ids
                .iter()
                .map(|slab_id| {
                    slab_block_counts
                        .get(slab_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_block_estimate = total_slab_blocks.saturating_sub(stats.live_block_refs);
            let stale_density_basis_points = if total_slab_blocks == 0 {
                0
            } else {
                stale_block_estimate.saturating_mul(10_000) / total_slab_blocks
            };
            let total_refs = stats.live_block_refs.saturating_add(stats.deleted_block_refs);
            let delete_marker_density_basis_points = if total_refs == 0 {
                0
            } else {
                stats.deleted_block_refs.saturating_mul(10_000) / total_refs
            };
            let layout_policy = compaction_layout_policy_for_model(&model_id);
            let stale_density_triggered = stale_density_basis_points > 0;
            let delete_marker_compaction_triggered =
                stats.deleted_block_refs > 0 || delete_marker_density_basis_points > 0;
            let object_block_packing_enabled = compaction_object_block_packing_enabled(&model_id);
            let layout_aware_rewrite_required = object_block_packing_enabled
                || matches!(
                    layout_policy,
                    "timestamped_chunked_blocks" | "context_timeline_or_sidecar_blocks"
                )
                || model_id == "control_state";
            ModelCompactionPolicyReport {
                layout_policy: layout_policy.to_string(),
                object_block_packing_enabled,
                model_id,
                live_block_refs: stats.live_block_refs,
                deleted_block_refs: stats.deleted_block_refs,
                total_slab_blocks,
                stale_block_estimate,
                stale_density_basis_points,
                delete_marker_density_basis_points,
                object_block_pack_group_count: stats.slab_ids.len() as u64,
                cold_block_rewrite_eligible_refs: stats.live_block_refs,
                compaction_action: compaction_action_for_policy(
                    stats.live_block_refs,
                    stats.deleted_block_refs,
                    stale_density_basis_points,
                    delete_marker_density_basis_points,
                )
                .to_string(),
                stale_density_triggered,
                delete_marker_compaction_triggered,
                layout_aware_rewrite_required,
            }
        })
        .collect()
}

pub(super) fn compaction_layout_policy_for_model(model_id: &str) -> &'static str {
    match model_id {
        "string" | "control_state" | "context_node" | "context_entity" | "context_embedding" => {
            "single_block_object"
        }
        "hash" | "set" => "component_block_object",
        "feature" | "sequence" => "timestamped_chunked_blocks",
        model if model.starts_with("context_") => "context_timeline_or_sidecar_blocks",
        _ => "generic_block_object",
    }
}

pub(super) fn compaction_object_block_packing_enabled(model_id: &str) -> bool {
    matches!(
        compaction_layout_policy_for_model(model_id),
        "single_block_object" | "component_block_object"
    )
}

pub(super) fn compaction_action_for_policy(
    live_block_refs: u64,
    deleted_block_refs: u64,
    stale_density_basis_points: u64,
    delete_marker_density_basis_points: u64,
) -> &'static str {
    if live_block_refs == 0 && deleted_block_refs > 0 {
        "drop_tombstones"
    } else if delete_marker_density_basis_points > 0 || deleted_block_refs > 0 {
        "rewrite_live_drop_tombstones"
    } else if stale_density_basis_points > 0 {
        "rewrite_stale_density"
    } else {
        "rewrite_cold_or_pack"
    }
}

#[derive(Debug, Default)]
pub(super) struct CompactionRewriteStats {
    pub(super) rewritten_block_refs: usize,
    pub(super) cold_block_rewrite_refs: usize,
    by_model: BTreeMap<String, ModelCompactionRewriteStats>,
    /// The slab this round is filling. A page already there does not move.
    target_block_slab_id: u64,
    /// Bytes this round may still relocate. Saturates at zero, and a round that reaches zero
    /// leaves the rest where it is for the next one.
    budget_bytes: u64,
    /// Page refs this round may still relocate, the bound that actually tracks the stall.
    /// Same saturating behaviour as `budget_bytes`; whichever runs out first ends the round.
    budget_block_refs: usize,
    pub(super) skipped_by_budget: usize,
    pub(super) skipped_by_budget_bytes: u64,
    /// Bytes this round committed to copying, charged at the same point the byte budget is.
    ///
    /// This is the cost of the round stated in the unit that matters. `rewritten_block_refs`
    /// counts relocations; a relocation reads a page and appends it verbatim somewhere else, so
    /// what it actually spends is its LENGTH, and two rounds with the same ref count can differ
    /// by orders of magnitude in what they moved.
    pub(super) relocated_bytes: u64,
    /// The slabs the caller asked this round to empty, or `None` for every live page.
    ///
    /// `None` is what a DIRECT compaction has always meant and still means: the operator RPC
    /// and the on-demand cycle are instructions, not suggestions. `Some` is what the PERIODIC
    /// loop issues, and it names the only slabs a relocation can recover anything for -- the ones
    /// carrying dead space that objects still hold pages on. A page on any OTHER slab comes out
    /// of a relocation byte for byte what it went in as, on a slab that is now the one carrying
    /// the dead space, so moving it recovers nothing and costs a read, an append and a share of
    /// an index record.
    drain_block_slab_ids: Option<BTreeSet<u64>>,
    /// Pages left where they are because emptying their slab was not asked for.
    ///
    /// NOT work left behind, and the distinction is the termination argument: a later round will
    /// not want these either, so this must never keep the round open. That is why
    /// `should_relocate` tests the drain set BEFORE the budget -- charging these to
    /// `skipped_by_budget` would make `left_work_behind` true for ever on any shard with a
    /// page outside the drain set, and the round would never close.
    pub(super) skipped_off_drain_set: usize,
}

#[derive(Debug, Default)]
pub(super) struct ModelCompactionRewriteStats {
    rewritten_block_refs: usize,
    cold_block_rewrite_refs: usize,
}

impl CompactionRewriteStats {
    /// A round that relocates onto `target_block_slab_id`, spending at most `budget_bytes` and
    /// `budget_block_refs`. Whichever runs out first ends the round.
    pub(super) fn for_round(
        target_block_slab_id: u64,
        budget_bytes: u64,
        budget_block_refs: usize,
    ) -> Self {
        Self {
            target_block_slab_id,
            budget_bytes,
            budget_block_refs,
            ..Self::default()
        }
    }

    /// The same round, restricted to the pages sitting on `drain_block_slab_ids`.
    ///
    /// The set comes off the reclaim plan the maintenance round has already built, so asking for
    /// it costs no walk of the shard -- see `compaction_drain_block_slab_ids`. A round built
    /// this way moves exactly the pages the relocation hint names: the ones whose movement
    /// empties a slab, and nothing else.
    pub(super) fn for_drain_round(
        target_block_slab_id: u64,
        budget_bytes: u64,
        budget_block_refs: usize,
        drain_block_slab_ids: BTreeSet<u64>,
    ) -> Self {
        Self {
            drain_block_slab_ids: Some(drain_block_slab_ids),
            ..Self::for_round(target_block_slab_id, budget_bytes, budget_block_refs)
        }
    }

    /// Whether this address should move, charging the budget when it should.
    ///
    /// Three reasons not to move one: it is already on the slab this round is filling, which is
    /// how a resumed round avoids redoing its predecessor's work; its slab is not one this round
    /// was asked to empty, so moving it would recover nothing; or the round has spent its budget,
    /// which is how it stays bounded. The budget is charged BEFORE the page is read, since
    /// reading is the expensive half and the length is known from the address.
    ///
    /// ORDER MATTERS between the last two. A page outside the drain set must not be charged to
    /// `skipped_by_budget`, because that is what `left_work_behind` reads to keep a round
    /// open for the next one -- and a round kept open by pages nobody will ever want moved never
    /// closes.
    pub(super) fn should_relocate(&mut self, address: &ElementEntry) -> bool {
        if address.block_slab_id() == self.target_block_slab_id {
            return false;
        }
        if self
            .drain_block_slab_ids
            .as_ref()
            .is_some_and(|drain| !drain.contains(&address.block_slab_id()))
        {
            self.skipped_off_drain_set = self.skipped_off_drain_set.saturating_add(1);
            return false;
        }
        if address.length() > self.budget_bytes || self.budget_block_refs == 0 {
            self.skipped_by_budget = self.skipped_by_budget.saturating_add(1);
            self.skipped_by_budget_bytes =
                self.skipped_by_budget_bytes.saturating_add(address.length());
            return false;
        }
        self.budget_bytes = self.budget_bytes.saturating_sub(address.length());
        self.budget_block_refs = self.budget_block_refs.saturating_sub(1);
        self.relocated_bytes = self.relocated_bytes.saturating_add(address.length());
        true
    }

    /// Whether the round stopped early, so the engine keeps it open for the next one.
    pub(super) fn left_work_behind(&self) -> bool {
