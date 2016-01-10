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
        self.skipped_by_budget > 0
    }

    fn record(&mut self, model_id: &str, cold_block: bool) {
        self.rewritten_block_refs = self.rewritten_block_refs.saturating_add(1);
        let model = self.by_model.entry(model_id.to_string()).or_default();
        model.rewritten_block_refs = model.rewritten_block_refs.saturating_add(1);
        if cold_block {
            self.cold_block_rewrite_refs = self.cold_block_rewrite_refs.saturating_add(1);
            model.cold_block_rewrite_refs = model.cold_block_rewrite_refs.saturating_add(1);
        }
    }

    pub(super) fn into_reports(
        self,
        before: &ShardCompactionUtilityReport,
    ) -> Vec<ModelCompactionRewriteReport> {
        self.by_model
            .into_iter()
            .map(|(model_id, stats)| {
                let before_policy = before
                    .model_policies
                    .iter()
                    .find(|policy| policy.model_id == model_id);
                ModelCompactionRewriteReport {
                    layout_policy: compaction_layout_policy_for_model(&model_id).to_string(),
                    model_id,
                    rewritten_block_refs: stats.rewritten_block_refs,
                    cold_block_rewrite_refs: stats.cold_block_rewrite_refs,
                    object_block_pack_group_count: before_policy
                        .map(|policy| policy.object_block_pack_group_count as usize)
                        .unwrap_or_default(),
                    delete_marker_density_basis_points: before_policy
                        .map(|policy| policy.delete_marker_density_basis_points)
                        .unwrap_or_default(),
                    stale_density_basis_points: before_policy
                        .map(|policy| policy.stale_density_basis_points)
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

pub(super) fn block_memory_resident(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    address: &ElementEntry,
    routing_bucket: Option<u32>,
) -> bool {
    // Compaction asking the serving cache a question is still a read of it, and it is charged the
    // same way the three serving primitives are. Charging it rather than excusing it is what lets
    // `every_production_cache_read_is_charged_to_the_read_class` carry no exemption list at all --
    // and an exemption list is where a site goes to stop being looked at.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CacheRead, || {
        cache
            .get_memory(&CacheKey::page_with_slot_generation(
                shard_id,
                address.block_slab_id(),
                address.offset(),
                address.length(),
                routing_bucket,
                address.generation(),
            ))
            .is_some()
    })
}

pub(super) fn compaction_model_layout_reports(
    block_store: &BlockStore,
    shard: &ShardState,
) -> Vec<ShardCompactionModelLayoutReport> {
    let slab_block_counts = slab_block_counts_by_slab(block_store);
    let mut reports = Vec::new();
    reports.push(compaction_layout_from_addresses(
        "string",
        shard.strings().len(),
        shard.strings().values().map(|slot| slot.address()),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "hash",
        shard.hashes.len(),
        shard
            .hashes
            .values()
            .flat_map(|fields| fields.values().cloned()),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "zset",
        shard.zsets.len(),
        shard
            .zsets
            .values()
            .flat_map(|members| members.values().map(|(_, address)| address.clone())),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "list",
        shard.lists.len(),
        shard
            .lists
            .values()
            .flat_map(|elements| elements.values().cloned()),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_layout_from_addresses(
        "set",
        shard.sets.len(),
        shard
            .sets
            .values()
            .flat_map(|members| members.values().cloned()),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "feature",
        shard.features(),
        &slab_block_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_node",
        shard.context_nodes().len(),
        shard.context_nodes().values().cloned(),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_event",
        shard.context_events(),
        &slab_block_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_index",
        shard.context_indexes(),
        &slab_block_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_audit",
        shard.context_audits(),
        &slab_block_counts,
    ));
    reports.push(compaction_layout_from_addresses(
        "context_entity",
        shard
            .context_entities()
            .values()
            .map(|slot| slot.owned_element_count())
            .sum(),
        shard
            .context_entities()
            .values()
            .flat_map(|series| series.addresses().cloned()),
        &slab_block_counts,
        None,
    ));
    reports.push(compaction_timestamped_layout(
        "context_child",
        shard.context_children(),
        &slab_block_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_summary",
        shard.context_summaries(),
        &slab_block_counts,
    ));
    reports.push(compaction_timestamped_layout(
        "context_compression",
        shard.context_compressions(),
        &slab_block_counts,
    ));
    reports.retain(|report| report.object_count > 0 || report.index_refs > 0);
    reports
}

/// GENERIC OVER THE SERIES AND THE KEY, for the reason the walk and the reconcile already are:
/// this reads a series by its addresses alone, and naming one container here is what made a
/// representation change all six maps or none. The key is abstract one step further out for the
/// same reason: the consolidated map these tenants migrate into is not keyed by `String`.
pub(super) fn compaction_timestamped_layout<K, S>(
    kind: &str,
    timelines: &HashMap<K, S>,
    slab_block_counts: &BTreeMap<u64, u64>,
) -> ShardCompactionModelLayoutReport
where
    S: super::timestamped_series::TimestampedSeries,
{
    let mut ref_counts = HashMap::<ElementEntry, usize>::new();
    for address in timelines
        .values()
        .flat_map(|series| series.addresses().cloned())
    {
        *ref_counts.entry(address).or_default() += 1;
    }
    let packed_blocks = ref_counts.values().filter(|count| **count > 1).count();
    compaction_layout_from_addresses(
        kind,
        timelines.len(),
        ref_counts.keys().cloned(),
        slab_block_counts,
        Some(packed_blocks),
    )
    .with_index_refs(ref_counts.values().sum())
}

pub(super) fn compaction_layout_from_addresses(
    kind: &str,
    object_count: usize,
    addresses: impl IntoIterator<Item = ElementEntry>,
    slab_block_counts: &BTreeMap<u64, u64>,
    packed_blocks: Option<usize>,
) -> ShardCompactionModelLayoutReport {
    let addresses = addresses.into_iter().collect::<Vec<_>>();
    let unique_addresses = addresses
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let live_slab_ids = unique_addresses
        .iter()
        .map(|address| address.block_slab_id())
        .collect::<BTreeSet<_>>();
    let total_blocks_in_live_slabs = live_slab_ids
        .iter()
        .map(|slab_id| {
            slab_block_counts
                .get(slab_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let unique_block_refs = unique_addresses.len();
    let packed_timestamped_blocks = packed_blocks.unwrap_or_default();
    let live_ref_density_basis_points = if total_blocks_in_live_slabs == 0 {
        0
    } else {
        (unique_block_refs as u64).saturating_mul(10_000) / total_blocks_in_live_slabs
    };
    ShardCompactionModelLayoutReport {
        kind: kind.to_string(),
        object_count,
        index_refs: addresses.len(),
        unique_block_refs,
        packed_timestamped_blocks,
        legacy_value_blocks: unique_block_refs.saturating_sub(packed_timestamped_blocks),
        stale_block_estimate: total_blocks_in_live_slabs.saturating_sub(unique_block_refs as u64),
        live_ref_density_basis_points,
    }
}

pub(super) trait CompactionLayoutIndexRefs {
    fn with_index_refs(self, index_refs: usize) -> Self;
}

impl CompactionLayoutIndexRefs for ShardCompactionModelLayoutReport {
    fn with_index_refs(mut self, index_refs: usize) -> Self {
        self.index_refs = index_refs;
        self
    }
}

/// Fault seam: make the block read inside a compaction round fail after N relocations.
///
/// The ONLY way a round fails partway in production is `read_block_bytes` returning `None` --
/// a torn or missing page -- and nothing else in the round can produce that state on demand. The
/// resume-anchor behaviour on the partial-failure path is therefore untestable without a seam, so
/// this is it. Thread-local, because a round runs on its caller's thread and tests that set it
/// must not reach a round another test is driving.
#[cfg(test)]
thread_local! {
    static FAIL_BLOCK_READ_AFTER: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Arm (`Some(n)`) or disarm (`None`) the seam above. `Some(0)` fails the first read.
#[cfg(test)]
pub(crate) fn fail_compaction_block_read_after_for_test(relocations: Option<usize>) {
    FAIL_BLOCK_READ_AFTER.with(|cell| cell.set(relocations));
}

/// `read_block_bytes`, with the test seam in front of it. Compiles to a direct call outside
/// tests: the hook exists only under `cfg(test)`, so no production round pays for it.
fn read_block_bytes_for_compaction(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    address: &ElementEntry,
    // WHICH PAGE, which this function could not say and now must. The element stays `None` for the
    // reason spelled out below; what changed is that the OBJECT is stated from the key rather than
    // recovered from the address, so a relocation cannot read a page it cannot name.
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
) -> Option<Vec<u8>> {
    #[cfg(test)]
    {
        let armed = FAIL_BLOCK_READ_AFTER.with(|cell| cell.get());
        if let Some(remaining) = armed {
            if remaining == 0 {
                return None;
            }
            FAIL_BLOCK_READ_AFTER.with(|cell| cell.set(Some(remaining - 1)));
        }
    }
    // `None`, AND THIS IS THE ONE PLACE IN THIS CHANGE THAT PASSES IT WITHOUT THE PAGE BEING
    // A WHOLE OBJECT. Compaction walks the model maps, and for the container kinds those maps
    // are keyed by the element's VALUE (a set's member bytes, a zset's member, a list's
    // sequence) rather than by the component the index filed the page under -- which is a
    // rendering of it: `hex::encode(member)` for a set, `{score:016x}` before that hex for a
    // zset, `{:016x}` of the biased sequence for a list. There is no shared renderer to call;
    // each write site spells it out inline. Re-spelling them here would be a second copy of
    // four rules, free to drift from the four originals, and a component that drifted would
    // be indistinguishable from this `None` in its effect.
    //
    // WHAT IT COSTS, stated rather than implied: the in-log fallback inside `read_block_bytes`
    // will not resolve a container ELEMENT page whose only durable copy is still a WAL record,
    // so compaction cannot relocate that page -- and the round FAILS rather than skipping it.
    // Both callers turn this `None` into `Status::error("block_compaction_failed", ...)` with `?`,
    // which is what CP4 requires: a round that stops partway must propagate, so the consistent
    // partial index is durably committed, or reclaim purges a slab the durable index still names.
    // The fault seam above says the same from the other side -- returning `None` here is THE way
    // a round fails partway in production. Do NOT read this paragraph as permission to make the
    // caller skip the entry and leave the address alone; the guard is
    // `part4::partial_compaction_failure_durably_persists_the_consistent_partial_index`, and that
    // misreading has already turned it red once. The bytes are never wrong either way -- what is
    // at stake is whether the failure stays loud.
    // Whole-object pages, which is every `string` and `control_state`, are unaffected: `None`
    // is their actual component.
    read_block_bytes(cache, block_store, shard_id, address, identity, routing_bucket)
}

/// How many elements one rewritten container page may hold.
///
/// A cap and not a target: the byte target below is what usually binds, and this stops a container
/// of tiny elements -- a set of eight-byte members, a hash of counters -- from putting tens of
/// thousands of them behind one address. Everything behind one address shares a cache entry, a
/// checksum and a read, so a page that holds the whole container turns every point read into a read
/// of the container.
///
/// 128 because that is the order the comparison design bounds its own delta chain at, and because
/// the measured saving is nearly all spent by then: `container_blocks`'s ladder puts a set of
/// eight-byte members at 12 bytes of framing per element at one per page and 3.1 at 128, and the
/// curve between 32 and 128 is worth half a byte.
pub const CONTAINER_BATCH_ELEMENT_CAP: usize = 128;

/// The byte target a rewritten container page is chunked against.
///
/// The SAME target the timestamped chunker uses (`context_block_target_bytes`), deliberately, so the
/// engine has one answer to "how big should a page be" rather than two that can drift. A container
/// page and a packed series page are read by the same block store through the same cache with the
/// same record header; nothing about a container makes a different size right, and a second constant
/// would be a second thing to tune.
fn container_batch_target_bytes() -> usize {
    crate::storage_config::context_block_target_bytes()
}

/// One element of a container, as compaction sees it: the bytes that name it, and where it lives.
///
/// The KEY BYTES and not the component. The component is a rendering of these -- hex for a set,
/// a score before that hex for a zset, the biased word for a list, the field name itself for a hash
/// -- and `container_blocks::component_from_element_key` is the one renderer for all four. Before it
/// existed, `read_block_bytes_for_compaction` documented at length why this function could not have
/// the component: "There is no shared renderer to call; each write site spells it out inline.
/// Re-spelling them here would be a second copy of four rules, free to drift from the four
/// originals." There is a renderer now, so the caller hands over key bytes and this asks it.
pub(super) struct ContainerElementRef<'a> {
    pub(super) key: Vec<u8>,
    pub(super) address: &'a mut ElementEntry,
}

/// Rewrite a container's pages as FEWER, LARGER pages that each hold several elements.
///
/// # WHAT THIS CHANGES AND WHAT IT DOES NOT
///
/// It changes the PAGE count and the ENTRY count falls with it. Several elements come to share one
/// address, and `block_index_handle` hashes the model spelling, the object key and that address and
/// nothing per-element, so entries converge on pages -- `entry_count_versus_block_count`'s
/// `one_block_is_one_entry_because_the_handle_can_no_longer_name_an_element` holds the mechanism and
/// `container_blocks_are_batched::a_compaction_round_folds_a_containers_pages_and_folds_its_entry_
/// count_with_them` measures it through a round, 40 entries over 40 pages before and 1 over 1 after.
///
/// THIS PARAGRAPH SAID THE OPPOSITE AND IT WAS TRUE WHEN IT WAS WRITTEN: "It changes the PAGE count
/// and not the ENTRY count ... `block_index_handle` hashes the component beside the address, so the
/// page index still holds one entry per element ... The entry count is a later stage's subject."
/// That later stage has landed -- `BlockIndex` has no element-name field -- so the entry count is
/// this round's subject too.
///
/// Page count is what a load path that reads every page pays, which is why it comes first: a replay
/// that read a page per element once read 987 MB off a 32 MB log, and the reason that was
/// affordable anywhere is that the timestamped kinds hold many items per page already.
///
/// # ONLY PAGES THIS ROUND WOULD HAVE MOVED ANYWAY
///
/// `should_relocate` is asked per element and an element it refuses is left where it is, its address
/// untouched, and is NOT pulled into a batch. So this adds no reads and no writes to a round: it
/// changes the shape of what a round was already going to rewrite. A round that relocates nothing
/// batches nothing.
///
/// # AND IT IS THE MIGRATION OFF UNFRAMED PAGES
///
/// A page written before `container_blocks` carries a bare value and no element key. Read through the
/// funnel with its component it hands that value back, and this function writes it into a frame that
/// names it. So an old store becomes a framed one as it is compacted, without a migration pass of
/// its own.
///
/// # THE READ THAT MUST NOT BE SILENTLY EMPTY, AND MUST NOT BE SILENTLY SKIPPED EITHER
///
/// An element whose page cannot be read FAILS THE ROUND, which is what `compact_block_addresses`
/// does for the same case and is not what this function did when it was first written. Both halves
/// of that matter and they are different failures:
///
///   * contributing an EMPTY value to the batch would rewrite a live element as a zero-length one,
///     and it would read back as PRESENT AND EMPTY -- #1989's shape, and worse than a miss because a
///     miss is visible. That is why the read's failure arm is not an `unwrap_or_default`.
///   * SKIPPING the element and carrying on would be safe for the element and unsafe for the ROUND.
///     CP4 is the recorded reason: a round that hits an unreadable page must propagate the error so
///     the handler durably commits the consistent partial index, because a round that returns
///     success having quietly left the volatile index advanced past the durable one lets the
///     independent reclaim path purge a slab the durable index still names.
///     `part4::partial_compaction_failure_durably_persists_the_consistent_partial_index` is the
///     guard, and it caught exactly this: the first version of this function skipped, so the round
///     succeeded and that test's `result.is_err()` went red.
///
/// Whatever was already flushed stays flushed and its addresses stay moved -- that IS the consistent
/// partial the handler commits. The batch under construction when the read failed is dropped
/// unwritten, so its elements keep their own pages.
/// `a_container_element_whose_block_cannot_be_read_fails_the_round_rather_than_folding_an_empty_value`
/// drives it.
#[allow(clippy::too_many_arguments)]
pub(super) fn compact_container_blocks_batched<'a>(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    // THE OBJECT THESE PAGES BELONG TO, and one value for the whole call rather than one per
    // element -- the shape `compact_feature_block_addresses` already uses, for the same reason: the
    // caller is standing on one object and every element it hands over is an element OF that object.
    // `compact_block_addresses` carries a key per ITEM because its string arm walks a map of many
    // objects; these four arms do not.
    //
    // AND IT IS NOT OPTIONAL, because a [`BlockIdentity`] cannot be built without it. The read below
    // asks for ONE ELEMENT of a page, and a read handed only a component and no object cannot say
    // which object's element it means.
    object_key: &str,
    routing_bucket: u32,
    elements: impl IntoIterator<Item = ContainerElementRef<'a>>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    let Some(spelling) = super::container_blocks::ElementKeySpelling::for_kind(model_id) else {
        // Not a kind whose pages name an element, so there is nothing to batch by. Never reached
        // from the arms below, which are the four container kinds; stated rather than asserted
        // because a fifth kind added to those arms should degrade to doing nothing, not panic.
        return Ok(());
    };
    let target_bytes = container_batch_target_bytes();

    // One batch under construction: the items, the addresses that will point at it, and whether any
    // of its pages was cold. Held as owned bytes because the frame is built from all of them at
    // once, after the last one is read.
    let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut destinations: Vec<&mut ElementEntry> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut cold_in_batch = false;
    let mut batches = 0usize;
    let mut blocks_folded = 0usize;

    // The object id every element of this object already carries. #2019 made it the OBJECT's rather
    // than one of its elements, which is what lets one page stand for several elements at all: a
    // batched page can carry only one object id, and before #2019 that id named one element and
    // would have been wrong for the rest.
    let mut object_id: Option<u64> = None;

    for element in elements {
        if !rewrite_stats.should_relocate(element.address) {
            continue;
        }
        let component = match super::container_blocks::component_from_element_key(
            spelling,
            &element.key,
        ) {
            Some(component) => component,
            // A key this spelling cannot render names no component, so no page can be found for it.
            // Counted by being skipped; the address is left alone.
            None => continue,
        };
        let value = read_block_bytes_for_container_element(
            cache,
            block_store,
            shard_id,
            element.address,
            // THE TERMS, NOT AN ID. The object comes from the key this call is standing on and the
            // element from the component just rendered, so the identity is derived here from both
            // halves rather than recovered from the address -- which is what #2024 made the only
            // way to name a page, precisely so a relocation cannot read a page it cannot name.
            BlockIdentity::of(shard_id, model_id, object_key, Some(component.as_str())),
            routing_bucket,
        )
        .ok_or_else(|| {
            Status::error(
                "block_compaction_failed",
                "missing container element page bytes during compaction",
            )
        })?;
        let cold = !block_memory_resident(cache, shard_id, element.address, Some(routing_bucket));
        if object_id.is_none() {
            object_id = Some(stable_block_object_id(shard_id, model_id, object_key));
        }

        // Would this element take the batch past either bound? Flush first, so a batch is sealed
        // BEFORE it exceeds the target rather than after -- the same order the timestamped chunker
        // splits in.
        let item_bytes = element.key.len() + value.len() + 8;
        if !items.is_empty()
            && (items.len() >= CONTAINER_BATCH_ELEMENT_CAP
                || batch_bytes + item_bytes > target_bytes)
        {
            blocks_folded += flush_container_batch(
                block_store,
                cache,
                shard_id,
                model_id,
                spelling,
                routing_bucket,
                object_id,
                batches,
                &mut items,
                &mut destinations,
                &mut batch_bytes,
                &mut cold_in_batch,
                rewrite_stats,
            )?;
            batches += 1;
        }

        batch_bytes += item_bytes;
        cold_in_batch |= cold;
        items.push((element.key, value));
        destinations.push(element.address);
    }

    if !items.is_empty() {
        blocks_folded += flush_container_batch(
            block_store,
            cache,
            shard_id,
            model_id,
            spelling,
            routing_bucket,
            object_id,
            batches,
            &mut items,
            &mut destinations,
            &mut batch_bytes,
            &mut cold_in_batch,
            rewrite_stats,
        )?;
        batches += 1;
    }
    note_container_batch_round(batches, blocks_folded);
    Ok(())
}

/// One element's VALUE out of the page its index entry names.
///
/// Through the read funnel WITH THE COMPONENT NAMED IN THE IDENTITY, which is the one thing that
/// makes this different from `read_block_bytes_for_compaction`: that one builds its identity with
/// `None` for the component and so gets the whole payload, because until `container_blocks` there was
/// no renderer to turn a model-map key into a component. There is one now, so this asks for the
/// element and gets its value whether the stored page is framed or is a bare value from before the
/// frame existed -- `select_container_element` answers `NotFramed` for the second and the funnel
/// hands back the whole page, which for an unframed page IS the element.
fn read_block_bytes_for_container_element(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    address: &ElementEntry,
    identity: BlockIdentity<'_>,
    routing_bucket: u32,
) -> Option<Vec<u8>> {
    #[cfg(test)]
    {
        let armed = FAIL_BLOCK_READ_AFTER.with(|cell| cell.get());
        if let Some(remaining) = armed {
            if remaining == 0 {
                return None;
            }
            FAIL_BLOCK_READ_AFTER.with(|cell| cell.set(Some(remaining - 1)));
        }
    }
    read_block_bytes(
        cache,
        block_store,
        shard_id,
        address,
        identity,
        Some(routing_bucket),
    )
}

/// Write one batch and point every contributing address at it. Returns how many pages it folded.
#[allow(clippy::too_many_arguments)]
fn flush_container_batch(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    spelling: super::container_blocks::ElementKeySpelling,
    routing_bucket: u32,
    object_id: Option<u64>,
    batch_ordinal: usize,
    items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    destinations: &mut Vec<&mut ElementEntry>,
    batch_bytes: &mut usize,
    cold_in_batch: &mut bool,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<usize, Status> {
    let borrowed: Vec<(&[u8], &[u8])> = items
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let bytes = super::container_blocks::encode_container_block(spelling, &borrowed);
    // WHICH page of its object this is, and it is the BATCH's ordinal rather than any element's.
    // #2008 threaded this so the record header and the address are built from ONE value, because
    // `decode_block_record` refuses a page whose two copies disagree. A batched page has one
    // header and one address, so it has one ordinal, and numbering batches is the only numbering
    // that stays true when the elements inside them move between pages.
    let ordinal = u32::try_from(batch_ordinal).unwrap_or(u32::MAX);
    let new_address = block_store
        .append_block_of_object(&bytes, object_id, Some(routing_bucket), ordinal)
        .map_err(|err| Status::error("block_compaction_failed", err.to_string()))?;
    let _ = cache.put(
        CacheKey::page_with_slot_generation(
            shard_id,
            new_address.block_slab_id(),
            new_address.offset(),
            new_address.length(),
            Some(routing_bucket),
            new_address.generation(),
        ),
        bytes,
    );
    let folded = destinations.len();
    for destination in destinations.drain(..) {
        *destination = new_address.clone();
        rewrite_stats.record(model_id, *cold_in_batch);
    }
    items.clear();
    *batch_bytes = 0;
    *cold_in_batch = false;
    Ok(folded)
}

/// Batches written and pages folded into them, so a guard can assert a round actually batched.
///
/// A COUNTER AND NOT A DERIVED FIGURE. "Pages went down" is measurable from the store, and it is
/// also what a round that relocated nothing reports, so a test that only looked at the store could
/// not tell batching from an idle round. These two say what this code DID.
static CONTAINER_BATCHES_WRITTEN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static CONTAINER_BLOCKS_FOLDED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_container_batch_round(batches: usize, blocks_folded: usize) {
    CONTAINER_BATCHES_WRITTEN.fetch_add(batches as u64, std::sync::atomic::Ordering::Relaxed);
    CONTAINER_BLOCKS_FOLDED.fetch_add(blocks_folded as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Batches written and element pages folded into them since the last reset.
pub fn container_batch_counts() -> (u64, u64) {
    (
        CONTAINER_BATCHES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
        CONTAINER_BLOCKS_FOLDED.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget the counts, so a test measures its own round.
pub fn reset_container_batch_counts() {
    CONTAINER_BATCHES_WRITTEN.store(0, std::sync::atomic::Ordering::Relaxed);
    CONTAINER_BLOCKS_FOLDED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// THE BUCKET AND THE KEY COME IN BESIDE EACH ADDRESS, because compaction reads and re-writes
/// pages and the read is keyed by both.
///
/// The bucket used to come off the address and the key was never carried at all. A compaction round
/// walks a model map and every entry in that map belongs to one object key, so the caller has both
/// already -- what it did not have was a reason to say so. `Item = (u32, &str, &mut BlockAddress)`
/// makes the caller say them, and the compiler name any caller that cannot.
///
/// THE KEY IS THE ADDITION, and it is the same doctrine one term further. The relocation read needs
/// a [`BlockIdentity`], the identity is `(shard, kind, key)`, and `model_id` beside this parameter is
/// only two of those three. Every arm in `recovery_sweep_compact` was ALREADY deriving its bucket
/// from the key it is iterating, so no arm had to find one it did not have.
///
/// A `Cow` AND NOT A `&str`, FOR ONE ARM. Twelve of the thirteen arms iterate a map whose key IS the
/// object key and can lend it for the length of the walk. The ENTITY arm cannot: a page there is
/// filed under `context_entity_key(..)`, the collection key with the entity hash appended, which is
/// COMPOSED per entity and so is a temporary no borrow can outlive. `Cow` lets that arm hand over
/// the string it already builds -- it has always built one, to derive the bucket -- instead of
/// forcing every arm to allocate, or forcing that arm to collect the whole walk into a `Vec` first.
pub(super) fn compact_block_addresses<'a>(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    addresses: impl IntoIterator<Item = (u32, std::borrow::Cow<'a, str>, &'a mut ElementEntry)>,
    rewrite_stats: &mut CompactionRewriteStats,
) -> Result<(), Status> {
    for (routing_bucket, object_key, address) in addresses {
        if !rewrite_stats.should_relocate(address) {
            continue;
        }
        let cold_block = !block_memory_resident(cache, shard_id, address, Some(routing_bucket));
        let bytes = read_block_bytes_for_compaction(
            cache,
            block_store,
            shard_id,
            address,
            BlockIdentity::of(shard_id, model_id, object_key.as_ref(), None),
            Some(routing_bucket),
        )
            .ok_or_else(|| {
                Status::error(
                    "block_compaction_failed",
                    "missing page bytes during compaction",
                )
            })?;
        // Compaction REWRITES a block that already exists, so it keeps the id that block
        // already has. A block id is an index inside its object: taking a fresh one here would
        // give every block of a multi-block object the same index, and the index entries would
        // collide -- which reads back as a reload losing rows.
        let new_address = block_store
            .append_block_of_object(
                &bytes,
                Some(stable_block_object_id(shard_id, model_id, &object_key)),
                Some(routing_bucket),
                address.block_id().unwrap_or_default() as u32,
            )
            .map_err(|err| Status::error("block_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot_generation(
                shard_id,
                new_address.block_slab_id(),
                new_address.offset(),
                new_address.length(),
                Some(routing_bucket),
                new_address.generation(),
            ),
            bytes,
        );
        rewrite_stats.record(model_id, cold_block);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compact_feature_block_addresses(
    block_store: &BlockStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    model_id: &str,
    // The object these series pages belong to. One series is one object, so this is one value for
    // the whole call rather than one per address -- which is why it is a parameter here and a term
    // of the iterator item in `compact_block_addresses`.
    object_key: &str,
    // GENERIC OVER THE SERIES, same argument as the layout report above: this rewrites addresses
    // in place and reads them back, both of which the series trait names.
    series: &mut impl super::timestamped_series::TimestampedSeries,
