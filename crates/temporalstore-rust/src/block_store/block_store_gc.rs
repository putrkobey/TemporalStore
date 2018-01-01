// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore garbage-collection methods, extracted from block_store.rs.

use super::*;

impl BlockStore {
    /// Take the per-slab live tally the INDEX maintains.
    ///
    /// Publishing rather than deriving, because the store has no way to derive it: a page dies
    /// when an index entry stops naming it, and that event never reaches here. The index keeps a
    /// running tally on its own mutation path and hands a snapshot over; this only reads it.
    ///
    /// Idempotent, and the last publish wins. Cheap: one map, once per maintenance round, sized
    /// by SLABS rather than by pages.
    pub fn publish_live_block_bytes(&self, live: BTreeMap<u64, BlockStoreSlabLive>) {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .live_block_bytes = Some(live);
    }

    /// What was last published, or `None` if nothing ever was.
    pub fn published_live_block_bytes(&self) -> Option<BTreeMap<u64, BlockStoreSlabLive>> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .live_block_bytes
            .clone()
    }

    /// How much of each slab is still live, for EVERY slab -- not just the collectable ones.
    ///
    /// This is the answer to "does `utility_basis_points` stop being uniformly zero", and it has
    /// to be asked of every slab to be worth asking. The GC candidate list cannot answer it: a
    /// candidate is a slab that no live page points at, so its live fraction is zero by
    /// construction and will stay zero however the figure is computed. The slabs with an
    /// interesting fraction are exactly the ones the collector is not allowed to touch.
    ///
    /// Empty live figures when nothing has published; the physical and logical columns still
    /// stand, so a caller can tell "no live pages" from "no tally".
    pub fn slab_live_fractions(&self) -> Result<Vec<BlockStoreSlabLiveFraction>, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let published = inner.live_block_bytes.clone().unwrap_or_default();
        let mut out = Vec::new();
        for block_slab_id in slab_ids_at(&inner.root)? {
            let physical_bytes = slab_path(&inner.root, block_slab_id)
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            let logical_bytes = inner
                .slabs
                .get(&block_slab_id)
                .map(|slab| slab.logical_bytes)
                .filter(|bytes| *bytes > 0)
                .unwrap_or(physical_bytes);
            let live = published.get(&block_slab_id).copied().unwrap_or_default();
            // Capped at the denominator. A live page can be counted at its logical length while
            // the slab total was written before that page was rewritten in place, and a fraction
            // above 1 is never a useful reading.
            let live_bytes = live.live_bytes.min(logical_bytes);
            let live_basis_points = if logical_bytes == 0 {
                0
            } else {
                live_bytes.saturating_mul(10_000) / logical_bytes
            };
            out.push(BlockStoreSlabLiveFraction {
                block_slab_id,
                physical_bytes,
                logical_bytes,
                live_block_refs: live.live_block_refs,
                live_bytes,
                live_basis_points,
                garbage_basis_points: 10_000_u64.saturating_sub(live_basis_points),
            });
        }
        Ok(out)
    }

    pub fn gc_slabs_before(
        &self,
        retain_from_block_slab_id: u64,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs(retain_from_block_slab_id, std::iter::empty())
    }

    pub fn gc_slabs_before_with_live_refs(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs_mode(
            retain_from_block_slab_id,
            live_block_slab_ids,
            false,
        )
    }

    pub fn gc_slabs_before_with_live_refs_delayed_destroy(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        self.gc_slabs_before_with_live_refs_mode(
            retain_from_block_slab_id,
            live_block_slab_ids,
            true,
        )
    }

    pub fn gc_slabs_before_with_live_refs_utility(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        max_destroy_slabs: usize,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        if max_destroy_slabs == 0 {
            return self.gc_slabs_before_with_live_refs_selected(
                retain_from_block_slab_id,
                live_block_slab_ids,
                delayed_destroy,
                Some(BTreeSet::new()),
            );
        }
        self.gc_slabs_before_with_live_refs_policy(
            retain_from_block_slab_id,
            live_block_slab_ids,
            BlockStoreGcPolicy::max_slabs(max_destroy_slabs),
            delayed_destroy,
        )
    }

    pub fn gc_slabs_before_with_live_refs_policy(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let selected = self
            .gc_policy_plan(
                retain_from_block_slab_id,
                live_block_slab_ids.iter().copied(),
                &policy,
            )?
            .selected_block_slab_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.gc_slabs_before_with_live_refs_selected(
            retain_from_block_slab_id,
            live_block_slab_ids,
            delayed_destroy,
            Some(selected),
        )
    }

    /// Policy-planned collection, narrowed to the slabs a caller's own analysis still allows.
    ///
    /// Two independent filters, deliberately kept separate. The POLICY decides which candidates
    /// are worth collecting (garbage ratio, age, per-round budget); `allowed_block_slab_ids`
    /// carries a decision the block store cannot make for itself -- which slabs are pinned by a
    /// follower's replay cursor, a snapshot floor or a retained manifest. Intersecting them here
    /// rather than folding one into the other keeps either side auditable on its own: the plan
    /// still reports what the policy skipped, and what the caller withheld does not masquerade
    /// as a policy decision.
    ///
    /// `None` allows everything the policy selected, which is what the unnarrowed entry does.
    pub fn gc_slabs_before_with_live_refs_policy_limited(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: BlockStoreGcPolicy,
        delayed_destroy: bool,
        allowed_block_slab_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStoreGcReport, BlockStoreError> {
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let selected = self
            .gc_policy_plan(
                retain_from_block_slab_id,
                live_block_slab_ids.iter().copied(),
                &policy,
            )?
            .selected_block_slab_ids
            .into_iter()
            .filter(|block_slab_id| {
                allowed_block_slab_ids
                    .as_ref()
                    .map(|allowed| allowed.contains(block_slab_id))
                    .unwrap_or(true)
            })
            .collect::<BTreeSet<_>>();
        self.gc_slabs_before_with_live_refs_selected(
            retain_from_block_slab_id,
            live_block_slab_ids,
            delayed_destroy,
            Some(selected),
        )
    }

    pub fn gc_policy_plan(
        &self,
        retain_from_block_slab_id: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        policy: &BlockStoreGcPolicy,
    ) -> Result<BlockStoreGcPolicyPlan, BlockStoreError> {
        let candidates =
            self.gc_utility_candidates(retain_from_block_slab_id, live_block_slab_ids)?;
        let mut selected_block_slab_ids = Vec::new();
        let mut selected_physical_bytes = 0_u64;
        let candidate_physical_bytes = candidates.iter().map(|candidate| candidate.bytes).sum();
        let candidate_total_bytes = candidates
            .iter()
            .map(|candidate| candidate.total_bytes)
            .sum::<u64>();
        let candidate_used_bytes = candidates
            .iter()
            .map(|candidate| candidate.used_bytes)
            .sum::<u64>();
        let candidate_stale_bytes = candidates
            .iter()
            .map(|candidate| candidate.stale_bytes)
            .sum::<u64>();
        let candidate_utility_basis_points = if candidate_total_bytes == 0 {
            0
        } else {
            candidate_used_bytes.saturating_mul(10_000) / candidate_total_bytes
        };
        let mut skipped_by_policy_count = 0_usize;
        let mut skipped_by_policy_physical_bytes = 0_u64;
        let mut skipped_by_budget_count = 0_usize;
        let mut skipped_by_budget_physical_bytes = 0_u64;

        for candidate in &candidates {
            let utility_allowed = policy
                .max_utility_score
                .map(|max_score| candidate.utility_score <= max_score)
                .unwrap_or(true);
            let age_allowed = policy
                .min_age_ms
                .map(|min_age| candidate.age_ms.unwrap_or_default() >= min_age)
                .unwrap_or(true);
            // Garbage-ratio gate (GetGarbageRate threshold): keep slabs whose
            // garbage ratio is below the floor. garbage = 10_000 - live-fraction.
            let garbage_allowed = policy
                .min_slab_garbage_basis_points
                .map(|floor| 10_000u64.saturating_sub(candidate.utility_basis_points) >= floor)
                .unwrap_or(true);
            if !utility_allowed || !age_allowed || !garbage_allowed {
                skipped_by_policy_count += 1;
                skipped_by_policy_physical_bytes =
                    skipped_by_policy_physical_bytes.saturating_add(candidate.bytes);
                continue;
            }

