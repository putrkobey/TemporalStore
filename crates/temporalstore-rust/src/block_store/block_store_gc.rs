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
