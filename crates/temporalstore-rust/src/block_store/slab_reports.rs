// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore slab descriptor/summary + stream-backed slab runtime report, extracted from block_store.rs.

use super::*;

/// Map a slab descriptor's lifecycle state to the index-log `SlabCatalogState` (1:1). Kept a free fn
/// so both directions of the MANIFEST-CONFORMANCE FOLD conversion share one mapping.
fn slab_state_to_catalog_state(state: BlockStoreSlabState) -> crate::index_log::SlabCatalogState {
    match state {
        BlockStoreSlabState::Active => crate::index_log::SlabCatalogState::Active,
        BlockStoreSlabState::Sealed => crate::index_log::SlabCatalogState::Sealed,
        BlockStoreSlabState::DelayedDestroy => crate::index_log::SlabCatalogState::DelayedDestroy,
        BlockStoreSlabState::Purged => crate::index_log::SlabCatalogState::Purged,
    }
}

fn catalog_state_to_slab_state(state: crate::index_log::SlabCatalogState) -> BlockStoreSlabState {
    match state {
        crate::index_log::SlabCatalogState::Active => BlockStoreSlabState::Active,
        crate::index_log::SlabCatalogState::Sealed => BlockStoreSlabState::Sealed,
        crate::index_log::SlabCatalogState::DelayedDestroy => BlockStoreSlabState::DelayedDestroy,
        crate::index_log::SlabCatalogState::Purged => BlockStoreSlabState::Purged,
    }
}

impl BlockStore {
    pub fn slab_descriptors(&self) -> Vec<BlockStoreSlabDescriptor> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs
            .values()
            .cloned()
            .collect()
    }

    /// How many sealed slabs this open kept from the manifest WITHOUT re-reading them.
    ///
    /// The skip is the default (`TS_REVERIFY_ALL_SLABS` unset) and it is where a descriptor is
    /// carried across an open untouched -- so it is the route worth guarding, and the one easiest
    /// to guard vacuously. Arming it takes THREE opens: the open that inspects a slab stamps
    /// `verified_source_mtime_unix_ms` on its descriptor and only then writes the manifest out,
    /// so the second open is the first that can read a stamped descriptor back and the third is
    /// the first where the stamp is already on disk when a test edits the manifest around it.
    /// A guard that asserts a property of the skip route without first asserting this count is
    /// non-zero is asserting it of a route that never ran.
    pub fn slabs_skipped_reinspection_on_open(&self) -> usize {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs_skipped_reinspection_on_open
    }

    /// Bytes and reads THIS store's open took off its own disk: the slab manifest, plus every
    /// slab the inspection did not skip.
    ///
    /// THIS IS THE ROW THAT READ ZERO. `BlockStoreStats::bytes_read` counts records handed back
    /// through the four read entry points, and an open hands back no record -- so a cold open of
    /// a populated store moved no counter in this engine at all, and a restore harness sampling
    /// the store either side of its load saw `bytes_read: 0` and could not tell that from a store
    /// that had read nothing. Measured at 50 and 200 slabs, the open read 152,760 and 611,360
    /// bytes while the store reported zero.
    ///
    /// Per store, and taken from the tallies the open itself used, so a suite opening stores in
    /// parallel does not read one store's open as another's -- which the process-wide
    /// [`block_store_file_read_counts`] cannot avoid.
    ///
    /// [`block_store_file_read_counts`]: crate::block_store::block_store_file_read_counts
    pub fn open_file_read_counts(&self) -> (u64, u64) {
        let inner = self.inner.lock().expect("block store lock poisoned");
        (inner.open_file_bytes_read, inner.open_file_reads)
    }

    /// MANIFEST-CONFORMANCE FOLD: project the in-memory slab catalog into the DURABLE `SlabCatalogEntry`
    /// subset kept in the index-log slab catalog. Only the durable fields ride in
    /// the fold; the slab descriptor's diagnostic fields (readable_prefix / corruption / errors)
    /// are deliberately dropped -- they are recomputed on load by scanning the slab, exactly as
    /// this design does not persist them. `slab_version` stamps every entry so a folded anchor
    /// carries a monotonically-versioned snapshot.
    pub fn slab_catalog(&self, slab_version: u64) -> Vec<crate::index_log::SlabCatalogEntry> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs
            .values()
            .map(|slab| crate::index_log::SlabCatalogEntry {
                block_slab_id: slab.block_slab_id,
                state: slab_state_to_catalog_state(slab.state),
                physical_bytes: slab.physical_bytes,
                logical_bytes: slab.logical_bytes,
                created_unix_ms: slab.created_unix_ms,
                updated_unix_ms: slab.updated_unix_ms,
                first_block_id: slab.first_block_id,
                last_block_id: slab.last_block_id,
                version: slab_version,
            })
            .collect()
    }

    /// MANIFEST-CONFORMANCE FOLD recovery: seed the slab catalog from a folded `SlabCatalogEntry` snapshot
    /// recovered from the index-log MetaItem. Applied on load AFTER the block store has already
    /// reconciled from durable pages (reconcile stays authoritative for on-disk physical bytes
    /// and diagnostics), so this only RESTORES the catalog fields a pure disk scan cannot infer:
    /// the exact lifecycle state, the creation/update timestamps, the logical byte count, and the
    /// first/last page-id range. It never deletes a slab reconcile found on disk and never
    /// downgrades physical bytes below what the slab actually holds -- so it cannot lose durable
    /// state; it is a metadata refinement layered on the lossless disk-derived catalog. Persists
    /// the merged manifest once. Returns whether anything changed.
