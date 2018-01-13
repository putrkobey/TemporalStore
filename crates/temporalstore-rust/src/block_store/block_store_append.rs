// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore append/append_batch methods, split from block_store.rs.
use super::*;
use super::record::sha256_bytes;

impl BlockStore {
    /// Force durability for writes made under relaxed (bulk) mode: fsync the
    /// active slab's data and persist the extent manifest once. No-op when
    /// nothing was deferred (e.g. the live per-append-fsync path).
    pub fn sync_durable(&self) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if !inner.relaxed_dirty {
            return Ok(());
        }
        let path = slab_path(&inner.root, inner.block_slab_id);
        if let Ok(file) = OpenOptions::new().append(true).open(&path) {
            file.sync_data()?;
        }
        inner.persist_slab_manifest_counted()?;
        inner.relaxed_dirty = false;
        Ok(())
    }

    pub fn append(&self, bytes: &[u8]) -> Result<ElementEntry, BlockStoreError> {
        self.append_with_object_id(bytes, None)
    }

    pub fn append_with_object_id(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
    ) -> Result<ElementEntry, BlockStoreError> {
        self.append_with_block_metadata(bytes, object_id, None)
    }

    pub fn append_with_block_metadata(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
    ) -> Result<ElementEntry, BlockStoreError> {
        self.append_block_of_object(bytes, object_id, routing_bucket, 0)
    }

