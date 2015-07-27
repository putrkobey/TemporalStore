// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::atomic::AtomicU64;

pub(super) const FEATURE_ADD_HARD_MAX_SIZE: usize = 100_000;
pub(super) const FEATURE_BLOCK_MAGIC: &[u8] = b"TSFPG1\n";

/// The byte format. Distinct from the JSON magic so a page written before it still identifies
/// itself, rather than being read as a length that happens to parse.
pub(super) const FEATURE_BLOCK_BINARY_MAGIC: &[u8] = b"TSFPB1\n";
#[cfg(test)]
pub(super) const TIMESTAMPED_KV_BLOCK_TARGET_BYTES: usize =
    crate::storage_config::DEFAULT_CONTEXT_BLOCK_TARGET_BYTES;
pub(super) const CONTEXT_TIMELINE_FANOUT: u64 = 1024 * 1024;
pub(super) const CONTEXT_DEFAULT_LIMIT: usize = 100;
pub(super) const CONTEXT_MAX_LIMIT: usize = 1000;
pub(super) const CONTEXT_MAX_FILTER_VALUES: usize = 32;
pub(super) const CONTEXT_MAX_RELATED_NODE_HASHES: usize = 64;
pub(super) const CONTEXT_MAX_AUDIT_REFS: usize = 512;
pub(super) const CONTEXT_MAX_PROPAGATE_DEPTH: u32 = 8;
pub(super) const CONTEXT_MAX_INDEX_NAME_BYTES: usize = 64;
pub(super) const CONTEXT_MAX_CANONICAL_NAME_BYTES: usize = 512;
pub(super) const CONTEXT_MAX_ENTITY_NAME_BYTES: usize = 512;
pub(super) const CONTEXT_MAX_ENTITY_VALUE_BYTES: usize = 4096;
