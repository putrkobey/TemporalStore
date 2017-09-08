// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Slab id / address helpers + delayed-destroy slab scanning, extracted from block_store.rs.

use super::*;
use std::path::Path;

pub(crate) fn block_slab_utility_score(below_retention_floor: bool, is_current: bool, is_live: bool) -> u64 {
    if is_current || is_live {
        100
    } else if below_retention_floor {
        0
    } else {
        50
    }
}

/// Rename one slab into quarantine, WITHOUT making the rename durable.
///
/// The caller owes a [`sync_delayed_destroy_dirs`] before it writes anything that asserts the
/// slab is quarantined -- read that function for why the two are split, and for what the split
/// does and does not change about a crash.
///
/// There is deliberately no rename-and-fsync spelling beside this one. A single-slab helper is
/// how the per-slab fsync got there: every caller is a loop, and one that is handed the fused
/// version fsyncs the same two directories once per iteration without anyone noticing.
pub(crate) fn move_slab_to_delayed_destroy_unsynced(
    root: &Path,
    block_slab_id: u64,
) -> Result<(), BlockStoreError> {
    let source = slab_path(root, block_slab_id);
    let trash_dir = delayed_destroy_dir(root);
    std::fs::create_dir_all(&trash_dir)?;
    let destination = delayed_destroy_path(root, block_slab_id);
    std::fs::rename(&source, &destination)?;
    Ok(())
}

/// fsync the two directories a quarantine rename moves between: the store root and the trash
/// directory.
///
/// ONE CALL MAKES EVERY RENAME IN THE ROUND DURABLE, NOT JUST THE LAST ONE. `fsync` on a
/// directory commits that directory's pending entry changes -- all of them, not the most recent
