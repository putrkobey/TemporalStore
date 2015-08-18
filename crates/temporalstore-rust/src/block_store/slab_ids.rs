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
/// -- so N renames followed by one fsync of each directory leave exactly the same entries on disk
/// as N rename/fsync pairs do. Every rename in the round moves between these same two
/// directories, which is the whole reason the per-slab version was redundant: it re-synced the
/// same two inodes N times to commit one entry each time.
///
/// WHAT THE SPLIT DOES CHANGE is the size of the window in which a crash can leave the batch
/// half-applied -- and the store already had to survive that window, because the collector
/// persists the slab manifest ONCE, after its loop. A crash mid-loop therefore already produced
/// files sitting in quarantine that the manifest never learned about, whether or not each rename
/// had been fsynced; `purge_delayed_destroy_slabs_selected` handles exactly that case, falling
/// back to the file's mtime for a slab with no descriptor stamp. Widening the window does not
/// introduce a state that was not already reachable.
///
/// THE ORDERING THAT MAKES THE RENAME DURABLE IS PRESERVED, and it is the reason this is called
/// where it is: every rename reaches the disk BEFORE the manifest that claims those slabs are
/// quarantined. Move this call below `persist_slab_manifest` and a crash between the two leaves a
/// manifest asserting a quarantine the directory does not show -- a slab recorded as
/// `DelayedDestroy` while its file is still at its old name, which is the one ordering this code
/// must not lose.
pub(crate) fn sync_delayed_destroy_dirs(root: &Path) -> Result<(), BlockStoreError> {
    sync_dir(root)?;
    sync_dir(&delayed_destroy_dir(root))?;
    Ok(())
}

/// Put a quarantined slab back where readers look for it.
///
/// The inverse of [`move_slab_to_delayed_destroy`], and the reason it has to exist: phase 1 of
/// the delayed destroy RENAMES the file out of the store, so by the time the grace window is
/// running the slab is already unreachable by path. A reader that still needs it cannot be
/// served by waiting -- it can only be served by moving the file back.
///
/// Returns `false` and moves NOTHING when a file already sits at the destination. Restoring over
/// it would replace a slab the store is currently serving with an older one of the same id, so
/// the conservative answer is to leave the quarantined copy alone: the caller keeps it in
/// quarantine rather than destroying it.
///
/// Like [`move_slab_to_delayed_destroy_unsynced`], this leaves the rename UNSYNCED. A restore
/// travels between the same two directories as a quarantine, in the other direction, so the same
/// [`sync_delayed_destroy_dirs`] after the loop commits it -- and the purge's loop is the only
/// caller, so the fsync it owes is one per round rather than one per restored slab.
pub(crate) fn restore_slab_from_delayed_destroy_unsynced(
    root: &Path,
    block_slab_id: u64,
    quarantined_path: &Path,
