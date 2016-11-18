// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot walks, reads, copies and rebuilds -- counted inside the primitives that do it.
//!
//! A snapshot spans four modules (`raft`, `raft::cluster_snapshot`, `engine`, `block_store`), so a
//! counter bumped at the call sites in `cluster_snapshot.rs` would have measured the call sites
//! rather than the work. Every counter here is bumped INSIDE the primitive -- inside
//! `BlockStore::read_slab`, inside `BlockStore::install_slab`, inside `collect_live_block_slab_ids`,
//! inside `Clone for RaftSnapshotStateImage` -- so a new call site cannot reach the work without
//! being counted, and no list of call sites has to be maintained.
//!
//! Thread-local, for the reason `wal::WAL_SEGMENT_LISTINGS` is: the suite runs tests in parallel
//! by default and a great many of them touch a block store, so a process-global counter would
//! report their work as this one's.
//!
//! `GuardMark` is armed for exactly the span in which this thread holds the raft cluster WRITE
//! guard and disarms in `Drop`, so an early return cannot leave it set and charge later work to a
//! guard that has been released. That guard is the one every `propose` needs the other half of, so
//! "under guard" here means "the cluster could not accept a write".

#![cfg(test)]

use std::cell::Cell;

thread_local! {
    static GUARD_DEPTH: Cell<u32> = const { Cell::new(0) };

    static IMAGE_BUILDS: Cell<u64> = const { Cell::new(0) };

    static LIVE_SLAB_SCANS: Cell<u64> = const { Cell::new(0) };
    static LIVE_SLAB_SCAN_ADDRESSES: Cell<u64> = const { Cell::new(0) };

    static SLAB_DIR_LISTINGS: Cell<u64> = const { Cell::new(0) };

    static SLAB_READS: Cell<u64> = const { Cell::new(0) };
    static SLAB_READ_BYTES: Cell<u64> = const { Cell::new(0) };

    static SLAB_INSTALLS: Cell<u64> = const { Cell::new(0) };
    static SLAB_INSTALL_BYTES: Cell<u64> = const { Cell::new(0) };
    static SLAB_INSTALL_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static IMAGE_CLONES: Cell<u64> = const { Cell::new(0) };
    static IMAGE_CLONE_BYTES: Cell<u64> = const { Cell::new(0) };
    static IMAGE_CLONE_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static ENGINE_REBUILDS: Cell<u64> = const { Cell::new(0) };
    static ENGINE_REBUILDS_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static ENGINE_PUBLISHES: Cell<u64> = const { Cell::new(0) };

    static WINDOW_ARMED: Cell<Option<InstallWindow>> = const { Cell::new(None) };
    static WINDOW_FIRINGS: Cell<u64> = const { Cell::new(0) };
}

/// An interleaving a test asks to happen in the window where the install loop builds its engines
/// holding NO cluster guard.
///
/// A test cannot get inside that window by racing for it -- the window is microseconds wide and a
/// lost race reads exactly like a check that fired -- so the interleaving is REQUESTED here and
/// performed by the install path itself, once, at the point where it holds nothing. The arm being
/// attacked can only fail in one direction, so it is constructed rather than waited for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallWindow {
    /// Apply one more entry, so every node's commit and applied indexes pass the snapshot's.
    /// The snapshot is now OLDER than the nodes it was about to be published into.
    ApplyOneMoreEntry,
    /// Bring a node back, so it qualifies for the snapshot without an engine having been built
    /// for it.
    ReviveNode(u64),
}

/// Ask the next install window on this thread to perform `window`. Consumed by the first window
/// that reaches it, so one arming is one interleaving.
pub fn arm_install_window(window: InstallWindow) {
    WINDOW_ARMED.with(|armed| armed.set(Some(window)));
}

/// Taken by the install path at the point where it holds no cluster guard.
pub fn take_install_window() -> Option<InstallWindow> {
    WINDOW_ARMED.with(|armed| armed.take()).inspect(|_| {
        bump(&WINDOW_FIRINGS, 1);
    })
}

/// How many armed interleavings were actually performed. A test that armed one and reads zero
/// here did not test what it thinks it tested.
pub fn install_window_firings() -> u64 {
    WINDOW_FIRINGS.with(Cell::get)
}

/// Armed while this thread holds the raft cluster write guard.
pub struct GuardMark;

impl GuardMark {
    pub fn new() -> Self {
        GUARD_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for GuardMark {
    fn drop(&mut self) {
        GUARD_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

fn under_guard() -> bool {
    GUARD_DEPTH.with(|depth| depth.get() > 0)
}

fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    counter.with(|value| value.set(value.get().saturating_add(by)));
}
