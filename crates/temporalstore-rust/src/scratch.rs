// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scratch directories: minting, ownership, and reclamation.
//!
//! # The leak this closes
//!
//! Every `Default` store constructor mints a unique directory under the system temp dir, and
//! nothing ever removed one: not the store (dropping it left the directory behind), and not
//! the next boot (the name is unique per process, so no later run ever reuses it). A box
//! that builds engines often accumulates these without bound — tens of thousands were found
//! on one shared machine.
//!
//! # Ownership
//!
//! A store built by `Default` OWNS its directory: it holds an [`ScratchDirGuard`] whose drop
//! removes the tree. The stores are `Clone` with shared inner state, so the guard is held as
//! an `Arc` inside that shared state — the directory lives exactly as long as the last
//! clone. A store built on a caller-supplied directory never holds a guard, so a real data
//! directory is never deleted.
//!
//! # Reclamation
//!
//! Drop cannot run on a killed process, so minting any scratch path also arms a
//! once-per-process background sweep: the minted name embeds the owning process id, and the
//! sweep removes `temporalstore-rust-*` directories whose owner is no longer alive. A live
//! owner's directories are always kept, as is anything whose name does not parse.

use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Mint a unique path under the system temp dir. The name embeds the process id, which is
/// what lets [`sweep_dead_scratch_dirs`] tell an abandoned directory (owner exited) from one
/// a live process is still using. Minting also arms the once-per-process background sweep.
pub(crate) fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    arm_background_sweep();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

/// A scratch directory the minting store owns: removed from disk when the guard drops.
/// Held as an `Arc` inside the owning store's shared inner state, so the directory outlives
/// every clone and dies with the last one.
#[derive(Debug)]
pub(crate) struct ScratchDirGuard {
    path: PathBuf,
}

impl ScratchDirGuard {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Mint a scratch directory together with its owning guard.
pub(crate) fn owned_scratch_dir(kind: &str) -> Arc<ScratchDirGuard> {
    Arc::new(ScratchDirGuard {
        path: unique_temp_path(kind),
    })
}

fn arm_background_sweep() {
    // Always armed. `TS_SCRATCH_SWEEP` used to be able to skip it. Nothing selected the off
    // position, and what it bought was a directory belonging to an exited process left on
    // disk -- which the sweep already reports before removing, and which only a live process
    // is ever kept for.
    static ARMED: std::sync::Once = std::sync::Once::new();
    ARMED.call_once(|| {
        std::thread::spawn(|| {
            let report = sweep_dead_scratch_dirs(&std::env::temp_dir());
            if report.removed > 0 || report.failed > 0 {
                eprintln!(
                    "temporalstore_scratch_sweep removed={} failed={} kept_live={}",
                    report.removed, report.failed, report.kept_live
                );
            }
        });
    });
}

#[derive(Debug, Default)]
struct SweepReport {
