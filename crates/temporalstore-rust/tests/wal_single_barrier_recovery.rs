// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WAL-replay recovery under the single write-path durability barrier -- now the DEFAULT (only the
//! WAL takes a synchronous fdatasync per write; the data-page fdatasync and the served-index delta
//! fdatasync are both deferred, and recovery is base-only). After a crash, EVERY acked write must
//! be reconstructed -- the WAL is self-sufficient (a record states its RESULTS: an outcome item
//! naming the page's address and, when no page backs it, carrying the bytes themselves), so even if
//! the deferred-fsync index-log tail AND, as a stronger stress, the pages are lost to a simulated
//! power cut, replay rebuilds every key. A final phase exercises the TS_WAL_LEGACY_RECOVERY escape
//! hatch (legacy multi-barrier write + delta-fold recovery) so the fallback stays covered. Each
//! phase runs in its own subprocess so any mode env var never leaks into the rest of the suite.
//!
//! # The hole this header used to describe is FIXED
//!
//! This header used to open "the premise above no longer holds, and five of these fail because of
//! it", and described a default-path hole in which every acked write was unrecoverable after a
//! power cut. It then told the reader the CI step carried `continue-on-error: true`, which made it
//! read as a suppressed data-loss bug sitting on main.
//!
//! IT IS FIXED. Measured 2026-09-12 on this file, with no mode env set:
//!
//! ```text
//! test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
//! ```
//!
//! including `wal_is_self_sufficient_when_all_non_wal_state_is_wiped`, which is the strongest case
//! in the file -- every non-WAL byte removed, replay rebuilding each acked key. A passing run of
//! that test IS the premise the old header said had stopped being true.
//!
//! What closed it: `WalOutcomeItem` carries a `value` beside its `address` (#380, "a record can now
//! say what a write DID, and a shard can be rebuilt from that alone"). The old analysis was right
//! that an outcome naming only an address states nothing recoverable when the page write is
//! deferred and then lost -- the answer was to let the outcome carry the bytes, which is what the
//! design being followed does with a `value` beside its `page` and a `meta_log` flag to tell them
//! apart.
//!
//! The CI note was accurate and remains so, but it is about the BASELINE, not about this file:
//! `continue-on-error` is set on the whole suite step while a small number of pre-existing lib
//! failures stand, and the workflow says to flip it off once the baseline is green. None of those
//! failures is here.
//!
//! Left as a warning rather than deleted, because the failure mode it describes is real whenever
//! an outcome can name a page that is not yet durable. If a future change makes outcomes
//! address-only again on the synchronous path, this is the file that will catch it, and
//! `examples/wal_scan_probe.rs` prints exactly what a record is carrying:
//!
//! ```text
//! carrying a COMMAND to re-run / carrying OUTCOMES / of those items, carrying a VALUE
//! ```
//!
//! A run where outcomes carry no value and no command is the shape of the old bug.

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_wal_single_barrier_crash_harness")
}

// Default-path helpers (single-barrier + group-commit are the DEFAULT, so no mode env is set).
// These focus on losing the deferred served-index delta log (its `drop-indexlog` kill point).
fn populate(root: &str, keys: &str, flush_at: Option<&str>) {
    let mut cmd = Command::new(bin());
    cmd.args(["--mode", "populate", "--root", root, "--keys", keys]);
    if let Some(f) = flush_at {
        cmd.args(["--flush-at", f]);
    }
    let out = cmd.output().expect("populate should run");
    assert!(
        !out.status.success(),
        "populate must end in an abrupt abort (crash simulation)"
    );
}

fn powerloss(root: &str, scope: &str) {
    let out = Command::new(bin())
        .args(["--mode", "powerloss", "--root", root, "--scope", scope])
        .output()
        .expect("powerloss should run");
    assert!(
        out.status.success(),
        "powerloss failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
