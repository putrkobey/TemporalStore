// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Scale reload-equality for upsert index-log delta records. `TS_INDEXLOG_UPSERT_DELTAS`
//! names the feature and no longer gates it -- nothing reads that variable.
//!
//! A ~45K-record production store built through batch-committing ingest reconstructed
//! EMPTY on reload while the small-store proofs passed, so this drives the same shape at
//! a scale the unit tests never reach: thousands of hash batches (every batch emits one
//! upsert delta record), a durably-logged config (config-log present), threshold dumps
//! mid-stream, SIGKILL-grade restarts between generations, and reload-equality asserted
//! over the full served view -- under BOTH recovery modes (default base-only + WAL
//! replay, and the TS_WAL_LEGACY_RECOVERY delta-fold path that the flat-log format needs
//! at scale). Every phase runs in its own subprocess so the upsert-emission gate and the
//! recovery-mode env never leak into the rest of the suite.

use std::path::Path;
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_upsert_reload_harness")
}

const GEN1_BATCHES: u64 = 2600;
const GEN2_BATCHES: u64 = 400;

fn run(mode: &str, root: &Path, batches: u64, extend: Option<u64>, legacy_recovery: bool) -> (bool, String) {
    let mut cmd = Command::new(bin());
    cmd.args([
        "--mode",
        mode,
        "--root",
        root.to_str().unwrap(),
        "--batches",
        &batches.to_string(),
    ]);
    if let Some(extend) = extend {
        cmd.args(["--extend", &extend.to_string()]);
    }
    // No emission gate is set here. `TS_INDEXLOG_UPSERT_DELTAS` used to be one; nothing reads
    // it now, so setting it selected nothing and this ran the default path either way. The
    // variable below is read, which is why it is still passed.
    if legacy_recovery {
        cmd.env("TS_WAL_LEGACY_RECOVERY", "1");
    }
    let out = cmd.output().expect("harness should run");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr);
    (
        out.status.success(),
        format!("stdout={stdout} stderr={stderr}"),
    )
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
