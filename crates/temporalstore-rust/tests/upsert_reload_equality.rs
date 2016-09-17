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
