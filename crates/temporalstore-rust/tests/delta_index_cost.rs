// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A/B micro-measurement of the per-write served-index cost.
//!
//! Reads the mode from the process env (`MATRIXARK_DELTA_SERVED_INDEX`) rather than setting
//! it, so the harness can run it twice (once OFF, once ON) as two separate processes with no
//! env race. It writes an increasing number of keys and times fixed-size write batches at a
//! growing store size, then reports the per-write wall time. On the default (OFF) path each
//! write re-serializes and atomically rewrites the whole `shard-{id}.index.json`, so the
//! per-write time climbs with the store size (O(store)); on the delta (ON) path the base
//! rewrite is deferred, so it stays flat (O(delta)). Run with `--nocapture` to see the report.

use std::path::PathBuf;
use std::time::Instant;

use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};
