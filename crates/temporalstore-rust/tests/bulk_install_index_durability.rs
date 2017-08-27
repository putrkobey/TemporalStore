// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Regression: manifest install-on-load must durably materialize the restored index even
//! under bulk-ingest mode.
//!
//! `persist_index_bytes` is deliberately a no-op under `MATRIXARK_BULK_INGEST` to turn the
//! per-record served-index rewrite from O(n^2) into one write per batch. But manifest
//! *install* is a one-shot recovery step, not the hot path: if it skips the durable index
//! write, `load_index()` keeps reading the stale pre-manifest index while the advanced
//! replay watermark (the manifest's wal_sequence) suppresses replay of the very records
//! the manifest embeds -- records that were already WAL-GC'd and now live ONLY in the
//! manifest. The net effect is silent data loss on a bulk-mode recovering load. Install must
//! therefore bypass the bulk gate (`persist_index_bytes_durable`).
//!
//! This test runs in its own integration-test process, so toggling the process-wide
//! `MATRIXARK_BULK_INGEST` env var cannot race the (single-process) library unit tests.

use std::path::PathBuf;

use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};

