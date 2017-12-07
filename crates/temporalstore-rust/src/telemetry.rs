// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Process-wide structured-logging (tracing) initialization for the TemporalStore
//! serving daemons.
//!
//! [`init`] installs a `tracing_subscriber` fmt subscriber whose level filter is
//! read from the `RUST_LOG` environment variable, falling back to `info` when the
//! variable is unset or unparseable. It is idempotent and safe to call from every
//! daemon `main`: the subscriber is installed at most once, and a pre-existing
//! global subscriber is left untouched.
//!
//! Logging cost on the serving hot path is near zero at the default `info` level.
//! `debug!`/`trace!` events compile to a cheap static level check that short-
//! circuits before any argument formatting, so per-request instrumentation stays
//! disabled unless `RUST_LOG` explicitly enables it.

use std::sync::Once;

use tracing_subscriber::EnvFilter;
