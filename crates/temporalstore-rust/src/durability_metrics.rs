// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Counts durability barriers by the call site that takes them.
//!
//! A write's latency is roughly `barriers x fsync`, so a total barrier count per write says how
//! much there is to win but not where to go looking. Attributing them by reasoning about the code
//! has produced confident wrong answers here more than once -- two changes aimed at a suspected
//! site passed their tests and then moved nothing. Each site reports itself instead, so the split
//! is measured rather than argued.
//!
//! The counters are process-wide and monotonic. A mutex per barrier is free next to an fsync
//! (microseconds against milliseconds), so this stays on rather than hiding behind a build flag
//! that would be off exactly when someone needs the numbers.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

fn counters() -> &'static Mutex<BTreeMap<&'static str, u64>> {
    static COUNTERS: OnceLock<Mutex<BTreeMap<&'static str, u64>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Record one durability barrier taken at `site`.
