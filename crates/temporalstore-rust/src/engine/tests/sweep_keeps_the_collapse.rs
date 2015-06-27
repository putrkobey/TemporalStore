// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A WHOLE COMPACTION SWEEP LEAVES THE COLLAPSED ENTRIES COLLAPSED.
//!
//! # THE HOLE THIS CLOSES
//!
//! The projection emits one entry per page under the gate, and that was measured. But it was
//! measured by folding first and turning the gate on afterwards, then re-deriving by hand. The
//! case that was NOT covered is the one that matters operationally: the gate on for the **whole
//! sweep**.
//!
//! It matters because `compact_shard_blocks` calls `rebuild_bucket_first_index` TWICE -- once on
//! the partial-failure commit path and once at the end of a successful round -- and that is the
//! fight predicted when this series was planned: compaction folds the pages, then the sweep
//! re-derives the index, so a filing change made anywhere else is undone before the sweep returns.
//!
//! The prediction after reading those two call sites is that nothing re-explodes, because both are
//! bare calls into the same projection the gate switches. **A prediction is not an observation**,
//! and "appears to be resolved" is how a stale premise starts, so this runs the real sweep with the
//! gate set before the first write and counts what survives it.
//!
//! # THE FLOORS, AND WHAT THEY FLOOR
//!
//! On EACH arm, because one arm reaching the sweep while the other does not is itself the
//! broken-arm case and a single shared floor cannot see it. And they floor the **reaching of the
//! path** -- that a fold actually happened and that pages were walked -- never the **value of the
//! result**, because the result under test may legitimately be a small number and a floor on it
//! would forbid the true answer.
//!
//! Each arm gets its own engine, so neither can answer out of the other's state.

#![allow(clippy::all)]
use super::*;

const MEMBERS: usize = 40;
const VALUE_WIDTH: usize = 24;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
