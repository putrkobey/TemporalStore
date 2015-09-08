// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE AUTHORITY CHECK COSTS ON THE EXECUTE PATH, GATE OFF AGAINST GATE ON.
//!
//! # THE REGRESSION THIS IS LOOKING FOR
//!
//! `promote_model_maps_to_bucket_index_authority` runs on the request path and asks, per live
//! model-map block, whether the bucket index names it. `contains_object_block_address` requires
//! `page.component.as_deref() == component`, so a projection that files entries with no component
//! while the check asks about a NAMED one would answer "missing" for every element -- and a single
//! missing block rebuilds the whole shard index. **Per execute.**
//!
//! That is a latency regression, not a failing test. No footprint guard would see it, and neither
//! would a correctness one: the index it rebuilds is correct. So it is measured here, with the
//! engine's own counters, before anything is asserted about it.
//!
//! # WHY IT MIGHT ALREADY BE CONSISTENT, WHICH IS THE THING TO FIND OUT
//!
//! The check does not have its own walk: it consumes `visit_model_live_blocks`, the same
//! projection the gate already switches. So under the gate both sides of the comparison may
//! already be speaking about pages rather than elements -- the walk emitting `None` and the index
//! holding `None` -- in which case this step needs no code change at all. Printed rather than
//! predicted.
//!
//! # WHERE IT RUNS, WHICH IS NOT WHERE I FIRST LOOKED
//!
//! It is NOT on the ordinary execute path. Its four production callers are `load_shard`, the single
//! post-WAL-replay fold -- deliberately once, which is "what turns an O(n^2) reload into O(n)" --
//! one further lifecycle path, and the stream-batch arm, which is guarded by a latch so it runs per
//! batch only until a freshly loaded shard has paid one scan.
//!
//! A first fixture here ran twenty ordinary writes and the FLOOR caught it: zero checks in both
//! arms. Without that floor the comparison would have passed as `0 <= 0` and reported "no
//! regression" over a path nothing reached. So the check is driven directly, on an engine-built
//! shard, which is the state it sees on load.
//!
//! # THE INSTRUMENT IS THE ENGINE'S OWN, AND ITS CONTRACT IS FOLLOWED
//!
//! `promote_model_map_check_counts` reports checks run, promotions that REBUILT, and blocks the
//! checks walked. Its own doc states the contract: reset immediately before the call being
//! measured, and read in a single-threaded suite. `PAGES` is there because a walk that visited
//! nothing is cheap for the wrong reason and would pass a cheapness assertion anyway.
//!
//! Each arm gets its OWN engine, so neither can answer out of the other's state -- the discipline
//! learned from a listing measurement that read a full answer out of a response cache when the
//! truth was zero.

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
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
