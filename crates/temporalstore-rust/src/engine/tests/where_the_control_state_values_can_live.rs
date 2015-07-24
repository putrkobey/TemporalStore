// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN THE THREE CONTROL-STATE VALUE MAPS ENTER THE CONSOLIDATED MAP, AND WHAT IS THE FLOOR?
//!
//! `control_state_changes` and `control_state_selection` are two of the twelve maps
//! `apply_key_state_field` folds (three of thirteen when this was written, before the two change
//! maps became one). The consolidated map stores an `ObjectSlot`
//! -- a record plus an element array, both of which describe WHERE PAGES LIVE. These three hold
//! domain values. This file pins what is true of them, so the floor on the goal is recorded with
//! its evidence rather than asserted in a report.
//!
//! # THE FOUR ANSWERS, EACH DRIVEN
//!
//! 1. DURABLE, NOT DERIVED. All three carry `#[serde(default)]` and no `skip`, nothing on the
//!    load path clears or rebuilds them, and [`all_three_maps_survive_a_reload`] drives the round
//!    trip rather than reading the attribute. The contrast is `expiry_by_deadline`
//!    (`#[serde(skip)]`, rebuilt from `expires_at_ms`) and `control_state_rollups`
//!    (`#[serde(skip)]`, rebuilt lazily) -- which is almost certainly where an earlier census got
//!    "two of the three are `#[serde(skip)]`" from. They are NEIGHBOURS of these three in the
//!    struct, with the same `control_state_` prefix, and they are the skipped ones.
//!
//! 2. ALL THREE HAVE KEY-CREATING PRODUCTION WRITERS, unlike `sequences` (none, deleted) and
//!    `context_nodes` (none key-creating). `control_state_changes` is written unconditionally by
//!    `hll::record_change`; a bucket is converted to its SKETCH representation by the same
//!    function only when the `control_distinct_sketch` gate is on, which
//!    [`the_sketch_map_has_no_writer_until_its_flag_is_on`] drives in both positions;
//!    `control_state_selection` is written from the command path.
//!
//! 3. THE TWO CHANGE MAPS ARE ONE LOGICAL MAP. `record_change` keeps them mutually exclusive per
//!    `(key, bucket)` -- on conversion it REMOVES the bucket from the exact map and inserts it
//!    into the sketch map -- which [`a_bucket_holds_exactly_one_representation`] drives across
//!    the conversion. That was the one consolidation available here that needed no change to
//!    `ObjectSlot`, and it has been taken.
//!
//! 4. NONE OF THEM HAS AN OUTER KIND KEY.
//!    [`none_of_these_maps_has_a_stored_model_kind`] asks `StoredModelKind::from_stored_name`
//!    rather than reading a list. `control_state` has a variant because its data is PAGES; these
//!    three have none because they have no pages at all.
//!
//! # THE FLOOR, AND THE ONE STEP OF IT THAT HAS SINCE BEEN TAKEN
//!
//! This module was written when `apply_key_state_field` folded THIRTEEN maps. NINE of them have
//! `ElementEntry` values -- a page address, which is what a slot exists to hold: `features`,
//! `context_nodes`, `context_events`, `context_indexes`, `context_audits`, `context_entities`,
//! `context_children`, `context_summaries`, `context_compressions`. FOUR did not, and the fourth
//! was the one no list of the problem had mentioned -- `expires_at_ms`, whose value is a deadline.
//! So 13 - 4 = 9, and 13 - 3 = 10 would have been wrong: the floor's NUMBER was right and its
//! stated reason was one map short.
//!
//! IT IS NOW NINE OF TWELVE, because the recommendation this module made has been taken.
//! `control_state_change_sketch` and `control_state_changes` were one logical map -- keyed
//! identically, kept mutually exclusive per `(key, bucket)` by `record_change` -- and they are now
//! one map whose bucket value is `hll::DistinctBucket`. The three that remain outside the slot are:
//!
//! | map | value | a page address? |
//! |---|---|---|
//! | `control_state_changes` | `BTreeMap<u64, DistinctBucket>` | no -- members or a sketch |
//! | `control_state_selection` | `ControlStateSelectionValue` | no -- time, bytes, kind |
//! | `expires_at_ms` | `u64` | no -- a deadline |
//!
//! So the count of maps moved and the count outside the slot moved with it, which is the whole of
//! what that consolidation bought. The nine did not change.

#![allow(clippy::all)]
use super::*;

use crate::engine::storage_bucket_internals::StoredModelKind;

const KEY: &str = "ctl:cap:tenant-7";
const BUCKET_MS: u64 = 1_000;
const HOUR_MS: u64 = 3_600_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

/// What the shard holds for `KEY`: exact buckets, sketched buckets, and whether a selection is
/// present.
///
/// THE FIRST TWO USED TO BE TWO MAPS AND ARE NOW TWO VARIANTS. The shape of this helper is
/// unchanged on purpose -- every assertion below is written against the same triple -- but where
/// it once read a length from each of two maps it now partitions one map by variant. A bucket can
/// only be counted once either way, which is the point.
fn held(engine: &TemporalEngine) -> (usize, usize, bool) {
    use crate::engine::hll::DistinctBucket;
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let (exact, sketched) = shard
        .control_state_changes
        .get(KEY)
        .map_or((0, 0), |buckets| {
            buckets.values().fold((0, 0), |(exact, sketched), bucket| match bucket {
                DistinctBucket::Exact(_) => (exact + 1, sketched),
                DistinctBucket::Sketch(_) => (exact, sketched + 1),
            })
        });
    (exact, sketched, shard.control_state_selection.contains_key(KEY))
}

/// Turn the distinct-sketch gate on through the real config path, and confirm the shard field the
/// writer reads actually moved -- the plumbing is `execute_on_shard`'s per-execute assignment, so
/// setting the config is not by itself evidence that `record_change` will see it.
fn enable_distinct_sketch(engine: &TemporalEngine) {
    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_distinct_sketch".to_string(), "on".to_string());
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config
            })
            .ok,
        "setting the distinct-sketch gate failed"
    );
    // One execute, so the per-execute assignment runs.
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateChangeAdd {
            key: KEY.to_string(),
            timestamp_ms: BUCKET_MS,
            value: b"seed".to_vec(),
            precision_ms: None,
            ttl_ms: None,
        },
    });
    assert!(answer.status.ok, "the seed write failed: {}", answer.status.message);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(
        shard.control_distinct_sketch,
        "the config gate is on but the shard field `record_change` reads is still false, so the \
         per-execute assignment in `execute_on_shard` is not reaching it and the arms below would \
         be measuring the default rather than the gate"
    );
}

/// Drive `record_change` past the conversion threshold on one bucket.
fn fill_past_the_threshold(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    for index in 0..=crate::engine::hll::DISTINCT_SKETCH_THRESHOLD {
        crate::engine::hll::record_change(
            shard,
            KEY,
            BUCKET_MS,
            format!("member-{index}").into_bytes(),
        );
    }
}

/// 1. DURABLE, NOT DERIVED -- driven as a round trip, not read off the attribute.
#[test]
fn all_three_maps_survive_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(1);

    // The sketch map only gets a key with the gate on, so turn it on first.
    enable_distinct_sketch(&engine);
    fill_past_the_threshold(&engine);

    let selection = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateSelectionSet {
            key: KEY.to_string(),
            value: b"chosen".to_vec(),
            occur_time_ms: BUCKET_MS,
            ttl_ms: HOUR_MS,
            selection_type: crate::types::ControlStateSelectionType::Last,
        },
    });
    assert!(
