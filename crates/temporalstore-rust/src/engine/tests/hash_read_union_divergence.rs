// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// THE HASH WHOLE-OBJECT READ SERVES BOTH SOURCES, AND SAYS SO WHEN THEY DISAGREE
// =================================================================================================
//
//! WHY THIS MODULE EXISTS, AND WHAT IT REPLACES.
//!
//! #2078 set out to prove that the durable container `shard.hashes` holds every field the fold
//! installs, by comparing it against the page index after a reload. IT DOES NOT PROVE THAT. The
//! LOAD PATH BUILDS A HASH FIELD MAP BY WALKING THE PAGE INDEX AND MERGES IT INTO THE CONTAINER,
//! from BOTH of the two functions in `storage_bucket_internals` that do it:
//! `rebuild_unserialized_model_maps_from_bucket_index` and
//! `reconcile_secondary_views_from_bucket_index`, each through
//! `RecordedHashContainer::reconcile_from_durable`. So after a
//! reload the container names everything the index names BY CONSTRUCTION, and the fixture compares
//! the index against a structure derived from it. Disabling the container carry outright leaves
//! that test passing. Measured while establishing this: `2 checks, 0 REBUILDS, 6 pages walked`, so
//! the reverse mechanism -- the index being rebuilt from the map -- did not fire either and a
//! decoded index does not rescue the comparison.
//!
//! THE CITATION THIS MODULE WAS FIRST WRITTEN WITH WAS WRONG TWICE, and is recorded here because
//! the wrong one is the plausible one. It named `reconcile_secondary_views_from_bucket_index`
//! alone, and it named the merge `shard.hashes = fill_absent_elements(hashes, persisted, ..)`.
//! There is no such expression: `fill_absent_elements` is assigned to `shard.lists`, `shard.zsets`
//! and `shard.sets` (`:5195`, `:5204`, `:5213`) and never to `hashes`, which takes the merge
//! through `reconcile_from_durable` instead. The refutation does not depend on either detail --
//! it is stronger without them, because it holds on BOTH load paths rather than one.
//!
//! AND ONE TRAP WORTH NAMING, because grepping for the wrong expression leads straight into it.
//! `recorded_hash_container_invariant.rs` asserts that
//! `assignments("    shard.hashes = fill_absent_elements(")` is NOT empty, which reads like
//! evidence that the tree contains such an assignment. It is not: the argument is a PLANTED
//! STRING LITERAL and the assertion is a self-test on the matcher, checking it can still see a
//! multi-line assignment. A sweep of `engine/` for `shard.hashes = ` finds only comments and test
//! prose. Read a guard's matcher and its arguments, not the shape of its name.
//!
//! WHAT IS HERE INSTEAD. `Command::HashGetAll` resolves field names from the container, still
//! consults the page index, and serves the UNION; and it counts, at read time, each field the
//! INDEX named that the CONTAINER lacked. That is a run-time measurement against real traffic,
//! which is strictly stronger than a fixture in one specific way: it samples whatever route each
//! request took, including routes no fixture drives, and there is no load path between the two
//! sources to fill one from the other.
//!
//! WHAT A ZERO FROM THAT COUNTER MEANS, AND WHAT IT DOES NOT. A zero means NO DIVERGENCE ON THE
//! ROUTES THE TRAFFIC TOOK. IT IS NOT PROOF OF COMPLETENESS ON THE ROUTES IT DID NOT TAKE. An
//! object no request read is an object this counter never looked at. Saying otherwise would
//! manufacture a second #2078.
//!
//! WHAT THESE TESTS DO, IN ORDER, AND WHY THE ORDER IS THE POINT. A divergence counter that has
//! never been seen to move is not evidence of anything -- it reads identically to a counter whose
//! increment was deleted. So the arms are: a NEGATIVE CONTROL on a store whose two sources agree
//! (the counter must not move), then a PLANT that removes one field from the container alone (the
//! counter must move, and the union must still serve that field correctly), then a plant in the
//! OPPOSITE direction that takes a field off the index alone (the union must serve it from the
//! container, and it must NOT be counted, because that direction loses no read).

#![allow(clippy::all)]
use super::*;

const PLANTED_KEY: &str = "tenant/1/union-divergence/object";

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
        table_name: "hash-read-union-divergence".to_string(),
        shard_uri: "local://hash-read-union-divergence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: crate::DEFAULT_END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
