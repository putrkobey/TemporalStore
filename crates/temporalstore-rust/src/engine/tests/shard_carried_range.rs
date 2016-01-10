// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TWO WRITERS THAT FILE A PAGE, AND THE RANGE THEY COULD NOT REACH.
//!
//! mx#1949 enumerated eight production sites that supply a routing range as a literal, fixed five,
//! found one right as written, and DECLINED TWO with a price. The two it declined are the only
//! ones of the eight that decide WHERE A PAGE GOES:
//!
//! ```text
//!   upsert_bucket_index_block_inner            storage_bucket_internals.rs
//!   sync_bucket_index_object_blocks_with_mode  storage_bucket_internals.rs
//! ```
//!
//! It held the defect as a live measurement rather than a wish: `inner-000000` filed in bucket
//! **1,422,005,296** on a shard holding `0..1023`, its own bucket being 398. That measurement is
//! not deleted here. It is INVERTED -- the same fixture, the same two numbers, now asserting the
//! page is in 398 and NOT in 1,422,005,296 -- so the bucket that was wrong is still named in a
//! test that fails if the filing goes back.
//!
//! # PLACES, NOT FILTERS -- ESTABLISHED FOR EACH BEFORE ANYTHING CHANGED
//!
//! mx#1945's refutation is the thing to not re-break: `rebuild_bucket_block_ownership` FILTERS
//! over a whole-shard image carrying buckets from whatever range wrote it, so narrowing it drops
//! live data (0 of 400 surviving). Neither of these two is that.
//!
//! * `upsert_bucket_index_block_inner` runs over ONE address, supplied by its caller for this
//!   shard. The bucket it computes is the KEY it inserts under. What it REMOVES first comes from
//!   `block_refs_for` -- where the object's pages actually are -- or, when the lookup is not
//!   established, from a sweep of every bucket in the map. Neither consults the computed bucket.
//! * `sync_bucket_index_object_blocks_with_mode` runs over ONE OBJECT'S address list, likewise
//!   supplied for this shard. Its removal set is `object_block_refs(kind, object_key)`, or every
//!   bucket in the map while the lookup is being established. Again: the computed bucket is used
//!   only to INSERT.
//!
//! So narrowing them moves where an unrouted page lands and can drop nothing.
//! `narrowing_the_writers_removes_no_block_that_was_already_filed` drives that rather than reading
//! it off the source, and `a_store_written_on_one_range_still_reads_back_whole_on_the_other`
//! drives the cross-range restore in BOTH directions, because both are silent when they fail.
//!
//! # THE SHAPE, AND WHY IT IS NOT THE OTHER ONE
//!
//! Two ways to get the range to a function that never sees `&self`. Both were priced with the
//! compiler, and the numbers are in the pull request rather than here. The range is carried ON
//! THE SHARD, as three `#[serde(skip)]` fields read through `ShardState::routing_range()`.
//!
//! `ShardState` IS the serialized index, so the first question is whether that stored shape moves.
//! A `#[serde(skip)]` field is absent from the Serialize impl entirely, so it does not -- and that
//! is DRIVEN, not asserted, by `the_routing_range_field_changes_no_serialized_byte`, which
//! serializes the same shard stamped and unstamped and compares the bytes, and by the two
//! restart arms of `a_store_written_on_one_range_still_reads_back_whole_on_the_other`, which
//! write with one range and read with the other in both directions.
//!
//! The unstamped default is the WHOLE range -- exactly what these two sites passed unconditionally
//! before -- so a `ShardState` that never entered the engine is unchanged. What makes that default
//! unreachable in the engine is that there is ONE function which installs a shard and it stamps;
//! `every_shard_the_engine_installs_carries_its_routing_range` holds the install sites as a list.

use super::*;
use crate::engine::hashing::block_routing_bucket;
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;
/// The end bucket `load_shard` uses, and the one the two writers used to spell.
const WIDE_END: u32 = u32::MAX;

/// The key mx#1949 named its measurement with, and the two buckets it named.
const MEASURED_KEY: &str = "inner-000000";
/// Where `0..1023` puts `inner-000000`.
const MEASURED_NARROW_BUCKET: u32 = 398;
/// Where `0..u32::MAX` put it, on a shard that holds 0..1023.
const MEASURED_WIDE_BUCKET: u32 = 1_422_005_296;

const RECORDS: usize = 200;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "shard-carried-range".to_string(),
        shard_uri: "local://shard-carried-range/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn seed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for index in 0..count {
        let key = format!("inner-{index:06}");
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
        keys.push(key);
    }
    keys
}

fn read_back(engine: &TemporalEngine, keys: &[String]) -> usize {
    keys.iter()
        .filter(|key| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: (*key).clone(),
                },
            });
            response.status.ok
                && matches!(
                    &response.response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
        })
        .count()
}

/// An address as an index written before the routing-bucket field carried it holds one, produced
/// by the ENGINE'S OWN DECODER rather than by the setter -- mx#1942's door, spelled the same way
/// in all four files that need it.
///
/// THE SLOT IS RETIRED FROM `BlockAddressWire`, so the two shapes have swapped: an older index is
/// the one CARRYING `rs`, and a current address is the one without it. The key is added here rather
/// than removed, and the address it decodes to is the same either way.
fn as_an_older_build_wrote_it(address: &ElementEntry) -> ElementEntry {
    let mut wire = serde_json::to_value(address).expect("an address serializes to its wire shape");
    let object = wire
        .as_object_mut()
        .expect("the address wire shape is a JSON object");
    // INVERTED WITH THE SLOT'S DEPARTURE: a current address carries no `rs`, so the older shape is
    // the one with the key ADDED rather than removed. The assertion inverts with it, and it is
    // still the same guard -- it fails if this helper has stopped producing a different shape from
    // the one the engine writes today.
    assert!(
        object.remove("rs").is_none(),
        "a current address still carries an `rs` key, so this helper is not producing an older \
         shape and every count taken through it is the current shape for the wrong reason"
    );
    object.insert("rs".to_string(), serde_json::json!(513u32));
    serde_json::from_value(wire)
        .expect("the engine's decoder still accepts an address carrying the retired `rs`")
}

/// THE DOOR IS NOW A NO-OP, AND THAT IS THE POINT.
///
/// A `BlockAddress` does not hold a routing bucket, so "put every page into the state an older index
/// decodes into" and "leave every page alone" are the same operation. The helper is kept because the
/// round trip it performs is the assertion that the `rs` key on the wire is inert: it must come back
/// EQUAL, and a page that changed would mean an index written before the field left decodes to a
/// DIFFERENT address -- which is what a migration would be for.
///
/// Returns how many pages it round-tripped, so a caller can still assert the fixture was covered.
fn round_trip_every_block_through_the_wire(
    shard: &mut crate::engine::state::ShardState,
) -> usize {
    let keys: Vec<String> = shard.strings().keys().map(|key| key.to_string()).collect();
    for key in &keys {
        let before = shard.string_address(key.as_str()).expect("key present").clone();
        let older = as_an_older_build_wrote_it(&before);
        assert_eq!(
            older, before,
            "page {key} changed when its `rs` key was removed from the wire; the key is inert and \
             this round trip has to be the identity"
        );
        shard.set_string_address(key.as_str().into(), &older);
    }
    keys.len()
}

/// Every bucket that holds at least one page, with the object keys it holds, sorted.
fn bucket_contents(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, Vec<String>> {
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut contents: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let mut keys: Vec<String> = bucket
            .block_index
            .iter()
            .map(|(handle, _block)| named.at(*routing_bucket, *handle).to_string())
            .collect();
        if keys.is_empty() {
            continue;
        }
        keys.sort();
        contents.insert(*routing_bucket, keys);
    }
    contents
}

fn block_total(contents: &BTreeMap<u32, Vec<String>>) -> usize {
    contents.values().map(Vec::len).sum()
}

fn buckets_outside(contents: &BTreeMap<u32, Vec<String>>, start: u32, end: u32) -> usize {
    contents
        .keys()
        .filter(|bucket| **bucket < start || **bucket > end)
        .count()
}

// =============================================================================================
// 0. THE FIXTURE'S OWN ARITHMETIC
// =============================================================================================

/// THE TWO BUCKETS mx#1949 NAMED ARE STILL THE TWO BUCKETS THIS KEY HASHES TO.
///
/// Every assertion below leans on `inner-000000` landing in 398 under `0..1023` and in
/// 1,422,005,296 under the whole range. Those are properties of the hash, not of this change, and
/// if the hash ever moves every measurement in this file would silently start describing a
/// different pair of buckets. Asserted here, once, so that failure has a name.
#[test]
fn the_measured_key_still_hashes_to_the_two_buckets_the_measurement_names() {
    let narrow = block_routing_bucket(MEASURED_KEY, 0, NARROW_END);
    let wide = block_routing_bucket(MEASURED_KEY, 0, WIDE_END);
    println!("  {MEASURED_KEY}: narrow {narrow}, wide {wide}");
    assert_eq!(
        MEASURED_NARROW_BUCKET, narrow,
        "`{MEASURED_KEY}` no longer hashes to {MEASURED_NARROW_BUCKET} on 0..{NARROW_END}"
    );
    assert_eq!(
        MEASURED_WIDE_BUCKET, wide,
        "`{MEASURED_KEY}` no longer hashes to {MEASURED_WIDE_BUCKET} on the whole range"
    );
    assert!(
        wide > NARROW_END,
        "the whole-range bucket {wide} is inside 0..{NARROW_END}, so a page filed there is not \
         actually outside the shard and every assertion in this file passes for the wrong reason"
    );
}

// =============================================================================================
// 1. THE RANGE REACHES THE SHARD
// =============================================================================================

/// A LOADED SHARD CARRIES THE RANGE IT WAS LOADED ON, AND AN UNSTAMPED ONE ANSWERS THE OLD DEFAULT.
///
/// Both halves matter. The first is the change. The second is what makes the change safe for every
/// `ShardState` that never entered the engine -- a decoded manifest image, a report's scratch copy
/// -- because an unstamped state answers exactly what these two writers passed unconditionally
/// before the field existed.
#[test]
fn a_loaded_shard_carries_the_routing_range_it_was_loaded_on() {
    let unstamped = crate::engine::state::ShardState::default();
    assert!(
        !unstamped.routing_range_known,
        "a default ShardState claims to know a routing range; then the fallback below is not \
         testing the unstamped case at all"
    );
    assert_eq!(
        (0, WIDE_END),
        unstamped.routing_range(),
        "an unstamped ShardState must answer the WHOLE range -- what both writers passed \
         unconditionally before this field existed"
    );

    for end in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end);
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        println!("  loaded on 0..{end}, shard carries {:?}", shard.routing_range());
        assert!(
            shard.routing_range_known,
            "a shard loaded on 0..{end} entered the served map without its range stamped"
        );
        assert_eq!(
            (0, end),
            shard.routing_range(),
            "a shard loaded on 0..{end} carries the wrong range"
        );
    }
}

// =============================================================================================
// 2. THE MEASUREMENT, INVERTED
// =============================================================================================

/// THE TWO SITES THAT FILE A PAGE NOW FILE IT UNDER THE SHARD'S OWN RANGE.
///
/// This is mx#1949's `the_two_sites_that_file_a_page_still_file_it_under_the_whole_range`, with
/// the same fixture and the same two buckets named, asserting the opposite. It covers BOTH sites
/// -- mx#1949's measured only the upsert -- because a fix that reached one of them and not the
/// other would pass a test written against either alone.
///
/// The DENOMINATOR is asserted first: the shard must be CARRYING the narrow range, or the two
/// placements are the same expression and this passes without exercising the site it names.
///
/// IT USED TO ASSERT THE ADDRESS WAS UNROUTED as well, because the range was consulted only for an
/// address carrying no bucket of its own. An address carries none, so that half is structural and the
/// round trip below is kept only to show the wire key is inert.
#[test]
fn the_two_sites_that_file_a_block_now_file_it_under_the_shards_own_range() {
