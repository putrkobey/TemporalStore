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
    for (label, sync_path) in [("upsert_bucket_index_block", false), ("sync_bucket_index_object_blocks", true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, 8);
        let key = keys[0].clone();
        assert_eq!(
            MEASURED_KEY, key,
            "the fixture's first key is no longer the key the measurement names"
        );

        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        assert_eq!(
            (0, NARROW_END),
            shard.routing_range(),
            "the shard under test is not carrying 0..{NARROW_END}, so what follows would measure \
             the fallback rather than the fix"
        );
        let before = shard.string_address(key.as_str()).expect("key present").clone();
        let address = as_an_older_build_wrote_it(&before);
        assert_eq!(
            address, before,
            "the wire round trip changed the address. `rs` is inert -- the address holds no routing \
             bucket -- so this has to be the identity."
        );
        shard.set_string_address(key.as_str().into(), &address.clone());

        if sync_path {
            crate::engine::storage_bucket_internals::sync_bucket_index_object_blocks(
                shard,
                1,
                "string",
                &key,
                vec![address],
                true,
            );
        } else {
            crate::engine::storage_bucket_internals::upsert_bucket_index_block(
                shard, 1, "string", &key, None, address, true,
            );
        }

        let contents = bucket_contents(shard);
        let in_narrow = contents
            .get(&MEASURED_NARROW_BUCKET)
            .is_some_and(|object_keys| object_keys.contains(&key));
        let in_wide = contents
            .get(&MEASURED_WIDE_BUCKET)
            .is_some_and(|object_keys| object_keys.contains(&key));
        println!(
            "  {label}: {key} -- in {MEASURED_NARROW_BUCKET} {in_narrow}, in \
             {MEASURED_WIDE_BUCKET} {in_wide}"
        );
        assert!(
            in_narrow,
            "`{label}` did not file {key} in bucket {MEASURED_NARROW_BUCKET}, the bucket the \
             shard's own range puts it in. Buckets holding it: {:?}",
            contents
                .iter()
                .filter(|(_, object_keys)| object_keys.contains(&key))
                .map(|(bucket, _)| *bucket)
                .collect::<Vec<_>>()
        );
        assert!(
            !in_wide,
            "`{label}` filed {key} in bucket {MEASURED_WIDE_BUCKET} -- the whole-range bucket, on \
             a shard that holds 0..{NARROW_END}. This is the defect mx#1949 measured and declined; \
             it has come back."
        );
        assert_eq!(
            0,
            buckets_outside(&contents, 0, NARROW_END),
            "the shard holds 0..{NARROW_END} and some page is filed outside it: {:?}",
            contents.keys().collect::<Vec<_>>()
        );
    }
}

// =============================================================================================
// 3. THE CONTROL: while every page is routed, the writers are value-identical
// =============================================================================================

/// THE WRITERS FILE BY THE STAMP, AND THE TWO STAMPS DISAGREE.
///
/// THIS WAS A CONTROL AND IS NOW THE SUBJECT. While an address carried its own routing bucket, that
/// bucket won and the shard's stamp was consulted only where a fallback would otherwise have fired
/// -- so a shard stamped `0..1023` and a shard stamped with the whole range filed the same page in
/// the same bucket, and the control said so.
///
/// An address carries no bucket. The stamp decides EVERY placement, so the two stamps must file the
/// same page DIFFERENTLY, and the assertion is inverted. The control that says the comparison can
/// express sameness moves to the same stamp twice.
///
/// Run at BOTH stamps, because "the shard's range IS the whole range" is exactly the condition under
/// which this change is invisible.
#[test]
fn the_two_writers_file_by_the_stamp_and_the_two_stamps_disagree() {
    let mut filings: Vec<BTreeMap<u32, Vec<String>>> = Vec::new();
    for stamp in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed(&engine, 32);

        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        // Re-stamp under the test's own hand. The engine loaded this shard on 0..1023 either way;
        // what is varied here is the ONE input this change added.
        shard.set_routing_range(0, stamp);

        // DENOMINATOR: the stamp took, which is the one input this change varies.
        assert_eq!(
            (0, stamp),
            shard.routing_range(),
            "the shard is not carrying 0..{stamp}, so the arm below files by something else"
        );
        for key in &keys {
            let address = shard.string_address(key.as_str()).expect("key present").clone();
            crate::engine::storage_bucket_internals::upsert_bucket_index_block(
                shard, 1, "string", key, None, address, true,
            );
        }
        let contents = bucket_contents(shard);
        println!(
            "  stamped 0..{stamp}: {} pages over {} buckets",
            block_total(&contents),
            contents.len()
        );
        assert!(block_total(&contents) >= keys.len(), "the fixture filed nothing");
        filings.push(contents);
    }
    // INVERTED. While an address carried its own routing bucket, that bucket won and the stamp was
    // consulted only where a fallback would otherwise have fired -- so the two stamps had to file
    // identically and this asserted equality. The stamp decides EVERY placement now, so they must
    // differ, and the buckets the wide stamp names must be ones the narrow shard does not hold.
    assert_ne!(
        filings[0], filings[1],
        "the two writers filed the same pages into the same buckets under a 0..{NARROW_END} stamp \
         and a 0..{WIDE_END} one. The stamp is the only input to the placement now, so this would \
         mean the writers are not reading it."
    );
    let narrow_outside = buckets_outside(&filings[0], 0, NARROW_END);
    let wide_outside = buckets_outside(&filings[1], 0, NARROW_END);
    assert_eq!(
        0, narrow_outside,
        "{narrow_outside} buckets of the NARROW stamp's filing sit above {NARROW_END}"
    );
    assert!(
        wide_outside > 0,
        "the WIDE stamp filed every page inside 0..{NARROW_END} as well, so the two stamps cannot \
         be told apart by where they put a page and the inequality above is measuring noise"
    );
}

// =============================================================================================
// 4. DIRECTION: narrowing places, it does not filter
// =============================================================================================

/// NOT ONE PAGE ALREADY FILED IS REMOVED WHEN THE WRITERS NARROW.
///
/// mx#1945's refutation is that narrowing a FILTER drops live data -- 0 of 400 surviving. These
/// two are not filters, and this drives it element by element rather than by count: the whole
/// bucket-contents map before and after, with the only difference being the one page re-filed.
///
/// The shard here is loaded on `0..1023` with its pages made UNROUTED, so the load path's own
/// rebuild files them on the shard's range; then one page is re-published through each writer.
/// Every other page must be exactly where it was.
#[test]
fn narrowing_the_writers_removes_no_block_that_was_already_filed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let mut shards = engine.shards.write().expect("shards lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let round_tripped = round_trip_every_block_through_the_wire(shard);
    assert_eq!(
        RECORDS, round_tripped,
        "the fixture round-tripped {round_tripped} of {RECORDS} addresses"
    );
    crate::engine::storage_bucket_internals::rebuild_bucket_block_ownership(
        1,
        shard,
        0,
        NARROW_END,
    );
    let before = bucket_contents(shard);
    assert_eq!(
        RECORDS,
        block_total(&before),
        "the fixture starts with {} pages, not {RECORDS}",
        block_total(&before)
    );
    assert_eq!(
        0,
        buckets_outside(&before, 0, NARROW_END),
        "the fixture starts with pages outside the shard's range, so this measures the rebuild \
         rather than the writers"
    );

    let key = keys[0].clone();
    let address = shard.string_address(key.as_str()).expect("key present").clone();
    crate::engine::storage_bucket_internals::upsert_bucket_index_block(
        shard, 1, "string", &key, None, address, true,
    );
    let after = bucket_contents(shard);

    println!(
        "  before: {} pages / {} buckets     after: {} pages / {} buckets",
        block_total(&before),
        before.len(),
        block_total(&after),
        after.len()
    );
    assert_eq!(
        block_total(&before),
        block_total(&after),
        "a page was lost or duplicated by an upsert that only re-files one key"
    );
    assert_eq!(
        0,
        buckets_outside(&after, 0, NARROW_END),
        "the upsert filed something outside 0..{NARROW_END}"
    );
    // ELEMENT BY ELEMENT: every key present before is present after, and in the same bucket
    // unless it is the one key that was re-published.
    let placement = |contents: &BTreeMap<u32, Vec<String>>| -> BTreeMap<String, u32> {
        let mut out = BTreeMap::new();
        for (bucket, object_keys) in contents {
            for object_key in object_keys {
                out.insert(object_key.clone(), *bucket);
            }
        }
        out
    };
    let before_placement = placement(&before);
    let after_placement = placement(&after);
    assert_eq!(
        before_placement.keys().collect::<Vec<_>>(),
        after_placement.keys().collect::<Vec<_>>(),
        "the set of object keys in the index moved"
    );
    let moved: Vec<&String> = before_placement
        .iter()
        .filter(|(object_key, bucket)| after_placement.get(*object_key) != Some(bucket))
        .map(|(object_key, _)| object_key)
        .collect();
    assert!(
        moved.is_empty(),
        "these keys changed bucket when only {key} was re-published: {moved:?}"
    );
}

// =============================================================================================
// 5. THE STORED SHAPE DOES NOT MOVE
// =============================================================================================

/// THE FIELD IS ABSENT FROM THE SERIALIZED INDEX, DRIVEN BY BYTES.
///
/// `ShardState` IS the serialized index. Its own `index_format_version` doc records what a change
/// to that shape has cost once already, so "a skipped field is not serialized" is exactly the kind
/// of claim that has to be driven rather than read off an attribute.
///
/// Both directions come out of one measurement, because there is only one byte stream: a shard
/// serialized with the field stamped and the same shard serialized unstamped produce IDENTICAL
/// bytes, so there is no old-index/new-index distinction for a decoder to get wrong. What a decode
/// then yields -- an UNSTAMPED state, answering the whole range -- is what an index written before
/// this change reads as after it.
#[test]
fn the_routing_range_field_changes_no_serialized_byte() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    seed(&engine, 16);

    let mut shards = engine.shards.write().expect("shards lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");

    let mut stamped = shard.clone();
    // The PRODUCTION codec, not the legacy one: `serialize_index` picks its payload encoding off
    // this field, and a byte comparison taken on the fallback path would not be a comparison of
    // what the engine actually writes.
    stamped.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    stamped.set_routing_range(0, NARROW_END);
    let mut unstamped = shard.clone();
    unstamped.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    unstamped.routing_range_start = 0;
    unstamped.routing_range_end = 0;
    unstamped.routing_range_known = false;
    assert_ne!(
        stamped.routing_range(),
        unstamped.routing_range(),
        "the two states under test answer the same range, so byte-equality below would prove \
         nothing about the field"
    );

    let stamped_bytes = crate::engine::serialize_index(&stamped);
    let unstamped_bytes = crate::engine::serialize_index(&unstamped);
    println!(
        "  stamped {} bytes, unstamped {} bytes",
        stamped_bytes.len(),
        unstamped_bytes.len()
    );
    assert!(
        stamped_bytes.len() > 256,
        "the fixture serialized {} bytes; that is not a populated index and byte-equality over \
         two empty buffers says nothing",
        stamped_bytes.len()
    );
    assert_eq!(
        stamped_bytes, unstamped_bytes,
        "stamping the routing range changed the serialized index. The field is supposed to be \
