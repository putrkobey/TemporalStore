// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE BUCKET A READER GUESSES, AGAINST THE BUCKET THE PAGE IS FILED UNDER.
//!
//! mx#1945 closed by naming a tenth site "no call site can correct": a routing range supplied as a
//! HARD-CODED LITERAL rather than taken as an argument, invisible to an argument-reading scan. It
//! named three. Enumerated with the compiler -- both doors renamed, iterated to a zero pass --
//! there are EIGHT, and they do not divide the way the three suggested.
//!
//! # THE SPLIT IS PLACES AGAINST ATTRIBUTES, AND ONLY ONE HALF NEEDS A RANGE
//!
//! Two of the eight decide WHERE A PAGE IS FILED (`upsert_bucket_index_block_inner`,
//! `sync_bucket_index_object_blocks_with_mode`). Those genuinely need the shard's range and could
//! not reach it: they take `&mut ShardState` and a `ShardId`, never `&self`, so
//! `shard_routing_range` -- which reads the engine's info rows -- is not available. They were NOT
//! changed here, and were held as a measured, named defect instead. mx#1953 fixed them by giving
//! the shard its own range to carry, and the measurement is inverted rather than deleted; see
//! `shard_carried_range` and section 5 below.
//!
//! FIVE do not decide placement at all. They ATTRIBUTE an already-filed page to a bucket, for a
//! report, for a dump-reuse comparison, or -- once -- to decide whether to DELETE the record. Each
//! asked "which bucket is this page in?" and answered it with `block_routing_bucket(key, 0,
//! u32::MAX)`: a hash over the WHOLE range, which is the bucket the page is filed under only when
//! the shard is loaded on the whole range too.
//!
//! **They never needed a range.** `collect_bucket_index_live_block_entries` walks `bucket_map`, so
//! the filed bucket is the KEY OF THE MAP IT IS WALKING -- and it was iterating `.values()` and
//! discarding it. Carrying it (`LiveBlockEntry::filed_routing_bucket`) removes the guess without
//! threading anything, and is correct under every range rather than under one.
//!
//! The eighth is a record field, not a placement, and is right as written. See
//! `the_delete_outcomes_routing_bucket_is_a_record_field_no_reader_consults`.
//!
//! # WHY THIS IS VALUE-IDENTICAL EXCEPT ON THE DEFECT
//!
//! THE FILING IS NOW THE ONLY ANSWER, which is what makes these five readers load-bearing. The
//! address used to answer first and `entry.filed_bucket()` was the fallback -- mx#1949 proved that
//! fallback production-INERT, and a mutant that broke it survived by construction. The address no
//! longer carries a bucket, so the walk's own filing answers every time.
//!
//! `the_five_readers_answer_the_filing_on_a_store_the_current_build_wrote` exercises all five on a
//! store this build wrote, with its denominator stated: every page reports a filing and attribution
//! agrees with it, at the operator's range and at the whole keyspace. The control is the same
//! attribution computed the pre-mx#1949 way, which agrees on NEITHER at the narrow range -- that
//! disagreement is what says the exercise is reading the filing rather than reading a hash that
//! happens to match.
//!
//! # WHAT mx#1945 LEFT SPLIT, MEASURED
//!
//! mx#1945 moved six REBUILD call sites to the shard's own range. The five readers here kept
//! guessing the whole range. On a store whose pages arrived unrouted -- mx#1942's door, an index
//! written before the wire ever carried a routing bucket, which is now every index, the slot
//! having been retired from `BlockAddressWire` -- that split is total:
//!
//! ```text
//!   after the load path's own rebuild on 0..1023:
//!     200 of 200 pages filed where the NARROW formula puts them
//!       0 of 200 pages filed where the WIDE   formula puts them
//!   the five readers agreed with the filing on   0 of 200
//!   an eviction round with delete_drop:  4 victims chosen, 0 objects dropped
//! ```
//!
//! So mx#1945's conclusion CHANGES. "The maintenance accounting is actively wrong" understates it:
//! `apply_storage_eviction` is not accounting, it is the actuator, and it dropped nothing. A round
//! that chooses victims and frees nothing does not look like a failure from outside -- it looks
//! like a store under memory pressure that will not come down.

use super::*;
use super::round_walk_scope::call_arguments;
use crate::engine::hashing::block_routing_bucket;
use crate::engine::storage_bucket_internals::{
    collect_live_block_entries, rebuild_bucket_first_index, refresh_bucket_runtime_flags,
    validate_bucket_ownership_index_from_entries,
};
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;
/// The end bucket `load_shard` uses, and the one all eight literal sites spell.
const WIDE_END: u32 = u32::MAX;

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
        table_name: "inner-range".to_string(),
        shard_uri: "local://inner-range/1".to_string(),
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
/// by the ENGINE'S OWN DECODER rather than by the setter -- mx#1942's door, reused so all three
/// files construct the same state the same way.
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
/// round-trip it performs is the assertion that the `rs` key on the wire is inert: it must come back
/// EQUAL, and `the_older_index_door_is_the_identity` fails if it does not.
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
            "page {key} changed when its `rs` key was removed from the wire. The address holds no \
             routing bucket, so the key is inert and this round trip has to be the identity -- if \
             it is not, an index written before the field left decodes to a DIFFERENT address and \
             this change needs a migration."
        );
        shard.set_string_address(key.as_str().into(), &older);
    }
    keys.len()
}

/// THE DOOR'S OWN CONTROL, because a helper that asserts equality of two things it made equal is
/// worth nothing unless the shape it produces is really different from the one it started with.
///
/// THE CONTROL INVERTED WITH THE DOOR. It used to require the `rs` key to be PRESENT, so that
/// removing it was not a no-op. The slot is retired from `BlockAddressWire`, so the door adds the
/// key instead, and what has to be true for it to be doing anything is that a current address does
/// not already carry one.
#[test]
fn the_older_index_door_is_the_identity() {
    let address = ElementEntry::from_parts(3, 4096, 128, Some(1), Some(2));
    assert_eq!(
        as_an_older_build_wrote_it(&address),
        address,
        "an index carrying the retired `rs` key must decode to the same address as one without it"
    );
    // And the key really is absent to begin with -- `as_an_older_build_wrote_it` asserts that too,
    // but from inside, where a caller cannot see it fail for the right reason.
    assert!(
        serde_json::to_value(&address)
            .expect("serializes")
            .get("rs")
            .is_none(),
        "a current address still carries an `rs` slot, so the door adds nothing and the identity \
         above is two copies of the same shape"
    );
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

/// What the five readers now compute for each page, and what they computed before: the bucket the
/// page is FILED under, against the whole-range hash of its key.
fn reader_attribution(
    shard: &crate::engine::state::ShardState,
) -> (usize, usize, usize) {
    let entries = collect_live_block_entries(shard);
    let total = entries.len();
    let mut agrees_with_filing = 0usize;
    let mut whole_range_agrees = 0usize;
    let filed_names = super::BlockObjects::of(&shard.bucket_index);
    let filed: BTreeMap<(String, u64), u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .flat_map(|(bucket_id, bucket)| {
            let filed_names = &filed_names;
            bucket.block_index.iter().map(move |(handle, block)| {
                (
                    (
                        filed_names.at(*bucket_id, *handle).to_string(),
                        block.address.offset(),
                    ),
                    *bucket_id,
                )
            })
        })
        .collect();
    for entry in &entries {
        let Some(actual) = filed.get(&(entry.object_key.to_string(), entry.address.offset())) else {
            continue;
        };
        // THE FIVE READERS' OWN EXPRESSION, which no longer has an address in it: the walk's
        // filing, with the whole-range hash as the last resort.
        let now = entry
            .filed_bucket()
            .unwrap_or_else(|| block_routing_bucket(&entry.object_key, 0, WIDE_END));
        // What they computed before mx#1949: the whole-range hash, every time.
        let before = block_routing_bucket(&entry.object_key, 0, WIDE_END);
        if now == *actual {
            agrees_with_filing += 1;
        }
        if before == *actual {
            whole_range_agrees += 1;
        }
    }
    (total, agrees_with_filing, whole_range_agrees)
}

// =============================================================================================
// 1. THE CONTROL: while every page is routed, nothing moves
// =============================================================================================

/// THE FIVE READERS ANSWER THE FILING ON A STORE THE CURRENT BUILD WROTE.
///
/// THE CONTROL THIS USED TO BE. While a page's address carried its own routing bucket, that bucket
/// won and `filed_bucket()` was consulted only where a fallback would otherwise have fired -- so the
/// old expression and the new one had to be value-identical on a freshly written store, and the
/// denominator was "every page is routed".
///
/// AN ADDRESS CARRIES NO BUCKET NOW, so there is no old expression to be identical to and no routed
/// population to assert. What is left is the claim that matters: the walk knows the filing for every
/// page it returns from the bucket-index arm, and what the readers compute from it IS where the page
/// is. The denominator moves with it -- every entry must report a filing, because an entry that does
/// not sends the reader to a whole-range hash.
///
/// Run at BOTH widths, because "the shard's range is the whole range" is exactly the condition under
/// which the defect mx#1949 fixed is invisible, and a control that only holds at one width says
/// nothing about the other.
#[test]
fn the_five_readers_answer_the_filing_on_a_store_the_current_build_wrote() {
    for end_routing_bucket in [NARROW_END, WIDE_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed(&engine, RECORDS);

        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let entries = collect_live_block_entries(shard);

        let filed = entries
            .iter()
            .filter(|entry| entry.filed_bucket().is_some())
            .count();
        assert_eq!(
            filed,
            keys.len(),
            "0..{end_routing_bucket}: {filed} of {} live pages report which bucket they are filed \
             in. All of them must: the bucket-index arm of the walk has the map key in hand, and an \
             entry that does not report it sends all five readers to a hash over the WHOLE \
             keyspace -- which on this shard names a bucket it does not hold.",
            keys.len()
        );

        let (total, now, before) = reader_attribution(shard);
        assert_eq!(
            total,
            keys.len(),
            "0..{end_routing_bucket}: the fixture wrote {} keys and the index holds {total} pages",
            keys.len()
        );
        assert_eq!(
            now, total,
            "0..{end_routing_bucket}: {now} of {total} pages are attributed to the bucket they are \
             filed under. The walk hands every reader the map key it was iterating, so all of them \
             must be."
        );
        // AND THE OLD EXPRESSION IS ONLY IDENTICAL AT THE WIDE END. `block_routing_bucket(key, 0,
        // u32::MAX)` is the same function of the same arguments as the shard's own placement when the
        // shard IS the whole keyspace, and a different one otherwise -- which is the defect mx#1949
        // fixed, and the reason this control is run at both widths.
        if end_routing_bucket == WIDE_END {
            assert_eq!(
                before, total,
                "0..{end_routing_bucket}: on the WIDE shard the whole-range hash and the shard's \
                 own placement are the same expression, so the old answer has to agree with the \
                 filing on all {total} pages"
            );
        } else {
            assert_eq!(
                before, 0,
                "0..{end_routing_bucket}: the whole-range hash agreed with the filing on {before} \
                 of {total} pages. It must agree with NONE -- it names a bucket above \
                 {end_routing_bucket} for every key, and the shard holds none of them -- or this \
                 control is not reproducing what mx#1949 fixed."
            );
        }
        println!(
            "  0..{end_routing_bucket}: {total} pages, {filed} reporting a filing, attribution \
             agrees {now}/{total} now and {before}/{total} under the pre-mx#1949 expression"
        );
    }
}

// =============================================================================================
// 2. THE MEASUREMENT: with the door open, the five readers agreed with nothing
// =============================================================================================

/// ELEMENT BY ELEMENT, NOT BY COUNT, IN BOTH DIRECTIONS.
///
/// The subject arm is a store whose pages arrived UNROUTED, filed by the load path's own rebuild
/// on the shard's own range -- which is what mx#1945 made that rebuild pass. The five readers then
/// hashed each key over the WHOLE range and named a bucket the shard does not hold.
///
/// THE CONTROL IS THE SAME FIXTURE WITH ROUTED ADDRESSES, above: a page whose address carries its
/// own bucket is attributed by that bucket whichever expression is used, so the control must come
/// out identical and does. The subject arm must differ on every page, and does.
#[test]
fn a_reader_attributes_a_reconstructed_block_to_the_bucket_it_is_actually_filed_under() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let (round_tripped, contents) = {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        // The door is the identity now -- see `round_trip_every_block_through_the_wire`. It is kept
        // in the fixture because it asserts that, per page, and because a page that DID change when
        // its `rs` key came off would make every number below mean something else.
        let round_tripped = round_trip_every_block_through_the_wire(shard);
        // The rebuild the load path runs, with the argument mx#1945 gave it.
        rebuild_bucket_first_index(1, shard, 0, NARROW_END);
        refresh_bucket_runtime_flags(shard);
        (round_tripped, bucket_contents(shard))
    };
    assert_eq!(
        round_tripped,
        keys.len(),
        "the door covered {round_tripped} of {} keys; a door that covered none would make every \
         number below zero for the wrong reason",
        keys.len()
    );

    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    // Element by element: every page is filed where the SHARD'S range puts it, and none where the
    // whole range does. Both directions asserted, because only one of them loses a page.
    let mut filed_narrow = 0usize;
    let mut filed_wide = 0usize;
    for (bucket_id, object_keys) in &contents {
        for key in object_keys {
            if *bucket_id == block_routing_bucket(key, 0, NARROW_END) {
                filed_narrow += 1;
            }
            if *bucket_id == block_routing_bucket(key, 0, WIDE_END) {
                filed_wide += 1;
            }
            assert!(
                *bucket_id <= NARROW_END,
                "page {key} is filed in bucket {bucket_id}, which a shard loaded on 0..{NARROW_END} \
                 does not hold"
            );
        }
    }
    assert_eq!(
        filed_narrow,
        keys.len(),
        "{filed_narrow} of {} pages are filed where the shard's own range puts them",
        keys.len()
    );
    assert_eq!(
        filed_wide, 0,
        "{filed_wide} pages are filed where the WHOLE range puts them, on a shard that holds \
         0..{NARROW_END}"
    );

    let (total, now, before) = reader_attribution(shard);
    println!("  {total} reconstructed pages filed on 0..{NARROW_END}");
    println!("    readers agree with the filing, NEW expression: {now} of {total}");
    println!("    readers agree with the filing, OLD expression: {before} of {total}");
    assert_eq!(
        now, total,
        "the five readers must attribute every page to the bucket it is filed under; they agree \
         on {now} of {total}"
    );
    assert_eq!(
        before, 0,
        "THE OLD EXPRESSION MUST AGREE WITH NOTHING HERE, or this fixture is not reproducing the \
         defect: it agreed on {before} of {total}. `block_routing_bucket(key, 0, u32::MAX)` names \
         a bucket above {NARROW_END} for every key, and the shard holds none of them."
    );
}

// =============================================================================================
// 3. THE ACTUATOR: an eviction round that chose victims and freed nothing
// =============================================================================================

/// `apply_storage_eviction` IS NOT ACCOUNTING. IT DELETES RECORDS.
///
/// Its filter asks which bucket each live page is in and keeps the page's object key when that
/// bucket is one of the round's victims. The victims come from `bucket_storage_summaries`, which
/// reads `bucket_map` -- so the victim buckets are where pages are FILED. Asking the whole-range
/// hash instead names a bucket no victim can be, so the filter matches nothing and the round frees
/// nothing.
///
/// Measured as a BAND, not a bound: a round that dropped everything would pass a floor, and a
/// round that dropped nothing passes a ceiling. Both ends are named.
#[test]
fn an_eviction_round_over_reconstructed_blocks_drops_the_objects_it_chose() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed(&engine, RECORDS);

    let filed_in_range = {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let round_tripped = round_trip_every_block_through_the_wire(shard);
        rebuild_bucket_first_index(1, shard, 0, NARROW_END);
        refresh_bucket_runtime_flags(shard);
        assert_eq!(
            round_tripped,
            keys.len(),
            "the door covered {round_tripped} of {} keys",
            keys.len()
        );
        // DENOMINATOR: the reconstruct filed every page inside the shard's own range, which is what
        // makes the victim buckets below reachable at all. It used to be "every page is unrouted",
        // a statement about addresses; the pages are what the round acts on.
        bucket_contents(shard)
            .iter()
            .filter(|(routing_bucket, _)| **routing_bucket <= NARROW_END)
            .map(|(_, object_keys)| object_keys.len())
            .sum::<usize>()
    };
    assert_eq!(
        filed_in_range,
        keys.len(),
        "{filed_in_range} of {} pages are filed inside 0..{NARROW_END}; below that this round \
         cannot reach them and the numbers below say nothing",
        keys.len()
    );

    let report = engine.apply_storage_eviction(1, 1, 4, false, true);
    let chosen = report.selected_victims.len();
    let dropped = report.dropped_object_count;
    println!("  victims chosen {chosen}, objects dropped {dropped}, of {} records", keys.len());

    assert!(
        chosen > 0,
        "the round chose {chosen} victims, so it did no choosing and the drop count below \
         measures nothing"
    );
    // THE BAND. A floor alone passes if the round started dropping the whole store; a ceiling
    // alone passes if it dropped nothing, which is the defect.
    assert!(
        dropped > 0,
        "the round chose {chosen} victim buckets and dropped {dropped} objects. Zero is what the \
         whole-range guess produces: the filter names a bucket above {NARROW_END} for every page, \
         no victim bucket can equal it, and the round frees nothing while reporting that it ran."
    );
    assert!(
        dropped < keys.len(),
        "the round dropped {dropped} of {} objects. Dropping the whole store means the filter \
         stopped filtering, which passes a floor just as happily as the right answer does.",
        keys.len()
    );
}

// =============================================================================================
// 4. THE CROSS-RANGE RESTORE, DRIVEN IN BOTH DIRECTIONS
// =============================================================================================

/// WRITE ON ONE RANGE, READ ON THE OTHER, BOTH WAYS -- AND IT IS NOW A REFUSAL.
///
/// mx#1945's refutation was found this way and cost a record: a whole-shard image installed on a
/// narrower shard is installed whole or truncated. The claim this test carried was that nothing
/// moves when a store written on one range is reopened on the other -- every record readable and
/// every page present.
///
/// THAT CLAIM WAS TRUE AND THAT WAS THE PROBLEM. Every record read, and every page sat in a bucket
/// the reopened shard did not hold -- outside the dump's bucket selection, eviction's victim
/// sampling, the reclaim floor and the release pass. `routing_range_stamp.rs` records the range a
/// store was built under and the load now REFUSES when the configured range disagrees, so the state
/// this test drove is no longer reachable through a load in either direction.
///
/// THIS TEST IS REPLACED, NOT RETARGETED, AND THE DIFFERENCE MATTERS. The retarget that suggests
/// itself -- delete the stamp so the load proceeds -- would put both arms on the PRE-STAMP
/// ADOPTION path, which honours the range the store was BUILT on. Each arm would then read on the
/// range it wrote on while the test's name still said "on the other": a pass over a different
/// population, which is the shape #1962 named and worse than a red.
///
/// The page-preservation claim it protected is still driven where it remains reachable -- the
/// manifest install, which installs a whole-shard image across ranges without going through a load,
/// in `shard_carried_range::a_manifest_installed_across_ranges_leaves_the_target_carrying_its_own_range`.
///
/// BOTH DIRECTIONS ARE REFUSED, and widening is not the safe one. Widening leaves every existing
/// page inside the new range, so it loses nothing -- but the same key's NEXT page routes to
/// `hash % 4294967296` where its existing pages are at `hash % 1024`, so one object's pages end up
/// in two different buckets, and a bucket is the unit of dumping and of release.
#[test]
fn a_store_written_on_one_range_is_refused_on_the_other_in_both_directions() {
    for (wrote_on, read_on) in [(NARROW_END, WIDE_END), (WIDE_END, NARROW_END)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys = {
            let engine = engine_on(dir.path());
            load_on(&engine, wrote_on);
            let keys = seed(&engine, RECORDS);
            assert_eq!(
                read_back(&engine, &keys),
                keys.len(),
                "0..{wrote_on}: the store was not readable before the restart, so a refusal after \
