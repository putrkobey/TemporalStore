// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SCOPING THE ROUND'S SUMMARY WALK TO THE DIRTY SET: WHAT HAS TO BE TRUE FIRST, AND IS NOT.
//!
//! mx#1938 priced the obvious fix for the empty round -- scope `bucket_storage_summaries` to the
//! dirty bucket set -- and declined it for a named reason:
//!
//! > The walk credits each page to `entry.address.routing_bucket()` with a hash fallback over the
//! > SHARD'S routing range, while `upsert_bucket_index_block_inner` files that same page under a
//! > fallback over `0..u32::MAX`, so a page whose address carries no explicit routing bucket can
//! > be filed under one bucket and summarised under another. Measured on this fixture the branch
//! > does not fire -- zero unrouted pages and zero misfiled pages at both sizes -- but "does not
//! > fire on this fixture" is not the same statement as "cannot fire".
//!
//! This file answers that, and the answer has three parts.
//!
//! # 1. THAT ZERO WAS AN IDENTITY, NOT A MEASUREMENT
//!
//! mx#1938 and mx#1934 both build their shard with `engine.load_shard(1)`, which loads it on
//! `start_routing_bucket = 0`, `end_routing_bucket = u32::MAX`. On THAT shard the two fallbacks
//! are not two placements that happened to agree: they are the same function applied to the same
//! three arguments. `block_routing_bucket(key, 0, u32::MAX)` is `block_routing_bucket(key, start,
//! end)` with `start = 0` and `end = u32::MAX` substituted. No page could have been misfiled, and
//! no fixture of that shape can ever report anything but zero.
//!
//! `a_wide_range_fixture_cannot_tell_the_two_placements_apart` asserts exactly that: on the wide
//! shard the misfiled count is 0 AND the two placement functions agree on every key, so the zero
//! is attributable to the identity rather than to the engine. The same assertion on a shard loaded
//! with the production narrow range -- `TS_SHARD_END_ROUTING_SLOT=1023`, the setting that cuts
//! resident memory 45% -- reports the two placements disagreeing on 2,000 of 2,000 keys.
//!
//! # 2. THE PAGE CAN BE CONSTRUCTED, AND HERE IT IS
//!
//! NO ADDRESS CARRIES A ROUTING BUCKET ANY MORE, so the disagreeing state this module was built to
//! bound cannot arise: the bucket is derived from the object key over the range the store is stamped
//! with, and a derived bucket is inside the range it was derived on. What remains on the wire is the
//! `rs` slot itself, kept because the index log packs the address POSITIONALLY and removing the field
//! would shift `g` and `h` down one place. It is read, ignored, and written nil.
//!
//! `dropping_the_routing_bucket_from_the_wire_decodes_to_the_same_address` is the door, and it is the
//! engine's own decoder rather than a test: a record carrying a real bucket, a record carrying nil,
//! and a record with the key absent altogether all decode to the SAME address. That is the property
//! that makes the slot's presence a content change and not a schema change.
//!
//! Put pages in that state into a narrow shard and run the reconstruct the WAL-replay tail runs --
//! `rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX)`, which is `lifecycle.rs`'s own call with
//! `lifecycle.rs`'s own arguments -- and every one of them is filed under a bucket a derivation over
//! the SHARD'S range would not choose. Not a hypothetical: a count, at a stated denominator, 600 of
//! 600.
//!
//! AND THAT NO LONGER COSTS ANYTHING, which is the half this file now has to say. The consequence it
//! was built to price was "a page filed under one bucket and summarised under another", and the
//! summary walk does not derive a placement any more -- `bucket_storage_summaries` credits each page
//! to the bucket the walk FILED it in, keeping the derivation only for a model-map entry that has no
//! filing to read. So the filed and the summarised bucket sets are equal at EVERY range, and a dump
//! naming the buckets the summaries name carries every one of their slabs.
//!
//! The same correction is what stopped a cross-range manifest install refusing itself with
//! `slot_dump_slot_summary_mismatch`: one half of a summary derived the bucket while the other half
//! read `bucket_map`, so the two halves answered different questions about the same page. Two
//! derivations of one quantity have to ask the same question -- and the cheapest way to make them is
//! for one of them to stop deriving.
//!
//! # 3. WHAT THAT DOES TO A SCOPED WALK, MEASURED IN BOTH DIRECTIONS
//!
//! A scoped walk cannot start from the live page set -- materialising that IS the cost being
//! removed. It has to enter at `bucket_map[dirty bucket]`, which makes the precondition a
//! statement about where a page is FILED, not about where it is summarised:
//!
//! > EVERY LIVE PAGE OF A DIRTY OBJECT MUST BE FILED IN A BUCKET THE DIRTY SET NAMES.
//!
//! The dirty set is keyed by `block_routing_bucket(key, start, end)` at both of the two sites that
//! mark an object dirty. The filing is decided by `address.routing_bucket()` with a `0..u32::MAX`
//! fallback -- at `upsert_bucket_index_block_inner`, and, WHEN THIS FILE WAS WRITTEN, at NINE of
//! the twenty production call sites that rebuild the bucket index. So the precondition failed for
//! every page that reached one of those without a routing bucket of its own, and it failed
//! SILENTLY: the page is still readable, still summarised, still counted. Only a walk that entered
//! through the dirty set would miss it.
//!
//! SIX OF THOSE NINE NOW PASS THE SHARD'S OWN RANGE. THREE ARE CORRECT AS WRITTEN, for two
//! different reasons, and neither reason is visible from the call site alone:
//!
//!   * TWO run `rebuild_bucket_block_ownership` over a DECODED MANIFEST INDEX -- a whole-shard
//!     image written by whatever range the SOURCE shard ran on. That rebuild FILTERS on the range
//!     it is given, so narrowing it there DELETES every page whose source bucket falls outside the
//!     installing shard's range. Driven, not argued: a cross-range restore read a record back as
//!     None.
//!   * ONE is a `#[cfg(test)]` helper on a shard it publishes as `0..u32::MAX`, for which the whole
//!     range IS the shard's range.
//!
//! `the_three_rebuild_call_sites_still_on_the_whole_range_are_each_right_to_be` holds the list, and
//! `engine/tests/bucket_filing_range.rs` measures what the change did, what had to be true before
//! it could be made, and what the filter costs when it is pointed at a foreign image.
//! `upsert_bucket_index_block_inner`'s own hard-coded fallback is untouched and is a separate
//! class: it takes no range argument at all, so no call site can correct it.
//!
//! THERE IS A THIRD PLACEMENT RULE, and it is the one thing here that is already safe. A RELEASED
//! bucket holds no entry in `bucket_map` at all; `collect_live_block_entries` supplements it from
//! the model maps, keeping only pages whose address carries an explicit routing bucket equal to
//! the bucket's own, with no hash fallback at all. A scoped walk opening `bucket_map[b]` for a
//! released `b` would find nothing. What stops that mattering is a precondition on the other
//! side: `release_bucket_blocks` refuses a bucket that is dirty, so a released bucket is never in
//! the dirty set. `assert_no_released_bucket_is_dirty` asserts it at every checkpoint in this
//! file -- it began life as a NOT-EXERCISED assertion and went red on the first run, because a
//! dumping round releases buckets and this fixture reaches eight of them.
//!
//! It does not un-fail either. `upsert_bucket_index_block_inner` stages its computed bucket into
//! the WAL outcome item and the index-log item, and `WalOutcomeItem::resolved_address` and
//! `IndexLogItem::restore_address_repeats` stamp that value back onto the address on every replay.
//! The fallback fires once; the bucket it chose is then explicit for ever.
//!
//! Both halves are counted separately -- a page a scoped walk cannot REACH, and a page it would
//! reach but CREDIT elsewhere -- because they are different defects. And both directions of the
//! set comparison are asserted separately, because only one of them loses data:
//!
//! ```text
//!   a dirty bucket the scoped walk MISSES   -- the page is never dumped. Silent.
//!   a clean bucket the scoped walk SKIPS    -- the saving. Waste avoided.
//! ```
//!
//! # WHAT THIS CHANGES: NOTHING, DELIBERATELY
//!
//! No engine code moves here. The scoped walk lives in this file as a measurement, so the saving
//! can be priced and the precondition can be watched, and `scoping_is_exact_only_while_every_block_carries_the_shards_own_placement`
//! is the guard that says when it becomes safe to move.
//!
//! # WHAT IT SAVES, AND WHAT IT DOES NOT
//!
//! mx#1938 wrote that scoping `bucket_storage_summaries` "would take 2,889,724 entries off the
//! empty round at 100,000 records". That is the WHOLE round's live-page cost, and this walk is one
//! of nine sites in five modules that make it up -- the same nine, by file and line, that the
//! measurement below charges. Measured directly rather than inherited:
//!
//! ```text
//!                             store 20,000   store 100,000    ratio   per record of STORE
//!   the empty ROUND               497,908       2,488,501    4.998x     24.895 -> 24.885
//!   THIS walk within it            79,582         397,696    4.997x      3.979 ->  3.977
//!   share of the round               16.0%           16.0%
//! ```
//!
//! Scoping this walk is SIXTEEN PER CENT of the empty round, not all of it. The 397,696 charged to
//! `storage_reporting.rs:171` reproduces mx#1938's 398,456 for the same site to within 0.2%, on a
//! shard loaded with a different routing range -- so the attribution is the same measurement, and
//! only the claim built on top of it moves.
//!
//! AND THE INTEGRAL AS PUBLISHED IS A THOUSAND TIMES TOO LARGE. mx#1938 gives it as "about
//! 28.9 N^2 / 2k -- 1.445e12 at a million records with a round every 10,000". The constant is
//! right (28.9 there, 24.9 here) and the formula is right, but `N^2 / 2k` at `N = 1e6`, `k = 1e4`
//! is 5e7, so 28.9 of them is **1.445e9**. Asserted below with mx#1938's own constant so a reader
//! quoting the exponent fails here.
//!
//! # THE RESTART CASE
//!
//! mx#1938 flagged that dump SELECTION does not survive a restart, and that this binds anyone
//! driving more of the round from the dirty set. It binds hard. After a restart the dirty set is
//! empty, so a walk scoped to it produces the EMPTY set where the full walk produces every bucket
//! in the store. That is the losing direction at full width: not one bucket missed, all of them.
//!
//! WHY it is empty is worth stating exactly, because mx#1938 attributes it to "the
//! clear-dirty-on-load contract" and that contract is about something else. `load_shard_with`
//! does clear every BUCKET and PAGE dirty flag. It does not clear `shard.dirty_objects`, and
//! nothing does: `dirty_objects.clear()` has ZERO production call sites in this crate -- two
//! tests, and nothing else. The index is empty after a load because it is `#[serde(skip)]` on
//! `ShardState`, so a shard decoded from a served index arrives with `DirtyObjectIndex::default()`
//! and there is no clear to perform. A mutant that empties `clear()` therefore survives every
//! test here, which is reported below rather than dressed up: it cannot reach the subject.
//! `a_round_immediately_after_a_restart_would_see_no_buckets_at_all` drives the restart and
//! asserts it, so the answer to "walk everything or prove it need not" is recorded as WALK
//! EVERYTHING, with the reason measured rather than argued.

use super::*;
use crate::engine::hashing::block_routing_bucket;
use crate::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use crate::engine::storage_bucket_internals::{
    collect_live_block_entries, rebuild_bucket_first_index, refresh_bucket_runtime_flags,
};
use crate::engine::storage_reporting::bucket_storage_summaries;
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with. `TS_SHARD_END_ROUTING_SLOT=1023` is the
/// setting that cuts resident memory 45%, and 1023 is the value it sets.
const NARROW_END_BUCKET: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses, which is what mx#1934 and mx#1938 measured on.
const WIDE_END_BUCKET: u32 = u32::MAX;

/// Corpus sizes, the same two mx#1934 and mx#1938 used, so the figures here sit beside theirs.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written per batch. A write BATCH is one log record however many objects it carries.
const SEED_BATCH: usize = 500;

/// Low enough that the round dumps on every call, which is what drains the dirty set.
const DUMPING_MIN_UNDUMPED_WAL_RECORDS: u64 = 1;

// ---------------------------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------------------------

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

/// A shard loaded on an explicit routing range, which is the whole point: `load_shard` loads on
/// `0..u32::MAX` and that range is what makes the two placements identical.
fn load_on_range(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "walk-scope".to_string(),
        shard_uri: "local://walk-scope/1".to_string(),
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

/// Write `count` records from `from`, returning the keys, so the caller holds its OWN list of what
/// is outstanding. That list is the witness the engine's dirty set is compared against, and it
/// shares no state with `shard.dirty_objects`.
fn seed(engine: &TemporalEngine, from: usize, count: usize) -> Vec<String> {
    let mut written = Vec::with_capacity(count);
    let mut index = from;
    let to = from + count;
    while index < to {
        let end = (index + SEED_BATCH).min(to);
        let mut commands = Vec::new();
        for cursor in index..end {
            let key = format!("k-{cursor:08}");
            commands.push(Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 128],
            });
            written.push(key);
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
    written
}

fn round_request(min_undumped_wal_records: u64) -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        eviction_memory_pressure_threshold: 1,
        eviction_batch_limit: 4,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        index_gc_index_log_bytes_threshold: 1,
        ..StorageManagerCycleRequest::default()
    }
}

fn one_round(engine: &TemporalEngine) -> StorageManagerCycleReport {
    engine.run_storage_manager_cycle(round_request(DUMPING_MIN_UNDUMPED_WAL_RECORDS))
}

/// The shard's own routing range, read from the authority the engine reads it from.
fn routing_range(engine: &TemporalEngine) -> (u32, u32) {
    engine
        .infos
        .read()
        .expect("info lock poisoned")
        .get(&1)
        .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
        .unwrap_or((0, u32::MAX))
}

// ---------------------------------------------------------------------------------------------
// The old-format door, and the two placements
// ---------------------------------------------------------------------------------------------

/// An address as an index written before the routing-bucket field carried it holds one.
///
/// Built by the engine's OWN serde impl: serialize the address to its wire shape, ADD the `rs` key
/// an older index carried, and hand it back to the decoder. It is the same code path that reads a
/// served index off disk -- `decode_index_bytes` takes plain JSON explicitly, "the served index as
/// an older build wrote it".
///
/// IT USED TO DROP THE KEY INSTEAD, and the swap is the whole point of the slot's retirement:
/// `BlockAddressWire` no longer declares `rs`, so what answers is not `#[serde(default)]` on a
/// field but serde's tolerance of a key no field claims.
///
/// Deliberately NOT `set_routing_bucket(None)`: a test that reaches for the setter proves only
/// that the setter works. This proves the DECODER produces the state.
fn as_an_older_build_wrote_it(address: &ElementEntry) -> ElementEntry {
    let mut wire = serde_json::to_value(address).expect("an address serializes to its wire shape");
    let object = wire
        .as_object_mut()
        .expect("the address wire shape is a JSON object");
    // INVERTED WITH THE SLOT'S DEPARTURE. This used to REMOVE `rs`, because a current build still
    // wrote one and an older index was the shape without it. `BlockAddressWire` has retired the
    // field, so the shapes have swapped places: a current address carries no `rs`, and an older
    // index is the one that carries it. The guard inverts with the helper and keeps doing the same
    // job -- failing if this stops producing a shape DIFFERENT from what the engine writes today.
    let already_there = object.remove("rs");
    assert!(
        already_there.is_none(),
        "a current address still carries an `rs` key, so the slot was not retired and this helper \
         is adding a key that is already present: {object:?}"
    );
    object.insert("rs".to_string(), serde_json::json!(513u32));
    serde_json::from_value(wire)
        .expect("the engine's decoder still accepts an address carrying the retired `rs`")
}

/// WHERE A RANGE WOULD PLACE A PAGE, which is not the same thing as where the summary walk credits
/// it. Read the name as "the placement a derivation over this range produces".
///
/// IT USED TO MIRROR `bucket_storage_summaries`' live-page loop, and it deliberately no longer does.
/// That loop read `address.routing_bucket()` with this derivation as the fallback; an address carries
/// no routing bucket now, and the loop reads the entry's FILING with the derivation as the fallback --
/// so mirroring it here would make every comparison below an identity. What this file compares is two
/// RANGES as functions of one key, and that is what this helper is: one of the two.
fn summarised_under(entry: &crate::engine::storage_bucket_internals::LiveBlockEntry, start: u32, end: u32) -> u32 {
    block_routing_bucket(&entry.object_key, start, end)
}

/// Where the dirty set files an object: always the shard's own range, never the address.
/// This mirrors `mark_object_dirty` and the synchronous mark in `engine.rs`.
fn dirty_set_would_file(object_key: &str, start: u32, end: u32) -> u32 {
    block_routing_bucket(object_key, start, end)
}

/// The buckets `bucket_map` actually holds pages in -- where the index FILED them.
fn filed_buckets(shard: &crate::engine::state::ShardState) -> BTreeSet<u32> {
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| !bucket.block_index.is_empty())
        .map(|(routing_bucket, _)| *routing_bucket)
        .collect()
}

/// The buckets the summary walk credits at least one live page to.
fn summarised_buckets(shard: &crate::engine::state::ShardState, start: u32, end: u32) -> BTreeSet<u32> {
    bucket_storage_summaries(shard, start, end)
        .into_iter()
        .filter(|summary| summary.block_ref_count > 0)
        .map(|summary| summary.routing_bucket)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The scoped walk, as a MEASUREMENT. No engine code moves.
// ---------------------------------------------------------------------------------------------

/// What `bucket_storage_summaries`' live-page half would produce if it visited only the pages of
/// the dirty buckets, and how many pages it visited to produce it.
///
/// Scoped the only way a scoped walk can be: straight into `bucket_map` at the dirty bucket ids,
/// never materialising the rest of the store. The page is still CREDITED the way the full walk
/// credits it, so the two can differ only in which pages were looked at.
fn scoped_live_block_summaries(
    shard: &crate::engine::state::ShardState,
    start: u32,
    end: u32,
) -> (BTreeMap<u32, u64>, u64) {
    let dirty: Vec<u32> = shard.dirty_objects.bucket_ids().collect();
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut credited = BTreeMap::<u32, u64>::new();
    let mut visits = 0u64;
    for routing_bucket in dirty {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        for (handle, _block) in &bucket.block_index {
            visits += 1;
            let credited_to =
                block_routing_bucket(named.at(bucket.routing_bucket, *handle), start, end);
            *credited.entry(credited_to).or_default() += 1;
        }
    }
    (credited, visits)
}

/// What the full walk produces, as the same shape, so the two are comparable element by element.
fn full_live_block_summaries(
    shard: &crate::engine::state::ShardState,
    start: u32,
    end: u32,
) -> BTreeMap<u32, u64> {
    let mut credited = BTreeMap::<u32, u64>::new();
    for summary in bucket_storage_summaries(shard, start, end) {
        if summary.block_ref_count > 0 {
            credited.insert(summary.routing_bucket, summary.block_ref_count);
        }
    }
    credited
}

/// A RELEASED bucket is a THIRD placement rule, and the one thing that keeps it out of the way.
///
/// A released bucket holds no page entries in `bucket_map` at all -- `collect_live_block_entries`
/// supplements it from the model maps, keeping only pages whose address carries an explicit
/// routing bucket equal to the bucket's own, with NO hash fallback. A walk scoped to
/// `bucket_map[dirty bucket]` would therefore find NOTHING for a released bucket.
///
/// What makes that safe is a precondition on the other side: `release_bucket_blocks` refuses any
/// bucket that is dirty. So a released bucket is never in the dirty set and a scoped walk never
/// tries to enter one. That is the invariant, and it is asserted rather than assumed -- this
/// started life as a NOT-EXERCISED assertion and went red on the first run, because a dumping
/// round releases buckets. The count is reported so a release that stopped happening shows up as
/// a denominator that collapsed rather than as a clean pass.
fn assert_no_released_bucket_is_dirty(shard: &crate::engine::state::ShardState) -> usize {
    let released = &shard.bucket_index.released_buckets;
    let dirty_and_released: Vec<u32> = shard
        .dirty_objects
        .bucket_ids()
        .filter(|routing_bucket| released.contains(routing_bucket))
        .collect();
    assert!(
        dirty_and_released.is_empty(),
        "{} buckets are BOTH dirty and released: {:?}. A released bucket holds no entry in \
         `bucket_map`, so a walk scoped to the dirty set opens it and finds nothing -- and \
         `release_bucket_blocks` is supposed to refuse a dirty bucket precisely so that cannot \
         happen. Every scoped-walk figure in this file rests on this.",
        dirty_and_released.len(),
        &dirty_and_released[..dirty_and_released.len().min(5)]
    );
    released.len()
}

// =============================================================================================
// 1. The two placements, and why a wide fixture cannot see the difference
// =============================================================================================

/// THE TWO FALLBACK RANGES ARE TWO DIFFERENT FUNCTIONS, ON EVERY KEY A SHARD HOLDS.
///
/// Stated as arithmetic before any engine runs, because it is arithmetic: `bucket_for_object`
/// reduces the key's hash modulo the bucket COUNT and adds the start, so narrowing the range from
/// 2^32 buckets to 1,024 changes the answer for every key whose hash exceeds 1,024 -- which is
/// every key, at a 64-bit hash.
///
/// rust-internal: arithmetic on the engine's own routing-bucket hash, no product behaviour
#[test]
fn the_two_fallback_ranges_are_different_functions_of_the_same_key() {
    const KEYS: usize = 2_000;
    let keys: Vec<String> = (0..KEYS).map(|index| format!("k-{index:08}")).collect();
    assert_eq!(keys.len(), KEYS, "the fixture built no keys to compare");

    let narrow_disagreements = keys
        .iter()
        .filter(|key| {
            block_routing_bucket(key, 0, NARROW_END_BUCKET)
                != block_routing_bucket(key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        narrow_disagreements, KEYS,
        "on a shard loaded with the production narrow range, `block_routing_bucket(key, start, \
         end)` and `block_routing_bucket(key, 0, u32::MAX)` place {narrow_disagreements} of \
         {KEYS} keys in different buckets. Anything other than all of them means this fixture's \
         keys are not what a shard holds and every count below is drawn from the wrong population."
    );

    // The control: the same comparison on the range mx#1934 and mx#1938 measured on.
    let wide_disagreements = keys
        .iter()
        .filter(|key| {
            block_routing_bucket(key, 0, WIDE_END_BUCKET)
                != block_routing_bucket(key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        wide_disagreements, 0,
        "on `0..u32::MAX` the two expressions are the same function of the same arguments, so a \
         disagreement here would mean `block_routing_bucket` is not deterministic"
    );
    println!(
        "  placements disagree on {narrow_disagreements}/{KEYS} keys at end={NARROW_END_BUCKET}, \
         {wide_disagreements}/{KEYS} at end=u32::MAX"
    );
}

/// THE FIXTURE mx#1938 MEASURED ON COULD NOT HAVE REPORTED ANYTHING BUT ZERO.
///
/// Its shard is loaded by `engine.load_shard(1)`, which is `0..u32::MAX`. The two placements are
/// then the same expression, so "zero misfiled pages at both sizes" is an identity. This asserts
/// BOTH halves -- the zero, and the identity that explains it -- so the reading is attributable.
///
/// rust-internal: prices the engine's own bucket placement, no product behaviour
#[test]
fn a_wide_range_fixture_cannot_tell_the_two_placements_apart() {
    const RECORDS: usize = 800;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    engine.load_shard(1);

    let (start, end) = routing_range(&engine);
    assert_eq!(
        (start, end),
        (0, WIDE_END_BUCKET),
        "`load_shard` is supposed to load on the whole range; this test's whole point is that it \
         does, so a change here invalidates the reading rather than the code"
    );

    let keys = seed(&engine, 0, RECORDS);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let _released = assert_no_released_bucket_is_dirty(shard);

    let entries = collect_live_block_entries(shard);
    assert!(
        !entries.is_empty(),
        "the fixture stored no live page, so the misfiled count below is zero because there is \
         nothing to misfile"
    );

    let misfiled = entries
        .iter()
        .filter(|entry| {
            summarised_under(entry, start, end)
                != block_routing_bucket(&entry.object_key, 0, WIDE_END_BUCKET)
        })
        .count();
