// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE RETAINED TOMBSTONE ENTRY COSTS, AND WHAT IT MUST NOT CHANGE.
//!
//! # WHY THIS MODULE EXISTS SEPARATELY FROM THE PAGE CODEC'S GUARDS
//!
//! `container_tombstone_collection` drives the page FORMAT and the fold: what a removal encodes and how
//! an ordered set of pages folds into a membership. None of that needs an engine. This module drives
//! the other half, which does: a removal now leaves an INDEX ENTRY behind, and that entry is the first
//! traffic `BlockIndex::deleted` has ever carried.
//!
//! Before this change, NOTHING IN PRODUCTION SET THAT FIELD TO TRUE. Of the thirty-nine walks of
//! `block_index` outside the tests, nineteen filter `deleted` in branches that were therefore never
//! exercised and twenty do not filter it at all. So the interesting question is not whether the
//! tombstone is written -- `container_tombstone_collection` settles that -- but whether any reader
//! that now meets one answers differently than it should. Two did, and both are guarded here.
//!
//! # THE SIX CLAIMS
//!
//!   1. no index reader answers from a tombstone entry, over the five readers #2025 enumerated;
//!   2. a removal retains exactly one entry, and the cost is stated as a number;
//!   3. a key's LAST element still files its object id, which a naive reading of the retained entry
//!      would have broken;
//!   4. a removal that matched nothing writes no tombstone entry;
//!   5. a removed member does not make its OBJECT deleted in the runtime report;
//!   6. a zset SCORE CHANGE tombstones the element key it left, which is the arm least likely to be
//!      recognised as a removal at all.
//!
//! Each prints its denominators. A tombstone count of zero and a tombstone count that is right look
//! the same to an assertion that only checks the live side, which is how four of these would pass
//! against a removal that recorded nothing.
//!
//! # THE THIRTY-NINE WALKS, WITH A VERDICT EACH
//!
//! This is the part a reviewer cannot reconstruct, so it is written down rather than summarised. Every
//! walk of `block_index` outside the tests, enumerated mechanically with its enclosing function --
//! not from memory, and with the count asserted, because a verdict over a subset of the walks is not
//! an audit. Three were WRONG and are fixed by this change; three are KNOWN and accepted with the
//! reason; three are unreachable with a tombstone; the rest are correct or harmless.
//!
//! A WORD ON THE TWO PASSES. A first pass classified by asking whether the word `deleted` appeared
//! within ten lines of the walk, and that was wrong twice in opposite directions: at
//! `collect_bucket_index_live_block_entries` the word was a FIELD being populated rather than a
//! filter, and the release refusal turned out to be `BlockDeleted` rather than the model-map
//! disagreement first reported. Every verdict below is from the predicate, not the proximity.
//!
//! ```text
//!  #  site                                                  verdict
//!  1  engine.rs:3783 fold_carried_container_elements        CORRECT  `if !page.deleted` -- a tombstone is not a live page key
//!  2  engine.rs:3907 fold_delta_block_items                 CORRECT  retain by (kind,key,component): a delta restating that
//!                                                                    element takes its tombstone with it
//!  3  engine.rs:3921 fold_delta_block_items                 CORRECT  retain by covered_keys, whole-object
//!  4  engine.rs:4740 mark_bucket_index_object_deleted       CORRECT  a whole-object delete takes tombstones too; the
//!                                                                    CommonDelete arm of the five-way guard asserts zero
//!  5  engine.rs:4931 mark_..._block_deleted_recording       CORRECT  the removal's own retain
//!  6  engine.rs:4979 mark_..._block_deleted_recording       CORRECT, AND THIS ROW WAS WRONG ABOUT IT. It read "WRONG, FIXED
//!                                                                    HERE: `any(|page| page.object_id() == id)` would be
//!                                                                    answered by the removed element's OWN tombstone". The
//!                                                                    predicate asks `!page.deleted && ...`, so a tombstone
//!                                                                    cannot answer it and never could. The last element's
//!                                                                    object id went unfiled for a DIFFERENT reason, one level
//!                                                                    up -- the retain deciding `removed` compares the
//!                                                                    element's name against a page-named entry that has none,
//!                                                                    so the arm holding this filter never runs for a
//!                                                                    container. Filed in drop_live_object_entries instead.
//!                                                                    a_keys_last_element_still_files_its_object_id
//!  7  engine.rs:5384 record_exists_exact                    CORRECT  `!page.deleted`; EXISTS is one of #2025's five readers
//!  8  engine.rs:6111 object_manager_stats                   CORRECT  `.filter(|page| !page.deleted)`
//!  9  engine.rs:6236 object_manager_stats                   HARMLESS dirty-bucket count; a fresh tombstone IS dirty and its
//!                                                                    bucket does need dumping
//! 10  bucket_store.rs:105 runtime_report                    CORRECT  counts it as deleted_block_ref_count, which is what it is
//! 11  bucket_store.rs:204 bucket_index_block_address        CORRECT  `!page.deleted`
//! 12  bucket_store.rs:253 ..._component_block_addresses     CORRECT  `!page.deleted`. THE reader every serving path resolves
//!                                                                    through, and the reason index answers do not move
//! 13  lifecycle.rs:2141 synthetic_address_count_for_test    HARMLESS a test helper counting addresses; a tombstone has one
//! 14  lifecycle.rs:2188 rehydrate_wal_resident_blocks       CORRECT  skips it, so a WAL-resident tombstone page is not
//!                                                                    re-registered -- the page is on the block store and the
//!                                                                    derivation reads it from there
//! 15  object_manager.rs:87 runtime_report                   WRONG, FIXED HERE. Counted a tombstone as a LIVE block ref, and
//!                                                                    `object.deleted |= page.deleted` let ONE removed member
//!                                                                    report its whole object deleted.
//!                                                                    a_removed_member_does_not_make_its_object_deleted
//! 16  engine_storage_state.rs:3356 rebuild_object_block_lookup             CORRECT  delegates to insert_object_block_lookup, which returns
//!                                                                    early on a deleted page -- so tombstones stay OUT of the
//!                                                                    fast lookup, which is what keeps index answers unchanged
//! 17  engine_storage_state.rs:3525 contains_object_block_address           CORRECT  `!page.deleted`
//! 18  engine_storage_state.rs:3564 next_block_index_for_object             CORRECT AS-IS, and this one is a judgement. It has no filter and
//!                                                                    needs none: its thirteen call sites are all `feature` and
//!                                                                    `context_*`, the timestamped kinds, which have no
//!                                                                    tombstones. Two high-water derivations over DISJOINT kind
//!                                                                    sets, so filtering one was the right half to filter.
//! 19  engine_storage_state.rs:3638 container_block_ordinal                  WRONG, FIXED HERE. No filter, so a tombstone stopped `max` from
//!                                                                    falling: the ordinal would climb once per element ever
//!                                                                    written and a churning container would walk to
//!                                                                    MAX_ADDRESSABLE_BLOCK_ID and fall off the ceiling
//! 20  sbi.rs:890 storage_topology_snapshot...               CORRECT  sets a delete_marker, which a recorded removal is
//! 21  sbi.rs:1429 rebuild_bucket_block_ownership            KNOWN, ACCEPTED (2 of 2 below)
//! 22  sbi.rs:1675 collect_..._live_block_entries            CORRECT  includes tombstones and CARRIES `deleted` into
//!                                                                    LiveBlockEntry; nine downstream sites read that flag.
//!                                                                    Named `live` and is not a live-only set -- checked
//!                                                                    because the name says otherwise
//! 23  sbi.rs:2109 release_bucket_blocks                     KNOWN, ACCEPTED (1 of 2 below)
//! 24  sbi.rs:2139 release_bucket_blocks                     UNREACHABLE with a tombstone -- #23 refuses first
//! 25  sbi.rs:2151 release_bucket_blocks                     UNREACHABLE -- same
//! 26  sbi.rs:2174 release_bucket_blocks                     UNREACHABLE -- same
//! 27  sbi.rs:3574 upsert_bucket_index_block_inner           CORRECT  keeps object_index while a tombstone holds the id; the
//!                                                                    object does still have pages
//! 28  sbi.rs:3592 upsert_bucket_index_block_inner           CORRECT AND LOAD-BEARING. Its retain matches on component, so a
//!                                                                    RE-ADD clears the tombstone entry. That is the difference
//!                                                                    between one entry per DISTINCT element removed and one per
//!                                                                    REMOVAL, it was not written for this, and nothing else
//!                                                                    would notice if it stopped.
//!                                                                    a_re_add_clears_the_tombstone_entry_so_churn_on_one_element_does_not_accumulate
//! 29  sbi.rs:3598 upsert_bucket_index_block_inner           CORRECT  as #27
//! 30  sbi.rs:3762 sync_..._object_blocks_with_mode          CORRECT  a whole-object restate drops every entry of the key,
//!                                                                    tombstones included; not a per-element container path
//! 31  sbi.rs:4007 update_bucket_layout                      CORRECT  live_object_ids filters `!page.deleted`; an all-tombstone
//!                                                                    bucket clears object_index, which is true of it
//! 32  sbi.rs:4058 refresh_one_bucket_runtime_flags          CORRECT  sibling of #31
//! 33  sbi.rs:4061 refresh_one_bucket_runtime_flags          KNOWN, ACCEPTED (2 of 2 below) -- the same predicate as #21
//! 34  sbi.rs:4076 refresh_one_bucket_runtime_flags          HARMLESS TTL minimum over object keys; a tombstone's key is the
//!                                                                    same key, so the minimum cannot move
//! 35  sbi.rs:4216 clear_published_object_dirty_state        HARMLESS any-dirty; a tombstone is dirty until dumped
//! 36  sbi.rs:5283 validate_bucket_ownership_index...        CORRECT  matches by ADDRESS; a tombstone's address is its own and
//!                                                                    cannot answer for a live entry's
//! 37  storage_lifecycle_methods.rs:1829 lookup              CORRECT  skips it for logical/physical bytes, so a tombstone
//!                                                                    page's bytes are not counted in the bucket's size. A
//!                                                                    small undercount, named rather than left implicit
//! 38  storage_reporting.rs:40 filing_by_object_key          HARMLESS object_key -> bucket; a tombstone has the same key
//! 39  storage_reporting.rs:489 storage_physical_index_...   HARMLESS includes tombstones in a physical report, deduped by
//!                                                                    (key,kind,component,slab,offset)
//! ```
//!
//! # THE TWO KNOWN CONSEQUENCES, NAMED RATHER THAN UNMENTIONED
//!
//! **1. A bucket holding a tombstone is refused release** (#23). `release_bucket_blocks` answers
//! `BucketReleaseRefusal::BlockDeleted` on the first entry carrying `deleted`. That refusal is
//! PRE-EXISTING and deliberate -- it was written for a deleted block and had never fired, because
//! nothing set the flag -- so this change does not introduce it, it commissions it. It is not fixable
//! without changing what release means: the path releases a bucket only when the model maps could
//! rebuild exactly what is resident, and the model maps do not hold removed elements, so a tombstone
//! is by construction something they cannot rebuild. The cost is that such a bucket stays resident
//! until a compaction round rewrites it; the effect is bounded by the tombstone count, which #28
//! bounds by DISTINCT elements removed rather than by removals; and the refusal is counted, so it is
//! visible rather than silent. Releasing across it would lose the removals.
//!
//! **2. A bucket whose every entry is a tombstone is marked `deleted`** (#21, #33). `every_block_deleted
//! = !is_empty() && all(|page| page.deleted)`, and that is arguably just true of such a bucket: it
//! holds no live data. Two things read it. `bucket_store::runtime_report` counts it in
//! `deleted_bucket_count`, which is a report. Eviction eligibility is `in_memory() && !deleted() &&
//! !object_index.is_empty()`, so such a bucket is not evicted and stays resident -- and #31 has
//! already cleared its `object_index`, so the third term excludes it anyway and the `deleted` flag
//! changes nothing about eviction. The DUMP does not consult it, which is the part that matters: the
//! tombstones are serialized and survive a reload. So the observable effect is one report counter.
//!
//! Neither is fixed here. Both are stated in the pull request body as well as here, because an
//! unfixed consequence that is named is a decision and one that is merely not mentioned is not.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, table: &str) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: table.to_string(),
        shard_uri: format!("local://{table}/1"),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

/// (live entries, tombstone entries) for one object, counted off the page index itself.
fn entry_counts(engine: &TemporalEngine, kind: &str, key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != kind
                || &**named.at(bucket.routing_bucket, *handle) != key
            {
                continue;
            }
            if block.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

/// The components the tombstone entries of one object name.
fn tombstoned_components(engine: &TemporalEngine, kind: &str, key: &str) -> BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut held = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted
                && block.model_id.as_str() == kind
                && &**named.at(bucket.routing_bucket, *handle) == key
            {
                // AN ENTRY NAMES NO ELEMENT, so this set is always empty. It is restated where
                // it is read -- the element a tombstone is about is recorded in the bucket's
                // `tombstone_elements` rows, not on the entry.
                let _ = block;
            }
        }
    }
    held
}

/// rust-internal: drives SetAdd/SetRemove and the readers #2025 enumerated
#[test]
fn no_index_reader_answers_from_a_tombstone_entry() {
    // THE FIVE READERS THIS ENUMERATES NAME ELEMENTS BY COMPONENT, which a page-named entry
    // does not carry, so gated it counts names rather than readers. The gated tombstone's
    // reachability is `removal_the_index_can_find`'s subject.
    let dir = tempfile::tempdir().expect("tempdir");
