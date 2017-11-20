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
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-readers");

    let key = "cte-set";
    let members: Vec<Vec<u8>> = (0..3).map(|m| format!("m{m}").into_bytes()).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let victim = members[1].clone();
    let victim_component = hex::encode(&victim);
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: victim.clone(),
        },
    );

    // DENOMINATOR FIRST: there must actually BE a tombstone entry, or every reader below answers
    // correctly for the trivial reason that there is nothing for it to answer from.
    let (live, tombstoned) = entry_counts(&engine, "set", key);
    println!("=== after removing one of three ===");
    println!("  entries: {live} live, {tombstoned} tombstone");
    assert_eq!(
        1, tombstoned,
        "DENOMINATOR: {tombstoned} tombstone entries, so this module is not testing what it claims"
    );
    // THREE, NOT TWO: A REMOVAL DOES NOT RETIRE THE LIVE ENTRY OVER THE PAGE IT VACATES.
    //
    // This asserted 2 -- one entry per surviving MEMBER. Under one entry a page the live entries
    // are the object's PAGES, and a gated removal deliberately keeps the page entry because the
    // page may still hold siblings; the tombstone is added BESIDE it. So the live count does not
    // fall on a removal, and this fixture's three members are three pages before and after.
    //
    // THE ASYMMETRY IS KNOWN AND RECORDED, not discovered here:
    // `index_entry_names_a_block`'s doc calls it "a defect of footprint rather than of answers" and
    // names the fix -- ask the resident map whether any sibling is still on the vacated page,
    // which `RecordedMap::block_an_element_vacates` is -- and why a removal cannot reach it, since
    // it enters the index through a door that takes no element type. What this arm still holds is
    // the half that is about ANSWERS: every reader below must refuse to answer FROM the tombstone,
    // and that is asserted member by member rather than by a count.
    assert_eq!(
        3, live,
        "{live} live entries after removing one of three. One entry a page means the live count is \
         the PAGE count, which a removal does not reduce -- if this is 2 the removal has started \
         retiring the vacated page's entry, which is the footprint fix the doc describes, and the \
         comment above it should be read rather than this number adjusted"
    );

    // READER 1: the page index reader every serving path resolves through.
    let named: BTreeSet<Vec<u8>> =
        crate::engine::bucket_store::bucket_index_component_block_addresses(
            &engine
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&1)
                .expect("shard is loaded"),
            "set",
            key,
        )
        .iter()
        .filter_map(|(component, _)| component.as_deref().and_then(|c| hex::decode(c).ok()))
        .collect();
    let returned = crate::engine::bucket_store::bucket_index_component_block_addresses(
        &engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&1)
            .expect("shard is loaded"),
        "set",
        key,
    )
    .len();
    println!("  index reader returns {returned} pair(s), naming {} member(s)", named.len());

    // THE QUESTION IS SHARPER NOW: NOT "DOES IT NAME THE REMOVED MEMBER" BUT "DOES IT RETURN THE
    // TOMBSTONE AT ALL".
    //
    // This asserted the reader named 2 of 2 surviving members and did not name the removed one.
    // The reader's pairs carry no name -- an entry has none -- so it names nobody, and an
    // assertion that it does not name the VICTIM would pass for the same reason it would pass for
    // every other member: vacuously.
    //
    // What can still fail, and is what the arm was really about: the reader must not hand back the
    // tombstone's page. It filters `!page.deleted`, so it returns one pair per LIVE page -- three
    // here, since a removal retains the vacated page's entry -- and never the fourth. If that
    // filter were lost this returns 4 and reddens, which is the defect "answering from a tombstone
    // entry" actually consists of.
    assert_eq!(
        0,
        named.len(),
        "the index reader named {} member(s). An entry carries no element name, so every pair it \
         returns must be nameless -- a name here means a component has come back onto the entry",
        named.len()
    );
    assert_eq!(
        live, returned,
        "the index reader returned {returned} pair(s) where {live} entries are live and \
         {tombstoned} tombstoned. It must return the LIVE pages and not the tombstone; {} would \
         mean it has stopped filtering `deleted` and is answering from the tombstone entry",
        live + tombstoned
    );

    // READER 2 and 3: the listing, and the count that IS the listing counted (SMEMBERS / SCARD).
    let listed = match read(
        &engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    println!("  SMEMBERS returns {} member(s), SCARD is that counted", listed.len());
    assert_eq!(2, listed.len(), "SMEMBERS returned {}", listed.len());
    assert!(
        !listed.contains(&victim),
        "SMEMBERS listed the removed member"
    );

    // READERS 4 and 5: EXISTS and TTL, which #2025 found answering for a removed element. Driven at
    // the boundary that made them wrong -- removing the LAST member, where the key itself must go.
    for member in [members[0].clone(), members[2].clone()] {
        write(
            &engine,
            Command::SetRemove {
                key: key.to_string(),
                member,
            },
        );
    }
    let (live_after, tombstoned_after) = entry_counts(&engine, "set", key);
    let exists = match read(
        &engine,
        Command::CommonExists {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("CommonExists answered {other:?}"),
    };
    let emptied = match read(
        &engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    println!("  after removing all three: {live_after} live, {tombstoned_after} tombstone entries");
    println!("  EXISTS answers {exists}, SMEMBERS returns {} member(s)", emptied.len());
    assert_eq!(
        0, live_after,
        "{live_after} live entries remain after every member was removed"
    );
    assert_eq!(
        3, tombstoned_after,
        "DENOMINATOR: {tombstoned_after} tombstone entries for three removals, so EXISTS below is \
         not being asked in the presence of the thing it must ignore"
    );
    assert_eq!(
        0, exists,
        "EXISTS answered {exists} for a key whose every member was removed -- which is #2025's \
         defect exactly, arriving from the tombstone entry instead of the resident map"
    );
    assert!(
        emptied.is_empty(),
        "SMEMBERS returned {} member(s) for an emptied key",
        emptied.len()
    );
}

/// rust-internal: drives SetAdd/SetRemove and counts index entries
#[test]
fn a_removal_retains_one_entry_and_nothing_yet_collects_it() {
    // ONE ENTRY PER LIVE MEMBER IS THE UNGATED COST MODEL. Gated, the live entry is retained
    // BESIDE the tombstone rather than replaced by it -- dropping it would take every other
    // member of the page with it -- so the counts here are not the gated ones. See
    // `removal_the_index_can_find`.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-cost");

    // THE COST IS STATED AS A NUMBER RATHER THAN DESCRIBED. A removal used to FREE an entry and now
    // retains one, so a workload that adds and removes distinct members holds one entry per element
    // ever written rather than one per live element.
    const MEMBERS: usize = 8;
    const REMOVED: usize = 3;
    let key = "cte-cost-set";
    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|m| format!("member-{m:03}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let (live_before, tombstoned_before) = entry_counts(&engine, "set", key);
    assert_eq!(
        (MEMBERS, 0),
        (live_before, tombstoned_before),
        "the seed produced {live_before} live and {tombstoned_before} tombstone entries"
    );

    for member in members.iter().take(REMOVED) {
        write(
            &engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let (live_after, tombstoned_after) = entry_counts(&engine, "set", key);
    println!("=== the cost of a removal, in entries ===");
    println!(
        "  {MEMBERS} members written, {REMOVED} removed: {live_after} live + {tombstoned_after} \
         tombstone = {} entries, where the live membership is {}",
        live_after + tombstoned_after,
        MEMBERS - REMOVED
    );
    // THE LIVE COUNT IS THE PAGE COUNT, AND A REMOVAL DOES NOT REDUCE IT.
    //
    // This asserted `MEMBERS - REMOVED` -- one live entry per surviving member. Under one entry a
    // page the live entries are the object's PAGES, and a removal keeps the page entry and adds a
    // tombstone beside it, so the live count stays at `MEMBERS` and the TOMBSTONE count is what
    // moves. Asserting the per-member figure made this an entry-per-element test wearing a
    // page-named label.
    //
    // THE COST THIS MODULE IS NAMED FOR IS STILL ITS SUBJECT, and it is now stated more sharply
    // than before: `MEMBERS` live plus `REMOVED` tombstones for a live membership of
    // `MEMBERS - REMOVED`, which is the retained-entry cost nothing yet collects. The printed line
    // above says exactly that, and the two assertions here are its two halves.
    assert_eq!(
        MEMBERS, live_after,
        "{live_after} live entries where {MEMBERS} pages were written. A removal retains the page \
         entry, so the live count must not fall -- if it has, the vacated page's entry is being \
         retired and this module's cost figure needs remeasuring rather than this number adjusting"
    );
    assert_eq!(
        REMOVED, tombstoned_after,
        "{tombstoned_after} tombstone entries for {REMOVED} removals -- one per removal is the cost \
         this change pays, and a different number means removals are not filing one each"
    );
    // THE TOTAL IS `MEMBERS + REMOVED`, NOT `MEMBERS`, AND THE DIFFERENCE IS THE WHOLE COST.
    //
    // This asserted the total was unchanged at `MEMBERS`, which was right while a removal REPLACED
    // a live entry with a tombstone -- net zero. A removal now ADDS a tombstone beside a retained
    // live entry, so each one costs a whole entry and the total rises by `REMOVED`.
    //
    // Stated as the sum of the two halves above rather than as a literal, so the three assertions
    // cannot drift apart: if a removal ever starts retiring the vacated page's entry, the live
    // half falls and this total falls with it, and both say so instead of one absorbing it.
    assert_eq!(
        MEMBERS + REMOVED,
        live_after + tombstoned_after,
        "the total entry count is {} where {MEMBERS} pages were written and {REMOVED} removed. A \
         removal adds a tombstone rather than replacing an entry, so the cost is exactly one entry \
         per removed element and the total must be their sum",
        live_after + tombstoned_after
    );

    // A COMPACTION ROUND REBUILDS THE INDEX, AND THE TOMBSTONES MUST SURVIVE IT.
    //
    // THE HARD PART, AND IT WAS A REFUTATION BEFORE IT WAS A FIX. `compact_shard_blocks` calls
    // `rebuild_bucket_block_ownership` directly, twice; that function does `bucket_map.clear()` and
    // rebuilds from `collect_model_live_block_entries`, which walks the RESIDENT MODEL MAPS. Those hold
    // only live elements -- a removed member is gone from `shard.sets` by construction -- so a rebuild
    // produced one entry per live element and NONE for a removal, and every round erased the entries
    // that kept the removals readable from the pages. A removal was durable in the pages only until the
    // next round.
    //
    // It is fixed by CARRYING the tombstone entries across the clear rather than re-deriving them,
    // because they cannot be re-derived: nothing outside the index knows a removal happened except the
    // WAL, and a rebuild is not a replay. `tombstones_refiled_count` is the instrument, and it is read
    // here rather than inferred -- "the count did not change" is also what a round that never rebuilt
    // reports.
    //
    // A NOTE ON THE FIRST ATTRIBUTION, kept because it was wrong in an instructive way: it blamed
    // `promote_model_maps_to_bucket_index_authority`, whose counters read ZERO over this round. The
    // mechanism was right and the ENTRY POINT was not, and a fix aimed at the wrapper would have left
    // two direct call sites doing it anyway.
    //
    // The source is still asserted, because it is what makes carrying them necessary rather than
    // merely sufficient: the resident map must hold the live members and NONE of the removed ones.
    crate::engine::reset_container_batch_counts();
    crate::engine::storage_bucket_internals::reset_tombstones_refiled_count();
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let refiled = crate::engine::storage_bucket_internals::tombstones_refiled_count();
    let (live_folded, tombstoned_folded) = entry_counts(&engine, "set", key);
    let (resident_live, resident_holds_removed) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let set = shard.sets.get(key);
        (
            set.map_or(0, |members| members.len()),
            set.map_or(false, |members| {
                members.iter().any(|(member, _)| {
                    (0..REMOVED).any(|m| *member == format!("member-{m:03}").into_bytes())
                })
            }),
        )
    };
    println!(
        "  after a fold round ({batches} batches, {folded} pages folded): {live_folded} live + \
         {tombstoned_folded} tombstone; {refiled} tombstone(s) carried across the rebuild"
    );
    println!(
        "  the resident map the rebuild reads: {resident_live} member(s), holds a removed one = \
         {resident_holds_removed}"
    );
    assert!(
        batches > 0 && folded > 0,
        "DENOMINATOR: the round folded nothing ({batches} batches, {folded} pages), so it says \
         nothing about what a round does to a tombstone"
    );
    assert!(
        refiled > 0,
        "DENOMINATOR: the round carried {refiled} tombstones across a rebuild, so either it did not \
         rebuild at all -- in which case the survival below is not evidence of anything -- or the \
         carry-over is not running"
    );
    assert_eq!(
        REMOVED, tombstoned_folded,
        "A FOLD ROUND ERASED THE TOMBSTONE ENTRIES: {tombstoned_folded} survive of {REMOVED}. The \
         rebuild reads the resident model maps, which cannot express a removal, so the entries have to \
         be CARRIED across `bucket_map.clear()`. Without that a removal stops being recorded in the \
         pages at the next compaction round and a page-derived membership resurrects the element."
    );
    // AND HERE IS THE MEASUREMENT THAT CHANGES THIS MODULE'S COST STORY: THE REBUILD COLLECTS THE
    // RETAINED ENTRIES.
    //
    // This asserted `MEMBERS - REMOVED` live entries after the round -- one per surviving member.
    // It is ONE. The rebuild re-derives from the resident maps through
    // `emit_one_entry_a_block`, which emits at most one entry per distinct PHYSICAL PAGE, and the
    // round folded the survivors onto a single page. So the rebuild files one live entry, and the
    // stale live entries that a removal left over its vacated pages are GONE.
    //
    // THE MODULE'S TITLE SAYS "NOTHING YET COLLECTS IT", AND THAT IS NOW TRUE ONLY UNTIL THE NEXT
    // RE-DERIVATION. Before the round this fixture holds `MEMBERS` live entries for
    // `MEMBERS - REMOVED` members -- asserted above, and that IS the retained-entry cost. After
    // the round it holds one. The cost is therefore TRANSIENT: it accrues per removal and is
    // cleared by the next compaction round, which is a materially smaller claim than an entry per
    // removal that nothing collects, and it is worth having measured rather than assumed in either
    // direction.
    //
    // Pinned against the folded PAGE count rather than a literal 1, so a fixture that folds
    // differently cannot make this vacuous -- and so that it still reads as "one entry a page".
    let live_blocks_folded = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        shard
            .bucket_index
            .bucket_map
            .values()
            .flat_map(|bucket| {
                bucket
                    .block_index
                    .iter()
                    .map(move |(handle, block)| (bucket.routing_bucket, *handle, block))
            })
            .filter(|(routing_bucket, handle, block)| {
                !block.deleted
                    && block.model_id.as_str() == "set"
                    && &**named.at(*routing_bucket, *handle) == key
            })
            .map(|(_, _, block)| {
                (
                    block.address.block_slab_id(),
                    block.address.offset(),
                    block.address.length(),
                )
            })
            .collect::<BTreeSet<_>>()
            .len()
    };
    assert!(
        live_blocks_folded < MEMBERS - REMOVED,
        "DENOMINATOR: the survivors resolve to {live_blocks_folded} live page(s) for {} members, so \
         nothing folded and 'one entry a page' is the same number as one entry an element here",
        MEMBERS - REMOVED
    );
    assert_eq!(
        live_blocks_folded, live_folded,
        "the rebuild produced {live_folded} live entries over {live_blocks_folded} live page(s). It \
         emits at most one per distinct page, so these must agree -- more means the stale entries a \
         removal left over its vacated pages survived the round, which is the retained-entry cost \
         this module prices and which the round is what clears"
    );
    // WHY CARRYING THEM IS NECESSARY, asserted at the source rather than argued.
    assert_eq!(
        MEMBERS - REMOVED,
        resident_live,
        "the resident map holds {resident_live} members where {} are live",
        MEMBERS - REMOVED
    );
    assert!(
        !resident_holds_removed,
        "THE RESIDENT MAP HOLDS A REMOVED MEMBER. If it did, a rebuild from it could re-derive the \
         removal and carrying the entries over would be unnecessary -- so this assertion is what makes \
         the carry-over the only available mechanism rather than one of two."
    );
}

/// rust-internal: drives SetAdd/SetRemove and reads the bucket tombstone index
#[test]
fn a_keys_last_element_still_files_its_object_id() {
    // THE LAST-ELEMENT CASE IS DEFINED BY THE LIVE ENTRY COUNT REACHING ZERO, which the gated
    // path does not do: it keeps the page entry. The test's own floor says so. The gated
    // removal across a reload is `gated_removal`'s subject.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-lastel");

    let key = "cte-last-set";
    let object_id = crate::engine::stable_block_object_id(1, "set", key);
    let members: Vec<Vec<u8>> = (0..2).map(|m| format!("m{m}").into_bytes()).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }

    // FILED WHERE IT IS READ, NOT MERELY FILED SOMEWHERE.
    //
    // This asked `bucket_map.values().any(|bucket| ...contains(id))`, which any fix filing the id in
    // some arbitrary bucket would satisfy. `object_manager::runtime_report` reads the index in the
    // PAGE's OWN bucket -- `deleted_object_index.contains(page.object_id())` inside the loop over
    // that bucket's pages -- so an id filed anywhere else answers nothing. Scoped to a bucket that
    // actually holds a page of this object, which is the set the report consults.
    let filed = |engine: &TemporalEngine| -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        shard.bucket_index.bucket_map.values().any(|bucket| {
            bucket.deleted_object_index.contains(&object_id)
                && bucket
                    .block_index
                    .iter()
                    .any(|(handle, _)| &**named.at(bucket.routing_bucket, *handle) == key)
        })
    };

    assert!(
        !filed(&engine),
        "DENOMINATOR: the object id is filed as deleted before anything was removed"
    );

    // NOT THE LAST ELEMENT. The id must NOT be filed -- a deleted element is not a deleted object,
    // and filing it here would make every survivor read as a deleted block ref.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: members[0].clone(),
        },
    );
    let after_first = filed(&engine);
    println!("=== the object tombstone ===");
    println!("  after removing one of two, the object id is filed = {after_first}");
    assert!(
        !after_first,
        "the object id was filed after ONE of two elements was removed"
    );

    // THE LAST ELEMENT. Now it must be filed -- and this is the half a retained tombstone entry would
    // have broken: the filter that decides it asks whether any page still carries the id, and the
    // removed element's own tombstone would have answered yes forever. It asks about LIVE pages.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: members[1].clone(),
        },
    );
    let after_last = filed(&engine);
    let (live, tombstoned) = entry_counts(&engine, "set", key);
    println!(
        "  after removing the last, the object id is filed = {after_last} ({live} live, \
         {tombstoned} tombstone entries)"
    );
    assert_eq!(
        0, live,
        "{live} live entries remain, so this is not the last-element case"
    );
    assert_eq!(
        2, tombstoned,
        "DENOMINATOR: {tombstoned} tombstone entries, so the filter below is not being tested in \
         the presence of the entries that would defeat it"
    );
    assert!(
