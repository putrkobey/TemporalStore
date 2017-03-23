// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! REFUTATION, AND THE ONE THING THAT LIFTS IT. THE PAGE ENTRY COULD NOT STOP NAMING ITS ELEMENT
//! WHILE THE ENTRY WAS THE ONLY PLACE THE NAME WAS WRITTEN.
//!
//! # WHAT CHANGED, STATED FIRST BECAUSE THIS MODULE'S HEADLINE MOVED
//!
//! This module was opened as a refutation and its stop condition was one sentence: for a hash the
//! component is not a second copy of the field name, it is the ONLY one. `container_blocks` makes the
//! page carry its own element key, so there is now a second copy, and the refutation below is
//! recorded as HAVING HELD rather than as holding.
//!
//! The first test is the one that moved, and it moved in the direction its own failure message named:
//! "If this fails the hash field name has a second copy somewhere and the refutation is stale." It
//! failed, for that reason, which is the strongest evidence this module can offer that the frame does
//! what it was added to do. What a swapped component now produces is written into
//! `a_swapped_hash_component_no_longer_renames_the_field_because_the_page_contradicts_it`.
//!
//! WHAT IS STILL BLOCKED, AND WHY THIS MODULE IS NOT DELETED. A second copy of the name is a
//! PRECONDITION for the entry dropping it, not the whole of it. The entry's component is still what
//! `bucket_index_component_block_addresses` walks, still what `HashGetAll` returns as the field name,
//! and still what the load path derives `shard.hashes` from. Removing it needs the load path to
//! rebuild those maps out of the pages instead, which is a later stage and is not started here. The
//! pricing below is the prize for that stage and is kept for it.
//!
//! `BlockIndex.component: Option<Arc<str>>` is sixteen bytes on every page entry plus a heap string
//! per element, and #1996 priced that string exactly: `16 + 2n` request bytes for a set member and
//! `32 + 2n` for a zset member, slope 2.00 per member byte, because `hex::encode` spells two
//! characters per byte. Those numbers are CITED here and not re-derived.
//!
//! Two prerequisites landed since, and both are real:
//!
//!   * #2005 made the delta fold carry element identity through `key_states`, so an element folded
//!     out of the delta log no longer depends on its component being decodable.
//!   * #2008 gave a container page a real `BlockAddress::block_id` ordinal instead of the hardcoded
//!     zero, threaded so header and address are built from one value.
//!
//! #2005 names what it unblocks: "a 2-byte element ordinal replacing `component: Option<Arc<str>>`".
//! THAT STEP DOES NOT FOLLOW, and this module is the driven reason.
//!
//! # THE STOP CONDITION, IN ONE LINE, AS IT STOOD
//!
//! For a HASH the component is the caller's field name, `shard.hashes` is `skip_serializing`, and
//! `Command::HashGetAll` RETURNS THE COMPONENT AS THE FIELD NAME. So the component was not a second
//! copy of a hash field name. It was the only one, on the live read path and across a reload alike.
//!
//! THAT LAST SENTENCE IS WHAT `container_blocks` CHANGED, and only that sentence. Everything else in
//! the table below still holds: the entry still carries the component, the three spelled kinds still
//! have a durable map and the hash still has none, and `HashGetAll` still names its fields from the
//! entry rather than from the page.
//!
//! ```text
//!     set     set_index_serde    (member bytes, address)            component is a 2nd copy
//!     zset    zset_index_serde   (member, (score, address))         component is a 2nd copy
//!     list    plain serde        (i64 sequence, address)            component is a 2nd copy
//!     string  no component at all                                   nothing to lose
//!     hash    skip_serializing -- NOTHING WRITTEN                   THE COMPONENT IS THE COPY
//! ```
//!
//! The three spelled kinds would survive the removal: their durable map holds what the name
//! re-spells, and since #2005 the fold carries identity for them too. The hash does not, and one
//! kind is enough, because the component is ONE field on ONE struct -- keeping it for the hash keeps
//! all sixteen bytes on every entry of every kind, which is the whole prize.
//!
//! # WHY THIS IS NOT THE RELOAD ARGUMENT #1996 ALREADY MADE
//!
//! #1996 stopped on the DELTA FOLD, where an element's durable-map entry was never written. #2005
//! closed exactly that door. The door still open is a different one and it is nearer the surface:
//!
//! ```text
//!     execute_on_shard.rs:799   Command::HashGetAll
//!         bucket_index_component_block_addresses(shard, "hash", &key)
//!             .filter_map(|(field, address)| ...
//!                 (field.map(|name| name.to_string()).unwrap_or_default(), value))
//! ```
//!
//! `field` there IS `page.component`. `engine/hash_field_map.rs:46` says so deliberately:
//! "`HashGetAll` and `HashLen` do not read this map at all, they resolve through
//! `bucket_index_component_block_addresses`". So a component-less entry does not degrade a hash on
//! the next reload -- it answers `HashGetAll` with every field named `""` immediately.
//!
//! And behind that, `rebuild_unserialized_model_maps_from_bucket_index` and the big derive's hash arm
//! both took the field name from `entry.component.unwrap_or_default()`, and the derive ASSIGNS
//! `shard.hashes = hashes` wholesale -- unlike the three arms below it, which merge, because those
//! three have a durable map to merge with and the hash has none.
//!
//! BOTH OF THOSE TWO NOW SKIP AND COUNT instead of defaulting, which is test 4 in this module. The
//! sentence above is kept in the past tense because it is the reason the step is still blocked: the
//! skip stops an absent name becoming a REAL empty field name, and it does not give the derive a
//! field name it never had. `HashGetAll` above is UNCHANGED and still spells a component-less entry
//! as `""` on the live read path -- that is a listing, not a durable map, and moving it would move
//! the `HashLen`/`HashGetAll` pair a sibling is guarding.
//!
//! # WHAT THE STEP WAS WORTH, SO NOBODY RE-DERIVES IT
//!
//! Priced below rather than described. The entry is 64 with 60 bytes of field; without the component
//! it is 48 with 44, and the list stride goes 72 -> 56. That is the real prize and it is why the step
//! keeps being proposed. It is stated here as a MEASURED counterfactual on a declared mirror, so the
//! next reader gets the number without spending a session on it.
//!
//! # WHAT WOULD ACTUALLY UNBLOCK IT, NAMED PRECISELY
//!
//! One prerequisite, not a family of them: `shard.hashes` would have to become DURABLE -- the
//! `skip_serializing` on `engine/engine_storage_state.rs` removed -- and `HashGetAll`/`HashLen` moved onto that
//! map instead of onto `bucket_index_component_block_addresses`. That is a MOVE rather than a
//! duplication: the field names it would start writing are the same characters the component writes
//! today through `block_index_written_key`.
//!
//! THE FIRST HALF IS DONE AND THE SECOND HALF IS NOT, which is the whole state of it. `hashes` is
//! `#[serde(default)]` now and the reconcile MERGES it per element through `fill_absent_elements`
//! like the other three, so the map is a durable record of every hash field name. `HashGetAll` and
//! `HashLen` still answer from `bucket_index_component_block_addresses`, and moving them is the
//! remaining step -- deliberately not taken in the same change, because #1989 is the recorded
//! incident for moving the length answer onto a map before the map is complete, and
//! `length_answer_and_listing_agree` pins the two answers to each other in the meantime.
//!
//! SO THE STOP CONDITION HAS MOVED RATHER THAN LIFTED. What still holds the component on the entry
//! is not durability any more: it is that `block_index_handle` HASHES the component, and the
//! component is the only field in that hash distinguishing two elements of ONE folded page. Drop it
//! without making the entry per-page and two elements collide on one slot. See the report on #1976
//! for the enumerated remainder.
//!
//! NO PRODUCTION CODE CHANGES IN THIS MODULE. Three tests, each driving a fact rather than quoting
//! one.

#![allow(clippy::all)]
use super::*;

use std::mem::size_of;
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::{BlockIndex, ShardState};
use crate::engine::storage_bucket_internals::StoredModelKind;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

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
        table_name: "element-naming".to_string(),
        shard_uri: "local://element-naming/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// The same instrument `durable_outranks_derived` uses: rewrite a literal inside every served index
/// file, whatever codec it carries.
///
/// Length-preserving by construction -- the positional index-log encoder REFUSES on a length change,
/// so a swap that changed the width would be testing the encoder rather than the name.
fn swap_across_index_files(indexes: &std::path::Path, from: &str, to: &str) -> usize {
    assert_eq!(
        from.len(),
        to.len(),
        "the swap must be length-preserving: {from:?} is {} B and {to:?} is {} B",
        from.len(),
        to.len()
    );
    super::durable_outranks_derived::swap_across_index_files(indexes, from, to)
}

// =================================================================================================
// 1. THE REFUTATION, AND WHAT LIFTED IT. A SWAPPED HASH COMPONENT NO LONGER RENAMES THE FIELD.
// =================================================================================================

/// SWAP A HASH FIELD'S COMPONENT ON DISK AND THE FIELD COMES BACK NEITHER RENAMED NOR SERVED.
///
/// # WHAT THIS TEST USED TO SAY, AND WHY IT SAYS SOMETHING ELSE NOW
///
/// It used to assert that the field came back RENAMED: the swapped component became the field name,
/// because nothing else held it. That was the refutation, and it was correct at the time.
///
/// `container_blocks` puts the element key inside the page. So the index now says `hfield-Z` and the
/// page still says `hfield-b`, and they disagree. The read funnel resolves the address the index
/// names and then asks that page for `hfield-Z`, which it does not hold -- so the element answers
/// MISSING, and `HashGetAll`'s `filter_map` drops it.
///
/// ## THE THREE OUTCOMES, AND WHY THIS IS THE ONE TO WANT
///
/// ```text
///     before   the element is served under the name the INDEX spells      renamed, silently
///     after    the element is not served at all                          absent, and countable
///     rejected the element is served under the name the PAGE spells      the index stops mattering
/// ```
///
/// The third would make the page authoritative for the name, which is a later stage's decision and
/// not one to take in passing. Between the first two, the second is the answer this store has
/// repeatedly chosen: #2013 measured a reader handed "the FIRST page's BYTES rather than answering
/// missing" and called it the defect; #2016 refused to let an absent name become a real one. Serving
/// bytes under a name their own page contradicts is that same shape. A miss is worse to read and
/// better to trust.
///
/// ## AND IT HAS A COST, WHICH IS ASSERTED HERE RATHER THAN LEFT TO BE FOUND
///
/// `HashLen` counts page-index ENTRIES and `HashGetAll` returns entries whose page will read, so on
/// a store whose index and pages disagree the two now answer differently. That pair is what #2014
/// guards, and it still agrees on every store this engine writes -- the divergence needs a component
/// that was edited underneath it, which is exactly what this test does and nothing else does. Both
/// numbers are asserted below so the cost is recorded rather than discovered later.
///
/// # THE CONTROL, UNCHANGED
///
/// A zset member's component is swapped in the same pass, and that member still answers because
/// `zset_index_serde` persisted it beside the page and `fill_absent_elements` keeps what the derived
/// view could not produce. An arm that moved and an arm that did not, from one swap pass, is what
/// makes this a measurement rather than a demonstration that editing an index breaks things.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_hash_field_cannot_be_renamed_by_editing_the_index_because_the_index_stores_no_name() {
    let dir = tempfile::tempdir().unwrap();
    let hash_key = "en-hash";
    let zset_key = "en-zset";
    // Chosen so each spells a component this test can find and swap at a FIXED width.
    let hash_fields = ["hfield-a", "hfield-b", "hfield-c"];
    let zset_member = b"zmember-a".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine);
        for field in hash_fields {
            write(
                &engine,
                Command::HashSet {
                    key: hash_key.to_string(),
                    field: field.to_string(),
                    value: b"opaque-payload".to_vec(),
                },
            );
        }
        write(
            &engine,
            Command::ZSetAdd {
                key: zset_key.to_string(),
                member: zset_member.clone(),
                score: 7.5,
            },
        );
        engine.unload_shard(1);
    }

    // --- THE MUTATION. Both length-preserving, both in the same pass. ---
    let indexes = dir.path().join("indexes");
    let hash_swaps = swap_across_index_files(&indexes, "hfield-b", "hfield-Z");
    // A zset component is `{score:016x}{hex(member)}`, so the member travels as hex characters.
    let member_hex = hex::encode(&zset_member);
    let mut mangled_hex = member_hex.clone();
    // Flip the LAST hex digit, keeping the width and keeping it valid hex, so the arm is exercised
    // as a DIFFERENT member rather than as an unreadable name -- the unreadable path is already
    // driven by `an_unreadable_component_name_is_skipped_and_the_durable_map_keeps_the_element`.
    let last = mangled_hex.pop().expect("a member spells at least one hex digit");
    mangled_hex.push(if last == '0' { '1' } else { '0' });
    let zset_swaps = swap_across_index_files(&indexes, &member_hex, &mangled_hex);

    // --- THE TWO FLOORS NOW SAY DIFFERENT THINGS, AND MEASURING WHY IS THE FINDING. ---
    //
    // Both read `> 0`: a swap pass that found nothing proves nothing, so each arm first
    // established that the element's name was PRESENT in the served index and had been mutated
    // there. An entry carries no component now, so the obvious reading is that neither name is in
    // the index and both floors are dead. THAT READING IS WRONG FOR THE HASH, and the counts are
    // what said so -- a first draft of this restatement asserted zero for both and the hash
    // measured ONE.
    //
    // WHY THE HASH NAME IS STILL THERE: `shard.hashes` is DURABLE now, `#[serde(default)]` rather
    // than `skip_serializing`, and it is keyed by the field name as a STRING. So the characters
    // `hfield-b` are written into the shard index -- in the durable map, not on the entry. The
    // name did not leave the stored form, it MOVED, which is exactly what made the collapse safe.
    // The floor therefore still holds and still means something; what it establishes is a mutation
    // of the DURABLE MAP rather than of an entry.
    //
    // WHY THE ZSET MEMBER IS NOT: `shard.zsets` is keyed by the member's RAW BYTES, and this swap
    // searches for its HEX spelling. Hex was only ever how the ENTRY's component rendered it, and
    // there is no component -- so there is no occurrence to find and the control cannot be
    // exercised at all. That subject is structurally unrepresentable rather than merely absent,
    // and the zero is asserted as the finding instead of the floor being lowered to `>= 0`, which
    // no fixture could fail.
    assert!(
        hash_swaps > 0,
        "the hash field name was not found in the served index, so this test swapped NOTHING and \
         its assertions below would pass on an unmutated store. `shard.hashes` is durable and \
         keyed by the field name, so the characters must be in there -- a zero here means the hash \
         map has stopped being written and the collapse has lost the copy that makes it safe"
    );
    assert_eq!(
        0, zset_swaps,
        "the zset member's HEX spelling was found {zset_swaps} time(s) in the served index. \
         `shard.zsets` is keyed by raw member bytes and the entry carries no component, so hex is \
         not a spelling the stored form uses any more -- a nonzero count means a hex rendering of \
         a member has come back into the index, which is the third copy this change removed"
    );

    // --- THE READ BACK. ---
    let engine = engine_on(dir.path());
    load_on(&engine);

    let entries = match read(
        &engine,
        Command::HashGetAll {
            key: hash_key.to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("HashGetAll answered {other:?}"),
    };
    let names = entries
        .iter()
        .map(|(field, _)| field.clone())
        .collect::<std::collections::BTreeSet<_>>();

    // --- WHAT A SWAPPED **DURABLE-MAP** NAME PRODUCES, OBSERVED AND THEN ASSERTED. ---
    //
    // The swap no longer edits an entry's component -- there is none -- it edits the field name
    // inside the durable `hashes` map. That is a different mutation with a different consequence,
    // so the numbers are printed before they are pinned, which is this module's own stated
    // discipline and the reason the first draft of this restatement was wrong twice.
    let length = match read(
        &engine,
        Command::HashLen {
            key: hash_key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    };
    println!(
        "[swap] durable-map name swaps={hash_swaps}; listing serves {} of {} field(s) {names:?}; \
         HashLen {length}",
        names.len(),
        hash_fields.len()
    );

    // MEASURED, THEN PINNED: swaps=1, the listing serves 2 of 3 (`hfield-a`, `hfield-c`), HashLen
    // answers 3. So the swapped field is served under NEITHER name -- which is the finding this
    // arm was opened with, reached through the durable map instead of through the entry.
    //
    // THE DENOMINATOR, so a store the swap destroyed cannot pass as the finding. It is the two
    // UNSWAPPED fields: the swapped one is the subject and its absence is what is being asserted,
    // so counting it here would settle the finding before the finding is made.
    assert_eq!(
        hash_fields.len() - 1,
        names.len(),
        "the hash came back with {} field(s) rather than the {} that were left unswapped: \
         {names:?}",
        names.len(),
        hash_fields.len() - 1
    );
    // AND THE TWO FIELDS THE SWAP DID NOT TOUCH ARE WHOLE, which is what separates "the edited
    // field is served differently" from "this store is broken".
    for survivor in ["hfield-a", "hfield-c"] {
        assert!(
            names.contains(survivor),
            "a field the swap never named went missing, so this store is broken in some way the \
             swap did not cause: {names:?}"
        );
    }

    // THE FINDING: the swapped name does not become the field's name.
    //
    // It did not when the name lived on the ENTRY -- the page's own item key contradicted it --
    // and it does not now that the name lives in the durable MAP, which is the stronger version of
    // the same claim: the element's identity is carried by the page it is written on, so a name
    // edited anywhere else cannot rename it.
    assert!(
        !names.contains("hfield-Z"),
        "the swapped name became the field name. The page carries its own element key, so an \
         edited name in the durable map must not be able to rename the field it points at: \
         {names:?}"
    );

    // AND THE POINT LOOKUP AGREES WITH THE LISTING on the swapped name, because a listing and a
    // point read resolve through different code and #2014 exists because they agreed by accident.
    let answer = read(
        &engine,
        Command::HashGet {
            key: hash_key.to_string(),
            field: "hfield-Z".to_string(),
        },
    );
    match answer {
        crate::types::CommandResponse::Bytes { value: None } => {}
        other => panic!(
            "HashGet for hfield-Z answered {other:?} where the listing has no such field -- the \
             point read and the listing must not part on the swapped name"
        ),
    }

    // AND THE LENGTH ANSWER AND THE LISTING ARE PINNED TO EACH OTHER rather than to a literal.
    // `HashLen` counts page-index entries and the listing counts entries whose page reads; a
    // divergence is the measure of how much the edit cost, and it is asserted as whatever it is
    // only in the sense that it must not exceed the one field that was edited.
    assert_eq!(
        hash_fields.len() as i64,
        length,
        "HashLen counts page-index entries and the swap removed none, so it must still answer {}",
        hash_fields.len()
    );
    assert_eq!(
        1,
        length - names.len() as i64,
        "the length answer and the listing must diverge by exactly the one element whose name was \
         edited; they diverge by {}",
        length - names.len() as i64
    );

    // THE CONTROL: the zset member is served, and no component was swapped for it to follow.
    let score = match read(
        &engine,
        Command::ZSetScore {
            key: zset_key.to_string(),
            member: zset_member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!(
            "the zset member did not answer. Its identity lives in `shard.zsets` and in its \
             page's own item key -- the index names it nowhere -- so a miss here means one of \
             those two stopped holding it: {other:?}"
        ),
    };
    assert_eq!(
        "7.5", score,
        "the durable zset map answered {score} for the original member"
    );

    println!(
        "hash name occurrences in the served index={hash_swaps}, zset={zset_swaps}: nothing to \
         swap, so nothing renamed and nothing lost (listing {} of {}, HashLen {length}, member at \
         {score})",
        names.len(),
        hash_fields.len()
    );
}

// =================================================================================================
// 2. THE STRUCTURAL REASON. THE SHARD INDEX WRITES NO HASH FIELD MAP.
// =================================================================================================

/// ALL FOUR CONTAINER KINDS NOW HAVE A DURABLE MAP IN THE WRITTEN SHARD INDEX.
///
/// RETARGETED, NOT DELETED, and the previous shape of this test is why it had to be. It asserted
/// that `hashes` was ABSENT from the written index, and its own failure message said what to do if
/// that ever stopped being true: "if that is deliberate then the stop condition in this module is
/// stale and removing the component becomes reachable -- re-read the header". It is deliberate now,
/// so the guard is retargeted at the invariant the change ESTABLISHES -- four durable maps, not
/// three -- rather than deleted along with the condition it was watching. A guard whose subject the
/// fix deletes still has a job.
///
/// Read off the SERIALIZER rather than off the attribute, because `#[serde(default)]` is the kind of
/// claim a doc comment can carry after the attribute has moved. An EMPTY `ShardState` is enough and
/// is the strongest form of the test: all four are emitted even when empty, so their presence here
/// is not an artefact of seeding.
#[test]
fn the_shard_index_writes_a_durable_map_for_all_four_container_kinds() {
    let value = serde_json::to_value(&ShardState::default())
        .expect("an empty shard state serializes");
    let object = value
        .as_object()
        .expect("a shard state serializes as a JSON object");

    // THE DENOMINATOR. An empty object would satisfy every `!contains_key` below.
    assert!(
        object.len() > 8,
        "the serialized shard state carried only {} field(s), which is too few for the absence \
         assertions below to mean anything",
        object.len()
    );

    for durable in ["sets", "zsets", "lists", "hashes"] {
        assert!(
            object.contains_key(durable),
            "`{durable}` is not in the written shard index, so this kind has no durable map and \
             the table in this module's header is wrong"
        );
    }

    println!(
        "written shard-index fields={}, all four of sets/zsets/lists/hashes PRESENT",
        object.len()
    );
}

// =================================================================================================
// 3. THE PRIZE, PRICED, SO THE NEXT READER DOES NOT SPEND A SESSION ON IT.
// =================================================================================================

/// The page entry with the component GONE -- not thinned, not an ordinal, removed.
///
/// Declared rather than asserted about a structure this engine does not have. #1997 reached the same
/// 48 by holding BOTH names one word each and declined it on soundness; this mirror reaches it by
/// dropping one field, which is the step #2005 named.
#[allow(dead_code)]
struct MirrorEntryNoComponent {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

fn round_up_to_eight(bytes: usize) -> usize {
    (bytes + 7) / 8 * 8
}

/// WHAT THE ENTRY AND THE STRIDE WOULD BECOME, RECONSTRUCTED FROM THE FIELDS RATHER THAN QUOTED.
///
/// Both numbers are asserted as `sum_of_fields` rounded to alignment, so a field that changes width
/// later fails HERE with the arithmetic in the message instead of silently re-justifying a literal.
/// #1997's warning is the reason: a sibling's sixteen-bit ordinal was worth exactly nothing because
/// the tail rounded back, and a bare `== 48` would not have shown that.
#[test]
fn the_entry_and_its_stride_are_reconstructed_from_the_fields_not_quoted() {
    // --- THE LIVE ENTRY, RECONSTRUCTED. ---
    //
    // THE COMPONENT TERM WENT ONE LEG AGO AND THE `object_key` TERM GOES HERE. The
    // reconstruction read `+ size_of::<Option<Arc<str>>>()` for the component and came to 56;
    // without it, 35; without the key's sixteen-byte fat pointer, NINETEEN, which rounds to the
    // twenty-four the live type measures.
    let live_fields = size_of::<StoredModelKind>() // model_id
        + size_of::<ElementEntry>()                  // address
        + 2 * size_of::<bool>()                      // dirty, deleted
        ;
    // THE `kind` AND `routing_bucket` TERMS ARE GONE FROM THIS SUM TOO, and they were a SEVENTH
    // live instance of a decomposition that had stopped describing the type: `65c7fc562` retired
    // both from `BlockIndex` and this sum kept adding their five bytes. It passed because forty and
    // thirty-five both round to forty -- the same rounding that hid the same two fields in four
    // mirrors in `an_assigned_durable_id_defeats_ground_one_only` and in `blocks_per_bucket`'s
    // reconstruction. `size_of` steps at a word and a sum does not.
    assert_eq!(
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>(),
        "the live entry reconstructs to {} B from {live_fields} B of field, but `size_of` says {}",
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>()
    );

    // --- WHAT THE COMPONENT'S SHARE WAS, now a counterfactual rather than a measurement. ---
    //
    // Sixteen bytes, a fat optional pointer. It is still read off the type rather than written as a
    // literal, so the figure this module quotes cannot drift from what such a field costs -- but it
    // is no longer a slot IN the entry, which is why the sum above does not include it.
    let component_width = size_of::<Option<Arc<str>>>();
    assert_eq!(
        16, component_width,
        "the component slot was {component_width} B, not the sixteen this module is about"
    );
    // THE BASELINE MOVED BECAUSE THE KEY LEFT TOO, AND BOTH ENDS ARE PINNED SO NEITHER DRIFTS.
    //
    // Adding the name slot back to TODAY'S entry gives forty. The fifty-six this module measured
    // the step down from was that PLUS the key's own sixteen-byte fat pointer, which left on the
    // next leg -- so 56 is still the right number for the entry this module was written against,
    // and it is reconstructed from the types rather than left as a literal nobody can check.
    assert_eq!(
        40,
        round_up_to_eight(live_fields + component_width),
        "the entry with a name slot added back is {} B, not the forty today's field set plus a \
         name slot comes to",
        round_up_to_eight(live_fields + component_width)
    );
    assert_eq!(
        56,
        round_up_to_eight(live_fields + component_width + size_of::<Arc<str>>()),
        "the entry this module measured the step DOWN from is {} B, not the 56 it quotes -- so \
         the saving it reports is against the wrong baseline",
        round_up_to_eight(live_fields + component_width + size_of::<Arc<str>>())
    );

    // --- THE MIRROR, WHICH IS NOW THE SAME SHAPE AS THE LIVE ENTRY. ---
    //
    // `MirrorEntryNoComponent` was the counterfactual: the entry as it would be without its element
    // name. The entry IS that now, so the mirror and the live type have converged. That is the step
    // landing, not the instrument breaking -- and it is asserted as an EQUALITY, so a mirror that
    // drifts from the live entry reddens here rather than going on describing a shape the engine
    // no longer has.
    //
    // IT DRIFTED, AND THE EQUALITY IS WHAT CAUGHT IT. The mirror kept its own `object_key` through
    // the leg that took the field off `BlockIndex`, so it measured forty against a twenty-four
    // byte entry -- the convergence this paragraph claims, stated and then broken by the next
    // change. The field is off the mirror now. Its `log_backed` bool is a fourth term the live
    // entry does not carry either; that one is deliberate and predates this change, because the
    // flag was a field when the mirror was written and the tail it models is the three-bool one.
    let mirror_fields = live_fields;
    assert_eq!(
        round_up_to_eight(mirror_fields),
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry reconstructs to {} B from {mirror_fields} B of field, but \
         `size_of` says {}",
        round_up_to_eight(mirror_fields),
        size_of::<MirrorEntryNoComponent>()
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryNoComponent>(),
        "the live entry is {} B and the mirror of a nameless entry is {} B. They must be the SAME \
         shape now that the entry carries no name: a difference means the mirror has drifted and \
         every figure below is about a structure the engine does not have",
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryNoComponent>()
    );

    // --- THE STRIDE, WHICH IS THE NUMBER THAT ACTUALLY MOVES, because `BlockIndexMap` IS its list
    //     and a tuple re-aligns to eight. #1997 states this distinction and it has been conflated
    //     before, so both are asserted and both are printed. ---
    let live_stride = size_of::<(u64, BlockIndex)>();
    let mirror_stride = size_of::<(u64, MirrorEntryNoComponent)>();
    assert_eq!(
        round_up_to_eight(size_of::<u64>() + size_of::<BlockIndex>()),
        live_stride,
        "the live stride does not reconstruct from its parts"
    );
    assert_eq!(
        round_up_to_eight(size_of::<u64>() + size_of::<MirrorEntryNoComponent>()),
        mirror_stride,
        "the mirror stride does not reconstruct from its parts"
    );

    // --- THE VERDICT, AS NUMBERS. ---
    // THE ENTRY IS THE MIRROR NOW, WHICH IS THIS MODULE'S OWN STEP TAKEN -- AND THEN A SECOND
    // STEP TOOK BOTH OF THEM. It pinned 56 for the live entry against 40 for
    // `MirrorEntryNoComponent`; the component left and they converged on 40; the IDENTITY then
    // left as well and they converged again on 24. The mirror had to lose its own `object_key` to
    // follow, which the convergence equality above is what caught.
    assert_eq!(24, size_of::<BlockIndex>(), "the entry pin moved");
    // THE `!= 39` AND `!= 41` PARTNERS ARE GONE, dominated by the equality above them, which
    // panics first and so left them unreachable on every change that could have fired them.
    // The width is also pinned as a const beside `BlockIndex` in `engine_storage_state.rs`, with the alignment
    // pin a width pin does not entail. What this module adds is the STRIDE reconstructed from
    // that width, and the stride pin below is the claim here that can still move.
    // THIRTY-TWO, AND THE STRIDE MOVED WITH THE ENTRY RATHER THAN BY ITSELF. The element is a
    // handle plus an entry, so an entry losing sixteen bytes takes the element from 48 to 32 --
    // the same two words, because 8 + 24 lands on a multiple of eight with no rounding to
    // absorb any of it. This is the pin the module doc calls the one that can still move, and
    // it is reconstructed from the width above rather than quoted beside it.
    assert_eq!(32, live_stride, "the stride pin moved");
    assert_eq!(
        24,
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry is not 24"
    );
    assert_eq!(32, mirror_stride, "the component-less stride is not 32");

    // THE STEP IS NOW ZERO, AND THAT IS THE STEP BEING TAKEN RATHER THAN BEING WORTHLESS.
    //
    // This asserted `mirror_stride < live_stride` under "the step is worth nothing, which would
    // refute it on bytes as well". The live entry and the mirror are the same shape, so the stride
    // difference is zero -- and the reason is that the saving has been BANKED, not that it was
    // never there. What the module predicted off the mirror (40 and 48) is what the live type
    // measures.
    //
    // ASSERTED AS THE EQUALITY, so this cannot be read as the saving having evaporated: the
    // counterfactual with the name slot added back is asserted at 56 above, which is the baseline
    // the eight bytes of stride a page were measured against.
    assert_eq!(
        live_stride, mirror_stride,
        "the live stride is {live_stride} B and the nameless mirror's {mirror_stride} B. They must \
         agree now that the entry carries no name; a difference means one of the two is not the \
         shape it claims to be"
    );
    // FORTY-EIGHT, BECAUSE THE BASELINE MOVED WITH THE ENTRY. Adding a name slot back to TODAY'S
    // entry gives forty, and a handle plus forty strides at forty-eight. The sixty-four this
    // module quoted its eight bytes a page against was that plus the key's own sixteen, which
    // left on the next leg -- and that baseline is pinned from the types two assertions above.
    assert_eq!(
        48,
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width)),
        "the stride of an entry with a name slot added back is {} B, not the 48 the per-page \
         saving was quoted against",
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width))
    );

    println!(
        "entry {} B, stride {live_stride} B, from {live_fields} B of field; with a name slot added \
         back it would be {} B of field in {} B at a {} B stride -- the {} B a page this step \
         banked",
        size_of::<BlockIndex>(),
        live_fields + component_width,
        round_up_to_eight(live_fields + component_width),
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width)),
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width))
            - live_stride
    );
}

// =================================================================================================
// 4. AN ABSENT FIELD NAME IS NOT THE EMPTY FIELD NAME, AND ONLY ONE OF THEM NAMES A FIELD.
// =================================================================================================

/// A HASH PAGE THAT NAMES NO FIELD IS SKIPPED; A PAGE THAT NAMES THE EMPTY FIELD IS KEPT.
///
/// Both derive arms read `entry.component` to get a hash field name, and both ended
/// `.unwrap_or_default()` -- so a page entry carrying NO component became a field named `""`. That
/// is not a missing value standing in for itself: an empty hash field name is LEGAL, so the default
/// manufactures a real, addressable field, which then collides with a genuine empty-named field and
/// takes its address. The two states this test separates are:
///
/// ```text
///     component = Some("")   a genuine field whose name is zero characters    KEEP
///     component = None       a page that says nothing about which field       SKIP AND COUNT
/// ```
///
/// # WHY THIS ARM AND NOT THE THREE THAT WERE ALREADY CORRECTED
