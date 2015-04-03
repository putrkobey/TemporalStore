// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER PAGE'S ORDINAL WAS A HARDCODED ZERO, AND THE FIELD TO HOLD IT WAS ALREADY STORED.
//!
//! # WHAT THIS IS, AND WHAT IT DELIBERATELY IS NOT
//!
//! `BlockAddress::block_id` is a stored `u16` -- a page's position INSIDE its object. Every
//! TIMESTAMPED kind already gets one: `next_block_index_for_object` has thirteen call sites covering
//! `feature` and the six `context_*` kinds, and `compaction` preserves the value across a rewrite
//! with a comment saying why ("taking a fresh one here would give every block of a multi-block
//! object the same index, and the index entries would collide"). No CONTAINER kind ever called it.
//! A container element reached the slab through `append_value` -> `append_with_block_metadata`,
//! and that function's whole body is `self.append_block_of_object(bytes, object_id, routing_bucket,
//! 0)`. The zero was an argument nobody supplied, not a missing field and not a format limit.
//!
//! THIS IS NOT THE COMPONENT'S REMOVAL. The component stays the element's identity: #1996 refuted
//! moving identity into an ordinal on two independent grounds, and the first of them -- the delta
//! fold delivers elements whose durable-map entry was never written -- is still true on this
//! revision. Nothing here reads the ordinal to find a row. Deletion still matches by component,
//! which is what `deletion_still_finds_its_element_by_component` holds. THE ENTRY no longer carries
//! that component: a container element's identity lives in its resident map and in its page's
//! payload, which is where every arm in this module now reads it from.
//!
//! # IT NAMES A POSITION, NOT AN ELEMENT
//!
//! That is forced by the tree rather than chosen: `max` FALLS after a delete and the next insert is
//! handed the ordinal just freed. `the_ordinal_names_a_position_and_a_delete_frees_it` drives exactly
//! that and asserts the reuse, because a reader that treated the ordinal as naming a particular
//! element would be silently corrupt the moment it happened.
//!
//! **THE REASON `max` FALLS HAS MOVED TWICE, AND THE CONCLUSION HAS NOT.** It used to fall because
//! the entry was GONE -- `mark_bucket_index_block_deleted_with` was named for a mark it did not
//! make, its body a `retain` returning false. A per-element removal then began KEEPING an entry,
//! and this paragraph said `max` fell because "`container_block_ordinal` FILTERS `deleted`". That
//! walk over entries is gone: the allocator asks the RESIDENT MAP, which is keyed by element and
//! holds exactly the object's live elements, so its high-water mark is over live elements by
//! construction and needs no filter. A removal drops its element from the map and `max` falls with
//! it.
//!
//! AND THE FILTER WOULD NOT HAVE BEEN ENOUGH, which is why this is worth more than a tidier
//! mechanism. A removal keeps the live entry over the page its element vacated -- not deleted, so
//! no `deleted` filter can see it -- so a walk over live ENTRIES does not fall after a removal at
//! all. The arms here assert both quantities wherever they part company: the POSITIONS from the
//! resident map, and the retained live entry as a footprint cost with its own message saying which
//! assertion to restate when the retirement lands.
//!
//! So the freed ordinal is held by two live entries and one tombstone -- the arrival's, the one the
//! removal left standing, and the tombstone that records the removal -- while exactly one live
//! ELEMENT occupies the position. That is asserted, not left implicit, because two
//! entries at one ordinal is also what corruption would look like and the whole difference is which is
//! live. The safety argument is unchanged and is the one this section opens with: identity lives in the
//! component, so a position may be refilled.
//!
//! # TWO DEPARTURES FROM THE SERIES DERIVATION, BOTH FORCED
//!
//!   * AN OVERWRITE KEEPS ITS ORDINAL. `HashSet`/`SetAdd` on an existing member REPLACES that
//!     member's page. `max + 1` there would climb once per WRITE rather than once per ELEMENT, and a
//!     single member rewritten 65,536 times would reach the ceiling on a set of one. Driven by
//!     `an_overwrite_keeps_the_members_ordinal_rather_than_climbing`, which fails at 4 instead of 0
//!     without the component lookup.
//!   * PAST THE CEILING NOTHING IS ASSIGNED. The stored width is sixteen bits, so the ceiling is
//!     65,535 -- and `narrow_block_id` PANICS above it rather than saturating, deliberately. An
//!     object's 65,536th element is not a caller's mistake, so the ordinal is simply left at the `0`
//!     it has on `main`: past the ceiling this change is a strict no-op. Driven directly on the
//!     helper by `past_the_ceiling_the_ordinal_is_left_unassigned_rather_than_panicking`.
//!
//! # THE ONE WAY THIS COULD HAVE BEEN SILENT DATA LOSS
//!
//! `decode_block_record` compares `address.block_id()` against the record header's own block id and
//! refuses the page with "page id mismatch" when they differ -- the one cross-check on a read that
//! can actually fire. It is inert today only because the address side is 0 on both sides. Stamping
//! the ordinal onto the INDEX's copy of the address after the append would have left the header
//! saying 0 and the address saying N, and every container page written would have become unreadable.
//! So the ordinal is threaded into `append_block_of_object`, which builds the header and the
//! returned address from the same value. `a_reloaded_container_still_reads_every_element` is the
//! guard: it unloads the shard, loads it again so nothing is served from a warm cache, and reads
//! every element back through the decode path.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = 1023;

// =================================================================================================
// HARNESS
// =================================================================================================

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
        table_name: "container-page-ordinal".to_string(),
        shard_uri: "local://container-page-ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
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
    assert!(response.status.ok, "the fixture read failed: {response:?}");
    response.response
}

/// The ordinals this object's LIVE page entries carry, in index order.
///
/// THE COMPONENT SLOT IS GONE FROM THIS TUPLE, BECAUSE IT HAD STOPPED CARRYING ONE. This returned
/// `Vec<(Option<String>, Option<u64>)>` and the first half was a hardcoded `None::<String>` once
/// the element-name field left `BlockIndex`. Three callers only ever took `.len()`, but two
/// FILTERED on that half -- and a filter over a column that is always `None` matches nothing, so
/// one of them counted zero stale pages and PASSED for that reason. Dropping the slot makes every
/// such filter a compile error instead. Element identity is `ordinal_for_component` below, which
/// asks the authority for it.
fn blocks_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted {
                continue;
            }
            if block.model_id.as_str() == kind
                && &**named.at(bucket.routing_bucket, *handle) == key
            {
                held.push(block.address.block_id());
            }
        }
    }
    held.sort();
    held
}

/// This object's TOMBSTONE entries: the element each is about, and the ordinal its page carries.
///
/// THE ELEMENT NOW COMES FROM THE BUCKET'S TOMBSTONE ROWS, NOT FROM THE ENTRY. It read
/// `page.component`, and a tombstone entry files `None` there: the element a removal is about is a
/// per-ELEMENT fact and lives beside the entries, where only a removal pays for it. Restated rather
/// than re-goldened -- the two arms below are about WHICH element a removal is recorded against,
/// and that question still has an answer; what moved is where the answer is kept. Reading the
/// entry here instead would make both of them assert `None == None`.
fn tombstone_blocks_of(
    engine: &TemporalEngine,
    kind: &str,
    key: &str,
) -> Vec<(Option<String>, Option<u64>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted
                && block.model_id.as_str() == kind
                && &**named.at(bucket.routing_bucket, *handle) == key
            {
                held.push((
                    bucket
                        .tombstone_element_at(&block.address)
                        .map(|row| row.component.to_string()),
                    block.address.block_id(),
                ));
            }
        }
    }
    held.sort();
    held
}

/// The ordinals this object's LIVE page entries carry, ascending, with absence spelled as `None`.
fn ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let mut ordinals = blocks_of(engine, kind, key);
    ordinals.sort();
    ordinals
}

/// The positions this object's live ELEMENTS hold, ascending -- the allocator's own input.
///
/// NOT THE SAME QUANTITY AS [`ordinals_of`], AND THE DIFFERENCE IS THE POINT. A removal keeps the
/// live entry over the page its element vacated, so the live INDEX still carries that element's
/// ordinal while the element itself is gone. `container_block_ordinal` reads the RESIDENT MAP, so
/// this is the set a position claim is actually about, and the two are asserted separately wherever
/// they part company.
fn resident_ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held: Vec<Option<u64>> = match kind {
        "hash" => shard
            .hashes
            .get(key)
            .map(|fields| fields.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        "set" => shard
            .sets
            .get(key)
            .map(|members| members.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        "zset" => shard
            .zsets
            .get(key)
            .map(|members| {
                members
                    .values()
                    .map(|(_score, address)| address.block_id())
                    .collect()
            })
            .unwrap_or_default(),
        "list" => shard
            .lists
            .get(key)
            .map(|entries| entries.values().map(|address| address.block_id()).collect())
            .unwrap_or_default(),
        other => panic!("no resident container map is claimed for kind {other}"),
    };
    held.sort();
    held
}

/// The ordinal the element with exactly this component holds, if the object holds that element.
///
/// RE-ATTRIBUTED TO THE AUTHORITY, AND IT IS THE SAME DOOR THE ALLOCATOR USES. This searched
/// [`blocks_of`] for an entry whose component matched, which answered `None` for every element once
/// an entry stopped naming one -- so three arms in this module read "no such element" for elements
/// that were plainly there. `state::resident_component_address` is what
/// `container_block_ordinal`'s overwrite arm reads, so an element's position and the number the
/// allocator would hand it back are one value read one way, and no component spelling is restated
/// here to drift from the engine's.
///
/// The two layers of `Option` are kept and mean what they did: the outer is whether the object
/// holds this element at all, the inner is whether its address carries an ordinal.
fn ordinal_for_component(
    engine: &TemporalEngine,
    kind: &str,
    key: &str,
    component: &str,
) -> Option<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    crate::engine::state::resident_component_address(shard, kind, key, component)
        .map(crate::block_store::ElementEntry::block_id)
}

const MEMBERS: usize = 8;

/// Seed one object of each container kind with `MEMBERS` distinct elements.
///
/// Returns the four `(kind, key)` pairs so no test hand-writes the list.
fn seed_one_of_each_kind(engine: &TemporalEngine) -> Vec<(&'static str, String)> {
    for m in 0..MEMBERS {
        write(
            engine,
            Command::SetAdd {
                key: "cpo-set".to_string(),
                member: format!("member-{m}").into_bytes(),
            },
        );
        write(
            engine,
            Command::ZSetAdd {
                key: "cpo-zset".to_string(),
                member: format!("member-{m}").into_bytes(),
                score: m as f64,
            },
        );
        write(
            engine,
            Command::ListPush {
                key: "cpo-list".to_string(),
                member: format!("member-{m}").into_bytes(),
                left: false,
            },
        );
        write(
            engine,
            Command::HashSet {
                key: "cpo-hash".to_string(),
                field: format!("field-{m}"),
                value: format!("value-{m}").into_bytes(),
            },
        );
    }
    vec![
        ("set", "cpo-set".to_string()),
        ("zset", "cpo-zset".to_string()),
        ("list", "cpo-list".to_string()),
        ("hash", "cpo-hash".to_string()),
    ]
}

// =================================================================================================
// 1. THE ASSIGNMENT, PER KIND, WITH THE DENOMINATOR PRINTED
// =================================================================================================

/// EVERY CONTAINER PAGE CARRIES ITS OWN ORDINAL, AND ON `main` EVERY ONE OF THEM CARRIES ZERO.
///
/// Four kinds, `MEMBERS` elements each, and the assertion is on the SET of ordinals rather than on
/// a count: `0..MEMBERS` exactly, no duplicate and none missing. A count alone would pass on a store that
/// handed every page the same number, which is precisely what `main` does.
///
/// The denominator is printed per kind because a kind that seeded nothing and a kind whose ordinals
/// are all correct both produce "no wrong ordinals".
///
/// rust-internal: drives four command arms through the engine, no external surface
#[test]
fn every_container_block_carries_its_own_ordinal_rather_than_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let kinds = seed_one_of_each_kind(&engine);

    let expected: Vec<Option<u64>> = (0..MEMBERS as u64).map(Some).collect();
    println!("\n=== container page ordinals, {MEMBERS} elements an object ===");
    let mut checked = 0usize;
    for (kind, key) in &kinds {
        let ordinals = ordinals_of(&engine, kind, key);
        println!(
            "  {kind:<5} pages={:<3} ordinals={:?}",
            ordinals.len(),
            ordinals
                .iter()
                .map(|o| o.map_or(-1i64, |v| v as i64))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ordinals.len(),
            MEMBERS,
            "{kind}/{key} holds {} pages, not the {MEMBERS} the seed wrote -- the arm under test \
             did not file what this asserts about",
            ordinals.len()
        );
        assert_eq!(
            ordinals, expected,
            "{kind}/{key} does not carry one distinct ordinal per element. On main every one of \
             these is Some(0), because `append_with_block_metadata` passes a hardcoded 0"
        );
        checked += ordinals.len();
    }
    assert_eq!(
        checked,
        kinds.len() * MEMBERS,
        "the sweep checked {checked} pages over {} kinds, not {}",
        kinds.len(),
        kinds.len() * MEMBERS
    );
    println!("  [denominator] {checked} pages over {} kinds", kinds.len());
}

// =================================================================================================
// 2. A CONTROL THAT MUST NOT MOVE
// =================================================================================================

/// THE CONTROL: A `string` PAGE IS UNTOUCHED, AT 0.00%.
///
/// `string` is the kind with no component at all, it is not a container, and its arm still calls
/// plain `append_value`. Every string page must still carry ordinal 0 -- if this moves, the change
/// reached an arm it was not supposed to reach, and the measurement above is measuring the harness.
///
/// rust-internal: drives StringSet, no external surface
#[test]
fn a_string_block_is_left_at_zero_which_is_the_control() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    const KEYS: usize = 6;
    for k in 0..KEYS {
        write(
            &engine,
            Command::StringSet {
                key: format!("cpo-string-{k}"),
                value: format!("value-{k}").into_bytes(),
            },
        );
    }

    let mut moved = 0usize;
    let mut seen = 0usize;
    for k in 0..KEYS {
        let ordinals = ordinals_of(&engine, "string", &format!("cpo-string-{k}"));
        assert_eq!(
            ordinals.len(),
            1,
            "a string key holds one page, not {}",
            ordinals.len()
        );
        seen += ordinals.len();
        if ordinals[0] != Some(0) {
            moved += 1;
        }
    }
    assert_eq!(seen, KEYS, "the control saw {seen} pages, not {KEYS}");
    let pct = 100.0 * moved as f64 / seen as f64;
    println!("\n=== control === string pages moved {moved}/{seen} = {pct:.2}%");
    assert_eq!(
        moved, 0,
        "{moved} of {seen} string pages changed ordinal. The control must sit at 0.00%: the \
         container arms are the only ones that were touched"
    );
}

// =================================================================================================
// 3. AN OVERWRITE KEEPS ITS ORDINAL
// =================================================================================================

/// AN OVERWRITE IS THE SAME ELEMENT IN THE SAME PLACE, SO IT KEEPS ITS ORDINAL.
///
/// This is the clause that bounds the ordinal by the object's live element count instead of by its
/// WRITE count. Without the component lookup, `max + 1` would hand the fifth write of one member
/// ordinal 4 on a set of one element -- and a member rewritten 65,536 times would reach a ceiling
/// that has nothing to do with how many elements the object holds.
///
/// Driven on both the kind whose component is content-derived (`set`) and the kind whose component
/// is the caller's own field name (`hash`).
///
/// rust-internal: drives repeated writes of one element, no external surface
#[test]
fn an_overwrite_keeps_the_members_ordinal_rather_than_climbing() {
    // ONE MEMBER REWRITTEN REPEATEDLY IS A ONE-PAGE SHAPE ONLY UNGATED -- gated it settles on
    // two pages and this test's own floor says so ("not measuring an overwrite"). The ordinal
    // under the collapsed projection is asserted by
    // `ordinal_under_the_gate::an_overwrite_keeps_the_members_ordinal_under_either_projection`,
    // which drives BOTH arms, so pinning this one loses nothing.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    const REWRITES: usize = 5;
    let member = b"the-one-member".to_vec();
    let component = hex::encode(&member);

    // One element, written REWRITES times.
    for _ in 0..REWRITES {
        write(
            &engine,
            Command::SetAdd {
                key: "cpo-ow-set".to_string(),
                member: member.clone(),
            },
        );
    }
    let set_blocks = blocks_of(&engine, "set", "cpo-ow-set");
    assert_eq!(
        set_blocks.len(),
        1,
        "a set of one member rewritten {REWRITES} times holds {} pages, not 1 -- this test is not \
         measuring an overwrite",
        set_blocks.len()
    );
    let set_ordinal = ordinal_for_component(&engine, "set", "cpo-ow-set", &component)
        .expect("the member's page is present");

    for i in 0..REWRITES {
        write(
            &engine,
            Command::HashSet {
                key: "cpo-ow-hash".to_string(),
                field: "the-one-field".to_string(),
                value: format!("v{i}").into_bytes(),
            },
        );
    }
    let hash_blocks = blocks_of(&engine, "hash", "cpo-ow-hash");
    assert_eq!(
        hash_blocks.len(),
        1,
        "a hash of one field rewritten {REWRITES} times holds {} pages, not 1",
        hash_blocks.len()
    );
    let hash_ordinal = ordinal_for_component(&engine, "hash", "cpo-ow-hash", "the-one-field")
        .expect("the field's page is present");

    println!(
        "\n=== overwrite === after {REWRITES} rewrites of ONE element: set ordinal {:?}, hash \
         ordinal {:?} (climbing would give Some({}))",
        set_ordinal,
        hash_ordinal,
        REWRITES - 1
    );
    assert_eq!(
        set_ordinal,
        Some(0),
        "the set member's ordinal climbed to {set_ordinal:?} over {REWRITES} rewrites. It must \
         stay 0: the ordinal counts elements, not writes"
    );
    assert_eq!(
        hash_ordinal,
        Some(0),
        "the hash field's ordinal climbed to {hash_ordinal:?} over {REWRITES} rewrites"
    );
}

// =================================================================================================
// 4. POSITION, NOT ELEMENT -- AND THE REUSE THAT PROVES IT
// =================================================================================================

/// THE ORDINAL NAMES A POSITION. A DELETE FREES IT AND THE NEXT INSERT IS HANDED IT BACK.
///
/// This is asserted rather than avoided, because it is what the tree does and the PR has to say
/// which of the two it is.
///
/// WHAT THE MECHANISM USED TO BE AND WHAT IT IS NOW. This said the page was REMOVED -- the body of
/// `mark_bucket_index_block_deleted_with` was a `retain` returning false -- and that was why `max`
/// fell. A removal now KEEPS an entry, pointing at the page that records it and carrying `deleted`,
/// so that a membership derived from the pages does not resurrect the element. The `max` still falls
/// for a different reason: `container_block_ordinal` filters `deleted`. So the CLAIM of this test is
/// unchanged and its MECHANISM moved, which is exactly the case worth spelling out -- a reader who
/// assumed the old mechanism would expect this test to be red.
///
/// AND THE SAFETY CONDITION IS ASSERTED IN THE SAME TEST: the page that inherits ordinal 2 carries
/// the NEW member's component, not the deleted one's. Identity stayed in the component, so the
/// reused ordinal names a place and never an element. If a future reader resolved an element by its
/// ordinal, this is the test that would have to change -- and it would be changing to describe
/// silent corruption.
///
/// rust-internal: drives ZSetAdd/ZSetRemove/ZSetAdd, no external surface
#[test]
fn the_ordinal_names_a_position_and_a_delete_frees_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let key = "cpo-pos-zset";
    for m in 0..3u64 {
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: format!("m{m}").into_bytes(),
                score: m as f64,
