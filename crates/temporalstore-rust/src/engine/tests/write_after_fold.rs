// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE AFTER A DERIVATION, WHICH IS THE ONLY THING THAT CAN SEE TWO FILERS DISAGREE.
//!
//! # WHY THIS SHAPE AND NOT A ROUND TRIP
//!
//! Two different pieces of code file a container's index entries: the PROJECTION
//! (`visit_model_live_blocks`, which a load and a compaction re-derive the whole index from) and
//! the WRITE PATH (`install_element` -> `upsert_bucket_index_block`, which files one entry as each
//! element is written). Under the one-entry-a-page gate the projection files one entry per PHYSICAL
//! PAGE carrying no component; the write path files one entry per ELEMENT carrying one.
//!
//! A test that writes, then reloads, cannot see that. A reload re-derives, so it reads the index
//! back the way the projection wrote it and the write path's filing is simply overwritten --
//! which is exactly why the store-boundary corpus test in this crate passes while the two routes
//! disagree, and why five landed steps that only RE-DERIVED could not find it. It takes a WRITE
//! AFTER A DERIVATION: fold the pages so the projection files one entry, then write again so the
//! write path files beside it, and compare.
//!
//! # THE THREE THINGS ASSERTED, AND WHY COUNTS ALONE WOULD NOT DO
//!
//!   1. ONE ENTRY PER DISTINCT PHYSICAL PAGE. Not "N entries", which means the collapse did not
//!      reach the write path; and the count is checked against the number of distinct page
//!      ADDRESSES rather than against a constant, so a fixture that happens to fold differently
//!      cannot make this vacuous.
//!
//!      THIS USED TO READ "and every entry carrying no component", asserted as a `named` counter
//!      beside each entry count. `BlockIndex` has no component field, so that half is held by the
//!      TYPE now and the counters are gone -- see `entry_census` for what enforces it instead.
//!      Each assertion that paired the two has been narrowed to the count, because pairing a live
//!      number with a structural constant hides which half failed.
//!   2. EVERY ELEMENT READS BACK, BY MEMBERSHIP AND NOT BY COUNT. This is the arm that catches the
//!      dangerous repair: filing one entry per element with NO component makes every element of one
//!      folded page share `(kind, object_key, component, address)`, so they collapse onto one
//!      handle and each write displaces the last. A container answering the right NUMBER of the
//!      wrong elements must fail here, so the members are compared as a set.
//!   3. THE WRITTEN KEY MATCHES WHAT A DERIVATION PRODUCES for the same page. `block_ref_key` is
//!      stored inside the lookup refs, so two routes that spell it differently file a page under a
//!      handle the other route will never compute -- a durably acknowledged write that reads
//!      MISSING. This is the arm the two `part4` rebuild-equivalence tests fail on.
//!
//! # EVERY FLOOR IS ON REACHING THE PATH
//!
//! `count == 1` is satisfied by an index that filed nothing at all, and `0 <= 0` has already passed
//! once in this campaign as "no regression" over a path nothing reached. So each arm first
//! establishes that the fixture actually folded, that the object actually has entries, and that the
//! durable map actually holds the elements -- and only then compares.

#![allow(clippy::all)]
use super::*;

const FIRST_BATCH: usize = 4;
const VALUE_WIDTH: usize = 24;
const KEY: &str = "waf/set";

/// Holds the gate at one value and puts back whatever was there -- on a normal drop AND while
/// unwinding, so a failing arm cannot leak it into every later test in the process.



fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("w-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

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
        table_name: "waf".to_string(),
        shard_uri: "local://waf/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn add(engine: &TemporalEngine, member: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetAdd {
            key: KEY.to_string(),
            member,
        },
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// This object's live entries: how many, the distinct physical pages
/// they resolve to, and the written key of each.
fn entry_census(engine: &TemporalEngine) -> (usize, std::collections::BTreeSet<(u64, u64, u64)>, Vec<String>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    // THE `named` TERM IS GONE FROM THIS CENSUS, AND THE CLAIM IT CARRIED IS NOW STRUCTURAL.
    //
    // It counted entries whose `component` was `Some`, and the arms below asserted it was zero --
    // "no live entry of any kind is filed under an element name". `BlockIndex` has no `component`
    // field, so the count could only ever be zero and an assertion on it could only ever pass:
    // nothing it observes could disagree, which is the definition of the vacuity this campaign
    // keeps deleting rather than leaving as apparent coverage.
    //
    // WHAT ENFORCES THE CLAIM INSTEAD, so it is not merely dropped: `engine_storage_state.rs` pins
    // `size_of::<BlockIndex>() == 40` with `!= 39 && != 41` beside it AND asserts the field sum
    // equals the width -- zero slack. Re-adding a name to the entry costs eight bytes at minimum
    // and sixteen for an `Option<Arc<str>>`, so it fails const-evaluation at the pin rather than
    // being caught by a counter here. The `written` keys this census still collects are the
    // second guard: `block_index_written_key` renders the stored spelling, and a name on an entry
    // would show up in it.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut blocks = std::collections::BTreeSet::new();
    let mut written = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            let object_key = named.at(bucket.routing_bucket, *handle);
            if block.deleted || block.model_id.as_str() != "set" || &**object_key != KEY {
                continue;
            }
            live += 1;
            blocks.insert((
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            ));
            written.push(crate::engine::state::block_index_written_key(object_key, block));
        }
    }
    written.sort();
    (live, blocks, written)
}

/// What the durable model map holds -- the authority for existence, independent of any entry.
fn durable_members(engine: &TemporalEngine) -> std::collections::BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .sets
        .get(KEY)
        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
        .unwrap_or_default()
}

/// What a client gets.
fn listed_members(engine: &TemporalEngine) -> std::collections::BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "the listing failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("expected Members, got {other:?}"),
    }
}

/// Re-derive the whole index from the model maps, which is what a load and a compaction do.
fn rederive(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 loaded");
    let (start, end) = shard.routing_range();
    crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);
}

/// THE WRITE PATH AND THE PROJECTION MUST FILE THE SAME ENTRY FOR THE SAME PAGE.
#[test]
fn a_write_after_a_fold_files_the_same_entry_the_projection_would() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // ---- Write a batch, then FOLD it, so the projection files one entry for one page. ----
    for index in 0..FIRST_BATCH {
        add(&engine, member_bytes(index));
    }
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (folded_live, folded_blocks, _) = entry_census(&engine);
    println!(
        "\n=== after the fold: {folded_live} live entr(ies), \
         {} distinct page(s)",
        folded_blocks.len()
    );

    // FLOOR: THE FIXTURE ACTUALLY FOLDED. Without this, "one entry per page" is trivially true
    // because every element is still its own page and the gate is doing nothing.
    assert_eq!(
        1,
        folded_blocks.len(),
        "the {FIRST_BATCH} members resolve to {} distinct pages after the fold, so they never came \
         to share one and this test has no collapse to observe",
        folded_blocks.len()
    );
    // RESTATED TO THE ONE TERM THAT CAN STILL VARY. This was `(1, 0) == (folded_live,
    // folded_named)`, pairing the entry count with the number of entries naming an element. The
    // second term is structurally zero -- `BlockIndex` has no name to carry -- so pairing it in
    // weakened the assertion rather than strengthening it: a wrong entry count and a constant zero
    // read as one failure, and the zero half could never contribute one. The count stands alone,
    // compared against the distinct page count asserted just above it rather than against a
    // literal.
    assert_eq!(
        1, folded_live,
        "after the fold the projection filed {folded_live} entr(ies) over {} distinct page(s); it \
         must file exactly one per page",
        folded_blocks.len()
    );

    // ---- NOW A WRITE AFTER THAT DERIVATION. This is the only thing that can see the two
    //      filers disagree: it is the write path filing beside what the projection just filed. ----
    add(&engine, member_bytes(FIRST_BATCH));
    let (live, blocks, written) = entry_census(&engine);
    let durable = durable_members(&engine);
    let listed = listed_members(&engine);
    println!(
        "  after one more write: {live} live entr(ies), \
         {} distinct page(s); durable holds {}, the listing serves {}",
        blocks.len(),
        durable.len(),
        listed.len()
    );

    // ---- FLOOR: THE STORE STILL HOLDS EVERYTHING, so a short listing below is about the index
    //      and not about a store that lost the members. ----
    let expected: std::collections::BTreeSet<Vec<u8>> =
        (0..=FIRST_BATCH).map(member_bytes).collect();
    assert_eq!(
        expected, durable,
        "the durable map holds {} of {} members after the extra write, so every number below is \
         about a store that lost them rather than about how they are filed",
        durable.len(),
        expected.len()
    );
    // FLOOR: the object still has entries at all.
    assert!(
        live > 0,
        "the object has no live entries, so 'one entry per page' below would pass over an index \
         that filed nothing"
    );

    // ---- 1. ONE ENTRY PER DISTINCT PHYSICAL PAGE, NONE NAMING AN ELEMENT. ----
    //
    // Compared against the number of distinct page addresses rather than a constant, so a fixture
    // that folds differently cannot make this vacuous.
    assert_eq!(
        blocks.len(),
        live,
        "the write path filed {live} entr(ies) for {} distinct page(s). Under this gate the page \
         IS the identity, so a write after a derivation must converge on the page's existing entry \
         rather than add one of its own",
        blocks.len()
    );
    // THE `named == 0` ARM IS DELETED, AND ITS SUBJECT IS STILL GUARDED -- by arm 3 below.
    //
    // It asserted that no entry named an element after a write following a derivation, to catch
    // the write path filing per-element identity while the projection files per-page. An entry has
    // no field for an element name, so this could only pass.
    //
    // WHAT THE ARM WAS REALLY FOR SURVIVES INTACT: the consequence it was watching for is that
    // "the two routes then spell the same page's handle differently", and the WRITTEN KEY
    // comparison further down asserts exactly that, by comparing the write path's keys against a
    // re-derivation's for the same pages. That arm can still fail, and it fails on the thing that
    // matters rather than on the mechanism that used to produce it.

    // ---- 2. EVERY ELEMENT READS BACK, BY MEMBERSHIP. ----
    //
    // THE ARM THAT CATCHES THE DANGEROUS REPAIR. Filing one entry per ELEMENT with no component
    // makes every element of one folded page share (kind, object_key, component, address), so they
    // collapse onto one handle and each write displaces the last -- the 39-of-40 loss, arriving by
    // the write path. A set answering the right COUNT of the wrong members passes a count check
    // and fails this one.
    assert_eq!(
        expected, listed,
        "the listing served {} member(s) where the store holds {}. Compared as a SET rather than a \
         count, because the loss this guards against replaces members rather than dropping them",
        listed.len(),
        expected.len()
    );

    // ---- 3. THE WRITTEN KEY MATCHES WHAT A DERIVATION PRODUCES FOR THE SAME PAGE. ----
    //
    // `block_ref_key` is stored inside the lookup refs, so two routes that spell it differently
    // file a page under a handle the other will never compute. Re-derive and compare the keys.
    rederive(&engine);
    let (dlive, dpages, derived_written) = entry_census(&engine);
    println!(
        "  after a re-derivation: {dlive} live entr(ies), \
         {} distinct page(s)",
        dpages.len()
    );
    // FLOOR: the re-derivation produced something to compare against.
    assert!(
        dlive > 0 && !derived_written.is_empty(),
        "the re-derivation filed nothing, so the key comparison below would hold vacuously"
    );
    assert_eq!(
        blocks, dpages,
        "the two routes disagree about WHICH pages are live, so comparing their written keys is \
         comparing different populations"
    );
    assert_eq!(
        derived_written, written,
        "the write path and the projection spell this page's WRITTEN KEY differently. That key is \
         stored inside the lookup refs, so a page filed by one route is looked up under a handle \
         the other route never computes -- a durably acknowledged write that reads MISSING"
    );
}

/// WHAT SUPERSEDES THE ENTRY NAMING AN ELEMENT'S PREVIOUS PAGE.
///
/// The repair for the spelling disagreement is to key the upsert's convergence on the ADDRESS
/// instead of the component, because under this gate the page is the identity. This measures the
/// case that decides whether that is sufficient on its own: a member REWRITTEN after a fold moves
/// to a NEW page, so an address-keyed predicate matching only the new address cannot select the
/// entry naming the OLD one. If the current component-keyed convergence is what removes it, the
/// repair has to be handed the superseded address as well -- and the write path already reads it,
/// to compute the ordinal.
#[test]
fn a_rewrite_after_a_fold_leaves_one_entry_per_block_today() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    for index in 0..FIRST_BATCH {
        add(&engine, member_bytes(index));
    }
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (_, folded_blocks, _) = entry_census(&engine);
    // FLOOR: the fixture folded, or "one entry per page" below is trivially true.
    assert_eq!(
        1,
        folded_blocks.len(),
        "the members resolve to {} pages after the fold, so there is no shared page to rewrite off",
        folded_blocks.len()
    );

    // REWRITE a member that is already on the folded page.
    add(&engine, member_bytes(0));
    let (live, blocks, _) = entry_census(&engine);
    let durable = durable_members(&engine);
    let listed = listed_members(&engine);
    println!(
        "\n=== rewrite after a fold: {live} live entr(ies), \
         {} distinct page(s); durable {}, listing serves {}",
        blocks.len(),
        durable.len(),
        listed.len()
    );

    // FLOOR: nothing was lost, so the counts above are about filing and not about a lost store.
    let expected: std::collections::BTreeSet<Vec<u8>> =
        (0..FIRST_BATCH).map(member_bytes).collect();
    assert_eq!(
        expected, durable,
        "the durable map holds {} of {} members after the rewrite",
        durable.len(),
        expected.len()
    );
    assert_eq!(
        expected, listed,
        "the listing served {} member(s) of {} after a rewrite -- compared as a SET, because a \
         wrong-member answer has the same count as a right one",
        listed.len(),
        expected.len()
    );

    // THE MEASUREMENT THE REPAIR TURNS ON. One entry per distinct page means the entry naming the
    // member's OLD page was superseded. If this holds today, it holds BECAUSE the convergence is
    // component-keyed, and an address-keyed replacement must be given the old address too.
    assert_eq!(
        blocks.len(),
        live,
        "{live} live entr(ies) for {} distinct page(s) after a rewrite. Recorded as the state an \
         address-keyed convergence has to reproduce: if this is one-to-one today, the superseded \
         page's entry is being removed by the COMPONENT match, and keying on the new address alone \
         would leave it behind naming a dead page",
        blocks.len()
    );
}

/// A COMPONENT-LESS KIND RELOCATING, WHICH IS WHY THE REPAIR CANNOT BE UNCONDITIONAL.
///
/// `String`, `ControlState` and `ContextNode` all file `component: None` with ONE page per object.
/// For them the component-keyed convergence is exactly what supersedes a relocated page -- so a
/// predicate keyed on the address for EVERY kind would leave a live entry naming the dead page on
/// every one of these writes. Measured here so the repair's condition is derived from behaviour
/// rather than from a list of kind names someone remembers.
#[test]
fn a_component_less_kind_rewritten_still_resolves_to_one_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let key = "waf/string";
    for round in 0..3usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: member_bytes(round),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    let (live, blocks) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let named = super::BlockObjects::of(&shard.bucket_index);
        let mut live = 0usize;
        let mut blocks = std::collections::BTreeSet::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for (handle, block) in &bucket.block_index {
                if block.deleted
                    || block.model_id.as_str() != "string"
                    || &**named.at(bucket.routing_bucket, *handle) != key
