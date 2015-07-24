// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! #2019 CHANGED THE INPUT TO STEP 1b'S REFUTATION, AND THE VERDICT DOES NOT MOVE.
//!
//! THIS ROUTE IS ALREADY REFUTED AND SAYING SO IS THE FIRST JOB OF THIS MODULE, because the next
//! reader will otherwise re-derive it. `block_entry_name_pointer` scores it as two of six dependent
//! verdicts:
//!
//!   * STEP 1b -- the key moved to the bucket's object list -- entry 48, stride 56, REFUTED. On
//!     bytes it is two populations with opposite signs: -15.76 to -15.84 B a page on containers but
//!     +8.11 to +9.10 on routed keys at the shipped `0..=1023` range, because the object list grows
//!     by a key for every object and a routed bucket holds forty of them. On identity it is refuted
//!     outright: with the key off the entry, an entry's only handle on its object is
//!     `BlockAddress::object_id`, an `Option` answered as ZERO when absent.
//!   * STEP 2 -- an object ordinal -- "needs exactly the id-to-key map 1b would have created, so it
//!     falls with it".
//!
//! SO WHY THIS MODULE EXISTS. Step 1b was measured while `bucket.object_index` held ONE ROW PER
//! PAGE -- #2007 measured 0.9983. #2019 collapsed that to one row per `(kind, key)`, measured 0.0400
//! per page on the container arm. The row count is the term step 1b's byte arithmetic divides by, so
//! the single number the refutation rested on changed by a factor of twenty-five two hours before
//! this module was written. That is a real reason to reopen it, and reopening it is what this module
//! does. THE VERDICT DOES NOT MOVE, and the reasons it does not are worth pinning because the next
//! collapse of the row count will raise the same question again.
//!
//! WHAT IS RE-MEASURED, AND WHAT IS NEW.
//!
//!   1. THE ARITHMETIC IS STILL SOUND, AND THE `u8` STILL BUYS NOTHING. Replacing `object_key`
//!      alone takes the entry 64 -> 48 and the stride 72 -> 56: sixteen bytes a page, reconstructed
//!      with `offset_of!` on mirror structures rather than asserted. A `u8` ordinal lands on the
//!      SAME 48, so its ceiling of 255 objects a bucket -- reachable, since a bucket is a hash
//!      bucket that fills with the corpus -- buys precisely zero additional bytes. Nothing should be
//!      spent on it. (`block_entry_name_pointer`'s step 2 row reads 40/48 because it composes an
//!      ordinal with step 1a's thin `component` as well; this row substitutes `object_key` only.)
//!
//!   2. #2019'S COLLAPSE DOES NOT REOPEN STEP 1b, and the reason is the arm it did not touch. The
//!      trade is `saving * pages - name_row * rows`, and it is positive only where a key holds more
//!      than one page. Measured per arm and never averaged: the container arm is where #2019's
//!      collapse lands and it pays, and THE ROUTED ARM STILL HOLDS ONE ROW PER PAGE -- its component
//!      was already absent, so #2019 was the identity on it -- which makes its trade a CEILING of
//!      zero before the list's own allocation is charged at all. The standing +8.11 to +9.10 is that
//!      ceiling with the allocation charged. #2019 published the distribution that decides which arm
//!      a real store is and it is repeated here rather than buried: ACROSS A REAL CORPUS, pages per
//!      `(kind, key)` is p50 1, p90 1, p99 1. This module's fixture is deliberately NOT that
//!      distribution -- 8 containers beside 200 routed keys puts a container at p99 -- because a
//!      sample matching the corpus would hold no visible container effect to measure the ceiling on.
//!      The fixture's own histogram is printed with its sample count so the two are not confused.
//!
//!   3. THE ORDINAL WAS NOT STABLE, AND THAT LEG IS NOW SETTLED THE OTHER WAY. It read: step 2
//!      falls as a DEPENDENT of 1b, which leaves unexamined whether the ordinal would work even if
//!      the map existed, and it would not -- `ObjectIndex::Many` was a SORTED RUN, so `insert` placed
//!      a new id at its bisection position and shifted every later ordinal, `remove` closed the hole
//!      the same way, and `shrink` collapsed the arm to `One` at length one so ordinal 1 stopped
//!      existing. An ordinal was therefore invalidated by an ORDINARY INSERT, the most common
//!      operation this engine performs.
//!
//!      THAT IS NO LONGER TRUE. `ObjectIndex::Many` IS A SLOT ARRAY WITH PLACEHOLDERS: `insert` takes
//!      the first free slot and otherwise appends, `remove` leaves a placeholder, nothing closes a
//!      hole, and the collapse to `One` survives only where it cannot renumber anything.
//!      `a_slot_into_the_object_list_survives_the_three_mutations_that_invalidated_an_ordinal` drives
//!      the same three mutations against the same watched position, and every one of them now leaves
//!      it naming the same object.
//!
//!      WHAT IT COST, both measured below rather than conceded. THE BISECTION:
//!      `ObjectIndex::contains` is a WALK now, because slot order is not id order -- and at the run
//!      lengths this store holds, p50 1 and MAX 2, a walk and a bisection are the same number, so the
//!      cost is zero at the shipped distribution and a divergence only on long container lists. THE
//!      BYTES: the placeholders cost nothing that persists -- the array is bounded by the bucket's own
//!      high water mark, 1.000 slots an object at two widths across four churn shapes -- but a SLOT is
//!      an `Option<u64>` at 16 bytes against the run element's 8, because object ids are full-range
//!      FNV-1a hashes and `u64::MAX` is a legitimate one, so no value can be reserved to mean free.
//!
//!      AND IT REOPENS NOTHING ON ITS OWN. Stability was one of three reasons. Legs 1, 2 and 4 are
//!      untouched: the entry still saves sixteen bytes and no more, the routed arm's ceiling is still
//!      zero, and -- the one that decides it -- the list still holds BARE IDS while 24 of 41
//!      production readers want the CHARACTERS. A stable slot resolves to an id the address already
//!      answers for nothing. So this is a precondition that now holds, not a verdict that has moved.
//!
//! WHAT THE LIST HOLDS, WHICH IS WHY 1b NEEDED A MAP AT ALL. `ObjectIndex` is
//! `Empty | One(u64) | Many(Box<ObjectSlots>)` -- bare ids in stable slots, no characters. An ordinal
//! into it resolves to an id the entry can ALREADY answer for nothing, out of its address. The readers
//! that matter want the characters, and the list has never held any. Making the slot stable did not
//! change that, which is why it changes no verdict by itself.
//!
//! THE READER COUNT HAS MOVED SINCE #1986 AND THE MOVEMENT IS REAL, not a counting error. #1986
//! recorded 47 production lines, twelve of them class 2. Walked at `7b049cb06`, the tree holds 41
//! production READS of this field across 26 functions plus 6 writes; 24 of the 41 want the
//! characters, to key `dirty_objects` and `expires_at_ms`, to probe the character-keyed
//! `ObjectBlockLookup`, to render `block_index_written_key`, or to report the key. Four have no
//! `BucketNode` in scope at all -- `block_index_handle` and `block_index_written_key` take a bare
//! `&BlockIndex`, and `block_index_handle` is the hottest reader in the tree, one call per page
//! insert -- so for those an ordinal has no bucket to resolve against. Six more sit inside `retain`
//! or `blocks_mut_unaccounted` closures where the node is MUTABLY borrowed. Those counts are stated
//! as a dated measurement, not as a list this module maintains.
//!
//! PRODUCT CODE DOES MOVE NOW -- `ObjectIndex` itself -- AND `SHARD_INDEX_FORMAT_VERSION` STILL DOES
//! NOT. What is stored is the IDS and not the slots: the Serialize impl writes them ascending, which
//! is the order the sorted run wrote and so the order already on disk, and a load re-files them
//! through `insert`, which hands slots out afresh. So a slot is a RESIDENT fact and no stamp is owed.
//! `the_slot_array_is_persisted_so_the_written_bytes_moved_and_the_stamp_was_taken` drives both
//! halves. It was `nothing_persists_a_slot_so_the_written_bytes_do_not_move`, and it is the
//! tripwire that went red when the slot became durable.
//!
//! THE STAMP IS WHAT THE NEXT STEP WOULD NEED, not this one. A page entry naming its object by slot
//! makes the slot DURABLE, and that is the change that takes the next value above
//! `SHARD_INDEX_FORMAT_VERSION` -- never a reserved one, because `persistence.rs` compares with `<`
//! and so ACCEPTS a lower stamp and misreads it rather than refusing it. #2019's own bump records the
//! shape of the hazard: an old index decodes CLEANLY because both sides of the generation check come
//! off the wire, and the disagreement appears later, on a recovery path.
//!
//! AND THE CHANGE THAT SHEDS `object_id` FROM `BlockAddress` TAKES 6. The constant was 5 when that
//! change landed, so 6 is the next value above what the tree actually held -- read at landing and
//! not at commit, because landing order is not commit order. 4 was reserved for it while the
//! constant was 3, and that reservation is VOID: spending it would LOWER the constant, and the
//! one-sided `<` named above turns a lowering into a silent ACCEPT rather than a refusal. A stamp
//! may only ever increase.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::hashing::stable_block_object_id;
use crate::engine::state::{BlockIndex, ObjectIndex};
use crate::engine::storage_bucket_internals::StoredModelKind;

const OPERATOR_END: u32 = 1023;
/// The population #2019 measured on, so the rows here are comparable to its line for line.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population: a string page's component is already `None`, one page per key, so #2019
/// was the identity on it and its trade has a ceiling of zero. Both facts are asserted, not assumed.
const ROUTED_KEYS: usize = 200;

/// What the entry gives up per page if `object_key` becomes a `u16` ordinal -- NOTHING, NOW.
///
/// It was sixteen, and the sixteen were real: an `Arc<str>` on every page against a two-byte
/// ordinal. THOSE SIXTEEN BYTES HAVE BEEN TAKEN, by a different route. The identity left the entry
/// altogether and lives once per object in `CoreIndex::object_block_lookup`; the entry is
/// twenty-four bytes and the stride thirty-two, which is exactly where the ordinal would have
/// landed. So an ordinal now narrows nothing at all.
///
/// AND THAT IS NOT THIS MODULE'S ROUTE BEING ADOPTED. Section 3 refuses the entry POINTING AT THE
/// BUCKET'S OBJECT LIST, on identity grounds that have nothing to do with width -- a list ordinal
/// is a position, and a position is not a way back to the characters. The lookup is a different
/// structure that already held one key per object before this change and holds exactly one now.
/// What changed is that the saving this module's section 1 priced is spent, so the refusal rests
/// on the identity grounds it always had PLUS the observation that there is nothing left to buy.
///
/// Derived below and asserted against the derivation, never read as this literal.
const CLAIMED_SAVING_PER_BLOCK: usize = 0;

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

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "entry-points-at-object-list".to_string(),
        shard_uri: "local://entry-points-at-object-list/1".to_string(),
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

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: format!("h{k}-f{f}-value").into_bytes(),
            });
        }
    }
    ack_batch(engine, commands);
}

fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: format!("s{i}-value").into_bytes(),
            })
            .collect(),
    );
}

/// `(live pages, distinct object-index rows)` for one kind, read off the buckets themselves. The
/// same shape #2019's `blocks_and_index_rows` uses, so the two modules' rows are comparable.
fn blocks_and_index_rows(
    engine: &TemporalEngine,
    kind: &str,
    keys: &BTreeSet<String>,
) -> (usize, usize) {
    let wanted: BTreeSet<u64> = keys
        .iter()
        .map(|key| stable_block_object_id(1, kind, key))
        .collect();
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut blocks = 0usize;
    let mut rows: BTreeSet<u64> = BTreeSet::new();
    for (_routing_bucket, _handle, object_key, block) in
        super::blocks_with_identity(&shard.bucket_index)
    {
        if block.deleted || block.model_id.as_str() != kind {
            continue;
        }
        if !keys.contains(&*object_key) {
            continue;
        }
        blocks += 1;
    }
    for bucket in shard.bucket_index.bucket_map.values() {
        for object_id in &bucket.object_index {
            if wanted.contains(object_id) {
                rows.insert(*object_id);
            }
        }
    }
    (blocks, rows.len())
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

/// The pages one `(kind, key)` holds, counted off the bucket index.
fn blocks_of_key(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = 0usize;
    for (_routing_bucket, _handle, object_key, block) in
        super::blocks_with_identity(&shard.bucket_index)
    {
        if !block.deleted && block.model_id.as_str() == kind && &*object_key == key {
            held += 1;
        }
    }
    held
}

// =================================================================================================
// THE MIRROR STRUCTURES
//
// The product entry with `object_key` replaced by an ordinal, and nothing else touched. Never
// constructed -- only its layout is read -- so the fields are dead by design.
// =================================================================================================

#[allow(dead_code)]
// THE MIRRORS LOSE THEIR NAME SLOT WITH THE REAL ENTRY'S.
//
// Both of these swap the entry's `object_key` for an ordinal into the bucket's object list, and
// both carried `component: Option<Arc<str>>` beside it because the real entry did. It does not, so
// a mirror that kept the slot would be sixteen bytes wider than the thing it mirrors and the
// counterfactual would price a shape the engine cannot reach. The ordinal proposal is about the
// OBJECT KEY and is unaffected by the element name leaving -- which is why these are restated
// rather than retired.
struct OrdinalEntryU16 {
    object_ordinal: u16,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

#[allow(dead_code)]
struct OrdinalEntryU8 {
    object_ordinal: u8,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// `(field bytes, slack)` for a hand-written field list against a measured total.
fn account(fields: &[(&'static str, usize, usize)], total: usize) -> (usize, usize) {
    let covered: usize = fields.iter().map(|(_, _, width)| *width).sum();
    (covered, total.saturating_sub(covered))
}

/// The width a `repr(Rust)` struct takes, reconstructed from a field list: every field whose width
/// is a whole number of words, plus the rest rounded up to the alignment.
fn reconstruct(fields: &[(&'static str, usize, usize)], word: usize) -> usize {
    let aligned: usize = fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word == 0)
        .sum();
    let tail: usize = fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word != 0)
        .sum();
    aligned + tail.div_ceil(word) * word
}

// =================================================================================================
// 1. THE ARITHMETIC, WHICH IS SOUND. SIXTEEN BYTES A PAGE, AND A `u8` BUYS NOTHING OVER A `u16`.
// =================================================================================================

/// THE ENTRY AND THE STRIDE, BEFORE AND AFTER, RECONSTRUCTED RATHER THAN ASSERTED AS LITERALS.
///
/// The stride is the number that matters and it is not the struct width: the page list is
/// `Vec<(u64, BlockIndex)>`, so a page costs its handle plus its entry. 64 + 8 = 72 today.
///
/// THE `u8` IS MEASURED AND DISMISSED IN THE SAME BREATH. Both ordinals leave a six- and a
/// five-byte tail, both round to eight, and both land the struct on 48 -- so the `u8`'s ceiling of
/// 255 objects a bucket buys precisely zero additional bytes. A bucket is a hash bucket that fills
/// with the corpus rather than a design with a bounded object count, so that ceiling is reachable:
/// 1,024 buckets times 255 is roughly 261,000 objects a shard. Nothing here should be spent on it.
///
/// rust-internal: reads this crate's own type layout, no product behaviour
#[test]
fn a_u8_ordinal_buys_nothing_over_a_u16_because_both_tails_round_the_same() {
    use std::mem::{align_of, offset_of, size_of};

    let entry_total = size_of::<BlockIndex>();
    let word = align_of::<BlockIndex>();

    // The product entry. One `offset_of!` per named field; `size_of` of the WRONG type still
    // compiles, which is what the reconstruction assertion below is for.
    let entry_fields: Vec<(&'static str, usize, usize)> = vec![
        // THE `object_key` ROW IS GONE WITH THE FIELD -- sixteen bytes of fat pointer, the whole
        // of the 40 -> 24 step. Each of these reconstructions is DERIVED from the field list it
        // holds rather than totalled as a literal, which is what makes deleting a row safe: a
        // list that stops matching the type fails at the reconstruction rather than describing a
        // structure that moved.
        ("model_id", offset_of!(BlockIndex, model_id), size_of::<StoredModelKind>()),
        // The `component` row is gone with the field: sixteen bytes between `model_id` and
        // `address`, and the whole of the 56 -> 40 step. The reconstruction assertion below is
        // what keeps this list honest against the type.
        ("address", offset_of!(BlockIndex, address), size_of::<ElementEntry>()),
        ("dirty", offset_of!(BlockIndex, dirty), size_of::<bool>()),
        ("deleted", offset_of!(BlockIndex, deleted), size_of::<bool>()),
    ];
    let (covered, slack) = account(&entry_fields, entry_total);

    let mut sorted = entry_fields.clone();
    sorted.sort_by_key(|(_, offset, _)| *offset);
    println!("\n--- BlockIndex, {entry_total} bytes, align {word} ---");
    let mut cursor = 0usize;
    for (name, offset, width) in &sorted {
        println!(
            "  +{offset:>3}  {name:<12} width {width:>2}  padding before {}",
            offset - cursor
        );
        cursor = offset + width;
    }
    println!("  tail padding {}", entry_total - cursor);
    println!("  field bytes {covered}, slack {slack}, total {entry_total}");

    assert_eq!(
        reconstruct(&entry_fields, word),
        entry_total,
        "the reconstruction of BlockIndex no longer adds up to its width. If a field's TYPE \
         changed, the field list above is what needs editing"
    );
    assert_eq!(covered + slack, entry_total);

    // THE COUNTERFACTUALS, measured off real mirror structures by this compiler.
    let u16_total = size_of::<OrdinalEntryU16>();
    let u8_total = size_of::<OrdinalEntryU8>();
    let u16_fields: Vec<(&'static str, usize, usize)> = vec![
        ("object_ordinal", offset_of!(OrdinalEntryU16, object_ordinal), size_of::<u16>()),
        ("model_id", offset_of!(OrdinalEntryU16, model_id), size_of::<StoredModelKind>()),
        ("address", offset_of!(OrdinalEntryU16, address), size_of::<ElementEntry>()),
        ("dirty", offset_of!(OrdinalEntryU16, dirty), size_of::<bool>()),
        ("deleted", offset_of!(OrdinalEntryU16, deleted), size_of::<bool>()),
        ("log_backed", offset_of!(OrdinalEntryU16, log_backed), size_of::<bool>()),
    ];
    assert_eq!(
        reconstruct(&u16_fields, align_of::<OrdinalEntryU16>()),
        u16_total,
        "the mirror structure's reconstruction does not add up, so its width is not evidence"
    );

    // The stride: a handle plus an entry, which is what the page list actually holds an element of.
    let stride_now = size_of::<(u64, BlockIndex)>();
    let stride_u16 = size_of::<(u64, OrdinalEntryU16)>();
    let stride_u8 = size_of::<(u64, OrdinalEntryU8)>();
    let saving = stride_now - stride_u16;

    println!("\n=== entry and stride, before and after ===");
    println!("  shape          entry  stride");
    println!("  object_key     {entry_total:>5}  {stride_now:>6}");
    println!("  u16 ordinal    {u16_total:>5}  {stride_u16:>6}");
    println!("  u8  ordinal    {u8_total:>5}  {stride_u8:>6}");
    println!("  saving per page: {saving} bytes of stride");

    assert_eq!(
        stride_now,
        size_of::<u64>() + entry_total,
        "the stride is not a handle plus an entry, so the page list is not the shape this \
         measurement prices"
    );
    // THE ORDINAL LANDS EXACTLY WHERE THE ENTRY ALREADY IS, which is why this is an equality and
    // not a `<`. It asserted `u16_total < entry_total` -- the ordinal narrows the entry -- and that
    // held while the entry carried an `Arc<str>`. The identity left; both are twenty-four.
    assert_eq!(
        u16_total, entry_total,
        "a u16 ordinal lands on {u16_total} against the live entry's {entry_total}. The sixteen \
         bytes a page it was priced at have already been taken by the identity leaving the entry, \
         so it must land on exactly the same width -- if it narrows further, something else has \
         moved and this module's arithmetic needs remeasuring"
    );
    assert_eq!(
        u16_total, u8_total,
        "a u8 ordinal lands on {u8_total} where the u16 lands on {u16_total}. The whole reason to \
         refuse the u8 is that it buys NOTHING over the u16 while capping a bucket at 255 objects; \
         if that has stopped being true this advice needs rewriting"
    );
    assert_eq!(
        stride_u16, stride_u8,
        "the two ordinal widths no longer share a stride"
    );
    assert_eq!(
        saving, CLAIMED_SAVING_PER_BLOCK,
        "the arithmetic this module refutes on OTHER grounds has itself moved: the stride saving \
         is {saving}, not {CLAIMED_SAVING_PER_BLOCK}. The per-arm trade is quoted against \
         this number and must be recomputed"
    );
    println!(
        "  VERDICT: the arithmetic is SPENT -- {saving} bytes a page, because the sixteen it \
         priced were taken by the identity leaving the entry. The u8 adds nothing to the u16 and \
         the u16 adds nothing to today. What follows is why the route was refused on identity \
         grounds that never depended on the width."
    );
}

// =================================================================================================
// 2. WHY 1b NEEDED A MAP: THE LIST HOLDS IDS, AND THE ENTRY ALREADY HAS THE ID FOR FREE.
// =================================================================================================

/// `object_index` IS A LIST OF `u64` OBJECT IDS. AN ORDINAL INTO IT CANNOT ANSWER A NAME.
///
/// This is the reason the proposal's "level one is built" is not true. The list that exists holds
/// ids; the readers that need the CHARACTERS -- 24 of the 41 walked at `7b049cb06` -- want the
/// KEY. And the id an ordinal would
/// resolve to is one the entry can already answer for nothing: `BlockIndex::object_id()` reads it
/// out of the address, which is why #2019 could delete the entry's own copy of it.
///
/// So the ordinal would buy a second route to something already free, and still leave every reader
/// of the name unserved. Asserted through the product's own accessor against the product's own
/// hash rather than by inspecting the type, so the claim is about behaviour and not declaration.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_object_list_holds_ids_and_the_entry_already_answers_the_id_without_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    let ordinal_names = super::BlockObjects::of(&shard.bucket_index);
    let mut rows_seen = 0usize;
    let mut blocks_seen = 0usize;
    let mut id_already_answered = 0usize;
    // Every row of every object list, and what an ordinal into it would resolve TO.
    for bucket in shard.bucket_index.bucket_map.values() {
        for _id in &bucket.object_index {
            // The item type of this iterator IS `&u64`. Binding it as one is the assertion: if the
            // list ever starts holding a name, this stops compiling and this module is what says
            // the refutation needs revisiting.
            let _: &u64 = _id;
            rows_seen += 1;
        }
        for (handle, block) in &bucket.block_index {
            // Only the kind this fixture seeded, so an incidental page of another kind cannot
            // decide the assertion below.
            if block.deleted || block.model_id.as_str() != "hash" {
                continue;
            }
            blocks_seen += 1;
            let object_key = ordinal_names.at(bucket.routing_bucket, *handle);
            // What the entry can answer WITHOUT an ordinal, from the address alone.
            let from_address = block.object_id(1, object_key);
            let recomputed = stable_block_object_id(1, block.model_id.as_str(), object_key);
            if from_address == recomputed {
                id_already_answered += 1;
            }
        }
    }

    println!("\n=== what an ordinal into the object list would resolve to ===");
    println!("  object-index rows walked : {rows_seen}");
    println!("  live pages walked        : {blocks_seen}");
    println!("  pages whose id the entry already answers from its address: {id_already_answered}");

    assert!(
        blocks_seen > 0 && rows_seen > 0,
        "DENOMINATOR: {blocks_seen} pages and {rows_seen} rows walked. A store that filed nothing \
         would satisfy every assertion below while observing nothing"
    );
    assert_eq!(
        id_already_answered, blocks_seen,
        "only {id_already_answered} of {blocks_seen} pages answer their own object id from the \
         address. The refutation's first reason is that an ordinal buys a second route to \
         something already free; if the address has stopped answering, that reason weakens"
    );
    println!(
        "  VERDICT: the list is {rows_seen} rows of u64 id. The id is ALREADY free from the \
         address on {id_already_answered}/{blocks_seen} pages, and the NAME is not in the list at \
         all -- so the ordinal serves no reader of `object_key`."
    );
}

// =================================================================================================
// =================================================================================================
// 3. THIS LEG IS SETTLED THE OTHER WAY NOW: THE LIST IS A SLOT ARRAY AND A SLOT IS STABLE.
//    WHAT IT COST IS MEASURED IN SECTIONS 5 AND 6. THE OTHER LEGS ARE UNAFFECTED.
// =================================================================================================

/// A SLOT INTO THE OBJECT LIST SURVIVES ALL THREE MUTATIONS THAT INVALIDATED AN ORDINAL.
///
/// WHAT THIS TEST USED TO SAY, because the record matters more than the verdict. It was
/// `an_ordinal_into_the_object_list_is_invalidated_by_an_ordinary_insert_not_only_a_delete`, and on
/// twenty-five ids it drove three separate losses against `ObjectIndex::Many` as a SORTED RUN:
/// `insert` placed a new id at its bisection position, so filing one unrelated SMALLER id took
/// position 12 from id 1300 to id 1200; `remove` closed the hole and took it to 1400; and `shrink`
/// collapsed the arm to `One` at length one, so position 1 stopped existing altogether. Its failure
/// message named the condition under which it would stop holding -- "if the list has stopped being a
/// sorted run, this module's third reason no longer holds" -- and that is what has happened.
///
/// `ObjectIndex::Many` IS NOW A SLOT ARRAY WITH PLACEHOLDERS. `insert` takes the first free slot and
/// otherwise appends, `remove` leaves a placeholder, and the collapse to `One` survives only where it
/// cannot renumber anything. So the SAME three mutations are driven here against the SAME watched
/// position, and every one of them must now leave it naming the same object.
///
/// DRIVEN THROUGH `id_at` AND NOT THROUGH `iter`. That distinction is the test: `iter` skips
/// placeholders, so a list read as a sequence still appears to shift when a hole opens below the
/// watched element -- which is exactly how a compacting implementation would pass a sequence-based
/// check while losing the slot. `id_at(12)` asks the question a stored slot would ask.
///
/// STILL TWENTY-FIVE ELEMENTS, and still each mutation separately: with one element the first insert
/// is also the last, and a fixture that only exercised deletes would report the insert as safe.
///
/// WHAT THIS DOES NOT REOPEN, which is the rest of the module. Legs 1, 2 and 4 are untouched: the
/// entry still saves sixteen bytes and no more, the list still holds bare ids while 24 of 41
/// production readers want the CHARACTERS, and the routed arm's ceiling is still zero. Stability was
/// one of three reasons and it is the only one this change removes.
///
/// rust-internal: drives this crate's own `ObjectIndex`, no product surface
#[test]
fn a_slot_into_the_object_list_survives_the_three_mutations_that_invalidated_an_ordinal() {
    const ELEMENTS: usize = 25;

    // Ids spaced so there is always room to insert strictly below an existing one -- the shape that
    // moved an ordinal with no delete anywhere near it.
    let seeded: Vec<u64> = (1..=ELEMENTS as u64).map(|i| i * 100).collect();
    let mut list = ObjectIndex::default();
    for id in &seeded {
        assert!(list.insert(*id), "the seed must file {id}");
    }
    assert_eq!(
        list.object_count(),
        ELEMENTS,
        "the fixture holds {} rows, not {ELEMENTS}, so nothing below is being observed",
        list.object_count()
    );
    assert!(
        ELEMENTS > 1,
        "a one-element fixture cannot observe a shift: the first insert would also be the last"
    );
    assert!(
        matches!(list, ObjectIndex::Many(_)),
        "the fixture never reached the multi-entry arm, so it is testing the inline id"
    );

    // The watched slot. Not the first and not the last, so a shift at either end is visible.
    let watched_slot = 12usize;
    let watched_id = list
        .id_at(watched_slot)
        .expect("the fixture must hold a live id in the watched slot");
    assert_eq!(
        list.slot_of(&watched_id),
        Some(watched_slot),
        "`slot_of` and `id_at` disagree on the fixture, so neither can witness stability"
    );

    println!("\n=== a slot's stability across the three mutations that moved an ordinal ===");
    println!("  rows {ELEMENTS}, watching slot {watched_slot}");
    println!("  before            : slot {watched_slot} -> id {watched_id}");

    // (a) INSERT BELOW. An unrelated key whose id sorts below everything held, no delete anywhere.
    // This is the mutation that made the route unsafe, and it is the most common thing the engine
    // does.
    let mut inserted = list.clone();
    assert!(inserted.insert(1), "the fixture must file the low id");
    let after_insert = inserted.id_at(watched_slot);
    println!(
        "  after insert(1)   : slot {watched_slot} -> id {after_insert:?}  (rows {}, slots {})",
        inserted.object_count(),
        inserted.slot_count()
    );
    assert_eq!(
        after_insert,
        Some(watched_id),
        "inserting an id BELOW the watched one moved slot {watched_slot} off id {watched_id}. The \
         slot array is not append-or-refill, so an insert still renumbers and the route is unsafe \
         again"
    );
    assert_eq!(
        inserted.slot_of(&1),
        Some(ELEMENTS),
        "the new id did not land in the first slot past the end, so it took a slot something else \
         was in"
    );

    // (b) REMOVE BELOW, which is the half the original proposal already suspected.
    let mut removed = list.clone();
    assert!(removed.remove(&100), "the fixture must remove the low id");
    let after_remove = removed.id_at(watched_slot);
    println!(
        "  after remove(100) : slot {watched_slot} -> id {after_remove:?}  (rows {}, slots {})",
        removed.object_count(),
        removed.slot_count()
    );
    assert_eq!(
        after_remove,
        Some(watched_id),
        "removing an id BELOW the watched one moved slot {watched_slot}: the hole was CLOSED rather \
         than left as a placeholder"
    );
    assert_eq!(
        removed.id_at(0),
        None,
        "slot 0 did not become a placeholder, so the array compacted"
    );
    assert_eq!(
        removed.slot_count(),
        ELEMENTS,
        "the array shortened after a removal from the MIDDLE of it, which it can only do by moving \
         something"
    );
    // And the hole is what the next insert takes, which is what bounds the array.
    let mut refilled = removed.clone();
    assert!(refilled.insert(7), "a fresh id must file into the hole");
    assert_eq!(
        refilled.slot_of(&7),
        Some(0),
        "the fresh id did not take the free slot, so holes are never reused and the array grows \
         with churn"
    );
    assert_eq!(
        refilled.slot_count(),
        ELEMENTS,
        "refilling a hole lengthened the array"
    );
    assert_eq!(
        refilled.id_at(watched_slot),
        Some(watched_id),
        "refilling a hole moved the watched slot"
    );

    // (c) THE ARM COLLAPSE, which is what retired position 1 outright. Two ids, remove the FIRST:
    // the survivor is in slot 1, so collapsing to `One` would renumber it to slot 0 and leave a
    // stored 1 dangling.
    let mut collapsing = ObjectIndex::default();
    assert!(collapsing.insert(100));
    assert!(collapsing.insert(200));
    assert_eq!(collapsing.object_count(), 2, "the collapse fixture must hold two");
    assert_eq!(collapsing.slot_of(&200), Some(1), "the second id must be in slot 1");
    assert!(collapsing.remove(&100), "remove the first of two");
    println!(
        "  arm collapse      : removed slot 0 of 2 -> rows {}, slots {}, slot0 {:?}, slot1 {:?}",
        collapsing.object_count(),
        collapsing.slot_count(),
        collapsing.id_at(0),
        collapsing.id_at(1)
    );
    assert!(
        matches!(collapsing, ObjectIndex::Many(_)),
        "the arm collapsed to `One` with a live id in slot 1. That is the collapse that retired \
         ordinal 1, and it renumbers the survivor to slot 0"
    );
    assert_eq!(collapsing.id_at(0), None, "slot 0 must be a placeholder");
    assert_eq!(collapsing.id_at(1), Some(200), "slot 1 must still name 200");
    assert_eq!(collapsing.object_count(), 1, "one id must be left");

    // (c') AND THE COLLAPSE THAT IS STILL ALLOWED, because it renumbers nothing: remove the SECOND
    // of two, and the survivor is already in slot 0 with no slot above it. The two cases differ by
    // exactly the thing being preserved, which is why both are driven.
    let mut collapsible = ObjectIndex::default();
    assert!(collapsible.insert(100));
    assert!(collapsible.insert(200));
    assert!(collapsible.remove(&200), "remove the second of two");
    assert!(
        matches!(collapsible, ObjectIndex::One(100)),
        "removing the LAST slot of two must give the allocation back: the survivor is in slot 0 \
         with nothing above it, so `One` and the array are indistinguishable through `id_at`. It is \
         {collapsible:?}"
    );
    assert_eq!(collapsible.id_at(0), Some(100), "slot 0 must still name 100");
    assert_eq!(collapsible.id_at(1), None, "there must be no slot 1");
    assert_eq!(collapsible.slot_count(), 1, "the array must be one slot long");

    // (d) EMPTYING gives everything back, which is safe because no live slot remains to preserve.
    let mut emptying = collapsing.clone();
    assert!(emptying.remove(&200), "remove the last id");
    assert!(
        matches!(emptying, ObjectIndex::Empty),
        "an emptied index must cost nothing again; it is {emptying:?}"
    );

    println!(
        "  VERDICT: slot {watched_slot} names id {watched_id} before and after an insert below, a \
         remove below, a refill of the hole, and the arm collapse. The three mutations that \
         invalidated an ordinal no longer do. What it cost is sections 5 and 6."
    );
}

// =================================================================================================
// 4. #2019'S ROW COLLAPSE DOES NOT REOPEN STEP 1b, BECAUSE IT DID NOT TOUCH THE ROUTED ARM.
//    REPORTED AS A CEILING ON THE PRIZE, AND THE CEILING ON THE ROUTED ARM IS ZERO.
// =================================================================================================

/// THE ROW COUNT #2019 COLLAPSED IS THE TERM STEP 1b DIVIDES BY, AND THE ROUTED ARM DID NOT MOVE.
///
/// A name the entry stops holding has to be held somewhere, once per row, so the trade is
/// `saving * pages - name_row * rows`. DIVIDED PER ARM AND NEVER AVERAGED: that division is what
/// #2007's routed arms failed, and an average across the two arms would report a win.
///
/// THIS IS A CEILING ON THE PRIZE, NOT THE PRIZE. Only the name pointer is charged here -- one
/// `Arc<str>` a row. The list's own allocation is NOT charged: a `Box<Vec<u64>>` widened to carry
/// names pays a vector header once a bucket, allocator rounding, and growth slack, none of which
/// appears below. So the real figure is WORSE than this on every arm, and the standing measurement
/// says how much worse: `block_entry_name_pointer` scores step 1b at -15.76 to -15.84 B a page on
/// containers and +8.11 to +9.10 on routed keys at the shipped range, where the sign has flipped
/// because the list grows by a key for every object and a routed bucket holds forty.
///
/// A CEILING IS THE RIGHT SHAPE FOR THIS CLAIM. If even the ceiling is zero on the routed arm, no
/// accounting of the list can make that arm pay, and the arm is where a real store's keys are.
///
/// THE CONTROL IS THE ROUTED ARM, whose component was already absent -- so #2019 was the IDENTITY
/// on it and its rows-per-page cannot have moved off 1.0000. Its page count and its row count are
/// both asserted BEFORE its result: a control that exercised no pages reports no movement for the
/// wrong reason, and a control over zero rows reports success whatever the trade.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_row_collapse_does_not_reopen_the_route_because_the_routed_arm_did_not_move() {
    use std::mem::size_of;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    let hash_keys: BTreeSet<String> = (0..CONTAINER_KEYS).map(|k| format!("h{k}")).collect();
    let string_keys: BTreeSet<String> = (0..ROUTED_KEYS).map(|i| format!("s{i}")).collect();

    let (hash_blocks, hash_rows) = blocks_and_index_rows(&engine, "hash", &hash_keys);
    let (string_blocks, string_rows) = blocks_and_index_rows(&engine, "string", &string_keys);

    // The saving is measured, not assumed, so this arithmetic moves with a sibling width step.
    let saving = size_of::<(u64, BlockIndex)>() - size_of::<(u64, OrdinalEntryU16)>();
    // What one row of the list that would have to exist costs: a shared pointer to the name.
    let row_cost = size_of::<Arc<str>>();

    let net_per_block = |blocks: usize, rows: usize| -> f64 {
        if blocks == 0 {
            return 0.0;
        }
        (saving as f64 * blocks as f64 - row_cost as f64 * rows as f64) / blocks as f64
    };

    let hash_net = net_per_block(hash_blocks, hash_rows);
    let string_net = net_per_block(string_blocks, string_rows);

    println!("\n=== the trade, per arm, never averaged -- A CEILING ON THE PRIZE ===");
    println!("  saving {saving} B/page of stride, name row {row_cost} B");
    println!("  the list's own allocation is NOT charged, so every row below is optimistic");
    println!("  arm       keys  pages  rows  rows/page  ceiling B/page");
    for (arm, keys, blocks, rows, net) in [
        ("hash", hash_keys.len(), hash_blocks, hash_rows, hash_net),
        ("string", string_keys.len(), string_blocks, string_rows, string_net),
    ] {
        println!(
            "  {arm:<8} {keys:>5} {blocks:>6} {rows:>5}   {:>8.4}  {net:>+10.2}",
            rows as f64 / blocks as f64
        );
    }

    // DENOMINATORS FIRST, BOTH ARMS.
    assert_eq!(
        hash_blocks,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the container arm exercised {hash_blocks} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        string_blocks, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_blocks} string pages, not {ROUTED_KEYS}. A control that \
         read nothing reports no movement for the wrong reason"
    );
    assert!(
        string_rows > 0,
        "CONTROL OVER ZERO ROWS: a control whose list is empty reports success whatever the trade"
    );
    assert!(
        hash_blocks > hash_rows,
        "DENOMINATOR: {hash_blocks} pages and {hash_rows} rows on the container arm. A fixture \
         whose keys held one page each would measure nothing"
    );

    // THE CONTROL. #2019 was the IDENTITY on this arm -- a string page's component was already
    // absent -- so its rows-per-page cannot have moved off 1.0000, and that is the whole reason the
    // collapse does not reopen the route.
    assert_eq!(
        string_rows, string_blocks,
        "the control holds {string_rows} rows for {string_blocks} pages. #2019 was the identity on \
         this arm, so one row per page is what it must still hold; if this has moved, the route IS \
         reopened and this module's verdict needs remeasuring"
    );
    assert!(
        string_net <= 0.0,
        "the control's CEILING is {string_net:+.4} B/page, above zero. Only the name pointer is \
         charged here, so a positive ceiling would mean the arm could pay once the list's \
         allocation is charged too -- which is the opposite of what the standing +8.11 to +9.10 \
         measures"
    );

    // THE CONTAINER ARM'S TWO CEILING ASSERTIONS ARE RETIRED, BECAUSE THE SAVING THEY WERE A
    // CEILING AGAINST IS ZERO.
    //
    // They read `hash_net > 0.0` and `hash_net < saving as f64`, where
    // `hash_net = (saving * pages - row_cost * rows) / pages`. The stride saving an ordinal would
    // buy is now nothing -- `CLAIMED_SAVING_PER_BLOCK` carries why -- so `hash_net` is the row cost
    // with a minus sign, and a ceiling expressed as a fraction of a zero saving is not a ceiling.
    //
    // THE FIRST ONE'S OWN MESSAGE SAYS WHAT TO CONCLUDE: "If containers have stopped paying,
    // nothing pays and the route is refuted even more cheaply." Nothing pays, because there is
    // nothing left to pay for. That is the conclusion rather than the failure, and it is recorded
    // here rather than asserted as a negative ceiling that would pass for the wrong reason.
    //
    // AND THIS TEST'S OWN SUBJECT IS UNTOUCHED, which is why it survives rather than being
    // retired whole. Its name is the claim: the row collapse does not reopen the route BECAUSE
    // THE ROUTED ARM DID NOT MOVE, and the control above -- one row per page on the string arm,
    // with a ceiling at or below zero -- is what holds that. Both of those still assert and both
    // still pass.
    assert!(
        hash_net <= 0.0,
        "the container arm's ceiling is {hash_net:+.4} B/page against a {saving} B/page stride \
         saving. With nothing left to save, the only thing this expression can report is the \
         list's own row cost as a negative -- a positive here would mean the saving is back"
    );

    println!(
        "  VERDICT: container ceiling {hash_net:+.2} B/page, CONTROL routed ceiling \
         {string_net:+.2} B/page over {string_blocks} pages and {string_rows} rows. #2019 collapsed \
         the container arm's rows/page by 25x and did NOT move the routed arm, whose ceiling is \
         zero before the list's allocation is charged at all -- so step 1b stays refuted."
    );

    // AND THE DISTRIBUTION THAT DECIDES WHICH ARM A REAL STORE IS, as #2019 published it. A
    // histogram rather than a mean, because the whole question is the tail.
    let mut per_key: Vec<usize> = Vec::new();
    for key in &hash_keys {
        per_key.push(blocks_of_key(&engine, "hash", key));
    }
    for key in &string_keys {
        per_key.push(blocks_of_key(&engine, "string", key));
    }
    per_key.sort();
    let single: usize = per_key.iter().filter(|blocks| **blocks <= 1).count();
    println!(
        "  pages per (kind,key) over {} keys: p50={} p90={} p99={} MAX={}",
        per_key.len(),
        percentile(&per_key, 50.0),
        percentile(&per_key, 90.0),
        percentile(&per_key, 99.0),
        per_key.last().copied().unwrap_or(0)
    );
    println!(
        "  keys holding ONE page: {single}/{} ({:.2}%) -- each of them nets exactly zero",
        per_key.len(),
        100.0 * single as f64 / per_key.len() as f64
    );
    assert_eq!(
        percentile(&per_key, 50.0),
        1,
        "p50 pages per key is not 1 on a fixture that is {ROUTED_KEYS} single-page keys beside \
         {CONTAINER_KEYS} containers, so the caveat this module repeats would be wrong"
    );
    assert_eq!(
        per_key.last().copied().unwrap_or(0),
        MEMBERS_PER_KEY,
        "MAX pages per key is not {MEMBERS_PER_KEY}: the container arm is what makes the effect \
         visible and it is not in this sample"
    );
    assert!(
        single > 0,
        "no key in the sample holds a single page, so the zero-net population is unobserved"
    );
}

// =================================================================================================
// 5. THE JOIN, PRICED IN PROBES RATHER THAN IN TIME.
// =================================================================================================

/// WHAT A READER OF THE NAME WOULD PAY, COUNTED IN EXAMINED ENTRIES -- AND WHAT THE SLOT ARRAY COST.
///
/// A timing instrument is useless here -- on this box it has read 485x idle against 11x busy off
/// identical code -- so the join is priced in the engine's own probe counters instead.
///
/// THIS TEST HAS CHANGED SIDES AND THE NUMBER IT ASSERTS IS NOW THE WALK. It used to read
/// `..._is_logarithmic_today_and_linear_with_stable_slots`, pinning the measured count to
/// `floor(log2(n)) + 1` and asserting only that a walk could not be cheaper. The slot array landed,
/// so the walk is what the product does: `ObjectIndex::contains` reads slots in order, placeholders
/// included, because slot order is not id order. Both models are still printed, and the bisection is
/// now the thing that was GIVEN UP rather than the thing that is.
///
/// AND AT THESE RUN LENGTHS THE TWO COINCIDE, which is reported rather than hidden: the measured runs
/// are p50 1 and MAX 2, and `floor(log2(n)) + 1 == n` for n of 1 and 2. So this module establishes
/// that the change costs NOTHING MEASURABLE at the distribution the shipped routing range produces,
/// and that the direction on a store whose buckets hold long object lists is a walk. It does NOT
/// establish that the walk is expensive -- that would need such a store. The inequality is asserted;
/// the equality at this distribution is asserted too, because it is the result.
///
/// PLACEHOLDERS ARE CHARGED. `locate` counts every slot it reads, so an array carrying holes pays for
/// them here. That is deliberate: it is the one place the waste measured in section 6 could hide.
///
/// rust-internal: reads this crate's own probe counters, no product surface
#[test]
fn the_join_a_name_reader_would_pay_is_a_walk_now_and_was_logarithmic_as_a_sorted_run() {
    use crate::engine::state::{
        entries_a_bisection_examines, object_index_entries_examined,
        reset_object_index_entries_examined,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    // The run lengths the object lists actually hold, off the buckets -- and the SLOT counts beside
    // them, which are what the walk reads. On a store that has only been written to they are equal;
    // the two are collected separately so a divergence would show rather than be assumed away.
    let mut run_lengths: Vec<usize> = Vec::new();
    let mut slot_counts: Vec<usize> = Vec::new();
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        for bucket in shard.bucket_index.bucket_map.values() {
            let len = bucket.object_index.object_count();
            if len > 0 {
                run_lengths.push(len);
                slot_counts.push(bucket.object_index.slot_count());
            }
        }
    }
    assert!(
        !run_lengths.is_empty(),
        "DENOMINATOR: no bucket holds an object list, so no join is being priced"
    );
    run_lengths.sort();
    slot_counts.sort();

    // One membership question per bucket, charged through the product's own door.
    reset_object_index_entries_examined();
    let probed = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let mut asked = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            if let Some(first) = bucket.object_index.iter().next().copied() {
                assert!(
                    bucket.object_index.contains(&first),
                    "a bucket's own first id is not in its own list"
                );
                asked += 1;
            }
        }
        asked
    };
    let examined = object_index_entries_examined();

    // The two models. The walk reads slots until it finds the id; asking for the FIRST id a bucket
    // holds means it stops at the first live slot, which on an array with no holes is slot 0 -- so
    // the walk's own worst case is the whole array and is printed beside it.
    let walk_worst: usize = slot_counts.iter().sum();
    let bisecting: u64 = run_lengths
        .iter()
        .map(|len| entries_a_bisection_examines(*len))
        .sum();

    println!("\n=== the join, in examined entries ===");
    println!("  buckets with a list : {}", run_lengths.len());
    println!(
        "  objects in a bucket : p50={} p90={} p99={} MAX={}",
        percentile(&run_lengths, 50.0),
        percentile(&run_lengths, 90.0),
        percentile(&run_lengths, 99.0),
        run_lengths.last().copied().unwrap_or(0)
    );
    println!(
        "  slots in a bucket   : p50={} p90={} p99={} MAX={}",
        percentile(&slot_counts, 50.0),
        percentile(&slot_counts, 90.0),
        percentile(&slot_counts, 99.0),
        slot_counts.last().copied().unwrap_or(0)
    );
    println!("  membership questions asked   : {probed}");
    println!("  entries examined, measured   : {examined}");
    println!("  what a bisection would charge : {bisecting}");
    println!("  the walk's worst case (all slots): {walk_worst}");
    if bisecting == examined {
        println!(
            "  NOTE: at these run lengths (MAX {}) the walk and a bisection COINCIDE, so the \
             divergence is asserted below as an inequality and is NOT demonstrated by this \
             fixture. floor(log2(n))+1 == n for n in 1..=2, which is where this store sits.",
            run_lengths.last().copied().unwrap_or(0)
        );
    }

    assert!(
        probed > 0 && examined > 0,
        "DENOMINATOR: {probed} questions and {examined} entries examined. A list that charged \
         nothing would report the join as free"
    );
    assert_eq!(
        run_lengths.len(),
        slot_counts.len(),
        "a bucket was counted in one sample and not the other, so the two models divide by \
         different denominators"
    );
    assert!(
        walk_worst as u64 >= examined,
        "the walk examined {examined} entries against its own worst case of {walk_worst}, which it \
         cannot exceed"
    );
    // A WALK IS BOUNDED AT BOTH ENDS AND A BISECTION IS NOT, which is the whole difference and is
    // why this is an interval rather than an equality. The question asked above is a bucket's own
    // FIRST id, so the walk stops at the first live slot -- its BEST case, one entry a bucket. A
    // bisection charges `floor(log2(n)) + 1` wherever the id is. So the measured walk can come out
    // CHEAPER than the model, and here it does: it is the low end of the interval, not a
    // disagreement between the probe and the model.
    assert!(
        examined <= walk_worst as u64,
        "the walk examined more than every slot it could have read"
    );
    assert!(
        examined >= run_lengths.len() as u64,
        "the walk examined {examined} entries for {} buckets, fewer than one apiece, so some \
         membership question did not go through the door that charges",
        run_lengths.len()
    );
    assert!(
        bisecting <= walk_worst as u64,
        "a bisection is modelled above the walk's own worst case over the same lists, which it \
         cannot be: {bisecting} against {walk_worst}"
    );
    println!(
        "  VERDICT: the walk examined {examined} entries for {probed} questions against the \
         {bisecting} a bisection would charge and its own worst case of {walk_worst}. At this \
         distribution -- every list one or two objects long -- the walk's best case is CHEAPER than \
         the bisection it replaced and its worst case EQUALS it, so this change costs nothing \
         measurable on the membership door. The divergence needs a store whose buckets hold long \
         object lists, which the shipped routing range does not produce."
    );
}

// =================================================================================================
// 6. WHAT THE SLOT ARRAY IS AND WHAT IT COSTS. THE WIDTH, THE PLACEHOLDER WASTE AT TWO CORPUS
//    SIZES, AND THE BYTES -- EACH WITH ITS DENOMINATOR.
// =================================================================================================

/// The slot array, mirrored. Never constructed -- only its layout is read -- so the fields are dead
/// by design, exactly as the entry mirrors above are.
#[allow(dead_code)]
struct MirrorObjectSlots {
    slots: Vec<Option<u64>>,
    valid: usize,
}

/// The enum, mirrored as the tag and payload a `repr(Rust)` enum lays out: the discriminant rounded
/// up to the payload's alignment, then the payload. `offset_of!` on this is how the payload's offset
/// is MEASURED rather than asserted, since `offset_of!` cannot name an enum variant's field.
#[allow(dead_code)]
struct MirrorTagAndPayload {
