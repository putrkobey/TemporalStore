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
