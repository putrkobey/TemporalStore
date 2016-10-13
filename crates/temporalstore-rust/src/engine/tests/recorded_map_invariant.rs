// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// CAN A MODEL-MAP MUTATION HAPPEN WITHOUT A DURABLE RECORD?
//
// One suite, because there is now one type. `recorded_hash_container_invariant` and
// `recorded_set_container_invariant` are gone with the two containers they guarded, and what they
// each asserted is asserted here once, parameterised over the kinds.
//
// Two mechanisms, as before:
//
//   * THE COMPILER holds the OUTSIDE. `RecordedMap::entries` is private to its module, so no writer
//     elsewhere can obtain `&mut` to an inner map -- for ANY kind, which is one of the things
//     collapsing the two types bought. This tree carries no compile-fail harness (no `trybuild`, no
//     `.stderr` fixtures), so that negative is DOCUMENTED rather than executed.
//   * THIS MODULE holds the INSIDE: a new `pub(super)` accessor added inside that module is the one
//     way the invariant can be lost, and the compiler cannot object to it.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

use crate::engine::recorded_map::{
    drop_object, install_element, install_element_staged_only, remove_element, HashKind, ListKind,
    RecordedMap, RepacksAfterDecode, ReplaysInstallsUnrecorded, SetKind, ZSetKind,
};
use crate::engine::storage_bucket_internals::mark_bucket_index_object_deleted_filed;

const SOURCE: &str = include_str!("../recorded_map.rs");

/// The part of the module that exists in a SHIPPED binary.
///
/// The hatch matcher runs over this rather than the whole file, and that distinction was driven: a
/// `#[cfg(test)]` fixture returning `Option<&mut ..>` made the first run of the set container's
/// version of this test fail. A fixture cannot be an escape hatch in a binary it is not compiled
/// into -- but it IS a `&mut` accessor, so the split has to be made rather than the needle dropped.
fn shipped(source: &str) -> &str {
    match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

const PLANTED_HATCH: &str = "\
pub(super) fn entries_mut(&mut self) -> &mut HashMap<String, K::Elements> {
    &mut self.entries
}
";

const PLANTED_HATCH_WRAPPED: &str = "\
pub(super) fn entries_mut(
    &mut self,
) -> &mut HashMap<String, K::Elements> {
    &mut self.entries
}
";

const PLANTED_PUBLIC_FIELD: &str = "\
pub(super) struct RecordedMap<K: RecordedKind> {
    pub(super) entries: HashMap<String, K::Elements>,
}
";

/// THE MATCHER, anchored on the arrow so a `rustfmt` line break cannot hide a return type.
fn mutable_borrows_of_the_inner_map(source: &str) -> Vec<String> {
    let mut found = Vec::new();
    for needle in [
        "-> &mut HashMap",
        "-> &mut std::collections::HashMap",
        "-> &'a mut HashMap",
        "-> Option<&mut HashMap>",
        "-> &mut K::Elements",
        "-> Option<&mut K::Elements>",
    ] {
        if source.contains(needle) {
            found.push(needle.to_string());
        }
    }
    found
}

fn visible_declarations_of_the_field(source: &str) -> Vec<String> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub")
                && !line.contains("fn ")
                && line.contains("entries: HashMap<String, K::Elements>")
                && line.ends_with(",")
        })
        .map(str::to_string)
        .collect()
}

fn exposed_surface(source: &str) -> Vec<String> {
    shipped(source)
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub(super) fn ")
                || line.starts_with("pub(super) struct ")
                || line.starts_with("pub(super) trait ")
        })
        .map(|line| {
            let after = line
                .trim_start_matches("pub(super) ")
                .trim_start_matches("fn ")
                .trim_start_matches("struct ")
                .trim_start_matches("trait ");
            let end = after
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            after[..end].to_string()
        })
        .collect()
}

/// THE SURFACE IS THE INVARIANT, SO THE SURFACE IS PINNED BY NAME -- ONCE, FOR EVERY KIND.
#[test]
fn the_recorded_map_exposes_no_way_to_mutate_any_kind_without_a_record() {
    // CONTROLS on the matchers.
    assert!(
        !mutable_borrows_of_the_inner_map(PLANTED_HATCH).is_empty(),
        "the matcher cannot see a one-line hatch, so its silence on the real module means nothing"
    );
    assert!(
        !mutable_borrows_of_the_inner_map(PLANTED_HATCH_WRAPPED).is_empty(),
        "the matcher cannot see a wrapped hatch, so `rustfmt` alone would blind it"
    );
    assert!(
        !visible_declarations_of_the_field(PLANTED_PUBLIC_FIELD).is_empty(),
        "the matcher cannot see the field made visible, which needs no accessor at all"
    );
    assert!(
        SOURCE.contains("pub(super) fn install_element"),
        "the source read is not `recorded_map.rs`: it has no `install_element`"
    );
    assert!(
        SOURCE.len() > 10_000,
        "the source read is {} bytes, too small to be the module",
        SOURCE.len()
    );
    // CONTROL ON THE SPLIT: the test half must contain a `&mut` accessor, or `shipped()` trims
    // nothing and the subject assertion is over the whole file by accident.
    assert!(
        !mutable_borrows_of_the_inner_map(SOURCE).is_empty(),
        "the module has no `&mut` accessor anywhere, so the shipped/test split is not exercised"
    );

    // THE SUBJECT, over the shipped half only.
    assert_eq!(
        mutable_borrows_of_the_inner_map(shipped(SOURCE)),
        Vec::<String>::new(),
        "`recorded_map` hands out a mutable borrow of an inner model map. That is the escape hatch \
         this type exists to remove -- and because there is now ONE type, one hatch would open \
         every kind at once."
    );
    assert_eq!(
        visible_declarations_of_the_field(shipped(SOURCE)),
        Vec::<String>::new(),
        "`RecordedMap::entries` has been given a visibility. The field being private to its module \
         IS the invariant."
    );

    // THE MUTATORS ARE PRIVATE, which is the half the one-call surface rests on.
    for private in [
        "\n    fn install(&mut self, recorded: RecordedElement<K>)",
        "\n    fn remove_element(&mut self, recorded: RecordedElementRemoval<K>)",
        "\n    fn remove_object(&mut self, recorded: RecordedObjectRemoval<K>)",
    ] {
        assert!(
            SOURCE.contains(private),
            "a recorded mutator is no longer a PRIVATE method: {private:?}. The one-call surface \
             only narrows enforcement if the mutators behind it are unreachable from outside."
        );
    }

    let expected: Vec<&str> = vec![
        // The one type, and what distinguishes a kind.
        "RecordedKind",
        "ReplaysInstallsUnrecorded",
        "RepacksAfterDecode",
        "RecordedMap",
        // The proofs. Generic, each carrying the identity of what was recorded.
        "RecordedElement",
        "RecordedElementRemoval",
        "RecordedObjectRemoval",
        // THE OPERATIONS, written once for every kind.
        "install_element",
        "install_element_staged_only",
        "remove_element",
        "drop_object",
        // What a level-2 container must supply, declared beside the operation that needs it.
        "RemovableElementMap",
        // RE-KEYS AND CANNOT RECORD ANYTHING, which is why it is allowed to exist. It
        // replaces one entry's key handle with an EQUAL one -- the text is equal by
        // definition, or the lookup that found it would not have -- and leaves the value
        // untouched, so no read can answer differently and there is nothing for a record
        // to describe. What it changes is how many allocations hold that text.
        //
        // IT WAS MISSING FROM THIS LIST, AND THAT IS WHY THIS TEST WAS RED. The entry was
        // added to `recorded_map` without being declared here, so the guard has been
        // failing on a tree whose surface is exactly what it intends to pin -- and a red
        // that means nothing is worse than no guard, because the next real addition reads
        // as the same noise.
        "adopt_key",
        // Reads, all borrows.
        "get",
        // IN SOURCE ORDER, which is what the matcher returns -- so an entry goes where its
        // DECLARATION is, not where it reads best in this list.
        // THE SAME SHAPE ONE LEVEL DOWN, and the same answer. `adopt_element_names` points
        // each ELEMENT name in each entry at a pooled copy of the identical text. It is
        // bounded by `PoolableElementNames` rather than written for `hashes` by name, so a
        // kind keyed by BYTES cannot be handed a pool of strings by mistake. Like
        // `adopt_key` it replaces a handle with an equal one and touches no value, so no
        // read can answer differently and there is nothing for a record to describe.
        //
        // WHY IT EXISTS AT ALL: the wire shape of a hash is a map of OWNED names, which is
        // what keeps the index format from moving, so a decode mints one handle per field
        // with the pool taking no part. Without this a recovered shard holds a copy of the
        // one constant field name per object, and nothing fails, because the names are
        // equal either way.
        "adopt_element_names",
        "contains_key",
        "len",
        "is_empty",
        "keys",
        "values",
        "iter",
        "entries",
        // A READ, AND IT CANNOT MUTATE A MAP. `block_an_element_vacates` takes `&self` and returns a
        // CLONE of an address the map already holds -- the page an element is moving off, which the
        // filer needs because an index entry no longer names its element. There is no `&mut` on it
        // and nothing it returns can be written through, so it is on the same footing as the reads
        // above rather than on the exception list below.
        "block_an_element_vacates",
        // The five shared exceptions.
        "from_decoded",
        "reconcile_from_durable",
        "fold_carried_elements",
        "replay_remove_element",
        "element_values_mut",
        // The two PER-KIND exceptions, each gated by its marker trait.
        "replay_install_element",
        "repack_decoded",
        // The traits the two gated exceptions need of a level-2 container.
        "RepackableElementMap",
        "ElementValuesMut",
        "iter_mut",
        "IterableMutElementMap",
        // READ-ONLY BY DECLARATION, both of its methods. `AddressedElementMap` is what lets the
        // generic emitter ask a concrete level-two container which address it holds for one element
        // and whether any OTHER element sits on a given page. Both take `&self`; neither can insert,
        // remove or retain, so a kind declaring it gains no way to change what its map holds. It is
        // the fourth of these small traits and the only one that is purely a read.
        "AddressedElementMap",
        // NOTE on what this list does and does not contain: the matcher collects items carrying an
        // explicit `pub(super)`, so a TRAIT METHOD declaration -- which has no visibility modifier
        // of its own -- is not collected. `RemovableElementMap::remove_element`,
        // `RepackableElementMap::repack` and `IterableMutElementMap::iter_values_mut` are therefore
        // absent by construction, while `ElementValuesMut::iter_mut` is present because it is an
        // inherent `pub(super) fn`. That is the matcher being consistent rather than blind: a trait
        // method cannot be added without its trait appearing above, and the traits ARE pinned.
        // The kinds themselves -- ALL FOUR container kinds now, which is the whole point: adding
        // one is a `RecordedKind` impl plus markers, and it appears HERE rather than as a new type.
        "HashKind",
        "SetKind",
        "ZSetKind",
        "ListKind",
        // THE BOUND ON THE ADOPTION ABOVE, and a level-two container declaring it gains no
        // way to change what it holds: its one method re-points equal names. One
        // implementor today, which is the point of the trait rather than an argument
        // against it -- it is what keeps the decode pass from naming `hashes` directly.
        "PoolableElementNames",
    ];
    let actual = exposed_surface(SOURCE);
    assert_eq!(
        actual, expected,
        "the surface of `recorded_map` changed. If an entry was ADDED, say in this list what it is \
         for and whether it can mutate a map without a record.\n  expected: {expected:?}\n  \
         actual:   {actual:?}"
    );
}

/// THE EXCEPTION ASYMMETRY IS DATA, AND THE COMPILER READS IT.
///
/// # WHY THIS IS NOT A COMMENT
///
/// Five paths mutate without recording. THREE are shared by every kind. TWO are not, and that is
/// the single thing two concrete containers established that one could not have:
///
///   * hash has a recovery arm that installs an element and files NO outcome record -- the replayed
///     log item IS the record for `write_context_node` -- so it needs `replay_install_element`. The
///     set replay arm re-files its block, so it does not. (This said the node "is never registered
///     in the bucket index", which is false: the index holds an entry for the block. The asymmetry
///     is about the outcome record, not the index.)
///   * set's level-2 container is a B-tree that a decode leaves half empty, so it needs
///     `repack_decoded`. `HashFieldMap` is a sorted vector whose length is its capacity, so it has
///     nothing to pack.
///
/// Flattening those into one surface, or into a sentence saying "only hash does this", would have
/// discarded exactly that. They are MARKER TRAITS instead, so the asymmetry is two `impl` lines and
/// a kind that has not declared one CANNOT CALL the method -- which the two functions below assert
/// at compile time by existing at all.
#[test]
fn the_per_kind_exceptions_are_declared_per_kind_and_the_compiler_enforces_it() {
    // COMPILE-TIME: these two functions accept only a kind that declares the marker. They are the
    // positive half, and they are checked by the crate compiling at all.
    fn only_a_kind_that_replays_unrecorded<K: ReplaysInstallsUnrecorded>() {}
    fn only_a_kind_that_repacks<K: RepacksAfterDecode>() {}
    only_a_kind_that_replays_unrecorded::<HashKind>();
    only_a_kind_that_repacks::<SetKind>();
    only_a_kind_that_repacks::<ZSetKind>();
    only_a_kind_that_repacks::<ListKind>();

    // THE NEGATIVE HALF CANNOT BE WRITTEN IN THE LANGUAGE -- Rust has no negative bounds, so
    // "`SetKind` does NOT replay unrecorded" is not expressible as a type check. It is expressible
    // as DATA about the module, which is what this reads: exactly one declaration of each marker,
    // and the kind each names.
    let replays: Vec<&str> = SOURCE
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("impl ReplaysInstallsUnrecorded for "))
        .collect();
    let repacks: Vec<&str> = SOURCE
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("impl RepacksAfterDecode for "))
        .collect();
    println!("[asymmetry] replay-install declared by: {replays:?}");
    println!("[asymmetry] decode-repack declared by:  {repacks:?}");

    assert_eq!(
        replays,
        vec!["impl ReplaysInstallsUnrecorded for HashKind {}"],
        "the set of kinds with an UNRECORDED replay install changed. A new one means a recovery arm \
         that installs an element and files nothing -- the shape hash's census found in \
         `write_context_node` -- and it has to arrive with the reason written down, not as an \
         `impl` line."
    );
    assert_eq!(
        repacks,
        vec![
            "impl RepacksAfterDecode for SetKind {}",
            "impl RepacksAfterDecode for ZSetKind {}",
            "impl RepacksAfterDecode for ListKind {}",
        ],
        "the set of kinds that repack after a decode changed. THREE of the four declare it, and the \
         one that does not is the point: `HashFieldMap` is a sorted vector whose length is its \
         capacity, so it has nothing to pack, while `sets`, `zsets` and `lists` are all B-trees a \
         decode fills by ascending insertion. Adding a B-tree kind here is routine; adding \
         `HashKind` would repack a vector on every load for no benefit, and removing one would \
         leave that kind's trees half empty after every decode."
    );

    // AND THE TWO SETS ARE DISJOINT, which is the asymmetry itself rather than two counts that
    // happen to be one.
    // THE OTHER THREE KINDS MUST NOT CLAIM THE UNRECORDED REPLAY INSTALL. Each of their replay
    // arms re-files its block through the bucket index before installing -- read from the arms
    // rather than assumed -- so hash remains the only kind whose recovery installs and files
    // nothing. `SetKind` is named separately because `ZSetKind` contains it as a substring.
    assert!(
        !replays.iter().any(|line| line.contains("for SetKind")),
        "`SetKind` now declares the unrecorded replay install. Its replay arm re-files its block, \
         so if that changed the arm changed."
    );
    for kind in ["ZSetKind", "ListKind"] {
        assert!(
            !replays.iter().any(|line| line.contains(kind)),
            "`{kind}` now declares the unrecorded replay install, but its replay arm re-files its \
             block. Either the arm changed or the marker is wrong; both need saying out loud."
        );
    }
    assert!(
        !repacks.iter().any(|line| line.contains("HashKind")),
        "`HashKind` now declares the decode repack, which would repack a sorted vector on every \
         load for no benefit."
    );
}

const TEST_SHARD: ShardId = 13;

fn shard_for_records() -> ShardState {
    let mut shard = ShardState::default();
    shard.set_routing_range(0, crate::DEFAULT_END_ROUTING_BUCKET);
    shard.set_shard_id(TEST_SHARD);
    shard
}

fn an_address() -> crate::ElementEntry {
    crate::ElementEntry::from_parts(3, 128, 64, Some(11), Some(22))
}

fn drain_records() {
    let _ = crate::engine::block_in_wal::take_outcomes();
    let _ = crate::engine::block_in_wal::take_staged();
}

fn staged() -> usize {
    crate::engine::block_in_wal::staged_outcome_count()
}

/// ONE MUTATION, ONE RECORD, COUNTED -- FOR EVERY KIND THROUGH THE ONE OPERATION.
///
/// # THIS WAS DRIVEN TO FAIL, TWICE, AND THE TWO FAILURES ARE DIFFERENT
///
/// Deleting the emission from `install_element` does not compile: the proof requires a `BlockFiled`
/// witness that only the emitter can mint, so the error is `cannot find value `filed` in this
/// scope`. Keeping the witness and undoing the record makes this assertion report
/// `staged 0 records, not 1`. Both were run and the source restored byte-identically each time.
///
/// THE WITNESS EXISTS BECAUSE OF A MISTAKE THIS CHANGE'S LINEAGE ALREADY MADE. The first version of
/// the one-call surface built the proof from the CALLER'S OWN ARGUMENTS; deleting the emission
/// compiled, and only this count noticed, at run time, while the module's doc claimed the compiler
/// would. That is why the witness lives in the emitter's module behind a private field.
#[test]
fn every_kind_records_exactly_once_through_the_one_operation() {
    drain_records();
    let mut shard = shard_for_records();

    // HASH: element is a `String` field name, which IS its component.
    let before = staged();
    install_element::<HashKind>(
        &mut shard,
        TEST_SHARD,
        "rec-hash",
        Some("field-a".to_string()),
        std::sync::Arc::from("field-a"),
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one HASH element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.hashes.get("rec-hash").map(|e| e.len()), Some(1));

    // SET: element is the member BYTES, and its component is those bytes in hex -- a different
    // rendering of one identity, which is why both are passed rather than one derived here.
    let member = b"member-a".to_vec();
    let before = staged();
    install_element::<SetKind>(
        &mut shard,
        TEST_SHARD,
        "rec-set",
        Some(hex::encode(&member)),
        member.clone(),
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one SET element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.sets.get("rec-set").map(|e| e.len()), Some(1));

    // ZSET: the element is the member bytes and the VALUE carries the score, which is why this
    // kind needed no composite key and no widened trait.
    let zmember = b"zmember".to_vec();
    let before = staged();
    install_element::<ZSetKind>(
        &mut shard,
        TEST_SHARD,
        "rec-zset",
        Some(format!("{:016x}{}", 7u64, hex::encode(&zmember))),
        zmember.clone(),
        (7u64, an_address()),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one ZSET element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.zsets.get("rec-zset").map(|e| e.len()), Some(1));

    // LIST: the element is an `i64` sequence, so this kind pays no name at all -- which is why its
    // row cost does not move with an element name the way a hash field's does.
    let before = staged();
    install_element::<ListKind>(
        &mut shard,
        TEST_SHARD,
        "rec-list",
        Some(format!("{:016x}", 0u64)),
        0i64,
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one LIST element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.lists.get("rec-list").map(|e| e.len()), Some(1));

    // THE STAGED-ONLY RECORD, which is a context node: never filed in the bucket index, so its
    // record is the staged outcome alone. One record, counted the same way.
    let before = staged();
    install_element_staged_only::<HashKind>(
        &mut shard,
        TEST_SHARD,
        "context_node",
        "rec-node",
        "meta",
        7,
        std::sync::Arc::from("meta"),
        an_address(),
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one context node staged {} records, not 1",
        staged() - before
    );

    // THE OBJECT DELETION, whose record is the bucket index's own deleted-object set rather than a
    // staged outcome -- so the staged count must NOT move, and ONE mark authorises BOTH kinds.
    let deleted_before: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|b| b.deleted_object_index.object_count())
        .sum();
    let staged_before = staged();
    let (marked, deletion) = mark_bucket_index_object_deleted_filed(&mut shard, "rec-hash");
    let deleted_after: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|b| b.deleted_object_index.object_count())
        .sum();
    assert_eq!(
        deleted_after - deleted_before,
        1,
        "deleting the object recorded {} deleted object ids, not 1",
        deleted_after - deleted_before
    );
    assert_eq!(
        staged(),
        staged_before,
        "the object deletion staged a WAL outcome; its record is the deleted-object set and a \
         staged outcome as well would have a load apply it twice"
    );
    // ONE record, TWO drops -- which is why the witness is `Clone` and why this is the one call
    // site in the engine where a token is threaded.
    let dropped_hash = drop_object::<HashKind>(&mut shard, "rec-hash", deletion.clone());
    let dropped_set = drop_object::<SetKind>(&mut shard, "rec-hash", deletion);
    assert!(
        marked | dropped_hash | dropped_set,
        "the recorded object deletion reported no change at all"
    );
    assert!(!shard.hashes.contains_key("rec-hash"));

    drain_records();
}

/// THE READS ARE STILL BORROWS, FOR EVERY KIND.
///
/// Pointer identity rather than an allocation count: the counting allocator is behind the
