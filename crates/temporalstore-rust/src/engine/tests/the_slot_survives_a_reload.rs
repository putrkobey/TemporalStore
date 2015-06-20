// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SLOT NAMES ONE OBJECT ACROSS A RELOAD, which is what makes it storable at all.
//!
//! #2057 made a slot stable against the three mutations that invalidated a sorted run's position:
//! `insert` takes the first free slot, `remove` leaves a placeholder, and `valid` is tracked apart
//! from the array length. Those rules held IN MEMORY, and that was the whole of it. Its own test
//! said so in as many words -- "a load re-files what is written through `insert`, so slots are
//! handed out afresh on every load and a slot is a RESIDENT fact" -- and named this step as the one
//! that would make the slot durable and take the next format stamp.
//!
//! # THE TWO THINGS THAT RE-HANDED A SLOT, AND BOTH ARE CLOSED HERE
//!
//!   * THE WIRE. `Serialize` wrote `sorted_ids()`: ascending, holes omitted. A load collected that
//!     through `insert`, which reproduces the written ORDER and not the written POSITIONS. It
//!     writes the slot array now, in slot order, with `null` for a placeholder, and reads it back
//!     by position.
//!   * THE REBUILD. `update_bucket_layout` assigned `bucket.object_index = live_object_ids.into()`
//!     from a `BTreeSet`, so every call re-handed every slot in ascending id order -- and it has
//!     eleven production callers, so a write touching any page in a bucket renumbered every object
//!     in it. It reconciles now, and what it reconciles is counted rather than overwritten.
//!
//! # WHY EACH TEST HERE CARRIES A CONTROL THAT WOULD FAIL UNDER THE OLD BEHAVIOUR
//!
//! Every assertion below is satisfied by an ascending fixture under the OLD code too, so a fixture
//! whose filing order happens to be ascending would pass for the wrong reason. Each test therefore
//! asserts its fixture is NOT in ascending order before asserting anything about it, and the round
//! trip tests additionally drive what the old form would have loaded to, so the difference is
//! measured rather than described.
//!
//! rust-internal: drives this crate's own serde impls and one maintenance path, no external surface

#![allow(clippy::all)]
use super::*;
use crate::engine::state::ObjectIndex;

/// Filed in an order whose slot order is NOT ascending, which is the only order that can tell a
/// positional round trip from an order-preserving one.
const FILED: [u64; 6] = [900, 5, 700, 1, 800, 0];

fn filed_index() -> ObjectIndex {
    let mut index = ObjectIndex::default();
    for id in FILED {
        assert!(index.insert(id), "the fixture filed {id} twice");
    }
    index
}

fn round_trip(index: &ObjectIndex) -> ObjectIndex {
    let written = serde_json::to_string(index).expect("an object list serializes");
    serde_json::from_str(&written).expect("an object list loads")
}

/// THE WHOLE POINT: every slot names the same object after a round trip through the stored form.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn a_slot_names_the_same_object_after_a_round_trip_of_the_stored_index() {
    let index = filed_index();
    assert!(
        matches!(index, ObjectIndex::Many(_)),
        "the fixture must reach the slot-array arm or it is not exercising slots at all"
    );

    let before: Vec<(u64, usize)> = FILED
        .iter()
        .map(|id| (*id, index.slot_of(id).expect("a filed id has a slot")))
        .collect();
    let slot_order: Vec<u64> = index.iter().copied().collect();
    let mut ascending = FILED.to_vec();
    ascending.sort_unstable();

    println!("\n=== what the stored index now carries ===");
    println!("  filed in   : {FILED:?}");
    println!("  slot order : {slot_order:?}");
    println!("  written    : {}", serde_json::to_string(&index).expect("serializes"));

    // THE CONTROL THAT MAKES THE REST MEAN SOMETHING. If the fixture's slot order were already
    // ascending, the old order-preserving load would reproduce these positions too.
    assert_ne!(
        slot_order, ascending,
        "the fixture's slot order is already ascending, so a load that re-handed slots in ascending \
         order would reproduce it and this test would pass under the behaviour it exists to forbid"
    );

    let loaded = round_trip(&index);
    for (id, slot) in &before {
        assert_eq!(
            Some(*slot),
            loaded.slot_of(id),
            "object {id} was in slot {slot} and came back in {:?}; a slot that moves across a round \
             trip cannot be stored on a page entry, which is the only reason this change exists",
            loaded.slot_of(id)
        );
        assert_eq!(
            Some(*id),
            loaded.id_at(*slot),
            "slot {slot} named object {id} and names {:?} after a round trip",
            loaded.id_at(*slot)
        );
    }
    assert_eq!(
        index.object_count(),
        loaded.object_count(),
        "the round trip changed the object count"
    );
    assert_eq!(
        index.slot_count(),
        loaded.slot_count(),
        "the round trip changed the LENGTH of the slot array, so a slot number is bounded \
         differently after a load than before one"
    );

    // AND THE OLD FORM WOULD NOT HAVE DONE THIS, driven rather than asserted about. An ascending
    // sequence of bare ids is exactly what `Serialize` used to write for this fixture.
    let old_form = serde_json::to_string(&ascending).expect("the old shape serializes");
    let from_old_form: ObjectIndex =
        serde_json::from_str(&old_form).expect("the old shape still loads");
    assert_ne!(
        from_old_form.slot_of(&FILED[0]),
        index.slot_of(&FILED[0]),
        "loading the OLD written form reproduces this fixture's slots, so the two forms are not \
         distinguishable and the stamp this change takes would be paying for nothing"
    );
}

/// A PLACEHOLDER IS PART OF THE STORED SHAPE, because closing a hole is what renumbers.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn a_placeholder_survives_the_stored_form_so_the_slots_above_it_do_not_move() {
    let mut index = filed_index();
    let evicted = FILED[1];
    let hole = index.slot_of(&evicted).expect("the id to remove has a slot");
    let above: Vec<(u64, usize)> = FILED
        .iter()
        .filter(|id| **id != evicted)
        .map(|id| (*id, index.slot_of(id).expect("a filed id has a slot")))
        .collect();
    assert!(
        above.iter().any(|(_, slot)| *slot > hole),
        "nothing is filed above the hole, so this fixture cannot show that a hole keeps the slots \
         above it where they are"
    );
    assert!(index.remove(&evicted), "the id to remove was not held");
    assert_eq!(None, index.id_at(hole), "the removal did not leave a placeholder");

    let written = serde_json::to_string(&index).expect("serializes");
    println!("\n=== a stored index carrying a hole ===");
    println!("  removed {evicted} from slot {hole}; written {written}");
    assert!(
        written.contains("null"),
        "the written form is {written} and carries no null, so the placeholder is not stored and \
         every slot above it closes up on the next load"
    );

    let loaded = round_trip(&index);
    assert_eq!(
        None,
        loaded.id_at(hole),
        "slot {hole} came back holding {:?} rather than staying a placeholder",
        loaded.id_at(hole)
    );
    for (id, slot) in &above {
        assert_eq!(
            Some(*slot),
            loaded.slot_of(id),
            "object {id} sat in slot {slot} above the hole and came back in {:?}",
            loaded.slot_of(id)
        );
    }

    // THE NEGATIVE CONTROL: the same ids written WITHOUT the hole -- which is what the old form
    // did -- loads one slot shorter, and that shortening is the renumber.
    let without_the_hole: Vec<u64> = above.iter().map(|(id, _)| *id).collect();
    let closed: ObjectIndex = serde_json::from_str(
        &serde_json::to_string(&without_the_hole).expect("serializes"),
    )
    .expect("loads");
    assert!(
        closed.slot_count() < loaded.slot_count(),
        "writing the same ids without the placeholder gives a {} slot array against {} with it, so \
         storing the hole changes nothing and this test is not measuring the renumber",
        closed.slot_count(),
        loaded.slot_count()
    );
}

/// AN INDEX WRITTEN BEFORE THIS CHANGE LOADS WHERE THE OLD LOADER WOULD HAVE PUT IT.
///
/// DEFENCE IN DEPTH, AND NOT THE UPGRADE PATH, which is worth being exact about. On upgrade the
/// stored stamp is below the constant and `persistence.rs` compares with `<`, so an older index is
/// REFUSED and rebuilt from the log; it is never decoded. What this test pins is that IF it is
/// decoded -- the one shape where a decode runs before the stamp check -- it lands where the old
/// loader would have put it rather than somewhere new. Asserted against an index built the old way
/// rather than against a literal.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn an_index_written_before_this_change_loads_to_the_slots_the_old_loader_would_have_given() {
    let mut ascending = FILED.to_vec();
    ascending.sort_unstable();
    let old_form = serde_json::to_string(&ascending).expect("the old written shape serializes");
    println!("\n=== an index written before this change ===");
    println!("  old form: {old_form}");

    let loaded: ObjectIndex = serde_json::from_str(&old_form).expect("an old index still loads");

    // What the OLD loader did: collect the written sequence through `insert`.
    let mut as_the_old_loader_would: ObjectIndex = ObjectIndex::default();
    for id in &ascending {
        as_the_old_loader_would.insert(*id);
    }

    for id in &ascending {
        assert_eq!(
            as_the_old_loader_would.slot_of(id),
            loaded.slot_of(id),
            "object {id} loads into slot {:?} and the old loader would have put it in {:?}; an \
             upgrade that moves a slot on a store already on disk is the one migration this change \
             must not need",
            loaded.slot_of(id),
            as_the_old_loader_would.slot_of(id)
        );
    }
    assert_eq!(
        as_the_old_loader_would.slot_count(),
        loaded.slot_count(),
        "the old form loads to a different array length than the old loader produced"
    );
    // AND THE SENSITIVITY CONTROL: the agreement above is a property of ASCENDING input, not of
    // any input.
    //
    // CORRECTED FROM A CONTROL THAT COULD NOT FAIL. This re-filed the positionally-loaded array
    // through `insert` IN SLOT ORDER and asserted the positions differed -- but inserting ids in
    // slot order reproduces slot order, so the two agreed for every fixture and the control was
    // vacuous. It failed on the first run, which is the only reason it is right now.
    //
    // What discriminates is the order the OLD WRITER wrote: ascending. A non-ascending array stored
    // positionally must load to DIFFERENT slots than the same ids written ascending, or the two
    // written forms are indistinguishable and the stamp pays for nothing.
    let filed = filed_index();
    let positional = round_trip(&filed);
    let as_the_old_writer_wrote: ObjectIndex = serde_json::from_str(
        &serde_json::to_string(&filed.sorted_ids()).expect("the old written shape serializes"),
