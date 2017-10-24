// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The consolidated object map: that its two levels behave, that its outer order is stable, and
//! that an unknown kind spelling is refused rather than aborting.
//!
//! THE MIRROR THIS MODULE WAS WRITTEN AROUND IS GONE. The control-state blocks live in
//! `ShardState::objects`, keyed kind then object key. A compatibility copy of the old top-level
//! key was written beside it, because every image at the then-current stamp carried that key and
//! dropping it would have lost those rows with every door shut. Its stated expiry was a change
//! that moves `SHARD_INDEX_FORMAT_VERSION` -- which makes those images refuse as stale and fall
//! back to replay -- and that change removed it.
//!
//! So the mirror, its decode fold, its agreement predicate, its disagreement counter, its
//! test-only injector and the tripwire that held it to an expiry condition are all gone, and what
//! is left here is the consolidated map on its own terms.
//!
//! rust-internal: measures and guards this crate's own resident structures, no product behaviour

use crate::engine::object_record::{ObjectRecord, ObjectSlot, OwnedElementTenancy};
use crate::engine::state::ShardState;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// A slot pointing at a page, built the way production builds one -- through `set_address` rather
/// than by hand, so a fixture cannot be the one place the narrow door is bypassed.
fn slot_at(slab: u64, offset: u64, length: u64) -> ObjectSlot {
    // ORDINAL PRESENT AND ZERO, which is what a single-page object carries by construction --
    // and what the record's inline arm can hold. The inline arm keeps the ordinal's PRESENCE
    // and not its value, so a fixture passing a nonzero ordinal would not round-trip and the
    // test would be measuring that rather than the pair.
    let entry = crate::block_store::ElementEntry::from_parts(
        slab,
        offset,
        length,
        Some(0),
        Some(11),
    );
    let mut record = ObjectRecord::default();
    record.set_address(&entry);
    ObjectSlot::from_record(record)
}

// =================================================================================================
// THE MUTATOR PAIR
// =================================================================================================

/// THE CONTROL-STATE MUTATORS KEEP THE CONSOLIDATED MAP RIGHT THROUGH A REMOVAL SEQUENCE.
///
/// # WHAT THIS TEST USED TO BE, AND WHAT LEFT WITH THE MIRROR
///
/// It drove this sequence and asserted after EVERY step that the consolidated map and a
/// compatibility mirror agreed, with a negative control that forced them apart to prove the
/// agreement check could fail. The mirror is gone -- removed by the change that moved the format
/// stamp, which is what its own expiry condition asked for -- so the per-step agreement check and
/// its negative control are gone with it. There is no second copy to drift.
///
/// THE SEQUENCE IS KEPT, and that is deliberate rather than inertia. Its value was never only the
/// mirror: it drives REPLACE, REMOVE, REMOVE-ABSENT, IN-PLACE MUTATION, MUTATE-ABSENT and
/// WHOLE-MAP REPLACEMENT against the one map, and each of those still has its own assertion about
/// what the authoritative map holds afterwards. The remove side in particular is the half that
/// gets forgotten, which was true of a mirrored pair and is just as true of a two-level map where
/// a removal has to reach into the right kind's inner table.
#[test]
fn the_control_state_mutators_keep_the_consolidated_map_right_through_removals() {
    let mut shard = ShardState::default();
    let a = slot_at(1, 0, 32);
    let b = slot_at(1, 64, 48);
    let c = slot_at(2, 128, 16);

    shard.insert_control_state_block("k1".into(), a.clone());

    shard.insert_control_state_block("k2".into(), b.clone());

    // REPLACING an existing key, which is a write to a key both copies already hold.
    shard.insert_control_state_block("k1".into(), c.clone());
    assert_eq!(
        shard.control_state_blocks().get("k1"),
        Some(&c),
        "the replacement did not take in the authoritative map"
    );

    // THE REMOVE SIDE.
    let removed = shard.remove_control_state_block("k2");
    assert_eq!(removed, Some(b), "remove must hand back what it removed");
    assert!(
        !shard.control_state_blocks().contains_key("k2"),
        "k2 survived its own removal in the authoritative map"
    );

    // REMOVING A KEY THAT WAS NEVER THERE must not invent one in either copy.
    assert_eq!(
        shard.remove_control_state_block("never-existed"),
        None,
        "removing an absent key must answer None"
    );

    // IN-PLACE MUTATION through the closure, which is the member that would otherwise hand out a
    // `&mut` to one copy alone.
    let other = crate::block_store::ElementEntry::from_parts(9, 512, 64, Some(0), Some(11));
    let touched = shard.with_control_state_block_mut("k1", |slot| {
        slot.set_address(&other);
    });
    assert!(touched.is_some(), "the in-place mutation did not find k1");
    assert_eq!(
        shard.control_state_blocks().get("k1").map(|slot| slot.address()),
        Some(other),
        "the in-place mutation did not reach the authoritative map"
    );

    // WHAT THE INLINE ARM DOES WITH A NONZERO ORDINAL, PINNED HERE BECAUSE THE TWO PATHS
    // DISAGREE. The record's inline arm holds the ordinal's PRESENCE and not its value -- correct,
    // and measured: a single-page object has page zero by construction, and 400 of 400 live
    // addresses carried ordinal 0. The CONVERSION path refuses a nonzero one by name
    // (`NonZeroBlockOrdinalDoesNotFitInline`). `set_address` does not refuse it; it writes the
    // presence bit and drops the value.
    //
    // That asymmetry is correct today only because the maps holding records are single-page. It
    // is asserted rather than trusted, so that the day a multi-page object reaches this door the
    // behaviour is a stated fact with a test on it rather than a surprise -- and the owned element
    // array is where such an object belongs.
    let nonzero_ordinal =
        crate::block_store::ElementEntry::from_parts(1, 0, 16, Some(7), Some(11));
    let mut probe = ObjectRecord::default();
    probe.set_address(&nonzero_ordinal);
    assert_eq!(
        probe.address().block_id(),
        Some(0),
        "the inline arm now round-trips a nonzero page ordinal. If that is deliberate, this          assertion and the note above are stale; if it is not, a page ordinal is being silently          zeroed on the compactor write-back path"
    );

    // Mutating a key that is not there must change nothing and say so.
    assert!(
        shard.with_control_state_block_mut("absent", |_| ()).is_none(),
        "mutating an absent key must answer None"
    );

    // THE WHOLE-MAP REPLACEMENT, which is where a field assignment would have left the mirror
    // holding every key the replacement dropped.
    let mut replacement = std::collections::HashMap::new();
    replacement.insert(std::sync::Arc::<str>::from("fresh"), a.clone());
    shard.set_all_control_state_blocks(replacement);
    assert_eq!(
        shard.control_state_blocks().len(),
        1,
        "the whole-map replacement left keys the replacement did not contain"
    );
    assert!(
        !shard.control_state_blocks().contains_key("k1"),
        "k1 survived a whole-map replacement that did not include it -- a ghost entry"
    );

}

// =================================================================================================
// THE OUTER MAP'S ORDER
// =================================================================================================

/// THE OUTER MAP ITERATES IN ONE ORDER WHATEVER ORDER ITS KINDS ARRIVED IN.
///
/// # WHY THIS IS NOT A TEST ABOUT A CONTAINER CHOICE
///
/// The dump manifest feeds its digest from an iteration, key by key, and recomputes and compares
/// it on restore. So the digest depends on iteration order -- and the only existing test over that
/// key list uses `.any()`, which is order-INSENSITIVE. An ordering regression there is therefore
/// silent today, and it would invalidate every manifest written before it.
///
/// This is the first thing in the tree asserting that order is stable at all, which is why it is
/// its own named test rather than an assertion buried inside a migration test.
///
/// WHAT IT STOPS, CONCRETELY. Swapping the outer `BTreeMap` for a `HashMap` on the entirely
/// reasonable grounds that there are only seventeen kinds, so the container cannot matter for
/// lookup cost. It does not matter for cost. It decides the digest.
#[test]
fn the_consolidated_outer_map_iterates_in_one_order_whatever_the_insertion_order() {
    let slot = slot_at(3, 96, 24);

    // The same four kinds, inserted in two deliberately different orders.
    let forward = [
        StoredModelKind::String,
        StoredModelKind::Feature,
        StoredModelKind::ControlState,
        StoredModelKind::ContextEvent,
    ];
    let mut backward = forward;
    backward.reverse();

    let build = |kinds: &[StoredModelKind]| {
        let mut shard = ShardState::default();
        for (n, kind) in kinds.iter().enumerate() {
            shard
                .objects
                .entry(*kind)
                .or_default()
                .insert(std::sync::Arc::from(format!("k{n}").as_str()), slot.clone());
        }
        shard.objects.keys().copied().collect::<Vec<_>>()
