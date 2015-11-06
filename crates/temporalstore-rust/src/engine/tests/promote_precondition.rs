// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE MODEL-MAP PROMOTION CHECK DECIDES, AND WHAT IT COSTS TO DECIDE IT.
//!
//! `promote_model_maps_to_bucket_index_authority` is the precondition in front of
//! `rebuild_bucket_block_ownership`: it asks whether the bucket index already names every live
//! model-map page, and rebuilds ownership only if it does not. #1822 is what happens when that
//! rebuild runs without one in front of it -- a restart served 0 of 6 hash fields against 6 of 6
//! strings -- and the invariant was written down only in a comment at the OTHER call site.
//!
//! #1888 then measured the check. It materialised a vector of every live block entry in the shard
//! to run one `any()` over it: 4.0 allocations per record the shard holds, 22% of a restore's
//! index fold, 10% of the whole restore, and on both configurations it measured the answer was
//! always "nothing is missing". The vector is now gone. The walk is not -- the walk IS the check.
//!
//! SO TWO SEPARATE THINGS HAVE TO BE TRUE, asserted separately and in this order so a mutant that
//! kills the first does not stop the second from being reached:
//!
//!   1. THE DECISION IS UNCHANGED. Five arms, each with a control:
//!      * no live model-map page at all -> `false`, establishing nothing, asserted on a shard
//!        with a NON-EMPTY index and again on a wholly empty one, where two arms both apply;
//!      * an index that names everything -> `false`, and the index is not moved;
//!      * an EMPTY index under live model pages -> `true` -- the other side of an `||` whose two
//!        halves each answer every case the other covers wrongly;
//!      * an index missing ONE page, in a shard whose other buckets -- the orphan's own included
//!        -- are present and populated, so it is the `any()` deciding and not the empty-map arm
//!        -> `true`, and every page is named afterwards;
//!      * a page routing to a RELEASED bucket -> not missing, against a control that shows the
//!        same fixture answering `true` without the release record.
//!   2. THE CHECK NO LONGER COSTS THE STORE, in allocations, over a denominator of the pages the
//!      walk actually offered it.
//!
//! IS THE POSITIVE ARM REACHABLE AT ALL? It has to be, or this is a precondition nobody can test
//! and "none is ever missing" is a property of the code rather than of two measured corpora.
//! `a_block_only_the_model_maps_know_about_is_found_and_filed` constructs it, out of the same
//! `upsert_bucket_index_block` a live write files through. `a_restore_says_which_arm_it_took`
//! then reports which arm a real restart takes over a store-sized denominator -- the negative
//! one, which is precisely why the vector was pure cost there.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::ShardState;
use crate::engine::storage_bucket_internals::{
    promote_model_map_check_counts, promote_model_maps_to_bucket_index_authority,
    reset_promote_model_map_check_counts, upsert_bucket_index_block,
};

const SHARD: ShardId = 1;
const SPREAD: u32 = 8;

/// THE RANGE THE FIXTURE RUNS ON, AND WHY IT IS NARROW.
///
/// A page's bucket is `block_routing_bucket(object_key, start, end)` now, so nothing about this
/// fixture chooses it -- it is decided by the key and the range. Over the WHOLE keyspace 64 keys land
/// in 64 distinct buckets, and the positive arm below needs the orphan's bucket to hold OTHER pages:
/// its whole point is that an implementation noticing only a wholly absent BUCKET would pass. Eight
/// buckets is what makes that collision certain rather than a one-in-67-million accident.
///
/// Every call into the engine here passes the same range, and `bucket_of` computes over it, so the
/// fixture and the code under test partition the shard the same way.
const END: u32 = SPREAD - 1;

/// A page address shaped like one the block store handed back.
///
/// # THE PAGE ORDINAL IS ZERO, AND ZERO IS THE ONLY VALUE THIS FIXTURE MAY FILE
///
/// Every page here is filed into the string map through `set_string_address`, whose row is an
/// `ObjectSlot`: the inline arm keeps the ordinal's PRESENCE and has no room for its VALUE, so a
/// row filed with ordinal `n != 0` hands back 0. `same_block_address` compares the ordinal and
/// `block_ref_key_from_parts` puts it in the index key, so an address filed through the ROW and the
/// same `ElementEntry` filed into the BUCKET INDEX were a different page to every reader -- which
/// is what made four of the five arms below answer the opposite of what they assert, in four
/// different places, none of which names an address.
///
/// The ordinal names WHICH page of an object an address is, and every page here belongs to a
/// single-page object, so zero is not a convenience: it is the only value the store can keep.
/// `ObjectRecord::from_object_rows` refuses a nonzero one by name as
/// `NonZeroBlockOrdinalDoesNotFitInline`, `set_address` coerces it and counts the coercion
/// (`the_write_back_door_and_the_block_ordinal`), and #2322 corrected the identical datum in
/// `model_kind_registry_guards`. So this changes what the fixture says, not what it tests.
///
/// IT IS NOT LEFT TO A LITERAL TO STAY RIGHT. `shard_with_unfiled` reads every row back as it
/// files it, and `the_model_maps_hand_back_the_addresses_the_index_was_filed_with` compares the two
/// columns directly, so a fixture that files an unkeepable ordinal again fails at the filing rather
/// than four assertions away.
///
/// The fifth argument is INERT -- `ElementEntry::try_from_parts` consumes only its presence, and
/// `generation()` is derived from the ordinal -- so the pages here are told apart by their OFFSET,
/// which is distinct per index.
fn address_at(index: u64) -> ElementEntry {
    ElementEntry::from_parts(7, index * 128, 128, Some(0), Some(9_000 + index))
}

fn key_of(index: u64) -> String {
    format!("k-{index:06}")
}

/// WHERE A PAGE OF THIS FIXTURE IS ACTUALLY FILED, and not a number this file picks.
///
/// The fixture used to choose a bucket (`index % SPREAD`) and stamp it onto the address, because the
/// filing site read the bucket off the address. It derives the bucket from the KEY now, so a fixture
/// that chose one would be filing pages into buckets the engine does not put them in -- and the
/// vacuity checks below, which look the orphan's own bucket up in the map, would look in the wrong
/// place. This is the engine's own expression.
fn bucket_of(index: u64) -> u32 {
    crate::engine::hashing::block_routing_bucket(&key_of(index), 0, END)
}

/// A shard holding `count` string pages in the model map, and in the bucket index too -- except
/// for pages routing to `unfiled_bucket`, which are left in the map alone.
///
/// Filed with `upsert_bucket_index_block`, the function a live write files through, so the index
/// half of the fixture is built the way production builds it rather than assembled by hand.
fn shard_with(count: u64, unfiled_group: Option<u64>) -> ShardState {
    shard_with_unfiled(count, unfiled_group).0
}

/// The same, returning the buckets whose pages were left OUT of the index.
///
/// `unfiled_group` selects by `index % SPREAD` rather than by bucket id, because the bucket a page
/// lands in is now `block_routing_bucket(key, ..)` and nothing chooses it. The buckets those pages
/// route to are returned, so the release test can record exactly them.
fn shard_with_unfiled(count: u64, unfiled_group: Option<u64>) -> (ShardState, BTreeSet<u32>) {
    let mut shard = ShardState::default();
    // `upsert_bucket_index_block` reads the shard's own range to decide where to file. An unstamped
    // state answers the whole keyspace, which would put every key in a bucket of its own.
    shard.set_routing_range(0, END);
    let mut unfiled_buckets = BTreeSet::new();
    let mut index = 0u64;
    while index < count {
        let address = address_at(index);
        shard.set_string_address(std::sync::Arc::from(key_of(index).as_str()), &address.clone());
        // READ THE ROW BACK, following #2322. A string row narrows a page ordinal, so a fixture
        // that files an address the row cannot keep presents one address to the model-map walk and
        // a different one to the bucket index below -- and every arm in this file compares the two.
        // Asserting it here fails at the filing instead of four assertions away.
        assert_eq!(
            shard.string_address(&key_of(index)).as_ref(),
            Some(&address),
            "the string map did not hand back the address this fixture filed for `{}`: the row \
             narrowed it, so the walk and the index are being compared on different pages",
            key_of(index),
        );
        if Some(index % u64::from(SPREAD)) == unfiled_group {
            unfiled_buckets.insert(bucket_of(index));
        } else {
            upsert_bucket_index_block(
                &mut shard,
                SHARD,
                "string",
                &key_of(index),
                None,
                address,
                false,
            );
        }
        index += 1;
    }
    (shard, unfiled_buckets)
}

/// WHAT THE MODEL MAPS HOLD versus WHAT A READER OF THEM SEES, SIDE BY SIDE.
///
/// Every arm below turns on ONE question -- is the address the model-map walk hands out the same
/// address the bucket index was filed with -- and when the answer is no they fail in four separate
/// places, none of which names an address. `an_empty_bucket_index_under_live_model_blocks_is_rebuilt_whole`
/// read `left: 0, right: 32`: thirty-two rows in the maps and not one of them named by an index
/// rebuilt from those same rows. That reads like a broken rebuild and was a one-field narrowing.
///
/// So this asks the question directly and prints both columns. A string row is an `ObjectSlot`
/// whose inline arm keeps a page ordinal's PRESENCE and drops its VALUE; `same_block_address`
/// compares the ordinal and `block_ref_key_from_parts` puts it in the index key, so a row filed
/// with a nonzero ordinal is a DIFFERENT page to every reader than the index entry filed from the
/// same `ElementEntry`. The nonzero case is measured beside the fixture's own, rather than argued.
///
/// rust-internal: guards this crate's own fixture against the row width, no product behaviour
#[test]
fn the_model_maps_hand_back_the_addresses_the_index_was_filed_with() {
    const BLOCKS: u64 = 32;
    let shard = shard_with(BLOCKS, None);

    let mut held = 0u64;
    let mut round_tripped = 0u64;
    let mut named_by_index = 0u64;
    let mut index = 0u64;
    while index < BLOCKS {
        let filed = address_at(index);
        if let Some(read_back) = shard.string_address(&key_of(index)) {
            held += 1;
            if read_back == filed {
                round_tripped += 1;
            }
            if shard.bucket_index.contains_object_block_address(
                "string",
                &key_of(index),
                None,
                &read_back,
            ) {
                named_by_index += 1;
            }
        }
        index += 1;
    }

    // THE NARROWING ITSELF, on its own shard, so the two columns are comparable rather than
    // described. The same `ElementEntry` goes into the row and into the index; what comes back out
    // of the row is then looked up in that index.
    let mut narrowing = ShardState::default();
    narrowing.set_routing_range(0, END);
    let unkeepable = ElementEntry::from_parts(7, 0, 128, Some(11), Some(22));
    narrowing.set_string_address(std::sync::Arc::from("probe"), &unkeepable.clone());
    upsert_bucket_index_block(
        &mut narrowing,
        SHARD,
        "string",
        "probe",
        None,
        unkeepable.clone(),
        false,
    );
    let narrowed = narrowing.string_address("probe").expect("the row was filed");
    let index_names_what_was_filed = narrowing
        .bucket_index
        .contains_object_block_address("string", "probe", None, &unkeepable);
    let index_names_what_the_row_returns = narrowing
        .bucket_index
        .contains_object_block_address("string", "probe", None, &narrowed);

    println!(
        "\n  WHAT THE MAPS HOLD vs WHAT A READER OF THEM SEES, at {BLOCKS} string pages\n\
         \n    model-map rows                                      {held}\n\
           .. handing back the address the fixture filed        {round_tripped}\n\
           .. named by the bucket index at THAT address         {named_by_index}\n\
         \n    ONE ROW, ONE INDEX ENTRY, ONE `ElementEntry`: page ordinal filed -> read back\n\
           {:?} -> {:?}   the fixture's own value\n\
           {:?} -> {:?}   nonzero: the row has room for the PRESENCE, not the VALUE\n\
           index names the address that was FILED         {index_names_what_was_filed}\n\
           index names the address the ROW HANDS BACK     {index_names_what_the_row_returns}\n",
        address_at(0).block_id(),
        shard.string_address(&key_of(0)).and_then(|at| at.block_id()),
        unkeepable.block_id(),
        narrowed.block_id(),
    );

    assert_eq!(held, BLOCKS, "VACUITY: the fixture filed no string row, so nothing here is compared");
    assert_eq!(
        round_tripped, BLOCKS,
        "{} of {BLOCKS} model-map rows hand back an address other than the one the fixture filed, \
         so the walk and the index in every arm below are being compared on different pages",
        BLOCKS - round_tripped,
    );
    assert_eq!(
        named_by_index, BLOCKS,
        "the bucket index names {named_by_index} of {BLOCKS} pages at the address the model map \
         hands out, so the promotion check is offered missing pages that are not missing",
    );

    // THE TRIPWIRE, and the condition for deleting all of this. While the row narrows, the fixture
    // above must file zero. The day the inline arm round-trips a nonzero ordinal -- the owned array
    // lands and the write-back door routes to it -- this fails, and that is the prompt to say so
    // here rather than to leave a literal pinned for a reason that has expired.
    assert_ne!(
        narrowed, unkeepable,
        "the string row now round-trips a nonzero page ordinal. If that is deliberate, this \
         assertion and the zero pinned in `address_at` are what should be deleted, in that change"
    );
    assert!(
        index_names_what_was_filed && !index_names_what_the_row_returns,
        "the row and the index no longer disagree about an unkeepable ordinal (filed: \
         {index_names_what_was_filed}, read back: {index_names_what_the_row_returns}), so the \
         reason the fixture files zero has changed and this file should say what it is now"
    );
}

/// ARM 1 OF 5: no live model-map page at all -> `false`, having established nothing.
///
/// Its own test because the per-execute caller in `engine.rs` reads exactly this answer to decide
/// whether to latch its fast-skip flag -- "promote returns false without establishing anything on
/// an empty shard" is the comment there -- so a check that answered `true` here would latch the
/// skip on a shard it had never actually checked.
