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
