// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a timestamped series has to be able to do, named so the helpers over it stop naming one
//! container.
//!
//! # WHY THIS EXISTS
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)`. Four helpers walked and
//! reconciled them, and all four spelled the series out concretely as
//! `BTreeMap<u64, ElementEntry>`. One of them, the apply path's kind dispatch, handed that single
//! type out for six different kinds.
//!
//! The consequence was recorded in the tree before this module existed, in
//! `the_bucket_index_holds_one_block_out_of_line_and_the_series_maps_hold_none`: the six "cannot be
//! adopted for one of them alone", so any change of representation was all six or none. That is
//! not a reviewable change. This trait is the seam that makes them independent -- one kind can move
//! to a different series representation while the other seven keep theirs, because the helpers now
//! require the OPERATIONS rather than the container.
//!
//! # WHY THE TRAIT IS THIS SMALL
//!
//! It is derived from what the four helpers' bodies actually touch, not from `BTreeMap`'s surface.
//! That distinction is the whole point rather than tidiness: a trait reproducing `BTreeMap`'s API
//! would make the next representation conform to the OLD CONTAINER instead of to the operations,
//! and a seam that only one shape can fit through is a rename. So there is no `entry`, no range
//! query, no `first_key_value`, no `retain` -- none of which any helper here calls.
//!
//! Five methods, each with the caller that needs it named:
//!
//!   * `len` -- the reports path, which prints a point count per key.
//!   * `get` -- the reconcile, which refreshes a persisted point from the block-derived view.
//!   * `addresses` -- the live-block walk, which asks "does any point of this key pass the
//!     filter" before paying for the per-point pass.
//!   * `points` -- the address collector, which needs each point once.
//!   * `points_mut` -- the reconcile again, which overwrites addresses in place.
//!
//! A sixth would need a caller to justify it.
//!
//! # WRITES ARE A SECOND, SEPARATE TRAIT, AND THAT IS NOT TIDINESS
//!
//! `TimestampedSeries` has no way to insert a point. The walk and the reconcile never add one --
//! the reconcile overwrites addresses at instants that are already there, and rebuilds a whole
//! absent series by moving it in -- so an insert on that trait would be reachable from code that
//! has no business adding points, and a bound is a statement about what the caller may do.
//!
//! The apply path does add points, so it takes `TimestampedSeriesWrite`. Keeping them apart means
//! the live-block walk cannot write through a series it was handed to read.

use crate::block_store::ElementEntry;
use std::borrow::Borrow as _;
use std::collections::BTreeMap;

/// One object's timestamped page addresses, whatever holds them.
pub(super) trait TimestampedSeries {
    /// How many points this series holds.
    fn len(&self) -> usize;

    /// Whether it holds none. Spelled out because clippy asks for it beside `len`, and because a
    /// representation may answer it without computing a length.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The address recorded at one instant, if any.
    fn get(&self, at: u64) -> Option<&ElementEntry>;

    /// Every address, in no promised order. For the "does ANY point pass" question, which must not
    /// pay for ordering it does not use.
    fn addresses(&self) -> impl Iterator<Item = &ElementEntry>;

    /// Every point, ascending by instant. The order IS promised here: callers collecting addresses
    /// for a stored image depend on it, and a representation that cannot promise it is not a
    /// timestamped series.
    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)>;

    /// The points in a window of instants, ascending.
    ///
    /// # THE TEST FOR WHETHER A METHOD BELONGS ON THIS TRAIT
    ///
    /// Not "do callers want it" -- they wanted this, and that is not the argument. The test is:
    /// **can a plausible alternative representation implement it without becoming the thing the
    /// trait exists to escape?** A range over a sorted array is two binary searches and a slice,
    /// which is cheaper than a tree's version rather than a concession to it. So it passes, and
    /// the trait stays honest.
    ///
    /// WHAT WOULD FAIL THE SAME TEST, so the line is visible rather than implied: a `range_mut`
    /// handing out a cursor that assumes stable node addresses, or anything implying O(log n)
    /// INSERTION in the middle. A sorted array cannot provide either without becoming a tree.
    ///
    /// AND WHY A SIXTH METHOD IS EXPECTED RATHER THAN A SURPRISE. This trait was sized to what the
    /// four shared HELPERS touch, which was a principled sample and still the wrong population:
    /// the helpers never read a window, and the first tenant to migrate immediately did. A seam is
    /// only as general as the dimension it varied -- and that applies to WHICH CALLERS WERE
    /// SAMPLED, not only to which type parameter was abstracted.
    /// DOUBLE-ENDED, because a reader of a time window wants its NEWEST point as often as its
    /// oldest: `load_context_summary_as_of` walks `.range(..).rev()` to decode one record instead of
    /// the window. Both representations can do it -- `BTreeMap::range` and a slice's `iter` are each
    /// double-ended, and `map` preserves that -- so a bare `impl Iterator` here was a bound narrower
    /// than either implementation, and it cost a caller its reverse walk the moment a map converted.
    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl DoubleEndedIterator<Item = (u64, &ElementEntry)>;

    /// Every point, ascending, for overwriting addresses in place. The instant is handed over by
    /// value because no caller mutates it -- an instant is this map's key, and a representation
    /// that let a walker change it could silently reorder itself.
    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)>;
}

/// THE REPRESENTATION EVERY SERIES STILL USES, and the only one for now.
///
/// This impl is what makes the change above a seam and not a migration: nothing moves to a new
/// representation here, so every behaviour is the behaviour the concrete type already had and every
/// method is a forward. A second impl arrives with the first kind that actually moves.
impl TimestampedSeries for BTreeMap<u64, ElementEntry> {
    fn len(&self) -> usize {
        BTreeMap::len(self)
    }

    fn get(&self, at: u64) -> Option<&ElementEntry> {
        BTreeMap::get(self, &at)
    }

    fn addresses(&self) -> impl Iterator<Item = &ElementEntry> {
        self.values()
    }

    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)> {
        self.iter().map(|(at, address)| (*at, address))
    }

    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl DoubleEndedIterator<Item = (u64, &ElementEntry)> {
        BTreeMap::range(self, bounds).map(|(at, address)| (*at, address))
    }

    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)> {
        self.iter_mut().map(|(at, address)| (*at, address))
    }
}

/// A series the apply path may add points to.
///
/// Separate from [`TimestampedSeries`] so that a bound says what a caller may do: the walk and the
/// reconcile take the read trait and therefore CANNOT add a point, which is a property of the
/// signature rather than of the body.
///
/// `Default` is required because the apply path creates a series for a key that has none -- the
/// `entry(..).or_default()` the old dispatch did at its call site, moved in here where the
/// representation is known.
pub(super) trait TimestampedSeriesWrite: TimestampedSeries + Default {
    /// Record an address at one instant, replacing whatever was there.
    fn set(&mut self, at: u64, address: ElementEntry);

    /// Forget the point at one instant, if any.
    fn clear(&mut self, at: u64);
}

impl TimestampedSeriesWrite for BTreeMap<u64, ElementEntry> {
    fn set(&mut self, at: u64, address: ElementEntry) {
        self.insert(at, address);
    }

    fn clear(&mut self, at: u64) {
        self.remove(&at);
    }
}

/// Set or clear one point of one object's series, and hand back the addresses that SURVIVE.
///
/// The surviving set is returned rather than read back by the caller because the caller needs it
/// for the bucket-index sync, and reaching back into the map for it would be a second lookup
/// against a series this function may just have removed.
///
/// AN EMPTIED SERIES TAKES ITS KEY WITH IT, which is the behaviour the apply path had at its call
/// site and is kept here so that no arm can forget it.
/// GENERIC OVER THE KEY TYPE TOO, and that was an omission rather than a flourish. The seam that made
/// the series abstract left `String` hard-coded here, while the consolidated map these tenants
/// migrate INTO is keyed by `ModelKey = Arc<str>` -- sixteen bytes against twenty-four, chosen
/// deliberately, and shareable where a `Box<str>` is not: two maps can hold ONE `Arc`
/// allocation, which is the whole reason this seam is worth pointing at the real destination. A seam is only as general as the dimension it varied, and pointing it at the real
/// destination is what found this.
///
/// `Borrow<str>` to look a key up and remove it from a `&str`, and `Clone` for the removal path --
/// a refcount bump for a shared handle, the copy it always was for an owned `String`. Creating a
/// key from a `&str` is NOT among the bounds any more: that is what minted a second allocation on
/// every call, and it now lives in `set_point_for_owned_key` for the kinds that need it.
pub(super) fn set_point_and_collect_live<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    set_point_inner(map, key, at, address, true)
}

/// THE SAME WRITE WITHOUT THE SURVIVING SET, FOR THE CALLERS THAT DO NOT WANT IT.
///
/// # WHY THIS EXISTS
///
/// The surviving set is a clone of EVERY address the series holds, so asking for it on a write to
/// a 3,250-point series copies 3,250 addresses into a fresh vector. That is the right price for
/// the one caller that needs them, and it was being paid by two that threw the vector away: both
/// context-event write arms in `execute_on_shard` called the collecting door and bound the result
/// to nothing. Every context event written to a node copied the node entire event series and
/// dropped it.
///
/// MEASURED on `what_one_context_event_append_costs_at_two_series_lengths`, whose signature is
/// what pointed here: a flat 122 allocations per append with bytes still climbing 2.66x over an
/// 8x series -- one allocation, sized by the entries held, which is exactly one whole-series
/// clone. That same probe had already ruled out the two causes assumed before it, the whole-object
/// index record and the shard-wide map in the ordinal allocator, the second by moving it and
/// re-measuring.
///
/// # ONE BODY, NOT TWO
///
/// Both doors run `set_point_inner`, because the empty-series rule is the kind of thing that gets
/// forgotten in a copy: AN EMPTIED SERIES TAKES ITS KEY WITH IT, and a second body would be one
/// more place for that to drift. `Vec::new()` does not allocate, so the non-collecting door moves
/// no bytes at all.
pub(super) fn set_point<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
) where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    let dropped = set_point_inner(map, key, at, address, false);
    debug_assert!(
        dropped.is_empty(),
        "the non-collecting door returned addresses, so it collected the series it exists not to collect"
    );
}

fn set_point_inner<K, S>(
    map: &mut std::collections::HashMap<K, S>,
    key: K,
    at: u64,
    address: Option<ElementEntry>,
    collect_live: bool,
) -> Vec<ElementEntry>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str> + Clone,
    S: TimestampedSeriesWrite,
{
    // THE KEY ARRIVES OWNED, AND THAT IS THE WHOLE CHANGE. This took a `&str` and minted its own
    // key with `K::from(object_key)` on every call -- so a map whose key CAN be shared got a fresh
    // allocation anyway, and converting the map's key type would have shared nothing. The caller
    // knows whether it already holds a handle; this does not, and should not guess.
    //
    // The clone is for the removal below: `entry` consumes the key, and the empty-series path still
    // needs the characters to remove by. For a shared handle that clone is a refcount bump; for an
    // owned `String` it is the copy it always was.
    let for_removal = key.clone();
    let series = map.entry(key).or_default();
    match address {
        Some(address) => series.set(at, address),
        None => series.clear(at),
    }
    // COLLECTED BEFORE THE REMOVAL BELOW, AND THAT ORDER IS LOAD-BEARING: a write that empties
    // the series takes its key out of the map, and the surviving set the collecting caller wants
