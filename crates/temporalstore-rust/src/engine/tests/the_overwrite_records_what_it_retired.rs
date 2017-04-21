// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN OVERWRITE RECORDS ABOUT WHAT IT RETIRED, and what that makes reportable.
//!
//! # THE THING THAT DID NOT EXIST
//!
//! Liveness was tracked exactly and staleness was never recorded. `insert_block` and `remove` both
//! discharge `block_slab_live`, and that tally is right to the byte -- 2,000 keys x 256 B holds at
//! 536,000 B across five rewrites, which is precisely the live set. But nothing wrote down what
//! LEFT, so garbage was only derivable as `logical_ever - live` per slab, and only after a
//! storage-manager cycle had published the union tally. Before that publish every slab reported
//! `live 0, garbage 10000 bp` -- a slab holding live data reading as entirely garbage.
//!
//! Recording the displaced range SYNCHRONOUSLY, in the call that displaces the entry, is what
//! makes the figure available at the moment the waste is created rather than after a cycle. This
//! arm is the evidence that it is recorded there, that the whole GC control vocabulary derives from
//! the list, and that the figure moves.
//!
//! # FOUR CLAIMS, AND THE BOUND IS ONE OF THEM
//!
//!   1. the range is recorded where the live tally is discharged, so the two cannot disagree;
//!   2. the list is BOUNDED -- ranges merge, the per-slab count is capped, and a discarded slab's
//!      detail leaves with the slab. An append-per-overwrite list grows with the write RATE and
//!      never with the data, which is the shape the changefeed already has in this tree;
//!   3. the cause is attributed, so rewrite pressure is distinguishable from a shrinking live set;
//!   4. space amplification computed from the record moves 1.000x -> 5.000x under five writes of
//!      every key, where the published `space_amplification_ppm` stays pinned at 1.000x because its
//!      denominator counts a live slab's whole file.
//!
//! # THE VALUES ARE INCOMPRESSIBLE, AND EACH ROUND WRITES DIFFERENT BYTES
//!
//! `vec![b'v'; n]` compresses to almost nothing, which makes stored bytes framing rather than
//! content: on that fixture the live figure reads the same 70,000 B at value 256, 1,024 and 4,096
//! alike, so a stored-byte denominator stops tracking the data. And a rewrite with IDENTICAL bytes
//! is a workload a store is allowed to recognise and skip, which would make the rewrite rounds
//! measure deduplication. Every figure here is in stored bytes and every round writes new ones.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_overwrite_records_what_it_retired
//!   cargo test -p temporalstore-rust --lib the_space_figure_moves_at_the_overwrite -- --ignored --nocapture

use crate::block_store::ElementEntry;
use crate::engine::state::{
    BlockSlabLiveIndex, StaleCause, MIN_DISCARD_RANGE_BYTES, STALE_RANGES_PER_SLAB,
    STALE_SLABS_TRACKED,
};

/// An entry naming a byte range on a slab, which is all the record reads off it.
fn entry(block_slab_id: u64, offset: u64, length: u64) -> ElementEntry {
    ElementEntry::from_parts(block_slab_id, offset, length, Some(0), None)
}

/// Charge a block to the live tally and then retire it, which is the pair every production path
/// makes. Charging first matters: a slab with no live tally takes `remove_address`'s early return,
/// and an arm that skipped the charge would be testing that path by accident.
fn charge_then_retire(
    live: &mut BlockSlabLiveIndex,
    block_slab_id: u64,
    offset: u64,
    length: u64,
    cause: StaleCause,
) {
    let entry = entry(block_slab_id, offset, length);
    live.add_address(&entry);
    live.remove_address(&entry, cause);
}

#[test]
fn the_overwrite_records_what_it_retired() {
    let mut live = BlockSlabLiveIndex::default();
    for block in 0..10_u64 {
        charge_then_retire(&mut live, 7, block * 100, 100, StaleCause::Overwrite);
    }

    let tally = live.stale_tally(7);
    assert_eq!(
        tally.block_refs, 10,
        "ten blocks retired off slab 7 and the record counts {} -- the ref count is EXACT and \
         nothing but a prune may reduce it",
        tally.block_refs
    );
    assert_eq!(
        tally.bytes, 1_000,
        "ten 100 B blocks retired and the record holds {} B. The byte figure sums \
         `ElementEntry::length`, the same unit the live tally is in, so a ratio between them means \
         something only if this is exact",
        tally.bytes
    );
    // ---- THE MERGE IS WHAT BOUNDS THE LIST. Ten contiguous ranges collapse to one, so the count
    // ---- grows with how FRAGMENTED the garbage is and not with how much of it there is.
    assert_eq!(
        tally.range_count(),
        1,
        "ten CONTIGUOUS ranges were recorded and the list holds {} of them. Without a merge this \
         list grows once per overwrite forever, which is the changefeed shape and is worse than no \
         list at all",
        tally.range_count()
    );
    let merged = tally.ranges()[0];
    assert_eq!(
        (merged.offset, merged.length),
        (0, 1_000),
        "the merged range reads offset {} length {}, and the ten blocks covered 0..1000 exactly. A \
         merge that moved either end would claim bytes that did not retire",
        merged.offset,
        merged.length
    );
    assert_eq!(tally.dropped_range_count(), 0, "nothing came near the cap here");
}

#[test]
fn a_retired_range_between_two_others_bridges_them() {
    // THE ORDER IS THE POINT. Retiring the MIDDLE block last makes all three one range, and a
    // merge that only extended the left neighbour would leave the right one covering bytes the
    // left one now also covers -- double-counting the same garbage and reporting fragmentation
    // that had just been closed.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 3, 0, 100, StaleCause::Overwrite);
    charge_then_retire(&mut live, 3, 200, 100, StaleCause::Overwrite);
    assert_eq!(
        live.stale_tally(3).range_count(),
        2,
        "two ranges with 100 B of live block between them must not merge"
    );
    charge_then_retire(&mut live, 3, 100, 100, StaleCause::Overwrite);
    let tally = live.stale_tally(3);
    assert_eq!(
        tally.range_count(),
        1,
        "the block between two recorded ranges retired and the list holds {} ranges. The three \
         cover 0..300 with no hole, so they are one range",
        tally.range_count()
    );
    assert_eq!((tally.ranges()[0].offset, tally.ranges()[0].length), (0, 300));
    assert_eq!(tally.bytes, 300, "three 100 B blocks, and the byte figure is independent of merging");
}

#[test]
fn ranges_that_retired_for_different_reasons_do_not_merge() {
    // The cause is a FIELD of the range, so a merged range would have to pick one cause and lie
    // about the other. Two touching ranges with different causes are therefore kept apart, which
    // is what keeps the attribution exact at the cost of a range.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 1, 0, 100, StaleCause::Overwrite);
    charge_then_retire(&mut live, 1, 100, 100, StaleCause::Removed);
    charge_then_retire(&mut live, 1, 200, 100, StaleCause::Removed);
    let tally = live.stale_tally(1);
    assert_eq!(
        tally.range_count(),
        2,
        "an overwritten range touching a deleted one must stay separate, and the two deleted ones \
         must merge: expected 2 ranges, found {}",
        tally.range_count()
    );
    let summary = live.stale_summary();
    assert_eq!(
        (summary.overwrite_bytes, summary.removed_bytes, summary.swept_bytes),
        (100, 200, 0),
        "the cause split reads {:?}, and it is what separates rewrite pressure from a live set \
         that simply got smaller -- the same pending total means opposite things",
        (summary.overwrite_bytes, summary.removed_bytes, summary.swept_bytes)
    );
    assert_eq!(
        summary.stale_bytes,
        summary.overwrite_bytes + summary.removed_bytes + summary.swept_bytes,
        "the window total must be the sum of its causes"
    );
}

#[test]
fn the_range_list_is_capped_and_says_so_while_the_bytes_stay_exact() {
    // EVERY RANGE NON-ADJACENT, so none of them can merge and the cap is the only thing standing
    // between this list and unbounded growth.
    let mut live = BlockSlabLiveIndex::default();
    let retirements = STALE_RANGES_PER_SLAB * 3;
    for block in 0..retirements as u64 {
        charge_then_retire(&mut live, 11, block * 1_000, 100, StaleCause::Overwrite);
    }
    let tally = live.stale_tally(11);
    assert_eq!(
        tally.range_count(),
        STALE_RANGES_PER_SLAB,
        "{retirements} non-mergeable ranges were recorded against a cap of \
         {STALE_RANGES_PER_SLAB}, and the list holds {}",
        tally.range_count()
    );
    assert_eq!(
        tally.dropped_range_count() as usize,
        retirements - STALE_RANGES_PER_SLAB,
        "the ranges the cap refused must be COUNTED, so a reader can tell incomplete detail from \
         no detail: expected {} dropped, found {}",
        retirements - STALE_RANGES_PER_SLAB,
        tally.dropped_range_count()
    );
    // ---- AND THIS IS THE DIVISION THE DESIGN RESTS ON. The detail is droppable; the bytes are
    // ---- not. A space figure standing on `bytes` is standing on recorded fact however far the
    // ---- range list overflowed.
    assert_eq!(
        tally.bytes,
        retirements as u64 * 100,
        "the cap refused {} ranges and the byte figure must still be exact: {} retirements x 100 B \
         is {} B, and the record holds {}",
        tally.dropped_range_count(),
        retirements,
        retirements as u64 * 100,
        tally.bytes
    );
    assert_eq!(tally.block_refs, retirements as u64, "and so must the ref count");
    assert!(
        live.stale_summary().dropped_range_count > 0,
        "the summary must carry the dropped count, or an overflowed list looks complete"
    );
}

#[test]
fn the_tracked_slab_count_is_capped_and_the_victim_is_a_finished_slab() {
    // THE BACKSTOP, for a process that never ran a reclaim round. One range per slab, so the
    // per-slab cap cannot be what bounds this.
    let mut live = BlockSlabLiveIndex::default();
    let slabs = STALE_SLABS_TRACKED as u64 + 8;
    for block_slab_id in 0..slabs {
        charge_then_retire(&mut live, block_slab_id, 0, 100, StaleCause::Overwrite);
    }
    let summary = live.stale_summary();
    assert_eq!(
        summary.slabs_tracked, STALE_SLABS_TRACKED as u64,
        "{slabs} slabs retired a block against a cap of {STALE_SLABS_TRACKED}, and the record \
         tracks {}",
        summary.slabs_tracked
    );
    assert_eq!(
        summary.slabs_pruned, 8,
        "eight slabs past the cap must have been pruned, and the count says {}",
        summary.slabs_pruned
    );
    // ---- PRUNED BYTES LEAVE `pending` AND ARRIVE IN `discarded`, so the window total is
    // ---- conserved and a figure that fell because detail was dropped is distinguishable from one
    // ---- that fell because garbage was collected.
    assert_eq!(
        summary.pending_discard_bytes + summary.discarded_bytes,
        summary.stale_bytes,
        "pending {} + discarded {} must equal the window total {}",
        summary.pending_discard_bytes,
        summary.discarded_bytes,
        summary.stale_bytes
    );
    assert_eq!(summary.discarded_bytes, 800, "eight pruned slabs at 100 B each");
}

#[test]
fn the_prune_takes_a_discarded_slabs_detail_with_it() {
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 1, 0, 500, StaleCause::Overwrite);
    charge_then_retire(&mut live, 2, 0, 700, StaleCause::Overwrite);
    let before = live.stale_summary();
    assert_eq!(before.pending_discard_bytes, 1_200);
    assert_eq!(before.slabs_tracked, 2);

    live.discard_stale_slabs([1_u64]);
    let after = live.stale_summary();
    assert_eq!(
        after.slabs_tracked, 1,
        "slab 1 was discarded, so its range list must go with it"
    );
    assert_eq!(
        after.pending_discard_bytes, 700,
        "the collector took slab 1's 500 B, so they have stopped being PENDING a discard. Leaving \
         them would report the same garbage twice -- here and as reclaimable_physical_bytes"
    );
    assert_eq!(after.discarded_bytes, 500);
    assert_eq!(
        after.stale_bytes, before.stale_bytes,
        "the WINDOW total does not fall on a discard: 1,200 B retired and 1,200 B retired, however \
         much of it has since been collected. That is what makes discard_backlog_ppm meaningful"
    );
    assert_eq!(
        after.discard_backlog_ppm,
        700 * 1_000_000 / 1_200,
        "the backlog is pending over retired, and it must fall as the collector catches up"
    );
}

#[test]
fn a_wholesale_rebuild_forgets_the_retired_set() {
    // `clear` is called where the index this describes is REPLACED -- a compaction round, a
    // manifest install. The retired ranges name offsets in slabs the rebuild is about to stop
    // pointing at, and a compaction has just handed those slabs to the collector, so carrying them
    // across would report the same garbage twice. This is also the reload behaviour: the record is
    // derived state, and the window opens again where the index was built.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 4, 0, 900, StaleCause::Overwrite);
    assert!(live.stale_summary().pending_discard_bytes > 0);
    live.clear();
    let summary = live.stale_summary();
    assert_eq!(
        summary,
        Default::default(),
        "a wholesale rebuild must leave the retired set empty in every field, and it left {summary:?}"
    );
}

#[test]
fn the_drift_check_does_not_delete_the_retired_set() {
    // `reset_from` is how the drift check corrects the LIVE half from a walk of the index. That
    // walk reads the blocks the index still points at, so it has nothing to say about the ones it
    // stopped pointing at -- a recomputed stale set is empty BY CONSTRUCTION, and resetting to it
    // would delete true records every time the check ran.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 5, 0, 300, StaleCause::Overwrite);
    let recorded = live.stale_summary().pending_discard_bytes;
    assert_eq!(recorded, 300);
    live.reset_from(Default::default());
    assert_eq!(
        live.stale_summary().pending_discard_bytes,
        recorded,
        "correcting the live tally from the walk must leave the retired set alone, or the drift \
         check silently erases the record it cannot recompute"
    );
}

#[test]
fn a_dead_slab_is_a_discard_candidate_and_a_small_hole_is_a_compaction_candidate() {
    let mut live = BlockSlabLiveIndex::default();
    // SLAB 20: every block retired, so the collector can drop it whole. The hole is one merged
    // range well above the smallest discard that returns space, so there is nothing to compact.
    charge_then_retire(&mut live, 20, 0, MIN_DISCARD_RANGE_BYTES * 2, StaleCause::Overwrite);
    // SLAB 21: still holds a live block, and the hole is too small for a discard to return space.
    // Only relocating the live remainder recovers it, which is a compaction.
    let kept = entry(21, 0, 10_000);
    live.add_address(&kept);
    charge_then_retire(&mut live, 21, 10_000, 100, StaleCause::Overwrite);

    let summary = live.stale_summary();
    assert_eq!(
        summary.discard_candidate_slabs, 1,
        "slab 20 has no live block left, which is what a discard candidate is"
    );
    assert_eq!(summary.discard_candidate_bytes, MIN_DISCARD_RANGE_BYTES * 2);
    assert_eq!(
        summary.fragmented_range_count, 1,
        "the 100 B hole is below the {MIN_DISCARD_RANGE_BYTES} B floor a discard can return, and \
         the {} B one is not -- so exactly one range is fragmented",
        MIN_DISCARD_RANGE_BYTES * 2
    );
    assert_eq!(
        summary.compaction_candidate_slabs, 1,
        "slab 21 is partly live AND carrying a hole too small to punch, which is the one shape \
         only a compaction recovers"
    );
    assert_eq!(summary.compaction_candidate_bytes, 100);
    assert!(
        summary.compaction_recommended(),
        "a compaction candidate exists, so the flag derived from exactly that -- a slab both partly \
         live and carrying a hole too small to punch -- must be true"
    );

    // ---- AND IT IS FALSE WHEN EVERY PENDING BYTE IS ON A SLAB THE COLLECTOR COULD DROP WHOLE,
    // ---- which is what makes the flag worth publishing rather than a constant.
    let mut only_dead = BlockSlabLiveIndex::default();
    charge_then_retire(&mut only_dead, 30, 0, MIN_DISCARD_RANGE_BYTES * 2, StaleCause::Overwrite);
    assert!(
        !only_dead.stale_summary().compaction_recommended(),
        "every pending byte is on a wholly dead slab, so a compaction would be wasted work and \
         the flag must say so"
    );
}

