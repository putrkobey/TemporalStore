// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN INDEX-LOG SWEEP'S PER-ROUND ENTRY BUDGET ACTUALLY BOUNDS.
//!
//! Two production sweeps walk this log and only one of them takes a per-round entry budget:
//!
//! - [`LocalIndexLogStore::gc_before_sequence_limited`] takes `max_entries_per_round`, and the
//!   storage-manager cycle passes 256 (`StorageManagerCycleRequest::default`);
//! - [`LocalIndexLogStore::gc_reflected_before_anchor`] takes no such parameter at all and
//!   constructs its report with `max_entries_per_round: 0`.
//!
//! That asymmetry reads as an oversight. It is not, and the numbers below are why.
//!
//! WHAT `0` MEANS, READ FROM THE CONSUMER. The only code that consumes the quantity is the
//! record loop in `gc_before_sequence_limited`, which guards it as `max_entries_per_round > 0 &&
//! removed_this_round >= max_entries_per_round`, and the `budget_exhausted` term, guarded the
//! same way. So `0` in the PARAMETER means "no budget". In `gc_reflected_before_anchor` there is
//! no parameter and no consumer: the `0` is a REPORT FIELD, and its value is already
//! `IndexLogGcReport::default()`'s. It reports honestly that the sweep applied no entry budget.
//!
//! AND AN ENTRY BUDGET IS NOT A BOUND ON WORK. When the budget is reached the record is pushed
//! onto `retained` -- the loop does not break. So the budget:
//!
//! - does NOT bound what the sweep READS (both loops read the piece being written to its end);
//! - does NOT bound how long the sweep holds `inner`, which is the lock `append_json` takes;
//! - does bound what the sweep REMOVES, and therefore makes the rewrite COPY MORE, not less.
//!
//! That is the inversion the note on the parameter already describes at 40,000 records in
//! milliseconds. [`an_entry_budget_copies_more_and_reads_exactly_as_much`] states it in counts.
//!
//! WHAT DOES BOUND EITHER SWEEP is the rolling threshold. Whole sealed pieces are unlinked by
//! name without being opened, so the walk and the rewrite see only the piece being written --
//! at most `TS_INDEX_LOG_SEGMENT_BYTES`, 64 KiB by default. That is a bound on what the sweep
//! READS, which the campaign's survey found the engine has exactly one of.
//!
//! Counted, never timed: the box these run on carries other tenants.

use super::*;
use std::collections::BTreeSet;

const SHARD: ShardId = 11;
/// The two corpus sizes. Ten times apart, and the large one must roll into many pieces or every
/// per-piece claim below is vacuous.
const SMALL: usize = 2_000;
const LARGE: usize = 20_000;

/// Set the rolling threshold for this thread and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// Put the removal probe back however the test leaves.
struct ArmedRemovals;
impl Drop for ArmedRemovals {
    fn drop(&mut self) {
        probe::disarm_removals();
    }
}
fn arm_removals() -> ArmedRemovals {
    probe::arm_removals();
    ArmedRemovals
}

fn item(key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: 0,
        block_ref_key: key.to_string(),
        object_key: key.into(),
        model_id: "m".to_string(),
        object_id: 1,
        deleted: false,
        // ADDRESS-LESS BY DESIGN: this item is the smallest a delta record can carry,
        // and an address would make it bigger rather than smaller. The `size` it used
        // to state was never restored on a row with no address to derive it from.
        entry: None,
    }
}

/// `records` delta records, every one anchored at WAL sequence 1, into a fresh store.
///
/// Anchored so the post-dump sweep finds every one of them reflected by a base at anchor 1:
/// the two sweeps are then being asked to remove the SAME set, which is what makes their costs
/// comparable at all.
fn seed(dir: &std::path::Path, records: usize) -> LocalIndexLogStore {
    let store = LocalIndexLogStore::new(dir);
    for value in 0..records {
        store
            .append_delta(
                SHARD,
                // Zero-padded: an unpadded key is one character longer at 20,000 records than at
                // 2,000 and would put fixture bytes into a number measuring the code.
                vec![item(&format!("tenant/1/object/{value:08}"))],
                Vec::new(),
                Some(1),
                None,
                false,
                false,
            )
            .unwrap();
    }
    store
}

/// What one sweep cost, read from the probes rather than from the report it is auditing.
struct SweepCost {
    records: usize,
    pieces_before: usize,
    frames_read: u64,
    frames_copied: u64,
    dir_listings: u64,
    piece_paths: u64,
    records_removed: usize,
    dropped_segments: usize,
}

impl SweepCost {
    /// Frames the sweep read per record OF STORE. This is the denominator that has hidden every
    /// defect of this shape -- it falls as the corpus grows whatever the sweep does.
    fn read_per_record_of_store(&self) -> f64 {
        self.frames_read as f64 / self.records as f64
    }
    /// Frames the sweep read per record OF WORK -- per record it actually removed. This one does
    /// not fall for free.
    fn read_per_record_of_work(&self) -> f64 {
        if self.records_removed == 0 {
            f64::INFINITY
        } else {
            self.frames_read as f64 / self.records_removed as f64
        }
    }
    fn piece_paths_per_record_of_store(&self) -> f64 {
        self.piece_paths as f64 / self.records as f64
    }
}

fn show(tag: &str, cost: &SweepCost) {
    println!(
        "  {tag:<28} records {:>7} | pieces {:>5} | frames read {:>6} | frames copied {:>6} | \
         dir listings {:>4} | piece paths {:>6} | removed {:>7} | pieces unlinked {:>5} | \
         read/record-of-store {:>8.5} | read/record-of-work {:>8.4} | piece-paths/record {:>8.5}",
        cost.records,
        cost.pieces_before,
        cost.frames_read,
        cost.frames_copied,
        cost.dir_listings,
        cost.piece_paths,
        cost.records_removed,
        cost.dropped_segments,
        cost.read_per_record_of_store(),
        cost.read_per_record_of_work(),
        cost.piece_paths_per_record_of_store(),
    );
}

/// Seed `records` and run the post-dump sweep once, counting what it cost.
fn measure_post_dump(dir: &std::path::Path, records: usize) -> SweepCost {
    let store = seed(dir, records);
    let pieces_before = store.piece_count(SHARD);
    probe::reset();
    let report = store
        .gc_reflected_before_anchor(SHARD, 1, records as u64 + 1, 0)
        .unwrap();
    SweepCost {
        records,
        pieces_before,
        frames_read: probe::sweep_frames_read(),
        frames_copied: probe::sweep_frames_copied(),
        dir_listings: probe::dir_listings(),
        piece_paths: probe::piece_paths(),
        records_removed: report.records_removed,
        dropped_segments: report.dropped_segments,
    }
}

/// WHAT THE UNBOUNDED SWEEP COSTS, AT TWO CORPUS SIZES, UNDER BOTH DENOMINATORS.
///
/// The sweep with no entry budget is NOT unbounded in the corpus. What it reads is the piece
/// being written, which the rolling threshold caps; the sealed pieces below the floor go by name
/// and are never opened. So frames read is FLAT at ten times the records, and read per record of
/// STORE falls by ten -- which is exactly the shape that would let an unbounded walk hide, so the
/// per record of WORK column is asserted beside it and is ALSO flat.
///
/// The term that does grow is the piece enumeration: ten times the pieces, ten times the paths
/// each listing hands back. That is the real per-round cost of a large store here, and an entry
/// budget does not touch it either.
#[test]
fn what_the_unbounded_post_dump_sweep_costs_at_two_corpus_sizes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let small = measure_post_dump(small_dir.path(), SMALL);
    let large = measure_post_dump(large_dir.path(), LARGE);

    println!("post-dump sweep (no entry budget), rolling at {DEFAULT_INDEX_LOG_SEGMENT_BYTES} B:");
    show("small", &small);
    show("large", &large);

    // THE DENOMINATOR, FIRST. Every claim below is about a log in many pieces; a fixture that
    // never rolled would report a flat per-piece cost for the uninteresting reason.
    assert!(
        large.pieces_before >= 10 && large.pieces_before >= small.pieces_before * 5,
        "the large fixture did not roll into many more pieces than the small one: {} against {}",
        large.pieces_before,
        small.pieces_before
    );
    assert!(
        small.records_removed > 0 && large.records_removed > 0,
        "a sweep removed nothing, so every cost below is a cost of doing nothing: {} and {}",
        small.records_removed,
        large.records_removed
    );
    assert!(
        small.dropped_segments > 0 && large.dropped_segments > 0,
        "a sweep unlinked no whole piece ({} and {}), so the walk was never the minority of it",
        small.dropped_segments,
        large.dropped_segments
    );

    // WHAT IT READS IS CAPPED BY THE PIECE, NOT BY THE CORPUS. Ten times the records, and the
    // walk reads no more frames -- within one piece's worth either way.
    assert!(
        large.frames_read <= small.frames_read.saturating_mul(2).saturating_add(64),
        "frames read grew with the corpus: {} at {} records against {} at {} records -- the walk \
         is reading sealed pieces again",
        large.frames_read,
        large.records,
        small.frames_read,
        small.records,
    );
    // And it is genuinely a small fraction of the store at the LARGE size, which is the claim
    // that would fail if the roll stopped working.
    assert!(
        large.frames_read * 10 < large.records as u64,
        "the walk read {} of {} records -- that is not a piece, that is the log",
        large.frames_read,
        large.records
    );

    // PER RECORD OF WORK, which the store denominator would have hidden. Flat, so the sweep is
    // not paying more per record removed as the store grows.
    assert!(
        large.read_per_record_of_work() <= small.read_per_record_of_work() * 2.0 + 1.0,
        "frames read per record REMOVED grew with the corpus: {:.4} against {:.4}",
        large.read_per_record_of_work(),
        small.read_per_record_of_work(),
    );

    // THE TERM THAT DOES GROW. Piece paths enumerated is linear in the pieces, and an entry
    // budget bounds none of it: the enumeration happens before the record loop is reached.
    assert!(
        large.piece_paths > small.piece_paths * 3,
        "piece paths did not grow with the piece count: {} against {} for {} against {} pieces -- \
         the per-piece term this measurement exists to name has gone missing",
        large.piece_paths,
        small.piece_paths,
        large.pieces_before,
        small.pieces_before,
    );
    // Flat in LISTINGS though: the cost is per piece handed back, not per listing taken.
    assert!(
        large.dir_listings <= small.dir_listings + 2,
        "directory listings grew with the corpus: {} against {}",
        large.dir_listings,
        small.dir_listings
    );
}

/// AN ENTRY BUDGET READS EXACTLY AS MUCH AND COPIES MORE.
///
/// The same log, swept twice: once with no budget and once with the storage-manager cycle's
/// shipped 256. The budgeted round reads the identical number of frames -- the loop has no
/// break -- removes fewer, and therefore RETAINS more, and every retained record is re-framed
/// and written out. So the budget buys nothing on the read, nothing on the lock hold, and costs
/// strictly more on the write.
///
/// THE CONTROL ARM IS A THIRD ROUND whose removable count is BELOW the budget, where the budget
/// cannot bite. It must come back identical to the unbudgeted round on every column. If that arm
/// ever goes flat with the others -- if all three rounds agree -- the fixture has stopped
/// reaching the budget at all and the comparison above is vacuous.
#[test]
fn an_entry_budget_copies_more_and_reads_exactly_as_much() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    // Rolling OFF for the subject arms: this test is about the record loop, and a log in pieces
    // would have the budget compete with the piece unlinking for the same records. One piece
    // makes the loop the whole of the round.
    let _one_piece = roll_at(0);
    const BUDGET: usize = 256;
    const RECORDS: usize = 2_000;
    // Every record below this floor is removable, so the removable count is 1,500 -- comfortably
    // above the 256 the budget is. CHECKED below rather than assumed: a fixture shrunk to afford
    // more mutants would make the threshold under test inert.
    const RETAIN_FROM: u64 = 1_501;

    let mut arms = Vec::new();
    for (label, budget, retain_from) in [
        ("no budget", 0usize, RETAIN_FROM),
        ("budget 256", BUDGET, RETAIN_FROM),
        // The control: only 100 records are removable, which no budget of 256 can reach.
        ("control, under budget", BUDGET, 101),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = seed(dir.path(), RECORDS);
        let pieces_before = store.piece_count(SHARD);
        probe::reset();
        let report = store
            .gc_before_sequence_limited(SHARD, retain_from, budget)
            .unwrap();
        let cost = SweepCost {
            records: RECORDS,
            pieces_before,
            frames_read: probe::sweep_frames_read(),
            frames_copied: probe::sweep_frames_copied(),
            dir_listings: probe::dir_listings(),
            piece_paths: probe::piece_paths(),
            records_removed: report.records_removed,
            dropped_segments: report.dropped_segments,
        };
        show(label, &cost);
        arms.push((label, budget, report, cost));
    }

    let (_, _, unbudgeted_report, unbudgeted) = &arms[0];
    let (_, _, budgeted_report, budgeted) = &arms[1];
    let (_, _, control_report, control) = &arms[2];

    // THE FIXTURE REACHES THE THRESHOLD UNDER TEST. Without this the whole comparison is
    // "256 is bigger than everything here", which is not what it claims to measure.
    assert!(
        unbudgeted_report.removable_records_before_budget > BUDGET * 4,
        "the fixture offers {} removable records against a budget of {BUDGET}; the budget cannot \
         bite and nothing below means anything",
        unbudgeted_report.removable_records_before_budget
    );
    assert_eq!(
        pieces_are_one(unbudgeted, budgeted, control),
        true,
        "an arm rolled into pieces ({}, {}, {}); the budget is then competing with the unlinking",
        unbudgeted.pieces_before,
        budgeted.pieces_before,
        control.pieces_before
    );

    // READ: IDENTICAL. Not "similar" -- the loop reads every frame in the piece either way, so
    // the two counts are the same integer.
    assert_eq!(
        budgeted.frames_read, unbudgeted.frames_read,
        "a budget of {BUDGET} changed what the sweep READ: {} frames against {} -- if this ever \
         becomes true the loop has learnt to break and the claim here is stale",
        budgeted.frames_read, unbudgeted.frames_read,
    );

    // REMOVES: FEWER, and exactly the budget.
    assert_eq!(
        budgeted_report.records_removed, BUDGET,
        "the budgeted round removed {} records, not the {BUDGET} it was budgeted",
        budgeted_report.records_removed
    );
    assert!(
        unbudgeted_report.records_removed > budgeted_report.records_removed,
        "the unbudgeted round removed {} and the budgeted one {}; the budget did nothing",
        unbudgeted_report.records_removed,
        budgeted_report.records_removed
    );
    assert!(
        budgeted_report.budget_exhausted,
        "the budgeted round did not report its budget exhausted, so it never reached it"
    );

    // COPIES: STRICTLY MORE. This is the whole finding. Asserted as a strict inequality on
    // counts and echoed on bytes, which is what the operator pays.
    assert!(
        budgeted.frames_copied > unbudgeted.frames_copied,
        "the budgeted round copied {} frames and the unbudgeted one {}; the inversion this test \
         exists to name has gone",
        budgeted.frames_copied,
        unbudgeted.frames_copied
    );
    assert!(
        budgeted_report.bytes_copied > unbudgeted_report.bytes_copied,
        "the budgeted round copied {} B and the unbudgeted one {} B",
        budgeted_report.bytes_copied,
        unbudgeted_report.bytes_copied
    );
    println!(
        "  INVERSION: budget {BUDGET} removed {} records ({} fewer) and copied {} B \
         ({} B MORE) than no budget at all, having read the same {} frames",
        budgeted_report.records_removed,
        unbudgeted_report
            .records_removed
            .saturating_sub(budgeted_report.records_removed),
        budgeted_report.bytes_copied,
        budgeted_report
            .bytes_copied
            .saturating_sub(unbudgeted_report.bytes_copied),
        budgeted.frames_read,
    );

    // THE CONTROL ARM, asserted by name with its own failure message. Below the budget the
    // budgeted call and the unbudgeted one must be the same round -- and it must NOT report its
    // budget exhausted, which is what says the arm is really below it.
    assert!(
        !control_report.budget_exhausted,
        "the control arm exhausted its budget, so it is not a control: it offered {} removable \
         records against {BUDGET}",
        control_report.removable_records_before_budget
    );
    assert!(
        control_report.removable_records_before_budget > 0,
        "the control arm had nothing to remove, so it agrees with everything and guards nothing"
    );
    assert_eq!(
        control_report.records_removed, control_report.removable_records_before_budget,
        "the control arm removed {} of {} removable records -- a budget it cannot reach took \
         something anyway",
        control_report.records_removed, control_report.removable_records_before_budget,
    );
    // And the arm must not be flat with the subject: if the control's numbers ever match the
    // budgeted arm's, the fixture stopped separating them.
    assert!(
        control_report.records_removed != budgeted_report.records_removed,
        "the control arm and the budgeted arm removed the same {} records; the arms have gone \
         flat and this test can no longer fail for the right reason",
        control_report.records_removed
    );
}

fn pieces_are_one(a: &SweepCost, b: &SweepCost, c: &SweepCost) -> bool {
    a.pieces_before == 1 && b.pieces_before == 1 && c.pieces_before == 1
}

/// THE SWEEP REMOVES EXACTLY THE REFLECTED SET, ELEMENT BY ELEMENT.
///
/// A count cannot see a sweep that removes the right NUMBER of the wrong records, and removing
/// the wrong record here is silent data loss: a delta carries an eviction that lives nowhere
/// else. So the removal sequence the sweep actually took is compared, sequence by sequence,
/// against a control the test computes itself by decoding the log before the sweep and applying
/// the retention rule independently.
///
/// The control is built from the FILE, not from the report being audited.
#[test]
fn the_post_dump_sweep_removes_exactly_the_set_a_control_computes() {
    let _one_piece = roll_at(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());

    // A mixture, so the predicate has something to decide: records the base reflects, records
    // above its anchor, and an anchor-less record that CARRIES CONTENT and must survive.
    let mut expected_removed = BTreeSet::new();
    let mut sequence = 0u64;
    for value in 0..120usize {
        let anchor = match value % 3 {
            0 => Some(1u64),  // reflected by a base at anchor 1
            1 => Some(9u64),  // above it, must survive
            _ => None,        // anchor-less WITH content, must survive
        };
        sequence = store
            .append_delta(
                SHARD,
                vec![item(&format!("tenant/1/object/{value:08}"))],
                Vec::new(),
                anchor,
                None,
                false,
                false,
            )
            .unwrap();
        if anchor == Some(1) {
            expected_removed.insert(sequence);
        }
    }
    let meta_sequence = sequence + 1;

    let _armed = arm_removals();
    probe::reset();
    probe::arm_removals();
    let report = store
        .gc_reflected_before_anchor(SHARD, 1, meta_sequence, 0)
        .unwrap();
    let actual_removed = probe::removals()
        .into_iter()
        .map(|(_, seq)| seq)
        .collect::<BTreeSet<_>>();

    // DENOMINATOR: the control must be a real set, and a PROPER SUBSET -- a control that expects
    // everything removed cannot tell a correct sweep from one that removes the log.
    assert!(
        expected_removed.len() >= 20,
        "the control expects only {} removals; too few to distinguish anything",
        expected_removed.len()
    );
    assert!(
        expected_removed.len() < 120,
        "the control expects every record removed, so it cannot fail on an over-removing sweep"
    );

    // ELEMENT BY ELEMENT, both directions named separately: what the sweep took that the control
    // did not (data loss), and what the control expected that the sweep left (space held).
    let over: Vec<u64> = actual_removed.difference(&expected_removed).copied().collect();
    let under: Vec<u64> = expected_removed.difference(&actual_removed).copied().collect();
    assert!(
        over.is_empty(),
        "the sweep removed {} record(s) the control says the base does NOT reflect -- that is \
         silent data loss: sequences {:?}",
        over.len(),
        over
    );
    assert!(
        under.is_empty(),
        "the sweep left {} record(s) the control says were reflected: sequences {:?}",
        under.len(),
        under
    );
    assert_eq!(
        report.records_removed,
        expected_removed.len(),
        "the report says {} removed and the control set holds {}",
        report.records_removed,
        expected_removed.len()
