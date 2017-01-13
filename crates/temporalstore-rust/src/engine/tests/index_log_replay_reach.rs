// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH LOAD PATH ACTUALLY FOLDS THE INDEX LOG, MEASURED RATHER THAN ARGUED.
//!
//! `fold_index_log_deltas` is the only production caller of
//! `for_each_delta_record_above_anchor`, and it is easy to assume from its name that every
//! restore runs it. It does not. `load_shard_with` branches on `wal_single_barrier()`, which is
//! `!wal_legacy_recovery()` and therefore TRUE unless an operator sets `TS_WAL_LEGACY_RECOVERY`:
//! the default arm calls `load_index_base_only`, which passes `fold_deltas = false`, and the fold
//! is not on that path at all. The fold is reached from the OTHER arm -- the escape hatch -- and
//! from `install_latest_manifest_if_newer_on_load`, which that arm calls first.
//!
//! So this module measures reachability instead of asserting it, and it measures BOTH arms over
//! ONE store, so the two rows are about the same files and the same records. The instrument is
//! floored: the arm that is supposed to fold must be shown to have read something, because a
//! reachability run that reads zero everywhere reads exactly like one whose apparatus never
//! spoke.
//!
//! The counter used is `fold_pieces_declined`, and it is used because only ONE entry point can
//! move it: a piece is declined only when a non-zero base anchor is passed, and
//! `fold_index_log_deltas` is the only caller that passes one. A zero in that column is therefore
//! "the load-path fold did not run", not "the fold ran and found nothing" -- provided the fixture
//! holds declinable pieces, which is floored against the other arm.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;
/// Small enough that a few thousand writes seal many pieces. Thread-local, and put back on drop.
const PIECE_BYTES: u64 = 8 * 1024;

struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        crate::index_log::set_index_log_segment_bytes_for_test(None);
    }
}

fn write_keys(engine: &TemporalEngine, first: usize, count: usize) {
    let commands = (first..first + count)
        .map(|value| Command::StringSet {
            key: format!("tenant/1/object/{value:08}"),
            value: vec![b'v'; 16],
        })
        .collect::<Vec<_>>();
    for chunk in commands.chunks(500) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
}

/// THE DEFAULT LOAD PATH DOES NOT FOLD THE INDEX LOG. THE ESCAPE HATCH DOES.
///
/// Both arms are driven over one store, in one test, so neither row can be explained by a
/// different fixture. What the rows say:
///
/// - the default `load_shard` declines no piece, because it never reaches the anchor-aware fold;
/// - `load_index_checked`, which is what the `TS_WAL_LEGACY_RECOVERY` arm and
///   `install_latest_manifest_if_newer_on_load` both call, declines most of them.
///
/// This is the whole reachability claim for the piece-level decline, and it is a NARROW one: the
/// saving lands on the escape hatch an operator flips in the field, not on every restart. It is
/// worth having there -- the population of that arm is the shards already in trouble -- but it
/// must not be described as a restore-path win in general.
#[test]
fn the_default_load_path_does_not_fold_the_index_log_and_the_checked_one_does() {
    crate::index_log::set_index_log_segment_bytes_for_test(Some(PIECE_BYTES));
    let _rolling = RollingThreshold;

    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache"), &blocks, &indexes);
    engine.load_shard(SHARD);

    // Everything below the base first, so the pieces these writes seal name anchors the base
    // will cover -- which is what makes them declinable at all.
    write_keys(&engine, 0, 4_000);
    engine.flush_shard_index(SHARD);
    let base_bytes = std::fs::read(engine.index_path(SHARD)).expect("the base index was written");
    let base_state = decode_index_bytes(&base_bytes).expect("the base index decodes");
    let base_anchor = base_state.applied_wal_sequence.unwrap_or(0);
    // DENOMINATOR ONE: a base anchor of 0 turns the decline off entirely, and every row below
    // would then be a zero that means nothing.
    assert!(
        base_anchor > 0,
        "the base index carries no anchor, so no piece can be declinable"
    );
    // A SUFFIX ABOVE IT, AND ENOUGH OF IT TO SEAL PIECES OF ITS OWN. The engine appends one
    // delta per BATCH, not per write, so a couple of hundred writes are one record and land in
    // the piece being written -- which is never declined, and would make "some pieces are not
    // declinable" true for a reason that has nothing to do with the predicate. Several batches
    // seal several pieces above the base instead.
    write_keys(&engine, 4_000, 2_000);

    // DENOMINATOR TWO: the log must be in several sealed pieces, and they must not all name the
    // same side of the base anchor -- some below it, some above. A fixture whose pieces are all
    // one or all the other cannot tell a correct per-piece decision from a constant one.
    let log_root = engine.index_dir.join("indexlogs");
    let mut named_anchors = Vec::new();
    for entry in std::fs::read_dir(&log_root)
        .expect("the index-log directory exists")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(middle) = name
            .strip_prefix(&format!("shard-{SHARD}.indexlog."))
            .and_then(|rest| rest.split('.').next())
        else {
            continue;
        };
        let fields: Vec<&str> = middle.split('-').collect();
        if fields.len() != 3 {
            continue;
        }
        let Ok(anchor) = fields[2].parse::<u64>() else {
            continue;
        };
        named_anchors.push(anchor);
    }
    let sealed = named_anchors.len();
    let below = named_anchors
        .iter()
        .filter(|anchor| **anchor <= base_anchor)
        .count();
    assert!(
        sealed >= 5,
        "the fixture sealed {sealed} pieces at {PIECE_BYTES} bytes -- too few for a per-piece \
         decision to be visible"
    );
    assert!(
        below > 0 && below < sealed,
        "of {sealed} sealed pieces {below} name an anchor at or below the base's {base_anchor} \
         -- the fixture must straddle the base or every piece gets the same answer: \
         {named_anchors:?}"
    );

    // PUT THE STALE BASE BACK. The engine reissues the base-index write as it goes, so by now
    // the file on disk anchors at the newest record and reflects the whole log -- at which point
    // every piece is declinable and the two sides of the predicate cannot both be observed.
    //
    // This is not an artificial state. Under the single-barrier default the base-index write is
    // ISSUED but its fsync is DEFERRED (`wal_only_sync`), so a crash leaves exactly this: an
    // index log that has run ahead of the base checkpoint that survived. Restoring the bytes
    // taken at the last `flush_shard_index` -- the last write that WAS fsynced -- reproduces it.
    // BEFORE EACH ARM, not once: a load RE-MATERIALIZES the base index, so arm A would hand
    // arm B a base that anchors at the newest record and every piece would be declinable again.
    // Both arms must meet the same store, which means the same stale base.
    let index_path = engine.index_path(SHARD);
    let restore_stale_base = || {
        std::fs::write(&index_path, &base_bytes).expect("the stale base writes back");
        let reread =
            decode_index_bytes(&std::fs::read(&index_path).unwrap()).expect("it decodes");
        assert_eq!(
            reread.applied_wal_sequence,
            Some(base_anchor),
            "the restored base anchors at {:?}, not the {base_anchor} the denominators were \
             computed against",
            reread.applied_wal_sequence
        );
    };

    // ARM A: the default load path, over the same files.
    let reader_default = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-default"),
        &blocks,
        &indexes,
    );
    restore_stale_base();
    crate::index_log::probe::reset();
    reader_default.load_shard(SHARD);
    let default_frames = crate::index_log::probe::fold_frames_read();
    let default_declined = crate::index_log::probe::fold_pieces_declined();

    // ARM B: the fold-aware load, which is what the escape-hatch arm calls. Same files.
    let reader_checked = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-checked"),
        &blocks,
        &indexes,
    );
    restore_stale_base();
    crate::index_log::probe::reset();
    let folded = reader_checked
        .load_index_checked(SHARD, false)
        .expect("the delta log is intact, so the checked load must not refuse it");
    let checked_frames = crate::index_log::probe::fold_frames_read();
    let checked_declined = crate::index_log::probe::fold_pieces_declined();

    println!(
        "index-log fold reachability over one store ({sealed} sealed pieces, base anchor \
         {base_anchor}):\n\
         \x20 load_shard        (the DEFAULT arm, wal_single_barrier) : {default_frames:>6} \
         frames  {default_declined:>4} pieces declined\n\
         \x20 load_index_checked (the TS_WAL_LEGACY_RECOVERY arm and the manifest install) : \
         {checked_frames:>6} frames  {checked_declined:>4} pieces declined"
    );

    // THE FLOOR ON THE APPARATUS, BEFORE ANY CLAIM. Arm B must have read frames and declined
    // pieces, or arm A's zeroes are a probe that never spoke rather than a path that never folds.
    assert!(
        checked_frames > 0,
        "APPARATUS: the fold-aware load read no frame at all, so arm A's zero says nothing"
    );
    assert!(
        checked_declined > 0,
        "APPARATUS: the fold-aware load declined no piece, so the fixture holds nothing \
         declinable and arm A's zero says nothing"
    );
    assert!(folded.is_some(), "APPARATUS: the fold-aware load returned no state");

    // THE FINDING: the default arm reaches none of it.
    assert_eq!(
        default_declined, 0,
        "the default load path declined {default_declined} pieces -- it now reaches the \
         anchor-aware fold, and this module's whole premise has changed"
    );
    // And the decline is exactly the pieces the base covers -- not everything, and not a
    // count that happens to match. The sealed pieces above the base are still read, and so is
    // the piece being written.
    assert_eq!(
        checked_declined as usize, below,
        "the fold-aware load declined {checked_declined} pieces; {below} of {sealed} name an \
         anchor at or below the base's {base_anchor}: {named_anchors:?}"
    );
}

// ===========================================================================================
// THE THIRD SITE: A RECORD WITH NO ANCHOR IS NOT A RECORD AT ANCHOR 0
//
// `applied_wal_sequence.unwrap_or(0) <= anchor` turns a MISSING anchor into anchor 0, which is
// at or below every anchor, so an anchor-less record compares as older than everything: already
// reflected, safe to discard. #1832 took that reading out of the post-dump record walk. #1842
// took it out of the piece NAME. It survived at a third site -- the load-path fold in
// `fold_index_log_deltas` -- and the three are not independent: #1842's fix exists to hold a
// piece of anchor-less deltas OPEN so the fold can see the records in it, and the fold then
// skipped exactly those records one line later.
//
// `an_expiry_round_under_replay_writes_a_delta_with_no_anchor` (engine/tests/expiry_scale.rs) is
// the counter that says production writes such a record: an expiry round that reaches its
// checkpoint before anything has anchored the shard emits no items, one key-state per removed
// key, and no anchor. What is built below is that exact shape, put in front of a base that
// anchors somewhere -- which is the state a crash leaves: under the single-barrier default the
// base-index write is ISSUED but its fsync DEFERRED, so the base that survives can be older than
// the delta log that survives beside it.
//
// EVERY SHAPE THE FOLD CAN MEET IS PLANTED, because only one of them is the subject:
//
//   * no anchor, carries content   -- no base reflects it. THE SUBJECT.
//   * anchored AT the base         -- in the base.
//   * anchored ABOVE the base      -- not in the base, and it moves the reconstructed anchor.
//
// A fixture holding only the first cannot tell this change from "fold everything".
//
// THE FOURTH SHAPE IS NOT PLANTED HERE, AND THAT IS A FACT ABOUT THE WALK RATHER THAN A GAP. A
// record carrying no anchor AND nothing the fold would apply -- the legacy whole-index line --
// never reaches the fold at all: `for_each_delta_record_above_anchor` drops it itself, with
// "only keep records that carry a delta payload OR a WAL anchor". So an engine-level fixture
// cannot hold one up in front of the fold; the append lands in the log and the walk swallows it.
// That filter is a FOURTH spelling of the same question, and it agrees with the predicate: the
// records it drops are exactly the ones the predicate calls reflected. The corner of the
// predicate it covers is asserted where the predicate is called directly, in
// `index_log::tests::what_a_base_anchor_reflects_at_every_corner` -- and it has to be asserted
// somewhere, because the post-dump sweep asks the same question of a RAW PAYLOAD, where the
// legacy line very much does appear.
// ===========================================================================================

/// Zero-padded so key order and assertion order agree.
fn deadline_key(index: usize) -> String {
    format!("tenant/1/deadline/{index:04}")
}

/// What the fixture planted, so every control below is built from how it was WRITTEN rather
/// than from the predicate under test.
struct AnchorLessFixture {
    engine: TemporalEngine,
    base_anchor: u64,
    /// Anchor-less records carrying one key-state per key. No base reflects these.
    with_content: Vec<u64>,
    /// Anchored records the base does NOT cover, planted with the HIGHER anchor FIRST so that
    /// "take the last anchor" and "take the highest anchor" give different answers.
    above: Vec<(u64, u64)>,
    /// An anchored record exactly AT the base anchor. The base covers it.
    at_base: u64,
}

impl AnchorLessFixture {
    /// The sequences the fold must apply, in log order.
    fn expected_applied(&self, records: &[crate::index_log::IndexDeltaRecord]) -> Vec<(ShardId, u64)> {
        records
            .iter()
            .filter(|record| !self.expected_reflected(record.sequence))
            .map(|record| (record.shard_id, record.sequence))
            .collect()
    }

    fn expected_reflected(&self, sequence: u64) -> bool {
        if self.with_content.contains(&sequence) {
            return false;
        }
        if self.above.iter().any(|(seq, _)| *seq == sequence) {
            return false;
        }
        true
    }

    /// The anchor a correct fold leaves behind: the HIGHEST anchor among the records it applied.
    fn expected_anchor(&self) -> u64 {
        self.above
            .iter()
            .map(|(_, anchor)| *anchor)
            .max()
            .expect("the fixture plants at least one record above the base")
    }
}

/// A base anchored above zero holding a deadline for every key, and one record of every shape
/// the predicate separates.
///
/// The deadline is ten minutes out ON PURPOSE: nothing here may actually expire. The question is
/// what the FOLD does with a record describing an expiry, not whether an expiry round runs, and a
/// deadline that could fire during the test would let a passing run be explained by the sweep.
fn base_with_deadlines_and_anchor_less_deltas(
    dir: &std::path::Path,
    keys: usize,
    with_content: usize,
) -> AnchorLessFixture {
