// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one reclaim round COSTS as the quarantine grows, measured at two sizes.

use crate::block_store::*;
use std::fs;

/// A quarantine of `slabs` freshly-written files, every one of them too young to destroy.
///
/// Raw files with NO MANIFEST DESCRIPTOR, so the purge's age falls back to the file mtime --
/// which is NOW. This is the CRASH shape, not the ordinary one, and the distinction has since
/// earned its keep: the collector quarantines through `set_slab_state`, which stamps a descriptor,
/// so a quarantine the collector built is fully described. A quarantine with files the manifest
/// never learned about is what a process that died between the renames and the manifest persist
/// leaves behind. `quarantine_fixture_the_collector_shape` is the other one, and a round can
/// decline to walk only the second.
fn quarantine_fixture(root: &std::path::Path, slabs: u64) {
    quarantine_files_from(root, 0, slabs);
}

/// `slabs` quarantined files starting at `first_id`.
///
/// THE FIRST ID IS NOT COSMETIC once a store is opened on top of the directory. Slab 0 is the id
/// a fresh store takes for its own ACTIVE slab, and the open-time reconcile will not let a
/// folded lifecycle state overwrite the active slab's -- so a fixture that starts at 0 comes back
/// one descriptor short of its own file count, which reads as the adoption having half failed. A
/// fixture that needs every file described starts at 1.
fn quarantine_files_from(root: &std::path::Path, first_id: u64, slabs: u64) {
    let trash = delayed_destroy_dir(root);
    fs::create_dir_all(&trash).unwrap();
    let mut id = first_id;
    while id < first_id + slabs {
        fs::write(
            trash.join(format!("block_segment_{id:020}.seg.deleted.{id}")),
            b"slab",
        )
        .unwrap();
        id += 1;
    }
}

/// Every entry the round NAMED, which for a quarantine of parseable names and no caller list is
/// every entry in the directory. The four lists are disjoint and together they are the loop body's
/// only exits, so this is a count of iterations rather than an estimate of one.
///
/// IT IS A SUM OF CLASSIFICATIONS, WHICH IS NOT THE SAME THING AS A COUNT OF ITERATIONS, and the
/// sentence above holds only while every iteration ends in one of the four. Turning the budget's
/// `break` into a `continue` breaks exactly that: the entries walked past after the budget is
/// spent are classified as nothing and this sum cannot see them. `examined_block_slabs` on the
/// report is the real count and is what the guards below assert; this stays because the older
/// measurements are quoted against it and the two agreeing is itself worth asserting.
fn entries_examined(report: &BlockStorePurgeDelayedDestroyReport) -> usize {
    report.purged_block_slab_ids.len()
        + report.restored_block_slab_ids.len()
        + report.restore_blocked_block_slab_ids.len()
        + report.retained_too_young_block_slab_ids.len()
}

/// The quarantine THE COLLECTOR leaves: every file in it described by a manifest descriptor whose
/// `updated_unix_ms` records when the slab was set aside.
///
/// Built by writing the files and opening the store AFTERWARDS, so the open-time reconcile adopts
/// each one -- the same route a restarted process takes, and far cheaper at eighty thousand slabs
/// than installing and collecting each. The caller must assert the denominator the fixture returns
/// before reading anything else: a store whose descriptors did not get adopted would let a purge
/// round walk for reasons that have nothing to do with what is being measured.
fn quarantine_fixture_the_collector_shape(root: &std::path::Path, slabs: u64) -> BlockStore {
    quarantine_files_from(root, 1, slabs);
    BlockStore::new(root)
}

/// How many quarantined slabs this store has a stamped descriptor for.
fn quarantined_with_a_stamp(store: &BlockStore) -> usize {
    store
        .slab_descriptors()
        .into_iter()
        .filter(|slab| {
            slab.state == BlockStoreSlabState::DelayedDestroy && slab.updated_unix_ms.is_some()
        })
        .count()
}

/// THE CAP BOUNDS A ROUND THAT IS DOING WORK. IT DOES NOT BOUND A ROUND IN THE QUARANTINE WINDOW.
///
/// `purge_delayed_destroy_slabs_capped` spends its budget on work done -- a destroy or a restore
/// -- and never on an entry it passes over. That is deliberate and it is what stops a blocked prefix
/// starving the drain for ever. It has a consequence that has not been measured: a slab that is
/// TOO YOUNG is also a skip, so it charges nothing either, so `processed` never reaches the
/// budget, so the `break` that makes a capped round cost the budget rather than the directory
/// never fires. The round walks every entry.
///
/// That is not a corner. A quarantined slab waits `DELAYED_DESTROY_MIN_AGE_MS` -- an hour -- and
/// for that whole hour EVERY entry is too young, so every round in the window walks the entire
/// directory, stats every file, and does nothing. The cap engages only once the slabs mature.
///
/// Measured at two sizes, ten times apart, as a COUNT of loop iterations rather than a duration:
///
///   quarantine     examined, in-window     examined, actionable
///      8,000                    8,000                    1,000
///     80,000                   80,000                    1,000
///   ratio                      10.00x                    1.00x
///
/// Same function, same budget, same fixture; the only difference is whether the entries are old
/// enough to act on. Run it:
///
///   cargo test -p temporalstore-rust --lib what_a_purge_round_examines \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_a_purge_round_examines_at_two_quarantine_sizes() {
    let mut in_window = Vec::new();
    let mut actionable = Vec::new();
    let sizes = [8_000u64, 80_000];

    let mut index = 0usize;
    while index < sizes.len() {
        let slabs = sizes[index];

        // HALF ONE: the shipped age, on a quarantine that has just been filled.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        quarantine_fixture(dir.path(), slabs);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "DENOMINATOR: the in-window round must really face {slabs} quarantined slabs"
        );
        let started = std::time::Instant::now();
        let report = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let window_ms = started.elapsed().as_secs_f64() * 1e3;
        let window_examined = entries_examined(&report);
        assert_eq!(
            report.processed_block_slabs, 0,
            "NON-VACUOUS ZERO: every entry is too young, so the round did no work at all -- if \
             this is non-zero the fixture matured and the rest of the reading is meaningless: \
             {report:?}"
        );
        assert!(
            !report.budget_exhausted,
            "and the budget was never spent, so the cap's break never fired: {report:?}"
        );
        assert_eq!(
            window_examined as u64, slabs,
            "the round walked every entry in the directory"
        );
        in_window.push((slabs, window_examined, window_ms));

        // HALF TWO: the same directory, the same budget, entries that are old enough to act on.
        // Ordered second so a mutant that kills the first half does not prevent this one running.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        quarantine_fixture(dir.path(), slabs);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "DENOMINATOR: the actionable round must face the same {slabs} slabs"
        );
        let started = std::time::Instant::now();
        let report = store
            .purge_delayed_destroy_slabs_capped(
                0,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let actionable_ms = started.elapsed().as_secs_f64() * 1e3;
        let actionable_examined = entries_examined(&report);
        assert_eq!(
            report.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            "the actionable round spends its whole budget: {report:?}"
        );
        assert!(
            report.budget_exhausted,
            "and says there is more to drain: {report:?}"
        );
        assert_eq!(
            report.retained_too_young_block_slab_ids.len(),
            0,
            "NON-VACUOUS ZERO the other way: nothing was held back for age here"
        );
        actionable.push((slabs, actionable_examined, actionable_ms));

        index += 1;
    }

    println!();
    println!("  ONE PURGE ROUND, budget {DELAYED_DESTROY_MAX_SLABS_PER_ROUND}");
    println!(
        "  {:>10}  {:>22}  {:>22}",
        "quarantine", "examined, in-window", "examined, actionable"
    );
    let mut row = 0usize;
    while row < sizes.len() {
        println!(
            "  {:>10}  {:>13} {:>7.1} ms  {:>13} {:>7.1} ms",
            in_window[row].0,
            in_window[row].1,
            in_window[row].2,
            actionable[row].1,
            actionable[row].2,
        );
        row += 1;
    }
    println!(
        "  ratio      {:>13.2}x {:>18.2}x",
        in_window[1].1 as f64 / in_window[0].1 as f64,
        actionable[1].1 as f64 / actionable[0].1 as f64,
    );
    println!();

    assert_eq!(
        in_window[1].1 / in_window[0].1,
        10,
        "the in-window round is LINEAR in the quarantine: ten times the directory, ten times the \
         walk"
    );
    assert_eq!(
        actionable[0].1, actionable[1].1,
        "while the actionable round is FLAT at the budget, which is what the cap was for"
    );
}

/// The same property, small enough for CI, asserted as two separate halves.
///
/// The guard is that the two rounds below -- identical but for whether their entries are old
/// enough -- examine DIFFERENT numbers of entries, and that the in-window one examines the whole
/// directory rather than the budget. A budget charged for the entries it passes over would make both halves equal
/// and is exactly the change `purge_delayed_destroy_slabs_capped` forbids, so this cannot be
/// satisfied by making the cap tighter.
#[test]
fn a_purge_round_inside_the_quarantine_window_is_not_bounded_by_its_budget() {
    let slabs = 4_000u64;
    let budget = 1_000usize;
    assert!(
        (slabs as usize) > budget * 2,
        "DENOMINATOR: the directory must be several budgets deep or the two halves cannot differ"
    );

    // HALF ONE: in the window. Nothing is actionable, so the cap cannot engage.
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are really in quarantine"
    );
    let window = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();
    assert_eq!(
        window.processed_block_slabs, 0,
        "NON-VACUOUS ZERO: no entry was old enough, so no budget was charged: {window:?}"
    );
    assert_eq!(
        entries_examined(&window) as u64,
        slabs,
        "so the round examined the WHOLE directory, not its budget: {window:?}"
    );
    assert!(
        !window.budget_exhausted,
        "and reports no budget exhaustion, so a draining caller stops here: {window:?}"
    );

    // HALF TWO: actionable. Same directory size, same budget, and now the cap does bound it.
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: the actionable half faces the same {slabs} slabs"
    );
    let actionable = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, budget)
        .unwrap();
    assert_eq!(
        actionable.processed_block_slabs, budget,
        "the actionable round spends its whole budget: {actionable:?}"
    );
    assert_eq!(
        entries_examined(&actionable),
        budget,
        "and examines the budget rather than the directory: {actionable:?}"
    );
    assert!(
        actionable.budget_exhausted,
        "and says there is more: {actionable:?}"
    );

    // THE DIFFERENCE, stated on its own so neither half can carry it alone.
    assert!(
        entries_examined(&window) > entries_examined(&actionable) * 3,
        "a round in the quarantine window walks the directory ({}) while an actionable round \
         walks its budget ({})",
        entries_examined(&window),
        entries_examined(&actionable),
    );
}

/// How many slabs the strace probes below run against. Literal default so the probe is
/// reproducible without the variable; the driver sets it to take the second reading.
fn probe_slabs() -> u64 {
    std::env::var("RECLAIM_SCALE_SLABS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(8_000)
}

/// THE COLLECTOR HALF, for `strace -y -e trace=fsync,fdatasync` to count by PATH.
///
/// Install, then one quarantine round. The fsyncs of the store root and of the trash directory
/// are the quantity: the loop used to issue one of each PER SLAB, and they were hoisted to one
/// pair per round. Counting them by the path strace prints separates them from the manifest's own
/// and from the per-slab install fsyncs, which no change here touches.
///
///   RECLAIM_SCALE_SLABS=8000 strace -f -y -e trace=fsync,fdatasync -o /tmp/q8 \
///       ./temporalstore_rust-<hash> probe_one_quarantine_round --exact --ignored --nocapture
#[test]
#[ignore]
fn probe_one_quarantine_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let mut id = 0u64;
    while id < slabs {
        store.install_slab(id, b"slab-contents").unwrap();
        id += 1;
    }
    assert_eq!(
        store.slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are installed before the quarantine round"
    );
    println!("PROBE-MARK install-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.delayed_destroy_block_slab_ids.len() as u64,
        slabs - 1,
        "the round must really have quarantined {} slabs, or the fsync count is of nothing",
        slabs - 1
    );
    println!(
        "PROBE-MARK quarantine-done slabs={} quarantined={} ms={ms:.1}",
        slabs,
        report.delayed_destroy_block_slab_ids.len()
    );
}
