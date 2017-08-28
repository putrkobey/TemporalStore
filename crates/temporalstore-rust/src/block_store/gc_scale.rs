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

/// THE PURGE HALF, in the quarantine window, for the same strace count.
///
/// A round that destroys and restores nothing must issue NO fsync at all -- there is no rename
/// and no unlink for one to commit. That zero is the one worth asserting, and it is non-vacuous
/// because the round really did walk every entry: `retained_too_young` names all of them.
#[test]
#[ignore]
fn probe_one_in_window_purge_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    println!("PROBE-MARK fixture-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        slabs,
        "DENOMINATOR: the round examined every one of the {slabs} entries"
    );
    println!(
        "PROBE-MARK in-window-purge-done slabs={slabs} examined={} ms={ms:.1}",
        entries_examined(&report)
    );
}

/// THE PURGE HALF, actionable, for the same strace count.
#[test]
#[ignore]
fn probe_one_actionable_purge_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    println!("PROBE-MARK fixture-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .purge_delayed_destroy_slabs_capped(
            0,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        "DENOMINATOR: the round really spent its whole budget"
    );
    println!(
        "PROBE-MARK actionable-purge-done slabs={slabs} destroyed={} ms={ms:.1}",
        report.purged_block_slab_ids.len()
    );
}

/// THE STORE-WIDE LOCK SPANS THE WHOLE WALK, counted rather than timed.
///
/// `purge_delayed_destroy_slabs_capped` takes `self.inner` before it opens the trash directory
/// and holds it until it returns, so every entry the round examines is examined under the lock
/// that every read of the block store also needs. In the actionable case the cap bounds how long
/// that is. In the quarantine window nothing bounds it.
///
/// The count is of PROBE READS COMPLETED BY A SECOND THREAD while the round runs -- each one a
/// `slab_ids()`, which takes the same lock. Under the round it is zero of the attempted set; the
/// control, an identical probe against an empty quarantine, completes all of them. Reporting both
/// is what makes the zero mean "blocked" rather than "the probe never ran".
#[test]
#[ignore]
fn the_store_lock_is_held_across_the_whole_in_window_walk() {
    let slabs = 80_000u64;
    let probes = 200usize;

    // CONTROL FIRST, so a mutant that stops the first half from finishing still leaves this
    // reading on the record: with no round in the way every probe read completes.
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(BlockStore::new(dir.path()));
    let mut control_completed = 0usize;
    let mut probe = 0usize;
    while probe < probes {
        store.slab_ids().unwrap();
        control_completed += 1;
        probe += 1;
    }
    assert_eq!(
        control_completed, probes,
        "DENOMINATOR: the probe read itself works {probes} times out of {probes}"
    );

    // AND NOW THE SAME PROBE AGAINST A ROUND THAT IS WALKING EIGHTY THOUSAND ENTRIES.
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(BlockStore::new(dir.path()));
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} entries are really in the directory the round will walk"
    );

    let round_store = std::sync::Arc::clone(&store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let round_barrier = std::sync::Arc::clone(&barrier);
    let round = std::thread::spawn(move || {
        round_barrier.wait();
        let started = std::time::Instant::now();
        let report = round_store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        (report, started.elapsed())
    });

    barrier.wait();
    // Give the round time to be inside the walk rather than still entering the function. The
    // walk is hundreds of milliseconds; this is a small fraction of it.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let probe_started = std::time::Instant::now();
    store.slab_ids().unwrap();
    let blocked_for = probe_started.elapsed();

    let (report, round_took) = round.join().unwrap();
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        slabs,
        "the round really walked every entry"
    );
    assert_eq!(
        report.processed_block_slabs, 0,
        "NON-VACUOUS ZERO: and did no work while it held the lock"
    );

    println!();
    println!(
        "  one in-window round over {slabs} entries took {:.1} ms; a probe read issued 50 ms in \
         waited {:.1} ms for the lock ({control_completed}/{probes} of the same read complete \
         instantly with no round running)",
        round_took.as_secs_f64() * 1e3,
        blocked_for.as_secs_f64() * 1e3,
    );
    println!();

    assert!(
        blocked_for.as_millis() >= 50,
        "a read issued while the round walks must wait for it: waited only {:.1} ms of a {:.1} ms \
         round",
        blocked_for.as_secs_f64() * 1e3,
        round_took.as_secs_f64() * 1e3,
    );
}

/// A SLAB THAT IS TOO YOUNG STAYS IN FRONT OF THE LOOP AND DOES NOT STARVE THE ONES BEHIND IT.
///
/// This is the #1719 property re-asked for the one skip reason that fires in the ordinary state.
/// A quarantine that mixes matured slabs with fresh ones must destroy every matured slab the
/// budget allows in a single round, wherever directory order happens to put the fresh ones -- the
/// budget is spent on work, so walking past a fresh entry costs the round a step and never a unit
/// of its budget. The cost of those steps is what
/// `a_purge_round_inside_the_quarantine_window_is_not_bounded_by_its_budget` measures; that the
/// drain still ADVANCES is this one.
#[test]
fn a_matured_slab_behind_a_fresh_one_is_still_destroyed_this_round() {
    let matured = 300u64;
    let fresh = 1_200u64;
    let budget = 1_000usize;
    assert!(
        (matured as usize) < budget,
        "DENOMINATOR: the budget must be able to take every matured slab in one round, or a \
         shortfall would mean the cap and not a starving skip"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());

    // The matured half goes through the real collector, so each slab gets a descriptor whose
    // stamp can be moved into the past.
    let mut id = 0u64;
    while id <= matured {
        store.install_slab(id, b"slab-contents").unwrap();
        id += 1;
    }
    let quarantined = store
        .gc_slabs_before_with_live_refs_delayed_destroy(matured, [matured])
        .unwrap()
        .delayed_destroy_block_slab_ids
        .len();
    assert_eq!(
        quarantined as u64, matured,
        "DENOMINATOR: the collector really set aside {matured} slabs"
    );
    let moved = store
        .backdate_delayed_destroy_stamps_for_test(2 * DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert_eq!(
        moved as u64, matured,
        "DENOMINATOR: and every one of them was backdated past the grace window"
    );

    // The fresh half is written straight into the trash directory with no descriptor, at ids far
    // above the matured ones, so the age falls back to a mtime of NOW.
    let trash = delayed_destroy_dir(dir.path());
    let mut id = 1_000_000u64;
    while id < 1_000_000 + fresh {
        fs::write(
            trash.join(format!("block_segment_{id:020}.seg.deleted.{id}")),
            b"slab",
        )
        .unwrap();
        id += 1;
    }
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        matured + fresh,
        "DENOMINATOR: the directory holds both halves"
    );

    let report = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();

    // HALF ONE: every matured slab was destroyed, in this one round.
    assert_eq!(
        report.purged_block_slab_ids.len() as u64,
        matured,
        "every matured slab must be destroyed this round however many fresh ones sit in front of \
         it in directory order: {report:?}"
    );
    // HALF TWO: and exactly the fresh ones were held, so none of them was charged for.
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        fresh,
        "and exactly the fresh half was held back for age: {report:?}"
    );
    assert_eq!(
        report.processed_block_slabs as u64, matured,
        "budget is spent on work done, so the {fresh} skipped entries cost none of it: {report:?}"
    );
}

/// THE SECOND ROUND IN THE WINDOW DOES NOT OPEN THE DIRECTORY.
///
/// The first one has to: nothing in a fresh process knows what is in quarantine, and finding out
/// is the walk. What it also does, on its way past every entry, is note the earliest moment any
/// of them arrived -- and from that the next round can answer the only question it had without
/// looking, because if the oldest slab in there is not old enough then no slab in there is.
///
/// Both rounds are run BEFORE either is asserted about. The second round is the claim and it
/// depends on the first having happened, so it cannot be moved in front of it; running them both
/// first is the next best thing, and it means a mutant that breaks the first round's reading
/// still leaves the second round's reading on the record.
#[test]
fn a_second_round_inside_the_window_declines_without_opening_the_directory() {
    let slabs = 4_000u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: every one of the {slabs} quarantined slabs must be described and stamped, \
         or a round would be walking for a reason this test is not about"
    );

    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let second = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let still_quarantined = store.delayed_destroy_slab_ids().unwrap().len();

    // HALF ONE: the first round pays the full walk, which is what there is to remove.
    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "the first round walks every entry: {first:?}"
    );
    assert_eq!(
        first.processed_block_slabs, 0,
        "NON-VACUOUS: and does no work at all while doing it, so the whole walk is waste: {first:?}"
    );
    assert!(
        !first.declined_without_examining,
        "and it cannot have declined -- a fresh store knows nothing yet: {first:?}"
    );

    // HALF TWO: the second round pays nothing, and the quarantine it declined to walk is still
    // there. A zero examined count against an empty directory would mean nothing.
    assert_eq!(
        still_quarantined as u64, slabs,
        "NON-VACUOUS ZERO: all {slabs} slabs are still in quarantine after both rounds"
    );
    assert!(
        second.declined_without_examining,
        "so the second round declines outright: {second:?}"
    );
    assert_eq!(
        second.examined_block_slabs, 0,
        "and examines nothing to do it: {second:?}"
    );
    assert_eq!(
        second.processed_block_slabs, 0,
        "having done, as the first round also did, no work: {second:?}"
    );
}

/// A LIVE SLAB IN QUARANTINE IS STILL RESCUED, AND THE DECLINE IS WHAT HAD TO ASK.
///
/// The liveness re-check restores a quarantined slab the caller still calls live, and it does so
/// at ANY age -- it runs before the age is consulted, because a live slab is not too young to
/// destroy, it is not for destroying. A round that declines on age alone would defer that restore
/// by up to the whole grace window, and the restore is the repair for a reader that cannot reach
/// its slab. So the decline asks first.
///
/// The third round is the control and it is what makes the second one mean something: same store,
/// same moment, same everything but the live set, and it declines. Without it a second round that
/// walked would be indistinguishable from a decline that never became available.
#[test]
fn a_live_quarantined_slab_is_restored_by_a_round_that_could_otherwise_have_declined() {
    let slabs = 2_000u64;
    let rescued = 7u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );

    // Round one earns the decline for every round after it.
    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    // Round two is offered the decline and must refuse it, because the caller names a live slab
    // that is sitting in the trash directory.
    let with_a_live_slab = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            [rescued],
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    // Round three is the control: nothing live, and the decline is taken.
    let control = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();

    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the first round really walked the directory: {first:?}"
    );

    // HALF ONE: the live slab came back.
    assert_eq!(
        with_a_live_slab.restored_block_slab_ids,
        vec![rescued],
        "the round the caller told about a live quarantined slab must restore it rather than \
         decline on age: {with_a_live_slab:?}"
    );
    assert!(
        !with_a_live_slab.declined_without_examining,
        "which means it did not decline: {with_a_live_slab:?}"
    );
    assert_eq!(
        with_a_live_slab.examined_block_slabs as u64, slabs,
        "and paid for the walk that found it: {with_a_live_slab:?}"
    );

    // HALF TWO, THE CONTROL: the decline was there to be taken, and only the live set withheld it.
    assert!(
        control.declined_without_examining,
        "the same round with nothing live declines, so it was the live slab and not the absence \
         of a decline that made the round above walk: {control:?}"
    );
    assert_eq!(
        control.examined_block_slabs, 0,
        "examining nothing: {control:?}"
    );
}

/// AN ENTRY THE MANIFEST DOES NOT KNOW ABOUT STOPS THE ROUND DECLINING, FOR EVER IF NEED BE.
///
/// A file in quarantine with no descriptor is what a crash between the collector's renames and
/// its manifest persist leaves behind, and it is the one entry whose liveness the decline cannot
/// test -- the test reads slab state out of the manifest, and there is no entry there to read.
/// Rather than decline on an incomplete picture, a round that meets one withdraws its opinion and
/// the next round walks. That is this fix declining to help in the one shape where helping would
/// mean guessing, and it is worth a guard of its own because the alternative fails SILENTLY: a
/// deferred restore looks exactly like no restore being needed.
#[test]
fn a_quarantined_file_with_no_descriptor_keeps_every_round_walking() {
    let slabs = 1_000u64;
    let dir = tempfile::tempdir().unwrap();
    // The store is opened FIRST and the files are written behind its back, so nothing reconciles
    // them and they stay undescribed.
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} files really are in the trash directory"
    );
    assert_eq!(
        quarantined_with_a_stamp(&store),
        0,
        "DENOMINATOR, the other way: and NONE of them is described, which is the whole premise"
    );

    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let second = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();

    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "the first round walks: {first:?}"
    );
    assert!(
        !second.declined_without_examining,
        "and so does the second -- an undescribed entry withdraws the round's opinion rather than \
         letting it decline on a picture with a hole in it: {second:?}"
    );
    assert_eq!(
        second.examined_block_slabs as u64, slabs,
        "which costs the whole directory again: {second:?}"
    );
}

/// THE BUDGET'S `break` COSTS THE BUDGET AND NOT THE DIRECTORY, ASSERTED ON THE COUNT THAT MOVES.
///
/// Until `examined_block_slabs` existed nothing in the crate could see this. The budget check
/// stops the round; turning that `break` into a `continue` leaves a round that walks every
/// remaining entry doing nothing -- and reports the same four lists, the same
/// `processed_block_slabs`, the same `budget_exhausted`, and passes. The directory is four
/// budgets deep here so the two readings cannot be confused for each other.
#[test]
fn a_round_that_stops_on_its_budget_examines_the_budget_and_not_the_directory() {
    let slabs = 4_000u64;
    let budget = 1_000usize;
    assert!(
        slabs as usize >= budget * 4,
        "DENOMINATOR: the directory must be several budgets deep or the two readings coincide"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are in quarantine"
    );

    // Actionable, so every entry the round reaches charges the budget and the cap engages.
    let report = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, budget)
        .unwrap();

    // HALF ONE: the round did a budget's worth of work and said there was more.
    assert_eq!(
        report.processed_block_slabs, budget,
        "the round spends its whole budget: {report:?}"
    );
    assert!(
        report.budget_exhausted,
        "and says the drain continues: {report:?}"
    );
    // HALF TWO: and it stopped rather than walking on. This is the half that pins the `break`,
    // and it is stated second so a mutant that breaks the first does not keep it from running.
    assert_eq!(
        report.examined_block_slabs,
        budget + 1,
        "a round that stops on its budget must EXAMINE its budget and the one entry that found \
         the budget already spent -- walking the remaining {} entries to classify none of them \
         would report every other number above unchanged: {report:?}",
        slabs as usize - budget - 1,
    );
    assert_eq!(
        entries_examined(&report) + 1,
        report.examined_block_slabs,
        "and that ONE is the whole difference between what the round looked at and what it \
         reached a decision about, which is how a stopped round is told from a walked one: \
         {report:?}"
    );
}

/// A ROUND THAT STOPPED ON ITS BUDGET IS NOT ENTITLED TO AN OPINION, SO THE NEXT ONE WALKS.
///
/// The entries behind the `break` are precisely the ones the round knows nothing about, and an
/// earliest arrival that does not cover them is an earliest arrival that could be wrong in the
/// one direction this cache must never be wrong in. So a capped-out round publishes nothing.
#[test]
fn a_round_that_stopped_on_its_budget_leaves_the_next_round_to_walk() {
    let matured = 1_500u64;
    let fresh = 1_000u64;
    let budget = 500usize;
    assert!(
        (matured as usize) > budget,
        "DENOMINATOR: the matured half must outlast the budget or the round would not stop early"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), matured);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        matured,
        "DENOMINATOR: {matured} described slabs to age"
    );
    let moved = store
        .backdate_delayed_destroy_stamps_for_test(2 * DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert_eq!(
        moved as u64, matured,
        "DENOMINATOR: and every one of them moved past the window"
    );
    // A fresh, described half on top, so the directory still holds in-window entries after the
    // matured ones are gone.
    let trash = delayed_destroy_dir(dir.path());
    let mut id = 1_000_000u64;
    while id < 1_000_000 + fresh {
        fs::write(
            trash.join(format!("block_segment_{id:020}.seg.deleted.{id}")),
            b"slab",
        )
        .unwrap();
        id += 1;
    }

    let stopped = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();
    let after = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();

    // HALF ONE: the first round really did stop early.
    assert!(
        stopped.budget_exhausted,
        "the first round must stop on its budget: {stopped:?}"
    );
    assert_eq!(
        stopped.purged_block_slab_ids.len(),
        budget,
        "having destroyed exactly a budget's worth: {stopped:?}"
    );
    // HALF TWO: and left nothing behind for the next round to decline on.
    assert!(
        !after.declined_without_examining,
        "so the round after it must walk -- the entries behind the break were never seen and an \
         earliest arrival that does not cover them cannot be published: {after:?}"
    );
    assert!(
        after.examined_block_slabs > 0,
        "which means it examined entries: {after:?}"
    );
}

/// A CALLER THAT ASKS FOR AN IMMEDIATE PURGE ALWAYS WALKS.
///
/// `min_age_ms == 0` makes the decline's first condition -- `now - earliest < min_age_ms` --
/// false whatever the clock says and whatever is cached, which is why every test written against
/// this function before the cache existed is unaffected by it. Worth a guard rather than a
/// remark: the arithmetic that makes it true is one `<` and a mutant can turn it into a `<=`.
#[test]
fn an_immediate_purge_walks_however_recently_the_round_before_it_declined() {
    let slabs = 1_500u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );

    let warm = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let declined = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let immediate = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, 0)
        .unwrap();

    // HALF ONE: the decline really was in force at the moment the immediate purge ran.
    assert_eq!(
        warm.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the warming round walked: {warm:?}"
    );
    assert!(
        declined.declined_without_examining,
        "NON-VACUOUS: and the round after it declined, so the cache was live: {declined:?}"
    );
    // HALF TWO: and the immediate purge went through it anyway, and destroyed everything.
    assert!(
        !immediate.declined_without_examining,
        "a purge asked for at zero age must never decline: {immediate:?}"
    );
    assert_eq!(
        immediate.purged_block_slab_ids.len() as u64,
        slabs,
        "and must destroy every quarantined slab: {immediate:?}"
    );
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len(),
        0,
        "leaving the trash directory empty"
    );
}

/// WHAT THE DECLINE REMOVES FROM AN IN-WINDOW ROUND, at two quarantine sizes ten times apart.
///
/// The first round in a fresh process walks whatever the quarantine holds; every round after it,
/// for the rest of the hour, examines nothing. Against an hourly grace window and a periodic
/// cycle measured in seconds that is almost all of them.
///
/// Counted, not timed, for the reason the sibling measurement above gives: the count is the claim
/// and a duration only agrees with it.
///
///   quarantine    examined, round 1    examined, round 2    examined, actionable
///      8,000                 8,000                    0                   1,000
///     80,000                80,000                    0                   1,000
///
/// Run it:
///
///   cargo test -p temporalstore-rust --lib what_the_decline_removes \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_the_decline_removes_from_an_in_window_round_at_two_quarantine_sizes() {
    let mut rows = Vec::new();
    let sizes = [8_000u64, 80_000];

    let mut index = 0usize;
    while index < sizes.len() {
        let slabs = sizes[index];
        let dir = tempfile::tempdir().unwrap();
        let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
        assert_eq!(
            quarantined_with_a_stamp(&store) as u64,
            slabs,
            "DENOMINATOR: the round must face {slabs} described, quarantined slabs"
        );

        let started = std::time::Instant::now();
        let first = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let first_ms = started.elapsed().as_secs_f64() * 1e3;
        let started = std::time::Instant::now();
        let second = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let second_ms = started.elapsed().as_secs_f64() * 1e3;

