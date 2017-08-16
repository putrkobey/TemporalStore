// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one raft snapshot COSTS at scale, counted inside the primitives that do the work.
//!
//! A snapshot here is a STATE IMAGE: the shard's served index plus every slab it references,
//! materialised as owned bytes. Its size is set by the STORE, not by the request, which makes the
//! interesting questions about it questions of multiplicity -- how many times is the corpus
//! walked, how many copies of the shard exist at once, and which of those happen while the raft
//! cluster write guard is held. That guard is the half of the lock every `propose` needs, so work
//! under it is not merely slow, it is unavailable.
//!
//! COUNTED, never timed. The same test on this box varies about 2.4x in wall time across one day
//! and the box sits anywhere between load 1 and 30; the quantities below -- walks, copies, reads,
//! rebuilds -- do not move with load at all. Everything runs at TWO corpus sizes and the RATIO is
//! asserted with both denominators printed, so a counter that has quietly stopped incrementing
//! reads as a failure and not as a perfect result.
//!
//! The in-order trace of one `maybe_trigger_snapshot` on a 3-node in-process cluster, at the two
//! sizes `SMALL` and `LARGE` below actually run -- 1,000 and 4,000 records of 128 bytes:
//!
//! ```text
//!                                       1,000        4,000     ratio   per record
//!   BUILD, holding no cluster guard
//!     state image builds                    1            1      1.00
//!     corpus walks for live slab ids        1            1      1.00
//!     block addresses visited           1,000        4,000      4.00        1.000
//!     served-index encodes                  1            1      1.00
//!     index bytes                      48,552      215,013      4.43
//!     slab directory listings               1            1      1.00
//!     slab reads                            4            4      1.00
//!     slab bytes read                 140,000      560,000      4.00          140
//!     encodes under a shard guard           0            0         -
//!   INSTALL
//!     engine rebuilds                       3            3      1.00
//!     slab writes                          12           12      1.00
//!     slab bytes re-installed         420,000    1,680,000      4.00          420
//!     whole-shard image copies              2            2      1.00
//!     image bytes copied              377,104    1,550,026      4.11
//!     ... of the above, UNDER THE CLUSTER WRITE GUARD
//!     engine rebuilds                       0            0         -
//!     slab bytes re-installed               0            0         -
//!     image bytes copied                    0            0         -
//! ```
//!
//! The build is one pass: one walk, one encode, one listing, one read per slab, nothing under any
//! guard. The INSTALL used to be all of it under the guard -- three whole engine rebuilds and
//! 420,000 / 1,680,000 slab bytes, 100% at both sizes. An engine rebuild is a pure function of the
//! snapshot, so it is built before the guard is taken; what is left under the guard is the
//! bookkeeping, and the eligibility check that has to be atomic with it. Which nodes install is
//! unchanged. One copy of the image was pure waste too -- the snapshot is dropped the moment the
//! loop ends, so the last install can have the original -- and in a DEPLOYED process, which
//! installs into exactly one node, that one was the only copy there was.

use super::*;
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records per corpus. The pair is a 4.00x step, small enough that the whole file runs inside the
/// ordinary gate on a loaded box and large enough that a per-record identity is worth three
/// significant figures.
const SMALL: usize = 1_000;
const LARGE: usize = 4_000;

/// How many slabs the corpus is spread over.
///
/// The slab target is a gibibyte, so a corpus any test can afford to write lands entirely in slab
/// zero -- and then the live set is `{0}`, the store's set is `{0}`, "one read per slab" is "one
/// read", and every per-slab multiplicity below is indistinguishable from a per-snapshot one. The
/// records are rolled onto fresh slabs as they are written so the image genuinely spans several,
/// and `assert_the_fixture_is_populated` refuses to measure a fixture that collapsed back to one.
pub(super) const SLABS: usize = 4;

/// A leader with `records` committed and applied, each a 128-byte value under a 12-byte key,
/// spread over `SLABS` slabs.
pub(super) fn cluster_with(records: usize) -> RaftCluster {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let roll_every = (records / SLABS).max(1);
    let mut index = 0usize;
    while index < records {
        if index > 0 && index % roll_every == 0 {
            // Target 1 byte: the production target is a gibibyte and is reachable only through a
            // process-wide env var, which is exactly what `prepare_next_slab_with_target` exists
            // to avoid mutating. `Some` is required, so a roll that silently did not happen fails
            // here rather than showing up as a one-slab image three assertions later.
            assert!(
                cluster
                    .node_engine_for_test(1)
                    .expect("the leader serves an engine")
                    .block_store()
                    .prepare_next_slab_with_target(1)
                    .expect("rolling onto a fresh slab must succeed")
                    .is_some(),
                "the slab did not roll at record {index}, so the corpus would land in one slab"
            );
        }
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
        index += 1;
    }
    cluster
}

/// Make the compaction threshold fire, and stop the catch-up hold from pre-empting it.
///
/// Both are deliberate production behaviours that would otherwise make this fixture measure
/// "nothing happened": the byte threshold is a gibibyte, and the retained-bytes ceiling holds
/// compaction while a live peer is still behind.
pub(super) fn force_the_threshold(cluster: &RaftCluster) {
    let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
    inner.config.max_applied_log_bytes = 1;
    inner.config.max_retained_log_bytes = 0;
    // And pin the FIXED cadence, which is what every measurement in this file and in
    // `snapshot_large_store` was taken under: a constant byte threshold, unrelated to the image
    // the snapshot will read. `snapshot_image_fraction_divisor` raises that threshold in
    // proportion to the last image, which is exactly the thing those files RECORD as absent --
    // leaving it at its shipped default would stop the second snapshot in every INCREMENT arm
    // from firing at all, and a recording of a defect must go on measuring the defect. The
    // relative cadence is measured in `snapshot_cadence`, against these same arms.
    inner.config.snapshot_image_fraction_divisor = 0;
}

/// Everything the snapshot path is about to be measured against, asserted NON-EMPTY.
///
/// The most expensive mistake available here is measuring a structure nothing filled: an image
/// with no slabs walks no slabs, and a cost that never occurred reads exactly like a low one. So
/// every denominator the rows below divide by is checked here, against the same cluster the
/// measurement runs on, and printed.
pub(super) fn assert_the_fixture_is_populated(cluster: &RaftCluster, records: usize) -> Fixture {
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test; an entry snapshot measures nothing");
    let payload = image.payload_bytes();
    println!("  fixture records        = {records}");
    println!("  fixture image bytes    = {payload}");
    println!("  fixture index bytes    = {}", image.index_bytes.len());
    println!("  fixture image slabs    = {}", image.slabs.len());
    assert!(
        image.index_bytes.len() > 1_024,
        "served index is {} bytes: nothing was indexed, so an index encode measures nothing",
        image.index_bytes.len()
    );
    assert!(
        image.slabs.len() > 1,
        "the image carries {} slab(s). With zero, every slab read, copy and re-install below is a \
         count of zero that would pass any bound put on it; with one, a per-slab cost and a \
         per-snapshot cost are the same number and neither can be told from the other",
        image.slabs.len()
    );
    let slab_bytes = image
        .slabs
        .iter()
        .map(|slab| slab.bytes.len())
        .sum::<usize>();
    assert!(
        slab_bytes > 0,
        "the image's slabs are all empty: {} slabs, 0 bytes",
        image.slabs.len()
    );

    // The scope of the image is the shard's LIVE slab set intersected with what the store holds.
    // An EMPTY live set is not an empty image -- it is the fallback that carries the whole store
    // -- so a fixture whose live set is empty is measuring the other branch entirely.
    let engine = cluster
        .node_engine_for_test(1)
        .expect("the leader serves an engine");
    let live = engine.live_block_slab_ids(1);
    println!("  fixture live slab ids  = {live:?}");
    assert!(
        live.len() > 1,
        "the shard's live slab set is {live:?}. Empty means the image took the carry-the-whole-\
         store fallback and none of the scoping being measured happened; a single id means the \
         set cannot distinguish a walk of the live set from a walk of the store"
    );

    // And the node set the install loop walks. One alive node makes "copies per node" and "copies"
    // the same number and the multiplicity being measured invisible.
    let alive = cluster
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .values()
        .filter(|node| node.alive)
        .count();
    println!("  fixture alive nodes    = {alive}");
    assert!(
        alive > 1,
        "only {alive} alive node: a per-node cost cannot be distinguished from a per-snapshot one"
    );
    Fixture {
        payload_bytes: payload,
        slabs: image.slabs.len(),
        alive_nodes: alive,
    }
}

/// The denominators one measurement divides by, read off the fixture it is about to run on.
#[derive(Debug, Clone, Copy)]
pub(super) struct Fixture {
    pub(super) payload_bytes: usize,
    pub(super) slabs: usize,
    pub(super) alive_nodes: usize,
}

pub(super) fn print_counts(label: &str, counts: &SnapshotCounts) {
    println!("  {label}: {counts:?}");
}

/// The BUILD is one pass over the corpus, and it holds no guard.
///
/// What is asserted is the SHAPE at two sizes, not a constant: the walk visits exactly one block
/// address per record at both sizes (so it is O(corpus) and nothing walks it twice), the store is
/// listed once and each slab read once however big the corpus is, and no served-index encode
/// happens while a shard guard is held.
#[test]
fn a_snapshot_build_walks_the_corpus_once_and_holds_no_guard() {
    let mut rows = Vec::new();
    for records in [SMALL, LARGE] {
        let cluster = cluster_with(records);
        println!("=== BUILD records={records} ===");
        let fixture = assert_the_fixture_is_populated(&cluster, records);

        snapshot_probe::reset();
        crate::engine::shard_write_guard::reset_index_encode_counts();
        let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
        let counts = snapshot_probe::counts();
        let encodes = crate::engine::shard_write_guard::index_encode_counts();
        print_counts("build", &counts);
        println!("  encodes: {encodes:?}");

        assert_eq!(
            counts.image_builds, 1,
            "one create_snapshot must build exactly one state image"
        );
        assert_eq!(
            counts.live_slab_scans, 1,
            "one create_snapshot must walk the corpus for live slab ids exactly once"
        );
        assert_eq!(
            counts.live_slab_scan_addresses, records as u64,
            "the live-slab walk must visit exactly one block address per record: {} for {records} \
             records",
            counts.live_slab_scan_addresses
        );
        assert_eq!(
            counts.slab_dir_listings, 1,
            "the build must list the store's slab directory exactly once"
        );
        assert_eq!(
            counts.slab_reads,
            snapshot
                .state_image
                .as_ref()
                .expect("state image")
                .slabs
                .len() as u64,
            "the build must read each carried slab exactly once"
        );
        assert_eq!(
            counts.image_clones, 0,
            "building a snapshot must not copy the image it just built"
        );
        assert_eq!(
            encodes.encodes_total, 1,
            "the build must encode the served index exactly once"
        );
        assert_eq!(
            encodes.encodes_under_guard, 0,
            "the served-index encode must not run while a shard-table guard is held"
        );
        rows.push((records, fixture, counts));
    }

    let (small_records, small_fixture, small) = &rows[0];
    let (large_records, large_fixture, large) = &rows[1];
    let corpus_ratio = *large_records as f64 / *small_records as f64;
    println!("=== BUILD ratio, {small_records} -> {large_records} ===");
    println!("  corpus ratio                 = {corpus_ratio:.3}");
    println!(
        "  addresses walked per record  = {:.3} -> {:.3}",
        small.live_slab_scan_addresses as f64 / *small_records as f64,
        large.live_slab_scan_addresses as f64 / *large_records as f64
    );
    println!(
        "  slab bytes read ratio        = {:.3}",
        large.slab_read_bytes as f64 / small.slab_read_bytes.max(1) as f64
    );
    println!(
        "  image bytes ratio            = {:.3}",
        large_fixture.payload_bytes as f64 / small_fixture.payload_bytes as f64
    );
    assert!(
        small.slab_read_bytes > 0 && large.slab_read_bytes > 0,
        "slab bytes read is zero at one of the two sizes, so the ratio below is vacuous: \
         {} and {}",
        small.slab_read_bytes,
        large.slab_read_bytes
    );
    // The walk is the finding: it is EXACTLY linear in the corpus, with no second pass hiding in
    // it. Asserted as an identity per record rather than as a bound, because a bound would also
    // be satisfied by a walk that had stopped happening.
    assert_eq!(
        small.live_slab_scan_addresses * (*large_records as u64),
        large.live_slab_scan_addresses * (*small_records as u64),
        "the live-slab walk must cost the same per record at both sizes: {} per {small_records} \
         against {} per {large_records}",
        small.live_slab_scan_addresses,
        large.live_slab_scan_addresses
    );
    // And the metadata around it does NOT grow with the corpus.
    assert_eq!(
        small.slab_dir_listings, large.slab_dir_listings,
        "the number of slab directory listings must not grow with the corpus"
    );
}

/// The INSTALL half, and what it holds the cluster write guard across.
///
/// This is the row the whole file exists for. `maybe_trigger_snapshot` is the ROUTINE snapshot --
/// the periodic compaction tick a deployed node runs, not the rare catch-up send. Each install
/// rebuilds a whole engine from the image, and it USED to do that inside the cluster write guard:
/// 3 of 3 rebuilds and 420,000 of 420,000 slab bytes at 1,000 records, 1,680,000 of 1,680,000 at
/// 4,000 -- 100% of the install work, at both sizes, inside the half of the lock every `propose`
/// needs.
///
/// The rebuild reads nothing but the snapshot, so it is built before the guard is taken. What is
/// asserted here is the identity in both directions: every rebuild still HAPPENS (a path that
/// stopped installing would also stop rebuilding under the guard), and none of them happens under
/// the guard.
#[test]
fn a_routine_snapshot_rebuilds_no_engine_under_the_cluster_write_guard() {
