// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the DUMP path costs at two corpus sizes.
//!
//! THE RESULT. A bucket dump manifest embeds a whole-shard index image, so it is proportional to
//! the STORE and not to the buckets dumped. That is a durability decision and these tests do not
//! argue with it. What they measure is how many times that document is read back, parsed and
//! re-serialised by work that never looks inside it. Counted rather than timed, because the box
//! these run on is shared:
//!
//! ```text
//!   records   manifest on disk   bytes ONE round reads back   re-serialised for checksum
//!     2,000            338 KB           10.2 MB -> 1.0 MB           10.9 MB -> 1.7 MB
//!    20,000          3,810 KB          114.9 MB -> 11.5 MB         122.6 MB -> 19.2 MB
//! ```
//!
//! The left-hand numbers were 11.28x over a 10.00x corpus, with ONE manifest on disk in both
//! arms. The multiplier was never the store -- it was that one round takes 27
//! manifest-directory listings, and every listing read every manifest file whole, parsed it
//! whole, and re-serialised it whole to verify its checksum, to get `wal_sequence`,
//! `index_log_sequence`, `bucket_ids` and `block_slab_ids`: four small fields, none of which
//! needs the index image.
//!
//! WHAT CHANGED. A round still takes 27 listings -- that is control flow and nothing here
//! touches it -- but it now reads THREE manifest files instead of thirty, because a listing
//! reuses a manifest it has already read and checksum-verified for as long as the directory
//! entry still describes the same bytes. Three is the number of distinct manifest FILES the
//! round sees: the one already on disk, plus the two it writes itself. The other 27 file-opens
//! were the same documents over again.
//!
//! WHAT IS FIXED HERE: the re-serialisation used to COPY the manifest first, once per manifest per
//! listing, to clear one string field. `bucket_dump_manifest_checksum_in_place` empties the field
//! on the value it already owns and puts it back. Counted, not timed:
//! `a_listing_no_longer_copies_every_manifest_to_check_its_checksum` drives both arms in one
//! process on one fixture.
//!
//! THE ROUND REALLY DOES WRITE A MANIFEST HALF WAY THROUGH ITS OWN LISTINGS, so a memo scoped to
//! "one round" would be wrong. Traced in order, the 27 listings fall into three epochs: twelve
//! see the manifest that was already there, then the round writes one; three see both; then the
//! round writes again and the prune removes the first, and the last twelve see only the newest.
//! That is why the memo is keyed on what `read_dir` and `stat` report about each entry rather
//! than on a round boundary or on mutation sites agreeing to announce themselves --
//! `a_listing_reuses_the_manifest_it_already_read_and_verified` states the rule in four
//! directions and `a_manifest_deleted_behind_the_memos_back_stops_being_listed` holds it to the
//! one that decides soundness.
//!
//! WHAT IS STILL NOT FIXED: the 27 listings themselves, and the fact that reading a manifest at
//! all materialises an index image nobody asked for. Parsing only the head would drop the
//! checksum verification that stops a corrupt manifest being chosen as newest, and this does not
//! do that: every manifest the memo serves was read whole and verified whole when its bytes were
//! first seen.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;

fn dump_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed_in_batches(engine: &TemporalEngine, shard_id: ShardId, count: usize, batch: usize) {
    let mut index = 0usize;
    while index < count {
        let end = (index + batch).min(count);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

fn round_request() -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..StorageManagerCycleRequest::default()
    }
}

/// ONE DUMP'S MANIFEST BYTES, AT TWO CORPUS SIZES, AND WHAT ELSE GOES THROUGH SERDE TO PRODUCE IT.
///
/// Two halves, asserted separately: that the manifest is proportional to the store (it embeds the
/// whole-shard index, so it must be), and that producing it serialises that document more than
/// once -- the checksum pass and the write pass.
#[test]
fn a_dump_serialises_its_whole_manifest_more_than_once_at_every_corpus_size() {
    fn one_size(records: usize) -> (u64, u64, u64, u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = dump_engine(dir.path());
        seed_in_batches(&engine, 1, records, 100);
        crate::engine::reset_bucket_dump_manifest_io_counts();
        let started = std::time::Instant::now();
        let manifest = engine
            .create_bucket_dump_manifest(1, Vec::<u32>::new())
            .expect("dump");
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let counts = crate::engine::bucket_dump_manifest_io_counts();
        println!(
            "  {records:>6} records: manifest written {:>9} B in {} file write(s) ({} under a \
             shard guard), checksum re-serialised {:>9} B in {} pass(es), {} copy(ies); dump took \
             {elapsed_ms} ms over {} buckets",
            counts.bytes_written,
            counts.file_writes,
            counts.writes_under_guard,
            counts.checksum_bytes,
            counts.checksum_serializes,
            counts.checksum_clones,
            manifest.bucket_ids.len()
        );
        (
            counts.bytes_written,
            counts.checksum_bytes,
            counts.checksum_serializes,
            counts.file_writes,
            counts.writes_under_guard,
        )
    }

    let (small_written, small_checksum, small_passes, small_writes, small_guarded) = one_size(SMALL);
    let (large_written, large_checksum, large_passes, large_writes, large_guarded) = one_size(LARGE);

    // DENOMINATOR. A dump that wrote nothing satisfies every ratio below.
    assert!(
        small_written > 0 && large_written > 0,
        "a dump wrote no manifest bytes at one of the two sizes ({small_written} / \
         {large_written}), so every ratio below is vacuous"
    );
    assert_eq!(
        (small_writes, large_writes),
        (1, 1),
        "one dump must write exactly one manifest file at each size, got {small_writes} / \
         {large_writes}"
    );

    // HALF ONE: the manifest is proportional to the STORE, not to the dumped buckets.
    let byte_ratio = large_written as f64 / small_written as f64;
    let size_ratio = LARGE as f64 / SMALL as f64;
    println!(
        "  manifest bytes {small_written} -> {large_written} = {byte_ratio:.2}x over a \
         {size_ratio:.2}x corpus"
    );
    assert!(
        byte_ratio > size_ratio * 0.5,
        "manifest bytes grew only {byte_ratio:.2}x over a {size_ratio:.2}x corpus, so the \
         manifest has stopped being proportional to the store and this test has stopped \
         describing it"
    );

    // HALF TWO, ASSERTED SEPARATELY: the extra serde pass, and no copy behind it.
    assert_eq!(
        (small_passes, large_passes),
        (1, 1),
        "the create path runs the checksum serialise exactly once per dump; got {small_passes} / \
         {large_passes}"
    );
    let small_multiple = (small_written + small_checksum) as f64 / small_written as f64;
    let large_multiple = (large_written + large_checksum) as f64 / large_written as f64;
    println!(
        "  bytes through serde per byte written: {small_multiple:.2}x at {SMALL}, \
         {large_multiple:.2}x at {LARGE}"
    );
    assert!(
        small_multiple > 1.5 && large_multiple > 1.5,
        "the checksum pass no longer re-serialises the manifest ({small_multiple:.2}x / \
         {large_multiple:.2}x); if that was fixed, fix this number too"
    );

    // HALF THREE: the durable write is not taken under a shard-table guard.
    assert_eq!(
        (small_guarded, large_guarded),
        (0, 0),
        "a manifest write -- a whole-store document, fsync'd, plus a parent-directory fsync -- \
         was taken while this thread held a shard-table guard ({small_guarded} / \
         {large_guarded} of {small_writes} / {large_writes}). Every write on the shard waits \
         behind that."
    );
}

/// WHAT ONE STORAGE-MANAGER ROUND READS BACK OFF THE MANIFEST DIRECTORY.
///
/// Nothing in a round needs a manifest's index image. The round reads `wal_sequence`,
/// `index_log_sequence`, `bucket_ids` and `block_slab_ids` -- four small fields. It gets them by
/// listing the directory 27 times, and it used to read every manifest file whole, parse every one
/// whole and re-serialise every one whole to verify its checksum on every one of those listings:
/// |listings| x |corpus|, with neither factor in the question.
///
/// It now reads each distinct manifest FILE once. The listings are unchanged; what is gone is
/// reading the same document twenty-seven times to answer the same four questions.
///
/// Halves, asserted separately and in this order: how many times a round lists the directory (a
/// count, and a property of control flow, so FLAT in the store); how many files it reads to
/// serve them (also flat, and now far below the listing count); what that costs in bytes (still
/// proportional to the store, because a manifest still embeds one); and what the memo COSTS,
/// which is counted beside what it saves so a version that only moved the cost could not read as
/// a win.
#[test]
fn a_round_reads_each_distinct_manifest_once_to_answer_four_small_questions() {
    fn one_size(records: usize) -> (u64, u64, u64, u64, u64, u64, u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = dump_engine(dir.path());
        seed_in_batches(&engine, 1, records, 100);
        // One dump, so the directory holds exactly one manifest when the round starts.
        engine
            .create_bucket_dump_manifest(1, Vec::<u32>::new())
            .expect("dump");
        let manifests = engine.list_bucket_dump_manifests(1).len() as u64;
        crate::engine::reset_bucket_dump_manifest_io_counts();
        let started = std::time::Instant::now();
        let report = engine.run_storage_manager_cycle(round_request());
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let counts = crate::engine::bucket_dump_manifest_io_counts();
        println!(
            "  {records:>6} records, {manifests} manifest(s) on disk, round {elapsed_ms} ms: \
             listed the dir {} time(s), read {} file(s) = {} B ({} read under a shard guard), \
             re-serialised {} manifest(s) for checksum = {} B (round errors {})",
            counts.dir_listings,
            counts.file_reads,
            counts.bytes_read,
            counts.reads_under_guard,
            counts.checksum_serializes,
            counts.checksum_bytes,
            report.errors.len()
        );
        println!(
            "     memo: {} hit(s), {} miss(es); COST {} owned copy/copies carrying {} B of index \
             image",
            counts.parse_hits, counts.parse_misses, counts.owned_copies, counts.owned_copy_bytes
        );
        if records == LARGE {
            println!("  WHO ASKS FOR A LISTING, and how many each round:");
            for (site, times) in crate::engine::bucket_dump_manifest_listing_sites() {
                println!("    {times:>3}x  {site}");
            }
        }
        (
            manifests,
            counts.dir_listings,
            counts.file_reads,
            counts.bytes_read,
            counts.checksum_bytes,
            counts.reads_under_guard,
            counts.owned_copies,
            counts.owned_copy_bytes,
        )
    }

    let (
        small_manifests,
        small_listings,
        small_reads,
        small_bytes,
        small_checksum,
        small_guarded,
        small_copies,
        small_copy_bytes,
    ) = one_size(SMALL);
    let (
        large_manifests,
        large_listings,
        large_reads,
        large_bytes,
        large_checksum,
        large_guarded,
        large_copies,
        large_copy_bytes,
    ) = one_size(LARGE);

    // DENOMINATOR.
    assert_eq!(
        (small_manifests, large_manifests),
        (1, 1),
        "each arm must start with exactly one manifest, or the per-manifest reading below is not \
         comparable: {small_manifests} / {large_manifests}"
    );
    assert!(
        small_listings > 0 && large_listings > 0,
        "a round listed the manifest directory zero times at one size ({small_listings} / \
         {large_listings}); either the round stopped asking or the counter stopped counting, and \
         every number below would then read as a win"
    );

    // HALF ONE: how many whole-file reads per round, and is it flat in the store.
    println!(
        "  directory listings per round: {small_listings} at {SMALL}, {large_listings} at \
         {LARGE}; whole-file reads {small_reads} -> {large_reads}"
    );
    assert_eq!(
        small_listings, large_listings,
        "the number of manifest-directory listings a round takes changed with the corpus \
         ({small_listings} -> {large_listings}); it is a property of the round's control flow, \
         not of the store"
    );
    assert_eq!(
        small_reads, large_reads,
        "whole-manifest file reads per round changed with the corpus ({small_reads} -> \
         {large_reads}) with the same number of manifests on disk"
    );
    // HALF ONE B, ASSERTED SEPARATELY: a listing no longer re-reads a manifest it has already
    // read and verified, so the reads are the count of distinct manifest FILES the round saw --
    // the one on disk plus the two it writes -- and not one per listing.
    assert!(
        small_reads < small_listings && large_reads < large_listings,
        "a round read {small_reads} / {large_reads} manifest files for {small_listings} / \
         {large_listings} listings. With one manifest on disk at the start and two written during \
         the round, a listing that consults the memo reads each distinct FILE once; one read per \
         listing means it is not being consulted"
    );

    // HALF TWO, ASSERTED SEPARATELY: the bytes, which are NOT flat.
    let read_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let size_ratio = LARGE as f64 / SMALL as f64;
    println!(
        "  bytes a round reads back: {small_bytes} -> {large_bytes} = {read_ratio:.2}x over a \
         {size_ratio:.2}x corpus; checksum re-serialisation on top: {small_checksum} -> \
         {large_checksum}"
    );
    assert!(
        read_ratio > size_ratio * 0.5,
        "the bytes a round reads back off the manifest directory grew only {read_ratio:.2}x over \
         a {size_ratio:.2}x corpus. If a round stopped materialising the embedded index to answer \
         its four small questions, that is the fix this test exists to notice -- update it."
    );

    // HALF TWO B, ASSERTED SEPARATELY: WHAT THE MEMO COSTS. A memo that stopped a round reading
    // 115 MB and made it copy 115 MB instead would move the cost, not remove it, and every byte
    // count above would still read as a win. The copies are the manifests handed to callers that
    // want a value they can keep; the round's own read sites take theirs shared.
    println!(
        "  owned copies a round takes off the memo: {small_copies} = {small_copy_bytes} B, \
         {large_copies} = {large_copy_bytes} B, against {small_bytes} / {large_bytes} B of \
         reading removed"
    );
    assert!(
        large_copy_bytes < large_bytes,
        "the round copied {large_copy_bytes} B out of the memo to avoid reading {large_bytes} B. \
         That is moving the cost rather than removing it -- a read site that needs to OWN a \
         manifest should take the shared listing instead"
    );
    assert!(
        small_copies <= small_listings && large_copies <= large_listings,
        "more owned copies ({small_copies} / {large_copies}) than listings ({small_listings} / \
         {large_listings}); a listing is handing out more than one copy"
    );

    // HALF THREE: none of that reading is taken under a shard-table guard.
    assert_eq!(
        (small_guarded, large_guarded),
        (0, 0),
        "manifest files were read while this thread held a shard-table guard ({small_guarded} of \
         {small_reads}, {large_guarded} of {large_reads}). Each read is a whole-store document off \
         disk, and every write on the shard waits behind it."
    );
}

/// A LISTING NO LONGER COPIES EVERY MANIFEST TO CHECK ITS CHECKSUM.
///
/// The checksum is taken over the manifest's own JSON with the checksum field emptied, and the
/// only way to produce that from a shared reference is to copy the whole document first -- the
/// whole-shard index image and one summary per bucket, once per manifest per listing. A listing
/// owns what it parsed, so it can empty the field in place and put it back.
///
/// TWO ARMS IN ONE PROCESS ON ONE FIXTURE. "No copies" is satisfied just as well by a listing that
/// stopped checking checksums, or by one that found no manifests, so the control arm forces the
/// copy back on and must produce a non-zero count -- and both arms must agree on the checksum,
/// which is what makes the removal a change of cost and not of meaning.
#[test]
fn a_listing_no_longer_copies_every_manifest_to_check_its_checksum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = dump_engine(dir.path());
    seed_in_batches(&engine, 1, SMALL, 100);
    engine
        .create_bucket_dump_manifest(1, Vec::<u32>::new())
        .expect("dump");

    // CONTROL FIRST, so a treatment that somehow removed the manifest cannot make the control
    // pass by finding nothing.
    crate::engine::bucket_dump_io::checksum_clones_manifest_for_test(true);
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let control = engine.list_bucket_dump_manifests(1);
    let control_counts = crate::engine::bucket_dump_manifest_io_counts();
    crate::engine::bucket_dump_io::checksum_clones_manifest_for_test(false);

    crate::engine::reset_bucket_dump_manifest_io_counts();
    let treatment = engine.list_bucket_dump_manifests(1);
    let treatment_counts = crate::engine::bucket_dump_manifest_io_counts();

    println!(
        "  one listing of {} manifest(s): copying arm {} copy(ies) / {} serialise(s), in-place arm \
         {} copy(ies) / {} serialise(s)",
        control.len(),
        control_counts.checksum_clones,
        control_counts.checksum_serializes,
        treatment_counts.checksum_clones,
        treatment_counts.checksum_serializes
    );

    // DENOMINATOR: a listing that read nothing proves nothing.
    assert_eq!(
        control.len(),
        1,
        "the control listing found {} manifests, not the 1 this fixture wrote",
        control.len()
    );
    assert_eq!(
        control_counts.checksum_serializes, 1,
        "the control listing verified {} checksums, not 1; it is not doing the work this test \
         is about",
        control_counts.checksum_serializes
    );
    assert!(
        control_counts.checksum_clones >= 1,
        "the control arm produced {} copies, so 'the treatment produced none' says nothing",
        control_counts.checksum_clones
    );

    // SAME ANSWER. The point is that the cost went, not the check.
    //
    // Compared by identity and integrity field rather than by whole value: a manifest carries one
    // summary per bucket, and a failure that prints two of them buries the sentence saying what
    // went wrong under a megabyte of bucket ids.
    let identify = |manifests: &[BucketDumpManifest]| {
        manifests
            .iter()
            .map(|manifest| (manifest.manifest_id.clone(), manifest.checksum.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        identify(&control),
        identify(&treatment),
        "the two arms disagreed about what the directory holds, so this is not a cost change"
    );
    assert_eq!(
        treatment_counts.checksum_serializes, control_counts.checksum_serializes,
        "the in-place arm verified {} checksums against the control's {}; it skipped the check \
         rather than doing it more cheaply",
        treatment_counts.checksum_serializes, control_counts.checksum_serializes
    );
    assert_eq!(
        treatment_counts.checksum_bytes, control_counts.checksum_bytes,
        "the two arms hashed different payload sizes ({} vs {}), so the checksum is not the same \
         function it was",
        treatment_counts.checksum_bytes, control_counts.checksum_bytes
    );
    assert_eq!(
        treatment_counts.checksum_clones, 0,
        "a listing still copies {} whole manifest(s) to check a checksum",
        treatment_counts.checksum_clones
    );
}

/// HOW MANY MANIFESTS A SHARD ACCUMULATES, AND WHETHER THE PRUNE KEEPS UP.
///
/// The prune retains the newest manifest plus any older one that is still the only dump covering
/// some bucket. A round that dumps EVERY dirty bucket therefore supersedes everything before it,
/// and the directory should sit at one. Ten dumps, and this counts what is left -- each one of
/// them would be a whole-shard index image on disk, and a whole-shard index image through serde on
/// every listing of every later round.
#[test]
fn repeated_dumps_do_not_accumulate_manifests_when_each_covers_the_whole_shard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = dump_engine(dir.path());
    seed_in_batches(&engine, 1, 2_000, 100);

    let mut counted = 0usize;
    let mut index = 0usize;
    while index < 10 {
        seed_in_batches(&engine, 1, 2_000 + (index + 1) * 50, 100);
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(
            report.errors.is_empty(),
