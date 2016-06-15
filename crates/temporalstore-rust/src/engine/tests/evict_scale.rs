// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN EVICTION ROUND COSTS ONCE THE STORE IS LARGE, and whether a round can keep up with one.
//!
//! Eviction has been measured here before, always at 500 to 4,000 objects. At that size every
//! term in a round is small enough that a term proportional to the STORE and a term proportional
//! to the BATCH are the same handful of microseconds, and the two cannot be told apart. These
//! tests run the same round two orders of magnitude further out and print the ratio beside the
//! corpus ratio, so each term names itself.
//!
//! THE RESULT, at 20,000 and 200,000 objects with the shipped batch limit of 16 victims:
//!
//! ```text
//!                                     20,000        200,000     ratio   corpus ratio 10.00x
//!   victims chosen                        16             16      1.00x   FLAT
//!   sampler scan budget                  320            320      1.00x   FLAT
//!   recency entries cloned                 0              0        --    FLAT (sampled arm)
//!   model-map addresses visited       20,000        200,000     10.00x   GROWS WITH THE STORE
//!   bucket-index entries visited      40,000        400,000     10.00x   GROWS WITH THE STORE
//!   blocks released                        0              0        --    the actuator releases
//!                                                                        nothing at the default
//! ```
//!
//! THE SHAPE. Choosing the victims is bounded and has been since the sampler landed -- the
//! sampler walks `samples * batch_limit * scan_turns` buckets and no more, and clones no recency
//! map. What is not bounded is everything the round does AROUND the choice:
//!
//!   * `release_bucket_blocks` asks the model maps what a reload would rebuild for its victims.
//!     The maps are keyed by object and the routing bucket is a field of the ADDRESS, so the pass
//!     runs `accept` on every live address in the shard to answer a question about at most
//!     `batch_limit` buckets. That walk is not new and the tree already argues it cannot be
//!     answered from an index. What is new is that it now has a counter: the one counter that
//!     watched this path, `BUCKET_SCOPED_MODEL_ENTRIES`, charges only what the walk EMITS, which
//!     is bounded by the victims -- so the audit row for a whole-store term read as a batch-sized
//!     term at every size ever measured.
//!   * the pressure gate reads `bucket_index_resident_bytes` before the round and again after it,
//!     and each read walks every node and every resident page in the bucket index.
//!
//! AND IT IS UNDER THE SHARD-TABLE WRITE GUARD. `apply_storage_eviction` takes `shards.write()`
//! and calls `release_bucket_blocks` inside it, so the whole-store walk is an interval every
//! serving read and write on the shard queues behind.
//!
//! WHAT THE ACTUATOR RELEASES AT THE SHIPPED SETTINGS: nothing. `eviction_dump_before_evict`
//! ships false, a bucket written and not yet dumped is dirty, and a dirty bucket is refused --
//! correctly, because the model maps carry no per-page dirty bit for a reload to restore. So a
//! default-configured round pays the whole-store walk and the two index walks to release zero
//! blocks. Turn the dump on and the same round releases its victims; both arms are measured, and
//! the refusal TERM is named rather than left as a count.
//!
//! WHETHER IT KEEPS UP. A round gives back at most `batch_limit` buckets however large the store
//! is, and costs a number of visits proportional to the store. The cost of one unit of relief is
//! therefore the store divided by the batch, and it is measured here at two sizes so that is a
//! number rather than an argument. `an_eviction_round_cannot_keep_up_with_the_store_it_is_draining`
//! runs real rounds rather than extrapolating from one, and reports how many.

#![allow(clippy::all)]
use super::*;
use crate::engine::storage_bucket_internals::{release_bucket_blocks, BucketReleaseOutcome};
use crate::Command;
#[cfg(feature = "alloc-probe")]
use crate::ExecuteRequest;
use std::collections::{BTreeMap, BTreeSet};

/// The two corpus sizes. A decade apart so a term that is flat and a term that tracks the store
/// separate by a factor of ten rather than by a residual.
/// Read by the `alloc-probe` arms only, which is where a corpus this size is affordable.
#[allow(dead_code)]
const SMALL: usize = 20_000;
#[allow(dead_code)]
const LARGE: usize = 200_000;

/// The sizes the always-on guards use. The claim they pin is the same one; what they give up is
/// the resolution, which is what the `alloc-probe` table above is for.
const GUARD_SMALL: usize = 2_000;
const GUARD_LARGE: usize = 8_000;

/// The shipped batch limit (`DEFAULT_EVICTION_BATCH_LIMIT`).
const BATCH: usize = 16;

fn evict_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine.use_sampled_eviction_for_test();
    engine
}

/// Seed `count` objects, rolling to a fresh block slab after every batch.
///
/// THE SLAB LAYOUT IS THE FIXTURE, AND TWO EARLIER SHAPES OF IT PROVED NOTHING.
///
/// The shipped slab target is 1 GiB, so a corpus of this size writes into ONE slab: every live
/// page a victim holds carries `block_slab_id` 0, and a guard asserting that the victims span
/// slabs compares `{0}` with `{0}` and passes whatever the selection does.
///
/// Rolling ONCE at the midpoint does not fix it, and the reason is the selection rather than the
/// layout. `block_routing_bucket` recency is stamped by every command, read or write, with
/// `now_ms()` (`engine.rs`), and the sampler ranks its pool coldest-first -- so the victims are
/// the EARLIEST-WRITTEN buckets in its scan window, and the earliest-written pages are on the
/// first slab by construction. Measured: a midpoint roll gave sixteen victims all on slab 0.
///
/// A third shape also failed and is worth naming because it looks like the fix. Keys of the form
/// `prefix-{ordinal:09}` hash to routing buckets that cluster: at 8,000 records the eighty LOWEST
/// routing buckets in the store came from ordinals 4,680 to 4,699 -- twenty consecutive ordinals
/// -- so the sampler's window was a twenty-ordinal slice of the write order, and a midpoint roll
/// put all of it on slab 1. Scattering the ordinal across the key space (see [`scale_key`]) fixes
/// the clustering and does NOT fix the span, because recency still decides.
///
/// What works is making the slab non-monotonic in write TIME at the granularity the ranking uses.
/// Rolling after every batch puts consecutive batches on consecutive slabs, so the coldest
/// buckets in any scan window are drawn from the first few batches and therefore from several
/// slabs -- and if a whole run of batches shares one `now_ms()`, the tie breaks on routing bucket,
/// which is scattered across the batches for the same reason. Both readings span.
///
/// Every roll is asked for with a target of one byte, which any slab that has been written to at
/// all is already at -- so each one rolls a slab with RECORDS IN IT, never an empty file.
fn seed_with_a_slab_roll(engine: &TemporalEngine, count: usize) {
    seed_range(engine, 0, count);
}

/// The key for one ordinal, with the ordinal SCATTERED across the key space first.
///
/// MEASURED, and the reason the obvious fixture does not work. With keys of the shape
/// `prefix-{ordinal:09}` the routing buckets of neighbouring ordinals land next to each other:
/// the store's object hash is FNV-1a, a run of ordinals differs only in its last few digits, and
/// at 8,000 records the eighty LOWEST routing buckets in the whole store came from ordinals 4,680
/// to 4,699 -- twenty consecutive ordinals. The sampler ranks its pool by routing bucket when
/// recency ties, so it chose sixteen victims from a twenty-ordinal window; pages are appended in
/// write order, so any slab boundary drawn on the ordinal puts that whole window on ONE slab, and
/// a guard asserting the victims span slabs compared `{1}` with `{1}` and failed -- or, had the
/// roll fallen the other way, passed while proving nothing.
///
/// Multiplying the ordinal by an odd constant first is a bijection on 32 bits, so the keys stay
/// distinct and one per ordinal, and neighbouring ordinals stop being neighbours in routing-bucket
/// space. The key is also a CONSTANT 22 bytes at every ordinal and every corpus size, which
/// matters because allocation counts move with key length and the two corpus arms are compared by
/// allocation.
fn scale_key(ordinal: usize) -> String {
    let scattered = (ordinal as u64).wrapping_mul(2_654_435_761) & 0xffff_ffff;
    format!("evict-large-{scattered:010}")
}

fn seed_range(engine: &TemporalEngine, from: usize, to: usize) {
    const BATCH_SIZE: usize = 500;
    let mut index = from;
    while index < to {
        let end = (index + BATCH_SIZE).min(to);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: scale_key(cursor),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(
            response.status.ok,
            "seed write failed at {index}: {:?}",
            response.status
        );
        let rolled = engine
            .block_store
            .prepare_next_slab_with_target(1)
            .expect("rolling the block slab between seed batches");
        assert!(
            rolled.is_some(),
            "the roll after the batch ending at {end} did not happen, so this batch and the next \
             share a slab and the layout the slab-span guard depends on is not the one described"
        );
        index = end;
    }
}

/// Live pages per object key, read straight off the model maps.
///
/// The COUNT per key, not merely which keys are present. Evicting too much is data loss and it is
/// silent, so the direction guard compares this map element by element against the same map taken
/// before the round: a key that kept its entry but lost a page would pass a presence check.
fn live_blocks_by_key(engine: &TemporalEngine) -> BTreeMap<String, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut counts = BTreeMap::new();
    for entry in crate::engine::collect_live_block_entries(shard) {
        *counts.entry(entry.object_key.to_string()).or_insert(0) += 1;
    }
    counts
}

/// Objects each routing bucket still claims, by bucket.
///
/// A release empties a bucket's RESIDENT page list and deliberately keeps its `object_index` --
/// that is what keeps a released bucket countable and distinguishes it from one that genuinely
/// holds nothing. The model maps do not record it, so [`live_blocks_by_key`] cannot see it go; a
/// release that dropped it would be silent everywhere else in this file.
fn objects_by_bucket(engine: &TemporalEngine) -> BTreeMap<u32, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.object_index.object_count()))
        .collect()
}

/// Slab ids the shard's live pages actually sit on, and how many pages sit on each.
fn live_blocks_by_slab(engine: &TemporalEngine) -> BTreeMap<u64, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut by_slab = BTreeMap::new();
    for entry in crate::engine::collect_live_block_entries(shard) {
        *by_slab.entry(entry.address.block_slab_id()).or_insert(0) += 1;
    }
    by_slab
}

/// The slab ids the named routing buckets hold resident pages on.
fn slabs_spanned_by(engine: &TemporalEngine, buckets: &BTreeSet<u32>) -> BTreeSet<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut slabs = BTreeSet::new();
    for routing_bucket in buckets {
        if let Some(bucket) = shard.bucket_index.bucket_map.get(routing_bucket) {
            for block in bucket.block_index.values() {
                if !block.deleted {
                    slabs.insert(block.address.block_slab_id());
                }
            }
        }
    }
    slabs
}

/// Everything one round of `apply_storage_eviction` cost and did, in counters rather than clocks.
///
/// Every field is read by at least one test in this file, but two of them only by the
/// `alloc-probe` arms, so the struct says so rather than turning a default build's warning list
/// into noise.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct RoundCost {
    records: usize,
    buckets: usize,
    victims: usize,
    /// Model-map addresses the round LOOKED AT. The whole-store term.
    model_visits: u64,
    /// Model-map pages the round MATERIALIZED. The batch-sized term, and the only one that had a
    /// counter before this file.
    model_emitted: u64,
    /// Bucket-index nodes and pages the two resident-bytes reads walked.
    resident_visits: u64,
    /// Whole-store live-page entries materialized anywhere in the round.
    live_scan: u64,
    /// Recency entries the selection copied. Zero on the sampled arm by construction.
    recency_cloned: u64,
    released_buckets: usize,
    released_blocks: usize,
    refused: usize,
    /// Times the release ran its whole-store derivation. At most one per `release_bucket_blocks`
    /// call, and zero when no candidate reached the model-map comparison.
    derivations: u64,
    index_bytes_before: u64,
    index_bytes_after: u64,
    round_nanos: u128,
}

/// Drive ONE round on a seeded engine and collect every counter around it.
///
/// Every counter is reset immediately before the call and read immediately after, which is what
/// makes a process-wide counter mean one round; the suite runs `--test-threads=1`.
fn one_round(engine: &TemporalEngine, records: usize, batch_limit: usize, dump: bool) -> RoundCost {
    let buckets = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.bucket_index.bucket_map.len())
            .unwrap_or_default()
    };
    crate::engine::reset_model_map_addresses_visited();
    crate::engine::reset_bucket_release_model_derivations();
    crate::engine::reset_bucket_scoped_model_entries();
    crate::engine::reset_bucket_index_resident_bytes_visits();
    crate::engine::reset_live_block_scan_entries();
    crate::engine::storage_lifecycle_methods::EVICTION_RECENCY_ENTRIES_CLONED
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let started = std::time::Instant::now();
    // Threshold 0 so the pressure gate admits and the round actually runs.
    let report = engine.apply_storage_eviction(1, 0, batch_limit, dump, false);
    let round_nanos = started.elapsed().as_nanos();
    let cost = RoundCost {
        records,
        buckets,
        victims: report.selected_victims.len(),
        model_visits: crate::engine::model_map_addresses_visited(),
        model_emitted: crate::engine::bucket_scoped_model_entries(),
        resident_visits: crate::engine::bucket_index_resident_bytes_visits(),
        live_scan: crate::engine::live_block_scan_entries(),
        recency_cloned: crate::engine::storage_lifecycle_methods::EVICTION_RECENCY_ENTRIES_CLONED
            .load(std::sync::atomic::Ordering::Relaxed),
        released_buckets: report.bucket_index_buckets_released,
        released_blocks: report.bucket_index_blocks_released,
        refused: report.bucket_index_release_refused,
        derivations: crate::engine::bucket_release_model_derivations(),
        index_bytes_before: report.bucket_index_bytes_before,
        index_bytes_after: report.bucket_index_bytes_after,
        round_nanos,
    };
    assert!(
        report.pressure_gate_open,
        "{records} records: the round never got past the pressure gate, so nothing was measured: \
         {}",
        report.skipped_reason
    );
    cost
}

fn ratio(small: u64, large: u64) -> f64 {
    if small == 0 {
        0.0
    } else {
        large as f64 / small as f64
    }
}

// -------------------------------------------------------------------------------------------
// FIXTURE PROOF. Runs first and on its own, so a failure here cannot let a claim below stand.
// -------------------------------------------------------------------------------------------

/// THE FIXTURE CAN EXPRESS WHAT THE TESTS BELOW MEASURE.
///
/// Four things have to be true before any number in this file means anything, and each is a
/// separate assertion with its own message:
///
///   1. the store holds MORE THAN ONE routing bucket -- otherwise "the victims" and "the store"
///      are the same object and every ratio is a comparison of a thing with itself;
///   2. the store holds MORE THAN ONE block slab, and both slabs hold RECORDS. A slab count taken
///      from the directory would pass on two empty files;
///   3. the victims a round chooses are the OLDEST-WRITTEN pages in the store, and not the
///      pages a scan-ordered selection would reach. This clause used to assert that the victims
///      SPAN slabs, and that was a property of the selection WINDOW rather than of the ordering.
///      Every bucket in this fixture holds exactly one page and nothing is ever read here, so a
///      bucket's recency IS its write time, and the fixture rolls a slab per seed batch -- which
///      makes the slab monotonic in that time. The coldest sixteen buckets in the store are
///      therefore the earliest-written sixteen and their pages are on the FIRST slab, by
///      construction, once the pass looks at enough of the store to find them. A selection that
///      spans slabs here is one that is NOT ordering by recency: the sixteen lowest ROUTING
///      buckets are scattered across the seed batches (see [`scale_key`]) and so across slabs,
///      which is exactly the set this clause now asserts the round does not take. Clause 2 is
///      what keeps this from comparing `{0}` with `{0}`: live pages sit on every slab, so
///      confining the victims to the first one is a choice the selection made;
///   4. the round chooses FEWER victims than there are buckets, which is what makes "per victim"
///      and "per record" different denominators at all.
#[test]
fn the_eviction_fixture_holds_several_slabs_and_the_victims_are_its_oldest_blocks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = evict_engine(dir.path());
    seed_with_a_slab_roll(&engine, GUARD_LARGE);

    let by_slab = live_blocks_by_slab(&engine);
    // The ids, not only the count: clause 3 below needs the set a SCAN-ordered selection would
    // reach, and `bucket_map` is ordered, so its leading entries are exactly that set.
    let all_buckets: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.bucket_index.bucket_map.keys().copied().collect())
            .unwrap_or_default()
    };
    let buckets = all_buckets.len();
    let report = engine.apply_storage_eviction(1, 0, BATCH, false, false);
    let victims = report
        .selected_victims
        .iter()
        .map(|victim| victim.routing_bucket)
        .collect::<BTreeSet<_>>();
    let victim_slabs = slabs_spanned_by(&engine, &victims);

    println!(
        "  fixture: {GUARD_LARGE} records -> {buckets} buckets, live pages by slab {by_slab:?}, \
         {} victims spanning slabs {victim_slabs:?}",
        victims.len()
    );

    assert!(
        buckets > 1,
        "the fixture produced {buckets} routing bucket(s); with one bucket the store and the \
         batch are the same object and every ratio below compares a thing with itself"
    );
    assert!(
        by_slab.len() > 1,
        "the fixture produced {} block slab(s) holding live pages; a single-slab store cannot \
         show that a selection spans slabs, and the mid-seed roll is there to prevent exactly \
         this: {by_slab:?}",
        by_slab.len()
    );
    for (slab, blocks) in &by_slab {
        assert!(
            *blocks > 0,
            "slab {slab} holds {blocks} live pages; a slab counted from the directory rather than \
             from its records would pass here on an empty file"
        );
    }
    assert!(
        !victims.is_empty(),
        "the round chose no victims, so there is nothing whose slab span could be asserted"
    );
    // CLAUSE 3, AND ITS CONTROL IS THE OTHER CANDIDATE ANSWER.
    //
    // `scan_order` is what a selection ordered by position in the key space would take: the
    // leading entries of the ordered bucket map. Those buckets' ordinals are scattered across the
    // seed batches, so their pages are scattered across slabs -- which is why this set is the
    // right control and why a span used to read as a pass. The round's own victims are the
    // oldest-written buckets, so their pages are on the earliest slabs. The two statements below
    // are each other's: the recency answer reaches no further forward in slab order than the
    // scan answer does, and it is not the same answer.
    let scan_order = all_buckets
        .iter()
        .take(BATCH)
        .copied()
        .collect::<BTreeSet<u32>>();
    let scan_order_slabs = slabs_spanned_by(&engine, &scan_order);
    println!(
        "  clause 3: victims {victims:?} on slabs {victim_slabs:?}; a scan-ordered selection \
         would take {scan_order:?} on slabs {scan_order_slabs:?}"
    );
    assert!(
        scan_order_slabs.len() > 1,
        "the {BATCH} lowest-numbered buckets sit on slab(s) {scan_order_slabs:?}; this control \
         has to span slabs or it cannot be told apart from the recency answer, and the scattering \
         in `scale_key` is what makes it span",
    );
    assert_ne!(
        victim_slabs, scan_order_slabs,
        "the round took the pages a scan-ordered selection would take ({victim_slabs:?}); the \
         victims have to be chosen by recency, and in this fixture recency is write time"
    );
    let newest_victim_slab = victim_slabs
        .iter()
        .copied()
        .max()
        .expect("the victim slab set is non-empty");
    let newest_scan_slab = scan_order_slabs
        .iter()
        .copied()
        .max()
        .expect("the control slab set is non-empty");
    assert!(
        newest_victim_slab < newest_scan_slab,
        "the newest slab the victims touch is {newest_victim_slab}, against {newest_scan_slab} \
         for the scan-ordered control. The round has to take the OLDEST pages, so its victims \
         cannot reach further forward in slab order than the selection it is supposed to beat"
    );
    assert!(
        victims.len() < buckets,
        "the round took {} of {buckets} buckets; with the batch equal to the store there is no \
         difference between a per-victim cost and a per-record cost",
        victims.len()
    );
}

// -------------------------------------------------------------------------------------------
// WHAT THE ACTUATOR RELEASES.
// -------------------------------------------------------------------------------------------

/// WHAT THE RELEASE ACTUALLY RELEASES, AT THE SHIPPED SETTING AND AT THE OTHER ONE.
///
/// An earlier note in this campaign recorded that the eviction round's actuator releases nothing.
/// This is that claim checked rather than inherited, and it is half right: at the shipped setting
/// it releases nothing, and the reason is a single named term rather than an absence.
///
/// `eviction_dump_before_evict` ships FALSE. A bucket that has been written and not dumped is
/// dirty, `release_bucket_blocks` refuses a dirty bucket, and it is right to -- the model maps
/// carry no per-page dirty bit, so a reload could not restore what a release of a dirty bucket
/// dropped. Turn the dump on, the round dumps its victims and clears their dirty state first, and
/// the same actuator releases them.
///
/// Both arms assert the refusal TERM, not just the count: eleven terms share `refused_buckets`,
/// and a round refused for `NotInMemory` and a round refused for `BucketDirty` are different
/// findings that a count cannot separate.
#[test]
fn the_release_actuator_is_refused_at_the_shipped_setting_and_releases_with_the_dump_on() {
    /// The round, plus a second release driven straight at the actuator so the refusal TERM can
    /// be read. `StorageEvictionReport` publishes the refusal COUNT and eleven terms share it.
    fn arm(dump: bool) -> (RoundCost, BucketReleaseOutcome) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, GUARD_SMALL);
        let cost = one_round(&engine, GUARD_SMALL, BATCH, dump);

        let cache_report = engine.storage_cache_inspection_report(1);
        let cache_by_bucket = cache_report
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.clone()))
            .collect::<BTreeMap<_, _>>();
        let candidates = engine
            .sampled_eviction_victims(1, BATCH, &cache_by_bucket)
            .iter()
            .map(|victim| victim.routing_bucket)
            .collect::<Vec<_>>();
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let outcome = release_bucket_blocks(shard, &candidates);
        assert_eq!(
            outcome.released_buckets.len() + outcome.refused_buckets,
            candidates.len(),
            "dump={dump}: the actuator acted on {} of {} candidates, so the terms below describe \
             a partial run",
            outcome.released_buckets.len() + outcome.refused_buckets,
            candidates.len()
        );
        assert_eq!(
            outcome.refusals.total(),
            outcome.refused_buckets,
            "dump={dump}: the named terms sum to {} against {} refusals, so a refusal was \
             counted without being attributed",
            outcome.refusals.total(),
            outcome.refused_buckets
        );
        (cost, outcome)
    }

    let (shipped, shipped_terms) = arm(false);
    let (dumped, dumped_terms) = arm(true);
    println!(
        "  dump_before_evict=false: {} victims -> {} buckets released, {} blocks, {} refused\n    \
         terms: {:?}\n  \
         dump_before_evict=true : {} victims -> {} buckets released, {} blocks, {} refused\n    \
         terms: {:?}",
        shipped.victims,
        shipped.released_buckets,
        shipped.released_blocks,
        shipped.refused,
        shipped_terms.refusals,
        dumped.victims,
        dumped.released_buckets,
        dumped.released_blocks,
        dumped.refused,
        dumped_terms.refusals,
    );

    // WHICH TERM. The shipped setting refuses on `bucket_dirty` and nothing else -- a round
    // refused for `not_in_memory` would be a different finding with the same count.
    assert_eq!(
        shipped_terms.refusals.bucket_dirty, shipped_terms.refused_buckets,
        "at the shipped setting {} of {} refusals were `bucket_dirty`; the claim recorded here \
         is that an undumped bucket is what the actuator declines, and another term would mean \
         something else is stopping it: {:?}",
        shipped_terms.refusals.bucket_dirty, shipped_terms.refused_buckets, shipped_terms.refusals
    );

    assert!(
        shipped.victims > 0 && dumped.victims > 0,
        "both arms must have chosen victims, or neither measured an actuator: {} and {}",
        shipped.victims,
        dumped.victims
    );
    assert_eq!(
        shipped.released_blocks, 0,
        "the shipped setting released {} blocks; this test exists to record that it releases \
         none, and a number here means the refusal it is built on has changed",
        shipped.released_blocks
    );
    assert_eq!(
        shipped.refused, shipped.victims,
        "at the shipped setting every victim must be refused, got {} refused of {} victims",
        shipped.refused, shipped.victims
    );
    assert!(
        dumped.released_blocks > 0,
        "with the dump on the actuator released {} blocks; if this is zero too then the release \
         path is inert at every setting and the finding is a different one",
        dumped.released_blocks
    );
    assert!(
        dumped.index_bytes_after < dumped.index_bytes_before,
        "with the dump on the resident bucket index did not shrink ({} -> {}), so the release \
         freed nothing the pressure gate can see",
        dumped.index_bytes_before,
        dumped.index_bytes_after
    );
    assert_eq!(
        shipped.index_bytes_after, shipped.index_bytes_before,
        "the shipped setting changed the resident index ({} -> {}) while releasing nothing",
        shipped.index_bytes_before,
        shipped.index_bytes_after
    );
}

// -------------------------------------------------------------------------------------------
// DIRECTION. Evicting too little is memory held; evicting too much is data loss, and silent.
// -------------------------------------------------------------------------------------------

/// A ROUND LOSES NOTHING, CHECKED ELEMENT BY ELEMENT AGAINST A CONTROL.
///
/// The strong form, in both arms. Not "the store is still non-empty" and not "the keys are still
/// there": the live-page COUNT for every key before the round is compared with the count after,
/// key by key, so a key that survived with one of its two pages dropped fails here. The control
/// is the same map taken on the same engine immediately before the round.
///
/// Both settings, because they take different paths through the actuator: one refuses every
/// victim and one releases them. A release drops RESIDENT entries and keeps the model maps
/// whole -- that is what makes it reversible -- so the live-page map must be identical across it.
#[test]
fn an_eviction_round_leaves_every_live_key_and_block_exactly_where_it_was() {
    for dump in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, GUARD_SMALL);

        let before = live_blocks_by_key(&engine);
        let buckets_before = objects_by_bucket(&engine);
        assert_eq!(
            before.len(),
            GUARD_SMALL,
            "dump={dump}: the control map holds {} keys for {GUARD_SMALL} seeded records, so it \
             is not describing the store this round runs on",
            before.len()
        );
        let cost = one_round(&engine, GUARD_SMALL, BATCH, dump);
        let after = live_blocks_by_key(&engine);

        assert!(
            cost.victims > 0,
            "dump={dump}: the round chose no victims, so it did nothing this could be the \
             control for"
        );
        let lost = before
            .iter()
            .filter(|(key, count)| after.get(*key) != Some(*count))
            .map(|(key, count)| (key.clone(), *count, after.get(key).copied()))
            .take(5)
            .collect::<Vec<_>>();
        assert!(
            lost.is_empty(),
            "dump={dump}: {} of {} keys changed their live-page count across one round -- \
             eviction is supposed to release RESIDENT state and lose nothing. First few \
             (key, before, after): {lost:?}",
            before
                .iter()
                .filter(|(key, count)| after.get(*key) != Some(*count))
                .count(),
            before.len()
        );
        assert_eq!(
            after.len(),
            before.len(),
            "dump={dump}: the store gained or lost keys across the round ({} -> {})",
            before.len(),
            after.len()
        );

        // THE SECOND DIRECTION, which the model maps cannot report. A released bucket keeps its
        // object index; a mutant that cleared it alongside the page list would leave every
        // assertion above green and the store uncountable.
        let buckets_after = objects_by_bucket(&engine);
        let emptied = buckets_before
            .iter()
            .filter(|(routing_bucket, count)| buckets_after.get(*routing_bucket) != Some(*count))
            .map(|(routing_bucket, count)| {
                (
                    *routing_bucket,
                    *count,
                    buckets_after.get(routing_bucket).copied(),
                )
            })
            .take(5)
            .collect::<Vec<_>>();
        assert!(
            emptied.is_empty(),
            "dump={dump}: a bucket's object index changed across the round -- a release drops \
             the resident page list and keeps the object index, so this must be EQUAL and not \
             merely non-zero. First few (bucket, before, after): {emptied:?}"
        );
    }
}

// -------------------------------------------------------------------------------------------
