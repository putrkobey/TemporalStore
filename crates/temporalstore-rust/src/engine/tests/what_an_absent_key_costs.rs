// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What it costs to answer "this key does not exist".
//!
//! WHY THIS EXISTS. Right after a cold start or a failover many readers retry at once for keys that
//! are not there. If each of those answers costs physical work, a burst of requests whose every
//! answer is "no" turns into a burst of storage work. So the question is not whether the store
//! answers correctly -- it does -- but what the answer COSTS, and whether it costs more when the
//! key's bucket is not resident.
//!
//! THE HALF THAT WAS ALREADY CHEAP, AND IT IS THE HALF WORTH SAYING FIRST. An absent key never
//! touches the block store. Not when its bucket is resident, not when its bucket has been released,
//! and not when its bucket has never existed: 0 block-store reads per lookup in all three arms, at
//! 500, 2,000 and 8,000 keys. There is no bucket reload, no page read and no manifest consultation
//! behind a "no". That is a clear negative and no plant can falsify it, which is said plainly at
//! the arm that holds it rather than left to look like a proven claim.
//!
//! AND THE READ PATH CAN TELL "ABSENT" FROM "NOT RESIDENT", WHICH IS WHY. A release drops a
//! bucket's `block_index` and its object-lookup entries and leaves the model maps alone -- the model
//! maps are what `reload_released_bucket` rebuilds FROM. So `shard.string_address(key)` is one map
//! get that answers a released bucket's keys exactly as it answers a resident one's, and the read
//! path never has to ask storage about residency in order to answer about existence.
//!
//! THE HALF THAT WAS NOT CHEAP, AND WHAT IT WAS. The two read routes were not the same price:
//!
//!   * `engine.execute` resolves through `read_bucket_index_value`, which asks
//!     `bucket_index_block_address` for an address and returns `None` without calling
//!     `read_block_bytes` when there is none. Nothing below the resident maps was touched: 0
//!     descents, 0 cache admissions.
//!
//!   * `engine.execute_durable` -- the route a deployment serves `GET` through, and the only one
//!     that enters `execute_read_only_fast_path` -- held `address: shard.string_address(key)` and
//!     then wrapped the read in `cached_response`, which is CACHE-FIRST. So an absent key descended
//!     every tier before anything looked at the address it already had: the memory tier, the
//!     persistent-memory tier, the SSD tier's store, and then `fs::read` on the entry's own disk
//!     path -- a failing `openat` for a key that was never written. Then it admitted the negative
//!     answer to the memory tier, so a burst of distinct absent keys was cache pressure too.
//!
//! MEASURED, BEFORE: 1.000 tier descents and 1.000 cache admissions per absent lookup on the
//! deployed route, flat across 500 / 2,000 / 8,000-key stores, identical in all three bucket
//! residencies, against 0.000 and 0.000 on the direct route for the same keys in the same process.
//!
//! THE REMEDY IS NOT A NEW STRUCTURE, AND THAT IS THE RESULT. The thing that can say "definitely
//! absent" without a read already exists, is already read two statements earlier in the same
//! function, and is EXACT: `shard.string_address(key)`. It costs no memory, has no false-positive
//! rate, and needs no invalidation story, because it is not a summary of the model map -- it IS the
//! model map. A sketch or filter sized for this would have been a second, approximate copy of an
//! answer already in hand. The change is that the fast path stops throwing that answer away, and
//! the sibling `FastPathRead::Hash` arm ten lines below it already worked this way.
//!
//! WHAT THIS ASSERTS NOW. Zero descents and zero cache admissions for an absent key, on both
//! routes, in all three residencies. And a PRESENT key, on the same fixture in the same process,
//! still paying a descent on its first read and still being served from memory on its second --
//! because a claim that a cost is gone is only falsifiable against a path that still pays it, and
//! because a change that had removed the cache from this route altogether would otherwise read as a
//! success in every arm here.
//!
//! ONE CONCLUSION PER TEST, DELIBERATELY. A plant stops at the first failing assertion, so a second
//! conclusion sitting behind an overlapping first one can never be reached by its own plant. The
//! descent clause and the admission clause are therefore separate tests per residency, each with
//! its own fixture and its own regime assertions ahead of it.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A probe whose buckets are all resident never reaches the
//! contested case and would agree with whatever it was asked. Each arm asserts the residency of the
//! bucket its absent keys ROUTE TO -- present and in memory, present and released, or absent from
//! the map entirely -- and fails when the fixture stops reaching that state. Each arm also asserts
//! that its keys really are absent and that its present keys really answer the bytes they were
//! written with, because a probe that accidentally reads a live key is measuring a hit.
//!
//! THE DENOMINATOR IS NAMED. `CacheStats::misses` counts exactly the reads that fell through every
//! tier, which is to say the reads that reached `read_ssd_block` and found nothing -- one SSD-store
//! `get` plus one failing `fs::read` each. `expired_reads` is the only other contributor to that
//! counter, and every span asserts its delta is zero, so a miss here is a tier descent and nothing
//! else. The figures are reported PER LOOKUP over the lookups driven.
//!
//! WHAT IS DELIBERATELY NOT CLAIMED. This never said the descent was slow in wall time; a
//! bloom-filtered store `get` and a failing `openat` are both cheap individually. It said the
//! descent HAPPENED, once per distinct absent key, unconditionally, and that it admitted a cache
//! entry. Those are counts, and counts are what a retry burst multiplies.

#![allow(clippy::all)]
#![allow(dead_code)]
use super::*;

const SHARD: u64 = 1;
/// Probe lookups per span. Every per-lookup figure below is over this.
const LOOKUPS: usize = 200;
/// The store size the single-size arms use. The sweep arm is what varies it.
const PROBE_KEYS: usize = 2_000;

/// The narrow range the existing-bucket arms use: 32 buckets, so every absent key routes into one
/// of them and the seed fills all of them.
const NARROW_START: u32 = 0;
const NARROW_END: u32 = 31;

/// A key that was never written, numbered so two spans cannot share one.
fn absent_key(arm: &str, index: usize) -> String {
    format!("absent-{arm}-{index:06}")
}

/// A key the fixture seeded.
fn present_key(index: usize) -> String {
    format!("seeded-{index:06}")
}

/// The value a seeded key holds, derived from the key so a read of the WRONG key fails loudly.
fn seeded_value(index: usize) -> Vec<u8> {
    let mut value = format!("{index:06}:").into_bytes();
    value.resize(96, b'v');
    value[95] = (index % 251) as u8;
    value
}

/// `keys` string records over a chosen routing range, dumped so their buckets are releasable.
///
/// THE RANGE IS A PARAMETER BECAUSE IT DECIDES THE SHAPE. At the default `0..u32::MAX` every key
/// gets a bucket of its own, so an absent key's bucket is almost certainly not in the map at all --
/// which is one of the three arms and is useless for the other two. A narrow range puts many keys
/// in each bucket, so every absent key routes into a bucket that EXISTS and whose residency the arm
/// can then set.
fn fixture(
    keys: usize,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> (tempfile::TempDir, TemporalEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: present_key(index),
                value: seeded_value(index),
            },
        });
        assert!(response.status.ok, "seed {index}: {:?}", response.status);
    }
    // A dirty bucket refuses release, so the dump is what makes the released arm reachable at all.
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a loaded shard can be dumped");
    engine.clear_dumped_bucket_dirty_state(SHARD, &manifest);
    // REGIME: the seed landed. An empty store has no buckets at all, and every arm below would
    // collapse into the no-bucket one without saying so.
    let seeded = engine
        .shards
        .read()
        .expect("shards lock poisoned")
        .get(&SHARD)
        .expect("shard is loaded")
        .strings()
        .len();
    assert_eq!(
        keys, seeded,
        "the seed did not land, so nothing measured on this fixture means anything"
    );
    (dir, engine)
}

/// Which route a lookup takes. Both answer `GET`; only one of them is the route a deployment uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// `engine.execute` -- no storage override, so `execute_read_only_fast_path` is never entered.
    Direct,
    /// `engine.execute_durable` -- what `RecordStore::Local` serves reads through, and the only
    /// route into the fast path.
    Durable,
}

fn read_through(engine: &TemporalEngine, route: Route, key: &str) -> Option<Vec<u8>> {
    let request = ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: key.to_string(),
        },
    };
    let response = match route {
        Route::Direct => engine.execute(request),
        Route::Durable => engine.execute_durable(request),
    };
    assert!(
        response.status.ok,
        "{route:?} read of {key}: {:?}",
        response.status
    );
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("{route:?} read of {key} answered {other:?}"),
    }
}

/// The physical work one span of lookups did, taken as a delta.
///
/// `block_store_file_read_counts` is PROCESS-WIDE and shared by every store in the process, so it
/// is only meaningful as a delta across a span -- which is what this is, and why these arms run on
/// one thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Work {
    /// Cache reads that fell through every tier: one SSD-store `get` plus one failing `fs::read`.
    cache_misses: u64,
    /// Cache entries admitted. On an absent key this was the negative answer being cached.
    cache_puts: u64,
    /// Reads served from the memory tier.
    cache_memory_hits: u64,
    /// Entries the memory tier evicted to make room.
    cache_memory_evictions: u64,
    /// Reads that found an entry past its time to live. The only other contributor to
    /// `cache_misses`, asserted zero so a miss is a tier descent and nothing else.
    cache_expired_reads: u64,
    /// Read calls against the block store's own files.
    block_store_reads: u64,
    /// Bytes read off the block store's own files.
    block_store_bytes: u64,
}

impl Work {
    fn take(engine: &TemporalEngine) -> Self {
        let stats = engine.cache().stats();
        let (bytes, reads) = crate::block_store::block_store_file_read_counts();
        Self {
            cache_misses: stats.misses,
            cache_puts: stats.puts,
            cache_memory_hits: stats.memory_hits,
            cache_memory_evictions: stats.memory_evictions,
            cache_expired_reads: stats.expired_reads,
            block_store_reads: reads,
            block_store_bytes: bytes,
        }
    }

    fn since(self, before: Work) -> Self {
        Self {
            cache_misses: self.cache_misses.saturating_sub(before.cache_misses),
            cache_puts: self.cache_puts.saturating_sub(before.cache_puts),
            cache_memory_hits: self
                .cache_memory_hits
                .saturating_sub(before.cache_memory_hits),
            cache_memory_evictions: self
                .cache_memory_evictions
                .saturating_sub(before.cache_memory_evictions),
            cache_expired_reads: self
                .cache_expired_reads
                .saturating_sub(before.cache_expired_reads),
            block_store_reads: self
                .block_store_reads
                .saturating_sub(before.block_store_reads),
            block_store_bytes: self
                .block_store_bytes
                .saturating_sub(before.block_store_bytes),
        }
    }

    fn print(&self, label: &str, route: Route, keys: usize, lookups: usize) {
        println!(
            "{label:<20} route={:<8} keys={keys:<6} misses={:<5} puts={:<5} mem_hits={:<5} \
             mem_evict={:<5} bs_reads={:<5} bs_bytes={:<9}  misses/lookup={:.3} \
             puts/lookup={:.3} bs_reads/lookup={:.3}",
            format!("{route:?}"),
            self.cache_misses,
            self.cache_puts,
            self.cache_memory_hits,
            self.cache_memory_evictions,
            self.block_store_reads,
            self.block_store_bytes,
            self.cache_misses as f64 / lookups as f64,
            self.cache_puts as f64 / lookups as f64,
            self.block_store_reads as f64 / lookups as f64,
        );
    }
}

/// How the bucket an absent key routes to is expected to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residency {
    /// In the bucket map and holding its pages.
    ResidentBucket,
    /// In the bucket map, pages released, reloadable.
    ReleasedBucket,
    /// Not in the bucket map at all.
    NoBucketAtAll,
}

/// Assert that every probe key in this span routes to a bucket in the expected state.
///
/// THIS IS THE REGIME CLAUSE AND IT COMES FIRST. Without it a span named "released" measures a
/// resident bucket the moment the fixture's range or seed changes, and agrees with whatever it is
/// asked.
fn assert_routing_regime(
    engine: &TemporalEngine,
    arm: &str,
    residency: Residency,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    let shards = engine.shards.read().expect("shards lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let mut checked = 0usize;
    for index in 0..LOOKUPS {
        let key = absent_key(arm, index);
        let routing_bucket = crate::engine::hashing::block_routing_bucket(
            &key,
            start_routing_bucket,
            end_routing_bucket,
        );
        let bucket = shard.bucket_index.bucket_map.get(&routing_bucket);
        match residency {
            Residency::ResidentBucket => {
                let bucket = bucket.unwrap_or_else(|| {
                    panic!(
                        "{arm}: key {key} routes to bucket {routing_bucket}, which is NOT in the \
                         map -- this span is not measuring a resident bucket"
                    )
                });
                assert!(
                    bucket.in_memory(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, which is in the map but \
                     NOT in memory -- this span is not measuring a resident bucket"
                );
                assert!(
                    !shard.bucket_index.released_buckets.contains(&routing_bucket),
                    "{arm}: bucket {routing_bucket} is in the released registry"
                );
            }
            Residency::ReleasedBucket => {
                let bucket = bucket.unwrap_or_else(|| {
                    panic!(
                        "{arm}: key {key} routes to bucket {routing_bucket}, which is NOT in the \
                         map -- a released bucket must stay routable, so this span is measuring \
                         the wrong thing"
                    )
                });
                assert!(
                    !bucket.in_memory() && bucket.meta_loaded() && !bucket.loading(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, whose residency flags do \
                     not say released -- this span is not measuring a non-resident bucket"
                );
                assert!(
                    shard.bucket_index.released_buckets.contains(&routing_bucket),
                    "{arm}: bucket {routing_bucket} is not in the released registry"
                );
            }
            Residency::NoBucketAtAll => {
                assert!(
                    bucket.is_none(),
                    "{arm}: key {key} routes to bucket {routing_bucket}, which IS in the map -- \
                     this span is not measuring a bucket that does not exist"
                );
            }
        }
        checked += 1;
    }
    assert_eq!(
        LOOKUPS, checked,
        "{arm}: the regime was checked for {checked} of {LOOKUPS} keys"
    );
}

/// Release every bucket on the shard, asserting that it actually happened.
///
/// THE RELEASE IS A REGIME CLAUSE, NOT A STEP. A release that reports a number and drops nothing
/// leaves the arm after it measuring a resident bucket under a released arm's name.
fn release_everything(engine: &TemporalEngine) {
    let (released_buckets, released_blocks, refused) =
        engine.release_all_releasable_bucket_index_blocks(SHARD);
    assert!(
        released_buckets > 0 && released_blocks > 0,
        "nothing was released ({released_buckets} buckets / {released_blocks} pages, {refused} \
         refused), so what follows is not about a released bucket"
    );
    let resident_blocks: usize = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&SHARD)
            .expect("shard is loaded")
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum()
    };
    assert_eq!(
        0, resident_blocks,
        "{released_blocks} pages were reported released but {resident_blocks} are still filed, so \
         the release reported a number and dropped nothing"
    );
}

/// Drive `LOOKUPS` ABSENT-key lookups on one route and count what they cost.
fn measure_absent(engine: &TemporalEngine, arm: &str, route: Route, keys: usize) -> Work {
    let before = Work::take(engine);
    let mut answered_absent = 0usize;
    for index in 0..LOOKUPS {
        let key = absent_key(arm, index);
        // A key that answers bytes is not an absent key, and everything after this would be the
        // cost of a HIT reported as the cost of a miss.
        assert_eq!(
            None,
            read_through(engine, route, &key),
            "{arm}/{route:?}: {key} was supposed to be absent"
        );
        answered_absent += 1;
    }
    assert_eq!(
        LOOKUPS, answered_absent,
        "{arm}/{route:?}: {answered_absent} of {LOOKUPS} lookups ran"
    );
    let work = Work::take(engine).since(before);
    assert_eq!(
        0, work.cache_expired_reads,
        "{arm}/{route:?}: {} expired reads landed in this span, so a cache miss here is not purely \
         a tier descent and the denominator is not what it says it is",
        work.cache_expired_reads
    );
    work.print(arm, route, keys, LOOKUPS);
    work
}

/// Drive `LOOKUPS` PRESENT-key lookups on one route and count what they cost.
///
/// THIS IS THE FALSIFIER, NOT A SECOND FINDING. The claim everywhere else is that one specific cost
/// is gone for ABSENT keys. A change that had removed the cache from this route altogether would
/// make every absent arm read zero and would be indistinguishable from the intended one -- so an
/// arm that STILL PAYS has to stay reachable, in the same process, on the same fixture.
fn measure_present(engine: &TemporalEngine, arm: &str, route: Route, keys: usize) -> Work {
    assert!(
        LOOKUPS <= keys,
        "{arm}: the fixture seeded {keys} keys and this span wants {LOOKUPS}"
    );
    let before = Work::take(engine);
    let mut answered_present = 0usize;
