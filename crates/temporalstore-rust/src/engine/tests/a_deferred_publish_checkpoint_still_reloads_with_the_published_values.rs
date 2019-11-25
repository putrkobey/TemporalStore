// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SKIPPED SERVED-INDEX CHECKPOINT MUST NOT LOSE A PUBLISHED VALUE.
//!
//! WHAT THIS IS FOR. `publish_shard_index_snapshot_for_keys` republished the pages of the keys it
//! was NAMED and then re-encoded and rewrote the whole served index, at a cost set by the size of
//! the STORE rather than the size of the publish --
//! `what_one_visibility_publish_writes_grows_with_the_store_not_the_publish` prices that at 376,012
//! encoded bytes to publish ONE key on an 8,000-key store, against a figure that does not move when
//! the named set widens 256-fold on a fixed store. The publish now spends the keys it published
//! against `ShardState::publish_index_flush_countdown` and only checkpoints when they run it out,
//! so most publishes write no index at all.
//!
//! THE WHOLE SAFETY CLAIM IS THAT REPLAY RE-DERIVES WHAT THE CHECKPOINT DEFERRED, and it is an
//! argument until something drives it. A publish relocates log-resident blocks into the block store
//! and repoints the index rows at them. It does NOT retire those blocks' registrations and it does
//! NOT clear any bucket's `first_dirty_wal_sequence` -- which is written to zero at exactly one
//! site, and only by a durable dump -- so both WAL retention floors still sit below the records it
//! rewrote: `min_registered_sequence` and the per-bucket first-dirty sequence the reclaim plan
//! reads. A reload therefore finds a served index with an older anchor and the records beyond it
//! still in the log, which is exactly the state replay exists for. This test puts a store into that
//! state on purpose -- publishes whose checkpoint was skipped, COUNTED, not assumed -- drops the
//! engine so only the disk is left, reopens over the same directories, and reads every value back.
//!
//! THE CONTESTED CASE IS ASSERTED, NOT HOPED FOR. If every publish in the arm happened to be due
//! then nothing was deferred and a clean reload proves nothing at all, so the arm counts the
//! publishes that published keys and wrote no index and refuses to conclude without at least one.
//! It also refuses to conclude without at least one key actually published: a publish that found
//! nothing log-resident reloads identically whatever the checkpoint did.
//!
//! AND IT ASSERTS THE OTHER DIRECTION IN THE SAME BREATH. "Every published value came back" is also
//! what a reload would report if it had quietly served the pre-publish copy of everything, so the
//! arm compares the FULL key set and every value BYTE FOR BYTE in both directions -- nothing
//! missing and nothing extra -- rather than checking that the published keys are present.
//!
//! THE SEED GOES THROUGH THE ASYNC-STORAGE ARM because that is the only write path that mints a
//! log-resident block address, and therefore the only one that leaves a publish anything to do. It
//! is also the regime the production caller runs in: the proxy's `matrixark_publish_visibility`
//! exists for stores opened with `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the encode happens
//! on the calling thread, so a single-threaded arm sees all of it and only it.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 31;
/// Resident keys. Well under the 4,096 default resident-block bound, so every seeded key is still
/// log-resident when the publishes start and none of them was swept out behind the test's back.
const KEYS: usize = 600;
const PUBLISHES: usize = 20;
/// Keys per publish. 20 x 4 = 80 published keys against the shipped interval of 64, so the arm
/// crosses it once and the rest of the publishes are deferred.
const PER_PUBLISH: usize = 4;

fn key_at(item: usize) -> String {
    format!("published-{item:06}")
}

/// An engine over `dir`, with the shard loaded. Reopening over the same `dir` is the reload.
fn engine_over(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

/// Seed string records through the async-storage arm, so their blocks are log-resident.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: key_at(item),
                    value: value_for(item),
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
    }
}

/// A value whose bytes name the key they belong to, so a reload that resolves the WRONG block is a
/// mismatch here rather than a silent pass.
fn value_for(item: usize) -> Vec<u8> {
    format!("value-for-{item:06}-{}", "p".repeat(100)).into_bytes()
}

/// Every seeded key's value as the engine serves it, read through the public command surface so a
/// reload is exercised the way a reader exercises it.
fn served_values(engine: &TemporalEngine) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut served = std::collections::BTreeMap::new();
    for item in 0..KEYS {
        let key = key_at(item);
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: key.clone() },
        });
        if !response.status.ok {
            continue;
        }
        if let CommandResponse::Bytes { value: Some(bytes) } = response.response {
            served.insert(key, bytes);
        }
    }
    served
}

/// One arm's reading.
struct PublishShape {
    /// Publishes that actually published something. A publish that published nothing is not a
    /// publish.
    publishes_with_keys: usize,
    /// Publishes that encoded the whole base index.
    materializations: usize,
    /// Publishes that published keys and encoded NOTHING. The contested case: if this is zero the
    /// arm never entered the regime it is arguing about and proves nothing.
    skipped: usize,
    /// Bytes encoded across the whole arm.
    encode_bytes: u64,
    /// The keys the arm published, in order.
    published: Vec<String>,
    /// How many keys EACH publish published, in order.
    ///
    /// The identity below replays the countdown over this rather than assuming every publish
    /// published the same number: a publish whose concurrent-write check skips a key moves where
    /// the next checkpoint falls, and a closed-form "about N/K" would read that as a defect.
    published_per_publish: Vec<u64>,
}

/// How many checkpoints a countdown starting at 0 (DUE, the post-reload value) owes over
/// `published_per_publish` at `interval`.
///
/// Written as the arithmetic the implementation does rather than as a ratio, which is what makes
/// it fail when the countdown is reloaded with the wrong number, is not spent at all, or is reset
/// on a publish that was not due.
fn materializations_due(published_per_publish: &[u64], interval: u64) -> usize {
    let mut countdown: u64 = 0;
    let mut due = 0usize;
    for published in published_per_publish {
        countdown = countdown.saturating_sub(*published);
        if countdown == 0 {
            countdown = interval;
            due += 1;
        }
    }
    due
}

/// Run `PUBLISHES` narrow publishes against `engine`, reading what each one encoded.
fn drive_publishes(engine: &TemporalEngine) -> PublishShape {
    let mut shape = PublishShape {
        publishes_with_keys: 0,
        materializations: 0,
        skipped: 0,
        encode_bytes: 0,
        published: Vec::new(),
        published_per_publish: Vec::new(),
    };
