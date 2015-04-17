// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY DOOR THAT TRUSTS A STORED INDEX NOW READS ITS STAMP, AND THESE ARMS DRIVE EACH ONE.
//!
//! # THE DEFECT THIS CLOSES
//!
//! `SHARD_INDEX_FORMAT_VERSION` exists because a stale index decodes CLEANLY -- the types are
//! unchanged -- while a field's MEANING has moved under it. The real instance is in the field's
//! own doc comment: `context_events` was rekeyed from `timeline_key` to `event_id_hash`, so a v1
//! index decodes with timeline keys sitting in the event-id slot and every time-windowed read
//! silently returns nothing.
//!
//! That constant is a claim about every door that turns stored bytes into a `ShardState` somebody
//! acts on. It was enforced at exactly ONE of them. `decode_index_bytes` has six production
//! callers and only `load_index_inner` compared the stamp; the version check inside
//! `decode_index_bytes` is on the msgpack-container arm and a plain-JSON index never meets it.
//!
//! Measured before this change: a stamp-13 image installed through
//! `install_bucket_dump_manifest` reached a live shard with all four `index_load_path_counts`
//! counters at ZERO. It did not skip the refusal -- it never entered the code the refusal lived
//! in. And the worst door was not an endpoint: the ordinary recovery load reads a durable dump
//! manifest's embedded index as its recovery base, so an old store with an old manifest met no
//! gate on a plain restart.
//!
//! # WHAT THIS MODULE IS FOR
//!
//! Eight arms, because a gate needs both halves driven and a behaviour change needs its
//! compatibility proved:
//!
//!   1, 2  the base-index door, refusing and accepting -- the comparator the rest are read
//!         against, and the door that already worked;
//!   3, 4  the dump-manifest install REFUSES, and refuses BEFORE it can write anything;
//!   5     the same manifest at the current stamp installs and round-trips its payload, so arm 3
//!         is the stamp and not a broken fixture;
//!   6     the ORDINARY recovery load refuses and falls back rather than failing;
//!   7     a manifest this binary actually writes still installs -- the arm that would catch this
//!         change refusing legitimate traffic;
//!   8     the preflight report names the blocker, so an operator sees it before trying.
//!
//! # THE PAYLOAD IS A `features` SERIES ON PURPOSE
//!
//! The measured data loss was a pre-fold `sequences` map, but that map is being removed in a
//! separate change on top of this one. A payload carried in `features` asks the identical
//! question -- does an object-keyed timestamped series survive this door -- and keeps these arms
//! meaningful after that removal lands.
//!
//! # WHY THE RETURN VALUES ARE THE EVIDENCE
//!
//! `index_load_path_counts` is four process-global atomics, so a parallel test's load moves them.
//! Every arm below asserts on something scoped to its own call -- a `Result`, a served response,
//! the presence of a file -- and treats the counters as corroboration, printed and asserted only
//! in the direction that can misreport as a pass rather than as a failure.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;

const SHARD: ShardId = 11;
const SERIES_KEY: &str = "series/written-before-the-stamp-moved";
const ROW_TIMESTAMP_MS: u64 = 1_700_000_000_123;

/// A source shard with real pages and a real dump manifest over it.
struct DumpFixture {
    dir: tempfile::TempDir,
    manifest: crate::engine::reports::BucketDumpManifest,
}

/// Seed a shard, give it a `features` series, and take a real manifest over the result.
fn a_real_dump_manifest() -> DumpFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    for i in 0..6 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("seed-{i}"),
                value: format!("seed-value-{i}").into_bytes(),
            },
        });
        assert!(response.status.ok, "seeding write {i}: {response:?}");
    }
    // The payload every arm tracks, written into the live shard so the manifest carries it the
    // way a real dump would rather than being edited in afterwards.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&SHARD).expect("the shard is loaded");
        let mut series = BTreeMap::new();
        series.insert(
            ROW_TIMESTAMP_MS,
            ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
        );
        shard.features_mut().insert(SERIES_KEY.into(), series.into());
    }
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a dump manifest over a seeded shard");
    // DENOMINATORS, both of them. An empty image or a manifest that never carried the payload
    // would make every arm below pass over nothing.
    assert!(
        !manifest.index_bytes.is_empty(),
        "the manifest carries no index image, so there is nothing for a door to trust"
    );
    assert!(
        crate::engine::decode_index_bytes(&manifest.index_bytes)
            .expect("a manifest this binary wrote must decode")
            .features()
            .contains_key(SERIES_KEY),
        "the manifest's image does not carry the series, so the payload arms are about nothing"
    );
