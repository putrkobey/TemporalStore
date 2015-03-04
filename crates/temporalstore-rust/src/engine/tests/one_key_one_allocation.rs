// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE KEY, ONE ALLOCATION -- the one-copy invariant, in a test a DEFAULT run executes.
//!
//! # WHY THIS FILE EXISTS BESIDE THE PROBES THAT ALREADY CHECK THIS
//!
//! `index_bytes_per_key` carries three guards on this invariant and a default `cargo test` runs
//! NONE of them: each is `#[cfg(feature = "alloc-probe")]` AND `#[ignore]`, so it needs a feature
//! flag and `--ignored` to execute. Of that file's 21 tests, 14 are ignored. An assertion has three
//! states -- true, false, and never evaluated -- and those were in the third.
//!
//! That matters more here than usual because of HOW the invariant breaks. A map that could share a
//! key allocation but is missing from `adopt_object_key_allocations` keeps its own copy and
//! **nothing fails**: the data is correct, every read answers, and a clone-based byte probe cannot
//! see the duplicate, because cloning an `Arc` is a refcount bump. Twice in one session a map was
//! found holding a second copy only because a probe was widened by hand -- first
//! `control_state_selection`, then `seen` and `buckets`. A guard that only runs when someone
//! remembers to pass `--ignored` would not have caught either.
//!
//! So this is the cheap version: eight keys, no counting allocator, no corpus, nothing ignored. It
//! measures by POINTER, which is the only way to tell one shared allocation from two equal copies.
//! The expensive probes keep their breakdowns and their byte figures; this one keeps the invariant
//! reachable.
//!
//! rust-internal: measures this crate's own resident structures, no product behaviour

use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::types::{Command, ExecuteRequest};
use crate::TemporalEngine;
use std::sync::Arc;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

#[test]
fn a_key_held_by_several_maps_is_one_allocation() {
    const KEYS: usize = 8;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // The same raw key through every family that keys a per-key map by it. Each command is here
    // because it puts the key in a DIFFERENT map; drop one and this stops covering that map.
    for index in 0..KEYS {
        let raw = format!("one-copy-{index}");
        for command in [
            Command::StringSet {
                key: raw.clone(),
                value: vec![b'v'; 16],
            },
            Command::FeatureAppend {
                key: raw.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_787_270_070_000 + index as u64,
                    value: vec![b'f'; 8],
                }],
            },
            Command::ControlStateSelectionSet {
                key: raw.clone(),
                value: vec![b's'; 8],
                occur_time_ms: 1_787_270_070_000 + index as u64,
                ttl_ms: 0,
                selection_type: crate::types::ControlStateSelectionType::Last,
            },
            Command::SeenCheck {
                key: raw.clone(),
                member: vec![b'm'; 8],
                window_ms: 60_000,
            },
            Command::BucketTake {
                key: raw.clone(),
                tokens: 1.0,
                capacity: 10.0,
                refill_per_sec: 1.0,
            },
            // The counter command, and it is here because its ABSENCE is what let a duplicate
            // live: with nothing writing `control_state`, that map reported zero keys, which
            // reads exactly like a map with nothing to share. It was in fact holding a second
            // copy of these characters, for every key.
            Command::ControlStateIncrement {
                key: raw.clone(),
                timestamp_ms: 1_787_270_070_000 + index as u64,
                amount: 1,
            },
            // THE SEVENTH HOLDER, and it was invisible to this guard until it was named. The
            // distinct-change map is filled by `hll::record_change` with
            // `.entry(key.to_string())` -- a fresh allocation per key, from a `&str` -- and its
            // key is this same raw object key. A guard that collects from a list of map names
            // cannot see a holder that is not on the list, which is the standing risk its own
            // doc names and this is that risk arriving.
            Command::ControlStateChangeAdd {
                key: raw.clone(),
                timestamp_ms: 1_787_270_070_000 + index as u64,
                value: format!("member-{index}").into_bytes(),
                precision_ms: None,
                ttl_ms: None,
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // (text, address of its characters) from every map keyed by the raw key. A `String` key and an
    // `Arc<str>` key both expose `as_ptr()`, so this needs no list of which map is keyed by what --
    // and an allowlist of map NAMES is what went stale the first time this guard was written.
    let mut held: std::collections::BTreeMap<&str, Vec<(&str, usize)>> =
        std::collections::BTreeMap::new();
    for (name, keys) in [
        (
            "strings",
            shard
                .strings()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect::<Vec<_>>(),
        ),
        (
            "features",
            shard
                .features()
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_selection",
            shard
                .control_state_selection
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "seen",
            shard
                .seen
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "buckets",
            shard
                .buckets
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        // Only the RAW-keyed entries of this map can ever share. The family operators file a
        // decorated `control_state:<family>:<key>` that no other map holds, so those are
        // filtered out rather than counted as holders that failed to share.
        (
            "control_state",
            shard
                .control_state()
                .keys()
                .filter(|k| !k.contains(':'))
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
        (
            "control_state_changes",
            shard
                .control_state_changes
                .keys()
                .map(|k| (&**k, k.as_ptr() as usize))
                .collect(),
        ),
    ] {
        for (text, ptr) in keys {
            held.entry(text).or_default().push((name, ptr));
        }
    }

    // --- DENOMINATORS, because every claim below divides by them. ---
    let multi: Vec<_> = held.iter().filter(|(_, h)| h.len() > 1).collect();
    assert_eq!(
        KEYS,
        multi.len(),
        "the fixture wrote {KEYS} keys through seven families but only {} ended up in more than \
         one map, so this run could not have detected a duplicate at all. Held: {:?}",
        multi.len(),
        held
    );
    assert!(
        multi.iter().all(|(_, h)| h.len() == 7),
        "a key reached fewer than seven maps, so one of the seven commands stopped writing its \
         map and this guard silently stopped covering it: {:?}",
        multi
    );

    // --- THE INVARIANT. ---
    let duplicated: Vec<_> = held
        .iter()
        .filter(|(_, h)| {
            h.iter()
                .map(|(_, p)| *p)
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1
        })
        .collect();
    assert!(
        duplicated.is_empty(),
        "{} key texts are stored more than once -- their holders report different addresses for \
         the same characters: {:?}. A map holding a key another map already holds must take a \
         `ModelKey` and join `ShardState::adopt_object_key_allocations`; nothing else fails when \
         it does not, because cloning an `Arc` is a refcount bump.",
        duplicated.len(),
        &duplicated[..duplicated.len().min(3)]
    );
}

/// EVERY ADOPTED CONTEXT MAP HOLDS THE PAGE INDEX'S OWN KEY -- the other half of the invariant,
/// and the half the guard above cannot reach.
///
/// # WHY A SECOND TEST AND NOT MORE COMMANDS IN THE FIRST
///
/// The guard above works by finding one key text in several maps and comparing their addresses.
/// That shape cannot cover the context family at all, and the reason is structural rather than a
/// shortfall in the fixture: each context map is keyed by its OWN COMPOSED KEY -- `ctx:node:`,
/// `ctx:event:`, `ctxidx:`, `ctx:audit:`, `ctx:entity:`, `ctx:child:`, `ctx:summary:`,
/// `ctx:compress:` -- so no two of them ever hold the same characters, and a cross-map address
/// comparison has nothing to compare. Only the event pair shares one text by construction.
///
/// So the invariant has to be stated against the thing they all borrow FROM.
/// `adopt_object_key_allocations` takes its handle from
/// `bucket_index.object_block_lookup.any_shared_object_key`, which means a correctly adopted map
/// does not merely avoid duplicating a SIBLING -- its key IS the page index's allocation. That is
/// checkable per map with no sibling needed, and it is strictly stronger: a single-holder map that
/// mints its own copy duplicates the page index's key, and this catches it where a sibling
/// comparison would report nothing at all.
///
/// # WHAT THIS CLOSES
///
/// `ShardState` has 27 resident maps and the adoption list names 21. Before this, a default `cargo
/// test` measured 6 of them -- the context maps were measured in the changes that converted them
/// and by nothing standing since. An invariant whose only evidence is a past measurement has no
/// guard: a map can be re-keyed, an adoption line deleted, or a write arm moved to mint its own
/// handle, and every read still answers.
///
/// # AND THREE MAPS THIS FIXTURE CANNOT SPEAK FOR, WHICH IT NAMES RATHER THAN PASSES OVER
///
/// `NOT_REACHABLE` below names them with the measured reason, and each exclusion carries an
/// assertion that FAILS if the reason stops being true -- an excused map that is silently excused
/// forever is the same third state of an assertion this file exists to remove.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn every_adopted_context_map_holds_the_block_index_own_key() {
    use crate::types::{
        ContextChildRef, ContextCompressionEvent, ContextEntity, ContextEvent, ContextIndexRef,
        ContextNode, ContextPackAudit, ContextSummary,
    };

    const NODES: u64 = 4;
    const TENANT: u64 = 7;
    const NOW_MS: u64 = 1_787_270_070_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    // One write per context family per node. Each command is here because it files a DIFFERENT
    // map; drop one and this stops covering that map, which is the failure mode the first guard's
    // doc describes.
    for node in 1..=NODES {
        for command in [
            Command::ContextUpsertNode {
                tenant_hash: TENANT,
                node: Box::new(ContextNode {
                    node_hash: node,
                    parent_hash: 0,
                    kind: 1,
                    canonical_name: format!("node-{node}"),
                    l0: format!("summary of node {node}"),
                    status: 0,
                    last_event_time_ms: NOW_MS,
                    l1_ref: String::new(),
                    raw_metadata_ref: String::new(),
                    vector: Vec::new(),
                    embedding_model_hash: 0,
                    embedding_updated_at_ms: 0,
