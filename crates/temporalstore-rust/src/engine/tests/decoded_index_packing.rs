// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a decoded index's nested series cost, against the same entries packed in one go.
//!
//! WHY THIS EXISTS. `serde`'s `Deserialize` for `BTreeMap` fills the map one `insert` at a time,
//! and the bytes it reads were written in key order -- so every nested series in a decoded index is
//! built by ASCENDING insertion. A B-tree grown that way splits each full leaf in half and never
//! comes back to the left half, so the finished map carries leaves that are about half empty for
//! the rest of the process's life. Packing the same entries in one go fills every leaf.
//!
//! THE CONTROL READS THE SAME SET. The control below is not a separately seeded map: it is built
//! from the DECODED map's own entries, so the two hold the same keys and the same values by
//! construction and the only difference between them is tree shape. A fixture that seeded the
//! control independently could report a difference that was really a difference in contents.
//!
//! NON-VACUITY. Every ratio is printed beside the entry count it is divided by, and the entry
//! count is asserted against what was seeded before any ratio is taken. A walk over an empty
//! series map reports "0 bytes over 0 points", which reads exactly like a map that costs nothing.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::block_store::ElementEntry;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

const SHARD: u64 = 1;

/// A shard loaded on the SHIPPED routing range, not the bare whole-u32 default.
fn packing_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);
    engine
}

/// Seed `series_keys` feature series of `series_points` points each, plus a handful of strings and
/// one list, so the decode has more than one nested container shape to put back.
fn packing_seed(
    engine: &TemporalEngine,
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands,
        });
        assert!(
            response.status.ok,
            "string seed must ack: {:?}",
            response.status
        );
    }
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(
                response.status.ok,
                "feature seed must ack: {:?}",
                response.status
            );
        }
    }
    for i in 0..64u64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ListPush {
                key: "l0".to_string(),
                member: vec![b'l'; 16],
                left: false,
            },
        });
        assert!(
            response.status.ok,
            "list seed must ack at {i}: {:?}",
            response.status
        );
    }
}

/// The encoded index bytes of a freshly seeded shard, and the state they were taken from.
fn encoded_index(
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) -> (Vec<u8>, Vec<u8>, usize, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = packing_engine(dir.path());
    packing_seed(&engine, strings_n, series_keys, series_points);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let points: usize = shard.features().values().map(|series| series.len()).sum();
    let list_entries: usize = shard.lists.values().map(|list| list.len()).sum();
    let container = crate::engine::encode_index_bytes(shard);
    let plain = crate::engine::encode_index_bytes_as_plain_json(shard);
    (container, plain, points, list_entries)
}

/// The same entries, packed in one go: `collect` into a `BTreeMap` sorts and bulk-builds, so every
/// leaf is filled.
// GENERIC OVER THE SERIES, through the trait, because `features` holds the owned array while the
// other kinds still hold B-trees -- and this control is about the PACKING of the map it returns,
// not about which representation the series arrived in. `points()` promises ascending order, so
// the rebuilt B-tree is identical either way.
fn packed_like<K, S>(map: &HashMap<K, S>) -> HashMap<String, BTreeMap<u64, ElementEntry>>
where
    K: std::fmt::Display,
    S: crate::engine::timestamped_series::TimestampedSeries,
{
    map.iter()
        .map(|(key, series)| {
            (
                key.to_string(),
                series
                    .points()
                    .map(|(at, address)| (at, address.clone()))
                    .collect::<BTreeMap<u64, ElementEntry>>(),
            )
        })
        .collect()
}

/// Bytes the heap gives up to hold a deep copy of `value` -- which is what `value` itself holds.
///
/// A `BTreeMap` clone preserves tree shape, so the half of a leaf that an ascending insert left
/// empty is counted here exactly as it is counted in the shard.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}

// =================================================================================================
// CORRECTNESS: the entries a decode puts back are the entries that were encoded.
// =================================================================================================

/// The repack moves entries between nodes. This says the entries themselves are untouched -- same
/// keys, same values, same order, through BOTH container shapes a reader accepts.
#[test]
fn a_decode_puts_back_exactly_the_series_that_were_encoded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = packing_engine(dir.path());
    packing_seed(&engine, 200, 3, 400);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");

    // DENOMINATORS, before any comparison: an empty map compares equal to an empty map.
    assert_eq!(3, shard.features().len(), "denominator: three series seeded");
    let points: usize = shard.features().values().map(|series| series.len()).sum();
    assert_eq!(1_200, points, "denominator: 400 points in each of three series");
    let list_entries: usize = shard.lists.values().map(|list| list.len()).sum();
    assert_eq!(64, list_entries, "denominator: 64 list entries seeded");
    assert!(
        !shard.strings().is_empty(),
        "denominator: the string map must hold keys"
    );

    for (label, bytes) in [
        ("container", crate::engine::encode_index_bytes(shard)),
        (
            "plain json",
            crate::engine::encode_index_bytes_as_plain_json(shard),
        ),
    ] {
        let decoded = crate::engine::decode_index_bytes(&bytes)
            .unwrap_or_else(|error| panic!("{label} index must decode: {error}"));
        assert_eq!(
            shard.features(), decoded.features(),
            "{label}: the feature series must come back entry for entry"
        );
        assert_eq!(
            shard.lists, decoded.lists,
            "{label}: the list series must come back entry for entry"
        );
        assert_eq!(
            shard.control_state(), decoded.control_state(),
            "{label}: the control-state series must come back entry for entry"
        );
