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
