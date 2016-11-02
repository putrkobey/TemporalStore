// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The committed-entry apply loops against the durable engine-WAL commit.
//!
//! `execute_raft_apply_batch_at` turns every otherwise-ok response into `wal_commit_failed`
//! when the coalesced barrier fails, so that raft apply "surfaces the durability failure
//! instead of acking". These tests hold both apply loops to that contract. The loops own the
//! monotonic exactly-once floor `max_applied_index`, documented as "never re-execute an index
//! that was already applied" -- so moving it over an entry whose bytes never reached the disk
//! bars the very replay that makes a lost engine-WAL tail recoverable.

#![allow(clippy::all)]
use super::*;
use crate::types::{ExecuteResponse, Status};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

const SHARD: ShardId = 7;
const COMMITTED: u64 = 3;

/// A node whose engine keeps a real on-disk WAL, with that WAL replaced by a DIRECTORY so the
/// next durable append cannot open it (EISDIR fails even for root, unlike a chmod which root
/// bypasses).
fn node_whose_durable_wal_is_broken(dir: &std::path::Path) -> RaftNode {
    let indexes = dir.join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.join("cache"), dir.join("blocks"), &indexes);
    engine.load_shard(SHARD);
    let baseline = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "baseline".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(
        baseline.status.ok,
        "positive control: the engine must accept a write BEFORE the WAL is broken, else the \
         test proves nothing about the break; got {:?}",
        baseline.status
    );

    let wal_path = indexes.join("wals").join(format!("shard-{SHARD}.wal.bin"));
    assert!(
        wal_path.is_file(),
        "positive control: the baseline write must have produced a WAL file at {wal_path:?}"
    );
    fs::remove_file(&wal_path).unwrap();
    fs::create_dir(&wal_path).unwrap();
