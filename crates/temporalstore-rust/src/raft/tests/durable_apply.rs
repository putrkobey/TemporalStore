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

    let mut node = new_node(1, RaftRole::Follower, SHARD);
    node.engine = engine;
    node
}

fn commit_string_writes(node: &mut RaftNode) {
    for index in 1..=COMMITTED {
        append_entry(
            node,
            RaftLogEntry {
                leader_time_ms: 0,
                term: 1,
                index,
                shard_id: SHARD,
                command: Command::StringSet {
                    key: format!("k{index}"),
                    value: b"v".to_vec(),
                },
            },
        );
    }
    node.commit_index = COMMITTED;
}

fn probe_batch(node: &RaftNode, command: impl Fn(u64) -> Command) -> Vec<ExecuteResponse> {
    let batch: Vec<(ExecuteRequest, Option<u64>)> = (1..=COMMITTED)
        .map(|index| {
            (
                ExecuteRequest {
                    shard_id: SHARD,
                    command: command(index),
                },
                Some(0u64),
            )
        })
        .collect();
    node.engine.execute_raft_apply_batch_at(batch)
}

/// Prove the treatment ran: with the WAL broken, a batch apply of exactly `COMMITTED` entries
/// must report `COMMITTED` durability failures -- not zero (a vacuous run) and not fewer.
fn assert_every_apply_is_a_durability_failure(node: &RaftNode) {
    let responses = probe_batch(node, |index| Command::StringSet {
        key: format!("probe{index}"),
        value: b"v".to_vec(),
    });
    assert_eq!(
        responses.len() as u64,
        COMMITTED,
        "denominator: the probe batch must produce one response per entry"
    );
    let failed = responses
        .iter()
        .filter(|response| !response.status.ok)
        .count() as u64;
    let durability_failed = responses
        .iter()
        .filter(|response| response.status.code == "wal_commit_failed")
        .count() as u64;
    // Halves asserted separately: "some response failed" and "the failure is the DURABILITY one"
    // are different claims, and one combined assertion would let either stand in for both.
    assert_eq!(
        failed, COMMITTED,
        "vacuity floor: all {COMMITTED} probe applies must fail on a broken WAL, got {failed}"
    );
    assert_eq!(
        durability_failed, COMMITTED,
        "all {COMMITTED} failures must be wal_commit_failed, got {durability_failed}: {:?}",
        responses
            .iter()
            .map(|response| response.status.code.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn apply_committed_does_not_advance_the_exactly_once_floor_over_a_failed_durable_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = node_whose_durable_wal_is_broken(dir.path());
    assert_every_apply_is_a_durability_failure(&node);
    commit_string_writes(&mut node);

    apply_committed(&mut node);

    // Halves asserted separately: the apply CURSOR and the exactly-once FLOOR are two different
    // fields with two different jobs, and only the floor is what bars a retry.
    assert_eq!(
        node.applied_index, 0,
        "apply cursor must stay at 0: none of the {COMMITTED} committed entries reached the disk"
    );
    assert_eq!(
        node.max_applied_index, 0,
        "the monotonic exactly-once floor must stay at 0, or raft replay is barred from \
         re-applying {COMMITTED} entries whose engine-WAL bytes were never made durable"
    );
    assert!(
        node.applied.is_empty(),
        "an entry whose durable commit failed must not stay in the applied set, or the next \
         apply pass skips it forever: {:?}",
