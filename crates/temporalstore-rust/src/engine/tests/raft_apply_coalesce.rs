// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for the raft state-machine apply-path fsync coalescing.
//! On the raft apply path the raft log is the durability + reconstruction source, so a batch of
//! committed entries (an AppendEntries batch on a follower, a recovery replay, or a pipelined
//! propose group) only needs ONE engine-WAL durability barrier instead of one per entry. These
//! tests prove:
//!   (a) with the gate ON, applying a batch of N committed commands via
//!       `execute_raft_apply_batch` takes exactly ONE fdatasync (coalesced) while every value is
//!       present + byte-exact on the live engine;
//!   (b) with the gate OFF, the same batch degrades to per-entry `execute_raft_apply` -> exactly
//!       N fdatasyncs (byte-identical legacy behavior);
//!   (c) the coalesced batch is durable: after a drop + WAL-replay reload every applied command is
//!       recovered byte-exact -- the coalesced barrier made the whole batch durable before the
//!       apply returned, so nothing acked-and-applied is lost.
#![allow(clippy::all)]
use super::*;

// These used to serialize against each other through a mutex, because the choice was a
// process-global env var and an OFF baseline could observe an ON window from a sibling. It is a
// property of an engine now, and each of these builds its own.

const BATCH: usize = 64;

fn new_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn batch_requests() -> Vec<ExecuteRequest> {
    (0..BATCH)
        .map(|i| ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            },
        })
        .collect()
}

fn assert_all_present(engine: &TemporalEngine) {
    for i in 0..BATCH {
        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("k{i}") },
        });
        match get.response {
            CommandResponse::Bytes { value: Some(value) } => {
                assert_eq!(value, format!("v{i}").into_bytes(), "wrong value for k{i}")
            }
            other => panic!("missing applied command k{i}: {other:?}"),
        }
    }
}

/// THE proof: a committed apply batch coalesces to ONE fdatasync with the gate ON, and pays one
/// fdatasync per entry with the gate OFF -- both applying every command correctly.
#[test]
fn raft_apply_batch_coalesces_to_one_fsync_while_off_is_one_per_entry() {

    // --- Baseline: per-entry execute_raft_apply -> one fsync per committed entry. ---
    let off_dir = tempfile::tempdir().unwrap();
    let off = new_engine(off_dir.path());
    off.apply_raft_batch_per_entry_for_test();
