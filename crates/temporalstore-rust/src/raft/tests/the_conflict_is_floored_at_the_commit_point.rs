// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The conflict truncation, floored at the commit point.
//!
//! Raft section 5.3 lets an AppendEntries replace a log suffix where the terms disagree at an
//! index. What keeps that safe is a floor at the commit point: a conflict search that stops at
//! `commit_index`, and reports `commit_index` itself when nothing above it differs, can never name
//! an index at or below the commit point -- so the truncation that follows cannot cut a committed
//! entry, and `applied_index <= commit_index` holds by construction rather than by hope.
//!
//! Neither path here had that floor, and the consequence is not a lost truncation but a SILENT
//! DISAGREEMENT between the log and the state machine:
//!
//! * The data path keeps a monotonic exactly-once floor `max_applied_index` that a truncation
//!   deliberately does NOT lower. Both apply loops skip any entry at or below it, advancing the
//!   apply cursor over the entry WITHOUT running it. So a conflicting append at or below the
//!   commit point replaced a committed entry in the log while the state machine kept the command
//!   it superseded -- and `applied_index` reported that index as applied either way.
//! * The meta path truncated on a bare index collision without comparing terms at all, deleting
//!   committed suffix entries for a mere duplicate or reordered append, and never rewound
//!   `applied` -- so `applied.insert` returned false for the replacement and the apply skipped it.
//!
//! With the floor the harmful case is unreachable rather than papered over: an entry is only ever
//! applied at or below `commit_index`, so `max_applied_index <= commit_index` always holds, and a
//! truncation confined to indexes ABOVE the commit point can never reach the floor. Each arm
//! below pins one clause: the refusal, what it reports, the surviving agreement between log and
//! state machine, and -- as the inverse arm -- that a conflict above the commit point still
//! truncates and still runs.

#![allow(clippy::all)]
use super::*;
use crate::meta::{MetaMutation, RegisterShardRequest};

const SHARD: ShardId = 11;
const KEY: &str = "k";

fn set_entry(index: u64, term: u64, value: &str) -> RaftLogEntry {
    RaftLogEntry {
        leader_time_ms: 0,
        term,
        index,
        shard_id: SHARD,
        command: Command::StringSet {
            key: KEY.to_string(),
            value: value.as_bytes().to_vec(),
        },
    }
}

/// What the STATE MACHINE holds, read out of the engine rather than inferred from a cursor.
/// `applied_index` is the number asserted about the apply; this is the artefact itself.
fn value_in_the_state_machine(node: &RaftNode) -> Option<Vec<u8>> {
    let response = node.engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: KEY.to_string(),
        },
    });
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("expected a byte value back from the engine, got {other:?}"),
    }
}

fn meta_entry(index: u64, term: u64, addr: &str) -> MetaLogEntry {
    MetaLogEntry {
        term,
        index,
        command: MetaCommand::ApplyMutation(MetaMutation::RegisterShard(RegisterShardRequest {
            shard_id: SHARD,
            server_addr: addr.to_string(),
            registered_at_ms: 0,
        })),
    }
}

fn addr_in_the_meta_state(node: &MetaRaftNode) -> Option<String> {
    node.state
        .shards
        .get(&SHARD)
        .map(|location| location.server_addr.clone())
}

#[test]
fn a_conflicting_entry_at_the_commit_point_is_refused_rather_than_absorbed() {
    let mut node = new_node(1, RaftRole::Follower, SHARD);
    for index in 1..=3 {
        append_entry(&mut node, set_entry(index, 1, &format!("v{index}")));
    }
    node.commit_index = 3;
    apply_committed(&mut node);

    // Vacuity floor: unless the fixture really applied all three, nothing below tests the floor.
    assert_eq!(
        node.applied_index, 3,
        "fixture must have applied all three committed entries before the conflict arrives"
    );
    assert_eq!(
        node.max_applied_index, 3,
        "fixture must have raised the exactly-once floor to 3, or the skip branch under test \
         cannot be reached by anything"
    );
    assert_eq!(
        value_in_the_state_machine(&node),
        Some(b"v3".to_vec()),
        "fixture must have RUN the committed commands, not merely moved a cursor"
    );

    // The conflict: a DIFFERENT term carrying a DIFFERENT command, at an index this group has
    // already committed. Raft forbids overwriting a committed entry, so the sender is wrong.
    append_entry(&mut node, set_entry(3, 2, "replacement"));

    assert_eq!(
