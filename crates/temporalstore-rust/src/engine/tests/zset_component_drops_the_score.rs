// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! GUARDS FOR "THE SCORE LEAVES THE ZSET COMPONENT", DRIVEN THROUGH `engine.execute` AND A REAL
//! COLD RELOAD.
//!
//! `BlockIndex` was 56 bytes with zero slack asserted; `component: Option<Arc<str>>` is 16 of
//! those 56. Set and list already collapse to one index entry a page; a zset could not, because
//! its component was not a name -- it was the score, `{biased:016x}` then the member in hex. This
//! change takes the score off the component (`zset_component` is `hex::encode(member)` alone
//! now), which makes zset collapsible like the others -- collapsing it is a separate, later step,
//! not this one.
//!
//! A HAND-BUILT `BlockIndex` FIXTURE MAKING ZERO ENGINE CALLS WOULD STAY GREEN UNDER ANY VERSION OF
//! THIS CUT, which is why every guard here goes through `engine.execute` and, for three of the
//! four, a real reload -- fresh `TemporalEngine` instances over the same on-disk directories,
//! never a resident-map peek standing in for one.
//!
//! # THE FOUR GUARDS
//!
//!   1. `n_members_at_distinct_scores_survive_a_cold_reload_in_score_order` -- the ordinary case.
//!   2. `a_removed_member_stays_removed_across_a_cold_reload` -- a loss guard is not a
//!      resurrection guard, so this is a second, separate test rather than one more assertion on
//!      the first.
//!   3. `a_rescore_leaves_exactly_one_member_across_a_cold_reload` -- `ZSetCard` and `ZSetScore`
//!      after `ZSetAdd m@1` then `ZSetAdd m@2`, through the collapse this change drives at the
//!      component level (see `container_tombstone_entry::a_rescore_sweeps_its_own_tombstone_...`
//!      for the index-entry mechanics; this guard is the user-visible shape).
//!   4. `a_zset_write_recovered_through_replay_alone_returns_its_score` -- **the guard that catches
//!      the data loss an earlier attempt at this change measured.** The score used to be
//!      recoverable from the component text on replay; it is not any more, so it has to ride the
//!      WAL outcome's own `value` slot (`RecordedKind::outcome_value`) instead. This guard writes a
//!      zset member and reloads WITHOUT an `unload_shard` -- `replay_under_the_gate`'s mechanism:
//!      an unload is what materializes the base index, so skipping it leaves the reload no choice
//!      but to replay the WAL, which is the only path that can lose the score this way.
//!
//! # THE DISCRIMINATING MUTATION, RECORDED HERE RATHER THAN LEFT ONLY IN THE PR BODY
//!
//! Make `ZSetKind::outcome_value` return `None` unconditionally. That must redden guard 4 ALONE:
//! guards 1-3 stay green because the index snapshot a normal `unload_shard` writes still carries
//! `zset_index_serde`'s copy of the score, and only guard 4 forces a path with no index snapshot
//! to fall back on. A `+1` on the score bits would redden everything and prove nothing about this
//! specific wire.

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, table: &str) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: table.to_string(),
        shard_uri: format!("local://{table}/1"),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

fn score_of(engine: &TemporalEngine, key: &str, member: &[u8]) -> Option<f64> {
    match read(
        engine,
        Command::ZSetScore {
            key: key.to_string(),
            member: member.to_vec(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).parse::<f64>().ok()
        }
        crate::types::CommandResponse::Bytes { value: None } => None,
        other => panic!("ZSetScore answered {other:?}"),
    }
}

fn card_of(engine: &TemporalEngine, key: &str) -> i64 {
    match read(
        engine,
        Command::ZSetCard {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("ZSetCard answered {other:?}"),
    }
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("zcd-member-{index:04}").into_bytes()
}

// =================================================================================================
// 1. PRESENT SURVIVES
// =================================================================================================

/// rust-internal: drives ZSetAdd through a real cold reload, no external surface
#[test]
fn n_members_at_distinct_scores_survive_a_cold_reload_in_score_order() {
    const N: usize = 12;
    let key = "zcd-present";
    let dir = tempfile::tempdir().expect("tempdir");

    {
