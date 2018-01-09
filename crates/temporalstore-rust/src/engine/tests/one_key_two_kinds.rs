// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Does the command surface let ONE key hold TWO kinds at once?
//!
//! # WHY THIS IS A DRIVEN TEST AND NOT A READ OF THE MAPS
//!
//! The resident maps are the layer that DESCRIBES what a shard holds. The command handler is the
//! layer that DECIDES what a shard is allowed to hold. `shard.strings` and `shard.hashes` being
//! separate maps says nothing about whether a client can populate both under one key -- a handler
//! that refused the second write would leave exactly the same two maps in the tree. Reading the
//! first layer and concluding about the second is how this question gets answered wrongly, so the
//! answer here comes from `execute`/`ExecuteRequest`, the surface a client actually calls.
//!
//! # THE THREE OUTCOMES, AND WHY ALL THREE ARE DISCRIMINATED
//!
//! A test that only asserts the one outcome it expects passes under two of the three, because
//! "the second write answered ok" is true for COEXIST and for SILENTLY REPLACED alike, and a
//! read-back of only the second kind cannot tell them apart. So both kinds are read back after
//! both writes and the outcome is CLASSIFIED before it is asserted:
//!
//!   * REFUSED -- the second write answers not-ok. A key addresses exactly one object, and
//!     something enforces it.
//!   * COEXIST -- both kinds read back. One key can hold two kinds.
//!   * SILENTLY REPLACED -- the second write answers ok and displaces the first. They cannot
//!     coexist, and nothing says so.
//!
//! A fourth shape -- ok, and the SECOND kind is the one that vanished -- is not one of the three
//! and panics rather than being folded into the nearest neighbour.

use super::*;

/// Which of the three the surface does. Named, so the assertion below reads as the finding.
#[derive(Debug, PartialEq, Eq)]
enum TwoKindOutcome {
    Refused,
    Coexist,
    SilentlyReplaced,
}

fn string_set(engine: &TemporalEngine, key: &str, value: &[u8]) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: value.to_vec(),
            },
        })
        .status
}

fn hash_set(engine: &TemporalEngine, key: &str, field: &str, value: &[u8]) -> Status {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: key.to_string(),
                field: field.to_string(),
                value: value.to_vec(),
            },
        })
        .status
}

/// `GET key` through the command surface. `None` for "the surface does not answer with a value",
/// whether that is a nil or a not-ok status -- both mean the string kind is not readable.
fn string_get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    if !answer.status.ok {
        return None;
    }
    match answer.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("StringGet {key} answered {other:?}"),
    }
}

/// `HGET key field` through the command surface, with the same convention as [`string_get`].
fn hash_get(engine: &TemporalEngine, key: &str, field: &str) -> Option<Vec<u8>> {
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGet {
            key: key.to_string(),
            field: field.to_string(),
        },
    });
    if !answer.status.ok {
        return None;
    }
    match answer.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("HashGet {key}/{field} answered {other:?}"),
    }
}

/// Classify from the second write status and BOTH read-backs taken after it.
fn classify(
    second_write: &Status,
    first_kind_after: bool,
    second_kind_after: bool,
) -> TwoKindOutcome {
    if !second_write.ok {
        return TwoKindOutcome::Refused;
    }
    match (first_kind_after, second_kind_after) {
        (true, true) => TwoKindOutcome::Coexist,
        (false, true) => TwoKindOutcome::SilentlyReplaced,
        (first, second) => panic!(
            "the second write answered ok and the outcome is none of the three: the first kind \
             reads back {first} and the second {second}"
        ),
    }
}

/// A string write then a HASH write at the same key.
#[test]
fn a_hash_write_lands_on_a_key_that_already_holds_a_string() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let first = string_set(&engine, "foo", b"string-value");
    assert!(first.ok, "the string write failed: {}", first.message);
    assert_eq!(
        string_get(&engine, "foo").as_deref(),
        Some(&b"string-value"[..]),
        "the string has to be readable BEFORE the hash write, or the outcome below is about an \
         empty key rather than about two kinds"
    );

