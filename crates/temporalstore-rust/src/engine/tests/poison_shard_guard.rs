// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one panic under the shard-table guard costs every later caller.
//!
//! `RwLock` marks itself POISONED when a thread unwinds while holding it. Every later
//! `.read()`/`.write()` on that lock returns `Err`, and the shard table's acquisitions all spell
//! that `.expect("engine lock poisoned")`. So the question this module answers with a number is
//! not "can the lock be poisoned" -- it can -- but what the NEXT ordinary request observes once
//! it has been, and for how long.
#![allow(clippy::all)]
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Requests driven in each arm. Larger than one so "the first call after the poison fails" and
/// "every call after the poison fails" are distinguishable outcomes rather than the same one.
const PROBES: usize = 3;

/// A loaded shard holding one string key.
fn engine_with_one_key(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let written = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(written.status.ok, "fixture write: {:?}", written.status);
    engine
}

/// Drive one `StringGet` down the plain serving route and say what the CALLER got.
///
/// Three outcomes are distinguished on purpose, because they are three different severities:
/// a clean error the client can retry, a wrong-but-successful answer, and an unwinding panic
/// that never returns a response at all.
enum Observed {
    Served(Option<Vec<u8>>),
    CleanError(String),
    Panicked(String),
}

fn observe_get(engine: &TemporalEngine) -> Observed {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        })
    }));
    match outcome {
        Err(payload) => Observed::Panicked(panic_message(payload)),
        Ok(response) if !response.status.ok => Observed::CleanError(response.status.code.clone()),
        Ok(response) => match response.response {
            CommandResponse::Bytes { value } => Observed::Served(value),
            other => panic!("expected Bytes, got {other:?}"),
        },
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Poison the shard table the way production would: a thread unwinds while holding the guard.
///
/// Nothing here reaches into the lock's internals -- the guard is taken through the same field
/// every acquisition uses, and the panic is a real unwind on a real thread, so the poison flag is
/// set by `RwLock` itself rather than simulated.
fn poison_shard_table(engine: &TemporalEngine) {
