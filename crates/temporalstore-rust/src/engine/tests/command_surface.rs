// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the user-facing command surface answers, and what it advertises.
//!
//! Two obligations live here, and neither is visible from inside a single command arm.
//!
//! ONE: a write that REPLACES a key's whole value discards the deadline it overwrote, and a
//! write that AMENDS an existing value keeps it. The engine spells those as two different
//! commands -- `StringSetConditional` with no `ttl_ms` clears the deadline, `StringSet` leaves
//! it in place -- so which one a RESP verb reaches for IS the semantic, and picking the wrong
//! one is spelled as an ordinary-looking line that no type checks.
//!
//! TWO: the `COMMAND` descriptor table is a hand-maintained list beside the dispatcher's
//! `match`, not derived from it, so the two can drift and only the table is wrong.

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, unix_time_ms, RespValue};

/// Drive one RESP command against a live engine on shard 1.
fn resp(engine: &TemporalEngine, args: &[&str]) -> RespValue {
    let argv: Vec<Vec<u8>> = args.iter().map(|arg| arg.as_bytes().to_vec()).collect();
    execute_redis_command(argv, 1, |command| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        if response.status.ok {
            Ok(response.response)
        } else {
            Err(response.status.message.clone())
        }
    })
}

/// `PTTL` in milliseconds: positive when a deadline is armed, -1 when the key exists with no
/// deadline, -2 when there is no key.
fn pttl(engine: &TemporalEngine, key: &str) -> i64 {
    match resp(engine, &["PTTL", key]) {
        RespValue::Integer(value) => value,
        other => panic!("PTTL {key} answered {other:?}"),
    }
}

/// What the SHARD itself holds for a key: (the record is still there, a deadline is still
/// recorded against it). Read directly, not through a command.
///
/// WHY THIS EXISTS AND `GET` IS NOT ENOUGH. `GET` cannot tell "the key was deleted" apart from
/// "the key was given a deadline one millisecond out, and that millisecond has since passed":
/// lazy expiry removes it either way before the next command runs, so the assertion is
/// satisfied by the CLOCK rather than by the behaviour under test. That is not hypothetical --
/// both mutations restoring the old `.max(1)` clamp left every `GET` answering nil and this
/// whole test PASSING, which is how they were found. The shard has no such ambiguity: a
/// clamped deadline is an entry in `expires_at_ms` with the record still sitting in `strings`,
/// and a deletion is neither of those, whatever the clock has done in between.
fn shard_state(engine: &TemporalEngine, key: &str) -> (bool, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.strings().contains_key(key),
        shard.expires_at_ms().contains_key(key),
    )
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match resp(engine, &["GET", key]) {
        RespValue::Bulk(value) => value,
        other => panic!("GET {key} answered {other:?}"),
    }
}

/// A long deadline, so nothing below can pass by the key having actually expired.
const HOUR_MS: &str = "3600000";

/// A write that REPLACES the value discards the deadline; a write that AMENDS it keeps one.
///
/// WHAT WENT WRONG. `SET` reaches for `StringSetConditional` and so clears the deadline, which
/// is right. `GETSET` and `MSET` reached for `StringSet`, which does not touch the expiry
/// index at all -- it calls `remove_if_expired` and then writes the page. So a caller who
/// armed a one-minute deadline on a key and then replaced its contents with `GETSET` or `MSET`
/// got a key that still vanished a minute later, carrying a deadline it never asked to keep
/// and that nothing in either answer mentions. `GETSET` returned the old value and `MSET`
/// returned `OK`; the key was gone by the next minute.
///
/// WHY THOSE TWO AND NOT THE OTHERS. The distinction is not "does it write a string" -- every
/// verb below writes a string. It is whether the caller supplied the WHOLE new value. `GETSET`
/// and `MSET` do, exactly as `SET` does, and carry the same "this key is now this, and nothing
/// else is implied" meaning. `APPEND`, `SETRANGE`, `INCR`, `INCRBY`, `DECR` and `INCRBYFLOAT`
/// derive the new value FROM the old one; they amend a key that is already there and on
/// purpose leave its lifetime alone. Those are asserted below as controls, because the wrong
/// fix here -- teaching `StringSet` itself to clear -- would satisfy every claim about `GETSET`
/// and `MSET` while silently making `INCR` reset a countdown.
///
/// HALVES ASSERTED SEPARATELY. `GETSET` and `MSET` are two arms and two claims. They were both
/// wrong in the same way, which is exactly the case where one combined "no deadline survives"
/// count reads full from a fixed arm and hides the other.
///
/// `MSETNX` IS DELIBERATELY NOT ASSERTED. It shares the corrected write, but it only writes
/// when NONE of its keys exist, and a key that does not exist has no deadline to discard --
/// `CommonExists` calls `remove_if_expired` first. There is no state in which its behaviour
/// differs, so a claim about it would pass no matter which write it used. Asserting it would
/// be a vacuous row, so it is named here instead of counted there.
#[test]
fn a_value_replacing_write_discards_the_deadline_it_overwrote() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: arming a deadline through this path really arms one ---------------
    // Without this every -1 below would also be produced by `SET ... PX` silently doing
    // nothing, and the test would pass while proving the opposite of what it claims.
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "replaced:getset", "v1", "PX", HOUR_MS]),
    );
    let armed = pttl(&engine, "replaced:getset");
    assert!(
        armed > 0,
        "DENOMINATOR: `SET k v PX {HOUR_MS}` must arm a deadline, and PTTL reported {armed}. \
         Nothing below means anything if a deadline is never armed in the first place.",
    );

    // ---- HALF ONE: GETSET -------------------------------------------------------------
    assert_eq!(
        RespValue::Bulk(Some(b"v1".to_vec())),
        resp(&engine, &["GETSET", "replaced:getset", "v2"]),
        "GETSET answers the value it replaced",
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "replaced:getset"),
        "GETSET really stored the new value",
    );
    assert_eq!(
        -1,
        pttl(&engine, "replaced:getset"),
        "GETSET replaced the whole value, so the deadline that was on the old one is gone. \
         A positive number here is the key still counting down toward a disappearance the \
         caller cancelled when they overwrote it.",
    );

    // ---- HALF TWO: MSET ---------------------------------------------------------------
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["SET", "replaced:mset", "v1", "PX", HOUR_MS]),
    );
    let armed = pttl(&engine, "replaced:mset");
    assert!(
        armed > 0,
        "DENOMINATOR: the MSET half needs its own armed deadline; PTTL reported {armed}",
    );
    assert_eq!(
        RespValue::SimpleString("OK".to_string()),
        resp(&engine, &["MSET", "replaced:mset", "v2"]),
    );
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "replaced:mset"),
        "MSET really stored the new value",
    );
    assert_eq!(
        -1,
        pttl(&engine, "replaced:mset"),
        "MSET replaced the whole value, so the deadline is gone -- same rule as SET, which is \
         what MSET is a batch of.",
    );

    // ---- CONTROLS, THE OTHER DIRECTION: an amending write KEEPS the deadline ------------
    // These are what stops the fix from being "make every string write clear the deadline".
    for (verb, args) in [
        ("APPEND", vec!["APPEND", "kept:append", "-tail"]),
        ("SETRANGE", vec!["SETRANGE", "kept:setrange", "0", "X"]),
        ("INCR", vec!["INCR", "kept:incr"]),
        ("INCRBY", vec!["INCRBY", "kept:incrby", "5"]),
        ("DECR", vec!["DECR", "kept:decr"]),
        ("INCRBYFLOAT", vec!["INCRBYFLOAT", "kept:incrbyfloat", "1.5"]),
    ] {
        let key = args[1];
        // Every one of these is seeded with "1" so the numeric verbs have something to add to
        // and the byte verbs have something to amend.
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "1", "PX", HOUR_MS]),
        );
        let armed = pttl(&engine, key);
        assert!(
            armed > 0,
            "DENOMINATOR for the {verb} control: PTTL reported {armed} before {verb} ran",
        );
        let answer = resp(&engine, &args);
        assert!(
            !matches!(answer, RespValue::Error(_)),
            "the {verb} control has to actually run: it answered {answer:?}",
        );
        let after = pttl(&engine, key);
        assert!(
            after > 0,
            "{verb} amends a value that is already there and must leave its lifetime alone; \
             PTTL reported {after}. A -1 here means the deadline-clearing write leaked into \
             the amending verbs, which resets a countdown every time a counter moves.",
        );
    }
}

/// Every command the dispatcher accepts is either advertised by `COMMAND`, or named here as
/// deliberately unadvertised -- and everything `COMMAND` advertises really is accepted.
///
/// WHY A GUARD AND NOT JUST THE FOUR MISSING ROWS. `redis_supported_commands()` is a
/// hand-written array in `command_table.rs`; the dispatcher is a `match` in `dispatch.rs`.
/// Nothing connects them. Adding a command arm is one edit in one file, and the table is a
/// second file the author has no reason to open, so the table drifts in exactly one direction:
/// it under-reports, quietly. That is how `INCR`, `DECR`, `INCRBY` and `DECRBY` came to be
/// implemented and unadvertised while `INCRBYFLOAT`, `HINCRBY` and `HINCRBYFLOAT` -- the rest
/// of the same family -- were listed. The omission is not a policy; it is four forgotten rows.
///
/// WHAT THE MISREPORT COSTS. `COMMAND INFO INCR` answers a null, which is the wire spelling of
/// "this server does not have that command", while `INCR k` works. `COMMAND COUNT` undercounts
/// by the same four. A client that introspects before it dispatches -- which is how cluster-
/// aware clients learn where the key sits in each command's argument list -- is being told the
/// wrong thing by the server itself, and the answer it gets is confidently wrong rather than
/// an error it could retry.
///
/// THE OTHER DIRECTION MATTERS TOO, AND IS CURRENTLY CLEAN. A table row with no arm behind it
/// advertises a command the dispatcher will reject. There are none today; this asserts it
/// stays that way, because removing an arm is exactly as one-sided an edit as adding one.
///
/// THE EXEMPTIONS ARE HAND-WRITTEN AND EXACT, NOT PREFIXES. A prefix rule would read
/// "everything starting with F is ours" and would silently swallow a future standard verb that
/// happens to start with F. Spelling each name means a new arm is unlisted AND unexempt, so it
/// fails here until someone decides which it is.
///
/// THIS SCAN'S OWN CONTROLS. The first version of it matched any capitalised string literal at
/// any indentation and reported `WITHSCORES`, `REV`, `FIRST` and `LAST` as commands -- they are
/// option words inside the sorted-set arms, not arms. The negative controls below are those
/// words, and they are what caught it.
#[test]
fn every_dispatched_command_is_advertised_or_named_as_unadvertised() {
    const DISPATCH: &str = include_str!("../../redis/redis_dispatch.rs");
    const TABLE: &str = include_str!("../../redis/command_table.rs");
