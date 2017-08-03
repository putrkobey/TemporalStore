// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Four expiry decisions on the command layer that nothing was watching.
//!
//! A mutation audit of this layer scored 7 killed out of 18 attempted -- the lowest of the nine
//! components it covered. Four of the survivors are guarded here. They are four separate
//! claims, in four tests, because a single test that fires for any of them says nothing about
//! which one it was watching.
//!
//! WHAT THE FOUR HAVE IN COMMON. Every one of them is invisible to the command's own ANSWER.
//! `TTL` replies -2 whether or not the collection was recorded; `PERSIST` replies 0 down both
//! branches; `EXPIRE key 0` replies 1 whether the key was discarded or merely given a deadline
//! of now; and a key whose deadline is exactly the current instant reads as absent either way
//! a millisecond later. So each test below asks the SHARD, not the reply -- and asks it before
//! anything else has had a chance to expire the key lazily.
//!
//! WHICH ENTRY POINT, AND WHY. All four go through `engine.execute`, not `execute_durable`.
//! That is a deliberate choice and not the default one: production reads reach
//! `execute_read_only_fast_path`, which only `execute_durable` can enter, and a test through
//! `execute` cannot see it. None of the four sites is on that fast path. Two of them
//! (`CommonTtl`, `CommonPersist`) are arms of `execute_on_shard`, which the fast path never
//! reaches because it answers before dispatch; one is in the RESP layer above the engine
//! entirely; and one is in `validate_command_preconditions`, which runs on the shared path
//! ahead of the fast-path check and is reached identically by both routes. `execute` is
//! therefore the narrower route that still contains all four, and going through
//! `execute_durable` would add a storage override that none of these claims is about.

#![allow(clippy::all)]
use super::*;

use crate::redis::{execute_redis_command, RespValue};

/// Drive one RESP command against a live engine on shard 1, through `engine.execute`.
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

/// Arm a deadline in the past WITHOUT letting any command collect the key on the way in.
///
/// Going through `EXPIRE` would run the very arms under test and answer the question there
/// instead; sleeping past a short deadline would make the test a race. This writes the
/// deadline straight into the shard, which is what the clock would have produced anyway.
fn backdate(engine: &TemporalEngine, key: &str) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
    crate::engine::set_expiry(shard, key.to_string(), past);
}

/// How many live blocks the shard's index accounts for, and whether a given key's routing
/// bucket is still one of them.
fn index_state(engine: &TemporalEngine, key: &str) -> (usize, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let bucket = crate::engine::block_routing_bucket(key, 0, u32::MAX);
    (
        shard.bucket_index.object_block_lookup.len(),
        shard.bucket_index.bucket_map.contains_key(&bucket),
    )
}

/// What the SHARD holds for a key: (the record is still there, a deadline is still recorded).
fn shard_state(engine: &TemporalEngine, key: &str) -> (bool, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.strings().contains_key(key),
        shard.expires_at_ms().contains_key(key),
    )
}

/// Forget which objects are waiting to be written out, so the next command's marking is the
/// only thing the count below can be measuring. A dump does exactly this when it completes.
fn forget_pending_writes(engine: &TemporalEngine) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    shard.dirty_objects.clear();
    shard.dirty_objects.len()
}

/// How many objects are marked as needing a write, and whether this key is one of them.
fn pending_writes(engine: &TemporalEngine, key: &str) -> (usize, bool) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.dirty_objects.len(),
        shard.dirty_objects.contains(key),
    )
}

// =====================================================================================
// ONE: a `TTL` that collects an expired key has to REPORT that it collected one.
// =====================================================================================

/// `TTL` on a key whose deadline has passed removes the record, and must say that it did.
///
/// THE LINE. `execute_on_shard`'s `CommonTtl` arm reads
/// `mutated |= drop_if_expired(cache, shard_id, shard, &key)`. Dropping the accumulation --
/// `let _ = drop_if_expired(..)` -- still collects the record and still invalidates its cached
/// copy, so every answer the command gives is unchanged. It survived 201 selected tests and
/// then 566 durability-selected ones.
///
/// WHY THE FLAG IS NOT BOOKKEEPING. `outcome.mutated` is the condition on a block in
/// `execute` that runs from line 915 to line 1344, and everything that keeps the shard's block
/// index honest is inside it. `CommonTtl` is NOT a write command and carries no object keys,
/// so for THIS arm the block reduces to one thing: `rebuild_bucket_block_ownership`, which
/// re-derives the index from the live model maps. Skip it after a record has left `strings`
/// and the index still accounts for a block belonging to a record that is gone -- which is a
/// block that compaction and reclaim will keep treating as live.
///
/// SO THE CLAIM IS COUNTED, NOT ASSERTED AS A FLAG. The test never reads `mutated`. It reads
/// how many blocks the index accounts for, before and after, and that number has to fall.
///
/// THE CONTROL IS A SECOND KEY THAT IS STILL LIVE. Without it, an index that had been emptied
/// wholesale -- or a rebuild that dropped everything it touched -- would satisfy the claim and
/// look like a result.
#[test]
fn a_ttl_that_collects_an_expired_key_maintains_the_block_index() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // ---- DENOMINATOR: two records, two blocks, both accounted for -----------------------
    for key in ["ttl:collected", "ttl:live"] {
        assert_eq!(
            RespValue::SimpleString("OK".to_string()),
            resp(&engine, &["SET", key, "v"]),
            "DENOMINATOR: {key} has to be written before its block can be accounted for",
        );
    }
    let (blocks_before, collected_bucket_before) = index_state(&engine, "ttl:collected");
    let (_, live_bucket_before) = index_state(&engine, "ttl:live");
    assert_eq!(
        2, blocks_before,
        "DENOMINATOR: the index must account for the two blocks just written, and it \
         accounted for {blocks_before}. Every count below is a change from this one, so if \
         writes are not registering here there is nothing for the rest of the test to measure.",
    );
    assert!(
        collected_bucket_before && live_bucket_before,
        "DENOMINATOR: both keys' routing buckets must be in the bucket map before either can \
         be observed leaving it",
    );

    // Only one of them expires, and it expires without any command seeing it.
    backdate(&engine, "ttl:collected");

    // ---- THE CLAIM: the collection is reported, so the index is maintained --------------
    assert_eq!(
        RespValue::Integer(-2),
        resp(&engine, &["TTL", "ttl:collected"]),
