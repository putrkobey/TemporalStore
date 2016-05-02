// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A bucket's WAL claim is its OLDEST undumped write, so a later batch must not move it forward.
//!
//! `first_dirty_wal_sequence` names the oldest write a bucket still holds the log for. The reclaim
//! plan reads it as `durable_wal_frontier = min(claim - 1)` over every dirty bucket no dump
//! manifest covers, then frees everything at or below that frontier
//! (`retain_from_wal_sequence = frontier + 1`). So the stamp has to be write-once-until-dumped: set
//! when the bucket goes dirty, cleared to 0 only by a durable dump.
//!
//! Let a LATER batch overwrite it with its own newer sequence and the bucket stops naming the write
//! it still needs and names the most recent one instead. The floor rises above records the bucket
//! has not dumped, and reclaim frees them.
//!
//! WHY THE EXISTING CLAIM GUARDS DO NOT SEE THIS. They ask two things: is a claim PRESENT on each
//! dirty bucket (`wal > 0`), and is the MINIMUM claim across buckets still the shard's first record
//! (`min == 1`). Both survive a claim that moves. Presence survives because a moved claim is still
//! non-zero. The minimum survives because a bucket the first batch dirtied and no later batch
//! re-entered keeps its `1` and holds the aggregate down by itself. One correct element hides every
//! wrong one -- which is why every assertion below is made PER BUCKET and never over a `min`, a
//! `max`, a `sum` or a `len`.
//!
//! ROUTING RANGE. These load the shard over a BOUNDED routing range. `load_shard` uses
//! `end_routing_bucket: u32::MAX`, which spreads N keys over N distinct buckets, so no bucket ever
//! holds two keys -- and a one-key bucket cannot be harmed by a claim that moves, because the only
//! write it needs IS the newest one. A real shard routes into a bounded range and its buckets hold
//! many keys each. That is the configuration where the stamp matters, so it is the one used here.

use super::*;

const CLAIM_HOLD_SHARD: ShardId = 1;

/// A bounded routing range, so each bucket holds many keys. See the module note.
const CLAIM_HOLD_END_BUCKET: u32 = 127;

fn claim_hold_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-blocks")),
        dir.join(format!("{name}-indexes")),
    )
}

fn load_bounded_shard(engine: &TemporalEngine) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: CLAIM_HOLD_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: CLAIM_HOLD_END_BUCKET,
        readonly: false,
        table_name: String::new(),
    });
}

/// Every dirty bucket's WAL claim, BY BUCKET.
///
/// Deliberately not reduced here. A reduction over this map is exactly what lets one correct bucket
/// stand in for the rest, so the reduction is never what gets asserted on.
fn wal_claims_by_bucket(
    engine: &TemporalEngine,
    shard_id: ShardId,
) -> std::collections::BTreeMap<u32, u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&shard_id) else {
        return std::collections::BTreeMap::new();
    };
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| bucket.dirty())
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.first_dirty_wal_sequence))
        .collect()
}

/// The buckets a set of keys routes into, computed the way the write path computes them.
fn buckets_touched_by(keys: &[String]) -> std::collections::BTreeSet<u32> {
    keys.iter()
        .map(|key| block_routing_bucket(key, 0, CLAIM_HOLD_END_BUCKET))
        .collect()
}

fn write_one_batch(engine: &TemporalEngine, keys: &[String], fill: u8) {
    let commands = keys
        .iter()
        .map(|key| Command::StringSet {
            key: key.clone(),
            value: vec![fill; 64],
        })
        .collect::<Vec<_>>();
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: CLAIM_HOLD_SHARD,
        commands,
    });
    assert!(response.status.ok, "batch write failed: {:?}", response.status);
}

fn claim_hold_keys(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index:05}"))
        .collect()
}

/// Dump every dirty bucket, clearing the claims the way a durable round clears them.
fn dump_everything(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: CLAIM_HOLD_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });
}

/// WAL records the newest dump manifest does NOT cover.
fn undumped_records(engine: &TemporalEngine, dumped_through: u64) -> usize {
    let (records, _truncated) = engine
        .wal_store()
        .scan_decoded(CLAIM_HOLD_SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan wal");
    records
        .iter()
        .filter(|(_, record)| record.sequence > dumped_through)
        .count()
}

fn newest_dump_wal_sequence(engine: &TemporalEngine) -> u64 {
    engine
        .list_bucket_dump_manifests(CLAIM_HOLD_SHARD)
        .iter()
        .map(|manifest| manifest.wal_sequence)
        .max()
        .unwrap_or_default()
}

/// How many of `keys` this engine can still return a value for.
fn readable_count(engine: &TemporalEngine, keys: &[String]) -> usize {
    keys.iter()
        .filter(|key| {
            let response = engine.execute(ExecuteRequest {
                shard_id: CLAIM_HOLD_SHARD,
                command: Command::StringGet {
                    key: (*key).clone(),
                },
            });
            matches!(response.response, CommandResponse::Bytes { value: Some(_) })
        })
        .count()
}

/// THE STAMP. A second batch into buckets a first batch already dirtied leaves every one of those
/// claims exactly where it was.
///
/// Asserted per bucket. The minimum across buckets is computed as well, but only to be PRINTED
/// beside the per-bucket answer -- it is the aggregate that stays right while the elements go
/// wrong, and having the two side by side in the failure message is the point.
#[test]
fn a_later_batch_must_not_move_a_bucket_wal_claim_forward() {
    const KEYS: usize = 2_000;

    let dir = tempfile::tempdir().unwrap();
    let first_keys = claim_hold_keys("first", KEYS);
    // Distinct keys rather than a rewrite of the same ones, on purpose: a rewrite supersedes the
    // first batch's records, and records nothing needs any more cannot show a floor that frees
    // records something does need.
    let later_keys = claim_hold_keys("later", KEYS);

    let engine = claim_hold_engine(dir.path(), "hold");
    load_bounded_shard(&engine);

    write_one_batch(&engine, &first_keys, 118);
    let before = wal_claims_by_bucket(&engine, CLAIM_HOLD_SHARD);

    write_one_batch(&engine, &later_keys, 119);
    let after = wal_claims_by_bucket(&engine, CLAIM_HOLD_SHARD);

    // DENOMINATORS FIRST. Without dirty buckets, and without the later batch genuinely routing
    // back into them, every comparison below runs over an empty set and passes saying nothing.
    assert!(
        !before.is_empty(),
        "the first batch of {KEYS} keys dirtied no bucket, so there is no claim to hold"
    );
    let unstamped = before.values().filter(|claim| **claim == 0).count();
    assert_eq!(
        0, unstamped,
        "{unstamped} of {} buckets the first batch dirtied recorded no claim at all, so this test \
         is measuring a different defect than the one it names",
        before.len()
    );
    // Re-entered means the LATER keys actually route here -- not merely that the bucket is still
    // dirty, which every bucket is.
    let later_buckets = buckets_touched_by(&later_keys);
    let re_entered = before
        .keys()
        .copied()
        .filter(|routing_bucket| later_buckets.contains(routing_bucket))
        .collect::<Vec<u32>>();
    assert!(
        re_entered.len() * 2 >= before.len() && !re_entered.is_empty(),
        "the later batch routed back into only {} of the {} buckets the first batch dirtied, so \
         the hold is barely exercised",
        re_entered.len(),
        before.len()
    );

    // PER BUCKET. Never a min, never a sum, never a count of non-zeroes.
    let moved = re_entered
        .iter()
        .map(|routing_bucket| {
            (
                *routing_bucket,
                before[routing_bucket],
                after[routing_bucket],
            )
        })
        .filter(|(_, was, now)| now != was)
        .collect::<Vec<(u32, u64, u64)>>();

    // The aggregate a min-shaped guard reads, kept only to be reported next to the real answer.
    let min_before = before.values().copied().min().expect("a claim");
    let min_after = after.values().copied().min().expect("a claim");
    println!(
        "claim hold: dirty_buckets={} re_entered={} moved={} min_across_buckets {min_before} -> \
         {min_after}",
        before.len(),
        re_entered.len(),
        moved.len()
    );

    assert!(
        moved.is_empty(),
        "{} of {} re-entered buckets had their WAL claim moved FORWARD by a later batch, so each \
         now names a write newer than the oldest it still holds the log for and the retain floor \
         rises above records it needs; first three (bucket, was, now) {:?}. The minimum across \
         buckets went {min_before} -> {min_after}, which is why a guard that reduces this \
         per-bucket quantity to a minimum cannot see it.",
        moved.len(),
        re_entered.len(),
        moved.iter().take(3).collect::<Vec<_>>()
    );
}

/// THE HARM. A moved claim only matters if it lets reclaim free something, so this drives it end to
/// end and counts what actually goes.
///
/// The shard reaches the state a running one reaches: a first write, a durable dump that clears the
/// claims, then two more writes. The second write is undumped and the buckets still hold the log
/// for it; the third write re-enters the same buckets. With the claims held the floor sits at the
/// second write and only the dumped first write is freed. With the claims moving, the floor rises
/// to the third write and the second write's records -- which NO manifest covers -- go with it.
///
/// The two halves are counted and asserted SEPARATELY: records freed that no dump covers, and keys
/// still readable after a restart replays the log. One combined number would let a log that froze
