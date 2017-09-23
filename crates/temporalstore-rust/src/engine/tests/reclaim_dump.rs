// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a write leaves behind for the reclaim floor to stand on.
//!
//! A dirty bucket that no dump manifest covers is allowed to hold the two logs only from its own
//! OLDEST UNDUMPED WRITE, and it has to be able to name that point in BOTH of them --
//! `first_dirty_wal_sequence` and `first_dirty_index_log_sequence`. A bucket that can name neither
//! is not treated as needing nothing; it is treated as unknown, and unknown refuses the whole
//! reclaim plan (`slot_generation_without_durable_dump`). So a write path that dirties buckets
//! without stamping the two claims stops the logs being reclaimed at all until a dump happens to
//! cover every bucket it touched -- and a dump is capped per round.

use super::*;

const RECLAIM_DUMP_SHARD: ShardId = 1;

/// Both halves of every bucket's claim, read straight off the bucket index.
///
/// Returned as two numbers per bucket and never as one total. The plan REQUIRES both halves and
/// they count in different sequences, so a state where one half is whole and the other is zero is
/// exactly the failure a combined count would report as "half full".
fn claims_by_bucket(engine: &TemporalEngine, shard_id: ShardId) -> Vec<(u32, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&shard_id) else {
        return Vec::new();
    };
    let mut out = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| bucket.dirty())
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                bucket.first_dirty_wal_sequence,
                bucket.first_dirty_index_log_sequence,
            )
        })
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn reclaim_dump_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-blocks")),
        dir.join(format!("{name}-indexes")),
    )
}

fn reclaim_dump_keys(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("claim-{index:04}")).collect()
}

/// The two write paths, one command at a time and one batch.
fn write_one_at_a_time(engine: &TemporalEngine, keys: &[String]) {
    for key in keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            },
        });
        assert!(response.status.ok, "write {key} failed: {:?}", response.status);
    }
}

fn write_as_batches(engine: &TemporalEngine, keys: &[String], batch: usize) {
    for chunk in keys.chunks(batch) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![118u8; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: RECLAIM_DUMP_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
}

/// Dump every dirty bucket, so the claims clear the way a real round clears them.
fn dump_everything(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: RECLAIM_DUMP_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });
}

fn wal_record_count(engine: &TemporalEngine) -> usize {
    let (records, _truncated) = engine
        .wal_store()
        .scan_decoded(RECLAIM_DUMP_SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan wal");
    records.len()
}

/// A bucket dirtied by a BATCH write can name its oldest undumped write, as one dirtied by a
/// single-command write already could.
///
/// The single-command path stamps both halves of the claim (engine.rs, after the WAL append and
/// after the index-log delta). The batch path marks the same buckets dirty through the same
/// `mark_object_dirty` and appends to both logs, and stamped neither -- so every bucket it
/// dirtied reported "no claim recorded", which the reclaim plan reads as unknown and refuses on.
///
/// The single-command arm is the CONTROL: it proves the fixture can produce claims at all, so a
/// zero on the batch arm is the path and not the fixture.
#[test]
fn a_bucket_dirtied_by_a_batch_write_can_name_its_oldest_undumped_write() {
    const KEYS: usize = 64;
    const BATCH: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let keys = reclaim_dump_keys(KEYS);

    let one_at_a_time = reclaim_dump_engine(dir.path(), "single");
    one_at_a_time.load_shard(RECLAIM_DUMP_SHARD);
    write_one_at_a_time(&one_at_a_time, &keys);

    let batched = reclaim_dump_engine(dir.path(), "batch");
    batched.load_shard(RECLAIM_DUMP_SHARD);
    write_as_batches(&batched, &keys, BATCH);

    let single_claims = claims_by_bucket(&one_at_a_time, RECLAIM_DUMP_SHARD);
    let batch_claims = claims_by_bucket(&batched, RECLAIM_DUMP_SHARD);

    // DENOMINATORS FIRST. Without dirty buckets on both sides every count below is 0 == 0.
    assert!(
        !single_claims.is_empty(),
        "the control wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert!(
        !batch_claims.is_empty(),
        "the batch arm wrote {KEYS} keys and dirtied no bucket, so it proves nothing"
    );
    assert_eq!(
        single_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        batch_claims
            .iter()
            .map(|(bucket, _, _)| *bucket)
            .collect::<Vec<_>>(),
        "the two arms dirtied different buckets, so they are not comparable"
    );

    // THE CONTROL, both halves separately.
    let single_wal = single_claims
        .iter()
        .filter(|(_, wal, _)| *wal > 0)
        .count();
    let single_index_log = single_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        single_wal,
        single_claims.len(),
        "the single-command control left a dirty bucket with no WAL claim: {single_claims:?}"
    );
    assert_eq!(
        single_index_log,
        single_claims.len(),
        "the single-command control left a dirty bucket with no index-log claim: \
         {single_claims:?}"
    );

    // THE SUBJECT, the same two halves, never added together.
    let batch_wal = batch_claims.iter().filter(|(_, wal, _)| *wal > 0).count();
    let batch_index_log = batch_claims
        .iter()
        .filter(|(_, _, index_log)| *index_log > 0)
        .count();
    assert_eq!(
        batch_wal,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the WAL, so \
         the reclaim plan reads them as unknown and refuses: {batch_claims:?}",
        batch_claims.len() - batch_wal,
        batch_claims.len()
    );
    assert_eq!(
        batch_index_log,
        batch_claims.len(),
        "{} of {} batch-dirtied buckets could not name their oldest undumped write in the index \
         log: {batch_claims:?}",
        batch_claims.len() - batch_index_log,
        batch_claims.len()
