// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a durable dump manifest still holds after the DEFAULT recovery arm restores from it.
//!
//! A manifest carries a serialized `ShardState`. Three of the model maps that
//! `collect_model_live_block_entries` walks are `skip_serializing` -- `hashes`, `context_events`,
//! `context_indexes` -- so a freshly decoded manifest index has them EMPTY while its
//! `bucket_index.bucket_map`, which does serialize, still names every one of their pages.
//!
//! `rebuild_bucket_block_ownership` clears `bucket_map` and repopulates it from the model maps.
//! Run against a decoded manifest index without first re-deriving the unserialized maps, it
//! therefore deletes from the index exactly the pages only the index still knew about.
//!
//! `install_bucket_dump_manifest` calls `rebuild_unserialized_model_maps_from_bucket_index` first
//! and says in a comment why. The default single-barrier recovery arm in `load_shard_with` decodes
//! the same bytes and calls only the ownership rebuild.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;
const HASH_FIELDS: usize = 6;
const STRING_KEYS: usize = 6;

/// How many fields of the one hash key a read can actually see.
fn readable_hash_fields(engine: &TemporalEngine) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashGetAll {
                key: "h".to_string(),
            },
        })
        .response
    {
        CommandResponse::HashEntries { entries } => entries.len(),
        other => panic!("HashGetAll answered {other:?}"),
    }
}

/// THE CONTROL. `strings` is NOT `skip_serializing`, so it survives the manifest round trip and
/// the ownership rebuild re-derives its pages from a map that still has them. If this ever drops
/// with the hash count, the fault is the restore in general and not the unserialized maps.
fn readable_strings(engine: &TemporalEngine) -> usize {
    (0..STRING_KEYS)
        .filter(|index| {
            matches!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: SHARD,
                        command: Command::StringGet {
                            key: format!("s{index}"),
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(_) }
            )
        })
        .count()
}

fn write_corpus(engine: &TemporalEngine) {
    for index in 0..HASH_FIELDS {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::HashSet {
                key: "h".to_string(),
                field: format!("f{index}"),
                value: format!("hash-value-{index}").into_bytes(),
            },
