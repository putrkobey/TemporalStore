// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::ShardId;

pub fn shard_id_for_key(
    key: &str,
    first_shard_id: ShardId,
    shard_count: u64,
    default_shard_id: ShardId,
) -> ShardId {
    if shard_count == 0 {
        return default_shard_id;
    }
    first_shard_id + bucket_id_for_key(key) % shard_count
}

pub fn bucket_id_for_key(key: &str) -> u64 {
    crc64_jones(key.as_bytes()) >> 34
}

pub fn crc64_jones(bytes: &[u8]) -> u64 {
    let mut crc = 0_u64;
    for byte in bytes {
        let mut entry = (crc ^ u64::from(*byte)) & 0xff;
        for _ in 0..8 {
            if entry & 1 == 1 {
                entry = (entry >> 1) ^ 0x95ac9329ac4bc9b5;
            } else {
                entry >>= 1;
            }
        }
        crc = entry ^ (crc >> 8);
    }
    crc
}

pub fn stable_key_hash(key: &str) -> u64 {
    crc64_jones(key.as_bytes())
}

pub fn key_is_dropped_by_percent(key: &str, drop_percent: u8) -> bool {
    let drop_percent = drop_percent.min(100);
    drop_percent > 0 && (stable_key_hash(key) % 100) < u64::from(drop_percent)
}

#[cfg(test)]
mod bucket_ownership_tests {
    use super::*;

    /// Where a key goes is decided by the bucket MODULO the shard count, not by the bucket
    /// ranges the metaserver publishes.
    ///
    /// Both exist. `bucket_id_for_key` puts a key in one of `1 << 30` buckets, the same slot
    /// space the metaserver uses, and `build_shards` gives every shard a contiguous
    /// `start_bucket..=end_bucket` slice of it (`bucket_count * offset / shard_count`). Nothing
    /// routes by those ranges: this function takes the bucket modulo the count instead, so the
    /// two describe different owners for the same key -- at four shards they disagree for about
    /// three quarters of the bucket space.
    ///
    /// That is not currently a fault, because the ranges are only reported (in the client's
    /// partition-set report, where they fall back to the same even split) and the data node
    /// serves whatever shard it is asked for. It matters for anything built on them later: a
    /// range split moves a contiguous slice of buckets, and this mapping would not follow it.
    ///
    /// This pins the mapping that is real, so that a change to range routing is a deliberate one
    /// and not something discovered afterwards.
    ///
    /// If the two are ever reconciled, the RANGES are the side to keep and this mapping is the
    /// side to change. Three things point that way, none of them a preference:
    ///
    /// - The ranges are already published. A key's owner is then a property of the topology the
    ///   metaserver hands out, so a client that reads them cannot disagree with the cluster about
    ///   who owns what; a modulo can, and does, the moment the two are derived differently.
    /// - The ranges survive a split. Moving a contiguous slice of buckets to a new shard changes
    ///   one range boundary and leaves every other key where it was. Changing `shard_count` under
    ///   a modulo remaps very nearly everything -- at four shards to eight, seven keys in eight.
    /// - A range lookup is not more expensive. It is a binary search over one entry per shard,
