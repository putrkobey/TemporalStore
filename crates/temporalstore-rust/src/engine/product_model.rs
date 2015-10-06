// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Product data-model read + aggregation helpers.
//!
//! Backs the engine's higher-level model queries: decoding sequence rows from the
//! shared feature timestamped-KV storage and applying `FeatureFilter` predicates,
//! and computing control-state family aggregates (Counter sum/count/min/max/
//! first/last, Distinct cardinality, Selection first/last). Write paths live in
//! the engine's execute path; this module is the read/aggregation side.

use super::timestamped_series::TimestampedSeries as _;
use std::collections::BTreeMap;

use crate::block_store::{ElementEntry, BlockStore};
use crate::types::{
    FeatureFilter, FeatureFilterOp, ControlStateFamily,
    ControlStateSelectionType, SequenceFeatureRow, ShardId,
};
use matrixcache::MultiLayerCache;

use super::packed_blocks::decode_feature_block_strict;
use super::state::PackedFeatureBlockDecode;
use super::hashing::BlockIdentity;
use super::{parse_i64, read_block_bytes, ShardState};
pub(super) fn read_sequence_row(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &ElementEntry,
    // WHICH PAGE, stated by the caller. A packed series page holds many points and is not an
    // element of its object, so the element half is `None` -- but the OBJECT half cannot be
    // recovered here: this function is handed an address and a timestamp, and neither names a key.
    // See [`BlockIdentity`] for why it is the terms rather than an id that cross.
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
) -> Option<SequenceFeatureRow> {
    let bytes = read_block_bytes(cache, block_store, shard_id, address, identity, routing_bucket)?;
    match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .and_then(|point| serde_json::from_slice(&point.value).ok()),
        PackedFeatureBlockDecode::Legacy => serde_json::from_slice(&bytes).ok(),
        PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

pub(super) fn sequence_filter_matches(row: &SequenceFeatureRow, filter: &FeatureFilter) -> bool {
    let lhs = match filter.field.as_str() {
        "gid" => row.gid,
        "action_type" => row.action_type as u64,
        "duration" => row.duration as u64,
        "author_id" => row.author_id,
        _ => return false,
    };
    match filter.op {
        FeatureFilterOp::Equal => lhs == filter.value,
        FeatureFilterOp::NotEqual => lhs != filter.value,
        FeatureFilterOp::GreaterThan => lhs > filter.value,
        FeatureFilterOp::GreaterOrEqual => lhs >= filter.value,
        FeatureFilterOp::LessThan => lhs < filter.value,
        FeatureFilterOp::LessOrEqual => lhs <= filter.value,
    }
}

pub(super) fn sequence_rows_in_range(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: &[FeatureFilter],
) -> Vec<SequenceFeatureRow> {
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket =
        crate::engine::hashing::block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    shard
        .features()
        .get(key)
        .map(|series| {
            series
                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row(
                        cache,
                        block_store,
                        shard_id,
                        timestamp_ms,
                        address,
                        BlockIdentity::of(shard_id, "feature", key, None),
                        Some(routing_bucket),
                    )
                })
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn aggregate_feature_values(values: &[Vec<u8>], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" => values.iter().filter_map(parse_i64).sum(),
        "avg" | "average" => {
            let numeric = values.iter().filter_map(parse_i64).collect::<Vec<_>>();
            if numeric.is_empty() {
                0
            } else {
                numeric.iter().sum::<i64>() / numeric.len() as i64
            }
        }
        "min" => values
            .iter()
            .filter_map(parse_i64)
            .min()
            .unwrap_or_default(),
        "max" => values
            .iter()
            .filter_map(parse_i64)
            .max()
            .unwrap_or_default(),
        "first" => values.first().and_then(parse_i64).unwrap_or_default(),
        "last" => values.last().and_then(parse_i64).unwrap_or_default(),
        "count" | "events" | "" => values.len() as i64,
        _ => values.len() as i64,
    }
}

pub(super) fn aggregate_control_state_values(values: &[i64], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" | "count" | "" => values.iter().sum(),
        "events" | "len" => values.len() as i64,
        "min" => values.iter().copied().min().unwrap_or_default(),
        "max" => values.iter().copied().max().unwrap_or_default(),
        "first" => values.first().copied().unwrap_or_default(),
        "last" => values.last().copied().unwrap_or_default(),
        _ => values.iter().sum(),
    }
}

pub(super) fn is_control_state_change_aggregator(aggregator: &str) -> bool {
    aggregator.eq_ignore_ascii_case("change")
}

/// UUID idempotency dedup window for control-state writes (300s), matching the
/// control_state `kUUIDExpiredTime` production value.
pub(super) const CONTROL_STATE_UUID_DEDUP_MS: u64 = 300_000;

