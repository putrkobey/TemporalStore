// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use crate::block_store::{ElementEntry, BlockStoreError, BlockStore};
use crate::types::{FeaturePoint, ShardId};
use matrixcache::{CacheKey, MultiLayerCache};

use super::constants::{FEATURE_BLOCK_BINARY_MAGIC, FEATURE_BLOCK_MAGIC};
use super::state::{PackedFeatureBlock, PackedFeatureBlockDecode};
use super::hashing::BlockIdentity;
use super::{append_value, read_block_bytes, read_block_bytes_cold, stable_block_object_id};
use crate::storage_config::context_block_target_bytes;
pub(super) fn sorted_feature_points(mut points: Vec<FeaturePoint>) -> Vec<FeaturePoint> {
    if points
        .windows(2)
        .all(|window| window[0].timestamp_ms < window[1].timestamp_ms)
    {
        return points;
    }
    let mut by_timestamp = BTreeMap::new();
    for point in points.drain(..) {
        by_timestamp.insert(point.timestamp_ms, point);
    }
    by_timestamp.into_values().collect()
}

/// Borrows the points instead of owning them, so the page can be serialised without first
/// copying every value. Mirrors `PackedFeatureBlock` field for field -- same names, same order,
/// same types -- and neither carries a serde attribute, so the two produce identical bytes.
#[derive(serde::Serialize)]
struct PackedFeatureBlockRef<'a> {
    version: u8,
    points: &'a [FeaturePoint],
}

/// A page's fixed header: the magic and the point count.
const FEATURE_BLOCK_HEADER_BYTES: usize = FEATURE_BLOCK_BINARY_MAGIC.len() + 4;

/// Per point: an 8-byte timestamp and a 4-byte length in front of the value.
const FEATURE_POINT_HEADER_BYTES: usize = 12;

/// Write a page as bytes.
///
/// A value is a `Vec<u8>`, and JSON writes a byte vector as an array of decimal numbers -- roughly
/// four characters per byte. Measured on the JSON form this replaces, a page was 3.6x its payload,
/// and 4.3x for a small one. The framing and compression work reached the WAL and the index log
/// and never reached the page store, which left this the last place a payload was kept as text.
///
/// The timestamp stays, at 8 bytes rather than the 29 it cost in JSON: it is what picks one point
/// out of a page holding several. Every context write passes a single point, but the feature
/// series path does not, and addressing a point by ordinal instead would change `BlockAddress` and
/// every index that stores one.
pub(super) fn encode_feature_block(points: &[FeaturePoint]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        FEATURE_BLOCK_HEADER_BYTES
            + points
                .iter()
                .map(|point| FEATURE_POINT_HEADER_BYTES + point.value.len())
                .sum::<usize>(),
    );
    bytes.extend_from_slice(FEATURE_BLOCK_BINARY_MAGIC);
    // A page with more points than a u32 can count cannot be written, and cannot occur: the
    // chunker splits on a byte target long before this.
    bytes.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for point in points {
        bytes.extend_from_slice(&point.timestamp_ms.to_le_bytes());
        bytes.extend_from_slice(&(point.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&point.value);
    }
    bytes
}

fn empty_feature_block_encoded_len() -> usize {
    FEATURE_BLOCK_HEADER_BYTES
}

fn feature_point_encoded_len(point: &FeaturePoint) -> usize {
    FEATURE_POINT_HEADER_BYTES + point.value.len()
}

/// State that a timestamped point now lives at an address.
///
/// Emitted from the producer rather than from its callers: eleven handlers append timestamped
/// pages -- feature series, sequences, control state, context events -- and each one having to
/// remember to record the outcome is exactly how a kind ends up silently missing. The series is
/// keyed by timestamp, so that is the component.
/// How a recorded item names the entry a write created.
///
/// Most timestamped series are keyed by the stored timestamp, so the timestamp alone names the
/// entry. A context event is not: its page is timestamp-keyed but its index entry is keyed by the
/// event id, so the timestamp alone names nothing and the identity has to travel with it. Packing
/// both is what list and zset already do with their own keys.
pub(super) fn timestamped_component(stored_key: u64, identity: Option<u64>) -> String {
    match identity {
        Some(identity) => format!("{stored_key:016x}{identity:016x}"),
        None => stored_key.to_string(),
    }
}

fn stage_timestamped_outcomes(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    refs: &[(u64, ElementEntry)],
    identity: Option<u64>,
) {
    if refs.is_empty() {
        return;
    }
    for (timestamp_ms, address) in refs {
        let component = timestamped_component(*timestamp_ms, identity);
        super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: kind.to_string(),
            object_key: key.to_string(),
            component: Some(component.clone()),
            object_id: stable_block_object_id(shard_id, kind, key),
            routing_bucket,
            address: Some(address.clone()),
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        });
    }
}

/// Record that a timestamped point is gone.
///
/// The insert above records where a point landed; without this its removal is invisible, and a
/// shard rebuilt from records alone would keep points the shard itself has dropped.
fn stage_timestamped_removal(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    timestamp_ms: u64,
) {
    let component = timestamp_ms.to_string();
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: key.to_string(),
        component: Some(component.clone()),
        object_id: stable_block_object_id(shard_id, kind, key),
        routing_bucket,
        address: None,
        value: None,
        ttl: None,
        deleted: true,
        meta: false,
    });
}

/// Drop the named points from a series, recording each one.
///
/// Returns whether the series changed, so a caller can mark the shard dirty without repeating
/// the test.
pub(super) fn drop_timestamped_points(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    timestamps: &[u64],
) -> bool {
    let mut dropped = false;
    for timestamp_ms in timestamps {
        if series.get(*timestamp_ms).is_some() {
            series.clear(*timestamp_ms);
            stage_timestamped_removal(shard_id, kind, key, routing_bucket, *timestamp_ms);
            dropped = true;
        }
    }
    dropped
}

/// Trim a series to its configured bound, oldest first, recording every point that went.
///
/// The bound lives in config rather than in the command, which is why replaying a command
/// reproduces this trim only if the config effective at the time is reproduced with it. Recording
/// the trimmed point instead states the result, and needs no config at all.
pub(super) fn trim_timestamped_series(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    max_size: usize,
) -> bool {
    let mut trimmed = false;
    while series.len() > max_size {
        // THE OLDEST POINT IS THE FIRST, because `points` promises ascending order. A
        // representation that could not promise it is not a timestamped series.
        let Some(oldest) = series.points().next().map(|(instant, _)| instant) else {
            break;
        };
        series.clear(oldest);
        stage_timestamped_removal(shard_id, kind, key, routing_bucket, oldest);
        trimmed = true;
    }
    trimmed
}

pub(super) fn append_timestamped_kv_blocks(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    append_timestamped_kv_blocks_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        promote_sync_writes,
        None,
        first_block_ordinal,
    )
}

/// Same, for a series whose index entry is keyed by something other than the stored timestamp.
///
/// The caller passes the key its map will actually use, so the recorded item can name the entry
/// the write created. Without it a record states where a page landed and not what it became.
#[allow(clippy::too_many_arguments)]
pub(super) fn append_timestamped_kv_blocks_keyed(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    identity: u64,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    append_timestamped_kv_blocks_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        promote_sync_writes,
        Some(identity),
        first_block_ordinal,
    )
}

#[allow(clippy::too_many_arguments)]
fn append_timestamped_kv_blocks_inner(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    identity: Option<u64>,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    let object_id = stable_block_object_id(shard_id, kind, key);
    let mut refs = Vec::new();
    let chunks = chunk_timestamped_kv_points(points);
    if !async_storage {
        let mut chunk_points = Vec::with_capacity(chunks.len());
        let mut encoded_blocks = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_blocks.push(encode_feature_block(&chunk));
            chunk_points.push(chunk);
        }
        // Hand the store a slice of each page instead of a clone of it. The page is kept anyway,
        // for the cache put below, so the clone was a second full copy of a JSON page -- and a
        // page is several times its payload, because a value is written as decimal numbers.
        let writes: Vec<crate::block_store::BlockAppendRecord<'_>> = encoded_blocks
            .iter()
            .enumerate()
            .map(|(chunk, packed)| {
                (
                    packed.as_slice(),
                    Some(object_id),
                    Some(routing_bucket),
                    first_block_ordinal.saturating_add(chunk as u32),
                )
            })
            .collect();
        // Carry these pages in this write's record, the way `append_value` does for a single
        // page. This writer batches straight to the block store, so it never staged anything: a
        // synchronous feature write's record named an address and carried nothing, and because it
        // carried nothing the record kept its operation instead. After a crash that loses the
        // un-fsynced block -- which the single barrier permits -- replay installed the outcomes,
        // the outcomes named a block that was never written, and the read had nothing to fall back
        // to. The whole series came back empty.
        for packed in &encoded_blocks {
            super::block_in_wal::stage(object_id, None, packed.as_slice());
        }
        let addresses = block_store.append_batch_with_block_metadata(writes)?;
        if addresses.len() != chunk_points.len() {
            return Err(BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "batch page append returned fewer addresses than chunks",
            )));
        }
        for ((chunk, packed), address) in chunk_points.into_iter().zip(encoded_blocks).zip(addresses)
        {
            if promote_sync_writes {
                cache.put_memory_only(
                    CacheKey::page_with_slot(
                        shard_id,
                        address.block_slab_id(),
                        address.offset(),
                        address.length(),
                        Some(routing_bucket),
                    ),
                    packed,
                );
            }
            refs.extend(
                chunk
                    .into_iter()
                    .map(|point| (point.timestamp_ms, address.clone())),
            );
        }
        stage_timestamped_outcomes(shard_id, kind, key, routing_bucket, &refs, identity);
        return Ok(refs);
    }

    for chunk in chunks {
        let packed = encode_feature_block(&chunk);
        let address = append_value(
            cache,
            block_store,
            shard_id,
            &packed,
            Some(object_id),
            // `None`, matching `stable_block_object_id(shard_id, kind, key)` at the
            // top of this function: a packed series page is not an ELEMENT of anything.
            None,
            Some(routing_bucket),
            async_storage,
        )?;
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    stage_timestamped_outcomes(shard_id, kind, key, routing_bucket, &refs, identity);
    Ok(refs)
}

pub(super) fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    // One point is always exactly one page, so measuring it is wasted work. The split below fires
    // only when `current` is non-empty, which a lone point never is, so its encoded length is
    // computed and never read -- and computing it is a FULL `serde_json` serialisation of the
    // point, the same work `encode_feature_block` is about to do again on the way to the page.
    //
    // A page carries its value as a `Vec<u8>`, which serde_json writes as an array of decimal
    // numbers, so that measurement costs several times the value it measures. Every context write
    // path -- summary, event, index, audit, child, compression -- passes exactly one point.
    if points.len() == 1 {
        return vec![points];
    }
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let empty_block_len = empty_feature_block_encoded_len();
    let mut current_encoded_len = empty_block_len;
    let block_target_bytes = context_block_target_bytes();

    for point in points {
        let point_encoded_len = feature_point_encoded_len(&point);
        let next_encoded_len = current_encoded_len
            .saturating_add(point_encoded_len)
            .saturating_add(if current.is_empty() { 0 } else { 1 });
        if next_encoded_len > block_target_bytes && !current.is_empty() {
            chunks.push(current);
            current = Vec::new();
            current_encoded_len = empty_block_len;
        }
        current_encoded_len = current_encoded_len
            .saturating_add(point_encoded_len)
            .saturating_add(if current.is_empty() { 0 } else { 1 });
        current.push(point);
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
pub(super) fn decode_feature_block(bytes: &[u8]) -> Option<Vec<FeaturePoint>> {
    match decode_feature_block_strict(bytes) {
        PackedFeatureBlockDecode::Packed(points) => Some(points),
        PackedFeatureBlockDecode::Legacy | PackedFeatureBlockDecode::Corrupt(_) => None,
    }
