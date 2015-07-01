// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStore, ElementEntry};
use crate::types::ShardId;
use matrixcache::MultiLayerCache;

use super::hashing::BlockIdentity;
use super::read_block_bytes;
use super::state::{
    object_component_lookup_key, object_block_lookup_key, ShardState, BucketLayoutState,
};

/// One bucket's resident runtime state, for the bucket-store runtime report.
///
/// NO `last_dump_sequence`. This row used to carry one, read straight off `BucketNode`, and
/// `runtime_report` is handed a shard and no manifest -- so the only per-bucket figure it could
/// have published was the node's, and the node no longer holds one. The dumped-log watermark is a
/// property of the newest dump manifest, not of a bucket, and the report that has a manifest in
/// hand (`storage_physical_index_report`) is where it is published. Nothing reads this field: the
/// whole of `runtime_report` has one caller, a test helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketRuntimeState {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub block_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub deleted_block_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketStoreRuntimeReport {
    pub bucket_store_runtime_module: bool,
    pub bucket_index_authority: bool,
    pub bucket_count: usize,
    pub block_ref_count: usize,
    pub dirty_bucket_count: usize,
    pub deleted_bucket_count: usize,
    pub empty_buckets: usize,
    pub single_object_buckets: usize,
    pub single_block_object_buckets: usize,
    pub multi_block_object_buckets: usize,
    pub multi_object_buckets: usize,
    pub deleted_block_ref_count: usize,
    pub loading_bucket_count: usize,
    pub in_memory_bucket_count: usize,
    pub ttl_bucket_count: usize,
    pub max_dirty_generation: u64,
    #[serde(rename = "slots")]
    pub buckets: Vec<BucketRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> BucketStoreRuntimeReport {
    let mut report = BucketStoreRuntimeReport {
        bucket_store_runtime_module: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        bucket_count: shard.bucket_index.bucket_map.len(),
        block_ref_count: 0,
        dirty_bucket_count: 0,
        deleted_bucket_count: 0,
        empty_buckets: 0,
        single_object_buckets: 0,
        single_block_object_buckets: 0,
        multi_block_object_buckets: 0,
        multi_object_buckets: 0,
        deleted_block_ref_count: 0,
        loading_bucket_count: 0,
        in_memory_bucket_count: 0,
        ttl_bucket_count: 0,
        max_dirty_generation: 0,
        buckets: Vec::new(),
    };

    for bucket in shard.bucket_index.bucket_map.values() {
        report.block_ref_count = report.block_ref_count.saturating_add(bucket.block_index.len());
        if bucket.dirty() {
            report.dirty_bucket_count = report.dirty_bucket_count.saturating_add(1);
        }
        if bucket.deleted() {
            report.deleted_bucket_count = report.deleted_bucket_count.saturating_add(1);
        }
        if bucket.loading() {
            report.loading_bucket_count = report.loading_bucket_count.saturating_add(1);
        }
        if bucket.in_memory() {
            report.in_memory_bucket_count = report.in_memory_bucket_count.saturating_add(1);
        }
        if bucket.ttl_ms.is_some() {
            report.ttl_bucket_count = report.ttl_bucket_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(bucket.dirty_generation);
        let deleted_block_ref_count = bucket.block_index.values().filter(|block| block.deleted).count();
        report.deleted_block_ref_count = report
            .deleted_block_ref_count
            .saturating_add(deleted_block_ref_count);
        match bucket.layout {
            BucketLayoutState::Empty => report.empty_buckets = report.empty_buckets.saturating_add(1),
            BucketLayoutState::SingleObject => {
                report.single_object_buckets = report.single_object_buckets.saturating_add(1)
            }
            BucketLayoutState::SingleBlockObject => {
                report.single_block_object_buckets = report.single_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiBlockObject => {
                report.multi_block_object_buckets = report.multi_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiObject => {
                report.multi_object_buckets = report.multi_object_buckets.saturating_add(1)
            }
        }
        report.buckets.push(BucketRuntimeState {
            routing_bucket: bucket.routing_bucket,
            layout: bucket_layout_name(bucket.layout).to_string(),
            object_ids: bucket.object_index.iter().copied().collect(),
            block_ref_count: bucket.block_index.len(),
            dirty: bucket.dirty(),
            deleted: bucket.deleted(),
            meta_loaded: bucket.meta_loaded(),
            loading: bucket.loading(),
            in_memory: bucket.in_memory(),
            ttl_ms: bucket.ttl_ms.ms(),
            dirty_generation: bucket.dirty_generation,
            deleted_block_ref_count,
        });
    }

    report
}

#[allow(dead_code)]
fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SingleBlockObject => "single_block_object",
        BucketLayoutState::MultiBlockObject => "multi_block_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

// THE LOOKUP IS THE ONLY DOOR ONTO ONE OBJECT'S PAGES, and these readers go through it.
//
// Each of them answered a question about ONE named object by walking every page of every bucket
// and comparing the entry's own copy of the identity. That comparison is the thing that stops
// being expressible when the entry carries no identity, and it was never the cheap way to ask: the
// object lookup maps (kind, object key) straight to the `(routing_bucket, handle)` of each of that
// object's pages, which is the same answer without the walk.
//
// WHAT THE WALK WAS GUARDING, AND WHY IT IS NOT NEEDED. Each site reached the walk only when the
// lookup held NOTHING -- `object_block_lookup.is_empty()` -- which is an index with pages and no
// lookup. That state was reachable between a decode and the rebuild that filled the lookup, and
// the decode fills it now, so the arm is unreachable in production rather than merely rare. The
// write path files every page it adds; `rebuild_bucket_block_ownership` files what it rebuilds.
//
// AND THE RE-COMPARISON AFTER THE WALK IS A CHECK, NOT A SOURCE. A page reached through its own
// object's ref list has already been matched on identity by the list that produced it; comparing
// the entry's copy again asks whether the lookup is right, not which object the page belongs to.
// The kind is still compared, because `block_refs_for` takes the kind as a key and a ref that
// survives a handle collision could name a page of another kind.

/// Where a read resolves an object's page from.
///
/// THIS IS THE READ PATH, not a reporting path: `read_bucket_index_value` is what
/// `Command::StringGet` calls once the response cache misses. It answers from the bucket index,
/// which is why releasing a bucket's page entries is a question about reads at all -- the fast
/// path in `execute_read_only_fast_path` goes straight to `shard.strings` and never noticed, so a
/// released bucket served every warm read and returned None for every cold one.
///
/// Every "cannot answer" now falls through to the released-bucket lookup rather than returning
/// None, which is what makes a released bucket serve reads identically to a resident one.
pub(super) fn bucket_index_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<ElementEntry> {
    if let Some(block_refs) = shard
        .bucket_index
        .block_refs_for(model_id, object_key, component)
    {
        for block_ref in block_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket) else {
