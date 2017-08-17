// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

use super::state::ShardState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectRuntimeState {
    pub object_id: u64,
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub object_keys: Vec<String>,
    pub model_ids: Vec<String>,
    pub block_ref_count: usize,
    pub hot_block_ref_count: usize,
    pub cold_block_ref_count: usize,
    pub deleted_block_ref_count: usize,
    pub residency: String,
    pub dirty: bool,
    pub deleted: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
}

/// `Default` is the "no shard, nothing to say" report: every count zero and no objects. It is
/// not a valid description of a populated shard and is never returned for one -- see
/// `runtime_report`, which returns it only when the state carries no shard id to derive with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectManagerRuntimeReport {
    pub object_manager_runtime_module: bool,
    pub bucket_index_authority: bool,
    pub live_object_count: usize,
    pub live_block_ref_count: usize,
    pub reused_object_ids: usize,
    pub dirty_object_count: usize,
    pub deleted_object_count: usize,
    pub hot_object_count: usize,
    pub cold_object_count: usize,
    pub mixed_residency_object_count: usize,
    pub object_block_transition_count: usize,
    pub loading_object_count: usize,
    pub in_memory_object_count: usize,
    pub ttl_object_count: usize,
    pub objects: Vec<ObjectRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> ObjectManagerRuntimeReport {
    // NO SHARD, NO REPORT. Every object id below is derived from (shard, kind, key), and a state
    // that never entered the engine has no shard to derive with. An empty report says "nothing to
    // say"; a report built on a guessed zero would name objects belonging to a different shard and
    // nothing downstream could tell the difference.
    let Some(shard_id) = shard.shard_id() else {
        return ObjectManagerRuntimeReport::default();
    };
    let mut object_ids = std::collections::BTreeSet::new();
    let mut object_ref_counts = std::collections::BTreeMap::<u64, usize>::new();
    let mut objects = std::collections::BTreeMap::<u64, ObjectRuntimeState>::new();
    let mut live_block_ref_count = 0usize;

    for bucket in shard.bucket_index.bucket_map.values() {
        for object_id in &bucket.object_index {
            object_ids.insert(*object_id);
            let object_deleted = bucket.deleted() || bucket.deleted_object_index.contains(object_id);
            let object = objects
                .entry(*object_id)
                .or_insert_with(|| ObjectRuntimeState {
                    object_id: *object_id,
                    routing_bucket: bucket.routing_bucket,
                    object_keys: Vec::new(),
