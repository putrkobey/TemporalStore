// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shared test helpers/fixtures, split from tests.rs.
#![allow(dead_code)]
use super::*;

pub(super) fn key_for_shard(table: &TemporalStoreTable, shard_id: ShardId) -> String {
    (0..10_000)
        .map(|index| format!("key-{shard_id}-{index}"))
        .find(|key| table.shard_id_for_key(key) == shard_id)
        .unwrap()
}

pub(super) fn free_local_addr() -> String {
