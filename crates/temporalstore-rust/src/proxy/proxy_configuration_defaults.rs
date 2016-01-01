// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::client::{ClientOptions, TemporalStoreClient};

use super::ProxyOptions;

pub(super) fn default_proxy_addr() -> String {
    "127.0.0.1:17000".to_string()
}

pub(super) fn default_service_registry_ttl_ms() -> u64 {
    30_000
}

pub(super) fn default_pin_primary_reads() -> bool {
    true
}

pub(super) fn default_context_first_shard_id() -> crate::types::ShardId {
    1
}

pub(super) fn default_context_shard_count() -> u64 {
    // 0 = follow the cluster. See `ProxyOptions::context_shard_count`.
    0
}

pub(super) fn default_heartbeat_interval_ms() -> u64 {
    // Unchanged from what `main` passed before this became configuration.
    10_000
}

pub(super) fn default_heartbeat_timeout_ms() -> u64 {
    5_000
}

pub(super) fn default_context_io_timeout_ms() -> u64 {
    30_000
}

pub(super) fn default_topology_check_interval_ms() -> u64 {
    50
}

pub(super) fn default_auto_register_min_interval_ms() -> u64 {
    60_000
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
