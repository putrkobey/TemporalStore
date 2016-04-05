// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Topology / placement / stats helpers, extracted from meta.rs.

use super::*;
use std::sync::atomic::Ordering;
use std::collections::BTreeSet;
use crate::types::{ShardId, Status};

pub(super) fn table_shard_id(
    table: &TableMetaInfo,
    offset: u64,
) -> Result<ShardId, crate::partition_id::PartitionIdError> {
    Ok(table.first_shard_id + offset)
}

/// Whether `table` owns `shard_id`.
///
/// A table's shard ids are `first_shard_id + offset` for every offset below its
/// shard count, which makes them a contiguous range -- so this is a bounds
/// check, not a search. It used to be written as a search: every candidate id
/// was generated and compared, one per shard in the table.
///
/// That cost is paid per lookup, and the two callers that matter look one up
/// for every registered shard in the cluster. Retention planning and placement
/// were therefore quadratic in the fleet and linear again in the width of each
/// table -- a hundred tables of a hundred shards each turned ten thousand
/// lookups into a hundred million comparisons, on a timer.
///
/// `saturating_add` rather than `+`: `first_shard_id` is supplied by the caller
/// that created the table, and a value near the top of the range would overflow
/// on the way to computing the end of it.
fn table_owns_shard(table: &TableMetaInfo, shard_id: ShardId) -> bool {
    let first = table.first_shard_id;
    shard_id >= first && shard_id < first.saturating_add(table.shard_count)
}

/// Which table owns each registered shard.
///
/// Built once per round. The obvious way -- asking [`table_for_shard`] for every
/// registered shard -- costs shards times tables, because that answers by
/// scanning every table, and the rounds that want this run on an interval
/// holding the read lock that heartbeats wait on.
///
/// A table's shards are a contiguous range, so the ranges sorted by their start
/// answer each shard with a binary search. Ranges are disjoint in any cluster
/// that assigns them the way `add_table` does; when two really do overlap the
/// per-shard scan still decides, because it resolves them by table map order and
/// that is the behaviour to keep.
pub(super) fn shard_owning_tables(state: &MetaState) -> BTreeMap<ShardId, &TableRecord> {
    // Learning the ranges costs a pass over the tables and a sort; the scan
    // costs a pass over the tables per shard. So the index only pays once there
    // are more shards than the log of the table count -- with a handful of
    // shards registered and a large table map, which is what a metaserver looks
    // like just after a restart, building it would be the more expensive way to
    // answer.
    let table_count = state.tables.len();
    let log_tables = (usize::BITS - table_count.leading_zeros()) as usize;
    if state.shards.len() <= log_tables {
        return state
            .shards
            .keys()
            .filter_map(|shard_id| Some((*shard_id, table_for_shard(state, *shard_id)?)))
            .collect();
    }
    let tables_in_order = state.tables.values().collect::<Vec<_>>();
    let mut ranges = tables_in_order
        .iter()
        .enumerate()
        .map(|(order, table)| {
            let first = table.info.first_shard_id;
            (first, first.saturating_add(table.info.shard_count), order)
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|(first, _, _)| *first);
    let disjoint = ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0);
    state
        .shards
        .keys()
        .filter_map(|shard_id| {
            let table = if disjoint {
                let above = ranges.partition_point(|(first, _, _)| first <= shard_id);
                let (first, end, order) = *ranges.get(above.checked_sub(1)?)?;
                (*shard_id >= first && *shard_id < end).then(|| tables_in_order[order])?
            } else {
                table_for_shard(state, *shard_id)?
            };
            Some((*shard_id, table))
        })
        .collect()
}

pub(super) fn table_for_shard<'a>(state: &'a MetaState, shard_id: ShardId) -> Option<&'a TableRecord> {
    if !state.table_ranges_overlap {
        // The shard belongs to the nearest table starting at or below it, if
        // that table's range reaches it. Nothing else can own it: every other
        // table starts above the shard, or ends before the nearest one begins.
        return state
            .table_starts
            .range(..=shard_id)
            .next_back()
            .and_then(|(_, key)| state.tables.get(key))
            .filter(|table| table_owns_shard(&table.info, shard_id));
    }
    // Two tables have claimed overlapping shards at some point, and this
    // answered those with the first in map order, which the index cannot
    // reproduce. So it answers them.
    state
        .tables
        .values()
        .find(|table| table_owns_shard(&table.info, shard_id))
}

pub(super) fn push_replica(
    state: &MetaState,
    replicas: &mut Vec<String>,
    seen_replicas: &mut BTreeSet<String>,
    used_hosts: &mut BTreeSet<String>,
    server_addr: &str,
) {
    if !seen_replicas.insert(server_addr.to_string()) {
        return;
    }
    let host = server_host(server_addr);
    if !host.is_empty() {
        used_hosts.insert(host);
    }
    replicas.push(server_addr.to_string());
}

pub(super) fn placement_shard_state_penalty(serving_state: &str) -> u8 {
    match serving_state {
        "serving" | "readonly" => 0,
        "queued" | "running" | "loading" => 1,
        "freezing" | "unloading" => 2,
        "failed" => 3,
        _ => 1,
    }
}

pub(super) fn server_host(server_addr: &str) -> String {
    if let Some(stripped) = server_addr.strip_prefix('[') {
        return stripped
            .split_once(']')
            .map(|(host, _)| host.to_string())
            .unwrap_or_else(|| server_addr.to_string());
    }
    server_addr
        .rsplit_once(':')
        .map(|(host, port)| {
            if port.chars().all(|ch| ch.is_ascii_digit()) {
                host.to_string()
            } else {
                server_addr.to_string()
            }
        })
        .unwrap_or_else(|| server_addr.to_string())
}

pub(super) fn server_endpoint(state: &MetaState, server_addr: &str) -> ServerEndpoint {
    ServerEndpoint {
        server_addr: server_addr.to_string(),
        location: state
            .servers
            .get(server_addr)
            .map(|server| server.location.clone())
            .unwrap_or_default(),
    }
}

pub(super) fn ensure_server(state: &mut MetaState, server_addr: &str) {
    state
        .servers
        .entry(server_addr.to_string())
        .or_insert_with(|| ServerMetaInfo {
            registered_at_ms: 0,
            reported_record_count: 0,
            reported_storage_bytes: 0,
            numa_nodes: Vec::new(),
            load_key_count: 0,
            load_memory_bytes: 0,
            worst_shard_state_penalty: 0,
            freeze_reason: FreezeReason::Unspecified,
            server_addr: server_addr.to_string(),
            node_id: 0,
            location: String::new(),
            state: MetaEntityState::Normal,
            last_heartbeat_ms: now_ms(),
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 0,
            reported_boot_time_ms: 0,
            reboot_detected: false,
            reports_shard_states: false,
            binary_version: String::new(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
}

