// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Table partition/placement construction, extracted from meta.rs.

use super::*;
use std::collections::BTreeSet;

/// The `load_version` the PRIMARY reports for this shard, or 0 when nothing is known.
///
/// This is the same value `validate_load_version` compares against on the data node, because it
/// is the server's own report of what it has loaded. Taking the max is deliberate: a server
/// mid-handoff can briefly report two entries for one shard, and the newer one is the one a write
/// should be fenced against.
///
/// 0 on every unknown -- no such server, no entry, not loaded -- so an unknown never produces a
/// token that would fence a client out.
fn reported_load_version(state: &MetaState, server_addr: &str, shard_id: ShardId) -> u64 {
    state
        .servers
        .get(server_addr)
        .and_then(|server| {
            server
                .shard_states
                .iter()
                .filter(|shard_state| shard_state.shard_id == shard_id && shard_state.loaded)
                .map(|shard_state| shard_state.load_version)
                .max()
        })
        .unwrap_or(0)
}

/// Whether the server a shard is registered to is in service.
///
/// An unknown address counts as serving: a route can outlive the server record
/// it names, and treating that as out of service would silently unroute shards
/// the metaserver simply has not heard about yet.
fn owner_is_serving(state: &MetaState, server_addr: &str) -> bool {
    state
        .servers
        .get(server_addr)
        .map(|server| server.state == MetaEntityState::Normal)
        .unwrap_or(true)
}

pub(super) fn build_shards(
    state: &MetaState,
    table: &TableMetaInfo,
    client_location: &str,
) -> Vec<TableShard> {
    /// A server being considered for a shard, borrowed from the metadata
    /// rather than copied out of it.
    ///
    /// These used to own their two strings, which is two heap allocations per
    /// live server on every topology request, for values dropped when the
    /// request ends. The state is read-locked for the whole of this function,
    /// so borrowing is sound and the copies bought nothing.
    #[derive(Debug)]
    struct PlacementCandidate<'a> {
        server_addr: &'a str,
        location: &'a str,
        degraded: bool,
        queue_depth: usize,
        background_queue_depth: usize,
        running_shard_count: usize,
        dirty_object_count: usize,
        dirty_shard_count: usize,
