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
        shard_state_penalty: u8,
        key_count: u64,
        memory_bytes: u64,
    }

    let mut normal_servers = state
        .servers
        .values()
        .filter(|server| server.state == MetaEntityState::Normal)
        .map(|server| {
            PlacementCandidate {
                server_addr: server.server_addr.as_str(),
                location: server.location.as_str(),
                degraded: !server.runtime_load.degraded_reasons.is_empty(),
                queue_depth: server.runtime_load.queue_depth,
                background_queue_depth: server.runtime_load.background_queue_depth,
                running_shard_count: server.runtime_load.running_shard_count,
                dirty_object_count: server.runtime_load.dirty_object_count,
                dirty_shard_count: server.runtime_load.dirty_shard_count,
                shard_state_penalty: server.worst_shard_state_penalty,
                key_count: server.load_key_count,
                memory_bytes: server.load_memory_bytes,
            }
        })
        .collect::<Vec<_>>();
    normal_servers.sort_by(|left, right| {
        (
            left.degraded,
            left.shard_state_penalty,
            left.queue_depth,
            left.background_queue_depth,
            left.running_shard_count,
            left.dirty_object_count,
            left.dirty_shard_count,
            left.key_count,
            left.memory_bytes,
            left.server_addr,
        )
            .cmp(&(
                right.degraded,
                right.shard_state_penalty,
                right.queue_depth,
                right.background_queue_depth,
                right.running_shard_count,
                right.dirty_object_count,
                right.dirty_shard_count,
                right.key_count,
                right.memory_bytes,
                right.server_addr,
            ))
    });
    // Every live server's parsed location, used to size the separation ladder.
    let candidate_locations = normal_servers
        .iter()
        .map(|candidate| Location::parse(candidate.location))
        .collect::<Vec<_>>();
    let caller = Location::parse(client_location);
    // The locations above, reachable by address, so ranking a shard's replicas
    // does not parse them again for every shard.
    let location_of = normal_servers
        .iter()
        .map(|candidate| candidate.server_addr)
        .zip(candidate_locations.iter())
        .collect::<BTreeMap<&str, &Location>>();
    // Derived from the candidate locations alone, so it is the same for every
    // shard of this table. It was being rebuilt per shard.
    let ladder = separation_ladder(&candidate_locations);
    let bucket_count = 1_u64 << 30;
    let mut shards = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let start_bucket = bucket_count * offset / table.shard_count;
        let end_bucket = (bucket_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_hosts = BTreeSet::new();
        let mut placed_locations: Vec<Location> = Vec::new();
        // A shard that is not serving gets no placement at all. Skipping only
        // its recorded owner is not enough: the candidate scan below would pick
        // a primary for it anyway, and it would stay routable.
        let serving = state
            .shards
            .get(&shard_id)
            .map(|location| location.state == MetaEntityState::Normal)
            .unwrap_or(true);
        if !serving {
            shards.push(TableShard {
                shard_id,
                start_bucket,
                end_bucket,
                primary: None,
                replicas: Vec::new(),
                primary_endpoint: None,
                replica_endpoints: Vec::new(),
                // Not serving, so there is no primary to be fenced against.
                load_version: 0,
            });
            continue;
        }
        // A serving shard can still be owned by a server that is not serving.
        // That is a separate question from the one above, and the owner is the
        // one entry that reaches the replica list without passing the Normal
        // filter the candidate scan applies.
        let owner = state
            .shards
            .get(&shard_id)
            .filter(|location| owner_is_serving(state, &location.server_addr));
        if let Some(location) = owner {
            if let Some(server) = state.servers.get(&location.server_addr) {
                placed_locations.push(Location::parse(&server.location));
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_hosts,
                &location.server_addr,
            );
        }
        // Spread replicas as far apart as the topology allows: try the widest
        // separation first (a different top-level domain) and only narrow when
        // nothing qualifies. Comparing whole location strings, as this used to,
        // treats two racks in one availability unit as "different locations" and
        // happily puts both replicas of a shard inside it.
        for &separation in &ladder {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            for (index, candidate) in normal_servers.iter().enumerate() {
                if replicas.len() >= table.replica_count as usize {
                    break;
                }
                if seen_replicas.contains(candidate.server_addr) {
                    continue;
                }
                // Parsed once, above, into candidate_locations. This scan runs
