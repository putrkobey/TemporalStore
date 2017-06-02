// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metaserver-driven raft leader failover planning.
//!
//! The metaserver's failure detector ([`SingleNodeMeta::freeze_stale_resources`]
//! in `meta/meta_lifecycle.rs`) marks a datanode whose heartbeats have gone stale as
//! [`MetaEntityState::Frozen`]. On its own that freeze is inert for a
//! raft-replicated shard: the surviving replicas are never told their leader is
//! gone, so their raft view keeps the dead node marked alive, `tick_election`
//! (`raft/cluster_election.rs`) never observes a dead leader, and writes to that
//! group stall indefinitely. That stall is the "freeze" this feature fixes.
//!
//! This module adds the missing detection->trigger bridge. Given the current
//! server membership, [`compute_raft_failover_triggers`] produces, for every
//! frozen server, the set of still-live (Normal) servers the metaserver should
//! notify so raft can re-elect. The metaserver binary drives the plan by POSTing
//! the dead node's liveness (`node_id`, `alive = false`) to each surviving
//! replica and then asking it to run its native failover
//! (`/raft/admin/failover` -> `RaftCluster::failover_primary`). That native path
//! is guarded by raft's own safety checks in `elect_leader`
//! (`raft/cluster_inner.rs`): a live majority is required (a minority partition
//! cannot elect -> no split-brain) and the promoted candidate's log must be
//! up to date (`candidate_log_would_win` -> no committed-write loss). The
//! metaserver never selects a leader itself.
//!
//! The planner is pure and deterministic (frozen nodes ordered by node id then
//! address, live targets ordered by address) so it is fully unit-testable.

use super::*;

/// A single planned raft failover trigger: one dead (frozen) server whose raft
/// leadership must move, plus the live servers to notify so a surviving replica
/// re-elects through its own native, safety-guarded election path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftFailoverTrigger {
    /// Raft node id of the dead/frozen server, marked not-alive on the replicas.
    pub dead_node_id: u64,
    /// Address of the dead/frozen server (for logging and per-episode dedup).
    pub dead_server_addr: String,
    /// Live (Normal) server addresses to drive the native raft failover on.
    pub live_targets: Vec<String>,
}

/// Pure planner: for every frozen server produce a failover trigger listing the
/// live (Normal) servers to notify. Deterministic — triggers ordered by dead
/// node id then address; targets ordered by address. Returns an empty plan when
/// there are no frozen servers or no live targets (nothing safe to drive).
/// A server as the failover planner sees it.
///
/// The planner needs to know which node is which and whether it is serving. A
/// server record also carries one shard load and one serving state per shard
/// the node holds, and cloning those to read three fields cost 4.7ms a tick on
/// 32 nodes holding 1000 shards each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverMember {
    pub server_addr: String,
    pub node_id: u64,
    pub state: MetaEntityState,
}

impl FailoverMember {
    pub fn of(server: &ServerMetaInfo) -> Self {
        Self {
            server_addr: server.server_addr.clone(),
            node_id: server.node_id,
            state: server.state,
        }
    }
}

pub fn compute_raft_failover_triggers(servers: &[FailoverMember]) -> Vec<RaftFailoverTrigger> {
    let mut live_targets: Vec<String> = servers
        .iter()
        .filter(|server| server.state == MetaEntityState::Normal)
        .map(|server| server.server_addr.clone())
        .collect();
    live_targets.sort();
    live_targets.dedup();
    if live_targets.is_empty() {
        // No surviving replica to elect onto — leave the group as-is rather than
        // fabricate a leader (mirrors compute_auto_rebalance's empty-live guard).
        return Vec::new();
    }

    let mut dead: Vec<&FailoverMember> = servers
        .iter()
        .filter(|server| server.state == MetaEntityState::Frozen)
        .collect();
    dead.sort_by(|a, b| {
        a.node_id
            .cmp(&b.node_id)
            .then_with(|| a.server_addr.cmp(&b.server_addr))
    });

    dead.into_iter()
        .map(|server| RaftFailoverTrigger {
            dead_node_id: server.node_id,
            dead_server_addr: server.server_addr.clone(),
            live_targets: live_targets.clone(),
        })
        .collect()
}

impl SingleNodeMeta {
    /// Compute the raft failover plan for the current membership: every frozen
    /// server maps to the live (Normal) servers the metaserver should drive to
    /// re-elect. Pure read of the shared meta state.
    pub fn plan_raft_failover(&self) -> Vec<RaftFailoverTrigger> {
