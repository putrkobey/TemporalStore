// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metaserver-driven automatic shard placement.
//!
//! The metaserver owns a plain shard→owner map ([`ShardLocation`]). On its own
//! that map is only ever rewritten by a datanode self-registering a shard, so a
//! membership change (a node going down, or a fresh node joining) never moves a
//! shard: an orphaned shard keeps pointing at a dead owner, and a new node is
//! never given any load.
//!
//! This module adds the missing placement brain. Given the current shard→owner
//! map and the set of live (Normal-state) servers, [`compute_auto_rebalance`]
//! produces the reassignments needed to (1) evacuate every shard whose owner is
//! no longer serving onto a live server, and (2) spread the shards roughly
//! evenly so a freshly-joined server actually receives load. The metaserver
//! binary drives the resulting plan — telling the target node to load the shard
//! and rewriting the owner map — so clients stop resolving to a dead node.
//!
//! The planner is pure and deterministic (ties break on the lowest server
//! address / shard id) so it is fully unit-testable and produces the same plan
//! on every node.

use std::collections::BTreeSet;

use super::*;

/// Why a shard is being reassigned to a new owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardReassignmentReason {
    /// The shard's current owner is not a live (Normal) server — it was frozen,
    /// dropped, or is absent — so the shard must move to a live server.
    OwnerUnavailable,
    /// The shard's owner is live, but load is uneven; the shard moves to spread
    /// shards more evenly across live servers (e.g. onto a newly-joined node).
    Rebalance,
    /// The shard's owner is live and healthy but is not serving this shard, so
    /// every read routed to it misses. Produced by [`ShardChecker`]; evacuation
    /// cannot catch it because the owner itself is perfectly available -- it is
    /// the shard that is gone, not the server.
    OwnerDiverged,
    /// The shard's owner is live but sits outside the location its table asked
    /// for, so the shard moves back into that location. Only produced by
    /// [`compute_placement_aware_rebalance`]; evacuation cannot catch this case
    /// because the owner is healthy.
    LocationViolation,
}

impl ShardReassignmentReason {
    /// Stable label for metrics and logs. Matches the serde representation, so
    /// a dashboard and a JSON payload name the same cause the same way.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerUnavailable => "owner_unavailable",
            Self::Rebalance => "rebalance",
            Self::LocationViolation => "location_violation",
            Self::OwnerDiverged => "owner_diverged",
        }
    }
}

/// A single planned shard ownership change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReassignment {
    pub shard_id: ShardId,
    /// The previous owner address, if the shard was registered before.
    #[serde(default)]
    pub from_server: Option<String>,
    /// The chosen new owner (always a live/Normal server).
    pub to_server: String,
    pub reason: ShardReassignmentReason,
}

/// Options controlling how aggressively the planner rebalances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoRebalanceOptions {
    /// Cap on the number of reassignments produced in one planning round.
    pub max_moves: usize,
    /// Enable the load-balancing pass (spread shards onto under-loaded live
    /// servers). Evacuation of unavailable owners always runs regardless.
    pub balance_load: bool,
    /// Confine placement to the location the shard's table prefers, and pull
    /// back shards that have drifted out of it. Read only by
    /// [`compute_placement_aware_rebalance`]; the flat planner has no notion of
    /// location and ignores this.
    pub location_scoped: bool,
    /// Balance each table across its eligible servers separately, instead of
    /// balancing total shard counts. Without it a table can be entirely homed on
    /// one node while global counts look even. Read only by
    /// [`compute_placement_aware_rebalance`].
    pub per_table_balance: bool,
    /// How many shards above its fair share a server may hold before it sheds
    /// load. Damps churn: without it the planner chases a one-shard imbalance
    /// and moves data every round. Read only by
    /// [`compute_placement_aware_rebalance`].
    pub balance_safe_gap: usize,
}
