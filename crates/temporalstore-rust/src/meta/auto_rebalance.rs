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

impl Default for AutoRebalanceOptions {
    fn default() -> Self {
        Self {
            max_moves: 64,
            balance_load: true,
            location_scoped: true,
            per_table_balance: true,
            balance_safe_gap: 0,
        }
    }
}

/// Pure planner: compute the reassignments that bring `shard_owners` onto the
/// set of `live_servers`, evacuating unavailable owners first and then (if
/// enabled) balancing load. `shard_owners` maps shard id → current owner addr
/// (owner may be a dead/frozen server, not present in `live_servers`).
/// Plan from owners the caller holds.
///
/// Kept for callers outside this crate that already have an owned map; it
/// borrows from theirs rather than copying it again. The metaserver uses
/// [`compute_auto_rebalance_borrowed`], which never makes the copies.
pub fn compute_auto_rebalance(
    shard_owners: &BTreeMap<ShardId, String>,
    live_servers: &BTreeSet<String>,
    options: AutoRebalanceOptions,
) -> Vec<ShardReassignment> {
    let borrowed = shard_owners
        .iter()
        .map(|(shard_id, owner)| (*shard_id, owner.as_str()))
        .collect::<BTreeMap<_, _>>();
    // A caller holding its own owner map has no pins to give; the metaserver's
    // own path reads them from the metadata and passes them.
    compute_auto_rebalance_borrowed(&borrowed, live_servers, &BTreeSet::new(), options)
}

/// Plan without owning the owner names.
///
/// They are compared and named as targets, never kept, and the state they come
/// from is read-locked for the whole round -- so copying one per serving shard
/// bought nothing.
pub fn compute_auto_rebalance_borrowed(
    shard_owners: &BTreeMap<ShardId, &str>,
    live_servers: &BTreeSet<String>,
    pinned: &BTreeSet<ShardId>,
    options: AutoRebalanceOptions,
) -> Vec<ShardReassignment> {
    let mut plans = Vec::new();
    if live_servers.is_empty() {
        // Nowhere to place shards; leave the map untouched rather than drop routes.
        return plans;
    }

    // Per-live-server load, counted from the current state. Counting borrows
    // the owner names rather than copying them.
    let mut load: BTreeMap<&str, usize> =
        live_servers.iter().map(|addr| (addr.as_str(), 0)).collect();
    for current in shard_owners.values() {
        if let Some(count) = load.get_mut(*current) {
            *count += 1;
        }
    }

    // The working owner map is what records where a shard went and what pass 2
    // reads to find one on the busiest server. Both are needed only once
    // something moves, and a cluster with every owner alive and its load even
    // moves nothing -- so this is built when the first move needs it rather
    // than for every round. Building it costs a walk of every shard in the
    // cluster, which is the whole cost of a round that decides to do nothing.
    let mut owner: Option<BTreeMap<ShardId, Option<&str>>> = None;

    // Pass 1 — evacuate every shard whose owner is unavailable onto the
    // least-loaded live server (ties: lowest address).
    for (shard_id, current) in shard_owners {
        if plans.len() >= options.max_moves {
            return plans;
        }
        if live_servers.contains(*current) {
            continue;
        }
        let target = least_loaded_server(&load);
        *load.get_mut(target).expect("target is live") += 1;
        owner_map(&mut owner, shard_owners, live_servers).insert(*shard_id, Some(target));
        plans.push(ShardReassignment {
            shard_id: *shard_id,
            from_server: Some((*current).to_string()),
            to_server: target.to_string(),
            reason: ShardReassignmentReason::OwnerUnavailable,
        });
    }

    // Pass 2 — spread load: while the busiest live server has more than one
    // shard beyond the idlest, move a shard from busiest → idlest. Deterministic
    // and guaranteed to terminate (each move strictly reduces the spread).
    if options.balance_load {
        while plans.len() < options.max_moves {
            let Some((busy_addr, busy_load)) = most_loaded_server(&load) else {
                break;
            };
            let idle_addr = least_loaded_server(&load);
            let idle_load = load.get(&idle_addr).copied().unwrap_or_default();
            if busy_load <= idle_load + 1 {
                break;
            }
            // Pick a shard currently on the busy server (highest id, for
            // determinism) to move to the idle server.
            // A pinned shard is where an operator put it, so balancing does
            // not take it away. Pass 1 above does not consult this: a pin is a
            // preference, and a preference must never leave a shard stranded on
            // an owner that is gone.
            let Some(shard_id) = owner_map(&mut owner, shard_owners, live_servers)
                .iter()
                .filter(|(shard_id, owner_addr)| {
                    **owner_addr == Some(busy_addr) && !pinned.contains(shard_id)
                })
                .map(|(shard_id, _)| *shard_id)
                .max()
            else {
                break;
            };
            owner_map(&mut owner, shard_owners, live_servers).insert(shard_id, Some(idle_addr));
            *load.get_mut(busy_addr).expect("busy is live") -= 1;
            *load.get_mut(idle_addr).expect("idle is live") += 1;
            plans.push(ShardReassignment {
                shard_id,
                from_server: Some(busy_addr.to_string()),
                to_server: idle_addr.to_string(),
                reason: ShardReassignmentReason::Rebalance,
            });
        }
    }

    plans
}

/// The working owner map, built the first time a move needs it.
///
/// A round that moves nothing never calls this, which is the point: building it
/// walks every shard in the cluster, and that walk was the whole cost of
/// deciding to do nothing.
fn owner_map<'slot, 'data>(
    slot: &'slot mut Option<BTreeMap<ShardId, Option<&'data str>>>,
    shard_owners: &'data BTreeMap<ShardId, &'data str>,
    live_servers: &'data BTreeSet<String>,
) -> &'slot mut BTreeMap<ShardId, Option<&'data str>> {
    slot.get_or_insert_with(|| {
        shard_owners
            .iter()
            .map(|(shard_id, current)| {
                let held = live_servers.get(*current).map(|addr| addr.as_str());
                (*shard_id, held)
            })
            .collect()
    })
}

fn least_loaded_server<'a>(load: &BTreeMap<&'a str, usize>) -> &'a str {
    load.iter()
        .min_by(|(addr_a, load_a), (addr_b, load_b)| {
            load_a.cmp(load_b).then_with(|| addr_a.cmp(addr_b))
        })
        .map(|(addr, _)| *addr)
        .expect("load map is non-empty when live servers exist")
}

fn most_loaded_server<'a>(load: &BTreeMap<&'a str, usize>) -> Option<(&'a str, usize)> {
    load.iter()
        .max_by(|(addr_a, load_a), (addr_b, load_b)| {
            load_a.cmp(load_b).then_with(|| addr_b.cmp(addr_a))
        })
        .map(|(addr, count)| (*addr, *count))
}

impl SingleNodeMeta {
    /// Snapshot of the current shard→owner locations.
    pub fn shard_locations(&self) -> Vec<ShardLocation> {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut locations = state.shards.values().cloned().collect::<Vec<_>>();
        locations.sort_by_key(|location| location.shard_id);
        locations
    }

    /// Compute the auto-rebalance plan for the current membership using default
    /// options. Only Normal-state servers are eligible placement targets.
    pub fn plan_auto_rebalance(&self) -> Vec<ShardReassignment> {
        self.plan_auto_rebalance_with_options(AutoRebalanceOptions::default())
    }

    pub fn plan_auto_rebalance_with_options(
        &self,
        options: AutoRebalanceOptions,
    ) -> Vec<ShardReassignment> {
        let state = self.inner.read().expect("meta lock poisoned");
        let live_servers = state
            .servers
            .values()
