// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Placement-rule-aware shard rebalancing.
//!
//! Two parts of the metaserver decide where a shard lives, and today they
//! disagree.
//!
//! [`build_shards`] (`meta/meta_partitioning.rs`) builds the topology a client reads.
//! It is placement-aware: it spreads a shard's replicas across distinct
//! locations and distinct hosts, because two replicas sharing a rack or a
//! machine are one fault away from being zero replicas.
//!
//! [`compute_auto_rebalance`] is the part that actually *moves* shards, and it
//! knows none of that. It sees a flat shard-to-owner map and a flat set of live
//! addresses, so it will happily:
//!
//! * evacuate a shard out of the location its table asked for, because a server
//!   in some other location happened to hold fewer shards;
//! * leave a shard sitting in a location its table no longer prefers, because
//!   the owner is live and that is the only thing evacuation checks;
//! * pile every shard of one table onto one node while the *total* shard counts
//!   look perfectly balanced, so losing that single node takes the whole table
//!   down -- which is exactly the outcome replica placement exists to prevent.
//!
//! This module supplies the placement rules the mover is missing:
//!
//! 1. **Location scoping.** A shard is only placed on a server in the location
//!    its table prefers, whenever that location has any live server. A shard
//!    already sitting outside it is moved back
//!    ([`ShardReassignmentReason::LocationViolation`]).
//! 2. **Per-table balancing.** Balance is computed per table rather than over
//!    total shard counts, so each table spreads across the nodes available to it
//!    instead of being allowed to single-home while the global picture looks
//!    even.
//! 3. **A safe gap.** A server sheds load only once it is more than
//!    [`AutoRebalanceOptions::balance_safe_gap`] shards above its domain's fair
//!    share, which stops the planner from chasing a one-shard imbalance and
//!    moving data on every round.
//!
//! Placement decisions are grouped into *balance domains*: shards that share
//! both the same set of eligible servers and (when per-table balancing is on)
//! the same table. Each domain is balanced independently, which is what makes
//! "spread this table across its own rack" expressible at all.
//!
//! Like [`compute_auto_rebalance`], the planner is pure and deterministic --
//! domains are visited in sorted order and every tie breaks on the lowest server
//! address or shard id -- so it is fully unit-testable and produces the same
//! plan on every node.

use std::collections::BTreeSet;

use super::*;

/// Eligible-set key for a shard with no location constraint.
const ANY_LOCATION: &str = "*";

/// A live placement target: an address plus the location it sits in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlacementTarget {
    pub server_addr: String,
    /// Location tag, or empty when the server declares none.
    pub location: String,
}

/// What the planner needs to know about one shard beyond its current owner.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPlacement {
    /// Namespace-qualified table the shard belongs to. Balancing groups by this
    /// so no single table ends up homed on one node.
    pub table_key: String,
    /// The location the shard's table prefers, or empty for "anywhere". Honoured
    /// only while that location still has a live server: a preference is not
    /// allowed to strand a shard.
    pub preferred_location: String,
}

/// A balance domain: the shards sharing an eligible server set and (when
/// per-table balancing is on) a table, plus that eligible set.
#[derive(Debug)]
struct BalanceDomain {
    eligible: BTreeSet<String>,
    shards: BTreeSet<ShardId>,
}

/// Everything the passes need, resolved once up front so the planner is plain
/// map lookups rather than repeated placement reasoning.
struct PlacementIndex {
    /// Eligible server set per key (`ANY_LOCATION`, or a location tag).
    eligible_sets: BTreeMap<String, BTreeSet<String>>,
    /// Which eligible set each shard resolves to.
    shard_eligible: BTreeMap<ShardId, String>,
    /// Which table each shard belongs to (empty when unknown).
    shard_table: BTreeMap<ShardId, String>,
}

impl PlacementIndex {
    fn eligible(&self, shard_id: ShardId) -> &BTreeSet<String> {
        let key = self
            .shard_eligible
            .get(&shard_id)
            .map(String::as_str)
            .unwrap_or(ANY_LOCATION);
        self.eligible_sets
            .get(key)
            .expect("every resolved key was inserted")
    }

    fn table(&self, shard_id: ShardId) -> &str {
        self.shard_table
            .get(&shard_id)
            .map(String::as_str)
            .unwrap_or("")
    }
}

/// Working state threaded through the passes: who owns what, and the two load
/// views target selection reads.
struct PlacementState {
    owner: BTreeMap<ShardId, Option<String>>,
    /// Total shards per server.
    load: BTreeMap<String, usize>,
    /// Shards of one table per server, keyed `(table, server)`.
    table_load: BTreeMap<(String, String), usize>,
}

impl PlacementState {
    fn place(&mut self, shard_id: ShardId, table: &str, from: Option<&str>, to: &str) {
        if let Some(from_addr) = from {
            if let Some(count) = self.load.get_mut(from_addr) {
                *count = count.saturating_sub(1);
            }
            if let Some(count) = self
                .table_load
                .get_mut(&(table.to_string(), from_addr.to_string()))
            {
                *count = count.saturating_sub(1);
            }
        }
        *self.load.entry(to.to_string()).or_default() += 1;
        *self
            .table_load
            .entry((table.to_string(), to.to_string()))
            .or_default() += 1;
        self.owner.insert(shard_id, Some(to.to_string()));
    }

    /// Pick the best placement target from `eligible`: fewest shards of this
    /// table first, then fewest shards overall, then the lowest address.
    ///
    /// Ordering on the table count first is what keeps one table from stacking
    /// onto a node that happens to be globally idle -- the same reason replicas
    /// are spread rather than packed.
    fn best_target(&self, eligible: &BTreeSet<String>, table: &str) -> String {
        eligible
            .iter()
            .min_by(|addr_a, addr_b| {
                let table_a = self
                    .table_load
                    .get(&(table.to_string(), (*addr_a).clone()))
                    .copied()
                    .unwrap_or_default();
                let table_b = self
                    .table_load
                    .get(&(table.to_string(), (*addr_b).clone()))
                    .copied()
                    .unwrap_or_default();
                let total_a = self.load.get(*addr_a).copied().unwrap_or_default();
                let total_b = self.load.get(*addr_b).copied().unwrap_or_default();
                table_a
                    .cmp(&table_b)
                    .then_with(|| total_a.cmp(&total_b))
                    .then_with(|| addr_a.cmp(addr_b))
            })
            .cloned()
            .expect("eligible set is non-empty")
    }
}

/// Pure planner: bring `shard_owners` onto `live_servers` while honouring the
/// placement rules in `shard_placement`.
///
/// Runs in three passes, all sharing [`AutoRebalanceOptions::max_moves`]:
/// evacuate shards whose owner is gone, pull back shards sitting outside their
/// table's location, then balance each domain. Shards with no entry in
/// `shard_placement` are unconstrained and share one domain, which is exactly
/// [`compute_auto_rebalance`]'s behaviour -- so a cluster that declares no
/// placement at all plans the same way.
pub fn compute_placement_aware_rebalance(
    shard_owners: &BTreeMap<ShardId, String>,
    shard_placement: &BTreeMap<ShardId, ShardPlacement>,
    live_servers: &[PlacementTarget],
    options: AutoRebalanceOptions,
) -> Vec<ShardReassignment> {
    let mut plans = Vec::new();
    if live_servers.is_empty() {
        // Nowhere to place shards; leave the map untouched rather than drop routes.
        return plans;
    }

    let index = build_index(shard_owners, shard_placement, live_servers, options);
    let all_servers = index
        .eligible_sets
        .get(ANY_LOCATION)
        .expect("the unconstrained set is always present");

    // Seed the working state from the current map. A shard on a dead owner is
    // recorded as unplaced so it contributes to nobody's load.
    let mut state = PlacementState {
        owner: BTreeMap::new(),
        load: all_servers.iter().map(|addr| (addr.clone(), 0)).collect(),
        table_load: BTreeMap::new(),
    };
    for (shard_id, current) in shard_owners {
        if all_servers.contains(current) {
            *state.load.get_mut(current).expect("seeded") += 1;
            *state
                .table_load
                .entry((index.table(*shard_id).to_string(), current.clone()))
                .or_default() += 1;
            state.owner.insert(*shard_id, Some(current.clone()));
        } else {
            state.owner.insert(*shard_id, None);
        }
    }

    // Pass 1 -- evacuate every shard whose owner is no longer live.
    for (shard_id, current) in shard_owners {
        if plans.len() >= options.max_moves {
            return plans;
        }
        if all_servers.contains(current) {
            continue;
        }
        let table = index.table(*shard_id).to_string();
        let target = state.best_target(index.eligible(*shard_id), &table);
        state.place(*shard_id, &table, None, &target);
        plans.push(ShardReassignment {
            shard_id: *shard_id,
            from_server: Some(current.clone()),
            to_server: target,
            reason: ShardReassignmentReason::OwnerUnavailable,
        });
    }

    // Pass 2 -- pull back shards whose live owner sits outside their table's
    // location. Evacuation never catches these: the owner is perfectly healthy,
    // it is just in the wrong place.
    if options.location_scoped {
        for (shard_id, current) in shard_owners {
            if plans.len() >= options.max_moves {
                return plans;
            }
            if !all_servers.contains(current) {
                continue; // already handled by pass 1
            }
            let eligible = index.eligible(*shard_id);
            if eligible.contains(current) {
                continue;
            }
            let table = index.table(*shard_id).to_string();
            let target = state.best_target(eligible, &table);
            state.place(*shard_id, &table, Some(current), &target);
            plans.push(ShardReassignment {
                shard_id: *shard_id,
                from_server: Some(current.clone()),
                to_server: target,
                reason: ShardReassignmentReason::LocationViolation,
            });
        }
    }

    // Pass 3 -- balance each domain independently.
    if options.balance_load {
        for domain in build_domains(&index, &state, options).values() {
            if plans.len() >= options.max_moves {
                return plans;
            }
            balance_domain(domain, &index, &mut state, &mut plans, options);
        }
    }

    plans
}

/// Resolve every shard's eligible server set and table once.
fn build_index(
    shard_owners: &BTreeMap<ShardId, String>,
    shard_placement: &BTreeMap<ShardId, ShardPlacement>,
    live_servers: &[PlacementTarget],
    options: AutoRebalanceOptions,
) -> PlacementIndex {
    let mut eligible_sets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let all = eligible_sets.entry(ANY_LOCATION.to_string()).or_default();
    for target in live_servers {
        all.insert(target.server_addr.clone());
    }
    // A preference is matched by hierarchical containment rather than string
    // equality, so a table pinned to `dc1` accepts any server beneath it -- with
    // exact matching, only a single rack could ever be named.
    let parsed_targets = live_servers
        .iter()
        .map(|target| (target, Location::parse(&target.location)))
        .collect::<Vec<_>>();
    let mut wanted_locations = BTreeSet::new();
    for placement in shard_placement.values() {
        if !placement.preferred_location.is_empty() {
            wanted_locations.insert(placement.preferred_location.clone());
        }
    }
    for wanted in wanted_locations {
        let pattern = Location::parse(&wanted);
        let matching = parsed_targets
            .iter()
            .filter(|(_, location)| location.belongs_to(&pattern))
            .map(|(target, _)| target.server_addr.clone())
            .collect::<BTreeSet<_>>();
        eligible_sets.insert(wanted, matching);
    }

    let mut shard_eligible = BTreeMap::new();
    let mut shard_table = BTreeMap::new();
    for shard_id in shard_owners.keys() {
        let placement = shard_placement.get(shard_id);
        if let Some(placement) = placement {
            shard_table.insert(*shard_id, placement.table_key.clone());
        }
        // A preference is honoured only while its location still has capacity;
        // otherwise the shard falls back to anywhere rather than being stranded.
        let key = match placement {
            Some(placement)
                if options.location_scoped
                    && !placement.preferred_location.is_empty()
                    && eligible_sets
                        .get(&placement.preferred_location)
                        .is_some_and(|servers| !servers.is_empty()) =>
            {
                placement.preferred_location.clone()
            }
            _ => ANY_LOCATION.to_string(),
        };
        shard_eligible.insert(*shard_id, key);
    }

    PlacementIndex {
        eligible_sets,
        shard_eligible,
        shard_table,
    }
}

/// Group the shards into balance domains, keyed for deterministic iteration.
/// The eligible set is part of the key: two tables pinned to different racks
/// must never be balanced against each other.
fn build_domains(
    index: &PlacementIndex,
    state: &PlacementState,
    options: AutoRebalanceOptions,
) -> BTreeMap<(String, String), BalanceDomain> {
    let mut domains: BTreeMap<(String, String), BalanceDomain> = BTreeMap::new();
    for shard_id in state.owner.keys() {
        let eligible_key = index
            .shard_eligible
            .get(shard_id)
            .cloned()
            .unwrap_or_else(|| ANY_LOCATION.to_string());
        let table_key = if options.per_table_balance {
            index.table(*shard_id).to_string()
        } else {
            String::new()
        };
        domains
            .entry((eligible_key, table_key))
            .or_insert_with(|| BalanceDomain {
                eligible: index.eligible(*shard_id).clone(),
                shards: BTreeSet::new(),
            })
            .shards
            .insert(*shard_id);
    }
    domains
}

/// Move shards off the busiest servers in one domain until nobody sits more than
/// `balance_safe_gap` above the domain's fair share. Each move strictly reduces
/// the spread, so this terminates.
fn balance_domain(
    domain: &BalanceDomain,
    index: &PlacementIndex,
    state: &mut PlacementState,
    plans: &mut Vec<ShardReassignment>,
    options: AutoRebalanceOptions,
) {
    if domain.eligible.is_empty() {
        return;
    }
    // Domain-local counts: only this domain's shards, only on its servers.
    let mut counts: BTreeMap<String, usize> = domain
        .eligible
        .iter()
        .map(|addr| (addr.clone(), 0))
        .collect();
    let mut placed = 0_usize;
    for shard_id in &domain.shards {
        if let Some(Some(addr)) = state.owner.get(shard_id) {
            if let Some(count) = counts.get_mut(addr) {
                *count += 1;
                placed += 1;
            }
        }
    }
    if placed == 0 {
        return;
    }
    // Ceiling share: with 5 shards over 2 servers the fair share is 3, not 2, so
    // a server holding 3 is not treated as overloaded.
    let fair_share = placed.div_ceil(domain.eligible.len());
    let safe_line = fair_share + options.balance_safe_gap;

    while plans.len() < options.max_moves {
        let Some((busy_addr, busy_count)) = counts
            .iter()
            .max_by(|(addr_a, count_a), (addr_b, count_b)| {
                count_a.cmp(count_b).then_with(|| addr_b.cmp(addr_a))
            })
            .map(|(addr, count)| (addr.clone(), *count))
        else {
            return;
        };
        if busy_count <= safe_line {
            return; // nobody is above their fair share plus the gap
        }
        let Some((idle_addr, idle_count)) = counts
            .iter()
            .min_by(|(addr_a, count_a), (addr_b, count_b)| {
                count_a.cmp(count_b).then_with(|| addr_a.cmp(addr_b))
            })
            .map(|(addr, count)| (addr.clone(), *count))
        else {
            return;
        };
        if busy_count <= idle_count + 1 {
            return; // moving would only invert the imbalance
        }
        // Highest shard id on the busy server, for a deterministic choice.
        let Some(shard_id) = domain
            .shards
            .iter()
            .filter(|shard_id| {
                state.owner.get(*shard_id).and_then(|addr| addr.as_deref())
                    == Some(busy_addr.as_str())
            })
            .max()
            .copied()
        else {
            return;
        };

        let table = index.table(shard_id).to_string();
        state.place(shard_id, &table, Some(&busy_addr), &idle_addr);
        *counts.get_mut(&busy_addr).expect("busy is in domain") -= 1;
        *counts.get_mut(&idle_addr).expect("idle is in domain") += 1;
        plans.push(ShardReassignment {
            shard_id,
            from_server: Some(busy_addr),
            to_server: idle_addr,
            reason: ShardReassignmentReason::Rebalance,
        });
    }
}

impl SingleNodeMeta {
    /// Live placement targets: Normal-state servers with the location they
    /// declared at registration.
    pub fn placement_targets(&self) -> Vec<PlacementTarget> {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut targets = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .map(|server| PlacementTarget {
                server_addr: server.server_addr.clone(),
                location: server.location.clone(),
            })
            .collect::<Vec<_>>();
        targets.sort();
        targets
    }

    /// Placement constraints for every registered shard, derived from the table
    /// that owns it. A shard whose table is missing, dropped or frozen carries
    /// no constraint and is planned as unconstrained.
    pub fn shard_placements(&self) -> BTreeMap<ShardId, ShardPlacement> {
        let state = self.inner.read().expect("meta lock poisoned");
        shard_owning_tables(&state)
