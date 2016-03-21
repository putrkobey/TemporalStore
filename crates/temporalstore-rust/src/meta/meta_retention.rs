// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Retention and garbage collection for dropped meta resources.
//!
//! Dropping a server, proxy or table sets its state to
//! [`MetaEntityState::Dropped`] and stops there — `apply_set_server_state` and
//! its siblings carry an explicit `MetaEntityState::Dropped => {}` arm. The
//! entry itself is never removed.
//!
//! So the metaserver's state only ever grows. Every node ever decommissioned,
//! every proxy ever retired, every table ever deleted stays in [`MetaState`] for
//! the lifetime of the cluster. That is not a tidiness problem:
//!
//! * [`MetaSnapshot`] carries all of it. Snapshots are exported and installed
//!   wholesale, including onto raft peers, so every dropped resource is
//!   re-serialised and re-shipped on every snapshot for as long as the cluster
//!   lives.
//! * `list_servers` and `list_tables` return the tombstones alongside the live
//!   resources, so an operator's view fills with things that no longer exist.
//! * The shard routes of a dropped table stay in the owner map even though
//!   `get_table_topology` already refuses to serve them: pure ballast that
//!   nothing can ever use.
//!
//! There was also nothing to age against, because dropping recorded no
//! timestamp — "dropped long enough ago to forget" was not expressible.
//!
//! This module adds both halves. `MetaState::dropped_since_ms` records when each
//! resource was dropped (kept beside the resources rather than inside them, so
//! the wire types are unchanged, and carried through snapshots so a peer keeps
//! ageing the tombstones it inherits). [`plan_meta_retention`] then decides,
//! purely, which tombstones are old enough to purge.
//!
//! Two guards keep collection from destroying information:
//!
//! * **A dropped server that still owns a shard is never purged.** The owner map
//!   would be left naming an address the metaserver knows nothing about, which
//!   is strictly worse than the tombstone. Such servers are reported as blocked;
//!   once rebalancing has evacuated the shard, the next round collects them.
//! * **Purges are capped per round.** Decommissioning a rack retires many
//!   resources at once, and a pass that rewrote most of the meta state in one
//!   step would stall every other meta operation behind the write lock.
//!
//! A dropped table's shard routes are purged *with* the table, since they are
//! already unroutable and keeping them would leave entries pointing at a table
//! that no longer exists.

use std::collections::BTreeSet;

use super::*;

/// Tuning for [`plan_meta_retention`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionOptions {
    /// How long a dropped server's tombstone is kept.
    pub server_retention_ms: u64,
    /// How long a dropped proxy's tombstone is kept.
    pub proxy_retention_ms: u64,
    /// How long a dropped table's tombstone is kept.
    pub table_retention_ms: u64,
    /// Most resources purged in one round, so a mass decommission does not
    /// rewrite the whole meta state under a single write lock.
    pub max_purges_per_round: usize,
}

impl Default for MetaRetentionOptions {
    fn default() -> Self {
        Self {
            server_retention_ms: 24 * 60 * 60 * 1_000,
            proxy_retention_ms: 24 * 60 * 60 * 1_000,
            table_retention_ms: 24 * 60 * 60 * 1_000,
            max_purges_per_round: 20,
        }
    }
}

/// One dropped resource the planner may collect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionCandidate {
    /// Address for a server or proxy, namespace-qualified key for a table.
    pub id: String,
    /// When it was dropped, or 0 if unknown (a tombstone predating the field).
    pub dropped_since_ms: u64,
}

/// What one retention round should remove.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionPlan {
    /// Dropped servers old enough to forget, ordered by address.
    pub servers: Vec<String>,
    /// Dropped proxies old enough to forget, ordered by address.
    pub proxies: Vec<String>,
    /// Dropped tables old enough to forget, ordered by key.
    pub tables: Vec<String>,
    /// Shard routes belonging to the purged tables, ordered by shard id.
    pub shards: Vec<ShardId>,
    /// Dropped servers held back because they still own a shard, ordered by
    /// address.
    pub blocked_servers: Vec<String>,
    /// How many otherwise-eligible resources the per-round cap held back.
    pub capped: usize,
}

impl MetaRetentionPlan {
    /// True when this round has nothing to do.
    pub fn is_empty(&self) -> bool {
        self.servers.is_empty()
            && self.proxies.is_empty()
            && self.tables.is_empty()
            && self.shards.is_empty()
    }

    /// Number of resources (not shard routes) this round purges.
    pub fn purge_count(&self) -> usize {
        self.servers.len() + self.proxies.len() + self.tables.len()
    }
}

/// What one retention round actually removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRetentionReport {
    pub status: Status,
    pub plan: MetaRetentionPlan,
}

/// Pure planner: decide which dropped resources are old enough to forget.
///
/// `shard_owners` maps shard id to its recorded owner address and `shard_tables`
/// maps shard id to the key of the table that owns it; both are needed for the
/// guards. Deterministic — every output vector is sorted, and the per-round cap
/// is spent in a fixed order (proxies, then tables, then servers), so the same
/// state always yields the same plan.
pub fn plan_meta_retention(
    servers: &[RetentionCandidate],
    proxies: &[RetentionCandidate],
    tables: &[RetentionCandidate],
    shard_owners: &BTreeMap<ShardId, String>,
    shard_tables: &BTreeMap<ShardId, String>,
    now_ms: u64,
    options: MetaRetentionOptions,
) -> MetaRetentionPlan {
    let mut plan = MetaRetentionPlan::default();

    // A dropped server whose address still appears in the owner map is load
    // bearing: the route names it, so forgetting the server strands the route.
    let owning_addrs = shard_owners.values().collect::<BTreeSet<_>>();

    // A tombstone with no timestamp predates this feature. It is left alone
    // rather than treated as infinitely old, which would purge the entire
    // history on the first round after an upgrade.
    let expired = |candidate: &RetentionCandidate, retention_ms: u64| -> bool {
        candidate.dropped_since_ms != 0
            && now_ms.saturating_sub(candidate.dropped_since_ms) >= retention_ms
    };

    let mut eligible_proxies = proxies
        .iter()
        .filter(|candidate| expired(candidate, options.proxy_retention_ms))
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    eligible_proxies.sort();

    let mut eligible_tables = tables
        .iter()
        .filter(|candidate| expired(candidate, options.table_retention_ms))
        .map(|candidate| candidate.id.clone())
        .collect::<Vec<_>>();
    eligible_tables.sort();

    let mut eligible_servers = Vec::new();
    for candidate in servers {
        if !expired(candidate, options.server_retention_ms) {
            continue;
        }
        if owning_addrs.contains(&candidate.id) {
            plan.blocked_servers.push(candidate.id.clone());
            continue;
        }
        eligible_servers.push(candidate.id.clone());
    }
    eligible_servers.sort();
    plan.blocked_servers.sort();

    // Spend the round's budget in a fixed order: proxies first because they are
    // the cheapest and have no dependents, servers last because they are the
    // ones a route can still be attached to.
    let total_eligible = eligible_proxies.len() + eligible_tables.len() + eligible_servers.len();
    let mut budget = options.max_purges_per_round;
    for (source, sink) in [
        (eligible_proxies, &mut plan.proxies),
        (eligible_tables, &mut plan.tables),
        (eligible_servers, &mut plan.servers),
    ] {
        for item in source {
            if budget == 0 {
                break;
            }
            budget -= 1;
            sink.push(item);
        }
    }
    plan.capped = total_eligible.saturating_sub(plan.purge_count());

    // A purged table's shard routes go with it: they already resolve to nothing.
    let purged_tables = plan.tables.iter().cloned().collect::<BTreeSet<_>>();
    plan.shards = shard_tables
        .iter()
        .filter(|(_, table_key)| purged_tables.contains(*table_key))
        .map(|(shard_id, _)| *shard_id)
        .collect();
    plan.shards.sort();
    plan
}

impl SingleNodeMeta {
    /// Compute the retention plan for the current state without applying it.
    pub fn plan_meta_retention_now(&self, options: MetaRetentionOptions) -> MetaRetentionPlan {
        let now = now_ms();
        let state = self.inner.read().expect("meta lock poisoned");
        // Start from the drop stamps rather than from every resource.
        //
        // The stamps are keyed `<kind>:<id>` and kept for exactly the resources
        // that are dropped and not yet forgotten, so they are already the small
        // set this round is looking for. Walking every table to find the few
        // dropped ones cost the whole table map on every interval, and a
        // resource with no stamp is never collected anyway -- `expired` insists
        // on a non-zero one -- so the scan was producing candidates for the
        // planner to throw away.
        let dropped_of_kind = |kind: &str| -> Vec<(String, u64)> {
            let prefix = format!("{kind}:");
            state
                .dropped_since_ms
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(key, at)| (key[prefix.len()..].to_string(), *at))
                .collect()
        };
        let servers = dropped_of_kind("server")
            .into_iter()
            .filter(|(id, _)| {
                state
                    .servers
                    .get(id)
                    .is_some_and(|server| server.state == MetaEntityState::Dropped)
            })
            .map(|(id, at)| RetentionCandidate {
                id,
                dropped_since_ms: at,
            })
            .collect::<Vec<_>>();
        let proxies = dropped_of_kind("proxy")
            .into_iter()
            .filter(|(id, _)| {
                state
                    .proxies
                    .get(id)
                    .is_some_and(|proxy| proxy.state == MetaEntityState::Dropped)
            })
            .map(|(id, at)| RetentionCandidate {
                id,
                dropped_since_ms: at,
            })
            .collect::<Vec<_>>();
        let tables = dropped_of_kind("table")
            .into_iter()
            .filter(|(id, _)| {
                state
                    .tables
                    .get(id)
                    .is_some_and(|table| table.info.state == MetaEntityState::Dropped)
            })
            .map(|(id, at)| RetentionCandidate {
                id,
                dropped_since_ms: at,
            })
            .collect::<Vec<_>>();
        // Only a dropped server can be held back by still owning shards, and
        // the owner map is read nowhere else -- so with nothing dropped there is
        // nothing for it to say.
        let shard_owners = if servers.is_empty() {
            BTreeMap::new()
        } else {
            state
                .shards
                .values()
                .map(|location| (location.shard_id, location.server_addr.clone()))
                .collect::<BTreeMap<_, _>>()
        };
        // A shard is only collected because the table that owns it is being
        // collected, so with no dropped tables this map cannot contribute a
        // single shard -- and deriving it walks every registered shard.
        let shard_tables = if tables.is_empty() {
            BTreeMap::new()
        } else {
            shard_owning_tables(&state)
                .into_iter()
                .map(|(shard_id, table)| {
                    (
                        shard_id,
                        table_key(&table.info.namespace, &table.info.table_name),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        plan_meta_retention(
            &servers,
            &proxies,
            &tables,
            &shard_owners,
            &shard_tables,
            now,
            options,
        )
    }

    /// Compute and apply one retention round: forget the dropped resources whose
    /// tombstones have aged out, along with the shard routes of any purged
    /// table.
    pub fn purge_expired_meta(&self, options: MetaRetentionOptions) -> MetaRetentionReport {
        let plan = self.plan_meta_retention_now(options);
        self.metrics.record_retention(&plan);
        if plan.is_empty() {
            return MetaRetentionReport {
                status: Status::ok(),
                plan,
            };
        }
        // Recorded before it is applied, so a restart does not resurrect what
        // this round forgot. Without this the log still holds the register and
        // drop of every purged resource, replay brings them all back, and the
        // GC has to forget them again on every boot.
        self.record_mutation(MetaMutation::PurgeMeta(plan.clone()));
        self.apply_meta_purge(&plan);
        MetaRetentionReport {
            status: Status::ok(),
            plan,
        }
    }

    /// Forget exactly what `plan` names. Takes the plan rather than recomputing
    /// one so that replay and the live round agree.
    pub(crate) fn apply_meta_purge(&self, plan: &MetaRetentionPlan) {
        let mut state = self.inner.write().expect("meta lock poisoned");
        for addr in &plan.proxies {
            state.proxies.remove(addr);
            state.dropped_since_ms.remove(&dropped_key("proxy", addr));
            record_topology_event(
                &mut state,
                "proxy_purged",
                format!("proxy:{addr}"),
                "reason=retention",
            );
        }
        for shard_id in &plan.shards {
            state.shards.remove(shard_id);
        }
        for key in &plan.tables {
            if let Some(record) = state.tables.get(key) {
                let info = record.info.clone();
                state.forget_table_range(key, &info);
            }
            state.tables.remove(key);
            state.dropped_since_ms.remove(&dropped_key("table", key));
            record_topology_event(
                &mut state,
                "table_purged",
                format!("table:{key}"),
                "reason=retention",
            );
        }
        for addr in &plan.servers {
            state.servers.remove(addr);
            state.dropped_since_ms.remove(&dropped_key("server", addr));
            record_topology_event(
                &mut state,
                "server_purged",
                format!("server:{addr}"),
                "reason=retention",
            );
        }
    }

    /// Background loop running [`Self::purge_expired_meta`] on an interval.
    pub fn start_meta_retention_loop(
        &self,
        options: MetaRetentionOptions,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            if meta.is_meta_change_muted() {
                thread::sleep(interval);
                continue;
            }
            let _ = meta.purge_expired_meta(options);
            thread::sleep(interval);
        })
    }
}

/// How long a frozen resource waits before it is dropped, per kind. Zero
/// disables aging for that kind, which is the default for tables: freezing a
/// table is an operator action, and an operator who froze it may still intend to
/// unfreeze it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreezeAgingOptions {
    /// How long a frozen server stays frozen before it is dropped.
    pub server_freeze_ms: u64,
    /// How long a frozen proxy stays frozen before it is dropped.
    pub proxy_freeze_ms: u64,
    /// How long a frozen table stays frozen before it is dropped. Zero (the
    /// default) never ages a table.
    pub table_freeze_ms: u64,
    /// Most resources dropped in one round.
    pub max_drops_per_round: usize,
}

impl Default for FreezeAgingOptions {
    fn default() -> Self {
        Self {
            server_freeze_ms: 6 * 60 * 60 * 1_000,
            proxy_freeze_ms: 6 * 60 * 60 * 1_000,
            table_freeze_ms: 0,
            max_drops_per_round: 20,
        }
    }
}

/// What one aging round should drop.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreezeAgingPlan {
    /// Frozen servers to drop, ordered by address.
    pub servers: Vec<String>,
    /// Frozen proxies to drop, ordered by address.
    pub proxies: Vec<String>,
    /// Frozen tables to drop, ordered by key.
    pub tables: Vec<String>,
    /// How many otherwise-eligible resources the per-round cap held back.
    pub capped: usize,
}

impl FreezeAgingPlan {
    pub fn is_empty(&self) -> bool {
        self.servers.is_empty() && self.proxies.is_empty() && self.tables.is_empty()
    }

    pub fn drop_count(&self) -> usize {
        self.servers.len() + self.proxies.len() + self.tables.len()
    }
}

/// What one aging round actually dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreezeAgingReport {
    pub status: Status,
    pub plan: FreezeAgingPlan,
}

/// Pure planner: decide which frozen resources have waited long enough to be
/// dropped. `candidates` carry the time each resource was frozen; a zero
/// timestamp means unknown and is never aged, for the same reason an
/// untimestamped tombstone is never purged. Deterministic — output is sorted and
/// the cap is spent proxies, then tables, then servers.
pub fn plan_freeze_aging(
    servers: &[RetentionCandidate],
    proxies: &[RetentionCandidate],
    tables: &[RetentionCandidate],
    now_ms: u64,
    options: FreezeAgingOptions,
) -> FreezeAgingPlan {
    let mut plan = FreezeAgingPlan::default();
    // A zero threshold disables aging for that kind entirely, rather than
    // meaning "immediately" — otherwise an unset knob would drop the fleet.
    let eligible = |candidates: &[RetentionCandidate], threshold_ms: u64| -> Vec<String> {
        if threshold_ms == 0 {
            return Vec::new();
        }
        let mut ids = candidates
            .iter()
            .filter(|candidate| candidate.dropped_since_ms != 0)
            .filter(|candidate| {
                now_ms.saturating_sub(candidate.dropped_since_ms) >= threshold_ms
            })
            .map(|candidate| candidate.id.clone())
            .collect::<Vec<_>>();
        ids.sort();
        ids
    };

    let eligible_proxies = eligible(proxies, options.proxy_freeze_ms);
    let eligible_tables = eligible(tables, options.table_freeze_ms);
    let eligible_servers = eligible(servers, options.server_freeze_ms);
    let total = eligible_proxies.len() + eligible_tables.len() + eligible_servers.len();

    let mut budget = options.max_drops_per_round;
    for (source, sink) in [
        (eligible_proxies, &mut plan.proxies),
        (eligible_tables, &mut plan.tables),
        (eligible_servers, &mut plan.servers),
    ] {
        for item in source {
            if budget == 0 {
                break;
            }
            budget -= 1;
            sink.push(item);
        }
    }
    plan.capped = total.saturating_sub(plan.drop_count());
    plan
}

impl SingleNodeMeta {
    /// Compute the freeze-aging plan for the current state without applying it.
    pub fn plan_freeze_aging_now(&self, options: FreezeAgingOptions) -> FreezeAgingPlan {
        let now = now_ms();
        let state = self.inner.read().expect("meta lock poisoned");
        let servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .map(|server| RetentionCandidate {
                id: server.server_addr.clone(),
                dropped_since_ms: server.frozen_since_ms,
            })
            .collect::<Vec<_>>();
        let proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .map(|proxy| RetentionCandidate {
                id: proxy.proxy_addr.clone(),
                dropped_since_ms: proxy.frozen_since_ms,
            })
            .collect::<Vec<_>>();
        // Tables carry no frozen-at field of their own, so the metaserver keeps
        // it beside them the same way it keeps drop times -- and that record is
        // the frozen set, so this walks it rather than every table. A freeze
        // with no timestamp is never aged, so the two reach the same tables.
        let frozen_prefix = "table:";
        let tables = state
            .frozen_since_ms
            .range(frozen_prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(frozen_prefix))
            .filter_map(|(key, at)| {
                let id = key[frozen_prefix.len()..].to_string();
                state
                    .tables
                    .get(&id)
                    .filter(|table| table.info.state == MetaEntityState::Frozen)
                    .map(|_| RetentionCandidate {
                        id,
                        dropped_since_ms: *at,
                    })
            })
            .collect::<Vec<_>>();
        plan_freeze_aging(&servers, &proxies, &tables, now, options)
    }

    /// Compute and apply one aging round, moving frozen resources that have
    /// waited out their cooldown to [`MetaEntityState::Dropped`].
    ///
    /// This is what makes [`Self::purge_expired_meta`] reachable for anything
    /// the failure detector froze: retention only collects dropped resources, so
    /// without this stage a frozen dead node stays in the meta state - and in
    /// every exported snapshot - forever.
    pub fn age_frozen_meta(&self, options: FreezeAgingOptions) -> FreezeAgingReport {
        let plan = self.plan_freeze_aging_now(options);
        if plan.is_empty() {
            self.metrics.record_freeze_aging(&plan, &plan);
            return FreezeAgingReport {
                status: Status::ok(),
                plan,
            };
        }
        // What the round actually got done, which stops matching the plan the
        // moment a drop is refused: the round returns there and leaves the rest
        // of the plan standing.
        let mut applied = FreezeAgingPlan::default();
        // Routed through the ordinary state setters so the drop is recorded in
        // the mutation log, stamps `dropped_since_ms`, and emits the same
        // topology event an operator-driven drop would.
        for addr in &plan.proxies {
            let response = self.drop_proxy(StateChangeRequest {
                endpoint: addr.clone(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Unspecified,
            });
            if !response.status.ok {
                self.metrics.record_freeze_aging(&plan, &applied);
                return FreezeAgingReport {
                    status: response.status,
                    plan,
                };
            }
            applied.proxies.push(addr.clone());
        }
        for key in &plan.tables {
            // `table_key` joins on '/', so the key must be split on the same
            // separator: a namespace or table name may legitimately contain a
            // dot, and splitting there recovers the wrong pair.
            let Some((namespace, table_name)) = key.split_once('/') else {
                continue;
            };
            let response = self.delete_table(DeleteTableRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
            });
            if !response.status.ok {
                self.metrics.record_freeze_aging(&plan, &applied);
                return FreezeAgingReport {
                    status: response.status,
                    plan,
                };
            }
            applied.tables.push(key.clone());
        }
        for addr in &plan.servers {
            let response = self.drop_server(StateChangeRequest {
                endpoint: addr.clone(),
                freeze_cooldown_ms: 0,
                reason: FreezeReason::Unspecified,
            });
            if !response.status.ok {
                self.metrics.record_freeze_aging(&plan, &applied);
                return FreezeAgingReport {
                    status: response.status,
                    plan,
                };
            }
            applied.servers.push(addr.clone());
        }
        self.metrics.record_freeze_aging(&plan, &applied);
        FreezeAgingReport {
            status: Status::ok(),
            plan,
        }
    }

    /// Background loop running [`Self::age_frozen_meta`] on an interval.
    pub fn start_freeze_aging_loop(
        &self,
        options: FreezeAgingOptions,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            if meta.is_meta_change_muted() {
                thread::sleep(interval);
                continue;
            }
            let _ = meta.age_frozen_meta(options);
            thread::sleep(interval);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 30 * 24 * 60 * 60 * 1_000;
    const DAY: u64 = 24 * 60 * 60 * 1_000;
    const HOUR: u64 = 60 * 60 * 1_000;

    fn aging(options: FreezeAgingOptions) -> FreezeAgingOptions {
        options
    }

    /// A meta with a mutation log, holding one dropped server, one dropped
    /// proxy and one dropped table, all with expired tombstones.
    fn purgeable(log_path: &std::path::Path) -> crate::meta::SingleNodeMeta {
        use crate::meta::*;
        let meta = SingleNodeMeta::with_mutation_log(log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: String::new(),
            location: "rack-1".to_string(),
            config_version: 0,
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        let drop_request = |endpoint: &str| StateChangeRequest {
            endpoint: endpoint.to_string(),
            freeze_cooldown_ms: 0,
            reason: FreezeReason::Unspecified,
        };
        assert!(meta.drop_server(drop_request("node-a")).status.ok);
        assert!(meta.drop_proxy(drop_request("proxy-a")).status.ok);
        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
            })
            .status
            .ok
        );
        meta
    }

    fn purge_everything() -> MetaRetentionOptions {
        MetaRetentionOptions {
            server_retention_ms: 0,
            proxy_retention_ms: 0,
            table_retention_ms: 0,
            ..MetaRetentionOptions::default()
        }
    }

    #[test]
    fn a_dropped_table_is_eventually_forgotten() {
        // Dropping a table is the one path that does not go through the shared
        // state setter, so it was the one path that recorded no drop time. A
        // table with no drop time is never eligible, which left every dropped
        // table -- and every shard route under it -- in the state and in every
        // exported snapshot for the life of the cluster.
        use crate::meta::*;
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "orders".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            partition_version: 0,
            serving_options: TableServingOptions::default(),
        });
        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "orders".to_string(),
            })
            .status
            .ok
        );

        let plan = meta.plan_meta_retention_now(purge_everything());
        assert_eq!(plan.tables, vec!["ns/orders"]);
    }

    #[test]
    fn dropping_a_table_twice_does_not_restart_its_clock() {
        // The retention clock must measure since the first drop; restarting it
        // would let a repeated no-op drop keep a table alive indefinitely.
        use crate::meta::*;
