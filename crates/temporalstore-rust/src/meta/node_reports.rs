// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta reporting methods (conformance/info/stats/preflight/topology_version), extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn control_plane_parity_report(&self) -> MetaControlPlaneParityReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let table_topology_ready = !state.tables.is_empty()
            && state
                .tables
                .values()
                .all(|table| table.info.shard_count > 0 && table.info.replica_count > 0);
        let transitional_state_model_ready = state
            .tables
            .values()
            .any(|table| table.info.state != MetaEntityState::Normal)
            || state
                .servers
                .values()
                .any(|server| server.state != MetaEntityState::Normal)
            || state
                .proxies
                .values()
                .any(|proxy| proxy.state != MetaEntityState::Normal)
            || state.topology_events.iter().any(|event| {
                matches!(
                    event.kind.as_str(),
                    "table_state" | "server_state" | "proxy_state"
                )
            });
        let topology_history_ready =
            state.topology_version > 0 && !state.topology_events.is_empty();
        let scheduler_owned_finish_load_ready = !state.scheduler_finish_generations.is_empty();
        let scheduler_generation_check_ready = scheduler_owned_finish_load_ready;
        let durable_replay_ready = self.mutation_log.is_some();
        let real_data_node_coordination_ready = state.servers.values().any(|server| {
            !server.shard_states.is_empty() || !server.runtime_load.degraded_reasons.is_empty()
        });

        let mut blockers = Vec::new();
        if !table_topology_ready {
            blockers.push("table/shard topology evidence missing".to_string());
        }
        if !transitional_state_model_ready {
            blockers.push("transitional table/server/proxy state evidence missing".to_string());
        }
        if !topology_history_ready {
            blockers.push("topology history evidence missing".to_string());
        }
        if !scheduler_owned_finish_load_ready {
            blockers.push("scheduler-owned finish_load token evidence missing".to_string());
        }
        if !durable_replay_ready {
            blockers.push("durable mutation log replay evidence missing".to_string());
        }
        if !real_data_node_coordination_ready {
            blockers.push(
                "real data-node heartbeat/lifecycle coordination evidence missing".to_string(),
            );
        }

        MetaControlPlaneParityReport {
            status: if blockers.is_empty() {
                Status::ok()
            } else {
                Status::error(
                    "metaserver_control_plane_parity_blocked",
                    blockers.join("; "),
                )
            },
            table_topology_ready,
            transitional_state_model_ready,
            topology_history_ready,
            scheduler_owned_finish_load_ready,
            scheduler_generation_check_ready,
            durable_replay_ready,
            real_data_node_coordination_ready,
            scheduler_finish_generation_count: state.scheduler_finish_generations.len(),
            topology_event_count: state.topology_events.len(),
            topology_version: state.topology_version,
            blockers,
        }
    }

    pub fn info(&self) -> MetaInfo {
        let meta_change_muted = self.is_meta_change_muted();
        MetaInfo {
            meta_change_muted,
            status: Status::ok(),
            stats: self.stats(),
            boot_time_ms: self.boot_time_ms,
            durable_mutation_log: self.mutation_log.is_some(),
        }
    }

    pub fn stats(&self) -> MetaStats {
        let state = self.inner.read().expect("meta lock poisoned");
        stats_from_state(&state, &self.counters)
    }

    /// Everything a metrics scrape reports, in one pass under one read lock.
    ///
    /// The scrape reports tables, namespaces and proxy groups by count alone,
    /// and servers and proxies by a handful of fields each. Getting there by
    /// listing all five cloned every resource whole -- including each server's
    /// per-shard loads and serving states, which the scrape never reads. On
    /// 4096 tables that was 940us of a 1093us scrape; on 32 servers holding
    /// 1000 shards each it was most of 6.1ms.
    pub fn metrics_report(&self) -> MetaMetricsReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let mut report = MetaMetricsReport {
            status: Status::ok(),
            tables: StateTally::default(),
            namespaces: StateTally::default(),
            proxy_groups: StateTally::default(),
            servers: Vec::with_capacity(state.servers.len()),
            proxies: Vec::with_capacity(state.proxies.len()),
        };
        for table in state.tables.values() {
            report.tables.record(table.info.state);
        }
        for namespace_state in state.namespaces.values() {
            report.namespaces.record(*namespace_state);
        }
        for group in state.proxy_groups.values() {
            report.proxy_groups.record(group.state);
        }
        for server in state.servers.values() {
            report.servers.push(ServerScrapeRow {
                server_addr: server.server_addr.clone(),
                state: server.state,
                reported_record_count: server.reported_record_count,
                reported_storage_bytes: server.reported_storage_bytes,
                last_meta_topology_version: server.runtime_load.last_meta_topology_version,
                rejected_total: server.runtime_load.rejected_total,
                timed_out_total: server.runtime_load.timed_out_total,
                canceled_total: server.runtime_load.canceled_total,
            });
        }
        for proxy in state.proxies.values() {
            report.proxies.push(ProxyScrapeRow {
                proxy_addr: proxy.proxy_addr.clone(),
                state: proxy.state,
                restart_count: proxy.restart_count,
            });
        }
        report
    }

    pub fn preflight_report(&self) -> MetaPreflightReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let normal_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .count();
        let frozen_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .count();
        let normal_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Normal)
            .count();
        let frozen_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .count();
        let dropped_tables = state
