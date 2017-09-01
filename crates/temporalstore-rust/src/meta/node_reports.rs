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
