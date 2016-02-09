// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta topology/listing/freeze/finish-load lifecycle methods, extracted from meta.rs.

use super::*;

/// One round of the simple failure detector, in the shape the metrics recorder
/// takes.
///
/// The fields left empty belong to the adaptive detector: safe-mode holds,
/// per-location damage, reboot detection, the orphan guard. This detector has
/// none of them, so empty is what actually happened rather than a placeholder.
fn simple_conviction_round(
    frozen_servers: Vec<String>,
    frozen_proxies: Vec<String>,
) -> AdaptiveConvictionReport {
    AdaptiveConvictionReport {
        status: Status::ok(),
        frozen_servers,
        frozen_proxies,
        held_by_safe_mode: Vec::new(),
        damage: Vec::new(),
        rebooted: Vec::new(),
        detector_paused: false,
        held_by_orphan_guard: Vec::new(),
        orphaned_shards: Vec::new(),
    }
}

impl SingleNodeMeta {
    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        let response = self.get_table_topology_answered(request);
        if !response.status.ok {
            // The request counter counts a refusal as a query like any other, so
            // a client looping against a name that is not there looked exactly
            // like one being served.
            self.metrics.record_topology_query_failed();
        }
        response
    }

    fn get_table_topology_answered(
        &self,
        request: GetTableTopologyRequest,
    ) -> TableTopologyResponse {
        // Timed around the whole answer, lock included: a client waiting on a
        // metaserver that is busy waits for the lock as surely as for the work,
        // and timing only the work would report the metaserver as fast while
        // every caller saw it as slow.
        let started = std::time::Instant::now();
        let response = self.get_table_topology_timed(request);
        self.metrics
            .record_topology_latency(started.elapsed().as_micros() as u64);
        response
    }

    fn get_table_topology_timed(
        &self,
        request: GetTableTopologyRequest,
    ) -> TableTopologyResponse {
        self.counters.topology_query_total.fetch_add(1, Ordering::Relaxed);
        // Resolving a topology only reads. It took the exclusive lock solely to
        // count, which serialised every client's and every proxy's hottest call
        // against each other and against all metadata writes.
        let state = self.inner.read().expect("meta lock poisoned");
        let Some(table) = state
            .tables
            .get(&table_key(&request.namespace, &request.table_name))
        else {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table not found"),
                table: None,
                shards: Vec::new(),
                unchanged: false,
            };
        };
        if table.info.state == MetaEntityState::Dropped {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table is dropped"),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: false,
            };
        }
        if table.info.state == MetaEntityState::Frozen {
            return TableTopologyResponse {
                status: Status::error("resource_frozen", "table is frozen"),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: false,
            };
        }
        // A namespace freeze covers every table in it, including tables
        // created after the freeze -- checking the table alone would let one
        // slip through.
        match state.namespaces.get(&request.namespace).copied() {
            Some(MetaEntityState::Dropped) => {
                return TableTopologyResponse {
                    status: Status::error("table_not_found", "namespace is dropped"),
                    table: Some(table.info.clone()),
                    shards: Vec::new(),
                    unchanged: false,
                };
            }
            Some(MetaEntityState::Frozen) => {
                return TableTopologyResponse {
                    status: Status::error("resource_frozen", "namespace is frozen"),
                    table: Some(table.info.clone()),
                    shards: Vec::new(),
                    unchanged: false,
                };
            }
            _ => {}
        }
        if request.old_topology_version >= table.info.topology_version {
            return TableTopologyResponse {
                status: Status::ok(),
                table: Some(table.info.clone()),
                shards: Vec::new(),
                unchanged: true,
            };
        }
        let shards = build_shards(&state, &table.info, &request.client_location);
        // A shard that is serving but holds fewer servers than the table asks
        // for is under-replicated, and the answer goes out saying so without
        // anything noticing. A shard with no primary is out of service on
        // purpose and is not short, it is stopped.
        let short = shards
            .iter()
            .filter(|shard| {
                shard.primary.is_some()
                    && (shard.replicas.len() as u64) < table.info.replica_count
            })
            .count();
        self.metrics.record_placement(short);
        TableTopologyResponse {
            status: Status::ok(),
            table: Some(table.info.clone()),
            shards,
            unchanged: false,
        }
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        // Tally every namespace in one pass. Counting each namespace's tables
        // by filtering the whole table set cost namespaces times tables, which
        // a metrics scrape pays on every tick: 151.8us at 32 namespaces of 32
        // tables, against 0.4us at 4 of 4.
        let mut table_counts: BTreeMap<&str, usize> = BTreeMap::new();
        for table in state.tables.values() {
            if table.info.state != MetaEntityState::Dropped {
                *table_counts
                    .entry(table.info.namespace.as_str())
                    .or_default() += 1;
            }
        }
        let namespaces = state
            .namespaces
            .iter()
            .map(|(namespace, state_value)| NamespaceMetaInfo {
                namespace: namespace.clone(),
                // A namespace holding no table is absent from the tally, not
                // zero, so it still reports zero here.
                table_count: table_counts
                    .get(namespace.as_str())
                    .copied()
                    .unwrap_or_default(),
                state: *state_value,
            })
            .collect();
        ListNamespacesResponse {
            status: Status::ok(),
            namespaces,
