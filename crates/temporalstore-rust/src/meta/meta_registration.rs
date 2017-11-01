// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta server/proxy/namespace registration + heartbeat, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn register_server(&self, mut request: RegisterServerRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        request.registered_at_ms = now_ms();
        self.record_mutation(MetaMutation::RegisterServer(request.clone()));
        self.apply_register_server(request)
    }

    pub(super) fn apply_register_server(&self, request: RegisterServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.server_register_total.fetch_add(1, Ordering::Relaxed);
        if let Some(existing) = state.servers.get(&request.server_addr) {
            let now = now_ms();
            if existing.state == MetaEntityState::Frozen && existing.freeze_cooldown_until_ms > now
            {
                return AckResponse {
                    status: Status::error("resource_frozen", "server is in freeze cooldown"),
                };
            }
            // A conviction the convicted node can undo by asking is not a
            // conviction. The freeze cooldown alone does not cover this: it
            // defaults to zero, so re-registering immediately returns the server
            // to Normal and the failure detector's decision is erased by the
            // very node it was about. An operator unfreeze is the way back.
            if existing.state == MetaEntityState::Frozen
                && existing.freeze_reason.is_conviction()
                && self.forbid_self_clearing_conviction
            {
                return AckResponse {
                    status: Status::error(
                        "conviction_requires_unfreeze",
                        format!(
                            "server was frozen by the metaserver ({}); an operator must unfreeze it",
                            existing.freeze_reason.as_str()
                        ),
                    ),
                };
            }
        }
        let now = now_ms();
        // Kept from the first registration: a node that restarted and
        // registered again has not newly joined the cluster.
        let registered_at_ms = state
            .servers
            .get(&request.server_addr)
            .map(|server| server.registered_at_ms)
            .filter(|first| *first != 0)
            .unwrap_or(request.registered_at_ms);
        let server_addr = request.server_addr.clone();
        state.servers.insert(
            server_addr.clone(),
            ServerMetaInfo {
                reported_record_count: 0,
                reported_storage_bytes: 0,
                load_key_count: 0,
                load_memory_bytes: 0,
                worst_shard_state_penalty: 0,
                freeze_reason: FreezeReason::Unspecified,
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                // Re-declared on every registration rather than merged: a
                // machine that came back with different hardware is describing
                // itself now, not amending what it said before.
                numa_nodes: request.numa_nodes,
                state: MetaEntityState::Normal,
                last_heartbeat_ms: now,
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                boot_time_ms: 0,
                // Registration is the server declaring a fresh identity, so any
                // previous reboot verdict is cleared and the anchor is re-taken
                // from its next heartbeat.
                reported_boot_time_ms: 0,
                reboot_detected: false,
                reports_shard_states: false,
                binary_version: request.binary_version,
                shard_loads: Vec::new(),
                shard_stat_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
                registered_at_ms,
            },
        );
        // Coming back clears the drop clock. Without this the tombstone
        // outlives the drop it recorded: `stamp_dropped_since` keeps the first
        // time it is given, deliberately, so that re-dropping an already
        // dropped resource cannot restart the clock -- and a resource dropped,
        // revived, and dropped again months later would inherit the original
        // time and be collected on the next round with no grace at all.
        stamp_dropped_since(
            &mut state,
            &dropped_key("server", &server_addr),
            MetaEntityState::Normal,
            now,
        );
        record_topology_event(
            &mut state,
            "register_server",
            format!("server:{server_addr}"),
            "state=normal",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    /// Relabel a registered server's location in place.
    ///
    /// Until now `location` could only be set at registration, so correcting a
    /// mislabelled node meant making it re-register -- which resets its
    /// heartbeat timestamp, its reported shard states, its runtime load and its
    /// freeze bookkeeping, and is refused outright while the node is in freeze
    /// cooldown. That is a disruptive way to fix a label, and it needs the
    /// datanode's cooperation, so an operator could not do it at all.
    ///
    /// The label is not cosmetic: since locations became hierarchical they drive
    /// replica spread and table pinning, so a wrong one actively degrades
    /// placement. This changes the label and nothing else, and bumps the
    /// topology version so clients pick up the placement that follows from it.
    pub fn update_server(&self, request: UpdateServerRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::UpdateServer(request.clone()));
        self.apply_update_server(request)
    }

    pub(super) fn apply_update_server(&self, request: UpdateServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get(&request.server_addr) else {
            return AckResponse {
                status: Status::error("server_not_found", "server not found"),
            };
        };
        // Matches the reference: a server that is not serving is not relabelled.
        // Its placement is not being consulted anyway, and an operator who wants
        // to relabel a frozen node can unfreeze it first.
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error(
                    "resource_frozen",
                    "only a serving server can be relabelled",
                ),
            };
        }
        if server.location == request.location {
            return AckResponse {
                status: Status::error("not_modified", "server location is unchanged"),
