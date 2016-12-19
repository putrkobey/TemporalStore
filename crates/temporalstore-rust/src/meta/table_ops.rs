// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta table CRUD operations (add/delete/freeze/update_table), extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        // A table lands inside a namespace, so a reserved namespace holds back
        // the tables that would be created in it too.
        if let Some(status) = self.admission_refusal(&MetaMutation::AddTable(request.clone())) {
            return AckResponse { status };
        }
        self.record_mutation(MetaMutation::AddTable(request.clone()));
        self.apply_add_table(request)
    }

    pub(super) fn apply_add_table(&self, request: AddTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        if request.shard_count == 0 {
            return AckResponse {
                status: Status::error("bad_request", "shard_count must be > 0"),
            };
        }
        if let Err(err) = validate_serving_options(&request.serving_options) {
            return AckResponse {
                status: Status::error("bad_request", err),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        self.counters.table_create_total.fetch_add(1, Ordering::Relaxed);
        // Creating a table into a namespace that is not serving would reopen
        // exactly the hole a namespace freeze exists to close.
        match state.namespaces.get(&request.namespace).copied() {
            Some(MetaEntityState::Frozen) => {
                return AckResponse {
                    status: Status::error("resource_frozen", "namespace is frozen"),
                };
            }
            Some(MetaEntityState::Dropped) => {
                return AckResponse {
                    status: Status::error("namespace_not_found", "namespace is dropped"),
                };
            }
            _ => {}
        }
        state
            .namespaces
            .entry(request.namespace.clone())
            .or_insert(MetaEntityState::Normal);
        let key = table_key(&request.namespace, &request.table_name);
        if state.tables.contains_key(&key) {
            return AckResponse {
                status: Status::error("already_exists", "table already exists"),
            };
        }
        let table_id = state.next_table_id;
        state.next_table_id += 1;
        let namespace = request.namespace.clone();
        let table_name = request.table_name.clone();
        let topology_version = record_topology_event(
            &mut state,
            "add_table",
            format!("table:{namespace}/{table_name}"),
            format!(
                "shards={},replicas={}",
                request.shard_count,
                request.replica_count.max(1)
            ),
        );
        let first_shard_id = request.first_shard_id;
        let info = TableMetaInfo {
            table_id,
            namespace: request.namespace,
            table_name: request.table_name,
            state: MetaEntityState::Normal,
            topology_version,
            first_shard_id,
            shard_count: request.shard_count,
            replica_count: request.replica_count.max(1),
            partition_version: request.partition_version,
            serving_options: request.serving_options,
        };
        state.note_table_range(&key, &info);
        state.tables.insert(key, TableRecord { info });
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        let at_ms = self.record_mutation(MetaMutation::DeleteTable(request.clone()));
        self.apply_delete_table(request, at_ms)
    }

    pub(super) fn apply_delete_table(
        &self,
        request: DeleteTableRequest,
