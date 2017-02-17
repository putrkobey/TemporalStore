// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub mod v1 {
    tonic::include_proto!("temporalstore.v1");
}

use std::sync::Arc;

use tonic::{Request, Response, Status as TonicStatus};

use crate::client::TemporalStoreTable;
use crate::engine::TemporalEngine;
use crate::types;
use crate::{ClientError, TableOptions, TemporalStoreClient};

pub trait TemporalStoreSdkExecutor: Send + Sync + 'static {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse;

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        let responses = request
            .commands
            .into_iter()
            .map(|command| {
                self.execute_sdk(types::ExecuteRequest {
                    shard_id: request.shard_id,
                    command,
                })
            })
            .collect::<Vec<_>>();
        let status = responses
            .iter()
            .find(|response| !response.status.ok)
            .map(|response| response.status.clone())
            .unwrap_or_else(types::Status::ok);
        types::BatchExecuteResponse { status, responses }
    }

    fn open_table_sdk(&self, _request: v1::OpenTableRequest) -> v1::OpenTableResponse {
        v1::OpenTableResponse {
            status: Some(error_status(
                "not_implemented",
                "open_table tonic adapter is not wired to metaserver yet",
            )),
            topology: None,
        }
    }

    fn sync_topology_sdk(&self, _request: v1::SyncTopologyRequest) -> v1::SyncTopologyResponse {
        v1::SyncTopologyResponse {
            status: Some(error_status(
                "not_implemented",
                "sync_topology tonic adapter is not wired to metaserver yet",
            )),
            topologies: Vec::new(),
            topology_version: 0,
        }
    }

    fn client_preflight_sdk(
        &self,
        _request: v1::ClientPreflightRequest,
    ) -> v1::ClientPreflightResponse {
        v1::ClientPreflightResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            route_cache_entries: 0,
            table_cache_entries: 0,
            backend_failure_entries: 0,
            topology_version: 0,
            degraded: false,
            warnings: Vec::new(),
        }
    }
}

impl TemporalStoreSdkExecutor for TemporalEngine {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse {
        self.execute(request)
    }

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        self.batch_execute(request)
    }
}

impl TemporalStoreSdkExecutor for TemporalStoreClient {
    fn execute_sdk(&self, request: types::ExecuteRequest) -> types::ExecuteResponse {
        match self.execute_with_options(request, crate::RequestOptions::default()) {
            Ok(response) => response,
            Err(err) => client_error_execute_response(err),
        }
    }

    fn batch_execute_sdk(
        &self,
        request: types::BatchExecuteRequest,
    ) -> types::BatchExecuteResponse {
        match self.batch_execute_with_options(request, crate::RequestOptions::default()) {
            Ok(response) => response,
            Err(err) => types::BatchExecuteResponse {
                status: client_error_status(err),
                responses: Vec::new(),
            },
        }
    }

    fn open_table_sdk(&self, request: v1::OpenTableRequest) -> v1::OpenTableResponse {
        let namespace = request.namespace_name;
        let table_name = request.table_name;
        let table = match self.open_table_from_meta(namespace.clone(), table_name.clone()) {
            Ok(table) => table,
            Err(ClientError::Status(message)) if message.contains("meta_addr is required") => self
                .open_table(
                    namespace.clone(),
                    table_name.clone(),
                    TableOptions::default(),
                ),
            Err(err) => {
                return v1::OpenTableResponse {
                    status: Some(client_error_status_to_sdk(err)),
                    topology: None,
                };
            }
        };
        v1::OpenTableResponse {
            status: Some(types_status_to_sdk(types::Status::ok())),
            topology: Some(table_to_sdk_topology(self, &table)),
        }
    }

    fn sync_topology_sdk(&self, request: v1::SyncTopologyRequest) -> v1::SyncTopologyResponse {
        let table_keys = if request.table_keys.is_empty() {
            self.open_table_keys()
        } else {
            request.table_keys
        };
        let mut topologies = Vec::new();
        let mut failures = Vec::new();
        for key in table_keys {
            let Some((namespace, table_name)) = split_table_key(&key) else {
                failures.push(format!("{key}:invalid_table_key"));
                continue;
            };
            match self.sync_table_topology(namespace.clone(), table_name.clone()) {
                Ok(_) => {
                    if let Some(table) = self.cached_table(namespace, table_name) {
                        topologies.push(table_to_sdk_topology(self, &table));
                    }
                }
                Err(err) => {
                    if let Some(table) = self.cached_table(namespace, table_name) {
                        topologies.push(table_to_sdk_topology(self, &table));
                    } else {
                        failures.push(format!("{key}:{}", client_error_code(&err)));
                    }
                }
            }
        }
        let topology_version = self.topology_cache_report().max_topology_version;
        let status = if failures.is_empty() {
            types::Status::ok()
        } else {
            types::Status::error("partial_sync_topology", failures.join(","))
        };
        v1::SyncTopologyResponse {
            status: Some(types_status_to_sdk(status)),
            topologies,
            topology_version,
        }
    }

    fn client_preflight_sdk(
        &self,
        _request: v1::ClientPreflightRequest,
    ) -> v1::ClientPreflightResponse {
        let report = self.preflight_report();
        v1::ClientPreflightResponse {
            status: Some(types_status_to_sdk(report.status)),
            route_cache_entries: report.route_cache_size as u64,
            table_cache_entries: report.table_cache_size as u64,
            backend_failure_entries: report.backend_failure_count as u64,
            topology_version: report.topology_cache.max_topology_version,
            degraded: !report.degraded_reasons.is_empty(),
            warnings: report.degraded_reasons,
        }
    }
}

#[derive(Clone)]
pub struct TemporalStoreTonicAdapter {
    executor: Arc<dyn TemporalStoreSdkExecutor>,
}

impl TemporalStoreTonicAdapter {
    pub fn new(executor: impl TemporalStoreSdkExecutor) -> Self {
        Self {
            executor: Arc::new(executor),
        }
    }

    pub fn from_arc(executor: Arc<dyn TemporalStoreSdkExecutor>) -> Self {
        Self { executor }
    }
}

#[tonic::async_trait]
impl v1::temporal_store_service_server::TemporalStoreService for TemporalStoreTonicAdapter {
    async fn execute(
        &self,
        request: Request<v1::ExecuteRequest>,
    ) -> Result<Response<v1::ExecuteResponse>, TonicStatus> {
        let request = sdk_execute_request_to_types(request.into_inner())?;
        let response = self.executor.execute_sdk(request);
        Ok(Response::new(types_execute_response_to_sdk(response)))
    }

    async fn batch_execute(
        &self,
        request: Request<v1::BatchExecuteRequest>,
    ) -> Result<Response<v1::BatchExecuteResponse>, TonicStatus> {
        let request = sdk_batch_request_to_types(request.into_inner())?;
        let response = self.executor.batch_execute_sdk(request);
        Ok(Response::new(types_batch_response_to_sdk(response)))
    }

    async fn open_table(
        &self,
        request: Request<v1::OpenTableRequest>,
    ) -> Result<Response<v1::OpenTableResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.open_table_sdk(request.into_inner()),
        ))
    }

    async fn sync_topology(
        &self,
        request: Request<v1::SyncTopologyRequest>,
    ) -> Result<Response<v1::SyncTopologyResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.sync_topology_sdk(request.into_inner()),
        ))
    }

    async fn get_client_preflight(
        &self,
        request: Request<v1::ClientPreflightRequest>,
    ) -> Result<Response<v1::ClientPreflightResponse>, TonicStatus> {
        Ok(Response::new(
            self.executor.client_preflight_sdk(request.into_inner()),
        ))
    }
}

pub fn sdk_execute_request_to_types(
    request: v1::ExecuteRequest,
) -> Result<types::ExecuteRequest, TonicStatus> {
    Ok(types::ExecuteRequest {
        shard_id: request.shard_id,
        command: sdk_command_to_types(required_command(request.command)?)?,
    })
}

pub fn sdk_batch_request_to_types(
    request: v1::BatchExecuteRequest,
) -> Result<types::BatchExecuteRequest, TonicStatus> {
    let commands = request
        .commands
        .into_iter()
