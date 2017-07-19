// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable context methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn context_upsert_node(
        &self,
        tenant_hash: u64,
        node: ContextNode,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextUpsertNode { tenant_hash, node: Box::new(node) })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_upsert_node",
                response,
            }),
        }
    }

    pub fn context_get_node(
        &self,
        tenant_hash: u64,
        node_hash: u64,
    ) -> Result<Option<ContextNode>, ClientError> {
        match self
            .execute(Command::ContextGetNode {
                tenant_hash,
                node_hash,
            })?
            .response
        {
            CommandResponse::ContextNode { node, .. } => Ok(node),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_get_node",
                response,
            }),
        }
    }

    pub fn context_write_event(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        event: ContextEvent,
        first_write_only: bool,
    ) -> Result<String, ClientError> {
        match self
            .execute(Command::ContextWriteEvent {
                tenant_hash,
                node_hash,
                event: Box::new(event),
                first_write_only,
                cold_storage: false,
            })?
            .response
        {
            CommandResponse::ContextObjectKey { object_key } => Ok(object_key),
            response => Err(ClientError::UnexpectedResponse {
                operation: "context_write_event",
                response,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn context_query_events(
        &self,
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: Option<usize>,
        current_valid_only: bool,
        as_of_ms: u64,
        kinds: Vec<u32>,
        statuses: Vec<u32>,
        min_confidence: f32,
        min_importance: f32,
    ) -> Result<Vec<ContextEvent>, ClientError> {
        match self
            .execute(Command::ContextQueryEvents {
                tenant_hash,
                node_hash,
