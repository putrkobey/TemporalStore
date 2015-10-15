// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable control_state methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn control_state_increment(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateIncrement {
            key: key.into(),
            timestamp_ms,
            amount,
        })
    }

    pub fn control_state_increment_with_options(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        amount: i64,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateIncrementWithOptions {
            key: key.into(),
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        })
    }

    pub fn control_state_change_add(
        &self,
        key: impl Into<String>,
        timestamp_ms: u64,
        value: impl Into<Vec<u8>>,
        precision_ms: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::ControlStateChangeAdd {
            key: key.into(),
            timestamp_ms,
            value: value.into(),
            precision_ms,
            ttl_ms,
        })
    }

    pub fn control_state_count(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateCount {
                key: key.into(),
                start_ms,
                end_ms,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_count",
                response,
            }),
        }
    }

    pub fn control_state_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        aggregator: impl Into<String>,
    ) -> Result<i64, ClientError> {
        match self
            .execute(Command::ControlStateQuery {
                key: key.into(),
                start_ms,
                end_ms,
                aggregator: aggregator.into(),
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value),
            response => Err(ClientError::UnexpectedResponse {
                operation: "control_state_query",
                response,
            }),
        }
    }

    pub fn control_state_detail(
        &self,
        key: impl Into<String>,
        start_ms: u64,
