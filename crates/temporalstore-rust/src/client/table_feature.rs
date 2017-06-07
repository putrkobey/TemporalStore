// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreTable feature-point + sequence methods, split from client.rs.
use super::*;

impl TemporalStoreTable {
    pub fn feature_append(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
    ) -> Result<(), ClientError> {
        self.expect_empty(Command::FeatureAppend {
            key: key.into(),
            points,
        })
    }

    pub fn feature_append_with_policy(
        &self,
        key: impl Into<String>,
        points: Vec<FeaturePoint>,
        policy: FeatureWritePolicy,
    ) -> Result<bool, ClientError> {
        match self
            .execute(Command::FeatureAppendWithPolicy {
                key: key.into(),
                points,
                policy,
            })?
            .response
        {
            CommandResponse::Integer { value } => Ok(value != 0),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_append_with_policy",
                response,
            }),
        }
    }

    pub fn feature_query(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQuery {
                key: key.into(),
                start_ms,
                end_ms,
                count,
            })?
            .response
        {
            CommandResponse::FeaturePoints { points } => Ok(points),
            response => Err(ClientError::UnexpectedResponse {
                operation: "feature_query",
                response,
            }),
        }
    }

    pub fn feature_query_filtered(
        &self,
        key: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: Vec<FeatureFilter>,
    ) -> Result<Vec<FeaturePoint>, ClientError> {
        match self
            .execute(Command::FeatureQueryFiltered {
                key: key.into(),
                start_ms,
                end_ms,
                count,
                filters,
            })?
