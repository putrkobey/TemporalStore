// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_error;
use crate::{FeatureFilter, Result, SequenceFeatureRow};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[("rows", serde_json::to_value(rows).map_err(json_error)?)],
        );
        self.proxy_service_execute("/ProxyService/SequenceAdd", body)
            .map(|_| ())
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let encoded_filters: Vec<serde_json::Value> = filters
            .iter()
            .map(|filter| {
                serde_json::json!({
                    "field": filter.field,
                    "op": filter.op as i32,
