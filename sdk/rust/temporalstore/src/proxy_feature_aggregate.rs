// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn feature_aggregate(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        aggregator: &str,
        count: Option<usize>,
    ) -> Result<i64> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
