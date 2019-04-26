// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_byte_array_to_string;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("field", serde_json::json!(field)),
                ("value", serde_json::json!(value.as_bytes())),
            ],
        );
        self.proxy_service_execute("/ProxyService/HSet", body)
            .map(|_| ())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        let response = self.proxy_service_execute("/ProxyService/HGet", body)?;
        Ok(json_byte_array_to_string(
            response
                .get("value")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        ))
    }

    pub fn hmset(&self, key: &str, entries: &[(&str, &str)]) -> Result<()> {
        let entries = entries
            .iter()
            .map(|(field, value)| serde_json::json!([field, value.as_bytes()]))
            .collect::<Vec<_>>();
