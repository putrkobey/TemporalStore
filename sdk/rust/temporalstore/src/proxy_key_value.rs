// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_byte_array_to_string;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("value", serde_json::json!(value.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/Set", body)
