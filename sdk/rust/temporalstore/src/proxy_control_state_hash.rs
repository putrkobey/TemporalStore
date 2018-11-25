// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{control_state_precision_ms, json_error, proxy_timestamp_ms};
use crate::{ControlStateHType, ControlStatePrecision, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_hset(&self, key: &str, timestamp_ms: u64, amount: i64) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("amount", serde_json::json!(amount)),
