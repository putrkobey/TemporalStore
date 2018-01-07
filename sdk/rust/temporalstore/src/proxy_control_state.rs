// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{
    control_state_precision_ms, control_state_window_ms, proxy_timestamp_ms,
};
use crate::{ControlStatePrecision, ControlStateWindow, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_increment(
        &self,
        key: &str,
        amount: i64,
        ttl_seconds: u64,
        precision: ControlStatePrecision,
        _uuid: &str,
        occur_time_seconds: u64,
    ) -> Result<()> {
        let timestamp_ms = proxy_timestamp_ms(occur_time_seconds);
        let body = self.proxy_service_body(
            key,
            &[
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("amount", serde_json::json!(amount)),
                (
                    "precision_ms",
                    serde_json::json!(control_state_precision_ms(precision)),
                ),
                (
