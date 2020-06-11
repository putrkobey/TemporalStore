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
