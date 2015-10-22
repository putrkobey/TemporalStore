// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::response_hash_entries_to_strings;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn control_state_manager(&self, key: &str) -> Result<Vec<(String, String)>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/ControlStateManager", body)?;
        Ok(response_hash_entries_to_strings(response))
    }

    pub fn control_state_manager_with_options(
        &self,
        key: &str,
        op_type: &str,
        field_list: &[(&str, &str)],
        start_offset: &str,
