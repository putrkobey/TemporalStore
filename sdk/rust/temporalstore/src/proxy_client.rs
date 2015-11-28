// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::Result;

#[cfg(feature = "proxy")]
#[derive(Clone, Debug)]
pub struct ProxyOptions {
    pub endpoint: String,
    pub namespace_name: String,
    pub table_name: String,
    pub api_key: String,
}

#[cfg(feature = "proxy")]
impl ProxyOptions {
    pub fn new(
        endpoint: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            api_key: String::new(),
        }
    }
}

#[cfg(feature = "proxy")]
pub struct ProxyClient {
    pub(crate) endpoint: String,
    pub(crate) options: ProxyOptions,
}

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn connect(options: ProxyOptions) -> Self {
        let endpoint = options.endpoint.trim_end_matches('/').to_string();
        Self { endpoint, options }
    }

    pub fn open_table(&self) -> Result<serde_json::Value> {
        self.open_table_with_options(None, None)
