// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::Result;

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
