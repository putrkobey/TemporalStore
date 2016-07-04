// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::json_error;
use crate::{FeatureFilter, Result, SequenceFeatureRow};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
