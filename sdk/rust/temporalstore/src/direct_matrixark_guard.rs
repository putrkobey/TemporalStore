// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::{Error, Result};

pub(crate) fn native_matrixark_c_api_bridge_allowed(op: &str) -> Result<()> {
    let allowed = std::env::var("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API")
        .map(|value| {
