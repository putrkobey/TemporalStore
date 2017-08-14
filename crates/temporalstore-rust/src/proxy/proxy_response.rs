// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::{CommandResponse, ExecuteResponse, Status};

pub(super) fn execute_error(
    code: impl Into<String>,
