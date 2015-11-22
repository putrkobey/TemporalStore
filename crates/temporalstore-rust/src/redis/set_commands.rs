// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis set-algebra commands (SDIFF / SINTER / SUNION), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetAlgebraOp {
    Diff,
    Inter,
    Union,
}

pub(crate) fn sorted_set_members(
    key: &str,
