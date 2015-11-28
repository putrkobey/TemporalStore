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
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Vec<Vec<u8>>, String> {
    match execute(Command::SetMembers {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Members { mut members }) => {
            members.sort();
            Ok(members)
        }
        Ok(_) => Err("ERR invalid smembers response".to_string()),
        Err(err) => Err(format!("ERR {err}")),
    }
}

pub(crate) fn set_algebra_response(
    keys: &[Vec<u8>],
    op: SetAlgebraOp,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
