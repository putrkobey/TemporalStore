// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis keyspace commands (KEYS/SCAN/TYPE + pattern matching), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};

pub(super) fn redis_keys_response(
    pattern: &[u8],
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let pattern = string_arg(pattern);
    RespValue::Array(
        live_matching_keys(&pattern, state, execute)
            .into_iter()
            .map(|key| RespValue::Bulk(Some(key.into_bytes())))
            .collect(),
    )
}

pub(super) fn redis_scan_response(
    args: &[Vec<u8>],
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let cursor = match parse_usize(&args[1], "cursor") {
        Ok(value) => value,
        Err(err) => return RespValue::Error(err),
    };
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut index = 2;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "MATCH" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                pattern = string_arg(value);
                index += 2;
            }
            "COUNT" => {
                let Some(value) = args.get(index + 1) else {
                    return RespValue::Error("ERR syntax error".to_string());
                };
                count = match parse_usize(value, "count") {
                    Ok(value) => value.max(1),
                    Err(err) => return RespValue::Error(err),
                };
                index += 2;
            }
            _ => return RespValue::Error("ERR syntax error".to_string()),
        }
    }
    let keys = live_matching_keys(&pattern, state, execute);
    let selected = keys
        .iter()
        .skip(cursor)
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor = cursor.saturating_add(selected.len());
    let next_cursor = if next_cursor >= keys.len() {
        0
    } else {
        next_cursor
    };
    RespValue::Array(vec![
        RespValue::Bulk(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(
            selected
                .into_iter()
                .map(|key| RespValue::Bulk(Some(key.into_bytes())))
                .collect(),
        ),
    ])
}

pub(super) fn parse_scan_tail_options(args: &[Vec<u8>]) -> Result<(String, usize), String> {
