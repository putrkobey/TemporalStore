// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis RESP response encoders and argument parsers, extracted from redis.rs.

use super::*;
use crate::types::{CommandResponse, FeatureFilterOp, FeaturePoint, FeatureWritePolicy};

pub(crate) fn bytes_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Bytes { value }) => RespValue::Bulk(value),
        Ok(_) => RespValue::Error("ERR invalid bulk response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn integer_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::Integer { value }) => RespValue::Integer(value),
        Ok(_) => RespValue::Error("ERR invalid integer response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn status_ok(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(_) => RespValue::SimpleString("OK".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn feature_points_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
        Ok(CommandResponse::FeaturePoints { points }) => feature_points_value(points),
        Ok(_) => RespValue::Error("ERR invalid feature points response".to_string()),
        Err(err) => RespValue::Error(format!("ERR {err}")),
    }
}

pub(crate) fn hash_entries_response(result: Result<CommandResponse, String>) -> RespValue {
    match result {
