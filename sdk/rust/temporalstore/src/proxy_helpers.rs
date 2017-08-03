// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::{
    ControlStatePrecision, ControlStateWindow, ControlStateWindowUnit, Error, FeaturePoint, Result,
};

pub(crate) fn json_error(err: serde_json::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

pub(crate) fn json_byte_array_to_string(value: serde_json::Value) -> String {
    let bytes = value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.as_u64().unwrap_or_default() as u8)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub(crate) fn response_hash_entries_to_strings(
    response: serde_json::Value,
) -> Vec<(String, String)> {
    response
        .get("entries")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.as_array()?;
            let field = entry.first()?.as_str()?.to_string();
            let value = json_byte_array_to_string(entry.get(1)?.clone());
            Some((field, value))
        })
        .collect()
}

pub(crate) fn response_feature_points(response: serde_json::Value) -> Result<Vec<FeaturePoint>> {
    serde_json::from_value(
        response
            .get("points")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(json_error)
}

pub(crate) fn proxy_timestamp_ms(occur_time_seconds: u64) -> u64 {
    if occur_time_seconds > 0 {
        return occur_time_seconds.saturating_mul(1000);
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(crate) fn control_state_precision_ms(precision: ControlStatePrecision) -> u64 {
    match precision {
