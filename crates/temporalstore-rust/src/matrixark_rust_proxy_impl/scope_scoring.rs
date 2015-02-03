// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Scope / session-continuity / cross-session scoring helpers, split from
// matrixark_rust_proxy_impl.rs. Textually include!d, so it shares the parent
// file's use-imports and flat scope; do not add use-statements or mod wrappers.

fn record_scope_value<'a>(record: &'a Value) -> Option<&'a Value> {
    if let Some(scope) = record.get("access_scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    if let Some(scope) =
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object())
    {
        return Some(scope);
    }
    if let Some(scope) = record.get("scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    json_field(record, &["envelope", "scope"]).filter(|value| value.is_object())
}

fn session_scope_mode(query: &Value) -> &str {
    match query
        .get("_session_scope")
        .or_else(|| query.get("session_scope"))
        .and_then(Value::as_str)
        .unwrap_or("prefer")
    {
        "only" | "strict" => "only",
        _ => "prefer",
    }
}

fn scope_key_explicit(scope: &Value, field: &str) -> bool {
    scope
        .get("_explicit_scope_keys")
        .and_then(Value::as_array)
        .map(|items| items.iter().any(|item| item.as_str() == Some(field)))
        .unwrap_or(false)
}

fn parse_scope_key(scope_key: &str) -> HashMap<String, u64> {
    scope_key
        .split('|')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            if key.is_empty() || value.is_empty() {
                return None;
            }
            value
                .parse::<u64>()
                .ok()
                .map(|parsed| (key.to_string(), parsed))
        })
        .collect()
}

fn scoped_string_value(scope: Option<&Value>, field: &str) -> Option<String> {
    scope
        .filter(|value| value.is_object())
        .and_then(|value| value.get(field))
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn record_scope_sources(record: &Value) -> [Option<&Value>; 5] {
    [
        Some(record),
        record.get("access_scope").filter(|value| value.is_object()),
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object()),
        record.get("scope").filter(|value| value.is_object()),
        json_field(record, &["envelope", "scope"]).filter(|value| value.is_object()),
    ]
}

fn candidate_scope_key(record: &Value) -> String {
    for source in record_scope_sources(record) {
        if let Some(value) = scoped_string_value(source, "scope_key") {
            return value;
        }
    }
    String::new()
}

fn scope_key_matches_query(record_scope_key: &str, query_scope: &Value) -> bool {
    if record_scope_key.is_empty() {
        return true;
    }
    let parts = parse_scope_key(record_scope_key);
    if let Some(tenant_hash) = query_scope.get("tenant_hash").and_then(Value::as_u64) {
        if tenant_hash != 0 && parts.get("t").copied() != Some(tenant_hash) {
            return false;
        }
    }
    if scope_key_explicit(query_scope, "user_id") {
        if let Some(user_hash) = query_scope.get("user_hash").and_then(Value::as_u64) {
            if user_hash != 0 && parts.get("u").copied() != Some(user_hash) {
                return false;
            }
        }
    }
    if scope_key_explicit(query_scope, "session_id") && session_scope_mode(query_scope) == "only" {
        if let Some(session_hash) = query_scope.get("session_hash").and_then(Value::as_u64) {
            if session_hash != 0 && parts.get("s").copied() != Some(session_hash) {
                return false;
            }
        }
    }
    true
}

fn scope_matches_record(record: &Value, query_scope: Option<&Value>) -> bool {
    let Some(query) = query_scope.filter(|value| value.is_object()) else {
        return true;
    };
    if !scope_key_matches_query(&candidate_scope_key(record), query) {
        return false;
    }
    for key in [
        "scope_key",
        "account_id",
        "tenant_id",
        "user_id",
        "session_id",
        "team",
        "project",
        "agent_name",
    ] {
        if key == "scope_key" {
            continue;
        }
        if matches!(key, "account_id" | "tenant_id" | "user_id" | "session_id")
            && !scope_key_explicit(query, key)
        {
            continue;
        }
        if key == "session_id" && session_scope_mode(query) == "prefer" {
            continue;
        }
        if matches!(key, "team" | "project" | "agent_name") && !scope_key_explicit(query, key) {
            continue;
        }
        let Some(query_value) = query.get(key) else {
            continue;
        };
        if query_value.is_null() || query_value.as_str() == Some("") {
            continue;
        }
        let actual = record_scope_sources(record)
            .into_iter()
            .find_map(|source| scoped_string_value(source, key));
        if actual
            .as_deref()
            .is_some_and(|value| Some(value) != query_value.as_str())
        {
            return false;
        }
    }
