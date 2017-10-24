// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// budget-token / source-role / layer-count parsing helpers, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).

fn parse_budget_tokens(request: &Value, field: &str, lowercase_keys: bool) -> BTreeMap<String, u64> {
    let config = request
        .get(field)
        .or_else(|| json_field(request, &["ranking", field]));
    let mut budgets = BTreeMap::new();
    let Some(object) = config.and_then(Value::as_object) else {
        return budgets;
    };
    for (key, value) in object {
        let key_name = if lowercase_keys {
            key.trim().to_ascii_lowercase()
        } else {
            key.trim().to_string()
        };
        if key_name.is_empty() {
            continue;
        }
        let Some(tokens) = value.as_u64() else {
            continue;
        };
        if tokens > 0 {
            budgets.insert(key_name, tokens);
        }
    }
    budgets
}

fn parse_source_role_budget_tokens(request: &Value) -> BTreeMap<String, u64> {
    parse_budget_tokens(request, "source_role_budget_tokens", true)
}

fn parse_memory_selection_policy_budget_tokens(request: &Value) -> BTreeMap<String, u64> {
    parse_budget_tokens(request, "memory_selection_policy_budget_tokens", false)
}

fn parse_extraction_phase_budget_tokens(request: &Value) -> BTreeMap<String, u64> {
    parse_budget_tokens(request, "extraction_phase_budget_tokens", true)
}

fn source_role_names(value: &Value) -> BTreeSet<String> {
    let mut roles = BTreeSet::new();
    if let Some(source_roles) = value.get("source_roles").and_then(Value::as_array) {
        for role in source_roles.iter().filter_map(Value::as_str) {
            let role_name = role.trim().to_ascii_lowercase();
            if !role_name.is_empty() {
                roles.insert(role_name);
            }
        }
    }
    if let Some(counts) = value.get("source_role_counts").and_then(Value::as_object) {
        for (role, count) in counts {
            if count.as_u64().unwrap_or(0) == 0 {
                continue;
            }
            let role_name = role.trim().to_ascii_lowercase();
            if !role_name.is_empty() {
                roles.insert(role_name);
            }
        }
    }
    roles
}

fn memory_selection_policy_names(value: &Value) -> BTreeSet<String> {
    let mut policies = BTreeSet::new();
    for source in [Some(value), value.get("metadata")] {
        let Some(source) = source else {
            continue;
        };
        if let Some(source_policies) = source
            .get("source_memory_selection_policies")
            .and_then(Value::as_array)
        {
            for policy in source_policies.iter().filter_map(Value::as_str) {
                let policy_name = policy.trim();
                if !policy_name.is_empty() {
                    policies.insert(policy_name.to_string());
                }
            }
        }
        if let Some(policy_name) = source
            .get("memory_selection_policy")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            policies.insert(policy_name.to_string());
        }
        if let Some(counts) = source
            .get("source_memory_selection_policy_counts")
            .and_then(Value::as_object)
        {
            for (policy, count) in counts {
                if count.as_u64().unwrap_or(0) == 0 {
                    continue;
                }
                let policy_name = policy.trim();
                if !policy_name.is_empty() {
                    policies.insert(policy_name.to_string());
                }
            }
        }
    }
    policies
}

