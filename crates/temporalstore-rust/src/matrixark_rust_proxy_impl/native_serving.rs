// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// native serving / memory-inventory / layer-budget helpers, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).

/// What a served ref carries only when lineage is asked for.
///
/// A constant rather than an inline list because the test that proves a default ref does
/// not leak these had its own hand-written copy, and the copies had already drifted: it
/// checked 25 of the 26 fields, missing `source_ref`. A field added to production and not
/// to that list would have leaked with the test still green, because the assertion only
/// ever looked for the names written into it.
pub(crate) const LINEAGE_ONLY_FIELDS: [&str; 22] = [
    "ref_hash",
    "node_hash",
    "node_path",
    "continuity_reason",
    "selection_reason",
    "source_session_ids",
    "source_entity_hashes",
    "source_entity_types",
    "source_roles",
    "source_role_counts",
    "budget_source_roles",
    "budget_source_role_counts",
    "source_hook_types",
    "source_hook_type_counts",
    "source_codex_events",
    "source_codex_event_counts",
    "source_memory_scopes",
    "source_session_continuities",
    "source_extraction_phases",
    "source_profile_promotion_policies",
    "source_profile_promotion_blockers",
    "source_ref",
];

/// What a served ref carries only when scores are asked for.
///
/// Separate from the lineage list, and it has to stay separate: the two answer different
/// questions, and folding them together would make someone debugging a score take 22
/// provenance fields they did not ask for.
pub(crate) const SCORE_ONLY_FIELDS: [&str; 4] = [
    "token_estimate",
    "score",
    "continuity_boost",
    "cross_session_rerank_boost",
];

/// Shape a ref for serving: lineage and scores never leave the process.
///
/// This used to be selectable, by `MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE` and
/// `MATRIXARK_CONTEXT_PACK_INCLUDE_SCORES`. Nothing shipped selected either, so the deployed
/// behaviour was always this one, and the second arm was a path the serving code carried without
/// taking. It is gone: one shape, and the lists below say what a served ref does not carry.
fn native_serving_ref(mut item: Value) -> Value {
    if let Some(object) = item.as_object_mut() {
        for field in LINEAGE_ONLY_FIELDS {
            object.remove(field);
        }
        for field in SCORE_ONLY_FIELDS {
            object.remove(field);
        }
    }
    item
}

/// What a served ref carries when the engine shapes it, rather than the caller.
///
/// The caller's allow-list, intersected with what a selected ref actually holds. `citation`,
/// `source_ref`, `resource_type` and their kin are in that allow-list and are never set on an
/// engine-built ref, so they cannot be lost by keeping this list -- and `memory_layer` is added
/// alongside, which the caller also does.
const COMPACT_SERVING_REF_FIELDS: &[&str] = &[
    "ref_type",
    "text",
    "text_preview",
    "citation",
    "resource_type",
    "sharing_scope",
    "event_type",
    "source_role",
    "entity_type",
    "entity_name",
    "summary_type",
    "operator",
    "memory_scope",
    "session_continuity",
    "profile_memory_kind",
    "profile_memory_class",
    "profile_entity_current",
    "profile_summary_current",
    "profile_current_state_representative",
    "memory_layer",
];

/// Whether a deployment permits the engine to emit the serving shape.
///
/// A kill switch, not the decision: the decision is the caller's, because only the caller knows
/// whether this retrieve wants debug refs, which carry lineage the serving shape drops.
/// `MATRIXARK_ENGINE_COMPACT_SERVING_REFS=0` returns every deployment to the old behaviour without
/// touching its callers.
fn engine_compact_serving_refs_allowed() -> bool {
    !matches!(
        std::env::var("MATRIXARK_ENGINE_COMPACT_SERVING_REFS")
            .unwrap_or_default()
            .trim(),
        "0" | "false" | "off" | "no"
    )
}

/// Keep only the fields a served ref is allowed to carry.
///
/// An empty value is dropped rather than sent, matching the caller: it tests
/// `value not in (None, "", [], {})`, so a zero or a `false` is KEPT and an empty string, list or
/// map is not.
fn compact_serving_ref(item: &Value) -> Value {
    let mut compact = serde_json::Map::new();
    let Some(object) = item.as_object() else {
        return item.clone();
    };
    for field in COMPACT_SERVING_REF_FIELDS {
        let Some(value) = object.get(*field) else {
            continue;
        };
        let empty = match value {
            Value::Null => true,
            Value::String(text) => text.is_empty(),
            Value::Array(items) => items.is_empty(),
            Value::Object(map) => map.is_empty(),
            _ => false,
        };
        if empty {
            continue;
        }
        // The caller lowercases and trims this one on its way out; matching it here keeps the two
        // shapes byte-identical.
        if *field == "memory_layer" {
            if let Some(text) = value.as_str() {
                compact.insert(
                    (*field).to_string(),
                    Value::String(text.trim().to_lowercase()),
                );
                continue;
            }
        }
        compact.insert((*field).to_string(), value.clone());
    }
    Value::Object(compact)
}

fn native_serving_refs(refs: &[Value], compact: bool) -> Vec<Value> {
    if compact && engine_compact_serving_refs_allowed() {
        return refs.iter().map(compact_serving_ref).collect();
    }
    refs.iter().cloned().map(native_serving_ref).collect()
}

fn increment_inventory_count(inventory: &mut Value, layer: &str, field: &str) {
    let Some(bucket) = inventory.get_mut(layer).and_then(Value::as_object_mut) else {
        return;
    };
    let current = bucket.get(field).and_then(Value::as_u64).unwrap_or(0);
    bucket.insert(field.to_string(), json!(current + 1));
}

fn inventory_layer_available(inventory: &Value, layer: &str) -> bool {
    inventory
        .get(layer)
        .and_then(Value::as_object)
        .map(|bucket| bucket.values().any(|value| value.as_u64().unwrap_or(0) > 0))
        .unwrap_or(false)
}

/// Count what each layer holds.
///
/// Takes BORROWED records: the caller reads them out of the shard cache, and counting has never
/// needed to own them.
fn native_retrieval_memory_inventory(
    records: &[&Value],
    query_scope: Option<&Value>,
) -> Value {
    let mut counts = empty_inventory_counts();
    count_records_into(&mut counts, records);
    finish_inventory(counts, query_scope)
}

/// The zeroed buckets a count starts from.
///
/// Separate from the finishing step because the counters do not depend on the query scope -- only
/// the `query_scope` block and the derived flags do -- so counts for one shard can be built once,
/// kept, and summed with another shard's.
fn empty_inventory_counts() -> Value {
    json!({
        "session": {
            "context_events": 0,
            "context_segments": 0,
            "context_entities": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0
        },
        "profile": {
            "context_entities": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0
        },
        "shared": {
            "resource_chunks": 0,
            "resource_manifests": 0,
            "skill_sections": 0,
            "skill_manifests": 0,
            "context_entities": 0,
            "context_indexes": 0
        },
        "profile_records_available_but_not_selected": false
    })
}

/// Add a set of records to a counts object.
fn count_records_into(inventory: &mut Value, records: &[&Value]) {
    for record in records.iter().copied() {
        let record_type = string_field(record, "record_type");
        let memory_scope = string_field(record, "memory_scope")
            .trim()
            .to_ascii_lowercase();
        let session_continuity = string_field(record, "session_continuity")
            .trim()
            .to_ascii_lowercase();
        let data_model = string_field(record, "data_model")
            .trim()
            .to_ascii_lowercase();
        let sharing_scope = record_scope_value(record)
            .and_then(|scope| scope.get("sharing_scope"))
            .and_then(Value::as_str)
            .or_else(|| record.get("sharing_scope").and_then(Value::as_str))
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let is_shared = matches!(sharing_scope.as_str(), "tenant_shared" | "global_shared")
            || matches!(
                record_type,
                "resource_chunk" | "resource_manifest" | "skill_section" | "skill_manifest" | "skill_registry_update"
            )
            || matches!(data_model.as_str(), "resource_chunk" | "skill_section");
        let is_profile = matches!(
            memory_scope.as_str(),
            "user_profile" | "profile" | "cross_session_profile"
        ) || data_model == "context_profile_entity"
            || (matches!(
                record_type,
                "context_entity" | "context_embedding" | "context_summary" | "context_summary_dirty"
            ) && session_continuity == "cross_session");
        let is_session = matches!(memory_scope.as_str(), "session" | "session_memory")
            || session_continuity == "same_session";

        if is_shared {
            match record_type {
                "resource_chunk" => increment_inventory_count(inventory, "shared", "resource_chunks"),
                "resource_manifest" => increment_inventory_count(inventory, "shared", "resource_manifests"),
                "skill_section" => increment_inventory_count(inventory, "shared", "skill_sections"),
                "skill_manifest" | "skill_registry_update" => {
                    increment_inventory_count(inventory, "shared", "skill_manifests")
                }
                "context_entity" => increment_inventory_count(inventory, "shared", "context_entities"),
                "context_index" => increment_inventory_count(inventory, "shared", "context_indexes"),
                _ => {}
            }
            continue;
        }

        if is_profile {
            match record_type {
                "context_entity" => increment_inventory_count(inventory, "profile", "context_entities"),
                "context_index" => increment_inventory_count(inventory, "profile", "context_indexes"),
                "context_summary" => increment_inventory_count(inventory, "profile", "context_summaries"),
                "context_summary_dirty" => {
                    increment_inventory_count(inventory, "profile", "summary_dirty_markers")
                }
                _ => {}
            }
            continue;
        }

        if is_session || matches!(record_type, "context_event" | "context_segment") {
            match record_type {
                "context_event" => increment_inventory_count(inventory, "session", "context_events"),
                "context_segment" => increment_inventory_count(inventory, "session", "context_segments"),
                "context_entity" => increment_inventory_count(inventory, "session", "context_entities"),
                "context_index" => increment_inventory_count(inventory, "session", "context_indexes"),
                "context_summary" => increment_inventory_count(inventory, "session", "context_summaries"),
                "context_summary_dirty" => {
                    increment_inventory_count(inventory, "session", "summary_dirty_markers")
                }
                _ => {}
            }
        }
    }

}

/// Turn counts into the inventory a request is served, which is where the scope comes in.
fn finish_inventory(mut inventory: Value, query_scope: Option<&Value>) -> Value {
    if let Some(object) = inventory.as_object_mut() {
        object.insert(
            "query_scope".to_string(),
            json!({
                "session_scope": query_scope.map(session_scope_mode).unwrap_or("prefer"),
                "has_session_id": query_scope
                    .and_then(|scope| scope.get("session_id"))
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false),
                "has_user_id": query_scope
                    .and_then(|scope| scope.get("user_id"))
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false),
                "has_tenant_id": query_scope
                    .and_then(|scope| scope.get("tenant_id"))
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false)
            }),
        );
    }
    let mut available_layers = Vec::new();
    for layer in ["session", "profile", "shared"] {
        if inventory_layer_available(&inventory, layer) {
            available_layers.push(json!(layer));
        }
    }
    let has_session_memory = inventory_layer_available(&inventory, "session");
    let has_profile_memory = inventory_layer_available(&inventory, "profile");
    let has_shared_memory = inventory_layer_available(&inventory, "shared");
    if let Some(object) = inventory.as_object_mut() {
        object.insert("available_layers".to_string(), Value::Array(available_layers));
        object.insert("has_session_memory".to_string(), json!(has_session_memory));
        object.insert("has_profile_memory".to_string(), json!(has_profile_memory));
        object.insert("has_shared_memory".to_string(), json!(has_shared_memory));
    }
    inventory
}

fn native_serving_memory_layer_budget(value: &Value) -> Value {
    let mut compact = value.clone();
    if let Some(object) = compact.as_object_mut() {
        for field in [
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
            "by_profile_promotion_policy",
            "by_profile_promotion_blocker",
        ] {
            object.remove(field);
        }
    }
    compact
}

fn native_serving_memory_layer_pressure(value: &Value) -> Value {
    let mut compact = value.clone();
    let lineage_dimensions: BTreeSet<&str> = [
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
        "by_profile_promotion_policy",
        "by_profile_promotion_blocker",
    ]
    .into_iter()
    .collect();
    if let Some(object) = compact.as_object_mut() {
        for list_field in ["pressure_dimensions", "dropped_dimensions"] {
            if let Some(values) = object.get_mut(list_field).and_then(Value::as_array_mut) {
                values.retain(|value| {
                    value
                        .as_str()
                        .map(|dimension| !lineage_dimensions.contains(dimension))
                        .unwrap_or(true)
                });
            }
        }
        if let Some(dimensions) = object.get_mut("by_dimension").and_then(Value::as_object_mut) {
            dimensions.retain(|dimension, _| !lineage_dimensions.contains(dimension.as_str()));
        }
        for field in [
            "assistant_memory_pressure",
            "user_memory_pressure",
            "tool_memory_pressure",
            "assistant_source_message_pressure",
            "user_source_message_pressure",
            "tool_source_message_pressure",
            "hook_boundary_source_pressure",
            "after_llm_source_pressure",
            "tool_result_source_pressure",
            "stop_event_source_pressure",
            "post_tool_use_source_pressure",
        ] {
            object.remove(field);
        }
    }
    compact
}

fn native_serving_dropped_refs(mut dropped_refs: Value) -> Value {
    if let Some(object) = dropped_refs.as_object_mut() {
        let dropped_ref_count = object
            .get("refs")
            .and_then(Value::as_array)
            .map(|refs| refs.len())
            .unwrap_or(0);
        object.remove("refs");
        if dropped_ref_count > 0 {
            object.insert(
                "dropped_ref_detail_available_in_audit".to_string(),
                json!(true),
            );
            object.insert("dropped_ref_count".to_string(), json!(dropped_ref_count));
        }
    }
    dropped_refs
}

fn increment_layer_bucket(breakdown: &mut Value, bucket_name: &str, key: &str, tokens: u64) {
    let Some(bucket_map) = breakdown
        .get_mut(bucket_name)
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let bucket = bucket_map
        .entry(key.to_string())
        .or_insert_with(|| json!({"refs": 0, "tokens": 0}));
    if let Some(bucket_object) = bucket.as_object_mut() {
        let refs = bucket_object
            .get("refs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        let total_tokens = bucket_object
            .get("tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + tokens;
        bucket_object.insert("refs".to_string(), json!(refs));
        bucket_object.insert("tokens".to_string(), json!(total_tokens));
    }
}

fn source_layer_values(
    record: &Value,
    source_field: &str,
    fallback_field: &str,
    default_value: &str,
) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(source_values) = record.get(source_field).and_then(Value::as_array) {
        for value in source_values
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !values.iter().any(|existing| existing == value) {
                values.push(value.to_string());
            }
        }
    }
    if values.is_empty() {
        if let Some(value) = record
            .get(fallback_field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            values.push(value.to_string());
        }
    }
    if values.is_empty() {
        values.push(default_value.to_string());
    }
    values
}

fn selected_ref_layer_budget(refs: &[Value]) -> Value {
    let mut breakdown = json!({
        "by_memory_layer": {},
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
        "by_profile_promotion_policy": {},
        "by_profile_promotion_blocker": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_selected_refs": refs.len(),
        "total_selected_tokens": 0
    });
    for item in refs {
        let tokens = item
            .get("token_estimate")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let total_tokens = breakdown
            .get("total_selected_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + tokens;
        if let Some(object) = breakdown.as_object_mut() {
            object.insert("total_selected_tokens".to_string(), json!(total_tokens));
        }
        let memory_layer = item
            .get("memory_layer")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                let ref_type = item
                    .get("ref_type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                broad_memory_layer(item, ref_type)
            });
        if !memory_layer.is_empty() {
            increment_layer_bucket(&mut breakdown, "by_memory_layer", &memory_layer, tokens);
        }
        for memory_scope in source_layer_values(item, "source_memory_scopes", "memory_scope", "unscoped") {
            increment_layer_bucket(&mut breakdown, "by_memory_scope", &memory_scope, tokens);
        }
        for session_continuity in source_layer_values(
            item,
            "source_session_continuities",
            "session_continuity",
            "neutral",
        ) {
            increment_layer_bucket(
                &mut breakdown,
                "by_session_continuity",
                &session_continuity,
                tokens,
            );
        }
        let extraction_phase = item
            .get("extraction_phase")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        for source_extraction_phase in source_layer_values(
            item,
            "source_extraction_phases",
            "extraction_phase",
            "unknown",
        ) {
            increment_layer_bucket(
                &mut breakdown,
                "by_extraction_phase",
                &source_extraction_phase,
                tokens,
            );
        }
        let ref_type = item
            .get("ref_type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        increment_layer_bucket(&mut breakdown, "by_ref_type", ref_type, tokens);
        for entity_type in source_layer_values(item, "source_entity_types", "entity_type", "") {
            if !entity_type.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_entity_type", &entity_type, tokens);
            }
        }
        for policy in source_layer_values(item, "source_profile_promotion_policies", "profile_promotion_policy", "") {
            if !policy.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_profile_promotion_policy", &policy, tokens);
            }
        }
        for blocker in source_layer_values(item, "source_profile_promotion_blockers", "profile_promotion_blocker", "") {
            if !blocker.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_profile_promotion_blocker", &blocker, tokens);
            }
        }
        if let Some(roles) = item.get("source_roles").and_then(Value::as_array) {
            for role in roles
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_source_role", role, tokens);
            }
        }
        if let Some(hook_types) = item.get("source_hook_types").and_then(Value::as_array) {
            for hook_type in hook_types
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_hook_type", hook_type, tokens);
            }
        }
        if let Some(codex_events) = item.get("source_codex_events").and_then(Value::as_array) {
            for codex_event in codex_events
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_codex_event", codex_event, tokens);
            }
        }
        increment_source_count_bucket(
            &mut breakdown,
            "source_message_counts_by_role",
            item.get("source_role_counts"),
        );
        increment_source_count_bucket(
            &mut breakdown,
            "source_hook_counts_by_type",
            item.get("source_hook_type_counts"),
        );
        increment_source_count_bucket(
            &mut breakdown,
            "source_codex_event_counts_by_event",
            item.get("source_codex_event_counts"),
        );
        if item
            .get("final_session_boundary")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let count = breakdown
                .get("final_session_boundary_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("final_session_boundary_ref_count".to_string(), json!(count));
            }
        }
        if extraction_phase == "provisional" {
            let count = breakdown
                .get("provisional_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("provisional_ref_count".to_string(), json!(count));
            }
        }
        if extraction_phase == "final" {
            let count = breakdown
                .get("final_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("final_ref_count".to_string(), json!(count));
            }
        }
    }
    breakdown
}

fn ref_type_budget_from_counts(
    counts: &HashMap<String, u64>,
    token_counts: &HashMap<String, u64>,
) -> Value {
    let mut buckets = serde_json::Map::new();
    for (class_name, refs) in counts {
        if *refs == 0 {
            continue;
        }
        buckets.insert(
            class_name.clone(),
            json!({
                "refs": refs,
                "tokens": token_counts.get(class_name).copied().unwrap_or(0),
            }),
        );
    }
    Value::Object(buckets)
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("")
}

fn entity_current_state_key(record: &Value) -> Option<String> {
    if string_field(record, "record_type") != "context_entity"
        && string_field(record, "ref_type") != "entity"
    {
        return None;
    }
    let entity_type = string_field(record, "entity_type").trim().to_ascii_lowercase();
    let entity_name = string_field(record, "entity_name").trim().to_ascii_lowercase();
    if entity_type.is_empty() || entity_name.is_empty() {
        return None;
    }
    Some(format!("{entity_type}::{entity_name}"))
}

fn profile_shadow_maps(scored: &[NativeScoredCandidate]) -> (HashMap<String, (u64, String)>, HashMap<String, (u64, String)>) {
    let mut by_entity: HashMap<String, (u64, String)> = HashMap::new();
    let mut by_source_entity_hash: HashMap<String, (u64, String)> = HashMap::new();
    for candidate in scored {
        let record = &candidate.record;
        if string_field(record, "memory_scope") != "user_profile"
            || string_field(record, "session_continuity") != "cross_session"
        {
            continue;
        }
        let Some(profile_hash) = record_ref_hash(record) else {
            continue;
        };
        let updated_at_ms = record.get("updated_at_ms").and_then(Value::as_u64).unwrap_or(0);
        if let Some(key) = entity_current_state_key(record) {
            let replace = by_entity
                .get(&key)
                .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                .unwrap_or(true);
            if replace {
                by_entity.insert(key, (updated_at_ms, profile_hash.clone()));
            }
        }
        if let Some(source_hashes) = record.get("source_entity_hashes").and_then(Value::as_array) {
            for source_hash in source_hashes {
                let source_key = source_hash
                    .as_u64()
                    .map(|value| value.to_string())
                    .or_else(|| source_hash.as_str().map(str::to_string));
                let Some(source_key) = source_key else {
                    continue;
                };
                let replace = by_source_entity_hash
                    .get(&source_key)
                    .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                    .unwrap_or(true);
                if replace {
                    by_source_entity_hash.insert(source_key, (updated_at_ms, profile_hash.clone()));
                }
            }
        }
    }
    (by_entity, by_source_entity_hash)
}

fn profile_shadow_for_candidate(
    candidate: &NativeScoredCandidate,
    by_entity: &HashMap<String, (u64, String)>,
    by_source_entity_hash: &HashMap<String, (u64, String)>,
) -> Option<(String, &'static str)> {
    if candidate.context_class != "entity" || string_field(&candidate.record, "memory_scope") != "session" {
        return None;
    }
    if let Some(ref_hash) = record_ref_hash(&candidate.record) {
        if let Some((_, profile_hash)) = by_source_entity_hash.get(&ref_hash) {
            return Some((profile_hash.clone(), "source_entity_lineage"));
        }
    }
    let key = entity_current_state_key(&candidate.record)?;
    by_entity
        .get(&key)
        .map(|(_, profile_hash)| (profile_hash.clone(), "same_entity_identity"))
}

/// Takes BORROWED refs: it reads a handful of fields off each and never keeps one, and its caller
/// holds them inside a snapshot that outlives the call.
fn profile_shadow_maps_from_selected_refs(
    selected_refs: &[&Value],
) -> (HashMap<String, (u64, String)>, HashMap<String, (u64, String)>) {
    let mut by_entity: HashMap<String, (u64, String)> = HashMap::new();
    let mut by_source_entity_hash: HashMap<String, (u64, String)> = HashMap::new();
    for selected_ref in selected_refs.iter().copied() {
        if string_field(selected_ref, "memory_scope") != "user_profile"
            || string_field(selected_ref, "session_continuity") != "cross_session"
        {
            continue;
        }
        let Some(profile_hash) = record_ref_hash(selected_ref) else {
            continue;
        };
        let updated_at_ms = selected_ref
            .get("updated_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(key) = entity_current_state_key(selected_ref) {
            let replace = by_entity
                .get(&key)
                .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                .unwrap_or(true);
