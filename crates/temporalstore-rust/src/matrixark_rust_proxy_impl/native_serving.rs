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
