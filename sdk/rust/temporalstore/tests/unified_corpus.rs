// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

fn corpus_path() -> PathBuf {
    if let Ok(path) = std::env::var("TEMPORALSTORE_UNIFIED_CORPUS") {
        return PathBuf::from(path);
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let external = manifest_dir.join("../../unified/temporalstore_unified_corpus.json");
    if external.exists() {
        return external;
    }
    manifest_dir.join("../../../compat/unified_temporalstore_cases.json")
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field}"))
}

fn int_field(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("missing integer field {field}"))
}

#[test]
fn unified_corpus_proxy_contract() {
    let path = corpus_path();
    let raw = fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {path:?}: {err}"));
    let corpus: Value =
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse {path:?}: {err}"));
    let schema_version = corpus.get("schema_version").and_then(Value::as_u64);
    if let Some(version) = schema_version {
        assert_eq!(version, 1);
    }

    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("cases must be an array");
    assert!(!cases.is_empty(), "unified corpus must have cases");

    // `coverage.required_case_names` is the corpus's own declaration of which cases
    // must exist. A declared case is allowed to carry no steps: the corpus ships one
    // such case and the engine-side runner executes it as a no-op. An UNDECLARED
    // step-less case is still a mistake, so the clause narrows rather than goes away.
    let declared_cases: BTreeSet<String> = corpus
        .get("coverage")
        .and_then(|coverage| coverage.get("required_case_names"))
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    let mut case_names = BTreeSet::new();
    let mut command_kinds = BTreeSet::new();
    let mut context_case_count = 0usize;
    let mut context_step_count = 0usize;

    for case in cases {
        let case_name = string_field(case, "name");
        assert!(
            case_names.insert(case_name.to_string()),
            "duplicate case {case_name}"
        );
        assert!(
            int_field(case, "shard_id") > 0,
            "{case_name} shard_id must be positive"
        );
        let steps = case
            .get("steps")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{case_name} steps must be an array"));
        assert!(
            !steps.is_empty() || declared_cases.contains(case_name),
            "{case_name} has no steps and is not declared in coverage.required_case_names"
        );
        let mut has_context = false;
        for step in steps {
            let command = step.get("command").expect("step needs command");
            let kind = string_field(command, "kind");
            command_kinds.insert(kind.to_string());
            if kind.starts_with("context_") {
                has_context = true;
                context_step_count += 1;
                validate_context_command(case_name, kind, command);
            }
        }
        if has_context {
            context_case_count += 1;
        }
    }

    if let Some(coverage) = corpus.get("coverage") {
        assert_required_strings(coverage, "required_case_names", &case_names);
        assert_required_strings(coverage, "required_command_kinds", &command_kinds);
    }

    let focused_generated_corpus = case_names.contains("context_pipeline_scale_e2e");
    let full_context_corpus =
        !focused_generated_corpus && case_names.contains("context_node_roundtrip");
    if full_context_corpus {
        for required in [
            "context_node_roundtrip",
            "context_event_index_audit_dirty_models",
            "context_management_ingest_retrieve_pipeline",
            "context_retrieval_qa_synonym_ranking",
            "context_events_segments_entities_child_refs",
            "context_embeddings_summaries_l0_l1_pipeline",
            "context_compression_secondary_index_query_debug_flow",
            "context_resource_skill_parser_reference_parity",
            "context_benchmark_fixture_gates",
            "context_benchmark_full_dataset_gates",
        ] {
            assert!(
                case_names.contains(required),
                "missing required context parity case {required}"
            );
        }
    } else {
        assert!(
            case_names.contains("context_pipeline_scale_e2e"),
            "focused generated context corpus must expose context_pipeline_scale_e2e"
        );
    }

    let required_commands: &[&str] = if full_context_corpus {
        &[
            "context_upsert_node",
            "context_write_event",
            "context_write_index_ref",
            "context_mark_summary_dirty",
            "context_get_node",
            "context_query_events",
            "context_query_index",
            "context_write_pack_audit",
            "context_query_pack_audit",
            "context_query_summary_dirty",
        ]
    } else {
        &[
            "context_api_ingest_raw_event",
            "context_batch_ingest_raw_events",
            "context_stream_ingest_raw_events",
            "context_upsert_node",
            "context_upsert_child_ref",
            "context_upsert_embedding",
            "context_upsert_summary",
            "context_write_compression",
