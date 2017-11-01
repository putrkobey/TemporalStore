// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::reports::{GoldenCaseReport, GoldenCorpusReport};
use crate::engine::TemporalEngine;
use crate::types::{
    parse_feature_filters, Command, CommandResponse, ExecuteRequest, FeatureFilterOp,
    FeaturePoint, ControlStateFamily, ControlStateSelectionType, SequenceFeatureRow,
};

pub fn native_feature_sequence_golden_corpus_report() -> GoldenCorpusReport {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut cases = Vec::new();

    let matching = SequenceFeatureRow {
        timestamp_ms: 1_000,
        gid: 42,
        action_type: 7,
        duration: 33,
        author_id: 9_001,
    };
    let replacement = SequenceFeatureRow {
        timestamp_ms: 1_001,
        gid: 43,
        action_type: 7,
        duration: 34,
        author_id: 9_002,
    };
    let non_matching = SequenceFeatureRow {
        timestamp_ms: 1_002,
        gid: 44,
        action_type: 8,
        duration: 35,
        author_id: 9_003,
    };

    record_golden_case(
        &mut cases,
        "native_feature_proto_roundtrip",
        SequenceFeatureRow::decode_feature_proto_value(
            matching.timestamp_ms,
            &matching.encode_feature_proto_value(),
        ) == Some(matching.clone()),
        "feature protobuf fields gid/action_type/duration/author_id round-trip",
    );

    let duplicate_filters = parse_feature_filters(["gid = 42", "duration > 30", "gid != 42"]);
    record_golden_case(
        &mut cases,
        "native_feature_filter_last_field_wins",
        matches!(duplicate_filters, Ok(ref filters) if filters.len() == 2
            && filters[0].field == "gid"
            && filters[0].op == FeatureFilterOp::NotEqual
            && filters[0].value == 42
            && filters[1].field == "duration"),
        "duplicate filter fields replace the previous field predicate",
    );

    let append = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "native-golden-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: matching.timestamp_ms,
                    value: matching.encode_feature_proto_value(),
                },
                FeaturePoint {
                    timestamp_ms: replacement.timestamp_ms,
                    value: replacement.encode_feature_proto_value(),
                },
                FeaturePoint {
                    timestamp_ms: non_matching.timestamp_ms,
                    value: non_matching.encode_feature_proto_value(),
                },
            ],
        },
    });
    record_golden_case(
        &mut cases,
        "native_feature_append_status",
        append.status.ok,
        "feature points append through the Rust engine",
    );

    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "native-golden-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: Some(10),
            filters: parse_feature_filters(["action_type = 7", "duration <= 34"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "native_feature_filtered_query",
        matches!(
            filtered.response,
            CommandResponse::FeaturePoints { ref points }
                if points.iter().map(|point| point.timestamp_ms).collect::<Vec<_>>()
                    == vec![matching.timestamp_ms, replacement.timestamp_ms]
        ),
        "binary protobuf feature filters select matching timestamp/value rows",
    );

    let aggregate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "native-golden-aggregate".to_string(),
            start_ms: 0,
            end_ms: 10,
            aggregator: "sum".to_string(),
            count: None,
        },
    });
    record_golden_case(
        &mut cases,
        "native_feature_empty_sum_aggregate",
        aggregate.response == CommandResponse::Aggregate { value: 0 },
        "Empty feature aggregate returns neutral zero",
    );

    let rows = vec![matching.clone(), replacement.clone(), non_matching.clone()];
    let add_rows = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "native-golden-sequence".to_string(),
            rows: rows.clone(),
        },
    });
    record_golden_case(
        &mut cases,
        "native_sequence_add_status",
        add_rows.status.ok,
        "sequence rows append through timestamped KV pages",
    );

    let sequence_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceQuery {
            key: "native-golden-sequence".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: 10,
            filters: parse_feature_filters(["gid >= 42", "action_type = 7"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "native_sequence_filtered_query",
        sequence_query.response
            == CommandResponse::SequenceRows {
                rows: vec![matching, replacement],
            },
        "sequence filters reuse the feature predicate semantics",
    );

    let block_layout = engine.storage_recovery_report(1).feature_block_layout;
    record_golden_case(
        &mut cases,
        "native_timestamped_kv_shared_block_layout",
        block_layout.packed_feature_blocks >= 1
            && block_layout.unique_feature_block_refs < block_layout.indexed_feature_points
            && !block_layout.has_errors(),
        "Timestamped feature/sequence values share packed pages without layout errors",
    );

    let total_cases = cases.len();
    let passed_cases = cases.iter().filter(|case| case.passed).count();
    GoldenCorpusReport {
        corpus: "feature_sequence_proto_v1".to_string(),
        total_cases,
        passed_cases,
        failed_cases: total_cases.saturating_sub(passed_cases),
        cases,
