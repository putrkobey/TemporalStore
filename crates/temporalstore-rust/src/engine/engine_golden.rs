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
