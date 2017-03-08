// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use temporalstore_rust::{
    wal::decode_wal_line, Command, CommandResponse, ContextEvent, ContextExtractedEventIndexes,
    ExecuteRequest, ScanStreamRequest, StreamKind, TemporalEngine,
};

fn cold_event(event_id_hash: u64, event_time_ms: u64, text: &str) -> ContextEvent {
    ContextEvent {
        event_id_hash,
        event_time_ms,
        ingestion_time_ms: event_time_ms,
        kind: 7,
        event_type: 7,
        actor_hash: 0,
        status: 0,
        valid_until_ms: 0,
        confidence: 0.95,
        importance: 0.85,
        text: text.to_string(),
        source_ref: "backfill://raw".to_string(),
        related_node_hashes: Vec::new(),
        compact_attrs: Vec::new(),
        // Raw ingestion carries no vector; empty is what a record without one holds.
        vector: Vec::new(),
    }
}

#[test]
fn cold_raw_ingestion_writes_wal_and_avoids_cache_promotion() {
    // Keep the OPERATION in each record: the assertions below read it, and a record carrying
    // results does not carry one. It has to be set before the writes, not before the reads --
    // the records are already on disk by then.
    std::env::set_var("TS_WAL_DATA_ONLY", "0");
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
