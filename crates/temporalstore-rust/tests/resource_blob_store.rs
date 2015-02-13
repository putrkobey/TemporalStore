// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Integration coverage for the engine-owned attachment blob store.
//!
//! One TemporalStore holds everything: chunks stay searchable in records, and the FULL
//! original attachment is stored beside the engine and fetchable again by its
//! `temporalstore://resources/{tenant}/{content-hash}` URI. These tests drive the public
//! command surface to prove:
//!   * a multi-part upload commits to a content-addressed URI and fetches back byte-identical,
//!   * the single-shot put equals begin+append+commit and re-putting identical content
//!     lands on the same URI (dedup by content),
//!   * range fetches honor offset/length and report eof exactly at the end,
//!   * the sweep deletes only unreferenced blobs, never referenced ones, and a fresh blob
//!     survives even when unreferenced (the manifest may not have landed yet),
//!   * URI parsing is strict, so a fetch cannot smuggle a path,
//!   * an oversized resource ingested through the context workflow gets a REAL external
//!     URI whose bytes fetch back as the original payload.

use std::path::PathBuf;

use temporalstore_rust::{
    ingest_resource_skill_context, Command, CommandResponse, ContextResourceSkillIngestRequest,
    ExecuteRequest, TemporalEngine,
};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

const SHARD_ID: u64 = 1;
const TENANT: u64 = 77;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-resource-blobs-{name}-{pid}"));
    root
}

fn new_engine(name: &str) -> TemporalEngine {
    let root = unique_root(name);
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command,
    });
    assert!(
        response.status.ok,
        "command failed: {:?}",
        response.status
    );
    response.response
}

fn put(engine: &TemporalEngine, payload: &[u8]) -> (String, u64, u64) {
    match run(
        engine,
        Command::ContextResourceBlobPut {
            tenant_hash: TENANT,
            payload_base64: BASE64.encode(payload),
        },
    ) {
        CommandResponse::ContextResourceBlobCommitted {
            uri,
            size_bytes,
            content_hash,
        } => (uri, size_bytes, content_hash),
        other => panic!("unexpected response: {other:?}"),
    }
}

fn fetch(engine: &TemporalEngine, uri: &str, offset: u64, length: u64) -> (Vec<u8>, u64, bool) {
    match run(
        engine,
        Command::ContextResourceBlobFetch {
            uri: uri.to_string(),
            offset,
            length,
        },
    ) {
        CommandResponse::ContextResourceBlobChunk {
            payload_base64,
            total_size,
            eof,
        } => (
            BASE64.decode(payload_base64).expect("valid base64"),
            total_size,
            eof,
        ),
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn multipart_upload_commits_and_fetches_back_byte_identical() {
    let engine = new_engine("multipart");
    let part_a = vec![7u8; 300_000];
    let part_b = vec![9u8; 200_000];

    let token = match run(
        &engine,
        Command::ContextResourceBlobBegin {
            tenant_hash: TENANT,
        },
    ) {
        CommandResponse::ContextResourceBlobUpload { upload_token, .. } => upload_token,
        other => panic!("unexpected response: {other:?}"),
