// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//
// Large-file / attachment throughput bench for the shared-store blob path.
// Measures append_blob (chunked write) MB/s + whole-object read MB/s for a
// range of attachment sizes against the FileObjectStore backend, and against
// the in-process MatrixObject store when built with `--features matrixobject`.
//
// Usage:
//   large_blob_throughput_bench [--backend file|matrixobject] \
//       [--sizes-mb 1,16,128] [--chunk-kb 1024] [--root <dir>]

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore};

struct Opts {
    backend: String,
    sizes_mb: Vec<usize>,
    chunk_kb: usize,
    root: Option<String>,
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        backend: "file".to_string(),
        sizes_mb: vec![1, 16, 128],
        chunk_kb: 1024,
