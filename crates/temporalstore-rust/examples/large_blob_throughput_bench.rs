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
        root: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(k) = args.next() {
        let v = args.next().unwrap_or_default();
        match k.as_str() {
            "--backend" => o.backend = v,
            "--sizes-mb" => {
                o.sizes_mb = v.split(',').map(|s| s.trim().parse().unwrap()).collect()
            }
            "--chunk-kb" => o.chunk_kb = v.parse().unwrap(),
            "--root" => o.root = Some(v),
            _ => {}
        }
    }
    o
}

async fn bench_store<O: ObjectStore>(store: Arc<O>, backend: &str, sizes_mb: &[usize], chunk_kb: usize) {
    let chunk_bytes = chunk_kb * 1024;
    let chunk = Bytes::from(vec![0x5au8; chunk_bytes]);
    println!("[");
    let mut first = true;
    for &size_mb in sizes_mb {
        let total_bytes = size_mb * 1024 * 1024;
        let n_chunks = total_bytes / chunk_bytes;
        let key = format!("bench/attach-{size_mb}mb.blob");
        // best-effort clean
        let _ = store.delete(&key).await;

