// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Append N delta records into a fresh index log, and nothing else.
//!
//! Run under `strace -f -c` at two record counts; every fixed cost -- process start, the
//! dynamic loader, the tempdir -- is identical in both arms and cancels in the difference.
//!
//!     cargo run --release --example index_log_append_syscall_probe -- <dir> <records>

use temporalstore_rust::index_log::{IndexItem, IndexItemKind, LocalIndexLogStore};

fn small_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.into(),
        model_id: "m".to_string(),
        object_id: 1,
        deleted: false,
        // ADDRESS-LESS BY DESIGN: the smallest row a delta record can carry. An address would
        // make it bigger, not smaller, and the size it stated was never restored without one.
        entry: None,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
