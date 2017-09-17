// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What a recovered record actually carries: a command to re-run, an outcome naming a page, or
//! the page bytes themselves. With the pages wiped, only the last two of those can rebuild a value.
//!
//!     cargo run --example wal_scan_probe -- <store-root>

use std::path::PathBuf;

use temporalstore_rust::wal::LocalWriteAheadLogStore;

fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("usage: wal_scan_probe <store-root>"));
    let wals = root.join("indexes").join("wals");
    let store = LocalWriteAheadLogStore::new(&wals);

    let records = match store.scan(1, 0, u64::MAX, u64::MAX) {
        Ok(records) => records,
        Err(err) => {
            println!("scan ERROR: {err}");
            return;
        }
    };
    println!("scan returned {} record(s)", records.len());

    let mut with_command = 0usize;
    let mut with_outcomes = 0usize;
    let mut outcome_items = 0usize;
    let mut outcomes_carrying_a_value = 0usize;
    let mut with_staged_blocks = 0usize;
    let mut staged_block_bytes = 0usize;
    let mut undecodable = 0usize;

