// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RESTORE READS, AND WHAT IS LEFT OVER WHEN EVERY NAMED READER IS SUBTRACTED.
//!
//! A restore brings a shard back from two logs: it replays the write-ahead log in bounded
//! windows, and it folds the index log's deltas over what the last dump reflected. #1936 put a
//! counter inside the first. This puts one inside the second, and then asks the question that
//! found both -- take the total from the KERNEL, subtract the readers the engine names, and look
//! at what is left.
//!
//! The residual is the subject. It is never the sum of the rows it audits, which is a number that
//! cannot fail; and when it reads ZERO it is distrusted, because an instrument that always
//! answered zero would say exactly that. A read of a file neither counter can see is planted
//! inside the same span and the residual is required to come back as exactly that many bytes.

#![cfg(test)]

use crate::index_log::{
    index_log_piece_read_counts_on_this_thread, set_index_log_segment_bytes_for_test, IndexItem,
    IndexItemKind, LocalIndexLogStore,
};
use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread, LocalWriteAheadLogStore,
};

/// Two sizes and the ratio between them. Four times the corpus.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;

const VALUE_BYTES: usize = 128;
const SHARD: crate::types::ShardId = 21;

/// The window the engine replays with and the size it rolls at, both scaled down by eight, so a
/// test log of a few hundred kilobytes still crosses several windows and several pieces. What is
/// measured is windows against the log, and that is set by the RATIO, which is preserved.
const WINDOW_BYTES: u64 = 64 * 1024;
const WAL_SEGMENT_BYTES: u64 = 32 * 1024;
const INDEX_SEGMENT_BYTES: u64 = 8 * 1024;

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_thread_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let mut rchar = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            rchar = rest.trim().parse().ok();
        }
    }
    Some(IoReading {
        rchar: rchar?,
        self_bytes: text.len() as u64,
    })
}

/// `/proc/thread-self/io`, not `/proc/self/io`: the process counter charges a span whatever
/// another thread happened to read, and a restore runs on one thread while the suite does not.
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE FIXTURE
// ---------------------------------------------------------------------------------------------

fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

fn index_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.into(),
        model_id: "m".to_string(),
        object_id: 1,
        deleted: false,
        // ADDRESS-LESS BY DESIGN: this item is the smallest a delta record can carry,
        // and an address would make it bigger rather than smaller. The `size` it used
        // to state was never restored on a row with no address to derive it from.
        entry: None,
    }
}

/// Put the two thresholds back on drop, panic included.
struct Thresholds;
impl Drop for Thresholds {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(None);
        set_index_log_segment_bytes_for_test(None);
    }
}

struct Corpus {
    _wal_dir: tempfile::TempDir,
    _index_dir: tempfile::TempDir,
    wal: LocalWriteAheadLogStore,
    index: LocalIndexLogStore,
    wal_bytes: u64,
    wal_pieces: usize,
    index_bytes: u64,
    index_pieces: usize,
}

fn build(records: usize) -> Corpus {
    set_wal_segment_bytes_for_test(Some(WAL_SEGMENT_BYTES));
    set_index_log_segment_bytes_for_test(Some(INDEX_SEGMENT_BYTES));

    let wal_dir = tempfile::tempdir().unwrap();
    let index_dir = tempfile::tempdir().unwrap();
    let wal = LocalWriteAheadLogStore::new(wal_dir.path());
    let index = LocalIndexLogStore::new(index_dir.path());

    for value in 0..records {
        wal.append(
            SHARD,
            Command::StringSet {
                key: format!("k{value:08}"),
                value: incompressible(VALUE_BYTES, value as u64),
            },
        )
        .expect("the fixture's writes must land");
        index
            .append_delta(
                SHARD,
                vec![index_item(
                    (value % 64) as u32,
                    &format!("tenant/1/object/{value:08}"),
                )],
                Vec::new(),
                Some(value as u64 + 1),
                None,
                false,
                false,
            )
            .unwrap();
    }

    let (wal_bytes, wal_pieces) = log_on_disk(wal_dir.path(), ".wal");
    let (index_bytes, index_pieces) = log_on_disk(index_dir.path(), "");

    Corpus {
        _wal_dir: wal_dir,
        _index_dir: index_dir,
        wal,
        index,
        wal_bytes,
        wal_pieces,
        index_bytes,
        index_pieces,
    }
}

/// Read back from the directory rather than assumed: a fixture that quietly produced one piece
/// would report a flat cost and be believed.
fn log_on_disk(dir: &std::path::Path, marker: &str) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut pieces = 0usize;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".lock") || !name.contains(marker) {
                continue;
            }
            pieces += 1;
            bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        }
    }
    (bytes, pieces)
}

// ---------------------------------------------------------------------------------------------
// THE RESTORE UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct Restore {
    /// Records the write-ahead replay handed back.
    replayed: usize,
    /// Records the index-log fold handed back.
    folded: usize,
    windows: u64,
    /// Bytes charged by #1936's counter, inside the write-ahead log's own read.
    wal_bytes: u64,
    wal_reads: u64,
    /// Bytes charged by this change's counter, inside the index log's own read.
    index_bytes: u64,
    index_reads: u64,
    /// Base-header reads this restore made -- one per piece per window.
    ///
    /// Each was an 8 KiB `BufReader` fill before this change and is a bounded
    /// `WAL_BASE_HEADER_PROBE_BYTES` window after it, so this count times the difference is
    /// exactly what the bound took off this restore. Exact rather than estimated because every
    /// piece in this fixture is longer than the buffer that used to be filled, which is asserted.
    header_reads: u64,
    /// What the kernel charged this thread over the same span.
    kernel_rchar: Option<u64>,
}

impl Restore {
    fn named(&self) -> u64 {
        self.wal_bytes + self.index_bytes
    }

    /// What this restore would have read before `read_wal_base` was bounded.
    fn bytes_before_the_bound(&self) -> u64 {
        self.named()
            + self.header_reads * (8 * 1024 - crate::wal::wal_base_header_probe_bytes() as u64)
    }
    /// THE SUBJECT. The kernel's total less every reader the engine names.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.named() as i64)
    }
}

/// Replay the write-ahead log in bounded windows and fold the index log, the way a restore does,
/// counting what both read.
fn restore(corpus: &Corpus, plant: Option<&std::path::Path>) -> Restore {
    let (wal_before, wal_reads_before) = wal_piece_read_counts_on_this_thread();
    let (index_before, index_reads_before) = index_log_piece_read_counts_on_this_thread();
    crate::wal::WAL_SEGMENT_HEADER_READS.with(|reads| reads.set(0));
