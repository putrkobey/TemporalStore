// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE LOG RECORD COSTS TO BUILD AND TO DECODE.
//!
//! The log's syscall behaviour is measured and flat. What nobody had counted is the RECORD: how
//! many allocations and bytes one costs to build, how many it costs to read back, and how much of
//! what a decode allocates it throws away before returning.
//!
//! Counted, never timed: this box sits between load 1 and 30 for hours, and a count does not move
//! with the neighbours. Every figure is taken at TWO corpus sizes with the ratio printed, so a
//! counter that quietly stopped incrementing reads as a failure rather than as a perfect result.
//!
//! WHOLE FIRST, THEN DECOMPOSED. Each side is measured as one production call over real records,
//! and only then split -- by measuring narrower production entry points over the same corpus, not
//! by adding up statements. Seven statements in this tree were once each measured at exactly 1.000
//! allocations alone while the real total was 9.000, because the buffer they shared grew twice.
//!
//! The store path is held constant and printed: allocated BYTES move at 6.0 per path character,
//! and allocation COUNTS do not move at all, which is why the counts carry every claim here.

#![cfg(test)]
#![allow(clippy::all)]

use crate::index_log::{IndexItem, IndexItemKind, LocalIndexLogStore};
use crate::types::{Command, ShardId};
use crate::wal::LocalWriteAheadLogStore;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;
const VALUE_BYTES: usize = 128;
const SHARD: ShardId = 1;

/// xorshift64*, seeded per record: the records are compressed, and a corpus of repeated bytes
/// measures the compressor rather than the record.
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

/// A key of FIXED WIDTH at every corpus size. An unpadded key is one character wider at 20,000
/// records than at 2,000, and it is written into every record -- which puts fixture into a number
/// that is supposed to be measuring the code.
fn wal_command(index: usize) -> Command {
    Command::StringSet {
        key: format!("tenant/1/object/{index:08}"),
        value: incompressible(VALUE_BYTES, index as u64),
    }
}

fn index_item(index: usize) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: (index % 64) as u32,
        block_ref_key: format!("tenant/1/object/{index:08}"),
        object_key: format!("tenant/1/object/{index:08}").into(),
        model_id: "m".to_string(),
        object_id: 1,
        // ADDRESS-LESS BY DESIGN: this item is the smallest a delta record can carry,
        // and an address would make it bigger rather than smaller. The `size` it used
        // to state was never restored on a row with no address to derive it from.
        entry: None,
        deleted: false,
    }
}

// -------------------------------------------------------------------------------------------
// The instrument, and the proof it is reading.
// -------------------------------------------------------------------------------------------

/// Without the counting allocator every figure below reads zero, which is indistinguishable from
/// a record that costs nothing. Make not-counting representable rather than silent.
#[test]
fn the_record_cost_probes_refuse_to_report_without_the_counting_allocator() {
    let counted = crate::alloc_probe::counted_now();
    #[cfg(feature = "alloc-probe")]
    assert!(
        counted.is_some(),
        "built with the counting feature and the counters are still absent"
    );
    #[cfg(not(feature = "alloc-probe"))]
    assert!(
        counted.is_none(),
        "built without the counting feature, so a reading here would be a table of zeros \
         presented as a measurement"
    );
}

/// The instrument recovers exactly what is planted in it.
///
/// A span counter that reports fewer allocations than were made reads as "this path is cheap".
/// Prove it recovers a known number before trusting it on the records.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_record_span_counter_recovers_exactly_the_allocations_planted_in_it() {
    const PLANTED: usize = 512;
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..PLANTED {
        std::hint::black_box(vec![0u8; 64 + index % 8]);
    }
    let counts: crate::alloc_probe::AllocCounts = probe.stop();
    assert_eq!(
        counts.allocs, PLANTED as u64,
        "planted {PLANTED} allocations and the counter recovered {}; every figure in this file \
         is taken with this instrument",
        counts.allocs
    );
}

// -------------------------------------------------------------------------------------------
// The measurements.
// -------------------------------------------------------------------------------------------

#[cfg(feature = "alloc-probe")]
#[derive(Clone, Copy, Default)]
struct Cost {
    allocs: u64,
    bytes: u64,
    frees: u64,
}

#[cfg(feature = "alloc-probe")]
impl Cost {
    fn per(&self, n: usize) -> f64 {
        self.allocs as f64 / n as f64
    }
    fn bytes_per(&self, n: usize) -> f64 {
        self.bytes as f64 / n as f64
    }
}

#[cfg(feature = "alloc-probe")]
fn measure<T>(work: impl FnOnce() -> T) -> (T, Cost) {
    let probe = crate::alloc_probe::Probe::start();
    let out = work();
    let counts: crate::alloc_probe::AllocCounts = probe.stop();
    (
        out,
        Cost {
            allocs: counts.allocs,
            bytes: counts.alloc_bytes,
            frees: counts.frees,
        },
    )
}

#[cfg(feature = "alloc-probe")]
struct WalArm {
    records: usize,
    build: Cost,
    read_and_decode: Cost,
    read_decode_no_collect: Cost,
    payload_decode: Cost,
    log_bytes: u64,
}

#[cfg(feature = "alloc-probe")]
fn wal_arm(dir: &std::path::Path, records: usize) -> WalArm {
    let store = LocalWriteAheadLogStore::new(dir);
    // BUILD, whole: the production append, over real commands, one record at a time.
    let (_, build) = measure(|| {
        for index in 0..records {
            store.append(SHARD, wal_command(index)).unwrap();
        }
    });

    // The raw framed bytes, fetched OUTSIDE any measured span, so the decode arm below measures
    // the decode and not the walk that produced its input.
    let raws = store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(
        raws.len(),
        records,
        "the walk found {} records for a corpus of {records}; a probe whose denominator collapsed \
         reports a flat cost and is believed",
        raws.len()
    );
    let log_bytes: u64 = raws.iter().map(|(_, raw)| raw.len() as u64).sum();

    // READ AND DECODE, whole: the production entry point recovery calls.
    let (decoded, read_and_decode) = measure(|| {
        store
            .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
            .unwrap()
            .0
    });
    assert_eq!(decoded.len(), records, "decoded record count");
    drop(decoded);

    // The SAME walk, decoding every record, collecting none of them: `record_count` projects each
    // decoded record to `()`. The difference from the line above is what holding the result costs.
    let (counted, read_decode_no_collect) = measure(|| store.record_count(SHARD).unwrap());
    assert_eq!(counted, records, "record_count denominator");

    // PAYLOAD DECODE alone, over bytes already in hand: no file, no framing walk, no collection.
    let (_, payload_decode) = measure(|| {
        for (_, raw) in raws.iter() {
            let body = crate::log_framing::record_body(raw.as_slice());
            std::hint::black_box(crate::wal::decode_wal_line(body).unwrap());
        }
    });

    WalArm {
        records,
        build,
        read_and_decode,
        read_decode_no_collect,
        payload_decode,
        log_bytes,
    }
}

#[cfg(feature = "alloc-probe")]
fn print_wal_arm(arm: &WalArm) {
    let n = arm.records;
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "BUILD (append, whole)",
        arm.build.per(n),
        arm.build.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "READ+DECODE (scan_decoded, whole)",
        arm.read_and_decode.per(n),
        arm.read_and_decode.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "  .. same walk, nothing collected",
        arm.read_decode_no_collect.per(n),
        arm.read_decode_no_collect.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "  .. payload decode alone",
        arm.payload_decode.per(n),
        arm.payload_decode.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "  .. walk minus payload decode",
        arm.read_decode_no_collect.per(n) - arm.payload_decode.per(n),
        arm.read_decode_no_collect.bytes_per(n) - arm.payload_decode.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12} {:>14.2}",
        "on-disk framed bytes/record", arm.log_bytes,
        arm.log_bytes as f64 / n as f64
    );
    println!(
        "  {:<34} {:>12} {:>14}",
        "allocations freed in the decode", arm.payload_decode.frees, ""
    );
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_one_write_ahead_record_costs_to_build_and_decode() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );
    println!("PROFILE: debug (cargo test). Allocation counts are profile-identical.");

    let small = wal_arm(&dir.path().join("small"), SMALL);
    let large = wal_arm(&dir.path().join("large"), LARGE);

    println!("\n  WRITE-AHEAD RECORD, per record, at {SMALL}");
    println!("  {:<34} {:>12} {:>14}", "term", "allocs", "bytes");
    print_wal_arm(&small);
    println!("\n  WRITE-AHEAD RECORD, per record, at {LARGE}");
    println!("  {:<34} {:>12} {:>14}", "term", "allocs", "bytes");
    print_wal_arm(&large);

    println!("\n  RATIO {LARGE} / {SMALL}, per record (1.00 = flat)");
    let ratio = |small: f64, large: f64| if small == 0.0 { 0.0 } else { large / small };
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "BUILD",
        ratio(small.build.per(SMALL), large.build.per(LARGE)),
        ratio(small.build.bytes_per(SMALL), large.build.bytes_per(LARGE))
    );
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "READ+DECODE",
        ratio(
            small.read_and_decode.per(SMALL),
            large.read_and_decode.per(LARGE)
        ),
        ratio(
            small.read_and_decode.bytes_per(SMALL),
            large.read_and_decode.bytes_per(LARGE)
        )
    );
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "payload decode alone",
        ratio(
            small.payload_decode.per(SMALL),
            large.payload_decode.per(LARGE)
        ),
        ratio(
            small.payload_decode.bytes_per(SMALL),
            large.payload_decode.bytes_per(LARGE)
        )
    );
}

#[cfg(feature = "alloc-probe")]
struct IndexArm {
    records: usize,
    build: Cost,
    read: Cost,
    read_no_collect: Cost,
    payload_decode: Cost,
    log_bytes: u64,
}

#[cfg(feature = "alloc-probe")]
fn index_arm(dir: &std::path::Path, records: usize) -> IndexArm {
    let store = LocalIndexLogStore::new(dir);
    let (_, build) = measure(|| {
        for index in 0..records {
            store
                .append_delta(
                    SHARD,
                    vec![index_item(index)],
                    Vec::new(),
                    None,
                    None,
                    false,
                    false,
                )
                .unwrap();
        }
    });

    let raws = store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(
        raws.len(),
        records,
        "the index-log walk found {} records for a corpus of {records}",
        raws.len()
    );
    let log_bytes: u64 = raws.iter().map(|(_, raw)| raw.len() as u64).sum();

    let (collected, read) = measure(|| store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap());
    assert_eq!(collected.len(), records);
    drop(collected);

    let (counted, read_no_collect) = measure(|| store.record_count(SHARD).unwrap());
    assert_eq!(counted, records, "index record_count denominator");

    let (_, payload_decode) = measure(|| {
        for (_, raw) in raws.iter() {
            let payload = crate::log_framing::next_frame(raw.as_slice())
                .unwrap()
                .expect("a frame")
                .1;
            let record: crate::index_log::IndexDeltaRecord =
                crate::index_log::decode_index_payload(payload).unwrap();
            std::hint::black_box(record);
        }
    });

    IndexArm {
        records,
        build,
        read,
        read_no_collect,
        payload_decode,
        log_bytes,
    }
}

#[cfg(feature = "alloc-probe")]
fn print_index_arm(arm: &IndexArm) {
    let n = arm.records;
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "BUILD (append_delta, whole)",
        arm.build.per(n),
        arm.build.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "READ (scan, whole, no decode)",
        arm.read.per(n),
        arm.read.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "  .. same walk, nothing collected",
        arm.read_no_collect.per(n),
        arm.read_no_collect.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12.2} {:>14.0}",
        "DECODE (payload alone)",
        arm.payload_decode.per(n),
        arm.payload_decode.bytes_per(n)
    );
    println!(
        "  {:<34} {:>12} {:>14.2}",
        "on-disk framed bytes/record", arm.log_bytes,
        arm.log_bytes as f64 / n as f64
    );
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_one_index_log_delta_record_costs_to_build_and_decode() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );
    println!("PROFILE: debug (cargo test). Allocation counts are profile-identical.");

    let small = index_arm(&dir.path().join("small"), SMALL);
    let large = index_arm(&dir.path().join("large"), LARGE);
