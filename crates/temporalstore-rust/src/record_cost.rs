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

    println!("\n  INDEX-LOG DELTA RECORD, per record, at {SMALL}");
    println!("  {:<34} {:>12} {:>14}", "term", "allocs", "bytes");
    print_index_arm(&small);
    println!("\n  INDEX-LOG DELTA RECORD, per record, at {LARGE}");
    println!("  {:<34} {:>12} {:>14}", "term", "allocs", "bytes");
    print_index_arm(&large);

    let ratio = |s: f64, l: f64| if s == 0.0 { 0.0 } else { l / s };
    println!("\n  RATIO {LARGE} / {SMALL}, per record (1.00 = flat)");
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "BUILD",
        ratio(small.build.per(SMALL), large.build.per(LARGE)),
        ratio(small.build.bytes_per(SMALL), large.build.bytes_per(LARGE))
    );
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "READ",
        ratio(small.read.per(SMALL), large.read.per(LARGE)),
        ratio(small.read.bytes_per(SMALL), large.read.bytes_per(LARGE))
    );
    println!(
        "  {:<34} {:>12.3} {:>14.3}",
        "DECODE",
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

// -------------------------------------------------------------------------------------------
// DECOMPOSITION: the whole was measured above; this splits it at production boundaries.
// -------------------------------------------------------------------------------------------

/// The record shapes both logs carry here, built once so every span below measures the same
/// bytes.
fn one_delta_record(index: usize) -> crate::index_log::IndexDeltaRecord {
    crate::index_log::IndexDeltaRecord {
        shard_id: SHARD,
        sequence: index as u64,
        items: vec![index_item(index)],
        meta: None,
        applied_wal_sequence: None,
        key_states: Vec::new(),
        upsert: false,
        shared_object_key: None,
    }
}

/// WHERE THE INDEX-LOG DELTA APPEND'S ALLOCATIONS GO.
///
/// The whole append is measured first, then the same corpus is driven through the production
/// primitives inside it one at a time. What none of them claims is printed as a residual and
/// named, because a residual computed from the rows that feed it cannot fail.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn where_the_index_log_delta_appends_allocations_go() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );
    println!("PROFILE: debug (cargo test). Allocation counts are profile-identical.");
    const N: usize = LARGE;

    // WHOLE, first and on its own: the production append over real items.
    let store = LocalIndexLogStore::new(&dir.path().join("whole"));
    let (_, whole) = measure(|| {
        for index in 0..N {
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
    assert_eq!(store.record_count(SHARD).unwrap(), N, "append denominator");

    // THE SAME PRODUCTION CALL CARRYING NO ITEMS. Everything an append does that does not depend
    // on the items -- resolving which piece the record belongs in, the roll check, the file open,
    // the write, the flush, the sequence bookkeeping -- is in this and the per-item work is not.
    // It is a real shape: a record carrying only an anchor has no items either.
    let empty_store = LocalIndexLogStore::new(&dir.path().join("empty"));
    empty_store
        .append_delta(SHARD, Vec::new(), Vec::new(), None, None, false, false)
        .unwrap();
    let (_, without_items) = measure(|| {
        for _ in 0..N {
            empty_store
                .append_delta(SHARD, Vec::new(), Vec::new(), None, None, false, false)
                .unwrap();
        }
    });

    // Now the parts, each the production function, over the same record shape.
    let (_, staging) = measure(|| {
        for index in 0..N {
            std::hint::black_box(vec![index_item(index)]);
        }
    });
    let (_, record_build) = measure(|| {
        for index in 0..N {
            std::hint::black_box(one_delta_record(index));
        }
    });
    let records: Vec<crate::index_log::IndexDeltaRecord> =
        (0..N).map(one_delta_record).collect();
    let (payloads, payload_encode) = measure(|| {
        records
            .iter()
            .map(|record| {
                crate::index_log::encode_index_payload(
                    record,
                    crate::index_log::INDEX_LOG_SHAPE_DELTA,
                )
                .unwrap()
            })
            .collect::<Vec<Vec<u8>>>()
    });
    let (frames, frame_encode) = measure(|| {
        payloads
            .iter()
            .map(|payload| crate::log_framing::encode_record(payload))
            .collect::<Vec<Vec<u8>>>()
    });
    let payload_bytes: usize = payloads.iter().map(|p| p.len()).sum();
    let frame_bytes: usize = frames.iter().map(|f| f.len()).sum();

    // The collecting vectors above are themselves allocations. Measure a run that collects
    // NOTHING, so the rows report the primitive and not the harness holding its output.
    let (_, payload_encode_nocollect) = measure(|| {
        for record in records.iter() {
            std::hint::black_box(
                crate::index_log::encode_index_payload(
                    record,
                    crate::index_log::INDEX_LOG_SHAPE_DELTA,
                )
                .unwrap(),
            );
        }
    });
    // The encoder PRODUCTION CALLS, over a buffer it reuses -- which is the whole difference
    // between the rows. A decomposition measuring the copy the append no longer makes reports a
    // NEGATIVE residual, which is what double counting looks like and is worth seeing.
    let (_, payload_encode_into) = measure(|| {
        let mut scratch = Vec::new();
        for record in records.iter() {
            crate::index_log::encode_index_payload_into(
                record,
                crate::index_log::INDEX_LOG_SHAPE_DELTA,
                &mut scratch,
            )
            .unwrap();
            std::hint::black_box(&scratch);
        }
    });
    let (_, frame_encode_into) = measure(|| {
        let mut scratch = Vec::new();
        for payload in payloads.iter() {
            crate::log_framing::encode_record_into(payload, &mut scratch);
            std::hint::black_box(&scratch);
        }
    });
    let (_, frame_encode_nocollect) = measure(|| {
        for payload in payloads.iter() {
            std::hint::black_box(crate::log_framing::encode_record(payload));
        }
    });

    println!("\n  INDEX-LOG DELTA APPEND, per record, at {N}");
    println!("  {:<44} {:>10} {:>12}", "term", "allocs", "bytes");
    let row = |label: &str, cost: &Cost| {
        println!(
            "  {:<44} {:>10.2} {:>12.0}",
            label,
            cost.per(N),
            cost.bytes_per(N)
        );
    };
    row("WHOLE append_delta", &whole);
    row("  the same call carrying NO items", &without_items);
    row("  staging the item vector", &staging);
    row("  building the record struct", &record_build);
    row("  encode into a REUSED buffer (production)", &payload_encode_into);
    row("  frame into a REUSED buffer (production)", &frame_encode_into);
    row("  [not called] encode to a fresh vector", &payload_encode_nocollect);
    row("  [not called] frame to a fresh vector", &frame_encode_nocollect);
    let named = record_build.per(N) + payload_encode_into.per(N) + frame_encode_into.per(N);
    let named_bytes = record_build.bytes_per(N)
        + payload_encode_into.bytes_per(N)
        + frame_encode_into.bytes_per(N);
    println!("  {:<44} {:>10.2} {:>12.0}", "  named rows summed", named, named_bytes);
    println!(
        "  {:<44} {:>10.2} {:>12.0}",
        "  RESIDUAL (whole minus rows)",
        whole.per(N) - named,
        whole.bytes_per(N) - named_bytes
    );
    println!(
        "  {:<44} {:>10.2} {:>12.0}",
        "    of which: an append carrying NO items",
        without_items.per(N),
        without_items.bytes_per(N)
    );
    println!(
        "  {:<44} {:>10.2} {:>12.0}",
        "    of which: PER-ITEM, not otherwise named",
        whole.per(N) - named - without_items.per(N),
        whole.bytes_per(N) - named_bytes - without_items.bytes_per(N)
    );
    println!(
        "\n  payload bytes/record {:.2}   framed bytes/record {:.2}   frame adds {:.2}",
        payload_bytes as f64 / N as f64,
        frame_bytes as f64 / N as f64,
        (frame_bytes - payload_bytes) as f64 / N as f64
    );
    println!(
        "  what the fresh-vector pair costs and the reused pair does not: {:.2} allocations and {:.0} bytes a record, spent prepending ONE container byte to {} bytes and then {} frame bytes to all of that",
        payload_encode_nocollect.per(N) + frame_encode_nocollect.per(N)
            - payload_encode_into.per(N)
            - frame_encode_into.per(N),
        payload_encode_nocollect.bytes_per(N) + frame_encode_nocollect.bytes_per(N)
            - payload_encode_into.bytes_per(N)
            - frame_encode_into.bytes_per(N),
        payload_bytes / N,
        (frame_bytes - payload_bytes) / N
    );
}

/// WHAT READING A RECORD BACK COSTS BEFORE ANYTHING DECODES IT.
///
/// The walk hands each record's framed bytes over as an owned vector. This measures the reader
/// alone, over bytes already in memory, so no file syscall is in the figure.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_framed_reader_allocates_per_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );
    const N: usize = LARGE;
    let store = LocalWriteAheadLogStore::new(&dir.path().join("wal"));
    for index in 0..N {
        store.append(SHARD, wal_command(index)).unwrap();
    }
    let raws = store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(raws.len(), N, "reader denominator");
    let mut stream = Vec::new();
    for (_, raw) in raws.iter() {
        stream.extend_from_slice(raw);
    }
    println!(
        "  stream {} bytes, {} records, {:.2} bytes/record",
        stream.len(),
        N,
        stream.len() as f64 / N as f64
    );

    // ABBA: the reader twice, so a one-time cost paid by whichever ran first cannot be read as
    // a per-record cost.
    let mut walked = [0usize; 2];
    let mut costs = [Cost::default(); 2];
    for round in 0..2 {
        let (count, cost) = measure(|| {
            let mut reader = std::io::BufReader::new(std::io::Cursor::new(stream.as_slice()));
            let mut count = 0usize;
            while let Some(raw) = crate::log_framing::read_raw_record(&mut reader).unwrap() {
                std::hint::black_box(&raw);
                count += 1;
            }
            count
        });
        walked[round] = count;
        costs[round] = cost;
    }
    assert_eq!(walked[0], N, "first walk denominator");
    assert_eq!(walked[1], N, "second walk denominator");
    println!(
        "\n  read_raw_record, per record: round 1 {:.2} allocs / {:.0} bytes, round 2 {:.2} / {:.0}",
        costs[0].per(N),
        costs[0].bytes_per(N),
        costs[1].per(N),
        costs[1].bytes_per(N)
    );
    println!(
        "  a record is {:.2} bytes on disk and the reader asks for {:.0}: the payload is read \
         into one vector and then copied whole into a second, and the first is dropped",
        stream.len() as f64 / N as f64,
        costs[1].bytes_per(N)
    );
}

/// ORDER EFFECT CONTROL for the read arms: the same two walks, in both orders.
///
/// The first table showed the collecting walk costing 3.23 allocations a record more than the
/// non-collecting one at 2,000 records and 0.12 more at 20,000, which is not a shape any
/// per-record cost has. Either it is a one-time cost charged to whichever ran first, or it is
/// real. This says which.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn the_read_arms_are_measured_in_both_orders() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );
    for &n in [SMALL, LARGE].iter() {
        let store = LocalWriteAheadLogStore::new(&dir.path().join(format!("abba{n}")));
        for index in 0..n {
            store.append(SHARD, wal_command(index)).unwrap();
        }
        // A, B, B, A -- an order effect cancels in the pairing and a real difference does not.
        let (a1, ca1) = measure(|| store.scan_decoded(SHARD, 0, u64::MAX, u64::MAX).unwrap().0.len());
        let (b1, cb1) = measure(|| store.record_count(SHARD).unwrap());
        let (b2, cb2) = measure(|| store.record_count(SHARD).unwrap());
        let (a2, ca2) = measure(|| store.scan_decoded(SHARD, 0, u64::MAX, u64::MAX).unwrap().0.len());
        assert_eq!([a1, b1, b2, a2], [n, n, n, n], "ABBA denominators at {n}");
        println!(
            "\n  at {n} records, per record:\n    scan_decoded  A1 {:.3}  A2 {:.3}\n    record_count  B1 {:.3}  B2 {:.3}\n    A mean {:.3}  B mean {:.3}  difference {:.3}",
            ca1.per(n),
            ca2.per(n),
            cb1.per(n),
            cb2.per(n),
            (ca1.per(n) + ca2.per(n)) / 2.0,
            (cb1.per(n) + cb2.per(n)) / 2.0,
            (ca1.per(n) + ca2.per(n)) / 2.0 - (cb1.per(n) + cb2.per(n)) / 2.0,
        );
    }
}

// -------------------------------------------------------------------------------------------
// THE FORMAT MUST NOT MOVE. Both changes are read/write-path only, and this is what says so.
// -------------------------------------------------------------------------------------------

/// The reader as it stood before the record's bytes were read into one buffer instead of two.
///
/// Kept verbatim as the CONTROL. A reader is judged by the bytes it hands back and by where it
/// stops, and neither can be judged against the implementation that replaced it -- so the old one
/// is here, and every assertion below is the two of them driven over the same bytes.
fn read_raw_record_previous<R: std::io::BufRead>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<u8>>> {
    let first = {
        let buffered = reader.fill_buf()?;
        buffered.first().copied()
    };
    let Some(first) = first else {
        return Ok(None);
    };
    if first == 0 {
        return Ok(None);
    }
    if first != crate::log_framing::FRAME_MAGIC_V3 {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(None);
        }
        if !line.ends_with(b"\n") {
            return Ok(None);
        }
        return Ok(Some(line));
    }
    let mut raw = Vec::with_capacity(64);
    let mut marker = [0u8; 1];
    if reader.read_exact(&mut marker).is_err() {
        return Ok(None);
    }
    raw.push(marker[0]);
    let mut declared: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        if reader.read_exact(&mut byte).is_err() {
            return Ok(None);
        }
        raw.push(byte[0]);
        declared |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "record length is not a varint",
            ));
        }
    }
    let mut digest = [0u8; 4];
    if reader.read_exact(&mut digest).is_err() {
        return Ok(None);
    }
    raw.extend_from_slice(&digest);
    let declared_len = declared as usize;
    let mut payload = Vec::new();
    let read = {
        use std::io::Read as _;
        match reader.by_ref().take(declared).read_to_end(&mut payload) {
            Ok(read) => read,
            Err(_) => return Ok(None),
        }
    };
    if read != declared_len {
        return Ok(None);
    }
    raw.extend_from_slice(&payload);
    Ok(Some(raw))
}

/// Walk `bytes` to exhaustion with one of the two readers, reporting every record it produced and
/// how far it got.
///
/// The POSITION matters as much as the records. A reader that stops one record early hands back a
/// prefix that compares equal element by element against a shorter control, so the walk reports
/// where it stopped and the comparison takes both.
fn walk_with(
    bytes: &[u8],
    reader_fn: fn(&mut std::io::BufReader<std::io::Cursor<&[u8]>>) -> std::io::Result<Option<Vec<u8>>>,
) -> (Vec<Vec<u8>>, usize, bool) {
    let mut reader = std::io::BufReader::new(std::io::Cursor::new(bytes));
    let mut records = Vec::new();
    let mut errored = false;
    loop {
        match reader_fn(&mut reader) {
            Ok(Some(raw)) => records.push(raw),
            Ok(None) => break,
            Err(_) => {
                errored = true;
                break;
            }
        }
    }
    let consumed: usize = records.iter().map(|record| record.len()).sum();
    (records, consumed, errored)
}

fn walk_new(bytes: &[u8]) -> (Vec<Vec<u8>>, usize, bool) {
    walk_with(bytes, |reader| crate::log_framing::read_raw_record(reader))
}

fn walk_old(bytes: &[u8]) -> (Vec<Vec<u8>>, usize, bool) {
    walk_with(bytes, |reader| read_raw_record_previous(reader))
}

/// Every byte shape the two logs can put in front of a reader.
///
/// Built from the production writers wherever a writer exists, and by hand only for the shapes a
/// writer cannot produce on purpose -- a torn tail, a corrupt varint, the reservation zeros.
fn reader_corpus() -> Vec<(String, Vec<u8>)> {
    let mut shapes: Vec<(String, Vec<u8>)> = Vec::new();

    // A real binary-framed write-ahead log, values from empty to past a varint boundary.
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(&dir.path().join("wal"));
    for (index, value_bytes) in [0usize, 1, 63, 127, 128, 1_000, 70_000].iter().enumerate() {
        store
            .append(
                SHARD,
                Command::StringSet {
                    key: format!("tenant/1/object/{index:08}"),
                    value: incompressible(*value_bytes, index as u64),
                },
            )
            .unwrap();
    }
    let mut wal_stream = Vec::new();
    for (_, raw) in store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap() {
        wal_stream.extend_from_slice(&raw);
    }
    assert!(!wal_stream.is_empty(), "the write-ahead corpus is empty");
    shapes.push(("wal, seven value widths".to_string(), wal_stream.clone()));

    // A real index log.
    let index_store = LocalIndexLogStore::new(&dir.path().join("index"));
    for index in 0..16 {
        index_store
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
    let mut index_stream = Vec::new();
    for (_, raw) in index_store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap() {
        index_stream.extend_from_slice(&raw);
    }
    assert!(!index_stream.is_empty(), "the index corpus is empty");
    shapes.push(("index log, sixteen deltas".to_string(), index_stream));

    // Nothing at all, and a stream that is only the reservation zeros.
    shapes.push(("empty".to_string(), Vec::new()));
    shapes.push(("reservation zeros only".to_string(), vec![0u8; 512]));

    // Records, then the zeros a preallocation leaves.
    let mut padded = wal_stream.clone();
    padded.extend_from_slice(&[0u8; 512]);
    shapes.push(("records then reservation zeros".to_string(), padded));

    // A tail torn at every offset inside the last record, plus one torn mid-varint and one torn
    // mid-header.
    for cut in [1usize, 2, 3, 6, 20, 100] {
        if wal_stream.len() > cut {
            let mut torn = wal_stream.clone();
            torn.truncate(wal_stream.len() - cut);
            shapes.push((format!("wal torn {cut} bytes short"), torn));
        }
    }

    // A varint that never terminates: eleven continuation bytes is longer than a u64 can be.
    let mut overlong = vec![crate::log_framing::FRAME_MAGIC_V3];
    overlong.extend_from_slice(&[0xffu8; 12]);
    shapes.push(("overlong varint".to_string(), overlong));

    // A frame declaring far more payload than the stream holds.
    let mut lying = vec![crate::log_framing::FRAME_MAGIC_V3];
    lying.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]); // a large varint
    lying.extend_from_slice(&[0u8; 4]);
    lying.extend_from_slice(b"only a few bytes");
    shapes.push(("frame declares more than it holds".to_string(), lying));

    // The delimited frame, which the reader still takes: a log written with the frame flag off.
    let text = crate::log_framing::encode_line(b"{\"s\":1,\"q\":1}");
    let mut text_stream = text.clone();
    text_stream.extend_from_slice(&crate::log_framing::encode_line(b"{\"s\":1,\"q\":2}"));
    shapes.push(("two delimited records".to_string(), text_stream));

    // A delimited record that never reached its delimiter.
    let mut unterminated = text.clone();
    unterminated.truncate(text.len() - 1);
    shapes.push(("delimited, no delimiter".to_string(), unterminated));

    shapes
}

/// THE READER HANDS BACK THE SAME BYTES AND STOPS IN THE SAME PLACE.
///
/// Reading too LITTLE on a log is silent data loss on recovery, so the assertion is the strong
/// one: the sequence of records, element by element, and the byte position the walk ended at --
/// not the count, which a reader that dropped one record and split another would satisfy.
#[test]
fn the_framed_reader_returns_what_the_previous_one_returned_over_every_shape() {
    let shapes = reader_corpus();
    assert!(
        shapes.len() >= 14,
        "the corpus collapsed to {} shapes; this guard would pass over almost nothing",
        shapes.len()
    );
    let mut records_compared = 0usize;
    for (label, bytes) in shapes.iter() {
        let (new_records, new_consumed, new_errored) = walk_new(bytes);
        let (old_records, old_consumed, old_errored) = walk_old(bytes);
        assert_eq!(
            new_records.len(),
            old_records.len(),
            "{label}: the reader produced {} records where the previous one produced {}",
            new_records.len(),
            old_records.len()
        );
        for (index, (new, old)) in new_records.iter().zip(old_records.iter()).enumerate() {
            assert_eq!(
                new, old,
                "{label}: record {index} differs between the two readers"
            );
            records_compared += 1;
        }
        assert_eq!(
            new_consumed, old_consumed,
            "{label}: the two readers stopped at different byte positions"
        );
        assert_eq!(
            new_errored, old_errored,
            "{label}: one reader reported an error and the other did not"
        );
    }
    assert!(
        records_compared >= 20,
        "only {records_compared} records were actually compared; a guard that compares nothing \
         passes"
    );
}

/// The comparison above reports a difference when one is put there.
///
/// Without this the whole equality claim rests on two walks over bytes that might both be empty.
#[test]
fn the_reader_comparison_reports_a_difference_when_one_is_injected() {
    let shapes = reader_corpus();
    let (_, wal) = shapes
        .iter()
        .find(|(label, _)| label.starts_with("wal, seven"))
        .expect("the write-ahead shape");
    let (records, consumed, _) = walk_new(wal);
    assert!(records.len() >= 2, "need at least two records to injure one");

    // One byte changed inside the second record's payload: the readers hand back bytes, so a
    // payload difference is exactly what a broken reader would show.
    let mut injured = wal.clone();
    let at = records[0].len() + records[1].len() - 1;
    injured[at] ^= 0xff;
    let (injured_records, _, _) = walk_new(&injured);
    assert_ne!(
        injured_records[1], records[1],
        "a flipped payload byte did not change what the reader returned, so the equality test \
         above is comparing nothing"
    );

    // And a difference in WHERE the walk stops, which the byte comparison alone would miss.
    let mut short = wal.clone();
    short.truncate(consumed - 1);
    let (_, short_consumed, _) = walk_new(&short);
    assert_ne!(
        short_consumed, consumed,
        "truncating the stream did not move the position the walk ended at"
    );
}

/// THE TWO INDEX-LOG PAYLOAD ENCODERS PRODUCE THE SAME BYTES.
///
/// Two live encoders now write this log: the delta append encodes into a buffer it reuses, and the
/// whole-index and anchor appends still build a fresh vector. A guard covering one of two live
/// copies lets the other keep the bug, so this holds them against each other -- over every record
/// shape, on the bytes, not on a length.
#[test]
fn the_two_index_payload_encoders_agree_byte_for_byte() {
    let shapes = index_record_shapes();
    assert!(
        shapes.len() >= 8,
        "only {} record shapes; this guard would pass over almost nothing",
        shapes.len()
    );
    let mut compared = 0usize;
    let mut compressed_shapes = 0usize;
    for (label, record) in shapes.iter() {
        for shape in [
            crate::index_log::INDEX_LOG_SHAPE_DELTA,
            crate::index_log::INDEX_LOG_SHAPE_WHOLE,
        ] {
            let fresh = crate::index_log::encode_index_payload(record, shape).expect("encode");
            let mut reused = Vec::new();
            crate::index_log::encode_index_payload_into(record, shape, &mut reused)
                .expect("encode");
            assert_eq!(
                fresh, reused,
                "{label}: the two encoders disagree at shape {shape}"
            );
            if fresh.len() > 256 {
                compressed_shapes += 1;
            }
            compared += 1;
        }
        // The reused buffer must also survive being reused: a second record encoded into the same
        // buffer has to come out the same as the first one did into a clean one.
        let mut reused = Vec::new();
        crate::index_log::encode_index_payload_into(
            &one_delta_record(7),
            crate::index_log::INDEX_LOG_SHAPE_DELTA,
            &mut reused,
        )
        .expect("encode");
        crate::index_log::encode_index_payload_into(
            record,
            crate::index_log::INDEX_LOG_SHAPE_DELTA,
            &mut reused,
        )
        .expect("encode");
        let fresh =
            crate::index_log::encode_index_payload(record, crate::index_log::INDEX_LOG_SHAPE_DELTA)
                .expect("encode");
        assert_eq!(
            fresh, reused,
            "{label}: a record encoded into a buffer that already held another one differs"
        );
        compared += 1;
    }
    assert!(
        compared >= 24,
        "only {compared} comparisons were made; a guard that compares nothing passes"
    );
    assert!(
        compressed_shapes > 0,
        "no shape reached the compression floor, so the compressed arm of the encoder was never \
         compared and this guard is blind to exactly the branch that rewrites the buffer"
    );
}

/// The encoder comparison reports a difference when one is put there.
#[test]
fn the_encoder_comparison_reports_a_difference_when_one_is_injected() {
    let record = one_delta_record(3);
    let mut altered = record.clone();
    altered.sequence += 1;
    let a = crate::index_log::encode_index_payload(&record, crate::index_log::INDEX_LOG_SHAPE_DELTA)
        .expect("encode");
    let b =
        crate::index_log::encode_index_payload(&altered, crate::index_log::INDEX_LOG_SHAPE_DELTA)
            .expect("encode");
    assert_ne!(
        a, b,
        "two different records encoded to the same bytes, so the equality test above is comparing \
         nothing"
    );
    // And the shape byte must reach the output, or the two-shape loop above is one comparison
    // repeated.
    let whole =
        crate::index_log::encode_index_payload(&record, crate::index_log::INDEX_LOG_SHAPE_WHOLE)
            .expect("encode");
    assert_ne!(a, whole, "the record shape did not reach the container byte");
}

/// Record shapes the index log actually writes, including ones past the compression floor.
fn index_record_shapes() -> Vec<(String, crate::index_log::IndexDeltaRecord)> {
    let mut shapes = Vec::new();
    shapes.push(("no items".to_string(), {
        let mut record = one_delta_record(0);
        record.items.clear();
        record
    }));
    shapes.push(("one item".to_string(), one_delta_record(1)));
    shapes.push(("eight items".to_string(), {
        let mut record = one_delta_record(2);
        record.items = (0..8).map(index_item).collect();
        record
    }));
    // Past the 256-byte compression floor, and compressible: the arm that rebuilds the buffer.
    shapes.push(("sixty-four items, compressible".to_string(), {
        let mut record = one_delta_record(3);
        record.items = (0..64).map(index_item).collect();
        record
    }));
    // Past the floor and NOT compressible, so the encoder declines and writes the packed form.
    shapes.push(("one item, incompressible key".to_string(), {
        let mut record = one_delta_record(4);
        let noise = incompressible(600, 99);
        let key: String = noise.iter().map(|byte| (b'a' + (byte % 26)) as char).collect();
        record.items[0].object_key = key.as_str().into();
        record.items[0].block_ref_key = key;
        record
    }));
    shapes.push(("a deleted item".to_string(), {
        let mut record = one_delta_record(5);
        record.items[0].deleted = true;
        record
    }));
    shapes.push(("an upsert record with a shared key".to_string(), {
        let mut record = one_delta_record(6);
        record.upsert = true;
        record.shared_object_key = Some("tenant/1/object/00000006".to_string());
        record
    }));
    shapes.push(("a record carrying an anchor".to_string(), {
        let mut record = one_delta_record(7);
        record.meta = Some(crate::index_log::MetaItem {
            version: 2,
            start_wal_sequence: 99,
            timestamp_ms: 12345,
            slabs: Vec::new(),
            slab_version: 4,
        });
        record.applied_wal_sequence = Some(77);
        record
    }));
    shapes.push(("a record carrying key states".to_string(), {
        let mut record = one_delta_record(8);
        record.key_states = vec![serde_json::json!({"k": "tenant/1/object/00000008", "v": [1,2,3]})];
        record
    }));
    shapes
}

/// WHAT A REPLAY APPLIES, ELEMENT BY ELEMENT.
///
/// Decoding too LITTLE on a log is silent data loss on recovery and decoding too much is merely
/// slow, so the assertion is on the SEQUENCE of records a replay would apply -- each one against
/// the control -- and not on how many there were.
#[test]
fn a_replay_applies_the_same_sequence_of_records_as_the_previous_reader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(&dir.path().join("wal"));
    let mut expected_keys = Vec::new();
    for index in 0..250usize {
        let key = format!("tenant/1/object/{index:08}");
        expected_keys.push(key.clone());
        // A mixed workload: values either side of the varint boundary, deletes, and a value
        // carrying the byte a delimited reader would have split on.
        let command = match index % 4 {
            0 => Command::StringSet {
                key,
                value: incompressible(index % 300, index as u64),
            },
            1 => Command::StringSet {
                key,
                value: b"a value with a \n newline and a \x1b escape inside".to_vec(),
            },
            2 => Command::StringDelete { key },
            _ => Command::StringSet {
                key,
                value: incompressible(1_500, index as u64),
            },
        };
        store.append(SHARD, command).unwrap();
    }

    // What replay walks: the production scan, decoded, in log order.
    let replayed = store.scan_decoded(SHARD, 0, u64::MAX, u64::MAX).unwrap().0;

    // The control: the previous reader over the same bytes, decoded the same way.
    let mut stream = Vec::new();
    for (_, raw) in store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap() {
        stream.extend_from_slice(&raw);
    }
    let (control_raws, _, _) = walk_old(&stream);
    let control: Vec<crate::wal::WriteAheadLogRecord> = control_raws
        .iter()
        .map(|raw| {
            crate::wal::decode_wal_line(crate::log_framing::record_body(raw.as_slice())).unwrap()
        })
        .collect();

    assert_eq!(
        replayed.len(),
        control.len(),
        "replay walked {} records and the control walked {}",
        replayed.len(),
        control.len()
    );
    assert!(
        replayed.len() >= 250,
        "only {} records replayed; the fixture did not produce a log to compare",
        replayed.len()
    );
    for (index, ((_, applied), expected)) in replayed.iter().zip(control.iter()).enumerate() {
        assert_eq!(
            applied, expected,
            "record {index} of the replay differs from the control"
        );
    }
    // Sequences ascend with no holes, which is the property a reader that skipped a record would
    // break while every record it DID return still compared equal.
    for (index, (_, record)) in replayed.iter().enumerate() {
        assert_eq!(
            record.sequence,
            index as u64 + 1,
            "replay record {index} carries sequence {}",
            record.sequence
        );
