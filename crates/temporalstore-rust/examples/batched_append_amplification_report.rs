// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE APPEND COSTS WHEN IT TRAVELS WITH OTHERS, per entry.
//!
//! # THE QUESTION
//!
//! One append of one entry costs a fixed envelope over the user's bytes, and at a small value that
//! envelope IS the amplification: an eight-byte value carries it whole. `publish_wal_entries_batch`
//! makes `N` entries share exactly TWO object-store appends instead of `2N`, so the obvious hope is
//! that it divides the envelope by `N`.
//!
//! IT DOES NOT, and this program is how that is established rather than argued. The envelope is
//! charged PER FRAME, and batching changes how many appends carry the frames, not how many frames
//! there are. Each entry still gets its own log frame and its own sidecar frame, so the per-entry
//! byte cost of a batch of two hundred and fifty-six is the per-entry byte cost of a batch of one.
//!
//! Measured, at an 8-byte value: 64.0 B per entry at `N = 1` and 70.0 B at `N = 256`. Flat, and
//! what movement there is goes the WRONG WAY -- see below.
//!
//! # IT IS NOT QUITE IDENTICAL EITHER, AND THE DIFFERENCE HAS NO STABLE SIGN
//!
//! The two paths write slightly different bytes for the same entries, because they fill the
//! sidecar's offset-shaped fields with different NUMBERS: a batch records the object length after
//! the whole batch where separate appends record it as it stood at their own append, and several
//! of those fields are varints whose width follows the value.
//!
//! The difference is a fraction of a percent and its SIGN IS NOT A PROPERTY OF BATCHING. Measured
//! at a 256-entry batch of 8-byte values it was +0.40% when the log frame carried a 32-byte digest
//! and -0.70% once that digest became a CRC32C. An earlier version of this program asserted the
//! sign from a single measurement and named the object-length field as the cause; narrowing the
//! frame flipped it. Several fields move together and this program does not separate them, so the
//! cause is NOT established here and the assertion is on magnitude rather than direction.
//!
//! That is the honest shape of the trade: batching buys appends and barriers, costs or saves a
//! handful of bytes per entry depending on the frame size, and does nothing whatsoever to the
//! envelope. It is still worth doing. It is just not an answer to the envelope.
//!
//! # WHAT BATCHING DOES AMORTISE, AND WHY THIS DENOMINATOR CANNOT SEE IT
//!
//! Two appends instead of `2N` is a real and large saving in the things an append costs besides
//! bytes: round trips, durability barriers, and one EXTENT per append in each object's metadata.
//! The published denominator sums `extent.length()` -- the DATA each extent holds -- so a hundred
//! extents holding a hundred bytes and one extent holding a hundred bytes are the same number.
//! That is not a flaw to be corrected by folding extent metadata into the ratio; it is a different
//! term. So the extent COUNT is reported beside the byte figure as its own column, and the
//! unbatched control is run at every point to show the counts actually diverge.
//!
//! # TWO SWEEPS, BECAUSE THERE ARE TWO VARIABLES
//!
//! `N` is swept at a fixed payload to test whether the per-entry cost depends on batch size, and
//! the payload is swept at a fixed `N` to fit `per_entry = rate * payload + envelope`. Quoting a
//! single ratio would hide whichever of the two was moving.
//!
//! The fitted line is `1.0010 * payload + 63.7` with a worst residual of 4.8 B, which puts the
//! per-entry ratio at 1.10x around a 637-byte value and 1.01x around a 7,100-byte one. Those
//! crossings are properties of the ENVELOPE, so they do not move with `N` either -- and they move
//! whenever the envelope does, which is why the program prints them rather than this comment
//! being the place they are quoted from.
//!
//! rust-internal: measures the shared store's own layout, no product behaviour

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, MatrixObjectObjectStore};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const KEY: &str = "amp-key";
const CLUSTER: &str = "cluster-batched-append";

/// What one run of `count` entries left in the shared WAL prefix.
struct Measured {
    /// key + value, summed over every entry the run published.
    user_bytes: u64,
    /// Sum of every extent's LENGTH over every object under the shared WAL prefix. The same
    /// numerator `one_append_amplification_report` uses, so the two are comparable.
    blob_bytes: u64,
    /// How many objects the prefix holds. Asserted, never assumed.
    objects: usize,
    /// How many extents those objects hold between them -- one per append that reached the store.
    /// This is what batching moves and what the byte figure cannot see.
    extents: usize,
}

/// Publish `count` entries of `payload` bytes each into a store of its own.
///
/// `batched` picks the path under test: one `publish_wal_entries_batch` call, or `count` separate
/// `publish_wal_entry` calls. Everything else is held identical so the difference between the two
/// is the path and nothing else.
async fn run(count: usize, payload: usize, batched: bool, options: &StoreOptions) -> Measured {
    let store =
        Arc::new(MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"));
    let replicator = SharedStoreReplicator::new(CLUSTER, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    let entries: Vec<SharedStoreWalEntry> = (0..count)
        .map(|at| SharedStoreWalEntry {
            shard_id: SHARD,
            wal_index: at as u64 + 1,
            command: Some(Command::StringSet {
                key: KEY.to_string(),
                value: vec![b'v'; payload],
            }),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        })
        .collect();
    let user_bytes = entries.len() as u64 * (KEY.len() + payload) as u64;

    if batched {
        let reports = replicator
            .publish_wal_entries_batch(&entries)
            .await
            .unwrap_or_else(|err| panic!("batch of {count} x {payload} B: {err:?}"));
        // The batch must have published every entry, or the byte total below is over fewer
        // entries than the divisor says.
        assert_eq!(
            reports.len(),
            count,
            "the batch reported {} entries for {count} published",
            reports.len()
        );
        assert!(
            reports.iter().all(|report| report.published),
            "the batch left an entry unpublished, so its bytes are not in this total"
        );
    } else {
        for entry in &entries {
            replicator
                .publish_wal_entry(entry.clone())
                .await
                .unwrap_or_else(|err| panic!("single append of {payload} B: {err:?}"));
        }
    }

    let prefix = format!("{CLUSTER}/shards/{SHARD}/shared/wal/");
    let objects = store.list(&prefix).await.expect("the wal listing");
    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");

    let mut blob_bytes = 0u64;
    let mut extents = 0usize;
    for key in &objects {
        if let Ok(object) = guard.get_object(BUCKET, key) {
            blob_bytes += object
                .metadata
                .extents
                .iter()
                .map(|extent| extent.length())
                .sum::<u64>();
            extents += object.metadata.extents.len();
        }
    }

    Measured {
        user_bytes,
        blob_bytes,
        objects: objects.len(),
        extents,
    }
}

/// Least squares, and the largest residual it leaves.
fn fit(points: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = points.len() as f64;
    let sx: f64 = points.iter().map(|p| p.0).sum();
    let sy: f64 = points.iter().map(|p| p.1).sum();
    let sxx: f64 = points.iter().map(|p| p.0 * p.0).sum();
    let sxy: f64 = points.iter().map(|p| p.0 * p.1).sum();
    let denom = n * sxx - sx * sx;
    let rate = if denom == 0.0 {
        0.0
    } else {
        (n * sxy - sx * sy) / denom
    };
    let envelope = (sy - rate * sx) / n;
    let worst = points
        .iter()
        .map(|p| (p.1 - (rate * p.0 + envelope)).abs())
        .fold(0.0f64, f64::max);
    (rate, envelope, worst)
}

/// Check a measurement is a measurement before any ratio is taken from it.
///
/// An empty listing sums to "0 bytes over 0 entries", which divides into something that reads like
/// a store writing nothing at all. So the denominator is asserted non-zero and the object count is
/// asserted to be the two objects this path writes.
fn assert_sound(measured: &Measured, count: usize, label: &str) {
    assert_eq!(
        measured.objects, 2,
        "{label}: {count} entries left {} objects under the shared WAL prefix, not the log and \
         its offset index. Every byte figure here is a sum over that listing, so a different \
         object count means this is measuring something else",
        measured.objects
    );
    assert!(
        measured.user_bytes > 0,
        "{label}: the user-byte denominator is zero, so any ratio over it is meaningless"
    );
    assert!(
        measured.blob_bytes >= measured.user_bytes,
        "{label}: {} stored bytes against {} user bytes -- an uncompressed append cannot store \
         less than it was given",
        measured.blob_bytes,
        measured.user_bytes
    );
}

#[tokio::main]
async fn main() {
    let options = StoreOptions {
        page_size: 1024,
        max_extent_bytes: 4096,
        chunk_size: 4096,
        replica_count: 3,
        write_quorum: 2,
        ..StoreOptions::default()
    };

    println!("WHAT A BATCHED APPEND COSTS, PER ENTRY");
    println!();
    println!(
        "  store: page_size {}  chunk_size {}  max_extent_bytes {}  replica_count {}  write_quorum {}",
        options.page_size, options.chunk_size, options.max_extent_bytes,
        options.replica_count, options.write_quorum
    );
    println!("  numerator: every extent's length over every object under");
    println!("             {{cluster}}/shards/{{shard}}/shared/wal/");
    println!("  denominator: user bytes -- key + value, summed over the batch");
    println!("  replication (x{}) is a separate multiplier and is NOT folded in", options.replica_count);
    println!();

    // ---- SWEEP ONE: does the per-entry cost depend on the batch size at all?
    const FIXED_PAYLOAD: usize = 8;
    let counts = [1usize, 2, 4, 16, 64, 256];
    println!("  SWEEP OVER BATCH SIZE, at a fixed {FIXED_PAYLOAD} B value");
    println!();
    println!("      N   user B   blob B   per-entry B   blob/user   extents   extents unbatched");

    let mut per_entry_by_count: Vec<f64> = Vec::new();
    for &count in &counts {
        let batched = run(count, FIXED_PAYLOAD, true, &options).await;
        assert_sound(&batched, count, "batched");
        let control = run(count, FIXED_PAYLOAD, false, &options).await;
        assert_sound(&control, count, "unbatched control");

        let per_entry = batched.blob_bytes as f64 / count as f64;
        per_entry_by_count.push(per_entry);
        println!(
            "    {:>3}   {:>6}   {:>6}   {:>11.1}   {:>9.2}   {:>7}   {:>17}",
            count,
            batched.user_bytes,
            batched.blob_bytes,
            per_entry,
            batched.blob_bytes as f64 / batched.user_bytes as f64,
            batched.extents,
            control.extents,
        );

        // ---- THE BYTES MUST NOT FALL WITH N. That is the claim. It is NOT a claim that the two
        // ---- paths write identical bytes, and it is NOT a claim about which of them is larger.
        // ----
        // ---- They do differ a little, because the two paths fill the sidecar's offset-shaped
        // ---- fields with different NUMBERS -- a batch records the object length after the whole
        // ---- batch where separate appends record it as it stood at their own append -- and
        // ---- several of those fields are varints whose width follows the value.
        // ----
        // ---- THE SIGN OF THAT DIFFERENCE IS NOT A PROPERTY OF BATCHING. Measured at a
        // ---- 256-entry batch it was +0.40% when a frame carried a 32-byte digest and -0.70%
        // ---- once the digest became a CRC32C. An earlier version of this program asserted the
        // ---- sign, from one measurement, with the object-length field named as the cause; the
        // ---- narrowing flipped it and the assertion caught that. The cause is NOT established
        // ---- here -- several fields move together and this program does not separate them.
        // ----
        // ---- What is asserted is that the difference stays SMALL, which is all the claim needs.
        // ---- Amortisation would not be small: it would divide the batched total by nearly N.
        let difference = batched.blob_bytes as i64 - control.blob_bytes as i64;
        let tolerance = 8 * count as i64;
        assert!(
            difference.abs() <= tolerance,
            "a batch of {count} stored {} B where {count} separate appends stored {} B, a \
             difference of {difference} B against a tolerance of {tolerance} B (eight per entry). \
             That is more than varint widths in the sidecar's offset fields can account for, so \
             some other term differs between the two paths -- find it before calling the \
             per-entry cost path-independent",
            batched.blob_bytes,
            control.blob_bytes
        );

        // ---- AND THE REGIME MUST HAVE BEEN REACHED. For N > 1 the batch has to have used fewer
        // ---- extents than the control, or no batching happened and this row is two identical
        // ---- runs agreeing with each other.
        if count > 1 {
            assert!(
                batched.extents < control.extents,
                "a batch of {count} used {} extents and {count} separate appends used {} -- the \
                 batch did not coalesce, so this row proves nothing about batching",
                batched.extents,
                control.extents
            );
        }
    }

    // ---- THE HEADLINE OF SWEEP ONE, as a spread rather than a pair of endpoints.
    let smallest = per_entry_by_count.iter().copied().fold(f64::MAX, f64::min);
    let largest = per_entry_by_count.iter().copied().fold(0.0f64, f64::max);
    println!();
    println!("  PER-ENTRY BYTES ARE FLAT IN THE BATCH SIZE: {smallest:.1} B at the cheapest N,");
    println!("  {largest:.1} B at the dearest, over N from {} to {}.", counts[0], counts[counts.len() - 1]);
    assert!(
        counts.len() >= 5 && counts[counts.len() - 1] >= 64 * counts[0],
        "the batch-size sweep spans too little for flatness in N to mean anything"
    );

    // ---- WHAT THIS TOLERANCE CANNOT SEE, said out loud rather than left implicit. The sidecar
    // ---- carries five offset-shaped fields -- start, end, bytes written, object length, first
    // ---- physical offset -- and each is a varint that gains a byte as the log grows past 127,
    // ---- then past 16383. Across this sweep that is a few bytes of unavoidable UPWARD drift per
    // ---- entry, and the bound has to admit it.
    // ----
    // ---- It is still an order of magnitude tighter than what it is guarding against: dividing
    // ---- an ~88 B envelope by the largest batch here would take per-entry bytes DOWN by ~87 B,
    // ---- not up by a handful. A tolerance that admitted that would be the wrong tolerance.
    assert!(
        largest - smallest < 10.0,
        "per-entry bytes moved from {smallest:.1} to {largest:.1} B across the batch-size sweep, \
         a spread of {:.1} B. That is more than varint widening in the sidecar's offset fields \
         can account for, so something else in the envelope DOES depend on N; find it before \
         quoting a flat figure",
        largest - smallest
    );

