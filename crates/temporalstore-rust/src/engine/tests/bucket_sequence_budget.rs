// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SEQUENCES ON EVERY BUCKET NODE: WHAT EACH ONE IS, AND WHETHER IT HAS TO BE THERE.
//!
//! THERE ARE THREE. There were four when this module was written, and the fourth is the reason the
//! heading changed: `last_dump_sequence` is gone from the node. This module's own item 6 below
//! reported that the node's copy did not reach the dump ordering the comment on it claimed, and
//! left the behaviour alone. Following that report to its end found that it did not reach anything
//! else either -- a compiler enumeration over the field's every use gives nine production
//! positions, and every one of them is the declaration, the wire, the rebuild that preserves it, the
//! single write from `manifest.wal_sequence`, or one of two reports. Not one is a branch, a
//! comparison or a sort key. So it was one shard-level watermark stored once per routing bucket, at
//! eight bytes each, and the reports take the figure from the newest dump manifest now, exactly as
//! `BucketStorageSummary` always did. The node is 168 bytes.
//!
//! Everything below is about the three that stay. Where a row read "one of the four" it now reads
//! "one of the three", and the narrowing table is re-derived rather than shifted: it loses its last
//! row and its base drops by a word.
//!
//! #1958 accounted for every byte of `BucketNode` and put four `u64` sequences in the
//! eight-aligned group -- 32 of the node's 184 bytes, once per routing bucket, more than a sixth
//! of the widest per-item structure in the engine. It declined them in one clause, "the sequences
//! are unbounded counters", which is a claim about their RANGE and not a measurement of it.
//!
//! (The node was 192 when this was written and is 184 now: the `BlockAddress` inside the inline
//! page entry stopped storing a `generation` it could derive. The sequences did not move.)
//!
//! THE ANSWER IS THAT THE THREE THAT REMAIN STAY, at 64 bits, on every key. This module is the
//! accounting for that, and every row of it is a number rather than a reading of the code.
//!
//! THE THREE, AND THEY ARE NOT THREE OF A KIND:
//!
//!   * `dirty_generation` -- a per-bucket COUNT, not a log position. WRITTEN by
//!     `saturating_add(1)` at six sites (`mark_object_dirty` and five delete/expire paths)
//!     and held at a captured value by a dump; never reset. STORED, as the `dirty_generation`
//!     key, and carried across `rebuild_bucket_block_ownership` deliberately. READ by
//!     `bucket_dump_summary_matches_current_generation`, which compares it for EQUALITY against
//!     the value a dump manifest recorded -- that equality is what lets WAL reclaim anchor on the
//!     manifest.
//!   * `first_dirty_wal_sequence` -- a position in the write-ahead log: where the bucket's OLDEST
//!     undumped write sits. `#[serde(skip)]`. WRITTEN at two sites, the single-command path in
//!     `engine.rs` and the batch path in `stream_batch_methods.rs`, each only when the field is 0,
//!     and cleared to 0 by a durable dump. READ as the dump ordering's primary key
//!     (`first_dirty_rank`) and as the reclaim plan's WAL floor.
//!   * `first_dirty_index_log_sequence` -- the same claim against the index log, on a different
//!     clock. `#[serde(skip)]`, the same two write sites, cleared by the same dump. READ as the
//!     reclaim plan's index-log floor.
//!   * and the one that left: `last_dump_sequence` -- the WAL sequence of the newest dump manifest
//!     that covered this bucket. It was STORED and WRITTEN only by
//!     `clear_dumped_bucket_dirty_state`, for the buckets a manifest names, and READ only into two
//!     reports. It is not on the node any more; see the head of this module.
//!
//! WHAT WAS MEASURED, AND WHAT IT SAYS:
//!
//!   1. THE ALIGNMENT ARITHMETIC IS NOT THE OBVIOUS ONE. The node is 166 bytes of field in 168 --
//!      160 of eight-aligned field and a six-byte tail rounded to eight, two bytes of slack.
//!      The natural reading of three words in the eight-aligned group is that narrowing one buys
//!      nothing, and at a six-byte tail that reading is now RIGHT -- but not for its reason,
//!      and it was wrong when this was written. The staircase is that every SECOND narrowing
//!      collects one rounding: a freed `u64` takes a word off the group and puts a `u32` back
//!      into the tail. Its PHASE depends on the tail. At ten bytes the first narrowing landed on
//!      14, which still rounded to 16, so the first step was worth eight. At SIX bytes -- the
//!      five `bool` are one `BucketFlags` byte since the node went 184 -> 176 -- the first
//!      narrowing lands on 10, which rounds to 16, and the word is handed straight back. The
//!      table is now 0, 8, 8 where it read 8, 8, 16, 16 over four sequences, and the eight bytes
//!      that left the head of it are the same eight the flag pack collected: two ways to take one
//!      rounding, and only one of them could be paid. REMOVING is different -- the bytes leave
//!      rather than move and the tail does not shift -- which is exactly how
//!      `last_dump_sequence` leaving took the node from 176 to 168 where narrowing it would have
//!      taken nothing.
//!      `what_narrowing_or_removing_each_sequence_would_make_the_node` prints the table against
//!      mirrors of the declaration, with the live mirror as its control and the reconstruction
//!      asserted on every row. So the saving belongs to the GROUP and there is no per-field
//!      saving to quote.
//!   2. NONE OF THE THREE CAN NARROW, and the measured ranges are exactly the trap. Over a real
//!      workload at two corpus sizes and two routing ranges the largest value any of them reached
//!      was 106 -- seven bits. The bound is not the corpus, it is the STORE'S LIFETIME: two of
//!      the three are positions in logs that are reclaimed but never renumbered, and the third is
//!      a count a load preserves and nothing resets. Each of the two claims is read as a retain
//!      FLOOR, and a wrapped claim is not a wrong number but a SMALLER one, so the floor falls
//!      and reclaim frees records the bucket still holds the log for; saturation is wrong in the
//!      other direction, naming a write newer than the one the bucket needs.
//!      `the_bound_on_each_sequence_is_the_stores_lifetime_not_the_corpus` drives both.
//!      The compiler was asked what a narrowing would touch: ten sites, five of them in
//!      production and two of those the write sites that would each need a checked conversion.
//!   3. THE INDEX-LOG CLAIM IS NOT DERIVABLE FROM THE WAL CLAIM, and the measurement that settles
//!      it is not the one that looks decisive. Every bucket holding BOTH halves holds them
//!      exactly one apart, at both corpus sizes -- which reads as a function. It is not one:
//!      582 of 1,024 buckets at 40,000 records hold the WAL half and NO index-log half, and all
//!      582 are exactly the buckets the reclaim plan refuses on. The missing half is not a value
//!      to reconstruct, it is the absence of a statement, and deriving it would hand those
//!      buckets a floor they have no grounds for. The control on the explanation: the two logs'
//!      own tails sit 38 apart at that corpus size, so the distance of one between the claims is
//!      a property of the write that stamped both, not of the clocks.
//!   4. HOISTING THE TWO TRANSIENT CLAIMS OFF THE NODE IS A LOSS AT THE CONFIGURED RANGE, which
//!      is the reading that decides it. It is worth a real 16 bytes a bucket and costs a side-map
//!      entry per DIRTY bucket, so it turns on the dirty fraction -- and that fraction is a
//!      property of the routing range and the dump cap, not of the workload. On `load_shard`'s
//!      `u32::MAX` default every key lands in a bucket of its own, half the buckets are clean
//!      after a round, and the hoist saves 5.0 B a record. On `TS_SHARD_END_ROUTING_BUCKET=1023`,
//!      the range `docs/runtime_tuning.md` tells an operator to set, the buckets fill, the dump
//!      cap of 64 a round cannot reach 1,024 of them, 99.0% and 100.0% of buckets are dirty, and
//!      the hoist COSTS 0.79 and 0.16 B a record -- and adds allocations at every range,
//!      including the one where it saves bytes. Both sides on the counting allocator, both
//!      figures in bytes and allocations, because those two have disagreed in sign on this
//!      structure's neighbours before.
//!   5. AND THE READ PATH IS THE OTHER HALF OF THAT TRADE. Today both claims are fields of a node
//!      the reclaim plan already has in hand; hoisted, each read becomes a descent of a second
//!      ordered map. Measured over the same buckets in the same order, ABBA: 46x.
//!   6. ONE DISAGREEMENT, REPORTED AND NOW FIXED. The node's `last_dump_sequence` did not reach
//!      the dump ordering that was commented on it. The sort reads
//!      `BucketStorageSummary::last_dump_sequence` -- a different field with the same name --
//!      and `bucket_storage_summaries` never fills that from the node; `merge_last_dump_sequence`
//!      fills it from the newest manifest's INDEX-LOG sequence, the same value for every bucket
//!      that manifest names and 0 for every bucket it does not. So the key separates "covered by
//!      the newest dump" from "not covered by it", which was not what it was described as doing.
//!      This module left the behaviour alone and corrected the comment. Following the report to
//!      its end found the node's copy reached NO decision at all, and it is gone; the reports take
//!      the figure from the manifest, and
//!      `the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node` now pins that
//!      the report does too, with an uncovered bucket as the control.
//!
//!   7. A BOUNDED RING IS SAFE IN ONE DESIGN AND LOSES DATA IN THE OTHER, AND THE DIFFERENCE IS
//!      WHETHER IT ALSO OWNS THE DIRTINESS. The two transient claims are `#[serde(skip)]` and
//!      cleared on load, so they are runtime-only -- which is exactly what a shard-level side
//!      structure could hold instead of sixteen bytes on every routing bucket, and a ring of slot
//!      ids is the bounded shape such a structure takes. A ring that has wrapped has FORGOTTEN a
//!      claim, and a claim is a retain FLOOR, so the wrap was DRIVEN rather than reasoned about.
//!      48 buckets at distinct claims, ring capacity 24, the oldest 24 claims evicted -- which is
//!      not an unlucky order, it is the order a ring has, and the oldest claims are the floor:
//!
//!      ```text
//!        CONTROL                                safe=true   retain_from_wal=3   missing=0
//!        ARM A  ring holds claims only          safe=FALSE  retain_from_wal=0   missing=24
//!        ARM B  ring IS the dirty set           safe=true   retain_from_wal=27  missing=0
//!      ```
//!
//!      ARM A retains the WHOLE log: a bucket that is still known dirty and can no longer name its
//!      claim reads as NO CLAIM RECORDED, the plan refuses, and the floor cannot rise. That errs
//!      CONSERVATIVE and is safe to take. ARM B moves the floor from 3 to 27, twenty-four records
//!      PAST the lowest forgotten claim, and reclaim would free WAL records a bucket still needs.
//!      That is committed data lost, and the design is refused.
//!      `a_wrapped_claim_ring_errs_conservative_only_while_it_keeps_dirtiness` drives both, with
//!      the control taken first on the same shard and the fixture's preconditions ENFORCED --
//!      including that the ring forgot claims OLDER than the ones it kept, which a first version
//!      of the fixture did not satisfy: batched writes stamp ONE sequence on every bucket the batch
//!      touched, 694 buckets all claiming 3, and forgetting half of an identical set moves nothing.
//!   8. AND WHAT THE PER-KEY CLAIM BUYS THAT A SINGLE WATERMARK CANNOT IS THE DUMP ORDER, NOT THE
//!      FLOOR. A shard-level watermark over the whole index is the minimum of these claims, which
//!      is the same floor the plan already computes, so the floor is not the argument. The argument
//!      is `first_dirty_rank`, the dump ordering's PRIMARY key: it reads each bucket's own claim to
//!      dump the bucket that is pinning the log FIRST, so the floor advances. A structure that
//!      holds one number for the shard cannot answer "which bucket", and the ordering falls back to
//!      what it used before -- which starved a bucket dirtied once behind low-id buckets re-dirtied
//!      every round, and is what
//!      `dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id` exists for.
//!      So all three sequences stay, and each is named with the reader that pins it above.
//!
//! HOW THE "IS IT READ" GUARDS WORK. Each one PERTURBS the field on a live shard and asserts the
//! decision it is named for changes, against a control taken on the same shard before the edit. A
//! guard that only reads the field back proves the field exists; these prove it is load-bearing,
//! and a mutation that stops a reader reading it turns them red.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It proposes no FURTHER change to `BucketNode`: the three
//! sequences that remain all stay, at 64 bits, on every key, and the only production edit made
//! alongside item 7 is a comment -- two words of one restored from the commit that wrote it, where
//! a doc comment had lost them to a bare backslash.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};

use crate::engine::state::{
    BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

const SEQ_SHARD: ShardId = 1;

/// The documented production routing range: `TS_SHARD_END_ROUTING_BUCKET=1023`, 1,024 buckets.
///
/// `docs/runtime_tuning.md` prints this in the worked configuration an operator is told to copy.
/// Measuring only at `load_shard`'s `u32::MAX` default puts every key in a bucket of its own by
/// construction, which is the shape #1959's fixture had and #1962's premise died on.
const CONFIGURED_END_BUCKET: u32 = 1023;

/// The dump cap an operator actually runs with: `default_storage_manager_max_dump_buckets_per_round`.
///
/// Not zero. Zero means "dump everything", which is what the existing claim fixtures use to clear
/// state deliberately, and it is the one setting under which the dirty fraction can reach zero.
const CONFIGURED_DUMP_CAP: usize = 64;

fn seq_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-blocks")),
        dir.join(format!("{name}-indexes")),
    )
}

fn load_at(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: SEQ_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

fn seq_keys(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index:06}"))
        .collect()
}

/// Write through the BATCH path, which is the one that stamps both claims in `stream_batch_methods`.
fn write_batches(engine: &TemporalEngine, keys: &[String], fill: u8) {
    for chunk in keys.chunks(512) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![fill; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SEQ_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
}

/// Write through the SINGLE-COMMAND path, which stamps them in `engine.rs`.
fn write_singles(engine: &TemporalEngine, keys: &[String], fill: u8) {
    for key in keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SEQ_SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![fill; 64],
            },
        });
        assert!(response.status.ok, "write {key} failed: {:?}", response.status);
    }
}

/// One storage round at the CONFIGURED dump cap, not at "dump everything".
fn one_round_at_the_configured_cap(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SEQ_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: CONFIGURED_DUMP_CAP,
        ..StorageManagerCycleRequest::default()
    });
}

/// The sequences of one bucket, plus the two flags that say which branch reads them.
///
/// THREE, not four. `last_dump_sequence` is not a field of the node any more, so it cannot be read
/// per bucket here: the figure lives on the newest dump manifest and the reports take it from
/// there.
#[derive(Clone, Copy, Debug)]
struct Sequences {
    routing_bucket: u32,
    dirty: bool,
    generation: u64,
    wal_claim: u64,
    index_log_claim: u64,
}

/// Read straight off the bucket index, PER BUCKET and never reduced here.
fn sequences_by_bucket(engine: &TemporalEngine) -> Vec<Sequences> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&SEQ_SHARD) else {
        return Vec::new();
    };
    let mut out = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| Sequences {
            routing_bucket: *routing_bucket,
            dirty: bucket.dirty(),
            generation: bucket.dirty_generation,
            wal_claim: bucket.first_dirty_wal_sequence,
            index_log_claim: bucket.first_dirty_index_log_sequence,
        })
        .collect::<Vec<_>>();
    out.sort_by_key(|row| row.routing_bucket);
    out
}

/// min / max / how many are non-zero, over one of the three.
fn spread(values: impl Iterator<Item = u64>) -> (u64, u64, usize, usize) {
    let mut total = 0usize;
    let mut non_zero = 0usize;
    let mut low = u64::MAX;
    let mut high = 0u64;
    for value in values {
        total += 1;
        if value > 0 {
            non_zero += 1;
            low = low.min(value);
            high = high.max(value);
        }
    }
    if non_zero == 0 {
        low = 0;
    }
    (low, high, non_zero, total)
}

/// Seed a shard to the state a RUNNING one is in: written, one round of dumping at the configured
/// cap, written again. Returns the rows.
fn seeded_rows(
    dir: &std::path::Path,
    name: &str,
    records: usize,
    end_routing_bucket: u32,
) -> (Vec<Sequences>, usize) {
    let engine = seq_engine(dir, name);
    load_at(&engine, end_routing_bucket);
    let first = seq_keys("first", records / 2);
    let later = seq_keys("later", records / 2);
    write_batches(&engine, &first, 118);
    // A handful through the single-command path as well, so both stamping sites are exercised
    // rather than only the batch one.
    write_singles(&engine, &first[..16.min(first.len())], 119);
    one_round_at_the_configured_cap(&engine);
    write_batches(&engine, &later, 120);
    let rows = sequences_by_bucket(&engine);
    (rows, records)
}

// -------------------------------------------------------------------------------------------
// 1. WHAT EACH OF THE THREE ACTUALLY REACHES.
// -------------------------------------------------------------------------------------------

/// WHAT EACH OF THE THREE SEQUENCES REACHES, at two corpus sizes and two routing ranges.
///
/// The routing range is the second axis on purpose. At `load_shard`'s `u32::MAX` default a key
/// lands in a bucket of its own, so a bucket's `dirty_generation` counts the writes to ONE key;
/// on the range `docs/runtime_tuning.md` configures, a bucket holds many keys and counts all of
/// them. Those are different distributions and only one of them is what an operator runs.
///
/// THE FIXTURE IS ASSERTED TO REACH THE CASES THIS MODULE CLAIMS. A shard where every bucket is
/// dirty cannot say anything about a cleared claim, and a shard where none is dirty cannot say
/// anything about a live one. Both populations are required to be non-empty before any figure is
/// read off, and both denominators are asserted before anything is divided by them.
#[test]
#[ignore = "seeds four shards up to 40,000 records; run by name"]
fn what_each_of_the_three_sequences_reaches_at_two_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut dirty_fractions: Vec<(String, f64)> = Vec::new();

    for (records, end_routing_bucket, range_label) in [
        (8_000usize, u32::MAX, "default range (load_shard's u32::MAX)"),
        (8_000usize, CONFIGURED_END_BUCKET, "0..1023 (the configured range)"),
        (40_000usize, u32::MAX, "default range (load_shard's u32::MAX)"),
        (40_000usize, CONFIGURED_END_BUCKET, "0..1023 (the configured range)"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let (rows, seeded) = seeded_rows(dir.path(), "seq", records, end_routing_bucket);

        assert!(
            !rows.is_empty(),
            "denominator: {seeded} records produced no routing bucket at all, so every spread \
             below is over an empty set"
        );
        let dirty = rows.iter().filter(|row| row.dirty).count();
        let clean = rows.len() - dirty;

        println!("\n=== {seeded} records, {range_label} ===");
        println!(
            "  buckets={} dirty={dirty} clean={clean} ({:.1}% dirty)",
            rows.len(),
            100.0 * dirty as f64 / rows.len() as f64
        );
        for (name, low, high, non_zero, total) in [
            {
                let (low, high, nz, total) = spread(rows.iter().map(|row| row.generation));
                ("dirty_generation", low, high, nz, total)
