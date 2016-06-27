// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT BRINGING A LARGE SHARD BACK COSTS, at 20,000 and 200,000 records, in both regimes.
//!
//! Every restore figure in this crate stops at 20,000 records. `restore_scale.rs` cuts a restart
//! into its five phases at 2,000 and 20,000 and reports the whole thing flat per record; this
//! file starts where that one stops and goes an order of magnitude past it. It also turns the
//! knob that decides more about a restart than its size does: HOW MUCH OF THE LOG IS UNREFLECTED
//! when the shard comes back.
//!
//! Two regimes, and they are different restores rather than two sizes of one:
//!
//!   * REFLECTED -- the shard dumped a checkpoint and then went away without taking another
//!     write, so nothing is past the checkpoint. The post-replay fold then does not run AT ALL:
//!     two allocations, at both corpus sizes. That is asserted exactly, because it is what makes
//!     this a second regime rather than a second fixture.
//!   * UNREFLECTED -- half the shard's writes landed after the last checkpoint.
//!
//! THE BILL, per stored record. 128-byte incompressible values, one checkpoint, three slabs with
//! a disjoint key range in each, two log pieces, and the store path 15 characters in every arm --
//! allocation counts move with the path length, so it is held equal and asserted:
//!
//! ```text
//!                              REFLECTED                     UNREFLECTED
//!                       20,000   200,000  ratio        20,000    200,000  ratio
//!   allocations          22.034    21.710  0.985        34.363     37.250  1.084
//!    manifest+base index 18.458    18.448  0.999         9.233      9.225  0.999
//!    publish_and_seed     3.109     3.107  0.999         3.109      3.107  0.999
//!    wal_replay           0.467     0.156  0.333         7.661     10.566  1.379
//!    index_fold          2 allocations, not a rate       14.360     14.352  0.999
//!   promotion-check pages 1.0000    1.0000  1.000        1.5000     1.5000  1.000
//!   manifest bytes read 190.28    191.23   1.005        93.535     95.421  1.020
//!   the log store's own
//!     bytes_read           0         0      --         150.892    150.905  1.000
//!   heap bytes not freed 897.6     833.0   0.928       837.3      808.5    0.966
//!   KERNEL rchar         431.06    396.96  0.921       757.44    5073.40   6.698
//!    of which no reader
//!    in this file claims 240.77    205.73  0.854       513.02    4827.08   9.409
//!   replay windows (n)       1         1                    6         59
//! ```
//!
//! WHAT IS FLAT. Everything the engine can see. Allocations per stored record in the reflected
//! regime, 22.03 -> 21.71. The checkpoint decode, 18.458 -> 18.448 and 9.233 -> 9.225. The seed,
//! 3.109 -> 3.107. The fold, 14.360 -> 14.352. The promotion check's walk, 1.0000 and 1.5000
//! live model-map pages per stored record to four decimal places -- which is the count the 4.0
//! allocations a page and 6.0 a stored record of #1884 and #1891 are rates over, and it has not
//! moved at ten times the store. Manifest bytes read, 190.28 -> 191.23. Two of the nine
//! allocation classes of #1922, 1.2309 -> 1.2289 and 1.0007 -> 1.0001. And the part of the span
//! no phase row accounts for: 8 or 9 allocations, at every size and in every regime, which is a
//! fixed edge cost and not a phase nobody wrote down.
//!
//! WHAT GROWS. Replay, and only replay -- 7.661 -> 10.566 allocations and 2,948 -> 11,183
//! allocated bytes per stored record, which carries the whole restore's allocated bytes from
//! 8,660 to 16,519 a record. And, far larger than any of it, what the KERNEL says the process
//! read.
//!
//! THE FINDING, AND IT IS ONLY VISIBLE FROM OUTSIDE THE ENGINE. Bringing back a 200,000-record
//! shard with half its writes past the checkpoint reads 1,014,680,664 bytes off disk -- a
//! gigabyte, to rebuild an 87.9 MB store. The log store's own counter says 30,180,893, the
//! manifest reader says 19,084,120, and nothing else in this engine claims a byte: 965,415,651
//! of that gigabyte is read by a reader with no counter on it. Per stored record the kernel's
//! number goes 757.44 -> 5,073.40 for ten times the store, and the unattributed part of it
//! 513.02 -> 4,827.08, while EVERY counter inside the engine reports the same restore as
//! perfectly flat -- the log store's own `bytes_read` moves 1.000.
//!
//! The mechanism is the replay WINDOW COUNT, which is the one thing that grew: 6 windows at
//! 20,000 records and 59 at 200,000, each re-reading the pieces behind it. `restore_scale.rs`
//! recorded the same shape at 2,000 and 20,000 from an `strace` and called it inherited and
//! bounded. At 200,000 it is 1.01 GB, it is 89% of what a restart reads, and it is paid at
//! exactly the moment a node is trying to return to service. Nothing here fixes it; what this
//! file adds is a number for it and a guard that fails if it stops being true.
//!
//! AND IN THE OTHER REGIME IT IS NOT THERE AT ALL. A shard dumped immediately before it went
//! away reads 396.96 bytes per stored record at 200,000 against 431.06 at 20,000 -- flat, and
//! slightly better. One window, no replay, no fold. A guard that measured only this regime would
//! report a healthy system at any size, which is why both are measured and the control arm is
//! asserted by name.
//!
//! WHAT A REFLECTED RESTART COSTS ANYWAY, which is the other half of the regime story: 21.710
//! allocations and 6,165 allocated bytes per stored record, 18.7 seconds and a peak of 1.60 GiB,
//! for a shard with NOTHING to recover. 85% of it is one phase -- decoding the checkpoint's
//! embedded whole-shard index image, 38,246,262 bytes of it, and re-serialising all 38,246,198
//! of them to verify a checksum. That cost is priced by the STORE while the work in front of it
//! is zero, which is the shape three of this campaign's findings already have.
//!
//! WALL TIME AND PEAK MEMORY -- the numbers nobody had. A 200,000-record shard comes back in
//! 18.7 s reflected and 23.8 s unreflected, holding a peak of 1,672,388 and 1,649,228 KiB. THE
//! BOX WAS LOADED: `/proc/loadavg` between 8.4 and 9.5 with four other build jobs on it, printed
//! beside every arm in the report. Those are UPPER BOUNDS and not clean figures. Every count in
//! the table above is not: three independent runs agreed to four decimal places on every
//! allocation column, and every comparison in this file rests on counts. The time is here
//! because a node's return to service is measured in seconds, and twenty-four of them is one
//! shard of 200,000 records.
//!
//! PEAK RSS UNDERSTATES A RESTORE and is reported anyway, beside something that does not: the
//! restore runs in a process that has just built the corpus, so the allocator serves most of it
//! out of memory it already holds. The span's allocated-minus-freed bytes are the same question
//! asked of a counter that cannot be fooled that way, and they are flat: 897.6 -> 833.0 and
//! 837.3 -> 808.5 bytes per stored record.
//!
//! THE CATEGORISED ACCOUNTING OF #1922 DOES NOT FIT THIS PATH, and the measured share says so:
//! 6.0% of a replaying restore's allocation calls and 2.4% of its bytes land in a named class,
//! and 0.0% of a reflected one's. Eight of the nine classes are charged inside WRITE primitives;
//! a restore's dominant costs -- decoding a checkpoint's index image, and rebuilding the bucket
//! index in the fold -- are inside none of them. The two classes it does enter, `bucket_index`
//! and `staged_outcome`, are reported above and are flat. Extending the classes to cover the
//! load path would be a change to nine production primitives and is not attempted here.
//!
//! THE RESIDUAL IS INDEPENDENT, and it is the only row here that could have caught the finding.
//! It is the kernel's `rchar` for this process across the `load_shard_with` call, minus the four
//! readers this file names -- never the sum of the rows it audits, which is a residual that
//! cannot fail. The instrument has its own control:
//! `the_kernel_byte_instrument_recovers_a_planted_read_exactly` plants 1,000,003 and 3,000,009
//! bytes in front of it and asserts it hands back exactly those numbers, and that an empty span
//! reads exactly zero, so a residual of zero provably comes from an instrument that can see a
//! hit. The instrument subtracts its own reading's bytes rather than estimating them, which is
//! what makes "exactly" available at all.
//!
//! WHAT STOPPED THIS AT 200,000: TIME, not disk. The two large corpora take 108.8 s and 88.0 s
//! to build and the four-arm run 330.7 s, on a box shared with four other threads; 43 GB of disk
//! were still free when it finished. A 2,000,000-record arm would build for something over
//! twenty minutes and then, on the window growth measured here, read on the order of a hundred
//! gigabytes to come back.
//!
//! No production code changes.

#![allow(clippy::all)]
// Most of this file only exists with `alloc-probe`, so its sizes and helpers go unread in an
// ordinary build.
#![allow(dead_code)]
use super::*;
use crate::alloc_probe::AllocClass;
use crate::engine::lifecycle::restore_phase_probe;
use crate::engine::storage_bucket_internals::{
    promote_model_map_check_counts, reset_promote_model_map_check_counts,
};

/// The size every existing restore figure stops at, kept as the small arm so this file starts
/// where `restore_scale.rs` ends.
const BASE: usize = 20_000;
/// An order of magnitude past it.
const BIG: usize = 200_000;
/// What the two ordinary-gate guards build. Large enough to roll more than one log piece and
/// more than one slab, small enough to belong in a gate.
const GATE: usize = 20_000;
const VALUE_BYTES: usize = 128;
/// Records per seeded batch, and therefore per LOG RECORD: the walk decodes batches, not keys.
const BATCH: usize = 100;
/// Spread across the whole key space, so a restore that recovered only the dumped half fails
/// here rather than being reported as a cheap recovery.
const PROBE_READS: usize = 64;

/// How much of the log is UNREFLECTED at load time -- the knob, and the reason this file measures
/// two regimes rather than one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Suffix {
    /// The shard dumped a checkpoint and then went away without taking another write. Nothing
    /// past the checkpoint; replay has no tail to apply.
    Reflected,
    /// Half the shard's writes landed after the last checkpoint. A long unreflected suffix.
    Unreflected,
}

impl Suffix {
    fn label(self) -> &'static str {
        match self {
            Suffix::Reflected => "REFLECTED (dumped immediately before the shard went away)",
            Suffix::Unreflected => "UNREFLECTED (half the writes are past the last checkpoint)",
        }
    }

    fn short(self) -> &'static str {
        match self {
            Suffix::Reflected => "reflected",
            Suffix::Unreflected => "unreflected",
        }
    }

    fn dumped(self, records: usize) -> usize {
        match self {
            Suffix::Reflected => records,
            Suffix::Unreflected => records / 2,
        }
    }
}

/// xorshift64*, seeded by index so runs repeat and no two records share a payload.
///
/// A repeated byte compresses, and a log built from one would hold a fraction of the bytes its
/// record count suggests.
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

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter for this process.
//
// `rchar` is bytes handed to this process by read-family syscalls, maintained by the kernel.
// Nothing in the engine feeds it, so a residual computed against it CAN be wrong and can be seen
// to be wrong -- which is the property a residual computed from the rows it audits does not have.
//
// Reading the counter is itself a read, and the kernel charges it. The reading therefore carries
// the LENGTH of the text it was read from, so the span's own instrument can be subtracted exactly
// rather than estimated: see `IoReading`.
// ---------------------------------------------------------------------------------------------

/// One reading of `/proc/self/io`, with the size of the text it came from.
#[derive(Debug, Clone, Copy)]
struct IoReading {
    rchar: u64,
    wchar: u64,
    /// Bytes this reading's own `read` was handed. `rchar` above excludes them -- procfs fills
    /// the buffer before the kernel charges for it -- so they land inside any span this reading
    /// OPENS, and subtracting this number removes the instrument from its own measurement.
    self_bytes: u64,
}

fn read_proc_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| -> Option<u64> {
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix(name) {
                if let Some(value) = rest.strip_prefix(':') {
                    return value.trim().parse().ok();
                }
            }
        }
        None
    };
    Some(IoReading {
        rchar: field("rchar")?,
        wchar: field("wchar")?,
        self_bytes: text.len() as u64,
    })
}

/// What the kernel says a span of work read and wrote, with this instrument's own reads removed.
#[derive(Debug, Clone, Copy, Default)]
struct KernelIo {
    rchar: u64,
    wchar: u64,
}

/// Run `work` between two readings of the kernel's counters.
///
/// `None` when `/proc` said nothing, so "the kernel was not counting" cannot be read as "the
/// kernel counted zero" -- the same rule the allocation probe follows.
fn kernel_io_span<T>(work: impl FnOnce() -> T) -> (T, Option<KernelIo>) {
    let before = read_proc_io();
    let value = work();
    let after = read_proc_io();
    let io = before.zip(after).map(|(before, after)| KernelIo {
        // The opening reading's own bytes fall inside the span; nothing else of this
        // instrument's does, because the closing reading is charged after it is taken.
        rchar: (after.rchar - before.rchar).saturating_sub(before.self_bytes),
        wchar: after.wchar - before.wchar,
    });
    (value, io)
}

fn vm_status_kb(field: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            if let Some(value) = rest.strip_prefix(':') {
                return value.trim().trim_end_matches("kB").trim().parse::<u64>().ok();
            }
        }
    }
    None
}

/// Reset this process's peak-RSS high-water mark, so the peak reported for a restore is the
/// restore's and not the corpus build's.
///
/// `5` to `clear_refs` is the kernel's "reset VmHWM" request. It is best-effort: where it is not
/// permitted the peak stays where it was, which shows up as a peak no smaller than the resident
/// size before the restore and is reported as measured rather than corrected.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

// ---------------------------------------------------------------------------------------------
// THE CORPUS
// ---------------------------------------------------------------------------------------------

fn dir_bytes(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(dir_bytes(&entry.path()));
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn files_in(root: &std::path::Path) -> (usize, u64) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return (0, 0);
    };
    let mut count = 0usize;
    let mut bytes = 0u64;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_file() {
            count += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    (count, bytes)
}

/// What a shard that went away without unloading left on disk.
#[derive(Debug, Clone)]
struct Corpus {
    records: usize,
    value_bytes: usize,
    suffix: Suffix,
    /// Records the last dump manifest covers. The rest are the unreflected suffix a restore
    /// has to replay.
    dumped: usize,
    tail: usize,
    wal_bytes: u64,
    wal_pieces: usize,
    manifest_files: usize,
    manifest_bytes: u64,
    index_log_bytes: u64,
    block_bytes: u64,
    store_bytes: u64,
    /// The `wal_sequence` each checkpoint anchored at, in the order they were taken. The pick
    /// takes a maximum over exactly this field, so a fixture whose manifests all carried the
    /// same value could not tell a correct pick from a constant.
    manifest_anchors: Vec<u64>,
    /// Slabs on disk after the writes, and how many of them hold live bytes. Both, because a
    /// store whose every live page sits in slab 0 compares `{0}` against `{0}`.
    slabs_on_disk: usize,
    slabs_holding_live_bytes: usize,
    /// Distinct keys the corpus actually wrote, counted from the strings rather than assumed.
    distinct_keys: usize,
    /// Length of the store's root path. Allocation counts move with it, so it is held equal
    /// across arms and asserted rather than assumed.
    store_path_len: usize,
}

impl Corpus {
    fn anchor(&self) -> u64 {
        self.manifest_anchors.last().copied().unwrap_or(0)
    }
}

/// A shard that took `records` writes, dumped `dumps` checkpoints over its first `dumped` of
/// them, and then went away.
///
/// The engine is DROPPED rather than unloaded. Unloading materialises the base index, which is
/// exactly the durable checkpoint a crash does not leave behind.
fn build_corpus(
    dir: &std::path::Path,
    records: usize,
    value_bytes: usize,
    dumps: usize,
    dumped: usize,
    suffix: Suffix,
) -> Corpus {
    let index_dir = dir.join("indexes");
    let mut manifest_anchors = Vec::new();
    let (slabs_on_disk, slabs_holding_live_bytes) = {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.join("cache"),
            dir.join("blocks"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        // WHERE THE SLABS ROLL. Deliberately NOT the checkpoint boundary: a fixture whose slab
        // edges sat on the dumped/unreflected split would confound the two, and one whose every
        // live page sat in slab 0 would compare `{0}` against `{0}`. A quarter and three
        // quarters puts a DISJOINT key range in each of three slabs and crosses the checkpoint
        // boundary at neither.
        let roll_at = [records / 4, records * 3 / 4];
        let mut taken = 0usize;
        while taken < dumps {
            seed(
                &engine,
                dumped * taken / dumps.max(1),
                dumped * (taken + 1) / dumps.max(1),
                value_bytes,
                &roll_at,
            );
            let manifest = engine
                .create_bucket_dump_manifest(1, Vec::<u32>::new())
                .expect("the corpus needs a durable dump manifest to recover from");
            manifest_anchors.push(manifest.wal_sequence);
            taken += 1;
        }
        seed(&engine, dumped, records, value_bytes, &roll_at);
        let store = engine.block_store();
        let on_disk = store.slab_ids().map(|ids| ids.len()).unwrap_or(0);
        let live = store
            .slab_usage()
            .iter()
            .filter(|usage| usage.live_bytes > 0)
            .count();
        (on_disk, live)
    };
    let (manifest_files, manifest_bytes) = files_in(&index_dir.join("slot-dumps").join("shard-1"));
    let (wal_pieces, wal_bytes) = files_in(&index_dir.join("wals"));
    let distinct_keys = (0..records)
        .map(|index| format!("k-{index:08}"))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    Corpus {
        records,
        value_bytes,
        suffix,
        dumped,
        tail: records - dumped,
        wal_bytes,
        wal_pieces,
        manifest_files,
        manifest_bytes,
        index_log_bytes: dir_bytes(&index_dir.join("indexlogs")),
        block_bytes: dir_bytes(&dir.join("blocks")),
        store_bytes: dir_bytes(dir),
        manifest_anchors,
        slabs_on_disk,
        slabs_holding_live_bytes,
        distinct_keys,
        store_path_len: dir.as_os_str().len(),
    }
}

fn seed(
    engine: &TemporalEngine,
    from: usize,
    to: usize,
    value_bytes: usize,
    roll_at: &[usize],
) {
    let mut index = from;
    while index < to {
        let end = (index + BATCH).min(to);
        // Roll the active slab when the corpus crosses a roll point, so the pages this shard
        // will be rebuilt from are spread over more than one slab.
        if roll_at
            .iter()
            .any(|point| *point > index && *point <= end && index > 0)
        {
            engine
                .block_store()
                .roll_slab()
                .expect("the fixture must be able to roll a slab");
        }
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: incompressible(value_bytes, cursor as u64),
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// What the restart cost.
#[derive(Debug, Clone)]
struct Restart {
    corpus: Corpus,
    cost: restore_phase_probe::RestoreCost,
    /// Allocations and bytes across the WHOLE `load_shard_with` call, split by what they were
    /// spent on, with an independent span total the class rows do not feed. `None` when nothing
    /// was counting.
