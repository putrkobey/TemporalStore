// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RECOVERY READS AT EACH INDEX-LOG SPACE BUDGET -- THE OTHER HALF OF THE TRADE.
//!
//! # THE NUMBER THIS MODULE EXISTS TO STATE
//!
//! The parent module establishes that the dump threshold IS a space budget: it is
//! `base_index / divisor`, so the index log may stand at `1/divisor` of the base,
//! `b = 1 + 1/divisor`, and the measured write amplification sits on `a <= b/(b-1)` to within 1%
//! at every point of an eight-fold sweep of `b`. There is no slack left in that mechanism, so the
//! only remaining lever is the constant -- and nobody had measured what the other side of the
//! trade costs. This is that side, on one corpus, at five budgets:
//!
//! ```text
//!   4,000 objects, shipped routing range 0..1023, scattered keys, 1 KiB rolling threshold
//!
//!   divisor      b       a   a/bound   index log      base      WAL   INDEX-LOG     NAMED  wall
//!                                       on disk      read      read     read        read    ms
//!        2  1.3563   3.810    1.0009     143,669   286,242   763,392   143,669  1,193,303  1671
//!        4  1.2125   5.715    1.0016      61,768   243,537   566,784    61,768    872,089   405
//!        8  1.1159   9.658    1.0030      28,807   223,389   493,056    28,807    745,252   252
//!       16  1.0609  17.511    1.0056      14,409   213,124   468,480    14,409    696,013   249
//!       32  1.0347  30.083    1.0099       7,245   208,503   443,904     7,245    659,652   228
//!
//!   the same five stores with NOTHING past the checkpoint -- the reflected control
//!        2  1.0002      --        --          69   286,242     3,760        69    290,071   193
//!        4  1.0003      --        --          69   243,537     3,796        69    247,402   147
//!        8  1.0003      --        --          69   223,389     3,706        69    227,164   124
//!       16  1.0003      --        --          69   213,124     3,736        69    216,929   135
//!       32  1.0003      --        --          69   208,503     3,776        69    212,348   128
//! ```
//!
//! # THE FINDING: A RECOVERY READS THE WHOLE INDEX LOG, AND IT IS NOT THE FOLD THAT READS IT
//!
//! `index_log_replay_reach` establishes that the DEFAULT load path does not FOLD the index log:
//! `load_shard_with` branches on `wal_single_barrier()`, which is `!wal_legacy_recovery()` and so
//! TRUE unless an operator sets `TS_WAL_LEGACY_RECOVERY`, and that arm calls
//! `load_index_base_only` -- `fold_deltas: false`. `the_fold_reaching_loaders_stay_gated` guards
//! the call-site set that keeps it closed. Both are correct and neither is re-derived here.
//!
//! DOES NOT FOLD IS NOT DOES NOT READ, and nobody had asked the second question. The default arm
//! reads EVERY BYTE of the index log, in the same `load_shard_with` call, through
//! `latest_slab_catalog`: the manifest-conformance seed that recovers the slab catalog. The
//! column above is exact rather than approximate -- the bytes charged inside the read EQUAL the
//! bytes on disk, in all ten readings, to the byte:
//!
//! ```text
//!   143,669 = 143,669    61,768 = 61,768    28,807 = 28,807    14,409 = 14,409
//!     7,245 =   7,245        69 =     69  (the reflected control, at all five budgets)
//! ```
//!
//! AND THE OBVIOUS FIX WAS MEASURED AND DOES NOT WORK, WHICH IS WHY THE READ IS A REAL COST AND
//! NOT AN OVERSIGHT. `latest_slab_catalog` wants the LAST record in the log carrying a slab
//! catalog, so it calls `for_each_delta_record`, which passes base anchor 0 -- and a zero anchor
//! declines nothing. The piece-name decline
//! `index_log_scale::what_an_index_log_replay_reads_at_two_corpus_sizes` measured, and
//! `the_load_path_fold_is_the_one_that_declines` placed, is on the OTHER entry point,
//! `for_each_delta_record_above_anchor` -- so the first thing to try is handing the catalog walk
//! the base anchor. IN THE STATE A BUDGET'S CRASH LEAVES, THAT BUYS NOTHING. The sweep behind the
//! last dump has already dropped every piece the base reflects, so what remains IS the
//! unreflected suffix and there is no piece left for an anchor to decline: driven over the same
//! files, the fold's shape reads the same 25,885 B and hands back the same 19 records as the
//! anchor-less walk, while a control anchor above every record in the log declines pieces and
//! reads strictly less. `the_whole_log_read_at_recovery_is_the_slab_catalog_walk` is those three
//! arms, and the control is there because "the decline declined nothing" and "the decline is
//! broken" read identically without it.
//!
//! So the budget's footprint is read back IN FULL at every restart, irreducibly under this
//! reader, and the relationship is a one-for-one identity rather than a slope with a residual:
//!
//! ```text
//!   index-log bytes a recovery reads  =  (b - 1) * base index on disk
//! ```
//!
//! What WOULD make it smaller is a different property of the same walk: it needs only the LAST
//! record carrying a catalog and reads every record in the log to find it. That is not attempted
//! here; what is recorded is that the budget is currently priced against an unbounded read.
//!
//! # AND THE WAL TAIL MOVES WITH THE BUDGET TOO, ON TOP OF THAT
//!
//! The dump the threshold fires is also what advances the DURABLE RECOVERY WATERMARK --
//! `flush_shard_index` fsyncs every block before advancing it, and
//! `durable_recovery_bucket_dump_manifest_at` is what the default arm reads. So a looser budget
//! dumps less often and leaves a longer unreflected write-ahead tail, and the replay reads
//! 1.69x to 2.91x the bytes the log holds on disk because each window re-reads the pieces behind
//! it -- the term `restore_large_scale` priced at 89% of a 200,000-record restart and measured
//! going 6.70x per stored record for a 10x store. Here the WAL read runs 443,904 B at the tight
//! end to 763,392 B at the loose one, off a log that is 262,144 B on disk in EVERY arm.
//!
//! # THE SAME BUDGET AT A LARGER STORE, AND WHICH TERM IS SCALE-FREE
//!
//! `the_recovery_a_budget_buys_grows_with_the_store` holds the shipped divisor and varies the
//! corpus. The index-log term is scale-free BY CONSTRUCTION and measured to be so -- the
//! threshold is a fraction of the base index, so the footprint is `1/divisor` of it at any size --
//! while the terms it is traded against are not:
//!
//! ```text
//!   objects   base index   index log   log/base    WAL read      NAMED   idx share   wall ms
//!     4,950      223,389      28,807     0.1290     493,056    745,252       3.9%        272
//!    20,700    1,156,918     145,160     0.1255     763,392  2,065,470       7.0%      1,167
//!             (5.179x)      (5.039x)               (1.548x)   (2.772x)
//! ```
//!
//! So a looser budget costs a FIXED FRACTION of the base image in index-log reads at every size,
//! and the base image is itself the largest single term once a store is real: 56% of what the
//! larger recovery reads, against 30% at the smaller one. WHAT DOMINATES therefore has two answers
//! and they are in different units. In BYTES it is the base image at a real store size and the WAL
//! replay at fixture scale. In TIME it is the base image's decode at every size -- 454-517 ms of
//! the 590-1,167 ms above -- while the whole-log read the budget pays for is 73-92 ms of it. The
//! index-log fold is not on this path and reads nothing.
//!
//! # THE SHAPE OF THE WHOLE TRADE, IN ONE LINE EACH
//!
//! ```text
//!   writes:   a  =  b/(b-1)  ~=  divisor + 1          paid on every byte of index logged
//!   recovery: (b-1) * base of index log, read whole   paid once per restart, plus a WAL tail
//!                                                     that grows with the same threshold
//! ```
//!
//! The two are reciprocal in `(b-1)`, so their product is near constant and the measurement
//! identifies no interior optimum on its own. What breaks the symmetry is that the recovery side
//! is the one with a term that is superlinear in its own size, and it is paid at the moment a node
//! is trying to return to service.
//!
//! # EVERY FIGURE'S DENOMINATOR, AND WHICH SURFACE IT IS ON
//!
//! - `base_index` -- `shard-{id}.index.json` on disk. The `TSIDX\x01` container, which is a ZSTD
//!   FRAME: a COMPRESSED size that cannot name a field, and one that moves in its third decimal
//!   place between runs because the index carries wall-clock values. `load_index_base_only` reads
//!   it with one whole-file `fs::read`, so the file's length IS what that reader read; it is
//!   reported as the base-image term rather than estimated.
//! - `index_log` -- `indexlogs/shard-{id}.indexlog*.bin`, the store's own binary framing over a
//!   json payload. UNCOMPRESSED, so it is not comparable byte for byte with the base index. The
//!   only ratio taken between the two is the space budget `b`, which is the quantity the mechanism
//!   is defined in.
//! - `wal` -- `wals/`, which lives UNDER the index root (`with_cache_block_store_and_index_dir`).
//!   The parent module excludes it from the write numerator by PATH for that reason and reports it
//!   beside; here it is a term under measurement, so the same path split is used in the other
//!   direction and the classes are asserted to PARTITION the root.
//! - what a recovery READ is never a directory walk. It is five readers, each with a counter
//!   charged INSIDE its own read: the base image's own file length,
//!   `wal_piece_read_counts_on_this_thread`, `index_log_piece_read_counts_on_this_thread`,
//!   `bucket_dump_manifest_io_counts` and the block store's `stats().bytes_read`. The two logs'
//!   own `stats().bytes_read` is NOT used as a cost: it counts what a scan HANDS BACK, and here
//!   the index log hands back 0 while reading 143,669 B, and the WAL hands back 112,320 B while
//!   reading 763,392. Both are printed, so the difference stays visible.
//!
//! # THE SECOND BOOK, TWICE
//!
//! - THE CRASH STATE: this module's own directory walk against the two stores' own byte counters,
//!   `log_len_bytes` and `probe_all_segment_bytes`, asserted EQUAL TO THE BYTE rather than close.
//! - WHAT THE RECOVERY READ: the kernel. `/proc/thread-self/io`'s `rchar` across the same span,
//!   less the five named readers, is the residual -- a total this module does not maintain, so it
//!   cannot be a sum of the rows it audits. It comes back at 52 B in every reflected arm and
//!   417-440 B in every at-budget arm: 0.04% of the span, flat, and the size of the probe's own
//!   handful of `statx` calls rather than a reader nobody wrote down.
//!   `the_recovery_byte_instrument_recovers_a_planted_read_exactly` plants reads of two known
//!   lengths in front of the same instrument and requires them back exactly, and requires an empty
//!   span to read exactly zero, so that residual is a reading rather than an identity.
//!
//! # THE CONTROLS
//!
//! - THE REFLECTED ARM. Every arm is recovered TWICE off the same store: once straight after the
//!   dump the round fired, with nothing accrued since, and once at the budget's high-water mark.
//!   An idle store holds no tail whatever the threshold says, so its WAL read must not move with
//!   the budget -- it runs 3,706 to 3,796 B across the sweep, a 90-byte spread against the
//!   319,488 B the at-budget arms move across. A measurement taken only in that regime would
//!   report a budget with almost no recovery cost at all, which is the fixture a careless run
//!   picks.
//! - THE IDLE CONTROL in its literal form: the index root is walked twice across an interval in
//!   which nothing is written and the two walks must agree file by file, so a byte that appears
//!   after the accrual is attributable to the accrual.
//! - THE BUDGET IS ASSERTED REACHED before anything is reported: the measured `(base + log)/base`
//!   at the crash state has to be the `1 + 1/divisor` the arm claims. An arm that stopped short
//!   measures a footprint the budget did not choose.
//! - THE AMORTISED REGIME, by the parent's `assert_amortised_regime` on the shipped routing range
//!   `0..1023` -- 7.81 objects a bucket, not the one-per-bucket shape `0..u32::MAX` leaves.
//! - THE RECOVERY SERVED THE CORPUS. A cheap recovery and a lossy one read the same, so every arm
//!   probes the recovered store across the whole key range and reads the WITHHELD ordinal at the
//!   same moment, which has to come back absent.
//! - AND ONE EXPECTATION THAT WAS WRONG, KEPT HERE RATHER THAN QUIETLY DROPPED: the page term was
//!   first required NON-ZERO, on the argument that a load warms the cache. It is ZERO at every
//!   budget, in both regimes, at both store sizes. The expectation was wrong, not the instrument;
//!   see the comment at the term itself.
//!
//! # WHAT IS ASSERTED AND WHAT IS ONLY PRINTED
//!
//! `LAST_REPLAY_WATERMARK`, `restore_phase_probe`'s record counts and `probe::fold_pieces_declined`
//! are PROCESS-GLOBAL, and this suite runs many threads at once, so a neighbouring load can
//! overwrite them between this load returning and this module reading them. They are printed and
//! never asserted. Everything asserted comes from a per-thread counter, a per-store counter or
//! this module's own walk. The watermark the tail is computed from is this module's own, derived
//! from the files by the same rule the default arm uses, and the engine's record is printed beside
//! it -- the two agreed in all ten readings of the sweep.
//!
//! # WHAT PLANTING EACH CLAUSE SAID ABOUT IT
//!
//! Ten plants, one clause at a time, each a production mutation a real regression could produce.
//! Seven CAUGHT. Three did not, and what they say is recorded here rather than left out:
//!
//! - THE CLAUSE WITH REDUNDANT SUPPORT. "A looser budget costs more to recover" survives the loss
//!   of `replay_start_after_sequence`'s piece skip -- planted, the tree still passed. The ordering
//!   across budgets has TWO maintainers: where in the retained log a replay starts, and how many
//!   windows the records it applies need. Removing one leaves the other, so a single-removal plant
//!   cannot isolate that clause, and "not caught" here means REDUNDANT rather than wrong.
//! - A PLANT THAT MADE THE FIXTURE QUADRATIC INSTEAD OF FAILING. Making `index_log_total_bytes`
//!   report the active piece -- the regression `log_len_bytes` documents -- also starves the dump
//!   cadence, which reads that same function, so the round never fires and the fixture writes
//!   100,000 records against a growing directory listing. The clause is defended by the drift
//!   planted inside `log_len_bytes` alone instead, which the cadence does not read: that one is
//!   CAUGHT, by the walk-against-the-counter assertion, at one byte of drift.
//! - A VOID, WHICH IS NOT A NEGATIVE. The checkpoint pick (`durable_recovery_bucket_dump_manifest_at`)
//!   cannot be reached from this fixture at all: it writes NO bucket dump manifests, which the
//!   manifest reader's own count says -- 0 file reads in all ten readings, in both regimes. So the
//!   reflected arm's idleness does not rest on the manifest pick; it rests on the base index's own
//!   anchor and on the WAL reclaim behind the dump. Planting THAT reclaim away is CAUGHT.
//!
//! Wall time and the per-phase microseconds are printed because a node's return to service is
//! measured in seconds and nobody had the number. NOTHING IS ASSERTED ON THEM: this box carries
//! other tenants, `/proc/loadavg` ran from 1.59 to 4.94 across the five arms above, and the same
//! round has been seen at 490 ms and at 1,077 ms. Every assertion in this file rests on a count.
#![allow(clippy::all)]
use super::*;

/// The store size the budget sweep runs, and the parent budget sweep's size -- so the `a` and `b`
/// measured per arm here are on the same corpus as the published write-side table, and the two
/// halves of the trade are one table rather than two.
const SWEEP_OBJECTS: usize = 4_000;

/// The second size, for the shape question. A dimensionless budget needs a RANGE of budgets and
/// not a large absolute store, which is why the sweep is at one size; what a second size answers
/// is whether the recovery term the budget buys grows with the store as well as with the budget.
const LARGE_OBJECTS: usize = 16_000;

/// Key reads spread across the whole corpus on every arm. A recovery that came back holding only
/// the dumped half reads exactly like a cheap one, so the SPREAD matters more than the count.
const PROBE_READS: usize = 64;

/// Which recovery is being measured. Two regimes rather than two fixtures: a store with nothing
/// past its checkpoint has no tail to replay WHATEVER the threshold says, which is what makes the
/// reflected arm a control on the budget rather than a smaller version of the same reading.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Regime {
    /// The dump has just fired and nothing has been written since. The idle control.
    Reflected,
    /// The index log stands at the budget's high-water mark -- the worst moment the budget admits,
    /// and the state a crash leaves at the instant before a dump.
    AtTheBudget,
}

impl Regime {
    fn short(self) -> &'static str {
        match self {
            Regime::Reflected => "reflected",
            Regime::AtTheBudget => "at-budget",
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT
// ---------------------------------------------------------------------------------------------

/// One reading of THIS THREAD's kernel read counter, and the length of the text it came from.
///
/// The length is carried because taking the reading is itself a read: the span below subtracts the
/// bytes of its own opening sample, which is what makes "exactly" available to the planted-read
/// control.
#[derive(Clone, Copy, Debug)]
struct KernelRead {
    rchar: u64,
    own_bytes: u64,
}

/// `/proc/thread-self/io`, not `/proc/self/io`.
///
/// A restore runs on the thread that called it; this suite runs many threads at once. The process
/// counter would charge this span whatever a neighbouring test happened to read, and taking the
/// total from outside the rows it audits is the whole point of the residual.
fn kernel_read_now() -> Option<KernelRead> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            return Some(KernelRead {
                rchar: rest.trim().parse().ok()?,
                own_bytes: text.len() as u64,
            });
        }
    }
    None
}

/// Bytes the kernel charged this thread across `work`, with the opening sample's own bytes taken
/// back out.
///
/// `None` when the file cannot be read, which every caller turns into an APPARATUS failure rather
/// than a zero: a residual of zero and a residual that was never measured read identically.
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = kernel_read_now();
    let value = work();
    let after = kernel_read_now();
    let charged = before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.own_bytes));
    (value, charged)
}

// ---------------------------------------------------------------------------------------------
// THE CRASH STATE
// ---------------------------------------------------------------------------------------------

type Tree = std::collections::BTreeMap<std::path::PathBuf, (u64, std::time::SystemTime)>;

/// Total length of every write-ahead log piece in a snapshot, by the same path split the parent
/// module excludes the log with -- used here in the other direction.
fn wal_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::WriteAheadLog)
        .map(|(_, (len, _))| *len)
        .sum()
}

fn base_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Base)
        .map(|(_, (len, _))| *len)
        .sum()
}

fn other_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Other)
        .map(|(_, (len, _))| *len)
        .sum()
}

/// Index-log pieces in a snapshot, counted off the filesystem rather than asked of the store.
fn log_pieces(tree: &Tree) -> usize {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Log)
        .count()
}

/// The watermark a recovery over these files WILL replay above, computed from the files by the
/// same rule `load_shard_with`'s default arm uses: the base index's own anchor, raised by the
/// newest durable dump manifest when that is ahead of it.
///
/// This module's OWN book. The engine records its decision in `LAST_REPLAY_WATERMARK`, which is
/// process-global and therefore not assertable under a parallel suite; it is printed beside this.
fn durable_watermark(dir: &std::path::Path) -> u64 {
    let index_dir = dir.join("indexes");
    let base = std::fs::read(index_dir.join(format!("shard-{SHARD}.index.json")))
        .ok()
        .and_then(|bytes| crate::engine::decode_index_bytes(&bytes).ok())
        .and_then(|state| state.applied_wal_sequence)
        .unwrap_or(0);
    match crate::engine::bucket_dump_io::durable_recovery_bucket_dump_manifest_at(&index_dir, SHARD)
    {
        Some(manifest) if manifest.wal_sequence > base => manifest.wal_sequence,
        _ => base,
    }
}

/// Copy a store directory, so the crash state can be recovered from WITHOUT being consumed.
///
/// A recovery is not read-only -- the load settles and stamps the routing range -- so recovering
/// twice from one directory would measure the second recovery against a store the first changed.
/// Both regimes of one arm are recovered off copies of the same files, which is what lets the two
/// readings be compared at all.
fn copy_tree(from: &std::path::Path, to: &std::path::Path) -> u64 {
    std::fs::create_dir_all(to).expect("APPARATUS: the crash-state copy needs a directory");
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(from)
        .expect("APPARATUS: the crash state must be readable to be copied")
        .flatten()
    {
        let src = entry.path();
        let dst = to.join(entry.file_name());
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            bytes += copy_tree(&src, &dst);
        } else {
            bytes += std::fs::copy(&src, &dst).expect("APPARATUS: a crash-state file must copy");
        }
    }
    bytes
}

/// An engine over `dir` that has NOT loaded the shard yet.
///
/// The parent's `shipped_range_engine` constructs and loads in one call, and the load is the span
/// under measurement here, so it has to be issued separately. The range is the same shipped
/// `0..1023` and is named rather than defaulted, for the same reason the parent names it.
fn unloaded_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache-recover"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

/// What the store held when the engine went away, and what the recovery over it read.
#[derive(Debug, Clone)]
struct Recovery {
    regime: Regime,
    divisor: u64,
    objects: usize,
    // ---- THE CRASH STATE, by the file each byte lives in
    /// `shard-{id}.index.json`. The `TSIDX\x01` container -- a zstd frame, a compressed size.
    base_index_bytes: u64,
    /// `indexlogs/`, the store's binary framing over json. Uncompressed.
    index_log_bytes: u64,
    index_log_pieces: usize,
    /// `wals/`, which lives under the index root and is no part of any index figure.
    wal_bytes: u64,
    // ---- WHAT THE RECOVERY READ, by reader, each counter charged inside its own read
    base_image_read: u64,
    manifest_bytes_read: u64,
    manifest_file_reads: u64,
    wal_read_bytes: u64,
    wal_reads: u64,
    index_log_read_bytes: u64,
    index_log_reads: u64,
    block_read_bytes: u64,
    page_reads: u64,
    /// The two stores' OWN `bytes_read`, which counts what a scan handed back rather than what it
    /// read off disk. Printed beside the physical counters so the difference stays visible; never
    /// used as a cost.
    wal_handed_back: u64,
    index_log_handed_back: u64,
    // ---- THE SECOND BOOK
    kernel_rchar: Option<u64>,
    // ---- WHAT THE REPLAY HAD TO DO
    /// This module's own watermark, from the files.
    watermark: u64,
    /// The engine's own record of the same decision. PROCESS-GLOBAL, printed, never asserted.
    engine_watermark: u64,
    wal_last_sequence: u64,
    /// Process-global, printed, never asserted.
    records_decoded: u64,
    records_behind_checkpoint: u64,
    /// Pieces a fold declined by name. Only `fold_index_log_deltas` passes a non-zero anchor, so
    /// this moves for no other entry point -- which is why it is the column that says the whole-log
    /// read was NOT the delta fold. PROCESS-GLOBAL, printed, never asserted.
    fold_pieces_declined: u64,
    replay_scans: u64,
    // ---- TIME. REPORTED, NOT CLAIMED.
    wall_ms: u64,
    nanos_base: u64,
    /// `publish_and_seed`, the phase that holds `latest_slab_catalog` -- the whole-log read.
    nanos_seed: u64,
    nanos_replay: u64,
    nanos_fold: u64,
    nanos_total: u64,
    // ---- AND WHETHER IT CAME BACK WITH THE DATA
    probed: usize,
    /// Of those positions, the ones the fixture actually wrote. COUNTED in the loop.
    live: usize,
    readable: usize,
    withheld_absent: bool,
}

impl Recovery {
    /// Every reader this module names. Never summed into the residual's total.
    fn named(&self) -> u64 {
        self.base_image_read
            + self.manifest_bytes_read
            + self.wal_read_bytes
            + self.index_log_read_bytes
            + self.block_read_bytes
    }

    /// THE SUBJECT: the kernel's total less the rows above.
    fn residual(&self) -> i64 {
        self.kernel_rchar.unwrap_or(0) as i64 - self.named() as i64
    }

    /// The space budget this crash state actually stood at.
    fn b(&self) -> f64 {
        assert!(
            self.base_index_bytes > 0,
            "APPARATUS: the {} arm at divisor {} left no base index on disk, so there is nothing \
             for the space figure to be a multiple of",
            self.regime.short(),
            self.divisor
        );
        (self.base_index_bytes + self.index_log_bytes) as f64 / self.base_index_bytes as f64
    }

    /// WAL records the recovery had to replay: the tail the budget left unreflected, from this
    /// module's own watermark.
    fn tail_records(&self) -> u64 {
        self.wal_last_sequence.saturating_sub(self.watermark)
    }
}

/// Write in batches until the PRODUCTION cadence would fire, and stop there WITHOUT firing it.
///
/// That instant is the budget's high-water mark and the worst moment a crash can pick, which is
/// what a budget has to be priced at. The gate is asked on every batch below the threshold and
/// must REFUSE, exactly as `run_round` asks it, so a gate that fires early is an apparatus failure
/// here too. It is deliberately NOT asked at or above the threshold, because asking is what fires
/// it; the pure predicate `should_dump_index_catalog` is asserted instead, which is the same
/// decision without the dump.
fn accrue_to_the_budget(
    engine: &TemporalEngine,
    next_ordinal: &mut usize,
    shape: KeyShape,
    divisor: u64,
    base_index_bytes: u64,
) -> u64 {
    let store = engine.index_log_store();
    let threshold_bytes = crate::index_log::effective_index_dump_threshold_bytes(
        FLOOR_BYTES,
        base_index_bytes,
        divisor,
    );
    assert!(
        threshold_bytes > FLOOR_BYTES,
        "FIXTURE: the accrual at divisor {divisor} held out for {threshold_bytes} B, which is the \
         configured floor {FLOOR_BYTES} B rather than a threshold read off the {base_index_bytes} \
         B base index. The relative term does not bind here, so the budget is not what decides \
         this tail"
    );
    for batch in 0..MAX_BATCHES {
        let accrued = store.undumped_len_since_dump(SHARD);
        if accrued >= threshold_bytes {
            assert!(
                crate::index_log::should_dump_index_catalog(accrued, threshold_bytes),
                "APPARATUS: {accrued} B accrued against a {threshold_bytes} B threshold and the \
                 production predicate says do not dump -- the stopping point and the cadence \
                 disagree, so this tail is not the one the budget chose"
            );
            return threshold_bytes;
        }
        let refused = engine
            .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                SHARD,
                FLOOR_BYTES,
                0,
                0,
                divisor,
            )
            .is_none();
        assert!(
            refused,
            "APPARATUS: the cadence FIRED on {accrued} B accrued, below the {threshold_bytes} B \
             `effective_index_dump_threshold_bytes` says it holds out for -- so the state this \
             accrual leaves is not the budget's high-water mark"
        );
        assert!(
            batch + 1 < MAX_BATCHES,
            "APPARATUS: an accrual at divisor {divisor} never reached its threshold: \
             {MAX_BATCHES} batches of {BATCH} against {threshold_bytes} B, with {accrued} B \
             accrued"
        );
        seed_range(engine, *next_ordinal, *next_ordinal + BATCH, shape);
        *next_ordinal += BATCH;
    }
    unreachable!("the loop above either returns or asserts");
}

/// Recover a crash state and count what the recovery read.
///
/// The engine is CONSTRUCTED first and the counters are taken around the LOAD alone, so nothing
/// the constructor reads -- the slab manifest among it -- is charged to the recovery.
fn recover(
    dir: &std::path::Path,
    regime: Regime,
    divisor: u64,
    objects: usize,
    shape: KeyShape,
) -> Recovery {
    let tree = index_tree(&dir.join("indexes"));
    let base_index_bytes = base_total(&tree);
    let index_log_bytes = log_total(&tree);
    let index_log_pieces = log_pieces(&tree);
    let wal_bytes = wal_total(&tree);
    // THE THREE CLASSES PARTITION THE ROOT. Without this the WAL could be inside the index-log
    // figure, which is how the parent module once read 263,8xx B a round as index.
    assert_eq!(
        base_index_bytes + index_log_bytes + wal_bytes + other_total(&tree),
        tree.values().map(|(len, _)| *len).sum::<u64>(),
        "APPARATUS: the four path classes do not partition the index root, so one of the three \
         figures above is carrying bytes that belong to another"
    );
    assert_eq!(
        base_index_bytes,
        base_index_on_disk(dir),
        "APPARATUS: the walk found {base_index_bytes} B of base index and the direct read of \
         `shard-{SHARD}.index.json` found {} B, so there is more than one file in the Base class \
         and the base-image term names the wrong one",
        base_index_on_disk(dir),
    );
    let watermark = durable_watermark(dir);

    let engine = unloaded_engine(dir);
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let (wal_before, wal_reads_before) = crate::wal::wal_piece_read_counts_on_this_thread();
    let (index_before, index_reads_before) =
        crate::index_log::index_log_piece_read_counts_on_this_thread();
    let blocks_before = engine.block_store().stats();
    let started = std::time::Instant::now();

    let (response, kernel_rchar) = kernel_read_span(|| {
        engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: SHARD,
            table_name: "index-recovery-cost".to_string(),
            shard_uri: "local://index-recovery-cost/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: SHIPPED_END_BUCKET,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        })
    });
    let wall_ms = started.elapsed().as_millis() as u64;
    let cost = crate::engine::lifecycle::restore_phase_probe::finish();
    assert!(
        response.status.ok,
        "APPARATUS: the recovery under measurement must succeed, or every number here describes a \
         failure: {:?}",
        response.status
    );

    let (wal_after, wal_reads_after) = crate::wal::wal_piece_read_counts_on_this_thread();
    let (index_after, index_reads_after) =
        crate::index_log::index_log_piece_read_counts_on_this_thread();
    // BEFORE the readback below, which would charge its own page reads to the recovery.
    let blocks_after = engine.block_store().stats();
    let manifest = crate::engine::bucket_dump_manifest_io_counts();
    // `stats`, not `raw_stats`: the cached `last_sequence` is set by an APPEND, and a store
    // that only replayed never appended, so `raw_stats` answered 0 here for every arm and the
    // tail it is subtracted from read as zero everywhere. `stats` scans for it, which costs a
    // read -- taken AFTER the counters above are captured, so the scan is not charged to the
    // recovery.
    let wal_stats = engine.write_ahead_log_store().stats(SHARD);
    let index_stats = engine.index_log_store().stats(SHARD);

    let mut readable = 0usize;
    let mut live = 0usize;
    let mut probed = 0usize;
    while probed < PROBE_READS {
        let ordinal = probed * objects / PROBE_READS;
        probed += 1;
        // COUNTED, not derived. The step is `objects / PROBE_READS`, so whether it ever lands on
        // the withheld ordinal depends on the corpus size -- at these sizes it does not, and an
        // expected count of `probed - 1` was wrong by exactly that for every arm.
        if ordinal == WITHHELD_ORDINAL {
            continue;
        }
        live += 1;
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: key_text(ordinal, shape),
            },
        });
        let got = matches!(
            &response.response,
            CommandResponse::Bytes { value }
                if value.as_ref().map(|bytes| bytes.len()) == Some(VALUE_LEN)
        );
        if response.status.ok && got {
            readable += 1;
        }
    }
    // THE NEGATIVE CONTROL, same build and same moment as the positive reads above.
    let withheld = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: key_text(WITHHELD_ORDINAL, shape),
        },
    });
    let withheld_absent = matches!(
        &withheld.response,
        CommandResponse::Bytes { value } if value.is_none()
    );

    Recovery {
        regime,
        divisor,
        objects,
        base_index_bytes,
        index_log_bytes,
        index_log_pieces,
        wal_bytes,
        base_image_read: base_index_bytes,
        manifest_bytes_read: manifest.bytes_read,
        manifest_file_reads: manifest.file_reads,
        wal_read_bytes: wal_after - wal_before,
        wal_reads: wal_reads_after - wal_reads_before,
        index_log_read_bytes: index_after - index_before,
        index_log_reads: index_reads_after - index_reads_before,
        block_read_bytes: blocks_after
            .bytes_read
            .saturating_sub(blocks_before.bytes_read),
        page_reads: blocks_after.reads.saturating_sub(blocks_before.reads),
        wal_handed_back: wal_stats.bytes_read,
        index_log_handed_back: index_stats.bytes_read,
        kernel_rchar,
        watermark,
        engine_watermark: crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .load(std::sync::atomic::Ordering::SeqCst),
        wal_last_sequence: wal_stats.last_sequence,
        records_decoded: cost.records_decoded,
        records_behind_checkpoint: cost.records_behind_checkpoint,
        fold_pieces_declined: crate::index_log::probe::fold_pieces_declined(),
        replay_scans: wal_stats.scans,
        wall_ms,
        nanos_base: cost.phase("manifest_and_base_index").nanos,
        nanos_seed: cost.phase("publish_and_seed").nanos,
        nanos_replay: cost.phase("wal_replay").nanos,
        nanos_fold: cost.phase("index_fold").nanos,
        nanos_total: cost.nanos(),
        probed,
        live,
        readable,
        withheld_absent,
    }
}

/// Everything that says a recovery reading is a reading of what it claims to be.
fn assert_recovery_can_express_it(reading: &Recovery) {
    let tag = format!("the {} arm at divisor {}", reading.regime.short(), reading.divisor);
    assert!(
        reading.base_index_bytes > 0,
        "FIXTURE: {tag} crashed with no base index on disk, so the recovery has no checkpoint to \
         read and is not the recovery a budget is paid for"
    );
    assert!(
        reading.wal_bytes > 0,
        "APPARATUS: {tag} attributed ZERO bytes to `wals/`. The write-ahead log shares the index \
         root and every write here goes through it, so a zero means the path split has stopped \
         matching anything -- and those bytes are inside the index-log figure, where the parent \
         module once read 263,8xx B a round"
    );
    assert!(
        reading.base_image_read > 0,
        "APPARATUS: {tag} has a zero base-image term, so the one reader every recovery has is \
         missing from the accounting"
    );
    // NO ASSERTION ON THE PAGE TERM, AND THE REASON IS A MEASUREMENT THAT CONTRADICTED AN
    // EXPECTATION. This file first required it NON-ZERO, on the argument that the load warms
    // the cache through `reconcile_secondary_views_from_bucket_index`. It is ZERO -- at every
    // budget, in both regimes, at both store sizes. The expectation was wrong rather than the
    // instrument: a recovery over a base image plus a WAL tail rebuilds the secondary views
    // from the decoded index and from the records it replays, and asks the block store for
    // nothing. The term stays in the accounting by NAME so that a recovery which does start
    // reading pages appears as a row rather than inside the residual, and the residual beside
    // it is what would catch those bytes if the counter were the thing at fault.
    assert!(
        reading.kernel_rchar.is_some(),
        "APPARATUS: {tag} found no rchar line in /proc/thread-self/io, so the second book was \
         never opened and its residual is not a reading"
    );
    assert!(
        reading.live > PROBE_READS / 2,
        "APPARATUS: {tag} probed {} position(s) of which only {} were ordinals the fixture wrote, \
         so the readback below is over almost nothing",
        reading.probed,
        reading.live,
    );
    assert_eq!(
        reading.readable, reading.live,
        "READBACK: {tag} came back serving {} of the {} live ordinals probed across the corpus. A \
         recovery that lost data reads exactly like a cheap one",
        reading.readable,
        reading.live,
    );
    assert!(
        reading.withheld_absent,
        "NEGATIVE CONTROL: {tag} serves the withheld ordinal {WITHHELD_ORDINAL}, which the \
         fixture never wrote -- so a present read anywhere in that readback proves nothing"
    );
}

fn show_recovery(reading: &Recovery) {
    eprintln!(
        "  d={:<3} {:<10} CRASH STATE base={:9} B  log={:8} B in {:3} piece(s)  wal={:9} B   \
         (b={:.4})",
        reading.divisor,
        reading.regime.short(),
        reading.base_index_bytes,
        reading.index_log_bytes,
        reading.index_log_pieces,
        reading.wal_bytes,
        reading.b(),
