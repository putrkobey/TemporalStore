// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CATALOG DUMP COSTS AND WHAT IT RELEASES, AT TWO CORPUS SIZES.
//!
//! Nothing bounds the retained index-log pieces except a completed dump -- measured at 20,000 and
//! 200,000 records in `index_log_scale::what_the_index_log_fold_costs_on_a_large_store`, which
//! found no per-round cap and no ceiling on the log's footprint separate from the records. The
//! dump is therefore the mechanism the whole embedded engine's boundedness rests on, and it had
//! only ever been measured small: `dump_scale` runs at 2,000 and 20,000, `reclaim_dump` at 64.
//! This starts where those stop.
//!
//! Counted, never timed. The box these run on carries five other tenants and one test has been
//! seen to vary 2.4x in a day, so every number below is a byte count, a barrier count or a
//! directory listing.
//!
//! THE RESULT, at a fixed 8 KiB rolling threshold, measuring the SECOND dump -- the one a running
//! store keeps paying, not the first:
//!
//! ```text
//!   records   the dump writes   it releases   written per byte released
//!    20,000         937,638 B     148,229 B                     6.3256
//!   200,000       7,730,157 B     148,271 B                    52.1353
//! ```
//!
//! THE RELEASE IS PROPORTIONAL TO THE WORK. 148,229 B against 148,271 B -- the same 4,000 records
//! written since the previous dump, priced the same at ten times the corpus. The index log goes
//! back to ONE piece at both sizes, so the bound the store rests on is reachable, and reaching it
//! costs nothing extra as the store grows.
//!
//! THE COST IS AFFINE IN THE STORE -- A PER-RECORD TERM PLUS A FIXED ONE. A dump serializes the
//! whole served index and writes it durably (`engine/engine_persistence.rs`, step 2 of
//! `dump_index_catalog_anchored`), so the bytes it writes are the shard's, not the round's:
//! 929,480 B of base index at 24,000 records held and 9,050,875 B at 240,000. Fitted on the two
//! PROPORTIONAL arms, a dump costs 37.598 B per record held PLUS about 35,291 B that does not
//! move with the corpus, and that line predicts the held-out FIXED arm at 204,000 records held to
//! 0.322%. The fixed term is visible without any fit as well: everything a dump writes that is
//! NOT the base index comes to 8,158 / 8,157 / 7,849 / 7,876 B across the four arms -- flat
//! across a 10x corpus. That is a durability decision and this does not argue with it. What it
//! measures is that the two halves scale differently, so the price of the bound climbs with the
//! corpus while what it buys does not.
//!
//! BOTH REGIMES, BECAUSE THE CADENCE IS A KNOB. `TS_INDEX_DUMP_WAL_GAP_BYTES` decides how much
//! index log accrues before a dump fires, which is the same thing as how many records a dump has
//! to show for itself.
//!
//! - FIXED GAP -- what ships: 1 MiB whatever the store holds, so a dump always releases about the
//!   same amount. Written per byte released goes 6.3256 -> 52.1353, a factor of 8.242 over a
//!   corpus factor of 10.
//! - PROPORTIONAL GAP -- the gap grows with the store, so the release grows with it and the
//!   per-record term cancels. What is left is the FIXED term amortised over a release ten times
//!   larger, so this arm does not climb -- it DECLINES slightly, 6.3256 -> 6.1125 (0.9663x). It
//!   is BOUNDED, not flat, and that distinction is the whole content of the affine fit above: a
//!   cost truly proportional to the store, with no fixed term, would make this arm flat to four
//!   figures. A measurement taken only here would still report a dump whose price does not move
//!   with the corpus.
//!
//! The shipped default is the first one. Derived from the two rates below -- 37.048 B of index
//! log per record, flat at both sizes, and the base index the dump writes -- the 1 MiB gap admits
//! about 28,300 records between dumps, and the base index, 37.599 B per record held plus 27,103 B
//! fitted over the same two arms, first exceeds that 1 MiB at about 27,200 records. Past that
//! point every dump the shipped cadence fires writes more bytes than the cadence will ever let it
//! release: at 200,000 records the base index alone is 7.365 times what the gap can release.
//!
//! CAN IT KEEP UP. `a_store_written_between_dumps_returns_to_its_floor_and_pays_the_whole_store`
//! writes 4,000 records and dumps, five times over. The log returns to 69 bytes every round and
//! the released figure is 160,229 B every round -- the bound is reached, every time. The dump's
//! own cost over those same five rounds is 1,584,685 / 1,848,016 / 2,112,666 / 2,375,802 /
//! 2,640,042 B. Written per byte released: 9.89, 11.53, 13.18, 14.83, 16.48. The store keeps up
//! and gets more expensive doing it, round after round, with nothing levelling off. Only the
//! released figure is asserted equal round to round; the costs are asserted to climb, because the
//! last digits of a byte count move a little between runs and the claim does not rest on them.
//!
//! WHAT HAPPENS WHEN IT CANNOT. Nothing sheds, throttles or signals. Searched across `engine.rs`,
//! `engine/`, `wal.rs`, `index_log.rs`, `storage_config.rs`, `data_node.rs` and `data_node/`:
//! `backpressure` appears only on the ingestion-stream and writeback paths, `shed` only in a
//! comment about follower divergence, and `throttle`, `slow_down`, `over_limit`, `too_many`,
//! `admission_control` and any spelling of dump-lag appear nowhere in the dump path at all. The
//! one mechanism that reacts to the dump falling behind is `TS_WAL_RESIDENT_BLOCKS` (default
//! 4,096), and it reacts by making the WRITER do more work -- a sweep that materializes the
//! oldest log-resident pages, taken on the write path after the ack. It bounds a set; it does not
//! slow, refuse or report anything.
//!
//! THE WRITE-AHEAD LOG HALF USED TO RELEASE NOTHING, AND SAID SO IN THE SAME WORDS AS SUCCESS.
//! At both corpus sizes the dump's WAL reclaim reported `0 -> 0 bytes, 0 records` against a log
//! holding 48 records in 524,288 bytes at 20,000 and 408 records in 3,145,728 bytes at 200,000,
//! while a control arm written one command at a time released 261,933 bytes in 1,199 records off
//! the same call. Underneath the zero, `gc_before_sequence` returned
//! `Corruption("binary record is incomplete")`, and `engine/engine_persistence.rs` took it with `.ok()`
//! and then `.unwrap_or_default()`, so an errored reclaim and a reclaim with nothing to do
//! produced a byte-identical `CatalogDumpReclaimReport`.
//!
//! THE TRIGGER WAS THE PAYLOAD'S LAST BYTE, and it is fixed. A record is written as a binary
//! frame, which carries no delimiter and ends where its declared length ends; the reclaim walk
//! ran `strip_suffix(b"\n")` over it anyway, so a payload whose final byte was `0x0A` lost it,
//! read one byte short of what the frame declares, and was refused as a torn frame. None of the
//! three candidates this module first drove could see it -- it is not the record count, not the
//! bytes in the log, and not a newline INSIDE the payload, which lands in the middle and changes
//! nothing. `wal_reclaim_frame_boundary` holds the decision, the two-sided fixture and the
//! element-by-element comparison; this module's subject arm now releases 254,342 bytes in 8
//! records where it released nothing, and asserts that rather than the zero.
//!
//! The `.ok()` stays -- a failed sweep must not fail a dump that has already completed -- but
//! `CatalogDumpReclaimReport` now carries `wal_sweep_failed`, so the three zeros of a refusal
//! are no longer the three zeros of an empty log. The embedded proxy's reclaim loop is the only
//! thing keeping that store's logs bounded and it prints exactly that report.
//!
//! DIRECTION. A dump that releases too much is silent data loss and a dump that releases too
//! little is merely slow, so the strong form is asserted: after a dump, a reclaim and a reload
//! from disk, the store serves the same records in the same order as a control that never
//! dumped, compared element by element rather than counted.
//!
//! THE RESIDUAL is the kernel's `wchar` for THIS THREAD minus the bytes of every file under the
//! store root whose length or mtime moved across the dump -- an attribution built from two
//! directory walks this test does itself, never from anything the dump reports. It is 0 bytes at
//! both corpus sizes and in both regimes. `the_byte_instrument_reports_a_planted_write` is the
//! control that stops that zero meaning the instrument is blind: it plants 4,096 bytes outside
//! the walked root and requires the residual to recover exactly 4,096.
//!
//! PER THREAD, NOT PER PROCESS, AND THAT DISTINCTION WAS EARNED. Read from `/proc/self/io` the
//! residual is 0 when this module runs alone and 3,194 at 20,000 records when it runs inside the
//! whole lib gate -- bytes written by a background thread an earlier test left running while the
//! measured span was open. A residual is only independent of what it audits if it is also
//! independent of everything else sharing the process, so the counter is read from
//! `/proc/thread-self/io`.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const SHARD: ShardId = 1;
const SMALL: usize = 20_000;
const LARGE: usize = 200_000;
/// Records written between the priming dump and the measured one, in the FIXED-gap regime.
///
/// Small against both corpora on purpose: the claim being made is that the cost tracks THE STORE,
/// which needs the store to be what differs between the two arms.
const FIXED_SUFFIX: usize = 4_000;
/// Denominator of the PROPORTIONAL-gap regime. At SMALL it comes out equal to `FIXED_SUFFIX`, so
/// the two regimes are the same measurement at 20,000 and can only diverge at 200,000.
const PROPORTIONAL_DIVISOR: usize = 5;
const VALUE_LEN: usize = 128;
const SEED_BATCH: usize = 500;
/// The rolling threshold, held FIXED across every arm. It is a deployment knob and not what is
/// under measurement; 8 KiB rather than the shipped 64 KiB for the reason
/// `index_log_scale::what_an_index_log_replay_reads_at_two_corpus_sizes` gives, that the decision
/// being priced is PER PIECE and a fixture in one piece cannot price it.
const ROLL_BYTES: u64 = 8 * 1024;
/// `TS_INDEX_DUMP_WAL_GAP_BYTES`' shipped default, quoted here so the derived cadence figures in
/// the header have their input written down beside them.
const SHIPPED_GAP_BYTES: u64 = 1024 * 1024;

/// WHAT THE DRIFT ON THESE COUNTS ACTUALLY IS, measured, because all three bands below are set
/// against it rather than guessed. It is NOT uniform across the arms, and the SMALL arms alone
/// understate it by four orders of magnitude -- which is the trap this comment exists to close.
///
/// At SMALL the apparatus is all but deterministic. The small FIXED arm and the small
/// PROPORTIONAL arm are independent runs of an IDENTICAL configuration -- same corpus, same
/// suffix, separate temporary directories -- and their costs came out 937,638 B against
/// 937,637 B: ONE byte, about 1e-6, with the released figure identical to the byte.
///
/// At LARGE it is far bigger and it is ONE-SIDED. The 200,000-record FIXED arm reads 52.1353
/// bytes written per byte released here and has been read at 52.2800 on another run of this
/// tree -- 0.28%, attributable entirely to the NUMERATOR. Across those runs the released side is
/// byte-identical (`index_log` 148,271 B, WAL 254,664 B) while the dump writes 21,458 B more,
/// which is 0.107 B per record; `kernel` on that arm is 99.90% `base_index`, so the movement is
/// in what a dump WRITES and not in what it frees. The denominator is ruled out independently:
/// `index_log` moves 42 B across a TEN-FOLD corpus here, where shifting the ratio from 52.1353
/// to 52.2800 by the denominator alone would take 412 B.
///
/// So 0.28% per large arm is what the bands below clear, not the 1e-6 the small arms suggest. It
/// is NAMED here rather than absorbed: a band has to clear it or this test flakes, but the
/// movement is a real one-sided effect in the bytes a dump writes, and a guard on it belongs on
/// the numerator -- `what_a_dump_writes_is_the_base_index` -- rather than on this ratio, which
/// cannot tell a numerator that grew from a denominator that shrank.
///
/// Band on the affine fit's HELD-OUT prediction. The fit is two points, so it has no residual of
/// its own and every bit of its falsifiable content is the arm it does not see: the FIXED arm at
/// 204,000 records held, predicted 7,705,232 B against a measured 7,730,157 B, 0.322% off, which
/// is the base index's own mild non-linearity. The two large arms drift independently, and the
/// fit carries 5/6 of the proportional arm's drift into the prediction, so the error this
/// assertion actually sees is 0.322% give or take 0.553% -- up to about 0.88% when the two drifts
/// oppose. 3% clears that worst case by 3.4x and is still 1/240th of the 724% climb the subject
/// arm has to show, so it refuses a regime change while tolerating the creep.
///
/// Re-measured across a base change that was expected to move these counts and did not: turning
/// one index entry a page on by default (#2117) left every arm's cost, base index and released
/// figure identical to the byte, the model error at 0.3224% and the decline at 0.9663x, with the
/// fitted intercept 35,292 B against 35,291 B -- one byte, the small-arm drift arriving in the
/// fit.
const AFFINE_MODEL_BAND: f64 = 0.03;
/// Band on the fitted FIXED term, as a share of what a dump costs at SMALL. Measured 3.764%
/// (about 35,291 B of 937,638 B).
///
/// This is the term that makes the cost affine rather than proportional, so the FLOOR is what
/// refuses a cost that has become proportional and the CEILING refuses a fixed term big enough to
/// mean the dump had stopped writing the whole store. It is the same claim as the decline band
/// below, counted once at the cost and once at the ratio: a 1% fixed term declines 0.9912x over
/// this corpus step and a 10% one declines 0.9102x. The 0.28% drift on the large arm moves this
/// share by about 0.30 of a percentage point, so the floor clears that by 9x and the ceiling by
/// 21x.
const FIXED_TERM_SHARE_FLOOR: f64 = 0.01;
const FIXED_TERM_SHARE_CEILING: f64 = 0.10;
/// Band on the PROPORTIONAL arm's written-per-byte-released, large over small. Measured 0.9663x.
///
/// This arm was asserted FLAT to within 1% until 2026-10-06, which is false about a correct
/// store: an affine cost over a proportional release DECLINES as the fixed term is amortised. The
/// CEILING is therefore the live edge here -- it is what refuses flatness -- and the FLOOR refuses
/// a climb, which is the control's original job. Only the large arm's numerator moves this ratio,
/// at 0.28%, and the measured value sits 2.87% below the ceiling -- about 10.6x that drift.
const PROPORTIONAL_DECLINE_FLOOR: f64 = 0.90;
const PROPORTIONAL_DECLINE_CEILING: f64 = 0.995;

/// Set the rolling threshold for THIS THREAD and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        crate::index_log::set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    crate::index_log::set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// The kernel's own count of bytes THIS THREAD has written, from outside every book the engine
/// keeps. `None` is an apparatus failure at every call site rather than a zero.
///
/// `/proc/thread-self/io`, not `/proc/self/io`. The process-wide counter was the first thing
/// tried and it measures the other tenants: run alone the residual below is 0 at every size, and
/// run inside the whole lib gate the same measurement charged this dump 3,194 bytes it never
/// wrote -- a background thread some earlier test left running, writing while the span was open.
/// A residual is only independent of the thing it audits if it is also independent of everything
/// ELSE sharing the process.
fn bytes_written_now() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("wchar:") {
            return value.trim().parse().ok();
        }
    }
    None
}

/// Every file under `root`, by path, with its length and mtime.
fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
            } else {
                let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                out.insert(path, (meta.len(), modified));
            }
        }
    }
    out
}

/// The files that are new or whose length or mtime moved between two snapshots, with the length
/// each ended at. THIS is the attributed row: it names what the operation touched without asking
/// the operation, so the residual beside it is a difference of two independent measurements and
/// not a sum of the rows it audits.
fn changed_files(
    before: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    after: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for (path, (len, modified)) in after {
        match before.get(path) {
            Some(prior) if prior == &(*len, *modified) => {}
            _ => out.push((
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("?")
                    .to_string(),
                *len,
            )),
        }
    }
    out
}

fn release_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

fn seed_in_batches_of(engine: &TemporalEngine, from: usize, to: usize, batch: usize) {
    let mut index = from;
    while index < to {
        let end = (index + batch).min(to);
        let commands = (index..end)
            .map(|cursor| Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; VALUE_LEN],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

fn seed_range(engine: &TemporalEngine, from: usize, to: usize) {
    seed_in_batches_of(engine, from, to, SEED_BATCH);
}

fn seed_one_at_a_time(engine: &TemporalEngine, from: usize, to: usize) {
    for index in from..to {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; VALUE_LEN],
            },
        });
        assert!(response.status.ok, "write failed: {:?}", response.status);
    }
}

/// The sealed index-log pieces, read off the directory BY THIS TEST rather than through the store.
///
/// A piece is named `shard-{shard}.indexlog.{start}-{end}-{max_applied_wal}.bin`. The third field
/// is the highest WAL sequence any record in the piece reflects, and it is the one field the
/// reclaim compares against the dump's anchor -- so a fixture whose pieces all carry the same
/// value there cannot tell a correct reclaim from a constant, and the fixture assertion below
/// requires every piece to name a distinct one.
fn sealed_pieces(index_dir: &Path) -> Vec<(u64, u64, u64, u64)> {
    let root = index_dir.join("indexlogs");
    let prefix = format!("shard-{SHARD}.indexlog.");
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(middle) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Some(middle) = middle
            .strip_suffix(".bin")
            .or_else(|| middle.strip_suffix(".jsonl"))
        else {
            continue;
        };
        let parts = middle.split('-').collect::<Vec<_>>();
        if parts.len() != 3 {
            continue;
        }
        let (Ok(start), Ok(end), Ok(max_applied)) = (
            parts[0].parse::<u64>(),
            parts[1].parse::<u64>(),
            parts[2].parse::<u64>(),
        ) else {
            continue;
        };
        let len = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        out.push((start, end, max_applied, len));
    }
    out.sort();
    out
}

/// One measured dump, with its cost and its release side by side.
#[derive(Debug)]
struct DumpAtSize {
    corpus: usize,
    suffix: usize,
    /// COST, from the kernel.
    kernel_bytes_written: u64,
    /// COST, attributed to named files by two directory walks this test does.
    attributed_bytes: u64,
    residual_bytes: i64,
    base_index_bytes: u64,
    barriers: u64,
    /// Pieces the log was in when the dump started.
    ///
    /// A reclaim unlinks whole pieces below its floor and fsyncs the directory entry once for the
    /// unlink; a log in ONE piece has no whole piece to unlink and never takes that barrier. Until
    /// the batch append started rolling, every log here was one piece, so the barrier count could
    /// be compared raw. It is the condition, not the barrier, that is recorded -- `sync_parent_dir`
    /// records every directory fsync under one name.
    wal_pieces_before: usize,
    dir_listings: u64,
    piece_paths: u64,
    /// FIXTURE.
    pieces_before: usize,
    sealed_before: usize,
    distinct_reflected_anchors: usize,
    undumped_before: u64,
    wal_records_before: usize,
    wal_bytes_on_disk_before: u64,
    /// RELEASE.
    index_log_released: u64,
    index_log_records_removed: usize,
    pieces_after: usize,
    wal_released: u64,
    wal_records_removed: usize,
    /// Whether the sweep errored. Its `Result` is dropped with `.ok()`, so without this the
    /// three fields above are defaults that read exactly like a reading.
    wal_sweep_failed: bool,
}

impl DumpAtSize {
    /// Bytes the dump wrote for each byte it freed. The whole question in one number.
    fn written_per_byte_released(&self) -> f64 {
        assert!(
            self.index_log_released > 0,
            "APPARATUS: a dump that released nothing cannot price what a release costs \
             (corpus {}, suffix {})",
            self.corpus,
            self.suffix
        );
        self.kernel_bytes_written as f64 / self.index_log_released as f64
    }

    fn index_log_bytes_per_record(&self) -> f64 {
        assert!(self.suffix > 0, "APPARATUS: empty suffix");
        self.undumped_before as f64 / self.suffix as f64
    }
}

/// Seed `corpus` records, dump once so the measured dump is a steady-state dump and not a first
/// one, write `suffix` more, then measure the dump that follows.
fn measure_dump(dir: &Path, corpus: usize, suffix: usize) -> DumpAtSize {
    let engine = release_engine(dir);
    let index_dir = dir.join("indexes");
