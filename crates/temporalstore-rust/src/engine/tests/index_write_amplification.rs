// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE INDEX WRITES PER BYTE OF INDEX IT LOGS, ON THE ROUTING RANGE A DEPLOYMENT RUNS.
//!
//! # THE NUMBER THIS MODULE EXISTS TO STATE
//!
//! Write amplification on the index, as ONE ratio of two physical byte counts taken off the
//! filesystem:
//!
//! ```text
//!   a = bytes this store writes under `indexes/` in a steady-state round
//!       --------------------------------------------------------------
//!       bytes of index-log record frame those same writes appended
//! ```
//!
//! BOTH SIDES ARE PHYSICAL BYTES ON DISK, which is the only surface a write-amplification figure
//! can live on. The three surfaces the per-object index figures are quoted on -- the json stream,
//! the msgpack payload and the `TSIDX\x01` container -- are surfaces an ENCODER produces, and a
//! ratio that took its numerator off one and its denominator off another would be a unit
//! conversion wearing a ratio's clothes. Where this module does quote a per-object figure it says
//! which surface it is on: the base index on disk is the `TSIDX\x01` container, so it is a
//! COMPRESSED size and names no field; the index-log record frames are the store's own binary
//! framing over a json payload.
//!
//! # WHAT THE NUMERATOR IS MADE OF, BY THE NAME OF THE FILE EACH BYTE LANDS IN
//!
//! Four terms, each one asserted NON-ZERO where the round can produce it, because a decomposition
//! can be wrong in every term and still sum to the whole:
//!
//! - `base_index` -- `shard-{id}.index.json`, the whole served index, rewritten entire by a dump.
//! - `log_appended` -- index-log record frames appended by the writes. This is also the
//!   DENOMINATOR, and it is read from the store's own `IndexLogStats::bytes_written` so the
//!   numerator and the denominator do not come from one book.
//! - `log_rewritten` -- what the post-dump sweep COPIES FORWARD into the piece it is writing. The
//!   sweep drops whole sealed pieces by name without opening them and rewrites only the active
//!   piece, so this term is bounded by the rolling threshold and not by the store.
//! - `other` -- everything else that changed under `indexes/`, NAMED BY FILE, because a term
//!   called "other" is where a surprise hides. The first run of this module read 263,8xx B here
//!   at every store size and every key shape, flat across a 4x corpus, and it was the write-ahead
//!   log: `with_cache_block_store_and_index_dir` puts the log at `indexes/wals/`, so a walk of
//!   `indexes/` charges a 256 KiB log piece to the index and reads a=16.9 where the index's own
//!   writes read 9.09. The `wals/` subtree is excluded by path and reported BESIDE the numerator
//!   rather than silently dropped, and a write phase asserts it NON-ZERO -- an exclusion that
//!   quietly stopped matching anything would take the WAL back into the index figure.
//!
//! EVERY PHASE NAMES A SECOND BOOK, and the two are asserted EQUAL rather than close -- a
//! tolerance here would admit exactly the mis-sized rewrite the numerator is built on:
//!
//! - a WRITE phase appends and nothing else, so the walk's length change must equal the store's
//!   append counter to the byte. This is what stops the counter being believed on its own, and it
//!   holds because a roll is a `fs::rename` and never a copy.
//! - a DUMP phase is checked against the SWEEP'S OWN byte report: the sweep states the log's
//!   length on either side of itself from inside the store, and this walk measures the same two
//!   lengths from outside it.
//!
//! What LEFT the log is then taken as a difference and never as a sum of per-file rows. A per-file
//! sum cannot tell the catalog anchor a dump appends from the anchor the sweep then keeps, and the
//! first draft of this module was wrong by exactly that one record.
//!
//! # WHY THE ROUTING RANGE IS NAMED IN EVERY ARM
//!
//! `load_shard` leaves the range at `0..u32::MAX`, on which a key's routing bucket is effectively
//! unique -- buckets come out EQUAL to objects, so every per-bucket cost divides by one object and
//! the index is a different shape from the one a meta-server-managed shard holds. Every arm here
//! loads `0..1023` and ASSERTS the amortised regime it claims to be in before it reports anything:
//! 8,000 objects over 1,024 buckets is 7.81 per bucket, and an arm that found one object per
//! bucket would be measuring the unbounded range and would say so.
//!
//! # WHY THE SIZES ARE SWEPT AND THE SLOPE IS REPORTED
//!
//! A per-key figure over a hash table is a sawtooth, and a division at one size is not a slope.
//! The sizes are 2,000 / 8,000 / 32,000 objects, a 16x range, and what is reported is the fitted
//! slope of bytes written per object beside the division at each point. `a` itself is
//! dimensionless, so what the sweep shows about `a` is whether it MOVES -- and whether it moves is
//! the whole question, because the shape being priced is "write the whole index, occasionally".
//!
//! # WHY TWO KEY SHAPES
//!
//! The base index on disk is a zstd frame, so a run of identical padding bytes is nearly free in
//! it: a padded fixture once measured a key at 0.009 B per byte of key, which is a statement about
//! the padding and not about the store. Every figure here is taken on a padded key AND on a
//! scattered one, and the two are printed side by side.
//!
//! # THE SPACE SIDE, AND THE BOUND THE TWO SIT ON TOGETHER
//!
//! The index is held as a base image plus an append-only log of records that have not yet been
//! folded into it. A dump folds the log into the base and the sweep then drops what the base now
//! reflects, so the two together are a window that slides: the base is what the window has already
//! absorbed, the log is what is still ahead of it, and reclamation is the deletion of a whole dead
//! piece by name rather than any compaction of one.
//!
//! Space amplification is then
//!
//! ```text
//!   b = (base index on disk + the log at its high-water mark) / base index on disk
//! ```
//!
//! and for a window that absorbs its log at a threshold proportional to the base, the two are one
//! relation rather than two independent numbers:
//!
//! ```text
//!   a <= b / (b - 1)
//! ```
//!
//! `the_measured_amplification_tracks_the_space_amplification_bound` drives the threshold divisor
//! across a sweep and checks the measured `a` against the bound `b` implies at each point, so the
//! relation is a measurement and not an argument. b = 2 admits 2x, b = 5 admits 1.25x, b = 10
//! admits 1.11x: it is a tunable trade of disk for bytes written, and what the sweep says about
//! WHERE the shipped divisor sits on that curve is this module's result.
//!
//! # COUNTED, NEVER TIMED
//!
//! Every number here is a byte count read off the filesystem or off a store counter. This box
//! carries other tenants and sits between load 2 and load 30 for hours at a time; the same round
//! has been seen at 490 ms and at 1,077 ms. A count does not move with load.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const SHARD: ShardId = 1;
/// The shipped routing range's end bucket -- the default an operator is told to set, and the one
/// regime in which the index's per-bucket terms amortise over the objects sharing a bucket.
const SHIPPED_END_BUCKET: u32 = 1023;
/// Buckets that range holds.
const SHIPPED_BUCKETS: usize = 1024;
/// The store sizes every figure is swept over, 8x apart end to end.
///
/// What showing a dimensionless ratio does NOT move needs a RANGE, not a large absolute size, and
/// the absolute cost of a dump at 20,000 and 200,000 records is already priced by
/// `dump_release_scale`. A 32,000-object top arm was measured and dropped: it reads the same
/// `a/bound` to three figures and adds minutes to every run of this suite forever.
const SIZES: [usize; 3] = [2_000, 8_000, 16_000];
/// The size the published per-object index figures are taken at, and the one this module quotes
/// its single-point division at. 7,999 objects over 1,024 buckets is the 7.81-per-bucket regime.
const PUBLISHED_SIZE: usize = 8_000;
/// Fixture key width, including the seven-byte ordinal tail.
const KEY_WIDTH: usize = 32;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';
const VALUE_LEN: usize = 128;
const SEED_BATCH: usize = 500;
/// The rolling threshold, held FIXED across every arm. A deployment knob and not what is under
/// measurement; smaller than the shipped 64 KiB so that a fixture rolls into many pieces -- the
/// sweep's drop-a-whole-piece decision is PER PIECE and a log in one piece cannot price it.
///
/// 1 KiB rather than the 8 KiB a neighbouring fixture uses, because the threshold the SMALLEST arm
/// here holds out for is an eighth of a small base index: at 8 KiB the 2,000-object round fired
/// with the log still in its first piece and `assert_round_can_express_it` refused it, which is
/// that clause doing its job rather than a tuning accident.
const ROLL_BYTES: u64 = 1024;
/// The configured accrual FLOOR every arm passes. Small enough that the relative term binds at
/// every size and every divisor in the sweep, which each arm asserts rather than assumes.
const FLOOR_BYTES: u64 = 2 * 1024;
/// Records written between two cadence checks.
///
/// Small against the smallest threshold any arm holds out for, so a round OVERSHOOTS the threshold
/// by at most one batch. The overshoot is not cosmetic: a round that released 48% more than it
/// held out for -- which 100-record batches did at the 2,000-object arm -- reads a SMALLER `a` and
/// a LARGER `b` than the cadence chose, and the two move together, which is how an arm can sit on
/// the bound for a reason the cadence had nothing to do with.
const BATCH: usize = 25;
/// A round that has not fired after this many batches is an apparatus failure, not a zero.
const MAX_BATCHES: usize = 4_000;
/// Measured rounds per arm in the STORE sweep. Two, so the reported figure is not one round's.
const ROUNDS: usize = 2;
/// The ordinal the fixture never writes, so an absent read is a control and not an absence.
const WITHHELD_ORDINAL: usize = 7;
/// The divisors the bound sweep drives. Each one is a different space-amplification budget: the
/// threshold a dump holds out for is `base_index / divisor`, so the log may stand at `1/divisor`
/// of the base and the budget is `b = 1 + 1/divisor`.
///
/// 32 at the tight end and 2 at the loose one, which spans a bound of about 3 to about 33 -- a
/// factor of eleven, so an arm that holds the bound is not holding the same number as every other
/// arm. Divisor 1 is NOT in the sweep and was dropped after being measured: it admits a log as
/// large as the whole base, so each round roughly doubles the store and the arm's THIRD round
/// wrote 35,000 records. It read a=3.286 against a bound of 3.285 -- a/bound 1.0002, the same
/// answer as every arm here -- and it cost more than the other four together.
const DIVISOR_SWEEP: [u64; 5] = [2, 4, 8, 16, 32];
/// Measured rounds per BUDGET arm. One, not [`ROUNDS`]: the priming dump already makes the first
/// measured round a steady-state round, the relation under test is a per-round one, and a budget
/// arm's round grows the store by `1/divisor` -- so the loose end of the sweep compounds with
/// every extra round while saying nothing new. Round-to-round stability is the STORE sweep's
/// claim, where every arm is on the shipped divisor and the store grows by an eighth a round.
const BUDGET_ROUNDS: usize = 1;

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

/// What a fixture key's leading bytes are made of.
///
/// The base index on disk is a zstd frame, so the two shapes bracket the one thing that depends on
/// a key's CONTENT rather than its width.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run -- compressible, and the shape a fixture reaches for by default.
    Padded,
    /// Pseudorandom letters, derived from the ordinal so the fixture is reproducible.
    Scattered,
}

impl KeyShape {
    fn label(self) -> &'static str {
        match self {
            KeyShape::Padded => "padded",
            KeyShape::Scattered => "scattered",
        }
    }
}

const SHAPES: [KeyShape; 2] = [KeyShape::Padded, KeyShape::Scattered];

/// A key of exactly [`KEY_WIDTH`] bytes whose last six characters are `ordinal`.
///
/// The ordinal is fixed-width so no key is a substring of another, and the padding is DERIVED from
/// the ordinal rather than stored, so a reader can rebuild the bytes a given ordinal's key would
/// have had.
fn key_text(ordinal: usize, shape: KeyShape) -> String {
    let pad = KEY_WIDTH - TAIL_WIDTH;
    let head: Vec<u8> = match shape {
        KeyShape::Padded => vec![PAD_BYTE; pad],
        KeyShape::Scattered => {
            let mut state =
                (ordinal as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
            state |= 1;
            (0..pad)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    b'a' + (state % 26) as u8
                })
                .collect()
        }
    };
    let mut text = String::from_utf8(head).expect("the padding is ASCII by construction");
    text.push_str(&format!("-{ordinal:06}"));
    assert_eq!(
        text.len(),
        KEY_WIDTH,
        "APPARATUS: a fixture key must be exactly {KEY_WIDTH} bytes"
    );
    text
}

/// An engine on the SHIPPED routing range. The range is named rather than defaulted: the range
/// `load_shard` leaves behind puts one bucket per object and is a different index shape.
fn shipped_range_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: "index-write-amplification".to_string(),
        shard_uri: "local://index-write-amplification/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "APPARATUS: the fixture could not load shard {SHARD} on 0..{SHIPPED_END_BUCKET}: {:?}",
        response.status
    );
    engine
}

/// Write ordinals `from..to`, skipping [`WITHHELD_ORDINAL`].
fn seed_range(engine: &TemporalEngine, from: usize, to: usize, shape: KeyShape) -> usize {
    let mut written = 0usize;
    let mut cursor = from;
    while cursor < to {
        let end = (cursor + SEED_BATCH).min(to);
        let commands = (cursor..end)
            .filter(|ordinal| *ordinal != WITHHELD_ORDINAL)
            .map(|ordinal| Command::StringSet {
                key: key_text(ordinal, shape),
                value: vec![b'v'; VALUE_LEN],
            })
            .collect::<Vec<_>>();
        written += commands.len();
        if !commands.is_empty() {
            let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: SHARD,
                commands,
            });
            assert!(
                response.status.ok,
                "APPARATUS: a seed batch failed: {:?}",
                response.status
            );
        }
        cursor = end;
    }
    written
}

/// Every file under `root`, by path, with its length and mtime.
///
/// Taken by THIS module off the filesystem rather than asked of the store, so the numerator is an
/// independent measurement of what the store did and not a sum of rows the store reports.
fn index_tree(root: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
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
                out.insert(
                    path,
                    (meta.len(), meta.modified().unwrap_or(std::time::UNIX_EPOCH)),
                );
            }
        }
    }
    out
}

/// Which class a file under the store's index root belongs to, decided by its PATH.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum IndexFile {
    /// `shard-{id}.index.json` -- the whole served index, rewritten entire.
    Base,
    /// `indexlogs/shard-{id}.indexlog*.bin` -- append-only, except the sweep's rewrite of the
    /// active piece.
    Log,
    /// `wals/` -- NOT THE INDEX. The write-ahead log shares the index root (see
    /// `with_cache_block_store_and_index_dir`), so it has to be excluded by path, and it is
    /// reported beside the numerator rather than dropped in silence.
    WriteAheadLog,
    Other,
}

fn classify(path: &Path) -> IndexFile {
    // The subtree first: a WAL piece carries neither of the two names below, so a name-only
    // classification put it in `Other` and it stayed there, 256 KiB a round.
    if path
        .components()
        .any(|part| part.as_os_str() == std::ffi::OsStr::new("wals"))
    {
        return IndexFile::WriteAheadLog;
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if format!("shard-{SHARD}.index.json") == name {
        IndexFile::Base
    } else if name.contains(".indexlog") {
        IndexFile::Log
    } else {
        IndexFile::Other
    }
}

/// Total length of every index-log piece in a snapshot.
fn log_total(tree: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Log)
        .map(|(_, (len, _))| *len)
        .sum()
}

/// The physical bytes written to the index across one phase, split by the file each landed in.
#[derive(Debug, Default, Clone)]
struct IndexBytes {
    base_index: u64,
    log_appended: u64,
    log_rewritten: u64,
    other: u64,
    /// The `other` term BY FILE NAME. A term called "other" is where a surprise hides, so it is
    /// never reported as one number.
    other_by_name: Vec<(String, u64)>,
    /// NOT PART OF THE NUMERATOR. The write-ahead log shares the index root, so it is measured and
    /// reported here to make the exclusion auditable instead of invisible.
    write_ahead_log: u64,
    /// Bytes of index log that LEFT the tree: whole sealed pieces unlinked by name, plus whatever
    /// the sweep's rewrite of the active piece did not carry forward. Not part of the numerator
    /// either -- it is what the window dropped behind itself.
    log_dropped: u64,
}

impl IndexBytes {
    /// THE NUMERATOR. The write-ahead log is not in it, and neither is what was dropped.
    fn total(&self) -> u64 {
        self.base_index + self.log_appended + self.log_rewritten + self.other
    }
}

/// Which phase an accounting covers, and therefore which second book checks it.
///
/// A phase that named no second book would be one measurement believed on its own.
#[derive(Debug, Clone, Copy)]
enum Phase {
    /// Pure writes. Nothing may leave the log and nothing may be rewritten, so the walk and the
    /// store's append counter must agree to the byte -- which is the check.
    Writes,
    /// A dump and the sweep behind it, checked against the SWEEP'S OWN byte report. The sweep runs
    /// after the dump has appended its catalog anchor, so its `bytes_before` is the walk's before
    /// plus that append, and its `bytes_after` is the walk's after.
    Dump { bytes_before: u64, bytes_after: u64 },
    /// A planted phase, which states what it planted.
    Planted { dropped: u64 },
}

/// What a phase wrote, from the two tree snapshots that bracket it plus the store's own
/// append counter.
///
/// `log_appended` is the store's counter and NOT the walk: the walk cannot tell an append from a
/// rewrite. The two books are then reconciled against whichever second book the [`Phase`] names,
/// never by deriving one from the other.
fn account(
    before: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    after: &BTreeMap<PathBuf, (u64, std::time::SystemTime)>,
    log_appended: u64,
    phase: Phase,
) -> IndexBytes {
    let mut out = IndexBytes {
        log_appended,
        ..IndexBytes::default()
    };
    for (path, (len, modified)) in after {
        let prior = before.get(path);
        let unchanged = prior == Some(&(*len, *modified));
        match classify(path) {
            IndexFile::Base => {
                if !unchanged {
                    // A whole rewrite: the dump serializes the served index and replaces the file,
                    // so what it wrote is the length it ended at and never a delta.
                    out.base_index += *len;
                }
            }
            IndexFile::Log => match prior {
                // Shrank: the sweep wrote a temporary holding what it carried FORWARD and renamed
                // it over the piece. What it WROTE is the length it ended at. This is the
                // rewrite-forward term, and the sweep reaches only the piece being written -- a
                // sealed piece is dropped by name without being opened -- so it is bounded by the
                // rolling threshold and not by the store.
                Some((was, _)) if *len < *was => out.log_rewritten += *len,
                // Grew, or is new because a roll renamed the active piece aside. Either way the
                // bytes are already in the store's append counter, so nothing is added here --
                // counting them again is exactly the double count the checks below refuse.
                _ => {}
            },
            IndexFile::WriteAheadLog => {
                if !unchanged {
                    out.write_ahead_log += *len;
                }
            }
            IndexFile::Other => {
                if !unchanged {
                    out.other += *len;
                    out.other_by_name.push((
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("?")
                            .to_string(),
                        *len,
                    ));
                }
            }
        }
    }
    // WHAT LEFT THE LOG, as a difference and not a sum of rows: everything that was there plus
    // everything appended, less what is there now. Whole sealed pieces unlinked by name land here
    // together with whatever the active piece's rewrite did not carry forward, because a per-file
    // sum cannot tell the anchor the dump appended from the anchor the sweep then kept.
    let had = log_total(before) + out.log_appended;
    let holds = log_total(after);
    assert!(
        had >= holds,
        "APPARATUS: the index log holds {holds} B after a phase in which it held {} B and had \
         {} B appended. It cannot hold more than was put into it, so one of the two books is \
         measuring something else",
        log_total(before),
        out.log_appended,
    );
    out.log_dropped = had - holds;
    match phase {
        Phase::Writes => {
            assert_eq!(
                out.log_dropped, 0,
                "APPARATUS: a WRITE phase dropped {} B of index log. Only a sweep drops, and a \
                 sweep does not run here, so the walk ({} B -> {holds} B) and the store's append \
                 counter ({} B) disagree",
                out.log_dropped,
                log_total(before),
                out.log_appended,
            );
            assert_eq!(
                out.log_rewritten, 0,
                "APPARATUS: a WRITE phase rewrote {} B of index log. An append-only phase shrinks \
                 no piece",
                out.log_rewritten,
            );
        }
        Phase::Dump {
            bytes_before,
            bytes_after,
        } => {
            // THE SECOND BOOK. The sweep reports the log's length on either side of itself from
            // inside the store; this walk measures the same two lengths from outside it. They are
            // asserted EQUAL rather than close: a tolerance here would admit exactly the
            // mis-sized rewrite the numerator is built on.
            assert_eq!(
                bytes_before, had,
                "APPARATUS: the sweep reports the log at {bytes_before} B before it ran; the walk \
                 measured {} B plus {} B the dump appended = {had} B",
                log_total(before),
                out.log_appended,
            );
            assert_eq!(
                bytes_after, holds,
                "APPARATUS: the sweep reports the log at {bytes_after} B after it ran; the walk \
                 measured {holds} B"
            );
        }
        Phase::Planted { dropped } => assert_eq!(
            out.log_dropped, dropped,
            "the walk attributed {} B dropped against a planted {dropped} B",
            out.log_dropped,
        ),
    }
    out
}

