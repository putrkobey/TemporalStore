// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::block_store::ElementEntry;
use crate::types::ShardId;

/// Counters for the scale probes, tests only.
///
/// Every quantity here is a COUNT, not a duration. This box sits between load 2 and load 30 for
/// hours at a time, and a duration taken on it says as much about the neighbours as about the
/// code -- the same round has been measured at 490 ms and at 1,077 ms. A count does not move.
///
/// Each counter sits where EVERY path through the thing it counts must pass:
/// `index_log_segment_paths` is the only enumeration of a shard's pieces, and the frame counter
/// is incremented before the whitespace `continue` rather than after it, so a frame skipped by
/// the fold is still a frame the fold read.
///
/// Process-global, so the probes that read them require `--test-threads=1` -- which is how this
/// suite is run. Each probe resets immediately before the call it measures.
#[cfg(test)]
pub(crate) mod probe {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;

    use crate::types::ShardId;

    /// Calls to [`super::index_log_segment_paths`] -- one directory listing each.
    pub(crate) static DIR_LISTINGS: AtomicU64 = AtomicU64::new(0);
    /// Piece paths those listings HANDED BACK, summed. This is the per-piece quantity: every
    /// caller of the enumeration then stats, opens or compares each path it got.
    pub(crate) static PIECE_PATHS: AtomicU64 = AtomicU64::new(0);
    /// Frames [`super::LocalIndexLogStore::for_each_delta_record`] read off disk.
    pub(crate) static FOLD_FRAMES_READ: AtomicU64 = AtomicU64::new(0);
    /// Sealed pieces that fold DECLINED TO OPEN because their name said the dumped base
    /// already reflects every record in them.
    ///
    /// Counted only under [`super::LocalIndexLogStore::for_each_delta_record_above_anchor`],
    /// which is the load-path fold. Every other walk passes anchor 0 and declines nothing, so a
    /// zero here from those callers is the right answer rather than a silent one.
    pub(crate) static FOLD_PIECES_DECLINED: AtomicU64 = AtomicU64::new(0);
    /// Times an append actually asked the filesystem to create the log directory -- one
    /// `mkdir` and, when it returns `EEXIST`, one `statx` behind it.
    pub(crate) static APPEND_ROOT_CREATES: AtomicU64 = AtomicU64::new(0);
    /// Times an append resolved WHICH FILE the shard's log is being written to. Each pass is
    /// at least one `statx`, and the append used to make two of them per record.
    pub(crate) static APPEND_PATH_PROBES: AtomicU64 = AtomicU64::new(0);
    /// Entries to the post-dump sweep,
    /// [`super::LocalIndexLogStore::gc_reflected_before_anchor`] -- counted on the FIRST line of
    /// the function, before any early return, so a shard with no log on disk is still an entry.
    ///
    /// Which of the two sweeps a deployment actually drives is otherwise argued rather than
    /// measured, and an argument about a cadence is exactly the shape that has been wrong here.
    pub(crate) static SWEEP_ENTRIES_POST_DUMP: AtomicU64 = AtomicU64::new(0);
    /// Entries to the budgeted sweep,
    /// [`super::LocalIndexLogStore::gc_before_sequence_limited`], on the same terms.
    pub(crate) static SWEEP_ENTRIES_BUDGETED: AtomicU64 = AtomicU64::new(0);
    /// Frames either sweep READ off the piece being written.
    ///
    /// This is the quantity a per-round entry budget does NOT touch: both loops read the piece
    /// to the end whatever the budget says, because the budget decides which side of the rewrite
    /// a record lands on rather than whether the loop continues.
    pub(crate) static SWEEP_FRAMES_READ: AtomicU64 = AtomicU64::new(0);
    /// Frames either sweep COPIED into the rewritten piece.
    ///
    /// Counted where the bytes are written, not from `retained.len()`: a change that builds the
    /// retained list correctly and then writes something else leaves the list untouched.
    pub(crate) static SWEEP_FRAMES_COPIED: AtomicU64 = AtomicU64::new(0);
    /// The sequences a sweep removed from the piece being written, in the order it decided them.
    ///
    /// ARMED EXPLICITLY, for the same reason [`FOLD_DECISIONS`] is: the scale fixtures measure
    /// what a sweep holds at tens of thousands of records, and a Vec that grows with the corpus
    /// would be measured along with it.
    pub(crate) static SWEEP_REMOVED_ARMED: AtomicBool = AtomicBool::new(false);
    pub(crate) static SWEEP_REMOVED: Mutex<Vec<(ShardId, u64)>> = Mutex::new(Vec::new());
    /// Appends that COMPLETED, counted beside `inner.stats.writes` at all three append sites --
    /// so it is incremented while the appender holds the very lock a sweep takes.
    pub(crate) static APPENDS_COMPLETED: AtomicU64 = AtomicU64::new(0);
    /// [`APPENDS_COMPLETED`] as a sweep read it at the moment it took the lock, and again at the
    /// last statement before it dropped it.
    ///
    /// The difference is how many appends landed WHILE the sweep held the lock, with no timing in
    /// it: not "few", not "few relative to a control window", but a number that is 0 if the lock
    /// is held and climbs the moment it is not. The ratio this replaces was load-dependent --
    /// 485x on an idle box and 11x on a busy one, off the same code.
    pub(crate) static SWEEP_APPENDS_AT_LOCK: AtomicU64 = AtomicU64::new(0);
    pub(crate) static SWEEP_APPENDS_AT_RELEASE: AtomicU64 = AtomicU64::new(0);

    /// Takes the two readings above. Declared AFTER the lock guard so it drops BEFORE it, which
    /// is what makes the second reading a reading taken while the lock is still held.
    pub(crate) struct SweepLockSpan;

    impl SweepLockSpan {
        pub(crate) fn new() -> Self {
            SWEEP_APPENDS_AT_LOCK.store(APPENDS_COMPLETED.load(Ordering::SeqCst), Ordering::SeqCst);
            SWEEP_APPENDS_AT_RELEASE
                .store(APPENDS_COMPLETED.load(Ordering::SeqCst), Ordering::SeqCst);
            SweepLockSpan
        }
    }

    impl Drop for SweepLockSpan {
        fn drop(&mut self) {
            SWEEP_APPENDS_AT_RELEASE
                .store(APPENDS_COMPLETED.load(Ordering::SeqCst), Ordering::SeqCst);
        }
    }

    pub(crate) fn appends_completed() -> u64 {
        APPENDS_COMPLETED.load(Ordering::SeqCst)
    }
    /// Appends that landed while the last sweep held the lock.
    pub(crate) fn appends_during_the_sweep_lock() -> u64 {
        SWEEP_APPENDS_AT_RELEASE
            .load(Ordering::SeqCst)
            .saturating_sub(SWEEP_APPENDS_AT_LOCK.load(Ordering::SeqCst))
    }
    /// Records the load-path fold was asked about, and what it was told.
    ///
    /// Counted inside [`super::delta_record_is_reflected_by`] rather than at the fold, so a
    /// second caller of that predicate cannot ask the question without being counted here.
    pub(crate) static FOLD_RECORDS_TESTED: AtomicU64 = AtomicU64::new(0);
    /// Of those, the ones that carry NO anchor and carry something the fold would apply, asked
    /// about by a base that anchors somewhere. This is the population #1832 and #1842 are about,
    /// met at the third site; a zero here is "the fold never saw one", not "it saw one and kept
    /// it".
    pub(crate) static FOLD_ANCHORLESS_WITH_CONTENT: AtomicU64 = AtomicU64::new(0);
    /// The per-record decision sequence, in the order the fold made it: (shard, sequence,
    /// reflected). A test compares this ELEMENT BY ELEMENT against a control, because a count
    /// is equally happy when the right number of the wrong records is kept.
    ///
    /// ARMED EXPLICITLY. The fold-cost fixtures in `index_log_scale` measure the memory the
    /// fold holds at 10,000 records; a Vec that grows with the corpus would be measured along
    /// with it. Unarmed this is one relaxed load per record and nothing else.
    pub(crate) static FOLD_DECISIONS_ARMED: AtomicBool = AtomicBool::new(false);
    pub(crate) static FOLD_DECISIONS: Mutex<Vec<(ShardId, u64, bool)>> = Mutex::new(Vec::new());
    /// The records the fold actually APPLIED, in order, as (shard, sequence).
    ///
    /// Recorded where the WORK LANDS -- after the page items are re-attached and the key-states
    /// are applied -- and not where the decision is taken. The two are separate on purpose: a
    /// change that asks the predicate and then ignores the answer leaves [`FOLD_DECISIONS`]
    /// exactly as it was, and is visible only here.
    pub(crate) static FOLD_APPLIED: Mutex<Vec<(ShardId, u64)>> = Mutex::new(Vec::new());

    /// Start recording the decision and application sequences, from empty. Put back by
    /// [`disarm_decisions`].
    pub(crate) fn arm_decisions() {
        FOLD_DECISIONS
            .lock()
            .expect("fold decision probe poisoned")
            .clear();
        FOLD_APPLIED
            .lock()
            .expect("fold decision probe poisoned")
            .clear();
        FOLD_DECISIONS_ARMED.store(true, Ordering::Relaxed);
    }

    /// Note that the fold APPLIED a record. Called from inside the fold, after the work.
    pub(crate) fn note_fold_applied(shard_id: ShardId, sequence: u64) {
        if FOLD_DECISIONS_ARMED.load(Ordering::Relaxed) {
            FOLD_APPLIED
                .lock()
                .expect("fold decision probe poisoned")
                .push((shard_id, sequence));
        }
    }

    pub(crate) fn applied() -> Vec<(ShardId, u64)> {
        FOLD_APPLIED
            .lock()
            .expect("fold decision probe poisoned")
            .clone()
    }

    pub(crate) fn disarm_decisions() {
        FOLD_DECISIONS_ARMED.store(false, Ordering::Relaxed);
    }

    pub(crate) fn decisions() -> Vec<(ShardId, u64, bool)> {
        FOLD_DECISIONS
            .lock()
            .expect("fold decision probe poisoned")
            .clone()
    }

    pub(crate) fn fold_records_tested() -> u64 {
        FOLD_RECORDS_TESTED.load(Ordering::Relaxed)
    }

    pub(crate) fn fold_anchorless_with_content() -> u64 {
        FOLD_ANCHORLESS_WITH_CONTENT.load(Ordering::Relaxed)
    }

    /// The counting half of [`super::delta_record_is_reflected_by`], kept beside the statics it
    /// moves so the predicate reads as the decision alone.
    pub(crate) fn note_fold_decision(
        record: &super::IndexDeltaRecord,
        base_anchor: u64,
        reflected: bool,
    ) {
        FOLD_RECORDS_TESTED.fetch_add(1, Ordering::Relaxed);
        if base_anchor > 0
            && record.applied_wal_sequence.is_none()
            && super::delta_record_carries_content_of(record)
        {
            FOLD_ANCHORLESS_WITH_CONTENT.fetch_add(1, Ordering::Relaxed);
        }
        if FOLD_DECISIONS_ARMED.load(Ordering::Relaxed) {
            FOLD_DECISIONS
                .lock()
                .expect("fold decision probe poisoned")
                .push((record.shard_id, record.sequence, reflected));
        }
    }

    pub(crate) fn reset() {
        DIR_LISTINGS.store(0, Ordering::Relaxed);
        PIECE_PATHS.store(0, Ordering::Relaxed);
        FOLD_FRAMES_READ.store(0, Ordering::Relaxed);
        FOLD_PIECES_DECLINED.store(0, Ordering::Relaxed);
        APPEND_ROOT_CREATES.store(0, Ordering::Relaxed);
        APPEND_PATH_PROBES.store(0, Ordering::Relaxed);
        SWEEP_ENTRIES_POST_DUMP.store(0, Ordering::Relaxed);
        SWEEP_ENTRIES_BUDGETED.store(0, Ordering::Relaxed);
        SWEEP_FRAMES_READ.store(0, Ordering::Relaxed);
        SWEEP_FRAMES_COPIED.store(0, Ordering::Relaxed);
        APPENDS_COMPLETED.store(0, Ordering::SeqCst);
        SWEEP_APPENDS_AT_LOCK.store(0, Ordering::SeqCst);
        SWEEP_APPENDS_AT_RELEASE.store(0, Ordering::SeqCst);
        SWEEP_REMOVED
            .lock()
            .expect("sweep removal probe poisoned")
            .clear();
        FOLD_RECORDS_TESTED.store(0, Ordering::Relaxed);
        FOLD_ANCHORLESS_WITH_CONTENT.store(0, Ordering::Relaxed);
        FOLD_DECISIONS
            .lock()
            .expect("fold decision probe poisoned")
            .clear();
        FOLD_APPLIED
            .lock()
            .expect("fold decision probe poisoned")
            .clear();
    }

    pub(crate) fn dir_listings() -> u64 {
        DIR_LISTINGS.load(Ordering::Relaxed)
    }
    pub(crate) fn piece_paths() -> u64 {
        PIECE_PATHS.load(Ordering::Relaxed)
    }
    pub(crate) fn fold_frames_read() -> u64 {
        FOLD_FRAMES_READ.load(Ordering::Relaxed)
    }
    pub(crate) fn fold_pieces_declined() -> u64 {
        FOLD_PIECES_DECLINED.load(Ordering::Relaxed)
    }
    pub(crate) fn append_root_creates() -> u64 {
        APPEND_ROOT_CREATES.load(Ordering::Relaxed)
    }
    pub(crate) fn append_path_probes() -> u64 {
        APPEND_PATH_PROBES.load(Ordering::Relaxed)
    }

    pub(crate) fn sweep_entries_post_dump() -> u64 {
        SWEEP_ENTRIES_POST_DUMP.load(Ordering::Relaxed)
    }
    pub(crate) fn sweep_entries_budgeted() -> u64 {
        SWEEP_ENTRIES_BUDGETED.load(Ordering::Relaxed)
    }
    pub(crate) fn sweep_frames_read() -> u64 {
        SWEEP_FRAMES_READ.load(Ordering::Relaxed)
    }
    pub(crate) fn sweep_frames_copied() -> u64 {
        SWEEP_FRAMES_COPIED.load(Ordering::Relaxed)
    }

    /// Start recording the removal sequence, from empty. Put back by [`disarm_removals`].
    pub(crate) fn arm_removals() {
        SWEEP_REMOVED
            .lock()
            .expect("sweep removal probe poisoned")
            .clear();
        SWEEP_REMOVED_ARMED.store(true, Ordering::Relaxed);
    }

    pub(crate) fn disarm_removals() {
        SWEEP_REMOVED_ARMED.store(false, Ordering::Relaxed);
    }

    /// Note that a sweep removed a record from the piece being written.
    pub(crate) fn note_sweep_removed(shard_id: ShardId, sequence: u64) {
        if SWEEP_REMOVED_ARMED.load(Ordering::Relaxed) {
            SWEEP_REMOVED
                .lock()
                .expect("sweep removal probe poisoned")
                .push((shard_id, sequence));
        }
    }

    pub(crate) fn removals() -> Vec<(ShardId, u64)> {
        SWEEP_REMOVED
            .lock()
            .expect("sweep removal probe poisoned")
            .clone()
    }
}

#[derive(Debug, Error)]
pub enum IndexLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed, newline-terminated served-index-log record failed its per-record
    /// integrity envelope (framing length / SHA-256 digest) or violated delta
    /// sequence-continuity. Surfaced as data loss so the load aborts instead of silently
    /// skipping a delta (an eviction/removal recorded ONLY in the delta -- not the WAL --
    /// would otherwise be lost, resurrecting the removed point or dangling its page ref).
    #[error("index-log record integrity error: {0}")]
    Corruption(String),
    /// A binary-container record could not be decoded: an unknown codec id (written by a
    /// newer binary) or a malformed payload. Distinct from `Corruption` because the framing
    /// envelope already passed -- the bytes arrived intact and this build cannot read them.
    #[error("index-log record encoding error: {0}")]
    Encoding(String),
}

/// Marks an index-log payload as a binary container rather than JSON.
///
/// A reader never has to be told which it is holding: a JSON record starts with `{`, a
/// container with this magic. That is the same discriminator the served-index container
/// uses, and it is what lets one log file hold both shapes while a deployment rolls.
/// Codec and shape, in one byte: codec in the high nibble, shape in the low one.
///
/// This replaced a nine-byte prefix -- a seven-byte magic, a codec byte and a shape byte -- on a
/// record averaging eighty-five bytes, so a tenth of the log was a header saying what the record
/// was. The magic was a second delimiter inside an already-delimited frame: every record reaches
/// disk through `log_framing`, which writes its own marker, a length and a CRC32C, so where a
/// record starts and whether it is intact are already answered before these bytes are read.
///
/// The magic also told a binary payload from the JSON one that predated it. Nothing writes JSON
/// any more, so that question has no second answer either.
fn index_container_byte(codec: u8, shape: u8) -> u8 {
    (codec << 4) | (shape & 0x0f)
}

/// The codec and shape a container byte carries.
fn index_container_parts(byte: u8) -> (u8, u8) {
    (byte >> 4, byte & 0x0f)
}

/// Payload codec: msgpack, struct-as-map.
pub(crate) const INDEX_LOG_CODEC_MSGPACK: u8 = 1;

/// Which record shape a payload holds.
///
/// Two shapes share this log: a whole-index record and a delta. While each row was a map the
/// reader could tell them apart by the names inside, and could read one as the other by matching
/// what it recognised. Rows carry no names, and a map is not proof of shape either -- a whole-index
/// record embeds a `serde_json::Value`, which is a map whatever the rows around it are. So the
/// container says which it holds instead of the reader guessing.
pub(crate) const INDEX_LOG_SHAPE_WHOLE: u8 = 0;
pub(crate) const INDEX_LOG_SHAPE_DELTA: u8 = 1;

/// The compaction anchor: which index a log position describes, without embedding it.
///
/// It used to be written as JSON, with the digest spelled out as 64 hex characters -- the only
/// thing in this log that was not binary, and the reason the reader had to keep a JSON path at
/// all. Its first two elements are the shard and the sequence, which is what the tail scan reads
/// and all it reads.
///
/// AND THE DIGEST IS NOW ACTUALLY RAW BYTES, which this note claimed before it was true. It carried
/// `bytes_serde` -- the JSON base64 codec -- on a msgpack path, so the 32 bytes were written as 46:
/// 44 base64 characters and a length prefix, against 34 for a `bin` string. Measured by
/// `what_the_durable_byte_encodings_cost` and corrected by pointing the field at
/// `bytes_serde::binary`. An anchor written the old way still loads, because the decoder was always
/// the same visitor and it accepts all three spellings.
pub(crate) const INDEX_LOG_SHAPE_ANCHOR: u8 = 2;

/// What an anchor says: the index it identifies, by digest and length.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexAnchorRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    #[serde(with = "crate::bytes_serde::binary")]
    pub index_sha256: Vec<u8>,
    pub index_len: u64,
}

/// Payload codec: msgpack, struct-as-map, zstd-compressed.
///
/// The index log is the largest durable tier on an ingest-heavy store and almost all of it
/// is text: object keys repeated thousands of times and ids written as decimal digits.
/// Measured on a one-box log of 14.4 MB: 24.8% of the bytes were long decimal ids and the
/// six most repeated key strings were another 20%, and the whole file compressed 11.1x.
///
/// A reader always understands this codec; a writer only emits it when asked, so a store
/// can be rolled forward and back without a format migration. An older binary refuses it
/// as an unknown codec rather than misreading it, which is what the codec byte is for.
pub(crate) const INDEX_LOG_CODEC_MSGPACK_ZSTD: u8 = 2;

/// The single place an index-log record becomes payload bytes.
///
/// Both append paths go through here so the whole-index record and the delta record cannot
/// drift into different shapes -- the reader tells them apart by their fields, which only
/// works while both are encoded the same way.
/// Whether new index-log payloads are written compressed. Off by default.
///
/// Reading codec 2 is unconditional; only writing it is gated, so a store can be rolled
/// forward and back without a migration.
fn index_log_compression_enabled() -> bool {
    matches!(
        std::env::var("TS_INDEX_LOG_COMPRESSION_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Payloads below this many bytes are stored uncompressed.
///
/// zstd on a very small record spends a frame header to save little; the page tier draws
/// the same line at 256 bytes for the same reason.
fn index_log_compression_min_bytes() -> usize {
    std::env::var("TS_INDEX_LOG_COMPRESSION_MIN_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(256)
}

/// The same encoding, into a buffer the caller owns and reuses.
///
/// [`encode_index_payload`] built the record in one vector and then copied the whole thing into a
/// second one so that a single container byte could go in front of it -- a full copy of every
/// record appended, thrown away immediately. Measured on a one-item delta record, that encode was
/// 5.99 allocations and 211 bytes per record, against 80.98 bytes of record.
///
/// The container byte goes in FIRST here and the serializer writes after it, so there is no second
/// buffer and nothing to copy. The buffer is CLEARED and not appended to, keeping its capacity,
/// which is what takes the growth out of a steady stream of appends: the write-ahead log's
/// `encode_scratch` is the same arrangement for the same reason.
///
/// The BYTES ARE UNCHANGED, and that is the binding constraint on this function: it writes the
/// same container byte, the same msgpack, and the same compressed form under the same two
/// conditions. `the_two_index_payload_encoders_agree_byte_for_byte` holds the two against each
/// other over every record shape this log writes.
pub(crate) fn encode_index_payload_into<T: serde::Serialize>(
    record: &T,
    shape: u8,
    out: &mut Vec<u8>,
) -> Result<(), IndexLogError> {
    out.clear();
    out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK, shape));
    let mut serializer = rmp_serde::Serializer::new(&mut *out);
    if serde::Serialize::serialize(record, &mut serializer).is_ok() {
        // `out` is the container byte plus the packed record, so the packed length -- the number
        // the two conditions below are written in terms of -- is one less than it.
        let packed_len = out.len() - 1;
        if index_log_compression_enabled() && packed_len >= index_log_compression_min_bytes() {
            if let Ok(squeezed) = zstd::stream::encode_all(&out[1..], 3) {
                // Only when it actually helps, and the comparison is between the PACKED forms,
                // exactly as it was: both shapes carry one container byte, so comparing
                // `squeezed` against `packed_len` and comparing the two whole payloads are the
                // same test.
                if squeezed.len() < packed_len {
                    out.clear();
                    out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK_ZSTD, shape));
                    out.extend_from_slice(&squeezed);
                }
            }
        }
        return Ok(());
    }
    // An encode failure must not cost the record: fall through to the bytes that always work,
    // which the reader still takes. This is the only path that now produces JSON, and it carries
    // NO container byte -- so the partial msgpack above has to come off first.
    out.clear();
    serde_json::to_writer(&mut *out, record)?;
    Ok(())
}

pub(crate) fn encode_index_payload<T: serde::Serialize>(
    record: &T,
    shape: u8,
) -> Result<Vec<u8>, IndexLogError> {
    // Struct-as-ARRAY: positional, with no field name written anywhere. `Serializer::new`
    // without `.with_struct_map()` is the compact form, and the rest of this comment only makes
    // sense that way -- `IndexRecordHead` takes the FIRST TWO VALUES of a record and ignores
    // whatever follows, which is a thing you can only do to an array.
    //
    // (This said "struct-as-MAP, not struct-as-array" while doing the opposite. On a durable
    // format that is not a typo: a reader who believes records are self-describing maps will
    // reorder or insert a field, and every record ever written then decodes into the wrong
    // fields with no error anywhere.)
    //
    // What makes positional safe is that nothing is ever skipped: every record writes every
    // field, so a position always means the same thing. That is the rule to keep. Adding a
    // field means adding it at the END and never reordering what precedes it.
    //
    // It is also why this encoding is SMALLER than the tagged alternative rather than larger.
    // Measured on a delta record carrying eight page items: 550 bytes here against 595 as
    // protobuf, because a tag costs a byte per present field and a position costs nothing.
    let mut packed = Vec::new();
    // Values in field order, for the record as well as for the rows inside it: nothing written
    // here spells a field name.
    let mut serializer = rmp_serde::Serializer::new(&mut packed);
    if serde::Serialize::serialize(record, &mut serializer).is_ok() {
        if index_log_compression_enabled() && packed.len() >= index_log_compression_min_bytes() {
            if let Ok(squeezed) = zstd::stream::encode_all(packed.as_slice(), 3) {
                // Only when it actually helps: a record that does not compress would
                // otherwise pay the frame overhead for nothing.
                if squeezed.len() < packed.len() {
                    let mut out = Vec::with_capacity(squeezed.len() + 1);
                    out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK_ZSTD, shape));
                    out.extend_from_slice(&squeezed);
                    return Ok(out);
                }
            }
        }
        let mut out = Vec::with_capacity(packed.len() + 1);
        out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK, shape));
        out.extend_from_slice(&packed);
        return Ok(out);
    }
    // An encode failure must not cost the record: fall through to the bytes that always work,
    // which the reader still takes. This is the only path that now produces JSON.
    Ok(serde_json::to_vec(record)?)
}

/// The single place index-log payload bytes become a record, whatever wrote them.
///
/// Every decode site goes through here. The previous attempt at a binary served index failed
/// because its decoders were scattered and could not move together; this file has four decode
/// sites and they move as one.
pub(crate) fn decode_index_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, IndexLogError> {
    let Some((container, body)) = payload.split_first() else {
        return Err(IndexLogError::Encoding(
            "index-log record has no container byte".to_string(),
        ));
    };
    let (codec, _shape) = index_container_parts(*container);
    match codec {
        INDEX_LOG_CODEC_MSGPACK => rmp_serde::from_slice(body)
            .map_err(|error| IndexLogError::Encoding(error.to_string())),
        INDEX_LOG_CODEC_MSGPACK_ZSTD => {
            let plain = zstd::stream::decode_all(body)
                .map_err(|error| IndexLogError::Encoding(error.to_string()))?;
            rmp_serde::from_slice(&plain)
                .map_err(|error| IndexLogError::Encoding(error.to_string()))
        }
        other => Err(IndexLogError::Encoding(format!(
            "unknown index-log payload codec {other}"
        ))),
    }
}

impl From<crate::log_framing::FramingError> for IndexLogError {
    fn from(err: crate::log_framing::FramingError) -> Self {
        IndexLogError::Corruption(err.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexLogRecord {
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    // Default so a delta-record line (which carries `items`/`meta` but no `index`) still
    // parses as an IndexLogRecord for the sequence-tail scan (`last_sequence_at`), which
    // only reads `.sequence`. Whole-index records always carry `index`.
    #[serde(default)]
    pub index: serde_json::Value,
}

/// Kind of a single delta item in the append-only served-index log. A write emits a
/// bounded set of these (the pages/objects it touched), so the log grows by O(delta)
/// per write instead of O(store). Follows the on-disk item taxonomy: a block item is a
/// concrete page-index entry change, an OBJECT item is an object-level change (e.g. a
/// TTL-only or whole-object tombstone), and a META item carries the compaction anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexItemKind {
    #[serde(rename = "block", alias = "page")]
    Block,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "meta")]
    Meta,
}

impl Default for IndexItemKind {
    fn default() -> Self {
        IndexItemKind::Block
    }
}

/// One delta item: the smallest change to the served index a write can produce. The
/// field set is the native Rust page-index projection -- `routing_bucket`/`page_ref_key`
/// locate the entry in the bucket map, and `address`/`size`/`in_log`/`deleted`/`model_id`/
/// `object_id`/`page_id` mirror the durable page metadata so replay reconstructs the same
/// `BlockIndex` the whole-index serialization would have produced. `deleted` is a
/// tombstone: replaying it removes the entry.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(from = "IndexItemRow")]
pub struct IndexItem {
    #[serde(
        rename = "k",
        alias = "kind",
        default,
        serialize_with = "item_kind_as_number",
        deserialize_with = "item_kind_either_shape"
    )]
    pub kind: IndexItemKind,
    #[serde(rename = "rb", alias = "routing_bucket", alias = "routing_slot", default)]
    pub routing_bucket: u32,
    /// The page handle. A `String` in this struct, a NUMBER on the wire whenever it holds one.
    ///
    /// It is a `u64` everywhere else -- `BlockLookupRef::page_ref_key` is one, and the write path
    /// stringifies it on the way in. As decimal text it was 20 bytes of a 161-byte item, 12.4%;
    /// as a number it is about nine.
    ///
    /// Both halves have landed. `block_ref_key_as_number_when_it_is_one` parses the string and
    /// serializes a `u64` when it parses, falling back to a string when it does not; the reader
    /// takes either shape, because records written before the writer half exist on disk and are
    /// not going to rewrite themselves.
    ///
    /// (This said "Nothing writes a number yet" while sitting directly above the serializer that
    /// writes one. That order was right when the reader half landed alone -- a writer emitting a
    /// number to a reader expecting a string would have been refused outright by msgpack rather
    /// than degrading -- and the sentence outlived the sequencing it described. On a durable
    /// format a stale claim about what is on the wire is the kind that gets acted on: the same
    /// file's encoder comment claimed struct-as-map while calling the array form.)
    #[serde(
        rename = "pk",
        alias = "page_ref_key",
        default,
        deserialize_with = "block_ref_key_either_shape",
        serialize_with = "block_ref_key_as_number_when_it_is_one"
    )]
    pub block_ref_key: String,
    /// SHARED WITH THE RESIDENT ENTRY, which holds this same text as an `Arc<str>`. A
    /// `String` spends a capacity word on a value that is never grown in place.
    #[serde(rename = "ok", alias = "object_key", default)]
    pub object_key: std::sync::Arc<str>,
    #[serde(
        rename = "mi",
        alias = "model_id",
        default,
        serialize_with = "model_id_as_number_when_it_is_known",
        deserialize_with = "model_id_either_shape"
    )]
    pub model_id: String,
    #[serde(rename = "oi", alias = "object_id", default)]
    pub object_id: u64,
    /// WHERE THE ELEMENT LIVES -- the SHARED payload, the same struct the resident model map
    /// holds as its level-2 value.
    ///
    /// # WHY FOUR FIELDS BECAME ONE
    ///
    /// This row used to carry `address`, `size`, `in_log` and `block_id` side by side, and three
    /// of those were DERIVATIONS of the first: `size` is `address.length()`, `block_id` is
    /// `address.block_id()`, and `in_log` is `address.block_id().is_none()`. Both builders said so
    /// in their own code -- one of them filled `in_log` by calling `BlockIndex::log_backed()`,
    /// which is the accessor added when the equivalent stored flag was removed from the RESIDENT
    /// entry for exactly this reason. Two spellings of one derivation, on a row, after the
    /// entry-side copy was already deleted.
    ///
    /// So they are gone as fields and computed where they are written. The payload that remains is
    /// the address, which is also what the model map's level-2 value holds -- one struct, two
    /// containers, and ONE place where the meaning of a field is defined.
    ///
    /// # AN `Option` AND NOT A DEFAULTED ENTRY
    ///
    /// `address` was an `Option` and could be absent. Collapsing an absent address into a
    /// defaulted all-zero one is the silent conflation this file refuses elsewhere: an old
    /// record's `ps` of 1 beside an `o` of 2 would land as a well-formed address for a block that
    /// is not the one that was written, and nothing would fail. So absence stays absence.
    /// `Option<ElementEntry>` is 24 bytes, exactly what `Option<BlockAddress>` was.
    ///
    /// # THE TWELVE SLOTS DID NOT MOVE
    ///
    /// Nothing here changes the wire. The index log is POSITIONAL and carries no struct version,
    /// and the format stamp cannot help, because it guards the NAMED served index. All twelve slots
    /// are still written; the three derived ones are computed in the serializer and CROSS-CHECKED
    /// in the conversion from [`IndexItemWire`], which is signal this row did not have before.
    ///
    /// # A CORRECTION: THE GUARANTEE THIS PARAGRAPH USED TO STATE DOES NOT EXIST
    ///
    /// It said a row whose LENGTH does not match the struct decoding it is REFUSED. There is no
    /// length check anywhere on this path. What refuses a mismatch is SLOT TYPING, and only where
    /// the types happen to disagree -- measured, with both arms driven: a stored twelve-slot row
    /// read by an eleven-field struct is refused with "wrong msgpack marker FixStr(2)" when slot 5
    /// holds a name and "invalid type: unit value, expected u64" when it holds nil. In both cases
    /// the shift put a string or a nil where a `u64` was expected. Nothing counted the slots.
    ///
    /// AND A ROW SHORT AT THE TAIL WAS ACCEPTED, WHICH IS WHAT [`IndexItemRow`] NOW REFUSES.
    /// Every field here carries `#[serde(default)]`, so a seq that simply ENDS EARLY had the rest
    /// filled in: measured, a row missing only its last slot decoded with `deleted: false` on a
    /// row that never said so, and a tombstone read as live is a resurrection. The TAIL was the
    /// one position where a slot leaving was silent rather than loud -- a property of position
    /// rather than of length, which is why nothing about slot typing could have caught it.
    ///
    /// THERE IS A LENGTH CHECK NOW, AND ONLY ON THE ROW. [`IndexItemRow`] reads the arity off the
    /// msgpack array header before any element is touched and refuses anything that is not
    /// [`INDEX_ITEM_SLOTS`]; measured, arities 11, 10, 9 and 8 were all accepted before it and
    /// are all refused after. The NAMED map arm is untouched, because a map has no tail to be
    /// short at. [`IndexDeltaRecord`] is deliberately left alone: its slots have GROWN at its own
    /// tail, so an older record really is short there and must keep loading.
    ///
    /// Leaving the false sentence in place beside a change that relies on slot typing would be
    /// worse than having no sentence: it is the shape that has misdirected decisions in this file
    /// before, which is why the wrong version is quoted rather than quietly replaced -- and the
    /// correction's own conclusion is restated above rather than deleted, for the same reason.
    #[serde(skip)]
    pub entry: Option<ElementEntry>,
    #[serde(rename = "d", alias = "deleted", default)]
    pub deleted: bool,
}

/// THE TWELVE SLOTS, EXACTLY AS THEY HAVE ALWAYS BEEN, for reading only.
///
/// [`IndexItem`] rides `serde(from)` over this, which is the pattern [`BlockAddress`] already uses
/// for its own wire struct. Two things fall out of it, and both are the reason it is shaped this
/// way rather than hand-written:
///
///   * BOTH SHAPES KEEP WORKING FOR FREE. The derived `Deserialize` accepts a positional row and a
///     named map, which is what the aliases below are for. Only the two msgpack paths exist today
///     and the map arm is DEFENSIVE -- there is no live JSON consumer of this row -- but a codec
///     that handles only the shape currently exercised is the kind of thing that breaks in silence
///     later, and the aliases are evidence a named shape was once read.
///   * THE CROSS-CHECK HAS SOMEWHERE TO STAND. The three derived slots are read here and compared
///     against the address in [`IndexItem::from`], rather than being dropped.
#[derive(Debug, Clone, Deserialize)]
// THE NAMED ARM REFUSES AN UNKNOWN KEY, AND IT HAS TO, because of what came off this struct.
//
// A derived `Deserialize` IGNORES a key it has no field for. The element-name slot is gone from
// this struct and from the row, so without this a stored row written as a MAP by a binary that
// still had the field would decode with its `"c"` silently thrown away -- a store that loads and
// then cannot be dumped, with no error anywhere, which is the one outcome this file refuses
// everywhere else. The POSITIONAL arm is refused by arity in [`IndexItemRow`]; this is the same
// refusal for the shape an arity cannot see.
//
// IT IS SAFE TO BE STRICT HERE AND IT WOULD NOT BE ON THE RECORD. This row's named arm is
// READ-ONLY LEGACY -- `IndexItem::serialize` is hand-written and only ever emits a sequence, so no
// writer in this crate produces a map -- and the row's slots have never GROWN. [`IndexDeltaRecord`]
// is the opposite case: its slots have grown at its tail, an older record really is short there,
// and it must stay tolerant. So the strictness goes on the row and nowhere else.
#[serde(deny_unknown_fields)]
struct IndexItemWire {
    #[serde(
        rename = "k",
        alias = "kind",
        default,
        deserialize_with = "item_kind_either_shape"
    )]
    kind: IndexItemKind,
    #[serde(rename = "rb", alias = "routing_bucket", alias = "routing_slot", default)]
    routing_bucket: u32,
    #[serde(
        rename = "pk",
        alias = "page_ref_key",
        default,
        deserialize_with = "block_ref_key_either_shape"
    )]
    block_ref_key: String,
    #[serde(rename = "ok", alias = "object_key", default)]
    object_key: std::sync::Arc<str>,
    #[serde(
        rename = "mi",
        alias = "model_id",
        default,
        deserialize_with = "model_id_either_shape"
    )]
    model_id: String,
    #[serde(rename = "oi", alias = "object_id", default)]
    object_id: u64,
    #[serde(rename = "pi", alias = "page_id", default)]
    block_id: u64,
    #[serde(rename = "a", alias = "address", default)]
    address: Option<ElementEntry>,
    #[serde(rename = "sz", alias = "size", default)]
    size: u64,
    #[serde(rename = "il", alias = "in_log", default)]
    in_log: bool,
    #[serde(rename = "d", alias = "deleted", default)]
    deleted: bool,
}

// THE DROPPED-NAME COUNTER IS GONE BECAUSE A DROP IS NO LONGER POSSIBLE, and that is the whole
// difference this step makes. `STORED_ELEMENT_NAMES_DROPPED` counted stored rows whose slot 5 held
// an element name this binary had no field for: the slot was still read, the value had nowhere to
// go, and counting it was the only way the drop was visible at all. The slot is now GONE from the
// row -- eleven slots, not twelve -- and both decode arms REFUSE a row that carries one: the
// positional arm by arity in [`IndexItemRow`], the named arm by `deny_unknown_fields` on
// [`IndexItemWire`]. A refusal needs no detector, so the counter, its reader and its reset come
// off with the thing they were watching. `DERIVED_SLOT_DISAGREEMENTS` below STAYS: its subject is
// a row that still decodes.

/// How many stored rows disagreed with their own address about a derived slot.
///
/// A DETECTOR, NOT A REFUSAL. Today a row carrying `sz` that disagrees with `address.length()` is
/// accepted in silence -- `restore_size_repeat` only fills the slot when it is zero, so a
/// disagreeing non-zero value is simply kept. Turning that into a decode error would refuse rows
/// that load today, which is a bigger change than this is allowed to be. So the disagreement is
/// COUNTED where it was previously invisible, and the count is what a test can assert on.
static DERIVED_SLOT_DISAGREEMENTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// How many derived slots have disagreed with their address since the last reset.
pub fn derived_slot_disagreements() -> usize {
    DERIVED_SLOT_DISAGREEMENTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the disagreement count, so a test measures its own rows and not another test's.
pub fn reset_derived_slot_disagreements() {
    DERIVED_SLOT_DISAGREEMENTS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// How many positional slots a stored index-log row carries.
///
/// ONE PLACE, because [`IndexItem`]'s serializer writes this many and [`IndexItemRow`] refuses a
/// row that does not carry this many. Two spellings of one arity is how a reader comes to refuse
/// what the writer beside it just wrote.
///
/// ELEVEN, NOT TWELVE, SINCE THE ELEMENT-NAME SLOT CAME OFF. Slot 5 held the element name and
/// then, for one step, a CONSTANT NIL -- kept so that every stored row would still decode while
/// nothing read the value. A slot written on every row forever to say nothing is the dead weight a
/// pre-release format is allowed to shed, so the slot is gone and the arity moved with it.
///
/// WHAT THAT COSTS IS STATED WHERE IT IS PAID: a stored TWELVE-slot row is now REFUSED, by this
/// constant, in a message naming both arities. `engine::SHARD_INDEX_FORMAT_VERSION` moved to 13 in
/// the same step so a store written by a twelve-slot binary is rebuilt rather than read -- but the
/// stamp is NOT what refuses the row, because it guards the NAMED served index and this log has no
/// version of its own. The arity check below is.
const INDEX_ITEM_SLOTS: usize = 11;

/// THE ARITY GATE ON THE POSITIONAL ROW: a row SHORT AT ITS TAIL is refused here.
///
/// # THE DEFECT THIS CLOSES
///
/// Every slot of [`IndexItemWire`] carries `#[serde(default)]`. The NAMED map arm needs that and
/// the POSITIONAL arm must not have it: serde's derived `visit_seq` fills a slot the array never
/// reached with `Default`, so an eleven-slot row loaded as a twelve-slot one whose last slot said
/// `false`. The last slot is `deleted`. A stored TOMBSTONE therefore decoded as a LIVE row and
/// the object it had removed came back, with no error anywhere -- which is the one outcome this
/// log's replay exists to prevent. The slot before it drops `in_log`, the one before that drops
/// the whole `address`, and each is the same failure wearing another field's name.
///
/// # WHY AN ARITY MAY BE CHECKED AT ALL
///
/// Because no legitimately-written row is ever short. The positional row landed carrying twelve
/// slots and `IndexItem::serialize` has written `INDEX_ITEM_SLOTS` of them unconditionally ever
/// since: there is no `skip_serializing_if` anywhere on the row, and no older writer emitted
/// fewer.
///
/// THAT IS NOT TRUE ONE LEVEL OUT, and the difference is the whole reason this gate is on the
/// ROW and not on the record. [`IndexDeltaRecord`] has GROWN slots at its tail -- `upsert` says
/// "every record written before this field existed", and `shared_object_key` says "appended at
/// the END, which is the only safe place to add to a positional record" -- so records written by
/// an older binary really are short and must keep loading. The record's arity is not checked and
/// must not be.
///
/// # WHAT WAS ALREADY REFUSED, AND WHAT WAS NOT
///
/// `rmp_serde` compares the array header against the slots the visitor consumed and refuses what
/// is LEFT OVER -- "array had incorrect length, expected 12" -- so a row longer than this build
/// reads has always been refused. Nothing looked at the other side. This makes the two sides
/// symmetric, and that is the whole of the change.
///
/// # REFUSED, NOT PADDED
///
/// Substituting a safe value for an absent slot is the same defect with better manners: the row
/// did not say `false`, and a reader that invents an answer cannot be told from one that read a
/// real one.
///
/// # IT IS AN `Encoding` ERROR AND NOT `Corruption`, DELIBERATELY
///
/// A short row cannot be torn bytes. The msgpack array header states the arity BEFORE the
/// elements, so losing bytes off the end leaves the header still claiming twelve and the decode
/// already fails on the marker it cannot read -- and `log_framing::next_frame` refuses a torn
/// tail earlier still, returning `Ok(None)` when fewer bytes are present than the frame declares
/// and `Err` when a complete record's CRC32C disagrees. So a WELL-FORMED eleven-slot array can
/// only have been written by something that writes eleven, which is a question about shape, and
/// [`IndexLogError::Encoding`] is defined as exactly that -- a malformed payload, "distinct from
/// `Corruption` because the framing envelope already passed". `decode_index_payload` already
/// maps every serde failure there, so the class follows from where the check lives rather than
/// from a special case that could drift away from it.
///
/// # WHERE IT DOES NOT REACH
///
/// Slot 8 is the address, which is ITSELF a positional array of five
/// (`block_store::BlockAddressWire`). This gate checks the ROW's arity, not the address's: an
/// address sub-array short at its tail still loads with `generation` absent, and that field's own
/// documentation says its PRESENCE is a meaning. Same class, one struct down, not closed here.
struct IndexItemRow(IndexItemWire);

impl From<IndexItemRow> for IndexItem {
    fn from(row: IndexItemRow) -> Self {
        IndexItem::from(row.0)
    }
}

impl<'de> serde::Deserialize<'de> for IndexItemRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Refuses to report exhaustion, so a derived `visit_seq` cannot default a slot the array
        /// never reached. The arity check below catches a short row first on every format that
        /// states an array's length up front -- msgpack does, and msgpack is the only shape this
        /// log is written in -- and this is what holds when one does not.
        struct EverySlotPresent<A>(A);

        impl<'de, A> serde::de::SeqAccess<'de> for EverySlotPresent<A>
        where
            A: serde::de::SeqAccess<'de>,
        {
            type Error = A::Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                match self.0.next_element_seed(seed)? {
                    Some(value) => Ok(Some(value)),
                    None => Err(<A::Error as serde::de::Error>::custom(format!(
                        "index-log row ran out of positional slots before all \
                         {INDEX_ITEM_SLOTS} were read; a missing tail slot reads back as its \
                         default, which turns a stored tombstone into a live row"
                    ))),
                }
            }

            fn size_hint(&self) -> Option<usize> {
                self.0.size_hint()
            }
        }

        struct EitherShape;

        impl<'de> serde::de::Visitor<'de> for EitherShape {
            type Value = IndexItemRow;

            fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    out,
                    "an index-log row: {INDEX_ITEM_SLOTS} positional slots, or a named map"
                )
            }

            /// THE POSITIONAL ARM, WHICH IS THE ONE THAT IS STRICT.
            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                if let Some(slots) = seq.size_hint() {
                    if slots != INDEX_ITEM_SLOTS {
                        return Err(<A::Error as serde::de::Error>::custom(format!(
                            "index-log row carries {slots} positional slots, not \
                             {INDEX_ITEM_SLOTS}: a missing tail slot reads back as its default, \
                             which turns a stored tombstone into a live row"
                        )));
                    }
                }
                <IndexItemWire as serde::Deserialize>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(EverySlotPresent(seq)),
                )
                .map(IndexItemRow)
            }

            /// THE NAMED ARM, WHICH STAYS TOLERANT ON PURPOSE. A map has no tail to be short at:
            /// an absent key is an absent key wherever it sits, the aliases on [`IndexItemWire`]
            /// are evidence this shape was once read, and the compatibility arms in this file
            /// build their fixtures this way. Nothing here changes for it.
            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                <IndexItemWire as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )
                .map(IndexItemRow)
            }
        }

        deserializer.deserialize_any(EitherShape)
    }
}

impl From<IndexItemWire> for IndexItem {
    fn from(wire: IndexItemWire) -> Self {
        // THE CROSS-CHECK. Each of the three derived slots is compared against the address that
        // defines it. `size` is checked only when the row carried a value: a zero is the STRIP
        // SENTINEL -- the encoder writes it when the size equals the address length -- and not a
        // disagreement.
        if let Some(address) = wire.address.as_ref() {
            let mut disagreed = 0usize;
            if wire.size != 0 && wire.size != address.length() {
                disagreed += 1;
            }
            if wire.block_id != address.block_id().unwrap_or_default() {
                disagreed += 1;
            }
            if wire.in_log != address.block_id().is_none() {
                disagreed += 1;
            }
            if disagreed > 0 {
                DERIVED_SLOT_DISAGREEMENTS
                    .fetch_add(disagreed, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Self {
            kind: wire.kind,
            routing_bucket: wire.routing_bucket,
            block_ref_key: wire.block_ref_key,
            object_key: wire.object_key,
            model_id: wire.model_id,
            object_id: wire.object_id,
            entry: wire.address,
            deleted: wire.deleted,
        }
    }
}

/// One per changed page per write, and transient -- it lives for the length of an append.
/// Pinned because it is the widest thing built per page anywhere in the engine.
///
/// WHAT THIS NUMBER IS MADE OF IS RECONSTRUCTED RATHER THAN RESTATED, by
/// `tests::what_every_byte_of_an_index_item_is_spent_on`: the eight-aligned fields come to 96
/// and the tail -- the routing bucket, the kind and the ONE remaining flag -- comes to 6, which
/// the aligner
/// rounds to 8. The probe asserts each field is really in the group it counts it in, so a field
/// that MOVES between the two fails there with the group it left.
///
/// (96, NOT 112, AND THAT IS ONE MORE DRIFT THAN THE FIVE THE NOTE AT THE END OF THIS CHAIN
/// COUNTS. This sentence said 112 while the assertion below said 104 -- and 112 plus the 8-byte
/// tail is 120, the width before the element name came off -- so the first thing a reader met
/// described the struct as it was one step ago. The arm itself has read 96 since that step; only
/// the prose lagged, which is exactly the failure the parenthetical below documents.)
///
/// THE TEST IS THE AUTHORITY, NOT THIS PARAGRAPH. It reconstructs the two groups from the
/// declaration and fails naming the group a field left, which is how the 120 below was caught
/// the moment three fields became derivations. Prose stating a width has drifted five times in
/// this area; a reconstruction has not drifted once.
///
/// AND THE TWO SPARE BYTES IN THAT TAIL ARE THE WHOLE VERDICT ON NARROWING THIS STRUCT. Seven bytes
/// of field in eight means a one-byte field added to the tail is FREE, and every narrowing inside
/// it is worth EXACTLY ZERO: the routing bucket at sixteen bits leaves five and rounds back to
/// eight, and deleting the kind and both flags outright leaves four and still rounds to eight.
/// Shedding eight bytes means taking a WHOLE WORD out of the eight-aligned group.
///
/// 152, not 160, since that `BlockAddress` then shed its `object_id` and became a derivation of the
/// terms beside it. The address is inside an `Option` here with no niche to put the discriminant
/// in, so the eight bytes that left the address take the `Option` from 32 to 24 and the item from
/// 160 to 152.
///
/// 160, not 168, since the `BlockAddress` it carries shed its `routing_bucket` and narrowed its
/// `block_id` to sixteen bits. The address is inside an `Option` here and the item is 8-aligned, so
/// the six bytes of payload that left round to one whole word -- the same step, and the same reason,
/// as in the page entry. 168, not 176, since that address merged its slab id and its offset into one
/// word; 176, not 184, since it shed its derived `generation` before that.
///
/// (THE CHAIN USED TO START AT 160 WHILE THE ASSERTION BELOW SAID 152. The step the address took
/// when its identity became derived moved this width and nothing moved the prose, so the first
/// sentence a reader met named a width this struct had not had for a release. This file has a
/// documented history of exactly that failure about its own wire format, and the same rule applies
/// to the resident one: a width chain that does not start where the assertion does is read as the
/// current width by whoever reads it next.)
const _: () = assert!(std::mem::size_of::<IndexItem>() == 104);
// BRACKETED, so 104 is EXACT and not a bound. 120 before the element name came off: the name was
// an `Option<Arc<str>>`, and `Arc<str>` is a fat pointer, so the `Option` was 16 bytes with the
// discriminant in the pointer's niche.
const _: () = assert!(std::mem::size_of::<IndexItem>() != 103);
const _: () = assert!(std::mem::size_of::<IndexItem>() != 105);

/// A field whose value is its default says nothing, and every field here carries
/// `#[serde(default)]` -- so a reader that meets an absent one fills in the same value it would
/// have read. That is what makes omitting them safe in a single step, with no ordering between
/// writers and readers: unlike a renamed field or a changed type, an absent field with a default
/// is exactly what an older reader already handles.
fn is_false(value: &bool) -> bool {
    !*value
}

/// See [`is_false`].
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Accept a page handle written either as text or as a number.
///
/// The handle is a `u64`; it has been carried as decimal text. This lets a reader consume a log
/// whose writer has moved to the number, so the writer can move whenever every reader has this.
/// Write the page-ref key as a NUMBER when it is one, and as text when it is not.
///
/// This is the second half of the two-step change the reader half landed for. The key is a `u64`
/// everywhere else and the write path stringifies it on the way in; as decimal text it was 20
/// bytes of a 161-byte item, and as a msgpack integer it is about nine.
///
/// Safe to flip because `block_ref_key_either_shape` already takes both, and it has shipped: a
/// reader that had only ever seen the string would have had msgpack refuse the type outright
/// rather than degrade, which is exactly why the reader went first.
///
/// A key that does NOT parse as a number still goes out as text, so nothing depends on the write
/// path's stringification being reversible.
impl IndexItem {
    /// Drop the composite key when the record already carries every part of it.
    ///
    /// `page_ref_key` is `block_ref_key_from_parts` of this item's own model, object key,
    /// component and address -- measured at 43 bytes of a 176-byte record, the largest single
    /// field in the index log, and every byte of it is spelled out again in the fields beside it.
    /// A reader rebuilds it with the same function, so the log carries the parts and not the
    /// concatenation.
    ///
    /// Cleared only on an exact match. Anything the derivation does not reproduce is written as
    /// it stands, so a key that is not the composite -- the numeric handle form, for one --
    /// survives untouched.
    /// # WHAT A FORMAT STAMP DOES AND DOES NOT PROTECT HERE, AND A CORRECTION TO THIS COMMENT
    ///
    /// Written down because the natural assumption is that a stored-shape change is made safe by
    /// bumping `SHARD_INDEX_FORMAT_VERSION`, and because the first version of this comment got the
    /// consequence WRONG in a way that read as rigorous. Both halves are kept, because the error is
    /// the more useful half.
    ///
    /// THE SHAPE OF THE HAZARD, which is real. A stamp protects a change to how bytes are LAID OUT.
    /// It cannot protect a change to how an OMITTED value is RECONSTRUCTED, because the fallback is
    /// the reconstructor: `persistence.rs` refuses an index whose stamp is below the constant and
    /// falls back to replay, and replay is where `restore_block_ref_key_repeat` rebuilds the key this
    /// function omitted, from these same eight parts. A bump sends such a record down that path
    /// rather than shielding it from it.
    ///
    /// THE PRECONDITION THAT WAS MISSING, AND IT IS THE WHOLE POINT. That only bites WHEN SOMETHING
    /// DOWNSTREAM READS THE RECONSTRUCTION. Here nothing does. `fold_delta_block_items` is the only
    /// thing that turns index-log items into pages, and it never mentions `block_ref_key`: it builds a
    /// `BlockIndex` from the item's own fields and hands it to `BlockIndexMap::insert`, which
    /// ALLOCATES the handle the map is keyed by. `persistence.rs` does not mention the field at all.
    /// Driven: a record whose handle was OMITTED, and a record carrying a handle that is no derivation
    /// of anything, BOTH load and serve -- filed, WAL-resident, slab, offset and length intact.
    ///
    /// SO THE CLAIM THIS COMMENT USED TO MAKE IS FALSE: that removing `component` would make every
    /// already-stripped record rebuild a different key and resolve a durably acknowledged write to
    /// MISSING, needing a migration no stamp could buy. It reasoned from "the field is an input to a
    /// derivation whose output is stored" to "changing it corrupts stored data", and never asked
    /// whether anything reads the output. Every step was true; the conclusion was not.
    ///
    /// `component` WAS BLOCKED FOR A SECOND AND STRONGER REASON, AND THE BLOCK IS DISCHARGED --
    /// recorded rather than deleted, because the condition it named is exactly what lifted it. The
    /// paragraph read: unlike `block_ref_key` the element name is carried into memory and
    /// participates in MATCHING, because `fold_delta_block_items` set it on the `BlockIndex` and the
    /// upsert `retain` compared `page.component.as_deref() == item.component.as_deref()`; it was the
    /// only discriminator between two elements of ONE folded page, which share model id, object key
    /// AND address; and it "cannot go until the entry stops being per-element".
    ///
    /// THE ENTRY HAS STOPPED BEING PER-ELEMENT. `BlockIndex::component` came off when the page entry
    /// went 56 bytes to 40, so the fold has no field to set, and that comparison is deleted as a
    /// tautology with the reason at its own site -- both sides were already `None` for every kind.
    /// What discriminates two elements of one page now is the page's own payload, which names every
    /// element on it, and the four durable maps, which are keyed by the element.
    ///
    /// So `IndexItem::component` is gone too and slot 5 carries a constant nil. The rule below is
    /// what settled it, asked in the right order: nothing reads the reconstruction. The compiler
    /// enumerated the readers -- 39 diagnostics across 10 files -- and the only ones were this
    /// function, its inverse, and the positional encoder.
    ///
    /// THE RULE, in the order the questions have to be asked. For a stored field omitted because it is
    /// derivable: find WHO READS THE RECONSTRUCTION first. If something does, the omission is a
    /// promise about the derivation, and changing any input needs a migration for the records that
    /// already relied on it. If nothing does, the stored field is dead weight and the derivation is
    /// free to change. Asking those two questions the other way round is what produced the error
    /// above.
    fn strip_block_ref_key_repeat(&mut self) {
        let Some(address) = self.entry.as_ref() else {
            return;
        };
        let derived = block_ref_key_from_parts(
            &self.model_id,
            &self.object_key,
            // `None`, AND IT MUST MATCH WHAT THE WRITER BUILT. This is a CLEAR-ON-EXACT-MATCH
            // strip, so if an emitter still names the element in the handle it builds, this
            // derivation disagrees and the strip DECLINES -- storing the whole composite key on
            // every row. That is not hypothetical: when `generation` stopped being
            // `block_id.or(object_id)` the two copies of this derivation drifted and the delta
            // went from 100 allocations and 4,520 bytes to 2,202 and 82,653 over twenty writes at
            // a 3,200-member set. `collect_upsert_index_items` passes `None` too, for this reason.
            None,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            address.block_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
        if derived == self.block_ref_key {
            self.block_ref_key.clear();
        }
    }

    /// Rebuild the composite key the writer left out.
    ///
    /// The inverse of `strip_block_ref_key_repeat`. A log written before that stripping carries the
    /// key, and this leaves it alone: it fills only what is absent.
    fn restore_block_ref_key_repeat(&mut self) {
        if !self.block_ref_key.is_empty() {
            return;
        }
        let Some(address) = self.entry.as_ref() else {
            return;
        };
        self.block_ref_key = block_ref_key_from_parts(
            &self.model_id,
            &self.object_key,
            // `None`, the inverse of the strip's own term. A row stored before this change carries
            // a name in slot 5 and this rebuilds a handle that does not spell it -- which is inert,
            // because nothing reads the rebuilt handle:
            // `page_entry_element_naming::the_stored_page_handle_decides_nothing_the_fold_files`
            // folds the same item under the named, the unnamed and a nonsense handle and gets one
            // identical entry, down to the handle the map allocated.
            None,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            address.block_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
    }

    /// Drop the size when the address already states it.
    ///
    /// `size` and `address.length` are the same number on a page item, written twice.
    /// ABSORBED INTO THE CODEC, and kept as a no-op only so the call sites that walk a record
    /// still read as one list of per-item steps.
    ///
    /// The size is no longer a field, so it cannot repeat the address's length: the serializer
    /// writes the strip sentinel unconditionally and the conversion from the wire restores it.
    /// What this function used to do is now true by construction.
    fn strip_size_repeat(&mut self) {}

    /// Put the size back from the address that carries it.
    /// ABSORBED INTO THE CODEC. See [`IndexItem::strip_size_repeat`].
    fn restore_size_repeat(&mut self) {}

    /// Drop the object id when it is the hash of what this row already says.
    ///
    /// An object id is `stable_block_object_id` of the shard, the kind and the key -- and a row
    /// carries the last two, with the record carrying the shard. So the nine bytes it takes are
    /// nine bytes restating a hash of fields sitting beside it.
    ///
    /// The component used to be a term of that hash and is not one now. It does not matter here:
    /// the row still carries it as its own field, and what this strips is a REPEAT of the
    /// derivation, which is recomputed from the same terms on the way back in.
    ///
    /// Stripped only when the derivation AGREES with what is stored. A row whose id came from
    /// somewhere else keeps it, so a disagreement costs bytes rather than correctness.
    fn strip_object_id_repeat(&mut self, shard_id: ShardId) {
        if self.object_id == self.derived_object_id(shard_id) {
            self.object_id = 0;
        }
    }

    /// Put the object id back by deriving it again.
    fn restore_object_id_repeat(&mut self, shard_id: ShardId) {
        if self.object_id == 0 {
            self.object_id = self.derived_object_id(shard_id);
        }
    }

    fn derived_object_id(&self, shard_id: ShardId) -> u64 {
        crate::engine::hashing::stable_block_object_id(
            shard_id,
            &self.model_id,
            &self.object_key,
        )
    }

    /// NOTHING LEFT TO STRIP, AND THE FUNCTION STAYS AS THE PLACE THAT RECORDS WHY.
    ///
    /// It stripped the object-id repeat that an item and its address both carried: for a page
    /// belonging to one object those were the same value, and the WAL side strips the same pair on
    /// its way to protobuf -- `item_to_proto` calls it "a full varint on every item whose page
    /// belongs to one object". MEASURED WHILE IT EXISTED, the pair was 18 bytes of a 142-byte item,
    /// 12.7%. That figure is kept rather than deleted with the behaviour, because it is the reason
    /// there was something to strip and therefore the reason there is now nothing.
    ///
    /// THE ADDRESS CARRIES NEITHER OF THEM NOW. The routing bucket went when an address stopped
    /// holding one; the object id went the same way, and the entry derives it from its own terms.
    /// So the item's own fields are the only copy on the wire, which is exactly the state this
    /// stripping existed to arrange -- it did not become unnecessary, it succeeded.
    ///
    /// `restore_address_repeats` is its inverse and is empty for the same reason: a row written
    /// before the width moved still carries an `oi`, the decode reads it for the generation
    /// cross-check, and the entry ignores it.
    fn strip_address_repeats(&mut self) {}

    /// Put back what the writer left out, from the fields that carry it.
    ///
    /// The inverse of `strip_address_repeats` for anything that writer produced. A log written
    /// before that stripping still carries both, and this leaves those alone: it fills only what is
    /// absent.
    fn restore_address_repeats(&mut self) {
        // NOTHING LEFT TO RESTORE, and the body that used to stand here was already an empty
        // `if` over the address. The routing bucket is not restored onto the address because the
        // address has nowhere to put it -- the item's `routing_bucket` field is what every reader
        // uses -- and the object id is derived by `restore_object_id_repeat`. Kept as a no-op so
        // the per-item restore steps still read as one list at their call sites.
    }
}

/// The model names this log writes as a number instead of as themselves.
///
/// A page header in this design spells the model as an integer; ours spelled it as its name, on
/// every item -- "string" and "hash" written out again for every page a shard has ever indexed,
/// measured at 7 bytes of a 176-byte item.
///
/// APPEND ONLY, and never reordered. The position IS what goes on disk, so moving a name changes
/// what an already-written log says. A name that is not in this table is written as itself, so the
/// table never has to be complete and a new model costs nothing until it is added here.
/// The item kind, as a number.
///
/// The item taxonomy this follows is an enum -- an integer on the wire. Ours was three words,
/// `"page"`, `"object"` and `"meta"`, written once per item, and the block kind was nearly every
/// item a shard writes. The word is now spelled `"block"` wherever the enum is serialized
/// directly; this encoding writes the number either way, and the inverse below reads both words.
///
/// The numbers are fixed by this function and its inverse; a kind added later takes the next one
/// and must never take another kind's. A name is still read, so a log written before this decodes
/// unchanged.
fn item_kind_as_number<S>(value: &IndexItemKind, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u64(match value {
        IndexItemKind::Block => 0,
        IndexItemKind::Object => 1,
        IndexItemKind::Meta => 2,
    })
}

fn item_kind_either_shape<'de, D>(deserializer: D) -> Result<IndexItemKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = IndexItemKind;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an item kind, as its name or as its number")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<IndexItemKind, E> {
            match value {
                // BOTH SPELLINGS, AND THIS IS A SECOND DECODER FOR THE SAME TOKEN. The enum's own
                // serde `rename`/`alias` does not reach this visitor: the row writes the kind
                // through `item_kind_as_number` and reads it back here, so widening the enum left
                // this arm single-form. A log written before the number encoding spells the kind
                // `page`, and anything that serializes the enum directly now spells it `block`;
                // both have to land on the same variant or an index-log replay refuses a row it
                // wrote itself.
                "block" | "page" => Ok(IndexItemKind::Block),
                "object" => Ok(IndexItemKind::Object),
                "meta" => Ok(IndexItemKind::Meta),
                other => Err(E::custom(format!("unknown item kind {other}"))),
            }
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<IndexItemKind, E> {
            self.visit_str(&value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<IndexItemKind, E> {
            match value {
                0 => Ok(IndexItemKind::Block),
                1 => Ok(IndexItemKind::Object),
                2 => Ok(IndexItemKind::Meta),
                // Refused rather than defaulted: a kind this cannot name would otherwise arrive as
                // a page item, and a page item names an address that an object or meta item does
                // not have.
                other => Err(E::custom(format!("unknown item kind number {other}"))),
            }
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<IndexItemKind, E> {
            if value < 0 {
                return Err(E::custom(format!("negative item kind {value}")));
            }
            self.visit_u64(value as u64)
        }
    }

    deserializer.deserialize_any(EitherShape)
}

/// Which shape a payload says it holds, if it says.
///
/// A payload with no container magic is the JSON fallback, which only the whole-index path ever
/// wrote, so it answers for that shape rather than for nothing.
pub(crate) fn index_payload_shape(payload: &[u8]) -> Option<u8> {
    match payload.first() {
        Some(container) => Some(index_container_parts(*container).1),
        // No container: written before there was one. Nothing writes that shape now -- even
        // `append_json` wraps its record -- so a payload without a container says nothing about
        // which record it holds, and answering None hands it to the decoder. That serves both
        // cases it can be: a record from an older writer decodes, and a well-framed payload of
        // garbage is REPORTED rather than skipped, because a sweep that quietly skips what it
        // cannot read turns committed corruption into silent data loss.
        None => None,
    }
}

/// The first two values of a record, whatever the rest of it holds.
///
/// Two record shapes share this log and the sweep reads one as the other: it wants a sequence and
/// nothing else, and a delta record also parses as a whole index record with its delta fields
/// dropped. A map allowed that, matching by name and defaulting what was absent. A row cannot: a
/// reader built for three values refuses a row of six.
///
/// Both shapes begin with the same two values, so this takes those by position and ignores
/// whatever follows. It reads a map too, so a record written before rows still answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexRecordHead {
    pub(crate) shard_id: ShardId,
    pub(crate) sequence: u64,
    pub(crate) applied_wal_sequence: Option<u64>,
}

impl<'de> serde::Deserialize<'de> for IndexRecordHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Head;

        impl<'de> serde::de::Visitor<'de> for Head {
            type Value = IndexRecordHead;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an index-log record, as a row or as a map")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<IndexRecordHead, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use serde::de::Error as _;
                let shard_id: ShardId = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("record has no shard"))?;
                let sequence: u64 = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("record has no sequence"))?;
                // Position two is the payload -- rows, or a whole index. A whole-index record
                // ends there; a delta carries its meta and then its applied sequence.
                let _payload: Option<serde::de::IgnoredAny> = seq.next_element()?;
                // Position three is the meta item on a delta record; a whole-index record ends
                // before it. It is skipped rather than read: reading it as the applied sequence
                // is a five-field struct answering a question about a number.
                let _meta: Option<serde::de::IgnoredAny> = seq.next_element()?;
                let applied_wal_sequence: Option<u64> =
                    seq.next_element::<Option<u64>>()?.flatten();
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(IndexRecordHead {
                    shard_id,
                    sequence,
                    applied_wal_sequence,
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<IndexRecordHead, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                use serde::de::Error as _;
                let mut shard_id = None;
                let mut sequence = None;
                let mut applied = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "s" | "shard_id" => shard_id = Some(map.next_value()?),
                        "q" | "sequence" => sequence = Some(map.next_value()?),
                        "aw" | "applied_wal_sequence" => applied = map.next_value()?,
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(IndexRecordHead {
                    shard_id: shard_id.ok_or_else(|| A::Error::custom("record has no shard"))?,
                    sequence: sequence.ok_or_else(|| A::Error::custom("record has no sequence"))?,
                    applied_wal_sequence: applied,
                })
            }
        }

        deserializer.deserialize_any(Head)
    }
}

/// A row, not a map: the values in field order, with the order fixed here.
///
/// Every row in this log has the same shape, so the shape does not belong in the row. As a map,
/// each row carried its own field names -- measured at about 25 bytes of a 126-byte record, the
/// largest single line item left in it. A protobuf design pays a tag for this; a self-describing
/// one pays a name; a row pays nothing and is read by position.
///
/// The record AROUND the rows stays a map on purpose. Two record shapes share this log and one is
/// read as the other -- a delta container is decoded as a whole index record by the sweep -- which
/// works because a map matches by name and a missing field defaults. Positions cannot do that, so
/// only the rows are positional.
///
/// The derived `Deserialize` takes either shape, so rows written before this still decode; the
/// order below must match the field order of the struct and must not be reordered.
impl serde::Serialize for IndexItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq as _;

        struct Kind<'a>(&'a IndexItemKind);
        impl serde::Serialize for Kind<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                item_kind_as_number(self.0, s)
            }
        }
        struct Handle<'a>(&'a str);
        impl serde::Serialize for Handle<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                block_ref_key_as_number_when_it_is_one(self.0, s)
            }
        }
        struct Model<'a>(&'a str);
        impl serde::Serialize for Model<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                model_id_as_number_when_it_is_known(self.0, s)
            }
        }

        let mut row = serializer.serialize_seq(Some(INDEX_ITEM_SLOTS))?;
        row.serialize_element(&Kind(&self.kind))?;
        row.serialize_element(&self.routing_bucket)?;
        row.serialize_element(&Handle(&self.block_ref_key))?;
        row.serialize_element(&self.object_key)?;
        row.serialize_element(&Model(&self.model_id))?;
        // SLOT 5 IS GONE, AND THAT IS A STORED-FORMAT CHANGE RATHER THAN A CHANGE OF VALUE.
        //
        // It held the element name, and then a constant nil for one step -- the slot kept so every
        // stored row would still decode while nothing read what it carried. The note that stood
        // here recorded the measurement that made keeping it the only option at the time, and that
        // measurement still holds as a statement about compatibility: a stored twelve-slot row read
        // by an eleven-field struct is refused ("wrong msgpack marker FixStr(2)" when the slot
        // holds a name, "invalid type: unit value, expected u64" when it holds nil), and an
        // eleven-slot row read by a twelve-field struct is refused too. BOTH DIRECTIONS BREAK,
        // which is exactly why this is spent as a format change and not smuggled in as a
        // compatible one.
        //
        // WHAT PAYS FOR IT: nothing is deployed, so there are no rows written by an older binary
        // that have to keep loading, and a slot written on every row to say nothing is dead weight.
        // What is NOT being done is the dangerous half -- no field something READS to reconstruct
        // state comes off here. `IndexItem` had already stopped carrying this one, the fold has no
        // field to set, and the rule above `strip_block_ref_key_repeat` settled who reads the
        // reconstruction: nothing does.
        //
        // AND THE REFUSAL IS THE DELIVERABLE, NOT THE SAVING. A stored twelve-slot row is refused
        // BY NAME in both decode arms -- by arity in `IndexItemRow`, in a message naming twelve and
        // eleven, and by `deny_unknown_fields` for the named shape -- so it cannot be half-applied.
        //
        // THE SAVING IS ONE BYTE A ROW, MEASURED: the hoisted common row went 30 B to 29 B in
        // `tests::the_hoisted_row_is_twenty_nine_bytes_and_the_kind_spelling_is_not_a_confound`,
        // which is the baseline every row figure in this file is quoted against. One byte is what a
        // msgpack nil costs, and the slot carried nothing else -- so the figure is also the whole
        // of it, on every row of every kind. Not quoted from arithmetic: that arm reddened at 30
        // against a measured 29, which is how the number was taken.
        row.serialize_element(&self.object_id)?;
        // THE THREE DERIVED SLOTS, computed here because here is where they are written.
        //
        // `sz` IS THE STRIP SENTINEL AND ALWAYS ZERO NOW, which is what keeps the stored bytes
        // identical. `strip_size_repeat` already wrote zero whenever the size equalled
        // `address.length()`, and with the field gone that equality is no longer representable --
        // so the harvest it banked is now unconditional rather than conditional. Writing the real
        // length here instead would have undone a 4 B/row saving with every row still decoding
        // and nothing failing.
        let address = self.entry.as_ref();
        row.serialize_element(&address.and_then(ElementEntry::block_id).unwrap_or_default())?;
        row.serialize_element(&address)?;
        row.serialize_element(&0u64)?;
        row.serialize_element(&address.map(|a| a.block_id().is_none()).unwrap_or(false))?;
        row.serialize_element(&self.deleted)?;
        row.end()
    }
}

/// The spellings a row may write as a NUMBER instead of a string, and the number IS the position.
///
/// APPEND-ONLY, AND THE POSITION IS THE WIRE VALUE. Index 0 is `"string"`, so a stored row holding
/// the number 0 means `"string"` and will mean it forever. Inserting or reordering an entry
/// renumbers every entry after it and silently reinterprets the model of every row already
/// written. New spellings go at the END and nowhere else.
///
/// AND THIS NUMBERING IS NOT THE REGISTRY'S, WHICH IS THE TRAP WORTH NAMING. The entry spelling's
/// own registry (`engine::storage_bucket_internals`'s `model_kind_registry!`) gives each kind an
/// EXPLICIT code with deliberate holes -- `"string"` is 1 there, `"hash"` is 2, and 5 and 15 belong
/// to retired names. This list is POSITIONAL and 0-based, so `"hash"` is 1 here and 1 is
/// `"string"` there. The two numbering schemes agree on nothing.
///
/// So if `IndexItem::model_id` ever becomes the one-byte `StoredModelKind` the resident entry
/// already holds -- which is worth 23 bytes resident and is the obvious next step -- THE WIRE
/// NUMBER MUST STILL BE THIS LIST'S POSITION AND NOT THE REGISTRY'S CODE. Serializing the
/// registry code would be slot-preserving in shape and incompatible in value: every stored row's
/// model would be reinterpreted as a different kind, with no error anywhere. The in-memory type
/// may change; the mapping through this list may not.
///
/// MEMBERSHIP IS GUARDED, because this is a second list of spellings sitting beside a registry
/// that derives its own retired half as the complement of its live half "so it cannot disagree
/// with the declaration". This list had no such tie and did go stale: `"zset"` and `"list"` were
/// added to the registry as live kinds and never added here, so two live kinds wrote their full
/// string spelling on every index-log row. `engine::tests::model_number_agreement` is the tie.
pub(crate) const MODEL_ID_NUMBERS: &[&str] = &[
    "string",
    "hash",
    "set",
    "feature",
    "sequence",
    "control_state",
    "context_node",
    "context_event",
    "context_index",
    "context_audit",
    "context_child",
    "context_embedding",
    "context_summary",
    "context_compression",
    "context_entity",
    // APPENDED, NOT INSERTED. Both are LIVE kinds the registry declares -- `"zset"` at its
    // code 6 and `"list"` at its code 12, the two codes the old packed report skipped -- and
    // this list never learned either, so both wrote their full string spelling on every row.
    // They go at the END so every position above keeps the meaning it already has on disk.
    "zset",
    "list",
];

fn model_id_as_number_when_it_is_known<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match MODEL_ID_NUMBERS.iter().position(|name| *name == value) {
        Some(index) => serializer.serialize_u64(index as u64),
        None => serializer.serialize_str(value),
    }
}

/// What the model slot costs for one spelling, priced THROUGH the adapter the row writes it with.
///
/// Exists so the guard on the two spelling lists can carry a byte figure rather than a correctness
/// claim alone, and so that figure cannot drift from the encoder: pricing this slot any other way
/// -- encoding the `&str` directly, say -- prices a row nobody emits.
/// What NUMBER the model slot actually carries for one spelling, read back THROUGH the adapter
/// that writes it.
///
/// The sibling of [`model_id_slot_bytes`] and for the same reason: pricing or pinning this slot any
/// other way pins a row nobody emits. This one returns the VALUE rather than the width, because the
/// hazard here is not size -- it is that the number written is `MODEL_ID_NUMBERS`' POSITION and not
/// the registry's `report_code()`, and those two agree on nothing. `None` means the spelling was
/// written as itself, which is what an unlisted model does.
#[cfg(test)]
pub(crate) fn model_id_slot_number(model: &str) -> Option<u64> {
    struct Model<'a>(&'a str);
    impl serde::Serialize for Model<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            model_id_as_number_when_it_is_known(self.0, s)
        }
    }
    let value: serde_json::Value =
        serde_json::to_value(Model(model)).expect("encode the model slot");
    value.as_u64()
}

#[cfg(test)]
pub(crate) fn model_id_slot_bytes(model: &str) -> usize {
    struct Model<'a>(&'a str);
    impl serde::Serialize for Model<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            model_id_as_number_when_it_is_known(self.0, s)
        }
    }
    rmp_serde::to_vec(&Model(model))
        .expect("encode the model slot")
        .len()
}

fn model_id_either_shape<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a model, as its name or as its number")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<String, E> {
            // A number this table cannot name is refused rather than guessed. Answering the wrong
            // model would file a page under a model that did not write it, which reads as missing
            // data for one model and foreign data for another.
            MODEL_ID_NUMBERS
                .get(value as usize)
                .map(|name| (*name).to_string())
                .ok_or_else(|| E::custom(format!("unknown model number {value}")))
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<String, E> {
            if value < 0 {
                return Err(E::custom(format!("negative model number {value}")));
            }
            self.visit_u64(value as u64)
        }
    }

    deserializer.deserialize_any(EitherShape)
}

/// One empty `Arc<str>`, shared, for blanking a key the record has hoisted.
///
/// `String::clear()` was free. An `Arc<str>` cannot be emptied in place, and
/// `Arc::from("")` allocates -- once per ITEM on the hoist path, which is the path the
/// hoist exists to make cheaper. Cloning one shared empty is an atomic increment and no
/// allocation at all, so the hoist keeps costing what it did.
fn empty_object_key() -> std::sync::Arc<str> {
    static EMPTY: std::sync::OnceLock<std::sync::Arc<str>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(|| std::sync::Arc::from("")).clone()
}

fn block_ref_key_as_number_when_it_is_one<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value.parse::<u64>() {
        Ok(number) => serializer.serialize_u64(number),
        Err(_) => serializer.serialize_str(value),
    }
}

fn block_ref_key_either_shape<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a page handle, as a string or an unsigned integer")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<String, E> {
            Ok(value.to_string())
        }
    }

    deserializer.deserialize_any(EitherShape)
}

/// Slab lifecycle state folded into the index-log MetaItem. 1:1 with
/// `block_store::BlockStoreSlabState` and with the on-disk slab-state encoding
/// (INIT/CREATED/FROZEN/RECYCLED): Active==CREATED, Sealed==FROZEN, DelayedDestroy/Purged
/// cover the RECYCLED grace. Serialized snake_case so it round-trips with the slab manifest's
/// own state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlabCatalogState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// One slab catalog entry folded into the index-log MetaItem, mirroring this design
/// the durable slab catalog in the index-log anchor. Carries the DURABLE catalog fields the
/// slab descriptor tracks -- lifecycle state, byte counts, timestamps, page-id range, version.
/// The slab descriptor's DIAGNOSTIC fields (readable_prefix_physical_bytes / has_corruption /
/// first_error*) are intentionally ABSENT: they are recomputed on load by scanning the slab
/// (`inspect_slab`, driven by `rebuild_slab_manifest_at` / reconcile-on-open), exactly as the
/// are not persisted. So this is the lossless durable projection of a slab.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlabCatalogEntry {
    #[serde(alias = "zone_id")]
    #[serde(alias = "page_slab_id")]
    pub block_slab_id: u64,
    pub state: SlabCatalogState,
    #[serde(alias = "total_bytes")]
    pub physical_bytes: u64,
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default)]
    pub created_unix_ms: Option<u64>,
    #[serde(default)]
    pub updated_unix_ms: Option<u64>,
    #[serde(rename = "first_block_id", alias = "first_page_id", default)]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default)]
    pub last_block_id: Option<u64>,
    #[serde(default)]
    pub version: u64,
}

/// One per SLAB, not per item: a slab is a gigabyte of pages by default, so this is here to be
/// ranked last rather than to be narrowed.
const _: () = assert!(std::mem::size_of::<SlabCatalogEntry>() == 104);

/// Compaction anchor for the delta log. `start_wal_sequence` is the lowest WAL sequence
/// still required to reconstruct the served index on top of the base snapshot: once the
/// base `shard-{id}.index.json` is rewritten at a compaction point, the anchor advances
/// and every delta record at or before it can be truncated. Matches
/// MetaItem's `start_WAL_id` role in the native WAL vocabulary.
///
/// `slabs` folds the slab catalog into the anchor, and serializes under its original
/// `zones` key. It
/// is populated ONLY at a threshold dump;
/// with the gate off it is always empty and (via `skip_serializing_if`) not serialized, so an
/// anchor record is byte-identical to the pre-fold record. Anchors without `zones`
/// deserialize to an empty catalog and replay unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaItem {
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub start_wal_sequence: u64,
    #[serde(default)]
    pub timestamp_ms: u64,
    #[serde(rename = "zones", default)]
    pub slabs: Vec<SlabCatalogEntry>,
    #[serde(alias = "zone_version", default)]
    pub slab_version: u64,
}

/// One appended delta record: either a batch of page/object item deltas (PAGE/OBJECT) or
/// a compaction anchor (META). Written one JSON line per record to the same append-only
/// log file as `IndexLogRecord`; the two are distinguished on read by the presence of the
/// `items`/`meta` fields, so legacy whole-index records replay untouched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDeltaRecord {
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    #[serde(default)]
    pub items: Vec<IndexItem>,
    #[serde(default)]
    pub meta: Option<MetaItem>,
    /// The WAL sequence this write reflected (the served-index anchor at append time). On
    /// load, deltas with a sequence at or below the base snapshot's anchor are already
    /// folded into the base and are skipped; folding the rest advances the reconstructed
    /// anchor so WAL replay re-executes only the uncaptured tail (never relocating the
    /// pages the deltas already pin at their original addresses).
    #[serde(rename = "aw", alias = "applied_wal_sequence", default)]
    pub applied_wal_sequence: Option<u64>,
    /// When true, this record's items are the exact pages the write produced: replay replaces
    /// each item's (kind, object, component) predecessor and inserts, WITHOUT the covered-key
    /// wipe -- mirroring the write path's upsert_bucket_index_block. When false (the default,
    /// and every record written before this field existed), the record snapshots each covered
    /// object's whole page set and replay wipes-then-restores. The snapshot shape is what made
    /// every append O(store): a batch touching the grow-with-the-store index hashes logged
    /// every page of each of them, every time.
    #[serde(rename = "u", alias = "upsert", default)]
    pub upsert: bool,
    /// Opaque per-touched-key state blobs (one JSON object per key) carrying the
    /// authoritative post-write value of the maps that are NOT reconstructable from a
    /// single page-index entry -- packed timestamped series (feature membership survives
    /// eviction) and the non-page maps (TTL, control-state change/selection, context
    /// nodes). Opaque here so the index-log layer stays decoupled from `ShardState`; the
    /// engine builds and applies them. Replaying these on load pins the exact membership a
    /// write produced, so reconstruction from physical pages cannot resurrect evicted data.
    #[serde(rename = "ks", alias = "key_states", default)]
    pub key_states: Vec<serde_json::Value>,
    /// The one object key every item carries, when they all carry the same one.
    ///
    /// A timestamped write stages ONE ITEM PER REF -- all for the same object, differing only in
    /// their component -- so a feature append of thirty-two points wrote that key thirty-two
    /// times. Measured on a record of that shape: 21 bytes per item, 24-31% of the record.
    ///
    /// Set only when every item agrees, and then each item's own `object_key` is written empty
    /// and this is what the reader puts back.
    ///
    /// Deliberately NOT a "same as the previous item" marker. That has to read an empty key as
    /// "repeat", and an item whose key really is empty cannot be ruled out here: the empty-key
    /// guards live in the HTTP handlers and the proxy, not in the engine. This is unambiguous in
    /// every case instead -- keys that differ leave it `None` and are all written, and a record
    /// whose items all share the empty string sets it to `Some(String::new())` and restores
    /// exactly that.
    ///
    /// No `skip_serializing_if`. The encoding is POSITIONAL, so a skipped field shifts every
    /// field after it; `None` goes on the wire as nil. Appended at the END, which is the only
    /// safe place to add to a positional record.
    ///
    /// AND THE RULE THAT MAKES THIS HOIST SAFE, WHICH IS NOT THE STAMP. Hoisting a repeated field
    /// onto the record means the writer puts a SENTINEL in each item's own slot and the reader
    /// puts the real value back. That is safe here for one reason and it is worth naming, because
    /// it does not generalise: AN EMPTY OBJECT KEY IS OUTSIDE THE FIELD'S LEGAL DOMAIN. A reader
    /// that does not know about this field -- an older binary -- folds items whose key is the
    /// empty string, and an empty key is not a key, so the damage is loud.
    ///
    /// `routing_bucket` is the counter-example, and it is why a bucket hoist is NOT available on
    /// the same pattern despite being worth a measured 2 B of a 30 B row. Its sentinel would have
    /// to be zero, and ZERO IS A LEGAL BUCKET: `bucket_for_object` returns
    /// `start_routing_bucket + hash % count`, so a shard whose range starts at zero owns bucket
    /// zero. An older binary reading a hoisted record would take that zero literally and fold
    /// EVERY entry into bucket zero, silently and with no error anywhere.
    ///
    /// AND A FORMAT STAMP CANNOT STOP IT. The guard at `engine::persistence`'s index load is
    /// one-sided -- `if shard.index_format_version < SHARD_INDEX_FORMAT_VERSION` -- so it refuses
    /// a store written by an OLDER binary and ACCEPTS one written by a newer. Raising the stamp
    /// protects a new binary from an old store and does nothing in the direction that matters
    /// here. So the rule is a property of the field, not of the version: HOIST ONLY WHEN THE
    /// SENTINEL IS OUTSIDE THE FIELD'S LEGAL DOMAIN, and when it is not, the field stays on the
    /// item however much it repeats.
    #[serde(rename = "sk", alias = "shared_object_key", default)]
    pub shared_object_key: Option<String>,
}

/// The key a page is written under, from its parts.
///
/// One definition, called from the page index's serialization and from the replay log, so the two
/// cannot drift into different spellings of the same page.
pub fn block_ref_key_from_parts(
    kind: &str,
    object_key: &str,
    component: Option<&str>,
    block_slab_id: u64,
    offset: u64,
    length: u64,
    block_id: u64,
    generation: u64,
) -> String {
    // Built into one buffer rather than with `format!`, which allocates its own and then
    // allocates again to hand back a String. This runs once per page on every dump of the index,
    // where it was the largest single source of allocations.
    use std::fmt::Write as _;
    let component = component.unwrap_or("");
    // Five u64 at their widest, plus the seven separators.
    let mut key = String::with_capacity(kind.len() + object_key.len() + component.len() + 7 + 100);
    key.push_str(kind);
    key.push(':');
    key.push_str(object_key);
    key.push(':');
    key.push_str(component);
    // `write!` into a String appends in place; it does not allocate.
    let _ = write!(
        key,
        ":{block_slab_id}:{offset}:{length}:{block_id}:{generation}"
    );
    key
}

/// The eight slots [`block_ref_key_from_parts`] composed, read back out of the key it built.
///
/// BORROWED, NOT OWNED. The text slots are slices of the key, so reading a page's identity back
/// out of the name it is filed under allocates nothing until a caller asks for an owned copy. The
/// served index's decode asks for exactly one -- the object key, as the `Arc<str>` the entry used
/// to carry -- so the parse costs what the removed field cost and no more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRefKeyParts<'a> {
    pub kind: &'a str,
    pub object_key: &'a str,
    pub component: &'a str,
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    pub block_id: u64,
    pub generation: u64,
}

/// WHY A KEY THAT WILL NOT PARSE IS AN `Err` AND NEVER A PANIC.
///
/// Every caller of the parse is a LOAD PATH. `ObjectRecord::try_from` and
/// `StoredModelKind::deserialize` both state the trade in as many words and this follows them: a
/// panic on the load path makes a store UNOPENABLE, while an `Err` becomes a decode failure that
/// `load_index` turns into a replay of the write-ahead log -- slower and correct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRefKeyUnparsable {
    pub key: String,
    pub because: &'static str,
}

impl std::fmt::Display for BlockRefKeyUnparsable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the stored page key {:?} is not a block-ref key: {}. A page's identity is read back \
             out of the name it is filed under, so a key this cannot invert is an index that \
             names no object -- refused here so the caller replays its log rather than serving a \
             page filed under a guess",
            self.key, self.because
        )
    }
}

/// A decimal slot is accepted only in the spelling [`block_ref_key_from_parts`] would WRITE.
///
/// `u64::from_str` takes a leading `+` and takes leading zeros, and both of those re-render as
/// something else -- so accepting them would make the parse something other than the renderer's
/// inverse, on keys that round-trip to a DIFFERENT string. Rejecting them is what lets the
/// recomposition be exact by construction rather than by a rebuilt string compared per page.
fn canonical_decimal(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if text.len() > 1 && text.as_bytes()[0] == b'0' {
        return None;
    }
    text.parse::<u64>().ok()
}

/// THE INVERSE OF [`block_ref_key_from_parts`], AND IT IS EXACT.
///
/// # THE SPLIT IS POSITIONAL FROM THE RIGHT, WHICH IS THE WHOLE DESIGN
///
/// An OBJECT KEY arrives from a caller and may hold a colon; the kind may not (every spelling in
/// `model_kind_registry` is a lowercase identifier, which
/// `the_key_a_page_is_filed_under_inverts_exactly::no_stored_model_spelling_holds_the_separator`
/// asserts rather than assumes) and a rendered `u64` may not. So the six right-hand fields are cut
/// from the RIGHT, the kind from the LEFT, and whatever is between them is the object key --
/// colons included. An object key that itself spells a plausible tail, `x:0:0:1:2:3`, therefore
/// comes back whole: the cuts are counted from the end, not searched for.
///
/// # WHERE IT IS NOT EXACT, AND WHY NOTHING THIS TREE WRITES CAN REACH THERE
///
/// A NON-EMPTY COMPONENT CONTAINING A COLON MAKES THE COMPONENT/OBJECT-KEY BOUNDARY
/// UNRECOVERABLE, and that is a property of the format rather than of this parse:
/// `block_ref_key_from_parts("string", "a:b", None, ..)` and
/// `block_ref_key_from_parts("string", "a", Some("b:"), ..)` compose the IDENTICAL key.
/// `what_the_core_index_puts_on_the_wire::the_stored_page_key_is_not_self_delimiting_so_no_parser_can_invert_it`
/// is the proof and it still stands. This parse resolves the ambiguity one way -- positionally,
/// reading that key as the object `a:b` with no component.
///
/// NO PAGE KEY THIS TREE WRITES CAN BE AMBIGUOUS, and that is structural rather than measured:
/// [`crate::engine::state::block_index_written_key`] takes no component parameter and passes a
/// literal `None`, so the slot is always empty. It is also MEASURED, on 8,000 pages of two key
/// shapes, by
/// `the_key_a_page_is_filed_under_inverts_exactly::the_renderer_and_the_parse_are_inverse_on_every_page_of_the_shipped_corpus`,
/// which asserts the component slot empty page by page. An index carrying the ambiguous shape
/// predates `SHARD_INDEX_FORMAT_VERSION` 27 and is refused by `persistence`'s `<` comparison at
/// every one of `decode_index_bytes`'s production callers before anything acts on it.
///
/// # EXACT BY CONSTRUCTION RATHER THAN BY A REBUILT STRING
///
/// The eight slots are SLICES of the key taken at the separator offsets, so concatenating them
/// with the seven separators reproduces the input byte for byte without anything having to check
/// it -- provided each decimal slot is in the one spelling the renderer writes, which
/// [`canonical_decimal`] is what enforces. A verification string built per page would cost an
/// allocation per page on every load to re-establish what the index arithmetic already gives.
/// `the_renderer_and_the_parse_are_inverse_on_every_page_of_the_shipped_corpus` drives the
/// property on 8,000 real pages and
/// `a_key_that_does_not_round_trip_is_refused_rather_than_guessed` drives the refusals.
pub fn block_ref_key_parts(key: &str) -> Result<BlockRefKeyParts<'_>, BlockRefKeyUnparsable> {
    let refuse = |because: &'static str| BlockRefKeyUnparsable { key: key.to_string(), because };
    // Six fields from the right -- the five numbers and the component slot -- so six separators.
    let mut cut = key.len();
    let mut at = [0usize; 6];
    for slot in 0..6 {
        match key[..cut].rfind(':') {
            Some(found) => {
                at[slot] = found;
                cut = found;
            }
            None => {
