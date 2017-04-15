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
                return Err(refuse(
                    "it holds fewer than seven separators, so it cannot be the eight-slot \
                     composite the page index is keyed by",
                ))
            }
        }
    }
    let head = &key[..at[5]];
    let (kind, object_key) = head.split_once(':').ok_or_else(|| {
        refuse(
            "its head holds no separator, so there is no slot in it for a kind and an object key \
             both",
        )
    })?;
    let numeric = |text: &str, which: &'static str| {
        canonical_decimal(text).ok_or(BlockRefKeyUnparsable { key: key.to_string(), because: which })
    };
    Ok(BlockRefKeyParts {
        kind,
        object_key,
        component: &key[at[5] + 1..at[4]],
        block_slab_id: numeric(
            &key[at[4] + 1..at[3]],
            "its slab id slot is not a decimal in the spelling the renderer writes",
        )?,
        offset: numeric(
            &key[at[3] + 1..at[2]],
            "its offset slot is not a decimal in the spelling the renderer writes",
        )?,
        length: numeric(
            &key[at[2] + 1..at[1]],
            "its length slot is not a decimal in the spelling the renderer writes",
        )?,
        block_id: numeric(
            &key[at[1] + 1..at[0]],
            "its block id slot is not a decimal in the spelling the renderer writes",
        )?,
        generation: numeric(
            &key[at[0] + 1..],
            "its generation slot is not a decimal in the spelling the renderer writes",
        )?,
    })
}

// ---------------------------------------------------------------------------------------------
// WHAT THIS STORE READS OFF DISK, COUNTED WHERE THE READING HAPPENS.
//
// `IndexLogStats::bytes_read` counts what a read HANDS BACK -- the records inside the window a
// caller asked for. That is a useful number and it is not this one. The load-path fold reads
// whole PIECES and hands back RECORDS, and it fed neither counter at all: #1936 measured a
// 200,000-record restore reading 8,119,256 bytes off this store's pieces in 1,069 reads while
// `index_log_store().stats()` answered `bytes_read: 0`. Measured again here at 2,000 and 8,000
// records, the fold read 163,236 and 655,236 bytes -- the whole log, both times -- against a
// store that reported zero.
//
// A READER WITH NO COUNTER CONTRIBUTES ZERO TO EVERY REPORT, WHICH IS INDISTINGUISHABLE FROM A
// READER DOING NO WORK. That is how 965,415,651 bytes -- 95% of a restore -- survived every
// previous measurement of that path.
//
// Process-wide rather than per-store, and deliberately: pieces are read from free functions that
// hold no store (`last_sequence_at`, `index_log_segment_span_of`), so charging a store would mean
// threading one through the whole read side to buy a number that is already obtainable. The
// thread-local beside it is what a single restore wants, because a restore runs on one thread
// while the suite does not.
// ---------------------------------------------------------------------------------------------

static INDEX_LOG_PIECE_BYTES_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static INDEX_LOG_PIECE_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    static INDEX_LOG_PIECE_BYTES_READ_HERE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static INDEX_LOG_PIECE_READS_HERE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Bytes and read calls this PROCESS has made against index-log pieces since it started.
pub fn index_log_piece_read_counts() -> (u64, u64) {
    (
        INDEX_LOG_PIECE_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOG_PIECE_READS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Bytes and read calls THIS THREAD has made against index-log pieces since it started.
///
/// A restore runs on one thread; the suite runs many at once. Measuring a restore against the
/// process counter reads another test's reads as this one's.
pub fn index_log_piece_read_counts_on_this_thread() -> (u64, u64) {
    (
        INDEX_LOG_PIECE_BYTES_READ_HERE.with(|bytes| bytes.get()),
        INDEX_LOG_PIECE_READS_HERE.with(|reads| reads.get()),
    )
}

/// What one span of reading against index-log pieces cost, published when it goes out of scope.
///
/// Held mutably by the [`CountedIndexLogFile`] it is passed to, so the handle cannot outlive it
/// and A NEW READ OF A PIECE CANNOT BE WRITTEN WITHOUT A TALLY TO HAND OVER.
pub(crate) struct IndexLogReadTally {
    bytes: u64,
    reads: u64,
}

impl IndexLogReadTally {
    fn start() -> Self {
        Self { bytes: 0, reads: 0 }
    }
}

impl Drop for IndexLogReadTally {
    fn drop(&mut self) {
        if self.reads == 0 {
            return;
        }
        INDEX_LOG_PIECE_BYTES_READ
            .fetch_add(self.bytes, std::sync::atomic::Ordering::Relaxed);
        INDEX_LOG_PIECE_READS.fetch_add(self.reads, std::sync::atomic::Ordering::Relaxed);
        INDEX_LOG_PIECE_BYTES_READ_HERE.with(|bytes| bytes.set(bytes.get() + self.bytes));
        INDEX_LOG_PIECE_READS_HERE.with(|reads| reads.set(reads.get() + self.reads));
    }
}

/// A readable handle on one piece of an index log, which charges every byte the kernel hands it.
///
/// This is the only thing [`open_index_log_read`] and [`open_index_log_repair`] return, and they
/// are the only way this module opens a piece to read it -- so the counting is not a call site
/// anyone has to remember. `std::fs::File` is deliberately NOT imported into this module any
/// more: a new read written the old way does not compile.
pub(crate) struct CountedIndexLogFile<'a> {
    file: std::fs::File,
    tally: &'a mut IndexLogReadTally,
}

impl CountedIndexLogFile<'_> {
    fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }
}

impl Read for CountedIndexLogFile<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.file.read(buf)?;
        self.tally.bytes += read as u64;
        self.tally.reads += 1;
        Ok(read)
    }
}

impl Seek for CountedIndexLogFile<'_> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

/// Open a piece of an index log for reading, and count what comes off it.
///
/// Every read-side open in this module goes through here or [`open_index_log_repair`], which is
/// what makes a total takeable at all: the handle this returns is the only readable one, and a
/// new read of a piece cannot be written without a tally to pass.
fn open_index_log_read<'a>(
    path: &Path,
    tally: &'a mut IndexLogReadTally,
) -> std::io::Result<CountedIndexLogFile<'a>> {
    Ok(CountedIndexLogFile {
        file: std::fs::File::open(path)?,
        tally,
    })
}

/// Open the piece being written, for the tail trim that reads it and then truncates it.
///
/// Read-WRITE, and still one of the read side's opens: the bytes it reads are charged like any
/// other, or the counter would stop seeing this branch the moment a torn tail appeared.
fn open_index_log_repair<'a>(
    path: &Path,
    tally: &'a mut IndexLogReadTally,
) -> std::io::Result<CountedIndexLogFile<'a>> {
    Ok(CountedIndexLogFile {
        file: OpenOptions::new().read(true).write(true).open(path)?,
        tally,
    })
}

/// Exactly `len` bytes from `at` in a piece, through the one handle the read side uses.
///
/// The planted-marker control for the piece counter needs a read whose byte count IT decided, and
/// the counted handle is private to this module.
#[cfg(test)]
pub(crate) fn read_index_log_piece_for_test(
    path: &Path,
    at: u64,
    len: usize,
) -> std::io::Result<Vec<u8>> {
    let mut tally = IndexLogReadTally::start();
    let mut file = open_index_log_read(path, &mut tally)?;
    file.seek(SeekFrom::Start(at))?;
    let mut bytes = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        match file.read(&mut bytes[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    bytes.truncate(filled);
    Ok(bytes)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    /// Bytes of RECORDS this store has handed back, which is not what it read off disk.
    ///
    /// The two came apart badly enough to hide a restore: see
    /// [`index_log_piece_read_counts`], which is charged inside the read itself and is the
    /// number to use for what a path COSTS. This one stays what it has always been, because a
    /// caller reporting how much record data it received is asking a real question -- it is
    /// simply a different one.
    pub bytes_read: u64,
    /// Records this store has read back and handed to a decoder.
    ///
    /// Beside `bytes_read`, which is the same work measured the other way. Both are what makes a
    /// claim about what a round COSTS checkable without a clock: a guard that asserts a bound on
    /// milliseconds asserts something about the machine, and this box cannot hold one still.
    #[serde(default)]
    pub records_read: u64,
    pub last_sequence: u64,
}

/// What the index-GC gate needs, and what answering it cost.
///
/// The gate decides on a REMOVABLE-RECORD RATIO, which is two counts. Getting them by scanning
/// and decoding the whole log made the maintenance round O(log size) even after reclaim itself
/// stopped being -- the cost moved from the collector to the thing that decides whether to
/// collect. Every one of these numbers except the last two is arithmetic on piece NAMES plus the
/// one piece being written.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogGateSummary {
    pub shard_id: ShardId,
    /// The retain floor the counts below were taken against.
    pub retain_from_sequence: u64,
    /// On-disk bytes of the whole log: one `stat` per piece, nothing opened.
    pub bytes: u64,
    /// How many records the log holds.
    pub records: usize,
    /// How many of them sit below the floor.
    pub removable_records: usize,
    /// Files the log is in, the one being written included. This is what the gate now costs.
    pub pieces: usize,
    /// How many of those were answered from the NAME alone -- every sealed one.
    pub pieces_named: usize,
    /// Bytes read and decoded: the piece being written, and nothing else. Bounded by the rolling
    /// threshold, not by the log.
    pub bytes_decoded: u64,
    /// Records read and decoded, bounded the same way.
    pub records_decoded: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    #[serde(default)]
    pub max_entries_per_round: usize,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    #[serde(default)]
    pub removable_records_before_budget: usize,
    #[serde(default)]
    pub budget_exhausted: bool,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// On-disk bytes the sweep found reclaimable, whether or not it rewrote the log for them.
    #[serde(default)]
    pub reclaimable_bytes: u64,
    /// The sweep read the log, decided what was reclaimable, and did NOT rewrite it -- because
    /// nothing was reclaimable, or too little was to be worth the barrier. Distinguishes a log
    /// that had nothing to give from one that was rewritten and gave nothing, which otherwise
    /// look identical: both report zero records removed and the same bytes before and after.
    #[serde(default)]
    pub rewrite_skipped: bool,
    /// Bytes the round COPIED: what it wrote into the replacement for the piece being written.
    ///
    /// This is what a round costs. The collector used to rewrite every record it retained, so
    /// this number was the size of everything it KEPT, and a round that removed less copied
    /// more. Earlier pieces are now unlinked instead, and nothing in them is read or copied, so
    /// this is bounded by the piece being written however deep the log is.
    #[serde(default)]
    pub bytes_copied: u64,
    /// Whole pieces of the log this round unlinked.
    #[serde(default)]
    pub dropped_segments: usize,
    /// What those pieces held on disk.
    #[serde(default)]
    pub dropped_segment_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct LocalIndexLogStore {
    inner: Arc<Mutex<IndexLogInner>>,
    /// One barrier gate per shard log, so appends that arrive while an fsync is in flight ride it
    /// instead of queueing to take an identical one.
    flush_gates: Arc<crate::flush_gate::FlushRegistry>,
}

#[derive(Debug)]
struct IndexLogInner {
    root: PathBuf,
    stats: IndexLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    /// MANIFEST-CONFORMANCE FOLD: the index-log byte length recorded at the last catalog dump, per
    /// shard. `undumped_len_since_dump` subtracts this from the current on-disk length to get the
    /// undumped gap that drives the threshold-dump cadence. Reset to 0
    /// on process restart, so the first post-restart cycle may dump once -- harmless (a dump only
    /// materializes durable state that is already recoverable).
    last_dumped_len_by_shard: HashMap<ShardId, u64>,
    /// When the last catalog dump of a shard completed. Read by `ms_since_catalog_dump` to hold
    /// the next dump off until the minimum interval has passed.
    ///
    /// An `Instant`, not a wall clock, because the question is "how long since", and a wall
    /// clock that steps backwards would hold a dump off for as long as the step, while one that
    /// steps forward would release it early. A shard never dumped in this process has no entry,
    /// and no entry means no floor: the first dump after a restart is never delayed.
    last_dumped_at_by_shard: HashMap<ShardId, std::time::Instant>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<std::sync::Arc<crate::scratch::ScratchDirGuard>>,
    /// Whether the log directory is known to exist, so an append need not ask again.
    ///
    /// `new` creates it. This records whether that SUCCEEDED, so the one case the per-append
    /// `create_dir_all` genuinely covered -- a root `new` could not make, whose creation the
    /// constructor discards because it cannot return an error -- is still retried, once, by the
    /// first append instead of by every append.
    ///
    /// It is never set back to false, and that is deliberate. An append whose directory has
    /// gone must FAIL, not re-create it: re-creating it turns "someone removed the log
    /// directory" into "the log quietly began again, empty", which is lost durable state
    /// wearing the costume of resilience. `an_append_fails_loudly_when_the_log_directory_is_
    /// removed_underneath_it` is that behaviour, asserted.
    root_created: bool,
    /// The buffer a delta append encodes its payload into, kept between appends.
    ///
    /// The encode used to start from an empty vector every time and grow it by doubling to the
    /// size of the record. Reusing one buffer leaves a steady stream of appends allocating
    /// nothing here after the first few, which is the arrangement the write-ahead log's
    /// `encode_scratch` already had. Held on `inner` rather than in a thread-local because the
    /// append holds this lock across the encode anyway, so there is no contention to avoid and
    /// no second place for it to live.
    encode_scratch: Vec<u8>,
    /// The buffer that payload is FRAMED into, kept for the same reason.
    ///
    /// Separate from the one above because the frame is built from the payload, so the two are
    /// alive at the same moment.
    frame_scratch: Vec<u8>,
}

/// The largest scratch buffer worth keeping between appends.
///
/// The buffers grow to the largest record this store has encoded and would otherwise hold that
/// capacity for the life of the process. Past this they are dropped instead. Same ceiling, and
/// the same reason, as `MAX_ENCODE_SCRATCH_BYTES` in the write-ahead encoder.
const MAX_INDEX_ENCODE_SCRATCH_BYTES: usize = 1024 * 1024;

impl IndexLogInner {
    /// Make the log directory if `new` could not, and then never ask again.
    ///
    /// `new` already created it, and NOTHING IN THIS TREE REMOVES IT UNDER A LIVE STORE: the
    /// scratch guard's drop runs when the last clone is gone, the scratch sweep keeps every
    /// directory whose owning process is still alive (its own included), and the crash
    /// harness removes the directory from a SEPARATE process, after the writer has aborted and
    /// before the next one constructs its store. So the `create_dir_all` this replaces asked
    /// the kernel, on every single append, a question whose answer it already had -- 10,001
    /// `EEXIST` mkdirs in 10,002 at the measured size.
    ///
    /// If the directory IS gone, this no longer puts it back: the append's own `open` fails
    /// with the directory named in the error. That is the intended direction. See the note on
    /// `root_created`.
    fn ensure_root(&mut self) -> Result<(), IndexLogError> {
        if self.root_created {
            return Ok(());
        }
        #[cfg(test)]
        probe::APPEND_ROOT_CREATES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        fs::create_dir_all(&self.root)?;
        self.root_created = true;
        Ok(())
    }
}

fn indexlog_enabled() -> bool {
    // The index-log is the delta served-index's durable per-write delta stream: the
    // load-time fold reconstructs the served index (at the original page addresses) from
    // these records, so it must be written in ALL builds, not just tests. It is kept
    // bounded by GC-at-compaction, which truncates every delta already folded into the
    // durably-rewritten base snapshot (retain-from-anchor). Always on.
    true
}

fn bulk_ingest_mode() -> bool {
    crate::engine::bulk_ingest_mode()
}

/// Defer the ack-path index-log fsync (WAL replay is the durable recovery source). This is the
/// single-barrier default; restored to a synchronous fsync only under the TS_WAL_LEGACY_RECOVERY
/// escape hatch (whose delta-fold recovery trusts the durable delta).
fn indexlog_wal_only_sync() -> bool {
    // See `block_store::block_wal_single_barrier`: one reader, in `engine`, so the three barriers
    // this hatch controls cannot end up disagreeing about whether it is set.
    !crate::engine::wal_legacy_recovery()
}

/// MANIFEST-CONFORMANCE FOLD, always on: the slab catalog is folded into the index-log
/// anchor at a threshold dump, and the per-write slab-manifest file is no longer the catalog's
/// source of truth (it is reconstructed on load from the durable pages plus the folded anchor).
/// `TS_INDEX_CATALOG_FOLD` used to be able to skip all of it. It shipped dark (the flip once
/// broke proxy tests), then defaulted on once the upsert-delta and single-barrier work landed,
/// and nothing ever selected the off position again -- the five tests that exercise the fold
/// pinned it ON rather than off. The threshold dump is also what lets the embedded (proxy)
/// engine reclaim its index-log and WAL at all, so the off side disabled reclaim entirely.
///
/// The off side's compatibility claim still holds and is not conditional on anything: an anchor
/// whose `slabs` is empty serializes with no `zones` key, byte-identical to a pre-fold MetaItem.
/// `meta_item_without_zones_serializes_byte_identically_to_pre_fold` asserts exactly that.
///
/// How many bytes a threshold dump may WRITE for each byte it RELEASES.
///
/// A dump's cost is the whole served index; its release is the index log accrued since the
/// previous dump. Against a constant accrual threshold those two quantities have nothing to do
/// with each other, so bytes written per byte released is linear in the store: measured at 9.89
/// on a 20,000-record store and 84.23 on a 200,000-record one, for the same 4,000 records of work
/// in both. Requiring the accrual to reach `base_index / INDEX_DUMP_BASE_FRACTION_DIVISOR` before
/// a threshold dump fires bounds that figure at the divisor, whatever the store holds.
///
/// It is bought with index-log footprint. Between dumps the log may now stand at
/// `base_index / divisor` rather than at the configured floor -- 12.5% of the base index on top
/// of it at 8 -- and it still returns to its floor at every dump. That is the trade: the store's
/// steady-state disk grows by an eighth, and the bytes it writes to hold that bound stop growing
/// with it.
///
/// A constant rather than a deployment knob. It is a dimensionless ratio, it is the same at every
/// size by construction, and the deployment already sets the FLOOR through
/// `TS_INDEX_DUMP_WAL_GAP_BYTES`.
///
/// # WHY 8, AND WHY THE EXPRESSION STAYS A DIVISION OF THE BASE INDEX
///
/// Chosen on measurement rather than inherited. `index_write_amplification` drives this divisor
/// across a sweep and prices the WRITE side; `index_recovery_cost` recovers the same five stores
/// at the budget's high-water mark -- the worst moment a crash can pick -- and prices the READ
/// side. At 4,000 objects on the shipped routing range `0..1023`, physical bytes throughout:
///
/// ```text
///   divisor      b        a    index log    base read    WAL read    NAMED read    wall
///         2  1.3563    3.810      143,669      286,242     763,392     1,193,303  1671 ms
///         4  1.2125    5.715       61,768      243,537     566,784       872,089   405 ms
///         8  1.1159    9.658       28,807      223,389     493,056       745,252   252 ms
///        16  1.0609   17.511       14,409      213,124     468,480       696,013   249 ms
///        32  1.0347   30.083        7,245      208,503     443,904       659,652   228 ms
/// ```
///
/// The two sides are RECIPROCAL in `b - 1`. `a = b/(b-1)` is about `divisor + 1`, and the index
/// log a recovery reads is `(b - 1)` times the base index -- read WHOLE, because the default load
/// path's `latest_slab_catalog` walks every record to find the last one carrying a catalog, and
/// the piece-name decline buys nothing in a post-sweep state (measured: the sweep has already
/// dropped every piece the base reflects, so there is no piece left to decline). A looser budget
/// also dumps less often, which leaves a longer unreflected write-ahead tail, so the replay term
/// moves with the same threshold -- and that is the one term in a restore that is superlinear in
/// its own size (`restore_large_scale`: 89% of what a 200,000-record restart reads, 6.70x per
/// stored record for a 10x store).
///
/// So neither direction is free and the sweep identifies no interior optimum on its own -- the two
/// costs are in different units, one paid continuously and one per restart, and no measurement
/// fixes the exchange rate between them. What the measurement DOES decide is both ends:
///
/// - TIGHTER IS A BAD TRADE. 8 -> 16 costs 81% more bytes written per byte logged (9.658 ->
///   17.511) and takes 6.6% off what a recovery reads (745,252 -> 696,013 B). The first is paid on
///   every write for as long as the store is up; the second once per restart.
/// - LOOSER PAST `b = 2` CANNOT BE EXPRESSED HERE, AND SHOULD NOT BE. `b = 1 + 1/divisor` for
///   integer `divisor >= 1` confines `b` to `(1, 2]`, so `a >= 2` structurally; reaching
///   `a <= 1.25` needs `b = 5`, a threshold FOUR TIMES the base index. At the measured 0.1255
///   share that is a 32-fold increase in both the whole-log read and the unreflected tail -- a
///   recovery that reads several times the store it is recovering. A multiplier, a rational or a
///   configured byte value would each reach it; none of them should.
/// - LOOSER WITHIN REACH IS ARGUABLE AND NOT FORCED. 8 -> 4 buys 41% off `a` for 17% more recovery
///   read. It is the only move this measurement leaves open, and it would spend the budget on a
///   read that is currently UNBOUNDED rather than on one that is minimal: the catalog walk needs
///   one record and reads the whole log for it. Bounding that read is the prerequisite for
///   revisiting this constant, not a reason to move it first.
///
/// AND THAT IS WHY THE FORM STAYS A DIVISION. Every budget the evidence supports lies in
/// `(1, 2]`, which a division of the base index expresses exactly; a multiplier or a rational
/// would only widen the reachable range into the region the recovery measurement refuses. A
/// configured byte value would give up the property every figure above depends on -- the threshold
/// is a FRACTION of the base index, so `b`, `a` and the recovery's index-log term are the same at
/// every store size, measured at 0.1290 and 0.1255 of the base index across a 5.2x corpus.
pub const INDEX_DUMP_BASE_FRACTION_DIVISOR: u64 = 8;

/// The accrual a threshold dump must have to show for itself: the configured floor, raised so a
/// dump never writes more than `divisor` bytes of base index per byte of index log it releases.
///
/// `configured_threshold_bytes` is a FLOOR and never a ceiling. A store whose base index is
/// smaller than `divisor * configured_threshold_bytes` keeps exactly the cadence it has today --
/// the relative term can only ever make a dump wait for MORE to release, never fire earlier -- so
/// nothing below the crossover changes behaviour at all.
///
/// A zero configured floor DISABLES threshold dumping: `should_dump_index_catalog` refuses every
/// undumped length against it, `u64::MAX` included, which is how an operator pins dumps to
/// compaction and unload only. The relative term is a `max` against a quantity that grows without
/// bound, so it would silently switch threshold dumping back ON for exactly the largest stores --
/// the ones an operator who set a zero was most likely protecting. A zero in either position
/// therefore returns the configured value unchanged and leaves the caller the decision it had.
pub fn effective_index_dump_threshold_bytes(
    configured_threshold_bytes: u64,
    base_index_bytes: u64,
    divisor: u64,
) -> u64 {
    if configured_threshold_bytes == 0 || divisor == 0 {
        return configured_threshold_bytes;
    }
    configured_threshold_bytes.max(base_index_bytes / divisor)
}

/// Threshold decision for the background catalog/index dump, mirroring this design
/// index-meta dump gate (the dump-delay check compares the undumped
/// WAL length against the 1 MiB gap). `undumped_bytes` is the served-index-log growth since
/// the last dumped watermark; when it crosses `gap_bytes` the dump fires. A zero gap disables
/// the cadence (never dump on threshold) so an operator can pin dumps to compaction/unload only.
pub fn should_dump_index_catalog(undumped_bytes: u64, gap_bytes: u64) -> bool {
    gap_bytes > 0 && undumped_bytes >= gap_bytes
}

/// The threshold decision above, with a floor on how often it may say yes.
///
/// `ms_since_dump` is `None` for a shard this process has never dumped, which is not a dump
/// "0 ms ago" -- it is no dump at all, and nothing to wait behind. Getting that backwards would
/// silence the first dump after every restart for the length of the interval, on exactly the
/// engine that has the most to reclaim.
///
/// A zero interval is no floor at all, so the byte gap alone decides. That is what a test which
/// is not exercising the timer passes, and it says so by passing it.
pub fn should_dump_index_catalog_now(
    undumped_bytes: u64,
    gap_bytes: u64,
    ms_since_dump: Option<u64>,
    min_interval_ms: u64,
) -> bool {
    if !should_dump_index_catalog(undumped_bytes, gap_bytes) {
        return false;
    }
    match ms_since_dump {
        Some(elapsed) => elapsed >= min_interval_ms,
        None => true,
    }
}

impl LocalIndexLogStore {
    /// On-disk byte length of a shard's index-log file (0 if absent). Used as the "undumped
    /// length" signal for the threshold-dump cadence: the growth of this file since the last
    /// dumped watermark is the undumped-length signal.
    pub fn log_len_bytes(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        // Every piece, not just the one being written. After a roll the active piece is the
        // NEWEST and smallest part of the log, so reporting its length as the log's length makes
        // a log SHRINK as it grows -- and the dump cadence that reads this would stop firing
        // exactly when there is most to dump.
        index_log_total_bytes(&inner.root, shard_id)
    }

    /// Undumped index-log length for a shard: the on-disk byte growth since the last catalog
    /// dump (`mark_catalog_dumped`). This is the native analog of this design
    /// the undumped WAL length, and is the signal compared against `index_dump_wal_gap_bytes`
    /// to decide a threshold dump. A shard never dumped this process (or freshly restarted)
    /// reports the whole current length.
    /// Milliseconds since this shard's last catalog dump, or `None` if it has not dumped in
    /// this process. Feeds `should_dump_index_catalog_now`'s interval check.
    pub fn ms_since_catalog_dump(&self, shard_id: ShardId) -> Option<u64> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        inner
            .last_dumped_at_by_shard
            .get(&shard_id)
            .map(|at| at.elapsed().as_millis() as u64)
    }

    pub fn undumped_len_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_total_bytes(&inner.root, shard_id);
        let dumped = inner
            .last_dumped_len_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        current.saturating_sub(dumped)
    }

    /// Record that a catalog dump captured the shard's index-log up to its current on-disk
    /// length, resetting the undumped gap to 0. Called by the engine ONLY after the dump's base
    /// index + folded anchor are durably written, so the watermark never advances past
    /// non-durable state (restart-during-dump re-dumps rather than skipping).
    pub fn mark_catalog_dumped(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_total_bytes(&inner.root, shard_id);
        inner.last_dumped_len_by_shard.insert(shard_id, current);
        // Stamped in the same call as the length, so the two halves of the cadence -- how much
        // has accumulated, and how long ago -- can never disagree about which dump they describe.
        inner
            .last_dumped_at_by_shard
            .insert(shard_id, std::time::Instant::now());
    }

    /// The most recent `MetaItem` anchor carrying a folded slab catalog, or `None` if no
    /// anchor with a non-empty `slabs` list has been written. Used on load (gate on) to seed the
    /// block-store slab catalog from the folded anchor when the slab-manifest file is absent.
    pub fn latest_slab_catalog(&self, shard_id: ShardId) -> Result<Option<MetaItem>, IndexLogError> {
        // The LAST matching record wins, so this cannot stop early -- but it never needed to
        // hold the records it walks past. It kept every record in the log, with every item each
        // one carries, to take one field out of one of them.
        let mut latest = None;
        self.for_each_delta_record(shard_id, 0, |record| {
            if let Some(meta) = record.meta {
                if !meta.slabs.is_empty() {
                    latest = Some(meta);
                }
            }
        })?;
        Ok(latest)
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        // The RESULT, not `let _`. This is the store's one directory creation; every append
        // then trusts it rather than repeating it. Keeping the answer is what lets an append
        // skip the `mkdir` + `statx` pair while still retrying the one case that mattered.
        let root_created = fs::create_dir_all(&root).is_ok();
        Self {
            inner: Arc::new(Mutex::new(IndexLogInner {
                root,
                stats: IndexLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                last_dumped_len_by_shard: HashMap::new(),
                last_dumped_at_by_shard: HashMap::new(),
                scratch: None,
                root_created,
                encode_scratch: Vec::new(),
                frame_scratch: Vec::new(),
            })),
            flush_gates: Arc::new(crate::flush_gate::FlushRegistry::default()),
        }
    }

    /// Append an index record, PARSING the bytes into a value first.
    ///
    /// Prefer [`append_index_bytes`](Self::append_index_bytes), which splices the already
    /// serialized bytes into the record instead. This one parses the whole index into a
    /// `serde_json::Value` and then re-encodes it, so a multi-megabyte index is walked twice
    /// more per append -- measured at 2.31 MB for a 2,000-key shard. It remains for callers
    /// that genuinely need the parsed record back; every engine caller writes an index it has
    /// just serialized and discards the result, so they all take the splicing path.
    pub fn append_json(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<IndexLogRecord, IndexLogError> {
        // Bulk backfill: skip the replay-log append entirely (deferred to a
        // single flush of the served index). Removes the per-record fsync bomb.
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(IndexLogRecord {
                shard_id,
                sequence: 0,
                index: serde_json::Value::Null,
            });
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        inner.ensure_root()?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        // ONE probe, for both questions. This resolves which file the shard's log is being
        // written to and how long it is; the roll needs the length, the open needs the name,
        // and asking separately is what made the append probe the same path twice per record.
        let (mut active, active_len) = active_index_log(&inner.root, shard_id);
        if roll_index_log_segment_at(&inner.root, shard_id, &active, active_len)? {
            // Re-resolve ONLY here. The seal RENAMES the piece away, and that rename is the one
            // event that can change which name the next record belongs under -- so the answer is
            // taken again exactly when it has been invalidated, and never cached past it. A roll
            // is 1 append in 1,000 at the rolling default, so this costs nothing per record.
            active = active_index_log(&inner.root, shard_id).0;
        }
        let next_sequence = last_sequence.saturating_add(1);
        let record = IndexLogRecord {
            shard_id,
            sequence: next_sequence,
            index: serde_json::from_slice(index_bytes)?,
        };
        // Frame the record with a length + SHA-256 digest (crate::log_framing) so a later
        // value-preserving bit-flip in this committed line is detected on read.
        let bytes =
            crate::log_framing::encode_record(&encode_index_payload(&record, INDEX_LOG_SHAPE_WHOLE)?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active)?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        #[cfg(test)]
        probe::APPENDS_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
    }

    pub fn append_index_bytes(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        // This once asserted the bytes parse as JSON, from when this appender spliced the whole
        // index into the record. It no longer does -- it writes a digest and a length, and never
        // looks inside -- so the JSON demand was a precondition that outlived its reason, and it
        // fires the moment the index is written in its binary container. Assert what this code
        // actually needs: that it was handed an index at all, in either format a reader accepts.
        debug_assert!(
            crate::engine::bytes_look_like_served_index(index_bytes),
            "index-log anchor was handed {} bytes that are not a served index in any known format",
            index_bytes.len()
        );
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        inner.ensure_root()?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        // ONE probe, for both questions. This resolves which file the shard's log is being
        // written to and how long it is; the roll needs the length, the open needs the name,
        // and asking separately is what made the append probe the same path twice per record.
        let (mut active, active_len) = active_index_log(&inner.root, shard_id);
        if roll_index_log_segment_at(&inner.root, shard_id, &active, active_len)? {
            // Re-resolve ONLY here. The seal RENAMES the piece away, and that rename is the one
            // event that can change which name the next record belongs under -- so the answer is
            // taken again exactly when it has been invalidated, and never cached past it. A roll
            // is 1 append in 1,000 at the rolling default, so this costs nothing per record.
            active = active_index_log(&inner.root, shard_id).0;
        }
        let next_sequence = last_sequence.saturating_add(1);
        // Record WHICH index this checkpoint anchors, not a second copy of it.
        //
        // This used to splice the whole served index into the record -- 2.31 MB for a 2,000-key
        // shard, written again here after it had just been written to the index file. Nothing
        // ever read it back: every path that reconstructs a shard decodes the index FILE or a
        // dump manifest, and the log's own readers take only `sequence` (the tail scan and the
        // GC anchor probe) or raw bytes (the debug stream). Removing the payload and running
        // the reload, recovery-sweep, storage-lifecycle, dump, index-format, load-index and
        // anchor suites changed nothing -- 77 tests, all still green.
        //
        // The digest keeps what the copy was actually good for: an anchor can still be checked
        // against the index file it claims to describe. `append_json` still embeds the whole
        // index for any caller that wants the record to carry it.
        let digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(index_bytes);
            hasher.finalize().to_vec()
        };
        let index_len = index_bytes.len();
        let anchor = IndexAnchorRecord {
            shard_id,
            sequence: next_sequence,
            index_sha256: digest,
            index_len: index_len as u64,
        };
        let bytes = crate::log_framing::encode_record(&encode_index_payload(
            &anchor,
            INDEX_LOG_SHAPE_ANCHOR,
        )?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active)?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        #[cfg(test)]
        probe::APPENDS_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(next_sequence)
    }

    /// Append one O(delta) served-index record: the page/object items a single write
    /// touched, optionally carrying a compaction anchor. Unlike `append_json`, this never
    /// serializes the whole index -- the appended bytes are proportional to the change,
    /// which is what turns the per-write served-index persist cost from O(store) into
    /// O(delta). Returns the assigned monotonic sequence.
    ///
    /// `durable` controls the fsync, and WHAT IT IS AT THE SHIPPED DEFAULT IS PER CALL SITE.
    /// There are exactly FOUR in the library, and they do not agree:
    ///
    /// | call site | expression | at the shipped default |
    /// |---|---|---|
    /// | `engine.rs` single-command ack | `!raft_applying() && !wal_single_barrier()` | **false** |
    /// | `engine/recovery_sweep_compact.rs` expiry flush | `!raft_applying() && !wal_single_barrier()` | **false** |
    /// | `engine/stream_batch_methods.rs` batch ack | `!raft_applying()` | **true** |
    /// | `engine/engine_persistence.rs` threshold catalog dump | `true` | **true** |
    ///
    /// `wal_single_barrier()` is `!wal_legacy_recovery()`, which reads `TS_WAL_LEGACY_RECOVERY`,
    /// off by default -- so the two expressions carrying that term evaluate false, and the raft
    /// term is what defers them on the apply path instead. The single-command ack therefore
    /// defers, because the WAL barrier has already committed before this is reached. The BATCH
    /// ack does not: its expression never had the `wal_single_barrier()` term, so on every
    /// single-node batch write it takes the barrier. And the dump passes a literal `true`, which
    /// is the point of a dump -- step 4 of `dump_index_catalog` advances the dumped watermark on
    /// the strength of this record being durable.
    ///
    /// # THIS DOC HAS NOW BEEN WRONG TWICE, IN OPPOSITE DIRECTIONS
    ///
    /// It first claimed the normal path fsync'd and only raft deferred. That was corrected to
    /// "under the shipped default no production caller passes `true`", over "both callers", and
    /// THAT IS ALSO WRONG -- the correction is mine and it is the worse of the two, because a
    /// reader who checks it finds a literal `true` at the second call site down.
    ///
    /// Two separate errors produced it. "Both callers" was a count taken from a grep: there are
    /// four, and only two compute the expression quoted. And "no caller passes `true`" read the
    /// term `!wal_single_barrier()` as though every call site carried it.
    ///
    /// The caller count here is the COMPILER's, not a grep's. `append_delta`'s `durable`
    /// parameter was removed and `cargo check --lib` run: four arity errors, the four rows above.
    /// A grep over the same tree reports 89 call sites and 28 that look like production -- the
    /// rest are inside `#[cfg(test)]` functions of the measurement modules, which a grep cannot
    /// see and the compiler does not compile.
    ///
    /// Either way the record is WRITTEN and flushed to the OS at every one of the four, so the
    /// served-index stream is the same shape regardless, and a tail lost from a deferred one is
    /// rebuilt by replay.
    ///
    /// The consumer-aware GC never truncates such a tail: its retain floor is
    /// `min(durable frontier, slowest cursor) + 1`, which
    /// `the_index_log_retain_floor_never_passes_the_durable_frontier` asserts rather than leaving
    /// to this sentence.
    pub fn append_delta(
        &self,
        shard_id: ShardId,
        items: Vec<IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        meta: Option<MetaItem>,
        upsert: bool,
        durable: bool,
    ) -> Result<u64, IndexLogError> {
        // The single and the batch write paths are two live callers of this, which is exactly the
        // pair a charge at one call site would have split: an exploratory scope placed at the
        // single-command site read ZERO on a batch ingest. Here both are counted, and the
        // disabled-log return below is inside the class so a store with the log off reports a
        // measured zero rather than an absent one.
        crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::IndexLogDelta, || {
            self.append_delta_inner(
                shard_id,
                items,
                key_states,
                applied_wal_sequence,
                meta,
                upsert,
                durable,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn append_delta_inner(
        &self,
        shard_id: ShardId,
        items: Vec<IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        meta: Option<MetaItem>,
        upsert: bool,
        durable: bool,
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        inner.ensure_root()?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        // ONE probe, for both questions. This resolves which file the shard's log is being
        // written to and how long it is; the roll needs the length, the open needs the name,
        // and asking separately is what made the append probe the same path twice per record.
        let (mut active, active_len) = active_index_log(&inner.root, shard_id);
        if roll_index_log_segment_at(&inner.root, shard_id, &active, active_len)? {
            // Re-resolve ONLY here. The seal RENAMES the piece away, and that rename is the one
            // event that can change which name the next record belongs under -- so the answer is
            // taken again exactly when it has been invalidated, and never cached past it. A roll
            // is 1 append in 1,000 at the rolling default, so this costs nothing per record.
            active = active_index_log(&inner.root, shard_id).0;
        }
        let next_sequence = last_sequence.saturating_add(1);
        // Do not write what the item already says. Each item carries `object_id` and
        // `routing_bucket`, and the address it points at repeats both -- 18 bytes of a 142-byte
        // item. `restore_address_repeats` at the decode site puts them back.
        let mut items = items;
        for item in items.iter_mut() {
            item.strip_block_ref_key_repeat();
            item.strip_size_repeat();
            // The address first, because it is stripped against the item's id, and the item's
            // id is what goes next.
            item.strip_address_repeats();
            item.strip_object_id_repeat(shard_id);
        }
        // Hoist the object key when every item names the same one, and blank the copies.
        //
        // LAST, after the strips above. `strip_block_ref_key_repeat` DERIVES the page handle from
        // the item's own fields and `object_key` is one of them, so blanking the key first would
        // derive a different handle and the strip would decline -- costing more than this saves.
        let shared_object_key = match items.split_first() {
            Some((first, rest)) if rest.iter().all(|item| item.object_key == first.object_key) => {
                Some(first.object_key.to_string())
            }
            _ => None,
        };
        if shared_object_key.is_some() {
            let blank = empty_object_key();
            for item in items.iter_mut() {
                item.object_key = blank.clone();
            }
        }
        let record = IndexDeltaRecord {
            shard_id,
            sequence: next_sequence,
            items,
            meta,
            applied_wal_sequence,
            key_states,
            upsert,
            shared_object_key,
        };
        // Frame the delta record with a length + SHA-256 digest (crate::log_framing) so a
        // value-preserving bit-flip (e.g. a flipped `deleted` flag or page address) in this
        // committed line is detected on read rather than replayed as truth on recovery.
        // Both buffers are taken off `inner` and put back below, because the framing borrows one
        // while the other is still being read and `inner` is needed either side.
        //
        // A failed append puts them back holding WHATEVER THEY HELD -- a partial encode, or the
        // previous record's frame -- and that is safe because both writers CLEAR before they
        // write, so no byte of a failed or an earlier record can reach the next one. What must
        // not happen is LOSING them, which would leave the next append allocating from nothing.
        let mut payload = std::mem::take(&mut inner.encode_scratch);
        let mut bytes = std::mem::take(&mut inner.frame_scratch);
        // Encode, frame and write inside one closure so that EVERY way out of it -- including the
        // three I/O failures -- passes through the restore below. A `?` here would drop the two
        // buffers on the floor and leave `inner` holding empty ones, so the next append would
        // allocate from nothing: not a correctness bug, but exactly the cost this removes,
        // reappearing after the first transient write error and never going away.
        let written = (|payload: &mut Vec<u8>, bytes: &mut Vec<u8>| -> Result<std::fs::File, IndexLogError> {
            encode_index_payload_into(&record, INDEX_LOG_SHAPE_DELTA, payload)?;
            crate::log_framing::encode_record_into(payload, bytes);
            let mut file = OpenOptions::new().create(true).append(true).open(&active)?;
            file.write_all(bytes)?;
            file.flush()?;
            Ok(file)
        })(&mut payload, &mut bytes);
        let written_len = bytes.len() as u64;
        // A buffer that grew to hold one enormous record does not keep that capacity for the life
        // of the process; past the ceiling it is dropped and the next append starts again.
        if payload.capacity() > MAX_INDEX_ENCODE_SCRATCH_BYTES {
            payload = Vec::new();
        }
        if bytes.capacity() > MAX_INDEX_ENCODE_SCRATCH_BYTES {
            bytes = Vec::new();
        }
        inner.encode_scratch = payload;
        inner.frame_scratch = bytes;
        let mut file = written?;
        inner.stats.writes += 1;
        #[cfg(test)]
        probe::APPENDS_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inner.stats.bytes_written += written_len;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        // The bytes are in the file and the bookkeeping is done, so claim a barrier and then let
        // the lock go before taking it. Holding the lock across the fsync meant writers could
        // never reach the barrier together, so each paid for one that would have covered all of
        // them.
        let barrier = durable.then(|| {
            let gate = self.flush_gates.gate((shard_id as u64, 0));
            let ticket = gate.register_write();
            (gate, ticket)
        });
        drop(inner);
        if let Some((gate, ticket)) = barrier {
            gate.await_durable(ticket, || {
                crate::durability_metrics::record_barrier("engine_index_log_append");
                file.sync_data()
            })?;
        }
        Ok(next_sequence)
    }

    /// Read every delta record for a shard in log order. Records at or before
    /// `retain_after_sequence` (the base snapshot's anchor) are skipped, so replay applies
    /// only the suffix the base does not already reflect. Legacy whole-index
    /// (`IndexLogRecord`) lines are ignored here -- they are not delta records.
    pub fn read_delta_records(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
    ) -> Result<Vec<IndexDeltaRecord>, IndexLogError> {
        let mut records = Vec::new();
        self.for_each_delta_record(shard_id, retain_after_sequence, |record| {
            records.push(record);
        })?;
        Ok(records)
    }

    /// The same walk, handing each record over instead of collecting them.
    ///
    /// The file was already read a frame at a time, so what a caller held was never the FILE --
    /// it was every record the file DECODES to, which is the larger of the two and lives until
    /// the caller drops the vector. A caller that folds the records into one answer has no use
    /// for that, and the folding caller here runs on the load path, which is when a store can
    /// least afford to hold the log twice over.
    ///
    /// `read_delta_records` keeps its shape on top of this, so a caller that genuinely wants all
    /// of them -- the tests that assert what a log holds -- is unchanged.
    pub fn for_each_delta_record(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
        take: impl FnMut(IndexDeltaRecord),
    ) -> Result<(), IndexLogError> {
        // Anchor 0 declines nothing, so this opens every piece exactly as it always has. Every
        // caller but the load-path fold asks about the LOG ITSELF -- what it holds, what its
        // newest catalog is -- and no base anchor makes any part of that answer redundant.
        self.for_each_delta_record_above_anchor(shard_id, retain_after_sequence, 0, take)
    }

    /// The same walk, declining WHOLE PIECES the dumped base already reflects, from their names.
    ///
    /// The load-path fold in `fold_index_log_deltas` hands every record it is given to the same
    /// test: a record whose `applied_wal_sequence` is at or below the base's anchor is already
    /// in the base, so folding it again would change nothing. That test was applied AFTER the
    /// frame had been read off the disk, the payload decoded and every stripped repeat put back
    /// -- so a restore whose base reflects the entire log still paid for the entire log.
    ///
    /// A sealed piece's name already carries the one number that test reads: `max_applied_wal`,
    /// the highest anchor any record in it carries. When that is at or below `base_anchor`,
    /// EVERY record in the piece fails the same test, so the piece can be passed over unopened.
    ///
    /// WHAT THE NAME PINS, AND WHY THAT IS ENOUGH HERE.
    ///
    /// - `max_applied_wal` is a max over the records' own anchors, taken at SEAL time by
    ///   [`index_log_segment_span_of`], after a complete append. A record carrying NO anchor
    ///   does not take part in that max at all: one carrying something the fold would apply
    ///   makes the whole name [`INDEX_SEGMENT_UNREFLECTED`], and one carrying nothing leaves
    ///   the max where it was. The load-path test is [`delta_record_is_reflected_by`], which
    ///   asks those same two questions of the same record, so the two agree about it. That
    ///   agreement is what makes the piece-level test equivalent to the per-record one.
    ///
    ///   THIS BULLET OUTLIVED WHAT IT DESCRIBED, AND IS KEPT AS THE RECORD OF HOW. It used to
    ///   read "A record carrying NO anchor contributes 0 to that max -- and the load-path test
    ///   reads a missing anchor as 0 in exactly the same way (`applied_wal_sequence
    ///   .unwrap_or(0)`), so the two agree about it." Both halves stopped being true, one at a
    ///   time. #1842 stopped the SEAL from reading a missing anchor as 0, because a piece of
    ///   nothing but anchor-less deltas named 0 and the post-dump sweep unlinked it unread. The
    ///   fold went on reading it as 0 for longer -- so a piece held open by #1842 precisely to
    ///   save an anchor-less record handed that record to a fold that dropped it one line
    ///   later, and the sentence above still said the two agreed. The agreement is a property
    ///   of the predicate both sides now call, not of this paragraph.
    /// - [`INDEX_SEGMENT_UNREFLECTED`] is excluded EXPLICITLY rather than left to the `<=`. It
    ///   is `u64::MAX`, so the comparison would already decline to fire for any real anchor --
    ///   but "already" is a property of the anchors a deployment happens to reach, not of this
    ///   code, and the one value that means "no base reflects this" must not be reachable by
    ///   arithmetic on the boundary.
    /// - Nothing renames, copies or appends to a sealed piece. The only writer of a sealed name
    ///   is the `fs::rename` in [`roll_index_log_segment_at`], which renames the piece being
    ///   WRITTEN to a name computed from a fresh walk of it; the two index-GC rewrites both
    ///   target [`index_log_path`], the active piece, which has no numbers in its name and is
    ///   never declined here. So a piece's name and its contents are fixed together.
    /// - The piece being written is never declined: it has no sealed name, so
    ///   [`sealed_index_log_span`] answers `None` for it and the walk reads it whole. The tail
    ///   of the log -- the part no dump can have reflected -- is therefore always read.
    ///
    /// WHICH WAY A WRONG ANSWER FAILS. Declining a piece that still holds a record the fold
    /// would have applied is SILENT DATA LOSS: an eviction or removal recorded only in that
    /// delta is recovered from neither the delta nor the write-ahead log, and the load reports
    /// success. Reading a piece that could have been declined is merely slow. Only one of those
    /// two is observable, so the tests attack that one: they assert the RECORDS THE FOLD HANDS
    /// BACK are identical with the decline and without it, and take the saving as a separate
    /// half.
    ///
    /// WHAT IS GIVEN UP. A declined piece is not checked for sequence continuity, because it is
    /// not read. The watermark is still carried across it from its name, so a hole ON a piece
    /// boundary is still refused; a hole INSIDE a declined piece is not. That is a strictly
    /// smaller concession than the one this project has already made: the post-dump sweep
    /// UNLINKS a piece on the same predicate ([`drop_reflected_index_segments`]), so a piece
    /// this fold declines to read is a piece reclaim would delete unread.
    pub fn for_each_delta_record_above_anchor(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
        base_anchor: u64,
        mut take: impl FnMut(IndexDeltaRecord),
    ) -> Result<(), IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        // Every piece of the log, oldest first. A log that has never rolled is one file, and this
        // is then the single path this fold has always read.
        //
        // `last_sequence` is carried ACROSS the pieces, not reset per piece: the continuity check
        // below is what refuses a holed delta stream, and per-piece it would stop seeing a hole
        // that falls on a boundary -- which is the only new place a hole can appear.
        let mut last_sequence = 0_u64;
        // ONE tally for the whole fold, borrowed by each piece's handle in turn and published
        // when the fold returns. This is the reader #1936 named and did not fix.
        let mut tally = IndexLogReadTally::start();
        for path in index_log_segment_paths(&inner.root, shard_id) {
            // From the NAME, before the `exists` probe and before the open, so a declined piece
            // costs neither. `sealed_index_log_span` parses the path string and touches no
            // filesystem, and the enumeration handed the path over already.
            if let Some(span) = sealed_index_log_span(&path, shard_id) {
                if piece_is_reflected_by(span, base_anchor) {
                    #[cfg(test)]
                    probe::FOLD_PIECES_DECLINED
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Carry the continuity watermark ACROSS the declined piece, from its name.
                    // Leaving it behind would put the next piece's first record up against a
                    // watermark from before this one, which is a WEAKER check than the walk has
                    // today -- it would pass a record that repeats a sequence inside the piece
                    // just declined. `end` is one past the last sequence the piece held when it
                    // was sealed. `max` rather than assignment, and no new refusal: a name that
                    // somehow ran backwards must not turn a load that succeeds today into one
                    // that fails.
                    last_sequence = last_sequence.max(span.end.saturating_sub(1));
                    continue;
                }
            }
            if !path.exists() {
                continue;
            }
            let mut reader = BufReader::new(open_index_log_read(&path, &mut tally)?);
            // Read by FRAME, not by line. A record's payload may be binary, and a binary payload
            // may contain 0x0A -- a reader splitting on newlines would cut such a record in half
            // and, being `lines()`, would also demand it be valid UTF-8. `read_frame` takes the
            // length the frame declares instead, and reads text-framed and legacy unframed
            // records unchanged, so one loop reads every shape the log has ever held. Streaming
            // rather than reading the file whole keeps memory bounded by the largest record.
            while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader)? {
                // Before the `continue` below, not after it: a frame this walk passes over is
                // still a frame this walk read off the disk, and that is what the probe counts.
                #[cfg(test)]
                probe::FOLD_FRAMES_READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                let payload = payload.as_slice();
                // PROPAGATE decode/parse failures instead of silently skipping the line. Silently
                // dropping an unparseable interior delta record and continuing the fold advances
                // the reconstructed anchor past it, so an eviction/removal recorded ONLY in that
                // delta (not the WAL) is recovered from neither source = silent loss / dangling
                // ref. `decode_line` also verifies the per-record integrity envelope, so a
                // value-preserving bit-flip surfaces here as `Corruption`.
                // Skip only a payload that SAYS it is the other shape. Anything else -- an
                // unrecognised shape, a truncated container -- goes to the decoder and is
                // reported, because a sweep that quietly skips what it cannot read is how
                // committed corruption becomes silent data loss.
                if index_payload_shape(payload) == Some(INDEX_LOG_SHAPE_WHOLE) {
                    continue;
                }
                let mut record: IndexDeltaRecord = decode_index_payload(payload)?;
                // Put back what the writer left out because the item already stated it. A record
                // written before that stripping carries both already, and this leaves those
                // alone. The hoisted key FIRST: everything below derives from the item's own
                // fields and `object_key` is one of them.
                if let Some(shared) = record.shared_object_key.as_deref() {
                    // Converted ONCE for the record, not once per item: every item in a
                    // hoisted record carries the same key, so they share one allocation.
                    let shared: std::sync::Arc<str> = std::sync::Arc::from(shared);
                    for item in record.items.iter_mut() {
                        item.object_key = shared.clone();
                    }
                }
                for item in record.items.iter_mut() {
                    // The item's id first: the address is restored FROM it.
                    item.restore_object_id_repeat(record.shard_id);
                    item.restore_address_repeats();
                    item.restore_block_ref_key_repeat();
                    item.restore_size_repeat();
                }
                // Enforce delta sequence-continuity: sequences are assigned strictly
                // monotonically across ALL appended records (whole-index and delta share one
                // counter), reclaim only ever removes a leading prefix, and pieces are read in
                // log order -- so each record's sequence must be strictly greater than the
                // previous one, boundaries included. A drop below or a duplicate means a lost /
                // reordered / corrupted record, or pieces read out of order; refuse rather than
                // fold a holed delta stream.
                if record.sequence <= last_sequence {
                    return Err(IndexLogError::Corruption(format!(
                        "index-log delta sequence continuity violation: record sequence {} is not greater than previous {}",
                        record.sequence, last_sequence
                    )));
                }
                last_sequence = record.sequence;
                // A whole-index IndexLogRecord also deserializes into IndexDeltaRecord (its
                // `index` field is ignored, leaving the delta fields empty). Only keep records
                // that carry a delta payload OR a WAL anchor (an anchor-only record still
                // advances the reconstructed watermark on load).
                if record.items.is_empty()
                    && record.meta.is_none()
                    && record.key_states.is_empty()
                    && record.applied_wal_sequence.is_none()
                {
                    continue;
                }
                if record.sequence > retain_after_sequence {
                    take(record);
                }
            }
        }
        Ok(())
    }

    /// Raw bytes of the log at `offset`.
    ///
    /// `offset` is a position in the LOG, not in whichever piece holds it: the pieces are read
    /// back to back, so a window that starts in one continues into the next rather than stopping
    /// at a boundary the caller cannot see.
    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let mut bytes: Vec<u8> = Vec::new();
        let mut at = 0_u64;
        let mut tally = IndexLogReadTally::start();
        for path in index_log_segment_paths(&inner.root, shard_id) {
            if bytes.len() as u64 >= size {
                break;
            }
            let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            if length == 0 {
                continue;
            }
            // A piece that ends before the window starts holds nothing the caller asked for, and
            // skipping it costs a stat rather than a read.
            if at.saturating_add(length) <= offset {
                at = at.saturating_add(length);
                continue;
            }
            let start_in_piece = offset.saturating_sub(at);
            let want = size.saturating_sub(bytes.len() as u64);
            let mut file = open_index_log_read(&path, &mut tally)?;
            file.seek(SeekFrom::Start(start_in_piece))?;
            file.take(want).read_to_end(&mut bytes)?;
            at = at.saturating_add(length);
        }
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        Ok(bytes)
    }

    /// The records in the window. Says nothing about whether the window was exhausted, which
    /// is why anything reporting completeness to a caller wants `scan_bounded` instead.
    pub fn scan(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexLogError> {
        self.scan_bounded(shard_id, start_offset, end_offset, max_bytes)
            .map(|(records, _)| records)
    }

    /// The records in the window, and whether `max_bytes` cut the scan short.
    ///
    /// The walk below stops for two unrelated reasons: the window ended, or the byte budget ran
    /// out. Returning only the records conflates them, and a caller that cannot tell them apart
    /// reports a truncated read as a complete one.
    pub fn scan_bounded(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<(Vec<(u64, Vec<u8>)>, bool), IndexLogError> {
        self.scan_collect(shard_id, start_offset, end_offset, max_bytes, |offset, raw| {
            (offset, raw)
        })
    }

    /// How many records the log holds, without building them.
    ///
    /// The caller that wanted this asked `scan(.., u64::MAX, u64::MAX)` and took `.len()` of the
    /// result, so it read the whole log into a vector to learn how many records were in it --
    /// on the plan path of every maintenance round.
    ///
    /// The same walk, projecting to `()`. A zero-sized element costs nothing per record, so peak
    /// memory is one record rather than all of them, and the count cannot drift from what a scan
    /// would have returned because it IS the scan. The write-ahead log store has the twin of
    /// this, for the same caller.
    pub fn record_count(&self, shard_id: ShardId) -> Result<usize, IndexLogError> {
        self.scan_collect(shard_id, 0, u64::MAX, u64::MAX, |_offset, _raw| ())
            .map(|(records, _truncated)| records.len())
    }

    /// The one walk every scan of this log shares, so they cannot drift about what a window
    /// contains. Mirrors `scan_collect` on the write-ahead log store.
    fn scan_collect<T>(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
        mut take: impl FnMut(u64, Vec<u8>) -> T,
    ) -> Result<(Vec<T>, bool), IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let segments = index_log_segment_paths(&inner.root, shard_id)
            .into_iter()
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        if segments.is_empty() {
            inner.stats.scans += 1;
            return Ok((Vec::new(), false));
        }
        let _ = last_sequence_at(&inner.root, shard_id)?;
        // A record's position is where it sits in the LOG, counted across the pieces in order --
        // not an offset into whichever file holds it, which would mean nothing to a caller once
        // there is more than one.
        let mut offset = 0_u64;
        let mut total = 0;
        let mut truncated = false;
        let mut records = Vec::new();
        let mut tally = IndexLogReadTally::start();

        'segments: for path in segments {
            let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            if offset.saturating_add(length) <= start_offset {
                offset = offset.saturating_add(length);
                continue;
            }
            let mut file = open_index_log_read(&path, &mut tally)?;
            let skip = start_offset.saturating_sub(offset);
            file.seek(SeekFrom::Start(skip))?;
            offset = offset.saturating_add(skip);
            let mut reader = BufReader::new(file);

            // Walk by RECORD, not by newline. What this returns is each record's raw framed
            // bytes -- the caller ships them onward untouched -- so the walk has to agree with
            // the writer about where a record ends. A newline scan decides that from a delimiter
            // a binary payload may itself contain, and would hand the caller half a record that
            // still looks like a whole one. The write-ahead log walks its own records with this
            // same reader, for this same reason.
            while let Some(raw) = crate::log_framing::read_raw_record(&mut reader)? {
                let read = raw.len() as u64;
                let next_offset = offset.saturating_add(read);
                if next_offset > end_offset {
                    break 'segments;
                }
                if total + read > max_bytes {
                    // Out of budget with the window not yet walked: there is more to read.
                    truncated = true;
                    break 'segments;
                }
                records.push(take(offset, raw));
                offset = next_offset;
                total += read;
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        inner.stats.records_read += records.len() as u64;
        Ok((records, truncated))
    }

    /// Answer the index-GC gate from the piece NAMES plus the piece being written.
    ///
    /// The gate wants three numbers -- the log's size, how many records it holds, and how many of
    /// them sit below a retain floor -- and used to get the last two by scanning the whole log and
    /// decoding every record. That is the cost `drop_covered_index_segments` stopped paying: a
    /// sealed piece's name already carries `start`, `end` and its highest WAL anchor, so
    ///
    /// - the piece holds `end - start` records, because sequences are assigned `last + 1` with no
    ///   holes -- the same arithmetic a round already reports its removals with, so the gate and the
    ///   collector cannot disagree about a piece;
    /// - the records in it below a floor are `min(end, floor) - start`, saturating, for exactly the
    ///   same reason: contiguity makes a count out of a subtraction, and a floor at or under
    ///   `start` leaves none.
    ///
    /// Only the piece being WRITTEN has no numbers in its name, so only that one is opened. It is
    /// at most the rolling threshold (64 KiB shipped), whatever the log has grown to, which is
    /// what makes this cost the number of PIECES rather than the number of records.
    ///
    /// THE ONE SHAPE THIS DOES NOT BOUND, said plainly rather than left to be discovered: a log
    /// that has never rolled is ONE piece and that piece is the whole log, so this decodes all of
    /// it -- exactly what the old gate did, no better and no worse. `TS_INDEX_LOG_SEGMENT_BYTES=0`
    /// and any store written before pieces are that shape. The bound is on the ROLLED log, and a
    /// guard asserting it has to say how many pieces its fixture actually had.
    ///
    /// Infallible on purpose. This is a GC-pressure metric, not the recovery path: a corrupt frame
    /// in the piece being written stops the walk and leaves the sealed pieces counted, where
    /// returning an error would have the caller read the whole log as EMPTY -- which reads as a
    /// log with nothing to collect rather than as a log that could not be measured. Replay is
    /// where a corrupt record is surfaced, and it still surfaces it.
    pub fn gate_summary(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> IndexLogGateSummary {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let root = inner.root.clone();
        let active = index_log_path(&root, shard_id);
        let mut summary = IndexLogGateSummary {
            shard_id,
            retain_from_sequence,
            ..IndexLogGateSummary::default()
        };
        let mut tally = IndexLogReadTally::start();
        for path in index_log_segment_paths(&root, shard_id) {
            // A `stat`, and for a sealed piece that is the whole of it. A piece that has just been
            // unlinked out from under this walk is simply not counted, the same way
            // `index_log_total_bytes` does not count it.
            let Ok(metadata) = path.metadata() else {
                continue;
            };
            summary.pieces += 1;
            summary.bytes = summary.bytes.saturating_add(metadata.len());
            if path != active {
                let Some(span) = sealed_index_log_span(&path, shard_id) else {
                    continue;
                };
                summary.pieces_named += 1;
                let held = span.end.saturating_sub(span.start);
                let removable = span.end.min(retain_from_sequence).saturating_sub(span.start);
                summary.records = summary.records.saturating_add(held as usize);
                summary.removable_records = summary
                    .removable_records
                    .saturating_add(removable as usize);
                continue;
            }
            let Ok(file) = open_index_log_read(&path, &mut tally) else {
                continue;
            };
            let mut reader = BufReader::new(file);
            // By FRAME, not by newline, for the reason every other walk of this log gives: a
            // binary payload may contain the delimiter a newline scan would stop at.
            while let Ok(Some((frame_bytes, payload))) = crate::log_framing::read_frame(&mut reader)
            {
                summary.bytes_decoded = summary.bytes_decoded.saturating_add(frame_bytes as u64);
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                summary.records_decoded += 1;
                summary.records += 1;
                // Through the index log's own decoder, and only the HEAD of the record: two shapes
                // share this log and both carry a sequence. A record that will not decode is
                // COUNTED but not called removable, which is what the scan this replaces did -- it
                // makes the ratio smaller, so it can only ever decline a round, never fire one.
                if let Ok(head) = decode_index_payload::<IndexRecordHead>(&payload) {
                    if head.sequence < retain_from_sequence {
                        summary.removable_records += 1;
                    }
                }
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += summary.bytes_decoded;
        inner.stats.records_read += summary.records_decoded as u64;
        summary
    }

    /// How many files this shard's log is in, the one being written included.
    pub fn piece_count(&self, shard_id: ShardId) -> usize {
        let inner = self.inner.lock().expect("index log lock poisoned");
        index_log_segment_paths(&inner.root, shard_id)
            .into_iter()
            .filter(|path| path.exists())
            .count()
    }

    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        self.gc_before_sequence_limited(shard_id, retain_from_sequence, 0)
    }

    pub fn gc_before_sequence_limited(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
        // The bound applies to the REWRITE of the piece being written, which is the only part of
        // a round that copies anything. Whole earlier pieces are unlinked, and that is not work a
        // round needs protecting from: a stat and an unlink each, whatever they hold.
        //
        // It used to bound the whole round, and bounding the whole round cost MORE: the round
        // rewrote what it retained, so removing fewer records meant copying more of them --
        // 40,000 records were 357 ms in one unlimited round against 492 ms limited to 200. That
        // inversion is what the pieces remove; see
        // `reclaim_costs_what_it_removes_not_what_it_keeps`.
        max_entries_per_round: usize,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        #[cfg(test)]
        probe::SWEEP_ENTRIES_BUDGETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Taken here and held to the end of the function -- past the walk, past the rewrite,
        // past the barrier and the rename. It is the same lock `append_json` takes, so for as
        // long as a sweep runs the shard's index log accepts no record. What bounds that hold is
        // therefore what bounds the sweep, and `max_entries_per_round` is not it: see the note
        // on the parameter above.
        let inner = self.inner.lock().expect("index log lock poisoned");
        // AFTER the guard, so it drops BEFORE it: the release reading is taken while the lock is
        // still held. `a_sweep_holds_the_lock_every_append_needs` reads the two.
        #[cfg(test)]
        let _lock_span = probe::SweepLockSpan::new();
        fs::create_dir_all(&inner.root)?;
        let root = inner.root.clone();
        let path = index_log_path(&root, shard_id);
        if !index_log_segment_paths(&root, shard_id)
            .iter()
            .any(|piece| piece.exists())
        {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence,
                max_entries_per_round,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = index_log_total_bytes(&root, shard_id);
        // Whole earlier pieces first. A piece whose sequences all sit below the floor is unlinked
        // without being opened, so what it HELD costs nothing -- which is the point: this round
        // used to pay for every record it kept.
        let (dropped_segments, dropped_segment_bytes, dropped_records) =
            drop_covered_index_segments(&root, shard_id, retain_from_sequence)?;

        let _ = last_sequence_at(&root, shard_id)?;
        let mut records_before = 0usize;
        let mut removed_this_round = 0usize;
        let mut removable_records_before_budget = 0usize;
        let mut retained = Vec::new();
        let mut reclaimable_bytes = 0u64;
        if path.exists() {
            let mut tally = IndexLogReadTally::start();
            // By frame, not by line: see the fold path above.
            let mut reader = BufReader::new(open_index_log_read(&path, &mut tally)?);
            while let Some((frame_bytes, payload)) = crate::log_framing::read_frame(&mut reader)? {
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                records_before += 1;
                #[cfg(test)]
                probe::SWEEP_FRAMES_READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Preserve the exact on-disk payload for retained records so a delta record is
                // not silently re-encoded as a whole-index record (which would drop its
                // items/meta). Decode verifies the integrity envelope; the retained raw payload
                // is re-framed on write-out below.
                let record: IndexRecordHead = decode_index_payload(&payload)?;
                if record.sequence < retain_from_sequence {
                    removable_records_before_budget =
                        removable_records_before_budget.saturating_add(1);
                }
                if record.sequence >= retain_from_sequence
                    || (max_entries_per_round > 0 && removed_this_round >= max_entries_per_round)
                {
                    retained.push(payload);
                } else {
                    removed_this_round = removed_this_round.saturating_add(1);
                    reclaimable_bytes = reclaimable_bytes.saturating_add(frame_bytes as u64);
                    #[cfg(test)]
                    probe::note_sweep_removed(shard_id, record.sequence);
                }
            }
        }

        // Nothing in the piece being written is reclaimable, which is the ordinary case once the
        // log rolls: the removable prefix lives in the pieces that were just unlinked. Rewriting
        // the piece would write it back byte for byte and take a barrier to do it.
        let mut bytes_copied = 0u64;
        let rewrite_skipped = removed_this_round == 0;
        if !rewrite_skipped {
            let temp_path = path.with_extension("gc.tmp");
            {
                // A WRITE, not a read: spelled in full because `File` is no longer in this
                // module's namespace, which is what stops a read being written uncounted.
                let mut temp = std::fs::File::create(&temp_path)?;
                for payload in &retained {
                    let framed = crate::log_framing::encode_record(payload);
                    bytes_copied = bytes_copied.saturating_add(framed.len() as u64);
                    #[cfg(test)]
                    probe::SWEEP_FRAMES_COPIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    temp.write_all(&framed)?;
                }
                temp.flush()?;
                crate::durability_metrics::record_barrier("engine_index_log_gc");
                temp.sync_all()?;
            }
            fs::rename(&temp_path, &path)?;
            sync_parent_dir(&path)?;
        }
        let bytes_after = index_log_total_bytes(&root, shard_id);
        let removed_from_active = records_before.saturating_sub(retained.len());
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence,
            max_entries_per_round,
            records_before: dropped_records.saturating_add(records_before),
            records_after: retained.len(),
            records_removed: dropped_records.saturating_add(removed_from_active),
            removable_records_before_budget: dropped_records
                .saturating_add(removable_records_before_budget),
            budget_exhausted: max_entries_per_round > 0
                && removable_records_before_budget > max_entries_per_round,
            bytes_before,
            bytes_after,
            reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
            rewrite_skipped,
            bytes_copied,
            dropped_segments,
            dropped_segment_bytes,
        })
    }

    /// Remove every record a completed catalog dump has made redundant, deciding retention per
    /// record on CONTENT rather than on log position alone.
    ///
    /// A dump durably materializes the base served index at WAL anchor `wal_anchor` and then
    /// appends its folded catalog anchor, which lands at index-log sequence `meta_sequence`.
    /// Everything the base reflects -- records whose own WAL anchor is at or below `wal_anchor`
    /// -- is redundant on load (the fold skips them), so it goes. Two kinds of record sit below
    /// `meta_sequence` yet must SURVIVE:
    ///
    /// - a delta a concurrent writer appended between the dump's serialization and its anchor
    ///   append: its WAL anchor is above `wal_anchor`, so the base does not reflect it, and
    ///   removing it would lose an eviction/removal that lives only in the delta stream;
    /// - a record carrying NO anchor that still holds items, a meta item or key-states. The
    ///   fold applies those whatever the anchor says, so nothing shows the base reflects them.
    ///   A legacy whole-index line also carries no anchor, but holds nothing the fold applies
    ///   and is read by no load path, so it IS treated as reflected and removed. Reading a
    ///   missing anchor as 0 made the two indistinguishable, and 0 is at or below every
    ///   anchor -- so every anchor-less record was removed, the content-bearing ones included.
    ///
    /// The folded catalog anchor itself is at `meta_sequence`, above the removal window, so the
    /// load-time catalog seed always survives. Position (`sequence < meta_sequence`) still
    /// bounds the sweep so a record appended AFTER the dump with a stale-looking anchor is never
    /// touched.
    /// `min_reclaimable_bytes` is the least the sweep must be able to reclaim before it will
    /// rewrite the log. Below it -- and always when nothing at all is reclaimable -- the log is
    /// left exactly as it is and the report says so.
    ///
    /// NO PER-ROUND ENTRY BUDGET, DELIBERATELY, and it is the sweep a background thread drives
    /// once a second while its budgeted sibling runs when an operator asks. Both halves of that
    /// are measured; [`POST_DUMP_SWEEP_ENTRY_BUDGET`] is the long form, and the short one is that
    /// a budget would bound neither the walk nor the lock this holds, and would make the rewrite
    /// copy more. What bounds this round is the rolling threshold: pieces below the floor are
    /// unlinked by name and never opened, so only the piece being written is walked.
    pub fn gc_reflected_before_anchor(
        &self,
        shard_id: ShardId,
        wal_anchor: u64,
        meta_sequence: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        #[cfg(test)]
        probe::SWEEP_ENTRIES_POST_DUMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Held to the end of the function, exactly as the budgeted sweep holds it, and for the
        // same reason: this is the lock `append_json` takes.
        let inner = self.inner.lock().expect("index log lock poisoned");
        #[cfg(test)]
        let _lock_span = probe::SweepLockSpan::new();
        fs::create_dir_all(&inner.root)?;
        let root = inner.root.clone();
        let path = index_log_path(&root, shard_id);
        if !index_log_segment_paths(&root, shard_id)
            .iter()
            .any(|piece| piece.exists())
        {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence: meta_sequence,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = index_log_total_bytes(&root, shard_id);
        // Whole earlier pieces first, and unconditionally: the threshold below exists to decline
        // a REWRITE that copies almost everything to reclaim almost nothing, and an unlink copies
        // nothing at all. A piece only goes when its name says every record in it is both below
        // the position bound and reflected by the base the dump wrote.
        let (dropped_segments, dropped_segment_bytes, dropped_records) =
            drop_reflected_index_segments(&root, shard_id, wal_anchor, meta_sequence)?;

        let _ = last_sequence_at(&root, shard_id)?;
        let mut records_before = 0usize;
        let mut retained = Vec::new();
        // `read_frame` hands back how many bytes the record OCCUPIED, which this loop used to
        // discard. It is the exact on-disk size of what a rewrite would drop, so the threshold
        // below is measured in the same bytes the file is measured in rather than in payloads.
        let mut reclaimable_bytes = 0u64;
        if path.exists() {
            let mut tally = IndexLogReadTally::start();
            // By frame, not by line: see the fold path above.
            let mut reader = BufReader::new(open_index_log_read(&path, &mut tally)?);
            while let Some((frame_bytes, payload)) = crate::log_framing::read_frame(&mut reader)? {
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                records_before += 1;
                #[cfg(test)]
                probe::SWEEP_FRAMES_READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Decode verifies the integrity envelope; the retained raw payload is re-framed
                // on write-out below, so a retained delta record keeps its exact on-disk bytes.
                let probe: IndexRecordHead = decode_index_payload(&payload)?;
                // THREE clauses, not two: below the position bound, AND the base PROVABLY
                // reflects it. `unwrap_or(0)` was not a proof -- it read a missing anchor as
                // anchor 0, which is at or below every anchor, so every anchor-less record was
                // classified reflected and removed. That is right for the legacy whole-index
                // lines named above and wrong for a DELTA that carries no anchor:
                // `for_each_delta_record` still folds its items and key-states, so removing it
                // loses exactly the eviction the fold would have applied. The expiry sweep
                // writes such a record whenever it reaches its checkpoint while
                // `shard.applied_wal_sequence` is still `None` -- no items, and every tombstone
                // of the round in its key-states.
                //
                // The missing clause is the FOLD'S OWN predicate, asked of the same record, so
                // the sweep and the walk cannot drift about which records matter. It costs a
                // second decode only for an anchor-less record, which is the rare one.
                let reflected = match probe.applied_wal_sequence {
                    Some(anchor) => anchor <= wal_anchor,
                    None => !delta_record_carries_content(&payload)?,
                };
                if probe.sequence >= meta_sequence || !reflected {
                    retained.push(payload);
                } else {
                    reclaimable_bytes = reclaimable_bytes.saturating_add(frame_bytes as u64);
                    #[cfg(test)]
                    crate::index_log::probe::note_sweep_removed(shard_id, probe.sequence);
                }
            }
        }

        // Rewriting while retaining every record writes the file back byte for byte, so the
        // barrier and the rename buy nothing at all -- that half is arithmetic, not policy. The
        // threshold is the policy: a rewrite that reclaims a handful of bytes still costs a full
        // read, a full write and an fsync, and the post-dump sweep runs on every dump.
        let removable = records_before.saturating_sub(retained.len());
        if removable == 0 || reclaimable_bytes < min_reclaimable_bytes {
            let bytes_after = index_log_total_bytes(&root, shard_id);
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence: meta_sequence,
                records_before: dropped_records.saturating_add(records_before),
                records_after: records_before,
                records_removed: dropped_records,
                removable_records_before_budget: dropped_records.saturating_add(removable),
                bytes_before,
                bytes_after,
                reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
                rewrite_skipped: true,
                bytes_copied: 0,
                dropped_segments,
                dropped_segment_bytes,
                ..IndexLogGcReport::default()
            });
        }

        let temp_path = path.with_extension("gc.tmp");
        let mut bytes_copied = 0u64;
        {
            // A WRITE, not a read. See the note on the other rewrite.
            let mut temp = std::fs::File::create(&temp_path)?;
            for payload in &retained {
                let framed = crate::log_framing::encode_record(payload);
                bytes_copied = bytes_copied.saturating_add(framed.len() as u64);
                #[cfg(test)]
                crate::index_log::probe::SWEEP_FRAMES_COPIED
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                temp.write_all(&framed)?;
            }
            temp.flush()?;
            crate::durability_metrics::record_barrier("engine_index_log_gc");
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        let bytes_after = index_log_total_bytes(&root, shard_id);
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence: meta_sequence,
            // NOT a bare zero. See the constant: an entry budget bounds neither this round's read
            // nor its lock hold, and setting one here would only make the rewrite copy more.
            max_entries_per_round: POST_DUMP_SWEEP_ENTRY_BUDGET,
            records_before: dropped_records.saturating_add(records_before),
            records_after: retained.len(),
            records_removed: dropped_records.saturating_add(removable),
            removable_records_before_budget: dropped_records.saturating_add(removable),
            budget_exhausted: false,
            bytes_before,
            bytes_after,
            reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
            rewrite_skipped: false,
            bytes_copied,
            dropped_segments,
            dropped_segment_bytes,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> IndexLogStats {
        let inner = self.inner.lock().expect("index log lock poisoned");
        IndexLogStats {
            last_sequence: last_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            ..inner.stats
        }
    }
}

impl Default for LocalIndexLogStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("index-logs");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("index log lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

/// The suffix an index log is written with.
///
/// Its records are binary -- the same framing the write-ahead log uses, a frame magic then a
/// payload -- and have been since the log stopped encoding JSON. A store written before this
/// carries the older name, which claimed a format it no longer held.
const INDEX_LOG_SUFFIX: &str = "bin";

/// What the same file used to be called. Still read, never written.
///
/// Dropping it rather than keeping it would find no index log where one exists, which reads as an
/// empty log rather than an error -- and an empty index log is a silently emptier shard.
const LEGACY_INDEX_LOG_SUFFIX: &str = "jsonl";

fn index_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    // Counted here as well as in `active_index_log`, and BEFORE the branches, so the probe is
    // "how many times was the active piece resolved, by any route" rather than "by the new
    // route". A change that put the append back on this function would otherwise read as free.
    #[cfg(test)]
    probe::APPEND_PATH_PROBES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let renamed = root.join(format!("shard-{shard_id}.indexlog.{INDEX_LOG_SUFFIX}"));
    if renamed.exists() {
        return renamed;
    }
    // An existing store keeps the name it already has, so one shard's log is never split across
    // two names. A store with neither is new, and starts under the current one.
    let legacy = root.join(format!("shard-{shard_id}.indexlog.{LEGACY_INDEX_LOG_SUFFIX}"));
    if legacy.exists() {
        return legacy;
    }
    renamed
}

/// The file the shard's log is being written to, AND how many bytes are in it, in ONE probe.
///
/// [`index_log_path`] answers the first question with `exists()`, and the roll then asked
/// `metadata()` for the second -- of the same path, a syscall later. `Path::exists()` IS
/// `metadata().is_ok()`, so the existence answer was already inside the length answer and the
/// append was buying it twice. This asks once and returns both.
///
/// The three-way answer is [`index_log_path`]'s, unchanged: the current name when it is there,
/// the legacy name when only that is, and the current name when neither is -- so one shard's
/// log is still never split across two names.
///
/// A length of 0 means an empty file OR no file, and both are "nothing to seal", which is the
/// only thing the caller asks the length for.
fn active_index_log(root: &Path, shard_id: ShardId) -> (PathBuf, u64) {
    #[cfg(test)]
    probe::APPEND_PATH_PROBES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let renamed = root.join(format!("shard-{shard_id}.indexlog.{INDEX_LOG_SUFFIX}"));
    if let Ok(metadata) = fs::metadata(&renamed) {
        return (renamed, metadata.len());
    }
    let legacy = root.join(format!("shard-{shard_id}.indexlog.{LEGACY_INDEX_LOG_SUFFIX}"));
    if let Ok(metadata) = fs::metadata(&legacy) {
        return (legacy, metadata.len());
    }
    (renamed, 0)
}

fn last_sequence_at(root: &Path, shard_id: ShardId) -> Result<u64, IndexLogError> {
    // A sealed piece SAYS what its last sequence is, in its name. It was sealed after a complete
    // append, so the number is authoritative -- and reading the piece back to learn it would make
    // every sequence probe cost the log's whole history again, which is the cost the pieces exist
    // to remove.
    let mut last = 0_u64;
    for piece in index_log_segment_paths(root, shard_id) {
        if let Some(span) = sealed_index_log_span(&piece, shard_id) {
            last = last.max(span.end.saturating_sub(1));
        }
    }
    let path = index_log_path(root, shard_id);
    if !path.exists() {
        return Ok(last);
    }
    // Only the piece being written can have a torn tail: a sealed piece was made durable and
    // renamed after a whole record landed, and nothing appends to it afterwards. So this trims
    // the active piece, exactly as it trimmed the single file before there were pieces.
    let mut tally = IndexLogReadTally::start();
    let mut reader = BufReader::new(open_index_log_repair(&path, &mut tally)?);
    let mut good_offset = 0_u64;
    loop {
        // By FRAME, not by newline. This function truncates: it trims the file back to the
        // last whole record. A newline scan decides where records end by looking for a
        // delimiter, which a binary payload may legitimately contain -- so it would find an
        // end that is not one, call the remainder torn, and set_len durable records away.
        // `read_frame` takes the length the record declares, and still reads text-framed and
        // legacy unframed records, so what counts as "whole" no longer depends on the payload
        // encoding. Mirrors wal.rs::last_wal_sequence_at.
        match crate::log_framing::read_frame(&mut reader) {
            // A complete record. Whitespace-only filler advances the good offset without
            // being parsed, exactly as the newline scan did.
            Ok(Some((consumed, payload))) => {
                good_offset = good_offset.saturating_add(consumed as u64);
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                // A COMPLETE record that fails to parse is committed corruption, not a torn
                // tail. Treating it as end-of-log would set_len the file down to the last
                // parseable record -- silently dropping durable index-log records after the
                // corrupt one AND rewinding the sequence counter (the next append reuses a
                // sequence that durable dump manifests already cite). Surface it as an error;
                // index-log replay returns DataLoss on a hole or digest mismatch, never trims.
                let record = decode_index_payload::<IndexRecordHead>(&payload)?;
                last = last.max(record.sequence);
            }
            // Nothing further, or fewer bytes than the record declares: a crash mid-append.
            // That tail is what `good_offset` trims below.
            Ok(None) => break,
            // A whole record whose digest does not match its payload: committed damage.
            Err(err) => return Err(IndexLogError::Corruption(err.0)),
        }
    }
    // Back out of the reader to truncate through the same handle: the bytes it read are
    // already charged, and taking a second descriptor to do this is what made the write-ahead
    // log's tail repair count one open where two happened.
    let file = reader.into_inner();
    if good_offset < file.metadata()?.len() {
        file.set_len(good_offset)?;
        crate::durability_metrics::record_barrier("engine_index_log_seq_probe");
        file.sync_all()?;
        sync_parent_dir(&path)?;
    }
    Ok(last)
}

/// TS_INDEX_LOG_SEGMENT_BYTES: roll the index log into a new piece once the one being written
/// passes this many bytes.
///
/// Zero never rolls, which is one file -- the shape every store written before this has, and the
/// shape this code still reads.
///
/// Rolling is what makes reclaim cost what it REMOVES. One file per shard has to be reclaimed by
/// reading it and rewriting every record it keeps, so a round that removes little copies almost
/// everything: 40,000 records took 357 ms in one unlimited round against 492 ms limited to 200 --
/// bounding the round made it DEARER, because the bound is on what it removes and the cost is on
/// what it retains. In pieces the removal is an unlink: a stat and a `remove_file`, whatever the
/// piece holds, and nothing is read or copied.
///
/// **Default 64 KiB**, chosen against the gate that lets this log grow rather than by feel. Index
/// GC fires at `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD`, 768 KiB, and only when at least 40%
/// of the records are removable -- so the log a round actually meets is about 768 KiB with a
/// removable prefix of about 300 KiB. A round can only unlink pieces that lie WHOLLY inside that
/// prefix, so the piece size is the granularity of reclaim:
///
/// | piece | pieces in 768 KiB | unlinked from a 40% prefix |
/// |---|---|---|
/// | 256 KiB (the write-ahead log's) | 3 | 1 -- a third of the log |
/// | 64 KiB | 12 | 4 -- 87% of what is removable |
/// | 8 KiB | 96 | 38 -- 99%, and 96 names per shard |
///
/// 64 KiB is where the granularity stops being the limit and the file count has not yet started
/// to be one. The write-ahead log's 256 KiB is larger because it is pinned to its preallocation
/// chunk, which this log does not have.
fn index_log_segment_bytes() -> u64 {
    if let Some(threshold) = INDEX_SEGMENT_BYTES_OVERRIDE.with(|value| value.get()) {
        return threshold;
    }
    std::env::var("TS_INDEX_LOG_SEGMENT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(DEFAULT_INDEX_LOG_SEGMENT_BYTES)
}

/// Rolling threshold when nothing sets one. See [`index_log_segment_bytes`].
pub const DEFAULT_INDEX_LOG_SEGMENT_BYTES: u64 = 64 * 1024;

/// The per-round entry budget the post-dump sweep applies: NONE, deliberately.
///
/// [`LocalIndexLogStore::gc_reflected_before_anchor`] takes no `max_entries_per_round` parameter
/// and has no budget test in its record loop. This is the value its report carries, so a reader
/// of an `IndexLogGcReport` off that path sees the absence stated rather than inferred -- and so
/// a third sweep cannot pick up a bare literal without meeting the reason for it.
///
/// WHAT `0` MEANS, READ FROM THE CONSUMER. The only code that consumes this quantity is
/// [`LocalIndexLogStore::gc_before_sequence_limited`], which guards it as
/// `max_entries_per_round > 0 && removed_this_round >= max_entries_per_round`, and the
/// `budget_exhausted` term, guarded the same way. `0` is therefore "no budget" and not "use the
/// default" -- `the_default_entry_budget_belongs_to_the_path_that_runs_when_asked` drives the
/// consumer at this value and asserts it removes everything it can.
///
/// WHY THIS PATH HAS NONE, AND WHY MATCHING THE OTHER PATH'S 256 WOULD BE A PESSIMISATION.
/// An entry budget is not a bound on the work of either sweep. When the budget is reached the
/// record is pushed onto `retained` and the loop CONTINUES, so the budget:
///
/// - does not bound what the round READS -- both loops read the piece being written to its end;
/// - does not bound how long the round holds `inner`, which is the lock `append_json` takes;
/// - bounds what the round REMOVES, and so makes the rewrite copy MORE, not less.
///
/// Measured in counts in `index_log_sweep_bound::an_entry_budget_copies_more_and_reads_exactly_as_much`:
/// the same log, budgeted at 256, reads the identical number of frames, removes fewer records
/// and copies strictly more bytes than the same round with no budget at all. The note on
/// `gc_before_sequence_limited`'s parameter records the same inversion in milliseconds at 40,000
/// records, from the shape that preceded piece unlinking.
///
/// WHAT DOES BOUND THIS SWEEP is [`DEFAULT_INDEX_LOG_SEGMENT_BYTES`]. Sealed pieces below the
/// floor are unlinked by NAME without being opened, so the walk and the rewrite see only the
/// piece being written -- at most the rolling threshold, whatever the store holds. That is a
