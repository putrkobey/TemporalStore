// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-ahead log.
//!
//! A shard's log is a sequence of pieces. `shard-{id}.wal.bin` is the one being written;
//! sealed ones are `shard-{id}.wal.{start_log_id:020}.bin`, named so they sort into log order.
//! Pieces written before the rename carry a `.jsonl` suffix; both are read, only `.bin` is written.
//! `TS_WAL_SEGMENT_BYTES` sets when a piece is sealed. It defaults to
//! [`DEFAULT_WAL_SEGMENT_BYTES`], 256 KiB, so a log ROLLS unless something says
//! otherwise. Zero is still accepted and still means never seal, which leaves the log a
//! single file behaving exactly as it did before pieces existed.
//!
//! Positions are **log ids**: a byte position in the log's whole history, not in whichever file
//! happens to hold it. Each piece records in its first bytes the log id its contents start at, so
//! a log id says which piece holds a record and where inside it, by arithmetic. A log id keeps
//! meaning across pieces, and survives reclaim.
//!
//! Reclaim unlinks whole pieces that hold nothing above the retain floor, and copies only within
//! the piece being written.
//!
//! # Two things deliberately absent
//!
//! **There is no per-piece index from record number to byte offset.** A segmented log usually
//! carries one, as a sidecar file, so that "read entries N through M" is a seek rather than a walk
//! from the start of the piece. Here it would have no reader. Random access is by log id, which is
//! already arithmetic and needs no index; and the only component that reads the log sequentially is
//! recovery (`engine::lifecycle::replay_wal_into_shard`), which starts at a watermark and reads
//! forward to the end -- in BOUNDED WINDOWS, so it seeks into the middle of a piece once per
//! window, and it does so by arithmetic on the log id, which is exactly why no index is wanted.
//! (This paragraph used to say a seek into the middle bought recovery nothing. It bought a
//! 200,000-record restore 927 MB, because without it every window re-read the piece from its
//! header to find where the last one stopped.) Replication does not read this
//! log at all -- the raft log is a separate structure under `raft::local_wal`. Adding the sidecar
//! would mean another file to write, checksum, recover and keep consistent with the piece beside
//! it, in exchange for nothing, and it would have to be correct across reclaim and a torn tail.
//!
//! Should something ever need to find a record by sequence without reading forward to it, this is
//! the thing to build, and the reason it was not built is only that nothing needed it.
//!
//! **Pieces are not preallocated.** Writing into a file that is already its full size avoids
//! persisting a new size on every barrier, which measured 42.7% cheaper per record at eight
//! records per barrier. It is not done because the file's length is what tells the log where its
//! records end -- used to pick the next write offset, find the tail, repair a torn tail, seal a
//! piece, and bound every reader -- so preallocating means threading a separate logical end
//! through all of them. Reserving blocks without changing the size (`FALLOC_FL_KEEP_SIZE`), which
//! would need none of that, was measured and made no difference: the cost is persisting the size,
//! not allocating the blocks.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Command, ShardId};

#[derive(Debug, Error)]
pub enum WriteAheadLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed, newline-terminated record failed its per-record integrity envelope (the
    /// length or CRC/SHA-256 digest did not match the payload). This is silent value-loss --
    /// a bit-flip that still parses as JSON -- so recovery surfaces it as data loss and
    /// refuses to trust the record rather than replaying the corrupted value.
    #[error("wal record integrity error: {0}")]
    Corruption(String),
}

impl From<crate::log_framing::FramingError> for WriteAheadLogError {
    fn from(err: crate::log_framing::FramingError) -> Self {
        WriteAheadLogError::Corruption(err.0)
    }
}

fn current_write_ahead_log_format_version() -> u32 {
    WRITE_AHEAD_LOG_FORMAT_VERSION
}

fn is_current_write_ahead_log_format_version(version: &u32) -> bool {
    *version == WRITE_AHEAD_LOG_FORMAT_VERSION
}

/// Decode one raw WAL line (framed or legacy-unframed) into a [`WriteAheadLogRecord`],
/// verifying the per-record integrity envelope when present. Used by every WAL reader
/// (`last_wal_sequence_at`, `scan` consumers, GC, `info`) so a value-preserving bit-flip in a
/// committed record surfaces as `Corruption` instead of replaying as truth.
/// Encode a record exactly as the append path would frame it.
///
/// Exposed so a test can compare encodings of the SAME record rather than of two separately
/// written logs, which is the only way to say anything about fidelity.
pub fn encode_wal_line_for_test(
    record: &WriteAheadLogRecord,
) -> Result<Vec<u8>, WriteAheadLogError> {
    Ok(crate::log_framing::encode_line(&encode_wal_payload(record)?))
}


/// Read one record's bytes AS FRAMED, or `None` at the end of the records.
///
/// Lives in `log_framing` now, because the served-index log needs exactly this reader for
/// exactly this reason and a second copy of the rule would be free to drift from this one.
fn read_raw_record<R: std::io::BufRead>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    crate::log_framing::read_raw_record(reader)
}

pub fn decode_wal_line(line: &[u8]) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
    let payload = crate::log_framing::decode_line(line)?;
    // Which encoding a payload is in is a property of the payload, never of configuration: a log
    // written across a flag change still reads end to end.
    if payload.first() == Some(&crate::wal_proto::BINARY_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::RAW_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::COMPRESSED_RAW_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::COMPRESSED_ESCAPED_PAYLOAD_MARKER)
    {
        return crate::wal_proto::decode(payload).map_err(|err| {
            WriteAheadLogError::Corruption(format!("engine wal record decode failed: {err}"))
        });
    }
    let (document, carried) = split_carried_payloads(payload)?;
    if carried.is_empty() {
        return Ok(serde_json::from_slice::<WriteAheadLogRecord>(document)?);
    }
    crate::bytes_serde::with_payloads(carried, || {
        serde_json::from_slice::<WriteAheadLogRecord>(document)
    })
    .map_err(WriteAheadLogError::from)
}

/// Separates the document from the payloads carried beside it.
///
/// A serde_json document never contains a literal 0x1f -- control bytes are escaped -- so this can
/// only be the separator this writer put there. A record written without carried payloads has none
/// and is returned whole.
const CARRIED_SEPARATOR: u8 = 0x1f;

fn split_carried_payloads(payload: &[u8]) -> Result<(&[u8], Vec<Vec<u8>>), WriteAheadLogError> {
    let Some(document_end) = payload.iter().position(|byte| *byte == CARRIED_SEPARATOR) else {
        return Ok((payload, Vec::new()));
    };
    let document = &payload[..document_end];
    let rest = &payload[document_end + 1..];
    let lengths_end = rest
        .iter()
        .position(|byte| *byte == CARRIED_SEPARATOR)
        .ok_or_else(|| {
            WriteAheadLogError::Corruption(
                "record carries payloads but does not say how long they are".to_string(),
            )
        })?;
    let lengths = std::str::from_utf8(&rest[..lengths_end])
        .map_err(|_| {
            WriteAheadLogError::Corruption("payload lengths are not readable".to_string())
        })?
        .split(',')
        .map(|length| {
            length.parse::<usize>().map_err(|_| {
                WriteAheadLogError::Corruption(format!("payload length is not a number: {length}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut carried = Vec::with_capacity(lengths.len());
    let mut at = lengths_end + 1;
    for length in lengths {
        let end = at.checked_add(length).filter(|end| *end <= rest.len()).ok_or_else(|| {
            WriteAheadLogError::Corruption(
                "record claims more carried payload than it holds".to_string(),
            )
        })?;
        carried.push(
            crate::bytes_serde::unescape_payload(&rest[at..end])
                .map_err(WriteAheadLogError::Corruption)?,
        );
        at = end;
    }
    if at != rest.len() {
        return Err(WriteAheadLogError::Corruption(
            "record holds more carried payload than it claims".to_string(),
        ));
    }
    Ok((document, carried))
}

/// Encode a record. Protobuf, always.
///
/// The JSON writer this used to choose between is gone. It cost a third more bytes on the wire
/// than protobuf before compression, could not be compressed at all -- the compressor is reached
/// only from `wal_proto::encode` -- and carried its byte payloads base64'd, which is a flat 1.33x
/// on every value. A live store that turned it off wrote 8,756 bytes per record; the same traffic
/// through this path writes 1,774.
///
/// READING json is a different question and is still supported: a payload says what it is by its
/// first byte, and logs already written -- here and in every fork and deployment of this project --
/// hold JSON records that must keep loading. See `decode_wal_line`, which is where that lives now.
fn encode_wal_payload(record: &WriteAheadLogRecord) -> Result<Vec<u8>, WriteAheadLogError> {
    crate::wal_proto::encode(record).map_err(|err| {
        WriteAheadLogError::Corruption(format!("engine wal record encode failed: {err}"))
    })
}
impl WalOutcomeItem {
    /// The address a recorded page installs at.
    ///
    /// IT USED TO PUT THE ROUTING BUCKET BACK, and there is nothing left to put back: a
    /// `BlockAddress` does not hold one. The item still carries `routing_bucket` as its own field
    /// and that is where the bucket comes from -- the item IS the container, and every caller here
    /// has it in hand beside the address.
    ///
    /// KEPT AS A NAMED ACCESSOR rather than folded into `self.address.clone()` at fifteen sites,
    /// because what it names is still a distinction worth reading: `address` is an `Option` that is
    /// `None` for an item that recorded no page, and "the address this item installs at, if it
    /// installs one" is the question every caller is asking.
    pub fn resolved_address(&self) -> Option<crate::block_store::ElementEntry> {
        self.address.clone()
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteAheadLogRecord {
    // The names below are deliberately short. They are repeated in every record for the life of
    // the log, and on a small write they cost more than the data does; the alias on each keeps
    // every record already written readable.
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    /// The operation this write performed, for a record that cannot say what it DID.
    ///
    /// Absent once a record carries its results, which is the point: replaying an operation
    /// reproduces state only if everything that influenced it is reproduced too, and a record that
    /// states results needs none of that. Present on every record written before results existed,
    /// and on any write that still records nothing, so both replay exactly as they always did.
    #[serde(
        rename = "c",
        alias = "command",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub command: Option<Command>,
    #[serde(
        rename = "m",
        alias = "metadata",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<WriteAheadLogRecordMetadata>,
    /// Pages this write produced, carried in the record that records the write.
    ///
    /// A page is often derived state rather than the command's own bytes, so it cannot be
    /// rebuilt from the command alone. Carrying it here is what lets a read serve the page back
    /// out of the log. Empty and skipped for every write that stages nothing, so records
    /// written without this are byte-identical to before.
    ///
    /// AT THE SHIPPED DEFAULTS THIS IS THE ONLY WAY BACK FOR AN UPSERT, which is the fact the
    /// durability note on `TS_WAL_DATA_ONLY` below used to be written without. A record no longer
    /// carries the operation, and 16 of an upsert cell's 20 results state nothing but a
    /// block-store address whose fdatasync the ack deferred -- so the bytes in this field are
    /// what replay rebuilds those 16 from. Staging is unconditional for them: every production
    /// `append_value` call site passes `Some(object_id)`, and `append_value_inner` stages for
    /// every one of those, on both the synchronous and the asynchronous arm.
    ///
    /// `tests/what_an_ack_promises.rs` walks all eight write shapes and prints the column.
    #[serde(
        rename = "p",
        alias = "staged_blocks",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub staged_blocks: Vec<StagedBlock>,
    /// What this write did, stated as results rather than as the operation that caused them.
    ///
    /// Called `outcomes` and not `items` on purpose: [`WriteAheadLogRecordMetadata`] already has
    /// an `items` field describing the record's shape, and two different `items` on one record
    /// would be read wrong by whoever came next.
    ///
    /// THIS IS WHAT A RECORD NORMALLY HOLDS, NOT AN OPT-IN EXTRA, and this doc used to read as
    /// the opposite. [`wal_outcome_items_enabled`] is DEFAULT ON and the variable opts OUT, and
    /// [`wal_data_only_enabled`] is default ON too -- so at the shipped defaults an upserting
    /// write records its results HERE and its record carries no operation at all. Measured by
    /// `tests/what_an_ack_promises.rs` across all eight write shapes: every upsert cell records
    /// 20 results and 0 operations.
    ///
    /// The exception is a write with no page to carry. A removal's record keeps its operation
    /// instead (12 of 12 records on both separate removal cells), because `record_command` asks
    /// whether the blocks behind these results can be found again and a removal has staged none.
    /// So a reader cannot assume either field is the populated one: which it is depends on the
    /// write shape, and the table in that test is the enumeration.
    ///
    /// Empty and skipped when it is empty, so a record written without it is byte-identical to a
    /// record from a build that had no such field.
    #[serde(
        rename = "t",
        alias = "outcomes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub outcomes: Vec<WalOutcomeItem>,
}
/// TS_WAL_DATA_ONLY: stop writing the operation into a record that already states its results.
///
/// Default ON. Carrying both is bigger, and for a record whose page survives the crash it buys
/// nothing -- the results are what replay installs, and the operation is consulted only when there
/// are none. Set to a falsey value to keep writing both, which is what a consumer reading records
/// directly would want.
///
/// # THE "0 OF 300" THIS PARAGRAPH USED TO REPORT IS STALE. RE-MEASURED, IT IS 300 OF 300
///
/// What it said was that an outcome can be installed only if the page it names is DURABLE; that
/// under single-barrier the data-page fdatasync is deferred past the ack; and that this flag has
/// therefore removed the only thing that could re-derive the value. It cited
/// `tests/wal_single_barrier_recovery.rs` recovering 0 of 300 acked keys, over a probe reading
/// "300 records, all decoding, 0 carrying a command, 0 whose outcome items carry a value".
///
/// The first half is sound and still is: `sync` is a fact about the LOG's barrier, and a page
/// fsync deferred past it is not made durable by it. What the reading MISSED is a third column.
/// A record can carry the page itself, and at these defaults it does. Run on this commit, with no
/// mode variable set:
///
/// ```text
/// test result: ok. 8 passed; 0 failed      (tests/wal_single_barrier_recovery.rs)
/// ```
///
/// including `wal_is_self_sufficient_when_all_non_wal_state_is_wiped`, which deletes every
/// non-WAL byte -- pages, served index and delta log -- and reads all 300 keys back. The probe
/// the old paragraph quoted prints the missing column itself, one line below the three it was
/// read from: `carrying STAGED PAGES`.
///
/// `tests/what_an_ack_promises.rs` takes the count per write shape, per RESULT rather than per
/// record, and matches each address-only result against the pages its own record carries:
///
/// ```text
/// shape                       records operation results  bytes removal deadline addr-only pages no-way-back
/// sync  / separate / upsert        20         0      20      4       0        0        16    20           0
/// sync  / batched  / upsert         1         0      20      4       0        0        16    20           0
/// async / separate / upsert        20         0      20      4       0        0        16    20           0
/// async / batched  / upsert         1         0      20      4       0        0        16    20           0
/// sync  / separate / removal       12        12      12      0       8        4         0     0           0
/// sync  / batched  / removal        1         0      12      0       8        4         0     0           0
/// async / separate / removal       12        12      12      0       8        4         0     0           0
/// async / batched  / removal        1         0      12      0       8        4         0     0           0
/// ```
///
/// Zero in the last column in all eight. An upsert's record carries its page; a removal's record
/// keeps its operation or states the removal. So this flag does not cost an ack its durability at
/// the shipped defaults, and the decision the old paragraph said was outstanding is not
/// outstanding on these numbers.
///
/// WHAT IS STILL WORTH WATCHING is the shape of the claim rather than the claim. Three of the
/// eight cells are recoverable only because a page travels in the record, so the batch path asks
/// `every_result_can_be_found_again` -- per RESULT. It used to decide from
/// `sync || !staged_blocks.is_empty()`, and BOTH halves of that were wrong. The `sync` term is
/// exactly the inference the first half of the old paragraph correctly rejects. The other half
/// asked a per-result question at BATCH granularity, so one carried page anywhere in a batch
/// answered for every result in it: measured on a plant that stops one command kind staging, that
/// left 12 of 20 acked results with no way back, and removing the `sync` term alone moved the 12
/// by nothing. `no_cell_leaves_an_acked_result_with_no_way_back` is what fails if a batch's pages
/// stop being carried, and `a_batch_is_not_recoverable_because_a_sibling_carried_a_block` is what
/// fails if the question is asked at batch granularity again.
///
/// AND THE LESSON ABOUT THE READING, which outlives both numbers: three counts were taken, all
/// three were zero, and the conclusion drawn was about a FOURTH channel that nobody counted. A
/// probe that prints four columns and an argument that uses three agree with each other right up
/// to the point where they do not.
pub fn wal_data_only_enabled() -> bool {
    // Default ON; the variable opts OUT. It used to recognise `0` and `false` and nothing else,
    // so `off` -- and `no`, and a `" 0"` a unit file leaves behind -- kept it on. Its neighbour
    // `wal_outcome_items_enabled` sixty lines below already took the whole off-set. Given what
    // the comment above says this flag decides, the two should not have differed.
    crate::env_flag::env_bool("TS_WAL_DATA_ONLY", true)
}

/// What a record should carry as its operation, given what it recorded and what is behind it.
///
/// A result names where a block LANDED. Installing it reproduces the write only if a reader can
/// then FIND that block, and there are two ways it cannot.
///
/// An asynchronous write leaves its block buffered, so a crash can leave a record whose result
/// points at bytes that were never written. And a write whose block travels INSIDE the record --
/// the log-resident path -- needs that block registered against the record's log id before any
/// read can resolve it, which the write path does and installing an address does not.
///
/// So results replace the operation when the block behind them can be found afterwards: a
/// synchronous write whose block is already in the block store, or ANY write carrying its block
/// inside the record, because replay now registers what it replays. What is left keeps its
/// operation and recovers by re-running it -- its result names a block-store address a crash may
/// leave unwritten, and registering cannot conjure a block that was never stored.
///
/// THAT LAST CASE IS NOT "THE ASYNCHRONOUS WRITE WITH NOTHING CARRIED", WHICH IS WHAT THIS
/// PARAGRAPH USED TO CALL IT. It is ANY write with nothing carried, synchronous included, and the
/// difference is visible in the `operation` column of `tests/what_an_ack_promises.rs`: the
/// synchronous separate removal cell keeps its operation on 12 of 12 records, exactly as the
/// asynchronous one does.
///
/// The reason is which caller reaches here. `Engine::concurrent_commit` is true on every engine
/// but the one test that measures the in-lock barrier, so a separate synchronous write takes
/// `append_for_group_commit`, and that call site passes `!staged_blocks.is_empty()` with no
/// `sync` term -- deliberately, as the comment there explains. The `sync ||` at the
/// `append_with_sync_inner` call site decides nothing at the shipped defaults: it is reached only
/// when `sync` is false, where the term is inert, or when the caller handed over carried blocks,
/// where the other half is already true.
///
/// The BATCH path used to be the one place `sync ||` was live. It no longer carries the term at
/// all -- it asks `every_result_can_be_found_again` -- so the only `sync ||` left is the inert one
/// above. Read this paragraph as a claim about which writes are exposed and it points at the wrong
/// half of the write path.
///
/// Both halves were found the same way: a recovery test failing 8 runs in 12 against 0 in 12 on
/// the code before this, measured interleaved on one machine because an uncontrolled comparison
/// had already told me the opposite once.
///
/// The carried-block half of that measurement predates replay registering blocks. The rule tried
/// then -- accepting carried blocks -- failed 5 runs in 12 precisely because nothing registered
/// them on the way back in. That is now done, which is what makes the same rule correct.
pub(crate) fn record_command(
    command: Command,
    outcomes: &[WalOutcomeItem],
    blocks_are_recoverable: bool,
) -> Option<Command> {
    if outcomes.is_empty() || !wal_data_only_enabled() || !blocks_are_recoverable {
        Some(command)
    } else {
        None
    }
}

/// The same question, asked of a BATCH, and asked PER RESULT.
///
/// A batch is ONE record holding many results, so "can the blocks behind these results be found
/// again" has as many answers as there are results. The rule the batch path used to form was
/// `sync || !staged_blocks.is_empty()`, and the SCOPE was the defect rather than the term: the
/// second half asks a per-result question at BATCH granularity, so one carried page anywhere in a
/// batch answered it for every result in that batch -- including results whose own page was never
/// staged.
///
/// Measured, on a batch of five command kinds where one kind stops staging its pages: that kind's
/// four pages kept the staged set non-empty, the operation was dropped anyway, and 12 of 20 acked
/// results were left naming a block-store address and nothing else. Dropping the `sync` term
/// moved that 12 by NOTHING, which is what separates the two halves: `sync` is a fact about the
/// LOG's barrier, inert here because the staged test was already answering true.
///
/// So a result is covered when it STATES ITSELF -- bytes, a removal, a new deadline -- or when
/// this batch carries a page for it. The page match UNDER-matches by design, the same way
/// `tests/what_an_ack_promises.rs` matches: a whole-object page spells its component `None` and is
/// accepted for any element of its object, so coverage can only ever be counted LOW and a `true`
/// here is the direction a durability claim needs.
///
/// `a_batch_is_not_recoverable_because_a_sibling_carried_a_block` is what fails if this is ever
/// asked at batch granularity again.
pub(crate) fn every_result_can_be_found_again(
    outcomes: &[WalOutcomeItem],
    blocks: &[StagedBlock],
) -> bool {
    outcomes.iter().all(|item| {
        item.deleted
            || item.ttl.is_some()
            || item.value.as_ref().map(|value| !value.is_empty()).unwrap_or(false)
            || blocks.iter().any(|block| {
                block.object_id == item.object_id
                    && (block.component.as_deref() == item.component.as_deref()
                        || block.component.is_none())
            })
    })
}

#[cfg(test)]
mod every_result_can_be_found_again_tests {
    use super::{every_result_can_be_found_again, StagedBlock, WalOutcomeItem};

    /// A result that names an address and states nothing about itself.
    fn names_only_an_address(object_id: u64, component: &str) -> WalOutcomeItem {
        WalOutcomeItem {
            kind: "page".to_string(),
            object_key: format!("object/{object_id}"),
            component: Some(component.to_string()),
            object_id,
            routing_bucket: 7,
            address: None,
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        }
    }

    fn block_for(object_id: u64, component: &str) -> StagedBlock {
        StagedBlock {
            object_id,
            component: Some(std::sync::Arc::from(component)),
            bytes: vec![b'p'; 8],
        }
    }

    /// A PAGE COVERS THE RESULT IT BELONGS TO AND NO OTHERS.
    ///
    /// This is the regression the per-result scope exists for. A batch-granular implementation --
    /// `!pages.is_empty()`, which is what this path carried before -- returns `true` for the mixed
    /// case below, and that is how a measured batch left 12 of 20 acked results with no way back
    /// while its rule said the batch was recoverable.
    #[test]
    fn a_batch_is_not_recoverable_because_a_sibling_carried_a_block() {
        let covered = names_only_an_address(1, "a");
        let uncovered = names_only_an_address(2, "b");
        let blocks = vec![block_for(1, "a")];
        // THE REGIME FIRST, so the `false` below is about the uncovered result and not about a
        // page set this helper failed to build: the covered result is covered on its own.
        assert!(
            every_result_can_be_found_again(std::slice::from_ref(&covered), &blocks),
            "a result whose own page IS staged must be covered, or the mixed case says nothing"
        );
        // THE CONCLUSION.
        assert!(
            !every_result_can_be_found_again(&[covered, uncovered], &blocks),
            "a result whose own page was never staged is not recoverable because a SIBLING's was"
        );
    }

    /// A result that states itself needs no page, which is what keeps a batched removal on the
    /// one-record writer rather than writing one command record per removal.
    #[test]
    fn a_result_that_states_itself_needs_no_block() {
        let mut removed = names_only_an_address(3, "c");
        removed.deleted = true;
        let mut expiring = names_only_an_address(4, "d");
        expiring.ttl = Some(600_000);
        let mut carrying = names_only_an_address(5, "e");
        carrying.value = Some(vec![b'v'; 4]);
        // THE REGIME: with nothing stated, nothing staged, the answer is NO. Without this the
        // conclusion below would pass against a function that returns `true` unconditionally.
        assert!(
            !every_result_can_be_found_again(
                std::slice::from_ref(&names_only_an_address(3, "c")),
                &[]
            ),
            "a result that states nothing and has no page must NOT be recoverable"
        );
        // THE CONCLUSION, each of the three statements on its own so one carrying the others is
        // not mistaken for all three working.
        for (label, item) in [
            ("a removal", removed),
            ("a deadline", expiring),
            ("its own bytes", carrying),
        ] {
            assert!(
                every_result_can_be_found_again(std::slice::from_ref(&item), &[]),
                "a result stating {label} needs no page and must be recoverable with none staged"
            );
        }
    }

    /// An empty batch is vacuously recoverable, and the caller is what keeps that from mattering:
    /// the data-only branch is also gated on the outcomes being non-empty.
    #[test]
    fn an_empty_batch_is_vacuously_recoverable() {
        assert!(every_result_can_be_found_again(&[], &[]));
    }
}


/// TS_WAL_OUTCOME_ITEMS: also record what a write DID, beside the command that did it.
///
/// The log records commands, so replay re-executes them -- which reproduces state only if
/// everything that influenced the original execution is reproduced too. That is why replay has
/// to walk a config log to re-apply the eviction config effective at each record, and why it
/// pins a replay clock so TTLs resolve against the leader's timestamp instead of the restart
/// clock. Both are scar tissue from logging operations rather than results.
///
/// An outcome states the result instead: this object's page now lives at this address, or this
/// object is gone. Replay can install that without running anything -- PROVIDED the page it
/// names is durable. Under single-barrier, the default, that fdatasync is deferred; see the
/// durability note on `TS_WAL_DATA_ONLY` above, because with both defaults on a crash between
/// the ack and the page write leaves nothing any replay can rebuild the value from.
///
/// DEFAULT ON. The comment here used to say "default OFF while both are carried", and described a
/// state that no longer exists: records no longer carry both. A mixed workload was walked across
/// every write shape -- synchronous and asynchronous, separate and batched -- and the command has
/// come out of the records that record results.
///
/// IT IS NOT "ZERO RECORDS CARRY AN OPERATION", WHICH IS WHAT THAT WALK WAS WRITTEN UP AS. The
/// walk covered upserts, and a write with no page to carry keeps its operation on purpose --
/// `record_command` asks whether the blocks behind the results can be found again, and a removal
/// has staged none. Re-walked over all eight cells by `tests/what_an_ack_promises.rs`, the two
/// SEPARATE removal cells keep an operation on every record:
///
/// ```text
/// sync  / separate / removal   12 records, 12 with an operation
/// async / separate / removal   12 records, 12 with an operation
/// sync  / batched  / removal    1 record,   0 with an operation (all 12 results state themselves)
/// async / batched  / removal    1 record,   0 with an operation (all 12 results state themselves)
/// ```
///
/// Both BATCHED removal cells answer the same way because the batch path asks
/// `every_result_can_be_found_again`: a removal states itself, so there is nothing to find. The
/// asynchronous one used to write twelve command records instead -- its rule asked whether the
/// BATCH had staged a page, and a removal stages none.
///
/// So "zero" was a count over the upsert half of the matrix read as a count over the whole of
/// it -- which is the same error as the three-column reading recorded on `TS_WAL_DATA_ONLY`
/// above, in the other direction.
///
/// The flip was correct; the comment simply outlived it. A flag comment that states the opposite
/// default from the code is worse than no comment, because the two are indistinguishable from
/// outside: a stale note and a default nobody meant to change read exactly alike.
pub fn wal_outcome_items_enabled() -> bool {
    // Default ON. Recording stopped costing the group-commit coalescing once results travelled
    // through the reserve-only append, and recovery prefers installing them over re-running an
    // operation wherever it has them. The variable now opts OUT.
    !matches!(
        std::env::var("TS_WAL_OUTCOME_ITEMS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn outcome_not_deleted(deleted: &bool) -> bool {
    !*deleted
}
/// The address inside a recorded outcome, without the routing bucket the item already carries.
///
/// The item and its address both state a routing bucket, always the same one, so every recorded
/// outcome said it twice.
///
/// Their `object_id`s look equally redundant and are NOT. For a timestamped kind the item's is
/// derived from (kind, key, COMPONENT) and the page's from (kind, key, None) -- per point against
/// per series -- so restoring one from the other writes the wrong id into the index entry. The
/// bucket-index half of the equivalence gate is what caught that; the field stays.
///
/// The page checksum stays too: the read path verifies it whenever it is present, and a rebuilt
/// index entry without one would quietly stop being integrity-checked.
mod outcome_address_serde {
    use crate::block_store::ElementEntry;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<ElementEntry>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // NOTHING TO TRIM ANY MORE. This used to clear the routing bucket before writing, because
        // the item carries its own and a second copy inside the address was bytes on every record.
        // The address no longer holds one, so the serialized form is the same bytes this produced
        // before -- which is why the trim goes rather than being kept as a no-op.
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ElementEntry>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<ElementEntry>::deserialize(deserializer)
    }
}


/// One index mutation a write produced, stated as a result.
///
/// Field for field this is the identity half of the log item being followed: the object it
/// names, the kind/model it belongs to, its routing bucket, its object id, and where its page
/// ended up. `deleted` covers the other outcome. Nothing here says which command ran, because
/// replay does not need to know -- that is the entire point.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalOutcomeItem {
    #[serde(rename = "k", alias = "kind")]
    pub kind: String,
    #[serde(rename = "o", alias = "object_key")]
    pub object_key: String,
    /// WHICH ELEMENT OF THAT OBJECT, FOR EVERY KEYED KIND THAT HAS NO PAGE TO ASK.
    ///
    /// # IT WAS PUT UP FOR REMOVAL AND THE ANSWER IS NO, ON THREE INDEPENDENT GROUNDS
    ///
    /// The case for removing it was good and half of it was acted on. A container page states its
    /// own elements, so for `hash`, `set`, `zset` and `list` this field was a second copy of
    /// something durable -- and `apply_outcome_item` now reads those four off the page instead,
    /// live and removed, at a measured cost of ZERO block reads. What is left is what the page
    /// cannot answer.
    ///
    /// THE COMPILER WAS ASKED RATHER THAN A GREP, which is this tree's own rule and the only thing
    /// that settles a reader count: the field was deleted and `cargo check --all-targets` produced
    /// 64 diagnostics. NONE of them is a container arm -- that is what the two commits before this
    /// bought -- and what remains is:
    ///
    ///   1. NINE READS IN `engine::lifecycle`, every one of them a TIMESTAMPED kind: `feature`,
    ///      `context_index`, `context_audit`, `context_child`, `context_summary`,
    ///      `context_compression`, and the entity/event/node/embedding arms below them. For these
    ///      the component is a stored key parsed as a `u64`, and
    ///      `container_blocks::ElementKeySpelling::for_kind` answers `None` for every one of them --
    ///      their pages carry NO element-key frame at all. The payload cannot supply what it does
    ///      not spell, so for these kinds this field is the only copy and removing it is data loss.
    ///   2. ITS ABSENCE IS A MEANING, which no removal can preserve. The timestamped arm opens
    ///      "No component on a removal means the whole series went, not one point of it", and
    ///      `wal_proto` encodes exactly that as `object_deleted: item.deleted &&
    ///      item.component.is_none()`. A field whose `None` is a statement cannot be deleted even
    ///      where its `Some` is derivable.
    ///   3. EIGHT READS IN `wal_proto`, where it is not stored as itself: `numeric_component`
    ///      renders it into the `timestamp_ms` and `entry_id` protobuf fields for the kinds whose
    ///      component is a number, and reads it back out of them. Removing the field means removing
    ///      a wire encoding, not a struct member.
    ///
    /// # WHAT WOULD ACTUALLY BUY IT, SO THE NEXT LANE DOES NOT RE-DERIVE THIS
    ///
    /// Giving the timestamped kinds an element-key page frame of their own -- a fifth, sixth and
    /// seventh `ElementKeySpelling`, and a `container_blocks` writer on their append paths. That is
    /// a PAGE-FORMAT change for six or more kinds, not a field deletion, and it has to carry the
    /// absence-is-a-meaning case in (2) across with it. Until then this field is load-bearing for
    /// everything except the four kinds that no longer read it.
    ///
    /// WHAT IT COSTS TO KEEP, MEASURED, so the trade is a number rather than a shrug. On the four
    /// kinds that no longer read it, it is dead weight on the wire --
    /// `durable_outranks_derived::what_resolving_the_element_from_the_block_costs_a_replay` prices it
    /// at 24 B of a 114 B hash record and 85 B of a 182 B set record carrying a 20-byte member,
    /// counting both this field and the carried page's own copy. That is what a later narrowing
    /// would collect, and narrowing is what is available: the field cannot go, and for four kinds
    /// it no longer has to be written.
    ///
    /// # AND THE ROUTE THAT WOULD HAVE BOUGHT THE REMOVAL IS PRICED AND REFUSED
    ///
    /// Blocker (1) above names the thing that would unblock it: give the timestamped kinds an
    /// element-key page frame, so the page answers for them as it does for the four container
    /// kinds. That was costed before it was built, by
    /// `durable_outranks_derived::what_the_element_name_on_a_timestamped_record_is_worth`, and the
    /// measurement refuses it. THREE NUMBERS, each with the denominator it is over:
    ///
    ///   * THE PAGE ALREADY NAMES ITS POINTS, so most of the frame does not need building.
    ///     `packed_blocks::encode_feature_block` writes an eight-byte `timestamp_ms` in front of
    ///     every point, and its own doc says why -- "it is what picks one point out of a page
    ///     holding several". Driven through the real decoder on a 64-point append: ONE page, 64 of
    ///     64 timestamps recovered, 0 outcomes whose object carried several pages. For every kind
    ///     whose component IS the stored key, there is nothing to add.
    ///   * ONLY ONE KIND CARRIES A SECOND TERM. `packed_blocks::timestamped_component` packs an
    ///     identity beside the stored key for `context_event` alone -- `{stored:016x}{id:016x}` --
    ///     and the page holds the timeline key but not the event id. So the frame work reduces to
    ///     ONE kind, at about 8 B per page to put the id in the frame.
    ///   * AND THE SAVING IS AN ORDER OF MAGNITUDE SMALLER THAN THE COMPONENT LOOKS, which is what
    ///     settles it. `wal_proto::numeric_component` stores this field as NUMBERS for all eight
    ///     timestamped kinds -- a `timestamp_ms`, plus an `entry_id` for `context_event` -- so the
    ///     32-character hex component of a `context_event` is never written as 32 characters.
    ///     Measured through the real framer: a feature record is 523 B and 418 B without the field,
    ///     which is 105 B over the 64 outcomes IN THAT RECORD = **1.64 B per outcome**; a
    ///     context_event record is 169 B and 146 B = 23 B.
    ///
    /// SO THE TRADE IS ~8 B PER PAGE AGAINST 1.64 B PER OUTCOME, and for the common case that is a
    /// NET LOSS -- the store would get bigger to remove a field. Refused on the measurement.
    ///
    /// THE GENERAL LESSON, WHICH IS WORTH MORE THAN THIS DECISION: A HEX-SPELLED COMPONENT IS NOT
    /// STORED AS HEX. Pricing a removal from a field's SPELLING rather than from its ENCODER
    /// overstated it by about twenty times here. It is the same error as pricing the index-log row's
    /// element name from its string width when slot 5 had already become a constant nil -- read the
    /// encoder, not the type.
    #[serde(
        rename = "c",
        alias = "component",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub component: Option<String>,
    #[serde(rename = "i", alias = "object_id")]
    pub object_id: u64,
    #[serde(rename = "b", alias = "routing_bucket")]
    pub routing_bucket: u32,
    /// Where the page went. `None` for state that no page backs -- the seen-sets and token
    /// buckets persist with the index snapshot and have no address to name.
    #[serde(
        rename = "a",
        alias = "address",
        default,
        with = "outcome_address_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub address: Option<crate::block_store::ElementEntry>,
    /// The bytes themselves, for an outcome with no page behind it.
    ///
    /// A coverage probe over twelve accepted writes found four that recorded nothing --
    /// BucketTake and SeenCheck because their state has no page, CommonDelete and CommonExpire
    /// because they remove or re-stamp rather than upsert. An item carrying only an address
    /// cannot state those outcomes, which is why the design being followed gives its log item a
    /// `value` beside its `page`, and a `meta_log` flag to tell them apart.
    #[serde(
        rename = "v",
        alias = "value",
        default,
        skip_serializing_if = "Option::is_none",
        with = "outcome_value_serde"
    )]
    pub value: Option<Vec<u8>>,
    /// The deadline this outcome set, if it set one.
    #[serde(
        rename = "x",
        alias = "ttl",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl: Option<u64>,
    /// The object is gone.
    #[serde(
        rename = "d",
        alias = "deleted",
        default,
        skip_serializing_if = "outcome_not_deleted"
    )]
    pub deleted: bool,
    /// This item states metadata rather than a page image -- their `meta_log`.
    #[serde(
        rename = "m",
        alias = "meta",
        default,
        skip_serializing_if = "outcome_not_deleted"
    )]
    pub meta: bool,
}

/// `Option<Vec<u8>>` through the same byte encoding every other payload gets.
mod outcome_value_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => {
                #[derive(Serialize)]
                struct Wrapper<'a>(#[serde(with = "crate::bytes_serde")] &'a Vec<u8>);
                Wrapper(bytes).serialize(serializer)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        #[derive(Deserialize)]
        struct Wrapper(#[serde(with = "crate::bytes_serde")] Vec<u8>);
        Ok(Option::<Wrapper>::deserialize(deserializer)?.map(|wrapper| wrapper.0))
    }
}

/// A page put aside during a write, to be carried in that write's log record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedBlock {
    /// The object the page belongs to, which is what a read has when it comes looking.
    pub object_id: u64,
    /// WHICH ELEMENT OF THAT OBJECT THIS PAGE IS.
    ///
    /// The object id alone cannot answer that. One record carries many pages -- a batch writing
    /// twenty-five fields of one hash writes one record with twenty-five pages -- and the reader
    /// picks one out of it with a `find` over the id. So the id is not merely the registry's key,
    /// it is the record's only discriminator BETWEEN ITS OWN PAGES, and it has had to stay
    /// per-element for that reason alone. Measured on the product's own read: two pages of one key
    /// sharing an id serve 1 of 2 correctly, and the page that loses is handed the FIRST page's
    /// BYTES rather than answering missing.
    ///
    /// The component and not the page's ordinal, on four grounds that are measured elsewhere and
    /// cited rather than re-derived here. `container_block_ordinal` derives the ordinal FROM this
    /// component, so it is a lossy projection of the thing it would replace; #2008 measured that a
    /// delete frees an ordinal for the next insert to reuse ("correct for a position and would be
    /// silent corruption for an identity"); the ordinal is left at 0 past an object's 65,535th
    /// element; and #1996 found the component is the ONLY copy of a member for an element the delta
    /// fold delivers without a durable-map entry. The one fact this file adds: the hot write's
    /// address is built with `block_id: None`, so on the very path this page exists to serve there
    /// is no ordinal on the address to read back.
    ///
    /// `None` means the page IS its whole object -- a string, a control state -- which is what
    /// `None` has always meant for `BlockIndex::component`. Skipped when absent, so a record for a
    /// whole-object page is byte-identical to one written before this field existed.
    #[serde(
        rename = "k",
        alias = "component",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub component: Option<std::sync::Arc<str>>,
    /// The page contents.
    ///
    /// Carried beside the document when that is smaller, and encoded into it otherwise -- the same
    /// choice every other payload gets. A page is the whole point of this record, so it is the
    /// field where the difference between carrying bytes and encoding them shows up most.
    #[serde(with = "crate::bytes_serde")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogRecordMetadata {
    /// Omitted while it is the current version: a record that does not say otherwise is current,
    /// and saying so in every record costs more than the statement is worth.
    #[serde(
        rename = "v",
        alias = "version",
        default = "current_write_ahead_log_format_version",
        skip_serializing_if = "is_current_write_ahead_log_format_version"
    )]
    pub version: u32,
    #[serde(rename = "t", alias = "timestamp_ms")]
    pub timestamp_ms: u64,
    /// Per-item description of the write.
    ///
    /// Every field is derived from the command in the same record, so this is a convenience
    /// rather than a source of truth -- see [`WriteAheadLogItemMetadata::from_command`], which
    /// reconstructs it. New records leave it empty and skip it entirely rather than spend 147
    /// bytes per write on data the record already contains; records written before that still
    /// carry theirs and still load.
    #[serde(
        rename = "i",
        alias = "items",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub items: Vec<WriteAheadLogItemMetadata>,
    // Atomic-batch framing (all three set together, or all absent for a standalone write). A
    // batch of N commands is written as N contiguously-sequenced records sharing one `batch_id`,
    // buffered and made durable by a SINGLE barrier after the last record. `batch_index` is
    // 1-based; the record with `batch_index == batch_size` is the commit marker. Replay drops a
    // trailing batch that is missing its commit marker (a crash between the buffered appends and
    // the barrier), so a partially-persisted batch is never applied -- all-or-nothing. Absent
    // (skipped in JSON) for every non-batch record, so standalone-write WALs are byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<u32>,
}

impl WriteAheadLogRecordMetadata {
    /// Metadata for a record carrying one command, without the per-item description.
    ///
    /// That description is derived entirely from the command this record already carries, and
    /// read by nothing, so writing it costs 147 fsynced bytes per record to say what the record
    /// says twice. `WriteAheadLogItemMetadata::from_command` reconstructs it for any caller that
    /// wants it, and `single_command_with_items` writes it for one that cannot.
    pub fn single_command(command: &Command) -> Self {
        Self::for_command(command, false)
    }

    /// The same, carrying the per-item description, for a consumer that reads records directly
    /// and has not moved to deriving it.
    ///
    /// `TS_WAL_ITEM_METADATA` used to select this for every writer in the process. Nothing set
    /// it, and the cost it added was per write, so restoring the description is a call rather
    /// than an export -- made by whoever knows the consumer that needs it.
    pub fn single_command_with_items(command: &Command) -> Self {
        Self::for_command(command, true)
    }

    fn for_command(command: &Command, with_items: bool) -> Self {
        Self {
            version: WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: current_time_ms(),
            items: if with_items {
                vec![WriteAheadLogItemMetadata::from_command(command)]
            } else {
                Vec::new()
            },
            batch_id: None,
            batch_size: None,
            batch_index: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogItemMetadata {
    pub item_kind: WriteAheadLogItemKind,
    pub model: WriteAheadLogModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "slot_id")]
    pub bucket_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub meta_log: bool,
    #[serde(default)]
    pub block_log: bool,
}

impl WriteAheadLogItemMetadata {
    pub fn from_command(command: &Command) -> Self {
        let object_key = command_object_key(command);
        let bucket_id = object_key.as_deref().map(command_bucket_id);
        let item_kind = command_item_kind(command);
        Self {
            item_kind,
            model: command_model(command),
            object_key,
            bucket_id,
            object_id: None,
            block_id: None,
            ttl_ms: command_ttl_ms(command),
            deleted: matches!(
                command,
                Command::CommonDelete { .. }
                    | Command::StringDelete { .. }
                    | Command::HashDelete { .. }
                    | Command::SetRemove { .. }
                    | Command::FeatureDelete { .. }
            ),
            meta_log: matches!(
                command,
                Command::CommonExpire { .. } | Command::CommonTtl { .. }
            ),
            block_log: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogItemKind {
    Kv,
    Block,
    Ttl,
    DeleteObject,
    Query,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogModel {
    Common,
    String,
    Hash,
    Set,
    Feature,
    Sequence,
    ControlState,
    Context,
    Admin,
    Unknown,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub flushes: u64,
    pub syncs: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
    pub last_flushed_sequence: u64,
    pub persistent_bytes: u64,
    /// Diagnostics: full-file `last_wal_sequence_at` rescans taken on the client append path for
    /// this shard. Without the log's `flat_append()` this increments once per append (O(writes));
    /// with it on the count stays O(1) once the shard's length cache is warm. Read by the phase-1 aging test.
    #[serde(default)]
    pub append_full_scans: u64,
    /// Diagnostics: full-file `last_wal_sequence_at` rescans taken inside `stats()`. The per-write
    /// index-anchor step reads `stats().last_sequence`; without `flat_append()` that rescans on
    /// every write (O(writes)); with it on the engine anchors off `cached_last_sequence` so this
    /// stays flat. Read (via `raw_stats`, which does NOT itself scan) by the phase-1 aging test.
    #[serde(default)]
    pub stats_full_scans: u64,
}

/// A log id that is the FIRST BYTE OF A RECORD.
///
/// A windowed replay resumes by seeking, and a seek is only safe at a position where a record
/// actually starts -- a log id landing mid-record would have the walk decode the tail of one
/// record as the head of another, which on the recovery path is the silent direction: a shard
/// that comes up missing durable writes rather than one that refuses to come up.
///
/// So the position is not a number a caller can make up. The only two ways to hold one are to
/// have been handed it by the walk, which produces it from the first byte of a record it refused
/// for budget, and [`LocalWriteAheadLogStore::replay_start_after_sequence`], which answers a
/// piece's base -- the first byte of that piece's first record -- or zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayPosition(u64);

impl ReplayPosition {
    /// The start of the log, which is where a replay with no durable index begins.
    pub fn at_start_of_log() -> Self {
        Self(0)
    }

    /// The log id, for a caller that wants to report or compare it.
    pub fn log_id(self) -> u64 {
        self.0
    }

    /// Private on purpose: everything that can honestly claim a record starts here lives in
    /// this module.
    fn at_record_start(log_id: u64) -> Self {
        Self(log_id)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// The retain floor actually used, after clamping. Differs from
    /// `retain_from_sequence` when the caller asked to reclaim further than the tail or the
    /// block-retention floor allowed.
    pub effective_retain_from_sequence: u64,
    /// The block-retention floor held this reclaim back: records at or above it may still be
    /// the only copy of a block's bytes.
    pub clamped_by_block_retention: bool,
    /// The durable-index anchor held this reclaim back: the caller asked to drop records that
    /// the durable served index does not yet reflect, so the ask was narrowed to what the index
    /// can actually replace.
    #[serde(default)]
    pub clamped_by_durable_index: bool,
    /// Bytes reclaimed from the head of this shard's log over its lifetime, after this pass.
    /// A record's log id minus this is where it now physically lives.
    pub base_offset: u64,
    /// Bytes rewritten to keep the survivors. Reclaim copies what it keeps, so this -- not
    /// `records_removed` -- is the cost of the pass, and it tracks the RETAINED size.
    pub bytes_copied: u64,
    /// The pass was declined because the copy it required bought too little space. The records
    /// are untouched and a later pass, once the prefix has grown, will take them.
    pub skipped_not_worth_rewrite: bool,
    /// The bytes this pass would have kept contain a closed block's footer, which cannot survive
    /// the copy -- see the site that computes it for why. Reports the CONDITION, not the
    /// decision, so a test can tell "the case was reached" from "the case was handled".
    #[serde(default)]
    pub retained_bytes_carry_a_block_footer: bool,
    /// Whole pieces of the log that went without being copied, because everything in them was
    /// below the floor.
    #[serde(default)]
    pub dropped_segments: usize,
    /// What those pieces held.
    #[serde(default)]
    pub dropped_segment_bytes: u64,
}

/// Evidence that the durable served index for a shard already reflects every WAL record at or
/// below `through_sequence` -- and therefore that those records may be reclaimed.
///
/// A WAL record may only be dropped once the state that supersedes it is on disk. That ordering
/// held here by convention: each reclaim site was expected to have written a manifest, or run a
/// dump, before calling. One site did not. The operator `/gc` RPC took a sequence off the wire
/// and reclaimed straight to it, with no check that anything durable could replace what it was
/// about to delete -- the tail-continuity and block-retention floors were the only things
/// standing in the way, and neither of those knows about the index.
///
/// Making the proof an argument turns that convention into a precondition of the call: a site
/// that wants to reclaim has to name the durable state it is reclaiming against. The clamp is
/// the safety net -- an anchor that understates what is durable costs an under-reclaim, which
/// the next pass picks up, while the alternative costs acked data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableIndexAnchor {
    shard_id: ShardId,
    through_sequence: u64,
}

impl DurableIndexAnchor {
    /// Assert that the shard's durable served index reflects every record at or below
    /// `through_sequence`.
    ///
    /// The caller is making a claim about work it has already completed -- a dump whose base
    /// index was written durably, or a set of bucket-dump manifests covering every live
    /// generation. Mint this from that completed work, never from an intent to do it.
    pub fn proven_durable_through(shard_id: ShardId, through_sequence: u64) -> Self {
        Self {
            shard_id,
            through_sequence,
        }
    }

    /// An anchor that proves nothing and therefore constrains nothing.
    ///
    /// For callers with no durable state to point at: measurement harnesses driving reclaim
    /// directly, and the operator RPC on a shard that has never dumped, where narrowing to a
    /// non-existent frontier would silently turn the endpoint into a no-op. Reclaim through one
    /// of these is exactly as safe as it was before the anchor existed -- which is the point of
    /// naming it, because the call site now says so.
    pub fn unproven(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            through_sequence: u64::MAX,
        }
    }

    /// The highest WAL sequence this anchor vouches for.
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// The shard this anchor speaks for. An anchor proves nothing about any other shard.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogFlushReport {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub last_sequence: u64,
    pub persistent_bytes: u64,
    pub synced: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogAppendReport {
    pub shard_id: ShardId,
    pub requested_sequence: u64,
    pub current_sequence: u64,
    pub appended: bool,
    pub offset: u64,
    pub size: u64,
    pub persistent_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogInfo {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub start_sequence: u64,
    pub current_sequence: u64,
    pub records: usize,
    pub length_bytes: u64,
    pub persistent_length_bytes: u64,
    pub last_flushed_sequence: u64,
    pub format_version: u32,
}

pub const WRITE_AHEAD_LOG_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct LocalWriteAheadLogStore {
    inner: Arc<Mutex<WriteAheadLogInner>>,
    // Group-commit coordinator: serializes WAL fsyncs so many concurrent writers
    // share one durability barrier. Always consulted -- `group_commit_configured` returns true,
    // and `TS_GROUP_COMMIT`, which this comment used to name as its condition, is read by
    // nothing.
    // Writers append their bytes under the `inner` lock, RELEASE it, then coalesce
    // their fsync here -- so a burst of concurrent writes amortizes onto ~1 fsync.
    sync_coord: Arc<Mutex<HashMap<ShardId, GroupCommitState>>>,
    /// Whether an append resolves its sequence from the warm cache. Read on the append path and
    /// on every execute, so it is an atomic beside the lock rather than a field inside it -- the
    /// engine asks without taking the log's lock.
    flat_append: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct GroupCommitState {
    // Highest WAL sequence proven durable (fdatasync'd) for this shard. A caller
    // whose sequence is already <= this returns without issuing its own fsync.
    durable_seq: u64,
    // The WAL file's parent-directory entry has been fsync'd at least once this
    // process lifetime. Appends only grow the file (inode), never the directory,
    // so the per-append dir fsync is redundant after the first.
    dir_synced: bool,
}

pub type WalError = WriteAheadLogError;
pub type WalRecord = WriteAheadLogRecord;
pub type WalStats = WriteAheadLogStats;
pub type WalGcReport = WriteAheadLogGcReport;
pub type LocalWalStore = LocalWriteAheadLogStore;

#[derive(Debug)]
struct WriteAheadLogInner {
    root: PathBuf,
    /// The active piece's path, built once per shard.
    ///
    /// `write_ahead_log_path` is a `format!` and a `join` -- two allocations -- and the append
    /// path asked for the same answer six times a write. The name is stable: a roll seals the old
    /// piece under a numbered name and recreates this one, so the active path outlives any roll.
    ///
    /// An `Arc` so callers can hold it while the rest of this struct stays borrowable; cloning it
    /// is a refcount bump.
    active_path_by_shard: HashMap<ShardId, std::sync::Arc<PathBuf>>,
    /// The append lock file, held open per shard.
    ///
    /// It used to be opened and closed on every append: a `format!`, a `PathBuf`, an open and a
    /// close, for a file whose name never changes. The lock itself is `flock`, still taken and
    /// released per append -- this only stops re-opening the thing being locked.
    ///
    /// Safe to hold open: `flock` guards against OTHER PROCESSES, and threads inside this one are
    /// already serialised by the mutex around this struct.
    append_lock_by_shard: HashMap<ShardId, std::sync::Arc<File>>,
    /// Reused to frame each record, instead of allocating a frame per append.
    ///
    /// The frame is the payload plus about ten bytes, so allocating one per write allocated the
    /// whole record again every time -- four kilobytes an append at a four-kilobyte value. Kept
    /// here so its capacity survives; a steady workload stops allocating for it entirely.
    encode_scratch: Vec<u8>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<std::sync::Arc<crate::scratch::ScratchDirGuard>>,
    stats: WriteAheadLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    /// Bytes appended for each shard since its last durable dump.
    ///
    /// A COUNTER, not a mark against the file's length. The obvious version -- remember the
    /// length at the dump, subtract it later -- is wrong here because reclaim TRUNCATES the log
    /// right after a dump: the file then sits below its own watermark and the subtraction
    /// saturates to zero, so the threshold never fires again. Counting growth is immune to that,
    /// and growth is what the threshold is about.
    undumped_bytes_by_shard: HashMap<ShardId, u64>,
    /// OBJECT MUTATIONS in the undumped log suffix, beside the bytes above.
    ///
    /// Neither quantity the dump budget compares today survives a change in how the writer groups
    /// its objects. The same 2,000 objects of 128 bytes read as 2,000 log records in batches of 1
    /// and 4 in batches of 500 -- a batch is one log record however many objects it carries -- and
    /// as 377,652 undumped bytes against 30,223, because per-record framing dominates a small
    /// write. Measured, at four batch shapes, in
    /// `the_dump_budget_moves_with_batching_in_records_and_not_in_bytes`.
    ///
    /// This one does survive it. A record made of outcomes carries one per object it mutated, and
    /// a record that carries none mutated one object -- so the sum is the number of mutations a
    /// restart must re-apply and the number reclaim cannot drop until a dump covers them. That is
    /// what delaying a dump actually costs, and it is the same number whichever way the writer
    /// grouped the writes.
    undumped_objects_by_shard: HashMap<ShardId, u64>,
    /// Per shard: how far the ACTIVE segment has actually been made durable, and the highest
    /// sequence covered by that barrier.
    ///
    /// `stats` is one struct for the whole store, so its `persistent_bytes` and
    /// `last_flushed_sequence` describe whichever shard synced most recently -- reporting them
    /// for a specific shard attributes another shard's barrier to this one. Durability is a
    /// property of a log, so it is tracked per log.
    durable_active_bytes_by_shard: HashMap<ShardId, u64>,
    durable_sequence_by_shard: HashMap<ShardId, u64>,
    /// Per shard, for the block footer: the offset and sequence of the last record that
    /// STARTED in the block currently being filled. When that block closes, this is what its
    /// footer records, and it is what a reopen reads instead of walking the log.
    block_last_record_by_shard: HashMap<ShardId, (u64, u64)>,
    /// Per shard: whether this log is written in blocks. Decided the first time this process
    /// appends to it and never revisited, because the answer is a property of the bytes already
    /// on disk rather than of the current configuration.
    block_mode_by_shard: HashMap<ShardId, bool>,
    // MANIFEST-CONFORMANCE / phase-1 flat-append cache (`flat_append()`). The WAL file byte length as
    // this process last left it after its own append (or after a full reconcile scan), per shard.
    // On the next append the fast path stats the file: if the on-disk length still equals this,
    // no other writer touched the file since we wrote it (the append lock is cross-process) and --
    // because we only ever append complete framed lines -- there is no torn tail, so the warm
    // `last_sequence_by_shard` is authoritative and the O(records) `last_wal_sequence_at` scan is
    // skipped. Any mismatch (external append, or first touch this process lifetime) falls back to
    // the full scan. Only consulted when `flat_append()`; harmless to maintain when off.
    verified_len_by_shard: HashMap<ShardId, u64>,
    // Lowest sequence whose record may still hold the only copy of a block's bytes -- the dump
    // watermark. A block written into the WAL is addressed by the byte offset of its record and
    // has no copy in a slab until it is dumped, so reclaiming that record destroys the block.
    // GC clamps its retain floor to this. Absent = no shard has WAL-resident blocks, and GC is
    // unconstrained, which is the behaviour when nothing registers a floor.
    block_retention_floor_by_shard: HashMap<ShardId, u64>,
    // Cached (reclaim base, header length) per shard so turning a record's physical offset into
    // the log id that survives reclaim costs no file read on the write path. Filled on first use
    // and refreshed by reclaim -- the only thing that moves the base.
    base_by_shard: HashMap<ShardId, (u64, u64)>,
    // Under TS_WAL_PREALLOCATE: the physical file size this process last grew the file to.
    // Records stop at `verified_len_by_shard`; the file itself ends here. The append fast path
    // compares the on-disk length against THIS (not the record end) to detect another writer,
    // because under preallocation those two are allowed to differ.
    prealloc_physical_by_shard: HashMap<ShardId, u64>,
    // Shards whose piece being written was created without its directory entry being made durable.
    // A roll creates a new file under the same name, so the entry has to reach disk again before
    // any record in that piece is acked -- otherwise the file can vanish and take an acked record
    // with it. The roll does not pay for that barrier itself: it records the debt here and the next
    // durable barrier, which was going to run anyway, settles it.
    dir_sync_owed_by_shard: HashSet<ShardId>,
}

impl LocalWriteAheadLogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(WriteAheadLogInner {
                root,
                active_path_by_shard: HashMap::new(),
                append_lock_by_shard: HashMap::new(),
                encode_scratch: Vec::new(),
                scratch: None,
                stats: WriteAheadLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                undumped_bytes_by_shard: HashMap::new(),
                undumped_objects_by_shard: HashMap::new(),
                durable_active_bytes_by_shard: HashMap::new(),
                block_last_record_by_shard: HashMap::new(),
                block_mode_by_shard: HashMap::new(),
                durable_sequence_by_shard: HashMap::new(),
                verified_len_by_shard: HashMap::new(),
                block_retention_floor_by_shard: HashMap::new(),
                base_by_shard: HashMap::new(),
                prealloc_physical_by_shard: HashMap::new(),
                dir_sync_owed_by_shard: HashSet::new(),
            })),
            sync_coord: Arc::new(Mutex::new(HashMap::new())),
            flat_append: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Whether an append resolves its sequence from the warm in-process cache.
    ///
    /// On, and settable off only by a test. Off, the live client-write append path calls
    /// `last_wal_sequence_at()` on EVERY append -- a full read of this shard's log from offset 0
    /// that decodes every record to find the max sequence, so an ingest costs O(records) per
    /// append and O(n^2) overall. That scan is the dominant cost of the work done under the
    /// engine's shards write lock: longer than the ~3.3 ms fsync and serialized under the same
    /// lock, so concurrent writers never overlap at the fsync barrier and group commit cannot
    /// coalesce.
    ///
    /// On, the cache is trusted whenever the file's on-disk length is still exactly what this
    /// process last left it -- an O(1) `metadata()` stat instead of the O(n) scan. That is safe
    /// because the append lock is cross-process, so any external appender changes the length and
    /// forces the full scan; and because only complete framed records are ever appended, so a
    /// matching length rules out a torn tail. The result is byte-identical to the scanning path.
    ///
    /// The same setting decides whether the engine repeats its promote reconcile scan, which is
    /// the other per-write cost that ages with the store. One switch flattens both, which is why
    /// it lives here rather than in each place that consults it.
    pub fn flat_append(&self) -> bool {
        self.flat_append.load(AtomicOrdering::Relaxed)
    }

    /// Rescan the whole log on every append, as builds before the warm cache did.
    ///
    /// For the test that measures both: it writes to one log with the cache trusted and to
    /// another without, and asserts the scan counts of the second track its write volume while
    /// the first stay flat.
    #[cfg(test)]
    pub(crate) fn rescan_on_every_append_for_test(&self) {
        self.flat_append.store(false, AtomicOrdering::Relaxed);
    }

    pub fn append(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync(shard_id, command, !wal_bulk_relaxed_durability())
    }

    /// Append a WAL record. `sync=true` fsyncs before returning (durable);
    /// `sync=false` writes the record but defers the fsync. The WAL
    /// writer ALWAYS records the entry; EVENT_REPLICATION_SYNC
    /// vs ASYNC_STORAGE only changes whether the commit blocks
    /// The WAL commit runs synchronously vs deferred.
    /// Append, and report the log id the record landed at.
    ///
    /// The log id is the record's position in the log's whole history, so it stays valid across
    /// a reclaim. That is what lets a block carried in this record be addressed by it.
    pub fn append_with_sync_reporting(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, Vec::new(), Vec::new())
    }

    /// Append, carrying pages this write produced, and report the log id it landed at.
    ///
    /// The pages travel in the same record as the command, so one durability barrier covers
    /// both and a read that finds the record finds the page with it.
    pub fn append_with_sync_staged(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_blocks: Vec<StagedBlock>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, staged_blocks, Vec::new())
    }

    /// [`append_with_sync_staged`](Self::append_with_sync_staged), also carrying what the write
    /// DID -- see [`WalOutcomeItem`].
    pub fn append_with_outcomes(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_blocks: Vec<StagedBlock>,
        outcomes: Vec<WalOutcomeItem>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, staged_blocks, outcomes)
    }

    pub fn append_with_sync(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, Vec::new(), Vec::new())
            .map(|(record, _)| record)
    }

    fn append_with_sync_inner(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_blocks: Vec<StagedBlock>,
        outcomes: Vec<WalOutcomeItem>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        // The durable barrier is deferred out of the append critical section (below), so the
        // byte-append records with sync=false and the fsync is coalesced across concurrent
        // writers. Every acked write is still durable before its ack returns; only the fsync is
        // shared.
        let group = sync;
        let record;
        let next_sequence;
        let log_id;
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            // Acquiring the append lock creates the directory the first time it opens the lock
            // file; doing it again on every append was a syscall to learn something unchanged.
            let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
            ensure_active_wal_segment(&mut inner, shard_id)?;
            let (last_sequence, on_disk_len) =
                resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
            inner.last_sequence_by_shard.insert(shard_id, last_sequence);
            let seq = last_sequence.saturating_add(1);
            let rec = WriteAheadLogRecord {
                shard_id,
                sequence: seq,
                metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
                // Durable AND in the block store: the two conditions under which installing an
                // address is enough on its own.
                // A carried block is findable after recovery now: replay registers the blocks
                // of every record it replays, which is the one thing the rule below was waiting
                // for. So a record carrying its own blocks can state results and drop the
                // operation, the same as a synchronous write whose blocks are already durable.
                //
                // What still cannot: an ASYNCHRONOUS write with nothing carried. Its result names
                // an address in the block store that a crash may leave unwritten, and no amount
                // of registering helps a block that was never stored.
                command: record_command(command, &outcomes, sync || !staged_blocks.is_empty()),
                staged_blocks,
                outcomes,
            };
            let report = append_record_locked(&mut inner, &rec, sync && !group, Some(on_disk_len))?;
            inner.stats.last_sequence = report.current_sequence;
            // Where the record landed, in the addressing that survives reclaim -- taken BEFORE any
            // roll. The record is in the piece being written now; a roll starts a new piece with a
            // new starting log id, and reading that afterwards would address this record as though
            // it were in the piece that comes next.
            let (base, header_len) = cached_wal_base(&mut inner, shard_id)?;
            log_id = base.saturating_add(report.offset.saturating_sub(header_len));
            // Roll after the record is written, so the piece being sealed is whole.
            let rolled = roll_wal_segment_if_due(&mut inner, shard_id, Some(report.persistent_bytes))?;
            // Record the file length we just left behind so the next append's fast path can
            // confirm no other writer touched the file (O(1) stat) and skip the full scan.
            if !rolled {
                // After a roll the piece being written is new and its length was recorded by the
                // roll; the length this record left behind belongs to the piece just sealed.
                inner
                    .verified_len_by_shard
                    .insert(shard_id, report.persistent_bytes);
            }
            // last_flushed_sequence is advanced by append_record_locked ONLY when the record was
            // actually fsynced (sync=true, non-group). An unconditional overwrite here reported an
            // async / bulk-mode (unsynced) record as durable -- overstating durability, a latent
            // trap for any future reclaim/ack gate that reads it. The group path advances it below
            // after the coalesced barrier actually reaches disk.
            inner.last_sequence_by_shard.insert(shard_id, seq);
            record = rec;
            next_sequence = seq;
            // `inner` and `_append_lock` are released here so a concurrent writer can
            // append while this writer's group-commit fsync is in flight (the fsync
            // duration is the natural batching window).
        }
        if group {
            self.group_commit_sync(shard_id, next_sequence)?;
        }
        Ok((record, log_id))
    }

    /// Phase 1 of a two-phase durable commit: append the record bytes and RESERVE its WAL
    /// sequence WITHOUT taking the durable barrier. The record is written with sync=false
    /// (bytes buffered, sequence assigned + `last_sequence_by_shard` advanced) and the
    /// returned `WriteAheadLogRecord.sequence` is then passed to `commit_barrier` AFTER the
    /// caller has released its own state lock, so the fsync coalesces across concurrent
    /// writers (see `group_commit_sync`). Mirrors the byte-append half of
    /// `append_with_sync`'s group branch, minus the in-line barrier call. The caller MUST
    /// await `commit_barrier(shard_id, record.sequence)` before acking a synchronous write.
    /// Append without the durable barrier, carrying what the write recorded.
    ///
    /// Outcomes were kept off this path on the assumption that anything a record must CARRY forces
    /// the staged branch. That is true of staged pages, whose addresses are back-patched once the
    /// log id exists. It is not true of outcomes: their addresses are already resolved by the time
    /// they are staged, so they travel in the record like any other field -- and keeping them out
    /// cost every write its place in the group-commit queue for no durability reason.
    pub fn append_for_group_commit(
        &self,
        shard_id: ShardId,
        command: Command,
        outcomes: Vec<WalOutcomeItem>,
        staged_blocks: Vec<StagedBlock>,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        // Acquiring the append lock creates the directory the first time it opens the lock
        // file; doing it again on every append was a syscall to learn something unchanged.
        let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
        // Start a piece if a crash left none, exactly as the single-record path does. Without it
        // the record creates the file with no base header, so it reads as starting at log id zero
        // -- an address the sealed pieces already own.
        ensure_active_wal_segment(&mut inner, shard_id)?;
        let (last_sequence, _on_disk_len) = resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
        inner.last_sequence_by_shard.insert(shard_id, last_sequence);
        let seq = last_sequence.saturating_add(1);
        let rec = WriteAheadLogRecord {
            shard_id,
            sequence: seq,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            // This path coalesces the fsync of a SYNCHRONOUS write. It used to assert that the
            // blocks behind these results were therefore durable by the time the barrier returned,
            // and drop the operation on that basis. They are not: the barrier this record waits on
            // is the LOG's, and the block fsync is deferred past it, so a power cut can leave these
            // results naming blocks that were never written. That is what makes a log carrying only
            // results unable to rebuild what it acked.
            //
            // So this path asks the same question every other one does -- can these blocks be found
            // again? -- and answers it from what the record actually carries.
            command: record_command(command, &outcomes, !staged_blocks.is_empty()),
            staged_blocks,
            outcomes,
        };
        // sync=false: write the bytes, defer the fdatasync to `commit_barrier`. Same as the
        // group branch of append_with_sync. `last_flushed_sequence` is NOT advanced here (the
        // record is not yet durable); the coalesced barrier advances it once it reaches disk.
        let report = append_record_locked(&mut inner, &rec, false, None)?;
        inner.stats.last_sequence = report.current_sequence;
        // Roll after the record lands, as the single-record path does. This is only safe because
        // sealing a piece now fsyncs its contents: the barrier this record is waiting for opens
        // the piece being WRITTEN, which after a roll is the new one, so without that fsync the
        // record would be sealed away with no barrier covering it.
        let rolled = roll_wal_segment_if_due(&mut inner, shard_id, Some(report.persistent_bytes))?;
        inner.last_sequence_by_shard.insert(shard_id, seq);
        if !rolled {
            // After a roll the piece being written is new and its length was recorded by the roll;
            // the length this record left behind belongs to the piece just sealed.
            inner
                .verified_len_by_shard
                .insert(shard_id, report.persistent_bytes);
        }
        // `inner` + `_append_lock` drop here so a concurrent writer can append while this
        // writer's (later, lock-released) group-commit fsync is in flight.
        Ok(rec)
    }

    /// Phase 2 of a two-phase durable commit: the coalesced durable barrier for a sequence
    /// reserved by `append_for_group_commit`. Returns once every record up to
    /// `required_sequence` is fdatasync'd (issuing at most one shared fsync per group; returns
    /// immediately if an earlier writer's barrier already covered `required_sequence`). Public
    /// wrapper over `group_commit_sync` so the engine can run the barrier AFTER releasing its
    /// `shards` write lock.
    pub fn commit_barrier(
        &self,
        shard_id: ShardId,
        required_sequence: u64,
    ) -> Result<(), WriteAheadLogError> {
        self.group_commit_sync(shard_id, required_sequence)
    }

    /// Append a group of commands as ONE crash-atomic batch: N contiguously-sequenced records
    /// sharing a single `batch_id`, buffered, then made durable by a SINGLE barrier after the
    /// last record (the commit marker). Either the whole batch is durable (barrier completed) or,
    /// on a crash before the barrier, the trailing partial batch is dropped on replay -- so a
    /// retry never double-applies a durable prefix of a non-idempotent / time-unspecified command
    /// (e.g. FeatureAppend occur_time=0). `sync=false` skips the barrier (async / bulk mode: the
    /// whole batch is buffered and lost-or-kept together, still all-or-nothing).
    ///
    /// Records are assigned a contiguous sequence block under a single `inner`-lock hold, so no
    /// other writer can interleave a record into the middle of the batch (the engine also
    /// serializes writes through its shard lock). A single command takes the standalone
    /// `append_with_sync` path (no batch framing, byte-identical WAL).
    /// Append a whole batch as ONE record carrying every item it produced.
    ///
    /// A batch has always been crash-atomic; what changed is how. Written as N records sharing a
    /// batch id, atomicity is bookkeeping: each record carries the id, the size and its index,
    /// the last one is the commit marker, and replay drops a trailing group whose marker never
    /// arrived. Written as one record it is atomic by construction -- a torn write leaves an
    /// incomplete frame, and an incomplete frame was never a record.
    ///
    /// So the three batch fields go unused, the commit marker stops being a concept, and the
    /// per-record cost is paid once instead of N times. Measured on eight items: 1015 bytes
    /// against 1224, and the eight barriers were already one.
    ///
    /// The caller decides whether this is allowed: it needs every item's block to be findable
    /// after a crash, which is the same rule a single write follows.
    pub fn append_batch_as_one_record(
        &self,
        shard_id: ShardId,
        outcomes: Vec<WalOutcomeItem>,
        staged_blocks: Vec<StagedBlock>,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
        // Start a piece if a crash left none, exactly as every other append entry point does.
        // Without it the record creates the file with no base header, so it reads as starting at
        // log id ZERO -- an address the sealed pieces already own, and two records answering to
        // one log id resolve to whichever the walk reaches first.
        //
        // This path could skip it for as long as it never rolled: a log that is always one piece
        // has no sealed pieces to collide with, so zero was always free. The roll below is what
        // makes sealed pieces exist here, and it is what makes this line load-bearing. The two
        // belong in the same change for that reason: adding the roll alone converts a crash
        // between the rename and the next batch into a duplicated address.
        ensure_active_wal_segment(&mut inner, shard_id)?;
        let (last_sequence, _on_disk_len) = resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
        let seq = last_sequence.saturating_add(1);
        let record = WriteAheadLogRecord {
            shard_id,
            sequence: seq,
            // No command: the items say what the batch did. No batch id, size or index either --
            // one record needs none of them to be recovered as a unit.
            command: None,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(
                &Command::StringGet {
                    key: String::new(),
                },
            )),
            staged_blocks,
            outcomes,
        };
        let report = append_record_locked(&mut inner, &record, sync, None)?;
        inner.stats.last_sequence = report.current_sequence;
        inner.last_sequence_by_shard.insert(shard_id, seq);
        // Roll AFTER the record, never before it and never inside it. A batch written this way
        // IS one record -- that is the whole point of the path -- so there is no inside to roll
        // in: the frame is written by one call and a torn frame was never a record. Rolling
        // BEFORE would be safe for atomicity too and is still wrong, because the decision it
        // takes is whether the piece the LAST record filled is full, and asking that before
        // this record lands seals a piece one record early and leaves the threshold measuring
        // the wrong thing.
        //
        // Until now this path asked nothing. It is the path the engine takes for every batch
        // that produced outcomes -- `wal_data_only_enabled` defaults ON -- so segmentation, and
        // with it reclaim-by-unlink and every walk that decides what to read from a piece's
        // NAME, stopped applying to the writer that writes the most. Measured: 200,000 records
        // left one 32,768,000-byte piece where rolling makes about 125.
        let rolled = roll_wal_segment_if_due(&mut inner, shard_id, Some(report.persistent_bytes))?;
        if !rolled {
            // After a roll the piece being written is new and its length was recorded by the
            // roll; the length this record left behind belongs to the piece just sealed.
            inner
                .verified_len_by_shard
                .insert(shard_id, report.persistent_bytes);
        }
        Ok(record)
    }

    pub fn append_batch_atomic(
        &self,
        shard_id: ShardId,
        commands: Vec<Command>,
        sync: bool,
    ) -> Result<Vec<WriteAheadLogRecord>, WriteAheadLogError> {
        if commands.len() <= 1 {
            let mut records = Vec::with_capacity(commands.len());
            if let Some(command) = commands.into_iter().next() {
                records.push(self.append_with_sync(shard_id, command, sync)?);
            }
            return Ok(records);
        }
        let batch_id = next_batch_id();
        let batch_size = commands.len() as u32;
        let mut records = Vec::with_capacity(commands.len());
        let last_sequence;
        let mut batch_end: Option<u64> = None;
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            // Acquiring the append lock creates the directory the first time it opens the lock
            // file; doing it again on every append was a syscall to learn something unchanged.
            let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
            // Start a piece if a crash left none. Without this the first record of the batch
            // creates the file with no base header, so it reads as starting at log id zero -- an
            // address the sealed pieces already own. The single-record path has always done this;
            // the batch path did not, and nothing about a batch makes it safe to skip.
            ensure_active_wal_segment(&mut inner, shard_id)?;
            let (disk_last_sequence, _) = last_wal_sequence_at(&inner.root, shard_id)?;
            let cached_last_sequence = inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or_default();
            let mut seq = cached_last_sequence.max(disk_last_sequence);
            // ONE open for the whole batch. Each record used to open, write, stat and close the
            // piece for itself -- 4N syscalls against the single barrier below, which is the one
            // thing a batch exists to amortise. Safe to hold across the loop because the append
            // lock and the `inner` lock are both held throughout and the roll happens AFTER the
            // batch, so nothing can seal and rename the piece underneath this handle.
            let batch_path = active_wal_path(&mut inner, shard_id);
            let prealloc = wal_preallocate_enabled();
            #[cfg(test)]
            WAL_FILE_OPENS.with(|opens| opens.set(opens.get() + 1));
            let mut batch_physical: u64 = 0;
            let mut batch_file = if prealloc {
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(batch_path.as_path())?
            } else {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(batch_path.as_path())?
            };
            for (index, command) in commands.into_iter().enumerate() {
                seq = seq.saturating_add(1);
                let mut metadata = WriteAheadLogRecordMetadata::single_command(&command);
                metadata.batch_id = Some(batch_id);
                metadata.batch_size = Some(batch_size);
                metadata.batch_index = Some(index as u32 + 1);
                let rec = WriteAheadLogRecord {
                    shard_id,
                    sequence: seq,
                    metadata: Some(metadata),
                    // No results on this path, so the operation is what replay has.
                    command: Some(command),
                    staged_blocks: Vec::new(),
                    outcomes: Vec::new(),
                };
                // Buffer every record (sync=false); the single durability barrier below covers
                // the whole batch. append_record_locked keeps last_flushed_sequence honest -- it
                // only advances on an actual fsync, which happens once, after the loop.
                let report = append_record_locked_on(
                    &mut inner,
                    &rec,
                    false,
                    None,
                    Some((&mut batch_file, &mut batch_physical)),
                )?;
                inner.stats.last_sequence = report.current_sequence;
                batch_end = Some(report.persistent_bytes);
                records.push(rec);
            }
            // Closed before the roll below, which may seal and rename this very piece.
            drop(batch_file);
            inner.last_sequence_by_shard.insert(shard_id, seq);
            last_sequence = seq;
            // Roll AFTER the batch, never inside it: rolling mid-batch would split one
            // crash-atomic group across two pieces for nothing, and taken here the batch stays
            // contiguous while the next one starts in the new piece. Without this a batch appends
            // into the piece being written without ever asking whether it is full, so segmentation
            // stops applying to the path that writes the most -- and reclaim cannot unlink the
            // piece being written, so the unlinking stops working with it.
            roll_wal_segment_if_due(&mut inner, shard_id, batch_end)?;
        }
        if sync {
            // One coalesced fdatasync makes the entire buffered batch durable. Reusing the
            // group-commit barrier keeps the durable-watermark bookkeeping consistent.
            self.group_commit_sync(shard_id, last_sequence)?;
        }
        Ok(records)
    }

    /// Coalesced WAL durability barrier (group commit). Ensures every byte appended for
    /// `shard_id` up to at least `required_sequence` is `fdatasync`'d to disk before
    /// returning. Concurrent callers serialize on `sync_coord`; the first to run fsyncs
    /// the file -- which flushes ALL pending appends, not just its own -- records the
    /// durable watermark, and any caller whose sequence is already covered returns
    /// without a second barrier. A burst of N concurrent writes therefore shares ~1
    /// fsync instead of paying N. Correctness: every appender writes its bytes under the
    /// `inner` lock and releases it before calling here, so the snapshotted high-water
    /// sequence names only bytes already in the page cache; the fsync makes exactly those
    /// durable, and the watermark is never advanced past them.
    fn group_commit_sync(
        &self,
        shard_id: ShardId,
        required_sequence: u64,
    ) -> Result<(), WriteAheadLogError> {
        let mut coord = self.sync_coord.lock().expect("wal sync coordinator poisoned");
        let entry = coord.entry(shard_id).or_default();
        if entry.durable_seq >= required_sequence {
            return Ok(());
        }
        let (path, snapshot, record_end) = {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            // The CACHED name, not a rebuilt one. `write_ahead_log_path` decides which of the
            // two piece names a store uses by asking whether each exists, so rebuilding it here
            // spent a `statx` per barrier re-deriving a name that cannot move while the log is
            // open -- a roll seals the old piece under a NUMBERED name and recreates this one.
            //
            // It is also the safer of the two questions. The record was written to
            // `active_wal_path`; a barrier has to cover the file the record went INTO, not
            // whatever the name resolves to by the time the barrier runs. Taking the same
            // cached answer the writer took makes those the same file by construction instead
            // of by coincidence.
            let path = active_wal_path(&mut inner, shard_id);
            let snapshot = inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(required_sequence)
                .max(required_sequence);
            // The byte watermark, snapshotted HERE beside the sequence and for the same reason
            // this function's own contract gives for the sequence: every appender wrote its
            // bytes under this lock and released it before calling, so what is in the file now
            // is exactly what the barrier below makes durable. Read AFTER the fsync instead --
            // which is where it used to be read -- it can also pick up an append that the
            // barrier never covered.
            //
            // And the RECORD END, not the file's length. See `note_durable_record_end`.
            // `verified_len_by_shard` is where the append primitive left the records, so this
            // costs no syscall at all; the length it replaces cost a `statx` per barrier.
            // Absent means this process has appended nothing to this shard and so has no bytes
            // of its own to claim durable, which is what zero says.
            let record_end = inner
                .verified_len_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(0);
            (path, snapshot, record_end)
        };
        // Asked by OPENING, not by an `exists()` before it. The open happens either way and
        // answers the same question in the same syscall, with no window between two answers for
        // the file to appear or vanish in. A shard with no file has nothing unsynced, which is
        // what the early return says.
        let file = match OpenOptions::new().read(true).write(true).open(path.as_path()) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                entry.durable_seq = entry.durable_seq.max(snapshot);
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };
        crate::durability_metrics::record_barrier("engine_wal_group_commit");
        file.sync_data()?;
        let owed = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.dir_sync_owed_by_shard.contains(&shard_id)
        };
        if !entry.dir_synced || owed {
            // The first durable append for this shard this process lifetime makes the directory
            // entry durable once, and afterwards nothing touches it -- until a roll creates a new
            // file under the same name, which is what `owed` reports.
            sync_parent_dir(path.as_path())?;
            entry.dir_synced = true;
            self.inner
                .lock()
                .expect("write-ahead log lock poisoned")
                .dir_sync_owed_by_shard
                .remove(&shard_id);
        }
        entry.durable_seq = entry.durable_seq.max(snapshot);
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.flushes += 1;
            inner.stats.syncs += 1;
            if snapshot > inner.stats.last_flushed_sequence {
                inner.stats.last_flushed_sequence = snapshot;
            }
            // The barrier above covered everything written to this shard's active segment, so
            // its records are durable as far as the end snapshotted before it. Recorded per
            // shard for the same reason the flush path does it.
            note_durable_record_end(&mut inner, shard_id, record_end, true);
            let sequence_entry = inner.durable_sequence_by_shard.entry(shard_id).or_default();
            *sequence_entry = (*sequence_entry).max(snapshot);
        }
        Ok(())
    }

    pub fn append_replayed_record(
        &self,
        record: WriteAheadLogRecord,
    ) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        // Start a piece if a crash left none, before anything reads where the log ends. Replay
        // writes a record whose sequence is already decided, so creating the file without a base
        // header here would give it an address the sealed pieces already own -- and a replayed
        // record is exactly the one nobody would think to re-check. No roll: replay is
        // reconstructing a log that already exists, and choosing new piece boundaries part-way
        // through that is a separate decision.
        ensure_active_wal_segment(&mut inner, record.shard_id)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&record.shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let (sequence, _) = last_wal_sequence_at(&inner.root, record.shard_id)?;
                inner
                    .last_sequence_by_shard
                    .insert(record.shard_id, sequence);
                sequence
            }
        };
        if record.sequence <= last_sequence {
            // Where the RECORDS end. Both fields here are about records -- `offset` is where the
            // next record would go and `persistent_bytes` is a durability figure -- and both
            // used to be the file's LENGTH, asked for twice, in two `statx` calls for one
            // number. Under preallocation that length is the reservation: the offset then points
            // past the records into the zeros, and the durability figure overstates. See
            // `note_durable_record_end`.
            let record_end = active_record_end(&mut inner, record.shard_id);
            return Ok(WriteAheadLogAppendReport {
                shard_id: record.shard_id,
                requested_sequence: record.sequence,
                current_sequence: last_sequence,
                appended: false,
                offset: record_end,
                size: 0,
                persistent_bytes: record_end,
            });
        }
        let report = append_record_locked(&mut inner, &record, true, None)?;
        inner
            .last_sequence_by_shard
            .insert(record.shard_id, record.sequence);
        inner.stats.last_sequence = record.sequence;
        inner.stats.last_flushed_sequence = record.sequence;
        Ok(report)
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        // `offset` is a log id: a position in the log's whole history, not in whichever file
        // happens to hold it. That is the only position that means anything once the log is in
        // pieces, and unlike a file offset it survives reclaim.
        let Some((path, physical)) = locate_log_id(&inner.root, shard_id, offset)? else {
            return Ok(Vec::new());
        };
        // The records may stop before the file does: under preallocation the active piece ends
        // in a zeros reservation, which is room, not log content, and must not be served to a
        // caller reading the stream. Sealed pieces end exactly at their records, so the clamp
        // is a no-op there.
        let (_, record_end) = last_wal_sequence_in(&path)?;
        let size = size.min(record_end.saturating_sub(physical));
        let bytes = read_at(&path, physical, size)?;
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
    ) -> Result<Vec<(u64, Vec<u8>)>, WriteAheadLogError> {
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
    ) -> Result<(Vec<(u64, Vec<u8>)>, bool), WriteAheadLogError> {
        self.scan_collect(
            shard_id,
            start_offset,
            end_offset,
            max_bytes,
            true,
            false,
            |log_id, line, _| Some((log_id, line)),
        )
        .map(|(records, truncated, _resume_at)| (records, truncated))
    }

    /// The records in the window, decoded once.
    ///
    /// `scan_bounded` returns the bytes, and every caller that wants records then decodes them --
    /// while this walk has already decoded each one to verify it. Recovery took that bill twice.
    /// This hands back what the verification produced, so the second decode does not happen.
    /// A blank line carries no record and is not returned.
    pub fn scan_decoded(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<(Vec<(u64, WriteAheadLogRecord)>, bool), WriteAheadLogError> {
        self.scan_collect(
            shard_id,
            start_offset,
            end_offset,
            max_bytes,
            true,
            false,
            |log_id, _line, decoded| decoded.map(|record| (log_id, record)),
        )
        .map(|(records, truncated, _resume_at)| (records, truncated))
    }

    /// One bounded window of decoded records, and where to resume reading.
    ///
    /// `scan_decoded` says whether it stopped early; this also says WHERE, so a caller replaying a
    /// long log can walk it in bounded pieces instead of holding all of it at once. Pass
    /// `verify_tail` on the FIRST window only -- see `scan_collect`.
    ///
    /// The position is a [`ReplayPosition`] rather than a log id, and that is what lets the walk
    /// SEEK to it. A log id a caller made up could sit in the middle of a record, and seeking to
    /// the middle of a record on the recovery path is how a restore starts decoding rubbish; a
    /// `ReplayPosition` can only have come from this walk or from
    /// [`Self::replay_start_after_sequence`], and both of those are the first byte of a record.
    pub fn scan_decoded_window(
        &self,
        shard_id: ShardId,
        from: ReplayPosition,
        max_bytes: u64,
        verify_tail: bool,
    ) -> Result<(Vec<(u64, WriteAheadLogRecord)>, bool, ReplayPosition), WriteAheadLogError> {
        self.scan_collect(
            shard_id,
            from.log_id(),
            u64::MAX,
            max_bytes,
            verify_tail,
            true,
            |log_id, _line, decoded| decoded.map(|record| (log_id, record)),
        )
        .map(|(records, truncated, resume_at)| {
            (records, truncated, ReplayPosition::at_record_start(resume_at))
        })
    }

    /// Where a replay that already holds everything up to `sequence` starts reading.
    ///
    /// The same answer as [`Self::log_id_after_sequence`], carried in the type that says what it
    /// is: a piece's base is the first byte of that piece's first record, and zero is the first
    /// byte of the log's.
    pub fn replay_start_after_sequence(
        &self,
        shard_id: ShardId,
        sequence: u64,
    ) -> Result<ReplayPosition, WriteAheadLogError> {
        self.log_id_after_sequence(shard_id, sequence)
            .map(ReplayPosition::at_record_start)
    }

    /// How many records the log holds, without building them.
    ///
    /// The caller that wanted this asked `scan(.., u64::MAX, u64::MAX)` and took `.len()` of the
    /// result -- so it read every record of the whole log into a vector, to learn how many there
    /// were. On a shard whose log has not been reclaimed that is the entire write-ahead log in
    /// memory, for a number.
    ///
    /// This is the same walk, through the same `scan_collect`, projecting to `()` instead of to
    /// the bytes. A zero-sized element costs nothing per record, so peak memory is one record
    /// rather than all of them, and the count cannot drift from what a scan would have returned
    /// because it IS the scan.
    ///
    /// The tail is not verified. A count is a diagnostic, and a torn tail is the recovery path's
    /// business; refusing to count because the last record is half-written would make a
    /// diagnostic fail exactly when it is most wanted.
    pub fn record_count(&self, shard_id: ShardId) -> Result<usize, WriteAheadLogError> {
        self.scan_collect(shard_id, 0, u64::MAX, u64::MAX, false, false, |_, _, _| {
            Some(())
        })
            .map(|(records, _truncated, _resume_at)| records.len())
    }

    /// The one walk both scans share, so they cannot drift about what a window contains.
    ///
    /// `start_record_aligned` says that `start_offset` is the FIRST BYTE OF A RECORD, which lets
    /// the walk seek to it instead of reading its way there. Only [`Self::scan_decoded_window`]
    /// passes it, and only because a [`ReplayPosition`] cannot be built any other way. The
    /// public scans pass `false` and keep exactly the semantics they have always had: a
    /// `start_offset` that lands in the middle of a record yields the next whole record after
    /// it, because the walk reads its way there and compares.
    fn scan_collect<T>(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
        verify_tail: bool,
        start_record_aligned: bool,
        mut take: impl FnMut(u64, Vec<u8>, Option<WriteAheadLogRecord>) -> Option<T>,
    ) -> Result<(Vec<T>, bool, u64), WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let segments = wal_segment_pieces(&inner.root, shard_id);
        // No file at all is "nothing to scan", not an error. Distinguishing it here lets recovery
        // treat a missing log as "nothing to replay" while still surfacing a genuine decode failure
        // (corruption) as data loss (see engine::lifecycle replay).
        //
        // Only the piece being WRITTEN can be missing here: the sealed ones came out of a
        // directory listing a moment ago. This used to `exists()` the whole list, which is a stat
        // per piece per window -- the windows and the pieces both grow with the log, so that
        // product is the shape this walk was quadratic in. A sealed piece reclaimed between the
        // listing and the read still costs nothing: `read_wal_base` answers `(0, 0)` for an absent
        // file and the length test in the loop steps over it, which is exactly what the filter
        // used to do. See `read_wal_base`.
        if segments.len() == 1 && !segments[0].path.exists() {
            inner.stats.scans += 1;
            return Ok((Vec::new(), false, start_offset));
        }
        // Finding the log's end also REPAIRS a torn tail, so it has to happen -- but it costs the
        // whole file rather than the window, and a caller walking the log in pieces that repeated
        // it per piece would trade a bounded read for a quadratic one. The first piece verifies;
        // the continuations of that same walk do not.
        if verify_tail {
            let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        }
        let mut total = 0;
        let mut truncated = false;
        // Where a caller that ran out of budget resumes. Only meaningful when `truncated`.
        let mut resume_at = start_offset;
        let mut records: Vec<T> = Vec::new();
        'segments: for index in 0..segments.len() {
            let path = &segments[index].path;
            // BEHIND THE WINDOW, DECIDED FROM THE NEXT PIECE'S NAME.
            //
            // A sealed piece is named for the log id its contents start at, so the piece AFTER
            // this one already says where this one stops: pieces never overlap in log-id space --
            // a duplicated log id would resolve to two different records -- so this piece ends at
            // or before the next one's start. If that start is already at or below the window,
            // this piece holds nothing the caller asked for and does not have to be opened at all.
            //
            // This is the whole of the fix. The walk always resumed its RECORDS -- each window
            // picks up exactly where the last stopped -- and never resumed its PIECE LIST: every
            // window restarted the piece walk at log id zero and re-read the header of every piece
            // it had already stepped over, to re-learn a number the piece's name had been carrying
            // all along. Windows and pieces both grow with the log, so their product grew with its
            // SQUARE.
            //
            // Nothing here decides what is REPLAYED. A skipped piece ends at or before
            // `start_offset`, and every record at or after `start_offset` is still read, decoded
            // and handed back by the loop below; the last piece (the one being written) has no
            // number in its name, so the piece before it is never skipped this way either.
            if let Some(next_start) = segments.get(index + 1).and_then(|piece| piece.starts_at) {
                if next_start <= start_offset {
                    #[cfg(test)]
                    WAL_PIECES_SKIPPED_BY_NAME.with(|skipped| skipped.set(skipped.get() + 1));
                    continue;
                }
            }
            // Each piece says where in the log's history its contents begin, so a record's position
            // is that plus how far into the piece it sits. Positions are log ids: an offset into
            // one file would mean nothing to a caller once there is more than one, and would not
            // survive reclaim.
            let (base, header_len) = read_wal_base(path)?;
            // A piece that ends before the window starts holds nothing the caller asked for. Its
            // start plus its length says where it ends, so skipping it costs a stat rather than a
            // read of every line in it. This is what makes a windowed scan cost the window: without
            // it the loop below still reads the whole log and merely declines to return most of it.
            // Also what skips a piece reclaimed since the listing above: an absent piece reads
            // as base zero with no length, so this says it ends before any window and the loop
            // moves on rather than opening a file that is gone. See `read_wal_base`.
            let piece_len = path.metadata().map(|meta| meta.len()).unwrap_or(0);
            if base.saturating_add(piece_len.saturating_sub(header_len)) <= start_offset {
                continue;
            }
            #[cfg(test)]
            WAL_PIECE_BODY_READS.with(|reads| reads.set(reads.get() + 1));
            // WHERE IN THE PIECE THE WINDOW STARTS, COMPUTED RATHER THAN WALKED TO.
            //
            // A log id is a byte position in the log's whole history and this piece's contents
            // begin at `base`, so the byte `start_offset` names sits at
            // `header_len + (start_offset - base)`. That is not an approximation: it is the
            // identity the loop below maintains for itself on every iteration, and the one
            // `locate_log_id` already resolves a random-access address with.
            //
            // Opening at the header and reading forward until the records reached the window is
            // what made a WINDOWED replay quadratic. #1917 stopped a window re-reading the
            // HEADER of every piece behind it by deciding from the piece's name; this is the
            // piece's CONTENTS, re-read window after window, which that change does not touch
            // and which a one-piece log -- the shape a rolling log has until it first seals --
            // shows in its purest form: every window opened the single piece at its header and
            // read, decoded and discarded everything before its own start. Over W windows the
            // walk read the window's own bytes W times, once for each window after it, so a log
            // of L bytes cost about L*(L + window)/(2 * window) rather than L.
            //
            // Nothing here changes WHICH records are replayed. The seek lands on the first byte
            // of the record at `start_offset`, and the `log_id < start_offset` test below still
            // stands for every position the caller did not name; it is simply never reached for
            // this piece, because the walk no longer starts behind the window. Only a caller
            // holding a [`ReplayPosition`] can ask for this, and the only two ways to hold one
            // are to have been handed it by this walk or to have taken it from
            // `replay_start_after_sequence` -- both of which are the first byte of a record.
            let seek_to = if start_record_aligned && start_offset > base {
                header_len.saturating_add(start_offset - base)
            } else {
                header_len
            };
            let mut tally = WalReadTally::start();
            let mut file = open_wal_read(path, &mut tally)?;
            file.seek(SeekFrom::Start(seek_to))?;
            let mut reader = BufReader::new(file);
            let mut log_id = base.saturating_add(seek_to.saturating_sub(header_len));
            loop {
                // Step over a block footer this lands on. The zero-run check below only catches a
                // boundary that has padding in front of its footer; a block whose records end
                // flush against the footer has none, and the footer would then be read as a
                // record -- its first byte is not a frame marker, so the reader falls to
                // newline-delimited mode and hands back a fragment. `last_wal_sequence_forward`
                // takes this same first step; the two walkers read one format and must agree.
                let at = header_len.saturating_add(log_id.saturating_sub(base));
                let stepped = skip_block_footer_if_due(&mut reader, at, header_len)?;
                if stepped != at {
                    // Log ids are byte positions in the log's history, so the footer bytes
                    // stepped over are counted here exactly as the closed-block branch counts
                    // the ones it skips.
                    log_id = log_id.saturating_add(stepped.saturating_sub(at));
                }
                // Reads a record by the length it declares when it is framed that way, and to
                // the newline when it is not, so one loop walks a log holding both. The
                // preallocated zero run that trails the records ends the loop from inside the
                // reader: no frame can start with a zero byte.
                let Some(line) = read_raw_record(&mut reader)? else {
                    // a scan must cross block boundaries: the zeros here are either the padding
                    // before a closed block's footer, with records continuing after it, or the
                    // reservation past the end. The footer is what tells them apart, and reading
                    // it wrong loses every record after the first boundary -- silently, because a
                    // short log looks exactly like a log that is short.
                    let physical = header_len.saturating_add(log_id.saturating_sub(base));
                    match block_is_closed(&mut reader, physical, header_len, piece_len)? {
                        Some(next_physical) => {
                            // Log ids are byte positions in the log's history, so the bytes
                            // stepped over have to be counted or every address after this block
                            // moves.
                            log_id =
                                log_id.saturating_add(next_physical.saturating_sub(physical));
                            continue;
                        }
                        None => break,
                    }
                };
                let read = line.len();
                if read == 0 {
                    break;
                }
                let next_log_id = log_id.saturating_add(read as u64);
                if log_id < start_offset {
                    // Before the window: skip it, but keep counting position.
                    log_id = next_log_id;
                    continue;
                }
                if next_log_id > end_offset {
                    break 'segments;
                }
                // `total > 0` so a window always yields at least one record: a record larger
                // than the whole budget would otherwise return nothing and leave a caller
                // resuming at the position it already asked for, which is a walk that cannot
                // advance rather than a window that is full.
                if total > 0 && total + read as u64 > max_bytes {
                    // Out of budget with the window not yet walked: there is more to read, and
                    // this record's own position is where reading resumes.
                    truncated = true;
                    resume_at = log_id;
                    break 'segments;
                }
                // Refuse a corrupt record here, where it is being read. This used to happen only as
                // a side effect of walking the whole file to find the log's end; that walk no longer
                // reads everything, and a guarantee that depends on unrelated work is not a
                // guarantee. A blank line carries nothing to verify and is passed through as before.
                // The decode that verifies the record is also the decode the caller needs. It
                // used to be thrown away here and repeated by every caller -- replay decoded the
                // whole window twice, once for nothing. A blank line carries no record and is
                // passed through undecoded, as before.
                let decoded = if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                    None
                } else {
                    Some(decode_wal_line(&line)?)
                };
                if let Some(item) = take(log_id, line, decoded) {
                    records.push(item);
                }
                log_id = next_log_id;
                total += read as u64;
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        Ok((records, truncated, resume_at))
    }

    /// Where reading can start, to see every record after `sequence` and no whole piece before it.
    ///
    /// Recovery knows a sequence -- the one its durable index already covers -- and needs a
    /// position to read from. Sequences ascend through the log, so a piece whose last sequence is
    /// at or below the watermark holds nothing left to replay, and reading can begin at the first
    /// piece that is not. The answer is conservative: it never skips a record after the watermark,
    /// and it may include some before it, which the caller already filters.
    ///
    /// A log in one piece answers zero, which is what it did before any of this existed.
    pub fn log_id_after_sequence(
        &self,
        shard_id: ShardId,
        sequence: u64,
    ) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let mut start = 0u64;
        for path in wal_segment_paths(&inner.root, shard_id) {
            if !path.exists() {
                continue;
            }
            let (last, record_end) = last_wal_sequence_in(&path)?;
            // An empty piece holds nothing either way; keep looking past it.
            if last > 0 && last > sequence {
                return Ok(start);
            }
            let (base, header_len) = read_wal_base(&path)?;
            // Where the RECORDS end, not where the file does: under preallocation the active
            // piece's file length includes the zeros reservation, which holds no log ids.
            start = base.saturating_add(record_end.saturating_sub(header_len));
        }
        Ok(start)
    }

    pub fn flush(&self, shard_id: ShardId) -> Result<WriteAheadLogFlushReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = active_wal_path(&mut inner, shard_id);
        // The record end comes back from this scan. It used to be discarded and the file's
        // LENGTH asked for instead -- see `note_durable_record_end` for why those differ.
        let (last_sequence, record_end) = last_wal_sequence_at(&inner.root, shard_id)?;
        if !path.exists() {
            return Ok(WriteAheadLogFlushReport {
                shard_id,
                path: path.as_ref().clone(),
                last_sequence,
                persistent_bytes: 0,
                synced: false,
            });
        }
        let file = OpenOptions::new().read(true).write(true).open(path.as_path())?;
        crate::durability_metrics::record_barrier("engine_wal_flush");
        file.sync_all()?;
        sync_parent_dir(&path)?;
        // Where the RECORDS end, not where the FILE does. Under preallocation the two differ
        // by up to a whole chunk, and this is a durability figure. See
        // `note_durable_record_end`. The `inner` lock is held across this whole function, so
        // nothing can have been appended between the scan above and the barrier just taken.
        let persistent_bytes = record_end;
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = last_sequence;
        inner.stats.persistent_bytes = persistent_bytes;
        // Per shard, so a later barrier on a different shard cannot be read as this one's.
        note_durable_record_end(&mut inner, shard_id, persistent_bytes, false);
        inner
            .durable_sequence_by_shard
            .insert(shard_id, last_sequence);
        Ok(WriteAheadLogFlushReport {
            shard_id,
            path: path.as_ref().clone(),
            last_sequence,
            persistent_bytes,
            synced: true,
        })
    }

    /// Hold WAL reclaim at `sequence`: records at or above it may still be the only copy of a
    /// block's bytes, so GC must not reclaim past it.
    ///
    /// Set this to the dump watermark and advance it as blocks are dumped into slabs. Until a
    /// shard registers a floor its GC is unconstrained, so this is inert for callers that do not
    /// put blocks in the WAL.
    /// Whether two handles address the SAME underlying log (clones of one store). Registrations
    /// keyed only by (shard, object) conflate engines -- every embedded engine in a process
    /// serves shard 1 -- so consumers that aggregate across a registry must filter by the log
    /// identity the registration actually points at.
    pub fn same_log(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn set_block_retention_floor(&self, shard_id: ShardId, sequence: u64) {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.insert(shard_id, sequence);
    }

    /// The shard's block-retention floor, if one is registered.
    pub fn block_retention_floor(&self, shard_id: ShardId) -> Option<u64> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.get(&shard_id).copied()
    }

    /// Drop the shard's floor, letting GC reclaim freely again. Call this only once no block
    /// resolves inside this shard's WAL.
    pub fn clear_block_retention_floor(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.remove(&shard_id);
    }

    /// Bytes reclaimed from the head of this shard's log so far.
    ///
    /// A record's log id is its byte offset in the log's whole history, which does not change
    /// when the head is reclaimed. This is the difference between that and where the record
    /// physically sits now.
    pub fn base_offset(&self, shard_id: ShardId) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        Ok(read_wal_base(&path)?.0)
    }

    /// Physical byte offset of the record with this log id, or `None` if the log id has been
    /// reclaimed and the record no longer exists.
    ///
    /// Returning `None` rather than a wrong offset is the point: a reclaimed log id would
    /// otherwise resolve into the middle of the file and parse as some other record.
    pub fn resolve_log_id(
        &self,
        shard_id: ShardId,
        log_id: u64,
    ) -> Result<Option<u64>, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        Ok(locate_log_id(&inner.root, shard_id, log_id)?.map(|(_, physical)| physical))
    }

    /// Log id of a record currently at `physical_offset`. The inverse of [`Self::resolve_log_id`],
    /// used when recording the address of a record just appended.
    pub fn log_id_at(
        &self,
        shard_id: ShardId,
        physical_offset: u64,
    ) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        let (base, header_len) = read_wal_base(&path)?;
        Ok(base.saturating_add(physical_offset.saturating_sub(header_len)))
    }

    /// Read `size` bytes of the record at `log_id`, following the reclaim base.
    ///
    /// `Ok(None)` means the log id was reclaimed.
    pub fn read_at_log_id(
        &self,
        shard_id: ShardId,
        log_id: u64,
        size: u64,
    ) -> Result<Option<Vec<u8>>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let Some((path, physical)) = locate_log_id(&inner.root, shard_id, log_id)? else {
            return Ok(None);
        };
        // A caller asking by log id does not know how long the record is, so it asks for an upper
        // bound. Reading past the end of the piece would fail and report the record as absent -- a
        // read that should have succeeded -- so clamp to what that piece actually holds.
        let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        let available = length.saturating_sub(physical);
        if available == 0 {
            return Ok(None);
        }
        let bytes = read_at(&path, physical, size.min(available))?;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        Ok(Some(bytes))
    }

    /// Reclaim this shard's WAL prefix below `retain_from_sequence`, never going past what the
    /// durable served index proves it can replace.
    ///
    /// See [`DurableIndexAnchor`] for why the proof is an argument. A caller that asks for more
    /// than its anchor covers gets a narrowed reclaim and `clamped_by_durable_index` set, not an
    /// error: the ask is a target, the anchor is the limit, and the difference is reported
    /// rather than silently folded into the counts.
    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
        durable_index: &DurableIndexAnchor,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        // An anchor minted for another shard says nothing about this one, so it authorizes
        // nothing. Retaining from 0 keeps every record.
        let ceiling = if durable_index.shard_id() == shard_id {
            durable_index.through_sequence().saturating_add(1)
        } else {
            0
        };
        let clamped = retain_from_sequence > ceiling;
        let mut report =
            self.gc_before_sequence_unchecked(shard_id, retain_from_sequence.min(ceiling))?;
        // Report the sequence the caller ASKED for. `effective_retain_from_sequence` already
        // carries what was actually used, and a reclaim that did less than requested should be
        // visible as such instead of looking like a smaller request that succeeded exactly.
        report.retain_from_sequence = retain_from_sequence;
        report.clamped_by_durable_index = clamped;
        Ok(report)
    }

    /// [`gc_before_sequence`](Self::gc_before_sequence) without the durable-index clamp,
    /// subject only to the tail-continuity and block-retention floors below.
    ///
    /// This is the reclaim primitive; the anchored wrapper is the way in. Tests drive this
    /// directly to exercise those two floors in isolation.
    pub(crate) fn gc_before_sequence_unchecked(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = active_wal_path(&mut inner, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogGcReport {
                shard_id,
                retain_from_sequence,
                effective_retain_from_sequence: retain_from_sequence,
                ..WriteAheadLogGcReport::default()
            });
            // (an absent log has reclaimed nothing, so the default base of 0 is correct)
        }

        let bytes_before = path.metadata()?.len();
        let (last_sequence, _) = last_wal_sequence_at(&inner.root, shard_id)?;
        // Never delete the highest-sequence record. The WAL file is the sequence-generator
        // source on restart (append seeds last_sequence_by_shard from last_wal_sequence_at), so
        // emptying it entirely on a full reclaim (retain_from > max) would regress the next
        // append to sequence 1 -> sequence REUSE + silent loss: the re-used seq is <= the
        // persisted applied_wal_sequence anchor, so replay's `sequence > watermark` filter drops
        // it. the zone-aligned wal Truncate always retains the tail zone holding the highest
        // sequence for exactly this continuity reason. Clamp the retain floor to keep the tail.
        let effective_retain = retain_from_sequence.min(last_sequence);
        // Never reclaim past the block-retention floor. A record at or above it may carry the
        // only copy of a block's bytes -- a block in the WAL has no copy in a slab until it is
        // dumped -- so removing it loses data that the served index still points at, and the
        // read fails at some later, unrelated moment.
        let floor = inner
            .block_retention_floor_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(u64::MAX);
        let clamped_by_block_retention = effective_retain > floor;
        let effective_retain = effective_retain.min(floor);
        // Whole pieces below the retain point go without being read or copied. This uses the
        // NARROWED point, not the one the caller asked for: the two clamps above exist to keep the
        // highest-sequence record and anything a block still depends on, and a piece unlinked here
        // is gone in a way the copy path below could never undo. What remains is the piece being
        // written, which that path handles as it always has.
        let (dropped_segments, dropped_bytes) =
            drop_covered_wal_segments(&inner.root, shard_id, effective_retain)?;
        // Records are appended in strictly ascending sequence -- the live path increments under
        // the append lock, and the replay path refuses anything at or below the last sequence --
        // so the records to keep are a contiguous SUFFIX and the ones to drop are a prefix.
        // That is what makes the whole reclaim expressible as one number: every survivor moves
        // down by exactly the length of the removed prefix.
        let (base_offset, header_len) = read_wal_base(&path)?;
        // Where the records stop. Under preallocation the file continues past them as a zeros
        // reservation, which this pass must neither decode as a record nor copy into the
        // rewritten file; the tail scan also repairs any torn non-zero tail first, exactly as
        // an append would.
        let (_, record_end) = last_wal_sequence_in(&path)?;
        // One line at a time. A log is not bounded by memory, so neither this search nor the
        // copy below may hold it: reclaiming a large log otherwise costs a transient allocation
        // the size of the whole file.
        let mut tally = WalReadTally::start();
        let mut source = open_wal_read(path.as_path(), &mut tally)?;
        source.seek(SeekFrom::Start(header_len))?;
        // Bounded by the cursor below rather than by `take`, because a Take is not seekable and
        // stepping over a block's footer is a seek. the reclaim walk crosses blocks now.
        let mut reader = BufReader::new(source);
        let walk_until = record_end;
        let mut records_before = 0usize;
        let mut records_after = 0usize;
        let mut split = None;
        let mut cursor = header_len;
        loop {
            if cursor >= walk_until {
                break;
            }
            // Step over a block footer this lands on, before reading. The branch below only
            // catches a boundary with PADDING in front of its footer; a block whose records end
            // flush against the footer has none, and the footer is then read as a record -- its
            // first byte is not a frame marker, so the reader falls to newline-delimited mode and
            // returns a fragment.
            //
            // That matters more here than in a scan that only counts: `cursor` becomes `split`
            // below, the byte offset this reclaim keeps from. A fragment read at a boundary moves
            // the line between what is copied and what is dropped, so the pass can retain or
            // discard the wrong records rather than merely report the wrong number of them.
            let stepped = skip_block_footer_if_due(&mut reader, cursor, header_len)?;
            if stepped != cursor {
                cursor = stepped;
                if cursor >= walk_until {
                    break;
                }
            }
            let Some(line) = read_raw_record(&mut reader)? else {
                // Padding before a closed block's footer, or the end. The footer decides.
                match block_is_closed(&mut reader, cursor, header_len, walk_until)? {
                    Some(next) => {
                        cursor = next;
                        continue;
                    }
                    None => break,
                }
            };
            let read = line.len();
            if read == 0 {
                break;
            }
            // The delimiter comes off a text record and NOTHING comes off a binary frame, which
            // ends where its declared length ends. Stripping a trailing `0x0A` off a frame takes
            // a byte of the payload, and `decode_wal_line` below then refuses the whole sweep
            // with `binary record is incomplete` -- so a log whose records happen to end in that
            // byte reclaimed nothing at all, and the caller's `.ok()` rendered it as success.
            // `info_at`'s walk already applied this rule; this one did not.
            let trimmed = crate::log_framing::record_body(line.as_slice());
            if !trimmed.iter().all(|byte| byte.is_ascii_whitespace()) {
                records_before += 1;
                if split.is_some() {
                    // Past the split every remaining record is retained, and the sequence is
                    // ascending, so there is nothing left to decide -- count it and move on
                    // rather than parsing it again.
                    records_after += 1;
                } else if decode_wal_line(trimmed)?.sequence >= effective_retain {
                    split = Some(cursor);
                    records_after += 1;
                }
            }
            cursor = cursor.saturating_add(read as u64);
        }
        // Nothing retained means everything before the end goes; the split is the end of file.
        let split = split.unwrap_or(cursor);
        let removed_bytes = split.saturating_sub(header_len);
        let new_base = base_offset.saturating_add(removed_bytes);
        let retained_bytes = cursor.saturating_sub(split);

        // Copying the survivors IS the cost of a pass, so reclaiming a sliver off the front of a
        // large log rewrites almost all of it to buy almost nothing. Decline that and let the
        // prefix grow: the same copy then frees far more. Nothing is lost by waiting -- the
        // records stay where they are -- and the condition is self-correcting, because a growing
        // prefix raises the freed fraction until a pass is worth running.
        //
        // Below the copy floor the ratio is meaningless and the rewrite is cheap either way, so
        // small logs reclaim exactly as they did before.
        // Whether the bytes this pass keeps contain a closed block's footer. Reported, not acted
        // on: the copy has to stay verbatim, and `block_is_closed` is what handles the footer it
        // displaces. This is here so a test can say "the case was reached".
        let retained_carries_a_block_footer = retained_bytes > 0 && {
            let block = block_of(split.saturating_sub(header_len));
            let slot = header_len + block_footer_at(block);
            split <= slot && slot + WAL_BLOCK_FOOTER_BYTES <= record_end
        };
        let worth_rewriting = reclaim_is_worth_rewriting(
            removed_bytes,
            retained_bytes,
            reclaim_min_copy_bytes(),
            reclaim_min_freed_percent(),
        );
        if !worth_rewriting {
            return Ok(WriteAheadLogGcReport {
                shard_id,
                retain_from_sequence,
                effective_retain_from_sequence: effective_retain,
                clamped_by_block_retention,
                clamped_by_durable_index: false,
                records_before,
                records_after: records_before,
                records_removed: 0,
                bytes_before,
                bytes_after: bytes_before,
                base_offset,
                bytes_copied: 0,
                skipped_not_worth_rewrite: true,
                retained_bytes_carry_a_block_footer: retained_carries_a_block_footer,
                dropped_segments,
                dropped_segment_bytes: dropped_bytes,
            });
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            // The base goes inside the file so it is swapped in by the same rename as the bytes
            // it describes. A base kept beside the file could survive a crash disagreeing with
            // them, and every address would then resolve to the wrong record.
            temp.write_all(&crate::log_framing::encode_base_header(new_base))?;
            // Copy the retained records byte for byte rather than decoding and re-encoding
            // them. Re-encoding could change a record's length, which would break the offset
            // arithmetic this whole scheme rests on, and it costs a parse per record.
            let mut copy_tally = WalReadTally::start();
            let mut source = open_wal_read(path.as_path(), &mut copy_tally)?;
            source.seek(SeekFrom::Start(split))?;
            std::io::copy(
                &mut BufReader::new(source.take(record_end.saturating_sub(split))),
                &mut temp,
            )?;
            temp.flush()?;
            temp.sync_all()?;
        }
        fs::rename(&temp_path, path.as_path())?;
        sync_parent_dir(&path)?;
        // Reclaim is the only thing that moves the base, so refresh the cache here rather than
        // letting an append compute a log id against a stale one.
        let new_header_len = crate::log_framing::encode_base_header(new_base).len() as u64;
        inner
            .base_by_shard
            .insert(shard_id, (new_base, new_header_len));
        let bytes_after = path.metadata()?.len();
        // The rewritten file ends exactly at its records: no reservation survives the rename,
        // and the record end IS the file length again. Leaving the old entries in place would
        // make the next append place its bytes against the pre-rewrite file.
        inner.verified_len_by_shard.insert(shard_id, bytes_after);
        // AND THE DURABLE FIGURE, for the same reason and through the same door.
        //
        // The rewrite made the retained records durable -- the temp file was `sync_all`'d and
        // the rename was followed by a directory sync -- and they are now the WHOLE of the
        // active piece, so `bytes_after` is exactly how far a barrier has reached into it.
        //
        // Left alone the entry kept the PRE-REWRITE figure. `roll_wal_segment_if_due` clears its
        // entry precisely because it replaces the active piece; a reclaim is the OTHER thing
        // that shortens one, and it did not. Measured on this tree before this line existed: a
        // log holding 5,223 bytes of records reported 20,273 durable, and after a second reclaim
        // a log holding 1,184 still reported 20,273 -- 17.1x, which is the OVERSTATING direction
        // `note_durable_record_end` and `info()` both exist to rule out.
        //
        // Written OUTRIGHT, not monotonically. The log is shorter than it was, so a `max` here
        // would keep exactly the figure that is now wrong -- which is what the group-commit
        // barrier's `max` did with it until a `flush` happened to overwrite it.
        note_durable_record_end(&mut inner, shard_id, bytes_after, false);
        inner.prealloc_physical_by_shard.remove(&shard_id);
        Ok(WriteAheadLogGcReport {
            shard_id,
            retain_from_sequence,
            records_before,
            records_after,
            records_removed: records_before.saturating_sub(records_after),
            bytes_before,
            bytes_after,
            base_offset: new_base,
            effective_retain_from_sequence: effective_retain,
            clamped_by_block_retention,
            clamped_by_durable_index: false,
            bytes_copied: retained_bytes,
            skipped_not_worth_rewrite: false,
            retained_bytes_carry_a_block_footer: retained_carries_a_block_footer,
            dropped_segments,
            dropped_segment_bytes: dropped_bytes,
        })
    }

    /// How many bytes this shard's log has taken since its last dump.
    ///
    /// This is the figure the dump cadence wants, and it is not the log's size. A log that is
    /// large but fully dumped has nothing undumped in it: measuring the whole file instead made
    /// a shard that had written one record since its last dump clear the byte threshold on every
    /// round, and a whole-index serialize per round is exactly what the cadence exists to avoid.
    ///
    /// Counted rather than derived from the file, because reclaim truncates the log immediately
    /// after a dump -- any watermark held against the file's length is stranded above it.
    ///
    /// A shard that has not dumped in this process reports what it has appended since start-up,
    /// which is 0 for one that has not written. That under-reports where the file-length version
    /// over-reported, and it is the safer direction to be wrong in only because the RECORD
    /// threshold is anchored on the durable manifest sequence and so survives a restart intact;
    /// the two are read together and either can release the dump.
    pub fn undumped_len_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner
            .undumped_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0)
    }

    /// Object mutations appended since this shard's last dump. See
    /// `undumped_objects_by_shard`.
    pub fn undumped_objects_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner
            .undumped_objects_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0)
    }

    /// Record that a dump captured this shard's log, resetting its undumped growth to 0.
    ///
    /// Called only once the dump manifest is durably written, so the counter never clears past
    /// state a crash could lose -- a restart mid-dump re-dumps rather than skipping.
    pub fn mark_dumped(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.undumped_bytes_by_shard.insert(shard_id, 0);
        inner.undumped_objects_by_shard.insert(shard_id, 0);
    }

    pub fn stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.stats.stats_full_scans = inner.stats.stats_full_scans.saturating_add(1);
        let path = active_wal_path(&mut inner, shard_id);
        // Durable bytes, not written bytes. An append puts a record in the file whether or not
        // a barrier followed it, so the file's length is what has been WRITTEN -- reporting it
        // as `persistent_bytes` says unsynced records are on disk to survive a crash, which is
        // the one thing this number exists to answer. Sealed pieces are durable by construction
        // (a piece is only sealed after its barrier); the active piece counts only as far as
        // its last barrier reached.
        let sealed_bytes = wal_all_segment_bytes(&inner.root, shard_id)
            .saturating_sub(path.metadata().map(|metadata| metadata.len()).unwrap_or(0));
        let durable_active = inner
            .durable_active_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        let durable_sequence = inner
            .durable_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        WriteAheadLogStats {
            last_sequence: last_wal_sequence_at(&inner.root, shard_id)
                .map(|(sequence, _)| sequence)
                .unwrap_or_default(),
            persistent_bytes: sealed_bytes.saturating_add(durable_active),
            last_flushed_sequence: durable_sequence,
            ..inner.stats
        }
    }

    /// Non-scanning snapshot of the accumulated counters (`inner.stats`) WITHOUT the full-file
    /// `last_wal_sequence_at` scan that `stats()` performs. `last_sequence` / `persistent_bytes`
    /// carry their last-appended cached values rather than a fresh disk read. Used by the phase-1
    /// aging test to read the scan counters without perturbing them.
    /// `stats`, WITHOUT the full-file scan, for callers read on a schedule.
    ///
    /// # WHY THIS EXISTS
    ///
    /// `stats` answers `last_sequence` with `last_wal_sequence_at`, a full-file scan. Measured by
    /// `what_one_stats_read_allocates`, that scan is **771,062 of the 776,311 bytes one
    /// `shard_stats` read allocates over a 320-page store -- 99.3%** -- and it grows with the log,
    /// while every other part of that read is flat or zero. `shard_stats` is what a node answers a
    /// stats request with, so the scan was paid again on every read for as long as the node was up.
    ///
    /// # WHY THE CACHED SEQUENCE IS SOUND HERE
    ///
    /// It is already trusted for strictly more: [`Self::cached_last_sequence`] records that the
    /// per-write index-anchor step used to call `stats` on EVERY write and was moved onto the cache
    /// because the embedded rescan was an `O(records)`-per-write cost "confirmed dominant by stack
    /// sampling". A value good enough to anchor the index on the write path is good enough for a
    /// report.
    ///
    /// # THE SCAN IS KEPT FOR THE ONE CASE THAT NEEDS IT
    ///
    /// A store that has neither appended nor scanned for this shard has no cached value, and
    /// returning 0 there would under-report after a cold load. So a miss scans -- and SEEDS the
    /// cache with what it found, which is the same seeding the append path performs on restart --
    /// making the scan once per shard per process instead of once per read.
    ///
    /// `persistent_bytes` is computed exactly as `stats` computes it. The segment walk behind it
    /// measured a flat 497 B, so there is nothing to win by changing it and a number to lose.
    pub fn stats_without_rescan(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        // CACHE FIRST, AND THE COUNTER ONLY MOVES WHEN A SCAN ACTUALLY HAPPENS. `stats`
        // increments `stats_full_scans` unconditionally; doing that here would report scans this
        // path no longer performs to the four probes that read the counter.
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                inner.stats.stats_full_scans = inner.stats.stats_full_scans.saturating_add(1);
                let scanned = last_wal_sequence_at(&inner.root, shard_id)
                    .map(|(sequence, _)| sequence)
                    .unwrap_or_default();
                inner.last_sequence_by_shard.insert(shard_id, scanned);
                scanned
            }
        };
        let path = active_wal_path(&mut inner, shard_id);
        let sealed_bytes = wal_all_segment_bytes(&inner.root, shard_id)
            .saturating_sub(path.metadata().map(|metadata| metadata.len()).unwrap_or(0));
        let durable_active = inner
            .durable_active_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        let durable_sequence = inner
            .durable_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        WriteAheadLogStats {
            last_sequence,
            persistent_bytes: sealed_bytes.saturating_add(durable_active),
            last_flushed_sequence: durable_sequence,
            ..inner.stats
        }
    }

    /// TEST-ONLY DOORS onto the two disk reads `stats` performs, so a probe can price them apart.
    ///
    /// Both are free functions over `inner.root`, which is private, and the question they answer --
    /// which of the two reads a scheduled `stats` call is paying for -- cannot be asked from
    /// outside without them. Neither door is reachable in a release build.
    #[cfg(any(test, feature = "alloc-probe"))]
    pub fn probe_last_sequence_scan(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        last_wal_sequence_at(&inner.root, shard_id)
            .map(|(sequence, _)| sequence)
            .unwrap_or_default()
    }

    #[cfg(any(test, feature = "alloc-probe"))]
    pub fn probe_all_segment_bytes(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        wal_all_segment_bytes(&inner.root, shard_id)
    }

    pub fn raw_stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        WriteAheadLogStats {
            last_sequence: inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(inner.stats.last_sequence),
            ..inner.stats
        }
    }

    /// O(1) read of the cached last WAL sequence for `shard_id` -- the value advanced by the most
    /// recent append on this store -- WITHOUT the full-file `last_wal_sequence_at` scan that
    /// `stats()` runs. The per-write index-anchor step (`shard.applied_wal_sequence = last
    /// sequence`) otherwise calls `stats()` on EVERY write, so its embedded rescan is a second
    /// O(records)-per-write cost under the engine `shards` lock (equal in weight to the append-path
    /// rescan, and confirmed dominant by stack sampling). Under flat append the engine anchors
    /// off this cached value instead. Returns 0 if this store has not yet observed the shard (no
    /// append and no scan); the write path always has, so the value equals the on-disk maximum.
    pub fn cached_last_sequence(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner
            .last_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn info(&self, shard_id: ShardId) -> Result<WriteAheadLogInfo, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogInfo {
                shard_id,
                path,
                format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                ..WriteAheadLogInfo::default()
            });
        }
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        let (_, header_len) = read_wal_base(&path)?;
        let mut tally = WalReadTally::start();
        let mut file = open_wal_read(&path, &mut tally)?;
        file.seek(SeekFrom::Start(header_len))?;
        let mut reader = BufReader::new(file);
        let mut start_sequence = 0_u64;
        let mut current_sequence = 0_u64;
        let mut records = 0_usize;
        // Read BYTE lines, not String lines.
        //
        // `lines()` yields a String and therefore demands valid UTF-8 of every record. That held
        // for as long as every payload was text, and it fails on the first binary record with
        // "stream did not contain valid UTF-8" -- before the decoder, which handles both
        // encodings, ever gets to look at it. Escaping the newline out of a binary payload keeps
        // records SPLITTABLE; it cannot make arbitrary bytes valid UTF-8, and nothing should
        // require them to be.
        let (_, info_header_len) = read_wal_base(&path)?;
        let info_len = path.metadata().map(|meta| meta.len()).unwrap_or(0);
        let mut info_at = info_header_len;
        loop {
            // The same first step the other walkers take. Without it a footer landed on flush
            // against a block's last record is read as a record, and the counts and sequences
            // reported here describe a log that stops at the first such boundary.
            info_at = skip_block_footer_if_due(&mut reader, info_at, info_header_len)?;
            let Some(mut line) = read_raw_record(&mut reader)? else {
                match block_is_closed(&mut reader, info_at, info_header_len, info_len)? {
                    Some(next) => {
                        info_at = next;
                        continue;
                    }
                    None => break,
                }
            };
            info_at = info_at.saturating_add(line.len() as u64);
            // Only a text record carries a trailing newline; a binary frame ends where its
            // declared length ends, and trimming it would take a byte of the payload.
            if line.first() != Some(&crate::log_framing::FRAME_MAGIC_V3) {
                while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                    line.pop();
                }
            }
            if line.is_empty() {
                continue;
            }
            let record = decode_wal_line(&line)?;
            if start_sequence == 0 {
                start_sequence = record.sequence;
            }
            current_sequence = current_sequence.max(record.sequence);
            records += 1;
        }
        // The whole log, not just its newest piece.
        let length_bytes = wal_all_segment_bytes(&inner.root, shard_id);
        let sealed_bytes = length_bytes.saturating_sub(path.metadata()?.len());
        let durable_active = inner
            .durable_active_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        Ok(WriteAheadLogInfo {
            shard_id,
            path,
            start_sequence,
            current_sequence,
            records,
            length_bytes,
            // Durable bytes and the sequence a barrier actually covered. The previous values --
            // the file length, and `last_flushed_sequence.max(current_sequence)` -- both took
            // the highest number in sight, so an unsynced append reported itself as durable.
            // Understating is survivable; overstating is the failure this field exists to
            // prevent, so a shard this handle has never synced reports 0 rather than guessing.
            persistent_length_bytes: sealed_bytes.saturating_add(durable_active),
            last_flushed_sequence: inner
                .durable_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(0),
            format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
        })
    }
}

impl Default for LocalWriteAheadLogStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("wals");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("write-ahead log lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

/// Read at most `size` bytes from `path`, starting at `physical`.
/// Drop whole pieces that hold nothing worth keeping.
///
/// A piece whose last record is below the retain floor holds nothing above it -- records are
/// appended in ascending sequence -- so the whole file goes, and nothing is copied. Stops at the
/// first piece that still holds something: the pieces are in order, so everything after it does too.
///
/// Returns how many pieces went and how many bytes they held.
fn drop_covered_wal_segments(
    root: &Path,
    shard_id: ShardId,
    retain_from_sequence: u64,
) -> Result<(usize, u64), WriteAheadLogError> {
    let active = write_ahead_log_path(root, shard_id);
    let mut dropped = 0usize;
    let mut freed = 0u64;
    // Stop after this many, so the lock every append needs is not held for the whole backlog.
    // Whatever is left is dropped by the next pass: pieces go from the front in order, so stopping
    // early leaves the rest exactly where the next pass would have looked anyway.
    let limit = wal_reclaim_max_segments_per_pass();
    let paths = wal_segment_paths(root, shard_id);
    for (index, path) in paths.iter().enumerate() {
        let path = path.clone();
        if limit > 0 && dropped >= limit {
            break;
        }
        if path == active || !path.exists() {
            continue;
        }
        // Prove it from the NEXT piece's first record when that is possible. Records ascend
        // strictly, so `last(N) < first(N+1)`, and `first(N+1) <= retain_from` therefore puts all
        // of piece N below the floor. That is one record read instead of decoding the whole file:
        // the walk below costs 20-27 ms on a real ~500 KB piece, which is what makes this pass
        // expensive enough to need a cap, and the cap is what stops a backlog draining.
        //
        // Only ever used to say YES. Anything unclear -- no next piece, an unreadable head, a
        // bound that does not clear the floor -- falls through to the authoritative walk, because
        // unlinking a piece that still holds a needed record cannot be undone.
        let covered_by_next = match paths.get(index + 1) {
            Some(next) => first_wal_sequence_in(next)
                .ok()
                .flatten()
                .is_some_and(|first_next| first_next <= retain_from_sequence),
            None => false,
        };
        if !covered_by_next {
            let (last, _) = last_wal_sequence_in(&path)?;
            if last == 0 {
                // Holds no record at all; nothing to keep and nothing to lose.
            } else if last >= retain_from_sequence {
                break;
            }
        }
        freed = freed.saturating_add(path.metadata().map(|meta| meta.len()).unwrap_or(0));
        fs::remove_file(&path)?;
        dropped += 1;
    }
    if dropped > 0 {
        sync_parent_dir(&active)?;
    }
    Ok((dropped, freed))
}

/// One byte at `offset`, or None at/past the end of the file.
fn byte_at(path: &Path, offset: u64) -> Result<Option<u8>, WriteAheadLogError> {
    let mut tally = WalReadTally::start();
    let mut file = open_wal_read(path, &mut tally)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    match std::io::Read::read(&mut file, &mut byte)? {
        0 => Ok(None),
        _ => Ok(Some(byte[0])),
    }
}

fn read_at(path: &Path, physical: u64, size: u64) -> Result<Vec<u8>, WriteAheadLogError> {
    let mut tally = WalReadTally::start();
    let mut file = open_wal_read(path, &mut tally)?;
    file.seek(SeekFrom::Start(physical))?;
    let mut bytes = vec![0; size as usize];
    let read = file.read(&mut bytes)?;
    bytes.truncate(read);
    Ok(bytes)
}

/// TS_WAL_SEGMENT_BYTES: roll the log into a new piece once the one being written passes this.
///
/// Zero never rolls, which is one file. Rolling lets reclaim unlink a whole earlier piece instead
/// of rewriting the file to keep the survivors.
///
/// **Default 256 KiB**, chosen by measurement rather than by feel:
///
/// * It matches `wal_preallocate_chunk`, so a piece is a whole number of the units the file is
///   already grown in and rolling costs no extra preallocation. Measured on-disk overhead against
///   never rolling was 1,258 bytes on a 1.3 MB log.
/// * It is the largest size that still rolls usefully inside the log the index-dump threshold
///   leaves. That threshold is a megabyte, so 1 MiB pieces would barely roll in a steady system;
///   256 KiB gives four or five.
///
/// What it buys, measured across two runs at 4,000 records:
///
/// | | reclaim keeping 10% | keeping 90% |
/// |---|---|---|
/// | never rolling | 34-68 ms, copies 119,748 B | copies 1,075,475 B |
/// | 256 KiB | 12-15 ms, copies 119,748 B, unlinks 4 pieces | copies 144,482 B |
///
/// The copy is IDENTICAL in the first column: reclaim copies survivors, and when most of the log
/// is dropped the survivors are small either way. The saving there is the SCAN -- a rolled reclaim
/// skips the pieces it unlinks instead of reading the file to find what stays. Rolling reduces the
/// COPY only when most of the log is kept, which is what a blocked floor produces.
///
/// The exposure to weigh against that is file count: pieces accumulate while reclaim is blocked,
/// and a log that reached a gigabyte would hold four thousand of them.
fn wal_segment_bytes() -> u64 {
    if let Some(threshold) = SEGMENT_BYTES_OVERRIDE.with(|value| value.get()) {
        return threshold;
    }
    std::env::var("TS_WAL_SEGMENT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(DEFAULT_WAL_SEGMENT_BYTES)
}

/// Rolling threshold when nothing sets one. See [`wal_segment_bytes`] for how it was chosen.
/// Zero is still accepted and still means "never roll".
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 256 * 1024;

thread_local! {
    /// Per-thread override of the rolling threshold.
    ///
    /// Per thread, not per process: appending happens on the calling thread, and a test that set a
    /// process-wide threshold would make every other test running beside it roll too.
    static SEGMENT_BYTES_OVERRIDE: std::cell::Cell<Option<u64>> =
