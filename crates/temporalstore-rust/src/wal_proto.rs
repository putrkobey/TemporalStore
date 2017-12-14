// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encode an engine write-ahead log record as a compact binary message rather than text.
//!
//! The text encoding spells out every field name on every record and has no byte-string type, so
//! binary values become arrays of decimal numbers. Measured on this path: a string write recorded
//! with its outcome costs 471.7 bytes, of which the outcome is 285.3 -- almost all of it field
//! names and a hex checksum. The node log already made this trade, and its own measurement put
//! text at roughly three bytes written per byte of data.
//!
//! **Fidelity comes before compactness.** This is the durability path, so anything this module
//! does not model explicitly travels byte for byte in the previous encoding rather than being
//! approximated by the nearest match. A command this module has never heard of still round-trips
//! exactly, and modelled arms can be added one at a time.
//!
//! The frame does not change. `log_framing` still wraps every record with its length and checksum,
//! so byte offsets, integrity checking and the scan path behave exactly as before. Only the
//! payload inside the frame differs, and its first byte says which encoding it is.

use prost::Message;

use crate::block_store::ElementEntry;
use crate::sdk::v1;
use crate::wal::{StagedBlock, WalOutcomeItem, WriteAheadLogRecord, WriteAheadLogRecordMetadata};

/// Marks a payload as protobuf.
///
/// A text payload always starts with `{`, so one byte separates the two and a log written before
/// this existed reads back unchanged.
pub(crate) const BINARY_PAYLOAD_MARKER: u8 = 0xB7;

/// Marker for a protobuf payload carrying NO escaping.
///
/// Escaping exists to keep a record free of the byte a line-oriented reader splits on. Inside a
/// length-framed record nothing splits on anything, so the stuffing is pure cost -- and it is
/// not small: protobuf writes field 1 as the tag byte 0x0A, so the payloads carrying the most
/// fields are the ones paying the most for a delimiter no reader is looking for.
///
/// A separate marker rather than a flag read at decode time: which encoding a payload is in has
/// to be a property of the payload, or a log written across a configuration change stops
/// reading halfway through.
pub(crate) const RAW_PAYLOAD_MARKER: u8 = 0xB8;

/// Marker for a protobuf payload that is zstd-compressed and carries no escaping.
///
/// A record's payload is the largest thing this log writes and the most repetitive: the same
/// field names, scope keys and policy blocks over and over. Measured on one live segment of the
/// hook store -- 151 records, 13.5 KB each -- zstd at level 3 takes 2,038,147 bytes to 236,714,
/// which is 8.61x, and projects to 617 MB off a 698 MB log.
///
/// Compressing the whole SEGMENT instead reaches 22.28x, and is not available here: a log id is a
/// byte position, page references point at those positions, and a block that has to be inflated
/// before any record inside it can be found does not have them. Per record keeps every record
/// independently addressable, which is the property the log is built on.
pub(crate) const COMPRESSED_RAW_PAYLOAD_MARKER: u8 = 0xB9;

/// The same, for a delimited frame, where the compressed bytes still have to be escaped.
///
/// Two markers rather than one plus a flag, for the reason the pair above gives: which encoding a
/// payload is in has to be a property of the payload, or a log written across a configuration
/// change stops reading halfway through.
pub(crate) const COMPRESSED_ESCAPED_PAYLOAD_MARKER: u8 = 0xBA;

/// The zstd level records are compressed at.
///
/// Measured on this path rather than inherited. The comment this replaces said the level matched
/// the page store's so the two would "make the same trade rather than two unexplained ones" --
/// which is a good instinct, and was never checked against what a WAL record actually looks like.
///
/// Checked now, on an encoded record whose value is prose, which is what the serving log holds
/// (its records compress about 8.6x, and this payload compresses 9.2x). Fastest of 400 runs, twice,
/// on a quiet box:
///
/// | value | level 1 | level 3 | level 6 |
/// |---|---|---|---|
/// | 1 KiB | 40.4 us, 531 B | 51.3 us, 528 B | 119.0 us, 526 B |
/// | 4 KiB | 47.1 us, 606 B | 60.9 us, 600 B | 168.4 us, 596 B |
///
/// Level 3 costs 27-29% more time than level 1 and returns under 1% in size. Level 6 costs two to
/// three times level 1 for another fraction of a percent. On a payload of random bytes the three
/// levels produce byte-IDENTICAL output, so the higher levels buy nothing there at all.
///
/// So: level 1. This is now a different level from the page store's, which the previous comment
/// was right to want to avoid -- but the two differ in what they compress. A page is large and
/// written once; a WAL record is small, written on the commit path, and its compressible part is
/// the envelope around a value that is usually already dense. A number that was matched for
/// tidiness is worth less than one that was measured, and if the page store's level is ever
/// measured the same way it may well move too.
const COMPRESSION_LEVEL: i32 = 1;

/// Below this many bytes a payload is written uncompressed.
///
/// Not a guess: the page store measured this exact question and found a 1-byte floor worse than
/// no compression at all -- the saving stopped while the median write rose, because a tiny
/// payload costs more to compress than it gives back. 256 is the floor that measurement chose.
const COMPRESSION_MIN_BYTES: usize = 256;

/// Whether new records are written compressed. **DEFAULT ON.**
///
/// Reading never consults this. A payload says what encoding it is in, so a log written across a
/// change reads end to end and turning it off again is not a one-way door.
///
/// It was built off, which meant every deployment paid to store a log it had the
/// code to shrink. Compression is applied only where it pays twice over: a payload under
/// `COMPRESSION_MIN_BYTES` is left alone, and a payload whose compressed form is not actually
/// smaller is written raw under its own marker.
///
/// The variable opts OUT, like `TS_WAL_BINARY_FRAME` beside it --
/// and for the same reason it is safe to flip either way: which encoding a payload is in is a
/// property of the payload, not of this flag.
///
pub(crate) fn compress_records_enabled() -> bool {
    !matches!(
        std::env::var("TS_WAL_COMPRESS_RECORDS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

thread_local! {
    /// The compressor, kept for the life of the thread rather than built per record.
    ///
    /// `zstd::stream::encode_all` builds a fresh compression context on every call, and a level-3
    /// context is tens of kilobytes. Measured per append, with an incompressible payload so the
    /// figures are the machinery and not the data: at a 1 KiB value compression added 6 allocations
    /// and 35,951 bytes, taking the append from 1,159 to 37,110 bytes -- 32x, for a record that is
    /// barely a kilobyte. At 4 KiB it added 45,167 bytes.
    ///
    /// The context does not depend on the payload, so there is no reason to build one per record.
    /// `bulk::Compressor` holds it and reuses it. What remains per record is the output buffer, which
    /// is the compressed bytes themselves and cannot be avoided here.
    ///
    /// Thread-local rather than shared: the compressor is `&mut` to use, so sharing it across threads
    /// would need a lock on the append path, and appends are the thing this is trying not to slow
    /// down.
    static RECORD_COMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// The largest scratch buffer worth keeping between records.
///
/// The buffer grows to the largest record a thread has encoded, and would otherwise hold that
/// capacity for the life of the thread. Past this it is dropped instead.
const MAX_ENCODE_SCRATCH_BYTES: usize = 1024 * 1024;

thread_local! {
    /// The payload buffer the compressing arm of `encode` builds into, kept between records.
    ///
    /// Thread-local for the same reason the compressor beside it is: no lock on the append path.
    /// Nothing borrows it across a call -- the borrow ends inside `encode`, before the compressed
    /// payload is framed -- so a second record on the same thread always finds it free.
    static ENCODE_SCRATCH: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Compress `encoded` if it is worth compressing, returning None when it is not.
///
/// None for a payload under the floor, and None when the compressed form is not actually smaller.
/// The second check is the page store's: compression that does not pay is written uncompressed
/// rather than trusted to pay on average.
fn compress_payload(encoded: &[u8]) -> Option<Vec<u8>> {
    if encoded.len() < COMPRESSION_MIN_BYTES {
        return None;
    }
    let compressed = RECORD_COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = zstd::bulk::Compressor::new(COMPRESSION_LEVEL).ok();
        }
        slot.as_mut()?.compress(encoded).ok()
    })?;
    (compressed.len() < encoded.len()).then_some(compressed)
}

/// The largest decompressed record the reused decompressor will size itself for.
///
/// The capacity comes from the frame header, which is bytes on disk. A corrupted header can
/// declare any length it likes, and `Decompressor::decompress` allocates whatever capacity it is
/// handed -- so without a bound a single bad byte becomes a huge allocation. Past this bound the
/// allocating decoder takes the record instead: it grows to fit what the frame actually holds
/// rather than what it claims to hold.
const MAX_REUSED_INFLATE_BYTES: usize = 16 * 1024 * 1024;

thread_local! {
    /// Thread-local for the same reason the compressor is: `decompress` takes `&mut self`, and sharing
    /// one across threads would put a lock on the path that replays the log.
    static RECORD_DECOMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// Inflate a compressed payload, reusing one decompression context per thread where the frame
/// allows it.
///
/// `zstd::stream::decode_all` builds a fresh decompression context per call and drops it again --
/// a flat 128 KiB allocated and freed for every compressed record, whatever the record's size.
/// Replay inflates every compressed record in the log, so that was paid once per record on every
/// recovery. Holding the context makes it once per thread.
///
/// Reusing it needs a capacity up front, and the honest source for one is the frame itself:
/// `ZSTD_getFrameContentSize` reports the length the compressor recorded in the header. Frames the
/// append path writes carry it; frames written by the streaming encoder -- every record in a log
/// older than this -- do not, and neither do frames declaring more than the bound above. Those
/// fall through to the decoder that shipped, so no record inflates differently than it used to and
/// none can fail to inflate that would have succeeded.
fn decompress_payload(framed: &[u8]) -> Result<Vec<u8>, String> {
    if let Some(capacity) = declared_inflated_len(framed) {
        let reused = RECORD_DECOMPRESSOR.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = zstd::bulk::Decompressor::new().ok();
            }
            slot.as_mut()?.decompress(framed, capacity).ok()
        });
        if let Some(inflated) = reused {
            return Ok(inflated);
        }
    }
    zstd::stream::decode_all(framed)
        .map_err(|err| format!("compressed wal payload did not inflate: {err}"))
}

/// The decompressed length this frame declares, when it declares one this will size for.
fn declared_inflated_len(framed: &[u8]) -> Option<usize> {
    capacity_within_bound(zstd::zstd_safe::get_frame_content_size(framed).ok()??)
}

fn capacity_within_bound(declared: u64) -> Option<usize> {
    let capacity = usize::try_from(declared).ok()?;
    (capacity <= MAX_REUSED_INFLATE_BYTES).then_some(capacity)
}

/// Escapes a newline out of an encoded payload.
///
/// The log is read with `reader.lines()`. A JSON payload can never contain a raw newline, so that
/// worked for as long as every record was text. Protobuf bytes contain 0x0A freely, and a record
/// carrying one splits into fragments that decode as nothing -- which does not fail loudly, it
/// loses the write.
///
/// Byte stuffing rather than base64: base64 costs a third of the payload, and this costs one byte
/// per newline actually present, which for encoded protobuf is a fraction of a percent. The
/// checksum in the frame is computed over the ESCAPED bytes, so the frame validates what it
/// actually holds.
const ESCAPE: u8 = 0x1B;
const ESCAPED_NEWLINE: u8 = 0x01;
const ESCAPED_ESCAPE: u8 = 0x02;

fn escape_newlines(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 8);
    for byte in bytes {
        match *byte {
            b'\n' => out.extend_from_slice(&[ESCAPE, ESCAPED_NEWLINE]),
            ESCAPE => out.extend_from_slice(&[ESCAPE, ESCAPED_ESCAPE]),
            other => out.push(other),
        }
    }
    out
}

fn unescape_newlines(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut iter = bytes.iter().copied();
    while let Some(byte) = iter.next() {
        if byte != ESCAPE {
            out.push(byte);
            continue;
        }
        match iter.next() {
            Some(ESCAPED_NEWLINE) => out.push(b'\n'),
            Some(ESCAPED_ESCAPE) => out.push(ESCAPE),
            Some(other) => return Err(format!("unknown escape 0x{other:02x} in wal payload")),
            None => return Err(String::from("wal payload ends mid-escape")),
        }
    }
    Ok(out)
}

/// The kinds common enough to be worth a code rather than a string on every item.
///
/// Order is the wire contract: a code means the same kind forever. A kind missing from this list
/// travels as its name, which is what keeps the list from having to be exhaustive.
const KIND_CODES: &[&str] = &[
    "string",
    "hash",
    "set",
    "list",
    "zset",
    "feature",
    "object",
    "seen",
    "bucket",
    "context_event",
    "context_index",
    "context_audit",
    "context_child",
    "context_summary",
    "context_compression",
    "context_node",
    "context_entity",
    "control_state",
    "control_counter",
    "control_change",
    "control_selection",
];

fn kind_code(kind: &str) -> Option<u32> {
    KIND_CODES
        .iter()
        .position(|known| *known == kind)
        .map(|index| index as u32 + 1)
}

fn kind_from_code(code: u32) -> Option<&'static str> {
    if code == 0 {
        return None;
    }
    KIND_CODES.get(code as usize - 1).copied()
}

/// The digest itself, not its hex transcription.
fn checksum_to_raw(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
        .collect()
}

fn checksum_from_raw(raw: &[u8]) -> String {
    raw.iter().map(|byte| format!("{byte:02x}")).collect()
}


/// The stored length a block carried in this record implies.
///
/// An address covers the block record whole -- its header and then its bytes -- so a record that
/// carries the bytes already states the length, and the address restates it. Only when there is
/// exactly one outcome and one block, the same rule the implied object id follows: with more than
/// one of either there is no saying which block an address belongs to.
///
/// A COMPRESSED block stores fewer bytes than it carries, so this will not match and the address
/// writes its length as before. That is the point of comparing rather than assuming.
pub(crate) fn implied_block_length(record: &WriteAheadLogRecord) -> Option<u64> {
    if record.outcomes.len() == 1 && record.staged_blocks.len() == 1 {
        let carried = record.staged_blocks[0].bytes.len() + crate::block_store::BLOCK_RECORD_HEADER_LEN;
        return Some(carried as u64);
    }
    None
}

fn address_to_proto(address: &ElementEntry, implied_length: Option<u64>) -> v1::WalBlockAddress {
    v1::WalBlockAddress {
        block_slab_id: address.block_slab_id(),
        offset: address.offset(),
        // `length` is a plain field, so a zero is not written -- and a block record is never zero
        // bytes, since it always carries a header. Zero therefore reads as "the carried block
        // says it".
        length: if Some(address.length()) == implied_length {
            0
        } else {
            address.length()
        },
        block_id: address.block_id(),
        // The address stopped carrying one; the item beside it carries the id.
        object_id: None,
        // Deliberately dropped, exactly as the text encoding drops it: the item carries the
        // routing bucket, and `resolved_address` puts it back. Writing it here made the two
        // encodings of one record decode differently, which is a divergence whether or not
        // anything currently reads the difference.
        routing_bucket: None,
        generation: address.generation(),
        // Derivable from the slab id, so the log stops restating it. The proto field keeps its
        // original name: `temporalstore.proto` is the wire, and nothing in this fold touches it.
        slab_id: None,
        // The digest, not its transcription. Half the bytes, same value.
        checksum: None,
        // In memory the digest is already the 32 bytes this field wants, so there is no
        // transcription left to undo.
        // The index does not hold a digest; the page envelope carries it.
        checksum_raw: None,
    }
}

fn address_from_proto(address: v1::WalBlockAddress, implied_length: Option<u64>) -> ElementEntry {
    let length = if address.length == 0 {
        implied_length.unwrap_or_default()
    } else {
        address.length
    };
    // `address.routing_bucket` is not passed on: a `BlockAddress` no longer holds one. The proto
    // still carries it, and the WAL item it came from is the container that answers for it.
    ElementEntry::from_parts(
        address.block_slab_id,
        address.offset,
        length,
        address.block_id,
        address.object_id,
    )
}
/// The numeric key a component is carrying, if it is carrying one.
///
/// Returns the stored key and, for a context event, the entry id packed beside it. Anything whose
/// component is genuinely text -- a hash field, a hex-encoded set member, a packed zset score --
/// returns None and keeps its string.
fn numeric_component(kind: &str, component: Option<&str>) -> Option<(u64, Option<u64>)> {
    let component = component?;
    match kind {
        "context_event" => {
            if component.len() != 32 {
                return None;
            }
            let (stored, entry) = component.split_at(16);
            Some((
                u64::from_str_radix(stored, 16).ok()?,
                Some(u64::from_str_radix(entry, 16).ok()?),
            ))
        }
        "feature" | "context_index" | "context_audit" | "context_child" | "context_summary"
        | "context_compression" | "control_counter" | "control_change" => {
            Some((component.parse::<u64>().ok()?, None))
        }
        _ => None,
    }
}


/// The component text a reader rebuilds from the numeric fields, absent if there are none.
///
/// Shared with `item_from_proto` on purpose. The writer decides whether to drop the object id by
/// deriving it, and it has to derive against the component the READER will hold -- a numeric
/// component does not round trip character for character, since `007` is written as the number
/// seven and read back as `7`. Deriving against the pre-trip form would drop an id that then
/// comes back different.
fn numeric_component_text(timestamp_ms: Option<u64>, entry_id: Option<u64>) -> Option<String> {
    match (timestamp_ms, entry_id) {
        (Some(stored), Some(entry)) => Some(format!("{stored:016x}{entry:016x}")),
        (Some(stored), None) => Some(stored.to_string()),
        (None, _) => None,
    }
}

/// The object id an item has to write, or absent when the rest of the item already says it.
///
/// An object id is `stable_block_object_id` of the shard, the kind and the key -- and an item
/// carries the last two while the record carries the shard. So the nine bytes a `fixed64` takes are
/// nine bytes restating a hash of the fields sitting beside it.
///
/// The component was a fourth term until #2019. It is still a field of the item; it is no longer a
/// term of this id.
///
/// Dropped only when the derivation AGREES with what the item holds. An id that came from
/// anywhere else is written out, so being wrong here costs bytes rather than correctness -- the
/// same rule the address, the key handle and the size already follow.
/// The key a record's own command already names, if it names one.
///
/// `command_object_keys` is the authoritative extractor -- the same one the write barrier and
/// dirty tracking delegate to -- so this cannot drift from what the engine considers a command's
/// key. It returns several for the commands that touch associated records; the first is the
/// primary, and a mismatch simply means nothing is stripped.
pub(crate) fn command_object_key(command: Option<&crate::types::Command>) -> Option<String> {
    command.and_then(|command| {
        crate::engine::command_object_keys(command)
            .into_iter()
            .next()
    })
}

/// The key a record's items may omit, which is only when the record carries ONE of them.
///
/// Measured, because the obvious version is wrong. Eight outcomes sharing a key encode to 88
/// bytes unstripped and 119 stripped, with compression on: zstd collapses the repeated key to
/// almost nothing, and removing the repetition takes away the redundancy it was feeding on. One
/// outcome gives compression nothing to find, and there the strip is 19 bytes off 57.
///
/// So the same "exactly one" rule the implied block length and the implied object id use here,
/// arrived at from the other direction: not because more than one is ambiguous, but because more
/// than one is already cheap.
fn strippable_command_key(record: &WriteAheadLogRecord) -> Option<String> {
    (record.outcomes.len() == 1)
        .then(|| command_object_key(record.command.as_ref()))
        .flatten()
}

/// The object key to put on the wire: absent when the record's command already says it.
///
/// A write stages its outcomes against the key the command names, so on the ordinary path these
/// are the same string and the item can stop repeating it. They are NOT always the same -- an
/// outcome staged against an associated record carries its own key -- so this compares rather
/// than assumes, and being wrong costs bytes instead of correctness.
fn item_object_key_to_write<'a>(
    item: &'a WalOutcomeItem,
    command_key: Option<&str>,
) -> Option<&'a str> {
    (command_key != Some(item.object_key.as_str())).then_some(item.object_key.as_str())
}

fn item_object_id_to_write(item: &WalOutcomeItem, shard_id: crate::types::ShardId) -> Option<u64> {
    // AS THE READER WILL SEE IT: a coded kind comes back through the code table, so the id is
    // derived from the round-tripped spelling rather than from this item's own.
    //
    // THE COMPONENT USED TO BE RECONSTRUCTED HERE TOO, and it no longer is, because the id no
    // longer takes one. That reconstruction existed only to feed this hash: a numeric component
    // travels as a number and returns as text, so the writer had to rebuild the exact string the
    // reader would rebuild or the two would derive different ids for the same page and this
    // omission would stop being safe. With the component out of the identity the whole round-trip
    // concern is gone -- the kind is the only term left that changes spelling on the wire.
    let kind = kind_code(&item.kind)
        .and_then(kind_from_code)
        .unwrap_or(item.kind.as_str());
    let derived = crate::engine::hashing::stable_block_object_id(shard_id, kind, &item.object_key);
    (item.object_id != derived).then_some(item.object_id)
}

pub(crate) fn item_to_proto(
    item: &WalOutcomeItem,
    shard_id: crate::types::ShardId,
    implied_length: Option<u64>,
    command_key: Option<&str>,
) -> v1::EngineWalItem {
    let block = item.address.as_ref().map(|address| {
        let mut encoded = address_to_proto(address, implied_length);
        // The item already says this. Repeating it costs a full varint on every item whose page
        // belongs to one object, which is every kind that is not timestamped.
        if encoded.object_id == Some(item.object_id) {
            encoded.object_id = None;
        }
        encoded
    });
    v1::EngineWalItem {
        item_kind: 0,
        model: 0,
        object_key: item_object_key_to_write(item, command_key).map(str::to_string),
        bucket_id: None,
        object_id: item_object_id_to_write(item, shard_id),
        block_id: None,
        ttl_ms: item.ttl,
        deleted: item.deleted,
        meta_log: item.meta,
        block_log: false,
        // A component that IS a number travels as one. What stays in `component` is what the
        // field is actually for: a hash field, a set member, a packed score.
        component: match numeric_component(&item.kind, item.component.as_deref()) {
            Some(_) => None,
            None => item.component.clone(),
        },
        timestamp_ms: numeric_component(&item.kind, item.component.as_deref()).map(|(key, _)| key),
        entry_id: numeric_component(&item.kind, item.component.as_deref()).and_then(|(_, id)| id),
        // A removal with no component is the whole object, which is now said rather than implied.
        object_deleted: item.deleted && item.component.is_none(),
        block,
        value: item.value.clone(),
        // The engine names more kinds than the enum does, and the name is what the apply path
        // dispatches on, so it is carried literally rather than squeezed through a lossy mapping.
        // A known kind travels as a code; anything else keeps its name.
        kind_name: match kind_code(&item.kind) {
            Some(_) => None,
            None => Some(item.kind.clone()),
        },
        kind_code: kind_code(&item.kind),
        routing_bucket: Some(item.routing_bucket),
    }
}

/// Bytes an outcome item occupies as an `EngineWalItem` body.
///
/// Paired with `put_wal_item` below, which must write exactly this many. The pair exists so the
/// item's `value` reaches the frame without being copied into a proto message first -- the same
/// reason `staged_blocks` has a hand-written encoder.
///
/// The presence rules are the fiddly part and are the reason this is checked byte for byte against
/// `item_to_proto`. Fields 3, 5, 7, 11, 13, 14, 15, 16, 17 and 19 are proto3 `optional`, which means
/// explicit presence: `Some(0)` and `Some("")` are WRITTEN. Fields 8, 9 and 18 are plain `bool`,
/// which means a false is omitted. The helpers in `raft::wal_proto` implement the plain rule, so
/// they are deliberately not used for the optional fields.
fn wal_item_body_len(item: &WalOutcomeItem, derived: &DerivedItem<'_>) -> usize {
    let mut len = optional_bytes_len(3, derived.object_key.map(str::len))
        + optional_fixed64_len(5, derived.object_id)
        + optional_varint_len(7, item.ttl)
        + plain_bool_len(8, item.deleted)
        + plain_bool_len(9, item.meta)
        + optional_bytes_len(11, derived.component.map(str::len));
    if let Some(block) = derived.block.as_ref() {
        len += crate::raft::wal_proto::len_delimited_len(12, block.encoded_len());
    }
    len += optional_bytes_len(13, item.value.as_ref().map(Vec::len))
        + optional_bytes_len(14, derived.kind_name.map(str::len))
        + optional_varint_len(15, Some(u64::from(item.routing_bucket)))
        + optional_varint_len(16, derived.timestamp_ms)
        + optional_varint_len(17, derived.entry_id)
        + plain_bool_len(18, derived.object_deleted)
        + optional_varint_len(19, derived.kind_code.map(u64::from));
    len
}

/// Write an outcome item as field `tag`, its key, component, value and kind name borrowed.
fn put_wal_item(tag: u32, item: &WalOutcomeItem, derived: &DerivedItem<'_>, out: &mut Vec<u8>) {
    let body = wal_item_body_len(item, derived);
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body as u64, out);

    // Ascending field order, because that is the order prost writes them in.
    put_optional_bytes(3, derived.object_key.map(str::as_bytes), out);
    put_optional_fixed64(5, derived.object_id, out);
    put_optional_varint(7, item.ttl, out);
    put_plain_bool(8, item.deleted, out);
    put_plain_bool(9, item.meta, out);
    put_optional_bytes(11, derived.component.map(str::as_bytes), out);
    if let Some(block) = derived.block.as_ref() {
        prost::encoding::encode_key(12, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(block.encoded_len() as u64, out);
        block.encode_raw(out);
    }
    put_optional_bytes(13, item.value.as_deref(), out);
    put_optional_bytes(14, derived.kind_name.map(str::as_bytes), out);
    put_optional_varint(15, Some(u64::from(item.routing_bucket)), out);
    put_optional_varint(16, derived.timestamp_ms, out);
    put_optional_varint(17, derived.entry_id, out);
    put_plain_bool(18, derived.object_deleted, out);
    put_optional_varint(19, derived.kind_code.map(u64::from), out);
}

/// What `item_to_proto` derives, computed once instead of three times.
struct DerivedItem<'a> {
    object_key: Option<&'a str>,
    object_id: Option<u64>,
    component: Option<&'a str>,
    kind_name: Option<&'a str>,
    kind_code: Option<u32>,
    timestamp_ms: Option<u64>,
    entry_id: Option<u64>,
    object_deleted: bool,
    block: Option<v1::WalBlockAddress>,
}

fn derive_item<'a>(
    item: &'a WalOutcomeItem,
    shard_id: crate::types::ShardId,
    implied_length: Option<u64>,
    command_key: Option<&str>,
) -> DerivedItem<'a> {
    // `item_to_proto` calls this three times for three fields; it is one answer.
    let numeric = numeric_component(&item.kind, item.component.as_deref());
    let code = kind_code(&item.kind);
    DerivedItem {
        object_key: item_object_key_to_write(item, command_key),
        object_id: item_object_id_to_write(item, shard_id),
        component: match numeric {
            Some(_) => None,
            None => item.component.as_deref(),
        },
        kind_name: match code {
            Some(_) => None,
            None => Some(item.kind.as_str()),
        },
        kind_code: code,
        timestamp_ms: numeric.map(|(key, _)| key),
        entry_id: numeric.and_then(|(_, id)| id),
        object_deleted: item.deleted && item.component.is_none(),
        block: item.address.as_ref().map(|address| {
            let mut encoded = address_to_proto(address, implied_length);
            if encoded.object_id == Some(item.object_id) {
                encoded.object_id = None;
            }
            encoded
        }),
    }
}

// Presence-aware field helpers. `optional` in proto3 means Some(0) and Some("") are written; the
// helpers in `raft::wal_proto` implement the PLAIN rule, where a default is omitted, and using them
// here would silently drop those values.
fn optional_varint_len(tag: u32, value: Option<u64>) -> usize {
    match value {
        Some(value) => {
            prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(value)
        }
        None => 0,
    }
}

fn optional_fixed64_len(tag: u32, value: Option<u64>) -> usize {
    match value {
        Some(_) => crate::raft::wal_proto::fixed64_field_len(tag),
        None => 0,
    }
}

fn put_optional_fixed64(tag: u32, value: Option<u64>, out: &mut Vec<u8>) {
    if let Some(value) = value {
        crate::raft::wal_proto::put_fixed64_field(tag, value, out);
    }
}

fn put_optional_varint(tag: u32, value: Option<u64>, out: &mut Vec<u8>) {
    if let Some(value) = value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(value, out);
    }
}

fn optional_bytes_len(tag: u32, len: Option<usize>) -> usize {
    match len {
        Some(len) => crate::raft::wal_proto::len_delimited_len(tag, len),
        None => 0,
    }
}

fn put_optional_bytes(tag: u32, payload: Option<&[u8]>, out: &mut Vec<u8>) {
    if let Some(payload) = payload {
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(payload.len() as u64, out);
        out.extend_from_slice(payload);
    }
}

fn plain_bool_len(tag: u32, value: bool) -> usize {
    if value {
        prost::encoding::key_len(tag) + 1
    } else {
        0
    }
}

fn put_plain_bool(tag: u32, value: bool, out: &mut Vec<u8>) {
    if value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(1, out);
    }
}

pub(crate) fn item_from_proto(
    item: v1::EngineWalItem,
    shard_id: crate::types::ShardId,
    implied_length: Option<u64>,
    command_key: Option<&str>,
) -> WalOutcomeItem {
    let kind = item
        .kind_code
        .and_then(kind_from_code)
        .map(str::to_string)
        .or(item.kind_name)
        .unwrap_or_default();
    // Absent means the record's own command says it, which is the only thing it can mean:
    // the writer omits it exactly when the two agree. This stays FIRST because `object_id` is
    // derived from it below -- restoring later would derive an id against an empty key, which is
    // the same ordering trap the id itself documents.
    let object_key = item
        .object_key
        .or_else(|| command_key.map(str::to_string))
        .unwrap_or_default();
    // Prefer the numeric fields; fall back to the string a record written before this carries.
    let component = numeric_component_text(item.timestamp_ms, item.entry_id).or(item.component);
    // Absent means the item's own fields say it, which is the only thing it can mean: the writer
    // omits it exactly when the derivation agrees. This goes FIRST because the address is
    // restored from it below.
    let object_id = item.object_id.unwrap_or_else(|| {
        crate::engine::hashing::stable_block_object_id(
            shard_id,
            &kind,
            &object_key,
        )
    });
    WalOutcomeItem {
        kind,
        object_key,
        component,
        object_id,
        routing_bucket: item.routing_bucket.unwrap_or_default(),
        // The address carries no object id to restore: the item's own `object_id` above is the
        // only copy, which is what the encoder's omission was arranging for.
        address: item
            .block
            .map(|block| address_from_proto(block, implied_length)),
        value: item.value,
        ttl: item.ttl_ms,
        deleted: item.deleted,
        meta: item.meta_log,
    }
}

/// WHETHER THIS RECORD'S LEADER CLOCK STAMP HAS A READER.
///
/// `metadata.timestamp_ms` is the leader's wall clock at the moment of the write, and on the
/// branch that RE-EXECUTES a command it is load-bearing. `engine::lifecycle`'s replay loop pins
/// the thread clock to it before executing, so a relative TTL in the command resolves to the
/// deadline the leader logged rather than to one extended by the restart clock -- the comment on
/// that line calls it "resolve-then-log", and `wal_replay_reads_an_unstamped_record_against_the_live_clock`
/// holds both halves of the rule.
///
/// A RECORD CARRYING OUTCOMES NEVER REACHES THAT LINE. The same loop installs the recorded items
/// and `continue`s above `set_replay_clock_ms`, because an installed outcome already carries the
/// absolute deadline the leader resolved; nothing about it is re-derived from a clock. So on the
/// shape this log writes by default -- `append_outcomes_locked` sets no command at all, "the items
/// say what the batch did" -- the stamp was six or seven fsynced bytes per append with no reader.
///
/// # WHY THIS NEEDS NO FORMAT MARKER AND NO MIGRATION
///
/// Absence already has a defined meaning that is already implemented. The decoder copies this
/// field verbatim, so an omitted stamp arrives as zero, and the replay loop reads a zero as "this
/// record never carried one, leave the live clock in charge" -- `.filter(|stamp| *stamp > 0)`,
/// added for records written before the log carried a stamp. A record written by this build is
/// therefore read correctly by a build that predates this change, and one written before it keeps
/// its stamp and is read as it always was.
///
/// # WHAT WOULD MAKE THIS WRONG
///
/// Replay starting to resolve anything against the clock on the OUTCOME branch. The guard for
/// that is `only_the_shape_that_re_executes_carries_the_leader_stamp`, which asserts both arms:
/// the stamp present where it is read and absent where it is not. If that test fails because
/// replay has changed, this function is the thing to change, not the test.
pub(crate) fn leader_stamp_has_a_reader(record: &WriteAheadLogRecord) -> bool {
    record.outcomes.is_empty()
}

fn metadata_to_proto(
    metadata: &WriteAheadLogRecordMetadata,
    stamp_has_a_reader: bool,
) -> Result<v1::EngineWalMetadata, String> {
    // The descriptive item list travels verbatim rather than field by field. It is off by default,
    // every field of it is derived from the command beside it, and modelling it would risk losing
    // one for no measurable saving.
    let items = if metadata.items.is_empty() {
        Vec::new()
    } else {
        vec![v1::EngineWalItem {
            item_kind: 0,
            model: 0,
            object_key: None,
            bucket_id: None,
            object_id: None,
            block_id: None,
            ttl_ms: None,
            deleted: false,
            meta_log: false,
            block_log: false,
            component: None,
            block: None,
            value: Some(serde_json::to_vec(&metadata.items).map_err(|err| err.to_string())?),
            kind_name: Some(String::from("__verbatim_items")),
            kind_code: None,
            routing_bucket: None,
            timestamp_ms: None,
            entry_id: None,
            object_deleted: false,
        }]
    };
    Ok(v1::EngineWalMetadata {
        // Omitted while it is the current version, which is exactly what the record's own serde
        // form already does: "a record that does not say otherwise is current". The binary form
        // said it on every record, so one encoding of a record stated something the other left
        // out. `version` is a plain proto3 field, so a zero is not written at all.
        version: if metadata.version == crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION {
            0
        } else {
            metadata.version
        },
        // Written only where replay reads it. See [`leader_stamp_has_a_reader`]: a plain proto3
        // field, so a zero is not written at all, and a zero is already read as "no stamp".
        timestamp_ms: if stamp_has_a_reader {
            metadata.timestamp_ms
        } else {
            0
        },
        items,
        batch_id: metadata.batch_id,
        batch_size: metadata.batch_size,
        batch_index: metadata.batch_index,
    })
}

fn metadata_from_proto(
    metadata: v1::EngineWalMetadata,
) -> Result<WriteAheadLogRecordMetadata, String> {
    let items = match metadata.items.into_iter().next() {
        Some(carried) if carried.kind_name.as_deref() == Some("__verbatim_items") => {
            serde_json::from_slice(&carried.value.unwrap_or_default())
                .map_err(|err| err.to_string())?
        }
        _ => Vec::new(),
    };
    Ok(WriteAheadLogRecordMetadata {
        // Absent means current, the reading half of the rule above. No record has ever been
        // written with a zero here, so nothing legitimate is being read as current.
        version: if metadata.version == 0 {
            crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION
        } else {
            metadata.version
        },
        timestamp_ms: metadata.timestamp_ms,
        items,
        batch_id: metadata.batch_id,
        batch_size: metadata.batch_size,
        batch_index: metadata.batch_index,
    })
}

/// The record split into the part written by hand and the part left to the generated encoder.
///
/// Fields one to three are written here; four to six are still a `prost` message. The split is
/// what lets the command borrow: only the command carries a payload worth not copying, and it is
/// the one field the generated encoder cannot be handed a borrow of.
struct RecordParts<'a> {
    command: Option<crate::raft::wal_proto::CommandEncoding<'a>>,
    /// The outcome items, derived from the record but not copied out of it.
    items: Vec<DerivedItem<'a>>,
    /// Fields four to six only. proto3 omits a scalar holding its default and `command` is None,
    /// so encoding this writes the tail and nothing else -- which is why the bytes come out in
    /// tag order and identical to encoding the whole record at once.
    tail: v1::EngineWalRecord,
    len: usize,
}

fn record_parts(record: &WriteAheadLogRecord) -> Result<RecordParts<'_>, String> {
    let command = match record.command.as_ref() {
        Some(command) => Some(
            crate::raft::wal_proto::command_encoding(command).map_err(|err| err.to_string())?,
        ),
        None => None,
    };
    let tail = v1::EngineWalRecord {
        shard_id: 0,
        sequence: 0,
        command: None,
        metadata: record
            .metadata
            .as_ref()
            .map(|metadata| metadata_to_proto(metadata, leader_stamp_has_a_reader(record)))
            .transpose()?,
        // Written by hand below, from the outcomes themselves, for the reason the staged blocks
        // are: filling this copied every outcome's `value` into a proto message before the encoder
        // copied it again into the frame, and the bytes allocated per append tracked the outcome
        // value one for one. `the_hand_written_item_is_the_same_item` holds the hand-written form
        // against this one -- which the test helper `owned_bytes` still builds -- byte for byte.
        items: Vec::new(),
        // Written by hand below, from the pages themselves. A staged page is a whole page, and
        // filling this field copied every one of them before the encoder copied them again.
        staged_blocks: Vec::new(),
    };
    let implied_length = implied_block_length(record);
    let command_key = strippable_command_key(record);
    let items = record
        .outcomes
        .iter()
        .map(|item| derive_item(item, record.shard_id, implied_length, command_key.as_deref()))
        .collect::<Vec<_>>();
    let items_len = record
        .outcomes
        .iter()
        .zip(items.iter())
        .map(|(item, derived)| {
            crate::raft::wal_proto::len_delimited_len(5, wal_item_body_len(item, derived))
        })
        .sum::<usize>();
    let len = crate::raft::wal_proto::varint_field_len(1, record.shard_id)
        + crate::raft::wal_proto::varint_field_len(2, record.sequence)
        + command.as_ref().map_or(0, |command| command.encoded_len_at(3))
        + tail.encoded_len()
        + items_len
        + record
            .staged_blocks
            .iter()
            .map(|block| {
                crate::raft::wal_proto::len_delimited_len(
                    6,
                    staged_block_body_len(block, implied_staged_object_id(record)),
                )
            })
            .sum::<usize>();
    Ok(RecordParts {
        command,
        items,
        tail,
        len,
    })
}

impl RecordParts<'_> {
    fn put(&self, record: &WriteAheadLogRecord, out: &mut Vec<u8>) -> Result<(), String> {
        crate::raft::wal_proto::put_varint_field(1, record.shard_id, out);
        crate::raft::wal_proto::put_varint_field(2, record.sequence, out);
        if let Some(command) = self.command.as_ref() {
            command.put_at(3, out);
        }
        // Ascending field order, the order prost would have written them: the tail carries field
        // four, the outcomes are field five, the staged blocks are field six.
        self.tail.encode(out).map_err(|err| err.to_string())?;
        for (item, derived) in record.outcomes.iter().zip(self.items.iter()) {
            put_wal_item(5, item, derived, out);
        }
        let implied = implied_staged_object_id(record);
        for block in &record.staged_blocks {
            crate::raft::wal_proto::put_staged_block(6, block, implied, out);
        }
        Ok(())
    }
}

/// The object id a staged block need not repeat, if the record already says it once.
///
/// Only when the record carries exactly one outcome and one block: then the block belongs to
/// that outcome and nothing else, so dropping the id from the block loses nothing a reader
/// cannot put back. With more than one of either, every block writes its own.
pub(crate) fn implied_staged_object_id(record: &WriteAheadLogRecord) -> Option<u64> {
    if record.outcomes.len() == 1 && record.staged_blocks.len() == 1 {
        return Some(record.outcomes[0].object_id);
    }
    None
}

/// Bytes a staged page occupies as a `WalStagedBlock` body.
///
/// Deliberately a call to the writer's own measurement rather than a second copy of the rule.
/// This length reserves the buffer the writer then fills, so if the two ever disagree the record
/// is written into the wrong number of bytes -- and they did disagree, because this was a
/// separate copy that still measured a varint after the writer had moved to a fixed field.
fn staged_block_body_len(block: &crate::wal::StagedBlock, implied: Option<u64>) -> usize {
    crate::raft::wal_proto::staged_block_body_len(block, implied)
}

/// A record measured but not yet written.
///
/// `encode` below builds the payload into its own buffer and the framing layer then copies it
/// into a second one, so a write carries the record twice. Handing the framing layer a length and
/// a writer instead lets it reserve once and have the payload land in the bytes that go to disk.
///
/// The length has to be exact, which is the whole reason this can exist: `RecordParts::len` is
/// asserted equal to the bytes written, for every arm, by
/// `the_borrowing_encoder_writes_the_same_bytes`.
pub(crate) struct PreparedRecord<'a> {
    parts: RecordParts<'a>,
}

impl PreparedRecord<'_> {
    /// Bytes the payload will occupy: the marker, then the record.
    pub(crate) fn payload_len(&self) -> usize {
        self.parts.len + 1
    }

    pub(crate) fn put(
        &self,
        record: &WriteAheadLogRecord,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        out.push(RAW_PAYLOAD_MARKER);
        self.parts.put(record, out)
    }
}

/// Measure a record so it can be framed without an intermediate buffer.
pub(crate) fn prepare(record: &WriteAheadLogRecord) -> Result<PreparedRecord<'_>, String> {
    Ok(PreparedRecord {
        parts: record_parts(record)?,
    })
}

/// Encode a record as protobuf, marker byte first.
pub(crate) fn encode(record: &WriteAheadLogRecord) -> Result<Vec<u8>, String> {
    // Few calls and the most bytes of any class: the record is built as whole buffers, so an
    // allocation COUNT reports this as very nearly free while the byte column says it is the
    // largest single thing a write allocates.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::LogRecord, || {
        encode_inner(record)
    })
}

fn encode_inner(record: &WriteAheadLogRecord) -> Result<Vec<u8>, String> {
    let parts = record_parts(record)?;
    if compress_records_enabled() {
        // Compression cannot use the borrowing writer above: that path reserves the frame from
        // `payload_len()` before the payload exists, and how long a compressed payload will be is
        // not knowable until it has been compressed. So this arm builds the payload first and the
        // frame around it, which is what the escaping arm below has always done.
        //
        // The buffer it builds into is kept between records rather than allocated per append. At a
        // four-kilobyte record that allocation measured 4,682 B, nearly half of everything this
        // path allocated, and none of it had to be new: the bytes are consumed before `encode`
        // returns.
        let compressed = ENCODE_SCRATCH.with(|cell| -> Result<Option<Vec<u8>>, String> {
            let mut scratch = cell.borrow_mut();
            scratch.clear();
            scratch.reserve(parts.len);
            parts.put(record, &mut scratch)?;
            let compressed = compress_payload(&scratch);
            if scratch.capacity() > MAX_ENCODE_SCRATCH_BYTES {
                // One outsized record should not leave every thread that saw it holding the buffer.
                *scratch = Vec::new();
            }
            Ok(compressed)
        })?;
        if let Some(compressed) = compressed {
            let escaping = !crate::log_framing::binary_frame_enabled();
            let mut out = Vec::with_capacity(compressed.len() + 8);
            out.push(if escaping {
                COMPRESSED_ESCAPED_PAYLOAD_MARKER
            } else {
                COMPRESSED_RAW_PAYLOAD_MARKER
            });
            if escaping {
                out.extend_from_slice(&escape_newlines(&compressed));
            } else {
                out.extend_from_slice(&compressed);
            }
            return Ok(out);
        }
        // Not worth compressing. Fall through and write it the way it would have been written
        // anyway, under its own marker -- a reader cannot tell that this record was considered.
    }
    if crate::log_framing::binary_frame_enabled() {
        // The frame declares its own length, so the payload is written as produced -- which means
        // the marker can go in FIRST and the message encode straight after it. `encode` appends,
        // so there is no second buffer and no second copy of the payload. That copy was the whole
        // record again: at a four-kilobyte value it was four kilobytes to prepend one byte.
        let mut out = Vec::with_capacity(parts.len + 1);
        out.push(RAW_PAYLOAD_MARKER);
        parts.put(record, &mut out)?;
        return Ok(out);
    }
    // The escaping fallback still needs the payload on its own, because escaping rewrites it.
    let mut encoded = Vec::with_capacity(parts.len);
    parts.put(record, &mut encoded)?;
    let mut out = Vec::with_capacity(encoded.len() + 8);
    out.push(BINARY_PAYLOAD_MARKER);
    out.extend_from_slice(&escape_newlines(&encoded));
    Ok(out)
}

/// Decode a payload this module wrote. The caller has already checked the marker byte.
pub(crate) fn decode(payload: &[u8]) -> Result<WriteAheadLogRecord, String> {
    let body = &payload[1..];
    let marker = payload.first().copied();
    let escaped = marker == Some(BINARY_PAYLOAD_MARKER)
        || marker == Some(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
    let compressed = marker == Some(COMPRESSED_RAW_PAYLOAD_MARKER)
        || marker == Some(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
    let unescaped;
    let framed: &[u8] = if escaped {
        unescaped = unescape_newlines(body)?;
        unescaped.as_slice()
    } else {
        body
    };
    let inflated;
    let bytes: &[u8] = if compressed {
        inflated = decompress_payload(framed)?;
        inflated.as_slice()
    } else {
        framed
    };
    let message = v1::EngineWalRecord::decode(bytes).map_err(|err| err.to_string())?;
    // Absent is a legitimate record now, not a malformed one: it carries results instead.
    let command = match message.command.and_then(|command| command.kind) {
        Some(kind) => Some(
            crate::raft::wal_proto::command_from_proto(kind).map_err(|err| err.to_string())?,
        ),
        None => None,
    };
    // The carried block states the length its address covers, so it is read off the block
    // before the items that point at it. Same shape as the implied object id below, and the same
    // one-of-each rule.
    let implied_length = if message.items.len() == 1 && message.staged_blocks.len() == 1 {
        let carried =
            message.staged_blocks[0].block.len() + crate::block_store::BLOCK_RECORD_HEADER_LEN;
        Some(carried as u64)
    } else {
        None
    };
    // The command is decoded above, so a key an item did not write is available here. Same
    // shape as `implied_length`: read off the record, handed to every item.
    let command_key = command_object_key(command.as_ref());
    let outcomes = message
        .items
        .into_iter()
        .map(|item| {
            item_from_proto(item, message.shard_id, implied_length, command_key.as_deref())
        })
        .collect::<Vec<_>>();
    // A block with no object id of its own takes it from the outcome it belongs to. The writer
    // drops it only when there is exactly one of each, so there is never a question of which
    // outcome an unlabelled block means.
    //
    // Taken from the RESTORED outcome rather than from the wire field, and that is the whole
    // ordering constraint of this change: an outcome may not have written its id either, so a
    // wire field that is absent on both would leave the block holding nothing.
    let implied = if outcomes.len() == 1 && message.staged_blocks.len() == 1 {
        Some(outcomes[0].object_id)
    } else {
        None
    };
    Ok(WriteAheadLogRecord {
        shard_id: message.shard_id,
        sequence: message.sequence,
        command,
        metadata: message.metadata.map(metadata_from_proto).transpose()?,
        staged_blocks: message
            .staged_blocks
            .into_iter()
            .map(|block| StagedBlock {
                object_id: block.object_id.or(implied).unwrap_or_default(),
                // Never defaulted the way the object id is. An absent component MEANS the page is
                // its whole object, and inventing one would make a string page claim to be an
                // element; an absent one on a record written before this field existed means the
                // same thing that record has always meant.
                component: block.component.map(std::sync::Arc::from),
                bytes: block.block,
            })
            .collect(),
        outcomes,
    })
}


#[cfg(test)]
mod command_key_tests {
    use super::*;
    use crate::types::Command;
    use crate::wal::{WalOutcomeItem, WriteAheadLogRecord};

    fn outcome(key: &str, component: Option<&str>) -> WalOutcomeItem {
        WalOutcomeItem {
            kind: "string".to_string(),
            object_key: key.to_string(),
            component: component.map(str::to_string),
            object_id: crate::engine::hashing::stable_block_object_id(
                7,
                "string",
                key,
            ),
            routing_bucket: 11,
            address: None,
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        }
    }

    fn record(command: Option<Command>, outcomes: Vec<WalOutcomeItem>) -> WriteAheadLogRecord {
        WriteAheadLogRecord {
            shard_id: 7,
            sequence: 3,
            command,
            metadata: None,
            staged_blocks: Vec::new(),
            outcomes,
        }
    }

    /// An outcome staged against its own command's key stops repeating it, and comes back whole.
    ///
    /// The round trip is the point, not the saving: `object_id` is DERIVED from `object_key`, so
    /// a key restored after the derivation would produce a different id and a silently wrong
    /// index. Comparing the decoded item field for field is what catches that.
    #[test]
    fn an_outcome_drops_the_key_its_command_already_names() {
        let key = "tenant/1/object/9";
        let with_command = record(
            Some(Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            }),
            vec![outcome(key, None)],
        );
        // Premise first: the extractor has to return the very string the outcome carries, or
        // nothing is stripped and every byte figure below is measuring something else.
        assert_eq!(
            command_object_key(with_command.command.as_ref()).as_deref(),
            Some(key),
            "the command-key extractor does not return the outcome's key"
        );
        let encoded = encode(&with_command).expect("encodes");

        // The control differs in ONE thing: the outcome names a key the command does not, so the
        // strip cannot fire. Same command, same key LENGTH. A record with no command at all
        // would have measured the command's absence, which is larger than the key it saves --
        // that was the first version of this fixture, and it compared two differences at once.
        let unstripped = record(
            Some(Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            }),
            vec![outcome("tenant/1/object/8", None)],
        );
        let bare = encode(&unstripped).expect("encodes");
        assert!(
            encoded.len() < bare.len(),
            "stripping saved nothing: {} bytes against {}",
            encoded.len(),
            bare.len()
        );
        eprintln!(
            "\n  one outcome: {} B when the command names the key, {} B when it does not \
             ({} B saved)\n",
            encoded.len(),
            bare.len(),
            bare.len() - encoded.len()
        );

        let decoded = decode(&encoded).expect("record round-trips");
        assert_eq!(decoded.outcomes.len(), 1);
        assert_eq!(decoded.outcomes[0].object_key, key, "the key must come back");
        assert_eq!(
            decoded.outcomes[0].object_id, with_command.outcomes[0].object_id,
            "the id derives from the key, so a late restore changes it"
        );
    }

    /// An outcome whose key is NOT the command's still carries it.
    ///
    /// The rule compares rather than assumes, so a staged outcome against an associated record
    /// keeps its own key. Without this the strip would be a guess that silently rewrote keys.
    #[test]
    fn an_outcome_with_a_different_key_still_writes_it() {
        let with_command = record(
            Some(Command::StringSet {
                key: "tenant/1/object/9".to_string(),
                value: b"v".to_vec(),
            }),
            vec![outcome("tenant/1/object/OTHER", None)],
        );
        let encoded = encode(&with_command).expect("encodes");
        let decoded = decode(&encoded).expect("record round-trips");
        assert_eq!(
            decoded.outcomes[0].object_key, "tenant/1/object/OTHER",
            "an outcome that names a different key must keep it"
        );
        assert_eq!(
            decoded.outcomes[0].object_id, with_command.outcomes[0].object_id
        );
    }

    /// A record with several outcomes KEEPS its keys, because stripping them costs more.
    ///
    /// This is the opposite of what the change set out to do, and the measurement is why. A
    /// packed write stages one outcome per component, all naming the same key, and with
    /// compression on those repeats cost almost nothing: eight of them encode to 88 bytes
    /// unstripped and 119 stripped. Removing the repetition takes away exactly the redundancy
    /// zstd was feeding on.
    ///
    /// So the strip is limited to single-outcome records, and this pins that boundary from the
    /// other side: the keys must still be on the wire here, and the record must still round-trip.
    #[test]
    fn a_record_with_several_outcomes_keeps_its_keys() {
        let key = "tenant/1/object/9";
        let outcomes = (0..8)
            .map(|index| outcome(key, Some(&format!("c{index}"))))
            .collect::<Vec<_>>();
        let record = record(
            Some(Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            }),
            outcomes,
        );
        assert!(
            strippable_command_key(&record).is_none(),
            "a record with more than one outcome must not strip: compression already has it"
        );

        let encoded = encode(&record).expect("encodes");
        let decoded = decode(&encoded).expect("round-trips");
        assert_eq!(decoded.outcomes.len(), 8);
        for (index, item) in decoded.outcomes.iter().enumerate() {
            assert_eq!(item.object_key, key, "outcome {index} lost its key");
            assert_eq!(
                item.object_id, record.outcomes[index].object_id,
                "outcome {index} came back with a different id"
            );
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;
    use crate::wal::{StagedBlock, WriteAheadLogRecord, WriteAheadLogRecordMetadata};

    /// The encoder this replaced: build the whole message, owning every payload, then serialise.
    ///
    /// Kept here because it is the specification. The hand-written head is only allowed to exist
    /// if it produces exactly these bytes -- a record already on disk is read by the generated
    /// decoder, and a head that decoded correctly but differed byte for byte would still be a
    /// format change, silently, on every record written from here on.
    fn owned_bytes(record: &WriteAheadLogRecord) -> Vec<u8> {
        let message = v1::EngineWalRecord {
            shard_id: record.shard_id,
            sequence: record.sequence,
            command: record.command.as_ref().map(|command| v1::WalCommand {
                kind: Some(crate::raft::wal_proto::command_to_proto(command).unwrap()),
            }),
            metadata: record
                .metadata
                .as_ref()
                .map(|metadata| metadata_to_proto(metadata, leader_stamp_has_a_reader(record)))
                .transpose()
                .unwrap(),
            items: record
                .outcomes
                .iter()
                .map(|item| {
                    item_to_proto(
                        item,
                        record.shard_id,
                        implied_block_length(record),
                        strippable_command_key(record).as_deref(),
                    )
                })
                .collect(),
            staged_blocks: record
                .staged_blocks
                .iter()
                .map(|block| v1::WalStagedBlock {
                    // The same rule the borrowing encoder applies, so the two agree byte for
                    // byte: a block drops the object id the record already says once.
                    object_id: crate::raft::wal_proto::staged_block_object_id(
                        block,
                        implied_staged_object_id(record),
                    ),
                    block: block.bytes.clone(),
                    routing_bucket: None,
                    component: block.component.as_deref().map(str::to_string),
                })
                .collect(),
        };
        let mut encoded = Vec::with_capacity(message.encoded_len());
        message.encode(&mut encoded).unwrap();
        encoded
    }

    /// A record big and repetitive enough to be worth compressing, which most real ones are.
    fn compressible_record() -> WriteAheadLogRecord {
        let mut record = record_with(Some(Command::StringSet {
            key: "compressible".to_string(),
            // Repetition is the point: a real record repeats field names, scope keys and policy
            // blocks, and a payload of one byte over and over would flatter the codec in a way
            // those do not. This alternates, so it is compressible without being trivial.
            value: (0..4096u32).map(|index| (index % 7) as u8).collect(),
        }));
        record.outcomes = vec![crate::wal::WalOutcomeItem {
            kind: "page".to_string(),
            object_key: "tenant/1/object/9".to_string(),
            component: Some("body".to_string()),
            object_id: 9,
            routing_bucket: 8539,
            address: None,
            value: Some(vec![3; 512]),
            ttl: Some(60_000),
            deleted: false,
            meta: false,
        }];
        record
    }

    /// Build the compressed payload by hand, so decoding is tested without touching the
    /// environment: what a reader must accept is a property of the bytes, not of a flag.
    fn compressed_payload(record: &WriteAheadLogRecord, escaping: bool) -> Vec<u8> {
        let mut encoded = Vec::new();
        record_parts(record).unwrap().put(record, &mut encoded).unwrap();
        let compressed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();
        let mut out = Vec::new();
        if escaping {
            out.push(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
            out.extend_from_slice(&escape_newlines(&compressed));
        } else {
            out.push(COMPRESSED_RAW_PAYLOAD_MARKER);
            out.extend_from_slice(&compressed);
        }
        out
    }

    /// The frames the append path writes declare their decompressed length.
    ///
    /// This is the positive control for the reused decompressor. `decompress_payload` falls back
    /// to the allocating decoder whenever a frame declares nothing, and that fallback is silent by
    /// design -- so if the append path ever stopped declaring a length, every read would quietly
    /// take the slow path and no test would fail. This is the test that would fail instead.
    #[test]
    fn the_frames_the_append_path_writes_declare_their_length() {
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();

        let written = compress_payload(&encoded).expect("a compressible record compresses");
        assert_eq!(
            declared_inflated_len(&written),
            Some(encoded.len()),
            "the frame must declare exactly the length the decompressor is sized for",
        );

        // The streaming encoder wrote every compressed record in a log older than this change, and
        // it declares nothing. That is the case the fallback exists for, not a defect.
        let streamed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();
        assert_eq!(
            declared_inflated_len(&streamed),
            None,
            "a streamed frame declares no length, so it must take the fallback",
        );
    }

    /// Both shapes of frame inflate to the same bytes, whichever path they take.
    #[test]
    fn a_frame_inflates_the_same_whether_or_not_it_declares_a_length() {
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();

        let declared = compress_payload(&encoded).expect("a compressible record compresses");
        let streamed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();

        assert_eq!(decompress_payload(&declared).unwrap(), encoded, "declared frame");
        assert_eq!(decompress_payload(&streamed).unwrap(), encoded, "streamed frame");
    }

    /// A declared length past the bound is refused, because the length came off disk.
    #[test]
    fn a_declared_length_past_the_bound_is_refused() {
        assert_eq!(capacity_within_bound(0), Some(0));
        assert_eq!(
            capacity_within_bound(MAX_REUSED_INFLATE_BYTES as u64),
            Some(MAX_REUSED_INFLATE_BYTES),
        );
        assert_eq!(capacity_within_bound(MAX_REUSED_INFLATE_BYTES as u64 + 1), None);
        assert_eq!(capacity_within_bound(u64::MAX), None);
    }

    /// What `encode` allocates, compressed against raw.
    ///
    /// The compressed arm builds three buffers per record: the payload, the compressed copy of it,
    /// and the output the compressed copy is then memcpy'd into. `compress_to_buffer` could put the
    /// compressor straight into the output and remove the middle one. This measures what that is
    /// worth before anyone writes it.
    #[test]
    #[ignore]
    #[cfg(feature = "alloc-probe")]
    fn what_encoding_a_record_allocates() {
        let record = compressible_record();
        let runs = 64usize;


        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "0");
        let raw_len = encode(&record).unwrap().len();
        let probe = crate::alloc_probe::Probe::start();
        for _ in 0..runs {
            std::hint::black_box(encode(std::hint::black_box(&record)).unwrap());
        }
        let raw = probe.stop();

        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "1");
        let squeezed_len = encode(&record).unwrap().len();
        let probe = crate::alloc_probe::Probe::start();
        for _ in 0..runs {
            std::hint::black_box(encode(std::hint::black_box(&record)).unwrap());
        }
        let squeezed = probe.stop();

        println!(
            "  ENCODE raw        {raw_len:>6} B out | {:>5.1} allocs {:>8.0} B per record",
            raw.allocs as f64 / runs as f64,
            raw.alloc_bytes as f64 / runs as f64,
        );
        println!(
            "  ENCODE compressed {squeezed_len:>6} B out | {:>5.1} allocs {:>8.0} B per record | the compress arm adds {:>5.1} allocs and {:>8.0} B",
            squeezed.allocs as f64 / runs as f64,
            squeezed.alloc_bytes as f64 / runs as f64,
