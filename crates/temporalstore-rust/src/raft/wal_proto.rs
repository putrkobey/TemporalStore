// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encode a WAL record as a compact binary message rather than text.
//!
//! The text encoding spells out every field name on every record and has no byte-string type, so
//! a binary value becomes an array of decimal numbers. Measured: a record carrying a 20-byte
//! payload cost 782 bytes, and one carrying a 1,034-byte payload cost 3,823 -- roughly three
//! bytes written for every byte of data. Every one of those bytes is paid twice: once writing it,
//! and again on each replay that parses it back.
//!
//! **Fidelity comes before compactness here.** This is the durability path, so a command this
//! module does not model explicitly is carried byte for byte in the previous encoding rather than
//! approximated by the nearest match. Modelled commands can then be added one at a time, and a
//! command this module has never heard of still replays exactly. The same applies to the record's
//! rarely-set fields, which travel whole in `rest`.

use std::io;

use prost::Message;

use super::*;
use crate::sdk::v1;

/// The record fields that sit at their default on nearly every record.
///
/// They are carried together in their existing encoding rather than modelled field by field:
/// modelling them would add a conversion to get wrong for each one, and save nothing on the
/// records that actually dominate the log, where every one of them is absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct RaftWalRecordRest {
    #[serde(default, skip_serializing_if = "RaftReplicaRole::is_default")]
    replica_role: RaftReplicaRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    joint_membership: Option<JointConsensusMembership>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_snapshot: Option<RaftSnapshot>,
    #[serde(default, skip_serializing_if = "RaftPeerPipelineRuntimeState::is_default")]
    pipeline_state: RaftPeerPipelineRuntimeState,
    #[serde(default, skip_serializing_if = "RaftReadSafetyRuntimeState::is_default")]
    read_safety_state: RaftReadSafetyRuntimeState,
    #[serde(default, skip_serializing_if = "RaftMembershipRuntimeEvidence::is_default")]
    membership_evidence: RaftMembershipRuntimeEvidence,
}

impl RaftWalRecordRest {
    fn is_empty(&self) -> bool {
        *self == RaftWalRecordRest::default()
    }
}

/// Split a checksum into the bytes to store and whether they are its text rather than its digest.
///
/// A checksum is a hex digest in practice, and storing the digest costs half what its text does.
/// Anything that is not a clean round-trip through hex is kept as its own bytes instead, so this
/// never has to assume what a checksum looks like in order to return it unchanged.
fn checksum_to_bytes(checksum: &str) -> (Vec<u8>, bool) {
    match decode_hex(checksum) {
        Some(digest) if encode_hex(&digest) == checksum => (digest, false),
        _ => (checksum.as_bytes().to_vec(), true),
    }
}

fn checksum_from_bytes(bytes: &[u8], is_text: bool) -> String {
    if is_text {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        encode_hex(bytes)
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 || text.is_empty() {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(text.len() / 2);
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Map a command onto its modelled form, or carry it verbatim.
///
/// A modelled arm is used ONLY when it can reproduce the command exactly. `StringSetEx` with a
/// zero TTL is the trap: the modelled form distinguishes the two commands by whether the TTL is
/// non-zero, so a zero-TTL `StringSetEx` would come back as a plain `StringSet`. It goes verbatim
/// instead -- a command that cannot round-trip through a modelled arm must not use one.
pub(crate) fn command_to_proto(command: &Command) -> io::Result<v1::wal_command::Kind> {
    Ok(match command_encoding(command)? {
        CommandEncoding::StringSet { key, value, ttl_ms } => {
            v1::wal_command::Kind::StringSet(v1::StringSet {
                key: key.to_owned(),
                value: value.to_vec(),
                ttl_ms,
            })
        }
        CommandEncoding::HashSet { key, field, value } => {
            v1::wal_command::Kind::HashSet(v1::HashSet {
                key: key.to_owned(),
                field: field.to_owned(),
                value: value.to_vec(),
            })
        }
        CommandEncoding::Owned(command) => command.kind.expect("verbatim arm always sets a kind"),
    })
}

/// A command in the shape it will be written in, borrowing its payload instead of owning a copy.
///
/// Building the owned message cost the payload twice on every write: once to fill the message and
/// once to serialise it. At a four-kilobyte value that is four kilobytes spent to describe four
/// kilobytes. The three modelled arms borrow their key and value here, so the write
/// copies them only into the buffer that goes to disk.
///
/// The verbatim arm still owns its bytes, and that is not an oversight: its JSON does not exist
/// until it is built, so there is nothing to borrow.
pub(crate) enum CommandEncoding<'a> {
    StringSet {
        key: &'a str,
        value: &'a [u8],
        ttl_ms: u64,
    },
    HashSet {
        key: &'a str,
        field: &'a str,
        value: &'a [u8],
    },
    Owned(v1::WalCommand),
}

/// Which arm a command takes. This is the ONLY place that decides, and `command_to_proto` is
/// written on top of it, so the two cannot answer differently -- the zero-TTL `StringSetEx` trap
/// documented above is decided once.
pub(crate) fn command_encoding(command: &Command) -> io::Result<CommandEncoding<'_>> {
    Ok(match command {
        Command::StringSet { key, value } => CommandEncoding::StringSet {
            key,
            value,
            ttl_ms: 0,
        },
        Command::StringSetEx { key, value, ttl_ms } if *ttl_ms > 0 => CommandEncoding::StringSet {
            key,
            value,
            ttl_ms: *ttl_ms,
        },
        Command::HashSet { key, field, value } => CommandEncoding::HashSet { key, field, value },
        other => CommandEncoding::Owned(v1::WalCommand {
            kind: Some(v1::wal_command::Kind::Verbatim(
                serde_json::to_vec(other).map_err(io::Error::other)?,
            )),
        }),
    })
}

/// Bytes a length-delimited field occupies: its key, its length, and its payload.
pub(crate) fn len_delimited_len(tag: u32, payload_len: usize) -> usize {
    prost::encoding::key_len(tag)
        + prost::encoding::encoded_len_varint(payload_len as u64)
        + payload_len
}

/// Bytes a varint field occupies, or none at all when the value is zero.
///
/// proto3 omits a scalar field holding its default, and the generated encoder does the same. A
/// hand-written field that emitted the zero would still decode, but it would no longer produce
/// the same bytes as the message it replaces -- which is the property the tests check.
pub(crate) fn varint_field_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        return 0;
    }
    prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(value)
}

/// A field whose value is a hash, and so has no small values to save on.
///
/// A varint is a saving only when the high bits are usually clear. An object id here is a hash:
/// measured over 10,000 records, every one of them needed 9 or 10 varint bytes, where fixed64
/// needs 8. For these fields the varint is a cost, not a saving.
pub(crate) fn fixed64_field_len(tag: u32) -> usize {
    prost::encoding::key_len(tag) + 8
}

pub(crate) fn put_fixed64_field(tag: u32, value: u64, out: &mut Vec<u8>) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::SixtyFourBit, out);
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_varint_field(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value == 0 {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
    prost::encoding::encode_varint(value, out);
}

/// Write a staged page as field `tag`, its bytes borrowed straight into `out`.
pub(crate) fn put_staged_block(
    tag: u32,
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
    out: &mut Vec<u8>,
) {
    let object_id = staged_block_object_id(block, implied_object_id);
    let body = staged_block_body_len(block, implied_object_id);
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body as u64, out);
    if let Some(object_id) = object_id {
        put_fixed64_field(1, object_id, out);
    }
    put_len_delimited(2, &block.bytes, out);
    // Ascending field order, the order prost would have written them, which is the property
    // `the_borrowing_encoder_writes_the_same_bytes` checks. Written unconditionally when present
    // rather than elided against an implied value the way the object id is: there is no second
    // copy of the component anywhere in the record to imply it from.
    if let Some(component) = block.component.as_deref() {
        // NOT `put_len_delimited`, which skips an empty payload. This field is `optional` in the
        // schema, so prost writes it for `Some("")` and omits it only for `None` -- and the
        // difference is load-bearing rather than cosmetic: a component that decoded back as `None`
        // would make an element page indistinguishable from the whole-object page of the same key,
        // which is the exact confusion this field exists to remove.
        prost::encoding::encode_key(4, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(component.len() as u64, out);
        out.extend_from_slice(component.as_bytes());
    }
}

/// The object id a staged block has to write, if any.
///
/// A record that carries one block for one outcome already says the object id once, in the
/// outcome. Writing it again in the block costs a second copy of a 64-bit hash on every write.
/// It is omitted only when there is exactly one of each and the two agree, so a reader never
/// has to guess which outcome an unlabelled block belongs to.
pub(crate) fn staged_block_object_id(
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
) -> Option<u64> {
    if implied_object_id == Some(block.object_id) {
        return None;
    }
    Some(block.object_id)
}

pub(crate) fn staged_block_body_len(
    block: &crate::wal::StagedBlock,
    implied_object_id: Option<u64>,
) -> usize {
    let id_len = match staged_block_object_id(block, implied_object_id) {
        Some(_) => fixed64_field_len(1),
        None => 0,
    };
    // This length RESERVES the buffer `put_staged_block` then fills, so the two must agree field
    // for field or the record is written into the wrong number of bytes -- which is exactly how
    // they disagreed once before, when this measured a varint after the writer had moved to a
    // fixed field. Every arm here mirrors a line there.
    id_len
        + if block.bytes.is_empty() {
            0
        } else {
            len_delimited_len(2, block.bytes.len())
        }
        + match block.component.as_deref() {
            // Counted for `Some("")` as well, because the writer above emits it: an `optional`
            // field is present or absent, not empty or absent.
            Some(component) => len_delimited_len(4, component.len()),
            None => 0,
        }
}

fn put_len_delimited(tag: u32, payload: &[u8], out: &mut Vec<u8>) {
    if payload.is_empty() {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

fn len_delimited_len_or_none(tag: u32, payload_len: usize) -> usize {
    if payload_len == 0 {
        return 0;
    }
    len_delimited_len(tag, payload_len)
}

impl CommandEncoding<'_> {
    /// Bytes of the `WalCommand` body -- the single oneof field, with no outer length.
    fn body_len(&self) -> usize {
        match self {
            Self::StringSet { key, value, ttl_ms } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, value.len())
                    + varint_field_len(3, *ttl_ms);
                len_delimited_len(1, inner)
            }
            Self::HashSet { key, field, value } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, field.len())
                    + len_delimited_len_or_none(3, value.len());
                len_delimited_len(2, inner)
            }
            Self::Owned(command) => prost::Message::encoded_len(command),
        }
    }

    /// Bytes this command occupies as field `tag` of the record that carries it.
    pub(crate) fn encoded_len_at(&self, tag: u32) -> usize {
        len_delimited_len(tag, self.body_len())
    }

    fn put_body(&self, out: &mut Vec<u8>) {
        match self {
            Self::StringSet { key, value, ttl_ms } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, value.len())
                    + varint_field_len(3, *ttl_ms);
                prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, out);
                prost::encoding::encode_varint(inner as u64, out);
                put_len_delimited(1, key.as_bytes(), out);
                put_len_delimited(2, value, out);
                put_varint_field(3, *ttl_ms, out);
            }
            Self::HashSet { key, field, value } => {
                let inner = len_delimited_len_or_none(1, key.len())
                    + len_delimited_len_or_none(2, field.len())
                    + len_delimited_len_or_none(3, value.len());
                prost::encoding::encode_key(2, prost::encoding::WireType::LengthDelimited, out);
                prost::encoding::encode_varint(inner as u64, out);
                put_len_delimited(1, key.as_bytes(), out);
                put_len_delimited(2, field.as_bytes(), out);
                put_len_delimited(3, value, out);
            }
            Self::Owned(command) => {
                prost::Message::encode_raw(command, out);
            }
        }
    }

    /// Write this command as field `tag`, payload borrowed straight into `out`.
    pub(crate) fn put_at(&self, tag: u32, out: &mut Vec<u8>) {
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(self.body_len() as u64, out);
        self.put_body(out);
    }
}

pub(crate) fn command_from_proto(kind: v1::wal_command::Kind) -> io::Result<Command> {
    Ok(match kind {
        v1::wal_command::Kind::StringSet(command) => {
            if command.ttl_ms > 0 {
                Command::StringSetEx {
                    key: command.key,
                    value: command.value,
                    ttl_ms: command.ttl_ms,
                }
            } else {
                Command::StringSet {
                    key: command.key,
                    value: command.value,
                }
            }
        }
        v1::wal_command::Kind::HashSet(command) => Command::HashSet {
            key: command.key,
            field: command.field,
            value: command.value,
        },
        v1::wal_command::Kind::Verbatim(bytes) => {
            serde_json::from_slice(&bytes).map_err(io::Error::other)?
        }
    })
}

fn entry_to_proto(entry: &RaftLogEntry) -> io::Result<v1::WalLogEntry> {
    Ok(v1::WalLogEntry {
        term: entry.term,
        index: entry.index,
        shard_id: entry.shard_id,
        command: Some(v1::WalCommand {
            kind: Some(command_to_proto(&entry.command)?),
        }),
        // Carried in BOTH directions here and in `entry_from_proto`. A binary record that dropped
        // it would restore a log whose entries resolve their deadlines against the restarting
        // node's clock, which is the divergence the field exists to close -- and it would do so
        // silently, because every other field round-trips.
        leader_time_ms: entry.leader_time_ms,
    })
}

fn entry_from_proto(entry: v1::WalLogEntry) -> io::Result<RaftLogEntry> {
    let kind = entry
        .command
        .and_then(|command| command.kind)
        .ok_or_else(|| io::Error::other("wal log entry is missing its command"))?;
    Ok(RaftLogEntry {
        leader_time_ms: entry.leader_time_ms,
        term: entry.term,
        index: entry.index,
        shard_id: entry.shard_id,
        command: command_from_proto(kind)?,
    })
}

/// Introduces a length-prefixed binary record. Chosen so it cannot be confused with either text
/// form: a record written before this change starts with `{`, and one wrapped in the integrity
/// envelope starts with `#`.
pub(super) const BINARY_RECORD_MAGIC: u8 = 0xA7;

/// Encode one record as it should appear in a segment file.
pub(super) fn encode_record_line(
    envelope: &RaftWalEnvelope,
    binary: bool,
) -> io::Result<Vec<u8>> {
    if !binary {
        let mut out = serde_json::to_vec(envelope).map_err(io::Error::other)?;
        out.push(b'\n');
        return Ok(out);
    }
    let payload = encode_envelope(envelope)?;
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(BINARY_RECORD_MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Read the next record, in whichever encoding it was written.
///
/// Returns how many bytes it occupied along with the record. `None` means what follows is not a
/// complete, decodable record -- a torn tail from a crash mid-append -- and the caller truncates
/// there, exactly as it did when every record was text.
pub(super) fn next_envelope(bytes: &[u8]) -> Option<(usize, RaftWalEnvelope)> {
    match bytes.first()? {
        &BINARY_RECORD_MAGIC => {
            let length_end = 5usize;
            if bytes.len() < length_end {
                return None;
            }
            let length =
                u32::from_le_bytes(bytes[1..length_end].try_into().ok()?) as usize;
            let end = length_end.checked_add(length)?;
            // A length running past the end of what was written is a torn tail, not a record.
            if bytes.len() < end {
                return None;
            }
            Some((end, decode_envelope(&bytes[length_end..end]).ok()?))
        }
        _ => {
            let line_len = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|position| position + 1)
                .unwrap_or(bytes.len());
            let raw_line = &bytes[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            Some((
                line_len,
                serde_json::from_slice::<RaftWalEnvelope>(line).ok()?,
            ))
        }
    }
}

/// Encode a replicated batch for the wire, behind the same magic byte the log uses.
///
/// A receiver tells the two encodings apart by the first byte: a text body always starts with
/// `{`, so a body starting with the magic can only be binary.
pub(super) fn encode_append_entries(
    request: &AppendEntriesRequest,
) -> io::Result<Vec<u8>> {
    let message = v1::RaftAppendEntriesRequest {
        shard_id: request.shard_id,
        term: request.term,
        leader_id: request.leader_id,
        target_id: request.target_id,
        prev_log_index: request.prev_log_index,
        prev_log_term: request.prev_log_term,
        entries: request
            .entries
            .iter()
            .map(entry_to_proto)
            .collect::<io::Result<Vec<_>>>()?,
        leader_commit: request.leader_commit,
        rpc: match &request.rpc {
            Some(rpc) => serde_json::to_vec(rpc).map_err(io::Error::other)?,
            None => Vec::new(),
        },
    };
    let mut out = Vec::with_capacity(message.encoded_len() + 1);
    out.push(BINARY_RECORD_MAGIC);
    message.encode(&mut out).map_err(io::Error::other)?;
    Ok(out)
}

/// Whether a body is the binary encoding rather than text.
pub(crate) fn is_binary_rpc(body: &[u8]) -> bool {
    body.first() == Some(&BINARY_RECORD_MAGIC)
}

/// Decode a replicated batch that arrived in the binary encoding.
pub(crate) fn decode_append_entries(body: &[u8]) -> io::Result<AppendEntriesRequest> {
    let message = v1::RaftAppendEntriesRequest::decode(&body[1..]).map_err(io::Error::other)?;
    Ok(AppendEntriesRequest {
        rpc: if message.rpc.is_empty() {
