// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage_config::effective_block_slab_target_bytes;

#[path = "block_store/block_store_paths.rs"]
mod paths;
mod slab_manifest;
mod slab_reports;
#[path = "block_store/block_store_append.rs"]
mod append;
#[path = "block_store/block_store_read.rs"]
mod read;
#[path = "block_store/block_store_gc.rs"]
mod gc;
mod slab_ids;
mod slab_backend;
pub use slab_backend::block_store_file_read_counts;
use slab_backend::{
    read_block_store_file, BlockStoreReadTally, LocalSlabBackend, SlabBackend,
};
#[path = "block_store/block_store_record.rs"]
mod record;

/// Bytes a block record spends on its header, before the block's own bytes.
///
/// Re-exported rather than redefined. This was a second constant whose entire body was the
/// first one, written to widen a `pub(super)` definition so a log that CARRIES a block could
/// work out the length its address will hold. Now that both spell the record the same way, two
/// constants of one name in two modules is a thing to trip over rather than a bridge.
pub(crate) use record::BLOCK_RECORD_HEADER_LEN;

/// The largest number of bytes a single block record can store: `encode_block_record` returns an
/// error above it rather than writing a truncated one.
///
/// Re-exported for the same reason as the header length above, and because it is the AUTHORITY
/// for the width of `BlockAddress::length`. A copy of the number written down beside that field
/// would go stale silently; `engine::tests::per_item_byte_budget` reads it from here.
pub(crate) use record::BLOCK_RECORD_LENGTH_MASK;

pub(crate) use record::block_index_checksums_enabled;

use paths::{
    delayed_destroy_dir, delayed_destroy_path, slab_manifest_path, file_created_unix_ms,
    file_modified_unix_ms, legacy_zone_manifest_path, now_unix_ms, slab_path, sync_dir,
    sync_parent_dir, system_time_unix_ms,
};
use record::{
    decode_block_record, default_block_record_compression_enabled,
    default_block_record_compression_level, default_block_record_compression_min_bytes,
    encode_block_record, inspect_slab, logical_range_from_slab,
    sha256_hex, summarize_slab,
    BlockRecordCompression,
};
use self::slab_manifest::*;
pub(crate) use slab_ids::*;
pub(crate) use slab_manifest::{manifest_file_writes, slab_descriptors_summarised};
#[cfg(test)]
use record::{BLOCK_RECORD_COMPRESSION_NONE, BLOCK_RECORD_COMPRESSION_ZSTD};

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "block checksum mismatch for slab {block_slab_id} offset {offset} length {length}: expected {expected}, got {actual}"
    )]
    ChecksumMismatch {
        block_slab_id: u64,
        offset: u64,
        length: u64,
        expected: String,
        actual: String,
    },
    #[error("corrupt block envelope for slab {block_slab_id} offset {offset}: {reason}")]
    CorruptBlockEnvelope {
        block_slab_id: u64,
        offset: u64,
        reason: String,
    },
    /// A slab id or an offset that does not fit the address word.
    ///
    /// Its own variant rather than an `Io`, so a caller that retries I/O does not retry this:
    /// the address does not exist and writing it again will not make it exist.
    #[error("{0}")]
    AddressOutOfRange(#[from] BlockAddressOutOfRange),
}

/// Which optional parts an address carries.
///
/// Five `Option`s cost 72 bytes to wrap values of 8, 8, 4, 8 and 8. A `u64` has no spare bit
/// pattern to mean "absent", so each one pays a whole extra word for its tag, and every block in
/// the index holds an address for the life of the shard.
///
/// A sentinel would be cheaper and is NOT available here: `0` is a legitimate `stored_slab_id` and a
/// legitimate `routing_slot` -- there is a test asserting `stored_slab_id == Some(0)` -- so "zero means
/// absent" would silently erase real values. A byte of presence bits costs almost nothing and
/// cannot make that mistake.
///
/// So this byte is a PRESENCE byte and nothing else: one bit per optional part -- block id and
/// generation -- rather than an `Option` wrapped around each value. It is in-memory only and
/// never a wire field, which is what lets the bit numbering keep the hole the retired
/// routing-bucket bit left instead of paying a renumbering to close it.
const ADDRESS_HAS_BLOCK_ID: u8 = 1 << 0;
/// Whether this address HAS a generation. Its VALUE is derived from the two identities above and
/// is not stored; this bit is all that is left of the field, and it costs nothing because the
/// presence byte was already here. See [`BlockAddress::generation`].
///
/// TWO BITS THAT USED TO SIT BETWEEN THESE ARE GONE -- the routing bucket's, and the object
/// id's with the field itself -- and neither position is reused: these bits are in-memory only
/// -- `present` is not a wire field -- so the numbering is free to have holes and a hole is
/// cheaper to read than a renumbering.
const ADDRESS_HAS_GENERATION: u8 = 1 << 3;

/// The address as it travels on the wire and on disk.
///
/// `BlockAddress` converts through this both ways, which keeps the in-memory packing separate
/// from the on-disk shape.
///
/// The names are SHORT. An address is written once per index item, and an index-log record was
/// measured at 65.3% field names -- these were the largest remaining group of them, and
/// `page_segment_id` alone cost more than the offset it labels. Every short name carries every
/// spelling this field has ever had as an alias, so anything already written still loads:
/// `block_slab_id` and its `page_segment_id` rename, `stored_slab_id` with its older
/// `extent_id`/`zone_id`.
///
/// This does change the shape a NEW record is written in, so a binary older than this cannot
/// read one -- the same trade the WAL and the index item made before it. Old to new is safe;
/// new to old is not.
///
/// # TWO ENCODERS READ THIS STRUCT AND THEY DO NOT AGREE
///
/// Everything about retiring a field here follows from that, so it is stated once, in the place
/// the fields are declared, rather than re-derived at each of them:
///
///   * THE INDEX LOG IS POSITIONAL. `index_log::encode_index_payload_into` builds a plain
///     `rmp_serde::Serializer`, so a field is a POSITION and not a name, and a row is an array.
///     A row LONGER than the struct decoding it is REFUSED -- measured, "array had incorrect
///     length, expected 5" -- and `index_log`'s replay propagates that with `?` rather than
///     skipping the row, deliberately: "a sweep that quietly skips what it cannot read is how
///     committed corruption becomes silent data loss". So this path fails HARD and it fails
///     LOUDLY, and that asymmetry is a decision rather than an oversight.
///
///     A ROW SHORTER THAN THE STRUCT IS NOT REFUSED, AND THIS PARAGRAPH USED TO SAY IT WAS.
///     "A row whose length does not match" was half of a length check and there is no length
///     check: serde's derived `visit_seq` fills a slot the array never reached with `Default`.
///     Measured on this struct, arities 4, 3 and 2 all load -- arity 4 loads an address whose
///     `generation` is absent, which is the one slot below whose PRESENCE is a meaning. What
///     looks like a refusal is arity 1 and under, and that is the absent `#[serde(default)]` on
///     `length` reported as "invalid length 1, expected struct BlockAddressWire with 5
///     elements": a missing REQUIRED FIELD, not an arity the decoder checked. `index_log`'s
///     `IndexItemRow` closes the same hole one struct up by reading the arity off the array
///     header; nothing closes it here yet.
///
///   * THE SERVED INDEX IS NAMED. `engine::encode_index_bytes` builds its serializer
///     `.with_struct_map()`, so a row is a map and a reader simply DROPS the keys its struct no
///     longer declares -- measured, silently, with every remaining value intact.
///
/// AND THE VERSION STAMP GUARDS THE TOLERANT ONE. `engine::SHARD_INDEX_FORMAT_VERSION` is checked
/// before the SERVED index decodes, and a mismatch is treated as an absent index so the caller
/// replays -- slower and correct. The positional index log carries no struct version at all: its
/// container byte holds a codec and a record SHAPE (whole / delta / anchor), not a version.
///
/// SO THE COST OF RETIRING A SLOT IS NOT "THE STORED SHAPE MOVES". It is whether the slot carried
/// a MEANING that the named path would drop in silence. `rs` and `h` carried none -- both were
/// read and ignored -- and they are gone. `g` carries one, because its PRESENCE is
/// `ADDRESS_HAS_GENERATION`, so retiring it needs the stamp to refuse the old shape first.
/// `shorter_struct_against_an_existing_row` at the end of this file drives all four cases.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BlockAddressWire {
    /// THE MERGED ADDRESS WORD, AND DELIBERATELY NOT AN ALIAS OF THE OLD SPELLING.
    ///
    /// It would have been one line to give this `alias = "ps"` and read an old index without a
    /// migration. That line is the whole hazard: an old record's `ps` of 1 beside an `o` of 2
    /// would land here as the word 1, which is slab 0 offset 1 -- a well-formed address for a
    /// block that is not the one that was written. Nothing would fail. So the old names are NOT
    /// aliases of this field, and an entry carrying them and not this one is REFUSED.
    ///
    /// AN `Option` ONLY SO THE ABSENCE IS VISIBLE. The conversion requires it and says what is
    /// missing; it is never written absent.
    ///
    /// WHY THE OLD NAMES ARE NOT KEPT HERE AS DECOYS. Naming `ps` and `o` as their own fields
    /// would give a better message, and it costs two slots on the wire that cannot be skipped:
    /// the index log packs this struct POSITIONALLY -- `encode_index_payload_into` uses a plain
    /// `rmp_serde::Serializer`, so a field is a position and not a name -- and a
    /// `skip_serializing_if` there shortens the array and shifts every field after it. Two nils
    /// on every stored address is a worse trade than a less specific message, and the refusal is
    /// loud either way: in a NAMED encoding the old keys are unknown and this field is absent, in
    /// the POSITIONAL one the old record is an array shorter than this struct has fields.
    ///
    /// AND THE LAST SENTENCE OF THAT IS NOW MEASURED RATHER THAN ASSERTED, which is what let two
    /// slots leave. A positional row LONGER than the struct decoding it is refused -- "array had
    /// incorrect length" -- and a named row simply drops the keys the struct stopped declaring.
    /// So the cost of retiring a slot is not "the stored shape moves and something reads it
    /// wrong"; it is exactly whether the slot carried a MEANING the named path would drop in
    /// silence. See `shorter_struct_against_an_existing_row` at the end of this file.
    #[serde(rename = "a", alias = "address_word", default)]
    address: Option<u64>,
    #[serde(rename = "l", alias = "length")]
    length: u64,
    #[serde(
        rename = "pi",
        alias = "page_id",
        default
    )]
    block_id: Option<u64>,
    #[serde(
        rename = "oi",
        alias = "object_id",
        default
    )]
    object_id: Option<u64>,
    /// THE ONLY OPTIONAL SLOT LEFT, AND IT IS NOT DEAD WEIGHT -- ITS PRESENCE IS A MEANING.
    ///
    /// `rs` (the routing bucket) and `h` (the digest) used to sit around this one, read and
    /// ignored and written as nil, because removing them looked like a format break for no gain.
    /// They are gone; `g` stays, and the difference between them is the whole of
    /// `a_named_row_keeps_g_by_name_which_is_why_this_change_needs_no_stamp` below.
    ///
    /// `rs` and `h` were dead in both directions: nothing read them and nothing wrote them. `g`
    /// is read. Its VALUE is cross-checked against `block_id.or(object_id)` just below, and its
    /// PRESENCE becomes `ADDRESS_HAS_GENERATION` -- so a reader that stopped declaring this field
    /// would not lose a nil, it would load every address with no generation at all and move every
    /// block handle it resolves through. On the named encoding that happens SILENTLY. Retiring
    /// this slot therefore needs the served-index version stamp to refuse the old shape first,
    /// which is a separate change; retiring the other two did not.
    #[serde(
        rename = "g",
        alias = "generation",
        default
    )]
    generation: Option<u64>,
}

/// A STORED `g` THAT DISAGREES IS REFUSED, LOUDLY AND BEFORE THE DECODE COMPLETES.
///
/// `generation` is no longer a field: it is derived as `block_id.or(object_id)`. Every write path
/// in this engine has always produced exactly that value, so a store this binary wrote round-trips
/// unchanged and the stored shape does not move -- `g` is still written and still read.
///
/// The one thing that MUST NOT happen silently is loading a store whose `g` says something else.
/// `generation` is hashed into [`block_index_handle`] and rendered into the block-ref key, and
/// those handles are written to disk inside the lookup's refs. Deriving a different value for an
/// index that stored its own would recompute every handle, and the refs already on disk would
/// point at nothing -- which is precisely how a counter in this position lost an object once
/// before. Rejecting the load surfaces that as a failure to open, not as a missing object later.
///
/// This is why the conversion is `try_from` rather than `from`: it is the only point where a
/// disagreement can be seen at all.
impl TryFrom<BlockAddressWire> for ElementEntry {
    type Error = String;

    fn try_from(wire: BlockAddressWire) -> Result<Self, Self::Error> {
        // THE SPLIT SPELLING IS REFUSED BEFORE ANYTHING IS DERIVED FROM IT.
        //
        // An index written before the merge carries the slab and the offset as two fields, and
        // there is no way to read one as a merged word that is not silently wrong: `ps: 1, o: 2`
        // taken as a word is slab 0, offset 1 -- a valid-looking address for the wrong block. So
        // the old names are not aliases of `a`, which makes such an entry arrive here with no
        // address at all, and this is the first thing the conversion does: ahead of the
        // generation check, ahead of building anything, and naming the migration rather than
        // just failing.
        let Some(address) = wire.address else {
            return Err(
                "this index entry has no address word: an index written before the slab id and \
                 the offset merged spells them as two separate fields (`ps` and `o`, or \
                 `page_segment_id` and `offset`), and this binary cannot read one. Reading a \
                 split address as a merged word would produce a well-formed address for a \
                 different block, so it is refused. Re-dump the index with the binary that wrote \
                 it, then load it here"
                    .to_string(),
            );
        };
        let block_slab_id = u64::from(extract_block_slab_id(address));
        let offset = u64::from(extract_block_offset(address));
        let derived = wire.block_id.or(wire.object_id);
        // A STORED VALUE is checked; a stored ABSENCE is honoured.
        //
        // Those are two different things and it matters which is which. A stored `g` of 7 beside a
        // `block_id` of 4 is a disagreement: the writer recorded a generation this binary cannot
        // reproduce, and deriving 4 instead would recompute the block handle. That is refused.
        //
        // A `g` that is not there at all is not a disagreement -- it is an index written before
        // the field existed. The OLDEST spelling this tree still loads is exactly that shape:
        // `page_segment_id` / `routing_slot` / `page_refs`, a `page_id`, and no generation key
        // anywhere (`engine::tests::part3::core_index_loads_legacy_bucket_page_field_names` holds
        // those bytes). Such an index has always loaded with NO generation, and it must keep
        // doing so -- deriving one for it would move every key it resolves through. So the
        // presence comes from the wire and only the VALUE is derived.
        if let Some(stored) = wire.generation {
            if Some(stored) != derived {
                return Err(format!(
                    "stored address generation {:?} disagrees with block_id.or(object_id) {:?} \
                     at slab {} offset {}: this index was written in a shape this binary cannot \
                     reproduce, and loading it would recompute every page handle",
                    wire.generation, derived, block_slab_id, offset
                ));
            }
        }
        // There is no routing bucket to not pass any more: the slot that used to be read off the
        // wire and discarded here is gone from the struct entirely.
        ElementEntry::try_from_wire_parts(
            block_slab_id,
            offset,
            wire.length,
            wire.block_id,
            wire.generation.is_some(),
        )
        .map_err(|out_of_range| out_of_range.to_string())
    }
}

/// THE CONVERSION LIVES ON THE HOLDER BECAUSE THE HOLDER IS THE ONLY SCOPE WITH ALL FOUR VALUES.
///
/// This used to be `From<BlockAddress>`, and it could be: the address held the length and the block
/// id itself, so being handed one address was being handed the whole row. It is not any more. The
/// address is one word, and `l` and `pi` are filled from fields that now sit beside it -- so the
/// conversion has to start from the thing that holds both halves.
///
/// AND THAT IS WHY THE SLOTS DID NOT MOVE. The wire struct is untouched: five slots, same names,
/// same order, same values. The alternative -- dropping `l` and `pi` because the address no longer
/// declares them -- was never available. The index log packs this struct POSITIONALLY with a plain
/// `rmp_serde::Serializer` and carries NO struct version, so a row that stopped declaring a slot
/// would be read by a build that still declares it, and the served index's format stamp cannot
/// help because it guards the NAMED path only. A stamp-free change was therefore the requirement,
/// not the happy accident, and moving this impl up one level is what buys it.
///
/// WHICH WAY ROUND THAT BITES IS NOT WHAT THIS USED TO SAY. "A shortened array is refused as
/// array had incorrect length" was the wrong direction: measured, a LONGER array is refused that
/// way and a SHORTER one is accepted with its missing tail slots read back as defaults. So the
/// risk in dropping a slot here is not a loud refusal of every stored row -- it is a SILENT
/// reinterpretation of every stored row by a reader that still expects the slot, which is worse
/// and was being counted as the safe case.
impl From<ElementEntry> for BlockAddressWire {
    fn from(entry: ElementEntry) -> Self {
        Self {
            address: Some(entry.address().address_word()),
            length: entry.length(),
            block_id: entry.block_id(),
            object_id: None,
            // `routing_bucket` and `sha256` used to be written here as nil, because the slots
            // were kept. Both slots are gone, so there is nothing to write: the bucket is the
            // container's answer and the block envelope carries the digest that verifies the bytes.
            generation: entry.generation(),
        }
    }
}

/// A BYTE BUDGET, NOT A DEFAULT WIDTH.
///
/// There are two of these per stored record -- one in the model map a read resolves through, one
/// inside the block-index entry -- and both live for the life of the shard, so this is the most
/// numerous structure in the engine. `engine::tests::per_item_byte_budget` counts them at two
/// corpus sizes and ranks every per-item structure by `size_of` x count; this one heads the list
/// at both.
///
/// `length` is 32 bits and `block_id` is 16 because THE ENCODER REFUSES ANYTHING WIDER, not
/// because either width looked like enough. `encode_block_record` returns an error above
/// `BLOCK_RECORD_LENGTH_MASK` (2^30 - 1) bytes for a record, and above `u16::MAX` for a block
/// id -- so `length` holds four times the largest length that can ever reach it and `block_id`
/// holds EXACTLY the largest block id that can.
///
/// `address` is ONE WORD HOLDING TWO NUMBERS: the slab in the high 32 bits, the offset inside
/// that slab in the low 32. See [`make_block_address_word`] for why each half fits, and
/// [`storage_config::MAX_BLOCK_SLAB_TARGET_BYTES`] for the knob that bounds the offset.
///
/// Every narrow field is read and written through `u64` (`length()`, `block_id()`,
/// `block_slab_id()`, `offset()`, `from_parts`, `set_block_id`), so nothing outside this file
/// knows the width. `length` SATURATES rather than truncating -- see `narrow` below. The two halves
/// of `address` and the `block_id` do NOT saturate, because each of them saturated is a perfectly
/// plausible name for a DIFFERENT block or page. They are checked and the caller is refused -- see
/// [`BlockAddress::try_from_parts`] and `narrow_block_id`.
///
/// `generation` used to sit here as a seventh field. It is now DERIVED -- see [`BlockAddress::generation`].
///
/// `routing_bucket` used to sit here as a sixth. IT IS NOW THE CONTAINER'S ANSWER, not the block's.
/// A block's bucket is `start + FNV-1a-64(object_key) % width`, and since the routing range a store
/// was built under is STAMPED beside its index -- a disagreeing range is refused before the decode,
/// and an unstamped store has its built range honoured and stamped -- that expression evaluated on
/// load is the same number the writer stamped. So the field was a cache of a pure function of the
/// key, in a structure there are two of per stored record. Six readers were ALREADY computing it
/// with `unwrap_or_else(block_routing_bucket(..))` whenever the field happened to be absent; they
/// are now simply the only shape, and the walk-based readers take the bucket they are walking
/// (`LiveBlockEntry::filed_bucket`). The read path takes it as an ARGUMENT, which is what the write
/// path (`append_value`) has always done.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockAddress {
    /// Slab id in the high 32 bits, offset in the low 32, and that is the WHOLE structure. Read it
    /// through [`BlockAddress::block_slab_id`] and [`BlockAddress::offset`]; the raw word is
    /// [`BlockAddress::address_word`] and is what the stored form carries.
    ///
    /// THE SIZE, THE PAGE ORDINAL AND THE STATUS BITS USED TO SIT HERE. They are now explicit
    /// fields on [`ElementEntry`], the holder every container stores, and the serde that writes
    /// this word onto the wire lives THERE rather than here -- it has to, because the wire row
    /// spells all four and only the holder has all four in scope.
    address: u64,
}

/// The width this structure is budgeted at, so a field added to it is a BUILD failure.
///
/// 16 bytes: one 64-bit address word, one 32-bit length, a 16-bit block id and the presence
/// byte -- 15 bytes of field and one of alignment. Nothing stopped this growing before it was
/// written down; it had already moved twice, and this sentence then said 24 for a while after
/// the structure was 16.
///
/// THE ASSERT BELOW IS THE AUTHORITY, NOT THIS SENTENCE. The 64-bit identity named above is
/// GONE -- its removal is what the rest of this entry is the record of -- and prose stating a
/// width cannot be trusted to move with the structure. That is why the reconstruction beside
/// the assert adds the groups up rather than restating a total.
///
/// IT WAS 48, THEN 40, THEN 32, AND EVERY STEP SHED A WHOLE EIGHT BYTES OF PAYLOAD. At 48 the
/// payload was 45 bytes, at 40 it was 37 and at 32 it was 29, and in each case every field shared
/// one eight-aligned group: taking any ONE field from 8 bytes to 4 -- or from 4 to 2 -- lands the
/// payload one step below the boundary and it rounds straight back up. So no SINGLE-FIELD
/// NARROWING has ever been worth anything here.
///
/// WHAT THAT RULE DOES NOT SAY, AND USED TO: it does not say a narrowing cannot help. It says a
/// narrowing of ONE field cannot. Fields narrowed or shed TOGETHER shed a whole eight bytes
/// between them and cross the step exactly as shedding one eight-byte field does. That is what
/// happened at 40 -> 32: `block_slab_id` and `offset` were 8 bytes each and became 4 and 4 in one
/// word. And it is what happens here.
///
/// 32 -> 24 IS TWO CHANGES NEITHER OF WHICH IS WORTH ANYTHING ALONE, and the record of what each
/// was worth in isolation is the point:
///
///   * `routing_bucket` LEAVES. Four bytes off a 29-byte payload is 25, which rounds straight back
///     to 32. Worth ZERO on its own.
///   * `block_id` goes from 32 bits to 16, which `encode_block_record` has always proved safe by
///     refusing anything wider. Two bytes off 29 is 27, which rounds back to 32. Worth ZERO on its
///     own -- this file said so, and said it correctly.
///
/// TOGETHER they shed six, the payload is 23, and it rounds to 24. The narrowing that was "worth
/// exactly zero" is worth the WHOLE STEP once the bucket is gone; what was wrong was not the
/// arithmetic but treating a per-field verdict as a verdict on the field.
///
/// THE FLOOR WAS 24 WHILE `object_id` WAS HELD, and the payload arithmetic said the field had to
/// LEAVE rather than narrow. It has left, so the floor below is 16 and the paragraphs that
/// follow are the record of why narrowing was never going to do it. Reaching 16 needs a tail of 8 over an eight-aligned group of 8, and
/// the tail is `object_id` plus the length (4), the block id (2) and the presence byte (1). So
/// `object_id` at four bytes leaves a tail of 11 and the struct at 24, at two bytes a tail of 9 and
/// the struct at 24, and only at ONE byte or GONE does the tail fit. That is this file's own rule
/// read forwards: a narrowing of one field is worth nothing, and here even the narrowing is worth
/// nothing unless it goes all the way.
///
/// AND THE FIELD IS A CACHE OF A PURE FUNCTION OF FIELDS THE ENTRY ALREADY HOLDS, which is what
/// decides it. `BlockIndex` carries `object_key`, `model_id` and `component` beside this address
/// and `ShardState` carries the shard, so `stable_block_object_id(shard, kind, key)` is
/// computable wherever a block entry is. Measured by
/// `engine::tests::address_footprint::the_object_id_on_a_live_block_entry_is_the_hash_of_fields_beside_it`
/// over every live block entry at two corpus sizes: the stored id equalled that derivation on 2,524
/// of 2,524 entries and on 20,632 of 20,632, and differed on NONE. #1974 established the derivation
/// at about thirty sites, `index_log` already STRIPS a derivable id from the row it writes
/// (`a_row_does_not_write_the_object_id_it_can_derive`), and the read sites already carry
/// `unwrap_or_else(|| stable_block_object_id(..))`. The in-memory field is the last copy.
///
/// WHAT ACTUALLY PINS IT, and it is NOT the registry's key. The claim here used to be that
/// `engine::block_in_wal::read_block` resolves a durably-acked block through a registry keyed
/// `(store, shard, object_id)` "with no bucket in it", and that a per-bucket ordinal cannot address
/// that registry. The second half is true and the first half is not the constraint: that registry is
/// live-path state that is NEVER PERSISTED -- a reload replays the WAL and `register_at` refills it
/// from the index -- so re-keying it costs nothing durable, and `read_block_bytes` already takes the
/// routing bucket as an argument and uses it two lines above the call to build its cache key. The
/// bucket is in hand. The field does not need an ordinal; it needs to be RECOMPUTED.
///
/// THE REAL CONSTRAINT IS THAT AN OMISSION AT A READ SITE IS SILENT. `read_block` is what serves a
/// block whose only durable copy is its WAL record, so a reader that cannot supply the identity does
/// not read a stale block -- it reads MISSING for a durably acknowledged write, which is the exact
/// hole `block_in_wal` exists to close. The routing bucket could be threaded as an `Option` because
/// an absent bucket only weakens a cache key; an absent identity loses data on read. So the field
/// can leave only behind an identity the COMPILER demands at every one of those sites -- the
/// doctrine `compaction::compact_block_addresses` already states, where `Item = (u32, &mut
/// BlockAddress)` "makes the caller say it, and the compiler name any caller that cannot". An
/// `Option<u64>` threaded to the same sites would compile with `None` and lose a block quietly.
///
/// THAT CONSTRAINT IS NOW SATISFIED, AND IT IS WHAT THIS PARAGRAPH USED TO BE WAITING FOR.
/// `engine::hashing::BlockIdentity` is that identity: `read_block_bytes` and `read_block_shared` take
/// one by value instead of recovering an object from `address.object_id()` with `and_then`, and the
/// type has NO constructor that accepts an id. `BlockIdentity::of` takes the shard, the kind and the
/// key and derives it, so a caller cannot pass a stale id, an id from somewhere else, or none at all
/// -- there is no value of that shape to pass. The read path no longer reads this field.
///
/// WHAT THE COMPILER NAMED WHEN THE TYPE WENT IN, which is the measurement of the doctrine rather
/// than a restatement of it: four production sites had no key in hand at all. `FastPathRead::Hash`
/// carried `fields` and a bucket derived FROM the key and then dropped the key;
/// `compact_block_addresses` took `Item = (u32, &mut BlockAddress)`, so thirteen relocation arms
/// each holding a key could not pass one; `packed_blocks::read_feature_point`,
/// `read_feature_point_cached`, `product_model::read_sequence_row` and `context::read_context_value`
/// took an address and a timestamp and nothing that named an object. Every one of them had a caller
/// that did hold the key, which is why the threading terminates.
///
/// AND IT FOUND SEVEN READS THAT NAMED THE WRONG ELEMENT. A context node is resolved
/// `hashes[key][CONTEXT_NODE_FIELD]` OR, for blocks written before that move, `context_nodes[key]`
/// -- and all seven sites named `Some(CONTEXT_NODE_FIELD)` for BOTH arms, with a comment asserting
/// they were the same lookup. `storage_bucket_internals::model_map_block_address` is the authority
/// and spells the second arm `("context_node", None)`. Under the old shape only the element was
/// wrong, because the object came off the address, so the in-log fallback built the block key
/// `(id, Some("meta"))` for a block registered as `(id, None)` and missed it -- MISSING for a
/// durably acknowledged write, on exactly the path this whole entry is about. The identity cannot be
/// built without naming a kind, so the arms had to separate and the element came right with them.
///
/// WHAT THE FIELD COST WHEN IT WENT, stated so the next change does not have to re-derive it.
/// Shedding `object_id` was ONE change and not two: it was a whole eight-byte field in the
/// eight-aligned group, so the group became 8, the tail stayed 4 + 2 + 1 = 7 and rounded to 8, and
/// the struct is 16. That is this file's own rule read correctly -- "a narrowing of ONE field
/// cannot [pay]" is about narrowing, and shedding eight bytes from the aligned group IS a whole
/// step. `BlockIndex` followed 64 -> 56 and the page-list stride 72 -> 64.
///
/// WHAT IT COST WAS THE STORED SHAPE, AND THE STAMP IS WHAT PAYS FOR IT. `generation` was
/// `block_id.or(object_id)`; it is now the block id alone. For a BLOCK-RESIDENT page that is the
/// same value it always was. For a WAL-RESIDENT page -- where the block id is absent -- the
/// generation WAS the object id and is now `None`, and `state::block_index_handle` hashes the
/// generation while `state::block_index_written_key` renders it into the ref key already on disk.
/// So every WAL-resident ref moves, and that is what the format stamp pays for.
///
/// THE STAMP LEDGER, WRITTEN DOWN HERE BECAUSE A VALUE IN A CONVERSATION IS NOT A LEDGER.
/// `engine::SHARD_INDEX_FORMAT_VERSION` was 3 when this paragraph was first drafted, and 4 was
/// RESERVED for this change on that basis. It is not 3 any more: the container-page tombstone took
/// it 3 -> 5, skipping 4 to hold the reservation. That reservation did not survive main moving past
/// it. This change therefore takes **6**, and the next one takes 7.
///
/// 7 IS NOW TAKEN, by the object list storing its SLOTS instead of a sorted set of its ids: the
/// sequence a bucket writes is in slot order and spells a placeholder `null`, so the bytes of every
/// bucket holding more than one object move. The next change takes 8. An index written before that
/// change still loads -- an ascending run of bare ids restores to ascending positions, which is
/// where the old loader would have put them -- so the stamp is paid for the WRITE side moving, not
/// for a read that would otherwise misparse.
///
/// 4 IS A HOLE AND MUST NEVER BE TAKEN. A hole is free going forwards and lethal going backwards,
/// because the two comparisons are not the same: `engine.rs` checks the served index with `!=` and
/// refuses either direction, but `persistence.rs` checks the BASE SNAPSHOT with `<`. A binary
/// carrying 4 computes `5 < 4 == false`, falls through to `IndexLoadPath::Accepted`, and reads a
/// 5-stamped store as though it were its own -- which is the silent misread the stamp exists to
/// prevent, reached by the one route the guard does not cover. So the rule is that a stamp may only
/// ever INCREASE: read the constant from the tree immediately before committing and take the next
/// value above it.
///
/// AND THE REASON THE STAMP IS NOT OPTIONAL IS THAT THE DECODE CANNOT SEE THE DISAGREEMENT. An
/// old row `{oi: 42, block_id: None, g: 42}` passes the generation cross-check in
/// [`TryFrom<BlockAddressWire>`] because BOTH sides of that comparison come off the wire and an
/// old row agrees with itself. It then builds an address whose `ADDRESS_HAS_GENERATION` bit is SET
/// while [`BlockAddress::generation`] answers `None` -- presence recorded, value absent -- and the
/// consequence appears later, on a recovery path. Refusing the load is the only point at which
/// that is visible at all.
///
/// THE `oi` SLOT STAYS ON THE WIRE AND IS WRITTEN EMPTY. `BlockAddressWire` is a separate struct
/// and the index log packs it POSITIONALLY, so retiring the slot would shorten that array and
/// refuse every existing row. Keeping it is also what lets the decode go on cross-checking an old
/// `g`. It cannot be written with a real value: `From<BlockAddress> for BlockAddressWire` is handed
/// an address and nothing else, and this type rides `#[serde(into)]` through fifteen nested
/// `ShardState` maps where the object key is the OUTER map key -- so there is no identity in scope
/// at the point the slot is written, and putting one there is a serializer rewrite rather than a
/// constructor change.
const _: () = assert!(std::mem::size_of::<BlockAddress>() == 8);

/// EIGHT BYTES: ONE WORD, AND NOTHING BESIDE IT.
///
/// The slab id is the high 32 bits and the offset the low 32, which is exactly 64 bits with no
/// spare bit -- see [`make_block_address_word`] for what bounds each half. There is no
/// bit-packing here and there is no room for any.
///
/// # WHAT THE THREE SIBLINGS COST WHEN THEY LEFT, WHICH WAS NOTHING
///
/// `length` (4), `block_id` (2) and `present` (1) did not vanish. They RELOCATED onto
/// [`ElementEntry`], and 8 + 4 + 2 + 1 = 15 rounds to 16 -- exactly what this structure cost
/// while it held them. The word sheds eight bytes and the holder sheds none, at every population
/// that holds an address: the model map level-2 value, the `BTreeMap` stride over it, the
/// optional holder, the row payload and the index entry are all unmoved.
/// `engine::tests::address_footprint::the_priced_shapes_for_an_address_and_its_siblings` measures
/// every one of them, with two controls that say the instrument is not blind -- one entry minus a
/// byte of field, which does NOT move, and one plus a whole word, which does.
///
/// SO THIS IS NOT A FOOTPRINT CHANGE AND MUST NOT BE READ AS ONE. It is a change of SHAPE: the
/// address is one opaque word, and the size, the page ordinal and the status bits are explicit
/// fields on the row that holds it instead of private fields inside the address. What that buys
/// is that each of the four is named where it is used, and can be narrowed, packed or dropped on
/// its own evidence. What it buys in bytes today is zero, and the row above says so.
///
/// # AND THE RULE IN THIS FILE NEEDED ONE MORE CLAUSE
///
/// The rule here was: a narrowing of ONE field is worth nothing, because the payload lands one
/// step below the eight-aligned boundary and rounds straight back; fields shed TOGETHER cross the
/// step and pay. Both halves are still true, and the 40 -> 32 and 32 -> 24 steps are the record
/// of the second half working.
///
/// What the rule did not say is that SHEDDING AND RELOCATING ARE NOT THE SAME MOVE. Three fields
/// left this structure together -- seven bytes, a whole step by that arithmetic -- and the total
/// paid is zero, because every holder had to take them.
///
/// So the clause is: A FIELD ONLY PAYS WHEN IT STOPS BEING STORED ANYWHERE, not when it stops
/// being stored HERE. Every earlier step out of this structure passed that test, which is why
/// each one was real: `routing_bucket` became a pure function of the object key, `generation` a
/// derivation of the block id, `object_id` a derivation of the shard, the kind and the key. Each
/// of those left the SYSTEM. `length` and `block_id` cannot: [`BlockStore::read`] sizes its read
/// with the length, so it is needed before a block can be read at all; and the live filter and
/// `BlockIndex::log_backed` read the block id precisely SO THAT they answer with no I/O.
const _: () = {
    let word = 8; // the slab in the high half, the offset in the low half
    let beside_it = 0; // length, block id and presence byte are on the holder now
    assert!(word + beside_it == std::mem::size_of::<BlockAddress>());
};

/// AND THE HOLDER IS THE WORD PLUS THE TAIL IT TOOK, reconstructed rather than restated so that a
/// field moving between the two cannot leave the arithmetic adding up for the wrong reason.
///
/// This is the assertion that makes the zero above checkable: if either width moves on its own,
/// this fails.
const _: () = {
    let group = std::mem::size_of::<BlockAddress>(); // 8 -- the word
    let tail = 4 + 2 + 1; // length, block id, presence byte
    assert!(group + (tail + 7) / 8 * 8 == std::mem::size_of::<ElementEntry>());
};

/// ONE WORD NAMES A BLOCK: slab in the high 32 bits, offset in the low 32.
///
/// Both halves are bounded, and neither bound is arithmetic -- each is something the engine
/// enforces:
///
///   * THE OFFSET is bounded by the slab target, which is now capped at
///     [`storage_config::MAX_BLOCK_SLAB_TARGET_BYTES`] and refused at configuration load above
///     it. `should_roll_before_append` rolls whenever `write_offset + record_len` would exceed
///     the target, and a record is never zero bytes, so the highest offset a store can record
///     is `target - 1`. At the capped target that is exactly `u32::MAX`.
///   * THE SLAB ID is bounded by [`MAX_ADDRESSABLE_BLOCK_SLAB_ID`], which sits below the two
///     sentinel ids. Slab ids are minted sequentially from zero, so a store would have to roll
///     four billion slabs -- sixteen exbibytes at the capped target -- to reach it.
///
/// Nothing here saturates. A saturated length reads as broken because it is larger than any
/// length the encoder accepts; a saturated address is a perfectly plausible address for a
/// DIFFERENT block, and a read that follows it returns the wrong bytes with no error anywhere.
/// So the constructors check, and refuse.
pub const fn make_block_address_word(block_slab_id: u32, offset: u32) -> u64 {
    ((block_slab_id as u64) << 32) | (offset as u64)
}

/// The slab half of an address word. See [`make_block_address_word`].
pub const fn extract_block_slab_id(address: u64) -> u32 {
    (address >> 32) as u32
}

/// The offset half of an address word. See [`make_block_address_word`].
pub const fn extract_block_offset(address: u64) -> u32 {
    (address & 0xFFFF_FFFF) as u32
}

/// The largest slab id an address can name.
///
/// The two sentinel slab ids -- `engine::HOT_BLOCK_SLAB_ID` and `wal_record::WAL_LOG_SLAB_ID` --
/// sit at the top of the 32-bit range, so a real slab id has to stay below them. They used to
/// sit at the top of the SIXTY-FOUR bit range, which is precisely what made the slab id look
/// un-narrowable: `record_cost` proved a bare `as u32` destroys them, and it does. Moving them
/// rather than narrowing around them is what makes the merge possible, and it is a change to the
/// STORED shape -- see [`BlockAddressWire`].
pub const MAX_ADDRESSABLE_BLOCK_SLAB_ID: u64 = (u32::MAX as u64) - 2;

/// The largest offset an address can name, which is the whole low half.
pub const MAX_ADDRESSABLE_BLOCK_OFFSET: u64 = u32::MAX as u64;

/// The largest block id an address can name, which is the ceiling
/// [`record::encode_block_record`] has always enforced on the wire.
///
/// NOT a width chosen here. The record header spells a block id as a `u16` and the encoder returns
/// an error above `u16::MAX`, so no record in any store has ever carried a wider one; the address
/// held it in 32 bits because nothing had asked.
pub const MAX_ADDRESSABLE_BLOCK_ID: u64 = u16::MAX as u64;

/// A slab id or an offset that does not fit in its half of the address word.
///
/// Carried as its own error rather than folded into an existing one so that a caller cannot
/// mistake it for an I/O failure and retry: nothing about retrying a write whose address does
/// not exist will make the address exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockAddressOutOfRange {
    pub block_slab_id: u64,
    pub offset: u64,
    /// Set only when it is the BLOCK ID that does not fit, so the message can say which half of
    /// the structure refused rather than blaming the address word for a block ordinal.
    pub block_id: Option<u64>,
}

impl std::fmt::Display for BlockAddressOutOfRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(block_id) = self.block_id {
            return write!(
                f,
                "block id {block_id} does not fit the block id field: a block id must be at most \
                 {MAX_ADDRESSABLE_BLOCK_ID}, which is the ceiling `encode_block_record` has always \
                 enforced on the record header. Truncating it would name a different page of the \
                 same object, so this is refused rather than narrowed (slab {}, offset {})",
                self.block_slab_id, self.offset
            );
        }
        write!(
            f,
            "block address (slab {}, offset {}) does not fit the address word: a slab id must be \
             at most {MAX_ADDRESSABLE_BLOCK_SLAB_ID} and an offset at most \
             {MAX_ADDRESSABLE_BLOCK_OFFSET}. Truncating either half would name a different block, \
             so this is refused rather than narrowed",
            self.block_slab_id, self.offset
        )
    }
}

impl std::error::Error for BlockAddressOutOfRange {}

/// SATURATE, NEVER TRUNCATE.
///
/// `value as u32` on a 4 GiB length yields a small plausible number and no error anywhere, and
/// this tree has been bitten by exactly that shape before: a value had to be pinned at `u64::MAX`
/// so a fixed-width format kept its width, and a missing value read as zero caused two separate
/// defects. A saturated length is larger than any length the encoder will accept, so it reads as
/// broken rather than as a shorter record.
///
/// Reachable only from an already-corrupt stored address or a caller passing a value the write
/// path would have refused; `engine::tests::per_item_byte_budget` holds both boundaries.
const fn narrow(value: u64) -> u32 {
    if value > u32::MAX as u64 {
        u32::MAX
    } else {
        value as u32
    }
}

/// REFUSE, NEVER SATURATE -- the block id is the one narrow field with no headroom.
///
/// `narrow` above can saturate because `u32::MAX` is four times the largest length the encoder
/// accepts, so a saturated length reads as broken. `u16::MAX` IS the largest block id the encoder
/// accepts, so saturating there produces a perfectly legal block id for a DIFFERENT block of the
/// same object -- and `decode_block_record`'s block-id arm, the one cross-check on a read that can
/// actually fire, would confirm it against that block's own record. So this panics, for the same
/// reason [`BlockAddress::from_parts`] panics on an out-of-range address word.
const fn narrow_block_id(value: u64) -> u16 {
    if value > MAX_ADDRESSABLE_BLOCK_ID {
        panic!(
            "block id does not fit the block id field: truncating it would name a different page \
             of the same object"
        );
    }
    value as u16
}

impl BlockAddress {
    /// Build an address from the raw word, which is what the stored form carries.
    ///
    /// UNCHECKED BY DESIGN, because there is nothing left to check: every 64-bit value is a
    /// well-formed word -- the high half is a slab id and the low half an offset, and both halves
    /// are total. The checking constructor is [`BlockAddress::try_from_slab_and_offset`], which is
    /// where a caller holding two numbers that might not fit their halves goes.
    pub fn from_word(address: u64) -> Self {
        Self { address }
    }

    /// THE CHECKED CONSTRUCTOR, AND THE ONLY PLACE THE TWO HALVES ARE PACKED.
    ///
    /// Every caller holding a slab id or an offset that is not bounded by construction comes
    /// through here and handles the refusal: the two block-store appends (an offset recovered from
    /// a slab file that a previous configuration grew past the cap), the hot-block mint (a
    /// process-wide counter), and the record decoder (a physical offset inside a slab file this
    /// binary did not write).
    ///
    /// NOTHING HERE SATURATES. A saturated length reads as broken because it is larger than any
    /// length the encoder accepts; a saturated address is a perfectly plausible address for a
    /// DIFFERENT block, and a read that follows it returns the wrong bytes with no error anywhere.
    /// So this checks, and refuses.
    pub fn try_from_slab_and_offset(
        block_slab_id: u64,
        offset: u64,
    ) -> Result<Self, BlockAddressOutOfRange> {
        // The two sentinels are ALLOWED above `MAX_ADDRESSABLE_BLOCK_SLAB_ID` -- they are what
        // that constant reserves room for. Everything else must be a slab id a store could
        // actually have minted.
        let slab_fits = block_slab_id <= MAX_ADDRESSABLE_BLOCK_SLAB_ID
            || crate::wal_record::is_wal_resident(block_slab_id);
        if !slab_fits || offset > MAX_ADDRESSABLE_BLOCK_OFFSET {
            return Err(BlockAddressOutOfRange {
                block_slab_id,
                offset,
                // NOT THE BLOCK ID, and the `None` is load-bearing: it is what makes the message
                // say the ADDRESS WORD refused rather than blaming a block ordinal. The block id
                // is checked one level up, on the holder, and sets this to `Some`. See
                // [`BlockAddressOutOfRange`].
                block_id: None,
            });
        }
        Ok(Self {
            address: make_block_address_word(block_slab_id as u32, offset as u32),
        })
    }

    /// The slab and offset packed into one word, which is what the stored form carries.
    pub fn address_word(&self) -> u64 {
        self.address
    }

    /// The offset of this block inside its slab.
    ///
    /// Answers in 64 bits, as it always has, over the low half of the address word. See
    /// [`make_block_address_word`] for what bounds it.
    pub fn offset(&self) -> u64 {
        u64::from(extract_block_offset(self.address))
    }

    /// The slab this address is in.
    ///
    /// Answers in 64 bits, as the field did, over the high half of the address word. See
    /// [`make_block_address_word`] for what bounds it.
    pub fn block_slab_id(&self) -> u64 {
        u64::from(extract_block_slab_id(self.address))
    }

    /// The slab this address is in, which is the slab it is in.
    ///
    /// Derived rather than stored. It was a function of the slab AND two configuration sizes,
    /// which is what made it unsafe to derive: a reader whose configuration had moved would
    /// reconstruct a different slab than the writer meant. With one size there is nothing to
    /// disagree about, so the slab is a fact about the address instead of a field beside it.
    ///
    /// Still an `Option` because every caller reads it as one, and it now answers `Some` for
    /// every address -- a slab is always known.
    pub fn slab_id(&self) -> Option<u64> {
        Some(self.block_slab_id())
    }
}

// `mod hex_digest` stood here. It spelled the `h` slot as 32 bytes in memory and hex on the wire,
// and the `sha256` field was its only caller -- so it left with the field rather than staying as a
// codec for a slot nothing writes. `record::sha256_hex` still exists and is still used; what is
// gone is only the serde adapter that put a digest ON THIS STRUCT.

impl BlockAddress {
    pub fn compact_slab_id(&self) -> Option<u32> {
        u32::try_from(self.block_slab_id()).ok()
    }

    pub fn compact_slab_offset(&self) -> Option<u32> {
        u32::try_from(self.offset()).ok()
    }

    pub fn compact_slab_address(&self) -> Option<u64> {
        compact_slab_address_from_parts(self.block_slab_id(), self.offset())
    }

}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreStats {
    pub writes: u64,
    pub reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    #[serde(default)]
    pub logical_bytes_written: u64,
    #[serde(default)]
    pub logical_bytes_read: u64,
    #[serde(default)]
    pub compressed_records_written: u64,
    #[serde(default)]
    pub compressed_records_read: u64,
    #[serde(default)]
    pub compression_bytes_saved: u64,
    /// Times the whole slab manifest was written out.
    ///
    /// Writing it costs the whole manifest, so one write per slab install made installing n slabs
    /// cost n manifests -- and each install cost time proportional to how many slabs already
    /// existed. Counted rather than timed, because a count says the same thing on a busy machine.
    ///
    /// COUNTED INSIDE `persist_slab_manifest`, which is the only thing that writes the file. It
    /// was counted at the CALL SITES instead, and one of the twelve routes did it while eleven did
    /// not: a store that rolled slabs read zero here while every roll wrote a whole manifest. This
    /// field is `pub` on a crate other people build on, so the wrong number was theirs too, not
    /// only our own guards'.
    #[serde(default)]
    #[serde(alias = "band_manifest_writes")]
    pub slab_manifest_writes: u64,
    /// Slabs fetched on-demand from a shared-storage read-through source (conformance
    /// lazy recovery). Each shared slab is fetched at most once, only when a read
    /// misses it locally; a nonzero count proves recovery did not install every slab
    /// up front.
    #[serde(default)]
    pub shared_slab_fetches: u64,
    /// `bytes_written` as it stood when a compaction last consolidated the live set.
    ///
    /// THIS IS A WINDOW BASELINE, NOT A COUNTER. `bytes_written` is never reset -- two published
    /// ratios read it (`storage_write_amplification_ppm` and
    /// `lifetime_write_amplification_ppm`), so zeroing it would silently change both. Recording
    /// where the window opened and subtracting gives the same window without mutating a live
    /// counter.
    ///
    /// WHY A WINDOW IS NEEDED AT ALL. A write-amplification ratio over a LIFETIME numerator and a
    /// CURRENT denominator rises without limit as a function of uptime, so it cannot be held to a
    /// threshold. Worse, compaction is itself a writer: measured on a three-slab store, one
    /// compaction round wrote 3,697,200 B to make 1,232,400 B reclaimable. Charged against a
    /// lifetime numerator, the operation that REDUCES space amplification RAISES write
    /// amplification, so the two figures work against each other. Opening a new window where the
    /// live set is consolidated is what makes the ratio a reading about the current shape of the
    /// store rather than about its history.
    ///
    /// Zero until the first compaction, which is the honest starting value: before any compaction
    /// the window is the whole lifetime, and the windowed ratio equals the lifetime one.
    #[serde(default)]
    pub bytes_written_at_last_compaction: u64,
}

/// Lazy read-through source for slabs that live only in shared storage after a
/// metadata-only (index + address map) recovery on the shared-filesystem backend.
/// On a local slab miss the block store asks the source for exactly that slab's
/// bytes, caches them locally, then serves the read, so old blocks are read lazily
/// by address rather than eagerly installed at recovery time. Implementations
/// resolve a slab id to its shared object and return its verified bytes, or `None`
/// when the slab is not part of the recovered checkpoint.
pub trait SharedSlabSource: Send + Sync + std::fmt::Debug {
    fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreOptions {
    #[serde(default = "default_block_record_compression_enabled")]
    pub compression_enabled: bool,
    #[serde(default = "default_block_record_compression_min_bytes")]
    pub compression_min_bytes: usize,
    #[serde(default = "default_block_record_compression_level")]
    pub compression_level: i32,
}

/// The payload is BORROWED. A caller already holds the encoded block -- it keeps it for the
/// cache put, or it holds the buffer it published from -- so owning it here forced every
/// caller to hand over a clone of a block that is several times its own payload.
/// A block to append: its bytes, the object it belongs to, that object's routing bucket, and
/// which block of that object it is.
///
/// The last of those used to be the block store's to invent, from a counter running across the
/// whole store. A block id is now an index INSIDE an object -- block 0, block 1 -- the way the
/// comparison design numbers a block within a slot rather than within a partition. Two objects
/// both having a block 0 is expected: a block is identified by its object and its index, and
/// every key that names one already carries the object key.
///
/// The store-wide counter is gone with it, and so is the walk that recovered the counter by
/// reading every block header in every slab to work out one integer.
pub type BlockAppendRecord<'a> = (&'a [u8], Option<u64>, Option<u32>, u32);

impl Default for BlockStoreOptions {
    fn default() -> Self {
        Self {
            compression_enabled: default_block_record_compression_enabled(),
            compression_min_bytes: default_block_record_compression_min_bytes(),
            compression_level: default_block_record_compression_level(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcReport {
    #[serde(alias = "retain_from_page_segment_id")]
    #[serde(alias = "retain_from_page_slab_id")]
    pub retain_from_block_slab_id: u64,
    #[serde(alias = "removed_page_segment_ids")]
    #[serde(alias = "removed_page_slab_ids")]
    pub removed_block_slab_ids: Vec<u64>,
    #[serde(alias = "retained_page_segment_ids")]
    #[serde(alias = "retained_page_slab_ids")]
    pub retained_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub removed_physical_bytes: u64,
    #[serde(default)]
    pub retained_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "delayed_destroy_page_segment_ids")]
    #[serde(alias = "delayed_destroy_page_slab_ids")]
    pub delayed_destroy_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub delayed_destroy_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_live_page_segment_ids")]
    #[serde(alias = "retained_live_page_slab_ids")]
    pub retained_live_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_current_page_segment_ids")]
    #[serde(alias = "retained_current_page_slab_ids")]
    pub retained_current_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_current_physical_bytes: u64,
    /// Slabs this round declined to reclaim because the PUBLISHED BYTE TALLY still credits them
    /// with live block bytes, even though the caller's live slab-id set did not name them.
    ///
    /// Not the same thing as `retained_live_block_slab_ids`, which is the slabs the caller's id
    /// set kept. These are the ones the two sources DISAGREE about, and reaching this list means
    /// a bug upstream: an id set and a tally derived from the same index should never contradict
    /// each other about the same slab. Non-empty is an alarm, and the point of the check is that
    /// the disagreement costs a round of reclaim instead of the bytes.
    #[serde(default)]
    #[serde(alias = "retained_live_bytes_page_slab_ids")]
    pub retained_live_bytes_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_bytes_physical_bytes: u64,
}

/// Live blocks on ONE slab, as the INDEX counts them.
///
/// The block store cannot derive this. It sees appends, and it sees whole slabs arrive and leave;
/// an index entry that stopped pointing at an offset reaches it nowhere. So this arrives from the
/// outside, through [`BlockStore::publish_live_block_bytes`], and the store only reads it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabLive {
    #[serde(alias = "live_page_refs")]
    pub live_block_refs: u64,
    /// Sum of the lengths of the live blocks on the slab. LOGICAL bytes -- the same quantity the
    /// slab descriptor's `logical_bytes` totals over every block ever appended to it, which is why
    /// that, and not the file size, is the denominator of the fraction below.
    pub live_bytes: u64,
}

/// How much of one slab is still live, for every slab the store holds.
///
/// The read side of the published tally, and the answer to "is `utility_basis_points` uniformly
/// zero". It is, for GC CANDIDATES, and necessarily so -- a candidate is a slab no live block
/// points at. Across the whole store it is not, and this is where that shows.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabLiveFraction {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    /// Total logical bytes ever appended to this slab. Only ever grows, which is correct FOR A
    /// DENOMINATOR and was the bug when the same field was read as a live figure.
    pub logical_bytes: u64,
    #[serde(alias = "live_page_refs")]
    pub live_block_refs: u64,
    pub live_bytes: u64,
    /// `live_bytes * 10_000 / logical_bytes`, or 0 when the slab has no logical bytes.
    pub live_basis_points: u64,
    /// `10_000 - live_basis_points`. What the page-GC garbage floor compares against.
    pub garbage_basis_points: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcUtilityCandidate {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub bytes: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub stale_bytes: u64,
    #[serde(default)]
    pub utility_basis_points: u64,
    pub utility_score: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicy {
    #[serde(default)]
    #[serde(alias = "max_destroy_segments")]
    pub max_destroy_slabs: usize,
    #[serde(default)]
    pub max_destroy_physical_bytes: u64,
    #[serde(default)]
    pub max_utility_score: Option<u64>,
    #[serde(default)]
    pub min_age_ms: Option<u64>,
    /// Reclaim only slabs whose garbage ratio (10_000 - utility_basis_points) is at
    /// least this many basis points. `None`/0 reclaims every eligible slab (today's
    /// behavior). The garbage-ratio gate (reclaim the most-garbage zones),
    /// expressed against Rust slabs.
    #[serde(default)]
    #[serde(alias = "min_band_garbage_basis_points")]
    pub min_slab_garbage_basis_points: Option<u64>,
}

impl BlockStoreGcPolicy {
    pub fn max_slabs(max_destroy_slabs: usize) -> Self {
        Self {
            max_destroy_slabs,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms: None,
            min_slab_garbage_basis_points: None,
        }
    }

    /// Reclaim eligible slabs whose garbage ratio is at least
    /// `min_slab_garbage_basis_points` (on the wire, `min_band_garbage_basis_points`),
    /// highest-garbage first, optionally bounded by a
    /// minimum slab age. Mirrors selecting the maximum-garbage-rate zone under GC.
    /// A garbage floor that now MEASURES THE RIGHT QUANTITY, and still excludes nothing in a
    /// running store. Both halves measured, neither assumed.
    ///
    /// WHAT CHANGED. `used_bytes` used to sum the file sizes of the slabs grouped under a
    /// candidate's stored id that are NOT collectable. A stored id names exactly one slab, and the
    /// candidate filter is the exact negation of that test, so a candidate could never contribute
    /// to its own used bytes: every candidate reported 0, 10,000 bp of garbage, and the floor
    /// cleared everything at every setting. It was not merely degenerate, it was answering "is
    /// this whole slab collectable" (always yes, by construction) instead of "how much of this
    /// slab is still live".
    ///
    /// It now sums the LIVE BLOCK BYTES on the slab itself, taken from the per-slab tally the index
    /// maintains on its own mutation path and publishes through
    /// `BlockStore::publish_live_block_bytes`. The denominator moved with it, from the file
    /// size to the slab descriptor's `logical_bytes` -- the total ever appended there -- because a
    /// live block is counted at its logical length. `a_published_live_tally_makes_used_bytes_mean_
    /// live_block_bytes` shows the floor excluding a 90%-live slab, which is the first time this
    /// knob has excluded anything.
    ///
    /// WHAT DID NOT CHANGE. In a running store the floor still excludes none of the COLLECTOR's
    /// candidates, and `can_the_block_gc_garbage_floor_bind` still asserts that. The reason has
    /// moved, and the new one is the useful one: a collector candidate is a slab that no live block
    /// points at -- `is_live` is checked before candidacy and again before removal -- so its
    /// maintained live bytes are genuinely zero. The floor is now measured against a real figure
    /// that is really zero, rather than against an artefact of two filters contradicting each
    /// other.
    ///
    /// So the remaining obstacle is the CANDIDATE PREDICATE, not the accounting: nothing offers
    /// this floor a partially-live slab, because a slab with one live block is not a candidate at
    /// all. That is the same all-or-nothing rule that lets one live block pin a whole slab, and
    /// widening it means relocating the survivors first -- a compaction decision with its own
    /// measurement, not a change to this constructor.
    ///
    /// (An earlier reading of this blamed the stored id for not grouping several slabs. It does
    /// not group, and it was never going to: the only grouping key ever used was the slab's own
    /// address, and `a_stored_slab_id_that_disagrees_with_its_descriptor_is_normalised_on_load`
    /// shows even a manifest cannot introduce one. Grouping was not the missing piece.)
    pub fn with_slab_garbage_floor(
        min_slab_garbage_basis_points: u64,
        min_age_ms: Option<u64>,
    ) -> Self {
        Self {
            max_destroy_slabs: 0,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms,
            min_slab_garbage_basis_points: Some(min_slab_garbage_basis_points),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicyPlan {
    #[serde(alias = "retain_from_page_segment_id")]
    #[serde(alias = "retain_from_page_slab_id")]
    pub retain_from_block_slab_id: u64,
    #[serde(alias = "selected_page_segment_ids")]
    #[serde(alias = "selected_page_slab_ids")]
    pub selected_block_slab_ids: Vec<u64>,
    pub selected_physical_bytes: u64,
    #[serde(default)]
    pub candidate_total_bytes: u64,
    #[serde(default)]
    pub candidate_used_bytes: u64,
    #[serde(default)]
    pub candidate_stale_bytes: u64,
    #[serde(default)]
    pub candidate_utility_basis_points: u64,
    pub candidate_count: usize,
    pub candidate_physical_bytes: u64,
    pub skipped_by_policy_count: usize,
    pub skipped_by_policy_physical_bytes: u64,
    pub skipped_by_budget_count: usize,
    pub skipped_by_budget_physical_bytes: u64,
    pub candidates: Vec<BlockStoreGcUtilityCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreDelayedDestroySlabReport {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

/// How long a reclaimed slab waits in quarantine before it may be destroyed.
///
/// Quarantine existed but nothing ever aged: the purge deleted every file it found, so a slab
/// moved there a second earlier went with the rest, and the "delay" lasted only until something
/// called purge. The hazard is written down a few files away, in the compaction commit path -- a
/// reclaim that quarantines and purges a slab, followed by a reload of an index that still names
/// it, dangles at a deleted slab. An age is what makes a delayed destroy delay anything.
///
/// One hour rather than the twenty-four the comparison design waits. The window this has to cover
/// is a reader holding a stale address, a replica catching up, or an index persist that failed and
/// is retried next cycle -- minutes, not a day -- and quarantine holds real bytes on a box with a
/// measured capacity wall. Long enough for all three, short enough not to hoard a day of garbage.
pub(crate) const DELAYED_DESTROY_MIN_AGE_MS: u64 = 60 * 60 * 1000;

/// How many quarantined slabs one purge round may act on before it stops and leaves the rest for
/// the next round.
///
/// THE PURGE HOLDS THE STORE-WIDE LOCK FOR THE WHOLE ROUND, and before this the round was
/// unbounded in the amount of work it did: it read the trash directory and acted on every slab it
/// found. Measured on this box with the re-checked, list-narrowed purge, and linear in the
/// quarantine size:
///
///   quarantined   purge duration   destroyed / restored / held
///         8,000        5,194.7 ms      6,400 /   800 /   800
///        80,000       58,661.7 ms     64,000 / 8,000 / 8,000
///
/// Nothing else can touch the store for that whole time. A thousand slabs is the largest round
/// that keeps the hold under a second at the per-slab cost measured here, which is the number
/// that matters: the bound is on the LOCK HOLD, not on the reclaim rate, and a caller that wants
/// the quarantine drained faster runs more rounds rather than one longer one.
///
/// THE BUDGET IS SPENT ON WORK DONE, NOT ON ENTRIES LOOKED AT -- see
/// [`BlockStore::purge_delayed_destroy_slabs_capped`], where the difference is what makes
/// the cap advance instead of stalling.
pub(crate) const DELAYED_DESTROY_MAX_SLABS_PER_ROUND: usize = 1_000;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStorePurgeDelayedDestroyReport {
    #[serde(alias = "purged_page_segment_ids")]
    #[serde(alias = "purged_page_slab_ids")]
    pub purged_block_slab_ids: Vec<u64>,
    pub purged_physical_bytes: u64,
    /// Slabs left in quarantine because they had not been there long enough yet.
    ///
    /// Non-zero means the purge ran and DECLINED: the space comes back on a later cycle. Without
    /// it a caller cannot tell "nothing to reclaim" from "not yet".
    #[serde(default)]
    pub retained_too_young_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_too_young_physical_bytes: u64,
    /// Slabs taken BACK OUT of quarantine because the last-chance re-check found them live.
    ///
    /// Non-empty is an alarm, not routine: the collector decided this slab was unreferenced and
    /// the re-check disagreed. The slab is returned to the store and the bytes are not freed.
    #[serde(default)]
    pub restored_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub restored_physical_bytes: u64,
    /// Slabs the re-check found live but could NOT return, because a file already occupies the
    /// id. They stay in quarantine: not destroyed, not restored.
    #[serde(default)]
    pub restore_blocked_block_slab_ids: Vec<u64>,
    /// Slabs this round destroyed or restored -- what the round's budget was spent on.
    ///
    /// NOT the same as `purged + restored` being nonzero, and not derivable from the lists a
    /// caller can already see once a round can stop early: this is the number compared against
    /// `max_slabs_per_round`, and a caller checking that the cap advances needs the two side by
    /// side.
    #[serde(default)]
    pub processed_block_slabs: usize,
    /// The round stopped on its budget with quarantined slabs still unexamined.
    ///
    /// `true` is NOT an error and NOT a decline: it says the work continues next round. A caller
    /// draining a quarantine loops while this holds. The distinction the `retained_too_young`
    /// list already draws is the one that matters here too -- a slab the round never reached is
    /// not being held back for any reason of its own, it simply was not this round's business.
    ///
    /// A ROUND THAT SPENDS ITS BUDGET EXACTLY AS THE WORK RUNS OUT STILL REPORTS `true`, and the
    /// next round then finds nothing and reports `false`. That costs one extra, empty round, and
    /// it is deliberate: the only way for the round to know the directory holds nothing more is
    /// to walk the rest of it, and not walking the rest of it is the entire point of the cap.
    /// Over-reporting here costs a round that does nothing; under-reporting would stop a caller's
    /// drain with slabs still in quarantine.
    #[serde(default)]
    pub budget_exhausted: bool,
    /// The budget this round ran under. 0 means uncapped.
    ///
    /// Reported so a round that did less than a caller expected can be told apart from a round
    /// that ran under a smaller cap than the caller thought.
    #[serde(default)]
    pub max_slabs_per_round: usize,
    /// Directory entries this round PULLED FROM THE LISTING -- what the walk actually cost.
    ///
    /// EVERY OTHER COUNT IN THIS REPORT IS OF A CLASSIFICATION, AND AN ENTRY WALKED PAST WITHOUT
    /// BEING CLASSIFIED APPEARS IN NONE OF THEM. That hole is not academic. The budget check at
    /// the top of the loop STOPS the round rather than advancing to the next entry, and until
    /// this field existed nothing in the crate could tell the two apart: turning that `break`
    /// into a `continue` leaves a round that walks the whole directory to do nothing, reports
    /// exactly the same four lists and the same `processed_block_slabs`, and passes every test
    /// there was. This is the number that moves -- a budget's worth under a `break`, the whole
    /// directory under a `continue`.
    ///
    /// AN ITERATION COUNT, NOT A CLASSIFICATION COUNT, and the two differ by one at the budget:
    /// a round that stops reports `max_slabs_per_round + 1`, because the entry that finds the
    /// budget already spent was pulled off the listing before it could be turned away. The same
    /// boundary `budget_exhausted` documents, for the same reason -- knowing the directory holds
    /// nothing more means looking, and not looking is the point. A round that ends because the
    /// listing ran out reports exactly what the listing held.
    #[serde(default)]
    pub examined_block_slabs: usize,
    /// The round returned WITHOUT opening the trash directory, because no quarantined slab could
    /// have matured yet.
    ///
    /// This is what makes the four lists above readable when they are all empty. Without it an
    /// empty `retained_too_young_block_slab_ids` says "the round walked and held nothing back",
    /// and a round that declined in advance would say the same thing while meaning the opposite:
    /// every entry would have been held back, which is precisely why walking to say so was not
    /// worth the store-wide lock.
    ///
    /// `true` implies `examined_block_slabs == 0` and every list empty. It never implies the
    /// quarantine is empty -- read `delayed_destroy_slab_ids()` for that.
    #[serde(default)]
    pub declined_without_examining: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStoreSlabState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// Metadata for a SEALED slab whose bytes live in shared storage and are restored lazily.
/// Passed to [`BlockStore::install_lazy_checkpoint_slabs`] so a lazy-restore installs
/// complete slab descriptors before the first on-demand slab fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LazyCheckpointSlab {
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub first_block_id: Option<u64>,
    pub last_block_id: Option<u64>,
    pub created_unix_ms: Option<u64>,
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabDescriptor {
    /// The same number as [`Self::block_slab_id`], always.
    ///
    /// The manifest is the ONE way a second spelling of a slab id enters the process, so this is
    /// the number that arrives from disk rather than one this code computed. It used to divide a
    /// slab's byte range by a separate size, so one of these could have grouped several slabs;
    /// nothing ever configured the two sizes differently and the grouping was never exercised,
    /// which is why the map is keyed by slab id with one descriptor per slab.
    ///
    /// It stays because it SERIALIZES -- under its original key, which the wire names below
    /// record as `zone_id` -> `extent_id` -> `band_id` -- and the compat corpora carry it, so
    /// dropping it is a wire break rather than a cleanup. Read `block_slab_id` in new code; the
    /// two cannot diverge, `reconcile_slab_manifest_with_disk` normalises this one from the map
    /// key on every open, and `a_slab_descriptor_carries_the_same_number_twice` fails if they
    /// ever do.
    #[serde(rename = "band_id", alias = "extent_id", alias = "zone_id")]
    pub stored_slab_id: u64,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub state: BlockStoreSlabState,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(rename = "first_block_id", alias = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    /// The file mtime this descriptor was last verified against.
    ///
    /// Every open re-reads and re-hashes every block record in every slab; on a 1.4 GB store that is
    /// ~30 s of CPU, and it is the same work every time for slabs nobody has touched. Recording the
    /// identity the descriptor was verified against makes "has this file changed" answerable from
    /// metadata instead of by decoding the slab again.
    ///
    /// None on a descriptor written before this existed, which simply verifies once and fills it in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_source_mtime_unix_ms: Option<u64>,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

/// One per slab. Six `Option`s of eight bytes each is 96 of its 168 bytes, which is the shape
/// `BlockAddress` replaced with a presence byte -- worth it there at one per stored block, not
/// worth a wire change here at one per gigabyte.
const _: () = assert!(std::mem::size_of::<BlockStoreSlabDescriptor>() == 168);

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabSummary {
    #[serde(alias = "active_zones")]
    #[serde(alias = "active_bands")]
    pub active_slabs: u64,
    #[serde(alias = "sealed_zones")]
    #[serde(alias = "sealed_bands")]
    pub sealed_slabs: u64,
    #[serde(alias = "delayed_destroy_zones")]
    #[serde(alias = "delayed_destroy_bands")]
    pub delayed_destroy_slabs: u64,
    #[serde(alias = "purged_zones")]
    #[serde(alias = "purged_bands")]
    pub purged_slabs: u64,
    pub active_physical_bytes: u64,
    pub sealed_physical_bytes: u64,
    pub delayed_destroy_physical_bytes: u64,
    pub purged_physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub total_known_physical_bytes: u64,
    #[serde(
        default,
        alias = "oldest_known_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_known_band_unix_ms")]
    pub oldest_known_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_known_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_known_band_age_ms")]
    pub oldest_known_slab_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_live_band_unix_ms")]
    pub oldest_live_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_live_band_age_ms")]
    pub oldest_live_slab_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_reclaimable_band_unix_ms")]
    pub oldest_reclaimable_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "oldest_reclaimable_band_age_ms")]
    pub oldest_reclaimable_slab_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabUsage {
    #[serde(rename = "band_id", alias = "extent_id", alias = "zone_id")]
    pub stored_slab_id: u64,
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    #[serde(alias = "storage_zone_id", default)]
    pub storage_slab_id: u64,
    #[serde(default)]
    #[serde(alias = "stream_segment_id")]
    pub stream_slab_id: u64,
    pub state: BlockStoreSlabState,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub live_bytes: u64,
    #[serde(default)]
    pub reclaimable_bytes: u64,
    #[serde(default)]
    pub purged_bytes: u64,
    #[serde(alias = "page_store_used_bytes")]
    pub block_store_used_bytes: u64,
    #[serde(alias = "live_page_store_used_bytes")]
    pub live_block_store_used_bytes: u64,
    #[serde(alias = "reclaimable_page_store_used_bytes")]
    pub reclaimable_block_store_used_bytes: u64,
    #[serde(alias = "purged_page_store_used_bytes")]
    pub purged_block_store_used_bytes: u64,
    #[serde(rename = "first_block_id", alias = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBackedSlabRuntimeReport {
    pub runtime_ready: bool,
    #[serde(default)]
    #[serde(alias = "band_lifecycle_states")]
    pub slab_lifecycle_states: Vec<String>,
    #[serde(alias = "band_count", alias = "extent_count", alias = "zone_count")]
    pub slab_count: u64,
    #[serde(alias = "active_zones")]
    #[serde(alias = "active_bands")]
    pub active_slabs: u64,
    #[serde(alias = "sealed_zones")]
    #[serde(alias = "sealed_bands")]
    pub sealed_slabs: u64,
    #[serde(alias = "delayed_destroy_zones")]
    #[serde(alias = "delayed_destroy_bands")]
    pub delayed_destroy_slabs: u64,
    #[serde(alias = "purged_zones")]
    #[serde(alias = "purged_bands")]
    pub purged_slabs: u64,
    #[serde(alias = "zone_stats_ready", default)]
    pub slab_stats_ready: bool,
    #[serde(alias = "zone_usage", default)]
    pub slab_usage: Vec<BlockStoreSlabUsage>,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default)]
    pub stream_record_count: u64,
    #[serde(rename = "first_block_id", alias = "first_page_id", default)]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default)]
    pub last_block_id: Option<u64>,
    #[serde(alias = "page_id_continuity_ready", default)]
    pub block_id_continuity_ready: bool,
    #[serde(default)]
    pub logical_stream_bytes_read: u64,
    #[serde(default)]
    #[serde(alias = "band_state_transition_count")]
    pub slab_state_transition_count: u64,
    pub logical_stream_read_ready: bool,
    pub append_roll_ready: bool,
    #[serde(alias = "extent_manifest_ready", alias = "zone_manifest_ready")]
    #[serde(alias = "band_manifest_ready")]
    pub slab_manifest_ready: bool,
    #[serde(default)]
    #[serde(alias = "band_manifest_rebuild_ready")]
    pub slab_manifest_rebuild_ready: bool,
    #[serde(default)]
    #[serde(alias = "band_manifest_reconciled_on_open")]
    pub slab_manifest_reconciled_on_open: bool,
    #[serde(default)]
    #[serde(alias = "band_manifest_disk_consistent")]
    pub slab_manifest_disk_consistent: bool,
    #[serde(default)]
    #[serde(alias = "manifest_missing_stream_bands")]
    pub manifest_missing_stream_slabs: u64,
    #[serde(default)]
    #[serde(alias = "manifest_extra_stream_bands")]
    pub manifest_extra_stream_slabs: u64,
    #[serde(default)]
    #[serde(alias = "corrupt_band_count")]
    pub corrupt_slab_count: u64,
    #[serde(default)]
    #[serde(alias = "partial_band_count")]
    pub partial_slab_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "partial_band_recovery_ready")]
    pub partial_slab_recovery_ready: bool,
    pub envelope_checksum_ready: bool,
    pub compression_stream_ready: bool,
    pub delayed_destroy_ready: bool,
    #[serde(default)]
    pub purge_lifecycle_ready: bool,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabReport {
    #[serde(rename = "block_segment_id", alias = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub block_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    /// ALWAYS 0 TODAY. `inspect_slab` fills this from `header.object_id`, and
    /// `parse_block_record_header` hardcodes that to `None` because the record envelope
    /// carries no object id -- the index holds it instead. The counting code in
    /// `inspect_slab` sits behind `if let Some(..)` on that field, so it never runs.
    /// Kept rather than removed: this is a serialised public field, so dropping it is a
    /// surface change. Read it as "not reported", never as "this slab has no objects".
    #[serde(default)]
    pub object_count: u64,
    /// ALWAYS 0 TODAY, for the same reason as `object_count`: `header.routing_bucket` is
    /// hardcoded to `None` by the header parser.
    #[serde(default)]
    #[serde(alias = "routing_slot_count")]
    pub routing_bucket_count: u64,
    pub compressed_records: u64,
    #[serde(rename = "first_block_id", alias = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_block_id", alias = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
    /// ALWAYS `None` TODAY -- see `routing_bucket_count`. With
    /// `skip_serializing_if = "Option::is_none"` this means the field is absent from the
    /// serialised report entirely, rather than present and empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "first_routing_slot")]
    pub first_routing_bucket: Option<u32>,
    /// ALWAYS `None` TODAY -- see `first_routing_bucket`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "last_routing_slot")]
    pub last_routing_bucket: Option<u32>,
    #[serde(default, alias = "page_index_count")]
    pub block_index_count: u64,
    #[serde(default, alias = "page_index_entries")]
    pub block_index_entries: Vec<BlockStoreBlockIndexReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreBlockIndexReport {
    #[serde(alias = "page_segment_id")]
    #[serde(alias = "block_segment_id")]
    #[serde(alias = "page_slab_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_address")]
    pub compact_slab_address: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_id")]
    pub compact_slab_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_offset")]
    pub compact_slab_offset: Option<u32>,
    #[serde(
        default,
        alias = "extent_id",
        alias = "zone_id",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "storage_segment_id")]
    pub storage_slab_id: Option<u64>,
    /// ALWAYS `None` TODAY. `inspect_slab` copies this straight from `header.object_id`,
    /// which `parse_block_record_header` hardcodes to `None`: the record envelope carries no
    /// object id, the index does. With `skip_serializing_if` the field is simply absent from
    /// the serialised entry. Kept rather than removed -- a public serialised field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    /// ALWAYS `None` TODAY: `inspect_slab` writes the literal `None` here, as nothing in the
    /// record envelope names a model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<u8>,
    #[serde(default, alias = "page_id", skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(alias = "page_size")]
    pub block_size: u64,
    pub stored_size: u64,
    pub dirty: bool,
    pub deleted: bool,
    #[serde(alias = "page_in_log")]
    pub block_in_log: bool,
    /// ALWAYS `None` TODAY, for the same reason as `object_id`: `header.routing_bucket` is
    /// hardcoded to `None` by the header parser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "routing_slot")]
    pub routing_bucket: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlockStoreSlabManifest {
    version: u32,
    /// ON-DISK KEY, NOT THE RUST NAME. `slab_manifest.json` is a format an already-deployed
    /// binary reads, and the descriptor's `band_id` carries no `#[serde(default)]`, so a manifest
    /// written under new keys is unreadable to it rather than merely unfamiliar.
    /// `a_folded_manifest_still_writes_the_keys_on_disk` pins both keys.
    #[serde(rename = "bands", alias = "extents", alias = "zones")]
    slabs: Vec<BlockStoreSlabDescriptor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreRollReport {
    #[serde(alias = "previous_page_segment_id")]
    #[serde(alias = "previous_page_slab_id")]
    pub previous_block_slab_id: u64,
    #[serde(alias = "new_page_segment_id")]
    #[serde(alias = "new_page_slab_id")]
    pub new_block_slab_id: u64,
}

#[derive(Debug, Clone)]
pub struct BlockStore {
    inner: Arc<Mutex<BlockStoreInner>>,
}

impl BlockStore {
    /// Which store this is, as a value that can be compared and hashed.
    ///
    /// Clones of one store share their inner handle, so they answer the same; two engines --
    /// which is what an embedded process runs several of -- never do. That distinction is the
    /// only thing separating two engines that both serve shard 1 and both hold a key called
    /// "user:1", so anything keyed per object has to include it.
    pub fn store_id(&self) -> usize {
        Arc::as_ptr(&self.inner) as *const u8 as usize
    }
}

#[derive(Debug)]
struct BlockStoreInner {
    root: PathBuf,
    // Set when a relaxed (bulk) append deferred its fsync + manifest persist;
    // cleared by sync_durable(). See bulk_relaxed_durability().
    relaxed_dirty: bool,
    block_slab_id: u64,
    write_offset: u64,
    next_block_id: u64,
    options: BlockStoreOptions,
    slabs: BTreeMap<u64, BlockStoreSlabDescriptor>,
    /// Slab installs since the manifest was last written out. Writing it costs the whole manifest,
    /// so it is written every so often rather than every install; the load rebuilds from the slabs
    /// when what it reads does not match them.
    slabs_unwritten: usize,
    slab_manifest_reconciled_on_open: bool,
    /// Sealed slabs this open kept from the manifest WITHOUT re-reading them. Not part of any
    /// report wire shape; it exists so a guard aimed at that route can prove the route ran.
    slabs_skipped_reinspection_on_open: usize,
    /// What THIS store's open read off its own disk: the slab manifest, plus every slab the
    /// inspection did not skip.
    ///
    /// Kept per store rather than only on the process counter because a process that opens more
    /// than one store -- which the suite does constantly -- cannot tell one open's reading from
    /// another's on a shared atomic.
    open_file_bytes_read: u64,
    open_file_reads: u64,
    /// Per-slab live block tallies, PUBLISHED by the index that maintains them.
    ///
    /// `None` until an index has published once, and the difference matters: an empty map means
    /// "an index looked and found no live blocks anywhere", while `None` means "nobody has told
    /// this store anything" -- and only the second may fall back to the older neighbour-sum
    /// figure.
    ///
    /// A snapshot, so it can be stale -- and staleness here is safe in ONE DIRECTION ONLY, which
    /// is why it is allowed. `used_bytes` feeds the garbage floor, and the floor only ever KEEPS a
    /// slab; it never grants permission to delete one. Deletion is gated by the live slab id set,
    /// computed fresh at the call. So a stale tally that overstates live bytes costs a round of
    /// collection, and one that understates them cannot reach anything the live-set test would
    /// have retained.
    live_block_bytes: Option<BTreeMap<u64, BlockStoreSlabLive>>,
    stats: BlockStoreStats,
    // Optional shared-storage read-through (on-demand lazy recovery): set by
    // attach_shared_slab_source() after a metadata-only restore. When present, a
    // read that misses a slab locally fetches it from here, caches it, then serves.
    shared_slab_source: Option<Arc<dyn SharedSlabSource>>,
    /// The earliest moment any slab now in quarantine was set aside -- when this process KNOWS it.
    ///
    /// WHAT IT BUYS. A quarantined slab waits `DELAYED_DESTROY_MIN_AGE_MS` -- an hour -- and for
    /// that whole hour the purge has nothing it may act on. It found that out by opening the trash
    /// directory and stat-ing every file in it, under the store-wide lock, once per round. Knowing
    /// the earliest arrival turns the question into one comparison: if the oldest thing in there is
    /// not old enough, nothing in there is, and the round can decline without the walk.
    ///
    /// TWO STATES, AND ONLY ONE OF THEM IS A CLAIM. `Some(t)` asserts that every slab still in
    /// quarantine arrived at or after `t`. `None` asserts nothing and forces the walk, which is
    /// the behaviour this whole field is an optimisation of -- so every uncertainty collapses to
    /// `None` and costs at worst what the round cost before.
    ///
    /// WHICH WAY IT MAY BE WRONG. Stale EARLY -- holding a `t` before the true earliest -- makes
    /// `now - t` larger, so the round walks when it need not: a wasted walk, exactly today's cost.
    /// Stale LATE would make the round decline while something had in fact matured, which costs a
    /// delayed destroy. The write rules exist to permit only the first: `t` is only ever RAISED by
    /// a round that walked the whole directory and read a manifest stamp for every entry that was
    /// still in it when the round ended, and it is lowered to `min(t, arrival)` by every new
    /// quarantine. Removing entries can only raise the true earliest, so a `t` left behind by a
    /// destroy or a restore is stale early, which is why neither has to clear it.
    ///
    /// NOT PERSISTED, AND THAT IS LOAD-BEARING. A crash can leave files in quarantine that the
    /// manifest never learned about, and those have no descriptor for the liveness test that
    /// guards the decline. They can only be introduced by a crash, a crash ends the process, and a
    /// fresh process starts here at `None` -- so the first round after any restart walks, and it
    /// is that round which decides whether a `Some` is justified.
    delayed_destroy_earliest_unix_ms: Option<u64>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<Arc<crate::scratch::ScratchDirGuard>>,
}

/// Slab installs allowed to go by before the slab manifest is written out.
///
/// Writing it costs the whole manifest, so writing it per install makes installing n slabs cost n
/// manifests. Deferring trades that for a rebuild after a crash, which the load does for itself
/// when what it reads does not match the slabs on disk.
const SLABS_UNWRITTEN_BEFORE_PERSIST: usize = 64;

/// Fold one still-quarantined entry into the earliest arrival a purge round may publish.
///
/// `arrived_at` is the entry's MANIFEST stamp and nothing else. `None` withdraws the round's
/// opinion outright rather than leaving the entry out of the minimum, and the difference is the
/// whole safety of the thing: an entry left out would be an entry the published earliest does not
/// cover, so a later round could decline while that entry was already old enough. There is no
/// weaker answer available here -- either the round can account for every entry that stayed, or it
/// publishes nothing and the next round walks.
fn observe_quarantined_arrival(
    arrived_at: Option<u64>,
    earliest: &mut Option<u64>,
    may_publish: &mut bool,
) {
    match arrived_at {
        Some(arrived_at) => {
            *earliest = Some(match *earliest {
                Some(current) => current.min(arrived_at),
                None => arrived_at,
            })
        }
        None => *may_publish = false,
    }
}

/// Does the caller's live set name any slab this store has in quarantine?
///
/// THE ONE THING A DECLINE CANNOT GUESS. The purge restores a quarantined slab the caller still
/// calls live, at any age, before the age is consulted at all -- so a round that declines in
/// advance must first rule that out, and the age cache says nothing about it. Reading the state
/// off `slabs` is what makes the answer free: the map is already in memory and already holds the
/// state the quarantine set, so this is a lookup per live id and no system call.
///
/// A slab in the trash directory with NO descriptor is invisible here, which is why an entry
/// without a manifest stamp stops the round publishing an earliest at all: with nothing published
/// there is no decline, and with no decline there is nothing for this to have missed.
fn live_set_names_a_quarantined_slab(
    slabs: &BTreeMap<u64, BlockStoreSlabDescriptor>,
    live_block_slab_ids: &BTreeSet<u64>,
) -> bool {
    live_block_slab_ids.iter().any(|id| {
        slabs
            .get(id)
            .map(|slab| slab.state == BlockStoreSlabState::DelayedDestroy)
            .unwrap_or(false)
    })
}

impl BlockStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_options(root, BlockStoreOptions::default())
    }

    pub fn with_options(root: impl Into<PathBuf>, options: BlockStoreOptions) -> Self {
        let root = root.into();
        let _ = std::fs::create_dir_all(&root);
        let block_slab_id = latest_slab_id_at(&root).unwrap_or_default();
        let mut write_offset = slab_path(&root, block_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let manifest_exists =
            slab_manifest_path(&root).exists() || legacy_zone_manifest_path(&root).exists();
        // WHAT THE OPEN READS, TAKEN ON THE OPEN'S OWN TALLY. Every byte below also lands on the
        // process counter when these tallies drop; this is the per-store figure beside it, and
        // the store keeps it because the process counter cannot answer "what did THIS open read"
        // in a process that opens more than one store.
        let mut open_tally = BlockStoreReadTally::start();
        let (mut slabs, mut manifest_rebuilt) = if manifest_exists {
            match load_slab_manifest_at(&root, &mut open_tally) {
                Ok(slabs) => (slabs, false),
                Err(_) => (
                    rebuild_slab_manifest_at(&root, &mut open_tally).unwrap_or_default(),
                    true,
                ),
            }
        } else {
            (
                rebuild_slab_manifest_at(&root, &mut open_tally).unwrap_or_default(),
                true,
            )
        };
        // Loaded BEFORE the block-id scan on purpose: the manifest already records `last_page_id`
        // per slab, and reading it turns a walk over every block record header -- 90% of a
        // steady-state open's reads -- into a few MB. Any slab the manifest cannot prove unchanged
        // is still walked, and the active slab always is.
        // Nothing allocates from a store-wide block counter any more: a block id is an index
        // inside its object, and the object is what knows how many blocks it has. Recovering a
        // counter here used to mean reading every block header in every slab to work out one
        // integer -- on a live-store copy, the bulk of a steady-state open.
        let next_block_id = 0;
        let reconciled = reconcile_slab_manifest_with_disk(&root, &mut slabs).unwrap_or_default();
        let slab_manifest_reconciled_on_open = reconciled.changed;
        let slabs_skipped_reinspection_on_open = reconciled.slabs_skipped_reinspection;
        // The manifest this open read, plus every slab the inspection read on its workers.
        let (loaded_bytes, loaded_reads) = open_tally.taken();
        let open_file_bytes_read = loaded_bytes.saturating_add(reconciled.file_bytes_read);
        let open_file_reads = loaded_reads.saturating_add(reconciled.file_reads);
        manifest_rebuilt |= slab_manifest_reconciled_on_open;
        ensure_slab_descriptor(
            &mut slabs,
            &root,
            block_slab_id,
            BlockStoreSlabState::Active,
        );
        // Fence a torn tail on the ACTIVE slab. After a crash mid-append the raw file length
        // includes uncommitted/partial bytes past the last intact record; reconcile computed
        // the intact `readable_prefix`. Resuming appends at raw EOF would embed the torn record
        // permanently mid-slab and, via the early-halting block-id scan, regress next_page_id ->
        // block-id/generation reuse -> stale reads. Mirror the resume-at-committed-length:
        // physically truncate the active slab to its readable prefix and resume there.
        let active_readable_prefix = slabs
            .get(&block_slab_id)
            .map(|slab| slab.readable_prefix_physical_bytes);
        if let Some(readable_prefix) = active_readable_prefix {
            if readable_prefix < write_offset {
                if let Ok(file) = OpenOptions::new()
                    .write(true)
                    .open(slab_path(&root, block_slab_id))
                {
                    if file.set_len(readable_prefix).is_ok() {
                        crate::durability_metrics::record_barrier("block_store_open");
                        let _ = file.sync_all();
                        if let Ok(dir) = std::fs::File::open(&root) {
                            let _ = dir.sync_all();
                        }
                        write_offset = readable_prefix;
                        if let Some(slab) = slabs.get_mut(&block_slab_id) {
                            slab.physical_bytes = readable_prefix;
                            slab.has_corruption = false;
                            slab.first_error_offset = None;
                        }
                        manifest_rebuilt = true;
                    }
                }
            }
        }
        // The open path has no store to count against yet, so it counts into the stats this open
        // is about to install. A rebuild-on-open IS a whole manifest write and belongs in the
        // number the caller reads afterwards.
        let mut stats = BlockStoreStats::default();
        if manifest_rebuilt {
            let _ = persist_slab_manifest(&root, &slabs, &mut stats);
        }
        Self {
            inner: Arc::new(Mutex::new(BlockStoreInner {
                root,
                relaxed_dirty: false,
                block_slab_id,
                write_offset,
                next_block_id,
                options,
                slabs,
                slabs_unwritten: 0,
                slab_manifest_reconciled_on_open,
                slabs_skipped_reinspection_on_open,
                open_file_bytes_read,
                open_file_reads,
                live_block_bytes: None,
                stats,
                shared_slab_source: None,
                // A FRESH PROCESS KNOWS NOTHING ABOUT THE QUARANTINE IT INHERITED, and that is
                // the state this has to open in: the directory on disk may hold slabs this
                // manifest never learned about, so the first purge round must walk.
                delayed_destroy_earliest_unix_ms: None,
                scratch: None,
            })),
        }
    }

    /// Attach a shared-storage read-through source (on-demand lazy recovery). After a
    /// metadata-only restore installs the served index and a slab address map, this
    /// lets a later read fetch a missing old slab on demand from shared storage,
    /// cache it locally, and serve it — instead of installing every slab up front.
    pub fn attach_shared_slab_source(&self, source: Arc<dyn SharedSlabSource>) {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .shared_slab_source = Some(source);
    }

    pub fn has_shared_slab_source(&self) -> bool {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .shared_slab_source
            .is_some()
    }

    /// The next free block id this store would assign on the next append. Recorded in a
    /// shared checkpoint so a lazy restore can advance the fresh owner's counter past it.
    pub fn next_block_id(&self) -> u64 {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .next_block_id
    }

    /// Reserve the slab-id (and block-id) range consumed by a lazily-restored checkpoint
    /// so replayed/new appends land in a FRESH slab beyond it, never overwriting a slab
    /// that is still served on-demand from shared storage. Matches the recovery behaviour
    /// model where old blocks stay addressable in shared storage while new writes roll
    /// forward. Called right after attaching the shared read-through on a fresh owner.
    pub fn reserve_lazy_checkpoint_range(
        &self,
        through_slab_id: u64,
        next_block_id_floor: u64,
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        std::fs::create_dir_all(&inner.root)?;
        let existing_max = slab_ids_at(&inner.root)?.into_iter().max();
        let new_slab_id = through_slab_id
            .max(inner.block_slab_id)
            .max(existing_max.unwrap_or_default())
            .saturating_add(1);
        let path = slab_path(&inner.root, new_slab_id);
        let file = std::fs::File::create(&path)?;
        crate::durability_metrics::record_barrier("block_store_checkpoint_reserve");
        file.sync_all()?;
        sync_parent_dir(&path)?;
        inner.block_slab_id = new_slab_id;
        inner.write_offset = 0;
        inner.next_block_id = inner.next_block_id.max(next_block_id_floor);
        let now = now_unix_ms();
        // Any previously-active local slab is now sealed; the reserved slab is active.
        for slab in inner.slabs.values_mut() {
            if slab.state == BlockStoreSlabState::Active {
                slab.state = BlockStoreSlabState::Sealed;
                slab.updated_unix_ms = Some(now);
            }
        }
        inner.slabs.insert(
            new_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: new_slab_id,
                block_slab_id: new_slab_id,
                state: BlockStoreSlabState::Active,
                physical_bytes: 0,
                logical_bytes: 0,
                created_unix_ms: Some(now),
                updated_unix_ms: Some(now),
                first_block_id: None,
                last_block_id: None,
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        inner.persist_slab_manifest_counted()?;
        Ok(())
    }

    /// Install SEALED slab descriptors for the slabs a lazy checkpoint restore backs from shared
    /// storage. The slab bytes are NOT local yet (they are fetched on demand through the attached
    /// shared read-through), but a GC/compaction cycle running between restore and the first fetch
    /// must still see these sealed slabs, or it accounts on an incomplete picture and could
    /// reclaim prematurely. Recording them here makes `slab_summary()`/`slab_descriptors()`
    /// complete immediately after restore. Any slab id that is the current active slab, or already
    /// has a descriptor (e.g. it was fetched or is local), is left untouched. Call AFTER
    /// [`reserve_lazy_checkpoint_range`] so the reserved slab is the active one and every
    /// checkpoint slab is correctly sealed.
    pub fn install_lazy_checkpoint_slabs(
        &self,
        slabs: &[LazyCheckpointSlab],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let root = inner.root.clone();
        let active = inner.block_slab_id;
        let mut changed = false;
        for slab in slabs {
            // Never touch the active (reserved) slab — it holds live local writes.
            if slab.block_slab_id == active {
                continue;
            }
            // If the slab is materialized locally (already fetched / a real local slab), its
            // existing descriptor reflects real on-disk bytes and is authoritative — leave it.
            if slab_path(&root, slab.block_slab_id).exists() {
                continue;
            }
            // Lazily-backed checkpoint slab: install (or replace a fresh-store placeholder — a
            // freshly opened block store seeds an empty Active descriptor for slab 0, which
            // `reserve_lazy_checkpoint_range` then seals; that stale empty descriptor must be
            // overwritten with the checkpoint's real slab metadata, not skipped) a complete
            // SEALED descriptor so accounting is correct before any fetch.
            let descriptor = BlockStoreSlabDescriptor {
                stored_slab_id: slab.block_slab_id,
                block_slab_id: slab.block_slab_id,
                state: BlockStoreSlabState::Sealed,
                physical_bytes: slab.physical_bytes,
                logical_bytes: slab.logical_bytes,
                created_unix_ms: slab.created_unix_ms,
                updated_unix_ms: slab.updated_unix_ms,
                first_block_id: slab.first_block_id,
                last_block_id: slab.last_block_id,
                readable_prefix_physical_bytes: slab.physical_bytes,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            };
            if inner.slabs.get(&slab.block_slab_id) != Some(&descriptor) {
                inner.slabs.insert(slab.block_slab_id, descriptor);
                changed = true;
            }
        }
        if changed {
            inner.persist_slab_manifest_counted()?;
        }
        Ok(())
    }

    /// Ensure `block_slab_id` is present locally, fetching it from the shared
    /// read-through source (if any) on a local miss, caching it, and counting the
    /// on-demand fetch. A no-op when the slab already exists locally or no source is
    /// attached; in the latter case the normal read path surfaces the miss.
    fn ensure_slab_present(&self, block_slab_id: u64) -> Result<(), BlockStoreError> {
        let (root, source) = {
            let inner = self.inner.lock().expect("block store lock poisoned");
            (inner.root.clone(), inner.shared_slab_source.clone())
        };
        if slab_path(&root, block_slab_id).exists() {
            return Ok(());
        }
        let Some(source) = source else {
            return Ok(());
        };
        if let Some(bytes) = source.fetch_slab(block_slab_id)? {
            self.install_slab(block_slab_id, &bytes)?;
            self.inner
                .lock()
                .expect("block store lock poisoned")
                .stats
                .shared_slab_fetches += 1;
        }
        Ok(())
    }

    pub fn roll_slab(&self) -> Result<BlockStoreRollReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        roll_slab_inner(&mut inner)
    }

    /// Roll the active slab ahead of the write that would otherwise have to, off the client
    /// path. Returns the roll report if a roll happened, `None` if the active slab still has
    /// room.
    ///
    /// Rolling is not cheap: `roll_slab_inner` fsyncs the outgoing slab, scans the slab
    /// directory to pick the next id, creates and fsyncs the new file, fsyncs the parent
    /// directory, and persists the slab manifest. Run inline from `append` -- which is where
    /// it runs today -- one unlucky client write pays all of that on top of its own
    /// durability barrier, a latency outlier unrelated to the size of the write that
    /// triggered it.
    ///
    /// So the roll is kept off the write path: the background storage-manager cycle runs a
    /// prepare step that no-ops while the active slab is under target and otherwise rolls to a
    /// fresh one, so a client append finds space already waiting. The inline roll in `append`
    /// stays as the fallback for a write that arrives before the background cycle got here.
    ///
    /// WHAT THE ROLL COSTS GROWS WITH THE SHARD. `the_roll_at_scale` measures it: the directory
    /// scan and the whole-manifest rewrite are both linear in the slab count, so the prepare step
    /// this method exists to serve gets more valuable, not less, as a shard ages.
    pub fn prepare_next_slab(&self) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        self.prepare_next_slab_with_target(effective_block_slab_target_bytes())
    }

    /// [`prepare_next_slab`] against an explicit target rather than the process-wide
    /// configured one.
    ///
    /// The slab target is only reachable through a global env var, so a test that wanted a
    /// small target would have to mutate process state — and if it then failed an assertion
    /// before restoring it, every later test in the process would inherit the small target.
    /// Taking the target as an argument keeps that class of cross-test failure impossible.
    pub fn prepare_next_slab_with_target(
        &self,
        slab_target_bytes: u64,
    ) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if !slab_is_at_target(inner.write_offset, slab_target_bytes) {
            return Ok(None);
        }
        roll_slab_inner(&mut inner).map(Some)
    }

    /// Whether [`prepare_next_slab`] would roll right now, without doing it. Lets a caller
    /// (a report, or a scheduler deciding whether the phase is worth running) ask about
    /// pressure without paying for it.
    pub fn needs_slab_preparation(&self) -> bool {
        self.needs_slab_preparation_with_target(effective_block_slab_target_bytes())
    }

    /// [`needs_slab_preparation`] against an explicit target.
    pub fn needs_slab_preparation_with_target(&self, slab_target_bytes: u64) -> bool {
        let inner = self.inner.lock().expect("block store lock poisoned");
        slab_is_at_target(inner.write_offset, slab_target_bytes)
    }

    /// Bytes written to the active slab. Exposed so a caller can reason about slab pressure
    /// (and so tests can drive the prepare threshold without touching global config).
    pub fn active_slab_write_offset(&self) -> u64 {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .write_offset
    }

    /// Every slab id in the store root, read from the directory.
    ///
    /// DELEGATES rather than repeats. This was a second, inline copy of `slab_ids_at`'s loop --
    /// same prefix, same suffix, same parse, same sort -- so the two walks could drift, and a
    /// counter or a guard placed on one of them saw none of the other's callers. That is the
    /// shape this crate has been bitten by before: a guard covering one of two live copies lets
    /// the other keep the defect.
    pub fn slab_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        #[cfg(test)]
        crate::snapshot_probe::note_slab_dir_listing();
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        slab_ids_at(&root)
    }

    pub fn slab_usage(&self) -> Vec<BlockStoreSlabUsage> {
        compute_slab_usage(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .slabs,
        )
    }


    pub fn delayed_destroy_slab_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_slab_ids_at(&root)
    }

    pub fn delayed_destroy_slab_reports(
        &self,
    ) -> Result<Vec<BlockStoreDelayedDestroySlabReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_slab_reports_at(&root)
    }

    pub fn purge_delayed_destroy_slabs(&self) -> Result<Vec<u64>, BlockStoreError> {
        Ok(self
            .purge_delayed_destroy_slabs_with_report()?
            .purged_block_slab_ids)
    }

    pub fn purge_delayed_destroy_slabs_with_report(
        &self,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
    }

    /// Purge quarantined slabs that have waited at least `min_age_ms`.
    ///
    /// The age is a parameter so a caller that wants an immediate purge has to SAY so, rather
    /// than a suite quietly depending on there being no delay at all.
    ///
    /// Purges WITHOUT a liveness re-check. Every caller that can name a live set should call
    /// [`Self::purge_delayed_destroy_slabs_checked`] instead; this spelling remains for the
    /// callers that genuinely have nothing to re-check against.
    pub(crate) fn purge_delayed_destroy_slabs_older_than(
        &self,
        min_age_ms: u64,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_checked(min_age_ms, std::iter::empty())
    }

    /// Purge quarantined slabs, re-checking liveness one last time before each destroy.
    ///
    /// THE DESTROY IS THE ONLY IRREVERSIBLE STEP, AND UNTIL NOW IT CONSULTED NOTHING. The purge
    /// read a directory and a timestamp; it took no live set and re-read no index. Everything it
    /// knew about whether a slab was still needed was decided in an earlier round, by the
    /// collector that quarantined it, against the state of the store at THAT moment. Between the
    /// two, a dump manifest can be written whose embedded index installs blocks in a slab that was
    /// unreferenced when the collector looked -- and the purge would unlink it without ever
    /// asking.
    ///
    /// So the destroy asks again, against the live set the caller holds NOW, and a slab that
    /// comes back live is not destroyed. It is taken back out of quarantine: renamed into the
    /// store, its descriptor returned to `Sealed`, and reported in `restored_block_slab_ids`.
    /// Un-quarantining is not a nicety here the way rolling a slab back to its pre-collection
    /// state would be elsewhere -- our phase 1 RENAMES the file, so a slab left sitting in the
    /// trash directory is unreadable by path no matter how long the grace window runs. Declining
    /// to destroy it would leave the reader just as broken. Moving it back is the whole repair.
    ///
    /// A restore is an ALARM. Reaching it means the collector and the re-check disagreed about
    /// the same slab, which is a bug upstream of this function; the point of the re-check is that
    /// the bug costs a log line and a rename instead of the data.
    pub fn purge_delayed_destroy_slabs_checked(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_selected(min_age_ms, live_block_slab_ids, None)
    }

    /// Purge only the quarantined slabs the caller NAMES.
    ///
    /// `None` purges every quarantined slab old enough, which is what this did before it could be
    /// told otherwise. `Some(set)` purges only the intersection, and a quarantined slab left out
    /// of the set is neither destroyed nor reported as too young -- it is simply not this round's
    /// business, and it comes back as a candidate next round.
    ///
    /// THIS EXISTS SO THE RECLAIM GATE CAN STOP BEING ALL-OR-NOTHING. The dependency plan works
    /// out, per slab, which candidates are pinned and which are free; until now the collector and
    /// the purge both consulted a single store-wide boolean derived from that plan, so one pinned
    /// slab suppressed the collection of every other candidate. Letting the plan's answer through
    /// per slab requires the purge to accept a list: otherwise a purge that ran because SOME slab
    /// was free would destroy the quarantined slabs the plan had specifically blocked, which is a
    /// loss path rather than an inefficiency. The two changes only make sense together.
    pub fn purge_delayed_destroy_slabs_selected(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        selected_block_slab_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_capped(
            min_age_ms,
            live_block_slab_ids,
            selected_block_slab_ids,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
    }

    /// The purge, bounded to `max_slabs_per_round` slabs of work. 0 means unbounded.
    ///
    /// THE BOUND IS ON THE LOCK HOLD. Everything above funnels into this function, and this
    /// function takes the store-wide lock and keeps it until it returns, so the round's length is
    /// the length of time nothing else can touch the store. Unbounded, that was 58.7 seconds at
    /// eighty thousand quarantined slabs, measured, and linear -- the quarantine has no ceiling of
    /// its own, so neither did the hold. The cap is the shipped default rather than an opt-in
    /// because a caller that forgets it does not get a slower purge, it gets a minute-long stall.
    ///
    /// THE BUDGET IS SPENT ON WORK DONE -- a destroy or a restore -- AND NOT ON ENTRIES EXAMINED.
    /// That is the whole reason this advances rather than stalling, and getting it the other way
    /// round is the failure this has to avoid: a slab that is skipped (not named by the caller's
    /// list, not old enough yet, or live-but-blocked) STAYS IN THE DIRECTORY, so if a skip cost
    /// budget, a quarantine whose first thousand entries were all blocked would spend every
    /// round's whole budget re-skipping the same thousand and destroy nothing, for ever. Spending
    /// budget only on work makes progress unconditional: every slab this round charges to the
    /// budget LEAVES the trash directory -- unlinked, or renamed back into the store -- so the
    /// actionable set is strictly smaller next round, and the skipped prefix the loop walks past
    /// is bounded by the number of slabs that are being skipped for a reason of their own.
    ///
    /// THE RE-CHECK IS NOT WHAT GETS CAPPED. Every slab the round reaches goes through the full
    /// liveness re-check and the un-quarantining restore before anything irreversible happens to
    /// it; the budget decides HOW MANY slabs a round reaches, never what happens to one it did.
    /// A round that stops early leaves no slab half-processed: each iteration destroys or
    /// restores one slab completely, and the manifest written after the loop records exactly the
    /// slabs whose state actually changed. A slab the round never reached is still quarantined,
    /// which is the state it was already in.
    ///
    /// AND A ROUND WITH NOTHING IT MAY ACT ON DOES NOT OPEN THE DIRECTORY AT ALL. The cap above
    /// bounds a round that is doing work; it cannot bound a round in the grace window, because a
    /// too-young slab is passed over and passing over is free, so `processed` never reaches the
    /// budget and the `break` never fires. Measured, one round, budget 1,000: eight thousand
    /// quarantined slabs cost eight thousand examined entries and eighty thousand cost eighty
    /// thousand, against a flat thousand once the same slabs are actionable -- the walk is linear
    /// in the quarantine and the cap is not in it. `DELAYED_DESTROY_MIN_AGE_MS` is an hour, so
    /// that was every round for an hour after any reclaim, each one holding the store-wide lock
    /// for the length of the walk.
    ///
    /// `delayed_destroy_earliest_unix_ms` is what lets the round know this without looking: if the
    /// oldest slab in quarantine is not yet old enough, no slab in quarantine is. Two conditions
    /// have to hold before the round may decline on it, and they are not the same condition:
    ///
    ///   * NOTHING CAN HAVE MATURED. `now - earliest < min_age_ms`. At `min_age_ms == 0` this is
    ///     false whatever the clock says, so a caller asking for an immediate purge always walks.
    ///   * AND NOTHING NEEDS RESCUING. The liveness re-check restores a quarantined slab the
    ///     caller still calls live, and it does that at ANY age -- it runs before the age is even
    ///     consulted, because a live slab is not too young to destroy, it is not for destroying.
    ///     Declining in advance would defer that restore, so the decline asks first whether any
    ///     id in the caller's live set is a slab this store has marked `DelayedDestroy`. That is
    ///     a lookup per live id in a map already in memory, and it issues no system call.
    ///
    /// The decline is therefore not a heuristic about a round that probably would have done
    /// nothing. It fires only in a state where the walk provably would have destroyed nothing,
    /// restored nothing, synced nothing and rewritten no manifest -- every entry classified
    /// `retained_too_young` and the round returning with `processed_block_slabs == 0`. What it
    /// does change is that the `retained_too_young` list comes back empty rather than naming all
    /// eighty thousand, which is what `declined_without_examining` is on the report to say.
    pub fn purge_delayed_destroy_slabs_capped(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        selected_block_slab_ids: Option<BTreeSet<u64>>,
        max_slabs_per_round: usize,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let trash_dir = delayed_destroy_dir(&inner.root);
        let mut purged = Vec::new();
        let mut purged_physical_bytes = 0;
        let mut retained_too_young = Vec::new();
        let mut retained_too_young_physical_bytes = 0;
        let mut restored = Vec::new();
        let mut restored_physical_bytes = 0;
        let mut restore_blocked = Vec::new();
        let mut processed = 0usize;
        let mut examined = 0usize;
        let mut budget_exhausted = false;
        if !trash_dir.exists() {
            return Ok(BlockStorePurgeDelayedDestroyReport {
                max_slabs_per_round,
                ..Default::default()
            });
        }
        // THE ROUND THAT DOES NOT HAPPEN. See the note above the signature for why this is an
        // equivalence rather than an approximation; both halves of the condition are required and
        // neither costs a system call.
        if let Some(earliest) = inner.delayed_destroy_earliest_unix_ms {
            if now_unix_ms().saturating_sub(earliest) < min_age_ms
                && !live_set_names_a_quarantined_slab(&inner.slabs, &live_block_slab_ids)
            {
                return Ok(BlockStorePurgeDelayedDestroyReport {
                    max_slabs_per_round,
                    declined_without_examining: true,
                    ..Default::default()
                });
            }
        }
        // WHAT THIS ROUND LEARNS ABOUT THE EARLIEST ARRIVAL, which it may only publish if it ends
        // up having seen every entry that is still in the directory when it finishes. `None` here
        // is not "no entries"; it is "this round is not entitled to an opinion", and it is the
        // value every uncertainty resolves to.
        let mut earliest_seen: Option<u64> = None;
        let mut may_publish_earliest = true;
        let root = inner.root.clone();
        // The purge's OWN walk of the same directory, counted under the same name as the two the
        // storage round takes through `delayed_destroy_slab_reports_at` before reaching here --
        // the point of the tally is the total per round, and which of the three is which is a
        // question for a profile rather than a counter.
        crate::durability_metrics::record_scan("block_store_trash_dir_walk", 1);
        for entry in std::fs::read_dir(&trash_dir)? {
            // COUNTED HERE, AT THE TOP, AND NOT WHERE THE ENTRY GETS A NAME. What this number is
            // for is telling a round that STOPPED from a round that walked on, and those two
            // differ in iterations, not in classifications -- put the increment after the budget
            // check below and both readings come back equal to the budget, which is the hole this
            // field exists to close.
            examined += 1;
            // THE BUDGET IS CHECKED BEFORE THE ENTRY IS EVEN NAMED, and it stops the round rather
            // than advancing to the next entry. Continuing would walk the remaining eighty
            // thousand directory entries to do no work; stopping is what makes a capped round
            // cost the budget rather than the directory. The unexamined entries stay exactly as
            // they are, and `budget_exhausted` tells the caller there are more.
            if max_slabs_per_round > 0 && processed >= max_slabs_per_round {
                budget_exhausted = true;
                break;
            }
            let entry = entry?;
            let Some(id) = delayed_destroy_slab_id_from_name(&entry.file_name()) else {
                continue;
            };
            // THE MANIFEST STAMP, READ ONCE AND USED TWICE. The age test below needs it, and so
            // does the earliest-arrival this round may publish -- which has to account for the
            // entries the age test never reaches, or a round narrowed by a caller's list could
            // never publish anything, and that list is exactly what the periodic cycle passes.
            // It is a lookup in a map already in memory, so reading it before the branch that
            // passes over the entry adds no system call and decides nothing.
            let stamped_at = inner.slabs.get(&id).and_then(|slab| slab.updated_unix_ms);
            let bytes = entry
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            // Not this round's business. Checked BEFORE the liveness re-check and before the
            // age, because a slab the caller did not name should be left exactly as it is: not
            // destroyed, not restored, and not reported as too young -- "too young" is a claim
            // about a slab that was considered, and this one was not.
            if selected_block_slab_ids
                .as_ref()
                .map(|selected| !selected.contains(&id))
                .unwrap_or(false)
            {
                // It stays in the directory, so it bounds what the next round may decline on.
                observe_quarantined_arrival(
                    stamped_at,
                    &mut earliest_seen,
                    &mut may_publish_earliest,
                );
                continue;
            }
            // THE LAST-CHANCE RE-CHECK, before the age is even consulted: a live slab is not
            // "too young to destroy", it is NOT FOR DESTROYING, and waiting longer would not
            // make it safe. It also must not be left where it is -- quarantine is a rename out
            // of the store, so the reader that needs it cannot reach it until the file is back.
            if live_block_slab_ids.contains(&id) {
                if restore_slab_from_delayed_destroy_unsynced(&root, id, &entry.path())? {
                    set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Sealed);
                    restored.push(id);
                    restored_physical_bytes += bytes;
                    // A restore is WORK: it renamed a file and changed a descriptor, and it is
                    // the expensive half of the measured round. A blocked restore is not -- it
                    // moved nothing, and charging budget for it would let a directory full of
                    // blocked slabs starve the destroys, which is the stall this cap must not
                    // have.
                    processed += 1;
                } else {
                    // A blocked restore moved nothing, so the slab is still in the directory and
                    // still bounds the next round.
                    observe_quarantined_arrival(
                        stamped_at,
                        &mut earliest_seen,
                        &mut may_publish_earliest,
                    );
                    restore_blocked.push(id);
                }
                continue;
            }
            // Quarantining goes through `set_slab_state`, which stamps `updated_unix_ms`, so the
            // manifest already records WHEN this slab was set aside.
            //
            // A DESCRIPTOR-LESS SLAB FALLS BACK TO THE FILE'S MTIME rather than being destroyed
            // on sight. The two are written at different moments and a crash fits between them:
            // the collector renames each slab into the trash directory inside its loop and
            // persists the manifest only once, after it. Die mid-loop and the restarted process
            // finds files in quarantine that the manifest never learned about -- freshly
            // quarantined slabs with no stamp, which the old reading purged on the very next
            // round with the full hour of grace skipped. The mtime is a weaker clock (a rename
            // keeps the mtime of the last append, so it runs EARLY and the window it grants is
            // shorter than the real one) but it is a clock, and a short window beats none.
            let quarantined_at = stamped_at.or_else(|| file_modified_unix_ms(&entry.path()));
            if let Some(quarantined_at) = quarantined_at {
                if now_unix_ms().saturating_sub(quarantined_at) < min_age_ms {
                    // HELD, SO STILL IN THE DIRECTORY -- and `stamped_at`, not `quarantined_at`.
                    // The mtime this may have fallen back to belongs to a slab the manifest does
                    // not know about, and an entry the manifest does not know about is one the
                    // decline's liveness test cannot see either. Passing the weaker clock on
                    // would be publishing an earliest that a later round could not defend, so the
                    // absent stamp withdraws this round's opinion instead.
                    observe_quarantined_arrival(
                        stamped_at,
                        &mut earliest_seen,
                        &mut may_publish_earliest,
                    );
                    retained_too_young.push(id);
                    retained_too_young_physical_bytes += bytes;
                    continue;
                }
            }
            purged_physical_bytes += bytes;
            std::fs::remove_file(entry.path())?;
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Purged);
            purged.push(id);
            processed += 1;
        }
        // WHAT THIS ROUND IS ENTITLED TO SAY ABOUT THE EARLIEST ARRIVAL.
        //
        // A round that stopped on its budget saw a prefix of the directory, and the entries
        // behind the `break` are exactly the ones it knows nothing about -- so it publishes
        // nothing, and the next round walks. Every other round reached the end of `read_dir` and
        // observed every entry that was still in the directory when it finished: the ones it
        // destroyed and the ones it restored had left, and the ones that stayed each went through
        // `observe_quarantined_arrival` on the way out of the loop body.
        //
        // `None` -- no entry stayed, or one that stayed had no manifest stamp -- means the next
        // round walks, which is the behaviour this replaces and therefore cannot be worse than.
        //
        // THE `budget_exhausted` ARM IS NOT PINNED BY A MUTANT, and saying so is better than
        // letting the next reader assume it is. Removing it survives the suite. The reason is
        // that a round only ever charges its budget to entries that LEAVE the directory, so in
        // every fixture whose directory order can be pinned the round that stops has observed no
        // stayer at all, `earliest_seen` is already `None`, and both arms return the same value.
        // Telling them apart needs a stayer in the walked prefix AND an older entry behind the
        // `break`, which needs control over the order `read_dir` returns -- an order that is not
        // the order of the ids, and that was measured here NOT to match what a separate listing
        // of the same directory reports. A guard resting on it would be measuring the filesystem.
        // `what_a_round_may_publish_about_the_earliest_arrival` asserts the arm's rule directly
        // and passes under the mutant for the reason just given. The arm stays because the case
        // it covers is real: an arrival behind the break can be older than anything in front of
        // it, and publishing the prefix's earliest would name a moment LATER than the truth.
        inner.delayed_destroy_earliest_unix_ms = if budget_exhausted {
            None
        } else if may_publish_earliest {
            earliest_seen
        } else {
            None
        };
        purged.sort_unstable();
        restored.sort_unstable();
        restore_blocked.sort_unstable();
        // BOTH directories, once, and before the manifest.
        //
        // The unlinks always needed the trash directory synced and always got it here -- one
        // fsync for the round, which is the shape the quarantine side has now been moved to. What
        // is new is the store ROOT: a restore renames a slab out of quarantine and back into the
        // store, and that rename used to be made durable by two fsyncs inside
        // `restore_slab_from_delayed_destroy`, once per restored slab. Syncing the root here
        // instead commits every one of this round's restores together. It was in fact already
        // reaching disk, as a side effect of `persist_slab_manifest` fsyncing the root to commit
        // its own rename -- which is exactly the kind of accident that survives until someone
        // reorders the two calls. It is stated here instead of relied upon there.
        //
        // AND ONLY WHEN THE ROUND ACTUALLY DID SOMETHING, which is the guard the collector half of
        // this stage already carries (`a_gc_round_that_reclaimed_nothing_does_not_rewrite_the_
        // manifest`) and this half did not. Both run in the same periodic round, from
        // `apply_storage_lifecycle`, and the reasoning recorded there applies unchanged: the
        // manifest write serialises every slab, fsyncs a temp file, renames it and fsyncs the
        // parent directory.
        //
        // `inner.slabs` is mutated in exactly two places in the loop above, both `set_slab_state`
        // -- one in the branch that pushes onto `restored`, one in the branch that pushes onto
        // `purged` -- and the only filesystem changes are the rename inside those restores and the
        // unlink inside those destroys. A `restore_blocked` entry moves nothing
        // (`restore_slab_from_delayed_destroy_unsynced` returns before its rename when the
        // destination is occupied) and a `retained_too_young` entry touches nothing at all. So with
        // both lists empty there is no rename and no unlink for an fsync to commit, and the
        // manifest would be rewritten with byte-identical content.
        //
        // That is the ordinary shape of a purge round, not a corner: a quarantined slab waits
        // DELAYED_DESTROY_MIN_AGE_MS -- an hour -- and every round in that window walks the whole
        // trash directory and acts on none of it. Measured on a sixteen-slab quarantine: three
        // directory fsyncs plus a full manifest rewrite per round, for no durable change.
        if !purged.is_empty() || !restored.is_empty() {
            sync_delayed_destroy_dirs(&root)?;
            inner.persist_slab_manifest_counted()?;
        }
        retained_too_young.sort_unstable();
        Ok(BlockStorePurgeDelayedDestroyReport {
            purged_block_slab_ids: purged,
            purged_physical_bytes,
            retained_too_young_block_slab_ids: retained_too_young,
            retained_too_young_physical_bytes,
            restored_block_slab_ids: restored,
            restored_physical_bytes,
            restore_blocked_block_slab_ids: restore_blocked,
            processed_block_slabs: processed,
            budget_exhausted,
            max_slabs_per_round,
            examined_block_slabs: examined,
            declined_without_examining: false,
        })
    }

    /// Move every quarantined slab's stamp `age_ms` into the past, so a test can reach the far
    /// side of [`DELAYED_DESTROY_MIN_AGE_MS`] without an hour of wall clock.
    ///
    /// This does NOT shorten the age the purge enforces. The scheduled round still calls
    /// `purge_delayed_destroy_slabs_with_report()` with the shipped one-hour minimum; what moves
    /// is the slab's own arrival time. So a purge observed after this is the real gate firing on
    /// a slab that is genuinely old by its own clock, which is a different and stronger claim
    /// than `purge_delayed_destroy_slabs_older_than(0)` -- that one proves only that a purge with
    /// the delay removed removes things.
    ///
    /// Returns how many descriptors moved, and a caller MUST assert that count before reading a
    /// purge result. A purge reporting zero because nothing was backdated looks exactly like a
    /// purge reporting zero because the mechanism is broken.
    /// The earliest quarantined arrival this store is currently prepared to claim.
    ///
    /// `None` is the state that forces a walk. Read directly rather than through a decline,
    /// because the rule being guarded is about WHAT A ROUND IS ENTITLED TO PUBLISH, and the
    /// consequences of publishing wrongly depend on the order the directory listing happens to
    /// return -- which is not the order of the ids, is not stable across the writes a test makes
    /// between two rounds, and is no basis for a guard. The rule itself has neither problem.
    #[cfg(test)]
    pub(crate) fn delayed_destroy_earliest_for_test(&self) -> Option<u64> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .delayed_destroy_earliest_unix_ms
    }

    #[cfg(test)]
    pub(crate) fn backdate_delayed_destroy_stamps_for_test(
        &self,
        age_ms: u64,
    ) -> Result<usize, BlockStoreError> {
        self.backdate_named_delayed_destroy_stamps_for_test(None, age_ms)
    }

    /// As above, but only for the quarantined slabs `only` names.
    ///
    /// A quarantine whose slabs all arrived at the same moment cannot tell an earliest arrival
    /// from a latest one, and the difference between those two is the difference between a cache
    /// that is stale EARLY -- which costs a wasted walk -- and one that is stale LATE, which costs
    /// a delayed destroy. Giving part of a quarantine a different age is what makes the two
    /// distinguishable without an hour of wall clock or a sleep in a test, and naming the part by
    /// id rather than by range lets a caller pick it out of the order the directory listing
    /// actually returns -- which is the order the round will walk and is not the order of the ids.
    #[cfg(test)]
    pub(crate) fn backdate_named_delayed_destroy_stamps_for_test(
        &self,
        only: Option<&BTreeSet<u64>>,
        age_ms: u64,
    ) -> Result<usize, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let root = inner.root.clone();
        let quarantined = delayed_destroy_slab_ids_at(&root)?;
        let mut moved = 0usize;
        for block_slab_id in quarantined {
            if only.map(|only| !only.contains(&block_slab_id)).unwrap_or(false) {
                continue;
            }
            if let Some(slab) = inner.slabs.get_mut(&block_slab_id) {
                let stamp = slab.updated_unix_ms.unwrap_or_else(now_unix_ms);
                slab.updated_unix_ms = Some(stamp.saturating_sub(age_ms));
                moved += 1;
            }
        }
        // THE ONE THING IN THE CRATE THAT MOVES AN ARRIVAL BACKWARDS, and therefore the one thing
        // that can leave `delayed_destroy_earliest_unix_ms` holding a moment LATER than the truth
        // -- the single direction that cache is not allowed to be wrong in. Every other write of
        // `updated_unix_ms` either stamps `now`, which cannot precede an arrival already counted,
        // or runs while the store is being opened, when the cache is `None` regardless. This is
        // what a clock stepping backwards would look like from the cache's side, and the answer
        // is the same in both cases: withdraw the claim and let the next round walk.
        inner.delayed_destroy_earliest_unix_ms = None;
        inner.persist_slab_manifest_counted()?;
        Ok(moved)
    }

    /// Per-slab size and block count, without decoding any block.
    ///
    /// What the reclaim planner needs out of `slab_reports()`, at the cost of a header walk
    /// instead of a CRC-and-decompress of every record in the store.
    pub fn slab_block_counts(&self) -> Result<Vec<(u64, u64, u64)>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut tally = BlockStoreReadTally::start();
        let mut out = Vec::new();
        for block_slab_id in slab_ids_at(&root)? {
            let bytes = LocalSlabBackend::new(&root)
                .read_all(block_slab_id, &mut tally)?
                .into_bytes_charged_to_the_process_only();
            let (block_count, physical_bytes) =
                crate::block_store::record::count_slab_blocks(&bytes, block_slab_id);
            out.push((block_slab_id, physical_bytes, block_count));
        }
        Ok(out)
    }

    pub fn slab_reports(&self) -> Result<Vec<BlockStoreSlabReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut tally = BlockStoreReadTally::start();
        let mut reports = Vec::new();
        for block_slab_id in slab_ids_at(&root)? {
            let bytes = LocalSlabBackend::new(&root)
                .read_all(block_slab_id, &mut tally)?
                .into_bytes_charged_to_the_process_only();
            reports.push(inspect_slab(&bytes, block_slab_id));
        }
        Ok(reports)
    }

    pub fn stats(&self) -> BlockStoreStats {
        self.inner.lock().expect("block store lock poisoned").stats
    }

    /// Open a new write-amplification window here, because the live set has just been
    /// consolidated.
    ///
    /// Records the current `bytes_written` as the window's baseline. Nothing is reset: see
    /// [`BlockStoreStats::bytes_written_at_last_compaction`] for why a baseline rather than a
    /// counter reset.
    ///
    /// THE BASELINE IS `bytes_written` AS IT STOOD BEFORE THE RELOCATION, NOT AFTER IT, so the
    /// compaction's OWN rewrite is the first thing in the new window rather than the last thing in
    /// the old one. That is the difference between a store reporting 0.000x immediately after a
    /// compaction -- true, but saying only "nothing has been written since" -- and reporting
    /// roughly 1.000x, which is the useful statement that what is on disk is about one copy of
    /// what is live. Measured on a 400-key fixture: the relocation wrote 414,400 B against 409,600
    /// B live, so the window opens at 1.012x.
    ///
    /// Called from the one inner funnel every compaction entry point routes through, so a caller
    /// cannot compact without opening a window.
    pub(crate) fn mark_write_window_at_compaction(&self, baseline: u64) {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        inner.stats.bytes_written_at_last_compaction = baseline;
    }
}

fn should_roll_before_append(
    write_offset: u64,
    record_len: u64,
    slab_target_bytes: u64,
) -> bool {
    write_offset > 0 && write_offset.saturating_add(record_len) > slab_target_bytes
}

/// Whether the active slab has reached its target and should be rolled ahead of the next
/// append.
///
/// Deliberately NOT the same predicate as `should_roll_before_append`: that one asks "will
/// THIS record overflow the slab", which needs the record length and is only answerable on
/// the write path. This one asks "is the slab full enough to roll now", which is answerable
/// in the background with no pending record. The `write_offset > 0` guard stops a freshly
/// rolled, still-empty slab from rolling again immediately — without it a background cycle
/// would mint an empty slab every pass.
fn slab_is_at_target(write_offset: u64, slab_target_bytes: u64) -> bool {
    write_offset > 0 && write_offset >= slab_target_bytes
}

/// Bulk backfill (MATRIXARK_BULK_INGEST) defers per-append fsync + manifest
/// persistence to an explicit sync_durable(), trading crash-durability *within a
/// resumable/WAL-backed chunk* for far fewer fsyncs. The live path (env unset)
/// keeps full per-append durability.
pub(crate) fn bulk_relaxed_durability() -> bool {
    crate::engine::bulk_ingest_mode()
}

/// On the live path, defer the per-record extent-manifest persist to sync_durable()/slab-seal
/// (the manifest is reconciled from disk on open). Single-barrier default; restored to a
/// synchronous persist only under the TS_WAL_LEGACY_RECOVERY escape hatch. Moves in lockstep
/// with `block_wal_single_barrier` (the intermediate "manifest-only" relaxation is gone).
pub(crate) fn block_wal_only_sync() -> bool {
    block_wal_single_barrier()
}

/// The single-barrier default also defers the per-write data-block fdatasync -- the last non-WAL
/// synchronous barrier. This is safe ONLY because the default also switches recovery to base-only
/// replay: reload trusts only the durable dump checkpoint (whose `flush_shard_index` fsyncs every
/// block BEFORE advancing the watermark) and re-derives every post-watermark block by replaying the
/// WAL tail exactly once. A block that was written but never fsync'd is therefore rebuilt from its
/// WAL command, never left as a dangling reference. The deferred block still becomes durable at the
/// next dump (`sync_durable` fsyncs the active slab; a rolled slab is fsync'd at roll). Restored to
/// a synchronous per-write data-block fdatasync (with delta-fold recovery) only under the
/// TS_WAL_LEGACY_RECOVERY escape hatch.
pub(crate) fn block_wal_single_barrier() -> bool {
    // One reader for the hatch, in `engine`. This parsed it itself, as did index_log, and the
    // three barriers they gate have to move together -- a copy that drifted would leave one of
    // them on the legacy path and the others on the default.
    !crate::engine::wal_legacy_recovery()
}

/// Slabs are neither preallocated nor recycled, and both are deliberate.
///
/// The log does preallocate, and it measured **2.16x** cheaper per append for doing so
/// (1131 us against 2444, ranges [1042-1285] and [2442-2565], six interleaved runs). So the same
/// treatment here looks obviously worth having. It is not, and the reason is the barrier rate
/// rather than anything about files.
///
/// That win comes entirely from not persisting a new file size on every barrier. Measured across
/// group sizes, it is 65.5% at one barrier per record, 42.7% at eight, and **gone by sixty-four**:
/// with no barrier, growing a file is page-cache work and costs nothing worth reclaiming.
///
/// This path has no barrier per write. `defer_data_sync` is
/// `bulk_relaxed_durability() || block_wal_single_barrier()`, and the second is true unless legacy
/// recovery is turned back on -- so by default the per-write block fdatasync is already deferred
/// (see the note on `block_wal_only_sync`). Preallocating would remove a cost that is not being
/// paid.
///
/// Recycling a slab rather than creating and unlinking one is the same story from the other end.
/// What it saves is the create and the unlink -- and slabs are large, so that turnover is rare
/// against the writes going through them. Reusing already-allocated blocks is the other half of the
/// preallocation argument, and it lapses for the same reason.
///
/// What WOULD change the answer: making block writes synchronous again (turning
/// `TS_WAL_LEGACY_RECOVERY` on, or anything else that stops deferring that fdatasync). Then this
/// path starts paying per barrier for a file that grows per write, and both are worth revisiting
/// together -- with the group-size table above as the guide to how much is there.
impl BlockStoreInner {
    /// Write the slab manifest out and count it against THIS store's stats.
    ///
    /// Every route that persists from a locked store goes through here, so the count cannot drift
    /// from the writes the way a per-call-site increment did.
    pub(crate) fn persist_slab_manifest_counted(&mut self) -> Result<(), BlockStoreError> {
        persist_slab_manifest(&self.root, &self.slabs, &mut self.stats)
    }
}

fn roll_slab_inner(
    inner: &mut BlockStoreInner,
) -> Result<BlockStoreRollReport, BlockStoreError> {
    std::fs::create_dir_all(&inner.root)?;
    let previous_block_slab_id = inner.block_slab_id;
    // The outgoing slab may hold relaxed (un-fsynced) bulk appends; make them
    // durable before we seal and stop writing to it.
    {
        let prev_path = slab_path(&inner.root, previous_block_slab_id);
        if let Ok(prev) = OpenOptions::new().append(true).open(&prev_path) {
            crate::durability_metrics::record_barrier("block_store_slab_roll_prev");
            let _ = prev.sync_data();
        }
    }
    let next_from_current = inner.block_slab_id.saturating_add(1);
    let next_from_disk = slab_ids_at(&inner.root)?
        .into_iter()
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or_default();
    inner.block_slab_id = next_from_current.max(next_from_disk);
    inner.write_offset = 0;
    let path = slab_path(&inner.root, inner.block_slab_id);
    let file = std::fs::File::create(&path)?;
    crate::durability_metrics::record_barrier("block_store_slab_roll");
    file.sync_all()?;
    sync_parent_dir(&path)?;
    let transition_unix_ms = now_unix_ms();
    if let Some(previous) = inner.slabs.get_mut(&previous_block_slab_id) {
        previous.state = BlockStoreSlabState::Sealed;
        previous.updated_unix_ms = Some(transition_unix_ms);
    }
    let new_slab = BlockStoreSlabDescriptor {
        stored_slab_id: inner.block_slab_id,
        block_slab_id: inner.block_slab_id,
        state: BlockStoreSlabState::Active,
        physical_bytes: 0,
        logical_bytes: 0,
        created_unix_ms: Some(transition_unix_ms),
        updated_unix_ms: Some(transition_unix_ms),
        first_block_id: None,
        last_block_id: None,
        readable_prefix_physical_bytes: 0,
        verified_source_mtime_unix_ms: None,
        has_corruption: false,
        first_error_offset: None,
        first_error: None,
    };
    let block_slab_id = inner.block_slab_id;
    inner.slabs.insert(block_slab_id, new_slab);
    inner.persist_slab_manifest_counted()?;
    Ok(BlockStoreRollReport {
        previous_block_slab_id,
        new_block_slab_id: inner.block_slab_id,
    })
}

fn slab_lifecycle_states(summary: &BlockStoreSlabSummary) -> Vec<String> {
    let mut states = Vec::new();
    if summary.active_slabs > 0 {
        states.push("active".to_string());
    }
    if summary.sealed_slabs > 0 {
        states.push("sealed".to_string());
    }
    if summary.delayed_destroy_slabs > 0 {
        states.push("delayed_destroy".to_string());
    }
    if summary.purged_slabs > 0 {
        states.push("purged".to_string());
    }
    states
}

fn compute_slab_usage(
    slabs: &BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Vec<BlockStoreSlabUsage> {
    #[derive(Debug, Clone)]
    struct SlabUsageAcc {
        usage: BlockStoreSlabUsage,
    }

    fn merged_slab_state(
        left: BlockStoreSlabState,
        right: BlockStoreSlabState,
    ) -> BlockStoreSlabState {
        use BlockStoreSlabState::*;
        match (left, right) {
            (Active, _) | (_, Active) => Active,
            (Sealed, _) | (_, Sealed) => Sealed,
            (DelayedDestroy, _) | (_, DelayedDestroy) => DelayedDestroy,
            (Purged, Purged) => Purged,
        }
    }

    let mut usage_by_slab = BTreeMap::<u64, SlabUsageAcc>::new();
    for slab in slabs.values() {
        let (live, reclaimable, purged) = match slab.state {
            BlockStoreSlabState::Active | BlockStoreSlabState::Sealed => {
                (slab.physical_bytes, 0, 0)
            }
            BlockStoreSlabState::DelayedDestroy => (0, slab.physical_bytes, 0),
            BlockStoreSlabState::Purged => (0, 0, slab.physical_bytes),
        };
        let entry = usage_by_slab
            .entry(slab.stored_slab_id)
            .or_insert_with(|| SlabUsageAcc {
                usage: BlockStoreSlabUsage {
                    stored_slab_id: slab.stored_slab_id,
                    block_slab_id: slab.block_slab_id,
                    storage_slab_id: slab.stored_slab_id,
                    stream_slab_id: slab.block_slab_id,
                    state: slab.state,
                    used_bytes: 0,
                    live_bytes: 0,
                    reclaimable_bytes: 0,
                    purged_bytes: 0,
                    block_store_used_bytes: 0,
                    live_block_store_used_bytes: 0,
                    reclaimable_block_store_used_bytes: 0,
                    purged_block_store_used_bytes: 0,
                    first_block_id: None,
                    last_block_id: None,
                },
            });
        let usage = &mut entry.usage;
        usage.block_slab_id = usage.block_slab_id.min(slab.block_slab_id);
        usage.stream_slab_id = usage.stream_slab_id.min(slab.block_slab_id);
        usage.state = merged_slab_state(usage.state, slab.state);
        usage.used_bytes = usage.used_bytes.saturating_add(slab.physical_bytes);
        usage.live_bytes = usage.live_bytes.saturating_add(live);
        usage.reclaimable_bytes = usage.reclaimable_bytes.saturating_add(reclaimable);
        usage.purged_bytes = usage.purged_bytes.saturating_add(purged);
        usage.block_store_used_bytes = usage
            .block_store_used_bytes
            .saturating_add(slab.physical_bytes);
        usage.live_block_store_used_bytes = usage.live_block_store_used_bytes.saturating_add(live);
        usage.reclaimable_block_store_used_bytes = usage
            .reclaimable_block_store_used_bytes
            .saturating_add(reclaimable);
        usage.purged_block_store_used_bytes =
            usage.purged_block_store_used_bytes.saturating_add(purged);
        usage.first_block_id = match (usage.first_block_id, slab.first_block_id) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, right) => right,
            (left, None) => left,
        };
        usage.last_block_id = match (usage.last_block_id, slab.last_block_id) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, right) => right,
            (left, None) => left,
        };
    }
    usage_by_slab.into_values().map(|acc| acc.usage).collect()
}

impl Default for BlockStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("pages");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("block store lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

#[cfg(test)]
mod address_size_tests {
    use std::fs::{self, File};
    use super::*;

    /// The shape this replaced, declared here so the comparison is measured rather than argued.
    /// Rust has no spare bit pattern in a `u64` to mean "absent", so each `Option` pays a whole
    /// extra word for its tag; five of them is the cost being removed.
    #[allow(dead_code)]
    struct OptionalShape {
        block_slab_id: u64,
        offset: u64,
        length: u64,
        block_id: Option<u64>,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        stored_slab_id: Option<u64>,
        sha256: Option<[u8; 32]>,
    }

    /// THE SUBJECT IS THE HOLDER, because the holder is what replaced this shape.
    ///
    /// `OptionalShape` spells a length, a block id and an object id beside the two address halves,
    /// so the structure it is being compared against is the one that still carries those -- which
    /// is [`ElementEntry`], not the one-word [`BlockAddress`]. Comparing the word against it would
    /// be a true assertion about the wrong pair.
    #[test]
    fn address_is_smaller_than_the_optional_shape() {
        let packed = std::mem::size_of::<ElementEntry>();
        let optional = std::mem::size_of::<OptionalShape>();
        assert!(
            packed < optional,
            "packing should shrink the address: {packed} vs {optional}"
        );
        // Guards the win rather than merely observing it: every block in the index holds one of
        // these for the life of the shard, so a regression here is a per-block regression.
        assert!(packed <= 104, "address grew to {packed} bytes");
    }

    /// Sum the widths and the slack of a laid-out struct, INDEPENDENTLY of the rows it audits.
    ///
    /// Takes the measured `(name, offset, width)` of every field and the struct's own `size_of`,
    /// and answers `(covered, padding)`, where `padding` is `total - covered`.
    ///
    /// THE OBVIOUS CROSS-CHECK IS A TAUTOLOGY, AND IS NOT HERE. It is tempting to also walk the
    /// slack between the sorted fields, add the tail, and assert that it equals `total - covered`
    /// -- "the padding from the two ends must agree with the holes I walked". This module carried
    /// exactly that, and a mutation run killed it: the walked slack is
    /// `sum(holes) + tail`, which telescopes to `total - sum(widths)`, which IS `total - covered`.
    /// The two are the same quantity written twice, so the assertion held for every input,
    /// including one with a field deliberately left off the list. An assertion that cannot fail
    /// reads as a second opinion and is not one.
    ///
    /// WHAT ACTUALLY DETECTS A MISSING FIELD is the alignment invariant. A struct whose fields
    /// are packed and then rounded cannot have `align` or more bytes of padding -- if it did, the
    /// rounding would have landed a step lower. Padding at or above the alignment therefore means
    /// the listed fields do not add up to the payload, whichever end the missing one is at. Both
    /// planted controls below -- one omitting the LAST field, one omitting a MIDDLE field -- are
    /// caught by this and only this.
    fn account(
        fields: &[(&'static str, usize, usize)],
        total: usize,
        align: usize,
    ) -> (usize, usize) {
        let mut sorted = fields.to_vec();
        sorted.sort_by_key(|(_, offset, _)| *offset);
        let mut covered = 0usize;
        let mut cursor = 0usize;
        for (name, offset, width) in &sorted {
            // Overlap is a different defect from omission, and this one IS independent: two
            // fields claiming the same byte cannot be seen in any total.
            assert!(
                *offset >= cursor,
                "field {name} at {offset} overlaps the field before it (ends at {cursor})"
            );
            covered += width;
            cursor = offset + width;
        }
        let residual = total - covered;
        assert!(
            residual < align,
            "{residual} bytes of padding in a {align}-aligned {total}-byte struct: a packed \
             layout rounded up cannot leave a whole alignment step, so a field is missing from \
             the list -- most likely the last one, where the two sums above agree with each other"
        );
        (covered, residual)
    }

    /// EVERY ONE OF THE TWENTY-FOUR BYTES, MEASURED RATHER THAN ASSUMED.
    ///
    /// The layout is `repr(Rust)`, so the compiler is free to reorder: nothing here assumes a
    /// declaration order. Each offset is measured with `offset_of!` and the padding falls out of
    /// the slack between them.
    ///
    /// WIDTH VERSUS ALIGNMENT, WHICH IS THE WHOLE POINT, AND THE PART THAT USED TO BE OVERSTATED.
    /// The payload is 23 bytes inside a 24-byte, 8-aligned struct, so there is exactly 1 byte of
    /// slack. Every field shares one eight-aligned group, which means NO SINGLE-FIELD NARROWING IS
    /// WORTH ANYTHING -- and that is still true at 24, not just at 32: halve any field here and the
    /// payload lands between 17 and 22, all of which round back to 24. The loop at the bottom
    /// asserts exactly that over the MEASURED payload rather than restating it.
    ///
    /// WHAT THAT DOES NOT SAY is that no narrowing can pay. It says no narrowing of ONE field can.
    /// Two of them together shed a whole eight bytes between them and cross the step exactly as
    /// shedding an eight-byte field does. That happened twice here. 40 -> 32 was `block_slab_id`
    /// and `offset`, 8 bytes each, becoming two halves of one word. 32 -> 24 is `routing_bucket`
    /// LEAVING (worth zero alone: 29 - 4 = 25, which rounds back to 32) beside `block_id` going
    /// from 32 bits to 16 (worth zero alone: 29 - 2 = 27, also back to 32). Both counterfactuals
    /// are asserted below, because the record of what each was worth in isolation is the whole
    /// reason the pair was available to take.
    ///
    /// PROVEN RANGES, not assumed ones. `length` is 32 bits and `block_id` is 16 because the
    /// encoder REFUSES anything wider -- `encode_block_record` errors above
    /// `BLOCK_RECORD_LENGTH_MASK` and above `u16::MAX` respectively. `length` SATURATES, because
    /// `u32::MAX` is four times the largest length that can reach it and so reads as broken;
    /// `block_id` is CHECKED, because `u16::MAX` IS the largest block id that can reach it and a
    /// saturated one would name a real block. `object_id` is a full FNV-1a hash and uses its range:
    /// measured at 18,403,644,112,878,577,117 on a seeded shard, which needs all 64 bits -- and it
    /// is the only field between this structure and 16 bytes. `address` is two 32-bit halves whose
    /// bounds are the capped slab target and the reserved sentinel ids -- see
    /// [`make_block_address_word`].
    ///
    /// `routing_bucket` used to be listed here at 4 bytes, measured at 4,294,692,422 -- all 32
    /// bits, under the whole-keyspace routing range. It is not a field any more: a block's bucket is
    /// `block_routing_bucket(object_key, ..)` over the range the store is stamped with, so the
    /// container a block is read through answers it. See the note on [`BlockAddress`].
    #[test]
    fn every_byte_of_an_address_and_of_the_holder_that_carries_it_is_accounted_for() {
        use std::mem::{align_of, offset_of, size_of};

        // THE WORD FIRST, and it is the whole structure: one field, no slack to account for.
        let word_total = size_of::<BlockAddress>();
        let word_fields: Vec<(&'static str, usize, usize)> =
            vec![("address", offset_of!(BlockAddress, address), size_of::<u64>())];
        let (word_covered, word_padding) =
            account(&word_fields, word_total, align_of::<BlockAddress>());
        println!("--- BlockAddress, {word_total} bytes, align {} ---", align_of::<BlockAddress>());
        println!("  field bytes {word_covered}, padding {word_padding}");
        assert_eq!(word_covered, 8, "one 64-bit word: the slab in the high half, the offset in the low");
        assert_eq!(
            word_padding, 0,
            "a single eight-byte field in an eight-aligned struct leaves no slack to reclaim,              which is why there is no narrowing left to try here"
        );
        assert_eq!(word_total, 8);

        // AND THEN THE HOLDER, which is where the other three fields went. The accounting below is
        // the one that used to be spelled over the address, because these are the same four fields
        // -- they are simply in two structures now instead of one.
        let total = size_of::<ElementEntry>();
        let fields: Vec<(&'static str, usize, usize)> = vec![
            ("address", offset_of!(ElementEntry, address), size_of::<BlockAddress>()),
            ("length", offset_of!(ElementEntry, length), size_of::<u32>()),
            ("block_id", offset_of!(ElementEntry, block_id), size_of::<u16>()),
            ("present", offset_of!(ElementEntry, present), size_of::<u8>()),
        ];

        let (covered, padding) = account(&fields, total, align_of::<ElementEntry>());

        // THE RELOCATION, ASSERTED RATHER THAN ARGUED, and this is the assertion the whole change
        // turns on. The word shed seven bytes of sibling; the holder took exactly those seven and
        // rounded to the same sixteen the word used to occupy while it held them. Nothing was
        // saved -- the bytes moved.
        assert_eq!(
            covered - word_total,
            7,
            "the siblings are the length (4), the block id (2) and the presence byte (1)"
        );
        assert_eq!(
            (word_total + 7).div_ceil(8) * 8,
            total,
            "the word as an eight-aligned group plus those seven as the tail IS the holder width"
        );
        assert_eq!(
            total, 16,
            "and sixteen is what the address cost on its own before the three moved out, so this              is a relocation and not a saving -- see              `engine::tests::address_footprint::the_priced_shapes_for_an_address_and_its_siblings`              for the same zero measured at every holder in the engine"
        );

        let mut sorted = fields.clone();
        sorted.sort_by_key(|(_, offset, _)| *offset);
        let mut cursor = 0usize;
        println!("--- ElementEntry, {total} bytes, align {} ---", align_of::<ElementEntry>());
        for (name, offset, width) in &sorted {
            let before = offset - cursor;
            println!("  +{offset:>2}  {name:<14} width {width}  padding before {before}");
            cursor = offset + width;
        }
        println!("  tail padding {}", total - cursor);
        println!("  field bytes {covered}, padding {padding}, total {total}");

        assert_eq!(covered, 15, "the payload is 15 bytes of field");
        assert_eq!(padding, 1, "and one of alignment");
        assert_eq!(covered + padding, total);
        assert_eq!(total, 16);
        assert_eq!(align_of::<ElementEntry>(), 8);

        // The reconstruction, not the total: the eight-aligned group plus the rounded tail.
        let eight_aligned: usize = 1 * 8;
        let tail: usize = 4 + 2 + 1;
        assert_eq!(eight_aligned + tail.div_ceil(8) * 8, total);

        // AND THIS STEP DID PAY ON ITS OWN, which is the one thing the per-field rule above never
        // said. `object_id` was a WHOLE eight-byte field in the eight-aligned group, so the group
        // went 16 -> 8 while the tail stayed 7 and rounded to 8: the payload 23 -> 15 and the width
        // 24 -> 16. Stated over the measured payload so it cannot drift.
        assert_eq!(
            covered + 8,
            23,
            "the payload was 23 bytes before the object id left; it is {covered} now, so the step \
             this change claims is not the step it took"
        );
        assert_eq!(
            (covered + 8).div_ceil(8) * 8,
            24,
            "and 23 bytes of payload rounded to 24, which is the width this shed from"
        );

        // EACH HALF OF THE PAIR, ON ITS OWN, IS WORTH ZERO -- asserted, because that is the claim
        // this entry exists to correct and a reader who only sees 24 cannot tell which half paid.
        assert_eq!(
            (29_usize - 4).div_ceil(8) * 8,
            32,
            "shedding the 4-byte routing bucket alone leaves a payload of 25, which rounds back to \
             32: worth nothing on its own"
        );
        assert_eq!(
            (29_usize - 2).div_ceil(8) * 8,
            32,
            "narrowing the block id from 32 bits to 16 alone leaves a payload of 27, which rounds \
             back to 32: worth nothing on its own"
        );

        // AND WHAT PUTTING AN OBJECT ID BACK WOULD COST, which is why 16 is the floor for as long
        // as an id of any width would have to live here. The payload is 15, so an id at ONE byte
        // would still fit inside 16 and at TWO would push to 24 -- the same arithmetic that made a
        // per-bucket ordinal worth nothing unless it fitted in 255 objects. Kept as the record of
        // what this width is the floor OF, now measured from the shed payload rather than towards
        // it.
        assert_eq!(covered, 15, "8 address + 4 length + 2 block id + 1 present");
        assert_eq!(
            (covered + 1).div_ceil(8) * 8,
            16,
            "an 8-bit object id would still fit inside 16"
        );
        assert_eq!(
            (covered + 2).div_ceil(8) * 8,
            24,
            "a 16-bit object id would not: 17 rounds up to 24"
        );

        // A NARROWING THAT DOES NOT CROSS THE STEP IS WORTH ZERO. Stated as arithmetic over the
        // measured payload rather than as prose, so it cannot quietly stop being true.
        for (name, _, width) in &fields {
            if *width < 2 {
                continue;
            }
            let halved = covered - width / 2;
            assert_eq!(
                halved.div_ceil(8) * 8,
                total,
                "halving {name} ({width} -> {}) changed the rounded width, which the accounting \
                 above says is impossible",
                width / 2
            );
        }
        // The loop above is only worth having if it actually ran over every field: a `fields` list
        // that lost its wide entries would make it vacuous, and this change removed one.
        assert_eq!(
            fields.iter().filter(|(_, _, width)| *width >= 2).count(),
            3,
            "denominator: address, length and block_id are the three fields the one-field rule is \
             asserted over -- it was four while `object_id` was one of them"
        );
    }

    /// The residual instrument must FAIL on a shape it should fail on.
    ///
    /// `account` reports padding from the two ends and cross-checks it against the slack it walked.
    /// Plant a field and leave it off the list: the two must disagree. Without this the residual
    /// could be the sum of its own rows and would report zero for anything.
    #[test]
    #[should_panic(expected = "a field is missing from the list")]
    fn the_accounting_notices_a_field_left_off_the_list() {
        use std::mem::{offset_of, size_of};

        #[repr(C)]
        struct Planted {
            a: u64,
            b: u64,
            planted: u64,
        }

        // `planted` is deliberately omitted, which is the defect this control plants. It is the
        // LAST field, which is the case the residual-against-walked-slack check alone cannot see
        // -- so this control is also the one that proves the alignment check earns its place.
        let fields: Vec<(&'static str, usize, usize)> = vec![
            ("a", offset_of!(Planted, a), size_of::<u64>()),
            ("b", offset_of!(Planted, b), size_of::<u64>()),
        ];
        account(&fields, size_of::<Planted>(), std::mem::align_of::<Planted>());
    }

    /// And the MIDDLE case, which the other check is the one to catch.
    ///
    /// Two controls rather than one, because the two assertions inside `account` fire on
    /// different defects and a single plant would leave one of them unproven.
    #[test]
    #[should_panic(expected = "a field is missing from the list")]
    fn the_accounting_notices_a_field_left_out_of_the_middle() {
        use std::mem::{offset_of, size_of};

        #[repr(C)]
        struct Planted {
            a: u64,
            planted: u64,
            z: u64,
        }

        let fields: Vec<(&'static str, usize, usize)> = vec![
            ("a", offset_of!(Planted, a), size_of::<u64>()),
            ("z", offset_of!(Planted, z), size_of::<u64>()),
        ];
        account(&fields, size_of::<Planted>(), std::mem::align_of::<Planted>());
    }

    /// And it must PASS on the same shape once the planted field is listed -- otherwise the
    /// control above would pass for any reason at all.
    #[test]
    fn the_accounting_balances_once_the_planted_field_is_listed() {
        use std::mem::{offset_of, size_of};

        #[repr(C)]
        struct Planted {
            a: u64,
            b: u64,
            planted: u64,
        }

        let fields: Vec<(&'static str, usize, usize)> = vec![
            ("a", offset_of!(Planted, a), size_of::<u64>()),
            ("b", offset_of!(Planted, b), size_of::<u64>()),
            ("planted", offset_of!(Planted, planted), size_of::<u64>()),
        ];
        let (covered, padding) =
            account(&fields, size_of::<Planted>(), std::mem::align_of::<Planted>());
        assert_eq!(covered, 24);
        assert_eq!(padding, 0);
    }

    /// A presence bit is not the same as a zero value. `0` is a legitimate OBJECT ID -- an
    /// FNV-1a hash can land on it -- so "zero means absent" would erase real values, and this is
    /// why the byte exists instead of a sentinel.
    ///
    /// THE EXAMPLE USED TO BE THE ROUTING BUCKET, whose zero is the first bucket of the whole
    /// keyspace. It is not a field any more, so the pair below is spelled over `object_id`: the
    /// claim was never about which field, it was about a zero being a value.
    ///
    /// The grouping id was a second example here and is no longer one: it is the slab's own id, so
    /// it is always known, never absent, and needs no bit.
    ///
    /// `generation` was a THIRD, and is no longer one either -- for a different reason. It is not
    /// that its zero is always absent, but that it no longer has a presence of its own to
    /// distinguish: it is `block_id.or(object_id)`, so a zero generation is present exactly when
    /// a zero `block_id` is, and that is what the second pair below states.
    #[test]
    fn zero_is_distinguishable_from_absent() {
        // THE OBJECT ID WAS THE OTHER HALF OF THIS AND IS GONE, so the pair is stated over the
        // identity that is still a field. A zero block id is a legitimate block id -- it is the
        // FIRST block of an object -- which is exactly why presence is a bit rather than a zero.
        let zero = ElementEntry::from_parts(1, 0, 0, Some(0), None);
        let absent = ElementEntry::from_parts(1, 0, 0, None, None);
        assert_eq!(zero.block_id(), Some(0));
        assert_eq!(absent.block_id(), None);
        assert_ne!(zero, absent);
        assert_eq!(zero.slab_id(), Some(1), "the slab id is derived, present either way");
        assert_eq!(absent.slab_id(), Some(1));

        // A zero generation is still distinguishable from an absent one -- through the block id it
        // is now derived from, which carries the presence bit that used to sit beside it.
        let zero_generation = ElementEntry::from_parts(1, 0, 0, Some(0), None);
        assert_eq!(zero_generation.generation(), Some(0));
        assert_eq!(absent.generation(), None);
        assert_ne!(zero_generation.generation(), absent.generation());
    }

    /// Clearing a value must clear its bit, or the next read reports a stale one as present.
    #[test]
    fn setters_track_presence_both_ways() {
        let mut address =
            ElementEntry::from_parts(1, 0, 0, Some(7), None);
        assert_eq!(address.block_id(), Some(7));
        address.set_block_id(None);
        assert_eq!(address.block_id(), None);
        address.set_block_id(Some(9));
        assert_eq!(address.block_id(), Some(9));
        // The second setter is `set_length`, since `set_object_id` went with the field. The claim
        // being tested is that one setter does not disturb another's presence bit, and it needs two
        // setters to mean anything.
        address.set_length(3);
        assert_eq!(address.length(), 3);
        assert_eq!(address.block_id(), Some(9), "one setter disturbed another");
    }

    /// The packing is an in-memory concern, and the field names are a wire concern: an address
    /// writes the short names and still reads the long ones an older index was written with.
    ///
    /// Both halves matter and they pull in opposite directions. Writing the long names again would
    /// undo the saving that shortening them bought -- fifteen characters to label an integer, once
    /// per index item forever. Failing to read them would make every index already on disk
    /// unresolvable, and the blocks it points at unreachable.
    #[test]
    fn wire_form_survives_the_packing() {
        let address = ElementEntry::from_parts(
            5,
            64,
            128,
            Some(1),
            Some(2),
        );
        let json = serde_json::to_value(&address).unwrap();
        // What is written now: the short names, and nothing else.
        // ONE WORD, TWO NUMBERS: slab 5 in the high half, offset 64 in the low half.
        assert_eq!(
            json["a"],
            make_block_address_word(5, 64),
            "the packed address word"
        );
        assert!(
            json.get("ps").is_none() && json.get("o").is_none(),
            "the split slab id and offset must not be written any more"
        );
        assert_eq!(json["l"], 128, "the length");
        // THE SLOT IS GONE, NOT WRITTEN EMPTY -- and this is asserted through `get` rather than
        // through indexing, because `json["rs"]` answers `Value::Null` for a key that is ABSENT
        // just as loudly as for one written null. The old form of this assertion was
        // `assert_eq!(json["rs"], Value::Null)`, which keeps passing after the slot is retired and
        // passes for the opposite reason. `get` can tell the two apart; indexing cannot.
        assert!(
            json.get("rs").is_none(),
            "the routing-bucket slot is retired: it must be absent, not written null"
        );
        assert!(
            json.get("h").is_none(),
            "the digest slot is retired: it must be absent, not written null"
        );
        assert!(
            json.get("b").is_none(),
            "the slab id is derived rather than written"
        );
        for long in ["page_segment_id", "offset", "routing_slot", "band_id", "object_id", "generation"] {
            assert!(
                json.get(long).is_none(),
                "{long} is a read alias now, not something to write"
            );
        }
        assert!(json.get("present").is_none(), "presence bits must not reach the wire");

        // What is still read: the same address spelled the old way, which is what an index already
        // on disk looks like.
        let round_tripped: ElementEntry = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, address, "an address written now must read back identical");
        let long_form = serde_json::json!({
            // The long names that are RENAMES still read; the split address does not,
            // and there is no long spelling of the merged word -- it is new.
            "a": (5_u64 << 32) | 64_u64,
            "length": 128_u64,
            "page_id": 1_u64,
            "object_id": 2_u64,
            // `routing_slot` names a field that no longer exists, and it is kept in this fixture
            // deliberately: an index already on disk carries it, and this line is the assertion
            // that a key no field claims is TOLERATED rather than refused. It stopped being a
            // read alias when the slot was retired; it did not stop being present on disk.
            "routing_slot": 3_u32,
            // The generation an index on disk carries: `page_id.or(object_id)`, which is what
            // every write path in this engine has always stored here. It was spelled `4` in this
            // fixture -- a value no writer produces -- until the field became derived.
            "generation": 1_u64,
            "band_id": 0_u64,
        });
        let from_long: ElementEntry = serde_json::from_value(long_form).unwrap();
        assert_eq!(
            from_long, address,
            "an index written with the long names must still load as the same address"
        );
    }

    /// THE STORED BYTES DO NOT MOVE. An address written now is written with `g`, exactly as
    /// before, and reads back as the same address.
    ///
    /// This is the round-trip control for the derivation: `generation` stopped being a field, but
    /// it did NOT stop being a key on the wire, so an index this binary writes is loadable by one
    /// that still has the field and vice versa -- as long as the value agrees, which is the whole
    /// content of the guard below.
    #[test]
    fn the_stored_shape_does_not_move_when_the_field_becomes_derived() {
        let address = ElementEntry::from_parts(5, 64, 128, Some(1), Some(2));
        let json = serde_json::to_value(&address).unwrap();
        assert_eq!(json["g"], 1, "the generation is still WRITTEN, and still under its own key");
        assert_eq!(json["pi"], 1, "and it is still the block id it is derived from");

        let bytes = serde_json::to_vec(&address).unwrap();
        let back: ElementEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, address);
        let rewritten = serde_json::to_vec(&back).unwrap();
        assert_eq!(
            bytes, rewritten,
            "loading and rewriting must produce the SAME bytes, not merely an equal address"
        );

        // And the derived value is the one the persisted composite key is built from, which is
        // what makes the agreement load-bearing rather than cosmetic.
        assert_eq!(back.generation(), back.block_id());
    }

    /// A STORED GENERATION THAT DISAGREES IS REFUSED, AND REFUSED BEFORE THE DECODE COMPLETES.
    ///
    /// A stored VALUE that disagrees is refused. A stored ABSENCE is not a disagreement: it is an
    /// index written before the field existed, it has always answered `None`, and it still does.
    /// Both are driven here, because the difference between them is the whole design.
    #[test]
    fn an_index_whose_generation_disagrees_is_refused_loudly() {
        let wrong_value = serde_json::json!({
            "a": 21474836544_u64, "l": 128_u64,
            "pi": 4_u64, "oi": 2_u64, "rs": 3_u32, "g": 7_u64,
        });
        let err = serde_json::from_value::<ElementEntry>(wrong_value).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("disagrees with block_id.or(object_id)"),
            "the refusal must say what disagreed, got: {text}"
        );
        assert!(text.contains("recompute every page handle"), "and why it matters, got: {text}");

        // THE ABSENT CASE, which must LOAD rather than be refused. This is the shape of an index
        // written before the generation existed: an identity, and no generation key at all. It
        // has always answered `None`, and if deriving handed it `Some(4)` instead, every key it
        // resolves through would move -- which is the same data loss the refusal above prevents,
        // arriving from the opposite direction.
        let absent_generation = serde_json::json!({
            "a": 21474836544_u64, "l": 128_u64,
            "pi": 4_u64, "oi": 2_u64, "rs": 3_u32,
        });
        let loaded: ElementEntry = serde_json::from_value(absent_generation)
            .expect("an index written before the generation existed must still load");
        assert_eq!(
            loaded.generation(),
            None,
            "an index that stored no generation must not acquire one on load"
        );
        assert_eq!(loaded.block_id(), Some(4), "and it keeps the identity it did store");

        // THE NEGATIVE CONTROL: the guard must accept the shape it is not guarding against, or it
        // would be refusing every index rather than the disagreeing ones.
        let agreeing = serde_json::json!({
            "a": 21474836544_u64, "l": 128_u64,
            "pi": 4_u64, "oi": 2_u64, "rs": 3_u32, "g": 4_u64,
        });
        let loaded: ElementEntry = serde_json::from_value(agreeing).unwrap();
        assert_eq!(loaded.generation(), Some(4));

        // And an address with NEITHER identity carries no generation, which is agreement, not
        // disagreement -- the band the guard must exclude.
        let neither = serde_json::json!({ "a": 21474836544_u64, "l": 128_u64 });
        let loaded: ElementEntry = serde_json::from_value(neither).unwrap();
        assert_eq!(loaded.generation(), None);
    }
}

#[cfg(test)]
#[path = "block_store/large_store_scale.rs"]
mod large_store_scale;

#[cfg(test)]
#[path = "block_store/open_read_scale.rs"]
mod open_read_scale;

#[cfg(test)]
mod tests {
    use std::fs::{self, File};

    /// The on-disk names, and the readers that have to agree with them.
    ///
    /// A rename that edits the BUILDERS and leaves the READERS behind produces a store the
    /// engine writes and then cannot find, and nothing goes red: a directory scan that
    /// matches nothing returns an empty list, not an error, so the store simply looks new.
    /// That is what PR #1333 did to two tests for six days. The builders are pinned to
    /// literal spellings here, and the quarantine parser is round-tripped against the
    /// builder, so moving one without the other fails.
    ///
    /// `page_zone_manifest.json` is deliberately NOT renamed and is asserted to keep its
    /// old spelling. It is how the tree notices a store written before the slab manifest
    /// existed; pointed at a new name it would look for a file that has never existed, and
    /// the caller falls through to a rebuild and succeeds, so the miss would be silent.
    #[test]
    fn the_on_disk_names_are_the_ones_the_readers_parse() {
        use crate::block_store::paths::{
            delayed_destroy_dir, delayed_destroy_path, legacy_zone_manifest_path,
            slab_manifest_path, slab_path,
        };
        use crate::block_store::slab_ids::{delayed_destroy_slab_id_from_name, slab_ids_at};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        assert_eq!(
            slab_path(root, 7).file_name().unwrap(),
            "block_segment_00000000000000000007.seg"
        );
        assert_eq!(
            slab_manifest_path(root).file_name().unwrap(),
            "block_extent_manifest.json"
        );
        assert_eq!(
            delayed_destroy_dir(root).file_name().unwrap(),
            ".block_segment_trash"
        );
        assert_eq!(
            legacy_zone_manifest_path(root).file_name().unwrap(),
            "page_zone_manifest.json",
            "the legacy detector must keep naming the legacy file, or it detects nothing"
        );

        // The quarantine parser must read what the quarantine builder writes. Move one
        // without the other and this round trip stops resolving.
        let trashed = delayed_destroy_path(root, 3);
        assert_eq!(
            delayed_destroy_slab_id_from_name(trashed.file_name().unwrap()),
            Some(3),
            "the quarantine parser must read the name the builder writes: {trashed:?}"
        );

        // And the directory scan must find a real file written under the real name. A
        // NON-ZERO count, not merely an Ok: an empty scan is what a half-rename looks like.
        std::fs::write(slab_path(root, 0), b"x").unwrap();
        std::fs::write(slab_path(root, 1), b"y").unwrap();
        let found = slab_ids_at(root).unwrap();
        assert_eq!(
            found,
            vec![0, 1],
            "the slab scan must find the slabs the builder just wrote, under {}",
            root.display()
        );
    }

    /// NO TWO FIELDS OF ONE SERDE STRUCT MAY CLAIM THE SAME WIRE NAME.
    ///
    /// A field's wire names are its `rename` -- or its identifier, when it has none -- plus every
    /// `alias`. When two fields of one struct claim the same name the result is not a compile
    /// error and not a decode error:
    ///
    ///   * on DECODE serde resolves the name to the FIRST declaration, so the other field is
    ///     never populated from the wire. Every other value in the document lands, so the symptom
    ///     is one field that is always its `default` -- a zero in a report, or an empty map in an
    ///     index -- with nothing failing anywhere.
    ///   * on ENCODE, if both claim it as the emitted name, the key is written TWICE and which one
    ///     a reader keeps is the reader's business.
    ///
    /// THIS IS NOT HYPOTHETICAL AND IT IS WHY THIS GUARD EXISTS. Widening a reader by adding an
    /// alias is normally free, which is exactly what makes it easy to add one that a sibling field
    /// already emits: `StorageGcSnapshot` carried
    /// `rename = "stale_page_tombstones", alias = "stale_block_tombstones"` on one field while the
    /// field below it emitted `stale_block_tombstones` outright. The twin pair exists on purpose --
    /// one quantity published under both words -- so the alias was aimed at a name that was never
    /// a second spelling of that field at all.
    ///
    /// The denominators are asserted first, because a walk that found nothing would hold this
    /// vacuously.
    #[test]
    fn no_two_fields_of_one_struct_claim_one_wire_name() {
        fn attribute_value<'a>(attributes: &'a str, key: &str) -> Option<&'a str> {
            let after = attributes.split_once(key)?.1;
            after.split_once('"').map(|(value, _)| value)
        }
        fn attribute_values(attributes: &str, key: &str) -> Vec<String> {
            let mut found = Vec::new();
            let mut rest = attributes;
            while let Some((_, after)) = rest.split_once(key) {
                match after.split_once('"') {
                    Some((value, tail)) => {
                        found.push(value.to_string());
                        rest = tail;
                    }
                    None => break,
                }
            }
            found
        }
        fn field_name(line: &str) -> Option<String> {
            let line = line.trim();
            if !line.ends_with(',') {
                return None;
            }
            let (head, _) = line.split_once(':')?;
            let head = head.trim();
            let head = head.rsplit(' ').next().unwrap_or(head);
            if head.is_empty() || !head.starts_with(|c: char| c.is_ascii_lowercase() || c == '_') {
                return None;
            }
            if !head
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                return None;
            }
            Some(head.to_string())
        }

        let crates_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the crate directory has a parent")
            .to_path_buf();
        let mut files = Vec::new();
        let mut pending_dirs = vec![crates_root];
        while let Some(path) = pending_dirs.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    if entry_path.file_name().and_then(|n| n.to_str()) == Some("target") {
                        continue;
                    }
                    pending_dirs.push(entry_path);
                } else if entry_path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    files.push(entry_path);
                }
            }
        }

        let mut structs_walked = 0_usize;
        let mut fields_walked = 0_usize;
        let mut collisions: Vec<String> = Vec::new();

        for file in &files {
            let Ok(text) = std::fs::read_to_string(file) else {
                continue;
            };
            let lines: Vec<&str> = text.lines().collect();
            let mut index = 0_usize;
            while index < lines.len() {
                let header = lines[index].trim();
                let is_struct = header.ends_with('{')
                    && header
                        .split_once("struct ")
                        .is_some_and(|(before, _)| before.is_empty() || before.starts_with("pub"));
                if !is_struct {
                    index += 1;
                    continue;
                }
                let name = header
                    .split_once("struct ")
                    .map(|(_, rest)| rest.trim_end_matches('{').trim())
                    .unwrap_or("?")
                    .to_string();
                let mut is_serde = false;
                let mut look_back = index;
                while look_back > 0 {
                    let previous = lines[look_back - 1].trim();
                    if previous.starts_with("#[") {
                        if previous.contains("Serialize") || previous.contains("Deserialize") {
                            is_serde = true;
                        }
                        look_back -= 1;
                        continue;
                    }
                    if previous.is_empty() || previous.starts_with("//") {
                        look_back -= 1;
                        continue;
                    }
                    break;
                }

                let mut claims: std::collections::BTreeMap<String, Vec<(String, usize, &str)>> =
                    std::collections::BTreeMap::new();
                let mut depth = 1_i32;
                let mut cursor = index + 1;
                let mut attributes = String::new();
                while cursor < lines.len() && depth > 0 {
                    let line = lines[cursor];
                    depth += line.matches('{').count() as i32;
                    depth -= line.matches('}').count() as i32;
                    if depth <= 0 {
                        break;
                    }
                    let trimmed = line.trim();
                    if trimmed.starts_with("#[") {
                        attributes.push_str(trimmed);
                        attributes.push(' ');
                        cursor += 1;
                        continue;
                    }
                    if trimmed.is_empty() || trimmed.starts_with("//") {
                        cursor += 1;
                        continue;
                    }
                    if is_serde && !attributes.contains("skip)") {
                        if let Some(field) = field_name(trimmed) {
                            fields_walked += 1;
                            let emitted = attribute_value(&attributes, "rename = \"")
                                .map(str::to_string)
                                .unwrap_or_else(|| field.clone());
                            claims.entry(emitted).or_default().push((
                                field.clone(),
                                cursor + 1,
                                "emitted",
                            ));
                            for alias in attribute_values(&attributes, "alias = \"") {
                                claims.entry(alias).or_default().push((
                                    field.clone(),
                                    cursor + 1,
                                    "accepted",
                                ));
                            }
                        }
                    }
                    attributes.clear();
                    cursor += 1;
                }
                if is_serde {
                    structs_walked += 1;
                    for (wire, claimants) in &claims {
                        let distinct: std::collections::BTreeSet<&String> =
                            claimants.iter().map(|(field, _, _)| field).collect();
                        if distinct.len() > 1 {
                            let who = claimants
                                .iter()
                                .map(|(field, line, kind)| format!("{field} ({kind}, line {line})"))
                                .collect::<Vec<_>>()
                                .join(" and ");
                            collisions.push(format!(
                                "{}: {name} publishes the wire name {wire:?} from more than one \
                                 field -- {who}",
                                file.display()
                            ));
                        }
                    }
                }
                index = cursor;
            }
        }

        assert!(
            structs_walked > 100,
            "the walk found only {structs_walked} serde-derived struct(s) over {} file(s), so \
             every check below would hold vacuously",
            files.len()
        );
        assert!(
            fields_walked > 1_000,
            "the walk found only {fields_walked} field(s), so every check below would hold \
             vacuously"
        );
        println!(
            "walked {structs_walked} serde structs and {fields_walked} fields over {} files",
            files.len()
        );
        assert!(
            collisions.is_empty(),
            "{} field pair(s) claim one wire name:\n{}\n\nA name claimed twice is resolved to the \
             FIRST declaration on decode, so the other field is never populated from the wire and \
             nothing fails. If the two fields are a twin pair publishing one quantity under both \
             words, neither needs an alias to the other -- they already both reach the wire.",
            collisions.len(),
            collisions.join("\n")
        );
    }

    /// A rename may not quietly drop a durable name.
    ///
    /// The vocabulary migration -- page to block, zone to slab, slot to bucket -- is deliberate and
    /// only half done, and what keeps it safe is that every historical spelling survives as a serde
    /// alias, so a store written by an older build still loads. `BlockAddressWire` states the rule
    /// directly: every short name carries every spelling the field has ever had.
    ///
    /// Nothing checked it. A rename that changed a field AND its alias would compile, pass every
    /// other test, and make existing data unreadable -- with the symptom appearing at load, on
    /// someone else's machine, later.
    ///
    /// The check runs one way. ADDING a durable name is free, which is what keeps this quiet while
    /// the migration proceeds; removing or respelling one fails here with the name in the message.
    /// If a removal is genuinely intended -- a field that no store in the world still carries --
    /// delete it from the list below in the same change, so the decision is written down.
    #[test]
    fn a_durable_name_is_never_quietly_dropped() {
        // Every serde `rename`/`alias` in the crate that carries the old vocabulary, as of the
        // change that added this test.
        const DURABLE_NAMES: &[&str] = &[
        "active_bands",
        "active_storage_zones",
        "active_zones",
        "band_count",
        "band_lifecycle_states",
        "band_manifest_disk_consistent",
        "band_manifest_ready",
        "band_manifest_rebuild_ready",
        "band_manifest_reconciled_on_open",
        "band_manifest_writes",
        "band_state_transition_count",
        "block_segment_id",
        "block_segment_target_bytes",
        "block_store_segment_api_ready",
        "compact_pages",
        "compact_segment_address",
        "compact_segment_id",
        "compact_segment_offset",
        "corrupt_band_count",
        "delayed_destroy_bands",
        "delayed_destroy_page_segment_ids",
        "delayed_destroy_page_slab_ids",
        "delayed_destroy_purged_segments",
        "delayed_destroy_segment_count",
        "delayed_destroy_zones",
        "dirty_slot_count",
        "dirty_slot_pressure",
        "dirty_slots",
        "dumped_slot_count",
        "expired_slot_object_scan_debt",
        "extent_count",
        "extent_id",
        "extent_manifest_ready",
        "extents",
        "fanout_segment_count",
        "first_routing_slot",
        "interrupted_slot_dump_installs",
        "last_routing_slot",
        "live_page_refs",
        "live_page_store_used_bytes",
        "manifest_extra_stream_bands",
        "manifest_missing_stream_bands",
        "max_destroy_segments",
        "min_band_garbage_basis_points",
        "new_page_segment_id",
        "new_page_slab_id",
        "oldest_known_band_age_ms",
        "oldest_known_band_unix_ms",
        "oldest_known_zone_age_ms",
        "oldest_known_zone_unix_ms",
        "oldest_live_band_age_ms",
        "oldest_live_band_unix_ms",
        "oldest_live_zone_age_ms",
        "oldest_live_zone_unix_ms",
        "oldest_reclaimable_band_age_ms",
        "oldest_reclaimable_band_unix_ms",
        "oldest_reclaimable_zone_age_ms",
        "oldest_reclaimable_zone_unix_ms",
        "page",
        "page_address_count",
        "page_addresses",
        "page_chain",
        "page_id",
        "page_id_continuity_ready",
        "page_in_log",
        "page_index_count",
        "page_index_entries",
        "page_ref_key",
        "page_refs",
        "page_segment_id",
        "page_segment_stale_density_basis_points",
        "page_segments_reclaimed",
        "page_size",
        "page_slab_id",
        "page_store",
        "page_store_used_bytes",
        "partial_band_count",
        "partial_band_recovery_ready",
        "previous_page_segment_id",
        "previous_page_slab_id",
        "purged_bands",
        "purged_page_segment_ids",
        "purged_page_slab_ids",
        "purged_page_store_used_bytes",
        "purged_zones",
        "reclaimable_page_store_used_bytes",
        "removed_page_segment_ids",
        "removed_page_slab_ids",
        "retain_from_page_segment_id",
        "retain_from_page_slab_id",
        "retained_current_page_segment_ids",
        "retained_current_page_slab_ids",
        "retained_live_bytes_page_slab_ids",
        "retained_live_page_segment_ids",
        "retained_live_page_slab_ids",
        "retained_page_segment_ids",
        "retained_page_slab_ids",
        "routing_slot",
        "routing_slot_count",
        "sealed_bands",
        "sealed_segment_count",
        "sealed_storage_zones",
        "sealed_zones",
        "segment",
        "segment_count",
        "segment_fields",
        "segment_id",
        "segment_integrity",
        "segment_open_count",
        "segment_samples",
        "segment_sealed_count",
        "segments",
        "selected_page_segment_ids",
        "selected_page_slab_ids",
        "selected_slots",
        "slot",
        "slot_dump_manifest",
        "slot_dump_manifest_id",
        "slot_fields",
        "slot_id",
        "slot_index",
        "slot_index_entry_count",
        "slot_object_ref_count",
        "slot_page_ref_count",
        "slot_samples",
        "slots",
        "staged_pages",
        "stale_page_segment_count",
        "stale_page_segment_pressure",
        "storage_segment_id",
        "storage_zone_count",
        "storage_zone_id",
        "storage_zone_stale_bytes",
        "storage_zone_total_bytes",
        "storage_zone_used_bytes",
        "stream_segment_count",
        "stream_segment_id",
        "unreadable_page_refs",
        "zone_count",
        "zone_id",
        "zone_manifest_ready",
        "zone_stats_ready",
        "zone_usage",
        "zone_version",
        "zones",
        ];

        // Retired by the first milestone: the vocabulary reached the wire and
        // these spellings were removed rather than aliased. The guard proves they
        // stay gone, so coverage does not shrink by deleting them from the list
        // above -- a retired name that quietly comes back fails here.
const RETIRED_NAMES: &[&str] = &[
        "active_page_segment_ids",
        "active_page_slab_ids",
        "actual_routing_slot",
        "block_store_zones",
        "blocked_page_segment_ids",
        "blocked_page_slab_ids",
        "bounded_max_dump_slots_per_round",
        "cache_slot_entry_count",
        "candidate_page_segment_ids",
        "candidate_page_slab_ids",
        "cold_slots_scanned",
        "compacted_page_segment_id",
        "compacted_page_slab_id",
        "configured_page_gc_raft_install_floor_segment_id",
        "corrupt_page_index_wal_snapshot_evidence_ready",
        "corrupt_page_segment_count",
        "corrupt_page_segment_ids",
        "corrupt_page_slab_count",
        "corrupt_page_slab_ids",
        "covered_slot_count",
        "deleted_slot_count",
        "dirty_slots_committed_before_truncate",
        "discovered_page_segment_count",
        "discovered_page_slab_count",
        "durable_slot_generation_frontier_index_log_sequence",
        "durable_slot_generation_frontier_wal_sequence",
        "empty_slots",
        "end_routing_slot",
        "end_slot",
        "expected_routing_slot",
        "first_class_slot_object_page_index_evidence",
        "first_class_slot_object_page_index_ready",
        "hot_slots_scanned",
        "in_memory_slot_count",
        "index_gc_commit_dirty_slots_before_truncation",
        "indexed_page_segment_count",
        "indexed_page_slab_count",
        "installed_slot_dump_install_count",
        "interrupted_slot_dump_install_count",
        "last_compacted_zone",
        "last_selected_slots",
        "legacy_page_zone_aliases_ready",
        "live_page_segment_count",
        "live_page_segment_ids",
        "live_page_slab_count",
        "live_page_slab_ids",
        "live_routing_slot_count",
        "load_cold_slots",
        "load_cold_slots_for_expire",
        "loading_slot_count",
        "manifest_page_segment_ids",
        "manifest_page_slab_ids",
        "manifest_slot_ids",
        "max_cold_slots_per_round",
        "max_dirty_slots",
        "max_dump_slots_per_round",
        "max_expire_cold_slots_per_round",
        "max_expire_hot_slots_per_round",
        "max_hot_slots_per_round",
        "max_orphan_page_segments",
        "max_orphan_page_slabs",
        "max_stale_page_segments",
        "max_stale_page_slabs",
        "metrics_slot_count",
        "missing_dump_slot_ids",
        "missing_page_segment_ids",
        "missing_page_slab_ids",
        "missing_routing_slot_count",
        "missing_slot_generations",
        "multi_object_slots",
        "multi_page_object_slots",
        "native_packed_slot_node_hex",
        "native_packed_slot_node_len",
        "native_packed_slot_node_size",
        "native_slot_store_layout_transition_evidence",
        "native_slot_store_layout_transition_ready",
        "orphan_page_segment_count",
        "orphan_page_segment_ids",
        "orphan_page_slab_count",
        "orphan_page_slab_ids",
        "packed_pages",
        "page_count",
        "page_gc_checkpoint_floor_segment_id",
        "page_gc_raft_install_floor_segment_id",
        "page_segment_ids",
        "page_segment_live_reports",
        "page_segment_manifest_ready",
        "page_segment_reports",
        "page_segments",
        "page_segments_removed",
        "page_segments_removed_physical_bytes",
        "page_segments_retained_live",
        "page_segments_retained_live_physical_bytes",
        "page_segments_retained_physical_bytes",
        "page_slab_count",
        "page_slab_ids",
        "page_slab_live_reports",
        "page_slab_manifest_ready",
        "page_slab_reports",
        "page_slab_stale_density_basis_points",
        "page_slabs",
        "page_slabs_reclaimed",
        "page_slabs_removed",
        "page_slabs_removed_physical_bytes",
        "page_slabs_retained_live",
        "page_slabs_retained_live_physical_bytes",
        "page_slabs_retained_physical_bytes",
        "page_store_bytes_written",
        "page_store_writes",
        "prepared_slot_dump_install_count",
        "prune_slot_dump_manifests",
        "reclaimable_page_segment_ids",
        "reclaimable_page_slab_ids",
        "reclaimable_stale_page_segment_count",
        "reclaimable_stale_page_slab_count",
        "require_slot_dump_manifest",
        "retain_page_segments_from_id",
        "retain_page_slabs_from_id",
        "roll_forward_slot_dump_installs",
        "routing_slots_embedded",
        "secondary_views_reconciled_from_slot_index",
        "selected_dirty_slot_count",
        "selected_dump_slots",
        "selected_routing_slots",
        "single_object_slots",
        "single_page_object_slots",
        "slot_count",
        "slot_dump_manifest_block_count",
        "slot_dump_manifest_count",
        "slot_entries",
        "slot_first",
        "slot_ids",
        "slot_index_authority",
        "slot_layout_states_after",
        "slot_layout_transition_count",
        "slot_map",
        "slot_nodes",
        "slot_object_page_authority_ready",
        "slot_store_layout_api_ready",
        "slot_store_runtime_module",
        "slot_summaries",
        "slot_warmup_ready",
        "source_manifest_slot_ids",
        "source_slot_coverage_missing_slot_ids",
        "stale_page_segment_ids",
        "stale_page_slab_count",
        "stale_page_slab_ids",
        "stale_page_slab_pressure",
        "start_routing_slot",
        "start_slot",
        "total_segment_pages",
        "ttl_slot_count",
        "uncovered_slot_count",
        "unknown_slot_dump_install_count",
        "zone_descriptors",
        "zone_summary",
        ];

        // The list above lives inside the tree this walk reads, so every entry would match its
        // own literal and the check would verify nothing. Excise the declaration from the text
        // before searching: a name must be found because some OTHER site spells it.
        let markers = [
            concat!("const ", "DURABLE_NAMES", ": &[&str] = &["),
            concat!("const ", "RETIRED_NAMES", ": &[&str] = &["),
        ];
        // Both list literals live in the tree this walk reads, so each name would match its
        // own entry and every check would verify nothing. Excise both declarations before
        // searching: a name must be found, or missed, because some OTHER site spells it.
        let strip_list_literal = |text: &str| -> (String, usize) {
            let mut kept = text.to_string();
            let mut removed = 0_usize;
            for marker in markers {
                let Some(open) = kept.find(marker) else {
                    continue;
                };
                let Some(close_rel) = kept[open..].find("];") else {
                    continue;
                };
                let close = open + close_rel + "];".len();
                removed += close - open;
                let mut next = String::with_capacity(kept.len());
                next.push_str(&kept[..open]);
                next.push_str(&kept[close..]);
                kept = next;
            }
            (kept, removed)
        };

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = String::new();
        let mut files_read = 0_usize;
        let mut excised_bytes = 0_usize;
        let mut files_excised = 0_usize;
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    pending.push(entry_path);
                } else if entry_path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    if let Ok(text) = std::fs::read_to_string(&entry_path) {
                        files_read += 1;
                        let (kept, removed) = strip_list_literal(&text);
                        if removed > 0 {
                            files_excised += 1;
                            excised_bytes += removed;
                        }
                        sources.push_str(&kept);
                        sources.push('\n');
                    }
                }
            }
        }
        assert!(
            sources.len() > 100_000,
            "the source walk found almost nothing ({} bytes over {files_read} files); the guard \
             would pass vacuously",
            sources.len()
        );
        // Vacuity, part one: the excision must have happened, exactly once, and must have taken
        // the whole list with it. If the declaration is ever reshaped so the marker stops
        // matching, this fails instead of quietly restoring the self-matching haystack.
        assert_eq!(
            files_excised, 1,
            "expected exactly one file to hold the durable-name list; excised it from \
             {files_excised} of {files_read} files"
        );
        assert!(
            excised_bytes > 1_000,
            "the list excision removed only {excised_bytes} bytes from {files_excised} file(s); \
             the list holds {} names and cannot be that small -- the haystack would still match \
             each name against its own list entry",
            DURABLE_NAMES.len()
        );
        assert!(
            !DURABLE_NAMES.is_empty(),
            "DURABLE_NAMES is empty; there is nothing to check"
        );

        let mut missing = Vec::new();
        let mut found = 0_usize;
        for name in DURABLE_NAMES {
            let quoted = format!("\"{name}\"");
            if sources.contains(&quoted) {
                found += 1;
            } else {
                missing.push(*name);
            }
        }
        // Vacuity, part two: if the haystack ever loses its content, every name goes missing and
        // the assertion below fires -- but a haystack that matched nothing at all would be a
        // broken walk, not 260 real regressions, so say which it is.
        assert!(
            found > 0,
            "not one of the {} durable names was found outside the list itself across \
             {files_read} files ({} bytes searched, {excised_bytes} excised); the walk is broken, \
             not the vocabulary",
            DURABLE_NAMES.len(),
            sources.len()
        );
        assert!(
            missing.is_empty(),
            "durable name(s) no longer written anywhere outside the list itself: {missing:?}\n\
             ({found} of {} names still have a real site, across {files_read} files.)\n\
             A store written by an older build still carries these. If a rename is intended, keep \
             the old spelling as a `serde(alias = ...)` rather than replacing it; if the name is \
             genuinely dead, remove it from DURABLE_NAMES in the same change.",
            DURABLE_NAMES.len()
        );

        // The other direction: a retired spelling must not quietly come back. Without this,
        // deleting a name from DURABLE_NAMES would be a way to make the guard stop looking.
        assert!(
            !RETIRED_NAMES.is_empty(),
            "RETIRED_NAMES is empty; the retirement half of this guard checks nothing"
        );
        let mut resurrected = Vec::new();
        for name in RETIRED_NAMES {
            let quoted = format!("\"{name}\"");
            if sources.contains(&quoted) {
                resurrected.push(*name);
            }
        }
        assert!(
            resurrected.is_empty(),
            "retired name(s) written again outside the list itself: {resurrected:?}\n\
             The first milestone removed these spellings from the wire. Re-adding one means a\n\
             client that was told the name was gone now sees it again; decide that on purpose\n\
             and move the name back into DURABLE_NAMES in the same change."
        );
    }

    /// THE SLAB IDENTITY READER TAKES BOTH SPELLINGS, PROVED AGAINST BYTES AND NOT A ROUND TRIP.
    ///
    /// `BlockStoreSlabDescriptor` is the on-disk slab manifest. Three of its keys are still
    /// page-spelled on the wire while the fields they land in are block-spelled: `page_segment_id`,
    /// `first_page_id` and `last_page_id`. Converging any of them is a STORED-FORMAT change, so the
    /// reader has to land first -- and "reader first" buys nothing if the reader is single-form,
    /// which all three WERE. `serde(rename)` REPLACES the accepted name rather than adding to it,
    /// so a manifest spelling `first_block_id` was dropped by a named map IN SILENCE and the field
    /// defaulted to `None`. Not an error. A writer flipped to the block spelling would have written
    /// a manifest this very binary read as a slab with no first or last block.
    ///
    /// So the accepted SET is what is asserted, in both directions, against bytes nobody can edit
    /// into agreement:
    ///
    ///   * the frozen page-spelled document below -- the shape every build through
    ///     `SHARD_INDEX_FORMAT_VERSION` 27 emits -- still loads with all three values POPULATED.
    ///   * the same document with those three keys rewritten to the block spelling loads to an
    ///     EQUAL descriptor, which is what would make flipping the writer safe rather than hopeful.
    ///
    /// A round trip could establish neither: it writes and reads with the same build, so both
    /// halves move together and agree. Populated-ness is asserted SEPARATELY from equality,
    /// because `Option` defaulting is exactly what the silent drop looks like and two `None`s
    /// compare equal -- an equality check alone is how this bug would pass.
    ///
    /// THE READER TAKES BOTH; THE WRITER HAS SINCE MOVED TO THE BLOCK SPELLING. This test is the
    /// installed-base half and stays exactly as it was: the frozen document below is what earlier
    /// builds wrote, and it must keep loading. `a_durable_name_is_never_quietly_dropped` above
    /// still finds all three page spellings because they are now the ALIASES, which is what that
    /// guard accepts -- it asks that a durable name still be accepted, not that it still be
    /// emitted.
    #[test]
    fn the_slab_descriptor_reader_takes_the_block_spelling_and_the_page_spelling() {
        // Frozen, page-spelled, and NOT regenerated: this records the shape an older build wrote.
        // `state` is `rename_all = "snake_case"`, hence the lowercase variant.
        const SLAB_DESCRIPTOR_AT_STAMP_27: &str = r#"{"band_id":4,"page_segment_id":7,"state":"sealed","physical_bytes":8192,"logical_bytes":4096,"first_page_id":11,"last_page_id":29}"#;

        const STORED_KEYS: [&str; 3] = [
            r#""page_segment_id":"#,
            r#""first_page_id":"#,
            r#""last_page_id":"#,
        ];

        // The fixture really is the old shape. Without this pair the arms below vary nothing: a
        // fixture quietly updated to the new spelling would make every assertion that follows
        // pass while proving the opposite of what it claims.
        for old in STORED_KEYS {
            assert!(
                SLAB_DESCRIPTOR_AT_STAMP_27.contains(old),
                "the frozen bytes no longer spell {old}, so this guard is not testing the stored \
                 shape any more"
            );
        }
        for new in ["block_segment_id", "first_block_id", "last_block_id"] {
            assert!(
                !SLAB_DESCRIPTOR_AT_STAMP_27.contains(new),
                "the frozen bytes already carry {new}, so they are not a pre-change document"
            );
        }

        let stored: BlockStoreSlabDescriptor = serde_json::from_str(SLAB_DESCRIPTOR_AT_STAMP_27)
            .expect("the frozen page-spelled slab manifest must still load");
        assert_eq!(
            stored.block_slab_id, 7,
            "[CLAUSE stored-slab-id] the slab identity did not come off the stored key"
        );
        assert_eq!(
            stored.first_block_id,
            Some(11),
            "[CLAUSE stored-first] the stored `first_page_id` did not reach `first_block_id`"
        );
        assert_eq!(
            stored.last_block_id,
            Some(29),
            "[CLAUSE stored-last] the stored `last_page_id` did not reach `last_block_id`"
        );

        // The block spelling in a document otherwise identical: built by rewriting the KEYS in the
        // frozen bytes, so the two arms differ in exactly three tokens and nothing else.
        let as_block = SLAB_DESCRIPTOR_AT_STAMP_27
            .replace(r#""page_segment_id":"#, r#""block_segment_id":"#)
            .replace(r#""first_page_id":"#, r#""first_block_id":"#)
            .replace(r#""last_page_id":"#, r#""last_block_id":"#);
        for old in STORED_KEYS {
            assert!(
                !as_block.contains(old),
                "the rewrite left {old} behind, so this arm is still testing the old spelling"
            );
        }

        let canonical: BlockStoreSlabDescriptor = serde_json::from_str(&as_block).expect(
            "a slab manifest spelling the block names must load, or the writer can never converge",
        );

        // POPULATED, clause by clause, and asserted BEFORE equality. A named map drops an
        // undeclared key in silence and leaves every other value intact, so "it loaded" is not the
        // property being established -- "it loaded CARRYING the identity" is.
        assert_eq!(
            canonical.block_slab_id, 7,
            "[CLAUSE block-slab-id] the block-spelled slab identity did not land"
        );
        assert!(
            canonical.first_block_id.is_some(),
            "[CLAUSE first-block-id] `first_block_id` came back None from a document that spells \
             it. This is the silent drop, not an error: the key was not in the accepted set"
        );
        assert!(
            canonical.last_block_id.is_some(),
            "[CLAUSE last-block-id] `last_block_id` came back None from a document that spells \
             it. This is the silent drop, not an error: the key was not in the accepted set"
        );
        assert_eq!(
            stored, canonical,
            "[CLAUSE whole-descriptor] the two spellings decoded to different descriptors, so the \
             reader took one of the forms rather than the set"
        );
        println!(
            "  page-spelled -> slab {} first {:?} last {:?}; block-spelled -> slab {} first {:?} last {:?}",
            stored.block_slab_id,
            stored.first_block_id,
            stored.last_block_id,
            canonical.block_slab_id,
            canonical.first_block_id,
            canonical.last_block_id
        );
    }

    /// EVERY SITE, NOT JUST THE ONE WITH A FIXTURE.
    ///
    /// The guard above proves the accepted set on `BlockStoreSlabDescriptor`, the one struct
    /// carrying all three stored keys. Sixteen other fields emit `page_segment_id` and five more
    /// each emit `first_page_id` and `last_page_id`, and a fixture for every one of them would be
    /// twenty-nine fixtures proving one property. This pins the property at the SITE instead: a
    /// field that emits one of these three stored names must also ACCEPT its block spelling, so a
    /// new site added without the alias fails here rather than shipping a single-form reader.
    ///
    /// Counted off the source text and asserted as an EQUALITY, not a floor -- a floor passes when
    /// a site is added without its alias, which is the whole failure this exists to catch. The
    /// emitting counts are also asserted non-zero: a walk that found nothing would satisfy an
    /// equality of two zeroes.
    ///
    /// `crates/temporalstore-snapshot` is walked too. It is OUTSIDE
    /// `a_durable_name_is_never_quietly_dropped`'s walk, which reads only this crate's `src`, and
    /// `snapshot_manifest_types.rs` emits `page_segment_id` on the snapshot manifest -- a durable
    /// name no registry in this repository covers.
    #[test]
    fn every_field_emitting_a_stored_page_name_also_accepts_the_block_name() {
        let crates_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the crate directory has a parent")
            .to_path_buf();
        let mut sources = String::new();
        let mut files_read = 0_usize;
        let mut pending = vec![crates_root];
        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    if entry_path.file_name().and_then(|n| n.to_str()) == Some("target") {
                        continue;
                    }
                    pending.push(entry_path);
                } else if entry_path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    if let Ok(text) = std::fs::read_to_string(&entry_path) {
                        files_read += 1;
                        sources.push_str(&text);
                        sources.push('\n');
                    }
                }
            }
        }
        assert!(
            sources.len() > 100_000,
            "the source walk found almost nothing ({} bytes over {files_read} files); every count \
             below would be zero and the equalities would hold vacuously",
            sources.len()
        );

        // COUNTED OVER ATTRIBUTE LINES ONLY, WHICH IS NOT A TIDINESS CHOICE.
        //
        // `rename = "pages"` also occurs inside a STRING LITERAL, in
        // `page_and_block_name_one_tier`'s source-text clause that pins that very attribute. A
        // whole-file substring count sees two sites for `pages` and one paired attribute, and
        // fails against a file that is entirely correct. Restricting the haystack to lines whose
        // trimmed form opens with `#[serde(` keeps the real attributes and drops the quotation,
        // and it is the same discrimination every count in this guard needs.
        let attribute_lines: Vec<&str> = sources
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("#[serde("))
            .collect();
        assert!(
            attribute_lines.len() > 100,
            "the walk found only {} serde attribute line(s) over {files_read} files; every count \
             below would be zero and the equalities would hold vacuously",
            attribute_lines.len()
        );
        let count_in_attributes = |needle: &str| -> usize {
            attribute_lines
                .iter()
                .map(|line| line.matches(needle).count())
                .sum()
        };

        // THE STORED NAMES WHOSE READER TAKES THE SET. Each now emits the BLOCK spelling and must
        // still ACCEPT the page one, as an EQUALITY rather than a floor -- a floor passes when a
        // site is added without its alias, which is the whole failure this exists to catch.
        //
        // THE WRITER MOVED, AND THAT IS WHY THE PAIR READS THIS WAY ROUND. It used to be
        // (page, block): emit the stored page name, accept the block one. The reader was widened
        // first, so every spelling a store on disk can carry is still accepted, and only then did
        // the writer flip -- what any earlier build wrote still loads through the alias, and what
        // this build writes is the canonical spelling. Reversing the pair without reversing the
        // attributes is what this equality refuses.
        for (stored, accepted) in [
            ("block_segment_id", "page_segment_id"),
            ("first_block_id", "first_page_id"),
            ("last_block_id", "last_page_id"),
            ("block", "page"),
            ("blocks", "pages"),
            ("object_block_layouts", "object_page_layouts"),
            // `stale_block_tombstones` is deliberately NOT here: its page spelling is a
            // SIBLING FIELD on the same struct rather than an alias, so pairing them would
            // require two fields to claim one wire name. See
            // `no_two_fields_of_one_struct_claim_one_wire_name` below.
            (
                "blocks_named_an_object_the_list_lacked",
                "pages_named_an_object_the_list_lacked",
            ),
            (
                "list_held_an_object_no_block_names",
                "list_held_an_object_no_page_names",
            ),
        ] {
            let emits = format!("rename = {:?}", stored);
            let accepts = format!("rename = {:?}, alias = {:?}", stored, accepted);
            let emitting = count_in_attributes(&emits);
            let paired = count_in_attributes(&accepts);
            assert!(
                emitting > 0,
                "no field emits the canonical name `{stored}` any more. The writer flip put it there, so its loss is \
                 a rename this guard has not been told about, or a removal; either way the list \
                 below has to move in the same change"
            );
            assert_eq!(
                emitting, paired,
                "{} of {emitting} field(s) emitting the stored name `{stored}` do not also accept \
                 `{accepted}`.\n\
                 `serde(rename)` REPLACES the accepted name, it does not add to it, so such a \
                 field reads a document spelling `{accepted}` as an ABSENT key -- silently, with \
                 every other value intact.",
                emitting - paired
            );
            println!("  {stored} -> emitted at {emitting} site(s), all accepting {accepted}");
        }

        // THE DECLINE THAT USED TO LIVE HERE HAS BEEN LIFTED, AND THIS RECORDS WHY RATHER THAN
        // JUST DISAPPEARING.
        //
        // `pages` on `object_record::BlockLayout` was deliberately single-form, pinned here so
        // that re-attempting the alias failed, because
        // `the_stored_object_layout_emits_the_page_spelling_and_reads_one_back` asserted in its
        // DECODE clause that the block spelling was REFUSED. The stated ground was that an
        // accepted alias there would make "which spelling is on disk" the open question rather
        // than "whether to move it".
        //
        // That question has now been answered rather than avoided: the writer moved, so the
        // canonical spelling is what goes to disk and `pages` is the alias an earlier build's
        // layout is read through. The guard that refused the alias was rewritten in the same
        // change -- it is now
        // `the_stored_object_layout_emits_the_block_spelling_and_reads_both_back`, and its decode
        // clause drives BOTH spellings and asserts they land on the same layout. That assertion is
        // strictly stronger than the refusal it replaced: a refusal only says the other spelling
        // is not accepted, while this says the two names mean the same thing.
        //
        // What keeps the pair honest is the `("blocks", "pages")` row in the table above, which
        // holds the same equality as every other stored name. The pin that demanded "change that
        // guard's DECODE clause in the same commit and move this row into the table above" was
        // satisfied exactly that way, which is the only reason this block is gone.
    }

    #[test]
    fn an_address_written_with_any_older_field_name_still_loads() {
        // Every spelling this wire form has ever used, in one record: the original names, the
        // `page_segment_id` / `routing_slot` renames, and the older `extent_id` / `zone_id` /
        // `checksum` aliases. All of them must still land, or an index already on disk stops
        // resolving and the blocks it points at become unreachable.
        let legacy = serde_json::json!({
            // THE RENAMES STILL LOAD; THE SPLIT ADDRESS DOES NOT, AND THEY ARE DIFFERENT THINGS.
            // Every other key here is the same number under an older name, which is free to
            // read. The slab id and the offset were two FIELDS and are now two halves of one
            // word, which is a shape change with no safe reading -- so this fixture carries the
            // merged word and `a_split_address_is_refused_and_a_merged_one_round_trips` drives
            // the refusal of the shape this line used to hold.
            "a": (3_u64 << 32) | 128_u64,
            "length": 126_u64,
            "page_id": 9_u64,
            "object_id": 122110326161599232_u64,
            "routing_slot": 545210715_u32,
            // `page_id.or(object_id)`, which is the only generation any writer in this engine has
            // ever stored. This fixture is about the NAMES still loading, and it spelled an
            // independent `2` here until the field became derived; an index carrying that value
            // is now refused rather than silently re-keyed, which
            // `an_index_whose_generation_disagrees_is_refused_loudly` drives directly.
            "generation": 9_u64,
            "zone_id": 4_u64,
            "checksum": "c38c2bf3055c516a98ac5d97f30e7c364e827bc0a1b2c3d4e5f60718293a4b5c"
        });
        let address: ElementEntry = serde_json::from_value(legacy).expect("a legacy address loads");
        assert_eq!(address.block_slab_id(), 3);
        assert_eq!(address.offset(), 128);
        assert_eq!(address.length, 126);
        // Every other field the legacy record carried, under whichever name it used: this is the
        // "all of them must still land" the comment above promises, and asserting one of them was
        // never enough to keep that promise.
        assert_eq!(address.block_id(), Some(9));
        // `object_id` IS STILL READ OFF THE WIRE AND NO LONGER LANDS IN A FIELD, which is the third
        // slot to reach that state after `routing_slot` and `sha256`. The legacy record carries a
        // real one; what it now feeds is the generation cross-check in `TryFrom`, not a field. So
        // the assertion that used to compare it is replaced by the generation it agrees with --
        // and that this record LOADS at all is the compatibility being tested.
        assert_eq!(address.generation(), Some(9), "the generation is the block id");
        // `routing_slot` IS STILL READ AND STILL IGNORED, and the SLOT IS NOW GONE from the
        // struct rather than written empty. Both halves matter and they are asserted separately.
        //
        // That the legacy record still LOADS with the key present is the first: the assertions
        // above all passed on a record carrying `routing_slot`, which is the compatibility this
        // change rests on -- a key no field claims is tolerated by a named decoder rather than
        // refused. There is no accessor left to compare its value through, and the bucket a
        // reader wants is `block_routing_bucket(object_key, ..)` over the store's stamped range.
        //
        // That a CURRENT address no longer writes the key is the second. It used to be written
        // empty so the index log's positional layout would not shift; the slot is retired now, and
        // `shorter_struct_against_an_existing_row` measures why that is safe -- a positional row
        // longer than the struct is refused by LENGTH, so the shift this nil was avoiding cannot
        // be reached silently.
        assert_eq!(
            serde_json::to_value(&address)
                .expect("an address serializes")
                .get("rs")
                .cloned(),
            None,
            "the slot is retired: a current address must not write `rs` at all, not even as nil"
        );
        // The generation this record stored, which is now the block id it is derived from. The
        // fixture spelled an independent `2` here until the field became derived; a record that
        // really carried one is refused rather than loaded, which
        // `an_index_whose_generation_disagrees_is_refused_loudly` drives.
        assert_eq!(address.generation(), Some(9));
        assert_eq!(address.generation(), address.block_id());
        // A slab is the slab now, so a slab STORED against a different slab is accepted and
        // ignored rather than believed. This record says slab 3 and zone 4, which could only have
        // been written under a configuration that sized slabs and slabs differently -- one the
        // tree never set, and no longer has a knob for.
        assert_eq!(
            address.slab_id(),
            Some(3),
            "the address answers the slab, whatever an older record stored beside it"
        );
        // And the digest is accepted and dropped rather than rejected: an index written before the
        // address stopped carrying one still loads, which is the whole point of keeping the alias.
        // The block envelope holds the digest that verifies the bytes.
    }

    #[test]
    fn an_address_costs_far_less_than_its_field_names_used_to() {
        // `page_segment_id` is fifteen characters spent to label an integer, written once per
        // index item forever.
        let address = ElementEntry::from_parts(
            3,
            128,
            126,
            Some(9),
            Some(122110326161599232),
        );
        let encoded = serde_json::to_string(&address).unwrap();
        println!("ENCODED = {encoded} ({} bytes)", encoded.len());
        // Not vacuous: the values that ARE still carried must be there before the size claim
        // means anything.
        assert!(encoded.contains("\"l\":126"), "the length must be written: {encoded}");
        assert!(encoded.contains("\"pi\":9"), "the block id must be written: {encoded}");
        // AND THE ID HANDED TO `from_parts` MUST NOT REACH THE WIRE. The parameter is inert: an
        // address carries no identity, and the entry derives it. This assertion used to be the
        // opposite -- `assert!(encoded.contains("122110326161599232"))` -- and it is inverted
        // rather than deleted, because "the id is not written" is the claim this change makes and
        // something has to hold it.
        assert!(
            !encoded.contains("122110326161599232"),
            "the id handed to from_parts reached the wire, so the address is still storing one: \
             {encoded}"
        );
        // THE SLOT ITSELF STAYS, WRITTEN EMPTY, and that is not the same as being retired: the
        // index log packs this struct POSITIONALLY, so dropping a field shortens the array and
        // refuses every row already on disk. `"oi":null` is the stored shape now, and the served
        // index's version stamp is what pays for the value moving.
        assert!(
            encoded.contains("\"oi\":null"),
            "the identity slot must still be present and empty: {encoded}"
        );
        // The routing bucket used to be checked here as `545210715`, then as `"rs":null` once the
        // address stopped holding one and the slot was written empty. The slot is retired now, so
        // neither it nor the digest appears at all.
        for gone in ["\"rs\"", "\"h\"", "page_segment_id", "routing_slot", "object_id", "generation"] {
            assert!(!encoded.contains(gone), "{gone} should not be written any more");
        }
        // SIXTY-TWO BYTES, measured, and the number is stated because it MOVED with this change.
        //
        // AND THE FIGURE IT REPLACES WAS ALREADY STALE, which is worth recording rather than
        // quietly overwriting. The comment here read "ninety-one bytes". The two retired slots
        // spell `,"rs":null` (ten bytes) and `,"h":null` (nine), so an encoding that measures 62
        // without them measured 81 with them -- not 91. The 91 had stopped describing this
        // fixture at some earlier change and nothing failed, because the assertion beside it was
        // `encoded.len() < 100`: a bound that loose cannot notice its own prose going wrong. It is
        // an equality now for that reason.
        //
        // The nils were there because a row is read by POSITION in the index log, so a field that
        // vanishes when empty moves every field behind it -- which once shifted `generation` into
        // `object_id` until a round-trip test caught it. That reasoning is intact and it is why
        // `g` is still written; what changed is that a retired slot is not a vanishing one, and
        // `shorter_struct_against_an_existing_row` measures the difference: a positional row
        // longer than the struct decoding it is refused by LENGTH rather than reinterpreted.
        assert_eq!(
            48,
            encoded.len(),
            "the compact address width moved again when the identity slot started being written \
             empty: an 18-digit id spelled `\"oi\":122110326161599232` is 23 bytes and `\"oi\":null` \
             is 9, so 62 becomes 48. If this is not 48, say what it is and why rather than \
             widening the bound: {encoded}"
        );
        // And it round-trips through its own new form.
        let back: ElementEntry = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back, address);
    }
    use super::*;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = BlockStore::default();
        let root = store.inner.lock().unwrap().root.clone();
        assert!(root.exists(), "Default must create its scratch dir");
        let clone = store.clone();
        drop(store);
        assert!(root.exists(), "a live clone must keep the scratch dir");
        drop(clone);
        assert!(!root.exists(), "the last clone's drop must remove the scratch dir");
    }

    #[test]
    fn explicit_root_survives_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        drop(store);
        assert!(dir.path().exists(), "a caller-supplied root must never be deleted");
    }

    #[test]
    fn active_slab_torn_tail_is_fenced_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let a1 = store.append(b"record-one").unwrap();
        let a2 = store.append(b"record-two").unwrap();
        drop(store);
        // Simulate a crash that left a partial/torn record (no valid envelope) on the ACTIVE
        // slab past the last committed record.
        let slab = slab_path(dir.path(), a2.block_slab_id());
        let clean_len = std::fs::metadata(&slab).unwrap().len();
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&slab)
                .unwrap();
            file.write_all(b"\x01\x02 torn partial record without a valid envelope")
                .unwrap();
            file.sync_all().unwrap();
        }
        assert!(std::fs::metadata(&slab).unwrap().len() > clean_len);
        // Reopen: the torn tail must be physically fenced back to the readable prefix (mirrors
        // resume-at-committed-length), not left embedded mid-slab.
        let reopened = BlockStore::new(dir.path());
        assert_eq!(
            std::fs::metadata(&slab).unwrap().len(),
            clean_len,
            "torn active-slab tail must be truncated to the readable prefix on reopen"
        );
        // Committed records survive and remain readable.
        assert_eq!(reopened.read(&a1).unwrap(), b"record-one");
        assert_eq!(reopened.read(&a2).unwrap(), b"record-two");
        // A new append lands right after the fenced prefix rather than on top of a committed
        // record. Checked by OFFSET: a block id is an index inside its object now, so three
        // blocks of three different objects all being block 0 is expected and says nothing
        // about where they landed.
        let a3 = reopened.append(b"record-three").unwrap();
        assert_eq!(a3.offset(), clean_len, "a new append starts at the fenced prefix");
        assert_ne!(a3.offset(), a1.offset());
        assert_ne!(a3.offset(), a2.offset());
        assert_eq!(reopened.read(&a3).unwrap(), b"record-three");
    }

    #[test]
    fn a_gc_round_that_reclaimed_nothing_does_not_rewrite_the_manifest() {
        // The sibling test above keeps the full manifest re-serialize off the APPEND path. The
        // page-GC path had no such guard and rewrote it unconditionally, once per round, on a
        // stage the periodic loop runs whenever block pressure holds.
        //
        // It is not a cheap write: it serialises every slab, fsyncs the temp file, renames it and
        // fsyncs the parent directory -- two fsyncs. A round that reclaimed nothing rewrites it
        // with byte-identical content, so unlike the append test the BYTES cannot tell the two
        // apart and the modification time is the observable.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        for index in 0..8u64 {
            store.append(format!("record-{index}").as_bytes()).unwrap();
        }
        store.sync_durable().unwrap();
        let manifest = slab_manifest_path(dir.path());
        assert!(manifest.exists(), "expected the manifest to exist after a durable sync");
        let before = std::fs::metadata(&manifest).unwrap().modified().unwrap();

        // `retain_from` 0 leaves every slab above the floor, so this round walks and reclaims
        // nothing.
        let report = store.gc_slabs_before(0).unwrap();
        assert!(
            report.removed_block_slab_ids.is_empty(),
            "the fixture was supposed to reclaim nothing, but removed {:?}",
            report.removed_block_slab_ids
        );
        // The denominator: a round that walked no slabs at all would satisfy the assertion below
        // for the wrong reason.
        assert!(
            !report.retained_block_slab_ids.is_empty(),
            "the round walked no slabs, so it proves nothing about skipping the write"
        );

        let after = std::fs::metadata(&manifest).unwrap().modified().unwrap();
        assert_eq!(
            before, after,
            "a page-GC round that reclaimed nothing rewrote the slab manifest"
        );
    }

    #[test]
    fn per_append_does_not_reserialize_the_slab_manifest_on_the_default_path() {
        // MANIFEST-CONFORMANCE FOLD no-O(n) proof: on the default single-barrier path the per-append
        // slab-manifest full re-serialize (the measured O(n) aging driver -- ~961 B rewritten per
        // write, growing with the slab count) is OFF the write path. Appending many records must
        // NOT rewrite `block_extent_manifest.json` each time; the catalog is deferred and made
        // durable in one shot at sync_durable()/seal. Proven by the manifest file bytes staying
        // byte-identical across a burst of appends, then changing exactly once at sync_durable.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let manifest = slab_manifest_path(dir.path());
        // Land one append so the manifest exists at a known state, then snapshot it.
        store.append(b"seed").unwrap();
