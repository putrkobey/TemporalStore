// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN THE PORTED INDEX-LOG RECORD BECOME THE LIVE INDEX-LOG RECORD? NOT AS WRITTEN, AND THE
//! BLOCKER IS THE OBJECT ID.
//!
//! `crate::index_log_record` is a direct port with no importer anywhere in the tree. Its header
//! states three load-bearing properties and a framing claim, and none of the four had ever been
//! exercised against the live log. This module measures them.
//!
//! # THE DECIDING QUESTION: WHERE A NUMERIC OBJECT ID WOULD COME FROM
//!
//! The ported `IndexItem` addresses an object by `object_id: u32` AND CARRIES NO CHARACTERS AT
//! ALL -- `the_ported_item_has_no_field_that_can_carry_a_character` walks its protobuf keys and
//! finds every one of the seven is a varint, so there is no length-delimited field on it in which
//! a key, an element name or a page handle could ride.
//!
//! A DURABLE NUMERIC OBJECT ID ALREADY EXISTS AND IS ALREADY ON THE LIVE ROW. It is
//! `object_id: u64` -- `hashing::stable_block_object_id(shard, kind, key)`, an FNV-1a 64 over the
//! key's bytes, so it is durable in the only sense that matters here: a restart recomputes the
//! same number from the same terms. The row does not even spend bytes on it, because
//! `strip_object_id_repeat` drops it whenever it agrees with the derivation from the two fields
//! beside it. So the port's premise is not missing from this tree; the port's WIDTH is.
//!
//! AND THE NARROWING TO 32 BITS IS REFUSED ON MEASUREMENT, not on principle.
//! `a_thirty_two_bit_object_id_collides_long_before_the_sixty_four_bit_one_does` searches for the
//! first pair of distinct keys whose low 32 bits agree and prints how few it takes; the 64-bit
//! ids of that same pair are distinct. The consequence is not a lost byte, it is a lost owner:
//! `the_fold_stores_the_rows_object_id_as_a_u64_and_the_truncation_is_not_a_member` drives
//! `fold_delta_block_items` and shows the row's id landing in `BucketNode::object_index`, a
//! STORED `u64` set whose members are compared for equality against `stable_block_object_id`
//! computed from text at sites all over the engine. A truncation agrees with none of them, and
//! the disagreement reads as a missing owner rather than as a failure.
//!
//! THE CHARACTERS ARE THE OTHER HALF AND THEY ARE ALREADY REFUSED IN THIS TREE.
//! `the_entry_cannot_name_its_object_by_slot` asks the narrower question -- is an ordinal
//! derivable from something the index already holds -- and answers no on two independent grounds.
//! Its own closing sentence is explicit about which one decides it: *"Ground 1 is a fixable
//! defect ... Ground 2 is not fixable without a durable ordinal-to-characters map, which is the
//! second index this leg exists to avoid. So the leg is refused on Ground 2, and Ground 1 is the
//! reason a partial fix would not rescue it."* Ground 2 is that THE ENTRY IS THE ONLY DURABLE
//! HOME OF THE KEY'S CHARACTERS. Ground 1 -- that the load throws away the slot positions the
//! wire preserved -- is the fixable one.
//!
//! That transfers directly, and it is why the port cannot be adopted as written rather than why
//! it needs work: `fold_delta_block_items` reads `item.object_key` to build
//! `BlockIndex::object_key`, so the row is where the entry's characters come from on the replay
//! path. A row shaped like the port's carries no characters, so there is nothing for the entry to
//! be rebuilt from and no second structure that holds them.
//!
//! > SO: ADOPTING THE PORTED ITEM'S FIELD SET IS IMPOSSIBLE WITHOUT EITHER WIDENING ITS
//! > `object_id` TO 64 BITS **AND** ADDING A CHARACTER-BEARING FIELD BACK -- at which point it is
//! > the live row -- OR ADDING A DURABLE ID-TO-CHARACTERS MAP, which is the second index the
//! > campaign's "just one index" constraint forbids.
//!
//! # THE FRAMING CLAIM IS ALMOST TRUE, AND THE PART THAT IS FALSE IS THE DANGEROUS PART
//!
//! The header says "same framing (see `crate::record_framing`)". The live logs are framed by
//! `crate::log_framing`, whose current binary frame is
//! `0xB3 | varint64(len) | le_u32(crc32c) | payload`; the ported frame is
//! `varint32(len) | le_u32(crc32c) | payload`. `the_ported_frame_is_the_live_binary_frame_minus_
//! its_marker_byte` measures that the two are byte-identical behind the marker.
//!
//! THE MARKER IS NOT DECORATION. `log_framing` reads four shapes out of one file -- `0xB3`
//! binary, `#tsf2` text, `#tsf1` text, and a legacy unframed JSON line ending at `\n` -- and
//! picks between them by that first byte. A frame with no marker is not refused by the live
//! reader: it falls through to the unframed arm, which treats everything up to the first newline
//! as the payload. The same test shows the live reader does NOT return an error on a ported
//! frame, with a positive control that it round-trips a live one exactly.
//!
//! # WHAT THE PORT PREDATES, AND IT IS NOT ONE OPTIMISATION
//!
//! Eight harvests have landed on the live row since this port was written, each measured at the
//! site. They are cited rather than re-pinned, because each already has a test beside it:
//!
//!   * the page handle is STRIPPED when it is the derivation of the fields beside it -- 43 B of a
//!     176-B record, "the largest single field in the index log" (`strip_block_ref_key_repeat`);
//!   * and when it is NOT stripped it goes out as a NUMBER rather than decimal text -- 20 B of a
//!     161-B item, 12.4%, down to about nine (`block_ref_key_as_number_when_it_is_one`);
//!   * the object id is STRIPPED when it is the hash of the row's own fields -- nine bytes
//!     (`strip_object_id_repeat`);
//!   * the size slot is the STRIP SENTINEL and is unconditionally zero, so the four bytes it used
//!     to cost are banked on every row (`IndexItem::serialize`);
//!   * the model spelling goes out as a POSITION IN `MODEL_ID_NUMBERS` -- 7 B of a 176-B item;
//!   * the item kind goes out as a number instead of `"page"` on nearly every item;
//!   * the object-id and routing-bucket repeats the ADDRESS used to carry are gone -- 18 B of a
//!     142-B item, 12.7% (`strip_address_repeats`, now a documented no-op);
//!   * the object key is HOISTED ONTO THE RECORD when every item shares it -- 21 B per item,
//!     24-31% of a timestamped record (`IndexDeltaRecord::object_key`).
//!
//! The port can express none of these. Three of them -- the stripped handle, the stripped object
//! id and the zero size slot -- are banked by writing a SENTINEL into a slot that still exists,
//! and a prost field has no sentinel: it is absent when it is zero and present otherwise, which
//! is a different mechanism with a different reader. The port's `size: u32` in particular is a
//! real value, so adopting it rewrites the length the live row stopped writing.
//!
//! AND ONE OF THE PORT'S THREE STATED LOAD-BEARING PROPERTIES IS A STEP THIS TREE TOOK AND
//! REVERSED. The header says `IndexItem::in_wal` "is the flag that makes an address resolvable
//! without a lookup table". The live row has no such field: it writes the flag as a DERIVATION of
//! the address in the serializer, and `BlockIndex::log_backed()` is, in `IndexItem`'s own words,
//! "the accessor added when the equivalent stored flag was removed from the RESIDENT entry for
//! exactly this reason". A stored copy of a derivation is the thing the row has been shedding.
//!
//! # ROUTING BUCKET: THE PORT PUTS IT ON THE RECORD, AND THE ROW IS THE ONLY CARRIER
//!
//! The ported record carries ONE `routing_bucket` for the whole record (tag 3, `fixed64`); the
//! live row carries one PER ITEM, because a live delta record is scoped to a SHARD and a write
//! may touch keys in many buckets. So the port's shape means one record per bucket per write.
//!
//! That matters because the field is load-bearing on the replay path and nothing else supplies
//! it. `the_fold_takes_the_bucket_from_the_row_and_not_from_a_default` drives the half that still
//! has a subject: the fold creates the `BucketNode` keyed by `item.routing_bucket`, and not a
//! bucket zero.
//!
//! > AND THE OTHER HALF HAS BEEN ACTED ON RATHER THAN WEAKENED. This module was written against an
//! > entry that still declared `BlockIndex::routing_bucket`, and it recorded that the field was
//! > WRITE-ONLY across a reload: `#[serde(skip)]`, no visitor injecting it despite a doc comment
//! > on the field claiming one did, so a round-tripped entry came back at zero -- which that same
//! > comment named, two sentences earlier, as the value that would fold every entry into bucket
//! > zero silently. That was a true finding at the layer that DESCRIBES against the layer that
//! > DECIDES, and `65c7fc562` retired the field on the strength of it. A field that is write-only
//! > across a reload cannot be read back wrong once it does not exist, so the assertion is gone
//! > and the finding is here. The row remains the only carrier, which is the half the port's
//! > costing actually turns on.
//!
//! # THE FORMAT STAMP
//!
//! The index log has NO version of its own -- there is no `*_VERSION` constant in `index_log.rs`,
//! and the row's own doc says so: "The index log is POSITIONAL and carries no struct version, so a
//! row whose length does not match the struct decoding it is REFUSED -- and the format stamp
//! cannot help, because it guards the NAMED served index." So a row-shape change is guarded by
//! LENGTH, not by a number, and the twelve-slot count is the whole of the protection.
//!
//! `SHARD_INDEX_FORMAT_VERSION` is pinned below AT THE BASE so a reader of this module cannot
//! cost the adoption against a tree that already bumped it. Cited, not pinned: a ref sweep over
//! this repository finds the constant has held 2, 3, 5, 6, 7, 8, 9, 11, 12 and 13 across all
//! history; 4 and 10 appear nowhere, and 13 is live on four branches. The refusal is TWO-SIDED --
//! `engine.rs`'s container decoder refuses on `!=` and runs before `persistence.rs` compares with
//! `<` -- so a value too high is refused as well as one too low. Both sides already have tests
//! beside them (`block_entry_names`), so neither is re-pinned here.
//!
//! # NO PRODUCTION CODE MOVES HERE
//!
//! This module adds measurements only. No stored shape changes, so no stamp is owed.
//!
//! EVERY ASSERTION HERE WAS SHOWN TO FAIL ON CODE THAT DOES NOT HOLD IT, AND EVERY DETECTOR WAS
//! SHOWN TO PASS ON A SUBJECT THAT DOES. Each test carries its own control inline and names it.
//!
//! rust-internal: drives this crate's own framing, index log and bucket index, no external surface

#![allow(clippy::all)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use prost::Message as _;

use crate::block_store::ElementEntry;
use crate::engine::hashing::stable_block_object_id;
use crate::engine::state::{
    BlockIndex, BlockSlabLiveIndex, BucketFlags, BucketNode, CoreIndex,
};

// =================================================================================================
// FRAMING
// =================================================================================================

/// Read one protobuf varint at `offset`, returning it and the bytes it took.
fn varint_at(bytes: &[u8], offset: usize) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for (index, byte) in bytes[offset..].iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    None
}

fn a_ported_record() -> crate::index_log_record::IndexLogRecord {
    crate::index_log_record::IndexLogRecord {
        version: crate::index_log_record::INDEX_LOG_RECORD_VERSION,
        routing_bucket: 4,
        items: vec![crate::index_log_record::IndexItem {
            object_id: 1,
            block_id: 2,
            address: 4096,
            size: 512,
            in_wal: true,
            ..Default::default()
        }],
        sequence: 8,
        wal_sequence: 77,
        ..Default::default()
    }
}

/// THE PORTED FRAME IS THE LIVE BINARY FRAME WITH ITS MARKER BYTE REMOVED, AND THE MARKER IS WHAT
/// TELLS FOUR SHAPES APART IN ONE FILE.
///
/// The header of the ported record claims "same framing". It is the same checksum, the same
/// little-endian four bytes and the same length prefix -- and it is NOT the same frame, because
/// `log_framing` reads `0xB3` binary, `#tsf2`, `#tsf1` and a legacy unframed JSON line out of one
/// file and picks between them by the first byte.
///
/// THREE CONTROLS, because two of the three claims are about a NEGATIVE result:
///   * the live reader round-trips a LIVE frame exactly (the reader is not simply broken);
///   * the ported decoder REFUSES a live frame (the ported decoder can refuse at all);
///   * and only then is "the live reader does not refuse a ported frame" worth anything.
#[test]
fn the_ported_frame_is_the_live_binary_frame_minus_its_marker_byte() {
    let record = a_ported_record();
    let payload = record.encode_to_vec();
    assert!(
        payload.len() < 0x80,
        "this test compares the two varints byte for byte, so the payload must fit one varint \
         byte; it is {} B",
        payload.len()
    );

    let live = crate::log_framing::encode_frame(&payload);
    let ported = crate::record_framing::encode_framed(&record);

    assert_eq!(
        live[0],
        crate::log_framing::FRAME_MAGIC_V3,
        "the live binary frame leads with its marker"
    );
    assert_ne!(
        ported[0],
        crate::log_framing::FRAME_MAGIC_V3,
        "the ported frame has no marker at all -- its first byte is the length"
    );
    assert_eq!(
        &live[1..],
        &ported[..],
        "behind the marker the two frames are byte-identical: varint length, le crc32c, payload"
    );

    // CONTROL ONE -- the live reader is not simply broken: it round-trips its own frame.
    let (consumed, body) = crate::log_framing::next_frame(&live)
        .expect("a live frame is not corruption")
        .expect("a live frame is complete");
    assert_eq!(consumed, live.len(), "the whole frame is consumed");
    assert_eq!(body, &payload[..], "and the payload comes back exactly");

    // CONTROL TWO -- the ported decoder can refuse. Handed a live frame it reads the 0xB3 marker
    // as the first byte of the length varint and claims a payload that is not there.
    let on_live: Result<(crate::index_log_record::IndexLogRecord, usize), _> =
        crate::record_framing::decode_framed_at(&live, 0);
    assert!(
        on_live.is_err(),
        "the ported decoder must refuse a live frame, or the claim below is vacuous"
    );

    // THE CLAIM. The live reader does not refuse a ported frame. It is not recognised as framed
    // at all, so it goes down the legacy-unframed arm instead of being surfaced as corruption.
    let on_ported = crate::log_framing::next_frame(&ported);
    assert!(
        on_ported.is_ok(),
        "a ported frame is not REFUSED by the live reader; it is mis-read: {:?}",
        on_ported.as_ref().err()
    );
    let mis_read = on_ported.expect("checked ok");
    match mis_read {
        None => println!(
            "MIS-READ: the live reader treats a ported frame as a TORN TAIL (no newline in the \
             payload), which a caller truncates"
        ),
        Some((consumed, body)) => {
            assert_ne!(
                body,
                &payload[..],
                "a ported frame read by the live reader must not accidentally yield the payload, \
                 or this hazard would be benign"
            );
            println!(
                "MIS-READ: the live reader consumed {consumed} of {} B and handed back {} B that \
                 are not the payload",
                ported.len(),
                body.len()
            );
        }
    }
}

// =================================================================================================
// THE PORTED ITEM CARRIES NO CHARACTERS
// =================================================================================================

/// EVERY ONE OF THE PORTED ITEM'S SEVEN FIELDS IS A VARINT, SO NOTHING ON IT CAN HOLD A CHARACTER.
///
/// This is the structural half of the refusal: the live row's `object_key` and `block_ref_key`
/// have no counterpart on the ported item, and not because the port chose different names --
/// because the port has no length-delimited field in which text could ride.
///
/// THE CONTROL IS THE WALKER ITSELF. A walker that cannot SEE a wire-type-2 field would report any
/// message as character-free, so the same walk is run over the ported RECORD, whose `items` is a
/// repeated message and therefore length-delimited, and is required to find one.
#[test]
fn the_ported_item_has_no_field_that_can_carry_a_character() {
    let item = crate::index_log_record::IndexItem {
        object_id: u32::MAX,
        block_id: u32::MAX,
        address: u64::MAX,
        size: u32::MAX,
        in_wal: true,
        deleted: true,
        model_id: u32::MAX,
    };
    let bytes = item.encode_to_vec();

    let mut tags: Vec<(u64, u64)> = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let (key, used) = varint_at(&bytes, offset).expect("a field key is a varint");
        offset += used;
        let wire_type = key & 7;
        tags.push((key >> 3, wire_type));
