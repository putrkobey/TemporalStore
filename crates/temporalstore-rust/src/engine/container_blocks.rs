// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER PAGE STATES WHICH ELEMENT IT HOLDS.
//!
//! Until now a container page carried a VALUE and nothing else. Which element of which object that
//! value belonged to was known only to the page index entry that named it -- `BlockIndex::component`
//! -- and to the object id folded into the address. The page itself said nothing, so the bytes were
//! only interpretable beside the entry that pointed at them.
//!
//! That is the one fact underneath four separate refutations, and it is worth stating plainly
//! because each of those looked like an independent dead end:
//!
//!   * the element name cannot leave the page entry, because for a hash the name IS the element and
//!     for a hash the entry is the only place it is written (#2009);
//!   * the resident maps cannot be rebuilt from anything but the page index, because the index is
//!     the only thing that names elements (#2016's arms skip an unnamed page for exactly this
//!     reason);
//!   * pages cannot be batched usefully while the element name is folded into the address the
//!     write returns (#1999);
//!   * and the resident map cannot become authoritative while it is merged from a snapshot the
//!     index outranks (#2017).
//!
//! All four are downstream of the page not naming its own element. This module is where it starts
//! to, and on its own it BUYS NOTHING: it costs bytes on every container write and every reader
//! keeps answering exactly as it did. What it buys is that the element name now exists in a second
//! place, which is the precondition for it leaving the first.
//!
//! # THE SHAPE
//!
//! ```text
//!     magic            7 bytes   b"TSCPG1\n"
//!     key spelling     1 byte    how a key in this page spells its index component
//!     item count       varint
//!     per item:
//!       key length     varint
//!       key            that many bytes
//!       value tag      varint    0 -> the value is a SUFFIX of the key, offset follows
//!                                n -> the value is the next n-1 bytes
//!       (offset)       varint    present only when the tag is 0
//!       (value)        bytes     present only when the tag is >= 1
//! ```
//!
//! ## WHY THE VALUE MAY BE A SUFFIX OF THE KEY, WHICH IS NOT A TRICK
//!
//! For a `set` the element key IS the member and the value IS the member -- #2017 measured that
//! `hex::decode(component)` equalled the page bytes on every one of sixty entries. Writing both
//! would make a set page carry its member twice, and a stage whose whole justification is that it
//! is a cheap foundation cannot afford to double the one kind where the redundancy was already
//! measured. For a `zset` the key is an eight-byte score followed by the member, so the value is
//! the key from byte eight. Both fall out of one rule, which the encoder detects rather than being
//! told: if the value equals the key's tail, store where the tail starts instead of storing it.
//!
//! An EMPTY value needs no case of its own -- the empty slice is the tail starting at the key's
//! end, so it encodes in two varints and decodes back to empty.
//!
//! ## WHY THE KEY SPELLING IS IN THE PAGE AND NOT IN THE SIGNATURE
//!
//! The obvious alternative was to key each item by the component STRING, so that selecting an
//! element is a string compare and nothing has to know the kind. Measured on the shapes this store
//! actually writes, that costs a `set` page 42 bytes over its member and a `zset` page 58, because
//! a component is hex and hex is two characters a byte. Carrying the key in its own bytes and one
//! byte saying how it spells costs 11 and 19. So the spelling byte is not generality for its own
//! sake; it is what keeps the foundation affordable for the two kinds whose keys are hex.
//!
//! It also means `read_block_bytes` needs no new argument. A page asked for one of its elements is
//! handed the component it was looked up by, and this module turns that back into key bytes without
//! being told the kind -- so the twenty-odd call sites of the read funnel are untouched, and no
//! mechanical signature rewrite goes past the guards that read those call sites.
//!
//! ## AND THE SPELLING IS A ROUND TRIP, ASSERTED BOTH WAYS
//!
//! [`component_from_element_key`] and [`element_key_from_component`] are inverses, and that is the
//! whole safety argument for this module: the page's key and the index's component are two
//! spellings of one value, so either can be derived from the other and a later stage may delete
//! whichever it no longer wants to store. If they were merely written consistently at the same
//! moment by the same call site -- which is how the component and the set payload agree TODAY, and
//! nothing re-checks them -- then the second spelling would be an unverified copy rather than a
//! derivation. `container_block_element_key` drives the round trip over every spelling and over the
//! edge cases each one has (an empty member, a score of exactly sixteen characters with nothing
//! after it, a field name that is not valid UTF-8).
//!
//! EXCEPT ONE DIRECTION OF ONE SPELLING, SINCE THE COMPONENT STOPPED SPELLING A ZSET'S SCORE.
//! `ScoreThenMember`'s `component_from_element_key` now drops a key's first eight bytes rather
//! than rendering them, so `element_key_from_component` cannot get them back and returns a
//! PLACEHOLDER there instead of the true score. The two are no longer inverses for this one
//! spelling -- `container_block_element_key` asserts the loss explicitly rather than asserting a
//! round trip that does not hold -- and the reason it is still safe is that nothing resolves a
//! `ScoreThenMember` key through `element_key_from_component` and then trusts its first eight
//! bytes: see `select_container_element`'s member-suffix comparison for that spelling.
//!
//! # WHAT DOES NOT MOVE
//!
//! No stored format is retired and no version stamp is spent. The magic DISCRIMINATES: a page
//! written before this module has no magic, [`decode_container_block`] answers `NotFramed`, and the
//! read funnel hands the bytes back exactly as it always did. That is the same trade
//! `encode_feature_block` already makes with `TSFPB1\n` against the JSON pages that came before it,
//! and it is why this stage does not move `SHARD_INDEX_FORMAT_VERSION` at all: the SHARD INDEX
//! does not change shape here, and the page payload is self-describing rather than versioned.
//! The stamp stood at 2 when this was written and is 5 now, moved by later stages; this stage
//! is not one of them, and the number above is the one it had then rather than the one to read
//! off the constant today.
//!
//! A payload that legitimately begins with these seven bytes would be mistaken for a frame. That is
//! the same exposure the two feature magics carry, on the same kind of bytes, and it is noted here
//! rather than guarded because a guard would have to be a length or checksum the block record
//! already provides.

/// How a key inside a container page spells the component its index entry is filed under.
///
/// One byte in the page, because the page has to be interpretable WITHOUT the entry that names it
/// -- that is the entire point of the module -- and the kind is not recoverable from the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElementKeySpelling {
    /// The component is the key's bytes as UTF-8. A hash field.
    Utf8,
    /// The component is `hex::encode(key)`. A set member.
    Hex,
    /// The component is sixteen hex characters of the key's first EIGHT BYTES read big-endian,
    /// followed by `hex::encode` of everything after them. A zset's biased score and member.
    ScoreThenMember,
    /// The component is sixteen hex characters of the key's eight bytes read big-endian, and the
    /// key is exactly eight bytes. A list sequence.
    BiasedWord,
}

impl ElementKeySpelling {
    fn to_byte(self) -> u8 {
        match self {
            ElementKeySpelling::Utf8 => 0,
            ElementKeySpelling::Hex => 1,
            ElementKeySpelling::ScoreThenMember => 2,
            ElementKeySpelling::BiasedWord => 3,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(ElementKeySpelling::Utf8),
            1 => Some(ElementKeySpelling::Hex),
            2 => Some(ElementKeySpelling::ScoreThenMember),
            3 => Some(ElementKeySpelling::BiasedWord),
            _ => None,
        }
    }

    /// The spelling the given stored kind uses, or `None` for a kind whose page is its whole
    /// object and carries no element key.
    ///
    /// Derived from the kind string the write sites already hold, in ONE place, so a fifth
    /// container kind cannot be given a page format by accident at a new call site.
    pub(super) fn for_kind(kind: &str) -> Option<Self> {
        match kind {
            "hash" => Some(ElementKeySpelling::Utf8),
            "set" => Some(ElementKeySpelling::Hex),
            "zset" => Some(ElementKeySpelling::ScoreThenMember),
            "list" => Some(ElementKeySpelling::BiasedWord),
            _ => None,
        }
    }
}

/// Seven bytes that say a page payload holds items rather than one value.
///
/// THE FIRST SHAPE, WHICH STILL READS. Every item in it is present: it has no way to say an element
/// was removed, which is the whole reason there is a second shape. Kept because stores written by
/// #2022 and #2027 hold these pages and a reader that could not walk them would lose their
/// elements -- see [`CONTAINER_BLOCK_MAGIC_V2`] for what changed and what it costs.
pub(super) const CONTAINER_BLOCK_MAGIC: &[u8] = b"TSCPG1\n";

/// Seven bytes that say a page payload holds items SOME OF WHICH MAY BE REMOVALS.
///
/// # WHY A SECOND MAGIC AND NOT A FLAG BYTE PER ITEM
///
/// A removal has to be a WRITTEN ITEM rather than an absence, because a page is never rewritten by
/// the removal path: #2027 folds pages only when a round was already rewriting them, and a folded
/// page stays live for its other elements. #2028 drove what that costs -- twelve members folded to
/// one page, one removed, the index naming eleven and the page holding twelve -- so a membership
/// derived from pages resurrected the removed member. This magic is where the page starts to be
/// able to say otherwise.
///
/// The obvious encoding was a `flags` varint beside every item, which costs ONE BYTE PER ITEM on
/// every page whether or not anything was ever removed. It does not have to: the item's VALUE TAG
/// already has an unused codepoint, and widening its meaning is free.
///
/// ```text
///     v1 tag   0 -> value is a suffix of the key, offset follows
///              n -> value is the next n-1 bytes          (so tag 1 is an empty inline value)
///
///     v2 tag   0 -> LIVE, value is a suffix of the key, offset follows
///              1 -> REMOVED. No value bytes at all, which is the point: a tombstone carries the
///                   key and nothing else, exactly as the comparison design clears the value
///                   before it logs the item.
///              n -> LIVE, value is the next n-2 bytes
/// ```
///
/// TAG 1 WAS ALREADY UNREACHABLE FROM THE ENCODER, which is what makes this free rather than a
/// widening. An empty inline value would be tag 1, and `suffix_offset_of(key, b"")` always answers
/// `Some(key.len())` -- the empty slice is the tail that starts at the key's end -- so the encoder
/// has never emitted it and `an_empty_value_and_a_removal_of_the_same_element_are_different_bytes` has driven that
/// since #2022. So a live suffix item is byte-identical between the two shapes, and a live inline
/// item is the same width except where `n - 1` and `n - 2` fall either side of a varint boundary.
/// `a_live_suffix_block_is_the_same_bytes_under_either_shape` prices both against the first.
///
/// # WHY EVERY NEW PAGE TAKES IT, INCLUDING ONE WITH NO REMOVAL IN IT
///
/// The cheaper-looking alternative is to write v1 while a page has no tombstone and v2 only when it
/// does. That makes "a page is v1 if and only if it holds no removal" an invariant, and nothing
/// would enforce it -- a writer that took the wrong shape for its contents would produce a page
/// whose tombstone decodes as an empty live value, which is the resurrection this whole stage
/// exists to close, reintroduced by a branch. One writer, one shape, and the cost is priced above.
pub(super) const CONTAINER_BLOCK_MAGIC_V2: &[u8] = b"TSCPG2\n";

/// The magic, the spelling byte, and the smallest possible item count.
const CONTAINER_BLOCK_HEADER_BYTES: usize = CONTAINER_BLOCK_MAGIC_V2.len() + 1 + 1;

/// Which shape a frame's magic named, because the value tag means different things in each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContainerBlockShape {
    /// `TSCPG1\n`. Every item is live; the shape cannot say otherwise.
    LiveOnly,
    /// `TSCPG2\n`. An item may be a removal.
    WithRemovals,
}

/// One element of a container page: the key that names it, the value it holds, and whether the item
/// is the RECORD OF ITS REMOVAL rather than its value.
///
/// A removed item's `value` is EMPTY and carries no information -- the encoder writes no value bytes
/// for it and the decoder does not invent any. Reading the value of an item whose `deleted` is set
/// is therefore always a mistake, and `deleted` is checked before `value` at every use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContainerBlockItem {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
    /// This item says its element is GONE, not what it holds.
    pub(super) deleted: bool,
}

/// What a payload turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ContainerBlockDecode {
    /// No magic. The payload is one value and the index entry beside it names the element -- which
    /// is every page written before this module, and every page of a kind that is its whole object.
    NotFramed,
    /// A frame, and these are its items in written order.
    Framed {
        spelling: ElementKeySpelling,
        /// Which magic it carried. A `LiveOnly` page cannot hold a removal, so a derivation reading
        /// one knows its silence about an element means nothing rather than meaning present.
        shape: ContainerBlockShape,
        items: Vec<ContainerBlockItem>,
    },
    /// The magic is there and what follows it is not a frame. NEVER treated as `NotFramed`: the
    /// bytes claim to be a frame, so handing them back as a value would serve framing bytes as
    /// data.
    Corrupt(String),
}

/// What asking a payload for one of its elements turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ContainerElementRead {
    /// No magic, so the whole payload is the value the caller asked for.
    NotFramed,
    /// The frame holds this element and this is its value.
    Found(Vec<u8>),
    /// The frame is well formed and does NOT hold this element. Distinct from `Corrupt` on purpose:
    /// this is an answer, and a missing element is a legitimate one.
    Absent,
    /// The frame holds this element's REMOVAL. Distinct from `Absent` because the two are different
    /// facts and only one of them is durable: `Absent` says this page never mentioned the element,
    /// so an older page still may; `Removed` says this page states it is gone, which is what makes a
    /// page-derived membership possible at all. A reader that only wants a value treats both as no
    /// value, and the read funnel does; a derivation must not, and
    /// `an_older_block_is_not_outranked_by_a_block_that_merely_does_not_mention_the_element` drives
    /// exactly that difference.
    Removed,
    /// The frame could not be walked.
    Corrupt(String),
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// How many bytes `value` occupies as a varint, so a test can state a width from the data.
pub(super) fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn take_varint(bytes: &[u8], cursor: &mut usize, what: &str) -> Result<u64, String> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| format!("container page truncated reading {what}"))?;
        *cursor += 1;
        if shift >= 64 {
            return Err(format!("container page {what} varint is wider than 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn take_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    what: &str,
) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| format!("container page {what} length overflows"))?;
    let slice = bytes
