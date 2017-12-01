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
