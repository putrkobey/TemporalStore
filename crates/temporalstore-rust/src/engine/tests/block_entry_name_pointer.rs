// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW WIDE IS THE POINTER TO A NAME -- SUPERSEDED, AND KEPT AS THE RECORD OF WHY.
//!
//! # THE SUBJECT OF THIS MODULE NO LONGER EXISTS
//!
//! It asked how wide the pointer to a name on `BlockIndex` has to be, and priced narrowing it.
//! THE NAME DID NOT GET NARROWER -- IT LEFT. The entry carries no identity of its own; the object
//! key lives once per object in `CoreIndex::object_block_lookup` and every reader resolves it from
//! the POSITION the page is filed at, through `BlockKeysByPosition`. There is no name slot on the
//! entry to make one word wide.
//!
//! SO ITS FOUR PRICING ARMS ARE RETIRED RATHER THAN RE-NUMBERED, and that distinction is the whole
//! of this note. A proposal whose subject is gone does not get new numbers: every one of its
//! savings was `size_of::<BlockIndex>() - size_of::<some mirror>()`, and with the entry at
//! twenty-four and the thin-pointer mirror at thirty-two those subtractions do not merely report a
//! smaller win, THEY UNDERFLOW. A re-numbered version of this module would be arithmetic about a
//! structure this tree does not contain, which is the exact failure its own step-zero cousins
//! recorded twice.
//!
//! WHAT WENT: `one_remaining_name_slot_takes_the_entry_from_forty_to_thirty_two_and_the_chunk_
//! class_refuses_it`, `what_a_one_word_name_slot_is_worth_on_the_chunk_column`,
//! `the_object_ordinal_and_the_packing_do_not_follow_and_here_is_what_they_would_have_been`, and
//! `the_only_thin_pointer_that_is_safe_to_build_here_costs_more_than_the_entry_saves`, with the ten
//! instruments and eight mirror types that served only them. THE COMPILER ENUMERATED THE
//! INSTRUMENTS; the mirrors it could not, because `#[allow(dead_code)]` sat on every one of them
//! and hid eight orphans behind an exemption. They were found by counting references by name.
//!
//! AND ONE OF THE FOUR WAS ALREADY FALSE, which is worth recording because nothing said so.
//! `what_a_one_word_name_slot_is_worth_on_the_chunk_column` is `#[ignore]`d and asserted
//! `entry_now - entry_no_key == size_of::<Arc<str>>()` over mirrors that had drifted to the same
//! width -- a comparison of a value with itself, in a test nothing ran. Its own comment records
//! that it was "CORRECTED FROM 48" for the same reason once before.
//!
//! # WHAT SURVIVES, AND WHY EACH STILL HAS A SUBJECT
//!
//!   * `an_arc_str_allocation_is_two_header_words_and_the_characters` -- a fact about `Arc<str>`,
//!     which is what the lookup now holds one of per object. The sixteen bytes did not leave the
//!     process when they left the entry, and this is where that allocation is priced.
//!   * `an_entry_names_its_object_from_its_own_terms` -- the id is derived from the identity beside
//!     the page and the shard, with no stored copy anywhere.
//!   * `a_packed_entry_reads_its_address_by_value_because_a_borrow_of_it_is_undefined` -- a
//!     soundness fact about `repr(packed)` that holds whatever the entry's field set is, and the
//!     reason `entry_width_at_each_step`'s packing row is a measurement and not a recommendation.
//!
//! # AND THE ROUTE THAT LANDED IS NEITHER OF THE TWO THIS MODULE MEASURED
//!
//! Its verdict table listed "step 1b  the key moved to the bucket's object list -- REFUTED", and a
//! reader could mistake that for a refutation of what shipped. IT IS NOT, on both of 1b's grounds:
//!
//!   * ON BYTES, 1b was `+8.11 to +9.10 a page on routed keys` because the OBJECT LIST grows by a
//!     key for every object it names. The key went to `object_block_lookup`, which already held one
//!     entry per (kind, object) before this change and still holds exactly one: the per-object key
//!     is not a new cost, it is the cost that was already there, and what left is the per-PAGE copy.
//!   * ON IDENTITY, 1b was refuted because an entry's only remaining handle on its object would be
//!     `BlockAddress::object_id` -- an `Option` answered as zero when absent. No reader resolves an
//!     identity that way now: they resolve it by POSITION through the lookup's inversion, which is
//!     a source 1b did not have and did not consider.
//!
//! THIS IS NOT A CLAIM THAT 1b WAS WRONG. It is a claim that 1b is a different proposal, and that
//! the next reader who finds "REFUTED" beside something that looks like what shipped should read
//! this paragraph before concluding the tree contradicts itself.
//!
//! # WHAT THE THREE MERGED REFUTATIONS ANSWERED, kept because the history is the argument
//!
//! THIS IS NOT THE QUESTION THE THREE MERGED REFUTATIONS ANSWERED, and saying so was the first job
//! of this module because the next reader would otherwise close it unread.
//!
//!   * #1986 asked whether the entry needs `object_key` AT ALL and found it does: 47 production
//!     lines, twelve of them class 2 -- they need the CHARACTERS -- and class 3 empty.
//!   * #1976 / #1985 asked whether `component` could be an ordinal and found it cannot: the
//!     ordinal does not exist for `zset`, `set`, `list` or `hash`.
//!   * #1986 also priced an interning HANDLE and declined it on arithmetic: a table entry at
//!     54.02 B resident, break-even 3.38 pages an object against a mixed store's 1.98.
//!
//! All three are about WHAT THE FIELD MEANS. This module is about HOW WIDE THE POINTER TO IT IS.
//! `Arc<str>` is a FAT pointer -- a data pointer and a length, side by side -- so each of the two
//! name fields costs two words to point at one allocation. A THIN pointer keeps the characters at
//! the same address, reachable by every one of those twelve class-2 readers with no table, no
//! handle, no map and no ordinal; only the length moves, from beside the pointer into the
//! allocation it points at. Nothing any of the three refutations established is reopened.
//!
//! WHAT THIS MODULE DECIDES, as six verdicts, because four of them are DEPENDENT and reporting them
//! as parallel options would overstate every one.
//!
//! AND IT IS THE STRIDE THAT IS REPORTED, NOT THE STRUCT. `engine_storage_state.rs` asserts that `BlockIndexMap`
//! IS its list -- `size_of::<BlockIndexMap>() == size_of::<Vec<(u64, BlockIndex)>>()` -- so what a
//! page costs in memory is `size_of::<(u64, BlockIndex)>()`, and that re-aligns to eight whatever
//! the entry's own alignment is. Measuring the struct instead of the stride is exactly how a step
//! worth nothing reads as a win:
//!
//!                                                              entry  stride
//!   today                                                         64      72
//!   step 1a  two one-word name slots (thin pointer)               48      56   measured, DECLINED
//!   step 1b  the key moved to the bucket's object list            48      56   measured, REFUTED
//!   step 2   an eight-bit object ordinal, needs 1b                40      48   does not follow
//!   step 3   an element ordinal for the component (another tree's) 24     32   does not follow
//!   step 4   the handle folded out of the list                    24      24   does not follow
//!   step 5   `repr(packed)`, needs 3                              22      32   WORTH NOTHING
//!
//! STEP 5 IS WORTH NOTHING AND THAT IS A MEASURED CORRECTION, not a judgement. An 18-byte entry in a
//! tuple with a `u64` still strides at 32, because the tuple's alignment is the `u64`'s. Packing
//! would spend an unaligned read on every address load, and undefined behaviour on every existing
//! `&entry.address`, to save nothing at all in the list.
//!
//! STEP 1a REACHES 48 AND WINS ON EVERY ARM MEASURED: -16.00 B a page on six of the eight and
//! -16.61 and -21.59 on the two where the list arm dominates. It wins because the name allocation
//! does not grow -- `Arc<str>` already spends two header words on a strong count and a weak count,
//! and a thin string spends two on a strong count and the LENGTH, and this tree uses `Weak` nowhere.
//! That is the whole mechanism, and the measurement carries the row that proves it: a thin string
//! KEEPING a weak count is only -6.33 to +0.00 a page, so the free ride is the weak count and
//! nothing else.
//!
//! IT IS DECLINED ON SOUNDNESS, NOT ON BYTES. `Arc<str>` cannot be made thin, and the only thin
//! shape that needs no unsafe is `Arc<String>` -- one word in the entry, two allocations per name --
//! which is +17.00 to +32.00 B a page on ALL EIGHT arms. A hand-rolled one would be the first
//! self-managed heap allocation in this crate, whose entire production unsafe surface is four
//! single-expression FFI calls, with no Miri available for the pinned toolchain, no sanitizer job
//! among the five workflows, and a Rust workflow whose test step is `continue-on-error`.
//!
//! STEP 1b REACHES THE SAME 48 AND LOSES ANYWAY, on two separate grounds. On BYTES it is two
//! populations with opposite signs: -15.76 to -15.84 a page on containers, but **+8.11 to +9.10 on
//! routed keys at the shipped 0..=1023 range** and 0.00 at the wide range, because the object list
//! grows by a key for every object and a routed bucket holds forty of them. That is #1994's shape
//! exactly. And on IDENTITY it is refuted outright: with the key off the entry, an entry's only
//! handle on its object is `BlockAddress::object_id`, which is an `Option` answered as ZERO when
//! absent and a 64-bit hash when present. Step 2 needs exactly the id-to-key map 1b would have
//! created, so it falls with it; steps 3 and 5 fall behind step 2.
//!
//! STEP 4 -- the list's `u64` handle -- is the one term that needs no step above it and no format
//! change at all. `block_index_handle` is a `DefaultHasher` over the entry's own `model_id`,
//! `object_key`, `component` and five address fields, so it is a CACHED DERIVATION OF THE ENTRY
//! BESIDE THE ENTRY -- the shape `generation` (#1966) and the component name (#1978) both had when
//! they came out. Nothing persists it: the page index's stored form is a map keyed by
//! `block_index_written_key`'s string, and `CoreIndex::object_block_lookup`, which holds the handle
//! in `BlockLookupRef`, is `#[serde(default, skip_serializing)]` and rebuilt on load. So there is no
//! stored shape to move.
//!
//! IT IS DECLINED ON TWO NUMBERS, both measured rather than argued.
//!
//!   * IT IS THE LIST ARM ONLY, and that is two populations, not one. The single-page arm holds its
//!     handle INSIDE a 24-byte enum whose width is set by the list arm either way, so taking it out
//!     of `One` reclaims nothing. MEASURED: 8.00 B a page on the six arms whose pages sit in lists,
//!     and **0.00 B a page on the two routed arms at 0..=u32::MAX, where all 40,000 buckets hold one
//!     page**. Charging it over every page would credit it with eight bytes where it is worth none --
//!     the same error #1994 caught from the other side, where a change was -324.44 B a bucket at one
//!     range and +0.70 at the other.
//!   * WHAT IT COSTS IS A HASH PER PROBE, counted and not timed. The list is sorted by the handle and
//!     `find_block` bisects it, so a recomputed key hashes `model_id`, the key, the component and five
//!     address fields at every probe of every warm page lookup, against zero today. MEASURED probe
//!     depth: 6 to 7 at the median bucket and up to 9 at the widest, which is **48 to 72 field hashes
//!     a warm page lookup**, and one of the eight is a 24-character string. The ORDER is safe -- the
//!     same function of the same fields gives the same order, so #1964's five readers taking the
//!     ascending walk as given keep it and no explicit sort appears -- and nothing persists the
//!     handle, so there is no stored shape to move. It is the probe count that declines it.
//!
//! SO 64 STANDS, and the reason is written down here rather than left to be re-derived.
//!
//! THE HONEST CEILING. The design that prompted this work holds a 17-byte entry with NO key and NO
//! component: names resolve to small integer ids at its API boundary. Three merged refutations
//! establish our readers need the characters somewhere, so 17 is not reachable without that object
//! model. Our own floor, with every step above taken and the alignment given up, is 22 -- and the
//! 18-byte figure quoted for it omits `model_id` and the three flag bytes, which are 4 of our 22.
#![allow(clippy::all)]
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::alloc_probe::documented_glibc_chunk;
use crate::engine::storage_bucket_internals::StoredModelKind;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// =================================================================================================
// THE INSTRUMENTS.
// =================================================================================================

/// The chunk glibc is documented to serve a request from, checked as a FLOOR rather than trusted as
/// an equality.
///
/// #1969 read a 104-byte request served out of 128. The rule is `max(32, round_up(request + 8, 16))`
/// and `alloc_probe::documented_glibc_chunk` is this tree's statement of it; this wrapper asserts the
/// three properties that make it a floor at every call, so a platform whose rule differs fails here
/// rather than inside a byte total.
fn chunk(request: usize) -> usize {
    let served = documented_glibc_chunk(request);
    assert!(
        served >= request,
        "the chunk rule served {served} B for a {request} B request, which is less than was asked for"
    );
    assert!(
        served > request,
        "the chunk rule served exactly {served} B for a {request} B request; the header word means a \
         served chunk is STRICTLY larger than the request, and an equality here means this is not the \
         rule being modelled"
    );
    assert_eq!(
        0,
        served % 16,
        "the chunk rule served {served} B, which is not a multiple of sixteen"
    );
    served
}

// =================================================================================================
// THE MIRRORS. Declared shapes this engine does not have, so a width can be quoted without
// asserting anything about a structure that does not exist.
// =================================================================================================

/// The slot a packed region needs: the id, an offset into the region, and a present flag.
///
/// A FLAG AND NOT A RESERVED ID VALUE, for the reason the slot array itself gives: object ids are
/// hashes spanning the whole of `u64`, so no value can be spared to mean absent. The flag is what
/// lets the offset land in padding the `Option`'s discriminant word was already wasting.
#[allow(dead_code)]
struct MirrorPackedSlot {
    id: u64,
    offset: u32,
    present: bool,
}

/// The multi-object arm of a packed object index: the slots, the valid count, and the region.
#[allow(dead_code)]
struct MirrorPackedSlots {
    slots: Vec<MirrorPackedSlot>,
    valid: usize,
    region: Vec<u8>,
}

/// The address with an EIGHT-BIT object ordinal in place of the 64-bit hash -- step 2.
///
/// `Copy` on purpose: inside a packed entry a field may not be borrowed, so every read has to take a
/// copy, and `a_packed_entry_reads_its_address_by_value_because_a_borrow_of_it_is_undefined` drives
/// that discipline rather than describing it.
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct MirrorAddressEightBitObject {
    address_word: u64,
    length: u32,
    block_id: u16,
    present: u8,
    object_ordinal: u8,
}

