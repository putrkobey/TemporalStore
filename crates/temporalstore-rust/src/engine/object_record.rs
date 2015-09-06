// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE RECORD PER OBJECT: the address, the deadline, the model and the state in a single
//! twenty-four byte value, so that an object costs one row instead of four to six.
//!
//! # WHY THIS EXISTS
//!
//! A live object in a shard is currently named by several independent key-addressed structures,
//! each of which pays its own tree-or-hash node AND its own copy-or-refcount of the object key's
//! characters. For one plain `string` that is the model map, the page lookup, the dirty index and
//! the page index; with a deadline it is two more. The width of any one of those rows is not the
//! cost -- the COUNT of them is, because the key copy multiplies by it.
//!
//! This type is the single row those collapse into: one address, one deadline, one model id and
//! one set of state bits. It is deliberately a VALUE and not a map, so that whatever holds it
//! decides the key. That is what makes it independent of the integer-key work in flight elsewhere:
//! held in a map keyed by the object key's characters it saves the SIBLING rows today, and held in
//! a map keyed by an assigned integer it additionally saves the characters. Neither is assumed
//! here.
//!
//! # WHAT IT DOES NOT REPLACE, AND WHY THAT IS NOT A SHORTFALL
//!
//! Two of the structures it sits beside answer ITERATION questions rather than point questions,
//! and an unordered per-object row cannot answer those at all:
//!
//!   * a deadline index ordered BY DEADLINE answers "which object expires next". This record
//!     answers "when does THIS object expire". Folding the first into the second replaces a
//!     prefix walk with a shard scan, so the ordered index survives and what this removes is its
//!     DUPLICATE of the deadline value, not its ordering.
//!   * a dirty index grouped BY BUCKET answers "which objects in this bucket are dirty", which a
//!     per-object bit cannot answer without visiting every object. That grouping survives too.
//!
//! Both are stated here rather than discovered later, because a merge that destroys an ordering is
//! a correctness loss wearing a saving's clothes.
//!
//! # THE TWO TAGGED WORDS ARE THE DESIGN
//!
//! The deadline word and the address word are each EITHER a value OR a handle naming a side
//! structure, and which one is decided by a TAG BIT, never by the value. That is the same
//! conclusion [`crate::block_store::ElementEntry`] already reached for its own presence byte, for
//! the same reason it wrote down: zero is a legal block id, so "zero means absent" would silently
//! erase a real first page. A deadline of zero and an address word of zero are both legal, so
//! neither can carry its own absence.
//!
//! The tags are enforced STRUCTURALLY and not by convention. Both words are private and
//! unreachable except through [`ObjectRecord::deadline`] and [`ObjectRecord::page_address`], which
//! return a named arm. There is no accessor that hands out a bare word a caller could dereference
//! without having consulted its tag -- that shape is how a handle becomes a wild pointer.
//!
//! # WHY A HANDLE AND NOT A POINTER
//!
//! The shape this follows spends a real heap pointer in the pointer arm. This holds an opaque
//! handle into a side table instead, for two reasons that are both about this codebase rather than
//! about taste. The record is DURABLE -- it is the value of a map that rides the shard index
//! snapshot -- and a heap pointer cannot be serialized, so a pointer arm would have to be rebuilt
//! on every load from something else, which is a second source of truth. And a handle cannot be
//! dereferenced by accident: the side table either holds it or refuses, where a stale pointer
//! reads whatever now lives there.
//!
//! # WHY NOT `repr(packed)`
//!
//! The shape this follows reaches twenty-four bytes with a packing attribute. Reproducing that
//! here would be a measured mistake: an earlier leg of this campaign priced `repr(packed)` on the
//! neighbouring index types at roughly 390 `E0793` unaligned-reference sites, and it buys nothing
//! where the field order already leaves no interior padding. The field order below reaches the
//! same twenty-four bytes at natural alignment, with one byte of TAIL slack and no packing, and
//! the width and the alignment are pinned SEPARATELY below.

use serde::{Deserialize, Serialize};

use super::storage_bucket_internals::StoredModelKind;
use crate::block_store::{
    extract_block_offset, extract_block_slab_id, make_block_address_word, ElementEntry,
};

// =================================================================================================
// THE FLAGS
// =================================================================================================

/// Eleven named bits in sixteen, five reserved.
///
/// # THE BIT LIST, AND THE TWO THAT ARE NOT IN THE SHAPE THIS FOLLOWS
///
/// Ten of these are the state a consolidated row carries: two tags, four object-level states and
/// four page-level states. Two more are here because a LOSSLESS conversion needs them, and both
/// were found by writing the conversion test rather than by reading the layout:
///
///   * `DEADLINE_PRESENT` (bit 10). One tag bit gives a word two meanings, and this record needs
///     THREE: absent, a value, or a handle. A sentinel value is refused for the reason stated in
///     the module header, so absence costs a bit. Ten bits cannot represent an absent deadline
///     without one.
///   * `BLOCK_GENERATION` (bit 11). [`ElementEntry`] carries a generation-present bit that is
///     independent of its block-id-present bit ON THE STORED PATH: an index written before the
///     generation existed carries an identity and no generation, and must keep loading that way.
///     Dropping this bit would hand such an entry a generation it never had and move every key it
///     resolves through, which is the defect that type's own doc records being caught by a round
///     trip.
///
/// Four bits remain reserved. A wider flags field would push this structure to thirty-two bytes,
/// so the four are the budget; a decoder REFUSES a reserved bit it finds set rather than ignoring
/// it, so a newer writer's record is rejected by name instead of being read with its meaning
/// dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct RecordFlags(u16);

impl RecordFlags {
    /// The address word is a handle naming a multi-page layout, not a page address.
    pub(super) const ADDRESS_IS_POINTER: u16 = 1 << 0;
    /// The deadline word is a handle naming a list of per-element deadlines, not one deadline.
    // BIT 1 IS RETIRED, into `RESERVED`. It tagged the deadline word, which this record no
    // longer carries.
    /// The object has unflushed writes.
    // BIT 2 IS RETIRED, into `RESERVED`. It marked the row dirty and nothing ever set it: the
    // question the flush path asks is "which objects in THIS BUCKET are dirty", which a
    // per-object bit cannot answer without visiting every object. The bucket-grouped index
    // answers that, so this bit never had a second reader to justify its width.
    /// The object's INDEX-LOG state has unflushed writes, independently of its data.
    ///
    /// NAMED FOR THE SPLIT THIS ENGINE ALREADY MAKES, which is between the write-ahead log and
    /// the index log: a bucket carries `first_dirty_wal_sequence` and
    /// `first_dirty_index_log_sequence` as two separate watermarks, and a round can clear one
    /// while the other still pins its log. `DIRTY` above is the data side of that split and this
    /// is the index side, so the pair reads as the engine's own two watermarks rather than as a
    /// generic notion of metadata.
    pub(super) const INDEX_DIRTY: u16 = 1 << 3;
    /// A load for this object is in flight.
    pub(super) const LOADING: u16 = 1 << 4;
    /// The object's bytes are resident.
    pub(super) const IN_MEMORY: u16 = 1 << 5;
    /// The page named by the address word has unflushed writes.
    pub(super) const BLOCK_DIRTY: u16 = 1 << 6;
    /// The page named by the address word is a tombstone.
    pub(super) const BLOCK_DELETED: u16 = 1 << 7;
    /// The page's only durable copy is an operation-log record.
    pub(super) const BLOCK_IN_LOG: u16 = 1 << 8;
    //
    // BIT 9 IS A HOLE, AND IT IS THE RECORD OF A BIT REMOVED AS REDUNDANT.
    //
    // It held a second flag whose documented meaning was "the page is deleted" -- which is
    // exactly what `BLOCK_DELETED` at bit 7 already says. Two bits for one fact is worse than
    // either of them alone, because nothing stops them disagreeing and then no reader knows
    // which to believe. So the fact keeps one bit and the other is gone.
    //
    // THE HOLE IS KEPT RATHER THAN RENUMBERED, following the same reasoning `ElementEntry`'s
    // presence byte gives for its own holes: nothing depends on the numbering being dense, and a
    // hole is cheaper to read than a renumbering. Here it also costs nothing, because bit 9 falls
    // into `RESERVED` below -- so a writer that sets the retired bit is refused by name as a
    // newer writer, which is the correct answer for a bit this reader no longer understands.
    //
    // BIT 10 IS RETIRED, into `RESERVED`. It said a deadline was present; the record carries no
    // deadline now, and a per-key deadline lives in its own structure where a sweep can walk
    // due keys in DEADLINE order rather than scanning every row to find them.
    /// The page carries a generation, independently of whether it carries an identity.
    pub(super) const BLOCK_GENERATION: u16 = 1 << 11;

    /// Bits 1, 2, 9, 10 and 12..=15. Set in a decoded record, these mean a writer newer than
    /// this reader -- 1, 2, 9 and 10 because they were retired, the rest because they were never
    /// spent. A decoder REFUSES any of them rather than reading a record with their meaning
    /// dropped.
    ///
    /// A RETIRED BIT JOINS THIS SET RATHER THAN BEING RENUMBERED AWAY, which is the convention bit
    /// 9 already established and the whole reason retiring one is safe: a record carrying it is
    /// refused by name instead of read as though the bit meant nothing. 0xF200 | 0x0406.
    pub(super) const RESERVED: u16 = 0xF606;

    /// Every bit this reader understands.
    pub(super) const KNOWN: u16 = !Self::RESERVED;

    pub(super) const fn empty() -> Self {
        Self(0)
    }

    pub(super) const fn bits(self) -> u16 {
        self.0
    }

    pub(super) const fn get(self, bit: u16) -> bool {
        self.0 & bit != 0
    }

    pub(super) fn set(&mut self, bit: u16, on: bool) {
        if on {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }

    #[must_use]
    pub(super) const fn with(self, bit: u16, on: bool) -> Self {
        Self(if on { self.0 | bit } else { self.0 & !bit })
    }
}

/// EIGHT NAMED BITS OF SIXTEEN, and eight refused. Three retired with the deadline word and the
/// dirty bit, and a retired bit joins `RESERVED` rather than being renumbered away -- so the two
/// counts move together, which the third assertion is here to state.
const _: () = assert!(RecordFlags::KNOWN.count_ones() == 8);
const _: () = assert!(RecordFlags::RESERVED.count_ones() == 8);
const _: () = assert!(RecordFlags::KNOWN.count_ones() + RecordFlags::RESERVED.count_ones() == 16);
const _: () = assert!(RecordFlags::KNOWN & RecordFlags::RESERVED == 0);

// =================================================================================================
// THE TWO TAGGED WORDS, AS NAMED ARMS
// =================================================================================================

/// A handle naming the pages of a multi-page object, held outside the record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct BlockLayoutHandle(u64);

impl BlockLayoutHandle {
    pub(super) const fn from_stored(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn get(self) -> u64 {
        self.0
    }
}

/// What the address word means. THE TAG DECIDES, NEVER THE VALUE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockAddress {
    /// One page, at this address word: the slab in the high half and the offset in the low.
    One(u64),
    /// Several pages, in a side layout this handle names.
    Many(BlockLayoutHandle),
}

// =================================================================================================
// THE RECORD
// =================================================================================================

/// One object's whole index row: a deadline, an address, a page size, the state bits and a model.
///
/// # THE LAYOUT, AND WHY THE FIELDS ARE IN THIS ORDER
///
/// Five fields, widest first: the two tagged words, then the size, then the bits, then the model.
///
/// WIDEST FIRST IS WHAT REMOVES THE INTERIOR PADDING. The two `u64`s are eight-aligned at offsets
/// 0 and 8, the `u32` is four-aligned at 16, the `u16` is two-aligned at 20, and the `u8` needs no
/// alignment at all. That leaves ONE byte standing empty at offset 23 -- at the TAIL, where it
/// costs nothing and where a field up to a byte wide would still be free. Any other order of the
/// same five fields pays for padding between them instead.
///
/// `size_of` is 24 and `align_of` is 8, both pinned below as separate equalities and both
/// measured rather than argued -- see `the_record_is_twenty_four_bytes_at_natural_alignment`,
/// which prints them. Twenty-three bytes of field in twenty-four, with no packing attribute: see
/// the module header for why reproducing one was refused on measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ObjectRecordWire", into = "ObjectRecordWire")]
pub(super) struct ObjectRecord {
    /// Value or handle. UNREADABLE except through [`ObjectRecord::deadline`].
    /// Value or handle. UNREADABLE except through [`ObjectRecord::page_address`].
    address: u64,
    page_size: u32,
    flags: RecordFlags,
    model_id: u8,
    /// The object's UNCOMPRESSED size, as a one-byte class. 0 means "not known".
    logical_size_class: u8,
}

/// THE WIDTH, PINNED ON ITS OWN.
///
/// A width pin dominates any neighbour inequality -- `!= 23 && != 25` passes at every width and
/// every alignment, which is an assertion that can only ever fail inside a test nothing runs. So
/// this is an equality, and the alignment is a SECOND equality rather than a clause of this one: a
/// change that keeps the width and moves the alignment is a different change, and each should name
/// itself.
const _: () = assert!(std::mem::size_of::<ObjectRecord>() == 16);
const _: () = assert!(std::mem::align_of::<ObjectRecord>() == 8);

/// The field sum, so a field added inside the tail slack is visible as arithmetic rather than
/// hiding behind a width that did not step.
const _: () = assert!(
    std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<RecordFlags>()
        + std::mem::size_of::<u8>()
        + std::mem::size_of::<u8>()
        == 16
);
/// THE TAIL SLACK IS NOW SPENT, AND THAT IS THE POINT. It was one byte, pinned so that a field
/// added inside it would show up as arithmetic rather than hide behind a width that did not step.
/// `logical_size_class` is that field: the sum above goes 15 -> 16 and the width does NOT move, so
/// the record still costs 16 bytes and the class is free. A further field now costs eight.
const _: () = assert!(std::mem::size_of::<ObjectRecord>() - 16 == 0);

impl Default for ObjectRecord {
    fn default() -> Self {
        Self {
            address: 0,
            page_size: 0,
            flags: RecordFlags::empty(),
            model_id: 0,
            logical_size_class: 0,
        }
    }
}

impl ObjectRecord {
    // --- The tagged words, as arms. ---



    /// The address, decided by its tag bit and not by its value.
    pub(super) fn page_address(&self) -> BlockAddress {
        if self.flags.get(RecordFlags::ADDRESS_IS_POINTER) {
            BlockAddress::Many(BlockLayoutHandle::from_stored(self.address))
        } else {
            BlockAddress::One(self.address)
        }
    }

    /// Set the address and its tag bit TOGETHER, so the two cannot disagree.
    pub(super) fn set_block_address(&mut self, address: BlockAddress) {
        match address {
            BlockAddress::One(word) => {
                self.address = word;
                self.flags.set(RecordFlags::ADDRESS_IS_POINTER, false);
            }
            BlockAddress::Many(handle) => {
                self.address = handle.get();
                self.flags.set(RecordFlags::ADDRESS_IS_POINTER, true);
            }
        }
    }

    // --- The plain fields. ---

    pub(super) const fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn set_block_size(&mut self, value: u32) {
        self.page_size = value;
    }

    /// The object's uncompressed size as this record knows it, or `None` when it carries no class.
    ///
    /// `None` is not zero: a record written before the class existed, or by a path that never had
    /// the writer's length, reports nothing rather than reporting an object of no size. The
    /// published `live_objects_without_size_class` counts them, so a partial total is visible as
    /// partial instead of reading as a small one.
    pub(super) fn logical_size(&self) -> Option<u64> {
        declass_logical_size(self.logical_size_class)
    }

    /// Record the object's uncompressed size, rounded to the nearest class.
    ///
    /// Returns the size the record held before, so a caller maintaining a live total can subtract
    /// the displaced value in the same statement that installs the new one -- the two cannot
    /// disagree about what left.
    pub(super) fn set_logical_size(&mut self, len: u64) -> Option<u64> {
        let previous = self.logical_size();
        self.logical_size_class = classify_logical_size(len);
        previous
    }

    pub(super) const fn model_id(&self) -> u8 {
        self.model_id
    }

    pub(super) fn set_model_id(&mut self, value: u8) {
        self.model_id = value;
    }

    pub(super) const fn flags(&self) -> RecordFlags {
        self.flags
    }

    pub(super) fn set_flag(&mut self, bit: u16, on: bool) {
        debug_assert_eq!(
            0,
            bit & RecordFlags::RESERVED,
            "a reserved flag bit cannot be set through this door; name it in RecordFlags first"
        );
        self.flags.set(bit, on);
    }


    // --- THE SHIMS THE MODEL MAPS HOLD THIS THROUGH. ---

    /// The address this record holds, as the holder every container stores.
    ///
    /// This exists so a map whose value became a record does not move its call sites: a reader
    /// that had `map.get(key)` and wanted an address still gets one, in one call.
    ///
    /// # IT PANICS ON THE HANDLE ARM, AND THAT IS DELIBERATE
    ///
    /// A record whose address word is a handle names a multi-page layout, and there is no single
    /// address to hand back -- the layout holds them. Returning a DEFAULT address instead would
    /// resolve the key to slab 0, offset 0, which is a perfectly legal address for a different
    /// block, and a read that followed it would return the wrong bytes with no error anywhere.
    /// That is the same argument [`ElementEntry::set_block_id`] makes for panicking rather than
    /// narrowing, and the same one [`crate::block_store::make_block_address_word`] makes for
    /// checking rather than saturating.
    ///
    /// It is unreachable for the maps that hold this today. `strings` and `control_state_blocks`
    /// store exactly ONE address per object -- that is what their `ElementEntry` value meant --
    /// so every record in them is in the inline arm by construction, and
    /// `every_record_in_a_single_address_map_is_in_the_inline_arm` holds that rather than trusting
    /// it. A caller that genuinely might hold a layout should match on
    /// [`ObjectRecord::page_address`] instead, which cannot panic.
    pub(super) fn address(&self) -> ElementEntry {
        match self.try_address() {
            Ok(address) => address,
            Err(refusal) => panic!(
                "a single-address map held a record with no single address: {refusal}. Match on \
                 `page_address()` instead if this caller can hold a multi-page layout."
            ),
        }
    }

    /// The same, as a `Result`, for a caller that cannot assume the inline arm.
    pub(super) fn try_address(&self) -> Result<ElementEntry, ObjectRecordRefusal> {
        let word = match self.page_address() {
            BlockAddress::One(word) => word,
            BlockAddress::Many(handle) => {
                return Err(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })
            }
        };
        ElementEntry::try_from_wire_parts(
            u64::from(extract_block_slab_id(word)),
            u64::from(extract_block_offset(word)),
            u64::from(self.page_size),
            (!self.flags.get(RecordFlags::BLOCK_IN_LOG)).then_some(0u64),
            self.flags.get(RecordFlags::BLOCK_GENERATION),
        )
        .map_err(|_| ObjectRecordRefusal::AddressWordOutOfRange { word })
    }

    /// Replace ONLY the address, leaving every other field of the record where it was.
    ///
    /// # WHY THIS IS A NARROW DOOR RATHER THAN A WHOLE-RECORD ASSIGNMENT
    ///
    /// The compactor rewrites addresses in place: it takes `&mut ElementEntry` and moves blocks
    /// between slabs. A map holding records has to serve that by materialising an address, letting
    /// it be rewritten, and writing it back -- and the obvious way to write it back is to build a
    /// fresh record from the new address. THAT SILENTLY DROPS EVERYTHING ELSE: the deadline, the
    /// dirty bits, the model id.
    ///
    /// And it would not fail today. The deadline and dirty slots are present and unused on the
    /// maps that hold this, so a fresh-record write-back passes every test there is, and breaks
    /// later, in a different change, when something starts filling them -- with nothing pointing
    /// back here.
    ///
    /// So the write-back door is this one, and it is narrow BY CONSTRUCTION rather than by a
    /// caller remembering: it writes the address word, the page size, and the two bits that are
    /// part of what an address MEANS (whether an ordinal is present, and whether a generation is).
    /// It cannot reach the deadline word, the deadline tags, `DIRTY`, `INDEX_DIRTY`, `LOADING`,
    /// `IN_MEMORY`, `BLOCK_DIRTY`, `BLOCK_DELETED`, or the model id. A careless caller gets the
    /// right answer.
    ///
    /// `compacting_a_record_keeps_everything_that_is_not_the_address` is the guard, and it asserts
    /// the surviving state and the tracking state SEPARATELY -- because a test that compared whole
    /// records would fail for the wrong reason, and a test that asserted "nothing changed" would
    /// be wrong about the two bits that are supposed to change.
    pub(super) fn set_address(&mut self, address: &ElementEntry) {
        self.set_block_address(BlockAddress::One(address.address_word()));
        // NOT SATURATED. `ElementEntry::length` is a `u32` field and its accessor widens, so a
        // value that does not fit here cannot exist -- and if one ever does, the honest answer is
        // to say so rather than to record a shorter page than the one that was written. A
        // saturated length reads as broken; a truncated one reads as a valid shorter block.
        self.page_size = u32::try_from(address.length()).unwrap_or_else(|_| {
            panic!(
                "an address length of {} does not fit the record's page size, which cannot happen \
                 while `ElementEntry` holds its length in a u32",
                address.length()
            )
        });
        self.flags.set(RecordFlags::BLOCK_IN_LOG, address.block_id().is_none());
        self.flags.set(RecordFlags::BLOCK_GENERATION, address.generation().is_some());
        // COUNTED, BECAUSE THE VALUE IS DROPPED HERE AND NOTHING ELSE SAYS SO.
        //
        // The inline arm keeps the ordinal's PRESENCE, set just above, and has no room for its
        // value -- correct for a single-page object, which has page zero by construction. A
        // NONZERO ordinal is therefore silently lost at this line, while the conversion path
        // refuses the same condition by name as `NonZeroBlockOrdinalDoesNotFitInline`. Two halves
        // of one contract disagreeing, and no test fed either path a nonzero ordinal, so nothing
        // could tell.
        //
        // Not made fatal and not made to skip: the destination for a multi-page object does not
        // exist yet, so a fatal refusal would make a recovery path unopenable and a skip would
        // drop the page's address -- the same loss by another mechanism. See
        // `dropped_nonzero_block_ordinals` for the whole argument, and `try_set_address` for the
        // refusing door the callers move to once such an object has somewhere to go.
        if address.block_id().is_some_and(|block_id| block_id != 0) {
            DROPPED_NONZERO_BLOCK_ORDINALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // --- THE CONVERSION. This is what proves the merge is real. ---

    /// Build the record that stands in for one object's separate rows.
    ///
    /// `address` is the row the model map holds today, `deadline` the row the deadline map holds,
    /// and `dirty` the row the dirty index holds. The model id is the fact the map's identity
    /// carries today.
    ///
    /// # WHAT IT REFUSES, AND WHY THE MEASUREMENT CHANGED THE ANSWER
    ///
    /// An [`ElementEntry`] carries a sixteen-bit page ordinal beside its address word, and the
    /// inline arm has no room for sixteen more bits: twenty-three of twenty-four bytes are spoken
    /// for and two more would push the structure to thirty-two. The first draft of this function
    /// therefore refused EVERY entry carrying an ordinal -- and measured against a real store that
    /// was 400 of 400 live string addresses, which would have sent every object through the
    /// side-allocating arm and made this record cost memory rather than save it.
    ///
    /// THE ORDINAL'S VALUE IS ZERO FOR A SINGLE-PAGE OBJECT, which is what the measurement
    /// actually said: 400 of 400 carried ordinal 0, the largest seen being 0. The ordinal names
    /// WHICH PAGE of the object this is, so an object with one page has page zero by construction.
    /// What the inline arm needs is the ordinal's PRESENCE -- a bit it already spends as
    /// `BLOCK_IN_LOG`, since an absent ordinal is exactly what log-residence means -- and not its
    /// value.
    ///
    /// So a NONZERO ordinal is what is refused, and a nonzero ordinal means a multi-page object,
    /// which belongs in the layout arm anyway: [`ObjectRecord::from_object_rows_with_layout`]
    /// holds it exactly. The refusal is an `Err` and not a silent drop, because a dropped page
    /// ordinal resolves a key to a different page of the same object and reads the wrong bytes
    /// with no error anywhere.
    pub(super) fn from_object_rows(
        address: &ElementEntry,
        model_id: u8,
        deadline: Option<u64>,
        dirty: bool,
    ) -> Result<Self, ObjectRecordRefusal> {
        let block_id = address.block_id();
        if let Some(ordinal) = block_id {
            if ordinal != 0 {
                return Err(ObjectRecordRefusal::NonZeroBlockOrdinalDoesNotFitInline {
                    block_id: ordinal,
                });
            }
        }
        let length = address.length();
        let page_size = u32::try_from(length)
            .map_err(|_| ObjectRecordRefusal::BlockSizeDoesNotFit { length })?;

        let mut record = Self {
            address: 0,
            page_size,
            flags: RecordFlags::empty(),
            model_id,
            // NOT KNOWN HERE. This layer is handed a stored address, whose `length()` is the
            // COMPRESSED size; the uncompressed size is the writer's and reaches the record
            // through `set_logical_size`. 0 is the honest value until it does, and
            // `live_objects_without_size_class` counts the records still carrying it.
            logical_size_class: 0,
        };
        record.set_block_address(BlockAddress::One(address.address_word()));
        // `block_id()` is `None` exactly while the block lives in the log, which is the one
        // derivation this type keeps rather than restates: see `ElementEntry::log_resident`. So
        // this ONE BIT carries the ordinal's presence, and the inline arm's ordinal value is zero.
        record.flags.set(RecordFlags::BLOCK_IN_LOG, block_id.is_none());
        record.flags.set(RecordFlags::BLOCK_GENERATION, address.generation().is_some());
        Ok(record)
    }

    /// The same, for an entry whose page identity the inline arm cannot hold.
    ///
    /// Returns the record -- whose address word is a HANDLE -- and the layout the handle names.
    /// This arm is lossless for any entry, at the cost of the side allocation the handle points
    /// into.
    pub(super) fn from_object_rows_with_layout(
        address: &ElementEntry,
        model_id: u8,
        deadline: Option<u64>,
        dirty: bool,
        handle: BlockLayoutHandle,
    ) -> Result<(Self, BlockLayout), ObjectRecordRefusal> {
        let length = address.length();
        let page_size = u32::try_from(length)
            .map_err(|_| ObjectRecordRefusal::BlockSizeDoesNotFit { length })?;
        let block_id = match address.block_id() {
            None => None,
            Some(id) => Some(
                u16::try_from(id)
                    .map_err(|_| ObjectRecordRefusal::BlockIdentityOutOfRange { block_id: id })?,
            ),
        };
        let layout = BlockLayout {
            blocks: vec![StoredBlock {
                address: address.address_word(),
                length: page_size,
                block_id: block_id.unwrap_or_default(),
                has_block_id: block_id.is_some(),
                has_generation: address.generation().is_some(),
            }],
        };
        let mut record = Self {
            address: 0,
            page_size,
            flags: RecordFlags::empty(),
            model_id,
            // NOT KNOWN HERE. This layer is handed a stored address, whose `length()` is the
            // COMPRESSED size; the uncompressed size is the writer's and reaches the record
            // through `set_logical_size`. 0 is the honest value until it does, and
            // `live_objects_without_size_class` counts the records still carrying it.
            logical_size_class: 0,
        };
        record.set_block_address(BlockAddress::Many(handle));
        record.flags.set(RecordFlags::BLOCK_IN_LOG, block_id.is_none());
        record.flags.set(RecordFlags::BLOCK_GENERATION, address.generation().is_some());
        Ok((record, layout))
    }

    /// Put the separate rows back, so the conversion can be shown to lose nothing.
    ///
    /// `layout` is consulted only when the address word is a handle, and its ABSENCE in that case
    /// is refused by name rather than papered over with a default address.
    pub(super) fn into_object_rows(
        &self,
        layout: Option<&BlockLayout>,
    ) -> Result<(ElementEntry, u8), ObjectRecordRefusal> {
        let (word, length, block_id, has_generation) = match self.page_address() {
            // THE INLINE ARM'S ORDINAL IS ZERO WHEN IT IS PRESENT AT ALL, and `BLOCK_IN_LOG`
            // carries that presence: set means no ordinal, clear means ordinal zero. See
            // `from_object_rows` for the measurement that settled this.
            BlockAddress::One(word) => (
                word,
                u64::from(self.page_size),
                (!self.flags.get(RecordFlags::BLOCK_IN_LOG)).then_some(0u64),
                self.flags.get(RecordFlags::BLOCK_GENERATION),
            ),
            BlockAddress::Many(handle) => {
                let layout =
                    layout.ok_or(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })?;
                let block = layout
                    .blocks
                    .first()
                    .ok_or(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })?;
                (
                    block.address,
                    u64::from(block.length),
                    block.has_block_id.then_some(u64::from(block.block_id)),
                    block.has_generation,
                )
            }
        };
        let entry = ElementEntry::try_from_wire_parts(
            u64::from(extract_block_slab_id(word)),
            u64::from(extract_block_offset(word)),
            length,
            block_id,
            has_generation,
        )
        .map_err(|_| ObjectRecordRefusal::AddressWordOutOfRange { word })?;
        Ok((entry, self.model_id))
    }
}

/// The pages of a multi-page object, named by a record whose address word is a handle.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BlockLayout {
    #[serde(rename = "blocks", alias = "pages")]
    blocks: Vec<StoredBlock>,
}

impl BlockLayout {
    pub(super) fn len(&self) -> usize {
        self.blocks.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

/// One page inside a layout: everything the inline arm cannot hold, held exactly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredBlock {
    address: u64,
    length: u32,
    block_id: u16,
    has_block_id: bool,
    has_generation: bool,
}

// =================================================================================================
// THE LAYOUTS A HANDLE NAMES
// =================================================================================================

/// Where the pages of a multi-page object actually live.
///
/// THIS IS THE PIECE WHOSE ABSENCE MADE THE HANDLE ARM DEAD CODE. `BlockAddress::Many` and
/// `from_object_rows_with_layout` have existed and been tested for some time, and
/// `RecordFlags::ADDRESS_IS_POINTER` with them -- but every production mention of all three was
/// inside this file, because a handle is only a number until something maps it to a layout. The
/// record keeps ONE WORD that is either a page address or a handle; this is what the handle half
/// points into, so an object's pages hang off the object's own record instead of being filed in a
/// second index keyed by the object key again.
///
/// # HANDLE ZERO IS NEVER A LAYOUT, AND THAT IS LOAD-BEARING
///
/// `BlockLayoutHandle` derives `Default`, so a zeroed or defaulted handle is `0`. If slot zero
/// backed handle zero, a record that never had a layout would resolve to whatever landed in the
/// first slot. So a slot at index `i` backs handle `i + 1` in the low half, and zero resolves to
/// nothing by construction rather than by a check someone has to remember.
///
/// # A STALE HANDLE FAILS CLOSED RATHER THAN NAMING SOMEONE ELSE'S PAGES
///
/// Vacated slots are reused, which on its own is the ABA problem: free a layout, allocate another
/// into the same slot, and the first handle now reads the second object's pages -- silently, and
/// as a plausible answer. The high half of the handle therefore carries the slot's GENERATION,
/// bumped on every removal, so a handle outliving its layout stops resolving instead of resolving
/// to the wrong thing. A raw pointer cannot express this; a handle can, for the cost of bookkeeping
/// in the half of the word that was going spare.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BlockLayoutStore {
    slots: Vec<LayoutSlot>,
    vacant: Vec<u32>,
    live: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct LayoutSlot {
    generation: u32,
    layout: Option<BlockLayout>,
}

impl BlockLayoutStore {
    /// Split a handle into the slot it names and the generation it was issued at.
    ///
    /// `None` for handle zero, which names no slot -- see the type's doc.
    fn decode(handle: BlockLayoutHandle) -> Option<(usize, u32)> {
        let word = handle.get();
        let low = (word & 0xFFFF_FFFF) as u32;
        let index = low.checked_sub(1)?;
        Some((index as usize, (word >> 32) as u32))
    }

    fn encode(index: usize, generation: u32) -> BlockLayoutHandle {
        let low = u64::from(index as u32 + 1);
        BlockLayoutHandle::from_stored((u64::from(generation) << 32) | low)
    }

    /// File a layout and return the handle a record should carry.
    pub(super) fn insert(&mut self, layout: BlockLayout) -> BlockLayoutHandle {
        self.live += 1;
        match self.vacant.pop() {
            Some(index) => {
                let slot = &mut self.slots[index as usize];
                slot.layout = Some(layout);
                Self::encode(index as usize, slot.generation)
            }
            None => {
                let index = self.slots.len();
                self.slots.push(LayoutSlot { generation: 0, layout: Some(layout) });
                Self::encode(index, 0)
            }
        }
    }

    /// The layout this handle names, or `None` if it names nothing any more.
    pub(super) fn get(&self, handle: BlockLayoutHandle) -> Option<&BlockLayout> {
        let (index, generation) = Self::decode(handle)?;
        let slot = self.slots.get(index)?;
        if slot.generation != generation {
            return None;
        }
        slot.layout.as_ref()
    }

    /// Give up a layout. The slot is reused, and its generation moves so the old handle cannot.
    pub(super) fn remove(&mut self, handle: BlockLayoutHandle) -> Option<BlockLayout> {
        let (index, generation) = Self::decode(handle)?;
        let slot = self.slots.get_mut(index)?;
        if slot.generation != generation {
            return None;
        }
        let layout = slot.layout.take()?;
        // SATURATING, so a slot recycled 2^32 times stops reusing rather than wrapping its
        // generation back onto a handle someone might still hold.
        slot.generation = slot.generation.saturating_add(1);
        if slot.generation != u32::MAX {
            self.vacant.push(index as u32);
        }
        self.live -= 1;
        Some(layout)
    }

    /// How many layouts are filed.
    pub(super) fn len(&self) -> usize {
        self.live
    }

    pub(super) fn is_empty(&self) -> bool {
        self.live == 0
    }
}

// =================================================================================================
// THE WIRE FORM, AND A DECODER THAT REFUSES BY NAME
// =================================================================================================

/// THE SLOT'S STORED SPELLING: a record, and the owned element array when there is one.
///
/// # WHY TWO FIELDS RATHER THAN A FLATTENED ONE
///
/// `#[serde(flatten)]` and `#[serde(untagged)]` both buffer the input before they can decide, which
/// on this type means a `Content` allocation per object on the load path -- the decode walks every
/// slot in the shard. Two named fields decide by key, so nothing is buffered.
///
/// # WHY AN OLD IMAGE IS REFUSED RATHER THAN MISREAD, IN BOTH DIRECTIONS
///
/// The slot's image used to be the bare record's, so it carried the record's own keys and no `r`.
/// Under this form `r` is REQUIRED, so an old image fails to decode -- and a stale stamp already
/// answers "no usable index", the same answer a missing file gets, so the store replays and rebuilds.
///
/// The stamp check is ONE-SIDED (`stored < current`), so a NEWER image is not refused by an older
/// binary. That direction is safe for the mirror-image reason: an older binary reading `{"r": ...}`
/// looks for the record's required fields, does not find them, and refuses. Undecodable also
/// replays. What makes a one-sided check survivable here is that neither direction can decode the
/// other's shape into something plausible.
///
/// `e` is skipped when empty, so the only cost for an element-free slot is the `r` wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectSlotWire {
    /// The fixed-width record. REQUIRED, which is what refuses an image of the older shape.
    r: ObjectRecord,
    /// The owned element array, absent when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    e: Vec<(u64, ElementEntry)>,
}

impl From<ObjectSlot> for ObjectSlotWire {
    fn from(slot: ObjectSlot) -> Self {
        Self {
            r: slot.record,
            e: slot.many.map(|many| *many).unwrap_or_default(),
        }
    }
}

impl From<ObjectSlotWire> for ObjectSlot {
    /// AN EMPTY ARRAY DECODES TO `None`, not to `Some(vec![])`.
    ///
    /// The two are equal to every reader here, and `None` is what the in-memory type produces for an
    /// emptied series -- so decoding an absent `e` into `Some(vec![])` would make a reloaded slot
    /// hold an allocation the same slot never held before the dump, and the footprint figures either
    /// side of a reload would stop matching.
    fn from(wire: ObjectSlotWire) -> Self {
        Self {
            record: wire.r,
            many: if wire.e.is_empty() {
                None
            } else {
                Some(Box::new(wire.e))
            },
        }
    }
}

/// The stored spelling. Short keys because this is one row per object and there are many of them.
/// THE ONE-BYTE SIZE CLASS: a four-bit exponent and a four-bit mantissa, rounded to nearest.
///
/// Code 0 means "no class recorded". Otherwise the value is `(16 + m) << e` for `e` in 0..16 and
/// `m` in 0..16, which spans 16 B to 1,015,808 B in steps of one sixteenth -- a worst-case 3.1%
/// per object, and less in a sum because the roundings go both ways. Measured at 1.5% on a corpus
/// of lengths chosen to fall between classes.
///
/// WHY A CLASS AND NOT AN EXACT SIZE. `ObjectRecord` pinned one byte of tail slack, so a `u8` costs
/// NOTHING: the field sum goes 15 -> 16 and the width does not move. An exact `u32` would take the
/// sum to 19 and, at `align_of` 8, the record from 16 bytes to 24 -- eight bytes an object for
/// three percent.
///
/// Lengths above the top class SATURATE there. Stated rather than hidden: the exponent would widen
/// if the corpus needed it, and `logical_size_saturates_at_the_top_class` holds the boundary.
fn classify_logical_size(len: u64) -> u8 {
    if len == 0 {
        return 0;
    }
    let mut best_code: u8 = 1;
    let mut best_error = u64::MAX;
    for code in 1u16..=255 {
        let exponent = (code - 1) >> 4;
        let mantissa = (code - 1) & 15;
        let value = (16u64 + mantissa as u64) << exponent;
        let error = value.abs_diff(len);
        if error < best_error {
            best_error = error;
            best_code = code as u8;
        }
    }
    best_code
}

/// The inverse of [`classify_logical_size`]. `None` for code 0, which means "no class recorded".
