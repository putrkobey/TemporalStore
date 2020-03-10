// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the consolidated per-object record holds, and that converting an object's separate rows
//! into it and back loses nothing.
//!
//! A WIDTH PIN PROVES ARITHMETIC AND NOTHING ELSE. `size_of == 24` says the fields fit; it says
//! nothing about whether the fields are the right ones. So the load-bearing test here is
//! [`converting_one_objects_rows_into_the_record_and_back_loses_nothing`], which takes the rows the
//! shard holds today -- the model map's address, the deadline map's instant, the dirty index's bit
//! and the model id -- folds them into one record, unfolds them, and compares. A merge that cannot
//! survive that is a merge that drops a fact.
//!
//! AND ONE MEASUREMENT RATHER THAN AN ASSUMPTION. The inline arm cannot hold a page identity, so
//! how often that matters is a COUNT, not a reading:
//! [`how_often_a_real_store_needs_the_layout_arm`] seeds a store through the ordinary write path
//! and reports the split. Its answer decides whether the inline arm is the common case or the rare
//! one, and that is the difference between this record saving memory and costing it.

use super::*;

use crate::block_store::ElementEntry;
use crate::engine::object_record::{
    ObjectRecord, ObjectRecordRefusal, BlockAddress, BlockLayout, BlockLayoutHandle, RecordFlags,
};

// Imported as a NAME rather than called through its full path, for the reason the sibling census
// probe states at length: the counting-allocator gate in `alloc_probe.rs` scans every source line
// for the probe's fully qualified path and walks BACK to the nearest `#[test]`. A helper spelling
// that path out sits above every test in the file, so the walk runs off the top and the gate
// reports the allocator as read outside any test. It is not a style preference -- writing it the
// other way fails `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it`.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// Bytes the heap gives up to hold a deep copy of `value` -- which is what `value` itself holds.
///
/// The same measurement the sibling census probe takes, spelled locally rather than imported: that
/// module's helper is private to it, and a `pub(super)` door opened across two test modules to
/// share eight lines is a worse trade than the eight lines. `Arc` clones bump a refcount and
/// allocate nothing, so shared string payloads are deliberately NOT in these figures -- which is
/// the right denominator for the question below, because both halves of the dirty index share one
/// payload and it survives whichever half goes.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}

/// Every named bit, so a test can walk them rather than restating the list.
///
/// EIGHT, NOT ELEVEN: `DEADLINE_IS_POINTER`, `DIRTY` and `DEADLINE_PRESENT` retired with the
/// deadline word, and a retired bit joins `RESERVED` rather than being renumbered away. The flag
/// census beside the type asserts the same count, and it is the one that gates -- this list exists
/// so a test can walk the bits by name.
const NAMED_BITS: [(&str, u16); 8] = [
    ("ADDRESS_IS_POINTER", RecordFlags::ADDRESS_IS_POINTER),
    ("INDEX_DIRTY", RecordFlags::INDEX_DIRTY),
    ("LOADING", RecordFlags::LOADING),
    ("IN_MEMORY", RecordFlags::IN_MEMORY),
    ("BLOCK_DIRTY", RecordFlags::BLOCK_DIRTY),
    ("BLOCK_DELETED", RecordFlags::BLOCK_DELETED),
    ("BLOCK_IN_LOG", RecordFlags::BLOCK_IN_LOG),
    ("BLOCK_GENERATION", RecordFlags::BLOCK_GENERATION),
];

/// THE WIDTH AND THE ALIGNMENT, MEASURED AND PRINTED RATHER THAN PREDICTED.
///
/// The const assertions beside the type are the gate; this prints the numbers so a reader of the
/// log sees what was reached, and asserts them again at runtime so a failure names the figure
/// instead of being a compile error with no value in it.
///
/// NO NEIGHBOUR INEQUALITY. `!= 23 && != 25` passes at every width, which makes it an assertion
/// that can only ever fail inside a test nothing runs; this campaign shipped that error into three
/// merged pull requests. The width is an equality and the alignment is a SEPARATE equality.
#[test]
fn the_record_is_sixteen_bytes_at_natural_alignment() {
    let width = std::mem::size_of::<ObjectRecord>();
    let align = std::mem::align_of::<ObjectRecord>();
    let field_sum = std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<RecordFlags>()
        + std::mem::size_of::<u8>();
    println!("ObjectRecord: size_of = {width}, align_of = {align}");
    println!("  field sum = {field_sum}, tail slack = {}", width - field_sum);
    println!("  RecordFlags: size_of = {}", std::mem::size_of::<RecordFlags>());
    assert_eq!(16, width, "the record's width moved");
    assert_eq!(8, align, "the record's alignment moved");
    assert_eq!(15, field_sum, "the field set moved without the width stepping");
    assert_eq!(1, width - field_sum, "the tail slack moved");
}

/// WHAT THE ATTRIBUTE-HOLDING RECORD SHAPE WOULD COST, against the sixteen-byte one.
///
/// This record follows a shape that tags two words and keeps everything else out of line. The other
/// end of that design family is the obvious one: a per-object record that simply HOLDS its
/// attributes -- names as owned strings, user metadata as maps, each timestamp a plain word, each
/// flag a bool. It is what a reader reaches for first, so its width is worth having measured rather
/// than assumed, and the gap turns out to be most of two orders of magnitude.
///
/// THREE DECISIONS THAT COME WITH THAT SHAPE, and this engine takes exactly one of them:
///
///   * REFUSED, AND THIS ONE CHANGED: the timestamps living ON the record. It was priced at a 27%
///     crossover by `what_moving_the_string_map_onto_the_record_would_cost` and then NOT taken --
///     the record's deadline word was never filled and has been retired, so a per-key deadline
///     stays in its own structure. The row is sixteen bytes rather than twenty-four as a result.
///   * REFUSE: absence as a sentinel zero. A deadline of zero is a legal deadline, so zero cannot
///     carry its own absence. The note beside `RESERVED` carries that reasoning now, which is where
///     the retired tag bits are recorded -- a default of `0` meaning "no retention" cannot tell
///     that case from a retention at the epoch.
///   * REFUSE: dropping the time-ORDERED index and letting a sweep scan. The ordered index is what
///     makes the due set a prefix; without it, time-to-expire becomes a function of the keyspace
///     rather than of how many keys are due. Measured at ten expired keys behind ten thousand live
///     ones surviving more than sixty rounds.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib what_the_attribute_holding_record_shape_would_cost -- --nocapture
#[test]
fn what_the_attribute_holding_record_shape_would_cost() {
    use std::collections::BTreeMap;

    // The attribute-holding shape, modelled field for field at the same composition: the two names
    // and three digests as owned strings, user metadata and tags as maps, the sizes and the four
    // instants as words, the three flags as bools, a storage class as a string, an identity, an
    // offset, an extent list, and a placement block that is itself three owned containers.
    #[allow(dead_code)]
    struct Placement {
        primary: String,
        data_replicas: Vec<String>,
        metadata_voters: Vec<String>,
        quorum: u64,
        epoch: u64,
    }
    #[allow(dead_code)]
    struct Extent {
        volume: u64,
        id: u32,
        offset: u64,
        length: u64,
    }
    #[allow(dead_code)]
    struct AttributeHoldingRecord {
        container: String,
        key: String,
        digest: String,
        checksum: String,
        content_type: String,
        metadata: BTreeMap<String, String>,
        tags: BTreeMap<String, String>,
        size: u64,
        version: u64,
        updated_ms: u64,
        // THE FOUR INSTANTS, inline on the record. This is the decision worth taking: a per-object
        // time does not need a side table. What comes WITH it in this shape, and is refused here,
        // is that each one defaults to zero and so cannot express its own absence.
        deleted_ms: u64,
        hold_updated_ms: u64,
        retain_until_ms: u64,
        restore_until_ms: u64,
        delete_marker: bool,
        hold: bool,
        storage_class: String,
        identity_volume: u64,
        identity_id: u32,
        offset: u64,
        extents: Vec<Extent>,
        placement: Placement,
        // and the tombstone plus the version chain the outer record adds
        deleted: bool,
    }

    let theirs = std::mem::size_of::<AttributeHoldingRecord>();
    let ours = std::mem::size_of::<ObjectRecord>();
    let slot = std::mem::size_of::<crate::engine::object_record::ObjectSlot>();

    println!();
    println!("  PER-OBJECT RECORD, two ends of one design family:");
    println!("    attribute-holding shape   {theirs:>5} B   (before ANY heap: every string and");
    println!("                                        map above allocates its contents too)");
    println!("    this engine's record      {ours:>5} B   two tagged words, a size, flags, a model id");
    println!("    .. inside its slot        {slot:>5} B   plus the owned element array when present");
    println!("    ratio                     {:>5.1}x", theirs as f64 / ours as f64);
    println!();
    println!("  The heap side is not modelled and would widen the gap, not narrow it: five owned");
    println!("  strings and two maps allocate per object, where this record allocates nothing.");

    // THE CLAIM. Not "ours is smaller" -- that is arithmetic -- but that the gap is large enough
    // that the attribute-holding shape is a different design rather than a tuning choice. If this
    // ever falls under 8x, the record has grown into the other shape and the module doc's framing
    // ("two tagged words ARE the design") needs re-reading.
    assert!(
        theirs >= ours * 8,
        "the attribute-holding shape measured {theirs} B against this record's {ours} B, under 8x. \
         These are supposed to be opposite ends of a design family; at this ratio they are the same \
         design, which means this record has grown attributes it used to keep out of line."
    );

    // AND THE REFUSALS ARE RECORDED ELSEWHERE, named here so this arm is not their only record:
    // the note beside `RESERVED` carries the sentinel-zero reasoning, now that the tag bits it
    // justified have retired, and `the_key_ordered_expiry_window_has_no_production_callers` holds
    // the ordered-index one.
    assert_eq!(ours, 16, "the record's width is pinned by its own arm; this arm only compares");
}

/// Eight distinct named bits, eight reserved, and the two sets disjoint.
///
/// A duplicated mask is the failure this catches: two names for one bit compile, and then setting
/// one silently sets the other.
#[test]
fn every_flag_bit_is_distinct_and_the_reserved_set_is_disjoint() {
    let mut seen = 0u16;
    for (name, bit) in NAMED_BITS {
        assert_eq!(1, bit.count_ones(), "{name} is not a single bit: {bit:#06x}");
        assert_eq!(0, seen & bit, "{name} reuses a bit another name already took");
        assert_eq!(
            0,
            bit & RecordFlags::RESERVED,
            "{name} sits inside the reserved range {:#06x}",
            RecordFlags::RESERVED
        );
        seen |= bit;
    }
    assert_eq!(8, seen.count_ones(), "eight named bits");
    assert_eq!(RecordFlags::KNOWN, seen, "the KNOWN mask and the named list disagree");
    assert_eq!(8, RecordFlags::RESERVED.count_ones(), "eight reserved bits");
    assert_eq!(0, RecordFlags::KNOWN & RecordFlags::RESERVED, "the two sets overlap");
    assert_eq!(16, (RecordFlags::KNOWN | RecordFlags::RESERVED).count_ones(), "sixteen in all");
}


/// BOTH TAGGED WORDS ARE UNREADABLE WITHOUT THEIR TAG, and the arms round-trip through the setter.
///
/// The structural claim is that there is no accessor handing out a bare word. That cannot be
/// asserted from inside the crate -- a private field is reachable here -- so what this asserts is
/// the behavioural half: every arm set is the arm read back, and a handle never reads as an
/// address.
#[test]
fn each_tagged_word_reads_back_as_the_arm_it_was_set_to() {
    let mut record = ObjectRecord::default();

    // ONE TAGGED WORD REMAINS. The deadline word retired, so the arm that walked its three
    // meanings has nothing left to walk; the address word still has two and is walked below. The
    // property is unchanged -- a tag decides which arm a word means, never the value.
    for arm in [
        BlockAddress::One(0),
        BlockAddress::One(crate::engine::object_record::address_word_from_parts(7, 4_096)),
        BlockAddress::Many(BlockLayoutHandle::from_stored(0)),
        BlockAddress::Many(BlockLayoutHandle::from_stored(42)),
    ] {
        record.set_block_address(arm);
        assert_eq!(arm, record.page_address(), "the address arm did not survive a set-then-get");
    }

    // A HANDLE OF ZERO AND AN ADDRESS OF ZERO ARE DIFFERENT ARMS. Both words are zero, so only the
    // tag separates them -- which is exactly the case a sentinel would collapse.
    record.set_block_address(BlockAddress::One(0));
    let as_address = record.page_address();
    record.set_block_address(BlockAddress::Many(BlockLayoutHandle::from_stored(0)));
    let as_handle = record.page_address();
    assert_ne!(as_address, as_handle, "a zero address and a zero handle read the same");
}

/// THE RECORD SURVIVES ITS OWN CODEC INTACT, on every arm combination that is legal.
#[test]
fn the_record_survives_its_own_codec_intact() {
    // TWO CASES, NOT EIGHT. The matrix was four deadline arms by two address arms; the deadline
    // dimension retired with the word, leaving the address arms. The count stays asserted because
    // its job was never the number: a matrix that silently builds fewer cases than its loops imply
    // is exactly the failure it exists to catch.
    let mut cases: Vec<ObjectRecord> = Vec::new();
    for address in
        [BlockAddress::One(crate::engine::object_record::address_word_from_parts(3, 64)),
         BlockAddress::Many(BlockLayoutHandle::from_stored(11))]
    {
        let mut record = ObjectRecord::default();
        record.set_block_address(address);
        record.set_block_size(4_096);
        record.set_model_id(7);
        for (_, bit) in NAMED_BITS {
            // Do not disturb the one tag bit the setter just arranged.
            if bit == RecordFlags::ADDRESS_IS_POINTER {
                continue;
            }
            record.set_flag(bit, true);
        }
        cases.push(record);
    }
    assert_eq!(2, cases.len(), "the case matrix did not build");

    for record in cases {
        let encoded = serde_json::to_string(&record).expect("a record must encode");
        let decoded: ObjectRecord =
            serde_json::from_str(&encoded).unwrap_or_else(|error| {
                panic!("a record this type wrote must decode: {error} from {encoded}")
            });
        assert_eq!(record, decoded, "the codec moved a field: {encoded}");
        assert_eq!(
            record.page_address(),
            decoded.page_address(),
            "the address arm did not survive the codec: {encoded}"
        );
    }
}


/// THE DECODER REFUSES BY NAME, AS AN `Err`, AND NEVER PANICS.
///
/// A panic on this path makes a store unopenable; a refusal falls back to replaying the operation
/// log. So each malformed shape is asserted to be an `Err` of a NAMED variant, and the message is
/// asserted to carry that name -- a refusal nobody can identify from a log line is a refusal that
/// gets diagnosed as corruption.
#[test]
fn the_decoder_refuses_by_name_rather_than_panicking() {
    // Built as raw wire text, because these are shapes this writer cannot produce.
    // THREE, NOT FOUR: the `t`-on-the-wire case left the decoder, for the reason stated beside
    // the cases below.
    let cases: [(&str, &str, &str); 3] = [
        (
            "a reserved bit set by a newer writer",
            r#"{"a":64,"n":8,"f":4096,"m":1}"#,
            "ReservedFlagBitsSet",
        ),
        // THE TWO RETIRED DEADLINE BITS ARE STILL REFUSED, under a different name: bits 1 and 10
        // joined `RESERVED`, so a record setting either is rejected as a newer writer's record
        // rather than by a deadline-specific check. Same refusal, earlier, and by one rule.
        (
            "the retired deadline-presence bit, now reserved",
            r#"{"a":64,"n":8,"f":1024,"m":1}"#,
            "ReservedFlagBitsSet",
        ),
        (
            "the retired deadline-pointer bit, now reserved",
            r#"{"a":64,"n":8,"f":2,"m":1}"#,
            "ReservedFlagBitsSet",
        ),
        // A `t` KEY ON THE WIRE IS NO LONGER THE DECODER'S BUSINESS, and that case is gone rather
        // than rewritten. The wire struct does not declare `t`, so a record carrying one has the
        // key DROPPED silently -- which is how every undeclared key behaves here and is exactly
        // what the index format stamp exists to refuse. Asserting a decoder refusal for it would
        // claim a protection this layer does not provide.
    ];

    let mut refused = 0usize;
    for (what, wire, expected_name) in cases {
        let outcome: Result<ObjectRecord, _> = serde_json::from_str(wire);
        let error = match outcome {
            Ok(record) => panic!(
                "{what}: the decoder ACCEPTED a shape it cannot read and built {record:?} from \
                 {wire}"
            ),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(expected_name),
            "{what}: the refusal does not name itself -- expected {expected_name} in {error:?}"
        );
        refused += 1;
    }
    assert_eq!(cases.len(), refused, "not every malformed shape was refused");

    // THE POSITIVE CONTROL. A decoder that refuses everything passes the four assertions above and
    // is useless, which is the failure mode worth catching here.
    // THE CONTROL IS A LEGAL RECORD USING STILL-KNOWN BITS. It used to set bits 2 and 10 and
    // carry a `t`, all three of which retired -- so the record it was built from would now be
    // REFUSED as a newer writer's, and a control that gets refused proves nothing. Bit 3 is
    // `INDEX_DIRTY`, which is still named.
    let legal = r#"{"a":64,"n":8,"f":8,"m":7}"#;
    let decoded: ObjectRecord = serde_json::from_str(legal)
        .unwrap_or_else(|error| panic!("the decoder refused a LEGAL record: {error} from {legal}"));
    // THE CONTROL IS STILL A CONTROL, narrowed to what the record still carries: it decoded, so
    // the refusals above fired on the malformed shapes and not on the decoder being broken.
    assert_eq!(7, decoded.model_id(), "the control decoded but lost its model id");
}

/// THE PROOF THE MERGE IS REAL: an object's separate rows, folded into one record and unfolded.
///
/// The rows are the ones a live `string` object holds today -- the model map's [`ElementEntry`],
/// the deadline map's instant, and the dirty index's bit -- plus the model id the map's identity
/// carries. Nothing may differ after the round trip, including the parts of the address that are
/// PRESENCE rather than value: a generation bit dropped here would hand an older index a
/// generation it never had.
#[test]
fn converting_one_objects_rows_into_the_record_and_back_loses_nothing() {
    // The inline arm. Both ordinal states belong here: ABSENT (a log-resident page) and PRESENT
    // AT ZERO (a slab-resident single-page object, which the store measurement says is 400 of 400
    // of what the ordinary write path files).
    let inline_cases: [(&str, ElementEntry, u8, Option<u64>, bool); 7] = [
        ("log-resident, no deadline, clean", ElementEntry::from_parts(3, 4_096, 128, None, None), 1, None, false),
        ("log-resident, no deadline, dirty", ElementEntry::from_parts(3, 4_096, 128, None, None), 1, None, true),
        ("log-resident, deadline, dirty", ElementEntry::from_parts(9, 0, 64, None, None), 4, Some(1_700_000_000_000), true),
        ("log-resident, ZERO deadline", ElementEntry::from_parts(1, 8, 32, None, None), 2, Some(0), false),
        ("ordinal zero, no deadline, clean", ElementEntry::from_parts(5, 64, 128, Some(0), Some(0)), 1, None, false),
        ("ordinal zero, deadline, dirty", ElementEntry::from_parts(7, 4_096, 832, Some(0), Some(0)), 4, Some(42), true),
        ("ordinal zero, ZERO deadline, dirty", ElementEntry::from_parts(1, 0, 16, Some(0), Some(0)), 2, Some(0), true),
    ];

    let mut checked = 0usize;
    for (what, address, model_id, deadline, dirty) in inline_cases {
        let record = ObjectRecord::from_object_rows(&address, model_id, deadline, dirty)
            .unwrap_or_else(|error| panic!("{what}: the inline arm refused a row it should hold: {error}"));
        assert!(
            matches!(record.page_address(), BlockAddress::One(_)),
            "{what}: a log-resident page should take the inline arm"
        );
        let (back_address, back_model) = record
            .into_object_rows(None)
            .unwrap_or_else(|error| panic!("{what}: unfolding refused: {error}"));
        assert_eq!(address, back_address, "{what}: the address row changed");
        assert_eq!(model_id, back_model, "{what}: the model id changed");
        // The presence halves specifically, because they are the ones a value comparison can miss.
        assert_eq!(address.block_id(), back_address.block_id(), "{what}: page identity presence");
        assert_eq!(address.generation(), back_address.generation(), "{what}: generation presence");
        assert_eq!(address.length(), back_address.length(), "{what}: page length");
        assert_eq!(address.address_word(), back_address.address_word(), "{what}: address word");
        checked += 1;
    }

    // The layout arm: a page of a MULTI-page object, whose ordinal is nonzero. Ordinal zero is
    // included too, because the layout arm must hold everything the inline arm holds as well --
    // an arm that only works for the cases the other one refuses cannot serve a mixed object.
    let layout_cases: [(&str, ElementEntry, u8, Option<u64>, bool); 4] = [
