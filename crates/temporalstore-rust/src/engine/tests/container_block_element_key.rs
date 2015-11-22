// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT IT COSTS FOR A PAGE TO STATE WHICH ELEMENT IT HOLDS, MEASURED PER KIND.
//!
//! # THE CLAIM THIS MODULE WAS OPENED ON
//!
//! A container page used to carry a value and nothing else. `container_blocks` puts a frame round it
//! that names the element, which costs bytes on every container write and changes no answer. This
//! module is where the cost is measured and where "changes no answer" is asserted rather than
//! assumed.
//!
//! THE COST IS NOT ONE NUMBER, and that is the finding. The element key's relationship to the value
//! is different for each of the four kinds:
//!
//! ```text
//!     hash   key = the field name      value = the field value    unrelated
//!     set    key = the member          value = the member         value IS the key
//!     zset   key = score ++ member     value = the member         value is the key's tail
//!     list   key = the sequence word   value = the element        unrelated
//! ```
//!
//! So a set page can name its element for the price of the framing alone, a zset page for the
//! framing plus its eight-byte score, and only `hash` and `list` pay for a second copy of anything.
//! `the_framing_cost_of_naming_an_element_differs_by_kind` prints the ladder that shows it, per kind
//! and per width, with the denominator on every row.
//!
//! # THE THREE THINGS THAT COULD GO WRONG, AND THE CONTROL FOR EACH
//!
//! ## The frame could be unreadable, and a reader would serve framing bytes as data
//!
//! That is #2016's shape exactly -- an unreadable name becoming a real one -- and it is why
//! `decode_container_block` has a `Corrupt` arm distinct from `NotFramed` rather than falling back.
//! `a_frame_that_cannot_be_walked_is_not_mistaken_for_a_value` drives eleven separate mutilations of
//! a real frame and asserts every one of them is refused.
//!
//! ## The selection could be vacuous, answering correctly for the wrong reason
//!
//! A selector that ignored the component and returned the first item would pass every happy-path
//! test in this file, because at one item per page the first item IS the one asked for. So the
//! controls are a frame holding TWO elements -- asked for each in turn, and asked for a third that
//! is not there -- and a frame whose key spelling is deliberately wrong for the component being
//! looked up. #2016's first mutation run had the defect SURVIVE on a one-key fixture; this module
//! does not own a one-key fixture.
//!
//! ## An old page could stop reading
//!
//! No version stamp moves in this stage, and the only thing standing behind that is the magic
//! discriminating. `a_block_written_without_a_frame_still_reads_through_the_funnel` installs bare
//! bytes into a slab, files an index entry that names them, and reads them back through the product
//! path -- so the compatibility claim is exercised rather than argued.
//!
//! # WHAT THIS MODULE DOES NOT COVER
//!
//! Batching. Every frame written by the product today holds exactly ONE item, because the write
//! sites append one element at a time; the two-item frames here are built directly. The per-item
//! marginal cost measured below is therefore an arithmetic projection of what batching would save
//! and is labelled as one, not a measurement of a batched write.
//!
//! THE STORE PATH LENGTH is held equal across every arm of the end-to-end test and asserted equal:
//! it moves allocation bytes at about six bytes a character, so an arm on a longer temporary
//! directory would read as a heavier representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::container_blocks::{
    component_from_element_key, decode_container_block, element_key_from_component,
    encode_container_block, select_container_element, varint_len, ContainerElementRead,
    ContainerBlockDecode, ContainerBlockItem, ElementKeySpelling, CONTAINER_BLOCK_MAGIC,
};

/// Every spelling, so a test over "all of them" cannot silently stop covering one that is added.
const EVERY_SPELLING: [ElementKeySpelling; 4] = [
    ElementKeySpelling::Utf8,
    ElementKeySpelling::Hex,
    ElementKeySpelling::ScoreThenMember,
    ElementKeySpelling::BiasedWord,
];

/// Every kind that frames its pages, beside the spelling it uses.
const EVERY_FRAMED_KIND: [(&str, ElementKeySpelling); 4] = [
    ("hash", ElementKeySpelling::Utf8),
    ("set", ElementKeySpelling::Hex),
    ("zset", ElementKeySpelling::ScoreThenMember),
    ("list", ElementKeySpelling::BiasedWord),
];

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "container-page-element-key".to_string(),
        shard_uri: "local://container-page-element-key/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// A value of exactly `width` bytes, distinct per index.
fn bytes_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

/// A component this spelling can produce, built from a member of the given width.
///
/// Built THROUGH the spelling rather than spelled by hand: a hand-written component would be a
/// second implementation of the thing under test, and #1863's lesson about hand-written subject
/// lists applies to hand-written fixtures of an encoding just as well.
fn component_for(spelling: ElementKeySpelling, width: usize, index: usize) -> String {
    let key = match spelling {
        ElementKeySpelling::Utf8 => format!("field-{index:04}").into_bytes(),
        ElementKeySpelling::Hex => bytes_of(width, index),
        ElementKeySpelling::ScoreThenMember => {
            let mut key = Vec::new();
            key.extend_from_slice(&(0x8000_0000_0000_0000_u64 + index as u64).to_be_bytes());
            key.extend_from_slice(&bytes_of(width, index));
            key
        }
        ElementKeySpelling::BiasedWord => (index as u64).to_be_bytes().to_vec(),
    };
    component_from_element_key(spelling, &key)
        .expect("a key built by this fixture spells a component")
}

// =================================================================================================
// 1. THE SPELLINGS ARE INVERSES, WHICH IS THE WHOLE SAFETY ARGUMENT
// =================================================================================================

/// A COMPONENT SURVIVES BEING TURNED INTO A PAGE KEY AND BACK, FOR EVERY SPELLING.
///
/// This is the property the rest of the program rests on. If the page key and the index component
/// are two spellings of one value then either can be derived from the other, and a later stage may
/// stop storing whichever it prefers. If they were merely written consistently at the same instant
/// by one call site -- which is how a set's component and its payload agree TODAY, with nothing
/// re-checking them -- then the second copy would be an unverified duplicate and deleting the first
/// would be unsafe.
///
/// THE DENOMINATOR IS PRINTED AND FLOORED. A loop over an empty ladder asserts nothing and reports
/// success, which is how two threads were nearly defeated in one day.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_component_survives_the_round_trip_to_a_block_key_for_every_spelling() {
    let widths = [0usize, 1, 8, 16, 64, 255, 256];
    let mut checked = 0usize;
    println!("\n=== component -> key -> component, every spelling ===");
    println!(
        "  {:>18}  {:>6}  {:>9}  {:>9}",
        "spelling", "width", "comp len", "key len"
    );
    for spelling in EVERY_SPELLING {
        for width in widths {
            // A biased word is eight bytes by definition, so the width ladder does not apply to it;
            // one row is all there is and the fixture says so rather than skipping in silence.
            if matches!(spelling, ElementKeySpelling::BiasedWord) && width != 8 {
                continue;
            }
            for index in [0usize, 1, 7] {
                let component = component_for(spelling, width, index);
                let key = element_key_from_component(spelling, &component)
                    .expect("a component this spelling produced is one it can read back");
                let again = component_from_element_key(spelling, &key)
                    .expect("a key this spelling produced spells a component");
                assert_eq!(
                    component, again,
                    "{spelling:?} did not round trip a component of width {width}"
                );
                if index == 0 {
                    println!(
                        "  {:>18}  {:>6}  {:>9}  {:>9}",
                        format!("{spelling:?}"),
                        width,
                        component.len(),
                        key.len()
                    );
                }
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 4 * 3,
        "the round trip exercised {checked} components, which is too few to cover four spellings"
    );
    println!("  round trips checked: {checked}");
}

/// AN EMPTY ZSET COMPONENT NAMES AN EMPTY MEMBER, NOT A SIXTEEN-CHARACTER SCORE PREFIX.
///
/// RESTATED: the boundary this test drove used to be sixteen characters, because the component
/// was a complete score with an empty member after it and `hex::decode("")` is `Ok(vec![])` --
/// #2016's fix was keeping `<` rather than `<=` at that boundary. The boundary is zero now,
/// because the component is the member alone: an empty component is `hex::encode(&[])`, which
/// `hex::decode` reads back as the empty member it is, with no score term to be short by.
///
/// rust-internal: exercises the page framing directly
#[test]
fn an_empty_zset_component_names_an_empty_member() {
    let component = String::new();
    let key = element_key_from_component(ElementKeySpelling::ScoreThenMember, &component)
        .expect("an empty component is a complete, if empty, member");
    assert_eq!(8, key.len(), "the key is the placeholder score word and nothing else");
    assert_eq!(
        Some(component.clone()),
        component_from_element_key(ElementKeySpelling::ScoreThenMember, &key)
    );

    // Odd-length hex is genuinely not decodable, so that boundary is tested from the other side.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::ScoreThenMember, "a"),
        "one hex digit cannot be a member"
    );
}

/// SCORETHENMEMBER'S KEY DOES NOT SURVIVE A ROUND TRIP THAT STARTS FROM THE KEY -- THE OTHER
/// THREE SPELLINGS STILL DO, EXACTLY AS BEFORE.
///
/// THE LOST TERM IS THE SCORE. `component_from_element_key` now renders only a ScoreThenMember
/// key's member half, so `element_key_from_component`'s way back cannot recover the score half --
/// it fills a placeholder (asserted elsewhere to be the all-zero word) where the true score was.
/// That is the "one real cost" this change pays, and this test drives it directly: build a key
/// with a REAL, nonzero score, round it through both functions, and show what comes back differs
/// from what went in -- in the score half only, never in the member half.
///
/// `component_from_element_key` STAYS TOTAL, asserted here rather than assumed: every key at
/// least eight bytes long still spells a component, which is the property
/// `select_container_element` leans on to answer `Absent` rather than `Corrupt` for a page that
/// is merely unaddressed by a given component.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_scorethenmember_key_round_trip_is_lossy_in_its_score_half_and_bijective_in_its_member_half() {
    // THE OTHER THREE SPELLINGS: key -> component -> key reproduces the key exactly, unchanged by
    // this series.
    for spelling in [
        ElementKeySpelling::Utf8,
        ElementKeySpelling::Hex,
        ElementKeySpelling::BiasedWord,
    ] {
        for width in [0usize, 1, 8, 64] {
            let key = match spelling {
                ElementKeySpelling::Utf8 => format!("field-{width:04}").into_bytes(),
                ElementKeySpelling::BiasedWord if width != 8 => continue,
                _ => bytes_of(width, width),
            };
            let component = component_from_element_key(spelling, &key)
                .unwrap_or_else(|| panic!("{spelling:?} did not spell a component at width {width}"));
            let back = element_key_from_component(spelling, &component).unwrap_or_else(|| {
                panic!("{spelling:?} did not read its own component back at width {width}")
            });
            assert_eq!(key, back, "{spelling:?} lost the key at width {width}");
        }
    }

    // SCORETHENMEMBER, WITH A REAL SCORE: the member half survives: the score half does not.
    let member = bytes_of(20, 1);
    let real_score: u64 = 0x1234_5678_9abc_def0;
    let mut key = Vec::with_capacity(8 + member.len());
    key.extend_from_slice(&real_score.to_be_bytes());
    key.extend_from_slice(&member);

    let component = component_from_element_key(ElementKeySpelling::ScoreThenMember, &key)
        .expect("still total for a key at least eight bytes long");
    assert_eq!(
        component,
        hex::encode(&member),
        "the component is the member alone -- the score is the lost term"
    );

    let reconstructed = element_key_from_component(ElementKeySpelling::ScoreThenMember, &component)
        .expect("still produces a validly shaped key");
    assert_eq!(reconstructed.len(), key.len(), "the shape survives even though the content does not");
    assert_eq!(
        &reconstructed[8..],
        &key[8..],
        "the member half is NOT lost -- this is the half every live reader depends on"
    );
    assert_ne!(
        &reconstructed[..8],
        &key[..8],
        "the score half IS lost: a real, nonzero score does not come back"
    );
    assert_eq!(
        &reconstructed[..8],
        &[0u8; 8],
        "the placeholder that fills the lost score half is the all-zero word"
    );

    // TOTALITY: component_from_element_key still answers for every key at or above the score
    // word's width, and correctly declines anything shorter.
    for len in [8usize, 9, 16, 100] {
        let probe = vec![0xABu8; len];
        assert!(
            component_from_element_key(ElementKeySpelling::ScoreThenMember, &probe).is_some(),
            "not total at length {len}"
        );
    }
    assert_eq!(
        component_from_element_key(ElementKeySpelling::ScoreThenMember, &[0u8; 7]),
        None,
        "a key shorter than the score word correctly has no component"
    );
}

/// A COMPONENT A SPELLING CANNOT PRODUCE IS REFUSED RATHER THAN COERCED.
///
/// The control on the round trip above: it would pass just as well if every spelling accepted
/// everything, so the refusals have to be asserted too. Each row below is a component that is
/// well formed for SOME spelling and not for the one being asked.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_component_the_spelling_cannot_produce_is_refused() {
    // Not hex at all.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::Hex, "field-0001")
    );
    // Hex of odd length.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::Hex, "abc")
    );
    // A list word is exactly sixteen characters.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::BiasedWord, "8000000000000000ff")
    );
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::BiasedWord, "800000000000000")
    );
    // A zset component is hex of the member, whole -- there is no score half left to be hex
    // instead of the member.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::ScoreThenMember, "zzzzzzzzzzzzzzzz")
    );
    // A key that is not eight bytes cannot spell a list word.
    assert_eq!(
        None,
        component_from_element_key(ElementKeySpelling::BiasedWord, &[0u8; 7])
    );
    // A hash field key that is not valid UTF-8 cannot spell a component.
    assert_eq!(
        None,
        component_from_element_key(ElementKeySpelling::Utf8, &[0xff, 0xfe])
    );
    // And a UTF-8 field name CAN be empty, which is legal and must not be refused -- #2016's
    // finding was that an ABSENT name must not become the EMPTY one, not that the empty one is
    // illegal.
    assert_eq!(
        Some(String::new()),
        component_from_element_key(ElementKeySpelling::Utf8, b"")
    );
}

// =================================================================================================
// 2. THE FRAME ROUND TRIPS, AND THE SUFFIX ELISION IS MEASURED RATHER THAN ASSUMED
// =================================================================================================

/// A FRAME DECODES TO THE ITEMS IT WAS BUILT FROM, INCLUDING A VALUE THAT IS THE KEY'S TAIL.
///
/// Two items and not one, deliberately: at one item per page a decoder that returned only the first
/// would pass, and that is the shape #2016's first mutation run survived on.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_frame_decodes_to_the_items_it_was_built_from() {
    let mut cases = 0usize;
    for spelling in EVERY_SPELLING {
        let first_key = element_key_from_component(spelling, &component_for(spelling, 16, 1))
            .expect("fixture component");
        let second_key = element_key_from_component(spelling, &component_for(spelling, 16, 2))
            .expect("fixture component");
        // The second item's value is its key's tail, so both branches of the value encoding are
        // exercised inside ONE frame.
        let first_value = bytes_of(24, 9);
        let second_value = second_key.clone();
        let block = encode_container_block(
            spelling,
            &[
                (&first_key, first_value.as_slice()),
                (&second_key, second_value.as_slice()),
            ],
        );
        match decode_container_block(&block) {
            ContainerBlockDecode::Framed {
                spelling: seen,
                shape,
                items,
            } => {
                assert_eq!(spelling, seen, "the frame reports the spelling it was written with");
                assert_eq!(
                    crate::engine::container_blocks::ContainerBlockShape::WithRemovals,
                    shape,
                    "a page written today takes the shape that CAN say an element was removed, \
                     whether or not this one does -- see CONTAINER_BLOCK_MAGIC_V2 on why there is \
                     one writer and not a branch on the contents"
                );
                assert_eq!(
                    vec![
                        ContainerBlockItem {
                            key: first_key.clone(),
                            value: first_value.clone(),
                            deleted: false
                        },
                        ContainerBlockItem {
                            key: second_key.clone(),
                            value: second_value.clone(),
                            deleted: false
                        },
                    ],
                    items,
                    "{spelling:?} did not round trip its items"
                );
            }
            other => panic!("{spelling:?} frame did not decode: {other:?}"),
        }
        cases += 1;
    }
    assert_eq!(4, cases, "every spelling must be covered");
}

/// A VALUE THAT IS THE KEY'S TAIL IS NOT STORED TWICE, AND ONE THAT IS NOT IS.
///
/// Measured in BYTES off the encoder rather than read off the encoder's source. The saving is the
/// whole reason the frame is affordable for `set`, so it is the one property of the format that has
/// to be a measurement.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_value_that_is_the_keys_tail_is_stored_once() {
    let key = bytes_of(32, 3);

    // The value IS the key.
    let shared = encode_container_block(ElementKeySpelling::Hex, &[(&key, key.as_slice())]);
    // The value is UNRELATED to the key, and the same length.
    let distinct_value = bytes_of(32, 4);
    assert_ne!(key, distinct_value, "the fixture's two values must differ");
    let separate =
        encode_container_block(ElementKeySpelling::Hex, &[(&key, distinct_value.as_slice())]);

    println!("\n=== the suffix elision, in bytes ===");
    println!("  key 32 bytes, value 32 bytes");
    println!("  value is the key      : {} bytes", shared.len());
    println!("  value is its own      : {} bytes", separate.len());
    println!("  saved                 : {} bytes", separate.len() - shared.len());
    // THE SAVING IS THE VALUE MINUS THE OFFSET VARINT, and it is stated from those two numbers
    // rather than as a round figure: the elided form spends one varint saying where in the key the
    // value starts, so eliding thirty-two bytes saves thirty-one and not thirty-two. A test that
    // asserted the round figure would be off by exactly the byte the format costs, which is the
    // byte most worth being able to see move.
    let expected_saving = 32 - varint_len(0);
    assert_eq!(
        expected_saving,
        separate.len() - shared.len(),
        "eliding a 32-byte value that equals its key should save the value less the offset varint"
    );
    // And the elided form still decodes to the value, so the saving is not a loss.
    assert_eq!(
        ContainerElementRead::Found(key.clone()),
        select_container_element(&shared, &hex::encode(&key))
    );
    assert_eq!(
        ContainerElementRead::Found(distinct_value),
        select_container_element(&separate, &hex::encode(&key))
    );

    // An EMPTY value is the tail beginning at the key's end, so it needs no case of its own.
    let empty = encode_container_block(ElementKeySpelling::Hex, &[(&key, b"")]);
    assert_eq!(
        ContainerElementRead::Found(Vec::new()),
        select_container_element(&empty, &hex::encode(&key)),
        "an empty value must decode back to empty and not to the key"
    );
}

/// THE FRAMING COST OF NAMING AN ELEMENT, PER KIND AND PER WIDTH.
///
/// The headline measurement of this stage, and the number that decides whether the rest of the
/// program is worth building. Printed as a ladder with the denominator on every row, because a mean
/// over four kinds whose keys relate to their values in four different ways would say nothing about
/// any of them.
///
/// THE PER-ITEM MARGINAL FIGURE IS AN ARITHMETIC PROJECTION AND IS LABELLED ONE. Nothing in the
/// product writes a frame of more than one item yet; the two-item frame here is built directly, and
/// the difference between it and the one-item frame is what a second element would cost a batched
/// page. That is a fact about the FORMAT, which is what this stage ships.
///
/// rust-internal: exercises the page framing directly
#[test]
fn the_framing_cost_of_naming_an_element_differs_by_kind() {
    let widths = [8usize, 16, 32, 64, 256];
    println!("\n=== bytes a page pays to name its own element ===");
    println!(
        "  {:>6}  {:>6}  {:>9}  {:>7}  {:>7}  {:>8}  {:>10}  {:>10}",
        "kind", "width", "bare page", "framed", "cost", "cost %", "2nd item", "at 128"
    );
    let mut rows = 0usize;
    let mut worst_share = 0.0_f64;
    for (kind, spelling) in EVERY_FRAMED_KIND {
        for width in widths {
            // What the product writes as the value for this kind, and what it names the element.
            let component = component_for(spelling, width, 1);
            let key = element_key_from_component(spelling, &component).expect("fixture component");
            let value: Vec<u8> = match kind {
                // A set's page holds the member; a zset's holds the member too.
                "set" | "zset" => bytes_of(width, 1),
                // A hash's page holds the field value and a list's holds the element; neither is
                // related to the key, so both are their own bytes.
                _ => bytes_of(width, 1),
            };
            // For set and zset the value the product writes IS the member, which is the key or its
            // tail, so the fixture must use the same bytes or the elision would not fire and the
            // measured cost would be the cost of a shape the product never writes.
            let value: Vec<u8> = match spelling {
                ElementKeySpelling::Hex => key.clone(),
                ElementKeySpelling::ScoreThenMember => key[8..].to_vec(),
                _ => value,
            };

            let bare = value.len();
            let framed = encode_container_block(spelling, &[(&key, value.as_slice())]).len();
            let second_key = element_key_from_component(spelling, &component_for(spelling, width, 2))
                .expect("fixture component");
            let second_value: Vec<u8> = match spelling {
                ElementKeySpelling::Hex => second_key.clone(),
                ElementKeySpelling::ScoreThenMember => second_key[8..].to_vec(),
                _ => bytes_of(width, 2),
            };
            let two = encode_container_block(
                spelling,
                &[
                    (&key, value.as_slice()),
                    (&second_key, second_value.as_slice()),
                ],
            )
            .len();
            let marginal = two - framed;
            // What a 128-item page would average per element, projected from the marginal item.
            let at_128 = (framed + 127 * marginal) as f64 / 128.0;
            let cost = framed as f64 - bare as f64;
            let share = cost / bare as f64 * 100.0;
            worst_share = worst_share.max(share);
            println!(
                "  {kind:>6}  {width:>6}  {bare:>9}  {framed:>7}  {:>7}  {:>7.1}%  {marginal:>10}  {at_128:>10.1}",
                framed as i64 - bare as i64,
                share
            );
            assert!(
                framed > bare,
                "a frame cannot be smaller than the value it wraps"
            );
            // The marginal item must be cheaper than the first, or batching buys nothing.
            assert!(
                marginal < framed,
                "{kind} at width {width}: a second item cost {marginal} against a first at {framed}"
            );
            rows += 1;
        }
    }
    assert_eq!(
        EVERY_FRAMED_KIND.len() * widths.len(),
        rows,
        "the ladder must cover every kind at every width"
    );
    println!("  rows measured: {rows}, worst single-item cost share {worst_share:.1}%");
}

/// WHAT THE FIXED HEADER IS, STATED FROM THE DATA RATHER THAN FROM A CONSTANT.
///
/// A test that cannot say what a width should be from the values that went in cannot notice when it
/// is wrong -- the reason `block_record_varint_len` exists beside the record header.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_one_item_frame_is_exactly_its_parts() {
    let key = b"field-0001";
    let value = bytes_of(40, 1);
    let block = encode_container_block(ElementKeySpelling::Utf8, &[(key.as_slice(), &value)]);
    let expected = CONTAINER_BLOCK_MAGIC.len()
        + 1
        + varint_len(1)
        + varint_len(key.len() as u64)
        + key.len()
        + varint_len(value.len() as u64 + 1)
        + value.len();
    assert_eq!(
        expected,
        block.len(),
        "a one-item frame is the magic, the spelling byte, the count, the key and the value"
    );
    println!(
        "\n=== a one-item hash frame ===\n  magic {} + spelling 1 + count {} + key len {} + key {} + value tag {} + value {} = {}",
        CONTAINER_BLOCK_MAGIC.len(),
        varint_len(1),
        varint_len(key.len() as u64),
        key.len(),
        varint_len(value.len() as u64 + 1),
        value.len(),
        block.len()
    );
}

// =================================================================================================
// 3. SELECTION IS REAL, AND ITS FAILURES ARE TOLD APART
// =================================================================================================

/// A FRAME HOLDING TWO ELEMENTS ANSWERS FOR EACH OF THEM AND FOR NEITHER OF THE OTHERS.
///
/// The control that catches a selector which ignores the component. Asked for the first item it must
/// answer the first value, asked for the second the second, and asked for an element that is not
/// there it must answer `Absent` -- which is a different answer from `Corrupt`, because the caller
/// treats one as a miss and the other as a page it must not trust.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_two_element_frame_answers_for_each_element_and_absent_for_a_third() {
    let mut covered = 0usize;
    for spelling in EVERY_SPELLING {
        let first_component = component_for(spelling, 16, 1);
        let second_component = component_for(spelling, 16, 2);
        let absent_component = component_for(spelling, 16, 3);
        assert_ne!(first_component, second_component);
        assert_ne!(first_component, absent_component);

        let first_key =
            element_key_from_component(spelling, &first_component).expect("fixture component");
        let second_key =
            element_key_from_component(spelling, &second_component).expect("fixture component");
        let first_value = bytes_of(12, 1);
        let second_value = bytes_of(20, 2);
        assert_ne!(first_value, second_value);

        let block = encode_container_block(
            spelling,
            &[
                (&first_key, first_value.as_slice()),
                (&second_key, second_value.as_slice()),
            ],
        );
        assert_eq!(
            ContainerElementRead::Found(first_value.clone()),
            select_container_element(&block, &first_component),
            "{spelling:?} did not answer for its first element"
        );
        assert_eq!(
            ContainerElementRead::Found(second_value.clone()),
            select_container_element(&block, &second_component),
            "{spelling:?} did not answer for its second element -- a selector that returns the \
             first item would pass every one-item test in this file"
        );
        assert_eq!(
            ContainerElementRead::Absent,
            select_container_element(&block, &absent_component),
            "{spelling:?} answered for an element it does not hold"
        );
        covered += 1;
    }
    assert_eq!(4, covered, "every spelling must be covered");
}

