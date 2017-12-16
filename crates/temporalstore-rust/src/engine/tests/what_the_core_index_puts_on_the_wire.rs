// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE CORE INDEX PUTS ON THE WIRE, ENUMERATED BY THE COMPILER AND NOT BY A COUNT.
//!
//! # WHY THIS EXISTS BEFORE THE CHANGE IT GUARDS
//!
//! `Serialize for CoreIndex` is DERIVED today. A proposal to let the page index be serialized with
//! a sibling field in scope -- so that a term which has left the entry can still be written --
//! needs that impl to be HAND-WRITTEN, because serde hands a `Serialize` impl no context and a
//! bucket's serializer therefore cannot see anything but the bucket.
//!
//! A HAND-WRITTEN IMPL THAT OMITS A FIELD COMPILES AND WRITES A NARROWER WIRE FORMAT. That is the
//! worst failure available here: the encode succeeds, the file is accepted, and what is missing
//! only surfaces when something tries to read it back. So the guard is not a nicety to add
//! afterwards -- it is what makes the change safe to make, and it is therefore landed FIRST,
//! against the derive, while the derive is still the thing being described.
//!
//! NOTHING HERE CHANGES THE IMPL, SO NO BYTES MOVE AND NO FORMAT STAMP IS OWED. This file only
//! observes what the current impl emits. Whoever replaces the derive owes the stamp question
//! separately, and owes it on whether the BYTES changed rather than on whether the code did: an
//! impl emitting the identical shape owes nothing, while one that reorders or renames a key does.
//!
//! # THE GUARD IS A DESTRUCTURING, NOT A NUMBER
//!
//! A field count is a literal that goes stale silently. An EXHAUSTIVE DESTRUCTURING does not: a
//! field added to `CoreIndex` makes the pattern below fail to compile, naming the field it does
//! not mention. So the enumeration is maintained by the compiler, and every field is forced to
//! declare in one place whether it reaches the wire. `ShardState` already carries this shape for
//! its own packing pass, with the skipped fields annotated in the pattern -- see the destructuring
//! around `engine_storage_state.rs:5765`.
//!
//! # AND THE WIRE IS OBSERVED, NOT READ OFF THE ATTRIBUTES
//!
//! `#[serde(skip)]` on a field is a claim about the wire; the wire is what the encoder emits. So
//! the key set is taken from a real encode of a populated index, and the five skipped fields are
//! POPULATED BEFORE ENCODING and asserted to come back at their defaults. That population is the
//! control: without it, "the skipped fields come back empty" would pass on an index where they
//! were empty all along, which is a test that cannot fail.
//!
//! SIX FIELDS, AND EXACTLY ONE REACHES THE WIRE. That measurement is the reason the hand-written
//! impl is a small change rather than a thirty-field transcription: the omission hazard applies to
//! one field, and the context it needs -- the object lookup -- is a sibling field of the same
//! struct, so there is nothing to plumb through.

use std::sync::Arc;

use crate::engine::state::{BucketNode, BucketTtl, CoreIndex};

/// A `CoreIndex` with EVERY ONE of its six fields non-default.
///
/// The on-wire field is given two buckets with distinguishable contents so the round trip has
/// something to compare field by field. The five skipped fields are populated for the reason in
/// the module docs: they are the control that makes "skipped" an observation.
fn an_index_with_every_field_populated() -> CoreIndex {
    let mut index = CoreIndex::default();

    let mut first = BucketNode::default();
    first.routing_bucket = 7;
    first.ttl_ms = BucketTtl::from_ms(Some(1_234));
    first.dirty_generation = 42;
    first.set_meta_loaded(true);
    first.set_in_memory(true);

    let mut second = BucketNode::default();
    second.routing_bucket = 9;
    second.ttl_ms = BucketTtl::ABSENT;
    second.dirty_generation = 7;
    second.set_meta_loaded(true);
    second.set_dirty(true);

    index.bucket_map.insert(7, first);
    index.bucket_map.insert(9, second);

    // The five skipped fields, each made non-default so the round trip below can tell "skipped"
    // from "was empty anyway".
    index.block_slab_live.mark_ready();
    let model: Arc<str> = Arc::from("string");
    let object: Arc<str> = Arc::from("an-object");
    index.object_block_lookup.file(
        &model,
        &object,
        crate::engine::state::BlockLookupRef {
            routing_bucket: 7,
            block_ref_key: 99,
        },
    );
    index.kind_pool.insert(Arc::clone(&model));
    index.object_component_block_refs = Some(3);
    index.released_buckets.insert(11);

    index
}

#[test]
fn every_field_of_the_core_index_declares_whether_it_reaches_the_wire() {
    let index = an_index_with_every_field_populated();

    // ===========================================================================================
    // THE GUARD. A field ADDED to `CoreIndex` fails to compile HERE, naming itself, rather than
    // being silently dropped from a hand-written encoder's output. Nothing is bound with `..`,
    // which is the whole mechanism: `..` would make this pattern accept a field set it has never
    // been told about.
    // ===========================================================================================
    let CoreIndex {
        // ON THE WIRE -- the only one. Everything a stored index carries from this struct is
        // inside this field.
        bucket_map,
        // SKIPPED, `#[serde(skip)]`. Derived per-slab live tallies, seeded wherever the index is
        // rebuilt wholesale.
        block_slab_live,
        // SKIPPED, `#[serde(default, skip_serializing)]` -- DECODED IF PRESENT BUT NEVER WRITTEN,
        // which is a different thing from `skip` and is why it is annotated separately. Rebuilt
        // from the bucket map on load.
        object_block_lookup,
        // SKIPPED, `#[serde(skip)]`. An interning pool for the model spellings.
        kind_pool,
        // SKIPPED, `#[serde(skip)]`. A derived total, `None` meaning not established.
        object_component_block_refs,
        // SKIPPED, `#[serde(skip)]`. Which buckets have had their resident blocks released.
        released_buckets,
    } = &index;

    // The populated-ness of all six is asserted, so a future edit that empties the fixture turns
    // the controls below vacuous LOUDLY rather than quietly.
    assert_eq!(bucket_map.len(), 2, "the on-wire field must be populated");
    assert!(block_slab_live.is_ready(), "skipped field must be populated");
    assert!(
        object_block_lookup.get("string", "an-object").is_some(),
        "skipped field must be populated"
    );
    assert!(!kind_pool.is_empty(), "skipped field must be populated");
    assert_eq!(
        *object_component_block_refs,
        Some(3),
        "skipped field must be populated"
    );
    assert!(!released_buckets.is_empty(), "skipped field must be populated");

    // ===========================================================================================
    // THE WIRE, OBSERVED. One key, and it is the bucket map.
    // ===========================================================================================
    let encoded = serde_json::to_value(&index).expect("a core index serializes");
    let object = encoded.as_object().expect("a core index encodes as a map");
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    println!("the core index puts these keys on the wire: {keys:?}");
    assert_eq!(
        keys,
        vec!["bucket_map"],
        "exactly one of the six fields reaches the wire; a change here is a change to the \
         stored shape and owes the format stamp question"
    );
}

#[test]
fn an_encode_then_decode_keeps_every_wire_field_and_drops_every_omitted_one() {
    let before = an_index_with_every_field_populated();

    // A ROUND TRIP AND NOT A BYTE GOLDEN. A golden records whatever today's encoder emits, so it
    // moves with any change to the encoder and cannot say whether the change was lossless. Encode
    // then decode, and compare what came back against what went in.
    let encoded = serde_json::to_string(&before).expect("a core index serializes");
    let after: CoreIndex = serde_json::from_str(&encoded).expect("a core index deserializes");

    // THE ON-WIRE FIELD, COMPARED TERM BY TERM rather than by a length or a debug string.
    assert_eq!(
        after.bucket_map.len(),
        before.bucket_map.len(),
        "the bucket count must survive"
    );
    for (routing_bucket, original) in &before.bucket_map {
        let reloaded = after
            .bucket_map
            .get(routing_bucket)
            .unwrap_or_else(|| panic!("bucket {routing_bucket} must survive the round trip"));
        assert_eq!(reloaded.routing_bucket, original.routing_bucket);
        assert_eq!(reloaded.ttl_ms.ms(), original.ttl_ms.ms());
        assert_eq!(reloaded.dirty_generation, original.dirty_generation);
        assert_eq!(reloaded.dirty(), original.dirty());
        assert_eq!(reloaded.deleted(), original.deleted());
        assert_eq!(reloaded.meta_loaded(), original.meta_loaded());
        assert_eq!(reloaded.loading(), original.loading());
        assert_eq!(reloaded.in_memory(), original.in_memory());
        assert_eq!(reloaded.layout, original.layout);
        assert_eq!(reloaded.block_index.len(), original.block_index.len());
        assert_eq!(
            reloaded.object_index.object_count(),
            original.object_index.object_count()
        );
    }

    // THE SKIPPED FIELDS, AND THIS IS THE HALF THE POPULATION ABOVE EXISTS FOR. Each went in
    // non-default and must come back at its default, which is what "skipped" means on the wire.
    assert!(
        !after.block_slab_live.is_ready(),
        "a skipped derived tally must not survive a round trip"
    );
    // THE LOOKUP IS STILL NOT WRITTEN, AND IT IS NO LONGER TRUE THAT A DECODE LEAVES IT EMPTY.
    //
    // It is `skip_serializing`, so nothing about it reaches the bytes -- that half is unchanged.
    // What changed is the other side: the decoder now FILES each decoded page into it, because the
    // walk that used to do that afterwards reads each page's identity off the entry and the entry
    // is about to have none. This fixture's two buckets hold no pages, so there is nothing to file
    // and it comes back empty -- which makes this assertion a statement about the FIXTURE now and
    // not about the field. `a_decode_files_every_block_into_the_object_lookup` is where the filling
    // is asserted, on an index that has pages.
    assert!(
        after.object_block_lookup.is_empty(),
        "nothing was written and this fixture has no page to file, so nothing can come back"
    );
    assert!(
        after.kind_pool.is_empty(),
        "a skipped interning pool must not survive"
    );
    // AND THE DERIVED TOTAL IS ESTABLISHED BY THE DECODE, WHERE IT USED TO BE LEFT UNKNOWN.
    //
    // This read `None`, under "a skipped derived total must come back unestablished rather than
    // zero". That was right while a rebuild ran after every load and was the only thing that could
    // establish it. The decode establishes the lookup now, so it must establish the count with it:
    // `insert_object_block_lookup` increments only `if let Some(total)`, so a total left at `None`
    // can never be incremented into a right answer, and a lookup that is established beside a
    // count that is not is the exact pair that made the stats path walk the shard.
    //
    // ZERO HERE AND NOT UNKNOWN, because this fixture files no page. The value is asserted as
    // `Some` and as the number of refs actually filed, so a decoder that forgot to establish it
    // fails and so does one that invents a count.
    assert_eq!(
        after.object_component_block_refs,
        Some(0),
        "the decode must establish the derived total, at the number of refs it filed"
    );
    assert!(
        after.released_buckets.is_empty(),
        "a skipped release record must not survive"
    );

    // NOT ASSERTED, AND DELIBERATELY: a bucket's two first-dirty sequence numbers. The node's own
    // visitor forces them to zero on every decode -- "Transient by declaration", at its closing
    // construction -- so asserting they survive would assert the opposite of the documented
    // behaviour, and asserting they are zero would restate the decoder to itself.
}

// =================================================================================================
// WHETHER THE ENTRY CAN LOSE ITS IDENTITY WITHOUT THE BYTES MOVING -- SETTLED HERE RATHER THAN
// REASONED ABOUT, BECAUSE THE TIDY VERSION OF THE STORY IS WRONG IN TWO PLACES.
//
// THE STORY UNDER TEST. The stored page index is STRING-KEYED and the key contains the object key,
// so the hope is that the encoder can keep writing the same bytes while sourcing the identity from
// the sibling object lookup, and the decoder can recover the identity from the map key. If that
// held, the entry could shed its field with nothing on disk moving and no format stamp owed.
//
// IT DOES NOT HOLD, AND THE TWO TESTS BELOW ARE WHY.
//
//   1. THE OBJECT KEY IS A MEMBER OF THE ENTRY ON THE WIRE. `kind` and `routing_bucket` carry
//      `#[serde(skip)]`; the object key does not. So removing the field removes a member from
//      every stored entry -- that IS a byte change -- unless the encoder keeps emitting it from
//      somewhere else.
//
//   2. THE MAP KEY IS NOT SELF-DELIMITING, so the decoder cannot be handed the job instead. The
//      stored key is `kind:object:component:slab:offset:length:block_id:generation` joined with
//      bare colons, and an object key MAY CONTAIN A COLON -- a context entity is filed under
//      `{collection_key}:{entity_hash}`, stated twice in `storage_bucket_internals.rs` at `:2754`
//      and `:3195`. The second test constructs two DIFFERENT (object key, component) pairs that
//      produce the IDENTICAL key, which is the proof that no parser can invert it in general.
//      The lookup's own flat key is length-prefixed for exactly this reason; this one is not.
//
// WHAT THAT LEAVES, STATED SO THE NEXT READER DOES NOT RE-DERIVE IT. With `component` fixed at
// `None` -- which every entry's is today -- the trailing five terms are numeric and a RIGHT-ANCHORED
// parse would work. That is a property held at a distance rather than one the format enforces, and
// the collision below is what it is being held against. So "recover the identity from the key" is
// available only while a property the format does not check continues to hold.
//
// SO THE ROUTE THAT KEEPS THE BYTES IDENTICAL IS THE THIRD ONE: the entry sheds the FIELD, the
// encoder keeps emitting the MEMBER from the sibling lookup, and the decoder reads that member into
// a temporary it uses for the handle and the lookup rebuild without storing it. Nothing on disk
// moves and no stamp is owed -- and the cost is that both directions become hand-written, not just
// the encoder.
//
// # AND A FOURTH ROUTE WAS TAKEN INSTEAD: THE MEMBER LEAVES AND THE STAMP PAYS FOR IT
//
// Everything above is unchanged as analysis and one of its conclusions no longer describes the
// tree. #2350 measured the member at 30.0 B/object of stream and 2.34 to 2.96 B/object of the
// shipped container -- so "that IS a byte change" stopped being a reason not to do it and became a
// price. `SHARD_INDEX_FORMAT_VERSION` moved 26 -> 27 and the entry writes three members, not five:
// `model_id` went with `object_key`, for the same reason and at a further 20.0 B/object.
//
// THE RIGHT-ANCHORED PARSE IS WHAT THE DECODER DOES NOW, and the property it is held against is no
// longer held at a distance. `index_log::block_ref_key_parts` cuts the six right-hand fields from
// the RIGHT and the kind from the LEFT, so an object key keeps every colon it arrived with --
// `{collection_key}:{entity_hash}` included -- and
// `the_key_a_block_is_filed_under_inverts_exactly::the_renderer_and_the_parse_are_inverse_on_every_block_of_the_shipped_corpus`
// asserts the component slot EMPTY and the parse exact on all 8,000 pages of two key shapes, rather
// than relying on it.
//
// THE COLLISION BELOW IS STILL REAL AND IS STILL THE LIMIT, which is why that test keeps its name
// and its claim. A non-empty component containing a colon makes the component/object-key boundary
// unrecoverable, and a positional parse resolves it one way: the key the test builds reads back as
// the object `a:b` with no component, which is one of its two identities and not the other. No
// writer in this tree composes a page key with a component -- `block_index_written_key` passes a
// literal `None` -- so no index this binary writes can contain the ambiguous shape; an index that
// DOES carry one predates the stamp and is refused by `persistence`'s `<` comparison on every one
// of `decode_index_bytes`'s six production callers before anything acts on it. That is the
// narrowness this block warned about, now written down as the boundary of a shipped decision
// rather than as a reason not to make one.
// =================================================================================================

/// THE OBJECT KEY IS NO LONGER A MEMBER OF THE ENTRY, AND THIS TEST'S CLAIM INVERTED WITH IT.
///
/// It was `a_page_entry_carries_its_object_key_as_a_member_on_the_wire` and it asserted exactly
/// that -- the ground for "removing the field removes a member from every stored entry, which IS a
/// byte change". The byte change has been made and paid for with `SHARD_INDEX_FORMAT_VERSION` 27,
/// so the member is gone and the test is RENAMED rather than re-goldened: a name that asserts the
/// opposite of what the body checks is worse than a stale golden.
///
/// WHAT IT ASSERTS NOW IS THE EXACT MEMBER SET, which is more than the old `contains` did. Three
/// members and no more, with the two historically skipped terms still absent -- so a member
/// arriving back, or a fourth arriving for the first time, fails here. And the stored key is
/// asserted to carry the characters, which is the half that has to stay true for the decoder's
/// parse to have a source at all.
#[test]
fn a_block_entry_no_longer_carries_its_object_key_as_a_member_on_the_wire() {
    use crate::engine::state::{BlockIndex, BlockSlabLiveIndex};

    let mut bucket = BucketNode::default();
    bucket.routing_bucket = 3;
    bucket.set_meta_loaded(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_block(&"an-object", 
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            dirty: false,
            deleted: false,
        },
        &mut live,
    );

    let encoded =
        serde_json::to_value(&crate::engine::state::NodeNamedBy(&bucket, &["an-object"])).expect("a bucket node serializes");
    let block_index = encoded
        .get("page_index")
        .and_then(serde_json::Value::as_object)
        .expect("a node writes its page index as a map");
    assert_eq!(block_index.len(), 1, "one page was inserted");
    let (stored_key, entry) = block_index.iter().next().expect("one entry");
    let entry = entry
        .as_object()
        .expect("a page entry is written as a map");
    let mut members: Vec<&str> = entry.keys().map(String::as_str).collect();
    members.sort_unstable();
    println!("a page entry writes these members: {members:?}");
    println!("under this stored key: {stored_key}");

    // THE CLAIM, AS AN EXACT SET. The object key and the kind are the composite key's second and
    // first slots and the decoder reads them back out of it, so neither is a member any more --
    // and the member count is part of the encoding, because the served index is msgpack
    // `with_struct_map` and `serialize_struct` writes a map header of exactly this length.
    assert_eq!(
        vec!["address", "deleted", "dirty"],
        members,
        "the stored member set of a page entry moved; it is three at \
         `SHARD_INDEX_FORMAT_VERSION` 27 and the arity is part of the encoding, so a member \
         arriving or leaving owes another stamp step"
    );
    // AND THE TWO HISTORICALLY SKIPPED TERMS ARE STILL ABSENT, which is the control showing the
    // list distinguishes skipped from written rather than being empty of everything.
    assert!(
        !members.contains(&"kind"),
        "`kind` is skipped, so its absence is what makes the set above informative"
    );
    assert!(
        !members.contains(&"routing_bucket") && !members.contains(&"routing_slot"),
        "the routing bucket is skipped and injected by the node, so it is absent too"
    );
    assert!(
        !members.contains(&"object_key") && !members.contains(&"model_id"),
        "the entry writes its own copy of an identity the key already spells: {members:?}"
    );

    // AND THE STORED KEY CARRIES THE CHARACTERS TOO, which is the half the hopeful story rested on.
    assert!(
        stored_key.contains("an-object"),
        "the stored key contains the object key: {stored_key}"
    );
}

#[test]
fn the_stored_block_key_is_not_self_delimiting_so_no_parser_can_invert_it() {
    use crate::index_log::block_ref_key_from_parts;

    // TWO DIFFERENT IDENTITIES, ONE KEY. The separator is a bare colon and both the object key and
    // the component may contain one, so the boundary between them is not recoverable.
