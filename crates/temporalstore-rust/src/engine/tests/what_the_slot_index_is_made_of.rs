// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE BYTES PER OBJECT INSIDE `slot_index` ARE, TERM BY TERM.
//!
//! # THIS MODULE'S HEADLINE PROPOSAL HAS LANDED, AND THE FIGURE IT DECOMPOSES MOVED WITH IT
//!
//! Read the rest of this header as the 226.35 B/object state it was written against. It is
//! **176.35 B/object** now, measured on the same corpus and the same range, and the step is
//! EXACTLY the 50.0 B/object this module predicted for two of its own terms -- `entry/object_key`
//! at 30.0 and `entry/model_id` at 20.0. A page entry no longer restates the `object_key` and
//! `model_id` that the composite key it is filed under already spells; the decoder parses the key
//! (`index_log::block_ref_key_parts`), which is the load-path change named below as the whole cost
//! of taking that direction, and `SHARD_INDEX_FORMAT_VERSION` moved 26 -> 27.
//!
//! THE PER-PAGE CLASS CARRIES THE WHOLE OF THE STEP: 177.015 -> 127.015 B/object, with per-bucket
//! at 49.331 and per-shard at 0.004 unmoved. Four things in this module changed with it, each
//! named where it is:
//!
//!   * `entry/object_key` and `entry/model_id` are GONE from `TERMS`, and an entry that writes
//!     either lands in the `no-unclassified` panic.
//!   * `page/map_key/kind` and `page/map_key/object_key` are `Identity` rather than `Redundancy`:
//!     the key is the only copy, and there is no reconstructor left to name.
//!   * `key-restates-the-entry` is retired and `address-restates-the-key` stands in its place --
//!     the surviving duplication runs the other way.
//!   * `the_page_keys_characters_are_not_read_back_by_the_decoder` is
//!     `the_block_keys_characters_are_read_back_by_the_decoder_now`: a handle-keyed page index is
//!     REFUSED where it used to decode byte-identically.
//!
//! AND THE THIRD FIELD DID NOT GO. `address` is still written twice and will stay: its five VALUES
//! are in the key, the two PRESENCE bits beside them are not, and
//! `the_key_a_block_is_filed_under_inverts_exactly::the_presence_bits_of_an_address_are_not_in_the_key_it_renders`
//! builds the colliding states and asserts the keys identical. Its container price is still
//! measured here, because pricing a block is what makes it a decision.
//!
//! WHY THIS EXISTS. On the routing range a deployment runs, `0..1023`, `slot_index` was 226.35 B
//! per stored object and 79.30% of the served index on the one stream that partitions exactly --
//! both
//! taken as a division at 8,000 objects, which is the quantity named throughout and not the 274.75
//! B/object SLOPE published beside it for the whole stream. That share was established without
//! anything being said about what is inside it. "The index is big and `slot_index` is most of it" is
//! not a number anyone can act on. This module names every byte of the 226, says how each term
//! scales, says which terms are reconstructible from other terms, and prices the removals on the
//! artefact that lands rather than only on the stream that names fields.
//!
//! # THE THREE SURFACES, NAMED AT EVERY NUMBER
//!
//!   * `json_stream` -- the struct's JSON serialization. The one stream that PARTITIONS exactly: a
//!     JSON object is one opening brace plus, per entry, its quoted name, a colon, its value and
//!     one separator byte (a comma, or the closing brace for the last). Every per-term attribution
//!     here is on this and nowhere else.
//!   * `msgpack_payload` -- the pre-compression payload the default codec writes, pulled back out
//!     of the shipped container by decompressing it. Reported as a total; no term is attributed on
//!     it, because this module's partition identity is a JSON identity.
//!   * `container` -- what lands on disk: `TSIDX\x01`, the codec id, the stamp, and a ZSTD FRAME.
//!     **A byte figure on this cannot name a field.** What is taken on it is a DIFFERENCE between
//!     two whole artefacts -- the index as written and the index with one term removed -- which
//!     measures the thing varied instead of attributing bytes, and is labelled as such everywhere.
//!
//! # WHAT IT MEASURED
//!
//! THE 50 CHARGED TERMS SUM TO 1,810,793 B AT 8,000 OBJECTS IN 1,024 BUCKETS, which is 226.349
//! B/object and 79.30% of the 285.43 B/object stream -- the published figures, reproduced by a
//! partition rather than by a division of the whole. Nothing is a remainder and nothing is unnamed.
//! 51 terms are DECLARED; the 51st is `node/tombstone_elements`, the one key the encoder writes
//! only for a bucket that has had a removal, and its absence from this fixture is asserted rather
//! than tolerated. Nine of the 51 are redundancy, each with a named reconstructor and each
//! reconstruction asserted on every page or bucket of the corpus:
//!
//! ```text
//!     payload      407,144 B     50.893 B/object    22.48%
//!     identity     654,694 B     81.837 B/object    36.16%
//!     envelope     293,725 B     36.716 B/object    16.22%
//!     redundancy   455,230 B     56.904 B/object    25.14%
//! ```
//!
//! AND THE PUBLISHED 226.35 IS A DIVISION AT 8,000 OBJECTS, NOT A SLOPE. On this range those are
//! different quantities and the difference is not small: `slot_index` grows 273.87 B/object over
//! 500 -> 2,000 objects and 201.07 over 2,000 -> 8,000, because the bucket count SATURATES at
//! 1,024. A figure quoted without saying which of the two it is travels wrong, so both are measured
//! here, the division is the one pinned against the published value, and the arm says which is
//! which.
//!
//! THE WHOLE OF IT IS THREE SCALES AND NOT TWO:
//!
//! ```text
//!     per-page      1,416,120 B    177.015 B/object    78.20%    linear in the store
//!     per-bucket      394,644 B     49.331 B/object    21.79%    212.38 -> 49.33 as buckets saturate
//!     per-shard            29 B      0.004 B/object     0.00%    two field names and three braces
//! ```
//!
//! A taxonomy of "grows" and "bounded" has nowhere to put the middle class, and the middle class is
//! the whole of the difference between this range's figure and the unbounded one: it is the only
//! class that is not a line, and the three classes are asserted to account for the whole of each
//! half of the sweep exactly.
//!
//! AND THE CLASS OF EVERY TERM IS MEASURED, NEVER READ OFF ITS NAME. `slot_index/bucket_map` is
//! bucket-keyed and holds one entry per BUCKET; the `block_index` inside each of those holds one
//! entry per PAGE, 7.81 of them per bucket on this range -- so a name-based reading of the
//! structure under `bucket_map` is wrong by that factor. Every term declares a scale and the sweep
//! asserts it against the measured instance count at three store sizes: all 50 held, exactly.
//!
//! EVERY PER-PAGE FACT IS WRITTEN TWICE, AND THE TWO COPIES ARE NOT THE SAME PRICE. A page's
//! object, kind and address are written once inside its composite map key --
//! `{kind}:{object_key}:{component}:{slab}:{offset}:{length}:{block_id}:{generation}` -- and once
//! as the `object_key`, `model_id` and `address` fields of the entry filed under it:
//!
//! ```text
//!     the composite MAP KEY           41.008 B/object    18.12% of `slot_index`
//!     the ENTRY's three fields       101.007 B/object    44.62%
//!                                    ---------------    ------
//!                                    142.015 B/object    62.74%
//! ```
//!
//! The same five address values are 12.008 B/object as decimals inside the key and 53.008 B/object
//! as the named `address` object beside it -- 4.41x, and every byte of the difference is field
//! names, braces, separators and a nil slot rather than a value. JSON field names are 94.099
//! B/object of the 226, 41.57% of the whole.
//!
//! THE RECONSTRUCTION IS ASSERTED, NOT ARGUED. The key is rebuilt from the entry's OWN WRITTEN
//! BYTES -- the address decoded back through its production `Deserialize`, the string re-formatted
//! by this module rather than by `block_index_written_key`, which would have compared one renderer
//! with itself -- and matched character for character on all 8,000 pages. `node/routing_slot` is
//! rebuilt from the map key its bucket is filed under, on all 1,024.
//!
//! AND THE KEY'S CHARACTERS ARE NOT READ BACK. The decoder takes the page index as a
//! `BTreeMap<String, StoredBlockEntry>` and consumes it with `into_values()`, so what the map key
//! spells reaches nothing. Measured rather than read off the source: every page key in a 2,000-key
//! index is replaced by the decimal of its handle, fed to the production decoder, and re-encoded --
//! and the `slot_index` that comes back is byte-identical to the undoctored one's, with the
//! instrument proved able to fire by a one-character change to one entry's `object_key`.
//!
//! AND THE LARGEST PER-BUCKET TERM RESTATES THE PAGES BESIDE IT. `node/object_index` is 22.603
//! B/object, second only to the entry's `object_key`, and it holds the ids of the objects a bucket
//! carries. Those ids are a pure function of the shard, the kind and the object key -- all three of
//! which the page entries in the SAME BUCKET write -- so on all 1,024 buckets the written
//! `object_index` is EXACTLY the set derivable from its own pages. Derived here rather than through
//! `engine::hashing::stable_block_object_id`, which bumps two process-wide atomics per call in a
//! test build that other arms read and reset: a probe calling it 8,000 times would move a counter
//! it is not measuring.
//!
//! AND TWO SLOTS ARE WRITTEN FOR NOTHING ON EVERY PAGE. The composite's component slot is empty on
//! all 8,000 pages -- `block_index_written_key` passes a literal `None` -- so one of its seven
//! separators delimits a hole. And a stored address writes `oi` as null on all 8,000, because
//! `impl From<ElementEntry> for BlockAddressWire` fills it from a literal `None` too -- the type it
//! converts from has no such field to read: 9.000 B/object of JSON for a slot the only conversion
//! that builds one cannot fill.
//!
//! # WHAT A REMOVAL IS WORTH, AND THE BIGGEST-LOOKING ONE IS WORTH LESS THAN NOTHING
//!
//! Duplication is not byte cost: the container is a zstd frame, and a term's weight there is not
//! its weight on the stream. Every removal is therefore priced on both, at two key shapes and two
//! store sizes -- four points -- and the direction of each headline price is asserted at all four.
//!
//! ```text
//!     removal                          stream B/obj     container B/obj (padded -> scattered)
//!     page key -> the handle                 19.6           -5.14 to -6.77      COSTS
//!     page key -> drop the empty slot         1.0           +0.12 to -0.71      noise
//!     address -> drop the nil `oi`           10.0           +0.06 to +0.29      ~free
//!     entry -> drop `object_key`             30.0           +2.34 to +2.96
//!     entry -> drop `model_id`               20.0           +0.26 to +0.37
//!     entry -> drop `address`                54.0           +4.27 to +4.52
//!     entry -> drop all three                104.0          +6.72 to +7.17      PAYS
//! ```
//!
//! THE COMPOSITE KEY IS ALREADY FREE WHERE IT IS PAID. It is the most duplicated structure in the
//! index and also the most COMPRESSIBLE one -- sorted, sharing long prefixes, mostly repeating
//! decimal runs -- so replacing it with the hash the live map already keys the page by removes 19.6
//! B/object of stream and lands a container 5.1 to 6.8 B/object LARGER, on all four points. A
//! removal whose stream saving is real and whose artefact saving is negative is a measured
//! negative, and it is the one a reader of the 41 B/object figure would have reached for first.
//!
//! THE OTHER DIRECTION PAYS: 104.0 B/object of stream and 6.7 to 7.2 B/object of container out of a
//! shipped 29.1 to 41.5, which is 17% to 23% of the artefact. WHAT IT REQUIRES IS NAMED RATHER THAN
//! GLOSSED -- the decoder would have to PARSE the key, which today it does not, so this is a
//! load-path change and not a byte change, and the two sides are mutually exclusive: exactly one
//! copy of each fact may go.
//!
//! # NOTHING IS A REMAINDER, AND AN UNNAMED KEY IS A PANIC
//!
//! The term vocabulary is CLOSED. Every byte attributed goes through [`Tally::add`], which resolves
//! its label in [`TERMS`] and panics on a label that is not declared there; and every field name
//! the walk meets is dispatched through a `match` whose fallback is a panic naming the key. A
//! decomposition can be wrong in every term and still sum to the whole when its last term is
//! "everything else", because the remainder absorbs the error -- so there is no such term, and two
//! independent sum identities are asserted: the whole of `slot_index` against the sum of all terms,
//! and the page-level subtree against the sum of the page-level terms alone. The second is what
//! would catch a term charged at the wrong level, which is the error the 7.81x name would invite.
//!
//! Every term is asserted NON-ZERO except the one that is structurally empty, which is asserted
//! EXACTLY zero and named for it.
//!
//! NO PRODUCTION CODE CHANGES HERE AND NO STORED SHAPE MOVES, so no `SHARD_INDEX_FORMAT_VERSION`
//! step is owed.
//!
//! rust-internal: measures this crate's own index codec, no product behaviour

use super::*;
use std::collections::BTreeMap;

const SHARD: u64 = 41;

/// The shipped routing range's end bucket -- the default an operator is told to set, and the range
/// every headline figure here is taken on.
const SHIPPED_END_BUCKET: u32 = 1023;
/// The key width the published per-object index figures were taken at.
const PUBLISHED_WIDTH: usize = 14;
/// The store sizes every slope is fitted over. A per-key figure taken at one point over a hash
/// table is a sawtooth, so nothing here is divided out of a single reading.
const SIZES: [usize; 3] = [500, 2_000, 8_000];
/// The corpus the headline decomposition and the counterfactuals are taken on.
const HEADLINE_KEYS: usize = 8_000;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';

/// What a fixture key's leading bytes are made of. The content figures do not care; the CONTAINER
/// figures care enormously, so both ends are measured and neither is quoted alone.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run plus the ordinal -- the shape the published per-object figures were taken
    /// on, and near-perfectly compressible. A LOWER bound on every container figure.
    Padded,
    /// The same width with pseudorandom letters, so the keys share no structure. The UPPER bound.
    Scattered,
}

impl KeyShape {
    fn label(self) -> &'static str {
        match self {
            KeyShape::Padded => "padded",
            KeyShape::Scattered => "scattered",
        }
    }
}

fn pad_of(ordinal: usize, width: usize, shape: KeyShape) -> Vec<u8> {
    let pad = width - TAIL_WIDTH;
    match shape {
        KeyShape::Padded => vec![PAD_BYTE; pad],
        KeyShape::Scattered => {
            let mut state = (ordinal as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ 0xD1B5_4A32_D192_ED03;
            state |= 1;
            (0..pad)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    b'a' + (state % 26) as u8
                })
                .collect()
        }
    }
}

fn key_text(ordinal: usize, width: usize, shape: KeyShape) -> String {
    let mut text = String::from_utf8(pad_of(ordinal, width, shape))
        .expect("the padding is ASCII by construction");
    text.push_str(&format!("-{ordinal:06}"));
    assert_eq!(text.len(), width, "the key must be exactly the width asked for");
    text
}

// =================================================================================================
// THE CLOSED VOCABULARY
// =================================================================================================

/// HOW A TERM GROWS, AND THERE ARE THREE OF THESE AND NOT TWO.
///
/// A two-class taxonomy of "grows with the store" and "bounded" has nowhere to put a term that
/// grows with the BUCKET count on a range where buckets saturate: per object such a term falls
/// without ever reaching a constant, which is both sub-linear and unbounded. That class is where
/// the amortisation the shipped range exists for happens, so it is named.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
enum Scale {
    /// One instance per stored page. The page count is asserted against the object count.
    PerBlock,
    /// One instance per resident bucket.
    PerBucket,
    /// One instance in the whole index.
    PerShard,
}

impl Scale {
    fn label(self) -> &'static str {
        match self {
            Scale::PerBlock => "per-page",
            Scale::PerBucket => "per-bucket",
            Scale::PerShard => "per-shard",
        }
    }
}

/// WHAT A TERM IS, and `Redundancy` is only ever assigned where the reconstruction is ASSERTED
/// below rather than argued in prose.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Nature {
    /// A stored state value nothing else in the index states.
    Payload,
    /// A name or an address a reader needs in order to reach the thing.
    Identity,
    /// Structure: punctuation, and the JSON field names a named encoding spends on every entry.
    Envelope,
    /// Reconstructible, character for character, from another term of the same index. Every term
    /// in this class has its reconstruction asserted on every page or bucket of the corpus.
    Redundancy,
}

impl Nature {
    fn label(self) -> &'static str {
        match self {
            Nature::Payload => "payload",
            Nature::Identity => "identity",
            Nature::Envelope => "envelope",
            Nature::Redundancy => "redundancy",
        }
    }
}

/// EVERY TERM `slot_index` IS MADE OF, AND NOTHING ELSE MAY BE CHARGED.
///
/// `(label, scale, nature, what reconstructs it)`. [`Tally::add`] resolves its label here and
/// PANICS on one that is not declared, so a byte can only be attributed to a term somebody named.
/// The fourth column is empty for everything that is not `Redundancy`, and non-empty for
/// everything that is -- asserted by [`the_term_table_is_well_formed`].
#[allow(clippy::type_complexity)]
const TERMS: &[(&str, Scale, Nature, &str)] = &[
    // ---- per-shard: the two envelopes between the shard and the buckets ----
    ("slot_index/field_name", Scale::PerShard, Nature::Envelope, ""),
    ("slot_index/open_brace", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/field_name", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/field_tail", Scale::PerShard, Nature::Envelope, ""),
    ("bucket_map/open_brace", Scale::PerShard, Nature::Envelope, ""),
    // ---- per-bucket: the map entry, the node envelope and the node's own eleven fields ----
    ("bucket/map_key", Scale::PerBucket, Nature::Identity, ""),
    ("bucket/map_key_colon", Scale::PerBucket, Nature::Envelope, ""),
    ("bucket/map_entry_tail", Scale::PerBucket, Nature::Envelope, ""),
    ("node/open_brace", Scale::PerBucket, Nature::Envelope, ""),
    ("node/field_tails", Scale::PerBucket, Nature::Envelope, ""),
    (
        "node/routing_slot",
        Scale::PerBucket,
        Nature::Redundancy,
        "the bucket's own map key, which it is filed under",
    ),
    ("node/layout", Scale::PerBucket, Nature::Payload, ""),
    ("node/dirty", Scale::PerBucket, Nature::Payload, ""),
    ("node/deleted", Scale::PerBucket, Nature::Payload, ""),
    ("node/meta_loaded", Scale::PerBucket, Nature::Payload, ""),
    ("node/loading", Scale::PerBucket, Nature::Payload, ""),
    ("node/in_memory", Scale::PerBucket, Nature::Payload, ""),
    ("node/ttl_ms", Scale::PerBucket, Nature::Payload, ""),
    ("node/dirty_generation", Scale::PerBucket, Nature::Payload, ""),
    (
        "node/object_index",
        Scale::PerBucket,
        Nature::Redundancy,
        "the stable object id of each page entry in the same bucket, from its kind and object key",
    ),
    ("node/deleted_object_index", Scale::PerBucket, Nature::Identity, ""),
    ("node/tombstone_elements", Scale::PerBucket, Nature::Payload, ""),
    ("node/page_index/field_name", Scale::PerBucket, Nature::Envelope, ""),
    ("page_index/open_brace", Scale::PerBucket, Nature::Envelope, ""),
    // ---- per-page: the composite map key, split into the eight slots it composes ----
    ("page/map_key_quotes", Scale::PerBlock, Nature::Envelope, ""),
    // AND THESE TWO STOPPED BEING REDUNDANCY, WHICH IS THE WHOLE OF WHAT THIS MODULE'S HEADLINE
    // PROPOSAL BOUGHT. They were classed `Redundancy` with the entry's own fields named as their
    // reconstructor, because the entry wrote both again. It does not: the decoder parses them out
    // of this key now (`index_log::block_ref_key_parts`), so the key is the ONLY copy and the only
    // copy of a fact a reader needs is `Identity`. A term is only `Redundancy` where a
    // reconstructor is NAMED, and there is nothing left to name.
    ("page/map_key/kind", Scale::PerBlock, Nature::Identity, ""),
    ("page/map_key/object_key", Scale::PerBlock, Nature::Identity, ""),
    (
        "page/map_key/component_slot",
        Scale::PerBlock,
        Nature::Envelope,
        "",
    ),
    (
        "page/map_key/block_slab_id",
        Scale::PerBlock,
        Nature::Redundancy,
        "the entry's own `address.a` word, high half",
    ),
    (
        "page/map_key/offset",
        Scale::PerBlock,
        Nature::Redundancy,
        "the entry's own `address.a` word, low half",
    ),
    (
        "page/map_key/length",
        Scale::PerBlock,
        Nature::Redundancy,
        "the entry's own `address.l` slot",
    ),
    (
        "page/map_key/block_id",
        Scale::PerBlock,
        Nature::Redundancy,
        "the entry's own `address.pi` slot",
    ),
    (
        "page/map_key/generation",
        Scale::PerBlock,
        Nature::Redundancy,
        "the entry's own `address.g` slot",
    ),
    ("page/map_key/separators", Scale::PerBlock, Nature::Envelope, ""),
    ("page/map_key/empty_slot_separator", Scale::PerBlock, Nature::Envelope, ""),
    ("page/map_key_colon", Scale::PerBlock, Nature::Envelope, ""),
    ("page/map_entry_tail", Scale::PerBlock, Nature::Envelope, ""),
    // ---- per-page: the entry the key is filed against ----
    ("entry/open_brace", Scale::PerBlock, Nature::Envelope, ""),
    ("entry/field_tails", Scale::PerBlock, Nature::Envelope, ""),
    // `entry/object_key` AND `entry/model_id` ARE GONE FROM THIS TABLE BECAUSE THEY ARE GONE FROM
    // THE INDEX. They were 30.0 and 20.0 B/object of stream and the two cheapest halves of the
    // 104.0 this module priced; the entry writes three keys now. Deleted rather than left at zero:
    // `Tally::add` panics on a label the table does not declare, so a term kept here would be a
    // term something could still be charged to, and `the_slot_index_term_table_is_well_formed`
    // asserts every declared term is non-zero except the one named structurally empty.
    ("entry/address/field_name", Scale::PerBlock, Nature::Envelope, ""),
    ("entry/address/open_brace", Scale::PerBlock, Nature::Envelope, ""),
    ("entry/address/field_tails", Scale::PerBlock, Nature::Envelope, ""),
    ("entry/address/a", Scale::PerBlock, Nature::Identity, ""),
    ("entry/address/l", Scale::PerBlock, Nature::Payload, ""),
    ("entry/address/pi", Scale::PerBlock, Nature::Identity, ""),
    ("entry/address/oi", Scale::PerBlock, Nature::Identity, ""),
    ("entry/address/g", Scale::PerBlock, Nature::Identity, ""),
    ("entry/dirty", Scale::PerBlock, Nature::Payload, ""),
    ("entry/deleted", Scale::PerBlock, Nature::Payload, ""),
];

/// The one term that is structurally empty and is asserted EXACTLY zero rather than non-zero.
const STRUCTURALLY_EMPTY: &str = "page/map_key/component_slot";

/// The declared scale and nature of a label, or a panic naming the label.
fn declared(label: &str) -> (Scale, Nature, &'static str) {
    TERMS
        .iter()
        .find(|(name, ..)| *name == label)
        .map(|(_, scale, nature, reconstructor)| (*scale, *nature, *reconstructor))
        .unwrap_or_else(|| {
            panic!(
                "[CLAUSE no-unclassified] `{label}` is charged bytes and is not declared in \
                 `TERMS`, so this decomposition has a term nobody named and its sum would be \
                 arithmetic over an unknown set"
            )
        })
}

/// One term's reading: its bytes, how many instances were charged, and how many of its bytes are
/// the JSON field name a named encoding spends on it.
#[derive(Clone, Copy, Default, Debug)]
struct Row {
    bytes: usize,
    instances: usize,
    name_bytes: usize,
}

/// The accumulator. Every byte of `slot_index` goes through [`Tally::add`] exactly once.
#[derive(Default)]
struct Tally {
    rows: BTreeMap<&'static str, Row>,
}

impl Tally {
    /// Charge `bytes` to `label`, of which `name_bytes` are the JSON key and its colon.
    fn add(&mut self, label: &'static str, bytes: usize, name_bytes: usize) {
        declared(label);
        let row = self.rows.entry(label).or_default();
        row.bytes += bytes;
        row.instances += 1;
        row.name_bytes += name_bytes;
    }

    fn total(&self) -> usize {
        self.rows.values().map(|row| row.bytes).sum()
    }

    /// The bytes charged to every term at a given scale.
    fn at_scale(&self, scale: Scale) -> usize {
        self.rows
            .iter()
            .filter(|entry| declared(entry.0).0 == scale)
            .map(|entry| entry.1.bytes)
            .sum()
    }

    fn of_nature(&self, nature: Nature) -> usize {
        self.rows
            .iter()
            .filter(|entry| declared(entry.0).1 == nature)
            .map(|entry| entry.1.bytes)
            .sum()
    }

    fn bytes_of(&self, label: &str) -> usize {
        self.rows.get(label).map(|row| row.bytes).unwrap_or(0)
    }

    /// The bytes charged to a named GROUP of terms, refusing a label that was never charged -- so a
    /// group cannot quietly shrink because one of its members was renamed.
    fn sum_of(&self, labels: &[&str]) -> usize {
        labels
            .iter()
            .map(|label| {
                let row = self.rows.get(label).unwrap_or_else(|| {
                    panic!(
                        "[CLAUSE group-is-whole] `{label}` is named in a group total and was never \
                         charged, so the group is summing over a term that is not in the index"
                    )
