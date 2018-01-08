// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT STANDS BETWEEN `slot_index` AND A FIXED-WIDTH FIFTY-BYTE ENTRY, PRICED ON THE ARTEFACT
//! THAT LANDS ON DISK.
//!
//! # WHY THIS EXISTS BESIDE [`super::what_the_slot_index_is_made_of`]
//!
//! That module partitions the JSON stream into terms and prices a removal as a difference between
//! two whole artefacts. It names two things it did not settle, and both of them are the reason a
//! fixty-byte entry is a design question rather than a shaving exercise:
//!
//!   * JSON FIELD NAMES ARE THE LARGEST SINGLE CROSS-CUT, and the alternative -- a positional
//!     encoding, in which a field is a slot rather than a name -- was DECLINED in a comment on
//!     `engine::encode_index_msgpack` rather than measured: "struct-as-MAP, not struct-as-array:
//!     the array form is positional, and positional is what mis-reads a struct that skipped an
//!     absent optional". That is a statement about the LOAD path. It says nothing about what the
//!     form is WORTH, and a decline with no price beside it cannot be re-opened by anyone. This
//!     module prices it, on the pre-compression payload AND on the shipped container, at two key
//!     shapes and two store sizes.
//!
//!   * `node/object_index` IS DECLARED `Redundancy` with the pages of its own bucket named as its
//!     reconstructor, and the reconstruction is asserted on all 1,024 buckets of a corpus in which
//!     every bucket is resident. A RELEASED bucket is not: `release_bucket_blocks` empties
//!     `block_index` and KEEPS `object_index`, and production says in as many words that the
//!     keeping "is the only thing that tells a released bucket from one legitimately holding
//!     nothing". So the set has a state in which no page names its members, and a term that cannot
//!     be rebuilt in a state the engine reaches is not reconstructible -- it is floor. Built here
//!     rather than argued.
//!
//! # THE SURFACE EVERY NUMBER NAMES
//!
//! Three, exactly as that module names them, and nothing here is quoted without one:
//!
//!   * `msgpack_payload` -- what the default codec writes BEFORE compression. This is the one
//!     surface on which a field NAME is a byte, so it is where the positional form's saving is
//!     attributable at all.
//!   * `container` -- `TSIDX\x01`, the codec id, the stamp and a ZSTD FRAME. What lands on disk,
//!     and the only figure a deployment pays. A byte count here CANNOT name a field; what is taken
//!     on it is a DIFFERENCE between two whole artefacts, which measures the thing varied.
//!   * `json_stream` -- not re-partitioned here. [`super::what_the_slot_index_is_made_of`] owns
//!     that partition and this module does not restate it.
//!
//! AND THE SHIPPED ARM IS PROVED TO BE THE SHIPPED CODEC rather than a lookalike: the named arm's
//! container is asserted BYTE-IDENTICAL to `engine::encode_index_bytes`'s output on the same shard.
//! A counterfactual whose baseline is a re-implementation of the encoder prices the
//! re-implementation.
//!
//! # WHAT IT MEASURED
//!
//! THE POSITIONAL FORM IS WORTH 39x TO 69x LESS ON DISK THAN ON THE PAYLOAD. Four points, two key
//! shapes by two store sizes, `named` minus `positional`, so a positive figure is what the
//! positional form SAVES:
//!
//! ```text
//!       shape    keys   buckets   payload B/obj   cont. B/obj   d payload    d cont.   collapse
//!      padded    2000       936         211.205         34.28    +106.368      +2.77        38x
//!      padded    8000      1024         156.476         29.42     +61.055      +0.87        70x
//!   scattered    2000       882         206.889         42.69    +102.777      +2.34        44x
//!   scattered    8000      1024         156.482         38.38     +61.055      +1.22        50x
//! ```
//!
//! THE CONTAINER COLUMNS ARE QUOTED TO TWO PLACES AND THE PAYLOAD COLUMNS TO THREE, because that is
//! the precision each one HAS. The payload is a pure function of the corpus and reproduces exactly;
//! the container moved in its third decimal between two runs of this arm on the same corpus (34.270
//! against 34.279, 29.431 against 29.418), because the index carries wall-clock and sequence values
//! and two runs therefore compress to slightly different sizes. Anything quoted off the artefact to
//! three places is quoting that jitter, and a guard pinned to a third decimal here would flake.
//!
//! So the names are 39% to 51% of the PAYLOAD and 2.3% to 8.1% of the ARTEFACT: the zstd frame was
//! already taking 96% to 99% of their removal, because a field name is the most compressible thing
//! in the index -- one short string repeated once per instance, identically, thousands of times.
//! THE SAVING IS REAL AND IT IS POSITIVE AT ALL FOUR POINTS, which is the half of this the comment
//! could not say; it is also one to three bytes an object, which is the half a reader of the 39.75%
//! figure would not have guessed.
//!
//! AND THE FORM DOES NOT LOAD, WHICH IS THE DECLINE'S OWN REASON AND IT BITES FURTHER UP THAN THE
//! COMMENT SAYS. The decoder refuses a positional index with `invalid type: sequence, expected a
//! core index` -- so it is `CoreIndex`'s own hand-written `Deserialize` that will not take an
//! array, before `BucketNode`'s or the page entry's is reached. Three hand-written visitors, a
//! `BucketNode` whose arity moves with `tombstone_elements`, a stamp step and a reader for the
//! named shape is the bill for one to three bytes an object.
//!
//! AND `node/object_index` IS NOT RECONSTRUCTIBLE. 342 released buckets held 400 object ids with
//! ZERO pages beside them to name one. At 22.603 B/object -- the largest single term in
//! `slot_index` -- that moves out of `Redundancy` and into the floor.
//!
//! rust-internal: measures this crate's own index codec, no product behaviour

use super::*;
use std::collections::BTreeMap;

const SHARD: u64 = 43;

/// The shipped routing range's end bucket. Every figure here is taken on `0..1023`; the unbounded
/// range puts one bucket per object and is not what a deployment runs.
const SHIPPED_END_BUCKET: u32 = 1023;
/// The key width the published per-object index figures were taken at.
const PUBLISHED_WIDTH: usize = 14;
/// `-` and six digits.
const TAIL_WIDTH: usize = 7;
/// The byte a padded key's run is made of.
const PAD_BYTE: u8 = b'q';
/// The two store sizes every figure is taken at, so a division can be told from a slope.
const SIZES: [usize; 2] = [2_000, 8_000];

/// What a fixture key's leading bytes are made of. The payload figures barely care; the CONTAINER
/// figures care enormously, so both ends are measured and neither is quoted alone.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyShape {
    /// A constant run plus the ordinal -- near-perfectly compressible, a LOWER bound on every
    /// container figure.
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

fn key_text(ordinal: usize, width: usize, shape: KeyShape) -> String {
    let pad = width - TAIL_WIDTH;
    let mut text = match shape {
        KeyShape::Padded => String::from_utf8(vec![PAD_BYTE; pad]).expect("ASCII by construction"),
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
                    char::from(b'a' + (state % 26) as u8)
                })
                .collect()
        }
    };
    text.push_str(&format!("-{ordinal:06}"));
    assert_eq!(text.len(), width, "the key must be exactly the width asked for");
    text
}

/// WHICH OF THE TWO MSGPACK STRUCT FORMS, AND THIS IS THE ONLY THING THE COUNTERFACTUAL VARIES.
///
/// A control that varies the wrong thing clears the real cause, so the two arms differ in exactly
/// one builder call and share the compression level, the magic, the codec id and the stamp.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StructForm {
    /// `with_struct_map()` -- what `engine::encode_index_msgpack` builds today. A field is a NAME.
    Named,
    /// `rmp_serde`'s default -- a struct is an ARRAY. A field is a POSITION and its name is not
    /// written at all.
    Positional,
}

impl StructForm {
    fn label(self) -> &'static str {
        match self {
            StructForm::Named => "named (shipped)",
            StructForm::Positional => "positional",
        }
    }
}

/// One form's two measurable sizes on one store.
#[derive(Clone, Copy, Debug)]
struct Sizes {
    payload: usize,
    container: usize,
}

/// The container, written exactly as `engine::encode_index_msgpack` writes it, with the one thing
/// this module varies.
fn sizes_of(shard: &ShardState, form: StructForm) -> Sizes {
    let mut encoded = Vec::new();
    match form {
        StructForm::Named => {
            let mut serializer =
                rmp_serde::Serializer::new(&mut encoded).with_struct_map();
            serde::Serialize::serialize(shard, &mut serializer)
                .expect("the shard must serialize as a named msgpack map");
        }
        StructForm::Positional => {
            let mut serializer = rmp_serde::Serializer::new(&mut encoded);
            serde::Serialize::serialize(shard, &mut serializer)
                .expect("the shard must serialize as a positional msgpack array");
        }
    }
    let payload = zstd::stream::encode_all(
        encoded.as_slice(),
        crate::engine::INDEX_ZSTD_LEVEL,
    )
    .expect("the payload must compress");
    let mut container = Vec::new();
    container.extend_from_slice(crate::engine::INDEX_CONTAINER_MAGIC);
    container.push(crate::engine::INDEX_CODEC_ZSTD_MSGPACK);
    container.extend_from_slice(&crate::engine::SHARD_INDEX_FORMAT_VERSION.to_be_bytes());
    container.extend_from_slice(&payload);
    Sizes { payload: encoded.len(), container: container.len() }
}

/// What one seeded store says about itself, so a regime can be asserted before anything is
/// reported off it.
#[derive(Clone, Copy, Debug)]
struct Regime {
    objects: usize,
    blocks: usize,
