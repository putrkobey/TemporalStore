// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE DURABLE PATHS CHARGE FOR A BYTE PAYLOAD — three clean negatives and one real toll.
//!
//! # THE QUESTION, AND WHY IT DID NOT CARRY OVER
//!
//! `the_payload_cache_per_user_byte` found the cached payload charging **two bytes for every byte at
//! or above 0x80**: msgpack serializes a `Vec<u8>` as an ARRAY OF INTEGERS, a positive fixint below
//! 0x80 and a two-byte uint8 above it. #2279 annotated that field as a byte string and the cost
//! went to 1.0x. The open question was what the same encoder costs on the paths that reach disk.
//!
//! Answered by reading the encoders, not by assuming the shape carries:
//!
//! | path | encoder | byte payload | toll |
//! |---|---|---|---|
//! | WAL record | `prost` | `#[prost(bytes = "vec")]` | **none** — length-delimited |
//! | index-log delta | msgpack | *no `Vec<u8>` field at all* | **none** |
//! | index-log anchor | msgpack | `index_sha256`, base64 via `bytes_serde` | **real, and backwards** |
//!
//! # THE ONE REAL FINDING IS THE OPPOSITE WAY ROUND
//!
//! `IndexAnchorRecord::index_sha256` carries `#[serde(with = "crate::bytes_serde")]`, which is the
//! **JSON** codec: it has no `is_human_readable` branch and always emits `serialize_str` of base64.
//! On this msgpack path a 32-byte digest is therefore written as **44 characters**, where a msgpack
//! `bin` string would be 34.
//!
//! The toll is small and bounded — one anchor record, not one per key. It is worth measuring for a
//! different reason than its size: **the anchor's own doc says it carries "the digest as its 32 raw
//! bytes", and it does not.** A record whose documentation names a wire shape it is not using is
//! what makes the next format decision wrong.
//!
//! # THIS ARM MEASURES AND DOES NOT CHANGE THE FORMAT
//!
//! Re-pointing that annotation is a durable-format change: anchors already on disk are base64
//! strings, so it needs a decoder that takes both spellings and a test that proves the old one still
//! loads. That is a separate change carrying a compatibility obligation. Pricing it first means the
//! decision gets made against a number.
//!
//! rust-internal: encodes this crate's own records, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_the_durable_byte_encodings_cost -- --ignored --nocapture

/// A digest-shaped payload: uniform bytes, so about half of them sit at or above 0x80 and the
/// array encoding charges two for each of those. A run of one low byte would hide the toll
/// entirely, which is the fixture mistake this whole area has paid for before.
fn digest_shaped(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
#[ignore = "encodes a few records and compares their lengths; run by name"]
fn what_the_durable_byte_encodings_cost() {
    const DIGEST: usize = 32;
    let bytes = digest_shaped(DIGEST);
    let above = bytes.iter().filter(|byte| **byte >= 0x80).count();

    // The three spellings, each encoded by the same msgpack serializer so only the SHAPE differs.
    struct AsArray<'a>(&'a [u8]);
    impl serde::Serialize for AsArray<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
