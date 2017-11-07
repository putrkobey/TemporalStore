// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ANCHOR DIGEST AS RAW BYTES: what it saves, and that every older spelling still loads.
//!
//! # THE CHANGE
//!
//! `IndexAnchorRecord::index_sha256` carried `bytes_serde` — the JSON base64 codec — on a msgpack
//! path, so its 32 bytes were written as 46. It now carries `bytes_serde::binary`, a `bin` string,
//! at 34. Twelve bytes an anchor, and the record's doc finally describes what it holds.
//!
//! # WHAT THIS ARM IS MOSTLY FOR
//!
//! A format change's obligation is the OLD spelling, so the saving is one assertion here and
//! compatibility is three — one per spelling this log has ever written:
//!
//!   * **base64 string**, what the anchor wrote until now;
//!   * **array of integers**, the oldest shape, from before the parent module existed;
//!   * **bin string**, the new one, round-tripped.
//!
//! Each old payload is built by ENCODING a mirror struct that carries the old annotation, never by
//! pasting a byte literal. A literal would pin what I believe the old encoder produced; a mirror
//! pins what it actually produces.
//!
//! # AND THE DOWNGRADE DIRECTION, WHICH IS THE ONE THAT USUALLY BITES
//!
//! A format change is dangerous when an older reader cannot read the newer bytes. Here the decoder
//! is a single visitor — `bytes_serde::EitherShape` — that accepts all three spellings, and it has
//! had `visit_bytes` since the parent module was introduced. So a build from before this change
//! reads the new anchor. Asserted by decoding the new payload through the PARENT module's
//! annotation, which is precisely what that build had.
//!
//! rust-internal: encodes this crate's own records, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_anchor_digest_shrinks_and_still_loads -- --nocapture

use crate::index_log::IndexAnchorRecord;

/// The anchor as it was written until this change: the digest base64'd by the JSON codec.
#[derive(serde::Serialize, serde::Deserialize)]
struct AnchorAsBase64 {
    shard_id: crate::types::ShardId,
    sequence: u64,
    #[serde(with = "crate::bytes_serde")]
    index_sha256: Vec<u8>,
    index_len: u64,
}

/// The anchor in the oldest shape: the digest as a plain sequence of integers.
#[derive(serde::Serialize, serde::Deserialize)]
struct AnchorAsArray {
    shard_id: crate::types::ShardId,
    sequence: u64,
    index_sha256: Vec<u8>,
    index_len: u64,
}

fn digest() -> Vec<u8> {
    // Digest-shaped: uniform, so about half the bytes sit at or above 0x80. A run of one low byte
    // would make the array spelling look free and hide what the encodings actually differ by.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..32)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
fn the_anchor_digest_shrinks_and_still_loads() {
    let bytes = digest();
    let above = bytes.iter().filter(|byte| **byte >= 0x80).count();
    let anchor = IndexAnchorRecord {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    };

    let now = rmp_serde::to_vec(&anchor).expect("the anchor packs");
    let as_base64 = rmp_serde::to_vec(&AnchorAsBase64 {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    })
    .expect("the base64 spelling packs");
    let as_array = rmp_serde::to_vec(&AnchorAsArray {
        shard_id: 1,
        sequence: 7,
        index_sha256: bytes.clone(),
        index_len: 4_096,
    })
    .expect("the array spelling packs");

    println!();
    println!("  THE ANCHOR RECORD, WHOLE, IN THREE SPELLINGS OF ITS DIGEST");
    println!();
    println!("    digest bytes at or above 0x80: {above} of 32");
    println!();
    println!("    {:<40} {:>8}", "spelling", "bytes");
    println!("    {:<40} {:>8}", "array of integers (oldest)", as_array.len());
    println!("    {:<40} {:>8}", "base64 string (until this change)", as_base64.len());
    println!("    {:<40} {:>8}", "bin string (now)", now.len());
    println!();
    println!("    saving against base64: {} bytes an anchor", as_base64.len() - now.len());
