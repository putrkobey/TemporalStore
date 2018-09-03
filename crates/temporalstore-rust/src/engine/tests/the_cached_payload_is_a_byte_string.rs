// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE CACHED RESPONSE PAYLOAD IS A BYTE STRING, NOT AN ARRAY OF INTEGERS.
//!
//! # THE TOLL THIS REMOVES
//!
//! Left to the derive, `rmp_serde` writes a `Vec<u8>` element by element: a byte below 0x80 is a
//! positive fixint and costs one byte, a byte at or above it takes a `uint8` marker and costs TWO.
//! So the cached response inflated its payload by a factor set by the payload's byte distribution
//! -- 1.0x for ASCII, ~1.5x for uniform random bytes, 2.0x worst case.
//!
//! Measured in `the_payload_cache_per_user_byte` before this change: at 2,000 keys x 1 KiB the
//! cache held 4,138,000 B for a run of `b'v'` and 5,161,820 B for incompressible noise, a residual
//! of exactly half the value size with fills unchanged at 2.00 per key -- bytes per entry, not an
//! extra entry. `b'v'` is 0x76, below the line, where the toll is exactly zero, which is why every
//! fixture in this area had read 1.0x and the toll went unpriced.
//!
//! # WHY IT IS SAFE TO CHANGE THE ENCODING
//!
//! `CommandResponse` is serialized in exactly two places in this crate, `encode_cached_response`
//! and `decode_cached_response`, and the put is `put_memory_only` -- the memory tier, which does
//! not survive a restart. Nothing durable and nothing on the wire carries this encoding, and both
//! halves of the codec change together.
//!
//! rust-internal: measures this crate's own encoding, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_cached_payload_is_a_byte_string -- --nocapture

use crate::engine::command_validation::{decode_cached_response, encode_cached_response};
use crate::types::CommandResponse;

/// Uniform bytes, so that about half of them land at or above 0x80 where the old encoding charged
/// two bytes each.
fn uniform_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
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
fn the_cached_payload_survives_its_codec_at_either_byte_distribution() {
    for (label, payload) in [
        ("all low (0x76)", vec![b'v'; 1_024]),
        ("uniform", uniform_bytes(1_024)),
        ("all high (0xFF)", vec![0xFF_u8; 1_024]),
        ("empty", Vec::new()),
    ] {
        let response = CommandResponse::Bytes { value: Some(payload.clone()) };
        let encoded = encode_cached_response(&response).expect("the response encodes");
        let decoded = decode_cached_response(&encoded).expect("the response decodes");
        // THROUGH THE REAL PAIR, so the `CachedBytes` mirror is in the path. A mirror left without
