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
