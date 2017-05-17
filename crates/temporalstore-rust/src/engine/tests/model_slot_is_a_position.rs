// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MODEL SLOT ON AN INDEX-LOG ROW CARRIES A POSITION, NOT A REGISTRY CODE.
//!
//! Two numbering schemes name the same seventeen model spellings and they agree on NOTHING:
//!
//!   * `index_log::MODEL_ID_NUMBERS` is POSITIONAL and 0-based. The position IS the wire value, so
//!     `"string"` is 0 and will mean 0 forever.
//!   * the entry spelling's own registry (`storage_bucket_internals`'s `model_kind_registry!`)
//!     gives each kind an EXPLICIT `report_code()` with deliberate holes -- `"string"` is 1 there,
//!     `"zset"` is 6, and 5 and 15 belong to retired names.
//!
//! WHY THIS IS PINNED AND NOT COMMENTED. The resident entry holds its model as the one-byte
//! `StoredModelKind`, and the log row holds it as a `String`; collapsing the row's onto the entry's
//! is worth 23 bytes and is the single largest step in making the two one type. A reader whose hand
//! is already on `StoredModelKind` has `report_code()` in easy reach and `MODEL_ID_NUMBERS` nowhere
//! in sight, so reaching for the wrong one is the natural mistake rather than a careless one.
//!
//! AND IT IS SLOT-PRESERVING IN SHAPE AND INCOMPATIBLE IN VALUE, which is the worst combination a
//! durable format offers: the row still has twelve slots, the slot still holds an integer, the
//! decode still succeeds, and every stored row's model silently becomes a DIFFERENT, VALID kind.
//! Nothing errors anywhere. This file asserts what the slot carries, and asserts that a swap is
//! DETECTABLE for every spelling rather than assuming it would be.
//!
//! rust-internal: reads the engine's own index-log encoder, no external surface

use crate::index_log::{model_id_slot_number, MODEL_ID_NUMBERS};

/// Every spelling the registry declares, with the code IT would have written.
///
/// Spelled out here rather than read from the registry on purpose: this file's job is to catch the
/// two numberings being confused, and a table derived from one of them cannot do that. It is tied
/// to the registry by the membership assertion below instead, so it cannot go stale silently.
const REGISTRY_REPORT_CODES: &[(&str, u64)] = &[
    ("string", 1),
    ("hash", 2),
    ("set", 3),
    ("feature", 4),
    ("sequence", 5),
    ("control_state", 7),
    ("context_node", 8),
    ("context_event", 9),
