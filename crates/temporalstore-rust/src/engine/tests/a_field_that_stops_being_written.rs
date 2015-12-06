// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A field the encoder stops writing must still decode an image that lacks it.
//!
//! # THE TRAP, AND WHY THE IMAGE CANNOT SHOW IT TO YOU
//!
//! `#[serde(skip_serializing)]` stops a key being written. `#[serde(default)]` makes a missing key
//! decode instead of failing. They are two halves of one decision, and the first without the
//! second is a store that cannot be opened: the first image written without the key makes every
//! subsequent load of it FAIL rather than decode an empty field.
//!
//! Measured rather than reasoned, because the two attributes differ in exactly the way that
//! matters and in no way that is visible:
//!
//! ```text
//! skip             + missing key -> OK   (Default is supplied automatically)
//! skip_serializing + missing key -> ERR  missing field
//! skip             writes: {"kept":0}
//! skip_serializing writes: {"kept":0}
//! ```
//!
//! BOTH WRITE THE SAME BYTES. The stored image gives no way to tell them apart; only the decode
//! side differs. That is why this needs a guard over the source rather than a check over an image.
//!
//! And it is why `skip` is NOT a trigger here: `skip_deserializing` supplies `Default` itself, so
//! `#[serde(skip)]` alone is correct, and flagging it would refuse a shape this tree uses on
//! purpose.
//!
//! # WHY NOW, AND WHICH FIELDS
//!
//! The consolidation is moving series maps off the shape they are stored in, and the way a map
//! stops being written is by gaining `skip_serializing`. So the trap is reachable, repeatedly.
//!
//! Four `ShardState` fields carry NO serde attribute at all. That reads as `#[serde(default)]`
//! while the key is still written -- both always write it -- and is not `default` on decode. Each
//! one therefore needs `default` added in the SAME change that stops writing it, never after.
//! The list is printed by this test rather than written here, so it cannot go stale.
//!
//! # HOW TWO AUDITS OF THIS GOT IT WRONG, WHICH IS WHY THE MATCHER IS SHAPED AS IT IS
//!
//! An attribute is bound to a field by CONTIGUITY, and a doc comment between them breaks the run.
//! An audit that paired attributes to fields by adjacency silently gave a bare field the PREVIOUS
//! field's attribute -- so a table whose shape cannot represent "absent" reported absent as
//! whatever came before it.
//!
//! A second attempt matched the attribute name as TEXT, which counted the dozen prose comments in
//! `engine_storage_state.rs` that explain why some field carries it -- reporting nine violations where there are
//! none.
//!
//! Both are guarded below by controls rather than by care.
//!
//! rust-internal: guards this crate's own stored shape, no product behaviour

/// One field of the struct: its line, its name, and the attributes CONTIGUOUSLY above it.
struct Field {
    line: usize,
    name: String,
    attributes: Vec<String>,
}

impl Field {
    /// Whether anything stops this field being written. `skip` is deliberately not included --
    /// see the module header for the measurement.
