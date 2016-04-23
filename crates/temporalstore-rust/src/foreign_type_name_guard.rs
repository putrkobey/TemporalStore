// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! NO FOREIGN TYPE NAMES IN RUST SOURCE.
//!
//! The vocabulary scrub every change here runs covers vendor IDENTITY -- product and company
//! names. It cannot catch what this module catches, and the two failures are different in kind:
//! naming a product merely points at something outside this repository, while a leaked
//! identifier is evidence of it, carried in the source of an Apache-2.0 project that anyone may
//! fork. This crate shipped `"HashOrSet<std::string,std::string>"` as a descriptor value for two
//! context models, and nothing failed, because no gate was looking for a type name.
//!
//! WHAT THIS DOES NOT COVER, deliberately: a DURABLE name. Every `#[serde(alias = ...)]` and
//! `#[serde(rename = ...)]` in this crate is a spelling already written into stores in the
//! world -- `block_in_log`, `page_size`, `routing_slot` -- and a name that entered this codebase
//! from anywhere is OURS once a store carries it, because dropping it is a store that stops
//! loading. The scan below is a scan for DESCRIPTIVE text: a string, a comment or an identifier
//! that names someone else's type where it could have named ours. It is not a renaming campaign
//! over the wire format, and it must never become one.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Namespace-qualified type spellings from another language's standard library. These are
/// unambiguous: a `.rs` file has no legitimate use for any of them, so no judgement call is
/// involved in a hit.
///
/// SCOPED, and this is the whole difficulty of the check. A bare `std::` matcher is useless --
/// `use std::collections::BTreeMap` is on hundreds of lines in this crate -- so each needle names
/// one specific type whose spelling Rust does not share, and the right-boundary rule below
/// rejects the Rust path that merely starts the same way (`std::string::String`,
/// `std::stringify!`). `std::vec`, `std::slice` and `std::array` are ABSENT from this list on
/// purpose: all three are real Rust paths used here, and it is `vector` rather than `vec` that
/// belongs to the other spelling.
const FOREIGN_NAMESPACE_TYPE_NEEDLES: &[&str] = &[
    "std::string",
    "std::vector",
    "std::pair",
    "std::map",
    "std::set",
    "std::list",
    "std::deque",
    "std::unique_ptr",
    "std::shared_ptr",
];

/// A namespace prefix with no Rust meaning at all, so it needs no right boundary -- whatever
/// follows it, the prefix itself does not belong in a `.rs` file.
const FOREIGN_NAMESPACE_PREFIXES: &[&str] = &["absl::"];

/// Fixed-width integer spellings Rust does not use. Rust spells these `u8`/`u16`/`u32`/`u64`/
/// `i64`, so the `_t` form only ever arrives by being copied in. Boundary-checked at both ends,
/// which is also what keeps `uint8_t` from being reported twice -- once as itself, once as the
/// `int8_t` sitting inside it.
const FIXED_WIDTH_INT_NEEDLES: &[&str] =
    &["uint8_t", "uint16_t", "uint32_t", "uint64_t", "int64_t"];

/// DISTINCTIVE TYPE AND METHOD NAMES FROM THE IMPLEMENTATION THIS ENGINE'S DESIGN WORK STUDIED.
///
/// Each row carries the reason it is here, because a list nobody justified becomes furniture that
/// outlives the reason for it. The test on this list asserts every row has a justification of
/// real length, so a future addition cannot be silent.
///
/// The common argument, which does most of the work: this crate's methods are `snake_case`, so a
/// `CamelCase` method name is already not Rust and its presence can only be a copy. The type
/// names each get a specific argument about the word OUR vocabulary uses instead.
const FOREIGN_NAME_NEEDLES: &[(&str, &str)] = &[
    (
        "META_DIRTY",
        "A flag name from the implementation this record's shape was studied against, removed by \
         the change that added this row. Our split is between the two LOGS a bucket tracks -- \
         `first_dirty_wal_sequence` and `first_dirty_index_log_sequence` -- so the index side of \
         dirtiness is spelled `INDEX_DIRTY` here and `dirty` everywhere else. We do not have a \
         generic notion of metadata to be dirty.",
    ),
    (
        "TRIVIAL_PAGE",
        "From the same source and removed by the same change, as REDUNDANT rather than merely \
         foreign: its documented meaning was that the page is deleted, which `BLOCK_DELETED` \
         already carries. Our word for this is `deleted`, which 141 files already use. Two bits \
         for one fact is the defect; the borrowed name was how it arrived.",
    ),
    (
        "HashOrSet",
        "Found in this crate and removed by the change that added this guard: it was the \
         `block_primitive` of two context models. Ours are the `model_kind_registry!` spellings \
         -- `hash`, `set`, `string` -- never a template composing two of them.",
    ),
    (
        "FeatureOrSet",
        "Found in this crate and removed by the same change, on six descriptors. `feature` alone \
         IS our spelling and is what those six now carry; the `OrSet` suffix is the borrowed half.",
    ),
    (
        "OrSetModel",
        "The third member of the same `OrSet` family. We say `ModelKind`, and a model's kind is \
         one of seventeen declared names, so there is no shape here for an `OrSet` to describe.",
    ),
    (
        "PersistentMap",
        "A named map template from that implementation. Ours is a `BTreeMap` in `ShardState` made \
         durable by the block index -- we never fold persistence into a type's name. Still \
         present in one `docs/` file, which this Rust-only scan does not reach.",
    ),
    (
        "MultiPageObject",
        "Our word for a stored unit is BLOCK, not page: `ElementEntry`, `block_size`, \
         `block_in_log`, and `page_segment_id` was renamed to `block_slab_id`. A new CamelCase \
         `*Page*` type name is therefore a copy rather than a coinage. Our own layout policies \
         spell this concept `single_block_object` and `component_block_object`, in snake_case, and \
         those are stored strings this scan leaves alone.",
    ),
    (
        "ObjectWithId",
        "We carry identity as a FIELD -- `object_id` on `ElementEntry` -- and name types for what \
         they are (`ElementEntry`, `ContextNodeModel`), never for which fields they happen to \
         hold.",
    ),
    (
        "SlotContextManager",
        "`slot` here is that implementation's word. Ours survives only as the retired wire alias \
         `routing_slot`, whose live spelling is `routing_bucket`; and `context` in this crate \
         names the context MODELS, which have no manager type.",
    ),
    (
        "GatherDirtySlot",
        "CamelCase method, so not Rust. Ours is `context_dirty_index` plus the `dirty` bit that \
         every block address carries.",
    ),
    (
        "MarkSlotDataDirty",
        "CamelCase method, so not Rust, and built on the `slot` vocabulary described above.",
    ),
    (
        "PushDirtySlot",
        "CamelCase method, so not Rust, and built on the `slot` vocabulary described above.",
    ),
    (
        "BlindDumpNewPages",
        "CamelCase method, so not Rust, and built on the `page`-for-`block` vocabulary above.",
    ),
    (
        "WriteKvLog",
        "CamelCase method, so not Rust. Our log-writing vocabulary is `wal`, `index_log` and \
         `roll_wal_segment_if_due`.",
    ),
    (
        "BYTE_ASSERT",
        "That implementation's assertion macro. Ours are `assert!` and `debug_assert!`. This row \
         is genuinely additive to the vendor scrub, which lists company and product names and \
         would not fire on this token at all.",
    ),
    (
        "bcache2",
        "A specific named cache component over there. This crate's caches are named for what \
         they hold, and not one of them is numbered.",
    ),
    (
        "StlWrapper",
        "A wrapper over the other language's template library. Nothing in a Rust crate has such \
         a thing to wrap, so the name cannot arrive here by being needed.",
    ),
    (
        "GetMinTtl",
        "CamelCase method, so not Rust. Our expiry vocabulary is `expiry_by_deadline` and \
         `ensure_expiry_order`.",
    ),
