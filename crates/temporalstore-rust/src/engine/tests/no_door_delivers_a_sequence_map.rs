// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! NO DOOR CAN DELIVER A `sequences` MAP, AND THAT IS WHAT MAKES THE FIELD SAFE TO DELETE.
//!
//! # THE ARGUMENT THAT IS NOT ENOUGH
//!
//! "Nothing writes it" is true -- every `insert` and `entry` on that map lived in
//! `engine/tests/engine_tests_conformance.rs`, a test model -- and on its own it is wrong, because the field
//! WAS on the wire when non-empty and `ShardState` carries no `deny_unknown_fields`. An index
//! still holding a series decodes *cleanly* against a struct without the field, and the series is
//! dropped with no error raised anywhere. A deletion resting on the absence of a writer loses that
//! data silently.
//!
//! So the question is not "who writes it" but "what can still ARRIVE carrying it". That is a
//! question about doors, and it took three passes to enumerate them. Each pass found the previous
//! answer had been a claim about one function reported as a claim about the system.
//!
//! # THE THREE DOORS, AND WHY EACH IS SHUT
//!
//! **Door 1 -- the base index file.** `load_index_inner` decodes it and refuses any stamp below
//! `SHARD_INDEX_FORMAT_VERSION`, before anything reads the decoded maps. Arms 1 and 2.
//!
//! **Door 2 -- the dump manifest.** `decode_index_bytes` has six production callers and for a long
//! time only `load_index_inner` compared the stamp; the version check inside `decode_index_bytes`
//! is on the msgpack-container arm, which a plain-JSON index never meets. A stamp-13 image
//! installed through `install_bucket_dump_manifest` reached a live shard with all four
//! `index_load_path_counts` counters at ZERO, and the install re-serialized the decoded state --
//! so with this field deleted the rows were written away irrecoverably. That hole is what the
//! change before this one closed, and arm 3 is the arm that failed before it and passes now.
//!
//! **Door 3 -- the base index file being ABSENT.** `load_index_inner`'s `Err(_)` arm takes
//! `ShardState::default()` and never reaches the stamp comparison at all, and
//! `fold_index_log_deltas` then runs with `base_anchor` 0 and folds the whole index log. Nothing
//! decodes on that path, so neither the stamp nor any decode-time fold fires there. Arms 5 and 6.
//!
//! Door 3 is shut by what the log can CARRY rather than by a gate, and that is checked at the
//! layer that decides rather than argued:
//!
//!   * `capture_key_states` -- the writer -- emits exactly thirteen field names
//!     (`features`, `expires_at_ms`, `control_state_changes`,
//!     `control_state_selection`, `context_nodes`, `context_events`, `context_indexes`,
//!     `context_audits`, `context_children`, `context_summaries`, `context_compressions`,
//!     `context_entities`) and `sequences` is not one of them, so no index log in existence
//!     carries such an entry;
//!   * `apply_key_states` -- the reader -- has exactly thirteen `apply_key_state_field` calls over
//!     those same names and never consults a `sequences` key, so even a hand-made log record
//!     naming it delivers nothing. Arm 5 drives that;
//!   * the live-block arm of the fold cannot produce one either: `visit_model_live_blocks` walks
//!     fifteen maps and `sequences` is not among them, which is consistent with
//!     `model_kind_registry` declaring `Sequence` under `retired` with "no arm emits it". That is
//!     also why this removal cannot make a block reclaimable: a block addressed only by that map
//!     was already invisible to the liveness walk.
//!
//! # EVERY ARM HAS A CONTROL, BECAUSE A CLOSED DOOR AND AN EMPTY FIXTURE READ ALIKE
//!
//! A "nothing arrived" from a fixture that planted nothing is indistinguishable from a door that
//! is shut. So each refusal arm is paired with the same fixture delivering the same series through
//! the same door under a map the struct still has (`features`), and that must ARRIVE.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;

const SHARD: ShardId = 13;
const SERIES_KEY: &str = "seq/a-series-written-before-the-fold";
const ROW_TIMESTAMP_MS: u64 = 1_700_000_000_123;

/// THE SERIES' ROWS ALONE, as JSON, built through the real value type.
///
/// This is the shape a KEY-STATE BLOB carries: `apply_key_state_field` deserializes the field's
/// value as the map's VALUE type and inserts it under the blob's `key`, so the value is the
/// series for that one key -- not a map of keys to series. A first version of the door-3 arm
/// passed the whole-map shape and its CONTROL refused to arrive, which is what caught it: the
/// retired name delivering nothing meant nothing while the live name delivered nothing either.
fn one_series_rows_as_json() -> serde_json::Value {
    let mut rows = BTreeMap::new();
    rows.insert(
        ROW_TIMESTAMP_MS,
        ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
    );
    serde_json::to_value(&rows).expect("the series rows serialize")
}

/// One series, as JSON, built through the real value type.
///
/// Lifted out of a throwaway `ShardState`'s `features` map: `features` and the retired
/// `sequences` map had IDENTICAL types, so this is byte-for-byte what a pre-fold binary wrote
/// under the other name. It cannot be built by setting the field -- this binary has not got one.
/// ONE PAYLOAD AGAIN, because both arms plant the same retired key now.
///
/// An earlier revision split this by field: the refusal arm planted the legacy series under
/// `sequences` and the control planted something a LIVE field's type would accept. That shape kept
/// breaking -- the control's field has to be "already on the wire", and consolidation took
