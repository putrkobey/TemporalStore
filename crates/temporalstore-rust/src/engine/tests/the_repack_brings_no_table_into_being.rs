// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A decode's repack pass must not bring a stored table into being.
//!
//! WHY THIS EXISTS, as a measurement rather than a worry. `repack_decoded_btrees` reached four
//! tables through accessors that CREATE on first use -- `u64s_in`, `counters_in`, and
//! `objects.entry(kind).or_default()` twice. An absent domain or kind is a table that is NOT in the
//! stored map; an empty one is a table that IS. So on a shard missing any of the four, a pass whose
//! whole job is to move entries between B-tree nodes invented a table and the stored bytes moved
//! with no contents moving:
//!
//!     side_tables at the encode BEFORE the repack:  4 entries, no `expiry`
//!     side_tables at the encode AFTER  the repack:  5 entries, an empty `expiry`
//!     decoded payload: 109,291 -> 109,306 bytes
//!
//! `a_decode_puts_back_exactly_the_series_that_were_encoded` caught the symptom -- it compares the
//! bytes before and after on one state -- and reported it as the stored form having moved, which
//! sent three hypotheses the wrong way before the cause came out. This test states the RULE: the
//! set of tables the pass hands back is the set it was given.
//!
//! TWO PARTS, BECAUSE A SEED CANNOT REACH THREE OF THE FOUR. The bug is only visible when a table
//! is ABSENT, and measured over three seeds -- lists only, strings only, features only -- the only
//! one ever absent after a decode is `side_tables/expiry`. The string kind, the feature kind and
//! the control-state counters are materialised by every decode, so asserting coverage of their
//! reaches from seeded shards would be a claim this test cannot support. Part 2 therefore removes
//! each table by hand and asks the same question, which is the rule at the level the rule lives:
//! the pass must not create a table that is not there, whoever took it out.
//!
//! WHY THE KEY SETS AND NOT ONLY THE BYTES. The byte equality is the property the stored form owes;
//! the key sets say WHICH table moved when it breaks, so the next person does not have to
//! decompress a zstd frame and read msgpack map headers to find out. Both are asserted.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;

/// The four tables the pass used to reach through a creating accessor.
const TARGETS: [&str; 4] = [
    "side_tables/expiry",
    "side_tables/control_state_counters",
    "objects/string",
    "objects/feature",
];

/// A shard on the shipped routing range, loaded empty.
fn engine_on_shard(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);
    engine
}

/// Which of the four tables are ABSENT in this state.
///
/// Named, not counted: when an assertion below fails, the name is the whole diagnosis.
fn absent_tables(state: &crate::engine::state::ShardState) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if crate::engine::state::u64s_ref(&state.side_tables, crate::engine::state::SideTable::Expiry)
        .is_none()
    {
        out.insert("side_tables/expiry".to_string());
    }
    if crate::engine::state::counters_ref(
        &state.side_tables,
        crate::engine::state::SideTable::ControlStateCounters,
    )
    .is_none()
    {
        out.insert("side_tables/control_state_counters".to_string());
    }
    if !state
        .objects
        .contains_key(&crate::engine::storage_bucket_internals::StoredModelKind::String)
    {
        out.insert("objects/string".to_string());
    }
    if !state
        .objects
        .contains_key(&crate::engine::storage_bucket_internals::StoredModelKind::Feature)
    {
        out.insert("objects/feature".to_string());
    }
    out
}

/// Every stored table of a state, by name, so a failure says which one appeared.
fn table_names(state: &crate::engine::state::ShardState) -> Vec<String> {
    let mut names: Vec<String> = state
        .side_tables
        .keys()
        .map(|domain| format!("side_tables/{domain:?}"))
        .chain(state.objects.keys().map(|kind| format!("objects/{kind:?}")))
        .collect();
    names.sort();
    names
}

/// Take one of the four tables out of a decoded state.
fn remove_table(state: &mut crate::engine::state::ShardState, target: &str) {
    match target {
        "side_tables/expiry" => {
            state
                .side_tables
                .remove(&crate::engine::state::SideTable::Expiry);
        }
        "side_tables/control_state_counters" => {
            state
                .side_tables
                .remove(&crate::engine::state::SideTable::ControlStateCounters);
