// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `page` and `block` NAME ONE TIER HERE, AND THIS IS THE GUARD THAT SAYS SO.
//!
//! # THE CURRENT CONTRACT
//!
//! `docs/temporalstore_page_block_address_contract.md` describes one block tier.
//! `PublicStorageContract` maps both published address labels to `ElementEntry`.
//! `BlockIndexNamedBy` carries a `BlockIndexMap`, and `BlockEntryAt` writes its entries.
//! Older page-named report fields are retained for consumers, not a second storage tier.
//! `PublicStorageFeatureShapes` describes report projections rather than Rust field layouts.
//!
//! The two names on a published statistic must continue to carry the same measurement.
//! Removing a published key would change the report contract rather than rename a field.
//!
//! # WHAT GOES WRONG WITHOUT THIS
//!
//! The failure is NOT a rename that breaks the build. It is the opposite: a mirror pair that
//! quietly stops being a mirror, so one published name starts carrying a second quantity and
//! every consumer reading the other name is silently wrong. That has happened twice here
//! already. A committed fixture recorded `page_reads: 4` beside `block_reads: 2` and was
//! believed for long enough to be copied into six places, and `dirty_object_count` was filled
//! with PAGES in one eviction arm and OBJECTS in the other, 3.5x apart, inside a sort key.
//!
//! `page_and_block_counter_ratio` pins the reads and writes pair against nine workloads.
//! NOTHING pinned the other mirrors. `page_index_entries` and `block_index_entries` are
//! assigned from one expression four lines apart in `engine/engine_persistence.rs`, `page_store_reads`
//! and `block_store_reads` are incremented on consecutive lines of one `else if` arm in
//! `engine/storage_reports.rs`, and `page_index_count` is assigned FROM
//! `physical_index.block_index_count` in `engine.rs`. Every one of those is one line away from
//! becoming two quantities, and nothing fails when it does.
//!
//! # WHY IT IS WRITTEN THIS WAY ROUND
//!
//! The assertion is NOT "a `page_` key exists". It resolves both spellings to ONE identity and
//! asserts the members of each identity agree. That holds whichever spelling survives: with two
//! members it compares them, with one it has nothing to disagree with. So a later rename slice
//! does not have to rewrite this file -- only the measured pair COUNT moves, and it is asserted
//! exactly so that moving it has to be deliberate.
//!
//! Canonicalisation is LONGEST MATCH FIRST (`pages` before `page`) and runs on `_`-delimited
//! segments only, so a word that merely contains the letters is untouched. That rule is not
//! hypothetical: this repository's own `LICENSE` contains `stoppage`, and `tools/portal` is full
//! of `pagehide` and `homepage`. A sweep without a boundary rule corrupts the Apache-2.0 text.
#![allow(clippy::all)]
use super::*;

/// Replace a `page`/`pages` SEGMENT with its block spelling, leaving every other segment alone.
///
/// Longest match first, and segment-exact rather than substring: `pages` becomes `blocks`,
/// `page` becomes `block`, and `homepage`, `pagehide` and `stoppage` are returned unchanged.
fn canonical_tier_name(name: &str) -> String {
    name.split('_')
        .map(|segment| match segment {
            "pages" => "blocks",
            "page" => "block",
            "Pages" => "Blocks",
            "Page" => "Block",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("_")
}

/// Every group of published keys that resolve to one canonical tier name.
///
/// THERE IS NO MEMBERSHIP FILTER HERE, AND THAT IS THE POINT. This function used to end
/// with `groups.retain(|_, members| members.len() >= 2)`, and the test below asserted the
/// resulting group COUNT. Those two together are a guard that is satisfied by shrinking
/// its own population: retire one spelling of a pair, the group falls below two, the
/// group disappears, the asserted count no longer matches -- and the obvious repair is to
/// lower the number. The moment that number is lowered, nothing in this file compares the
/// two spellings of that pair any more, and every assertion here is green. The count
/// moved and the coverage went with it, with no failing test anywhere to say so.
///
/// So the filter is gone and the population is DECLARED instead, by
/// [`TWINNED_ON_SHARD_STATS`] and [`SINGLE_ON_SHARD_STATS`]. A one-member group is still a
/// group here, which is what lets the test below say WHICH spelling survived a retirement
/// instead of losing sight of the canonical name altogether.
fn mirror_groups(
    value: &serde_json::Value,
) -> std::collections::BTreeMap<String, Vec<(String, serde_json::Value)>> {
    let object = value
        .as_object()
        .expect("the published stats must serialize as a JSON object");
    let mut groups: std::collections::BTreeMap<String, Vec<(String, serde_json::Value)>> =
        std::collections::BTreeMap::new();
    for (key, held) in object {
        groups
            .entry(canonical_tier_name(key))
            .or_default()
            .push((key.clone(), held.clone()));
    }
    groups
}

/// Canonical tier names on `ShardCanonicalStorageStats` published under BOTH spellings,
/// and the exact pair each one must carry.
///
/// Not remembered -- each pair is one line away from carrying two quantities, and the
/// line is named. `page_index_entries` and `block_index_entries` are assigned from one
/// expression five lines apart in `engine/engine_persistence.rs`; `page_reads`/`block_reads` and
/// `page_writes`/`block_writes` are declared beside each other on this struct in
/// `control.rs` and filled from one snapshot.
///
/// TO RETIRE A SPELLING, move the canonical name to [`SINGLE_ON_SHARD_STATS`] together
/// with the spelling that survived. That one-line move is the reviewable artefact, and
/// the clauses in the test below refuse every other way of shrinking this population --
/// including the one that used to work, which was to leave the roster alone and change a
/// number.
const TWINNED_ON_SHARD_STATS: &[(&str, [&str; 2])] = &[
    (
        "block_index_entries",
        ["block_index_entries", "page_index_entries"],
    ),
    ("block_reads", ["block_reads", "page_reads"]),
    ("block_writes", ["block_writes", "page_writes"]),
];

/// Canonical tier names on the canonical shard stats that are DOWN to one spelling, with
/// the spelling that survived.
///
/// EMPTY TODAY, and every clause below still runs over it, so the first retirement adds
/// one line and writes no new machinery.
///
/// The surviving spelling is asserted to be the BLOCK one, because that is the outcome a
/// pair count can never tell apart from the right one: dropping `block_reads` and keeping
/// `page_reads` lowers the count by exactly as much as dropping `page_reads` does, and
/// leaves the tier published under the word this crate has decided is not canonical.
const SINGLE_ON_SHARD_STATS: &[(&str, &str)] = &[];

/// The page-spelled keys the published storage contract carries.
///
/// A COUNT here has the same defect the pair count had: retiring one of the two lets the
/// 2 be written as a 1, and nothing then says which key left or that the other is still
/// published. The set is named instead.
const PAGE_SPELLED_CONTRACT_KEYS: &[&str] = &["page_address", "page_index_entry"];

fn mirror_engine(dir: &std::path::Path, cache_name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join(cache_name),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn mirror_set(engine: &TemporalEngine, key: &str, value: Vec<u8>) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value,
        },
    });
    assert!(
        response.status.ok,
        "write of {key} failed: {:?}",
        response.status
    );
}

fn mirror_get(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert!(
