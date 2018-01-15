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
        response.status.ok,
        "read of {key} failed: {:?}",
        response.status
    );
}

/// The segment rule, against the three words in this repository that would be corrupted by a
/// substring sweep.
///
/// `stoppage` is in `LICENSE` (Apache-2.0, "work stoppage"), `pagehide` and `homepage` are in
/// `tools/portal` and in four `Cargo.toml` files. A regex without a boundary rule rewrites all
/// three, and the LICENSE one is a licence-text change.
#[test]
fn the_segment_rule_leaves_words_that_merely_contain_the_letters_alone() {
    assert_eq!(canonical_tier_name("page_reads"), "block_reads");
    assert_eq!(canonical_tier_name("page_index_entries"), "block_index_entries");
    assert_eq!(canonical_tier_name("staged_pages"), "staged_blocks");
    assert_eq!(canonical_tier_name("page"), "block");
    assert_eq!(canonical_tier_name("pages"), "blocks");

    // Already canonical: the function is idempotent, which is what makes it direction-agnostic.
    assert_eq!(canonical_tier_name("block_reads"), "block_reads");
    assert_eq!(canonical_tier_name("block_index_entries"), "block_index_entries");

    // Words that merely contain the letters.
    for untouched in ["stoppage", "homepage", "pagehide", "pagination", "repaged"] {
        assert_eq!(
            canonical_tier_name(untouched),
            untouched,
            "{untouched} is not the storage tier and must survive the sweep unchanged"
        );
    }
}

/// `page_address` and `block_address` are published as ONE type, and the contract says so.
///
/// This is the pair the contract document describes as logical against physical. In this engine
/// both entries carry the string `ElementEntry`, because one `ElementEntry` is both the address
/// of a page and the address of the block backing it. If a future change gives the physical
/// half an identity of its own, these two stop being equal and this fails -- which is the point
/// at which the contract document becomes true of this engine and the rename becomes wrong.
#[test]
fn the_published_contract_names_one_type_for_both_address_spellings() {
    let contract = crate::engine::reports::PublicStorageContract::default();
    let encoded = serde_json::to_value(&contract).expect("the contract must serialize");

    assert_eq!(
        contract.page_address, contract.block_address,
        "page_address is published as {:?} and block_address as {:?}; in this engine both are \
         one ElementEntry, and a difference here means the two words have stopped naming one \
         tier -- see the module header before renaming either",
        contract.page_address, contract.block_address,
    );
    assert!(
        !contract.page_address.is_empty(),
        "the address entries must name a type, not an empty string"
    );
    assert_eq!(
        contract.page_address, "ElementEntry",
        "the published address type moved; re-read the module header, because the whole \
         one-tier finding rests on both spellings resolving to this type"
    );

    // Denominator, printed. Provenance: `PublicStorageContract` declares 17 fields, of which
    // these are the page-spelled ones -- counted at 2701ed26e with
    // `awk '/pub struct PublicStorageContract/,/^}/' engine/engine_reports.rs | grep -c 'pub page_'`.
    let block_keys: Vec<&String> = encoded
        .as_object()
        .expect("the contract must serialize as an object")
        .keys()
        .filter(|key| key.split('_').any(|segment| segment == "page"))
        .collect();
    println!("contract page-spelled keys: {block_keys:?}");
    let found: Vec<&str> = block_keys.iter().map(|key| key.as_str()).collect();
    assert_eq!(
        found, PAGE_SPELLED_CONTRACT_KEYS,
        "the published contract carries the page-spelled keys {found:?} and this file declares \
         {PAGE_SPELLED_CONTRACT_KEYS:?}. A key that LEFT was retired, which is a change to the \
         published report and not a rename; a key that ARRIVED is a published page spelling \
         nothing has held"
    );
}

/// Every mirror pair on the canonical shard stats carries one value, and the POPULATION
/// of pairs is the declared one.
///
/// Driven over a real engine rather than a default, because a `Default` is all zeroes and
/// an equality between two zeroes is not a measurement. Each pair is asserted NON-ZERO
/// first, so this cannot pass by comparing nothing.
///
/// FIVE CLAUSES, because the agreement check is the easy half. The hard half is that the
/// agreement check keeps being APPLIED to the pairs that exist:
///
///   1. the rosters are well formed -- every listed spelling canonicalises to the name it
///      is listed under, no name is on both rosters, and a retired name's survivor is the
///      block spelling;
///   2. the set of canonical names published under two or more spellings IS
///      [`TWINNED_ON_SHARD_STATS`], in both directions, so a pair that leaves without a
///      roster move fails, and a pair that arrives unnoticed fails too;
///   3. each declared pair is present under exactly the two spellings declared;
///   4. each retired name is present under exactly its surviving spelling;
///   5. the spellings of each declared pair hold one value, and that value is not zero on
///      every spelling.
#[test]
fn every_page_named_shard_stat_agrees_with_its_block_twin() {
    // --- CLAUSE 1: the rosters are well formed, before anything is measured. ---
    for (canonical, pair) in TWINNED_ON_SHARD_STATS {
        for spelling in pair {
            assert_eq!(
                canonical_tier_name(spelling),
                *canonical,
                "{spelling} is listed under {canonical} and canonicalises to {} instead",
                canonical_tier_name(spelling)
            );
        }
        assert_ne!(
            pair[0], pair[1],
            "{canonical} lists one spelling twice, so its agreement check would compare a \
             value with itself"
        );
        assert!(
            !SINGLE_ON_SHARD_STATS
                .iter()
                .any(|(retired, _)| retired == canonical),
            "{canonical} is on both rosters; it is either twinned or down to one spelling"
        );
    }
    for (canonical, survivor) in SINGLE_ON_SHARD_STATS {
        assert_eq!(
            canonical_tier_name(survivor),
            *canonical,
            "{survivor} is listed as the survivor of {canonical} and canonicalises to {} instead",
            canonical_tier_name(survivor)
        );
        assert_eq!(
            survivor, canonical,
            "{canonical} is recorded as retired down to {survivor}, which is not the block \
             spelling. The block spelling is the one that survives a retirement on this tier -- \
             see the module header -- and keeping the other half publishes the tier under the \
             word this crate does not use"
        );
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    // Write ten keys, then read them back from a restart with a COLD cache directory, so the
    // read counters cannot be served from memory and every mirror below is non-zero.
    {
        let engine = mirror_engine(&root, "cache-write");
        for index in 0..10 {
            mirror_set(&engine, &format!("mirror-{index}"), vec![b'm'; 512]);
        }
    }
    let engine = mirror_engine(&root, "cache-read");
    for index in 0..10 {
        mirror_get(&engine, &format!("mirror-{index}"));
    }
    // Then ten writes ON THE SAME ENGINE, because the stats below are one snapshot and the
    // write mirror is zero on an engine that has only read. The first run of this test asserted
    // the write pair against 0 == 0 and its own non-vacuity clause caught it.
    for index in 10..20 {
        mirror_set(&engine, &format!("mirror-{index}"), vec![b'm'; 512]);
    }

    let stats = engine.loaded_shard_stats();
    assert_eq!(stats.len(), 1, "expected exactly one loaded shard");
    let encoded =
        serde_json::to_value(&stats[0].storage).expect("the canonical storage stats must serialize");

    let groups = mirror_groups(&encoded);
    for (canonical, members) in &groups {
        let on_a_roster = members.len() >= 2
