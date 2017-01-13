// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The side-table codec resolves a table's SHAPE from its SPELLING, and that is checked here.
//!
//! # WHAT THE MERGE CLAIMS, AND WHY IT NEEDS A GUARD
//!
//! `address_maps` and `counter_series` became one field holding two different table shapes. The
//! cheap way to serialize that is an externally tagged enum, which writes a variant wrapper around
//! every table. This does not: each stored spelling has exactly ONE shape, so the decode reads the
//! key first and deserializes the value as that key's type, and no tag reaches the wire.
//!
//! That buys the stored bytes staying what they were. It costs a new invariant: **the spelling and
//! the shape must agree, and nothing in the type system says so.** Three things therefore have to
//! hold, and all three are asserted below rather than argued:
//!
//!   1. every domain round-trips as its own shape, with its contents intact;
//!   2. the four spellings are DISTINCT -- a collision would make two domains indistinguishable on
//!      the wire, silently merging them;
//!   3. the spellings are the ones the predecessor fields used, because a serialized map key is a
//!      durable name and changing one is a format change this bump did not pay for.
//!
//! # THE FORWARD-COMPATIBILITY ARM, AND WHY IT IS A SKIP AND NOT A REFUSAL
//!
//! A spelling this binary does not declare is SKIPPED. That is deliberate: a reader must survive a
//! domain a newer binary added, and the stamp is what refuses an image this binary should not serve
//! at all. A refusal here would turn a forward-compatible read into a load failure, which is the
//! opposite of the division of labour the stamp exists for.

use super::state::{SideTable, SideTableData, SideTableShape};
use crate::block_store::ElementEntry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A map holding one entry in every domain, each with recognisable contents.
fn every_domain() -> HashMap<SideTable, SideTableData> {
    let mut tables = HashMap::new();
    for domain in SideTable::ALL {
        let key: Arc<str> = Arc::from(format!("k-{}", domain.as_str()).as_str());
        // EXHAUSTIVE OVER THE SHAPE, not a bool. A fourth shape then fails to compile here rather
        // than leaving a domain silently unexercised by this guard.
        match domain.shape() {
            SideTableShape::Addresses => {
                let mut table = HashMap::new();
                table.insert(key, ElementEntry::from_parts(1, 64, 32, Some(7), Some(9)));
                tables.insert(*domain, SideTableData::Addresses(table));
            }
            SideTableShape::Counters => {
                let mut series = BTreeMap::new();
                series.insert(1_700_000_000_000_u64, 42_i64);
                let mut table = HashMap::new();
                table.insert(key, series);
                tables.insert(*domain, SideTableData::Counters(table));
            }
            SideTableShape::U64s => {
                let mut table = BTreeMap::new();
                table.insert(key, 1_700_000_000_123_u64);
                tables.insert(*domain, SideTableData::U64s(table));
            }
        }
    }
    tables
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Carrier {
    #[serde(default, with = "crate::engine::state::side_table_serde")]
    side_tables: HashMap<SideTable, SideTableData>,
}

#[test]
fn every_side_table_domain_round_trips_as_its_own_shape() {
    let carrier = Carrier { side_tables: every_domain() };
    let json = serde_json::to_string(&carrier).expect("the side tables must serialize");
    let back: Carrier = serde_json::from_str(&json).expect("and deserialize");

    assert_eq!(
        back.side_tables.len(),
        SideTable::ALL.len(),
        "a domain was lost in the round trip. Written: {}, read back: {}. The decode resolves a \
         table's shape from its spelling, so a lost domain means a spelling did not resolve.",
        SideTable::ALL.len(),
        back.side_tables.len()
    );

    for domain in SideTable::ALL {
        match (domain.shape(), back.side_tables.get(domain)) {
            (SideTableShape::Addresses, Some(SideTableData::Addresses(table))) => {
                let entry = table
                    .get(&*format!("k-{}", domain.as_str()))
                    .unwrap_or_else(|| panic!("{} lost its key", domain.as_str()));
                assert_eq!(
                    entry.block_id(),
                    Some(7),
                    "{} round-tripped its table but not its entry's block id",
                    domain.as_str()
                );
            }
            (SideTableShape::Counters, Some(SideTableData::Counters(table))) => {
                let series = table
                    .get(&*format!("k-{}", domain.as_str()))
                    .unwrap_or_else(|| panic!("{} lost its key", domain.as_str()));
                assert_eq!(
                    series.get(&1_700_000_000_000),
                    Some(&42),
                    "{} round-tripped its table but not its series point",
                    domain.as_str()
                );
            }
            (SideTableShape::U64s, Some(SideTableData::U64s(table))) => {
                let held = table
                    .get(&*format!("k-{}", domain.as_str()))
                    .unwrap_or_else(|| panic!("{} lost its key", domain.as_str()));
                assert_eq!(
                    *held, 1_700_000_000_123,
                    "{} round-tripped its table but not its value",
                    domain.as_str()
                );
            }
            (wanted, other) => panic!(
                "{} came back as the WRONG SHAPE. It should hold {wanted:?}; read back: {}. This \
                 is the invariant the whole codec rests on -- the spelling decides the shape -- \
                 and nothing in the type system enforces it.",
                domain.as_str(),
                match other {
                    Some(SideTableData::Addresses(_)) => "an address table",
                    Some(SideTableData::Counters(_)) => "a counter table",
                    Some(SideTableData::U64s(_)) => "a u64 ledger",
                    None => "absent",
                }
            ),
        }
    }
}

#[test]
fn the_six_spellings_are_distinct_and_are_the_ones_the_old_fields_used() {
    // DISTINCT, because the wire key is the only thing separating two domains. A collision would
    // not fail -- it would merge them, and the second written would win.
    let mut seen = std::collections::HashSet::new();
    for domain in SideTable::ALL {
        assert!(
            seen.insert(domain.as_str()),
            "the spelling {:?} is used by more than one domain. Two domains sharing a wire key \
             are indistinguishable on decode and silently become one.",
            domain.as_str()
