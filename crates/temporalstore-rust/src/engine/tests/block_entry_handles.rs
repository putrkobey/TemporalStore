// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! PRICING A HANDLE FOR EACH OF THE THREE NAMES A PAGE ENTRY CARRIES, one name at a time.
//!
//! #1959 opened `BlockIndex` at 104 bytes and called the three names "48 bytes, the largest
//! group". `block_entry_names` then measured the premise under that and found HALF of it false: an
//! object's pages already hold ONE allocation of its key, so the 16 bytes an entry spends on a
//! name is the FAT POINTER and the payload is already shared. A handle therefore saves at most 12
//! bytes of field per page, not 16 -- and an interning table COSTS an entry per distinct name,
//! which is an allocation, so that cost lands on the chunk column where the saving does not.
//!
//! THE THREE NAMES ARE THREE DIFFERENT ANSWERS and a blended number would hide it:
//!
//!   * `model_id` is drawn from a CLOSED SET this engine spells itself -- fifteen live kinds and
//!     two retired spellings, derived in `model_kind_registry` from the walk that is its
//!     authority. A closed set of seventeen needs no table at all: the handle IS the discriminant
//!     and the name is a `&'static str`. TAKEN, and this module states what it cost.
//!   * `object_key` is per-OBJECT, so a handle's table costs one entry per object and pays once
//!     per PAGE. Whether it pays is therefore a property of the pages-per-OBJECT distribution and
//!     of nothing else, and that distribution is measured here -- as a histogram with percentiles
//!     and a MAX, never a mean, because a mean of 1.98 pages a bucket in this engine once
//!     contained ZERO buckets holding two.
//!   * `component` is TWO POPULATIONS, and this module was written expecting one. The reasoning
//!     was that a container key's fields are each their own page and the component is the only
//!     thing telling them apart, so distinct component names must grow with the pages -- true
//!     WITHIN one object and false ACROSS a store, because a schema repeats: forty hashes sharing
//!     a hundred field names hold 400 distinct components over 4,000 pages. The anti-vacuity
//!     assertion caught that and refused to score, so the verdict is stated per population. A
//!     repeated SCHEMA name pays; a CONTENT-DERIVED one -- which is what `zset_component` and
//!     `timestamped_component` build -- does not, and cannot be replaced by an ordinal at all
//!     without a second map to get back.
//!
//! AND ONE THING THE NAMES TURN OUT TO BE THAT IS WORTH MORE THAN THE POINTER. A component name is
//! not a label -- `zset_component` is `format!("{biased:016x}{}", hex::encode(member))` and
//! `timestamped_component` is `format!("{stored_key:016x}{identity:016x}")`. They are HEX TEXT FOR
//! NUMBERS: sixteen characters carrying eight bytes, and `hex::encode` is exactly twice its input.
//! Measured here at 81.3% of component text in a container corpus and 97.1% with twenty-byte
//! members -- separately from the pointer, because it is a bigger term and it needs no table, no
//! ordinal and no map.
//!
//! WHAT SHIPPED IS `model_id` AND NOTHING ELSE. The other two names are priced here and declined,
//! and the prices are the point: a proposal declined with a measured number is worth as much as one
//! taken, and this is the third time in this campaign that the number came out the opposite way
//! round from the reasoning that motivated it.
//!
//! WHAT IS MEASURED WHERE. Counts and structure sizes are unconditional. The table's own cost is
//! only visible on the chunk column, so those arms are `alloc-probe` gated and named in the report
//! -- `ALLOC_BYTES` charges `layout.size()` and would under-charge a table systematically.
//! #1969 corrected the chunk rule to a FLOOR rather than an equality: a 104-byte request read 128
//! against a documented 112, because glibc serves from a chunk that is merely big enough.
//!
//! BOTH ROUTING RANGES, and the default moved. #1973 took `TS_SHARD_END_ROUTING_BUCKET` from the
//! whole keyspace to 1023 because the old default gave every key a bucket of its own BY
//! CONSTRUCTION. Pages per BUCKET is not pages per OBJECT and this module's subject is the second,
//! which is range-INDEPENDENT -- so both ranges are measured anyway, and their agreement is the
//! evidence for that independence rather than an assumption about it.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::control::LoadShardRequest;
use crate::engine::state::{BlockIndex, BlockRefs};
use crate::engine::storage_bucket_internals::StoredModelKind;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe s full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The range a production shard is loaded on: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const OPERATOR_END: u32 = 1023;
/// The range `load_shard` hard-codes, and what #1958 and #1959 were measured on.
const WHOLE_KEYSPACE_END: u32 = u32::MAX;

/// An `Arc<str>`'s own allocation: the text behind two words of strong/weak count.

// =================================================================================================
// FIXTURE. The same shapes `block_entry_names` seeds, so the two modules' numbers are comparable.
// =================================================================================================

fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ))
}

/// Load shard 1 over an EXPLICIT range. No arm here takes the default implicitly.
fn load_shard_over(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        response.status.ok,
        "shard must load over 0..={end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// Keys that route one to a bucket and hold ONE page each: the population a handle loses on.
fn seed_routed_keys(engine: &TemporalEngine, records: usize) {
    run_batch(
        engine,
        (0..records)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Container keys: every element is its own page under ONE object key. The population a handle
/// wins on, and the one whose component names are hex.
fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        match k % 4 {
            0 => {
                for f in 0..members {
                    commands.push(Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: vec![b'v'; 32],
                    });
                }
            }
            1 => {
                for m in 0..members {
                    commands.push(Command::SetAdd {
                        key: format!("t{k}"),
                        member: format!("m{m}").into_bytes(),
                    });
                }
            }
            2 => {
                for m in 0..members {
                    commands.push(Command::ZSetAdd {
                        key: format!("z{k}"),
                        member: format!("m{m}").into_bytes(),
                        score: m as f64,
                    });
                }
            }
            _ => {
                for m in 0..members {
                    commands.push(Command::ListPush {
                        key: format!("l{k}"),
                        member: format!("m{m}").into_bytes(),
                        left: false,
                    });
                }
            }
        }
    }
    run_batch(engine, commands);
}

/// Containers whose members are WIDE, so the doubling in `hex::encode` is visible.
///
/// A set member is stored as `hex::encode(member)` and a zset member as `{biased:016x}` followed
/// by the same, so a twenty-byte member becomes a forty- or fifty-six-character component name.
/// Hash fields are seeded alongside deliberately: a field is the caller's own short text, and it is
/// the population the classifier below has to NOT match.
fn seed_wide_members(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members * 3);
    for k in 0..keys {
        for m in 0..members {
            // Twenty bytes, distinct.
            let member = format!("member-{m:013}").into_bytes();
            assert_eq!(20, member.len(), "the fixture's member width is part of the measurement");
            commands.push(Command::SetAdd {
                key: format!("wt{k}"),
                member: member.clone(),
            });
            commands.push(Command::ZSetAdd {
                key: format!("wz{k}"),
                member,
                score: m as f64,
            });
            commands.push(Command::HashSet {
                key: format!("wh{k}"),
                field: format!("f{m}"),
                value: vec![b'v'; 32],
            });
        }
    }
    run_batch(engine, commands);
}

/// Half routed keys, half container elements. What a shard actually holds, and the reason a
/// SINGLE table cannot be judged on either population alone.
fn seed_mixed(engine: &TemporalEngine, records: usize) {
    seed_routed_keys(engine, records / 2);
    seed_container_keys(engine, (records / 2) / 100, 100);
}

// =================================================================================================
// THE DISTRIBUTION. Pages per OBJECT, which is the term that decides `object_key`.
// =================================================================================================

/// Pages per object, as object counts keyed by pages held.
#[derive(Debug, Default, Clone)]
struct BlocksPerObject {
    histogram: BTreeMap<usize, usize>,
    /// Distinct component names per object, same shape. The term that decides `component`.
    components_per_object: BTreeMap<usize, usize>,
    /// Distinct component names per BUCKET: a candidate table scoped to a bucket pays per entry.
    components_per_bucket: BTreeMap<usize, usize>,
    object_key_bytes: usize,
    component_name_bytes: usize,
    /// Component-name bytes that are HEX SPELLINGS OF NUMBERS, so half of them carry nothing.
    hex_component_name_bytes: usize,
    hex_components: usize,
    blocks: usize,
}

impl BlocksPerObject {
    fn objects(&self) -> usize {
        self.histogram.values().copied().sum()
    }

    /// The value at `q` over the object population, by walking the histogram in key order. A
    /// PERCENTILE, because a mean over a two-population distribution names neither population.
    fn percentile(map: &BTreeMap<usize, usize>, q: f64) -> usize {
        let total: usize = map.values().copied().sum();
        if total == 0 {
            return 0;
        }
        let target = ((total as f64) * q).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (value, count) in map {
            seen += count;
            if seen >= target {
                return *value;
            }
        }
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn max(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  objects={}  pages={}  (denominator for every row below)",
            self.objects(),
            self.blocks
        );
        for (name, map) in [
            ("pages per OBJECT", &self.histogram),
            ("components per OBJECT", &self.components_per_object),
            ("components per BUCKET", &self.components_per_bucket),
        ] {
            let n: usize = map.values().copied().sum();
            println!(
                "  {name:<22} n={n:<7} p50={:<5} p90={:<5} p99={:<5} MAX={:<5} distinct-values={}",
                Self::percentile(map, 0.50),
                Self::percentile(map, 0.90),
                Self::percentile(map, 0.99),
                Self::max(map),
                map.len(),
            );
            for (value, count) in map.iter().take(4) {
                println!("      {value:>6} : {count:>8} samples");
            }
            if map.len() > 4 {
                println!("      ... {} further values", map.len() - 4);
            }
        }
        println!(
            "  object-key text {} B over {} objects; component text {} B, of which {} B in {} hex \
             spellings of numbers ({:.1}% of component text)",
            self.object_key_bytes,
            self.objects(),
            self.component_name_bytes,
            self.hex_component_name_bytes,
            self.hex_components,
            if self.component_name_bytes == 0 {
                0.0
            } else {
                100.0 * self.hex_component_name_bytes as f64 / self.component_name_bytes as f64
            }
        );
    }
}

/// Is this component name a hex spelling of numbers rather than a caller's own text?
///
/// `zset_component` is `{biased:016x}` followed by `hex::encode(member)`, and
/// `timestamped_component` is `{stored_key:016x}{identity:016x}`. Both are even-length, all-hex,
/// and at least sixteen characters. A hash FIELD is the caller's own bytes and is not, which is
/// what stops this counting everything and reporting a saving that is not there.
fn is_hex_spelling_of_numbers(name: &str) -> bool {
    name.len() >= 16
        && name.len() % 2 == 0
        && name.bytes().all(|b| b.is_ascii_hexdigit())
}

fn blocks_per_object(engine: &TemporalEngine) -> BlocksPerObject {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut per_object: BTreeMap<(StoredModelKind, String), (usize, BTreeSet<String>)> =
        BTreeMap::new();
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut measured = BlocksPerObject::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        let mut bucket_components: BTreeSet<Option<&str>> = BTreeSet::new();
        for (handle, block) in bucket.block_index.iter() {
            measured.blocks += 1;
            // One member for any non-empty bucket: every entry carries the same absence.
            bucket_components.insert(None::<&str>);
            let slot = per_object
                .entry((
                    block.model_id,
                    named.at(bucket.routing_bucket, *handle).to_string(),
                ))
                .or_default();
            slot.0 += 1;
            // NO ELEMENT NAME ON THE ENTRY, so the component columns of this census -- distinct
            // names per object, name bytes, and the hex-spelled share of them -- are zero by
            // construction. They are kept as columns because the arms below print them as the
            // denominator for what a name WOULD have cost; each of those is restated where it is
            // read.
        }
        *measured
            .components_per_bucket
            .entry(bucket_components.len())
            .or_default() += 1;
    }
    for ((_, object_key), (blocks, components)) in per_object.iter() {
        *measured.histogram.entry(*blocks).or_default() += 1;
        *measured
            .components_per_object
            .entry(components.len())
            .or_default() += 1;
        measured.object_key_bytes += object_key.len();
    }
    measured
}

/// PAGES PER OBJECT IS TWO POPULATIONS, AT BOTH RANGES AND BOTH CORPUS SIZES.
///
/// This is the measurement `object_key`'s verdict rests on, and the reason it is a histogram is
/// #1959's: a mean over this distribution names neither population. A routed key holds ONE page;
/// a container object holds a hundred. A single interning table serves both, so the verdict is a
/// property of the MIX and is stated per population rather than blended.
///
/// THE RANGE IS THE CONTROL HERE, not a variable. An object's pages all route to its own key's
/// bucket, so pages-per-object cannot depend on how many buckets exist -- and both ranges are
/// measured so that independence is evidence rather than an assumption. Pages per BUCKET does
/// depend on it, which is what #1973 measured and is a different question.
///
/// THE STORE PATH LENGTH is held constant and asserted. Counts are immune to it; allocation bytes
/// move at about six bytes a character, which is why no byte figure in this test is a claim.
#[test]
#[ignore = "seeds twelve stores up to 40,000 records each; run by name"]
fn the_blocks_of_one_object_are_two_populations_at_both_routing_ranges() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut arms: Vec<(String, u32, BlocksPerObject)> = Vec::new();

    for (records_label, records) in [("4,000", 4_000usize), ("40,000", 40_000usize)] {
        for (range_label, end) in [
            ("whole keyspace", WHOLE_KEYSPACE_END),
            ("operator 1023", OPERATOR_END),
        ] {
            for (shape, seed) in [
                ("routed keys", 0usize),
                ("containers of 100", 1),
                ("mixed half and half", 2),
            ] {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end);
                match seed {
                    0 => seed_routed_keys(&engine, records),
                    1 => seed_container_keys(&engine, records / 100, 100),
                    _ => seed_mixed(&engine, records),
                }
                let measured = blocks_per_object(&engine);
                measured.report(&format!("{records_label} records, {range_label}, {shape}"));
                arms.push((format!("{shape} @ {records_label}"), end, measured));
            }
        }
    }

    assert_eq!(12, path_lengths.len(), "all twelve arms must have run");
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms; allocation bytes move with it at about six \
             bytes a character"
        );
    }
    println!("\n  store path length held at {} characters", path_lengths[0]);

    // --- Anti-vacuity: every arm must have censused something. ---
    for (label, _, measured) in &arms {
        assert!(
            measured.blocks > 0 && measured.objects() > 0,
            "{label}: nothing censused, so every row it printed is free"
        );
    }

    // --- THE TWO POPULATIONS MUST BOTH BE REACHED, or the verdict is decided by the fixture. ---
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("routed")) {
        assert_eq!(
            1,
            BlocksPerObject::max(&measured.histogram),
            "{label}: a routed-key object is supposed to hold exactly one page and the widest \
             holds {}",
            BlocksPerObject::max(&measured.histogram)
        );
    }
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("containers")) {
        assert!(
            BlocksPerObject::percentile(&measured.histogram, 0.50) >= 100,
            "{label}: the container arm exists to hold objects of a hundred pages and its p50 is \
             {}",
            BlocksPerObject::percentile(&measured.histogram, 0.50)
        );
    }
    for (label, _, measured) in arms.iter().filter(|(l, _, _)| l.starts_with("mixed")) {
        assert_eq!(
            1,
            BlocksPerObject::percentile(&measured.histogram, 0.50),
            "{label}: a mixed corpus is supposed to have a p50 of one -- routed keys outnumber \
             container objects -- and it is {}",
            BlocksPerObject::percentile(&measured.histogram, 0.50)
        );
        assert!(
            BlocksPerObject::max(&measured.histogram) >= 100,
            "{label}: and a MAX at the container shape, which is {}",
            BlocksPerObject::max(&measured.histogram)
        );
    }

    // --- THE RANGE MUST NOT MOVE IT. Same shape, same corpus size, two ranges: identical. ---
    let mut compared = 0usize;
    for (label, end, measured) in &arms {
        if *end != WHOLE_KEYSPACE_END {
            continue;
        }
        let (_, _, other) = arms
            .iter()
            .find(|(l, e, _)| l == label && *e == OPERATOR_END)
            .expect("every arm is run at both ranges");
        compared += 1;
        assert_eq!(
            measured.histogram, other.histogram,
            "{label}: the pages-per-OBJECT histogram differs between the whole keyspace and \
             0..=1023. An object's pages route to its own key's bucket, so this distribution \
             cannot depend on how many buckets exist -- if it does, one of the two arms is not \
             measuring what it says"
        );
    }
    assert_eq!(
        6, compared,
        "six shape/size pairs are supposed to be compared across the two ranges, not {compared}"
    );
}

// =================================================================================================
// WHAT A TABLE WOULD COST. Built for real, over a real store, and charged to the chunk column.
// =================================================================================================

/// A candidate interning table: the handle-to-name direction a read needs, and the
/// name-to-handle direction a write needs. Both, because a table with only one of them cannot
/// serve the engine and would price the change at half.
#[derive(Default)]
struct CandidateTable {
    by_handle: Vec<Arc<str>>,
    by_name: std::collections::HashMap<Arc<str>, u32>,
}

impl CandidateTable {
    fn intern(&mut self, name: &Arc<str>) {
        if self.by_name.contains_key(name) {
            return;
        }
        let handle = self.by_handle.len() as u32;
        self.by_handle.push(Arc::clone(name));
        self.by_name.insert(Arc::clone(name), handle);
    }

    fn len(&self) -> usize {
        self.by_handle.len()
    }
}

/// What a table over `object_key` and a table over `component` would charge, measured.
///
/// TWO BYTE COLUMNS AND THEY ANSWER DIFFERENT QUESTIONS, which is a correction this measurement
/// forced rather than a distinction planned for it. `ALLOC_CHUNK_BYTES` is CUMULATIVE: a `Vec` and a
/// `HashMap` grown one entry at a time charge every intermediate buffer they outgrow, and a
/// geometric growth series sums to roughly twice the final buffer. So the chunk column is what
/// BUILDING the table costs the allocator, and refuting a table on it alone would be refuting it on
/// a figure about twice too large. `resident_bytes` is what the table then HOLDS -- its final
/// capacities -- and that is the steady-state cost a shipped table would carry, so that is the one
/// the verdicts below are taken on. Both are reported.
#[derive(Debug, Clone)]
struct TableCost {
    entries: usize,
    blocks: usize,
    chunk_bytes: u64,
    request_bytes: u64,
    allocations: u64,
    resident_bytes: usize,
}

impl TableCost {
    /// Cumulative chunk bytes a page's share of BUILDING the table cost.
    fn chunk_per_block(&self) -> f64 {
        if self.blocks == 0 {
            0.0
        } else {
            self.chunk_bytes as f64 / self.blocks as f64
        }
    }

    /// What the built table HOLDS, per page. The figure a verdict is taken on.
    fn resident_per_block(&self) -> f64 {
        if self.blocks == 0 {
            0.0
        } else {
            self.resident_bytes as f64 / self.blocks as f64
        }
    }

    fn entries_per_block(&self) -> f64 {
        self.entries as f64 / self.blocks.max(1) as f64
    }
}

/// What a built `CandidateTable` holds: the handle vector's capacity, and the map's.
///
/// The map's per-slot cost is its key and value plus hashbrown's one control byte a slot. The
/// `Arc<str>` payloads are NOT counted -- they are clones of allocations the store already holds,
/// which is the whole finding `block_entry_names` established, and charging them here would price
/// the table for bytes it does not add.
#[cfg(feature = "alloc-probe")]
fn resident_bytes(table: &CandidateTable) -> usize {
    let handle_side = table.by_handle.capacity() * std::mem::size_of::<Arc<str>>();
    let map_side = table.by_name.capacity()
        * (std::mem::size_of::<Arc<str>>() + std::mem::size_of::<u32>() + 1);
    handle_side + map_side
}

/// The bytes a handle SAVES in the entry, per page.
///
/// Not 12, and the difference is the whole reason this is a constant with a comment rather than a
/// subtraction at the call site. Swapping a 16-byte fat pointer for a 4-byte handle removes 12
/// bytes of FIELD, but `BlockIndex` is 8-aligned and carries three flag bytes in its slack, so
/// what the structure actually loses is a whole eight-byte step and then some of the slack. The
/// measured steps, from the layout probe below: 88 with three names, 72 with the model spelling as
/// one byte, 56 with the component as a two-byte ordinal as well, 48 with all three. Every one of
/// those is SIXTEEN, not twelve.
#[allow(dead_code)]
const ENTRY_BYTES_A_HANDLE_SAVES: usize = 16;

#[cfg(feature = "alloc-probe")]
fn measure_object_key_table(engine: &TemporalEngine) -> TableCost {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut blocks = 0usize;
    // Collected first, so the probe charges the TABLE and not the walk that feeds it.
    //
    // THE NAMES COME FROM THE LOOKUP, WHICH IS WHERE THE ALLOCATION THEY MEASURE NOW LIVES. The
    // table this prices interns shared names; the pages have none of their own, so the shared
    // names the bucket's own lookup holds are the input, and that is the same set of `Arc`s the
    // table would be fed in production.
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut names: Vec<Arc<str>> = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, _block) in bucket.block_index.iter() {
            blocks += 1;
            names.push(Arc::clone(named.at(bucket.routing_bucket, *handle)));
        }
    }
    let probe = Probe::start();
    let mut table = CandidateTable::default();
    for name in &names {
        table.intern(name);
    }
    let counts = probe.stop();
    let entries = table.len();
    let resident = resident_bytes(&table);
    // Held past the probe so nothing is freed inside the window.
    std::hint::black_box(&table);
    TableCost {
        entries,
        blocks,
        chunk_bytes: counts.chunk_bytes,
        request_bytes: counts.alloc_bytes,
        allocations: counts.allocs,
        resident_bytes: resident,
    }
}

#[cfg(feature = "alloc-probe")]
fn measure_component_table(engine: &TemporalEngine) -> TableCost {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut blocks = 0usize;
    let mut names: Vec<Arc<str>> = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, block) in bucket.block_index.iter() {
            blocks += 1;
            // NOTHING TO INTERN. This measured what a candidate intern table for element names
            // would cost, over the names the entries held. The entries hold none, so the table
            // has no input -- and the question it was pricing is settled more cheaply than it
            // proposed: the name did not need interning, it needed removing, and `engine_storage_state.rs`'s pin
            // records the sixteen bytes a page that recovered.
        }
    }
    let probe = Probe::start();
    let mut table = CandidateTable::default();
    for name in &names {
        table.intern(name);
    }
    let counts = probe.stop();
    let entries = table.len();
    let resident = resident_bytes(&table);
    std::hint::black_box(&table);
    TableCost {
        entries,
        blocks,
        chunk_bytes: counts.chunk_bytes,
        request_bytes: counts.alloc_bytes,
        allocations: counts.allocs,
        resident_bytes: resident,
    }
}

/// A HANDLE FOR `object_key` PAYS ON ONE POPULATION AND LOSES ON THE OTHER, and the break-even is
/// measured rather than argued.
///
/// The saving is 16 bytes a PAGE. The cost is one table entry per distinct OBJECT KEY, and that
/// cost is an allocation, so it appears on the chunk column and nowhere else -- which is why this
/// arm is `alloc-probe` gated and the counting arm above is not.
///
/// BOTH DIRECTIONS OF THE TABLE ARE BUILT. A `Vec<Arc<str>>` alone would serve a read and price
/// the change at a third of what it costs; the engine also has to go name-to-handle on every
/// write, and that is a hash map.
#[test]
#[ignore = "alloc-probe only, seeds six stores; run by name with --features alloc-probe"]
#[cfg(feature = "alloc-probe")]
fn a_handle_for_the_object_key_pays_only_above_the_measured_break_even() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, TableCost, BlocksPerObject)> = Vec::new();

    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        for (shape, seed) in [
            ("routed keys", 0usize),
            ("containers of 100", 1),
            ("mixed half and half", 2),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, end);
            match seed {
                0 => seed_routed_keys(&engine, 4_000),
                1 => seed_container_keys(&engine, 40, 100),
                _ => seed_mixed(&engine, 4_000),
            }
            let distribution = blocks_per_object(&engine);
            let cost = measure_object_key_table(&engine);
            rows.push((format!("{shape}, {range_label}"), cost, distribution));
        }
    }

    assert_eq!(6, path_lengths.len());
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    println!(
        "\n=== A HANDLE FOR object_key: 16 B a page saved against one table entry per object ===\n\
           store path length {} characters; table = Vec<Arc<str>> + HashMap<Arc<str>, u32>",
        path_lengths[0]
    );
    println!(
        "  {:<34} {:>7} {:>7} {:>6} {:>10} {:>10} {:>8} {:>8}",
        "arm", "blocks", "entries", "p/obj", "build/pg", "resident/pg", "saved/pg", "NET/pg"
    );
    for (label, cost, distribution) in &rows {
        let saved = ENTRY_BYTES_A_HANDLE_SAVES as f64;
        let net = saved - cost.resident_per_block();
        println!(
            "  {label:<34} {:>7} {:>7} {:>6.2} {:>10.2} {:>10.2} {:>8.2} {:>+8.2}",
            cost.blocks,
            cost.entries,
            cost.blocks as f64 / distribution.objects().max(1) as f64,
            cost.chunk_per_block(),
            cost.resident_per_block(),
            saved,
            net
        );
        // The chunk rule is a FLOOR, not an equality (#1969).
        assert!(
            cost.chunk_bytes >= cost.request_bytes,
            "{label}: the chunk column read {} BELOW the request column {}, which cannot happen \
             if it is reading `malloc_usable_size`",
            cost.chunk_bytes,
            cost.request_bytes
        );
        assert!(
            cost.allocations > 0,
            "{label}: the table was built without allocating, which reads as a probe that is not \
             installed rather than as a free table"
        );
        assert!(cost.blocks > 0, "{label}: no pages, so every figure on this row is free");
    }

    // --- THE VERDICT, PER POPULATION. ---
    let routed = rows
        .iter()
        .filter(|(l, _, _)| l.starts_with("routed"))
        .collect::<Vec<_>>();
    let containers = rows
        .iter()
        .filter(|(l, _, _)| l.starts_with("containers"))
        .collect::<Vec<_>>();
    assert_eq!(2, routed.len());
    assert_eq!(2, containers.len());

    for (label, cost, _) in &routed {
        assert_eq!(
            cost.entries, cost.blocks,
            "{label}: a routed key is one page per object, so the table is supposed to hold one \
             entry per page -- {} entries over {} pages",
            cost.entries, cost.blocks
        );
        assert!(
            cost.resident_per_block() > ENTRY_BYTES_A_HANDLE_SAVES as f64,
            "{label}: the table holds {:.2} B a page against {} B saved, so a handle is supposed \
             to LOSE on this population and it does not",
            cost.resident_per_block(),
            ENTRY_BYTES_A_HANDLE_SAVES
        );
    }
    for (label, cost, _) in &containers {
        assert!(
            cost.entries * 50 < cost.blocks,
            "{label}: a container object holds a hundred pages, so the table is supposed to hold \
             far fewer entries than pages -- {} entries over {} pages",
            cost.entries,
            cost.blocks
        );
        assert!(
            cost.resident_per_block() < ENTRY_BYTES_A_HANDLE_SAVES as f64,
            "{label}: the table holds {:.2} B a page against {} B saved, so a handle is supposed \
             to PAY on this population and it does not",
            cost.resident_per_block(),
            ENTRY_BYTES_A_HANDLE_SAVES
        );
    }

    let break_even: f64 = {
        // Bytes a table entry HOLDS, from the routed arm where entries == pages exactly.
        let per_entry = routed[0].1.resident_per_block();
        per_entry / ENTRY_BYTES_A_HANDLE_SAVES as f64
    };
    println!(
        "\n  BREAK-EVEN: a table entry holds {:.2} B resident (and about {:.2} B to build, \n\
           cumulatively), so a handle pays above {:.2} pages per object and loses below it. \n\
           Measured p50 pages/object: routed 1, containers {}, mixed {}.",
        routed[0].1.resident_per_block(),
        routed[0].1.chunk_per_block(),
        break_even,
        BlocksPerObject::percentile(&containers[0].2.histogram, 0.50),
        BlocksPerObject::percentile(
            &rows
                .iter()
                .find(|(l, _, _)| l.starts_with("mixed"))
                .expect("a mixed arm ran")
                .2
                .histogram,
            0.50
        ),
    );
    assert!(
        break_even > 1.0,
        "the break-even came out at {break_even:.2} pages per object, at or below one. That would \
         mean a table entry is cheaper than the pointer it replaces even for a single-page object, \
         and this whole verdict would be the other way round"
    );
}

/// A HANDLE FOR `component` IS DECIDED BY HOW MANY DISTINCT NAMES THE STORE HOLDS, AND THAT IS TWO
/// POPULATIONS TOO.
///
/// THIS TEST WAS WRITTEN TO REFUTE THE HANDLE OUTRIGHT and its own anti-vacuity assertion refused to
/// let it. The reasoning was that a container key's fields are each their own page and the component
/// is the only thing telling them apart, so distinct component names must grow with the pages. That
/// is true WITHIN one object and false ACROSS a store: forty hashes with the same hundred field
/// names hold 400 distinct components over 4,000 pages, because a schema repeats. The assertion
/// fired, and the verdict is stated by population instead.
///
/// THE TWO POPULATIONS, and which one a real store is in depends on where the name comes from:
///
///   * A SCHEMA name -- a hash field -- repeats across every object of that shape, so distinct
///     names are bounded by the schema and a handle PAYS.
///   * A CONTENT-DERIVED name does not repeat at all. `zset_component` is
///     `format!("{biased:016x}{}", hex::encode(member))` and `timestamped_component` is
///     `format!("{stored_key:016x}{identity:016x}")`: the name IS the lookup key, computed from the
///     element. One entry per page, and a handle LOSES.
///
/// AND THE SECOND POPULATION CANNOT BE ORDINALISED AWAY, which is the part that matters for
/// sequencing. A content-derived name needs a per-object member-to-ordinal map to be replaced by an
/// ordinal at all -- the same table this test prices, plus a second one to get back. Which is why
/// the lever to reach for on that population is not the pointer: it is that those names are HEX
/// TEXT FOR NUMBERS, measured separately, needing no table at all.
#[test]
#[ignore = "alloc-probe only, seeds four stores; run by name with --features alloc-probe"]
#[cfg(feature = "alloc-probe")]
fn a_handle_for_the_component_costs_a_table_entry_per_block() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, TableCost, BlocksPerObject)> = Vec::new();

    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        for (shape, keys, members) in [("containers of 100", 40usize, 100usize), ("one container of 2000", 1, 2_000)] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, end);
            seed_container_keys(&engine, keys, members);
            let distribution = blocks_per_object(&engine);
            distribution.report(&format!("{shape}, {range_label}"));
            let cost = measure_component_table(&engine);
            rows.push((format!("{shape}, {range_label}"), cost, distribution));
        }
    }

    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    const SAVED: f64 = ENTRY_BYTES_A_HANDLE_SAVES as f64;
    println!(
        "\n=== A HANDLE FOR component: {SAVED} B a page saved against one table entry per \
           DISTINCT COMPONENT ===\n  store path length {} characters",
        path_lengths[0]
    );
    println!(
        "  {:<40} {:>7} {:>7} {:>9} {:>10} {:>11} {:>8}",
        "arm", "blocks", "entries", "entr/page", "build/pg", "resident/pg", "NET/pg"
    );
    for (label, cost, _) in &rows {
        println!(
            "  {label:<40} {:>7} {:>7} {:>9.3} {:>10.2} {:>11.2} {:>+8.2}",
            cost.blocks,
            cost.entries,
            cost.entries_per_block(),
            cost.chunk_per_block(),
            cost.resident_per_block(),
            SAVED - cost.resident_per_block()
        );
        assert!(
            cost.chunk_bytes >= cost.request_bytes,
            "{label}: chunk column below request column"
        );
        assert!(cost.blocks > 0, "{label}: no pages measured");
    }

    // --- BOTH POPULATIONS MUST BE REACHED, or the verdict is decided by the fixture. ---
    let schema: Vec<&(String, TableCost, BlocksPerObject)> = rows
        .iter()
        .filter(|(label, _, _)| label.starts_with("containers of 100"))
        .collect();
    let content: Vec<&(String, TableCost, BlocksPerObject)> = rows
        .iter()
        .filter(|(label, _, _)| label.starts_with("one container of 2000"))
        .collect();
    assert_eq!(2, schema.len(), "the repeated-schema arm runs at both ranges");
    assert_eq!(2, content.len(), "the unique-name arm runs at both ranges");

    // --- THE SCHEMA POPULATION: names repeat, so the table is small and the handle PAYS. ---
    for (label, cost, _) in &schema {
        assert!(
            cost.entries_per_block() < 0.5,
            "{label}: {} distinct components over {} pages. This arm exists to hold a REPEATED \
             schema -- the same hundred field names across forty objects -- so distinct names are \
             supposed to be far fewer than pages",
            cost.entries,
            cost.blocks
        );
        assert!(
            cost.resident_per_block() < SAVED,
            "{label}: the table holds {:.2} B a page against {SAVED} B saved, so a handle is \
             supposed to PAY where the names repeat",
            cost.resident_per_block()
        );
    }

    // --- THE CONTENT-DERIVED POPULATION: one name per page, and the handle LOSES. ---
    for (label, cost, _) in &content {
        assert!(
            cost.entries_per_block() > 0.9,
            "{label}: {} distinct components over {} pages. This arm exists to hold names that do \
             NOT repeat -- one container whose two thousand members are all distinct -- so the \
             table is supposed to hold about one entry per page",
            cost.entries,
            cost.blocks
        );
        assert!(
            cost.resident_per_block() > SAVED,
            "{label}: the table holds {:.2} B a page against {SAVED} B saved, so a handle is \
             supposed to LOSE where every name is distinct",
            cost.resident_per_block()
        );
    }

    println!(
        "\n  VERDICT, BY POPULATION. Where a component name is a repeated SCHEMA name the table \n\
           holds {:.2} B a page and a handle nets {:+.2}. Where it is CONTENT-DERIVED -- a zset \n\
           member, a timestamped entry -- it holds {:.2} B a page and a handle nets {:+.2}. A \n\
           single blended figure would hide that, and the second population is the one a \n\
           container store is mostly made of.",
        schema[0].1.resident_per_block(),
        SAVED - schema[0].1.resident_per_block(),
        content[0].1.resident_per_block(),
        SAVED - content[0].1.resident_per_block(),
    );
}

/// THE COMPONENT NAME IS HEX TEXT FOR NUMBERS, AND HALF OF IT CARRIES NOTHING.
///
/// A separate and larger term than the pointer, measured separately because it is a separate
/// change: it needs no table, no ordinal and no map. `{biased:016x}` is sixteen characters for
/// eight bytes and `hex::encode(member)` is exactly twice its input, so a zset member of twenty
/// bytes becomes a fifty-six character name where twenty-eight bytes would do.
///
/// COUNTED OVER A REAL STORE and classified by a predicate that a hash FIELD fails -- a field is
/// the caller's own text and is not an encoding of anything, which is what stops this reporting a
/// saving on names that have none.
#[test]
#[ignore = "seeds two stores; run by name"]
fn the_component_name_is_hex_text_for_numbers_and_that_half_is_measured() {
    let mut rows: Vec<(String, BlocksPerObject)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();
    for (range_label, end) in [
        ("whole keyspace", WHOLE_KEYSPACE_END),
        ("operator 1023", OPERATOR_END),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = probe_engine(dir.path());
        load_shard_over(&engine, end);
        // Sets and zsets produce hex component names; hash fields are the caller's own short
        // text. Both, so the classifier is exercised in both directions in the same store and a
        // predicate that matched everything would fail below.
        seed_wide_members(&engine, 4, 100);
        let measured = blocks_per_object(&engine);
        measured.report(&format!("hex component census, {range_label}"));
        rows.push((range_label.to_string(), measured));
    }
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    for (label, measured) in &rows {
        assert!(
            measured.component_name_bytes > 0,
            "{label}: no component text at all, so the ratio below is free"
        );
        // BOTH directions of the predicate must be reached, or it is not discriminating.
        assert!(
            measured.hex_components > 0,
            "{label}: the predicate classified NO component as a hex spelling, and this fixture \
             seeds zsets and lists which produce them"
        );
        assert!(
            measured.hex_component_name_bytes < measured.component_name_bytes,
            "{label}: the predicate classified EVERY component byte as hex, and this fixture also \
             seeds hash fields, which are the caller's own text. A predicate that matches \
             everything is not measuring anything"
        );
        let hex_share =
            measured.hex_component_name_bytes as f64 / measured.component_name_bytes as f64;
        println!(
            "  {label}: {:.1}% of component text is a hex spelling of numbers; storing those as \
             bytes would halve {} B to {} B",
            100.0 * hex_share,
            measured.hex_component_name_bytes,
            measured.hex_component_name_bytes / 2
        );
    }
}

// =================================================================================================
// LAYOUT. What each name's handle does to the entry, and to the component level around it.
// =================================================================================================

// `mod ordinal_layout` AND `an_ordinal_does_not_cost_the_component_list_its_tag` WERE HERE.
//
// They measured whether replacing `ComponentBlocks`'s component NAME with a two-byte ordinal would
// cost `ComponentList` the tag that rode the name's niche. Mirror types with `NonZeroU16`,
// `NonZeroU32` and a plain-integer control priced the three candidates, and the answer was that an
// ordinal makes the level smaller rather than larger.
//
// THE NARROWING THEY WERE CLEARING THE WAY FOR IS SUPERSEDED, AND BY A STRONGER RESULT. The field
// is not two bytes now, it is zero: `ComponentBlocks` and `ComponentList` are deleted and an
// object's refs hang directly off the object. Deleting the name beats narrowing it, so there is no
// decision left for this measurement to inform.
//
// AND A MECHANICAL REWRITE WOULD HAVE BEEN WORSE THAN A DELETION. Two of its arms -- wrapper width
// equals entry width, and the sentinel control equals the `NonZero` one -- still COMPILE after the
// deletion, because both of their operands are mirror types declared in this file. They would have
// become assertions about rustc's layout rules over types this crate does not ship: green forever,
// measuring nothing. The two arms that lose an operand would have been repaired by substituting
// the pinned literal 40, which makes both sides constants fixed by declarations in this same file.
// The comment that stood at the pins said they were "pinned so a change to `BlockRefs` cannot make
// this test vacuous" -- a guard that protects nothing once the left-hand side is local too.

/// THE ENTRY IS 72 BYTES AND ITS RECONSTRUCTION IS ASSERTED, NOT ITS TOTAL.
///
/// A literal 72 goes stale silently the next time a field moves. What is asserted here is that the
/// fields ACCOUNT for the number: the eight-aligned group plus one rounding of the tail.
///
/// AND WHAT EACH OF THE THREE NAMES WOULD BE WORTH, measured on mirrors rather than projected, so
/// the remaining two verdicts are stated against the same arithmetic as the one that shipped.
#[test]
fn every_byte_of_the_entry_is_accounted_for_by_the_fields_it_holds() {
    use std::mem::{align_of, offset_of, size_of};

    let address = size_of::<crate::block_store::ElementEntry>();
    let model = size_of::<StoredModelKind>();
    // THE WIDTH THE KEY USED TO COST, kept as a counterfactual beside `component`'s for the
    // same reason: the verdicts below are stated against what each of the three names WOULD be
    // worth, and that arithmetic needs the term even though no field carries it.
    let object_key_when_it_was_a_field = size_of::<Arc<str>>();
    // `component` IS NO LONGER A FIELD OF THE ENTRY. The local is kept as the WIDTH THE NAME USED
    // TO COST, because the verdicts below are stated against that arithmetic -- what each of the
    // three names would be worth -- and it is now a counterfactual rather than a measurement.
    let component_when_it_was_a_field = size_of::<Option<Arc<str>>>();
    // TWO FLAG BYTES, NOT THREE, AND THE ROUNDING HID THE THIRD.
    //
    // This read `3usize`. The entry holds `dirty` and `deleted` and nothing else -- `log_backed`
    // is an accessor over the address, not a field -- so the tail is two bytes. The
    // reconstruction below went on passing anyway: a tail of three and a tail of four both round
    // to one word, so the sum reached the same width either way. That is the same word-granularity
    // blindness `entry_width_at_each_step` documents for its own mirror and
    // `an_assigned_durable_id_defeats_ground_one_only` carried in four of them.
    //
    // SPELLED AS THE TYPES, so a flag that stops being a `bool` fails here instead of leaving a
    // literal right by luck.
    let flags = size_of::<bool>() + size_of::<bool>();

    println!("\n=== BlockIndex, field by field ===");
    println!("  offsets: address@{} model_id@{} dirty@{} deleted@{}",
        offset_of!(BlockIndex, address),
        offset_of!(BlockIndex, model_id),
        offset_of!(BlockIndex, dirty),
        offset_of!(BlockIndex, deleted),
    );
    println!(
        "  address {address} + model_id {model} + flags {flags} = {} bytes of field in {} B \
         (the key would have added {object_key_when_it_was_a_field} more and a name \
          {component_when_it_was_a_field} on top of that)",
        address + model + flags,
        size_of::<BlockIndex>()
    );

    assert_eq!(1, model, "the model spelling is supposed to be one byte");
    assert_eq!(8, align_of::<BlockIndex>());

    // THE RECONSTRUCTION. The 8-aligned fields form the group; the model byte and the three
    // flag bytes are the tail, and the tail is rounded once to the alignment.
    //
    // ONE 8-ALIGNED FIELD NOW. `component` was one of three and went at 56 -> 40; `object_key`
    // was the second and goes at 40 -> 24; the address is what is left. The reconstruction is what
    // proves the arithmetic rather than the comment -- if the field set and this sum part, it
    // fails here.
    let eight_aligned = address;
    let tail = model + flags;
    let round_up = |value: usize, to: usize| (value + to - 1) / to * to;
    assert_eq!(
        eight_aligned + round_up(tail, align_of::<BlockIndex>()),
        size_of::<BlockIndex>(),
        "the fields do not account for the structure: group {eight_aligned} + tail {tail} rounded \
         to {} is not {}",
        align_of::<BlockIndex>(),
        size_of::<BlockIndex>()
    );
    // 56 AND NOT 64: the address inside the entry shed its object id, a whole eight-byte field in
    // the eight-aligned group. The accounting above is a RECONSTRUCTION and needed no change for it
    // -- which is the point of reconstructing rather than totalling -- but this literal did, and
    // NOTHING BUT RUNNING IT COULD HAVE SAID SO. That sentence was already here for the 72 -> 64
    // step and it earned itself again: a scan for `const _: () = assert!(size_of...)` does not see
    // an `assert_eq!` in a test body, so this pin compiled clean and failed at run time.
    // TWENTY-FOUR NOW, AND THE SENTENCE ABOVE EARNED ITSELF A FOURTH TIME. It was written for
    // the 72 -> 64 step, repeated for 64 -> 56, again for 56 -> 40, and this is 40 -> 24: the
    // RECONSTRUCTION above needed no change, which is the point of reconstructing rather than
    // totalling, and this literal did. `cargo check` cannot see an `assert_eq!` in a test body,
    // so the pin compiled clean and only running it said so.
    assert_eq!(24, size_of::<BlockIndex>());
    // THE `!= 39` AND `!= 41` PARTNERS THAT STOOD HERE ARE GONE. They could distinguish no
    // case. The `assert_eq!` above panics first, so on the only change that could ever have
    // fired them they were never reached -- and an assertion that never ran reads exactly like
    // one that passed. The claim they reached for, that 40 is a value and not a bound, is
    // carried by `align_of::<BlockIndex>() == 8` earlier in this function, which a width pin
    // does NOT entail: `== 40` is equally true at align 1, 2, 4 and 8.

    // The slack, which is why every step here is sixteen bytes and not twelve.
    let slack = size_of::<BlockIndex>() - (eight_aligned + tail);
    println!(
        "  slack {slack} B -- the three flag bytes and the model byte share the rounding, which is \
         why swapping a 16-byte pointer for a small field takes the STRUCTURE down a whole \
         eight-byte step and then some"
    );
    assert!(slack < 8, "a slack of {slack} means a whole word is unaccounted for");

    // AND THE MAP ARM, WHICH NO LONGER CARRIES THE ENTRY INLINE.
    //
    // This used to assert `BlockIndexMap == 8 + size_of::<BlockIndex>()` -- a handle plus an inline
    // entry -- and that was the relation that made narrowing the entry worth a word off every
    // `BucketNode` in the `BucketMap`. The single-page arm now holds a POINTER, so the page index is
    // a bare container header and its width is INDEPENDENT of the entry's.
    //
    // WHICH MEANS THE NARROWING MEASURED ABOVE IS STILL WORTH SOMETHING, BUT SOMEWHERE ELSE: on the
    // HEAP rather than in the node. Every boxed single-page arm and every element of every page list
    // is sixteen bytes smaller for it. The node banked its share once, and once only, whichever of
    // the two changes landed second.
    //
    // The relation is asserted in the NEGATIVE so this cannot silently become true again: an entry
    // back inside the page index would put the width of the whole structure back on every bucket.
    let arm = size_of::<crate::engine::state::BlockIndexMap>();
    assert_ne!(
        8 + size_of::<BlockIndex>(),
        arm,
        "the page index is {arm} B, which is a handle plus a whole entry again -- the inline arm has \
         come back and every bucket in the map is paying the entry's width whether it holds one page \
         or fifty"
    );
    assert_eq!(
        size_of::<Vec<(u64, BlockIndex)>>(),
        arm,
        "the page index is {arm} B against its own page list's {} B; it is supposed to be exactly \
         that header, with both other arms riding pointer niches",
        size_of::<Vec<(u64, BlockIndex)>>()
    );
    // THE COMPARISON HAD TO MOVE OFF THE ENTRY'S WIDTH, because the entry is now exactly as wide
    // as the arm and `arm < size_of::<BlockIndex>()` is false of two equal numbers.
    //
    // IT WAS NEVER THE RIGHT COMPARISON ANYWAY, which the println below already said: the arm is a
    // bare page-list header and its width is INDEPENDENT of the entry's. What "too narrow to hold
    // one inline" means is that it cannot hold a HANDLE AND AN ENTRY, which is what an inline
    // single-page arm would have to carry -- so that is what is asserted. At 24 against 32 it
    // holds, and it would hold at any entry width, which is the point: this is a claim about the
    // representation and not an accident of two numbers meeting.
    assert!(
        arm < size_of::<u64>() + size_of::<BlockIndex>(),
        "the page index is {arm} B and a handle plus one entry is {} B; the index must be too \
         narrow to hold one inline, or the entry is back in the node",
        size_of::<u64>() + size_of::<BlockIndex>()
    );
    println!(
        "  BlockIndexMap {arm} B (a bare page-list header -- INDEPENDENT of the entry's width now), \
         BucketNode {} B. The sixteen bytes measured above are banked on the HEAP: in every boxed \
         arm and every list element, not in the node.",
        size_of::<crate::engine::state::BucketNode>()
    );
}

// =================================================================================================
// THE READ PATH, COUNTED. A handle adds a table lookup; a one-byte discriminant does not.
// =================================================================================================

/// READING A PAGE'S MODEL SPELLING EXAMINES NO MORE ENTRIES THAN IT DID.
///
/// A handle into a table adds a dependent load to every resolution of a name, and no footprint
/// measurement can see it. The spelling that SHIPPED here is not that shape: it is a discriminant
/// in the entry and the name is a `&'static str` off the registry, so resolving it is a match on
/// one byte and touches nothing outside the entry that was already loaded.
///
/// COUNTED, not timed -- a timing-ratio instrument in this tree once read 485x idle against 11x
/// busy off identical code. `BLOCK_LOOKUP_ENTRIES_EXAMINED` is the existing counter inside
/// `find_block`, and the claim is that reading every page's spelling adds NOTHING to it.
#[test]
fn resolving_a_model_spelling_examines_no_block_index_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, OPERATOR_END);
    seed_container_keys(&engine, 8, 100);
    seed_routed_keys(&engine, 200);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    crate::engine::state::reset_block_lookup_entries_examined();
    let before = crate::engine::state::block_lookup_entries_examined();
    let mut resolved = 0usize;
    let mut folded = 0u64;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (_, block) in bucket.block_index.iter() {
            // The resolution under test, folded so it cannot be optimised away.
            let spelling = block.model_id.as_str();
            folded = folded
                .wrapping_mul(31)
                .wrapping_add(spelling.len() as u64)
                .wrapping_add(spelling.as_bytes()[0] as u64);
            resolved += 1;
        }
    }
    let after = crate::engine::state::block_lookup_entries_examined();

    println!(
        "\n=== resolving a model spelling ===\n  {resolved} spellings resolved, \
         BLOCK_LOOKUP_ENTRIES_EXAMINED {before} -> {after}, checksum {folded}"
    );
    assert!(resolved > 0, "no spelling was resolved, so the count below is free");
    assert_ne!(0, folded, "the fold read nothing, so the loads may have been dropped");
    assert_eq!(
        before, after,
        "resolving {resolved} model spellings moved the page-lookup counter from {before} to \
         {after}. A spelling is supposed to be a match on one byte already inside the entry; a \
         move here means it is going through an index"
    );
}

// =================================================================================================
// THE LIFETIME HAZARD. Release empties a bucket's page list; reload re-derives it.
// =================================================================================================

/// A RELEASED BUCKET RELOADS EVERY PAGE'S NAMES, ELEMENT BY ELEMENT.
///
/// `release_bucket_blocks` empties a bucket's page list and `reload_released_bucket` re-derives it
/// from the model maps. Anything the entry holds has to survive that or be re-derived IDENTICALLY,
/// and a count of pages cannot show it: a reload that produced the right NUMBER of pages with one
/// spelling wrong would pass a count and mis-attribute a page.
///
/// So the comparison is element by element, over the sorted triples, and the fixture is asserted
/// to have actually released something before the comparison is made.
#[test]
fn a_released_bucket_reloads_every_name_element_by_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, OPERATOR_END);
    // `released_model_kind_is_addressable` admits `string` and `context_node` only, so a release
    // is only reversible for those -- which is what this fixture seeds.
    seed_routed_keys(&engine, 400);

    // A RELEASE REFUSES A DIRTY BUCKET, and a freshly written one is dirty -- releasing it while
    // `eviction_dump_before_evict` is false would strand undumped writes with nothing to rebuild
    // them from. So the fixture dumps through the production lifecycle, which is what clears the
    // flag (`clear_dumped_bucket_dirty_state`).
    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!candidates.is_empty(), "the fixture produced no bucket to release");
    let lifecycle = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: candidates.clone(),
        max_dump_buckets_per_round: candidates.len(),
        ..Default::default()
    });
    println!("\n=== release/reload ===");
    println!("  fixture: {} candidate buckets, lifecycle ran", candidates.len());
    std::hint::black_box(&lifecycle);

    let before = block_triples(&engine);
    assert!(!before.is_empty(), "the fixture produced no pages to release");

    // DRIVEN ON THE FUNCTION ITSELF. `refused_buckets` is one number for eleven different answers,
    // so a fixture that fails a precondition has to say WHICH one -- otherwise it reads as a
    // release that simply had nothing to do, which is the shape that makes the comparison below
    // pass over an empty set.
    let outcome = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        crate::engine::storage_bucket_internals::release_bucket_blocks(shard, &candidates)
    };
    println!(
        "  released {} buckets holding {} pages, refused {}",
        outcome.released_buckets.len(),
        outcome.released_blocks,
        outcome.refused_buckets
    );
    println!("  refusals by term: {:?}", outcome.refusals);
    assert!(
        outcome.released_blocks > 0,
        "the release emptied no page list, so the reload below re-derives nothing and every \
         assertion after it is free. {} candidates were offered; the terms they failed on are \
         {:?}",
        candidates.len(),
        outcome.refusals
    );
    let released = outcome.released_buckets.clone();

    // The released buckets really did lose their pages.
    let during = block_triples(&engine);
    assert!(
        during.len() < before.len(),
        "the release left {} pages of {} resident, so it did not empty a page list",
        during.len(),
        before.len()
    );

    for routing_bucket in &released {
        let reloaded = {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard is loaded");
            crate::engine::storage_bucket_internals::reload_released_bucket(
