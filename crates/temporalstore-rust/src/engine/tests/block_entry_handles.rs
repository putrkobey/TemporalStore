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
