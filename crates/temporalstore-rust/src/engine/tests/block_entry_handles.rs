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
