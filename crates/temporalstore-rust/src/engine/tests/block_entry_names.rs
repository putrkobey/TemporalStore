// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THREE NAMES A PAGE ENTRY CARRIES, and whether they are per-page or per-bucket facts.
//!
//! #1959 ranked `BlockIndex` as the next dominant per-item term -- per PAGE rather than per
//! bucket, and so 52.0x the `BucketNode` total in a container store -- and opened up its 104
//! bytes: the address was 48, three flag bytes sat inside the alignment rounding, and **the three
//! names were 48**, the largest group. It called them "the three shared names" and priced holding
//! them as thin pointers at 80 bytes an entry.
//!
//! EVERY ONE OF THOSE FIGURES IS HISTORY NOW, and they are left in the past tense rather than
//! updated because they are what this module was written against. The entry is 64, the address 24
//! and the names 33 -- `model_id` stopped being a fat pointer and became a one-byte spelling -- and
//! a thin pointer would price at 48 rather than 80. `block_entry_name_pointer.rs` carries that
//! arithmetic, reads every width off the field rather than off a type named at the call site, and
//! declines the change.
//!
//! "SHARED" IS AN ASSUMPTION ABOUT WHERE THE NAMES COME FROM, and this module measures it instead.
//! The three are `object_key`, `model_id` and `component`. If all three were the same for every
//! page of a routing bucket, storing them once per page would be pure duplication and the entry
//! could carry a handle to the bucket's copy. That is the change this module was written to
//! justify, and it does not survive the measurement.
//!
//! WHAT THE COUNTS SAY, over real stores seeded through the engine:
//!
//!   * `component` is a PER-PAGE fact, by construction. A container key's fields, members and
//!     elements are each their own page and they all route to the container key's one bucket; the
//!     component is the only thing that tells them apart. A bucket of 100 pages holds 100 distinct
//!     components. Hoisting it would file a hundred hash fields as one page.
//!   * `object_key` and `model_id` are per-OBJECT facts, and a routing bucket is not an object.
//!     A bucket is `hash(key) % routing_bucket_count`, so whether one bucket holds one object or
//!     forty is a property of the SHARD'S CONFIGURED BUCKET RANGE and nothing else. At the range
//!     `load_shard` uses -- 0..u32::MAX, which is what #1958 and #1959 both measured on -- a
//!     bucket holds one object and the names look shared. At `0..=1023`, which is what
//!     `ingestion.rs` and `metaserver.rs` spell for a real shard, forty objects share a bucket and
//!     the names are not shared at all.
//!
//! SO THE PREMISE IS CONFIGURATION-DEPENDENT, WHICH IS THE WORST WAY FOR IT TO BE WRONG. A change
//! that hoisted the names to the node would pass every test on a store built by `load_shard` and
//! lose pages on a store built by the cluster path -- and it would lose them SILENTLY, because a
//! page filed under the wrong object name is still on its slab and nothing looks for it. Both
//! ranges are measured here, in the same module, so the two cannot be confused again.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::engine::state::BlockIndex;
use crate::control::LoadShardRequest;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe s full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// ---------------------------------------------------------------------------------------------
// THE INSTRUMENT.
// ---------------------------------------------------------------------------------------------

/// How many DISTINCT values of each of the three names a routing bucket's pages carry, as bucket
/// counts keyed by the distinct count.
///
/// A histogram and not a mean for the reason #1959 established: a mean of "1.02 object keys a
/// bucket" is consistent with a store where every bucket holds one object and with a store where
/// one bucket in fifty holds two, and only the second loses data when the name is hoisted.
#[derive(Debug, Default, Clone)]
struct NameSpread {
    object_key: BTreeMap<usize, usize>,
    model_id: BTreeMap<usize, usize>,
    component: BTreeMap<usize, usize>,
    /// Buckets keyed by pages held, so the container case can be asserted reached.
    blocks_held: BTreeMap<usize, usize>,
    /// Buckets whose every page carries the SAME component, counted separately: a bucket holding
    /// one page trivially does, and a claim built on those would be vacuous.
    multi_block_buckets_with_one_component: usize,
    multi_block_buckets: usize,
}

impl NameSpread {
    fn buckets(&self) -> usize {
        self.blocks_held.values().copied().sum()
    }

    fn blocks(&self) -> usize {
        self.blocks_held.iter().map(|(held, n)| held * n).sum()
    }

    fn buckets_with_more_than_one(map: &BTreeMap<usize, usize>) -> usize {
        map.iter().filter(|(d, _)| **d > 1).map(|(_, n)| *n).sum()
    }

    fn widest(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  buckets={} pages={} widest bucket={} page(s)",
            self.buckets(),
            self.blocks(),
            Self::widest(&self.blocks_held)
        );
        for (name, map) in [
            ("object_key", &self.object_key),
            ("model_id", &self.model_id),
            ("component", &self.component),
        ] {
            let buckets = self.buckets();
            let multi = Self::buckets_with_more_than_one(map);
            println!(
                "  {name:<10} : buckets holding >1 distinct value = {multi:>8}  ({:>7.3}%)  widest = {}",
                if buckets == 0 {
                    0.0
                } else {
                    100.0 * multi as f64 / buckets as f64
                },
                Self::widest(map)
            );
            for (distinct, count) in map.iter().take(6) {
                println!("      {distinct:>5} distinct : {count:>8} buckets");
            }
            if map.len() > 6 {
                println!("      ... {} further distinct-counts", map.len() - 6);
            }
        }
        println!(
            "  multi-page buckets = {}, of which all-one-component = {}",
            self.multi_block_buckets, self.multi_block_buckets_with_one_component
        );
    }
}

/// Walk every bucket of the loaded shard and count distinct names across its pages.
///
/// `component` is counted over the OPTION, so a bucket where some pages have a component and some
/// do not counts two -- which is the honest answer for "could one stored value serve them all".
fn name_spread(engine: &TemporalEngine) -> NameSpread {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut spread = NameSpread::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        let mut keys: BTreeSet<&str> = BTreeSet::new();
        let mut models: BTreeSet<&str> = BTreeSet::new();
        let mut components: BTreeSet<Option<&str>> = BTreeSet::new();
        let mut held = 0usize;
        for (handle, block) in bucket.block_index.iter() {
            held += 1;
            keys.insert(named.at(bucket.routing_bucket, *handle));
            models.insert(block.model_id.as_str());
            // Every entry's name is the same absence, so this set holds one member for any
            // non-empty bucket. The histogram it feeds is restated where it is read.
            components.insert(None::<&str>);
        }
        *spread.blocks_held.entry(held).or_default() += 1;
        *spread.object_key.entry(keys.len()).or_default() += 1;
        *spread.model_id.entry(models.len()).or_default() += 1;
        *spread.component.entry(components.len()).or_default() += 1;
        if held > 1 {
            spread.multi_block_buckets += 1;
            if components.len() == 1 {
                spread.multi_block_buckets_with_one_component += 1;
            }
        }
    }
    spread
}

pub(super) fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ))
}

/// Load shard 1 over an EXPLICIT routing-bucket range.
///
/// `load_shard` hard-codes `0..u32::MAX`; `ingestion.rs` and `metaserver.rs` both spell
/// `0..=1023` for a shard of a real cluster. Which one a store was built under decides every
/// number below, so no arm here is allowed to take the default implicitly.
pub(super) fn load_shard_over(engine: &TemporalEngine, end_routing_bucket: u32) {
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

fn ack(response: &crate::types::BatchExecuteResponse) {
    assert!(response.status.ok, "seed must ack: {:?}", response.status);
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        ack(&response);
    }
}

/// Keys that route one to a bucket: plain strings.
pub(super) fn seed_routed_keys(engine: &TemporalEngine, strings_n: usize) {
    run_batch(
        engine,
        (0..strings_n)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Container keys: a hash, a set, a sorted set and a list, each of `members` elements. Every
/// element is its own page and they all route to the container key's one bucket.
pub(super) fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
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

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT.
// ---------------------------------------------------------------------------------------------

/// ARE THE THREE NAMES THE SAME FOR EVERY PAGE OF A BUCKET? Counted, at two corpus sizes and over
/// BOTH routing-bucket ranges this engine ships.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted. Counts are immune to it --
/// allocation bytes are not, at about six bytes a character -- which is why the counts carry the
/// claim and no byte figure appears in this test.
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn the_three_names_in_a_block_entry_are_per_object_and_a_bucket_is_not_an_object() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (label, wide-range spread, cluster-range spread)
    let mut wide_routed: Vec<NameSpread> = Vec::new();
    let mut wide_container: Vec<NameSpread> = Vec::new();
    let mut narrow_routed: Vec<NameSpread> = Vec::new();
    let mut narrow_container: Vec<NameSpread> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        for (range_label, end_routing_bucket) in
            [("0..u32::MAX (load_shard)", u32::MAX), ("0..=1023 (a cluster shard)", 1_023u32)]
        {
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_routed_keys(&engine, records);
                let spread = name_spread(&engine);
                spread.report(&format!("{label}, {range_label}: keys that route one to a bucket"));
                if end_routing_bucket == u32::MAX {
                    wide_routed.push(spread);
                } else {
                    narrow_routed.push(spread);
                }
            }
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_container_keys(&engine, records / 100, 100);
                let spread = name_spread(&engine);
                spread.report(&format!("{label}, {range_label}: container keys, 100 elements each"));
                if end_routing_bucket == u32::MAX {
                    wide_container.push(spread);
                } else {
                    narrow_container.push(spread);
                }
            }
        }
    }

    assert_eq!(
        8,
        path_lengths.len(),
        "all eight arms must have run, or a comparison below compares an arm with itself"
    );
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms ({} then {length})",
            path_lengths[0]
        );
    }

    // --- Denominators, read off the shard, before anything divides by them. ---
    for spread in wide_routed
        .iter()
        .chain(wide_container.iter())
        .chain(narrow_routed.iter())
        .chain(narrow_container.iter())
    {
        assert!(spread.buckets() > 0, "denominator: no routing buckets in this arm");
        assert!(spread.blocks() > 0, "denominator: no page entries in this arm");
    }

    // --- 1. `component` is a PER-PAGE fact, in both ranges. ---
    //
    // The container arm is where this is visible at all, and the assertion is EQUALITY with the
    // pages held rather than ">1": a bucket of a hundred pages holding two distinct components
    // would still refute hoisting, but it would not be the structure this engine actually has.
    for (index, spread) in wide_container.iter().chain(narrow_container.iter()).enumerate() {
        assert_eq!(
            0, spread.multi_block_buckets_with_one_component,
            "container arm {index}: {} multi-page buckets hold ONE distinct component; every page \
             of a container is a distinct field, member or element, so a bucket whose pages shared \
             a component would mean the component had stopped telling pages apart",
            spread.multi_block_buckets_with_one_component
        );
        assert!(
            spread.multi_block_buckets > 0,
            "container arm {index}: no bucket holds more than one page, so this arm cannot say \
             anything about whether a name is shared ACROSS pages -- it would be vacuous"
        );
        assert!(
            NameSpread::widest(&spread.component) >= 100,
            "container arm {index}: the widest bucket holds {} distinct components; the fixture is \
             supposed to reach the container case where the per-page entry dominates at 52x",
            NameSpread::widest(&spread.component)
        );
    }

    // --- 2. Over 0..u32::MAX the OBJECT names look per-bucket. This is the trap. ---
    for (index, spread) in wide_routed.iter().chain(wide_container.iter()).enumerate() {
        assert_eq!(
            0,
            NameSpread::buckets_with_more_than_one(&spread.object_key),
            "wide arm {index}: a bucket already holds more than one object key at 0..u32::MAX"
        );
        assert_eq!(
            0,
            NameSpread::buckets_with_more_than_one(&spread.model_id),
            "wide arm {index}: a bucket already holds more than one model id at 0..u32::MAX"
        );
    }

    // --- 3. Over 0..=1023 they are NOT, and that is the configuration a cluster shard uses. ---
    for (index, spread) in narrow_routed.iter().enumerate() {
        let multi = NameSpread::buckets_with_more_than_one(&spread.object_key);
        assert!(
            multi > 0,
            "cluster-range routed arm {index}: NO bucket holds more than one object key. The \
             whole point of this arm is that a finite bucket space forces objects to share a \
             bucket; if it does not reach that, every claim below is untested"
        );
        // Not EVERY bucket: at 4,000 keys over 1,024 buckets the tail of the hash still leaves a
        // few holding one key, and at 40,000 it leaves none. The claim is that the OVERWHELMING
        // majority hold several -- a rate one unlucky bucket could not produce.
        assert!(
            10 * multi >= 9 * spread.buckets(),
            "cluster-range routed arm {index}: only {multi} of {} buckets hold more than one \
             object key; at 1,024 buckets and thousands of keys nearly all of them should, and a \
             rate this low would mean the arm is not the configuration it is named for",
            spread.buckets()
        );
        assert!(
            NameSpread::widest(&spread.object_key) > 1,
            "cluster-range routed arm {index}: widest bucket holds {} distinct object keys",
            NameSpread::widest(&spread.object_key)
        );
    }

    // --- 4. And a cluster-range CONTAINER store mixes model ids inside one bucket too. ---
    for (index, spread) in narrow_container.iter().enumerate() {
        assert!(
            NameSpread::buckets_with_more_than_one(&spread.model_id) > 0,
            "cluster-range container arm {index}: no bucket holds more than one model id, so the \
             claim that the KIND is not per-bucket either is untested here"
        );
    }

    // --- 5. The two ranges disagree about the same question, which is the finding. ---
    assert_eq!(
        0,
        NameSpread::buckets_with_more_than_one(&wide_routed[0].object_key),
        "control: the wide arm must be the one where the names look shared"
    );
    assert!(
        NameSpread::buckets_with_more_than_one(&narrow_routed[0].object_key) > 0,
        "control: the cluster arm must be the one where they are not. If both arms agreed, this \
         module would be measuring one configuration twice and reporting it as two"
    );
}

// ---------------------------------------------------------------------------------------------
// WHAT THE ENTRY CAN STOP DOING, given that it must keep all three names.
// ---------------------------------------------------------------------------------------------

/// Distinct ALLOCATIONS of each name behind the pages of one object, counted by pointer.
///
/// Contents cannot tell one shared allocation from two equal ones; pointers can. This is the same
/// instrument `part4::the_block_and_the_lookup_point_at_one_object_key` uses, applied across the
/// pages of ONE OBJECT rather than between one page and the lookup -- which is the axis that test
/// cannot see, because every object in its fixture has exactly one page.
#[derive(Debug, Default, Clone)]
struct KeyAllocationCensus {
    /// Objects keyed by how many distinct object-key allocations their pages hold.
    object_key: BTreeMap<usize, usize>,
    /// Objects keyed by how many distinct model-id allocations their pages hold.
    model_id: BTreeMap<usize, usize>,
    objects: usize,
    blocks: usize,
    /// Sum over objects of (distinct allocations - 1), i.e. copies that carry no new information.
    surplus_object_key_allocations: usize,
    /// Bytes those surplus copies hold: the text plus an `Arc` header of two words.
    surplus_object_key_bytes: usize,
}

impl KeyAllocationCensus {
    fn objects_holding_more_than_one(map: &BTreeMap<usize, usize>) -> usize {
        map.iter().filter(|(d, _)| **d > 1).map(|(_, n)| *n).sum()
    }

    fn widest(map: &BTreeMap<usize, usize>) -> usize {
        map.keys().copied().next_back().unwrap_or_default()
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!("  objects={} pages={}", self.objects, self.blocks);
        println!(
            "  object_key : objects holding >1 allocation = {} , widest = {}",
            Self::objects_holding_more_than_one(&self.object_key),
            Self::widest(&self.object_key)
        );
        println!(
            "  model_id   : objects holding >1 allocation = {} , widest = {}",
            Self::objects_holding_more_than_one(&self.model_id),
            Self::widest(&self.model_id)
        );
        println!(
            "  surplus object-key allocations = {} ({:.4} a page), {} B ({:.2} B a page)",
            self.surplus_object_key_allocations,
            if self.blocks == 0 {
                0.0
            } else {
                self.surplus_object_key_allocations as f64 / self.blocks as f64
            },
            self.surplus_object_key_bytes,
            if self.blocks == 0 {
                0.0
            } else {
                self.surplus_object_key_bytes as f64 / self.blocks as f64
            }
        );
    }
}

/// An `Arc<str>`'s own allocation: the text, behind two words of strong/weak count.
const ARC_HEADER_BYTES: usize = 16;

fn key_allocation_census(engine: &TemporalEngine) -> KeyAllocationCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // (model_id, object_key) -> the distinct allocation addresses its pages hold.
    let mut by_object: BTreeMap<(String, String), (BTreeSet<usize>, BTreeSet<usize>, usize)> =
        BTreeMap::new();
    // READ THROUGH THE POSITION MAP, AND THE OBJECT-KEY COLUMN IS NOW A STRUCTURAL CONTROL.
    //
    // This counted the distinct ALLOCATIONS of one object key that its own pages held, and
    // `surplus_object_key_allocations` was the copies beyond the first. A page entry holds no
    // key, so there is nothing per page to be a copy OF: the pages of an object are all handed
    // the one `Arc` their bucket's lookup holds, and the column reads 1 BY CONSTRUCTION.
    //
    // KEPT, FOR THE REASON THE `model_id` COLUMN BESIDE IT IS KEPT -- and the comment there says
    // it first. It is no longer evidence that a sharing pool was consulted; it still
    // DISCRIMINATES, because a read path that copied the text per page instead of cloning the
    // `Arc` would report two here. That is not hypothetical: forty walks in this module tree take
    // a key per page through this same inversion, and a copy per page would reinstate the exact
    // duplication this change removed.
    let named = super::BlockObjects::of(&shard.bucket_index);
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in bucket.block_index.iter() {
            let object_key = named.at(bucket.routing_bucket, *handle);
            let slot = by_object
                .entry((block.model_id.to_string(), object_key.to_string()))
                .or_default();
            slot.0.insert(object_key.as_ptr() as usize);
            // The spelling is a `&'static str` off the registry now, not an allocation: one
            // address per kind for the whole process, so this census reads 1 BY CONSTRUCTION
            // rather than because a pool happened to be consulted. Kept as the control it was.
            slot.1.insert(block.model_id.as_str().as_ptr() as usize);
            slot.2 += 1;
        }
    }
    let mut census = KeyAllocationCensus::default();
    for ((_, object_key), (key_ptrs, model_ptrs, blocks)) in by_object.iter() {
        census.objects += 1;
        census.blocks += blocks;
        *census.object_key.entry(key_ptrs.len()).or_default() += 1;
        *census.model_id.entry(model_ptrs.len()).or_default() += 1;
        let surplus = key_ptrs.len().saturating_sub(1);
        census.surplus_object_key_allocations += surplus;
        census.surplus_object_key_bytes += surplus * (object_key.len() + ARC_HEADER_BYTES);
    }
    census
}

/// THE PAGES OF ONE OBJECT HOLD ONE ALLOCATION OF ITS KEY.
///
/// `part4::the_block_and_the_lookup_point_at_one_object_key` already asserts that a page and the
/// object lookup point at one copy. Its fixture is 64 STRING keys -- one page each -- so the
/// question it asks is only ever asked of a single page, and the axis where the answer differs is
/// the one it cannot reach: an object with MANY pages, filed one page at a time.
///
/// That is the container workload, and it is precisely the workload where #1959 measured the page
/// entry at 52.0x the node total. A hash of a hundred fields is a hundred pages of ONE object; if
/// each page's entry built its own `Arc::from(object_key)`, the store would hold a hundred
/// allocations of one short string and ninety-nine of them would carry nothing the first did not.
///
/// Counted by POINTER. Two equal strings are indistinguishable by contents and cost twice as much,
/// which is the whole reason this is not an equality assertion.
///
/// THE ANTI-VACUITY CHECK IS THE FIXTURE ITSELF: an object with one page trivially holds one
/// allocation, so the assertion is made only after the census is shown to contain objects with
/// many pages, and the widest is asserted to reach the hundred-page case.
#[test]
#[ignore = "seeds four stores; run by name"]
fn the_blocks_of_one_object_hold_one_allocation_of_its_object_key() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut containers: Vec<KeyAllocationCensus> = Vec::new();
    let mut routed: Vec<KeyAllocationCensus> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, u32::MAX);
            seed_container_keys(&engine, records / 100, 100);
            let census = key_allocation_census(&engine);
            census.report(&format!("{label}: container keys, 100 elements each"));
            containers.push(census);
        }
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            load_shard_over(&engine, u32::MAX);
            seed_routed_keys(&engine, records);
            let census = key_allocation_census(&engine);
            census.report(&format!("{label}: keys that route one to a bucket"));
            routed.push(census);
        }
    }

    assert_eq!(4, path_lengths.len(), "all four arms must have run");
    for length in &path_lengths {
        assert_eq!(path_lengths[0], *length, "the store path length moved between arms");
    }

    // --- The fixture must reach the many-page object, or every claim below is free. ---
    for (index, census) in containers.iter().enumerate() {
        assert!(
            census.objects > 0 && census.blocks > 0,
            "container arm {index}: nothing censused"
        );
        assert!(
            census.blocks / census.objects >= 100,
            "container arm {index}: {} pages over {} objects; this arm exists to hold objects of \
             a hundred pages and it does not reach them",
            census.blocks,
            census.objects
        );
    }

    // --- The claim. ---
    for (index, census) in containers.iter().chain(routed.iter()).enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.object_key),
            "arm {index}: an object's pages hold up to {} distinct allocations of one object key; \
             {} surplus copies across {} pages, {} B",
            KeyAllocationCensus::widest(&census.object_key),
            census.surplus_object_key_allocations,
            census.blocks,
            census.surplus_object_key_bytes
        );
        assert_eq!(
            0,
            census.surplus_object_key_allocations,
            "arm {index}: {} allocations of an object key carry nothing the first copy did not",
            census.surplus_object_key_allocations
        );
    }

    // --- And the KIND is shared too, which is the control: a census that reported everything as
    // shared regardless would say the same thing here whether or not the pool existed.
    //
    // SINCE THE ENTRY STOPPED HOLDING A STRING FOR IT, this control is structural: the spelling
    // is a `&'static str` off `model_kind_registry`, so every page of a kind reads the same
    // address and no allocation exists to be duplicated. It is kept because it still discriminates
    // -- a census that reported two here would mean it is no longer reading the registry's
    // spelling -- but it is no longer evidence about a pool. ---
    for (index, census) in containers.iter().enumerate() {
        assert_eq!(
            1,
            KeyAllocationCensus::widest(&census.model_id),
            "container arm {index}: the interned kind is held as {} allocations across one \
             object's pages; if this ever exceeds one the census is measuring something else",
            KeyAllocationCensus::widest(&census.model_id)
        );
    }
}

// ---------------------------------------------------------------------------------------------
// CAPTURE. Prints the bytes the goldens above are pinned against. Not a claim; an instrument.
// ---------------------------------------------------------------------------------------------

/// Print the stored spelling of a page entry in four shapes, and a whole small index.
///
/// Run at a named revision, its output IS the golden. It asserts nothing, so it cannot pass by
/// accident -- it exists to be read.
#[test]
#[ignore = "capture instrument; run by name and read its output"]
fn capture_the_stored_spelling_of_a_block_entry() {
    use crate::block_store::ElementEntry;
    use crate::engine::state::{BucketLayoutState, BucketNode, CoreIndex};

    fn block(
        key: &str,
        model: &str,
        component: Option<&str>,
        slab: u64,
        offset: u64,
        length: u64,
        flags: (bool, bool, bool),
    ) -> BlockIndex {
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::stored_model_kind(model),
            address: ElementEntry::from_parts(
                slab,
                offset,
                length,
                Some(4),
                Some(42),
            ),
            dirty: flags.0,
            deleted: flags.1,
        }
    }

    // THROUGH THE WRAPPER. The entry has no `Serialize` of its own, and it no longer takes an
    // identity either: `object_key` and `model_id` are the composite key's own second and first
    // slots and the decoder reads them back out of it, so the wrapper writes three keys and the
    // bytes printed here are two keys lighter than they were.
    fn encoded(entry: &BlockIndex) -> String {
        serde_json::to_string(&crate::engine::state::BlockEntryAt { block: entry }).unwrap()
    }

    println!(
        "PLAIN          = {}",
        encoded(&block("k", "string", None, 1, 2, 3, (false, false, true)))
    );
    println!(
        "WITH_COMPONENT = {}",
        encoded(&block("k", "string", Some("f0"), 1, 2, 3, (false, false, true)))
    );
    println!(
        "ALL_FLAGS      = {}",
        encoded(&block("k", "string", None, 1, 2, 3, (true, true, true)))
    );
    println!(
        "OVER_WIDE      = {}",
        encoded(&block("k", "string", None, 1, 2, u64::MAX, (false, false, true)))
    );

    // A whole index: one bucket, five pages of one object (one with a component) and a second
    // object in the SAME bucket -- the shape a cluster-range shard produces routinely.
    let mut node = BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::MultiObject,
        flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
        dirty_generation: 3,
        ..BucketNode::default()
    };
    node.object_index.insert(42);
    // THE IDENTITY IS NAMED BESIDE EACH PAGE RATHER THAN READ OFF IT, which is what the fixture
    // now has to do and what every production placement already does.
    //
    // THIS FIXTURE IS WHY THE REFUSAL IS WORTH HAVING. It built a `CoreIndex`, inserted a node
    // holding six pages, and never filed the object lookup -- so the inversion named no position
    // and every stored key below was composed from the entry's own copy. That was invisible while
    // the entry held one. The encoder refuses it now, with the window named, and the fix is to
    // file what a placement files: "bucket 7 holds a page whose stored key would name no object
    // ... the inversion names NO position at all".
    //
    // THE BYTES DID NOT MOVE. `INDEX` below is character-for-character the 1,125 bytes this test
    // printed before the identity became a neighbour, which is the whole claim of the step.
    let blocks: Vec<(Arc<str>, BlockIndex)> = (0..4u64)
        .map(|offset| {
            (
                Arc::from("k"),
                block("k", "string", None, 1, offset, 3, (false, false, true)),
            )
        })
        .chain(std::iter::once((
            Arc::from("k"),
            block("k", "hash", Some("f0"), 1, 9, 3, (true, false, false)),
        )))
        .chain(std::iter::once((
            Arc::from("other"),
            block("other", "string", None, 2, 1, 5, (false, false, true)),
        )))
        .collect();

    let mut live = crate::engine::state::BlockSlabLiveIndex::default();
    for (object_key, entry) in &blocks {
        node.block_index.insert(object_key, entry.clone(), &mut live);
    }

    let mut index = CoreIndex::default();
    index.bucket_map.insert(7, node);
    // ONE FILING PER PLACED PAGE, through the same door the engine files through, with the handle
    // computed from the same terms the map keyed the page under.
    for (object_key, entry) in &blocks {
        let handle = crate::engine::state::block_index_handle(object_key, entry);
        index.insert_object_block_lookup(7, handle, object_key, entry);
    }
    let text = serde_json::to_string(&index).unwrap();
    println!("INDEX_LEN      = {}", text.len());
    println!("INDEX          = {text}");
    println!("PAGES ----------------------------------------------------------");
    let named = super::BlockObjects::of(&index);
    for bucket in index.bucket_map.values() {
        for (handle, entry) in bucket.block_index.iter() {
            println!(
                "        (\"{}\", \"{}\", {}, {}, {}, {}, {}, {}, {}),",
                named.at(bucket.routing_bucket, *handle),
                entry.model_id,
                // The component column is `None` on every row now: an entry names a page. The
                // column is kept in the printed shape so an existing golden still lines up.
                "None".to_string(),
                entry.address.block_slab_id(),
                entry.address.offset(),
                entry.address.length(),
                entry.dirty,
                entry.deleted,
                entry.log_backed()
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE CAPTURED BYTES. Produced by `capture_the_stored_spelling_of_a_block_entry` run at
// `15583789e` and pasted here verbatim, then RE-CAPTURED when the address's slab id and offset
// merged into one word: `"ps":1,"o":2` became `"a":4294967298`, which is 1 in the high 32 bits
// and 2 in the low. The values are the ones the engine printed, not ones computed here -- the
// three guards below print what they built beside what is pinned, so a fixture edited to match a
// wrong output would have to match a wrong output that the engine actually produced.
//
// Every OTHER key in these fixtures is untouched, which is the point of keeping them: the slab id
// and the offset are the only part of the stored spelling this change moves.
// ---------------------------------------------------------------------------------------------

// AND THE THREE ARE KEPT AT THE SPELLING THEY HAD AT STAMP 26, WHICH IS WHAT THEY ARE FOR.
//
// The entry stopped writing `object_key` and `model_id` -- the composite key's own second and
// first slots -- at stamp 27. Editing these three constants to match would hand this module its
// own output and leave the comparison below asserting nothing about what moved. So they stay as
// the bytes a stamp-26 binary wrote, and the expectation is DERIVED from them by naming the two
// keys that left and removing them, with each substitution asserted to have FIRED. That is the
// same shape `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed`
// uses on the whole-index fixture, and for the same reason.

const BLOCK_ENTRY_PLAIN_AT_STAMP_26: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;


const BLOCK_ENTRY_ALL_FLAGS_AT_STAMP_26: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":null,"g":4},"dirty":true,"deleted":true}"#;

const BLOCK_ENTRY_OVER_WIDE_AT_STAMP_26: &str = r#"{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":4294967295,"pi":4,"oi":null,"g":4},"dirty":false,"deleted":false}"#;

/// The entry's own copy of the object key, as a stamp-26 row spells it -- with its trailing comma,
/// so removing it leaves well-formed JSON.
const ENTRY_OBJECT_KEY_AT_STAMP_26: &str = r#""object_key":"k","#;
/// And its own copy of the kind, which is the key's leftmost slot.
const ENTRY_MODEL_ID_AT_STAMP_26: &str = r#""model_id":"string","#;

/// A whole shard index written at `15583789e`: one bucket holding six pages -- five of one object
/// (four plain, one carrying a component and a different kind) and one of a SECOND object in the
/// same bucket, which is what a cluster-range shard produces routinely.
/// The routing bucket an index written before this change carries, and what this binary writes in
/// its place -- which is NOTHING, the slot having been retired from the wire struct rather than
/// held open and written nil. It was `"rs":null,` while the slot was still declared.
/// See `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed`.
const STORED_ROUTING_BUCKET: &str = r#""rs":7,"#;
/// The identity slot as an OLD row spells it, and as this binary writes it back.
///
/// The slot is not retired -- the index log packs the address POSITIONALLY, so dropping a field
/// shortens the array and refuses every existing row. It is written EMPTY instead, which is a
/// change to the stored bytes and is what `SHARD_INDEX_FORMAT_VERSION` 6 pays for.
const STORED_OBJECT_ID: &str = r#""oi":42,"#;
/// The log-resident flag AS THE OLD INDEX STORED IT, both polarities, each with its leading comma
/// so removing one leaves well-formed JSON. The fixture carries six of these across its entries.
const STORED_LOG_RESIDENT_TRUE: &str = r#","log_backed":true"#;
const STORED_LOG_RESIDENT_FALSE: &str = r#","log_backed":false"#;
const EMPTIED_OBJECT_ID: &str = r#""oi":null,"#;
const EMPTY_ROUTING_BUCKET: &str = r#""#;

const OLD_STORE_INDEX: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:4":{"object_key":"k","model_id":"hash","component":"f0","address":{"a":4294967305,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967296,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967297,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:4":{"object_key":"k","model_id":"string","address":{"a":4294967299,"l":3,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:4":{"object_key":"other","model_id":"string","address":{"a":8589934593,"l":5,"pi":4,"oi":42,"rs":7,"g":4},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// THE SAME INDEX, CARRYING THE COMBINATION NO WRITER PRODUCES.
///
