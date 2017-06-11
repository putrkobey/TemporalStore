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
/// Byte for byte the text above as it stood before `generation` became derived: `"g":9` beside a
/// `"pi":4`, and block-ref keys ending `:4:9` to match. This engine has never written that --
/// every production constructor passed `block_id.or(object_id)` -- but an index that DID carry it
/// must not be loaded and silently re-keyed, because the generation is hashed into the page
/// handle and rendered into the key, and those handles are on disk inside the lookup refs.
const OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION: &str = r#"{"bucket_map":{"7":{"routing_slot":7,"layout":"MultiObject","dirty":false,"deleted":false,"meta_loaded":true,"loading":false,"in_memory":true,"ttl_ms":null,"dirty_generation":3,"last_dump_sequence":11,"object_index":[42],"deleted_object_index":[],"page_index":{"hash:k:f0:1:9:3:4:9":{"object_key":"k","model_id":"hash","component":"f0","address":{"a":4294967305,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":true,"deleted":false,"log_backed":false},"string:k::1:0:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967296,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:1:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967297,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:2:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967298,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:k::1:3:3:4:9":{"object_key":"k","model_id":"string","address":{"a":4294967299,"l":3,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true},"string:other::2:1:5:4:9":{"object_key":"other","model_id":"string","address":{"a":8589934593,"l":5,"pi":4,"oi":42,"rs":7,"g":9},"dirty":false,"deleted":false,"log_backed":true}}}}}"#;

/// What this binary must write back after loading `OLD_STORE_INDEX`: the same bytes, MINUS the one
/// key the node no longer holds.
///
/// Named rather than called "the text above", because there are now two index fixtures here and
/// only one of them is the one that loads.
///
/// `last_dump_sequence` is DELETED FROM THE STORED TEXT rather than a second fixture typed out
/// beside it, so every other byte of the comparison is still against what an older binary wrote.
/// The test asserts the deletion actually changed the string, or the expectation would be the
/// stored text unmodified and the round trip would be an equality with itself.
const REMOVED_KEY: &str = "\"last_dump_sequence\":11,";

/// The element name as a stored row carries it, and the page handle that renders it.
///
/// TWO PLACES, BECAUSE `block_index_written_key` RENDERS THE NAME INTO THE MAP KEY as well as the
/// entry carrying it as a field. A canonicalisation that took only one of them would leave the
/// write-back comparison reporting a difference it had caused itself.
const STORED_ELEMENT_NAME: &str = r#""component":"f0","#;
const STORED_NAMED_HANDLE: &str = r#""hash:k:f0:1:9:3:4:4""#;
/// The same handle as this binary renders it: the element slot between the object key and the slab
/// id is empty, which is the shape the five string rows of this fixture already have.
const BLOCK_NAMED_HANDLE: &str = r#""hash:k::1:9:3:4:4""#;

/// Every page of that index, spelled out independently of the text it came from.
///
/// `(object_key, model_id, component, slab, offset, length, dirty, deleted, log_backed)`.
#[allow(clippy::type_complexity)]
const OLD_STORE_BLOCKS: &[(&str, &str, Option<&str>, u64, u64, u64, bool, bool, bool)] = &[
    ("k", "hash", Some("f0"), 1, 9, 3, true, false, false),
    ("k", "string", None, 1, 0, 3, false, false, true),
    ("k", "string", None, 1, 1, 3, false, false, true),
    ("k", "string", None, 1, 2, 3, false, false, true),
    ("k", "string", None, 1, 3, 3, false, false, true),
    ("other", "string", None, 2, 1, 5, false, false, true),
];

// ---------------------------------------------------------------------------------------------
// THE STORED FORMAT, pinned over several shapes.
// ---------------------------------------------------------------------------------------------

/// A page entry in a named shape, built from the declared types so the golden moves when the
/// declaration does rather than going quietly stale.
/// TWO FLAGS, NOT THREE, AND THE SIGNATURE IS WHAT ENFORCES IT.
///
/// This took a three-tuple while the entry carried three flags. When the third left, the body
/// stopped reading `flags.2` and every caller went on passing a bool that went nowhere -- no
/// warning, because a tuple field is not an unused variable. Narrowing the tuple makes the
/// compiler demand the change at each call site, which is the only way a caller finds out.
///
/// AND THE `component` ARGUMENT WENT THE SAME WAY, for the same reason and one step later. The
/// literal below stopped setting an element name when the field left `BlockIndex`, so the argument
/// decided nothing: `block_fixture(Some("f0"), ..)` and `block_fixture(None, ..)` returned the SAME
/// value, and the golden over the second shape was a second copy of the first. It is removed rather
/// than renamed to `_component`, so a caller that still tries to name an element is a compile error.
///
/// THE LITERAL IS EXHAUSTIVE AND HAS NO `..`, which is what makes it the tripwire the fourth shape
/// used to be: an element-name field returning to the entry breaks THIS function to compile, before
/// any golden has a chance to be re-goldened around it.
/// THE OBJECT THIS FIXTURE'S PAGES BELONG TO, named once rather than at each encode.
///
/// The entry carries its own copy today and will not; what writes the stored key is the identity
/// held BESIDE the page, so a fixture that encodes one has to state which object it means. This is
/// the same characters `block_fixture` puts on the entry, and a test comparing the two would be
/// comparing a constant with itself -- the goldens below are what hold them together.
pub(super) const BLOCK_FIXTURE_OBJECT_KEY: &str = "k";

pub(super) fn block_fixture(length: u64, flags: (bool, bool)) -> BlockIndex {
    BlockIndex {
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::String,
        address: crate::block_store::ElementEntry::from_parts(
            1,
            2,
            length,
            Some(4),
            Some(42),
        ),
        dirty: flags.0,
        deleted: flags.1,
    }
}

/// THE STORED SPELLING OF A PAGE ENTRY, character for character, over three shapes.
///
/// Pinned because this line of work is about the entry's REPRESENTATION, and the one way a
/// representation change becomes data loss is by moving a byte nobody was watching.
///
/// THREE SHAPES, NOT FOUR, AND THE FOURTH WAS NOT DROPPED FOR CONVENIENCE. It read: "Four shapes
/// rather than one, because the component is `skip_serializing_if = "Option::is_none"`: a golden
/// over a single shape would pin the branch that omits it and say nothing about the branch that
/// writes it." There is no branch that writes it. `BlockIndex` has no element-name field, so the
/// WITH_COMPONENT shape serialized to the same bytes as PLAIN -- the two goldens were one golden
/// twice, and the control asserting them different (`assert_ne!(plain, with_component)`, "a page
/// carrying a component must not write the same bytes as one without it") had become a comparison
/// of a value with itself, asserted unequal.
///
/// WHAT REPLACES IT IS STRONGER THAN A FOURTH GOLDEN. The shape it was pinning cannot be
/// constructed: `block_fixture`'s literal names every field with no `..`, so the field returning is
/// a COMPILE error here rather than a golden someone updates.
///
/// AND THE THREE THAT REMAIN WERE CHECKED, NOT RE-GOLDENED. `PAGE_ENTRY_PLAIN`,
/// `PAGE_ENTRY_ALL_FLAGS` and `PAGE_ENTRY_OVER_WIDE` are byte-for-byte what they were: the entry's
/// serialized form lost the element-name key and nothing else, and none of these three ever carried
/// one. The stamp this change spends is accounted for in the module header.
///
/// WITH ITS OWN CONTROL. Two entries that both serialized to nothing would pass an equality test
/// against each other, so the shapes are asserted to differ from one another and each spelling is
/// asserted longer than 100 bytes. A comparison that cannot report a difference is vacuous.
/// AND IT DID MOVE, IN EXACTLY ONE SLOT. This test was called
/// `the_stored_spelling_of_a_page_entry_did_not_move` and it is renamed rather than re-goldened,
/// because a test whose NAME asserts the opposite of what it checks is worse than a stale golden:
/// the name is what a future reader greps for.
///
/// WHAT MOVED: `"oi":42` became `"oi":null`. `BlockAddressWire::object_id` carries `rename`,
/// `alias` and `default` but NO `skip_serializing_if`, so an address that no longer holds an id
/// writes the slot as a null instead of dropping it.
///
/// WHY IT IS WRITTEN RATHER THAN RETIRED: the index log packs this struct POSITIONALLY --
/// `encode_index_payload_into` uses a plain `rmp_serde::Serializer`, so a field is a position and
/// not a name -- so retiring the slot would shorten the array and refuse every row already on
/// disk. Keeping it empty is also what lets the decode go on cross-checking an old `g` against
/// `block_id.or(object_id)`, which `an_old_store_whose_generation_disagrees_is_refused_before_the_decode`
/// drives.
#[test]
fn the_stored_spelling_of_a_block_entry_moved_in_exactly_one_slot() {
    // THROUGH THE WRAPPER, WHICH NO LONGER TAKES A KEY. `BLOCK_FIXTURE_OBJECT_KEY` was the
    // characters the derive read off the entry and then the characters this fixture injected; the
    // entry writes neither the object key nor the kind now, so the wrapper takes the page alone and
    // the three goldens below are two keys lighter.
    let plain = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(3, (false, false)),
    })
    .expect("a page entry serializes");
    let all_flags = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(3, (true, true)),
    })
    .expect("a page entry serializes");
    let over_wide = serde_json::to_string(&crate::engine::state::BlockEntryAt {
        block: &block_fixture(u64::MAX, (false, false)),
    })
    .expect("a page entry serializes");

    println!("PLAIN     = {plain}");
    println!("ALL_FLAGS = {all_flags}");
    println!("OVER_WIDE = {over_wide}");

    // THE EXPECTATION IS DERIVED FROM THE STAMP-26 BYTES, NOT RE-GOLDENED FROM THIS RUN.
    //
    // Two keys left the entry, each asserted present in the stamp-26 spelling BEFORE it is removed
    // and absent after -- so a substitution that matched nothing cannot leave the comparison
    // reporting a sameness it never tested, and a key that came back is a failure here rather than
    // a golden somebody updates.
    let at_stamp_27 = |was: &str, label: &str| -> String {
        for (what, spelling) in [
            ("the object key", ENTRY_OBJECT_KEY_AT_STAMP_26),
            ("the kind", ENTRY_MODEL_ID_AT_STAMP_26),
        ] {
            assert_eq!(
                1,
                was.matches(spelling).count(),
                "the stamp-26 {label} spelling does not carry {what} exactly once, so removing it \
                 is not what the derivation below is reporting: {was}"
            );
        }
        let derived = was
            .replace(ENTRY_OBJECT_KEY_AT_STAMP_26, "")
            .replace(ENTRY_MODEL_ID_AT_STAMP_26, "");
        for key in ["\"object_key\"", "\"model_id\""] {
            assert_eq!(
                0,
                derived.matches(key).count(),
                "{key} survives the derivation of the {label} expectation"
            );
        }
        assert_ne!(
            was, derived.as_str(),
            "the derivation of the {label} expectation changed nothing"
        );
        derived
    };

    assert_eq!(
        at_stamp_27(BLOCK_ENTRY_PLAIN_AT_STAMP_26, "plain"),
        plain,
        "the stored spelling of a page entry moved in some way other than dropping the object key \
         and the kind the composite key already spells; if that is intended, the version stamp has \
         to move with it"
    );
    assert_eq!(
        at_stamp_27(BLOCK_ENTRY_ALL_FLAGS_AT_STAMP_26, "all flags"),
        all_flags,
        "the stored spelling of a page entry with every flag set moved"
    );
    assert_eq!(
        at_stamp_27(BLOCK_ENTRY_OVER_WIDE_AT_STAMP_26, "over wide"),
        over_wide,
        "the stored spelling of a page entry whose length saturated moved"
    );
    // AND NO SHAPE WRITES AN ELEMENT-NAME KEY, which is what the retired fourth golden was for.
    // Asserted over all three rather than inferred from the equalities above, because a golden
    // updated in step with a regression would not report one and this will.
    for (name, spelling) in [
        ("plain", &plain),
        ("all flags", &all_flags),
        ("over wide", &over_wide),
    ] {
        assert!(
            !spelling.contains("\"component\""),
            "the {name} spelling writes an element-name key: {spelling}"
        );
    }

    // --- Controls. Without these the assertions above could all be comparing "" with "". ---
    for (name, spelling) in [
        ("plain", &plain),
        ("all flags", &all_flags),
        ("over wide", &over_wide),
    ] {
        // SIXTY, NOT A HUNDRED, AND THE NUMBER MOVED BECAUSE THE DOCUMENT DID. The three spellings
        // lost the object key and the kind at stamp 27, which is about 37 bytes of each, so a
        // hundred-byte floor would now fail on the shortest of them for the very reason this
        // change exists. The floor is there to stop a comparison of two near-empty documents, and
        // sixty is still well clear of that.
        assert!(
            spelling.len() > 60,
            "the {name} spelling is {} bytes; a golden over a near-empty document cannot fail",
            spelling.len()
        );
    }
    assert_ne!(
        plain, all_flags,
        "the flags must reach the stored form, or a dirty page would load clean"
    );
    assert_ne!(
        plain, over_wide,
        "a saturated length must reach the stored form"
    );

    // --- SATURATES RATHER THAN WRAPS. An over-wide length must come back as a value no encoder
    // will accept, never as its own low bits -- `u64::MAX` truncated to `u32` is 4,294,967,295
    // either way, so the case that decides it is the one below. ---
    let wrapped_would_be = (u64::MAX as u32) as u64; // what `as u32` yields: the low bits.
    let saturated = block_fixture(u64::MAX, (false, false)).address.length();
    assert_eq!(
        u32::MAX as u64,
        saturated,
        "an over-wide length must saturate at u32::MAX, not wrap"
    );
    let low_bits = block_fixture(0x1_0000_0003, (false, false)).address.length();
    assert_eq!(
        u32::MAX as u64,
        low_bits,
        "4 GiB + 3 must saturate to {} and not come back as the plausible small number {}",
        u32::MAX,
        3
    );
    assert_ne!(
        3, low_bits,
        "a length that wrapped would read as a shorter record and no error anywhere"
    );
    let _ = wrapped_would_be;
}

/// AN INDEX WRITTEN BEFORE THIS CHANGE, LOADED BY THIS BINARY, COMPARED ELEMENT BY ELEMENT.
///
/// The text below is the shard index a store built at `15583789e` writes for a bucket holding
/// five pages of one object -- four plain and one carrying a component -- plus a second object in
/// the same bucket. It is the bytes, not a paraphrase of them.
///
/// THE DIRECTION THAT MATTERS is the one nothing reports. A reader that cannot find a page does
/// not fail: the page is still on its slab and nothing looks for it. So the assertion is EQUALITY
/// of the whole page set, field by field, against what went in -- never that the set is non-empty
/// afterwards.
///
/// AND THE REVERSE IS NO LONGER THE SAME CLAIM, so it is no longer in this name. "An old index
/// still loads" and "it is written back byte for byte" were one test for as long as both were
/// true. The second stopped being true when the entry shed `log_backed`: this binary writes the
/// stored form one key lighter, by construction, and the format stamp is what pays for it. One
/// name over both would have had to be WEAKENED until it passed, and a weakened version would no
/// longer pin the half that is still exact -- so the write-back claim is restated, as what this
/// binary actually writes, in
/// `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed`.
///
/// THE GENERATION IN THESE BYTES MOVED, AND ONLY IT. When `generation` became derived from
/// `block_id.or(object_id)`, the fixture behind this capture stopped being able to express the
/// independent `9` it had chosen beside a `block_id` of `4` -- a combination no production
/// constructor in this engine has ever emitted. The bytes were regenerated with the capture
/// instrument in this module rather than hand-edited, and they came back the same 1,349 bytes
/// with `"g":4` and keys ending `:4:4`. An index that really did carry the old combination is
/// REFUSED rather than re-keyed; `an_old_store_whose_generation_disagrees_is_refused_before_the_decode`
/// drives it -- named, rather than called "the test directly below", which it stopped being the
/// moment this test split in two.
#[test]
fn an_index_written_before_this_change_loads_block_for_block() {
    let index: crate::engine::state::CoreIndex =
        serde_json::from_str(OLD_STORE_INDEX).expect("an index written at 15583789e must load");

    let bucket = index
        .bucket_map
        .get(&7)
        .expect("the stored bucket must load");

    // --- Element by element, in the order the stored map spells them. ---
    let mut loaded: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool)> =
        super::blocks_with_identity(&index)
            .into_iter()
            .map(|(_, _, object_key, block)| {
            (
                object_key.to_string(),
                block.model_id.to_string(),
                None::<String>,
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
                block.dirty,
                block.deleted,
            )
        })
        .collect();
    loaded.sort();

    let mut expected: Vec<(String, String, Option<String>, u64, u64, u64, bool, bool)> =
        OLD_STORE_BLOCKS
            .iter()
            // THE NINTH COLUMN IS READ NO LONGER, AND THE FIXTURE KEEPS IT ON PURPOSE. It records
            // what the old index STORED, which is the fixture's whole job -- and in some of these
            // rows what it stored was WRONG: a log-resident flag set on a slab-backed page. That
            // disagreement was a documented defect. The property is derived from the address now,
            // so there is no stored copy left to round-trip and nothing to compare it against.
            //
            // AND THE COMPONENT COLUMN IS NOW THE SAME KIND OF COLUMN: KEPT, AND DERIVED THROUGH.
            // It reads `Some("f0")` for the hash row because that is what the old index stored,
            // and the table is the record of those bytes spelled out independently of the text. The
            // expectation is DERIVED from it by applying the one difference this binary makes --
            // the entry has no field to put an element name in, so the decode drops it -- rather
            // than by editing `Some("f0")` to `None`. Editing the table would destroy the record
            // and leave this arm asserting that an index with no element name loads as one with no
            // element name, which is a statement about nothing.
            .map(|(key, model, _component, slab, offset, length, dirty, deleted, _log_backed)| {
                (
                    (*key).to_string(),
                    (*model).to_string(),
                    None::<String>,
                    *slab,
                    *offset,
                    *length,
                    *dirty,
                    *deleted,
                )
            })
            .collect();
    expected.sort();

    assert_eq!(
        expected.len(),
        loaded.len(),
        "the page COUNT moved: {} written, {} loaded",
        expected.len(),
        loaded.len()
    );
    for (index_of, (want, got)) in expected.iter().zip(loaded.iter()).enumerate() {
        assert_eq!(want, got, "page {index_of} came back different");
    }

    // --- Anti-vacuity: the fixture has to hold the shapes the claim is about. ---
    assert!(
        loaded.len() >= 5,
        "the fixture holds {} pages; it is supposed to hold a multi-page object",
        loaded.len()
    );
    // THE DERIVATION ABOVE MUST HAVE SOMETHING TO DERIVE, asserted on the SOURCE rather than on the
    // result. This read `loaded.iter().any(|page| page.2.is_some())` -- "no loaded page carries a
    // component, so the component's stored round trip is untested" -- and no loaded page CAN carry
    // one, so it could only ever fail. What it was protecting is still worth protecting: that the
    // stored bytes really do contain an element name for the decode to drop. So it is asked of the
    // fixture, in both of the places the name appears in a stored row.
    assert!(
        OLD_STORE_BLOCKS.iter().any(|block| block.2.is_some()),
        "no row of the stored page table carries an element name, so dropping it in the \
         derivation above changes nothing and this arm tests no shed key"
    );
    assert!(
        OLD_STORE_INDEX.contains(STORED_ELEMENT_NAME),
        "the stored index text carries no {STORED_ELEMENT_NAME} key, so the tolerant decode this \
         arm is about is not being exercised"
    );
    assert!(
        OLD_STORE_INDEX.contains(STORED_NAMED_HANDLE),
        "the stored index text carries no element-named page handle, so the key shape this arm is \
         about is not being exercised"
    );
    // AND NOTHING THAT LOADED CARRIES ONE, which is the other half of the same statement: the
    // decode is tolerant of the key and drops it rather than failing or keeping it.
    assert!(
        loaded.iter().all(|block| block.2.is_none()),
        "a loaded page carries an element name; the entry has no field for one, so this would mean \
         the field has come back"
    );
    assert!(
        loaded.iter().filter(|block| block.0 == "k").count() > 1,
        "no object in the fixture holds more than one page, so the axis this change is about is \
         not covered by this golden"
    );
    assert!(
        bucket.block_index.len() > 1,
        "the stored bucket holds {} page(s); a single-page bucket would exercise only the inline \
         arm and say nothing about the map arm",
        bucket.block_index.len()
    );

    // --- The control: a DIFFERENT page set must not compare equal, or the comparison above is
    // reporting sameness it cannot actually detect. ---
    let mut injected = loaded.clone();
    injected[0].5 = injected[0].5.wrapping_add(1);
    assert_ne!(
        injected, loaded,
        "the element-by-element comparison cannot report a difference when one is injected"
    );
}

/// WHAT THIS BINARY WRITES BACK WHEN IT IS HANDED AN INDEX WRITTEN BEFORE THIS CHANGE.
///
/// THE OTHER HALF OF THE TEST ABOVE, SPLIT OFF RATHER THAN RELAXED -- see its note for why one
/// name could not keep both claims once the second stopped being true.
///
/// THE FIXTURE IS NOT RESTATED, and the direction this test runs in is the reason.
/// `OLD_STORE_INDEX` records YESTERDAY'S INPUT: the bytes a store at `15583789e` really wrote,
/// whose whole job is to prove that a pre-change index still decodes. Re-goldening it would hand
/// this test today's own output, and it would then assert nothing about the past. So the
/// expectation is DERIVED from those bytes instead, by naming each difference this binary makes to
/// a stored row and applying it -- with every substitution asserted to have FIRED, so none of them
/// can quietly match nothing and leave the comparison reporting a sameness it never tested.
///
/// FOUR DIFFERENCES ARE NAMED, and none of them is a number that moved: a key that is no longer
/// written, a slot retired from the wire struct, a slot written empty, and the flag the entry shed.
#[test]
fn an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed() {
    let index: crate::engine::state::CoreIndex =
        serde_json::from_str(OLD_STORE_INDEX).expect("an index written at 15583789e must load");

    // --- And back: the same bytes but for the keys the node no longer holds. ---
    let without_the_removed_key = OLD_STORE_INDEX.replace(REMOVED_KEY, "");
    assert_ne!(
        OLD_STORE_INDEX, without_the_removed_key.as_str(),
        "the stored fixture does not contain {REMOVED_KEY}, so the expectation below is the stored \
         text unmodified and the round trip proves nothing about the removal"
    );
    // AND THE ROUTING BUCKET IS NOT WRITTEN BACK AT ALL, which is the second content change this
    // round trip has to account for. The address does not hold a routing bucket, and the SLOT is
    // now retired from the wire struct rather than held open and written nil -- see
    // `block_store::shorter_struct_against_an_existing_row` for why a dead slot can leave. So an
    // index written before this loads with its `rs` read and ignored, and is written back with the
    // key ABSENT.
    //
    // That this fixture still LOADS, carrying six `"rs":7` keys no field claims, is the
    // compatibility half of this test and it is the half that matters: a named decoder tolerates a
    // key nothing declares.
    let stored_bucket_keys = without_the_removed_key.matches(STORED_ROUTING_BUCKET).count();
    assert_eq!(
        6, stored_bucket_keys,
        "the fixture must carry a stored routing bucket on each of its six page entries, or the \
         canonicalisation below is not reporting what happens to one"
    );
    let canonical = without_the_removed_key.replace(STORED_ROUTING_BUCKET, EMPTY_ROUTING_BUCKET);
    // AND THE RETIRED IDENTITY SLOT IS WRITTEN BACK EMPTY, which is the second content change this
    // binary makes to a stored row. The slot STAYS -- the log packs positionally -- so what the
    // writer emits is `"oi":null` where the old row carried the id. Canonicalised here for the same
    // reason the routing bucket is: the expectation has to be what THIS binary writes, and the
    // replacement is asserted to have fired so it cannot quietly match nothing.
    let stored_object_id_count = canonical.matches(STORED_OBJECT_ID).count();
    assert_eq!(
        6, stored_object_id_count,
        "the fixture must carry a stored object id on each of its six page entries, or the \
         canonicalisation below is not reporting what happens to one"
    );
    let canonical = canonical.replace(STORED_OBJECT_ID, EMPTIED_OBJECT_ID);
    assert_eq!(
        0,
        canonical.matches(STORED_OBJECT_ID).count(),
        "every stored object id must be emptied in the expectation, not left as it was"
    );
    assert_eq!(
        6,
        canonical.matches(EMPTIED_OBJECT_ID).count(),
        "and each one must be emptied to the null the writer actually emits"
    );
    assert_ne!(
        without_the_removed_key, canonical,
        "the stored fixture does not contain {STORED_ROUTING_BUCKET}, so the expectation below \
         cannot be reporting what happens to a stored routing bucket"
    );
    // COUNTED ON WHAT REMAINS, not on the replacement. `EMPTY_ROUTING_BUCKET` is the empty string
    // now that the slot is retired, and `str::matches("")` answers once per character boundary --
    // so counting occurrences OF it would report the length of the document and pass for any text
    // at all. What has to be true is that no routing-bucket key survives.
    assert_eq!(
        0,
        canonical.matches("\"rs\"").count(),
        "every stored routing bucket must be gone from the expectation, not rewritten to nil: \
         the slot is retired from the wire struct"
    );
    // AND THE LOG-RESIDENT KEY IS DROPPED FROM THE EXPECTATION, COUNTED BOTH WAYS.
    //
    // THE CLAIM THIS TEST MAKES HAS SPLIT IN TWO, and only the first half is unchanged. An index
    // written before this change still LOADS page for page -- asserted above, and that is the
    // tolerant decoder doing its job, since the wire struct declines no unknown key. What it no
    // longer does is write back the SAME BYTES: it writes back one key lighter, because the entry
    // shed the flag nothing maintained. That is precisely what the format stamp is spent on.
    //
    // Canonicalised rather than asserted around, in the same counted shape as the object id above:
    // the count is checked BEFORE so the replacement cannot be reporting on nothing, and checked
    // to zero after so it cannot have half-fired.
    let log_resident_keys = canonical.matches(STORED_LOG_RESIDENT_TRUE).count()
        + canonical.matches(STORED_LOG_RESIDENT_FALSE).count();
    assert_eq!(
        6, log_resident_keys,
        "the fixture must carry a stored log-resident flag on each of its six page entries, or \
         the canonicalisation below is not reporting what happens to one"
    );
    let canonical = canonical
        .replace(STORED_LOG_RESIDENT_TRUE, "")
        .replace(STORED_LOG_RESIDENT_FALSE, "");
    assert_eq!(
        0,
        canonical.matches("\"log_backed\"").count(),
        "every stored log-resident flag must be gone from the expectation: the key is retired \
         from the entry and this binary does not write it"
    );

    // AND THE ELEMENT NAME IS DROPPED FROM THE EXPECTATION IN BOTH PLACES IT APPEARS, COUNTED.
    //
    // THE FIFTH DIFFERENCE, AND THE ONLY ONE THIS BRANCH ADDS. `BlockIndex` has no element-name
    // field, so a stored row that carries one is written back without it -- and the name appears
    // TWICE in a stored row, which is why this is two substitutions and not one:
    //
    //   * the entry's own `"component":"f0"` key, which the wire struct no longer declares; and
    //   * the block_index MAP KEY, `"hash:k:f0:1:9:3:4:4"`. `block_index_written_key` renders the
    //     element name into that key, and it passes `None` now, so the slot between the object key
    //     and the slab id is empty -- the same shape the five string rows in this fixture already
    //     have.
    //
    // A SUBSTITUTION THAT MATCHED ONLY ONE OF THE TWO WOULD LEAVE THE COMPARISON REPORTING A
    // DIFFERENCE IT HAD CAUSED ITSELF, so both are counted before and after, in the same shape as
    // the object id and the log-resident flag above.
    assert_eq!(
        1,
        canonical.matches(STORED_ELEMENT_NAME).count(),
        "the fixture must carry exactly one stored element name, on its one hash row, or the \
         canonicalisation below is not reporting what happens to one"
    );
    assert_eq!(
        1,
        canonical.matches(STORED_NAMED_HANDLE).count(),
        "the fixture must carry exactly one element-named page handle, or the canonicalisation \
         below is not reporting what happens to one"
    );
    let canonical = canonical
        .replace(STORED_ELEMENT_NAME, "")
        .replace(STORED_NAMED_HANDLE, BLOCK_NAMED_HANDLE);
    assert_eq!(
        0,
        canonical.matches("\"component\"").count(),
        "every stored element name must be gone from the expectation: the key is retired from the \
         entry and this binary does not write it"
    );
    assert_eq!(
        0,
        canonical.matches(STORED_NAMED_HANDLE).count(),
        "the element-named page handle must be gone from the expectation, not left as it was"
    );
    assert_eq!(
        1,
        canonical.matches(BLOCK_NAMED_HANDLE).count(),
        "and it must be rewritten to the page-named handle this binary renders"
    );

    // AND THE SIXTH DIFFERENCE: THE ENTRY NO LONGER WRITES THE OBJECT KEY OR THE KIND.
    //
    // Both are slots of the composite key the entry is filed under -- the second and the first --
    // and the decoder reads them back out of it, so the entry writes three keys instead of five.
    // The fixture holds six entries and TWO kinds, so each spelling is counted at its own arity:
    // six object keys across two distinct values, five `string` rows and one `hash` row.
    //
    // COUNTED BEFORE AND AFTER, in the same shape as the four differences above, so a substitution
    // that matched nothing cannot leave the comparison reporting a difference it caused itself.
    for (spelling, expected) in [
        (r#""object_key":"k","#, 5),
        (r#""object_key":"other","#, 1),
        (r#""model_id":"string","#, 5),
        (r#""model_id":"hash","#, 1),
    ] {
        assert_eq!(
            expected,
            canonical.matches(spelling).count(),
            "the fixture must carry {spelling} exactly {expected} time(s), or the removal below \
             is not reporting what happens to it"
        );
    }
    let canonical = canonical
        .replace(r#""object_key":"k","#, "")
        .replace(r#""object_key":"other","#, "")
        .replace(r#""model_id":"string","#, "")
        .replace(r#""model_id":"hash","#, "");
    for key in ["\"object_key\"", "\"model_id\""] {
        assert_eq!(
            0,
            canonical.matches(key).count(),
            "{key} must be gone from the expectation: the composite key already spells it and this \
             binary does not write it on the entry"
        );
    }

    let rewritten = serde_json::to_string(&index).expect("the index re-serializes");
    assert_eq!(
        canonical, rewritten,
        "the index this binary writes back differs from the index it was given in some way other \
         than dropping last_dump_sequence, retiring the address's routing bucket, emptying its \
         identity slot, dropping the log-resident flag the entry shed, dropping the element name \
         from both the entry and its page handle, and dropping the object key and the kind the \
         composite key already spells"
    );
    assert!(
        rewritten.len() > 500,
        "the re-serialized index is {} bytes; a comparison of two near-empty documents cannot \
         report a difference",
        rewritten.len()
    );
}

/// A MIS-VERSIONED STORE IS DRIVEN, AND IT FAILS LOUDLY -- BEFORE THE DECODE COMPLETES.
///
/// The index here is the one above as it stood before `generation` became derived: `"g":9` beside
/// a `"pi":4`, with block-ref keys ending `:4:9` to match. No production constructor in this
/// engine emits that combination, but if one ever reached disk, loading it under the derivation
/// would compute `4` where the writer stored `9` -- and the generation is hashed into the page
/// handle and rendered into the key, so every page in the bucket would be filed under a name the
/// refs already on disk do not use. That is a lost object, reported by nothing.
///
/// So the load must FAIL, and fail where it can still be understood: at the address, during
/// deserialization, naming the two values. The assertions below are on the MESSAGE as well as the
/// failure, because a load that fails for some unrelated reason would pass a bare `is_err`.
///
/// WITH ITS CONTROL, which is the test directly above: the same index with an agreeing generation
/// loads page for page and rewrites as the same 1,349 bytes. Without that, this test would pass
/// just as well if the loader had stopped accepting any index at all.
#[test]
fn an_old_store_whose_generation_disagrees_is_refused_before_the_decode() {
    let outcome: Result<crate::engine::state::CoreIndex, _> =
        serde_json::from_str(OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION);

    let error = outcome.err().expect(
        "an index carrying a generation this binary cannot reproduce must NOT load: deriving a \
         different one silently re-keys every page in the bucket",
    );
    let text = error.to_string();
    println!("REFUSED WITH: {text}");
    assert!(
        text.contains("disagrees with block_id.or(object_id)"),
        "the refusal must name what disagreed, got: {text}"
    );
    assert!(
        text.contains("recompute every page handle"),
        "and say why that matters, got: {text}"
    );

    // The two values themselves, so the message is actionable rather than merely alarming.
    assert!(text.contains('9'), "the stored generation must appear in the message: {text}");
    assert!(text.contains('4'), "and the derived one: {text}");

    // NON-VACUITY: the text this test drives must actually be the disagreeing shape, or the
    // refusal above could be about anything at all.
    assert!(
        OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION.contains(r#""g":9"#),
        "the fixture has stopped carrying the independent generation it exists to drive"
    );
    assert!(
        OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION.contains(r#":4:9""#),
        "the fixture has stopped carrying the keys that match it"
    );
    assert_ne!(
        OLD_STORE_INDEX, OLD_STORE_INDEX_WITH_AN_INDEPENDENT_GENERATION,
        "the two fixtures are the same text, so one of them is not what it claims to be"
    );
}

// ---------------------------------------------------------------------------------------------
// THE READ PATH, and the footprint on the counting allocator.
// ---------------------------------------------------------------------------------------------

// THE SHARED-VERSUS-COPIED TIMING A/B IS RETIRED, WITH ITS THREE HELPERS, AND THIS IS WHAT IT
// MEASURED AND WHY NO VERSION OF IT SURVIVES.
//
// `reading_a_name_is_not_slower_when_an_objects_pages_share_one_allocation` timed two stores
// holding identical text in two REPRESENTATIONS: one where the pages of an object all pointed at
// one `Arc<str>`, and one where `unshare_object_keys` had given each page its own. Its question
// was whether sharing cost a dependent load on the read path.
//
// ITS PREMISE WAS THE FIELD, STATED IN ITS OWN DOC, AND THE DOC HAD BECOME FALSE. It read: "This
// change does not do that: the entry still holds its own `Arc<str>` and reads it exactly as
// before. What moves is which allocation the pointer lands on." The entry holds no `Arc<str>`.
// There is no pointer on it to land anywhere, no second representation to build, and
// `unshare_object_keys` cannot be written at all -- there is no field to reseat.
//
// WHY IT IS NOT RESTATED AGAINST THE LOOKUP. The question it asked is answerable only where two
// arms differ, and the surviving store has ONE arm: every page of an object is handed the one
// allocation its bucket's lookup holds, and an engine that gave each page its own copy is not a
// state this tree can reach. Its own guard said what a one-armed comparison is worth -- "this
// comparison has two identical arms and would report 1.00x whatever the truth is" -- so keeping
// it with the treatment arm removed is the failure it was built to refuse.
//
// AND THE COST IT WAS WATCHING FOR IS NOW A DIFFERENT COST, MEASURED ELSEWHERE. What a reader
// pays to learn a page's object is no longer a pointer chase through the entry; it is a lookup
// inversion, built once per walk. `block_identities_in` and `BlockKeysByPosition` carry that price
// in their own docs, and `the_block_and_the_lookup_point_at_one_object_key` holds the property
// this A/B's "shared" arm stood for: the identity a reader is handed IS the lookup's allocation
// and not a copy of its text.
//
// `widest_object_key_allocations` and `fold_every_name` went with it; both had this one caller.
// `key_allocation_census`, which the first wrapped, has three others and stays.

/// BYTES AND ALLOCATIONS PER PAGE, on the counting allocator, at two corpus sizes.
///
/// ONE INSTRUMENT for both figures. #1959 found a published decline whose sign was wrong because
/// it set a `size_of` saving against an allocator cost; nothing here mixes the two.
///
/// The allocator's own rounding can swallow a byte win while the COUNT stays exact, so both are
/// reported and the count is what is claimed.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe"]
fn what_a_container_store_charges_the_allocator_a_block() {
    for (label, keys) in [("4,000 records", 40usize), ("40,000 records", 400usize)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_length = dir.path().as_os_str().len();
        let engine = probe_engine(dir.path());
        load_shard_over(&engine, u32::MAX);

        let probe = Probe::start();
        seed_container_keys(&engine, keys, 100);
        let counts = probe.stop();

        let census = key_allocation_census(&engine);
        assert!(census.blocks > 0, "denominator: nothing seeded");
        assert!(
            census.blocks / census.objects >= 100,
            "{label}: {} pages over {} objects; the container case is not reached",
            census.blocks,
            census.objects
        );
        println!(
            "\n=== {label} (store path {path_length} chars) ===\n  pages={} objects={}\n  \
             alloc calls={} ({:.4} a page)\n  alloc bytes={} ({:.2} B a page)\n  widest object-key \
             allocation count={}",
            census.blocks,
            census.objects,
            counts.allocs,
            counts.allocs as f64 / census.blocks as f64,
            counts.alloc_bytes,
            counts.alloc_bytes as f64 / census.blocks as f64,
            KeyAllocationCensus::widest(&census.object_key)
        );
        assert!(
            counts.allocs > 0,
            "the probe reported zero allocations for a seed of {} pages, which reads as a path \
             that does not allocate",
            census.blocks
        );
    }
}

/// A COMPONENT CANNOT CROWD A KIND OUT OF THE POOL THEY SHARE.
///
/// `part4::blocks_of_one_kind_spend_one_byte_and_share_one_static_spelling` asserts the PAGE's
/// half of this: a page entry holds one byte for its kind and reads the registry's `&'static str`
/// through it, so a page cannot hold a pooled copy at all. Its fixture is 200 strings and 40
/// hashes whose field is always `"f"` -- ONE component name in the whole store -- so the pool it
/// would have checked holds three entries against a cap of sixty-four and can never be full. The
/// failure this test is about needs the pool FULL, and the only thing in this engine that fills it
/// is a container.
///
/// WHO STILL NEEDS THE POOL, now that the entry does not: the object lookup, whose `by_model` head
/// is keyed by the kind's SHARED name. That is the holder this test protects, and it is why a
/// component crowding a kind out still costs something -- one allocation per (kind, object) head
/// instead of one per kind.
///
/// Both counts are read, and the kind count is the claim. The component pool is asserted to be AT
/// ITS CAP in the same breath, so this cannot pass by the containers having quietly stopped
/// producing components.
#[test]
#[ignore = "seeds a container store; run by name"]
fn a_container_cannot_fill_the_pool_the_kinds_are_interned_in() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine(dir.path());
    load_shard_over(&engine, u32::MAX);
    // Four container keys, one of each kind, a hundred elements each: 400 distinct component
    // names against a pool cap of 64.
    seed_container_keys(&engine, 4, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let pooled = shard.bucket_index.kind_pool.len();
    println!("kind_pool={pooled} entries");

    // The pool must be PAST the component ceiling, or the crowding-out this test is about cannot
    // happen and every assertion below is free.
    assert!(
        pooled > 64,
        "the pool holds {pooled} entries; this fixture is supposed to produce four hundred \
         distinct component names and so drive it past the component ceiling of 64. Below that, \
         a component could not have crowded a kind out even before this change"
    );
    // And still bounded: components stop at 64, kinds may use 16 more, nothing else may.
    assert!(
        pooled <= 64 + 16,
        "the pool holds {pooled} entries against a ceiling of 80; it is supposed to stay bounded \
         by the sum of the two ceilings"
    );

    // AND WHO STILL HOLDS THE POOLED NAME, which is the half of this test the entry no longer
    // decides. A page entry carries the one-byte spelling and no string, so it cannot hold a
    // pooled copy and cannot fail to. The remaining holder is the OBJECT LOOKUP, whose `by_model`
    // head is keyed by the shared name -- that is why the pool is still consulted at all, and it
    // is what the ceilings above are protecting.
    let mut lookup_heads = 0usize;
    let mut pooled_heads = 0usize;
    for (model_id, _object_key, _refs) in shard.bucket_index.object_block_lookup.iter() {
        lookup_heads += 1;
        if let Some(pooled) = shard.bucket_index.kind_pool.get(model_id) {
            if Arc::ptr_eq(pooled, model_id) {
                pooled_heads += 1;
            }
        }
    }
    assert!(
        lookup_heads > 0,
        "the lookup holds no (model, object) head, so there is nothing whose key could be pooled"
    );
    assert_eq!(
        lookup_heads, pooled_heads,
        "{pooled_heads} of {lookup_heads} lookup heads are keyed by the POOLED allocation of \
         their kind; the rest hold a copy, which is the cost the pool exists to remove"
    );

    // And the page's own side of it: one byte, no string, nothing to pool.
    let mut blocks = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        blocks += bucket.block_index.iter().count();
    }
    assert!(blocks > 0, "no pages were recorded; nothing was measured");
    assert_eq!(
        1,
        std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>(),
        "the page entry is supposed to spend one byte on the spelling the pool holds for the lookup",
    );
}

// ---------------------------------------------------------------------------------------------
// LOUD OR SILENT: what a store stamped with the wrong struct version actually does.
