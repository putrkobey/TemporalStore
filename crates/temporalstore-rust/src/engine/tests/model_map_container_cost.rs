// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH CONTAINER EACH RESIDENT MODEL MAP SHOULD BE, MEASURED RATHER THAN MATCHED.
//!
//! `ShardState` declares twelve resident address maps. Eleven of them are `HashMap<String, _>`
//! whose value is either a bare `BlockAddress` or an ORDERED inner map; ONE -- `hashes`, at
//! `engine_storage_state.rs:123` -- is `HashMap<String, HashMap<String, BlockAddress>>`, the only nested hashed
//! container in the structure. `the_hash_inner_map_is_the_only_hashed_model_map` asserts that from
//! the declaration text itself rather than from a list written here, because a hand-written
//! subject list goes stale and nothing fails.
//!
//! THE TWO QUESTIONS ARE DIFFERENT AND THE ANSWERS DO NOT CARRY EACH OTHER.
//!
//!   1. THE INNER CONTAINER of `hashes`. A hashed table pays a per-INSTANCE control block and
//!      rounds its bucket count to a power of two; an ordered map or a sorted vector sizes to what
//!      it holds. Whether that matters depends entirely on FIELDS PER HASH, which is why the
//!      histogram comes before any change.
//!
//!   2. THE OUTER MAPS, all five keyed by object key. Measured here and NOT changed: the outer
//!      map is on a read path (`Command::StringGet` resolves straight through `shard.strings`, as
//!      the `BucketNode` doc note in `engine_storage_state.rs` says in as many words), and a flat vector is only
//!      viable at BUCKET granularity, which is a move into the bucket node and not a container
//!      swap.
//!
//! WHAT THE HISTOGRAM TURNED OUT TO BE, and it is a property of the WRITE PATH rather than of a
//! fixture. `write_context_node` in `execute_on_shard.rs` is commented "the one producer of a node
//! page", and it does exactly one thing to the model map:
//!
//!     shard.hashes.entry(object_key.to_string()).or_default()
//!          .insert(CONTEXT_NODE_FIELD.to_string(), address);
//!
//! `CONTEXT_NODE_FIELD` is the constant `"meta"` (`constants.rs:36`). Every read of a context node
//! spells the same constant -- `shard.hashes.get(key).and_then(|f| f.get(CONTEXT_NODE_FIELD))` at
//! nine production sites. So every context node in the store is a hash holding EXACTLY ONE FIELD,
//! and the only producer of a wider one is the Redis-compatible `HashSet`/`HashMultiSet` surface.
//! `the_context_node_write_path_produces_exactly_one_field_per_hash` asserts the site count and
//! the occupancy, so the claim is checked against the tree and not quoted from it.
//!
//! WHICH MAKES THE SHAPE BIMODAL, and a bimodal shape is a warning and not a licence: #1986 found
//! a mixed histogram holding only 1 and 100, and a mean over it would have described neither arm.
//! Both arms are therefore reported with their own sample counts and denominators, and the
//! container verdict is required to hold at BOTH.
//!
//! BOTH BYTE COLUMNS, and the chunk column is the one that decides. `ALLOC_BYTES` charges
//! `layout.size()` -- what the caller asked for -- and `ALLOC_CHUNK_BYTES` reads
//! `malloc_usable_size`, what the allocator actually set aside. #1967's whole finding was that an
//! out-of-line 112-byte payload takes a 128-byte chunk, which inverted a published sign, and
//! #1969 measured a 104-byte request reading 128. The chunk column is asserted as a FLOOR over the
//! request column and never as an equality.
//!
//! PER-INSTANCE AND PER-ENTRY ARE SEPARATE NUMBERS and are reported separately. A container's cost
//! is not linear in its occupancy -- a table steps at every power of two -- so this module does not
//! fit a line through two points and call the intercept an overhead. It measures the FULL cost at
//! each occupancy and reports the MARGINAL cost between neighbouring occupancies beside it, which
//! is what a step function permits.
//!
//! THE FIELD NAME TEXT IS CHARGED OUTSIDE THE PROBE WINDOW, deliberately and stated here so the
//! numbers are not read as totals. Every arm is handed the same pre-built `(String, BlockAddress)`
//! pairs and MOVES them in, so the probe sees the container's own allocation and nothing else --
//! otherwise the shapes would differ by how many times they copied a name and the container
//! comparison would be confounded. What the name text costs is priced on its own, in
//! `what_the_field_name_text_costs_when_every_hash_allocates_its_own`, because for a context node
//! it is the same four characters in every hash in the store.
//!
//! THE STORE PATH LENGTH is held constant across every engine arm and asserted equal: allocation
//! bytes move with it at about six bytes a character, and #1998 watched that confound masquerade
//! as a profile difference.
//!
//! rust-internal throughout: these tests read this crate's own declarations and its counting
//! allocator, and assert no product behaviour.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The declaration text this module reasons about, read at COMPILE TIME from the file that owns
/// it. A copy of the declarations written out here would go stale the moment one moved.
const STATE_DECLARATIONS: &str = include_str!("../engine_storage_state.rs");

/// The narrow end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped
/// default since #1973.
const NARROW_END: u32 = 1023;

/// Context nodes in the small arm, and in the large arm. Two corpus sizes because a table's cost
/// is a step function of occupancy and one size cannot show a step.
const SMALL_KEYS: usize = 4_000;
const LARGE_KEYS: usize = 40_000;

/// The occupancies the inner container is priced at. 1 is the context-node shape -- the whole
/// product path -- and the rest bracket it far enough either side to find where the verdict flips.
const OCCUPANCIES: [usize; 8] = [1, 2, 3, 4, 8, 16, 64, 256];

/// Instances built per occupancy. Enough that a per-instance figure is not one allocator
/// coincidence divided by one.
const INSTANCES: usize = 2_000;

// =================================================================================================
// 1. THE DECLARATIONS, AS THEY ARE IN THE TREE
// =================================================================================================

/// NO NESTED MODEL MAP IS A HASHED TABLE ANY MORE, AND `hashes` IS THE NAMED CONTAINER.
///
/// RETARGETED, and the reason is worth keeping. This guard was written to assert that `hashes` was
/// the ONE nested hashed map -- the finding this module opened on -- and the change it supports
/// REMOVED that declaration, so the guard failed on the very commit that fixed what it was watching.
/// A guard whose subject the fix deletes has to be retargeted at the INVARIANT the fix establishes,
/// not deleted with the subject: zero nested hashed maps, `hashes` carrying the named container, and
/// the ordered floor kept so the count cannot quietly go to zero on both sides at once and agree.
///
/// Derived from the authority -- the `engine_storage_state.rs` source -- rather than from a list of names typed
/// here, which is the shape that goes stale while every assertion still passes.
///
/// rust-internal: reads this crate's own declaration text, no product behaviour
#[test]
fn no_nested_model_map_is_a_hashed_table_and_hashes_carries_the_named_container() {
    let nested_hashed: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .filter(|line| line.contains("pub(super)") && line.contains("HashMap<String, HashMap<"))
        .map(|line| line.trim())
        .collect();
    // THE ORDERED MAPS, WHEREVER THEY NOW LIVE -- which after the container work is two places.
    //
    // This counted `engine_storage_state.rs` alone and the floor below was seventeen. `sets` has since moved its
    // declaration INTO `RecordedSetContainer`, so a `engine_storage_state.rs`-only scan reported sixteen and the
    // floor fired. That is the floor doing its job: it exists so the hashed count above cannot
    // reach zero by declarations VANISHING. The right answer is therefore to follow the
    // declaration, not to lower the number -- lowering it would have retired the only thing
    // stopping this guard from agreeing vacuously as each kind gets a container.
    // ONE MODULE NOW, because there is one type. This read two and concatenated them; the
    // two containers collapsed into `recorded_map`, so the scan follows the declaration
    // there rather than having its floor lowered -- which is the third time this guard has
    // been moved rather than re-goldened.
    let container_declarations: String = include_str!("../recorded_map.rs").to_string();
    let ordered_inner: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .chain(container_declarations.lines())
        .filter(|line| {
            (line.contains("pub(super)") || line.trim().starts_with("entries:"))
                && (line.contains("HashMap<String, BTreeMap<")
                    || line.contains("HashMap<String, K::Elements>"))
        })
        .map(|line| line.trim())
        .collect();
    // AND THE CONTAINER SIDE MUST ACTUALLY CONTRIBUTE, or this is a `engine_storage_state.rs` scan wearing a
    // wider name and the floor would start drifting down again silently.
    assert!(
        container_declarations.contains("entries: HashMap<String, K::Elements>"),
        "the set container's inner declaration is not findable, so the ordered count below is          really a `engine_storage_state.rs`-only count and its floor no longer means what it says"
    );

    println!("--- nested HASHED model maps ({}) ---", nested_hashed.len());
    for line in &nested_hashed {
        println!("  {line}");
    }
    println!("--- nested ORDERED model maps ({}) ---", ordered_inner.len());
    for line in &ordered_inner {
        println!("  {line}");
    }

    assert!(
        !STATE_DECLARATIONS.is_empty() && STATE_DECLARATIONS.len() > 100_000,
        "the declaration text did not load, so every count below would be a zero presented as a \
         finding: {} bytes",
        STATE_DECLARATIONS.len()
    );
    assert_eq!(
        0,
        nested_hashed.len(),
        "a nested HASHED model map is back. This is the shape this module measured at 272 chunk \
         bytes to hold 48 bytes of payload at one field, and a new one needs its own histogram \
         before it ships: {nested_hashed:?}"
    );
    let named: Vec<&str> = STATE_DECLARATIONS
        .lines()
        .filter(|line| line.contains("pub(super) hashes:"))
        .map(|line| line.trim())
        .collect();
    assert_eq!(
        1,
        named.len(),
        "the `hashes` declaration is no longer findable, so this guard is reading nothing: {named:?}"
    );
    // READ THROUGH THE WRAPPER, because the claim did not change when the declaration did.
    //
    // `hashes` is declared as `RecordedHashContainer` now -- the type that owns the map together
    // with the record of its mutations -- and the sorted-vector container this module priced is one
    // layer down, inside it. So this asserts BOTH halves: that the field is the recorded container,
    // and that the recorded container is still a map of the named sorted vector. Relaxing this to
    // "contains anything" would have let the inner container change back to a hashed table without
    // a single test noticing, which is exactly what the 272-versus-64 measurement bought.
    assert!(
        named[0].contains("RecordedMap<super::recorded_map::HashKind>"),
        "`hashes` is no longer the recorded container, so the mutation-record invariant may be \
         gone as well as the pricing: {}",
        named[0]
    );
    let container = include_str!("../recorded_map.rs");
    assert!(
        container.len() > 8_000,
        "the recorded container's source did not load, so the inner-container check below would \
         pass on an empty string: {} bytes",
        container.len()
    );
    // THE EXACT DECLARATION, not the name anywhere. Asking for a line CONTAINING
    // `entries: HashMap<String, HashFieldMap>` found three -- the field, a `_for_test` setter's
    // PARAMETER and a `From` impl's parameter -- and the guard failed on two lines that declare
    // nothing. That was driven. The field declaration is one exact line, so that is what is asked
    // for, and the control below is the other direction: the string must still be findable at all.
    let inner: Vec<&str> = container
        .lines()
        .map(|line| line.trim())
        .filter(|line| *line == "type Elements = super::hash_field_map::HashFieldMap;")
        .collect();
    assert!(
        container.contains("type Elements = super::hash_field_map::HashFieldMap;"),
        "the inner declaration is not findable in any form, so the exact match below would report \
         zero for the wrong reason"
    );
    assert_eq!(
        1,
        inner.len(),
        "`HashKind` no longer names `HashFieldMap` as its level-two container, so \
         `hashes` no longer carries the named container this module priced: {inner:?}"
    );
    assert!(
        ordered_inner.len() >= 17,
        "fewer than seventeen nested ORDERED model maps were found. The floor is here so the \
         hashed count above cannot reach zero by the declarations vanishing rather than by the \
         container changing -- which would make this whole guard agree vacuously: {ordered_inner:?}"
    );
    println!(
        "nested containers: 0 hashed, {} ordered, 1 named sorted-vector (`hashes`)",
        ordered_inner.len()
    );
}

/// THE ONE PRODUCER OF A CONTEXT NODE PAGE FILES IT UNDER ONE FIELD, and nine readers spell that
/// same field.
///
/// This is the fact the histogram below is a measurement OF. It is asserted from the source text
/// because it is a property of the write path and not of any fixture: a fixture that happened to
/// write one field would prove nothing, and #1959's whole measurement was a fixture artefact of
/// exactly that kind.
///
/// rust-internal: reads this crate's own source text, no product behaviour
#[test]
fn the_context_node_write_path_produces_exactly_one_field_per_hash() {
    let execute = include_str!("../execute_on_shard.rs");
    let context = include_str!("../engine_context.rs");
    let constants = include_str!("../engine_constants.rs");

    assert!(
        execute.len() > 50_000 && context.len() > 10_000 && constants.len() > 500,
        "a source file did not load, so the counts below would be zeros presented as findings: \
         {} / {} / {}",
        execute.len(),
        context.len(),
        constants.len()
    );

    assert!(
        constants.contains("CONTEXT_NODE_FIELD: &str = \"meta\""),
        "CONTEXT_NODE_FIELD is no longer the constant this module priced"
    );

    // Every place a context node's address is taken out of the inner map spells the ONE constant.
    let readers = execute.matches("fields.get(CONTEXT_NODE_FIELD)").count()
        + context.matches("fields.get(CONTEXT_NODE_FIELD)").count();
    // And the one write site files that constant and nothing else.
    //
    // THE SHAPE HAS MOVED TWICE NOW, AND THIS MATCHER HAS MOVED WITH IT BOTH TIMES. The producer
    // first spelled `.insert(CONTEXT_NODE_FIELD.to_string(), address)` straight into the model map.
    // Then it minted a record and installed a proof, because the map's inner field became private.
    // Now it calls ONE function that does both. Each time, the fact this test pins -- exactly one
    // producer of a context-node page -- was unchanged, and each time the matcher reported zero
    // until it was updated. That is the right failure mode: a rename makes this test fail loudly
    // rather than pass while counting nothing, and the assertion below names the number it saw.
    //
    // The count is also no longer the whole guarantee: `install_context_node_element` is the only
    // function that can reach the model map under a context node's field, because the mutator it
    // calls is private to the container's module. So a second producer cannot appear without
    // appearing HERE.
    // RESTATED A FOURTH TIME, and the count of restatements is itself the point: this matcher has
    // tracked an inline `.insert(CONTEXT_NODE_FIELD.to_string(), address)`, then a record mint, then
    // a one-call `install_context_node_element`, and now the ONE generic operation named with the
    // kind it is for. Each time the FACT it pins -- exactly one producer of a context-node page --
    // was unchanged, and each time the matcher reported zero until it was moved. Reporting zero
    // loudly is the right failure mode; being relaxed to accept anything would not be.
    //
    // The count is also no longer the whole guarantee. `install_element_staged_only` is the only
    // operation that files a context node's record, the mutator behind it is private to
    // `recorded_map`, and the map's inner field is private -- so a second producer cannot appear
    // without appearing HERE.
    let writers = execute
        .matches("install_element_staged_only::<super::recorded_map::HashKind>(")
        .count();
    // VACUITY: the name must be findable at all, so a future rename cannot make `writers == 0` and
    // then have someone "fix" this test by lowering the expected count to zero.
    assert!(
        execute.contains("install_element_staged_only"),
        "the context-node install is not findable by name in `execute_on_shard.rs`, so the count          below would be zero for the wrong reason"
    );

    println!("context-node inner-map readers spelling the one constant field: {readers}");
    println!("context-node inner-map writers inserting it:                   {writers}");

    assert!(
        readers >= 6,
        "fewer readers than expected resolve a context node through the single constant field, so \
         the one-field claim is no longer what the code says: {readers}"
    );
    assert_eq!(
        1, writers,
        "there is no longer exactly one producer inserting the single context-node field, so \
         `write_context_node`'s own comment -- \"the one producer of a node page\" -- is stale and \
         the occupancy claim has to be re-derived: {writers}"
    );

    // The field name itself, which every hash in a context store allocates its own copy of.
    assert_eq!(
        4,
        crate::engine::constants::CONTEXT_NODE_FIELD.len(),
        "the field width this module prices moved"
    );
}

// =================================================================================================
// 2. THE HISTOGRAM. NEVER A MEAN.
// =================================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "mmcc".to_string(),
        shard_uri: "local://mmcc/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
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

fn node_at(node_hash: u64) -> crate::types::ContextNode {
    crate::types::ContextNode {
        node_hash,
        parent_hash: 0,
        kind: 1,
        canonical_name: format!("node_{node_hash:08}"),
        l0: "a node".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    }
}

/// Seed `keys` context nodes through the PRODUCTION command path.
fn seed_context_nodes(engine: &TemporalEngine, keys: usize) {
    let commands: Vec<Command> = (0..keys as u64)
        .map(|n| Command::ContextUpsertNode {
            tenant_hash: 7,
            node: Box::new(node_at(n + 1)),
        })
        .collect();
    run_batch(engine, commands);
}

/// Seed `keys` Redis-surface hashes of `fields` fields each, through the production command path.
fn seed_wide_hashes(engine: &TemporalEngine, keys: usize, fields: usize) {
    let commands: Vec<Command> = (0..keys)
        .flat_map(|k| {
            (0..fields).map(move |f| Command::HashSet {
                key: format!("h-{k:06}"),
                field: format!("f-{f:04}"),
                value: vec![b'v'; 16],
            })
        })
        .collect();
    run_batch(engine, commands);
}

/// Seed `keys` sets of `members` members each, through the production command path. This is the
/// CONTROL's population: `sets` is one of the seventeen nested ORDERED model maps and the change
/// does not touch it.
fn seed_sets(engine: &TemporalEngine, keys: usize, members: usize) {
    let commands: Vec<Command> = (0..keys)
        .flat_map(|k| {
            (0..members).map(move |m| Command::SetAdd {
                key: format!("s-{k:06}"),
                member: format!("m-{m:06}").into_bytes(),
            })
        })
        .collect();
    run_batch(engine, commands);
}

/// The occupancies of every inner map in the loaded shard, ascending.
fn fields_per_hash(engine: &TemporalEngine) -> Vec<usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut occupancies: Vec<usize> = shard.hashes.values().map(|fields| fields.len()).collect();
    occupancies.sort_unstable();
    occupancies
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// Print a histogram -- the distinct occupancies with their counts and share -- plus percentiles,
/// MAX and the denominator. Never a mean standing alone: a mean of 1.98 pages a bucket in this
/// engine covered a population with ZERO buckets holding two.
fn report_histogram(label: &str, sorted: &[usize]) {
    println!("--- fields per hash: {label} ---");
    println!("  sample count (hash keys): {}", sorted.len());
    let total_fields: usize = sorted.iter().sum();
    println!("  total fields:             {total_fields}");
    let mut run: Vec<(usize, usize)> = Vec::new();
    for &value in sorted {
        match run.last_mut() {
            Some((v, c)) if *v == value => *c += 1,
            _ => run.push((value, 1)),
        }
    }
    println!("  distinct occupancies: {}", run.len());
    for (value, count) in &run {
        println!(
            "    {value:>6} field(s): {count:>8} keys  ({:>6.2}% of {})",
            100.0 * *count as f64 / sorted.len() as f64,
            sorted.len()
        );
    }
    println!(
        "  p50 {}  p90 {}  p99 {}  MAX {}  (mean {:.2}, quoted only beside the shape above)",
        percentile(sorted, 50.0),
        percentile(sorted, 90.0),
        percentile(sorted, 99.0),
        sorted.last().copied().unwrap_or(0),
        total_fields as f64 / sorted.len().max(1) as f64
    );
}

/// THE FIELDS-PER-HASH HISTOGRAM AT TWO CORPUS SIZES, ON BOTH PRODUCERS.
///
/// The context arm is the product path and the wide arm is the Redis-compatible surface. Both are
/// reported with their own sample count and denominator, and the fixture is asserted to have
/// reached the population claimed before any figure is read off it.
///
/// rust-internal: reads this crate's own resident maps, no product behaviour
#[test]
fn the_fields_per_hash_histogram_at_two_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();

    for keys in [SMALL_KEYS, LARGE_KEYS] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, keys);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            keys,
            sorted.len(),
            "the fixture did not reach the population it claims at {keys} context nodes -- every \
             figure below would describe a different store than the one named"
        );
        report_histogram(&format!("{keys} context nodes, the product write path"), &sorted);

        assert_eq!(
            Some(&1),
            sorted.last(),
            "a context-node corpus produced a hash holding more than one field, so the write \
             path is no longer the single-field shape this module measured"
        );
    }

    // The other producer: the Redis-compatible surface, which is where a wide hash comes from.
    for fields in [8usize, 100] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        let keys = 400;
        seed_wide_hashes(&engine, keys, fields);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            keys,
            sorted.len(),
            "the wide-hash fixture did not reach {keys} keys at {fields} fields each"
        );
        assert_eq!(
            keys * fields,
            sorted.iter().sum::<usize>(),
            "the wide-hash fixture did not write {fields} fields on every key"
        );
        report_histogram(&format!("{keys} HashSet hashes of {fields} fields"), &sorted);
    }

    // And the mixed corpus, because the two producers share one map and a real store holds both.
    {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine);
        seed_context_nodes(&engine, SMALL_KEYS);
        seed_wide_hashes(&engine, 40, 100);

        let sorted = fields_per_hash(&engine);
        assert_eq!(
            SMALL_KEYS + 40,
            sorted.len(),
            "the mixed fixture did not reach both populations"
        );
        report_histogram("mixed: 4,000 context nodes + 40 hashes of 100 fields", &sorted);
        println!(
            "  THE SHAPE IS BIMODAL: {:.2}% of keys hold exactly one field and carry {:.2}% of \
             the fields; a mean over this describes neither arm.",
            100.0 * sorted.iter().filter(|&&n| n == 1).count() as f64 / sorted.len() as f64,
            100.0 * sorted.iter().filter(|&&n| n == 1).count() as f64
                / sorted.iter().sum::<usize>() as f64
        );
    }

    assert!(
        path_lengths.windows(2).all(|w| w[0] == w[1]),
        "the store path length differs between arms: {path_lengths:?} -- allocation bytes move at \
         about six a character, so the arms are not comparable"
    );
    println!("store path length held at {} characters", path_lengths[0]);
}

// =================================================================================================
// 3. WHAT EACH CANDIDATE CONTAINER COSTS, PER INSTANCE AND PER ENTRY
// =================================================================================================

/// One priced shape's reading at one occupancy.
#[cfg(feature = "alloc-probe")]
#[derive(Debug, Clone, Copy)]
struct Reading {
    occupancy: usize,
    allocs_per_instance: f64,
    request_per_instance: f64,
    chunk_per_instance: f64,
}

/// Pre-built field-name/address pairs, one batch per instance. Built OUTSIDE every probe window so
/// the name text is charged to none of the arms and the comparison is of containers only.
#[cfg(feature = "alloc-probe")]
fn pairs_for(instances: usize, occupancy: usize) -> Vec<Vec<(String, ElementEntry)>> {
    (0..instances)
        .map(|i| {
            (0..occupancy)
                .map(|f| (format!("f-{i:05}-{f:05}"), ElementEntry::default()))
                .collect()
        })
        .collect()
}

#[cfg(feature = "alloc-probe")]
fn report_shape(name: &str, readings: &[Reading]) {
    println!("--- {name} ---");
    println!(
        "  {:>9}  {:>10}  {:>12}  {:>12}  {:>14}  {:>14}",
        "fields", "allocs", "request B", "chunk B", "marginal req B", "marginal chk B"
    );
    for (i, r) in readings.iter().enumerate() {
        let (mreq, mchk) = if i == 0 {
            (f64::NAN, f64::NAN)
        } else {
            let prev = readings[i - 1];
            let dn = (r.occupancy - prev.occupancy) as f64;
            (
                (r.request_per_instance - prev.request_per_instance) / dn,
                (r.chunk_per_instance - prev.chunk_per_instance) / dn,
            )
        };
        println!(
            "  {:>9}  {:>10.2}  {:>12.1}  {:>12.1}  {:>14.1}  {:>14.1}",
            r.occupancy,
            r.allocs_per_instance,
            r.request_per_instance,
            r.chunk_per_instance,
            mreq,
            mchk
        );
    }
}

/// WHAT A HASH FIELD MAP COSTS PER INSTANCE AND PER FIELD, ON BOTH BYTE COLUMNS, FOR ALL THREE
/// CANDIDATE SHAPES.
///
/// PER-INSTANCE AND PER-FIELD ARE REPORTED AS SEPARATE COLUMNS AND NEITHER IS A FITTED
/// INTERCEPT. A table's cost steps at every power of two, so a line through two points would put
/// a step into the intercept and call it an overhead. The full per-instance cost is printed at
/// each occupancy and the MARGINAL cost per added field beside it; the occupancy-1 row IS the
/// per-instance overhead for the shape the product path actually writes.
///
/// The field-name text is charged OUTSIDE the probe window for every arm -- the pairs are built
/// first and MOVED in -- so the only thing measured is the container's own allocation.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_a_hash_field_map_costs_per_instance_and_per_field() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    let mut hashed: Vec<Reading> = Vec::new();
    let mut ordered: Vec<Reading> = Vec::new();
    let mut flat: Vec<Reading> = Vec::new();

    for occupancy in OCCUPANCIES {
        // --- today's shape: a hashed table per hash key ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            let built: Vec<HashMap<String, ElementEntry>> = batches
                .into_iter()
                .map(|batch| batch.into_iter().collect())
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|m| m.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the hashed arm did not hold the population it claims at occupancy {occupancy}"
            );
            assert!(
                counts.chunk_bytes > counts.alloc_bytes,
                "the chunk column equals the request column at occupancy {occupancy}, which is \
                 what the platform fallback looks like -- the reading is not coming from \
                 malloc_usable_size"
            );
            hashed.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }

        // --- the siblings' shape: an ordered map per hash key ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            let built: Vec<BTreeMap<String, ElementEntry>> = batches
                .into_iter()
                .map(|batch| batch.into_iter().collect())
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|m| m.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the ordered arm did not hold the population it claims at occupancy {occupancy}"
            );
            ordered.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }

        // --- #1964's shape for the page index: one exact-sized sorted vector ---
        {
            let batches = pairs_for(INSTANCES, occupancy);
            let probe = Probe::start();
            // A FRESH, EXACT-SIZED buffer allocated INSIDE the window. Sorting the batch in place
            // and calling `shrink_to_fit` on it read 0.00 B at every occupancy -- the buffer had
            // been allocated by `pairs_for`, outside the window, and the arm was charged for
            // nothing. The vacuity guard at the end of this test is what caught it.
            let built: Vec<Vec<(String, ElementEntry)>> = batches
                .into_iter()
                .map(|batch| {
                    let mut exact: Vec<(String, ElementEntry)> = Vec::with_capacity(occupancy);
                    exact.extend(batch);
                    exact.sort_by(|l, r| l.0.cmp(&r.0));
                    exact
                })
                .collect();
            let counts = probe.stop();
            let entries: usize = built.iter().map(|v| v.len()).sum();
            assert_eq!(
                INSTANCES * occupancy,
                entries,
                "the flat arm did not hold the population it claims at occupancy {occupancy}"
            );
            flat.push(Reading {
                occupancy,
                allocs_per_instance: counts.allocs as f64 / INSTANCES as f64,
                request_per_instance: counts.alloc_bytes as f64 / INSTANCES as f64,
                chunk_per_instance: counts.chunk_bytes as f64 / INSTANCES as f64,
            });
            drop(built);
        }
    }

    println!(
        "{INSTANCES} instances per occupancy; field-name text built outside every probe window and \
         MOVED in, so these are container allocations only."
    );
    println!(
        "entry width: String {} B + ElementEntry {} B = {} B inline",
        std::mem::size_of::<String>(),
        std::mem::size_of::<ElementEntry>(),
        std::mem::size_of::<String>() + std::mem::size_of::<ElementEntry>()
    );
    report_shape("TODAY: HashMap<String, ElementEntry> per hash key", &hashed);
    report_shape("CANDIDATE A: BTreeMap<String, ElementEntry>", &ordered);
    report_shape("CANDIDATE B: sorted Vec<(String, ElementEntry)>", &flat);

    println!("--- chunk bytes per hash INSTANCE, by occupancy, all three shapes ---");
    println!(
        "  {:>9}  {:>14}  {:>14}  {:>14}  {:>12}  {:>12}",
        "fields", "hashed", "ordered", "flat", "ord vs hash", "flat vs hash"
    );
    for i in 0..OCCUPANCIES.len() {
        println!(
            "  {:>9}  {:>14.1}  {:>14.1}  {:>14.1}  {:>+11.2}%  {:>+11.2}%",
            OCCUPANCIES[i],
            hashed[i].chunk_per_instance,
            ordered[i].chunk_per_instance,
            flat[i].chunk_per_instance,
            100.0 * (ordered[i].chunk_per_instance / hashed[i].chunk_per_instance - 1.0),
            100.0 * (flat[i].chunk_per_instance / hashed[i].chunk_per_instance - 1.0),
        );
    }

    // THE ONE-FIELD ROW IS THE PRODUCT PATH. If a table did not cost materially more there, this
    // whole module is a refutation and should say so rather than shipping a change.
    assert!(
        hashed[0].chunk_per_instance > 0.0 && flat[0].chunk_per_instance > 0.0,
        "an arm allocated nothing at occupancy 1, so the comparison is vacuous"
    );
}

/// WHAT THE FIELD NAME TEXT COSTS WHEN EVERY HASH ALLOCATES ITS OWN COPY.
///
/// A context store's inner maps all hold the same four characters, `"meta"`, and today each one
/// owns a separate `String`. This prices the second copy on its own -- separately from the
/// container -- because it is a smaller change than swapping the container and might be where the
/// bytes actually are.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_field_name_text_costs_when_every_hash_allocates_its_own() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );
    let field = crate::engine::constants::CONTEXT_NODE_FIELD;

    // THE SPINE IS ALLOCATED OUTSIDE EVERY WINDOW AND IS *NOT* THE SAME WIDTH IN THE TWO ARMS --
    // `String` is 24 bytes and `Arc<str>` is a 16-byte fat pointer -- so a first draft of this
    // test that measured both spines inside the window and said "the spine cancels" was wrong on
    // its own terms. Each spine is pre-reserved here and the probe sees only the TEXT.
    let mut owned: Vec<String> = Vec::with_capacity(LARGE_KEYS);
    let probe = Probe::start();
    for _ in 0..LARGE_KEYS {
        owned.push(field.to_string());
    }
    let own_counts = probe.stop();
    assert_eq!(LARGE_KEYS, owned.len(), "the owned arm did not build");
    assert_eq!(
        LARGE_KEYS,
        owned.capacity(),
        "the owned arm's spine grew inside the probe window, so the reading is not text-only"
    );
    assert!(
        owned.iter().all(|s| s == field),
        "the owned arm did not store the field this module prices"
    );

    // THE ALTERNATIVE: one shared Arc<str>, cloned per hash.
    let shared: Arc<str> = Arc::from(field);
    let mut clones: Vec<Arc<str>> = Vec::with_capacity(LARGE_KEYS);
    let probe = Probe::start();
    for _ in 0..LARGE_KEYS {
        clones.push(Arc::clone(&shared));
    }
    let arc_counts = probe.stop();
    assert_eq!(LARGE_KEYS, clones.len(), "the shared arm did not build");
    assert_eq!(
        LARGE_KEYS,
        clones.capacity(),
        "the shared arm's spine grew inside the probe window"
    );
    assert!(
        clones.iter().all(|c| Arc::ptr_eq(c, &shared)),
        "the shared arm allocated instead of sharing, so it is not the shape being priced"
    );

    println!("--- the field name text at {LARGE_KEYS} hashes, name {:?} ---", field);
    println!(
        "  owned String per hash : {:>10} allocs  {:>12} request B  {:>12} chunk B  ({:.1} B/hash \
         chunk)",
        own_counts.allocs,
        own_counts.alloc_bytes,
        own_counts.chunk_bytes,
        own_counts.chunk_bytes as f64 / LARGE_KEYS as f64
    );
    println!(
        "  shared Arc<str> clone : {:>10} allocs  {:>12} request B  {:>12} chunk B  ({:.1} B/hash \
         chunk)",
        arc_counts.allocs,
        arc_counts.alloc_bytes,
        arc_counts.chunk_bytes,
        arc_counts.chunk_bytes as f64 / LARGE_KEYS as f64
    );
    println!(
        "  spines pre-reserved and excluded; the INLINE width differs too and is not an allocation: \
         String {} B against Arc<str> {} B, a further {} B a field held in whatever container \
         stores it.",
        std::mem::size_of::<String>(),
        std::mem::size_of::<Arc<str>>(),
        std::mem::size_of::<String>() - std::mem::size_of::<Arc<str>>()
    );
    assert!(
        own_counts.allocs > arc_counts.allocs,
        "the owned arm did not allocate more than the shared one, so there is no second copy to \
         price and this test is measuring nothing: {} vs {}",
        own_counts.allocs,
        arc_counts.allocs
    );
}

// =================================================================================================
// 4. THE OUTER MAPS. MEASURED, NOT CHANGED.
// =================================================================================================

/// WHAT THE OUTER MODEL MAPS COST PER MAP AND PER KEY, AND HOW MUCH OF IT IS LOAD-FACTOR SLACK.
///
/// MEASUREMENT ONLY. Nothing in this module changes an outer map: `Command::StringGet` resolves
/// straight through `shard.strings`, so the outer map is on a read path, and the flat-vector shape
/// is only viable at BUCKET granularity -- which is a move of the model maps into the bucket node,
/// not a container swap, and far too large to ride along with an inner-container change.
///
/// SLACK is read from `capacity()` against `len()` rather than inferred from the byte columns, so
/// the rounding claim is a property of the container and not of an allocator coincidence.
///
/// rust-internal: reads this crate's own counting allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture --test-threads=1"]
fn what_the_outer_model_maps_cost_per_map_and_per_key() {
    assert!(
        crate::alloc_probe::counted_now().is_some(),
        "the counting allocator is not installed, so every figure below would be a zero presented \
         as a measurement"
    );

    println!("--- outer map, HashMap<K, V> against BTreeMap<K, V>, keys MOVED in ---");
    println!(
        "  {:>8}  {:>9}  {:>12}  {:>12}  {:>12}  {:>12}  {:>9}",
        "keys", "shape", "allocs", "request B", "chunk B", "B per key", "slack"
    );

