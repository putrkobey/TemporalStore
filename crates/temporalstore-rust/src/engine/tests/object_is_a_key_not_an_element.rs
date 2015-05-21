// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OBJECT IS A KEY, NOT ONE OF ITS ELEMENTS.
//!
//! `stable_block_object_id` folded the component into the object's identity, so an object held
//! exactly one page: a hash of twenty-five fields was twenty-five objects that happened to share a
//! key. #1986 recorded the result as "the object id names a page's triple, not an object" -- a
//! DESCRIPTION OF A DEFECT rather than a property to preserve. The parameter is REMOVED here, not
//! merely ignored, because keeping it and discarding its value would leave every call site
//! compiling while the meaning changed underneath it.
//!
//! # WHY THIS IS AVAILABLE ONLY NOW
//!
//! #2011 refuted it, and the mechanism was precise: the object id was the only discriminator
//! between pages INSIDE ONE WAL RECORD, because `StagedBlock` was `{ object_id, bytes }` and
//! `block_in_wal::read_block` picked with `find(|page| page.object_id == object_id)`. One record
//! routinely carries many pages -- `append_batch_as_one_record` writes a whole batch as one -- so a
//! component-free id served the FIRST page's bytes for the second. Measured there: 2/2 today,
//! 1/2 with WRONG BYTES component-free, 96.00% of hash pages unreachable at batch scale.
//!
//! #2013 closed exactly that. `stage()` takes a component, `StagedBlock` carries it, and both
//! in-record `find` sites share one predicate: `page.object_id == object_id &&
//! page.component.as_deref() == component`. The registry key became `(u64, Option<Arc<str>>)`.
//! Its measured result was 96.00% -> 0.00% unreachable with a negative control still at 1/2.
//!
//! So the acceptance test for THIS change is #2011's failing case passing on the product's own
//! read path, and it lives in `carried_block_identity` where the BEFORE numbers were taken. That
//! module's `free` id is no longer a simulation -- it is simply what `stable_block_object_id`
//! returns.
//!
//! # WHAT THIS MODULE HOLDS
//!
//! The consequences that are NOT the in-record read:
//!
//!   1. THE PRIZE, read off `bucket.object_index` itself rather than off a grouping of page
//!      entries, with the distribution that says where the effect exists.
//!   2. THE TOMBSTONE, which is where this most easily becomes data loss. `deleted_object_index`
//!      is filed per removed page's id, and one id now covers every element of a key.
//!   3. HLEN, asserted unmoved beside the listing it must agree with.
//!   4. THE READ PATH, asserted from outside to select on the component at every arm, including
//!      the first-match fallback #1964 enumerates.
//!
//! # THE HANDLE CONSEQUENCE, AND THE STAMP
//!
//! `state::block_index_handle` hashes `address.generation()`, `generation` is
//! `block_id.or(object_id)`, and for a WAL-resident page the block id is absent -- so the
//! generation IS the object id and every such handle moves with it. Handles are written to disk
//! inside the lookup's refs. `engine::SHARD_INDEX_FORMAT_VERSION` is bumped 2 -> 3 for that
//! reason, and the reason is spelled out where the constant is declared: an old index decodes
//! CLEANLY, because `BlockAddress::try_from` compares the stored `g` against the stored
//! `block_id.or(object_id)` and an old row agrees with ITSELF. The disagreement appears later,
//! between a stored `object_index` and a recomputed `stable_block_object_id`, on a recovery path.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

use crate::engine::hashing::stable_block_object_id;

const OPERATOR_END: u32 = 1023;
/// Eight keys and twenty-five fields: the population `carried_block_identity` and
/// `entry_object_identity` both use, so the rows here are comparable to theirs line for line.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population. A string page's component is already `None`, so this change is the
/// IDENTITY on it and the mechanism predicts no movement whatever.
const ROUTED_KEYS: usize = 200;

// =================================================================================================
// HARNESS
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
        table_name: "object-is-a-key".to_string(),
        shard_uri: "local://object-is-a-key/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
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

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture read failed: {response:?}");
    response.response
}

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: format!("h{k}-f{f}-value").into_bytes(),
            });
        }
    }
    ack_batch(engine, commands);
}

fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: format!("s{i}-value").into_bytes(),
            })
            .collect(),
    );
}

/// `(live pages, distinct object-index rows)` for one kind, read off the buckets themselves.
///
/// `object_index` is the thing this change shrinks, so it is read DIRECTLY rather than inferred
/// from a grouping of page entries. Only rows belonging to the kind's own keys are counted, so one
/// arm's rows cannot leak into the other's.
fn blocks_and_index_rows(
    engine: &TemporalEngine,
    kind: &str,
    keys: &BTreeSet<String>,
) -> (usize, usize) {
    let wanted: BTreeSet<u64> = keys
        .iter()
        .map(|key| stable_block_object_id(1, kind, key))
        .collect();
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut blocks = 0usize;
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut rows: BTreeSet<u64> = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted || block.model_id.as_str() != kind {
                continue;
            }
            if !keys.contains(&**named.at(bucket.routing_bucket, *handle)) {
                continue;
            }
            blocks += 1;
        }
        for object_id in &bucket.object_index {
            if wanted.contains(object_id) {
                rows.insert(*object_id);
            }
        }
    }
    (blocks, rows.len())
}

fn tombstone_holds(engine: &TemporalEngine, object_id: u64) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .any(|bucket| bucket.deleted_object_index.contains(&object_id))
}

/// The components of one key's live pages, sorted. An absent component reads as the empty string,
/// which is distinguishable here because no fixture in this module writes one.
/// LIVE PAGE ENTRIES THIS OBJECT HOLDS. A COUNT, AND IT SAYS SO IN ITS NAME.
///
/// THIS WAS `live_components` RETURNING A `Vec<String>` AND IT HAD STOPPED RETURNING COMPONENTS.
/// When the element-name field came off `BlockIndex` the body was left pushing `String::new()` per
/// live entry, so every caller that only took `.len()` still measured the right thing and the two
/// callers that looked at the CONTENTS were reading a run of blanks -- one of them asking whether
/// any element was named "f0", which could only ever answer no. A helper whose name promises
/// identity and whose body yields blanks is worse than a missing helper, so it returns the count it
/// actually computes. Element identity is `resident_fields` below.
fn live_block_count(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut held = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if !block.deleted
                && block.model_id.as_str() == kind
                && &**named.at(bucket.routing_bucket, *handle) == key
            {
                held += 1;
            }
        }
    }
    held
}

/// THE FIELD NAMES THIS OBJECT HOLDS, from the authority for them.
///
/// `shard.hashes` is durable (`#[serde(default)]`) and is what `HashLen` and `HashGetAll` answer
/// from. It is where element identity went when it came off the entry, so it is where the questions
/// about element identity in this module are now asked.
fn resident_fields(engine: &TemporalEngine, key: &str) -> std::collections::BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .hashes
        .get(key)
        .map(|fields| fields.keys().map(|name| name.to_string()).collect())
        .unwrap_or_default()
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

// =================================================================================================
// 1. THE PRIZE: ONE OBJECT-INDEX ROW PER KEY, READ OFF THE INDEX ITSELF.
// =================================================================================================

/// `bucket.object_index` HOLDS ONE ROW PER KEY INSTEAD OF ONE PER PAGE.
///
/// #2007 measured the row count at 0.9983 per page, and grouping the same pages on `(kind, key)`
/// at MAX 100 per object, saving 6.409-6.496 B/page. THE HONEST CAVEAT FROM THAT WORK IS REPEATED
/// HERE RATHER THAN BURIED: across a real corpus, pages per `(kind, key)` is p50 1, p90 1, p99 1.
/// Ninety-nine percent of keys hold ONE page, so on a general population this saves almost
/// nothing -- it is NOT 30 bytes an object. It is the CONTAINER population -- hashes, sets, zsets,
/// lists -- where a key holds many elements and the row count collapses, and that is the
/// population seeded here.
///
/// So the number below is not a claim about a whole store. It is the size of the effect where the
/// effect exists, printed beside the distribution that says where that is.
///
/// THE CONTROL is the routed string arm, whose component was already `None`: one page per key
/// before and after, so its rows-per-page must be 1.0000. Its page count is asserted FIRST -- a
/// control that read nothing reports no movement for the wrong reason.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_object_index_holds_one_row_per_key_rather_than_one_per_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    let hash_keys: BTreeSet<String> = (0..CONTAINER_KEYS).map(|k| format!("h{k}")).collect();
    let string_keys: BTreeSet<String> = (0..ROUTED_KEYS).map(|i| format!("s{i}")).collect();

    let (hash_blocks, hash_rows) = blocks_and_index_rows(&engine, "hash", &hash_keys);
    let (string_blocks, string_rows) = blocks_and_index_rows(&engine, "string", &string_keys);

    println!("\n=== bucket.object_index rows against live pages ===");
    println!("  kind     keys  pages  rows  rows/page");
    for (kind, keys, blocks, rows) in [
        ("hash", hash_keys.len(), hash_blocks, hash_rows),
        ("string", string_keys.len(), string_blocks, string_rows),
    ] {
        println!(
            "  {kind:<7} {keys:>5} {blocks:>6} {rows:>5}   {:>8.4}",
            rows as f64 / blocks as f64
        );
    }

    // DENOMINATORS FIRST.
    assert_eq!(
        hash_blocks,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the container arm exercised {hash_blocks} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        string_blocks, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_blocks} string pages, not {ROUTED_KEYS}"
    );

    // THE DELIVERABLE.
    assert_eq!(
        hash_rows, CONTAINER_KEYS,
        "the container arm holds {hash_rows} object-index rows for {CONTAINER_KEYS} keys and \
         {hash_blocks} pages. One key is one object now, so the row count is the KEY count -- if it \
         is the PAGE count, the component is still reaching the id"
    );
    assert!(
        hash_blocks > hash_rows,
        "DENOMINATOR: {hash_blocks} pages and {hash_rows} rows. A fixture whose keys held one page \
         each would satisfy the assertion above while measuring nothing"
    );

    // THE CONTROL, flat by construction.
    assert_eq!(
        string_rows, ROUTED_KEYS,
        "the control holds {string_rows} rows for {ROUTED_KEYS} one-page keys. Its component was \
         already `None`, so this change is the identity on it and the count cannot move"
    );
    println!(
        "  VERDICT: container rows/page {:.4} (was 1.0000, one row per page); CONTROL string \
         rows/page {:.4}, unmoved, {string_blocks} pages exercised.",
        hash_rows as f64 / hash_blocks as f64,
        string_rows as f64 / string_blocks as f64
    );

    // AND THE DISTRIBUTION THAT SAYS WHERE THIS APPLIES, as #2007 measured it. A histogram rather
    // than a mean, because the whole question is the tail.
    let mut per_key: Vec<usize> = Vec::new();
    for key in &hash_keys {
        per_key.push(live_block_count(&engine, "hash", key));
    }
    for key in &string_keys {
        per_key.push(live_block_count(&engine, "string", key));
    }
    per_key.sort();
    println!(
        "  pages per (kind,key) over {} keys: p50={} p90={} p99={} MAX={}",
        per_key.len(),
        percentile(&per_key, 50.0),
        percentile(&per_key, 90.0),
        percentile(&per_key, 99.0),
        per_key.last().copied().unwrap_or(0)
    );
    assert_eq!(
        percentile(&per_key, 50.0),
