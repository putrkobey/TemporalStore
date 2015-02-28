// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CARRIED PAGE NOW STATES WHICH ELEMENT IT IS, AND THAT IS WHAT FREES THE OBJECT ID.
//!
//! # THE HOLE THIS CLOSES, MEASURED BEFORE IT WAS CLOSED
//!
//! `block_in_wal::read_block` serves a page whose only durable copy is its WAL record. It resolves
//! a record and then picks one page out of it. Until this change the only thing it could pick by
//! was the object id:
//!
//! ```text
//!     pages.iter().find(|page| page.object_id == object_id)
//! ```
//!
//! `StagedBlock` was `{ object_id, bytes }` and nothing else, so the object id was not merely the
//! registry's KEY -- it was the record's only discriminator BETWEEN ITS OWN PAGES. And one record
//! routinely carries many: `append_batch_as_one_record` puts a whole batch in one record, so a
//! batch writing twenty-five fields of one hash writes one record holding twenty-five pages.
//!
//! Measured on `ddecb8e10`, driven through the product's own read, before any line of this change:
//!
//! ```text
//!     today (kind:key:component)      2 / 2 correct, 0 wrong bytes, 0 missing
//!     component-free id               1 / 2 correct, 1 WRONG BYTES, 0 missing
//!     at batch scale                  96.00% of hash pages unreachable (200 pages, 8 groups)
//!     string control                  0.00%, 200 pages exercised
//! ```
//!
//! The losing page was served the FIRST page's BYTES rather than answering missing. `find` returns
//! the first match and cannot see an ambiguity, so a caller asking for one field of a hash was
//! handed a plausible page of a different field, on the one path that exists to stop a durably
//! acknowledged write reading as missing.
//!
//! # WHAT CHANGED
//!
//! `StagedBlock` gained `component: Option<Arc<str>>` -- which element of the object this page
//! holds -- and both in-record `find` sites now test it. The registry key gained it too. The
//! numbers above are the BEFORE; the tests here are the AFTER, on the same read path.
//!
//! # WHY THE COMPONENT AND NOT `BlockAddress::block_id`
//!
//! The ordinal was the other candidate, and #2008 threaded a real one onto container pages. FOUR
//! of the five reasons it loses are already measured elsewhere in this tree and are CITED rather
//! than re-derived here:
//!
//!   1. `container_block_ordinal` takes `component: &str` and resolves the ordinal BY looking the
//!      component up. The ordinal is a projection of the component, so it cannot carry more.
//!   2. #2008: "`max` FALLS after a delete and the next insert is handed the ordinal just freed...
//!      a reader that treated the ordinal as naming a particular element would be silently corrupt
//!      the moment it happened." Driven there by `the_ordinal_names_a_position_and_a_delete_frees_it`.
//!   3. #2008 again: past an object's 65,535th element the ordinal is left at 0, so those pages
//!      share it. Driven by `past_the_ceiling_the_ordinal_is_left_unassigned_rather_than_panicking`.
//!   4. #1996: the delta fold delivers elements whose durable-map entry was never written, and for
//!      those the component is the ONLY copy of the member.
//!
//! THE FIFTH IS NEW AND IS DRIVEN BELOW, because it is the one that decides the question for THIS
//! path rather than in general. The mandate asked whether an ordinal is available at STAGE time.
//! It is -- `block_ordinal` is an argument to `append_value_of_object`, computed by
//! `container_block_ordinal` immediately before the call. The stage is not where it fails.
//! **It fails at READ time**: the asynchronous arm of `append_value_inner` builds its address with
//! `BlockAddress::try_from_parts(HOT_BLOCK_SLAB_ID, ticket, len, None, object_id)` -- `block_id`
//! is `None` -- and that synthetic address is the one `is_wal_resident` sends down this path. So
//! on the very path a carried page exists to serve, there is no ordinal on the address to read
//! back. `a_wal_resident_address_carries_no_ordinal_to_read_back` measures it.
//!
//! # WHAT THIS DOES NOT BUY
//!
//! On its own, NOTHING. It makes records LARGER. It is worth landing only because it removes the
//! single obstacle to making `stable_block_object_id` component-free, and that change's own worth
//! is measured elsewhere. The growth is measured here so the trade is stated in both directions.

#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::engine::hashing::stable_block_object_id;
use crate::wal::StagedBlock;

const OPERATOR_END: u32 = 1023;
/// Eight keys and twenty-five fields: the same fixture the BEFORE numbers were taken on, so the
/// rows below are comparable to them line for line rather than merely similar.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population. A string page's component is already `None`, so every transformation
/// this module measures is the IDENTITY on it and the mechanism predicts no effect whatever.
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
        table_name: "carried-page-identity".to_string(),
        shard_uri: "local://carried-page-identity/1".to_string(),
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

/// The value a field is written with, distinct per (key, field) so that serving one field's page
/// for another is DETECTABLE rather than a coincidence of equal bytes.
fn hash_value(k: usize, f: usize) -> Vec<u8> {
    format!("h{k}-f{f}-value-{}", "x".repeat(16)).into_bytes()
}

fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: hash_value(k, f),
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

/// Every live page of one kind, as `(object_key, component)`, read off the page entries themselves.
/// EVERY LIVE PAGE OF A KIND, WITH THE ELEMENT IT HOLDS -- READ FROM THE AUTHORITY FOR THAT.
///
/// RE-ATTRIBUTED, AND THE OLD BODY IS WHY THIS COMMENT IS LONG. It returned the entry's own
/// element name until the width step removed that field, and was then left handing back a
/// hardcoded `None::<String>` under a note saying "an entry names a page, not an element -- this
/// module's subject". That made the two groupings in
/// `the_element_beside_the_id_makes_every_block_of_a_batch_reachable` the SAME computation: "id
/// alone" and "id + element" both grouped on `(id, None)`, so the arm's deliverable compared a
/// value with itself and reported the hazard row twice.
///
/// THE ELEMENT NAME IS STILL CARRIED -- JUST NOT THERE. `wal::StagedBlock` keeps a per-element
/// discriminator and says in its own doc why it has to ("the record's only discriminator BETWEEN
/// ITS OWN PAGES"), which is this module's whole subject. What moved is where a READER recovers it
/// from: `shard.hashes` is durable and is the authority for which field a hash page holds.
///
/// JOINED BY ADDRESS, WHICH IS THE JOIN THE ENGINE ITSELF MAKES. The resident map holds an
/// `ElementEntry` per field and the index entry holds the same address, so the address is what
/// identifies a page in both. `RecordedMap::block_an_element_vacates` asks this exact question on
/// the write path, so this is not a fixture-only correspondence.
fn live_blocks(engine: &TemporalEngine, kind: &str) -> Vec<(String, Option<String>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    // Address -> element name, for the kinds that have elements. `string` has none, which is what
    // makes it this module's control.
    let mut element_at: BTreeMap<(u64, u64, u64), String> = BTreeMap::new();
    if kind == "hash" {
        for (object_key, fields) in shard.hashes.iter() {
            let _ = object_key;
            for (field, address) in fields.iter() {
                element_at.insert(
                    (
                        address.block_slab_id(),
                        address.offset(),
                        address.length(),
                    ),
                    field.to_string(),
                );
            }
        }
    }
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted || block.model_id.as_str() != kind {
                continue;
            }
            let at = (
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            );
            held.push((
                named.at(bucket.routing_bucket, *handle).to_string(),
                element_at.get(&at).cloned(),
            ));
        }
    }
    held.sort();
    held
}

/// Percentiles of a sorted sample, by nearest-rank. A mean over this distribution says nothing --
/// the whole question is the tail.
fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

fn carrier(label: &str) -> Command {
    Command::StringSet {
        key: format!("carrier-{label}"),
        value: b"carrier".to_vec(),
    }
}

// =================================================================================================
// 1. THE DELIVERABLE: THE SAME READ PATH, A COMPONENT-FREE ID, AND BOTH PAGES BACK.
// =================================================================================================

/// TWO CARRIED PAGES OF ONE KEY, SHARING AN OBJECT ID, EACH SERVE THEIR OWN BYTES.
///
/// This is the test the change exists to pass. It does not simulate `read_block`; it calls it.
///
/// A record is appended carrying two pages with different bytes -- the shape a batch writing two
/// fields of one hash produces -- and registered exactly as `register_record` registers a real
/// append. **Both pages carry the SAME object id**, the one a component-free
/// `stable_block_object_id` would hand them. Before this change that arm read 1 of 2 correctly and
/// served the first page's bytes for the second. It now reads 2 of 2, because the record says
/// which element each page is and `read_block` asks for one.
///
/// THREE ARMS, and the third is what makes the first mean anything:
///
///   * TODAY'S IDENTITY, the control. Two distinct ids, both pages back. If this arm ever fails,
///     nothing below is attributable to anything.
///   * THE COMPONENT-FREE IDENTITY, the deliverable. One id, two components, both pages back.
///   * ONE ID AND ONE COMPONENT, the NEGATIVE control. Two pages that really are indistinguishable
///     still collapse to 1 of 2. Without it, a `read_block` that had simply started returning
///     both pages for any question would pass the second arm exactly as a correct one does.
///
/// THREE OUTCOMES, NOT TWO, and the split is the whole severity of the finding this closes. A page
/// that reads MISSING is a read fault: the caller falls through and the WAL replay still holds the
/// value. A page that reads ANOTHER PAGE'S BYTES is silent corruption. Folding them into one
/// "wrong" counter would let this test pass at 1/2 whichever of them the collapse produced.
///
/// rust-internal: drives the WAL append and the block-in-WAL read directly, no external surface
#[test]
fn two_carried_blocks_of_one_key_each_serve_their_own_bytes_under_a_component_free_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let first_bytes = b"field-f0-bytes".to_vec();
    let second_bytes = b"field-f1-bytes-which-differ".to_vec();
    assert_ne!(
        first_bytes, second_bytes,
        "the two pages must differ or serving one for the other is undetectable"
    );

    println!("\n=== read_block over one record carrying two pages of one key ===");
    println!("  identity                          pages  correct  wrong bytes  missing");

    // THE PRODUCT'S OWN IDENTITY, since the component left `stable_block_object_id`. It was
    // reachable only as a simulation when this module was written; it is now simply the id.
    let free = stable_block_object_id(1, "hash", "hk");
    // THE CONTROL'S TWO IDS. The folded identity used to produce a distinct id per element, and
    // there is no longer a call that does. What the control is FOR is unchanged -- two pages
    // bearing DIFFERENT ids must both come back, or nothing below attributes anything to the
    // change -- so the two distinct numbers come from two distinct keys instead.
    let distinct_a = stable_block_object_id(1, "hash", "hk:f0");
    let distinct_b = stable_block_object_id(1, "hash", "hk:f1");
    assert_ne!(
        distinct_a, distinct_b,
        "the control needs two DIFFERENT ids and got one number twice"
    );
    assert!(
        free != distinct_a && free != distinct_b,
        "the control's ids collide with the object's own id, so its arm would not be a control"
    );
    let mut results: Vec<(&str, usize, usize, usize)> = Vec::new();

    for (label, first, second) in [
        (
            "CONTROL: two distinct ids",
            (distinct_a, Some("f0")),
            (distinct_b, Some("f1")),
        ),
        (
            "component-free id + element",
            (free, Some("f0")),
            (free, Some("f1")),
        ),
        (
            "NEGATIVE: one id, one element",
            (free, Some("f0")),
            (free, Some("f0")),
        ),
    ] {
        let staged = vec![
            StagedBlock {
                object_id: first.0,
                component: first.1.map(std::sync::Arc::from),
                bytes: first_bytes.clone(),
            },
            StagedBlock {
                object_id: second.0,
                component: second.1.map(std::sync::Arc::from),
                bytes: second_bytes.clone(),
            },
        ];
        let (record, log_id) = engine
            .wal_store
            .append_with_sync_staged(1, carrier(label), true, staged.clone())
            .expect("the carrier record must append");
        crate::engine::block_in_wal::register_record(
            &engine.block_store,
            1,
            &staged,
            log_id,
            record.sequence,
            &engine.wal_store,
        );

        let mut correct = 0usize;
        let mut other_blocks_bytes = 0usize;
        let mut missing = 0usize;
        for ((object_id, component), want) in
            [(first, &first_bytes), (second, &second_bytes)]
        {
            match crate::engine::block_in_wal::read_block(
                &engine.block_store,
                1,
                object_id,
                component,
            ) {
                Some(got) if &got == want => correct += 1,
                Some(_) => other_blocks_bytes += 1,
                None => missing += 1,
            }
        }
        println!(
            "  {label:<32} {:>5} {:>8} {other_blocks_bytes:>12} {missing:>8}",
            staged.len(),
            correct
        );
        results.push((label, correct, other_blocks_bytes, missing));
    }

    assert_eq!(
        (results[0].1, results[0].2, results[0].3),
        (2, 0, 0),
        "CONTROL FAILED: two distinct ids served {} of 2 pages correctly ({} wrong bytes, {} \
         missing). Until this arm is 2/0/0 the arms below attribute nothing to the change",
        results[0].1,
        results[0].2,
        results[0].3
    );
    assert_eq!(
        (results[1].1, results[1].2, results[1].3),
        (2, 0, 0),
        "THE DELIVERABLE FAILED: a component-free id served {} of 2 pages correctly ({} wrong \
         bytes, {} missing). The record carries `component` on each page and `read_block` takes \
         one, so both pages must be reachable",
        results[1].1,
        results[1].2,
        results[1].3
    );
    assert_eq!(
        (results[2].1, results[2].2, results[2].3),
        (1, 1, 0),
        "NEGATIVE CONTROL FAILED: two pages sharing BOTH terms served {}/{}/{} rather than \
         1/1/0. They are genuinely indistinguishable, so the discriminator must not separate \
         them -- if it does, the second arm above is passing for some other reason",
        results[2].1,
        results[2].2,
        results[2].3
    );
    println!(
        "  VERDICT: 2/2 under a component-free id (was 1/2 with the first page's bytes served for \
         the second), while two genuinely identical pages still collapse to 1/2."
    );
}

// =================================================================================================
// 2. THE SAME QUESTION AT BATCH SCALE, WITH ITS DENOMINATOR AND ITS CONTROL.
// =================================================================================================

/// AT BATCH SCALE: 96.00% UNREACHABLE ON THE ID ALONE, 0.00% ONCE THE ELEMENT IS BESIDE IT.
///
/// Every live page is grouped twice: by the identity a component-free `stable_block_object_id`
/// would give it, and by the PAGE KEY this change made the record and the registry use, which is
/// that identity plus the element. A group of size N > 1 is N pages the lookup cannot tell apart,
/// of which at most one is reachable.
///
/// THE FIRST GROUPING IS NOT A BUG BEING REPORTED -- it is the hazard the second grouping removes,
/// and it is here so the two are read off ONE population rather than two runs. This module's own
/// BEFORE numbers put it at 96.00%, and it reproduces here unchanged, because this change does not
/// touch the index: it changes what the RECORD can say about its own pages.
///
/// THE CONTROL IS THE STRING ARM and it is why the hash arm means anything. A string page's
/// component is already `None`, so adding the element to the key is the IDENTITY on it: the
/// mechanism predicts every group is size one under BOTH groupings and no page is ever lost. Its
/// exercised page count is asserted first, because a control that read nothing reports 0.00% lost
/// and "not exercised" wears the face of "did not move".
///
/// rust-internal: reads a seeded store's own page entries, no external surface
#[test]
fn the_element_beside_the_id_makes_every_block_of_a_batch_reachable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    println!("\n=== pages per identity, grouped two ways ===");
    println!("  kind    grouped by        pages  groups  p50  p90  p99  MAX  unreachable   share");
    let mut rows: Vec<(&str, &str, usize, usize)> = Vec::new();
    for kind in ["hash", "string"] {
        let blocks = live_blocks(&engine, kind);
        assert!(
            !blocks.is_empty(),
            "DENOMINATOR: no {kind} pages were exercised, so every share below is 0.00% for the \
             wrong reason"
        );
        for grouping in ["id alone", "id + element"] {
            let mut groups: BTreeMap<(u64, Option<String>), usize> = BTreeMap::new();
            for (object_key, component) in &blocks {
                let id = stable_block_object_id(1, kind, object_key);
                let term = if grouping == "id alone" {
                    None
                } else {
                    component.clone()
                };
                *groups.entry((id, term)).or_default() += 1;
            }
            let mut sizes: Vec<usize> = groups.values().copied().collect();
            sizes.sort();
            let unreachable: usize = sizes.iter().map(|size| size.saturating_sub(1)).sum();
            let share = 100.0 * unreachable as f64 / blocks.len() as f64;
            println!(
                "  {kind:<6}  {grouping:<16} {:>5} {:>7} {:>4} {:>4} {:>4} {:>4} {:>12} {share:>6.2}%",
                blocks.len(),
                sizes.len(),
                percentile(&sizes, 50.0),
                percentile(&sizes, 90.0),
                percentile(&sizes, 99.0),
                sizes.last().copied().unwrap_or(0),
                unreachable
            );
            rows.push((kind, grouping, blocks.len(), unreachable));
        }
    }

    let find = |kind: &str, grouping: &str| -> (usize, usize) {
        let row = rows
            .iter()
            .find(|(k, g, _, _)| *k == kind && *g == grouping)
            .expect("both groupings ran for both kinds");
        (row.2, row.3)
    };

    // The hazard, unchanged by this PR and reproduced from the BEFORE run.
    let (hash_blocks, hash_id_only) = find("hash", "id alone");
    assert_eq!(
        hash_blocks,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the hash arm exercised {hash_blocks} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        hash_id_only,
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1),
        "grouped on the id alone the hash arm loses {hash_id_only} pages, not {} -- one survivor \
         per key",
        CONTAINER_KEYS * (MEMBERS_PER_KEY - 1)
    );

    // The deliverable.
    let (_, hash_with_element) = find("hash", "id + element");
    assert_eq!(
        hash_with_element, 0,
        "with the element beside the id, {hash_with_element} of {hash_blocks} hash pages are still \
         unreachable. Every page of this fixture is a distinct field of a hash, so every group \
         must hold exactly one page"
    );

    // THE CONTROL, exercised first and then asserted flat under both groupings.
    let (string_blocks, string_id_only) = find("string", "id alone");
    let (_, string_with_element) = find("string", "id + element");
    assert_eq!(
        string_blocks, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_blocks} string pages, not {ROUTED_KEYS}"
    );
    assert_eq!(
        (string_id_only, string_with_element),
        (0, 0),
        "the control lost ({string_id_only}, {string_with_element}) of {string_blocks} pages under \
         the two groupings. A kind whose component is already `None` cannot be moved by putting \
         the component into the key, so both must be zero"
    );
    println!(
        "  CONTROL: string, {string_blocks} pages exercised, 0 unreachable under BOTH groupings, \
         0.00%.\n  VERDICT: 96.00% -> 0.00% unreachable for hash; the control does not move."
    );
}

// =================================================================================================
// 3. THE REASON THE ORDINAL LOSES THAT IS NOT ALREADY IN THE TREE.
// =================================================================================================

/// A WAL-RESIDENT ADDRESS CARRIES NO ORDINAL, SO THE READ COULD NOT HAVE USED ONE.
///
/// The mandate asked whether a page ordinal is available at STAGE time. IT IS, and saying
/// otherwise would have been wrong: `append_value_of_object` takes `block_ordinal` as an argument
/// and `container_block_ordinal` computes it immediately before the call. The stage is not where an
/// ordinal fails.
///
/// IT FAILS AT READ TIME. `read_block` is reached from `read_block_bytes`, whose handle is a
/// `BlockAddress`. For a page served out of its record that address is the SYNTHETIC one the
/// asynchronous arm minted, and that arm passes `None` for `block_id`. So the reader has no
/// ordinal to pass even when the writer had one.
///
/// Driven on real writes through the product, not on a hand-built address: the store is opened
/// with asynchronous storage so the pages stay log-resident, the addresses are read off the served
/// index, and every one is asserted to be WAL-resident first -- an arm that measured only durable
/// addresses would report "no ordinals" for the wrong reason.
///
/// rust-internal: reads the engine's own index entries, no external surface
#[test]
fn a_wal_resident_address_carries_no_ordinal_to_read_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    // Asynchronous storage is a per-REQUEST replication mode here, and it is what makes a
    // page stay in its record: the write never hands the page to the block store, so its
    // address is the synthetic one this test is about.
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            let response = engine.execute_replicated(
                ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: hash_value(k, f),
                    },
                }
                .with_async_storage(),
            );
            assert!(response.status.ok, "seed must ack: {:?}", response.status);
        }
    }

    let (resident, with_ordinal, durable) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let mut resident = 0usize;
        let mut with_ordinal = 0usize;
        let mut durable = 0usize;
