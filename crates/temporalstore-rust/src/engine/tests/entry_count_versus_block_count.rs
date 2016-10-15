// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER HOLDING MANY ELEMENTS IN ONE PAGE WOULD REDUCE THE INDEX ENTRIES THAT NAME THEM.
//!
//! A container in this engine gives every element its own page and therefore its own page-index
//! entry: `Command::HashSet`, `SetAdd`, `ZSetAdd` and `ListPush` each call `append_value` once per
//! element and `upsert_bucket_index_block` once per element, and `HashMultiSet` LOOPS, so a bulk
//! command already decomposes into per-element pages. `container_member_shadow` priced what one
//! such element costs. The question here is the one before the price: if a page held many
//! elements, would the ENTRY COUNT fall with it?
//!
//! THE ANSWER WAS NO AND IT IS YES NOW, AND THE REASON IS STILL `block_index_handle`. The handle a
//! page is filed under hashed the model spelling, the object key, the ELEMENT NAME and the address.
//! Two elements sharing one page shared the address and differed in the name, so they hashed to two
//! handles and the page index held two entries -- one page, two entries. `BlockIndex` has no
//! element-name field any more: the handle is the model spelling, the object key and the address,
//! two elements over one page hash ALIKE, and the entry count is the PAGE count.
//! `one_block_is_one_entry_because_the_handle_can_no_longer_name_an_element` drives exactly that,
//! and it is the whole of the mechanism.
//!
//! THE INVERSION IS WHY THIS HEADER IS RESTATED RATHER THAN RE-NUMBERED. A reader who took the old
//! answer from this paragraph and the new entry counts from the arms would conclude the arms had
//! been relaxed. The old claim was true of the engine it was written against, and the arm that held
//! it has been rewritten rather than loosened -- its own failure message asked for that.
//!
//! THIS ENGINE ALREADY RUNS BOTH SHAPES, WHICH IS WHY THE CONTRAST IS MEASURABLE RATHER THAN
//! PROJECTED. The timestamped kinds take a different route to the same index:
//!
//!   * `packed_blocks::append_timestamped_kv_blocks` chunks many points into ONE page against
//!     `context_block_target_bytes`, so items per page is greater than one; and
//!   * `sync_bucket_index_object_blocks_with_mode` files what comes back deduplicated BY PHYSICAL
//!     ADDRESS, with `component: None`, so the entry count is the PAGE count.
//!
//! So a feature series files one entry per page and a container files one per element, in the same
//! page index, on the same shard.
//! `how_many_index_entries_each_kind_files_for_the_same_element_count` measures both at two corpus
//! sizes ten times apart, as a histogram with per-row denominators.
//!
//! BATCHING NEEDED BOTH HALVES AND IT HAS BOTH NOW, WHICH IS WHY THIS SECTION IS HISTORY RATHER
//! THAN A PREREQUISITE. It read "Sharing pages without dropping the component moves nothing;
//! dropping the component moves the entry count only once pages are shared ... Dropping the
//! component is what costs, because for a container the component is not decoration", and then
//! listed three readers for which the element name on the entry was the only copy of an element's
//! identity. The name has been dropped and all three were moved first, so the list below is the
//! bill that was paid and not one that is outstanding:
//!
//!   * `hashes` WAS `skip_serializing` on `ShardState` and rebuilt FROM the bucket index on load,
//!     which made the element name on the entry the durable spelling of a hash field. It carries
//!     `#[serde(default)]` now, as do `sets`, `zsets` and `lists`: all four resident maps are
//!     durable and are the authority. (The comment above `hashes` in `engine::state` that still
//!     says "rebuildable ... do not duplicate in checkpoints" predates that and is corrected
//!     there.)
//!   * the `"zset"` arm of `apply_outcome_item` rebuilt the member and the score out of the
//!     element name and never read the page. The score rides the WAL outcome's `value` slot and
//!     the member comes from the resident map.
//!   * `apply_key_states` folds thirteen maps -- `features`, `expires_at_ms`, four
//!     `control_state_*` and seven `context_*` -- and `sets`, `zsets`, `lists` and `hashes` are
//!     STILL not among them, while `fold_delta_block_items` DOES restore the page items.
//!     `the_maps_the_delta_fold_restores_do_not_include_the_container_maps` holds that list, so a
//!     map added to the fold shows up here by name. What closed this door is the other side of it:
//!     `collect_upsert_index_items` reads the resident map, so a delta item exists only where the
//!     map held the element, and an entry can no longer arrive naming an element nothing else
//!     holds.
//!
//! SO THE PREREQUISITE PARAGRAPH IS DISCHARGED. It said a batched page "would have to carry element
//! identity in its PAYLOAD, and the fold's reconciliation deliberately does not read payloads", and
//! called batching a lossy migration until the fold carried element bytes. The page payload does
//! name its elements -- that is what makes several pages foldable into one -- and the recovery path
//! that needed the entry's copy now reads the durable resident maps instead. This module still
//! measures the prize and changes no production code; what it no longer does is name an outstanding
//! blocker.
//!
//! WHAT THE READ SIDE WOULD PAY, measured and not projected. Today a single-element read fetches
//! the element's own page, and the page's length is a STORED FACT read back off the index -- not a
//! counter some path could forget to increment, which is the failure mode that reported a restore
//! as flat while the kernel saw 6.70x. A batched page is built with the engine's OWN encoder
//! (`packed_blocks::encode_feature_block`) at several batch widths and its length read off the
//! bytes, so the amplification below is two measurements and no arithmetic.
//! `what_one_element_read_fetches_today_and_what_a_batched_block_would_fetch` reports it.
//!
//! THE CONTROL IS THE STRING KIND, at 0.00%. A string key owns exactly one page and one entry
//! already, so batching predicts no change for it and a measurement that moved it would be
//! measuring the fixture. `the_string_control_has_nothing_for_batching_to_win` asserts the zero.
//!
//! THE STORE PATH LENGTH is held constant across arms and asserted equal: it moves allocation
//! bytes at about six a character, and an arm on a longer temporary directory reads as a heavier
//! representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::{
    block_index_handle, BlockIndex, BlockIndexMap, BlockSlabLiveIndex,
};
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default since
/// #1973. THE OPERATOR'S RANGE.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses when nothing says otherwise. Every key lands in
/// a bucket of its own BY CONSTRUCTION at this width, so it is an artefact of the default and not a
/// workload. Entries per OBJECT cannot move with it -- routing takes the object key and never sees
/// the component, so all of an object's pages land in one bucket at either range -- and
/// `how_many_index_entries_each_kind_files_for_the_same_element_count` asserts that equality rather
/// than assuming it.
const WIDE_END: u32 = u32::MAX;

/// The two corpus sizes, ten times apart. Elements under one object key at each.
const SMALL_ELEMENTS: usize = 10;
const LARGE_ELEMENTS: usize = 100;

/// Object keys per kind. Small, because the number this module reports is per OBJECT and more keys
/// only repeat the row.
const KEYS_PER_KIND: usize = 4;

/// The element payload width, held equal across kinds so a byte figure is comparable between them.
const VALUE_WIDTH: usize = 32;

/// The batch width the control projects at: the widest the read-side row prices, so the control is
/// checked against the most favourable projection this module reports rather than a gentle one.
const CONTROL_BATCH_WIDTH: usize = 128;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "entry-count".to_string(),
        shard_uri: "local://entry-count/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard must load for any figure below to mean anything: {:?}",
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

/// The four container kinds plus the two shapes that are NOT element-scaled, each as
/// (kind spelling, object keys, the commands that fill them).
///
/// One helper rather than four, so a kind cannot be seeded at a different element count than the
/// row it is reported on -- which is the shape of fixture error that makes a ratio a fixture
/// property. Every arm writes exactly `elements` elements under every key it returns.
fn seed_all_kinds(engine: &TemporalEngine, elements: usize) -> Vec<(&'static str, Vec<String>)> {
    let mut seeded: Vec<(&'static str, Vec<String>)> = Vec::new();
    let mut commands: Vec<Command> = Vec::new();

    let hash_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("h{k}")).collect();
    for key in &hash_keys {
        for f in 0..elements {
            commands.push(Command::HashSet {
                key: key.clone(),
                field: format!("f{f}"),
                value: vec![b'v'; VALUE_WIDTH],
            });
        }
    }
    seeded.push(("hash", hash_keys));

    let set_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("t{k}")).collect();
    for key in &set_keys {
        for m in 0..elements {
            commands.push(Command::SetAdd {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
            });
        }
    }
    seeded.push(("set", set_keys));

    let zset_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("z{k}")).collect();
    for key in &zset_keys {
        for m in 0..elements {
            commands.push(Command::ZSetAdd {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
                score: m as f64,
            });
        }
    }
    seeded.push(("zset", zset_keys));

    let list_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("l{k}")).collect();
    for key in &list_keys {
        for m in 0..elements {
            commands.push(Command::ListPush {
                key: key.clone(),
                member: format!("m{m}").into_bytes(),
                left: false,
            });
        }
    }
    seeded.push(("list", list_keys));

    // THE CONTROL. One page, one entry, already -- nothing for batching to win.
    let string_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("s{k}")).collect();
    for key in &string_keys {
        commands.push(Command::StringSet {
            key: key.clone(),
            value: vec![b'v'; VALUE_WIDTH],
        });
    }
    seeded.push(("string", string_keys));

    run_batch(engine, commands);

    // THE PAGE-SCALED SHAPE, driven separately because it is one command per key and not a batch of
    // per-element ones. That asymmetry IS the finding: the whole series goes in as one command and
    // comes back chunked into pages.
    let feature_keys: Vec<String> = (0..KEYS_PER_KIND).map(|k| format!("f{k}")).collect();
    for key in &feature_keys {
        let points = (0..elements)
            .map(|t| crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000 + t as u64,
                value: vec![b'f'; VALUE_WIDTH],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: key.clone(),
                points,
            },
        });
        assert!(
            response.status.ok,
            "series seed must ack: {:?}",
            response.status
        );
    }
    seeded.push(("feature", feature_keys));

    seeded
}

/// Live page-index entries and DISTINCT pages, for one (kind, object key).
