// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A REWRITE COSTS AND WHAT IT FOLDS, WHEN A CONTAINER'S PAGES ARE BATCHED.
//!
//! # THE CLAIM
//!
//! A container gives every element its own page. `container_blocks` made each of those pages name the
//! element it holds, which is what lets several of them be folded into one: a page holding many
//! elements is only readable if it says which is which.
//!
//! This module measures the fold. It is done at COMPACTION and nowhere else, so nothing about the
//! hot write path changes and no round does work it was not already going to do: `should_relocate`
//! is asked per element, and an element the round refuses is left alone and is not pulled into a
//! batch.
//!
//! # WHAT MOVES AND WHAT DOES NOT, STATED SO THE MEASUREMENT IS NOT READ AS MORE THAN IT IS
//!
//! ```text
//!     pages per container      FALLS -- that is the whole change
//!     index entries per container   FALLS WITH IT, which it did NOT when this module was
//!                                  written. `BlockIndex` has no element-name field, so
//!                                  `block_index_handle` is decided by the model spelling, the
//!                                  object key and the ADDRESS alone. Elements sharing a page
//!                                  share an address and therefore a handle, so the index files
//!                                  ONE entry a page. Measured by the arm below: 40 entries over
//!                                  40 pages before a round, 1 over 1 after.
//!     bytes per element        BARELY MOVES -- measured 12 -> 3 of framing for a set of
//!                                  eight-byte members, which is a small number of a small number
//! ```
//!
//! THE MIDDLE ROW IS RESTATED BECAUSE IT INVERTED, NOT BECAUSE ITS NUMBER WENT STALE. It read
//! "UNCHANGED -- `block_index_handle` hashes the component beside the address, so elements sharing
//! a page still hash to different handles". The field that element name lived on has been removed,
//! so the term is gone from the handle and two elements on one page hash ALIKE. The entry count is
//! the PAGE count now, and the row that used to say what the fold did not buy says what it does.
//!
//! `entry_count_versus_block_count`'s
//! `one_block_is_one_entry_because_the_handle_can_no_longer_name_an_element` drives the mechanism.
//! `a_compaction_round_folds_a_containers_blocks_and_folds_its_entry_count_with_them` below asserts
//! it from this module's own fixture, because a reader of this module should not have to go and
//! find that one to learn what the fold buys.
//!
//! # THE FOUR THINGS THAT COULD GO WRONG
//!
//! ## An element could be folded as empty
//!
//! The fold reads each element's value out of its page. A read that failed and contributed an empty
//! value would rewrite a live element as a zero-length one, and it would read back as PRESENT AND
//! EMPTY -- which is #1989's shape, a member served that should not be. So the read's failure arm is
//! never an `unwrap_or_default`.
//!
//! ## And it could be SKIPPED, which is a different failure and was the first version's
//!
//! Leaving the element alone and carrying on is safe for the ELEMENT and unsafe for the ROUND. A
//! round that hits an unreadable page must propagate, so the partial-failure handler durably commits
//! the consistent partial index -- CP4, and the reason is that a round returning success with the
//! volatile index advanced past the durable one lets the independent reclaim path purge a slab the
//! durable index still names. The first version of the fold skipped, the round returned success, and
//! `part4::partial_compaction_failure_durably_persists_the_consistent_partial_index` went red on its
//! `result.is_err()`. #2026 corrected the comment in `read_block_bytes_for_compaction` that had said
//! the caller skips.
//!
//! `a_container_element_whose_block_cannot_be_read_fails_the_round_rather_than_folding_an_empty_value`
//! drives BOTH halves through the block-read failure seam the compaction tests already use.
//!
//! ## The fold could lose an element
//!
//! Every element is read back after the round, by name, and after a RELOAD as well -- the reload
//! being the arm that proves the page index and the pages agree once several entries share one
//! address.
//!
//! ## The fold could report success without folding
//!
//! "Pages went down" is also what an idle round reports. So the instrument is two counters inside
//! the fold itself -- batches written and pages folded into them -- and every row asserts both are
//! non-zero before it reads anything else. A round that batched nothing fails here rather than
//! passing quietly.
//!
//! # THE CONTROL, AT ZERO, WITH ITS EXERCISED ROWS ASSERTED
//!
//! A `string` key owns one page and one element, so there is nothing to fold and the fold must
//! report zero batches for it WHILE the round still relocates its page. Both halves are asserted:
//! a control over a store where nothing was relocated reports zero for the wrong reason.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// Elements per container in the folding fixtures. Above the element cap for one arm, below it for
/// the others, so the cap is exercised rather than assumed.
const FOLDABLE_ELEMENTS: usize = 40;
const OVER_CAP_ELEMENTS: usize = crate::engine::CONTAINER_BATCH_ELEMENT_CAP + 30;

/// Element payload width, held equal across kinds so a per-element byte figure is comparable.
const VALUE_WIDTH: usize = 24;

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
        table_name: "container-batching".to_string(),
        shard_uri: "local://container-batching/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
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
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

fn bytes_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

/// Distinct page addresses, and index entries, this object's live pages resolve to.
///
/// Both off the engine's own page index rather than off the model maps: the index is what a load
/// reads and what `HashLen` counts, and the point of the fold is that the two numbers come apart.
fn addresses_and_entries(engine: &TemporalEngine, kind: &str, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut addresses = BTreeSet::new();
    let mut entries = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.deleted
                || block.model_id.as_str() != kind
                || &**named.at(bucket.routing_bucket, *handle) != object_key
            {
                continue;
            }
            entries += 1;
            addresses.insert((
                block.address.block_slab_id(),
                block.address.offset(),
                block.address.length(),
            ));
        }
    }
    (addresses.len(), entries)
}

/// Seed one container of each kind under one object key per kind.
fn seed(engine: &TemporalEngine, object_key: &str, elements: usize) {
    for index in 0..elements {
        write(
            engine,
            Command::HashSet {
                key: object_key.to_string(),
                field: format!("f{index:04}"),
                value: bytes_of(VALUE_WIDTH, index),
            },
        );
        write(
            engine,
            Command::SetAdd {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 1_000),
            },
        );
        write(
            engine,
            Command::ListPush {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 2_000),
                left: false,
            },
        );
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetAdd {
                key: object_key.to_string(),
                member: bytes_of(VALUE_WIDTH, index + 3_000),
                score: index as f64,
            },
        });
        assert!(response.status.ok, "a zset write failed: {response:?}");
    }
    write(
        engine,
        Command::StringSet {
            key: format!("{object_key}-string"),
            value: bytes_of(VALUE_WIDTH, 9_999),
        },
    );
}

// =================================================================================================
// 1. THE FOLD, PER KIND, WITH BOTH COUNTERS FLOORED
// =================================================================================================

/// A COMPACTION ROUND FOLDS A CONTAINER'S PAGES AND FOLDS ITS ENTRY COUNT WITH THEM.
///
/// The headline. Pages per container before and after, off the engine's own page index, for all four
/// container kinds, with the string control beside them at zero.
///
/// THE COUNTERS ARE THE DENOMINATOR. "Fewer distinct addresses" is also what a round that relocated
/// nothing reports, so the fold's own two counters are asserted non-zero first and printed on the
/// row.
///
/// RESTATED, AND THE CLAIM INVERTED RATHER THAN THE NUMBER CORRECTED. This read "AND LEAVES ITS
/// ENTRY COUNT ALONE", and that was true of an entry that named its element: the handle hashed the
/// element name beside the address, so two elements sharing one page hashed to two handles and a
/// fold moved the page column only. The name is off the entry now, so the handle cannot tell two
/// elements of one page apart and the entry count IS the page count. The old name is the refuted
/// claim, which is why it is not kept beside a relaxed assertion.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_compaction_round_folds_a_containers_blocks_and_folds_its_entry_count_with_them() {
    // ENTRIES CONVERGE ON PAGES ON EVERY PATH, because there is no longer a path where they do
