// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MARKING AN OBJECT DIRTY REACHES THE PAGES IT ALREADY HAS, AT MARK TIME.
//!
//! # THE INVARIANT, AND IT IS THE WHOLE OF THE CHANGE
//!
//! A bucket is dirty when any page in it is dirty. It used to also be dirty when any page's
//! OBJECT was dirty, which asked a page which object it belonged to -- on the write path, in a
//! scope holding nothing but the bucket. The dirtiness is pushed onto the pages at mark time now,
//! so the predicate is `any(page.dirty)` and the question is never asked.
//!
//! That is only equivalent if EVERY SITE THAT MARKS AN OBJECT DIRTY ALSO PUSHES. There are exactly
//! two, and the fail-first for this module is omitting the push at one of them: the bucket then
//! reads clean while its object is dirty, which is a silent wrong answer rather than a failure.
//!
//! # WHY THE NEAREST EXISTING GUARD CANNOT STAND IN FOR THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted refresh against the sweep and
//! does include dirtiness -- as a BOOLEAN, `bucket.dirty()`. It can see THAT a bucket is dirty and
//! not WHY, so it passes whether the dirtiness arrived through the object set or through the pages.
//! It is insensitive to this change rather than wrong about it, and it was cited in prose as
//! holding the equivalence, which is the worse failure: it stops the next reader looking. This
//! module asserts the mechanism it cannot see.
//!
//! # AND THE PREMISE THAT FAILED TWICE, KEPT BECAUSE IT IS WHY THE CODE IS SHAPED THIS WAY
//!
//! The push can only find pages through the object lookup, which is derived and empty until
//! something builds it. The first version asserted that a mark could not reach a shard holding
//! buckets with an unbuilt lookup, reasoning that a load rebuilds before returning the shard. TWO
//! TESTS FIRED THAT ASSERTION IMMEDIATELY, and it was replaced by a rebuild.
//!
//! BOTH THE ASSERTION AND THE TWO TESTS WERE ABOUT THE WRONG CONDITION. "Buckets present" is not
//! "pages present", and "pages present" is not "a LIVE page present" -- a tombstone is a page a
//! mark must not dirty. Measured over a 107-name set of mark-reaching tests, 97 verdicts: six
//! reaches had no pages at all, two had only tombstones, and the only two reaches with a live page
//! were the fixtures in this module, each writing into `bucket_map` directly with no filing
//! anywhere in the process.
//!
//! AND A CENSUS MAKES THAT STRUCTURAL. All nine production sites that install a page into a
//! bucket's map file it in the same enclosing function; the only non-filing doors have no callers
//! at all or none outside tests. So a live page cannot be resident unfiled in production, the
//! rebuild served no reachable state, and `push_object_dirty_onto_its_existing_blocks` now ASSERTS
//! that invariant where it used to repair it -- which makes every one of the 926 tests that enter
//! it the check, instead of one fixture here.

use std::sync::Arc;

use crate::engine::hashing::block_routing_bucket;
use crate::engine::state::{BlockIndex, BlockSlabLiveIndex, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    mark_object_dirty, refresh_bucket_runtime_flags,
};

const WHOLE_RANGE_END: u32 = u32::MAX;

fn a_shard_holding_one_clean_block(object_key: &str) -> (ShardState, u32) {
    let routing_bucket = block_routing_bucket(object_key, 0, WHOLE_RANGE_END);
    let mut shard = ShardState::default();
    let mut bucket = BucketNode::default();
    bucket.routing_bucket = routing_bucket;
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_block(&object_key, 
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            // CLEAN, which is the only interesting starting state: a page that is already dirty
            // would satisfy every assertion below whatever the mark did.
            dirty: false,
            deleted: false,
        },
        &mut live,
    );
    shard.bucket_index.bucket_map.insert(routing_bucket, bucket);
    (shard, routing_bucket)
}

/// A shard holding one clean page THAT NOTHING HAS FILED -- a state production cannot reach.
///
/// Kept, and kept unfiled, because it is the subject of
/// [`the_push_builds_the_object_lookup_when_it_finds_it_unbuilt`]'s successor: the push now
/// REFUSES this state rather than repairing it. The census that says production cannot reach it is
/// written at the assertion in `push_object_dirty_onto_its_existing_blocks`; this is the fixture
/// that proves the assertion can fire.
///
/// Every other arm in this module wants [`a_shard_holding_one_filed_clean_block`], because their
/// subjects are positive -- that a mark reaches its pages, and that a mark on a different object
/// does not -- and neither is testable in a state the engine refuses.
fn a_shard_holding_one_filed_clean_block(object_key: &str) -> (ShardState, u32) {
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_block(object_key);
    // FILED, AS ALL NINE PRODUCTION INSTALL SITES FILE: in the same breath as the install.
    //
    // THIS IS NOT A FIXTURE QUIETLY REPAIRED TO MAKE A TEST PASS. The unfiled state used to be the
    // only thing in the tree describing the window the push's rebuild existed for, which is why
    // editing it would have erased the finding. The assertion in
    // `push_object_dirty_onto_its_existing_blocks` is that description now, held on every run by
    // every test that marks -- so filing here restores a positive claim the assertion has made
    // safe to restore, rather than hiding anything.
    let placed: Vec<(u64, BlockIndex)> = shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .block_index
        .iter()
        .map(|(handle, block)| (*handle, block.clone()))
        .collect();
    let key: Arc<str> = Arc::from(object_key);
    for (handle, block) in &placed {
        shard
            .bucket_index
            .insert_object_block_lookup(routing_bucket, *handle, &key, block);
    }
    (shard, routing_bucket)
}

fn any_block_dirty(shard: &ShardState, routing_bucket: u32) -> bool {
    shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .block_index
        .values()
        .any(|block| block.dirty)
}

#[test]
fn marking_an_object_dirty_sets_the_dirty_bit_on_the_blocks_it_already_has() {
    // THE FILED FIXTURE, because this arm's subject is POSITIVE -- that a mark reaches the pages
    // an object already has -- and that is not testable in a state the engine refuses.
    let (mut shard, routing_bucket) = a_shard_holding_one_filed_clean_block("pushed-object");

    // THE STARTING STATE IS ASSERTED, or "it is dirty afterwards" could be true of a fixture that
    // was dirty all along and the mark could be doing nothing.
    assert!(
        !any_block_dirty(&shard, routing_bucket),
        "the fixture page must start clean"
    );

    mark_object_dirty(&mut shard, "pushed-object", 0, WHOLE_RANGE_END);

    assert!(
        any_block_dirty(&shard, routing_bucket),
        "marking the object must reach the page it already has; otherwise the bucket predicate, \
         which now reads only `page.dirty`, answers false for a dirty object"
    );

    // AND THE PREDICATE THE PUSH EXISTS TO SERVE actually answers true through it. The sweep
    // recomputes the bucket flag from the pages alone.
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("bucket")
            .dirty(),
        "the sweep must see the bucket as dirty, reading only the pages"
    );
}

#[test]
fn an_unmarked_object_leaves_its_blocks_clean() {
