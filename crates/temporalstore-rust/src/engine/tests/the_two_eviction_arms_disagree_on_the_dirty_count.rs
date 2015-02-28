// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two eviction arms must agree on what a victim's dirty count means.
//!
//! WHY THIS EXISTS. `StorageEvictionVictim::dirty_object_count` is filled by two different arms of
//! `apply_storage_eviction`, from two different quantities, and both feed the SAME weight:
//!
//!     weight = cache_memory*4 + cache_disk*2 + physical_bytes + dirty_object_count*1024
//!
//! * the SAMPLED arm -- the shipped default -- walks `bucket.block_index` and counts PAGES whose
//!   own `dirty` bit is set;
//! * the FULL-SCAN arm takes `summary.dirty_object_count`, which comes from the object-keyed dirty
//!   index and counts OBJECTS.
//!
//! An object can hold several pages, so these are not the same number, and the field name says
//! object while one arm puts pages in it. The consequence is not cosmetic: the two arms rank
//! victims differently for a reason that has nothing to do with eviction policy, and a weight is a
//! sort key.
//!
//! WHAT THIS TEST REFUSES TO DO. It does not assert a particular definition is correct -- that is a
//! policy question. It asserts the two arms AGREE, which is the part neither answer can excuse.
//!
//! NON-VACUITY, asserted rather than hoped for. On a fixture where every object happens to hold one
//! page, pages and objects are equal and the two arms agree for the wrong reason -- such a fixture
//! would pass against the defect. So the fixture seeds a multi-page object and the test asserts
//! that some bucket really does hold more dirty pages than dirty objects BEFORE comparing the arms.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 11;

/// A store seeded so that at least one bucket holds a multi-page object.
///
/// `page_size` is deliberately small so one feature series spans several pages: a fixture whose
/// objects are all single-page cannot tell a page count from an object count.
fn seeded(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);

    // One wide series: many points under ONE object key, so its pages outnumber it.
    for chunk in 0..6usize {
        let points = (0..64u64)
            .map(|t| crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000 + (chunk as u64) * 64 + t,
                value: vec![b'f'; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::FeatureAppend {
                key: "wide".to_string(),
                points,
            },
