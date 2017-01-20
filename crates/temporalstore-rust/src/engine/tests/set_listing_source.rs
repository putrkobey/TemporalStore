// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SET LISTING DECODES EACH PAGE ONCE AND STILL TAKES ITS IDENTITY FROM THE ENTRIES.
//!
//! # WHAT CHANGED, AND WHAT DELIBERATELY DID NOT
//!
//! The listing read one page per entry. After a fold every entry of an object names the SAME page,
//! so a forty-member set read one page forty times and each read was a linear walk of that page's
//! packed keys. It now decodes each distinct page ONCE and answers each entry out of that.
//!
//! WHAT DID NOT MOVE IS WHERE IDENTITY COMES FROM. The entry walk still says which members exist.
//! That is not conservatism, it is the correctness argument, and it is here because the first
//! version of this change got it wrong.
//!
//! # THE REJECTED VERSION, RECORDED BECAUSE IT LOOKED RIGHT
//!
//! The first attempt enumerated members out of the payload and dropped the entry walk. It read one
//! page instead of forty. It had a source-proof -- drop an entry, the member survives -- and a
//! working negative control. It also **resurrected a removed member**, and `tombstone_reload_path`
//! caught it.
//!
//! The mechanism is the thing to remember: a fold followed by a removal leaves the folded page
//! still naming the member as LIVE, while the removal lives in a SEPARATE tombstone page and a
//! DELETED index entry. A reader enumerating the payload sees neither of those, so it serves an
//! element the store was told to forget.
//!
//! And the guard that missed it was not badly built -- it was built over the wrong property.
//! **Presence and absence are two properties.** A loss guard asks "is everything that should be
//! here, here?"; a resurrection guard asks "is everything that should be gone, gone?" The
//! source-proof and the negative control were both about presence. So this module asserts both,
//! and the absence arm is the one that would have failed.

#![allow(clippy::all)]
use super::*;

/// Forty on one compacted page is the measured, real shape.
const MEMBERS: usize = 40;

/// Member payload width, wide enough that a member is not confusable with framing.
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
        table_name: "set-listing-source".to_string(),
        shard_uri: "local://set-listing-source/1".to_string(),
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

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("member-{index:04}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a fixture write failed: {response:?}");
}

/// A loaded shard holding one set of `MEMBERS` members, folded onto as few pages as the batcher
/// will use. The fold is asserted, because every arm here rests on members sharing a page.
fn folded_set(object_key: &str) -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    for index in 0..MEMBERS {
        write_to(
            &engine,
            Command::SetAdd {
                key: object_key.to_string(),
                member: member_bytes(index),
            },
        );
    }
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the compaction round failed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "the fixture folded nothing: {batches} batch(es), {folded} page(s). Every arm below rests \
         on the members sharing a page, so this is not a fixture detail."
    );
    (engine, dir)
}

/// Distinct page addresses and LIVE index entries this object resolves to, plus tombstoned ones.
fn block_and_entry_counts(
    engine: &TemporalEngine,
    object_key: &str,
) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
