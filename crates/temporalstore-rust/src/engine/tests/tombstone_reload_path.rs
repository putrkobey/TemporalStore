// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH RELOAD PATH RAN, AND THE MEMBERSHIP SURVIVING BOTH.
//!
//! # THE PROBLEM THIS MODULE EXISTS FOR
//!
//! A reload that silently rebuilds from the WAL and one that read the persisted index END IN THE SAME
//! STATE. Both leave `index_format_version` at the current constant, both serve the right answers, and
//! nothing afterwards distinguishes them. So a test that reloads and checks its members proves the
//! store is correct and says NOTHING about which code produced it -- and this change moves a stored
//! format, so the path is exactly what needs proving.
//!
//! `persistence::index_load_path_counts` is the discriminator, added by this change for the reason
//! below rather than as scaffolding for these tests.
//!
//! # WHY THE STAMP BUMP MAKES THIS THE LARGEST OPERATIONAL CONSEQUENCE
//!
//! `SHARD_INDEX_FORMAT_VERSION` goes 3 -> 5. `load_index_inner` refuses any index whose stamp is
//! BELOW the constant and returns `Ok(None)`, which the caller cannot tell from an absent index, and
//! then replays the log. So every existing store pays a full replay on its first load under this
//! binary. That is the stamp working as designed; what was missing is any way to SEE it happen.
//!
//! # THE THREE THINGS ASSERTED
//!
//!   1. a reload of a store this binary wrote ACCEPTS the index -- so the tombstone entries and the
//!      pages that record removals survive a round trip through the persisted index itself, not just
//!      through a replay;
//!   2. a reload of a store stamped with the PREVIOUS version REFUSES it and replays;
//!   3. the membership is identical either way, and is the membership after the removal rather than
//!      before it. A counter saying "replayed" beside a resurrected member would be worse than no
//!      counter at all.
//!
//! Each arm asserts the counter tuple rather than a total: a sum cannot tell the two paths apart,
//! which is the whole distinction.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

const MEMBERS: usize = 6;
const KEY: &str = "trp-set";

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
        table_name: "tombstone-reload-path".to_string(),
        shard_uri: "local://tombstone-reload-path/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn members_of(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "SMEMBERS failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("SetMembers answered {other:?}"),
    }
}

/// (live, tombstoned) entries for the set under test.
fn entry_counts(engine: &TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != KEY
            {
                continue;
            }
            if block.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

/// Seed the store, remove one member, and unload so the index is materialized.
fn seed_and_unload(dir: &std::path::Path) -> (BTreeSet<Vec<u8>>, Vec<u8>) {
    let engine = engine_on(dir);
    load_on(&engine);
    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|m| format!("member-{m:03}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: KEY.to_string(),
                member: member.clone(),
            },
        );
    }
    // A FOLD FIRST, so the survivors share a page and the removed member's page is one a fold had
    // already rewritten -- the state #2028's refutation was about.
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let victim = members[2].clone();
    write(
        &engine,
        Command::SetRemove {
            key: KEY.to_string(),
            member: victim.clone(),
        },
    );
    let expected: BTreeSet<Vec<u8>> = members
        .iter()
        .filter(|m| **m != victim)
        .cloned()
        .collect();
    assert_eq!(
        expected,
        members_of(&engine),
        "the fixture does not hold what it expects BEFORE the unload"
    );
    let (live, tombstoned) = entry_counts(&engine);
    // THE DENOMINATOR, STATED FOR WHICHEVER PROJECTION IS FILING, and as the invariant rather than
    // as a number. The fixture FOLDS before removing, so the survivors share one page:
    //
    //   * per-element, one entry names each surviving member -- MEMBERS - 1 of them;
    //   * one entry a page, ONE entry names the folded page they all share.
    //
    // Either way exactly one tombstone names the member removed, and the membership assertion
    // directly above this one is what says the survivors are all there -- so this is a count of
    // how they are FILED and not of how many there are. Both of this module's tests failed here,
    // two assertions before the accept-or-replay decision they exist to drive, which is why this
    // is the fixture's statement to correct and not theirs.
    //
    // AND THESE TWO ARE THE TRIPWIRE FOR A STEP THAT HAS NOT HAPPENED YET. `SHARD_INDEX_FORMAT_VERSION`
    // does not move for the collapse, because no stored field does: the entry struct is unchanged
    // and the tombstone's element name was always a field the per-element path filled. What IS new
    // is the combination a gated index holds -- live entries carrying no element name beside
    // tombstones that carry one -- and today nothing in the load path looks at that. If a later
    // step makes the reader's acceptance check consider entry SHAPE rather than only the stamp
    // value, these two tests are where it would surface, because they are the only ones that drive
    // the accept-or-replay decision over a store this binary wrote itself. That is the moment the
    // stamp question reopens.
    // ONE LIVE ENTRY, UNCONDITIONALLY: the per-element projection this chose against is retired.
    let expected_live = 1;
    assert_eq!(
        (expected_live, 1),
        (live, tombstoned),
        "before the unload there are {live} live and {tombstoned} tombstone entries, where this \
         projection files {expected_live} live and 1"
    );
    // Unload materializes the base index, which is what the reload below has to read.
    engine.unload_shard(1);
    (expected, victim)
}

