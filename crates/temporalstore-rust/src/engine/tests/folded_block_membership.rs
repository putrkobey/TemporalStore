// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FOLDED PAGE IS NOW AUTHORITATIVE FOR MEMBERSHIP, AND THIS IS THE CHECK THAT SAID IT WAS NOT.
//!
//! # WHAT THIS MODULE USED TO ASSERT, AND WHY IT IS INVERTED RATHER THAN DELETED
//!
//! #2027 folds a container's element pages into one at compaction. #2028 then drove the refutation of
//! the stage that was to follow: twelve set members folded to one page, one member removed, the live
//! index naming eleven and the page holding twelve. A removal reached the resident map and the page
//! INDEX and did not reach the PAGE, because a folded page stays live for its other elements and
//! nothing rewrites it. So the page set was a SUPERSET of the membership, and deriving membership
//! from it resurrected the removed member -- exactly the over-complete state #2025 enumerated five
//! readers for, four of them live, arriving from the other side.
//!
//! The assertion that carried that constraint said, in its own failure message, that a red here would
//! mean the constraint had been LIFTED and was a result rather than a break. It has been. The
//! assertion is inverted in place rather than moved to a new module so that the diff is the record:
//! the row counts either side of it are the same measurement, and the verdict flipped.
//!
//! # WHAT LIFTED IT
//!
//! `container_blocks`' second shape carries a per-item removal flag, and the removal path appends a
//! page that states the removal instead of only dropping the index entry. So the page set is no longer
//! a superset of the membership; it is a LOG of it, folded by `container_membership::derive_membership`
//! in append order.
//!
//! # WHAT THIS ASSERTS NOW
//!
//! Three things, and the third is the one that would have been easy to leave out:
//!
//!   1. the PAGES name the removed member as REMOVED -- not merely absent from a page, which is a
//!      different and much weaker fact;
//!   2. the membership DERIVED from the pages equals what the RESIDENT MAP holds, element for
//!      element. That side was the page index's element names until the width step took the
//!      element-name field off the entry; `shard.sets` is the durable authority and the served
//!      answer now, and the pair is stronger for it -- the entry name and the page payload were
//!      both written by one append, while the map and the pages are maintained by different paths;
//!   3. and the derivation is COMPLETE -- no page failed to read, none failed to walk, none was
//!      unframed. An incomplete derivation that happened to agree would agree by luck, and #2028's
//!      own fix to `insert_timestamped_secondary_view` is the recorded case of a read failure
//!      producing a silently short derived view that nothing else held a copy of.
//!
//! # DENOMINATORS
//!
//! Every side is counted and asserted non-zero before it is compared. A fixture that folded nothing,
//! or that read no page, would make two sides agree for the wrong reason. The fold's own counters are
//! asserted first, because "one page per container" is also what an object with one element looks
//! like -- and the tombstone's own page is asserted to exist, because a removal that wrote no page
//! would leave the derivation agreeing with the index only because the member was in neither.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// Members written. More than one page's worth would obscure the point; twelve fold into one page,
/// which is the state the refutation was about.
const MEMBERS: usize = 12;

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
        table_name: "folded-page-membership".to_string(),
        shard_uri: "local://folded-page-membership/1".to_string(),
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

/// rust-internal: drives the engine's own command surface
#[test]
fn a_folded_blocks_membership_now_states_the_member_the_live_index_no_longer_names() {
    // THE FIXTURE'S SERVED SIDE IS THE RESIDENT MAP, NOT THE PAGE INDEX. This comment predicted
    // the red rather than fixing it: "THE FIXTURE COUNTS MEMBERS THE LIVE INDEX NAMES, which is
    // zero once entries name pages instead of elements -- its floor says the removal did not do
    // what this fixture assumes." It is zero, and the fix is to read the side where membership
    // actually lives. See the re-attribution at SIDE 1 below.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|i| format!("member-{i:04}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: "folded".to_string(),
                member: member.clone(),
            },
        );
    }

    // FOLD FIRST. The counters are the instrument, because "one page per container" is also what an
    // idle round over a one-element object reports.
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "nothing folded ({batches} batches, {folded} pages), so this fixture is not in the state it \
         claims to be asking about"
    );

