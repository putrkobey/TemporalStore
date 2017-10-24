// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE GATED LISTING ANSWERS FROM THE PAGES, AND A REMOVED MEMBER STAYS GONE.
//!
//! # WHAT CHANGED AND WHY IT IS THE RISKY DIRECTION
//!
//! The ungated listing asks the ENTRIES which members exist and the page only what each one holds.
//! Its own `None` arm states why that cannot survive this gate: "an entry naming no component can
//! only be answered by a page that names no element either". Under the gate every entry names no
//! component and every page IS framed, so that arm drops every member and the listing answered
//! EMPTY -- measured at `listed=0` against 40 ungated, by two independent fixtures.
//!
//! So the gated listing takes identity out of the PAYLOAD. That is the direction that
//! **resurrected a removed member** when it was tried before, and the resurrection is the thing
//! this module exists to rule out rather than to hope about.
//!
//! # THE TWO THINGS THAT MAKE IT SAFE, BOTH DRIVEN RATHER THAN ARGUED
//!
//!   1. the tombstone page is REACHABLE from the index -- a gated removal files a page-named
//!      tombstone entry beside the live entry, which
//!      `a_gated_removal_leaves_a_tombstone_block_the_index_can_reach` drives;
//!   2. `container_membership::derive_membership` owns the ORDERING, folding every page of the
//!      object by `append_position` with a value and a tombstone as the same kind of statement, so
//!      the later page wins. A hand-rolled loop over the payload lacks exactly that precedence,
//!      and that is the loop that resurrected a member.
//!
//! # WHY THERE ARE COUNTERS AND NOT JUST A MEMBER LIST
//!
//! The gated branch declines an INCOMPLETE fold and answers from the durable map instead. That
//! fallback returns the right members, so a test that only checked the members would pass
//! identically whether the fold ran or never ran -- the exact shape that made an earlier
//! measurement in this series report a clean result over a branch nothing reached. So the branch
//! counts which path answered, and the floor here requires the FOLD to have served.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

const MEMBERS: usize = 40;
const KEY: &str = "gated-listing-set";




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
        table_name: "gated-listing".to_string(),
        shard_uri: "local://gated-listing/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:03}").into_bytes()
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn listed(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
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

/// Seed, fold, and remove one member. Returns the expected survivors and the victim.
fn seed_fold_remove(engine: &TemporalEngine) -> (BTreeSet<Vec<u8>>, Vec<u8>) {
    load_on(engine);
    for index in 0..MEMBERS {
        write(
            engine,
            Command::SetAdd {
                key: KEY.to_string(),
                member: member_bytes(index),
            },
        );
    }
    // FOLD FIRST. Before a fold each member owns its page, so removing one takes its page with it
    // and the pages never disagree -- the fixture would pass for the wrong reason.
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let victim = member_bytes(13);
    write(
        engine,
        Command::SetRemove {
            key: KEY.to_string(),
            member: victim.clone(),
        },
    );
    let expected: BTreeSet<Vec<u8>> = (0..MEMBERS)
        .map(member_bytes)
        .filter(|m| *m != victim)
        .collect();
    (expected, victim)
}

/// rust-internal: the gated listing folds the pages, and the removed member does not come back
#[test]
fn a_gated_listing_folds_the_blocks_and_the_removed_member_stays_gone() {
    // SEPARATE STORES PER ARM. One shared store would let the second arm read the first's cached
    // answer, which has already produced a wrong figure once in this campaign.
    let gated_dir = tempfile::tempdir().expect("tempdir");
    let plain_dir = tempfile::tempdir().expect("tempdir");

    // ---- THE GATED ARM ----
    let expected = {
        let engine = engine_on(gated_dir.path());
        let (expected, victim) = seed_fold_remove(&engine);
        crate::engine::execute_on_shard::reset_gated_listing_counts();
        let seen = listed(&engine);
        let (derived, declined) = crate::engine::execute_on_shard::gated_listing_counts();

        println!("\n=== the gated listing ===");
        println!(
            "  members listed: {} of {} expected; victim present: {}",
            seen.len(),
            expected.len(),
            seen.contains(&victim)
        );
        println!("  answered by: derived={derived}, declined={declined}");

        // THE FLOOR, on the reaching of the path and not on the answer: the FOLD must have served.
        // The decline path answers from the durable map and would return the same members, so
        // without this the arm below passes whether or not the fold ran.
        assert_eq!(
            1, derived,
            "the gated listing was answered by the fold {derived} time(s) and declined \
             {declined} time(s). The decline path answers from the durable map and returns the \
             same members, so this assertion is what distinguishes a tested fold from an \
             untested one"
        );
        assert_eq!(
            0, declined,
            "the gated listing DECLINED the fold {declined} time(s) and fell back to the durable \
             map. A decline means a page could not be read, decoded, or walked -- the members \
             below would be right for a reason that has nothing to do with the pages"
        );

