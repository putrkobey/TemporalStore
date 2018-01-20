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
