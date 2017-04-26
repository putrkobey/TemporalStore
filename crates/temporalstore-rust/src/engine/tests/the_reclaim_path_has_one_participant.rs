// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RECLAIM HAS ONE PARTICIPANT, which is why twelve comparison fields are out of scope.
//!
//! # THE TWELVE, AND WHAT THEY PRESUPPOSE
//!
//! Twelve names in the storage-metric comparison describe a reclaim coordinated across chunk
//! servers: endpoints to enumerate and disable, control fences, route-state fence matching, plan and
//! raft-entry checksums, trim flow rate limits, and per-node budget and pressure counts.
//!
//! Every one presupposes **multiple reclaim participants**. This store's reclaim has one: a shard
//! owning local slabs. There is nothing for those fields to report, and filling them with degenerate
//! values — one endpoint meaning self, fencing always false, a plan checksum over a plan nobody else
//! sees — would produce fields that *look* like the format and mean nothing. That is the defect this
//! whole surface has been corrected for: a figure carrying a name that does not describe its own
//! arithmetic.
//!
//! # WHY THAT NEEDS A TEST AND NOT A PARAGRAPH
//!
//! "Out of scope" was an assertion in a pull-request body, and the premise under it is a **property
//! of the code**. If reclaim ever gains a second participant, those twelve stop being out of scope
//! and nothing would say so.
//!
//! So the premise is pinned here. `block_store/block_store_gc.rs` imports exactly `super::*` and contains none
//! of the constructs a remote participant needs. The day that changes, this fails and names what
//! came into scope with it.
//!
//! Same instrument as its two neighbours: `a_point_read_fills_the_physical_tier` pins a declined
//! design so it cannot return silently, `the_comparison_counterparts_still_exist` pins a mapping so
//! it cannot become fiction, and this pins a scope boundary so it cannot go stale.
//!
//! # WHAT IT DOES NOT CLAIM
//!
//! Not that the store is single-node — it plainly is not, and replication, raft and a shared store
//! all contact peers. The claim is narrower and is the one the exclusion rests on: **the block
//! reclaim path itself** decides and performs reclamation without contacting anybody.
//!
//! rust-internal: reads this crate's own source, no product behaviour

/// The twelve this premise excludes, listed so the failure message can name them.
const EXCLUDED_BY_SINGLE_PARTICIPANT: &[&str] = &[
    "candidate_chunk_endpoints",
    "best_reclaim_candidate_endpoint",
    "skipped_disabled_chunk_endpoints",
    "chunk_control_fenced",
    "block_route_state_fence_matched",
    "expected_plan_checksum",
    "prepared_raft_entry_checksum",
    "trim_flow_max_ops_per_second",
    "trim_flow_throttled_requests",
    "effective_max_trim_ops",
    "reclaim_budget_limited_node_count",
    "reclaim_pressure_node_count",
];

/// Constructs the block reclaim path would need in order to involve a second participant. Chosen to
/// be what a remote call ACTUALLY requires rather than words that merely sound remote: a socket, an
/// http client, an async boundary, a thread, or a channel.
const REMOTE_PARTICIPANT_CONSTRUCTS: &[(&str, &str)] = &[
    ("reqwest", "an http client"),
    ("TcpStream", "a socket"),
    ("UdpSocket", "a socket"),
    (".await", "an async boundary, which a local synchronous walk does not need"),
