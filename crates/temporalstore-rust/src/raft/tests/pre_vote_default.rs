// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Pre-vote must be on wherever a raft runtime takes `RaftConfig::default()`.
//!
//! Every env-driven entry point in this crate builds its raft config from
//! `RaftConfig::default()` and never names `enable_pre_vote`:
//!
//! * `bin/metaserver.rs` `runtime_options_from_env` -- `config: RaftConfig::default()`
//! * `bin/raft_node.rs` `runtime_options_from_env` -- `config: RaftConfig::default()`
//! * `bin/server.rs` `raft_config_from_env` -- overlays three knobs over `..defaults`
//!
//! so the default is what all three run, and the two public constructors
//! (`RaftCluster::new_single_shard`, `MetaRaftCluster::new`) hand the same default to
//! library consumers. Without pre-vote a node that was partitioned and rejoins bumps its
//! term and forces an election, unseating a leader that was serving fine.
//!
//! The halves are asserted separately: the default itself, and what a runtime built the
//! way production builds it actually ends up with.

use super::*;

/// What `enable_pre_vote` a meta raft runtime ends up with when it is built exactly the
/// way `bin/metaserver.rs` `runtime_options_from_env` builds it.
fn meta_runtime_pre_vote() -> bool {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        snapshot_check_interval_ms: 30_000,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 1,
        nodes: vec![ProductionRaftNode {
            node_id: 1,
            addr: "127.0.0.1:17101".to_string(),
        }],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 10_000,
        stale_server_after_ms: 30_000,
        forbid_self_clearing_conviction: false,
    })
