// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![doc = include_str!("../README.md")]

// COMPILED INTO EVERY BUILD, not only test builds.
//
// It used to be `#[cfg(test)]`, which was right while everything that read the counters was a
// test. The allocation-class scopes that say what a write's memory is spent on are not: they sit
// inside the primitives that own each sink, because that is the only placement a new call site
// cannot go around -- the lesson of #1882 and #1899, where counters at one call site of nine saw
// one write in twelve.
//
// The cost of that in a build without the `alloc-probe` feature below is nothing at all:
// `in_class` is its closure, with no atomic, no thread-local and no branch, and what remains is a
// few hundred bytes of never-incremented counters. What the feature changes is not whether the
// module exists but whether anything counts -- and `classified_now` answers `None` rather than a
// table of zeros when nothing does, so an uninstrumented build cannot be read as a clean one.
pub mod alloc_probe;

// Tests only. Counts what a raft snapshot walks, reads, copies and rebuilds, bumped inside the
// primitives that do the work rather than at the call sites that ask for it.
#[cfg(test)]
pub mod snapshot_probe;

// Tests only, and only under the `alloc-probe` feature. RSS cannot resolve a per-request
// allocation change -- 71% of the proxy's resident memory was measured as allocator retention
// rather than live data -- so memory claims about a request path need the allocations counted
// directly.
//
// Feature-gated rather than always-on in tests: with this installed unconditionally, a full
// single-threaded run turned 17 socket-bound proxy and raft tests red that pass individually, as a
// group, and in a full run without it. Two atomics on every allocation in the process is enough to
// disturb tests that wait on sockets, and an instrument that changes the result of the suite
// checking the change is worse than no instrument.
#[cfg(all(test, feature = "alloc-probe"))]
#[global_allocator]
static COUNTING_ALLOCATOR: alloc_probe::CountingAllocator = alloc_probe::CountingAllocator;

pub mod bytes_serde;
pub mod block_store;
pub mod checksum;
#[path = "temporalstore_client.rs"]
pub mod client;
pub mod context_workflow;
pub mod control;
pub mod data_node;
pub mod e2e;
pub mod durability_metrics;
pub mod flush_gate;
pub mod memory_trim;
pub mod engine;
pub mod env_flag;
pub mod http;
pub mod index_log;
pub mod ingestion;
pub(crate) mod log_framing;
#[cfg(feature = "matrixobject")]
pub mod matrixobject_store;
pub mod meta;
pub mod partition_id;
pub mod proxy;
pub mod raft;
pub mod readiness;
pub mod rebalance;
pub mod redis;
mod scratch;
pub mod sdk;
pub mod shared_store;
pub mod storage_backend;
pub mod storage_config;
pub mod storage_descriptor;
pub mod telemetry;
#[path = "command_protocol_types.rs"]
pub mod types;
pub mod fault;
pub mod wal;
/// What an append costs per record and what a replay costs per log -- tests only.
#[cfg(test)]
mod wal_batch_roll;
mod wal_replay_scale;
/// What a WINDOWED replay reads against the log it replays -- tests only.
mod wal_replay_windows;
/// What reading a log piece's base header COSTS -- tests only.
mod wal_base_header_read;
/// What a RESTORE reads, and what no reader claims -- tests only.
mod restore_read_residual;
mod wal_scale;
/// What one log RECORD costs to build and to decode -- tests only.
mod record_cost;
mod wal_proto;
