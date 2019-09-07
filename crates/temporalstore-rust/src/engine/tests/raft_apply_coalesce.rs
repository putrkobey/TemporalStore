// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for the raft state-machine apply-path fsync coalescing.
//! On the raft apply path the raft log is the durability + reconstruction source, so a batch of
//! committed entries (an AppendEntries batch on a follower, a recovery replay, or a pipelined
//! propose group) only needs ONE engine-WAL durability barrier instead of one per entry. These
//! tests prove:
//!   (a) with the gate ON, applying a batch of N committed commands via
//!       `execute_raft_apply_batch` takes exactly ONE fdatasync (coalesced) while every value is
//!       present + byte-exact on the live engine;
//!   (b) with the gate OFF, the same batch degrades to per-entry `execute_raft_apply` -> exactly
//!       N fdatasyncs (byte-identical legacy behavior);
//!   (c) the coalesced batch is durable: after a drop + WAL-replay reload every applied command is
//!       recovered byte-exact -- the coalesced barrier made the whole batch durable before the
//!       apply returned, so nothing acked-and-applied is lost.
#![allow(clippy::all)]
use super::*;

// These used to serialize against each other through a mutex, because the choice was a
// process-global env var and an OFF baseline could observe an ON window from a sibling. It is a
// property of an engine now, and each of these builds its own.

const BATCH: usize = 64;
