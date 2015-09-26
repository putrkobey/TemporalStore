// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shares one durability barrier between everything that is waiting for one.
//!
//! Used by the raft node log and by the index log: both append and fsync per write, and both
//! had every writer pay for a barrier that would have covered all of them.
//!
//! An `fsync` makes every byte already written to the file durable, not just the caller's own, so
//! a barrier taken while other writers are queued behind it covers them too. Taking one barrier
//! per writer therefore pays repeatedly for work a single barrier would have done: measured at 1,
//! 4 and 16 concurrent writers, barriers-per-write stayed flat at exactly 1.0 --
//! sixteen writers cost sixteen barriers where one would have sufficed.
//!
//! The rule here is that at most one barrier is ever in flight per file. A writer that arrives
//! while one is running does not start a second: it waits, and the next barrier covers it.
//! Barriers-per-write then falls roughly as 1/concurrency instead of staying flat, while a lone
//! writer still takes exactly one barrier and waits no longer than it does today.
//!
//! Ordering is what makes this safe. A writer registers its ticket only after its `write_all` has
//! returned, and a barrier claims a target of "every ticket registered so far", so a ticket a
//! barrier counts is always already in the file. Sequence numbers only increase, so
//! `durable >= mine` is a complete test for "my bytes are on disk".

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// A writer's claim on a future barrier: "make everything up to here durable".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushTicket(u64);

#[derive(Debug, Default)]
struct GateState {
    /// Tickets handed out so far. Every one of these is already written to the file.
    issued: u64,
    /// The highest ticket a completed barrier has covered.
    durable: u64,
    /// Whether a barrier is running right now. At most one ever is.
