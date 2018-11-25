// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE STALE RECORD'S TWO PARTITIONS, CHECKED FROM THE PUBLISHED STATUS.
//!
//! # WHY NOT THE ARM THAT ALREADY CHECKS THEM
//!
//! `the_overwrite_records_what_it_retired` asserts both identities over `SlabStaleSummary`, inside
//! the engine, where every term is in hand. This asserts them over `ShardCanonicalStorageStats` —
//! what a consumer receives — and until the eight unpublished figures were carried onto it, neither
//! identity was checkable there, because each was missing a term.
//!
//! The two arms fail for different reasons, which is the point of having both: the inner one if the
//! record's arithmetic breaks, this one if the status stops carrying a term that arithmetic needs.
//! **An assignment that drops a field passes the inner arm.**
//!
//! # THE TWO PARTITIONS
//!
//! ```text
//! pending_discard_bytes + stale_bytes_discarded          == stale_bytes_recorded
//! by_overwrite + by_removal + by_sweep                   == stale_bytes_recorded
//! ```
//!
//! Integers on both sides, so these are exact — no rounding, no tolerance. A partition constrains
//! itself in a way a ratio cannot, which is why the published figures are shaped this way.
//!
//! # AND A SUM THAT ACCOUNTS CAN STILL BE ATTRIBUTED WRONGLY
//!
//! Three terms summing to the right total, with the weight in the wrong term, is a passing sum and a
//! wrong answer. So this also asserts that the cause the fixture CREATES is the cause the status
//! attributes: a pure-overwrite workload must put every retired byte in `by_overwrite` and none in
//! the other two. That is the shape of a cause-label error measured once at 100% wrong — a pure
//! overwrite corpus reporting its bytes under removal — and a sum alone would not have caught it.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_published_stale_record_accounts_for_itself -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 1_000;
