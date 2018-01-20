// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WRITE PATH NO LONGER RECOMPUTES A BUCKET'S TTL MINIMUM, AND THE SWEEP STILL DOES.
//!
//! # WHY THE TERM CAME OFF THE WRITE PATH
//!
//! The minimum is one map lookup per page in the bucket, keyed by the page's object key, and it is
//! the last per-page pass in bucket maintenance that does not short-circuit. Every reader of the
//! field it produces is a REPORT or the serializer; real expiry runs off the shard's expiry map and
//! its deadline-ordered mirror instead, and no expiry, eviction or serving decision consults a
//! bucket's TTL.
//!
//! # WHAT BOUNDS THE STALENESS, which is the claim a later reader needs
//!
//! The index flush runs the FULL sweep immediately before it serializes, so THE PERSISTED VALUE IS
//! NEVER STALE -- it is recomputed right before being written. What can lag is the in-memory value
//! the reports read, and it lags by at most one index flush. A load recomputes it as well.
//!
//! # WHY THE EXISTING EQUIVALENCE TEST DOES NOT COVER THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted path against the sweep and does
//! include the TTL -- but as `ttl_ms.is_some()`, a BOOLEAN. A value that is stale and still present
//! compares equal to a fresh one, so that test is insensitive to exactly the difference this change
//! introduces, and it passes either way. MEASURED: it passes with this change in place. That is not
//! evidence the change is correct; it is evidence the test cannot see it. Hence this module, which
//! asserts the VALUE.
//!
//! # THE CONTROL IS THE SECOND HALF OF EACH PAIR
//!
//! "The write path left it alone" is also what a broken computation looks like, so each case asserts
//! the sweep then COMPUTES it on the same shard. Without that second half, deleting the computation
//! outright would pass.
//!
