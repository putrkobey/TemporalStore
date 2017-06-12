// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Do a deleted hash field and a deleted set member survive a restart?
//!
//! `hash` and `set` are two of the five kinds whose typed removal goes through
//! `mark_bucket_index_block_deleted`, which stages `address: None, deleted: true`, while their arms
//! in `apply_outcome_item` open by demanding an address. `list`, `zset` and `string` are confirmed
//! to refuse the load because of it; these two share the arm shape and had no coverage at all.
//!
//! **Both fail**, which completes the set at five:
//!
//!     hash outcome at sequence 3 ... (address UNRESOLVED, component present, 5 chars)
//!     set  outcome at sequence 3 ... (address UNRESOLVED, component present, 10 chars)
//!
//! -- the hash field `alpha`, and the same member hex-encoded for the set. So EVERY typed removal
//! refuses the load: a deleted hash field, a removed set member, a popped list element, a rescored
//! zset member, a deleted string.
//!
//! They pass with the deleted-branch this change adds, and fail without it.//!
//! Dropped, NOT unloaded. `unload_shard` flushes the index, so the reopened engine reads a base
//! that already has the delete and never replays the tail -- a test written that way passes
//! without touching the path it is about, which is how the string case was briefly and wrongly
//! declared healthy.

use std::path::PathBuf;

use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, LoadShardRequest, TemporalEngine};

const SHARD_ID: u64 = 1;
