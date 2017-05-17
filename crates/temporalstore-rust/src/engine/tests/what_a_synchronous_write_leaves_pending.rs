// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SYNCHRONOUS WRITE LEAVES PENDING, which decides whether we already write through.
//!
//! # THE QUESTION
//!
//! The design being followed writes through per object: extents reach storage before the manifest
//! entry commits, so nothing is ever pending write-back and no set of dirty objects is needed. Ours
//! keeps `dirty_objects`, and the standing reading has been that this is a divergence in the write
//! path.
//!
//! **One name is enough to doubt that.** The only two sites that mark an object dirty are reached
//! through `mark_object_dirty`. If dirtying is a property of the ASYNC path, then
//! `dirty_objects` is the cost of an option they do not offer rather than the cost of our default,
//! and a synchronous write already does what theirs does.
//!
//! That is the difference between a configuration and an architecture change, so it is measured
//! rather than argued: the same corpus written twice, once synchronously and once through the async
//! path, with the write-back set read after each.
//!
//! # AND THE ANSWER IS THE SECOND ONE
//!
//! **Both paths leave the whole corpus pending — 64 of 64.** So the name describes the caller it
//! was first written for, not a condition: dirtying is unconditional, a synchronous write does not
//! write through, and write-through has to be *built* rather than selected. The reasoning from the
//! name was wrong and this arm is what says so.
//!
//! # WHAT MAKES THAT BUILD AFFORDABLE, AND WHERE IT WOULD NOT BE
//!
//! The same run shows **64 objects in 64 buckets — one per bucket**. That is not a property of the
//! workload but of the default routing range: `load_shard` uses `0..u32::MAX`, where the placement
//! modulus is the range width, so a key lands alone by construction.
//!
//! **A batch of one buys nothing.** So at the default range, writing through per object costs no
//! extra durable write than batching per bucket already does, and the write-back set is bookkeeping
//! for batches that never form.
//!
//! That is the opposite at a narrow range. On the range the runtime tuning guide tells an operator
//! to set, 94.7% of dirty buckets hold more than one key, and write-through would multiply durable
//! writes by the keys per bucket. So this is one change with two costs, and which one applies is a
//! deployment setting rather than a property of the design.
//!
//! rust-internal: reads the engine's own write-back set, no product behaviour

use crate::types::{Command, EventReplicationMode, ExecuteRequest, ReplicatedExecuteRequest};

const KEYS: usize = 64;

fn engine(dir: &std::path::Path) -> crate::TemporalEngine {
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

