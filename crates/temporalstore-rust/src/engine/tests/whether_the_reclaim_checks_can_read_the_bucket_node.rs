// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN THE WAL-RECLAIM CHECKS READ THE BUCKET'S OWN DIRTY FLAG? The over-reporting direction,
//! measured at every state a dump cycle passes through.
//!
//! # WHAT THIS IS FOR
//!
//! The dirty-BUCKET question has one authority -- the bucket's own node -- and the presence
//! readers were asking a side index of dirty OBJECT KEYS instead.
//! `whether_a_bucket_can_count_its_own_dirty_objects` measures the two records agreeing on every
//! bucket in both directions and measures the node's flag to be an independent record rather than
//! a cache of the set. `which_record_the_dirty_bucket_consumers_read` moves the three readers whose
//! error costs a report or a dump.
//!
//! THE TWO WAL-RECLAIM CHECKS ARE NOT THOSE, AND THIS IS THE TEST THAT DECIDES THEM.
//! `dirty_object_count == 0` is the arm that counts a bucket as COVERED and contributing no log
//! floor. A bucket wrongly reading dirty skips that arm, cannot name a claim -- its
//! `first_dirty_wal_sequence` was cleared to 0 in the same breath as `dirty = false` -- lands in
//! `missing_bucket_generations`, and blocks the whole reclaim plan. That regression is recorded at
//! the site itself as measured: 0 of 10,666 records freed and `persistent_bytes` flat at 1,344,852
//! for ever.
//!
//! So agreement is not the bar here. ONE DIRECTION IS, and it is the opposite of the dump filter's:
//!
//!     node reads dirty where the set holds NOTHING    must be EMPTY, or reclaim must not move
//!     set holds a mark where the node reads clean     the dump filter's hazard, not this one
//!
//! # WHY A FULL DUMP IS NOT THE CASE TO MEASURE
//!
//! `clear_dumped_bucket_dirty_state` walks `manifest.bucket_ids` and nothing else, so only a
//! DUMPED bucket has `set_dirty(false)` and `first_dirty_wal_sequence = 0` applied to it, and only
//! a dumped bucket's keys are drained from the set. A dump covering every dirty bucket therefore
//! clears both records together and cannot separate them -- which is the state the agreement above
//! was measured in.
//!
//! A PARTIAL dump is the case that can. It is also the ordinary one:
//! `max_dump_buckets_per_round` exists to ask for it, and the selection is truncated to that bound
//! every round on a shard with more dirty buckets than the bound. So this measures the direction
//! after a dump of ONE bucket, with other buckets left dirty, as well as after a full one.
//!
//! The sibling path is `clear_published_object_dirty_state`, which drops a key from the set
//! unconditionally and reassigns the bucket flag only for buckets holding a PAGE of the published
//! object. A bucket whose only dirt is a non-page-backed `control_state:cpc:*` record holds no
//! page, so it is never recomputed -- and a dirty key leaving the set while the flag stays is
//! exactly the over-reporting state. That is why the figure below is taken after each phase rather
//! than once at the end.
//!
//! # WHAT IT MEASURES
//!
//! Printed per phase, and asserted per phase so a failure names the phase that produced it. The
//! figure is the one that licenses or refuses the move; the assertion is the license.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Every bucket's two records of the same fact, plus the claim it could name over the log.
fn phase_state(engine: &crate::TemporalEngine) -> BTreeMap<u32, (bool, u64, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                (
                    bucket.dirty(),
                    shard.dirty_objects.bucket_count(*routing_bucket),
                    bucket.first_dirty_wal_sequence,
                ),
            )
        })
        .collect()
}

/// The buckets whose node reads dirty while the object-keyed set holds nothing for them.
///
/// THE ONE FIGURE THIS TEST EXISTS FOR. Each such bucket is one the reclaim plan would stop
/// counting as covered, and -- because its claim was cleared alongside its flag -- one that cannot
/// name a WAL or index-log position either, so it lands in `missing_bucket_generations` and blocks
/// the plan outright.
fn over_reporting(state: &BTreeMap<u32, (bool, u64, u64)>) -> Vec<(u32, u64)> {
    state
        .iter()
        .filter(|(_bucket, (node_dirty, count, _claim))| *node_dirty && *count == 0)
        .map(|(bucket, (_node_dirty, _count, claim))| (*bucket, *claim))
        .collect()
}

