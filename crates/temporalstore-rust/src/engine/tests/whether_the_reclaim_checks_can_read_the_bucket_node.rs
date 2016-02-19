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

/// The buckets the set marks while their node reads clean -- the dump filter's hazard, reported
/// here so the two directions are never read off one number.
fn under_reporting(state: &BTreeMap<u32, (bool, u64, u64)>) -> Vec<(u32, u64)> {
    state
        .iter()
        .filter(|(_bucket, (node_dirty, count, _claim))| !*node_dirty && *count > 0)
        .map(|(bucket, (_node_dirty, count, _claim))| (*bucket, *count))
        .collect()
}

fn print_phase(label: &str, state: &BTreeMap<u32, (bool, u64, u64)>) {
    let node_dirty = state.values().filter(|(dirty, _, _)| *dirty).count();
    let set_marked = state.values().filter(|(_, count, _)| *count > 0).count();
    let over = over_reporting(state);
    let under = under_reporting(state);
    println!(
        "    {label:<34} buckets {:>3}  node.dirty {node_dirty:>3}  set-marked {set_marked:>3}  \
         OVER {:>3}  under {:>3}",
        state.len(),
        over.len(),
        under.len()
    );
    if !over.is_empty() {
        println!("        OVER-reporting (bucket, first_dirty_wal_sequence): {over:?}");
    }
    if !under.is_empty() {
        println!("        under-reporting (bucket, set count): {under:?}");
    }
}

#[test]
fn whether_the_reclaim_checks_can_read_the_bucket_node() {
    // ---- A NARROW ROUTING RANGE, so the removals' non-page-backed records route to buckets of
    // ---- their own -- the population `clear_published_object_dirty_state` cannot recompute, and
    // ---- so the one that could leave a flag standing over an empty set.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "reclaim-presence".to_string(),
        shard_uri: "local://reclaim-presence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1 on 0..{END_ROUTING_BUCKET}: {:?}",
        loaded.status
    );

    let mut phases: Vec<(String, BTreeMap<u32, (bool, u64, u64)>)> = Vec::new();

    for index in 0..KEYS {
        let key = format!("reclaim-presence-{index:04}");
        for command in [
            Command::StringSet { key: key.clone(), value: vec![b'v'; 96] },
            Command::HashSet {
                key: key.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 96],
            },
            Command::SetAdd { key: key.clone(), member: vec![b's'; 96] },
        ] {
            let out = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(out.status.ok, "fixture write {index}: {:?}", out.status);
        }
    }
    phases.push(("after the ingest".to_string(), phase_state(&engine)));

    // ---- A FULL DUMP: every dirty bucket selected, so both records are cleared together.
    let _full = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    phases.push(("after a FULL dump".to_string(), phase_state(&engine)));

    // ---- RE-DIRTY A FEW AND REMOVE TWO. The removals are what put a non-page-backed record in
    // ---- the set, routed to a bucket holding no page.
    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("reclaim-presence-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }
    for index in [2usize, 6] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("reclaim-presence-{index:04}") },
        });
        assert!(out.status.ok, "removing {index}: {:?}", out.status);
    }
    phases.push(("after re-dirtying and removals".to_string(), phase_state(&engine)));

    // ---- A PARTIAL DUMP, one bucket at a time, which is the case a full dump cannot produce:
    // ---- `clear_dumped_bucket_dirty_state` touches only the manifest's own bucket ids, so every
    // ---- OTHER dirty bucket keeps both its flag and its keys while this one loses both. Each
    // ---- round is measured, because the state that separates the two records may exist only
    // ---- between two of them.
    let dirty_now: Vec<u32> = phase_state(&engine)
        .iter()
        .filter(|(_bucket, (node_dirty, _count, _claim))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    assert!(
        !dirty_now.is_empty(),
        "[clause: regime_partial_dump_has_buckets] nothing is dirty before the partial dumps, so \
         the phase that can separate the two records never runs and the figure below is about a \
         shard that was never in the interesting state"
    );
    for (round, bucket) in dirty_now.iter().copied().enumerate() {
        let report = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: vec![bucket],
            ..Default::default()
        });
        let _ = report;
        phases.push((
            format!("after a PARTIAL dump of bucket {bucket} (round {})", round + 1),
            phase_state(&engine),
        ));
    }

    // ---- THEN ADVANCE THE LOG PAST THE MANIFEST THAT COVERS MOST BUCKETS. The floor arm's only
    // ---- observable is the frontier, and it holds a dirty bucket's floor at the sequence of the
    // ---- manifest covering it -- so if that manifest sits AT the current log position, the arm
    // ---- runs and moves nothing, and a probe reading the frontier cannot see it. Measured: with
    // ---- every bucket's newest manifest written by the last partial round, forcing a bucket dirty
    // ---- left retain(wal, index-log) at (150, 150) exactly.
    //
    // One more write and one more dump fixes that. The dump names only the bucket that write
    // dirtied, so every OTHER bucket's newest covering manifest now predates the current position
    // and a floor held at it is a figure that differs.
    let tail_key = format!("reclaim-presence-{:04}", 3);
    let out = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet { key: tail_key, value: vec![b't'; 96] },
    });
    assert!(out.status.ok, "the tail write failed: {:?}", out.status);
    let tail_dirty: BTreeSet<u32> = phase_state(&engine)
        .iter()
        .filter(|(_bucket, (node_dirty, _count, _claim))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    let _tail_dump = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    phases.push((
        "after a tail write and its dump".to_string(),
        phase_state(&engine),
    ));

    // ---- AND A WAL RECLAIM PLAN, which is the consumer under discussion. Run last so the figure
    // ---- above is not taken on a shard this call has already changed.
    let reclaim = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
    phases.push(("after a reclaim plan".to_string(), phase_state(&engine)));

    println!();
    println!("  {KEYS} keys on a {}-bucket routing range", END_ROUTING_BUCKET + 1);
    println!();
    for (label, state) in &phases {
        print_phase(label, state);
    }
    println!();
    println!("    reclaim plan: blockers {:?}", reclaim.blocker_reasons);
    println!(
        "    reclaim plan: missing_bucket_generations {:?}",
        reclaim.missing_bucket_generations
    );
    println!();

    // ---- AND WHICH RECORD THE TWO CHECKS ACTUALLY READ. The figures above are the LICENCE: they
    // ---- say the state that would break this does not arise. They are also why no ordinary
    // ---- assertion can say which record the plan consulted -- while the two agree, a plan reading
    // ---- either one satisfies any comparison against the truth. So force ONE bucket's node dirty
    // ---- over an empty set and read the plan again.
    //
    // The shard is fully dumped at this point: every node reads clean, the set is empty, and the
    // plan above found every bucket covered with the floor at the CURRENT log position. Forcing one
    // flag changes exactly one thing, and the two arms answer it differently, so each is pinned on
    // its own below. Taken AFTER `phases` is captured, so the measurement above is untouched by it.
    // A BUCKET THE NEWEST MANIFEST DOES NOT NAME, so the manifest covering it predates the current
    // log position and a floor held at it is a figure that differs from the settled one.
