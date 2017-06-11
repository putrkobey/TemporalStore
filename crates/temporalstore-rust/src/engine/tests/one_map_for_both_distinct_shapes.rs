// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TWO DISTINCT-COUNT MAPS ARE ONE MAP, AND THE STAMP THAT PAYS FOR IT.
//!
//! `control_state_change_sketch` has left the stored image and `control_state_changes`' bucket
//! value has become [`DistinctBucket`], so `SHARD_INDEX_FORMAT_VERSION` moved 14 -> 15. This file
//! holds the four things that bump owes.
//!
//! # WHAT THE STAMP IS FOR, AND WHY A TEST HAS TO PROVE THE REFUSAL
//!
//! `decode_index_bytes` checks the version BEFORE decoding, deliberately: the payload is
//! addressed by field order, and decoding it against a different struct shape "does not error, it
//! produces a plausible and wrong `ShardState`". So the refusal is the guard, and a bump whose
//! refusal is untested is a bump that might not refuse.
//!
//! [`a_pre_bump_index_is_refused_rather_than_read`] drives a stale-stamped image through the real
//! load path and asserts `RefusedStaleStamp`.
//! [`an_index_at_the_new_stamp_is_accepted`] is the positive control -- the same fixture at 15 is
//! accepted and its data comes back -- without which the refusal arm passes just as well when the
//! fixture never reached the load path.
//!
//! # AND A SECOND, INDEPENDENT LAYER THAT THIS PARTICULAR CHANGE HAPPENS TO HAVE
//!
//! [`the_pre_bump_bucket_shape_cannot_be_read_as_the_new_one`] asks what would happen if the stamp
//! were NOT there: it takes a current index, rewrites one bucket back into the pre-bump shape --
//! a bare member array where an enum now lives -- and asserts the decode FAILS.
//!
//! That is worth measuring rather than assuming, and it cuts both ways. It means the stamp is
//! belt-and-braces for THIS change rather than the only thing standing between an old image and a
//! plausible-and-wrong state, which is a weaker claim than the general argument for the stamp and
//! an honest one. It does not make the stamp optional: the sketch map leaving is a field no reader
//! claims, `ShardState` sets no `deny_unknown_fields`, and that half WOULD be read silently as
//! "there were never any sketched buckets".
//!
//! # THE REPLAY CONSEQUENCE
//!
//! Every index written before 15 is refused on load, counted as `RefusedStaleStamp`, and the shard
//! rebuilds from its WAL. Slower and correct.
//!
//! # WHAT THIS CHANGE IS NOT
//!
//! It is not a bug fix. [`DistinctBucket`]'s own doc records why, and
//! [`the_old_overlap_state_would_have_counted_correctly`] drives it: a bucket present in both of
//! the old maps would still have produced the right distinct count, because an `Hll` and a
//! `BTreeSet` both absorb a duplicate. The merge deletes a map and makes an invariant
//! unbreakable; it repairs nothing.

#![allow(clippy::all)]
use super::*;

use crate::engine::hll::{count_changes, record_change, DistinctBucket, DISTINCT_SKETCH_THRESHOLD};
use crate::engine::persistence::{index_load_path_counts, reset_index_load_path_counts};

const SHARD: ShardId = 4093;
const KEY: &str = "ctl:change:tenant-9";
const BUCKET_MS: u64 = 1_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn base_index_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes")
        .join(format!("shard-{SHARD}.index.json"))
}

/// A store holding one exact change bucket with three members, flushed.
fn build_fixture(dir: &std::path::Path) {
    let engine = engine_at(dir);
    engine.load_shard(SHARD);
    for value in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        let answer = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ControlStateChangeAdd {
                key: KEY.to_string(),
                timestamp_ms: BUCKET_MS,
                value,
                precision_ms: None,
                ttl_ms: None,
            },
        });
        assert!(answer.status.ok, "a change write failed: {}", answer.status.message);
    }
    engine.flush_shard_index(SHARD);
    drop(engine);
}

/// How many distinct members the shard holds for `KEY`, and in which representation.
fn held(engine: &TemporalEngine) -> Option<(u64, bool)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD)?;
    let bucket = shard.control_state_changes.get(KEY)?.get(&BUCKET_MS)?;
    Some((
        bucket.distinct_hint(),
        matches!(bucket, DistinctBucket::Sketch(_)),
    ))
}

/// A PRE-BUMP INDEX IS REFUSED BY THE STAMP, NOT READ.
#[test]
fn a_pre_bump_index_is_refused_rather_than_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());

    let path = base_index_path(dir.path());
    let bytes = std::fs::read(&path).expect("a flushed base index exists");
    let mut restored = crate::engine::decode_index_bytes(&bytes).expect("the base index decodes");
    assert_eq!(
        restored.index_format_version,
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        "denominator: the index just written must carry the CURRENT stamp, or staling it below \
         changes nothing"
    );
    restored.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION - 1;
    std::fs::write(&path, crate::engine::encode_index_bytes(&restored)).expect("write");

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(SHARD);
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    println!(
        "  stamp {} against {} -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION - 1,
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        stale_n > 0,
        "an index stamped below {} must be counted as a STALE-STAMP refusal; got stale {stale_n} \
         decode {decode_n} absent {absent_n} accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    // Refused and REBUILT, not refused and lost: replay re-derives the three members.
    let after = held(&reloaded);
    println!("  after the refusal, replay rebuilt: {after:?}");
    assert_eq!(
        Some((3, false)),
        after,
        "the refusal must fall back to WAL replay and re-derive the bucket exactly -- three \
         distinct members, still EXACT. A refusal that loses the data is not the trade this bump \
         was approved on."
    );
}

/// THE POSITIVE CONTROL: the same fixture at the new stamp is accepted and its data comes back.
#[test]
fn an_index_at_the_new_stamp_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());

    reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(SHARD);
    let (accepted_n, stale_n, absent_n, decode_n) = index_load_path_counts();
    println!(
        "  stamp {} -> stale {stale_n}, decode {decode_n}, absent {absent_n}, accepted {accepted_n}",
        crate::engine::SHARD_INDEX_FORMAT_VERSION
    );
    assert!(
        accepted_n > 0,
        "an index at the current stamp must be ACCEPTED, or the refusal arm is measuring a \
         fixture that never reached the load path; got stale {stale_n} decode {decode_n} absent \
         {absent_n} accepted {accepted_n}"
    );
    assert_eq!(
        Some((3, false)),
        held(&reloaded),
        "the accepted index must deliver the bucket it carries"
    );
}

/// WHAT THE PRE-BUMP BUCKET SHAPE DOES AGAINST THE NEW ONE, WITH THE STAMP TAKEN OUT OF THE WAY.
#[test]
fn the_pre_bump_bucket_shape_cannot_be_read_as_the_new_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path());
    let bytes = std::fs::read(base_index_path(dir.path())).expect("a flushed base index exists");
