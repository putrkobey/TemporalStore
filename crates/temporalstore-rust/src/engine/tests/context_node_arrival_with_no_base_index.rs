// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THIRD DOOR: a load with NO BASE INDEX FILE AT ALL, which reads no stamp.
//!
//! # WHY THIS DOOR IS NOT COVERED BY THE OTHER TWO
//!
//! `persistence::load_index_inner` compares `index_format_version` against the constant on its
//! `Ok` arm, having read the base index file. Its `Err(_)` arm -- the file is absent or unreadable
//! -- takes `ShardState::default()` and **never reaches that comparison**. Then `fold_index_log
//! _deltas` runs with `base_anchor` 0, so it folds EVERY record in the index log rather than a
//! suffix, and each record's `key_states` go through `apply_key_states` ->
//! `apply_key_state_field(&mut shard.context_nodes, ...)`.
//!
//! So a stamp check -- on the base index, where there is one, or on the dump-manifest image,
//! where that is added -- cannot reach this path. Nothing is stamped here, because nothing is
//! read.
//!
//! # WHAT THIS FILE MEASURES, AND WHAT IT DOES NOT CLAIM
//!
//! The paragraph above describes what the FOLD-AWARE loader does with an absent base. It is not a
//! claim that the load driven below takes that arm, and this file must not be read as making one:
//! `load_shard` on the single-barrier default reaches `load_index_base_only`, which passes
//! `fold_deltas: false`. #2152 measures both halves with the fold probe -- the fold, driven
//! directly, DOES deliver the entry, and the shipped path never asks it -- so the door is held
//! shut by the recovery arm rather than by anything about this map.
//!
//! Every assertion here is therefore about an OUTCOME: what the fixture put on disk, which load
//! path was taken (by the presence of a file, not by a counter), and whether the entry is in the
//! map afterwards. None of them rests on the fold having declined the record rather than never
//! having been asked, because from here those two are indistinguishable and #2152 is what tells
//! them apart.
//!
//! # WHY THIS IS DRIVEN AND NOT ARGUED
//!
//! There is a tidy argument that this door cannot carry a `context_nodes` entry: the key-state
//! capture arrived in `49fe2f933` (2026-08-15), two months after the production writer was removed
//! in `0ed2bb7e9` (2026-06-16), so for a record to carry an entry the map must already have been
//! non-empty, which needs an accepted index, which the stamp refuses. That is a CIRCULARITY
//! ARGUMENT ABOUT A POPULATION, and it is inference rather than measurement: it reasons about what
//! could have been written rather than about what the code does with what it is given. The thing
//! it would license -- "guard the stamped doors and the map is removable" -- is load-bearing on a
//! removal, so it is measured here instead.
//!
//! # THE FIXTURE, AND THE THREE CONTROLS THAT MAKE A VERDICT MEAN SOMETHING
//!
//! One fixture, built by [`build_fixture`]:
//!
//!   1. `StringSet` on the key, giving it a live page;
//!   2. a `context_nodes` entry planted under the SAME key, as a clone of that live string entry
//!      -- `ElementEntry` has no public constructor, and an address no slab holds would be
//!      dropped on load for a reason that is not the door;
//!   3. `CommonExpire` on the key. That arms a deadline where there was none, which is one of the
//!      two halves `delta_key_state_change` fires on, so `capture_key_states` runs and writes a
//!      blob for the key -- and the blob carries `context_nodes`, because the map holds it. It is
//!      `CommonExpire` and not `StringSetEx` on purpose: a value-replacing write would relocate
//!      the string's page and leave the planted entry naming a superseded address, which is a
//!      second variable.
//!
//! The controls:
//!
//!   * CONTROL 1, THE FIXTURE REACHED THE LOG. The index-log file on disk contains the literal
//!     `context_nodes`. Without this, a no-arrival verdict is equally well explained by a capture
//!     that never fired, and the two read identically from the far end.
//!   * CONTROL 2, THE PLANT IS DELIVERABLE AND READABLE. A twin directory holding the SAME
//!     fixture, with its base index flushed, is loaded and the entry is there. That is the open
//!     path, and it is what makes a no-arrival through the third door a fact about the door.
//!   * CONTROL 3, THIS REALLY IS THE THIRD DOOR. The base index file is asserted ABSENT at the
//!     moment of the load.
//!
//! CONTROL 3 IS A FILE CHECK AND NOT A COUNTER CHECK, DELIBERATELY. `index_load_path_counts` is
//! process-global, so a second test loading a shard on another thread lands in the same four
//! numbers and nothing can attribute them back. The absence of the file IS the door, it is local
//! to this test, and no other test can perturb it. The counters are printed as a diagnostic and
//! asserted on only in the direction that cannot be faked by someone else's load.

#![allow(clippy::all)]
use super::*;

/// The key the string, the planted entry and the deadline all share.
const KEY: &str = "ctx:node:legacy";

/// Long enough that nothing below can pass by the key having actually expired.
const HOUR_MS: u64 = 3_600_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn base_index_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes").join("shard-7.index.json")
}

/// Every index-log file for shard 7, whatever suffix and layout this build writes.
///
/// A RECURSIVE WALK OF THE LOG ROOT AND NOT A NAME PATTERN IN THE INDEX DIRECTORY. The first
/// version of this looked for `shard-7.indexlog.*` directly under the index directory and found
/// nothing, because `with_local_dirs` puts the log store at `index_dir/indexlogs` -- and the
/// suffix has moved once already (`jsonl` -> `bin`), with sealed segments carrying a span in the
/// name. So this takes every file under the log root and lets the content check below decide,
/// which cannot go stale on either the directory or the naming.
fn index_log_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(at: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(at) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
