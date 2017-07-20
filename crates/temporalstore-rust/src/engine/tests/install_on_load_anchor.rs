// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What `install_latest_manifest_if_newer_on_load` does to the durable index when it is about to
//! refuse the load anyway.
//!
//! The function decides whether a durable dump manifest is newer than the served index, and
//! installs it if so -- and installing overwrites `shard-{id}.index.json`. It read its
//! `served_anchor` through `load_index`, which is documented as the TOLERANT wrapper: a corrupt
//! served-index delta comes back as `None`. `None` became an anchor of 0, an anchor of 0 made
//! every manifest look newer, and the install overwrote the durable index. Only then did
//! `load_shard_with` reach its own `load_index_checked` and refuse the load -- so the load failed
//! AFTER destroying what it was refusing to read, in precisely the case where that file is the
//! last good copy.
//!
//! This is the `TS_WAL_LEGACY_RECOVERY` arm, which nothing in a shipped configuration selects.
//! It is the emergency escape hatch an operator flips in the field, which makes its population
//! exactly the shards already in trouble -- the ones most likely to have a corrupt delta.
//!
//! Driven directly rather than through `load_shard`, because selecting that arm means setting a
//! process-wide environment variable and the gate runs every test in one process. The function is
//! the unit under test either way.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

/// The index-log file for `SHARD`, whichever of the two names this store uses. The log lives
/// under `index_dir/indexlogs`, not `index_dir` itself.
fn index_log_file(index_dir: &std::path::Path) -> std::path::PathBuf {
    let root = index_dir.join("indexlogs");
    let framed = root.join(format!("shard-{SHARD}.indexlog.bin"));
    if framed.exists() {
        return framed;
    }
    root.join(format!("shard-{SHARD}.indexlog.jsonl"))
}

/// A durable index holding two keys, a manifest that predates the second, and a corrupt delta.
///
/// The manifest is created BEFORE the second key so that installing it would visibly roll the
/// durable index backwards -- if the two were byte-identical, an install that happened anyway
/// would leave no trace and this test would pass without measuring anything.
#[test]
fn a_refused_install_on_load_leaves_the_durable_index_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "before-manifest".to_string(),
