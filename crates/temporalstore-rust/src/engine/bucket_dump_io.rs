// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bucket-dump manifest/install-marker file I/O helpers, split from engine.rs.
use super::*;

/// How much of the manifest DIRECTORY one operation read off disk, counted rather than timed.
///
/// Every manifest embeds a whole-shard index image (`index_bytes`, see
/// `create_bucket_dump_manifest`), and `list_bucket_dump_manifests_at` reads each file WHOLE,
/// parses it WHOLE, and re-serialises it WHOLE to verify its checksum -- for callers that go on to
/// read one `u64` off one of them. So the cost of asking "which dump is newest" is proportional to
/// the corpus times the number of manifests on disk, and nothing in the question is.
///
/// THREAD-LOCAL, for the reason `shard_write_guard`'s tallies are: every test in this crate shares
/// a process, and a process-global counter would attribute another test's round to this one.
thread_local! {
    static MANIFEST_DIR_LISTINGS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_FILE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_BYTES_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_READS_UNDER_GUARD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_CHECKSUM_SERIALIZES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_CHECKSUM_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_CHECKSUM_CLONES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Listings served from the memo below, and listings that had to read and verify a file.
    static MANIFEST_PARSE_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_PARSE_MISSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// What the memo COSTS: owned copies handed to callers that need a value they can keep, and
    /// the bytes of index image in them. A memo that traded 115 MB of parsing for 115 MB of
    /// copying would be no fix at all, so the copies are counted beside the reads they replace.
    static MANIFEST_OWNED_COPIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_OWNED_COPY_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_FILE_WRITES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_BYTES_WRITTEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MANIFEST_WRITES_UNDER_GUARD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Listings attributed to the source location that asked for one. 27 listings in a round is
    /// a number; 27 listings across eleven call sites is something someone can act on.
    static MANIFEST_LISTING_SITES: std::cell::RefCell<BTreeMap<String, u64>> =
        std::cell::RefCell::new(BTreeMap::new());
}

/// Every manifest-path counter at once.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BucketDumpManifestIoCounts {
    pub dir_listings: u64,
    pub file_reads: u64,
    pub bytes_read: u64,
    pub reads_under_guard: u64,
    pub checksum_serializes: u64,
    pub checksum_bytes: u64,
    pub checksum_clones: u64,
    pub parse_hits: u64,
    pub parse_misses: u64,
    pub owned_copies: u64,
    pub owned_copy_bytes: u64,
    pub file_writes: u64,
    pub bytes_written: u64,
    pub writes_under_guard: u64,
}

/// Read the manifest-path counters for THIS thread.
pub fn bucket_dump_manifest_io_counts() -> BucketDumpManifestIoCounts {
    BucketDumpManifestIoCounts {
        dir_listings: MANIFEST_DIR_LISTINGS.with(|cell| cell.get()),
        file_reads: MANIFEST_FILE_READS.with(|cell| cell.get()),
        bytes_read: MANIFEST_BYTES_READ.with(|cell| cell.get()),
        reads_under_guard: MANIFEST_READS_UNDER_GUARD.with(|cell| cell.get()),
        checksum_serializes: MANIFEST_CHECKSUM_SERIALIZES.with(|cell| cell.get()),
        checksum_bytes: MANIFEST_CHECKSUM_BYTES.with(|cell| cell.get()),
        checksum_clones: MANIFEST_CHECKSUM_CLONES.with(|cell| cell.get()),
        parse_hits: MANIFEST_PARSE_HITS.with(|cell| cell.get()),
        parse_misses: MANIFEST_PARSE_MISSES.with(|cell| cell.get()),
        owned_copies: MANIFEST_OWNED_COPIES.with(|cell| cell.get()),
        owned_copy_bytes: MANIFEST_OWNED_COPY_BYTES.with(|cell| cell.get()),
        file_writes: MANIFEST_FILE_WRITES.with(|cell| cell.get()),
        bytes_written: MANIFEST_BYTES_WRITTEN.with(|cell| cell.get()),
        writes_under_guard: MANIFEST_WRITES_UNDER_GUARD.with(|cell| cell.get()),
    }
}

/// Which source locations asked for a manifest-directory listing, and how many each took.
pub fn bucket_dump_manifest_listing_sites() -> BTreeMap<String, u64> {
    MANIFEST_LISTING_SITES.with(|sites| sites.borrow().clone())
}

/// Zero them, so a probe measures one operation rather than a process.
pub fn reset_bucket_dump_manifest_io_counts() {
    MANIFEST_DIR_LISTINGS.with(|cell| cell.set(0));
    MANIFEST_FILE_READS.with(|cell| cell.set(0));
    MANIFEST_BYTES_READ.with(|cell| cell.set(0));
    MANIFEST_READS_UNDER_GUARD.with(|cell| cell.set(0));
    MANIFEST_CHECKSUM_SERIALIZES.with(|cell| cell.set(0));
    MANIFEST_CHECKSUM_BYTES.with(|cell| cell.set(0));
    MANIFEST_CHECKSUM_CLONES.with(|cell| cell.set(0));
    MANIFEST_PARSE_HITS.with(|cell| cell.set(0));
    MANIFEST_PARSE_MISSES.with(|cell| cell.set(0));
    MANIFEST_OWNED_COPIES.with(|cell| cell.set(0));
    MANIFEST_OWNED_COPY_BYTES.with(|cell| cell.set(0));
    // A probe measures ONE operation, so it starts with nothing remembered. Clearing only ever
    // costs a re-read, which is the direction this whole memo is built to fail in.
    clear_bucket_dump_manifest_parse_memo();
    MANIFEST_FILE_WRITES.with(|cell| cell.set(0));
    MANIFEST_BYTES_WRITTEN.with(|cell| cell.set(0));
    MANIFEST_WRITES_UNDER_GUARD.with(|cell| cell.set(0));
    MANIFEST_LISTING_SITES.with(|sites| sites.borrow_mut().clear());
}

pub(super) fn note_manifest_file_write(bytes: usize) {
    MANIFEST_FILE_WRITES.with(|cell| cell.set(cell.get().saturating_add(1)));
    MANIFEST_BYTES_WRITTEN.with(|cell| cell.set(cell.get().saturating_add(bytes as u64)));
    if crate::engine::shard_write_guard::any_shard_guard_held() {
        MANIFEST_WRITES_UNDER_GUARD.with(|cell| cell.set(cell.get().saturating_add(1)));
    }
}

/// Keep the CLONE in `bucket_dump_manifest_checksum`, as the code did before the in-place
/// version. The control arm: an assertion that the clone is gone is satisfied just as well by a
/// path that stopped computing a checksum, or did not run, so an arm in the same process on the
/// same fixture has to be able to produce a non-zero clone count.
#[cfg(test)]
thread_local! {
    static CHECKSUM_CLONES_MANIFEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn checksum_clones_manifest_for_test(clone: bool) {
    CHECKSUM_CLONES_MANIFEST.with(|cell| cell.set(clone));
}

#[cfg(test)]
fn checksum_should_clone() -> bool {
    CHECKSUM_CLONES_MANIFEST.with(|cell| cell.get())
}

#[cfg(not(test))]
fn checksum_should_clone() -> bool {
    false
}

pub(super) fn bucket_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

pub(super) fn bucket_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    bucket_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

pub(super) fn bucket_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<BucketDumpManifest>, std::io::Error> {
    let path = bucket_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<BucketDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

pub(super) fn bucket_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &BucketDumpInstallMarker,
) -> PathBuf {
    bucket_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

pub(super) fn write_bucket_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &BucketDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = bucket_dump_install_marker_path(index_dir, marker);
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    // Install roll-forward recovery keys off these markers; write durably + atomically
    // so a crash cannot leave a torn/lost commit-phase marker (was a bare fs::write).
    atomic_write_bytes(&path, &bytes)
}

pub(super) fn bucket_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(BucketDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = bucket_dump_manifest_dir(index_dir, shard_id);
    let mut markers = Vec::new();
    if !dir.exists() {
        return Ok(markers);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "marker")
            .unwrap_or(false)
        {
            continue;
        }
        let path = entry.path();
        let marker = serde_json::from_slice::<BucketDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            bucket_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

pub(super) fn list_bucket_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<BucketDumpInstallMarker>, std::io::Error> {
    Ok(bucket_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

pub(super) fn interrupted_bucket_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<BucketDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, BucketDumpInstallMarker>::new();
    for marker in list_bucket_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                bucket_dump_install_phase_rank(&marker.phase)
                    > bucket_dump_install_phase_rank(&existing.phase)
                    || (bucket_dump_install_phase_rank(&marker.phase)
                        == bucket_dump_install_phase_rank(&existing.phase)
                        && marker.created_unix_ms > existing.created_unix_ms)
            })
            .unwrap_or(true);
        if replace {
            latest_by_manifest.insert(marker.manifest_id.clone(), marker);
        }
    }
    Ok(latest_by_manifest
        .into_values()
        .filter(|marker| marker.phase != "commit")
        .collect())
}

pub(super) fn remove_obsolete_bucket_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in bucket_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

pub(super) fn bucket_dump_install_phase_counts(markers: &[BucketDumpInstallMarker]) -> (usize, usize, usize) {
    let mut prepared = 0usize;
    let mut installed = 0usize;
    let mut unknown = 0usize;
    for marker in markers {
        match marker.phase.as_str() {
            "prepare" => prepared = prepared.saturating_add(1),
            "install" => installed = installed.saturating_add(1),
            _ => unknown = unknown.saturating_add(1),
        }
    }
    (prepared, installed, unknown)
}

pub(super) fn bucket_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

pub(super) fn bucket_dump_manifest_chain_issues<M>(
    manifests: &[M],
) -> Vec<BucketDumpManifestChainIssue>
where
    M: std::borrow::Borrow<BucketDumpManifest>,
{
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.borrow().manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .map(std::borrow::Borrow::borrow)
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| BucketDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

/// Manifests retained unconditionally: the NEWEST one, and nothing else.
///
/// Each manifest embeds a complete, self-contained index (`index_bytes`), and installing one
/// never walks the parent chain to reconstruct anything -- the chain is lineage metadata, not
/// a recovery path. So an ancestor is not needed to recover from its descendant.
///
/// This used to walk the whole parent chain. Because every manifest is created with its parent
/// set to the previous one, that retained every manifest ever written: `prunable` was always
/// empty, dump manifests grew without bound on disk, and the follower-cursor / raft-snapshot
/// guards were inert -- they can only hold back a manifest that would otherwise be pruned, so
/// their block counters were structurally stuck at zero.
///
/// Anything older is now prunable unless a follower cursor or raft snapshot ref pins it, which
/// is exactly what those cursors are for.
pub(super) fn retained_bucket_dump_manifest_ids<M>(manifests: &[M]) -> BTreeSet<String>
where
    M: std::borrow::Borrow<BucketDumpManifest>,
{
    // The newest manifest, PLUS any older one that is still the only dump covering some bucket.
    //
    // Reclaim advances its frontier only for buckets that have a manifest matching their current
    // generation. Keeping just the newest meant an ordinary cycle -- which dumps only the few
    // DIRTY buckets -- deleted the manifest covering everything else, and coverage collapsed to
    // those few. Seen on a live store: a round covering all 3,190 buckets let reclaim apply and the
    // log fall 806 -> 478 MB, then an ordinary cycle within the hour left `covered_slot_count: 0`
    // and a durable frontier of 0, so the log grew back and every cold start paid for it.
    //
    // Self-limiting: walking newest first, once a manifest covers everything, older ones add no
    // coverage and are pruned exactly as before.
