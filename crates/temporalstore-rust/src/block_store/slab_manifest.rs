// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Slab manifest load/rebuild/reconcile/persist + slab descriptor maintenance, extracted from block_store.rs.

use super::*;
use super::slab_ids::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Every `write(2)` the manifest file is handed, counted for the life of the process.
///
/// COUNTED RATHER THAN TIMED, for the reason `directory_fsyncs` is: how many syscalls one persist
/// costs is the shape itself, and it reads the same number on a loaded box as on an idle one.
///
/// THE COUNTER SITS UNDER THE BUFFER, ON THE FILE. `serde_json` writes a JSON document in
/// fragments -- a key, a colon, a number, a comma -- and hands each one to the writer separately.
/// Against a bare `File` that is one syscall per fragment: measured at 8,000 slabs, two persists
/// of a 2,813,816-byte manifest cost 1,600,144 `write` calls, about 3.5 bytes each, and 97.8% of
/// the process's syscall time. A counter placed ABOVE the buffer would still read those 800,000
/// fragments per persist and so could never tell the two arrangements apart, which is the one
/// thing it exists to do.
static MANIFEST_FILE_WRITES: AtomicU64 = AtomicU64::new(0);

/// `write(2)` calls the manifest file has taken so far.
///
/// Process-global and shared by every store in the process, so a caller measuring one persist must
/// take a DELTA across it, never an absolute.
pub(crate) fn manifest_file_writes() -> u64 {
    MANIFEST_FILE_WRITES.load(Ordering::Relaxed)
}

/// A writer that counts the syscalls it actually issues.
pub(super) struct CountedFileWrites<W: Write>(pub(super) W);

impl<W: Write> Write for CountedFileWrites<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        MANIFEST_FILE_WRITES.fetch_add(1, Ordering::Relaxed);
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

pub(super) fn load_slab_manifest_at(
    root: &Path,
    tally: &mut BlockStoreReadTally,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
    let current_path = slab_manifest_path(root);
    let legacy_path = legacy_zone_manifest_path(root);
    let path = if current_path.exists() {
        current_path
    } else {
        legacy_path
    };
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let raw = read_block_store_file(&path, tally)?
        .into_bytes_charged_to_the_process_only();
    let manifest: BlockStoreSlabManifest =
        serde_json::from_slice(&raw).map_err(|err| {
            BlockStoreError::CorruptBlockEnvelope {
                block_slab_id: 0,
                offset: 0,
                reason: format!("corrupt slab manifest: {err}"),
            }
        })?;
    Ok(manifest
        .slabs
        .into_iter()
        .map(|slab| (slab.block_slab_id, slab))
        .collect())
}

pub(super) fn rebuild_slab_manifest_at(
    root: &Path,
    tally: &mut BlockStoreReadTally,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
    let mut slabs = BTreeMap::new();
    let latest = latest_slab_id_at(root)?;
    for block_slab_id in slab_ids_at(root)? {
        let path = slab_path(root, block_slab_id);
        let bytes = read_block_store_file(&path, tally)?
            .into_bytes_charged_to_the_process_only();
        let report = inspect_slab(&bytes, block_slab_id);
        slabs.insert(
            block_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: block_slab_id,
                block_slab_id,
                state: if block_slab_id == latest {
                    BlockStoreSlabState::Active
                } else {
                    BlockStoreSlabState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: report.logical_bytes,
                created_unix_ms: file_created_unix_ms(&path)
                    .or_else(|| file_modified_unix_ms(&path)),
                updated_unix_ms: file_modified_unix_ms(&path)
                    .or_else(|| file_created_unix_ms(&path)),
                first_block_id: report.first_block_id,
                last_block_id: report.last_block_id,
                readable_prefix_physical_bytes: report.readable_prefix_physical_bytes,
                // This path inspected the slab, so record the identity it was verified against;
                // leaving it empty makes the next reconcile re-read a slab this one just proved.
                verified_source_mtime_unix_ms: file_modified_unix_ms(&path)
                    .or_else(|| file_created_unix_ms(&path)),
                has_corruption: report.has_corruption,
                first_error_offset: report.first_error_offset,
                first_error: report.first_error,
            },
        );
    }
    for delayed in delayed_destroy_slab_reports_at(root)? {
        slabs
            .entry(delayed.block_slab_id)
            .and_modify(|slab| {
                slab.state = BlockStoreSlabState::DelayedDestroy;
                slab.updated_unix_ms = delayed.modified_unix_ms;
                slab.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreSlabDescriptor {
                stored_slab_id: delayed.block_slab_id,
                block_slab_id: delayed.block_slab_id,
                state: BlockStoreSlabState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_block_id: None,
                last_block_id: None,
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            });
    }
    Ok(slabs)
}

/// Re-verify every slab on every open, as before this was made skippable.
///
/// The escape hatch for a deployment that suspects its slabs: it costs the full cold-start CPU
/// again, which is the point.
fn reverify_all_slabs() -> bool {
    std::env::var("TS_REVERIFY_ALL_SLABS")
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// What one reconcile did, beyond whether it changed anything.
///
/// `slabs_skipped_reinspection` is the DENOMINATOR for the skip route. The skip is on by default
/// (`TS_REVERIFY_ALL_SLABS` unset) but needs a slab that is sealed AND already carries a
/// `verified_source_mtime_unix_ms` matching the file -- which the open that stamped it only wrote
/// out afterwards. A guard that opens the store twice therefore skips NOTHING and tests nothing;
/// it takes a third open. Reporting the count is what lets such a guard prove the route ran
/// before it asserts anything about it.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SlabManifestReconcileOutcome {
    pub changed: bool,
    pub slabs_skipped_reinspection: usize,
    /// What the slab inspection read off disk, summed from the WORKERS OWN TALLIES.
    ///
    /// Carried back rather than read off the process counter because the inspection runs on
    /// threads this function spawns: a caller sampling the process counter either side of this
    /// call would read every other store in the process along with its own.
    pub file_bytes_read: u64,
    pub file_reads: u64,
}

pub(super) fn reconcile_slab_manifest_with_disk(
    root: &Path,
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Result<SlabManifestReconcileOutcome, BlockStoreError> {
    let mut changed = false;
    let mut file_bytes_read = 0_u64;
    let mut file_reads = 0_u64;
    let live_slab_ids = slab_ids_at(root)?.into_iter().collect::<BTreeSet<_>>();
    let delayed_slabs = delayed_destroy_slab_reports_at(root)?
        .into_iter()
        .map(|report| (report.block_slab_id, report))
        .collect::<BTreeMap<_, _>>();
    let latest = live_slab_ids
        .iter()
        .next_back()
        .copied()
        .unwrap_or_default();

    // Read and hash the slabs in parallel, then apply the slab updates in the original order.
    //
    // Inspecting a slab re-verifies the sha256 of every page record in it, and this runs at EVERY
    // engine open. Measured on a live store: ~950 MB of slabs took about 70 s of a cold start
    // (13.5 MB/s, against hundreds of MB/s for sha256) with the process pinned at 99% of one core.
    // Only the reads and hashing are shared out; the update loop below is unchanged and still walks
    // `live_slab_ids` in order, so it makes the same decisions in the same sequence.
    let mut ordered_slab_ids = live_slab_ids.iter().copied().collect::<Vec<_>>();
    // Leave out the sealed slabs whose file is still exactly what their descriptor was verified
    // against. Inspecting one re-reads it and re-hashes every record in it, which is the bulk of a
    // cold open -- 30.7 s of CPU in a 32.0 s restart on this store, 96% CPU-bound -- and for a slab
    // nobody has touched it recomputes an answer already on disk.
    //
    // Deliberately narrow. Only a SEALED slab already in the manifest, not marked corrupt, already
    // in the state it should be in, whose size AND mtime both still match what was verified. The
    // active slab is always inspected: it is the one being appended to. Anything unreadable or
    // unrecorded falls through to a full inspection, so the skip can only ever be taken on evidence.
    if !reverify_all_slabs() {
        ordered_slab_ids.retain(|block_slab_id| {
            if *block_slab_id == latest {
                return true;
            }
            let Some(slab) = slabs.get(block_slab_id) else {
                return true;
            };
            if slab.has_corruption || slab.state != BlockStoreSlabState::Sealed {
                return true;
            }
            let Some(verified_mtime) = slab.verified_source_mtime_unix_ms else {
                return true;
            };
            let path = slab_path(root, *block_slab_id);
            let Ok(meta) = std::fs::metadata(&path) else {
                return true;
            };
            if meta.len() != slab.physical_bytes {
                return true;
            }
            file_modified_unix_ms(&path) != Some(verified_mtime)
        });
    }
    let slabs_skipped_reinspection = live_slab_ids.len().saturating_sub(ordered_slab_ids.len());
    let workers = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .clamp(1, 8)
        .min(ordered_slab_ids.len().max(1));
    type InspectedSlab = (u64, BlockStoreSlabReport, Option<u64>, Option<u64>, u64, u64);
    // A BATCH at a time. Fanning out over the whole list first was the same 2x, but held every
