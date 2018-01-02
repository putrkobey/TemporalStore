// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index/manifest persistence + load/flush helper methods for TemporalEngine, split from engine.rs.
use super::*;

/// WHICH WAY A LOAD OF THE PERSISTED INDEX WENT.
///
/// # WHY THIS IS NOT TEST SCAFFOLDING
///
/// A reload that silently rebuilds from the WAL and one that read the index correctly END IN THE SAME
/// STATE: both leave `index_format_version` at the current constant, both serve the right answers, and
/// nothing afterwards distinguishes them. So "the reload worked" has never been a statement about
/// which code ran, and a change that moves the index's MEANING cannot otherwise say whether the path
/// it cares about was exercised at all.
///
/// This change bumps `SHARD_INDEX_FORMAT_VERSION` from 3 to 5, so EVERY EXISTING STORE'S INDEX IS
/// REFUSED ON ITS FIRST LOAD and rebuilt from the log. That is correct -- it is the whole purpose of
/// the stamp -- and it is the largest operational consequence of this change, costing whatever a full
/// replay costs on a large store. Before this counter nothing reported it: the refusal is a
/// `return Ok(None)` that the caller cannot tell from an absent index.
///
/// `a_reload_after_the_stamp_bump_replays_rather_than_reading_the_index` turns the counter into a
/// proof, and asserts the membership is right on BOTH paths -- a counter saying "replayed" beside a
/// wrong answer would be worse than no counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLoadPath {
    /// The file was there, decoded, and its stamp was current. The index was USED.
    Accepted,
    /// The file was there and decoded and its stamp was OLD, so it was refused and the caller
    /// replays. This is the arm the stamp exists for and the one nothing could see before.
    RefusedStaleStamp,
    /// There is no index file. A crash before the first compaction looks like this.
    Absent,
    /// The file was there and could not be decoded at all.
    Undecodable,
}

static INDEX_LOADS_ACCEPTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_REFUSED_STALE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_ABSENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INDEX_LOADS_UNDECODABLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// IS THIS IMAGE'S STAMP TOO OLD TO TRUST? THE COMPARISON, WRITTEN ONCE.
///
/// `index_format_version` exists because a stale index decodes CLEANLY -- the types are
/// unchanged -- while a field's MEANING has moved under it, so trusting it serves wrong data with
/// no error anywhere. The constant is therefore a claim about every door that turns stored bytes
/// into a `ShardState` somebody acts on, and it used to be enforced at exactly one of them.
///
/// MEASURED, which is why this exists: `decode_index_bytes` has six production callers and only
/// `load_index_inner` read the stamp. A stamp-13 image installed through
/// `install_bucket_dump_manifest` reached a live shard with all four counters below at ZERO -- it
/// did not skip the refusal, it never entered the code the refusal lived in. See
/// `engine::tests::the_stamp_refuses_a_legacy_sequence_map`.
///
/// Spelled as a predicate rather than copied inline at each door so the ORDER of the comparison
/// cannot drift between them: `<` and not `!=`, because a FUTURE stamp is a different question
/// (`decode_index_bytes`'s binary arm refuses that one on equality) and because the direction
/// that fails silently is a stamp that is too LOW.
pub(super) fn index_stamp_is_stale(shard: &ShardState) -> bool {
    shard.index_format_version < super::SHARD_INDEX_FORMAT_VERSION
}

/// The same question at a door that INSTALLS state, which also counts the refusal.
///
/// Counted so a refusal is distinguishable from an absence: every door here answers its caller
/// with the same "no usable index" it would give for a missing file, so without the counter
/// nothing downstream can tell "refused because stale" from "there was nothing there". The
/// report and plan paths deliberately use the bare predicate above instead -- they install
/// nothing, and counting them would make the tally mean something other than "an image was
/// refused on the way in".
pub(super) fn refuse_stale_index_stamp(shard: &ShardState) -> bool {
    if index_stamp_is_stale(shard) {
        note_index_load(IndexLoadPath::RefusedStaleStamp);
        return true;
    }
    false
}

fn note_index_load(path: IndexLoadPath) {
    let counter = match path {
        IndexLoadPath::Accepted => &INDEX_LOADS_ACCEPTED,
        IndexLoadPath::RefusedStaleStamp => &INDEX_LOADS_REFUSED_STALE,
        IndexLoadPath::Absent => &INDEX_LOADS_ABSENT,
        IndexLoadPath::Undecodable => &INDEX_LOADS_UNDECODABLE,
    };
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// (accepted, refused-stale, absent, undecodable) since the last reset.
///
/// A TUPLE AND NOT A SUM. A guard that only had the total could not tell a store that read its index
/// from one that replayed, which is the entire distinction this exists to draw.
pub fn index_load_path_counts() -> (u64, u64, u64, u64) {
    (
        INDEX_LOADS_ACCEPTED.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_REFUSED_STALE.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_ABSENT.load(std::sync::atomic::Ordering::Relaxed),
        INDEX_LOADS_UNDECODABLE.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget the counts, so a test measures its own loads rather than the suite before it.
pub fn reset_index_load_path_counts() {
    INDEX_LOADS_ACCEPTED.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_REFUSED_STALE.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_ABSENT.store(0, std::sync::atomic::Ordering::Relaxed);
    INDEX_LOADS_UNDECODABLE.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl TemporalEngine {
    pub(super) fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    /// Bytes of base index a dump of this shard would have to write, as the last completed dump
    /// left it on disk.
    ///
    /// A `stat`, not a serialize. The cadence is asked on every background round -- the embedded
    /// proxy's reclaim thread asks it once a second -- and what it needs is a SCALE, not a current
    /// answer. The file lags the live index by exactly the accrual the cadence is about to weigh,
    /// which is the quantity being bounded, and a shard that has never dumped has no file and
    /// reports 0, which leaves the configured floor deciding on its own.
    pub(super) fn base_index_bytes_on_disk(&self, shard_id: ShardId) -> u64 {
        fs::metadata(self.index_path(shard_id))
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    /// The accrual this shard's next threshold dump must have to show for itself.
    ///
    /// The ONE place a production cadence check gets its threshold, which is what makes
    /// `every_production_dump_cadence_check_reads_the_store_for_its_threshold` able to say so by
    /// reading this file: every production caller passes this, and the deployment's configured
    /// value is read here and nowhere else.
    pub(super) fn index_dump_threshold_bytes(&self, shard_id: ShardId) -> u64 {
        // The configured value is a FLOOR. Raise it so a dump does not write more than
        // INDEX_DUMP_BASE_FRACTION_DIVISOR bytes of base index per byte of index log it frees.
        crate::index_log::effective_index_dump_threshold_bytes(
            crate::storage_config::index_dump_wal_gap_bytes(),
            self.base_index_bytes_on_disk(shard_id),
            crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR,
        )
    }

    /// The single funnel through which every consumer reads the COMPLETE served index for
    /// a shard. It always returns a full, current `serialize_index`-shaped byte image of
    /// the `ShardState`, so callers never see a partial or stale index.
    ///
    /// - Shard loaded, not bulk: serialize the LIVE in-memory shard. This is the
    ///   authoritative current state and is what makes deferring the per-write base rewrite
    ///   safe -- readers reconstruct from memory rather than from the on-disk base.
    /// - Otherwise: read the on-disk base `shard-{id}.index.json`. This is the correct source
    ///   in bulk mode, where the base is deliberately frozen at the last flush anchor while
    ///   the WAL tail races ahead (serving the live tail would over-advance the manifest
    ///   replay watermark), and for a shard not currently loaded.
    pub(super) fn load_served_index_bytes(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<u8>, std::io::Error> {
        if !bulk_ingest_mode() {
            if let Some(shard) = self
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&shard_id)
            {
                return Ok(serialize_index(shard));
            }
        }
        fs::read(self.index_path(shard_id))
    }

    pub(super) fn persist_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            bucket_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        // Compact, not pretty. This document carries one summary per bucket -- thousands of them
        // -- and pretty-printing spends a newline and an indent on every field of every one. It is
        // machine-read on load and on install preflight; nobody reads it by eye at this size.
        // Whitespace is not semantic in JSON, so every reader that could parse the pretty form
        // parses this one, which is why this needs no version gate.
        let bytes = serde_json::to_vec(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        note_manifest_file_write(bytes.len());
        // The manifest is the durable reclaim watermark (wal_sequence) + recovery
        // index; WAL reclaim durably truncates the WAL based on it. It MUST be written
        // durably and atomically -- a bare fs::write left it in the page cache, so a
        // crash after the (durable) WAL reclaim but before the manifest reached disk
        // lost the records in between (the dumped index must be committed before
        // advancing the dumped-log id; both are waited on together). Atomic temp+rename
        // also prevents a torn manifest from hiding the whole listing on load.
        atomic_write_bytes(&path, &bytes)
    }

    pub(super) fn persist_bucket_dump_install_marker(
        &self,
        manifest: &BucketDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_bucket_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.wal_sequence,
            manifest.index_log_sequence,
        )
    }

    pub(super) fn persist_bucket_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        wal_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_bucket_dump_install_marker(
            &self.index_dir,
            &BucketDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                wal_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    pub(super) fn validate_bucket_dump_generation_for_install(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        let source_manifest_ids = manifest
            .source_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for existing in self.list_bucket_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || source_manifest_ids.contains(&existing.manifest_id)
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_buckets = existing.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_buckets.is_empty()
                || existing_buckets.is_empty()
                || !requested_buckets.is_disjoint(&existing_buckets);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.wal_sequence >= manifest.wal_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
