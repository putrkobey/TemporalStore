// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index install/recovery + expiry sweep + page compaction methods for TemporalEngine, split from engine.rs.
use crate::engine::timestamped_series::TimestampedSeries as _;
use super::*;

/// OBSERVATION SEAM: was the shard-table write guard still held when a round PUBLISHED its
/// durable index?
///
/// `the_compaction_flush_stays_inside_its_write_guard` counts served-index ENCODES taken under
/// the guard, and the encode is only the first third of the flush. Dropping the guard between the
/// encode and `persist_index_bytes` leaves that count untouched -- the encode still happened
/// inside the region -- while opening the entire window the invariant exists to close: from the
/// moment the guard drops, `storage_lifecycle_plan` can take its own read lock, derive stale
/// slabs from a live set that no longer names the slabs this round vacated, and reclaim them,
/// while the durable index on disk is still the PRE-compaction one that does name them.
/// Compaction advances no `applied_wal_sequence` and emits no record, so nothing replays that
/// back.
///
/// Measured rather than asserted structurally because the guard is an RAII value: whether it is
/// still alive at a given statement is a fact about the generated code, and reading the
/// write-guard depth at the publish point is the only way to ask.
#[cfg(test)]
thread_local! {
    static FLUSH_PUBLISHED_UNDER_GUARD: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

/// Record the write-guard depth at the moment the round publishes. Called once per round.
#[cfg(test)]
fn note_compaction_flush_publish() {
    let held = super::shard_write_guard::held();
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.set(Some(held)));
}

/// `None` until a round has published since the last reset; then whether the guard was held.
#[cfg(test)]
pub(crate) fn compaction_flush_published_under_guard_for_test() -> Option<bool> {
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.get())
}

#[cfg(test)]
pub(crate) fn reset_compaction_flush_publish_observation_for_test() {
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.set(None));
}

/// WHERE AN EXPIRY ROUND'S SHARD-TABLE WRITE HOLD GOES, phase by phase.
///
/// The sibling logged-deletion path has had `DeleteDropGuardNanos` since #1907, and it is what
/// made "the cache sweep is most of the hold" a number instead of a guess. This sweep had no such
/// seam, so the same question could not be asked of it at all -- the round reports what it
/// REMOVED, never what it HELD.
///
/// THE RESIDUAL IS THE POINT, and it is why `total` is timed by its own clock rather than summed
/// from the rows. `unattributed` is the hold minus every phase below it, so work that belongs to
/// no phase -- or that drifts out of one -- lands there and is visible. A table that summed its
/// own rows would balance no matter what the round actually did.
///
/// Every row is nanoseconds, process-wide, and reset by the reader immediately before the round it
/// measures; the suite that reads them runs `--test-threads=1`.
pub(crate) struct ExpiryGuardNanos {
    /// The whole hold, from the moment the guard is acquired to the moment before it drops.
    pub total: std::sync::atomic::AtomicU64,
    /// `ensure_expiry_order` + both `due_window` reads: choosing what the round will expire.
    pub select: std::sync::atomic::AtomicU64,
    /// `delete_record`, once per due key: the in-memory removal from the shard index. This is the
    /// only part of the removal that actually needs the shard guard.
    pub delete: std::sync::atomic::AtomicU64,
    /// The record-cache pass. ONE `invalidate_records_all_batched` per round; it is handed
    /// `&MultiLayerCache` and a key slice and touches NOTHING on the shard.
    pub invalidate: std::sync::atomic::AtomicU64,
    /// The per-key WAL tombstone loop, including the one mirror lookup it takes first.
    pub wal_append: std::sync::atomic::AtomicU64,
    /// Building the checkpoint the unlocked flush is handed: once per round.
    pub checkpoint: std::sync::atomic::AtomicU64,
}

impl ExpiryGuardNanos {
    const fn zeroed() -> Self {
        Self {
            total: std::sync::atomic::AtomicU64::new(0),
            select: std::sync::atomic::AtomicU64::new(0),
            delete: std::sync::atomic::AtomicU64::new(0),
            invalidate: std::sync::atomic::AtomicU64::new(0),
            wal_append: std::sync::atomic::AtomicU64::new(0),
            checkpoint: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn cells(&self) -> [&std::sync::atomic::AtomicU64; 6] {
        [
            &self.total,
            &self.select,
            &self.delete,
            &self.invalidate,
            &self.wal_append,
            &self.checkpoint,
        ]
    }

    pub(crate) fn reset(&self) {
        for cell in self.cells() {
            cell.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// `(total, select, delete, invalidate, wal_append, checkpoint)` in nanoseconds.
    pub(crate) fn read(&self) -> (u64, u64, u64, u64, u64, u64) {
        let [total, select, delete, invalidate, wal_append, checkpoint] = self
            .cells()
            .map(|cell| cell.load(std::sync::atomic::Ordering::Relaxed));
        (total, select, delete, invalidate, wal_append, checkpoint)
    }

    /// The hold this round did not attribute to any phase above. Saturating, so an ordering
    /// surprise reads as zero rather than as an enormous number.
    pub(crate) fn unattributed(&self) -> u64 {
        let (total, select, delete, invalidate, wal_append, checkpoint) = self.read();
        total
            .saturating_sub(select)
            .saturating_sub(delete)
            .saturating_sub(invalidate)
            .saturating_sub(wal_append)
            .saturating_sub(checkpoint)
    }

    fn add_since(&self, cell: &std::sync::atomic::AtomicU64, since: std::time::Instant) {
        cell.fetch_add(
            since.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

pub(crate) static EXPIRY_GUARD_NANOS: ExpiryGuardNanos = ExpiryGuardNanos::zeroed();

/// The same predicate the seam reads, exposed so a test can show it is capable of saying `false`.
#[cfg(test)]
pub(crate) fn shard_write_guard_held_for_test() -> bool {
    super::shard_write_guard::held()
}

/// What an expiry round hands to its flush: a description of what the round REMOVED, or the
/// whole served index.
///
/// Two shapes rather than one because the whole-index write is the measurement arm's subject --
/// see `expiry_index_flush_whole`. Production only ever builds the delta.
enum ExpiryIndexCheckpoint {
    /// O(what changed): one index-log delta record naming the keys this round removed.
    Delta(Box<ExpiryIndexDelta>),
    /// O(shard): the entire served index, re-encoded and rewritten.
    Whole(Box<ShardState>),
}

/// The delta record an expiry round writes, built under the shard write guard and appended after
/// it drops.
struct ExpiryIndexDelta {
    /// The page items the record carries. Always empty: the round's deletes leave no page for a
    /// covered key, and an empty list against a covered key is how the fold spells a removal.
    /// Kept as a field because it is what the record's wire shape has, and because a future
    /// checkpoint that DOES have pages to name would fill it rather than grow a second path.
    items: Vec<crate::index_log::IndexItem>,
    /// One blob per covered key, carrying no map fields: the fold reads an absent field as a
    /// removal, so these are tombstones across every per-key map, `expires_at_ms` included.
    key_states: Vec<serde_json::Value>,
    /// The WAL sequence this checkpoint reflects -- anchored AFTER the round's tombstones, so
    /// folding the anchor and folding the deletions are the same act.
    applied_wal_sequence: Option<u64>,
}

impl TemporalEngine {
    /// Install a shard index received from elsewhere -- a snapshot, a checkpoint, a restore.
    ///
    /// TAKES THE ROUTING RANGE, AND IT IS NOT OPTIONAL. An index names buckets, and which bucket a
    /// key belongs to is `start + hash(key) % (end - start + 1)` -- so an index without its range is
    /// a set of bucket numbers with no way to know what produced them. Installing one and letting
    /// the load pick a range is how a store ends up with every block outside every per-bucket sweep,
    /// readable and invisible.
    ///
    /// The range therefore has to TRAVEL with the bytes, and this signature is what forces every
    /// sender to carry it: a caller that cannot say which range the index was built on is a caller
    /// whose payload is missing a field, and the compiler says so here rather than a load failing
    /// somewhere else later.
    ///
    /// Writes the stamp BEFORE the index, so a crash between the two leaves a stamp with no index --
    /// which reads as "no on-disk state", the harmless order. The reverse leaves an index with no
    /// stamp, which is refused.
    pub fn install_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
        routing_range: crate::engine::routing_range_stamp::RoutingRangeStamp,
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        crate::engine::routing_range_stamp::write_routing_range_stamp(
            &self.index_dir,
            shard_id,
            routing_range,
        )?;
        fs::write(self.index_path(shard_id), bytes)
    }

    pub fn storage_recovery_report(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let mut report = self.storage_recovery_report_without_boundary(shard_id);
        report.boundary = self.storage_recovery_boundary_report(shard_id);
        report.slab_integrity =
            storage_slab_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    /// The reclaim candidates, built once from the narrow view and once from the full report.
    ///
    /// Same selection function, two sources for the tally it reads. The narrow view leaves the
    /// read-dependent fields at zero, so this is the check that the planner never looked at
    /// them.
    #[cfg(test)]
    pub(crate) fn reclaim_candidates_two_ways_for_test(
        &self,
        shard_id: ShardId,
    ) -> (Vec<StorageReclaimCandidate>, Vec<StorageReclaimCandidate>) {
        let live = self
            .live_block_slab_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let stale = self
            .block_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live.contains(id))
            .collect::<BTreeSet<_>>();
        let narrow = storage_reclaim_candidates_from_slab_reports(
            &self.storage_reclaim_slab_reports(shard_id),
            &stale,
        );
        let full = storage_reclaim_candidates_from_slab_reports(
            &self.storage_recovery_report(shard_id).block_slab_live_reports,
            &stale,
        );
        (narrow, full)
    }

    /// Each dirty bucket's first undumped write sequence, for the test that pins it.
    #[cfg(test)]
    pub(crate) fn first_dirty_sequences_for_test(&self, shard_id: ShardId) -> Vec<(u32, u64)> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let mut out = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.first_dirty_wal_sequence))
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    /// The per-slab live/stale tally the reclaim planner reads, without the whole-store scan.
    ///
    /// `storage_reclaim_candidates_from_slab_reports` consumes seven fields off each of these:
    /// the slab id, its physical bytes and page count, its live page refs and live physical
    /// bytes, and the two figures derived from those. Not one of them needs the page itself --
    /// a page's physical size is in the address that names it. The recovery report supplied
    /// them anyway, and supplied them by reading every live page off the block store to fill
    /// in `live_logical_bytes`, which the planner never reads.
    ///
    /// The fields that DO require the read -- `live_logical_bytes`, `readable_live_page_refs`,
    /// `unreadable_live_block_refs` -- are left at zero here, so this is not a drop-in for the
    /// report: it is the planner's view, and `the_reclaim_planner_sees_the_same_candidates`
    /// pins it to the answer the report produced.
    pub(super) fn storage_reclaim_slab_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<StorageRecoverySlabLiveReport> {
        // Counted by header walk, not `slab_reports()`. That function calls
        // `decode_block_record` on every record in every slab -- a CRC32C verify and a
        // decompress each -- which is a full integrity pass over the whole store, and the
        // selection below reads two fields out of it: the slab's size and its block count.
        // Measured at 32,000 records: 12.95 ms against 0.37 ms, 35x, with identical counts.
        //
        // `logical_bytes` is left at zero here for the same reason as the read-dependent
        // fields: `storage_reclaim_candidates_from_slab_reports` does not read it, and
        // `the_reclaim_planner_sees_the_same_candidates` is what holds that true.
        let block_slab_counts = self.block_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        // THE MAINTAINED TALLY FIRST, AND THE WALK ONLY IF THERE ISN'T ONE.
        //
        // Everything below builds a per-slab live/stale view out of `live_page_refs` and
        // `live_physical_bytes`, and those two are exactly what the shard now keeps a running
        // total of. Taking them from the tally makes this round cost the number of SLABS; walking
        // for them costs the number of live BLOCKS, on a loop that ticks every thirty seconds per
        // shard for the life of the process.
        //
        // The fallback is not decoration. A shard whose tally has never been derived reports
        // `is_ready() == false`, and an empty tally is indistinguishable from a shard holding no
        // live pages at all -- which would make every slab look like pure garbage. So an
        // underived tally costs the old cost rather than producing a confident wrong answer.
        //
        // `live_object_count` and `live_routing_bucket_count` are left at zero on the maintained
        // path, joining the read-dependent fields already documented above:
        // `storage_reclaim_candidates_from_slab_reports` reads neither, and
        // `the_reclaim_planner_sees_the_same_candidates` is what holds that true.
        let maintained = shards
            .get(&shard_id)
            .filter(|shard| shard.bucket_index.block_slab_live.is_ready())
            .map(|shard| shard.bucket_index.block_slab_live.iter().collect::<Vec<_>>());
        // ENTRIES, NOT ADDRESSES, AND THAT IS NOT A NEW COST. `collect_live_block_addresses` is
        // `collect_live_block_entries(..).map(|entry| entry.address)`, so the entries were already
        // being materialized and thrown away; keeping them drops a second `Vec` rather than adding
        // work. What the key buys is `live_routing_bucket_count`, which the address alone can no
        // longer answer.
        let live_entries = match maintained {
            Some(_) => Vec::new(),
            None => shards
                .get(&shard_id)
                .map(collect_live_block_entries)
                .unwrap_or_default(),
        };
        let mut reports = block_slab_counts
            .iter()
            .map(|(block_slab_id, physical_bytes, block_count)| {
                (
                    *block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: *block_slab_id,
                        physical_bytes: *physical_bytes,
                        block_count: *block_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if let Some(maintained) = maintained {
            for (block_slab_id, tally) in maintained {
                let slab_report =
                    reports
                        .entry(block_slab_id)
                        .or_insert(StorageRecoverySlabLiveReport {
                            block_slab_id,
                            ..StorageRecoverySlabLiveReport::default()
                        });
                slab_report.live_block_refs = tally.block_refs;
                slab_report.live_physical_bytes = tally.bytes;
            }
        }
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for live_entry in &live_entries {
            let address = &live_entry.address;
            let slab_report = reports.entry(address.block_slab_id()).or_insert(
                StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id(),
                    ..StorageRecoverySlabLiveReport::default()
                },
            );
            slab_report.live_block_refs = slab_report.live_block_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length());
            {
                let objects = live_object_ids.entry(address.block_slab_id()).or_default();
                objects.insert(expected_live_block_object_id(shard_id, live_entry));
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = live_entry.filed_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id())
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
        }
        reports
            .into_values()
            .map(|mut report| {
                report.stale_block_estimate =
                    report.block_count.saturating_sub(report.live_block_refs);
                report.live_ref_density_basis_points = if report.block_count == 0 {
                    0
                } else {
                    report.live_block_refs.saturating_mul(10_000) / report.block_count
                };
                report
            })
            .collect()
    }

    /// The object-lifecycle view on its own, without the whole-store scan around it.
    ///
    /// The recovery report produces this field as a by-product of reading EVERY live page off
    /// disk, which is how it counts the readable ones. Two callers on the maintenance path want
    /// nothing else from that report, so they were paying a full-store read to get it -- on a
    /// loop that runs every thirty seconds, for the life of the process.
    ///
    /// Nothing here reads a page. Every count comes from the shard's own maps, the ownership
    /// validation and the slab reports, which is all the field was ever made of:
    ///
    /// | at 32,000 live pages | the report | this |
    /// |---|---|---|
    /// | wall time | ~840 ms | ~45 ms |
    /// | pages read from the block store | 32,000 | 0 |
    ///
    /// `object_lifecycle_snapshot_matches_the_recovery_report` holds the two to the same answer,
    /// so a change to either that separates them fails there rather than in a shipped round.
    /// The snapshot, and the live page count, for the tests that hold it to the report.
    #[cfg(test)]
    pub(crate) fn storage_object_lifecycle_snapshot_for_test(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        self.storage_object_lifecycle_snapshot(shard_id)
    }

    #[cfg(test)]
    pub(crate) fn live_block_count_for_test(&self, shard_id: ShardId) -> usize {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(|shard| collect_live_block_addresses(shard).len())
            .unwrap_or_default()
    }

    pub(super) fn storage_object_lifecycle_snapshot(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        // Header walk, not a full decode of every record -- see `storage_reclaim_slab_reports`.
        // Only the per-slab block count is read below.
        let block_slab_counts = self.block_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StorageObjectLifecycleReport::default();
        };
        // ONE walk, three consumers.
        //
        // This used to walk the shard THREE times over identical, immutable state: once inside
        // `validate_shard_block_ownership`, once inside `storage_object_lifecycle_report`, and once
        // in `collect_live_block_addresses`. Each call to `collect_live_block_entries` materializes
        // every live page in the shard into a fresh Vec, and measured at 4,000 objects this single
        // function accounted for 3.0x the shard -- the largest piece of `apply_storage_lifecycle`,
        // itself 12.0x (`what_each_plan_call_walks`).
        //
        // Hoisting is safe HERE in a way it is not across a maintenance round: the read lock is
        // held for all three, `&ShardState` is immutable throughout, and nothing between them can
        // change what a walk would find. Elsewhere in the round the stages genuinely mutate
        // between calls, which is why this is a hoist and not a cache.
        //
        // Order matters only because the report CONSUMES the entries: borrow for the two cheap
        // derivations first, then hand the Vec over last.
        let entries = collect_live_block_entries(shard);

        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let ownership = validate_bucket_ownership_index_from_entries(
            shard_id,
            shard,
            &entries,
            start_routing_bucket,
            end_routing_bucket,
        );

        // stale_object_ids is the per-slab shortfall of live refs against the slab's own page
        // count, summed. An address naming a slab the store has no report for contributes
        // nothing (the report path gives it block_count 0, so its shortfall saturates to 0).
        let mut live_block_refs_by_slab = BTreeMap::<u64, u64>::new();
        for entry in &entries {
            *live_block_refs_by_slab
                .entry(entry.address.block_slab_id())
                .or_default() += 1;
        }

        let mut report = object_lifecycle_report_from_entries(
            shard_id,
            shard,
            entries,
            &BTreeSet::new(),
            |_| 0,
        );
        report.owner_mismatch_block_refs = ownership.mismatches.len() as u64;
        report.missing_owner_block_refs = ownership.missing_owner_block_refs as u64;
        report.stale_object_ids = block_slab_counts
            .iter()
            .map(|(block_slab_id, _physical_bytes, block_count)| {
                block_count.saturating_sub(
                    live_block_refs_by_slab
                        .get(block_slab_id)
                        .copied()
                        .unwrap_or_default(),
                )
            })
            .sum();
        report
    }

    pub(super) fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        self.storage_recovery_report_without_boundary_sampled(shard_id, 0)
    }

    /// The same report, reading at most `readable_probe_limit` live pages this call.
    ///
    /// The readability check is the only part of this report that reads a page, and it reads
    /// EVERY live one: measured at 32,000 records it is 575 ms and 32,000 reads, about a fifth
    /// of a maintenance round, growing with the store.
    ///
    /// The maintenance cycle wants this report for `manifest_chain_issues`, the two dump
    /// sequences and the two replay sequences -- none of which reads a page. It never consults
    /// `unreadable_page_refs` or `unreadable_page_bytes`, but it does CARRY them in the report it
    /// returns, so simply not filling them in would be a silent lie to whoever reads that report.
    /// Sampling is the honest version: corruption is still found, over rounds rather than all in
    /// one, and `readable_probe_limit` says how much of the store this particular call looked at.
    ///
    /// "Over rounds" needs the sample to MOVE, which is the part that was missing. A bounded
    /// call read `addresses[0 .. limit]` and began at the front again next round, so the pages
    /// past the first window were never read by the periodic loop -- on a shard with more live
    /// pages than the budget, corruption outside that prefix was undiscoverable, in any number
    /// of rounds, while the report kept saying the pages it had read were fine. A bounded call
    /// now resumes at `recovery_probe_cursors` and wraps, so the rounds together cover the whole
    /// shard; `readable_probe_cursor` on the report says where this one started.
    ///
    /// 0 means no bound, matching every other round bound here. The diagnostic endpoint and the
    /// harnesses keep passing 0 and so keep scanning everything.
    pub(super) fn storage_recovery_report_without_boundary_sampled(
        &self,
        shard_id: ShardId,
        readable_probe_limit: usize,
    ) -> StorageRecoveryReport {
        // Durable served-index size. The base is materialized only at compaction, so a fresh
        // crash-recovered shard has no base file yet -- the durable served index is the base
        // folded with the index-log deltas, whose reconstructed size we report (via the
        // served-index funnel) so recovery diagnostics reflect real durable state.
        let base_index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let index_bytes = if base_index_bytes == 0 {
            self.load_served_index_bytes(shard_id)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or_default()
        } else {
            base_index_bytes
        };
        // Counted, not collected. Both of these used to read their whole log into a vector
        // and take its length -- on the plan path of every maintenance round.
        let wal_records = self.wal_store.record_count(shard_id).unwrap_or_default();
        let index_log_records = self.index_log_store.record_count(shard_id).unwrap_or_default();
        let active_block_slab_ids = self.block_store.slab_ids().unwrap_or_default();
        let slab_descriptors = self.block_store.slab_descriptors();
        let slab_summary = self.block_store.slab_summary();
        let block_slab_reports = self.block_store.slab_reports().unwrap_or_default();
        let shards = self.shards_read_marked();
        // ONE walk, three consumers, under the guard that is already held.
        //
        // This function walked the whole live-page set THREE times: here for the addresses it
        // probes, again inside `validate_shard_block_ownership`, again inside
        // `storage_object_lifecycle_report`. `a_round_walks_the_live_block_set_a_constant_number_of_times`
        // counts all three, and they were three of the whole-store passes one periodic
        // round makes.
        //
        // Safe for the same reason the hoist in `storage_object_lifecycle_snapshot` above is, and
        // NOT for the reason a round-scoped memo would need: the `shards` read guard taken on the
        // line above is held across all three, so `&ShardState` cannot change between them and a
        // second walk could only rebuild what the first one already holds. Nothing here is cached
        // past the guard, so there is no invalidation question to get wrong.
        //
        // The addresses vector is GONE rather than moved: the probe loop below reads
        // `entry.address` straight out of the entries it now has, so the second `Vec` this
        // function used to keep alive beside them is not allocated at all.
        // Called directly rather than through `.map`, so `#[track_caller]` attributes the walk
        // to this file and `live_block_scan_sites_snapshot` keeps naming a real site.
        let live_entries = match shards.get(&shard_id) {
            Some(shard) => collect_live_block_entries(shard),
            None => Vec::new(),
        };
        let total_block_refs = live_entries.len();
        // WHERE this call reads, not just how much.
        //
        // A bounded call used to read `addresses[0 .. limit]` and nothing else, every round. The
        // budget made the cost constant; starting from the front every time made the COVERAGE
        // constant too, so a live page at an index past the budget was never read by the
        // periodic loop at all. The doc above promises corruption is "still found, over rounds
        // rather than all in one", and that promise needs the window to MOVE.
        //
        // So a bounded call resumes where the last one stopped and wraps at the end. Unbounded
        // calls (`readable_probe_limit == 0`) read everything, so they start at 0 and leave the
        // stored position untouched -- a diagnostic call must not shift the loop's window.
        //
        // The position is clamped rather than trusted: the live-page vector is rebuilt each
        // round and can shrink (compaction, eviction, expiry), and a stale index past its end
        // would otherwise skip the whole round.
        let probe_window_start = if readable_probe_limit > 0 && total_block_refs > 0 {
            self.recovery_probe_cursors
                .read()
                .expect("recovery probe cursor lock poisoned")
                .get(&shard_id)
                .copied()
                .filter(|start| *start < total_block_refs)
                .unwrap_or(0)
        } else {
            0
        };
        let probe_window_len = if readable_probe_limit == 0 {
            total_block_refs
        } else {
            readable_probe_limit.min(total_block_refs)
        };
        // Whether index `position` falls in the window `[start, start + len)` taken modulo the
        // live-page count. Written as a distance from the start so the wrap needs no second
        // range and no branch on whether the window crosses the end.
        let in_probe_window = |position: usize| -> bool {
            if probe_window_len == 0 {
                return false;
            }
            if probe_window_len >= total_block_refs {
                return true;
            }
            let distance = if position >= probe_window_start {
                position - probe_window_start
            } else {
                position + total_block_refs - probe_window_start
            };
            distance < probe_window_len
        };
        let mut readable_block_refs = 0usize;
        let mut probed_block_refs = 0usize;
        let mut unreadable_block_refs = Vec::new();
        let mut owner_mismatch_block_refs = Vec::new();
        let mut missing_owner_block_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_block_layout = StorageFeatureBlockLayoutReport::default();
        let mut block_slab_live_reports = block_slab_reports
            .iter()
            .map(|report| {
                (
                    report.block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: report.block_slab_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        block_count: report.block_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for (position, live_entry) in live_entries.iter().enumerate() {
            let address = &live_entry.address;
            let slab_report = block_slab_live_reports
                .entry(address.block_slab_id())
                .or_insert(StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id(),
                    ..StorageRecoverySlabLiveReport::default()
                });
            slab_report.live_block_refs = slab_report.live_block_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length());
            {
                let objects = live_object_ids.entry(address.block_slab_id()).or_default();
                objects.insert(expected_live_block_object_id(shard_id, live_entry));
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = live_entry.filed_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id())
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
            // Outside this call's window it stops READING, and keeps everything above that does
            // not need a read -- the per-slab live tallies are what the reclaim planner and the
            // object-lifecycle report are built from, and they must stay complete whatever the
            // window is. Only the reads below are sampled.
            //
            // This used to test `probed_page_refs >= readable_probe_limit`, which is the same
            // budget but anchored at the front: it always admitted the first `limit` entries and
            // never any other. The window test admits `limit` entries too -- so the per-round
            // cost is unchanged -- but a different `limit` of them each round.
            if !in_probe_window(position) {
                continue;
            }
            probed_block_refs += 1;
            // Counted against the shard-table guard. This probe DOES read under the read guard,
            // and unlike the warm-up it is bounded -- `readable_probe_limit` stops the reads
            // while the per-slab tallies above keep going. Routed through the counter so the
            // measurement covers both of the engine's maintenance page readers and a claim about
            // one of them is made against a total that includes the other.
            match self.read_block_counted(address) {
                Ok(bytes) => {
                    readable_block_refs += 1;
                    slab_report.readable_live_block_refs =
                        slab_report.readable_live_block_refs.saturating_add(1);
                    slab_report.live_logical_bytes = slab_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    slab_report.unreadable_live_block_refs =
                        slab_report.unreadable_live_block_refs.saturating_add(1);
                    unreadable_block_refs.push(StorageRecoveryBlockError {
                        block_slab_id: address.block_slab_id(),
                        offset: address.offset(),
                        length: address.length(),
                        error: err.to_string(),
                    });
