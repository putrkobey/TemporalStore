// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage lifecycle / WAL-reclaim / eviction methods for TemporalEngine, split from engine.rs.
use super::*;

/// How many dirty-object keys the dump drain has looked at, across every call.
///
/// The drain's cost is not visible from outside -- it is a closure inside a `retain` -- and
/// deriving it as |dirty objects| x |buckets| is arithmetic about the code rather than a
/// measurement of it. This counts what actually happens, which is the only version that keeps
/// being true after someone changes the loop.
pub(crate) static DIRTY_DRAIN_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bucket-recency entries COPIED on the way into an eviction round, across every call.
///
/// The round used to clone the whole `bucket_recency` map before looking at which selection path
/// was going to run, and only the EXHAUSTIVE path ever read it. Under the shipped default -- the
/// sampled path -- every round copied one entry per bucket the shard had ever touched and dropped
/// the copy unread. That is a cost that tracks the store sitting in front of a selection path
/// whose whole purpose is to cost the batch instead.
///
/// COUNTED, not timed, and counted in ENTRIES rather than bytes: the question is whether the
/// round touches a per-bucket structure at all, and an entry count answers that identically on an
/// idle box and a loaded one. Process-wide, so a reader must reset it immediately before the call
/// it is measuring, and the suite it is read in runs single-threaded.
/// Buckets the sampled selector LOOKED AT, summed over every pass in this process.
///
/// WHY A COUNTER AND NOT A REPORT FIELD. This is the number the sampler's whole bound is stated
/// in -- a pass walks at most `samples * batch_limit * scan_turns` buckets -- and nothing on the
/// shipped report carried it, so the one clause that makes a steady-state pass CHEAP had no
/// engine-side guard: a selector handed a constant clock instead of a real one sees no candidate
/// as cold, walks its whole ceiling on every pass for ever, and is indistinguishable from a
/// correct one by every other number a round publishes. `the_second_pass_stops_at_its_minimum_\
/// scan_once_the_first_has_stamped_its_clock` is what reads it.
///
/// PROCESS-WIDE, like the counter below it, so a test that reads it has to run serially. CI runs
/// this suite with `--test-threads=1`.
pub(crate) static EVICTION_SAMPLER_BUCKETS_SCANNED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Read the counter above. The module is private to `engine`, so the only way a fixture outside
/// it can see this number is through a re-exported accessor -- the same shape
/// `storage_plan_build_counts` already uses.
pub fn eviction_sampler_buckets_scanned() -> u64 {
    EVICTION_SAMPLER_BUCKETS_SCANNED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Zero it, so a fixture can bracket ONE round rather than read a process total it did not cause.
pub fn reset_eviction_sampler_buckets_scanned() {
    EVICTION_SAMPLER_BUCKETS_SCANNED.store(0, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) static EVICTION_RECENCY_ENTRIES_CLONED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// WAL records a `delete_drop` eviction round appends WHILE HOLDING the shard-table write guard.
///
/// The round's index flush was already moved out of the guard -- a stamped clone carries the
/// state out and the encode and both file writes happen unlocked. The per-key WAL tombstones were
/// not, and the comment beside them says so. This counts them, because the shape has been
/// mis-read twice in this engine by argument and settled twice by a counter.
///
/// Incremented at the append itself, which is lexically inside the `shards.write()` block and
/// before the `drop(shards)` below it, so a record counted here was appended under the guard by
/// construction rather than by reading. Process-wide: a reader resets it immediately before the
/// round it is measuring, and the suite it is read in runs single-threaded.
pub(crate) static EVICTION_DELETE_DROP_WAL_APPENDS_UNDER_GUARD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Where a `delete_drop` round's time under the shard-table write guard actually goes, split by
/// phase, in nanoseconds.
///
/// The append counter above establishes that the tombstone loop runs once per dropped key. It
/// does NOT establish that the loop is worth moving: lifting a phase that is 3% of the hold buys
/// 3%, and the cost of moving it is an ordering argument about `applied_wal_sequence` that nobody
/// has written down. These rows are what makes that question answerable instead of arguable.
///
/// Read as FRACTIONS of one round's own hold, never as absolute times. A fraction is a ratio of
/// two clocks taken microseconds apart on the same thread, so it survives a loaded box in a way
/// an absolute figure does not -- which is why the probe that reads these PRINTS them and asserts
/// only on counts.
///
/// `total` is taken by its OWN clock spanning the whole guarded region -- from the write guard
/// being acquired to it being dropped -- rather than by summing the rows. So `total` minus the
/// rows is an independent residual and not an identity: work that drifts out of every phase lands
/// in it, where a self-summing table would still balance.
pub(crate) struct DeleteDropGuardNanos {
    /// The whole hold, by its own clock.
    pub total: std::sync::atomic::AtomicU64,
    /// `collect_live_block_entries` + the victim-bucket filter: O(shard).
    pub collect: std::sync::atomic::AtomicU64,
    /// `delete_record`, once per key: the in-memory removal from the shard index. This is the
    /// only half of the drop loop that actually needs the shard guard.
    pub delete: std::sync::atomic::AtomicU64,
    /// `invalidate_record_all`, once per key. Takes `&MultiLayerCache` and a key and touches
    /// NOTHING on the shard -- so what it costs is charged to a guard it does not need.
    pub invalidate: std::sync::atomic::AtomicU64,
    /// The per-key WAL tombstone loop, including the mirror hand-off: once per dropped key.
    pub wal_append: std::sync::atomic::AtomicU64,
    /// Reading the sequence the round anchors `applied_wal_sequence` to: once per round.
    pub anchor: std::sync::atomic::AtomicU64,
    /// `shard.clone()`, the stamped snapshot the unlocked index flush is handed: once per round.
    pub snapshot: std::sync::atomic::AtomicU64,
}

impl DeleteDropGuardNanos {
    const fn zeroed() -> Self {
        Self {
            total: std::sync::atomic::AtomicU64::new(0),
            collect: std::sync::atomic::AtomicU64::new(0),
            delete: std::sync::atomic::AtomicU64::new(0),
            invalidate: std::sync::atomic::AtomicU64::new(0),
            wal_append: std::sync::atomic::AtomicU64::new(0),
            anchor: std::sync::atomic::AtomicU64::new(0),
            snapshot: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Process-wide, so a reader resets immediately before the round it is measuring, and the
    /// suite it is read in runs single-threaded.
    pub(crate) fn reset(&self) {
        for cell in self.cells() {
            cell.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn cells(&self) -> [&std::sync::atomic::AtomicU64; 7] {
        [
            &self.total,
            &self.collect,
            &self.delete,
            &self.invalidate,
            &self.wal_append,
            &self.anchor,
            &self.snapshot,
        ]
    }

    /// `(total, collect, delete, invalidate, wal_append, anchor, snapshot)` in nanoseconds.
    pub(crate) fn read(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        let [total, collect, delete, invalidate, wal_append, anchor, snapshot] = self
            .cells()
            .map(|cell| cell.load(std::sync::atomic::Ordering::Relaxed));
        (
            total, collect, delete, invalidate, wal_append, anchor, snapshot,
        )
    }

    /// The hold this round did not attribute to any phase above. Saturating, so an ordering
    /// surprise reads as zero rather than as an enormous number.
    pub(crate) fn unattributed(&self) -> u64 {
        let (total, collect, delete, invalidate, wal_append, anchor, snapshot) = self.read();
        total
            .saturating_sub(collect)
            .saturating_sub(delete)
            .saturating_sub(invalidate)
            .saturating_sub(wal_append)
            .saturating_sub(anchor)
            .saturating_sub(snapshot)
    }

    fn add_since(&self, cell: &std::sync::atomic::AtomicU64, since: std::time::Instant) {
        cell.fetch_add(
            since.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

pub(crate) static DELETE_DROP_GUARD_NANOS: DeleteDropGuardNanos = DeleteDropGuardNanos::zeroed();

/// How many O(shard) plans one round builds, counted rather than timed.
///
/// A round builds these in several stages and each walks the shard. Whether that is duplicated
/// work is a question about COUNTS, and a count is immune to whatever else is running on the box
/// -- timing it beside another build would measure the machine, not the loop.
pub(crate) static LIFECYCLE_PLAN_BUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The same, for the WAL reclaim plan. Separate because the two have different costs and a
/// single total would hide which one a change moved.
pub(crate) static WAL_RECLAIM_PLAN_BUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Both counters, for a probe measuring one round.
pub fn storage_plan_build_counts() -> (u64, u64) {
    (
        LIFECYCLE_PLAN_BUILDS.load(std::sync::atomic::Ordering::Relaxed),
        WAL_RECLAIM_PLAN_BUILDS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Zero both, so a probe measures a round rather than a process.
pub fn reset_storage_plan_build_counts() {
    LIFECYCLE_PLAN_BUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
    WAL_RECLAIM_PLAN_BUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl TemporalEngine {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        LIFECYCLE_PLAN_BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // DO NOT WALK THE SHARD TO FIND OUT THERE IS NOTHING TO DO.
        //
        // `bucket_storage_summaries` materialises every live page in the shard. This call sat at
        // the top of the plan unconditionally, so a settled shard that nobody had written to paid
        // a whole-shard walk on every maintenance round, for ever.
        //
        // It used to be load-bearing: dump selection read `dirty_object_count` off the summaries.
        // #1709 ended that. `dirty_object_count` is now populated from exactly ONE place --
        // `shard.dirty_objects.bucket_counts()`, the last loop in `bucket_storage_summaries` --
        // and every other loop in that function leaves it at its `Default` of 0. So
        //
        //     "some summary has dirty_object_count > 0"   IFF   "the dirty index is non-empty"
        //
        // exactly, and the right-hand side is a `BTreeMap::is_empty` that touches no pages. When
        // the index is empty the filter below selects nothing, `dirty_buckets` is empty, and the
        // walk's only remaining product is the report field -- which is why it survived, and why
        // that field is now an `Option` rather than a vector that reads as a measured zero.
        //
        // The walk is NOT deleted, and one caller can still demand it below:
        // `dump_refreshes_a_vacated_slab` selects a dump with no bucket dirty, and needs the full
        // bucket list to do it. That branch takes the walk itself, so skipping here cannot starve
        // it -- see the `get_or_insert_with` at its site.
        //
        // WHICH BUCKETS ARE DIRTY is read off their own nodes in the same lock acquisition, and the
        // filter below asks that instead of `dirty_object_count > 0`. See `dirty_bucket_nodes`: the
        // node's flag and the object-keyed set are two independent records of the same fact, so a
        // dirty-BUCKET question can be asked of the bucket. The short-circuit above is unchanged
        // and still gates on the SET, because it is the thing that decides whether the summaries
        // were materialised at all -- with no summaries the filter selects nothing either way.
        let (shard_has_dirty_objects, dirty_bucket_nodes) = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&request.shard_id)
            .map(|shard| {
                (
                    !shard.dirty_objects.is_empty(),
                    crate::engine::storage_reporting::dirty_bucket_nodes(shard),
                )
            })
            .unwrap_or_default();
        let mut bucket_summaries: Option<Vec<BucketStorageSummary>> = shard_has_dirty_objects
            .then(|| self.bucket_storage_summaries(request.shard_id));
        // Select the least-recently-dumped (most overdue) dirty buckets first, matching the
        // WAL-reclaim routine's oldest-first-dirty ordering (dirty buckets are consumed
        // in non-decreasing first-dirty-log-id order).
        // bucket_summaries arrives in ascending routing_bucket order (a BTreeMap), so truncating to
        // max_dump_buckets_per_round always dropped the same high-id buckets -- a bucket dirtied
        // once could be starved forever by low-id buckets re-dirtied every round, never
        // checkpointed and pinning the WAL reclaim floor. Ordering by last_dump_sequence
        // ascending makes an overdue bucket rise to the top and guarantees every dirty bucket is
        // eventually selected; routing_bucket is a stable tiebreaker.
        //
        // WHICH `last_dump_sequence` THIS IS. There is now only one, and that is a change: there
        // used to be two with the same name and different values. The sort below reads
        // `BucketStorageSummary::last_dump_sequence`, written by `merge_last_dump_sequence` from
        // the NEWEST manifest's `index_log_sequence` -- the same value for every bucket that
        // manifest names, and 0 for every bucket it does not. So what this key separates is
        // "covered by the newest dump" from "not covered by it", not "dumped recently" from
        // "dumped long ago".
        //
        // The bucket NODE used to carry its own `last_dump_sequence`, written from
        // `manifest.wal_sequence` by `clear_dumped_bucket_dirty_state`, and it never reached this
        // sort -- it was read into two reports and nothing else, so it was a watermark of the
        // whole shard's newest dump wearing a per-bucket costume, at eight bytes per routing
        // bucket. It is gone, and both reports take the figure from the manifest.
        // `the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node` pins that the
        // summary's figure is the manifest's, which is now the only place it can come from.
        //
        // Left as it is rather than repointed at the node: this is a TIEBREAKER behind
        // `first_dirty_rank` below, which is the key that decides the order for every bucket that
        // can name its oldest undumped write.
        // Empty EXACTLY when the dirty index is empty, which is the case that skipped the walk
        // above -- so this selects the same buckets it always did, without a fallback that could
        // mistake "did not look" for "looked and found none".
        let dirty_bucket_source: &[BucketStorageSummary] = bucket_summaries.as_deref().unwrap_or(&[]);
        // THE BUCKET'S OWN FLAG DECIDES WHETHER IT IS DUMPED. The error this can make is the SAFE
        // one: a bucket reading dirty that holds nothing is a bucket dumped for no reason, where
        // the opposite -- a dirty bucket read as clean -- is undumped state no round would pick up.
        let mut dirty_bucket_summaries = dirty_bucket_source
            .iter()
            .filter(|summary| dirty_bucket_nodes.contains(&summary.routing_bucket))
            .collect::<Vec<_>>();
        // Oldest undumped write first: the bucket that has held the log longest is dumped first.
        //
        // `last_dump_sequence` answers "is this bucket in the newest dump manifest" (see the note
        // above it), which is a proxy for "most overdue" and not the same question. What holds
        // the log is the bucket's OLDEST UNDUMPED WRITE, and `first_dirty_wal_sequence` is that
        // -- so ordering by it dumps the bucket pinning the log's floor first, and no bucket can
        // be starved indefinitely while older ones keep being re-dirtied.
        //
        // Read from the bucket node rather than added to `BucketStorageSummary`: a summary is
        // stored in the dump manifest and compared field-by-field when a manifest is validated,
        // so a new field there is a durability contract, not a report field.
        //
        // 0 means no claim recorded, which sorts LAST: a bucket we cannot place in the log is not
        // evidence of being old, and the ones we can place are the ones whose dump moves the
        // floor.
        let first_dirty_by_bucket = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&request.shard_id)
            .map(|shard| {
                shard
                    .bucket_index
                    .bucket_map
                    .iter()
                    .map(|(routing_bucket, bucket)| {
                        (*routing_bucket, bucket.first_dirty_wal_sequence)
                    })
                    .collect::<std::collections::HashMap<u32, u64>>()
            })
            .unwrap_or_default();
        let first_dirty_rank = |routing_bucket: u32| -> u64 {
            match first_dirty_by_bucket.get(&routing_bucket).copied() {
                Some(0) | None => u64::MAX,
                Some(sequence) => sequence,
            }
        };
        dirty_bucket_summaries.sort_by(|left, right| {
            first_dirty_rank(left.routing_bucket)
                .cmp(&first_dirty_rank(right.routing_bucket))
                .then_with(|| left.last_dump_sequence.cmp(&right.last_dump_sequence))
                .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
        });
        let dirty_buckets = dirty_bucket_summaries
            .iter()
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        let latest_bucket_dump_manifest =
            latest_bucket_dump_manifest_shared_at(&self.index_dir, request.shard_id);
        let latest_dump_wal_sequence = latest_bucket_dump_manifest
            .as_ref()
            .map(|manifest| manifest.wal_sequence)
            .unwrap_or_default();
        let wal_stats = self.wal_store.stats(request.shard_id);
        let current_wal_sequence = wal_stats.last_sequence;
        let undumped_wal_records =
            current_wal_sequence.saturating_sub(latest_dump_wal_sequence);
        let explicit_buckets = !request.selected_dump_buckets.is_empty();
        // Durable bytes that are UNDUMPED, not the log's size. `persistent_bytes` is the whole
        // log across every segment, so a log that is large but fully dumped cleared this
        // threshold on every round: a shard that had written one record since its last dump
        // earned another whole-index serialize, which is the cost this cadence exists to avoid.
        let undumped_wal_bytes = self.wal_store.undumped_len_since_dump(request.shard_id);
        // Reported, not compared. See `StorageLifecyclePlan::undumped_wal_objects`.
        let undumped_wal_objects = self.wal_store.undumped_objects_since_dump(request.shard_id);
        // Each threshold can only RELEASE the dump, never hold it: a delay needs both to agree
        // there is not enough yet. Requiring both to be CROSSED instead would let the byte
        // threshold suppress a dump the record count had already earned, which is the opposite
        // of bounding the log.
        let records_say_wait = request.min_undumped_wal_records > 0
            && undumped_wal_records < request.min_undumped_wal_records;
        let bytes_say_wait = request.min_undumped_wal_bytes == 0
            || undumped_wal_bytes < request.min_undumped_wal_bytes;
        let dump_delayed = !explicit_buckets && records_say_wait && bytes_say_wait;
        let mut selected_dump_buckets = if explicit_buckets {
            request.selected_dump_buckets.clone()
        } else if dump_delayed {
            Vec::new()
        } else {
            dirty_buckets.clone()
        };
        if request.max_dump_buckets_per_round > 0
            && selected_dump_buckets.len() > request.max_dump_buckets_per_round
        {
            selected_dump_buckets.truncate(request.max_dump_buckets_per_round);
        }
        let live_block_slab_ids = self.live_block_slab_ids(request.shard_id);
        let live_block_slab_set = live_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let stale_block_slab_ids = self
            .block_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live_block_slab_set.contains(id))
            .collect::<Vec<_>>();
        let reclaim_slab_reports = self.storage_reclaim_slab_reports(request.shard_id);
        let stale_block_slab_set = stale_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        // REFRESH A DUMP THAT STILL NAMES A SLAB COMPACTION HAS EMPTIED.
        //
        // `run_gc_inner` and `storage_block_gc_dependency_plan` both hold back every slab a bucket
        // dump manifest names, and they are right to: installing that dump READS those pages, so
        // destroying the slab makes the dump uninstallable and a lagging replica unservable.
        // #1565 stopped such a slab freezing the retain FLOOR; it deliberately did not make the
        // slab deletable, and said so.
        //
        // What that leaves is a dead slab standing for ever on an IDLE shard. Compaction copies
        // the live pages onto a fresh slab, and the newest manifest goes on naming the slab it
        // emptied -- because a manifest is only ever replaced by a DUMP, a dump is selected from
        // DIRTY buckets, and an idle shard has none. Measured on a settled shard that nobody is
        // writing to, in EVERY settled round, at 8,000 records and again at 80,000:
        // slabs 2, stale 1, reclaim_candidates 1, relocatable 0.
        //
        // So ask for the dump rather than keeping the slab. It re-exports the index compaction
        // has already persisted, the replacement manifest names only slabs that are live, the
        // superseded one stops adding coverage and is pruned, and the vacated slab is collected
        // on the next round. Nothing is destroyed while a manifest still needs it: the order is
        // dump, prune, then collect, each in its own round.
        //
        // TERMINATING BY CONSTRUCTION, which is what keeps this off a busy shard and off a loop.
        // The condition reads the NEWEST manifest only, and the dump it selects BECOMES the
        // newest and names live slabs alone -- so it cannot ask twice for the same vacated slab.
        // An OLDER manifest kept because it is the only dump covering some bucket can still hold
        // a slab back; that is a retention decision this does not override, and it does not
        // re-arm this either.
        //
        // RE-CHECKED after `block_slab_ids` was widened to every slab the manifest's whole-shard
        // index can install, rather than only the dumped buckets'. The argument turns on one
        // premise -- that a fresh dump names LIVE slabs only -- and widening makes that premise
        // exact rather than weakening it: the widened set is derived from the live page refs of
        // the index the dump exports, which IS the shard's live slab set at that moment, so every
        // id in it is live by construction. What changes is how often this fires, not whether it
        // ends: a slab holding nothing but unnamed-bucket pages is now named, so vacating it now
        // arms this where before it armed nothing and the slab was destroyed under a manifest
        // that needed it. Each firing still replaces the newest manifest with one naming live
        // slabs alone, so it still cannot fire twice for the same vacated slab.
        let latest_manifest_names_a_vacated_slab = latest_bucket_dump_manifest
            .as_ref()
            .map(|manifest| {
                manifest
                    .block_slab_ids
                    .iter()
                    .any(|block_slab_id| stale_block_slab_set.contains(block_slab_id))
            })
            .unwrap_or(false);
        let dump_refreshes_a_vacated_slab = !explicit_buckets
            && selected_dump_buckets.is_empty()
            && latest_manifest_names_a_vacated_slab;
        if dump_refreshes_a_vacated_slab {
            // Every bucket the shard holds now, and NOT truncated to
            // `max_dump_buckets_per_round`. A replacement that covers less than what it
            // supersedes supersedes nothing: the older manifest stays retained for the coverage
            // it alone holds, the slab stays pinned, and the round above will not ask again. The
            // cap costs nothing to skip here -- `create_bucket_dump_manifest` exports the whole
            // index whatever the selection is, so a narrower one would be the same work for a
            // result that releases nothing.
            //
            // THIS BRANCH FIRES ON AN IDLE SHARD BY DESIGN -- no bucket is dirty, which is
            // precisely the state that skipped the walk at the top of this function. So take it
            // here. `get_or_insert_with` walks only if the plan has not already, which keeps a
            // dirty round at one walk and makes this the only round that pays for a second.
            selected_dump_buckets = bucket_summaries
                .get_or_insert_with(|| self.bucket_storage_summaries(request.shard_id))
                .iter()
                .map(|summary| summary.routing_bucket)
                .collect::<Vec<_>>();
        }
        let mut reclaim_candidates = storage_reclaim_candidates_from_slab_reports(
            &reclaim_slab_reports,
            &stale_block_slab_set,
        );
        let delayed_destroy_reports = self
            .block_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        reclaim_candidates.extend(delayed_destroy_reports.iter().map(|report| {
            StorageReclaimCandidate {
                block_slab_id: report.block_slab_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: 0,
                stale_physical_bytes: report.physical_bytes,
                reclaim_score: report.physical_bytes.saturating_mul(2),
                reason: "delayed_destroy".to_string(),
                ..StorageReclaimCandidate::default()
            }
        }));
        reclaim_candidates.sort_by(|left, right| {
            right
                .reclaim_score
                .cmp(&left.reclaim_score)
                .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
                .then_with(|| left.block_slab_id.cmp(&right.block_slab_id))
        });
        let mut reasons = Vec::new();
        if dump_refreshes_a_vacated_slab {
            // A distinct reason, not "dirty_slot_dump": no bucket is dirty, and an operator
            // reading the plan needs to see that this dump is the collector's precondition
            // rather than a write being checkpointed.
            reasons.push("slot_dump_refresh_after_relocation".to_string());
        } else if !selected_dump_buckets.is_empty() {
            reasons.push("dirty_slot_dump".to_string());
        } else if dump_delayed && !dirty_buckets.is_empty() {
            reasons.push("dirty_slot_dump_delayed".to_string());
        }
        if !stale_block_slab_ids.is_empty() {
            reasons.push("stale_block_segment_gc".to_string());
        }
        if !reclaim_candidates.is_empty() {
            reasons.push("ranked_reclaim_candidates".to_string());
        }
        if request.purge_delayed_destroy && !delayed_destroy_reports.is_empty() {
            reasons.push("delayed_destroy_purge".to_string());
        }
        let block_gc_dependency_plan = self.storage_block_gc_dependency_plan(
            request.shard_id,
            reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id),
            request.block_gc_shared_store_cursors.clone(),
            request.block_gc_raft_snapshot_refs.clone(),
            request.block_gc_checkpoint_floor_slab_id,
            request.block_gc_raft_install_floor_slab_id,
            request.block_gc_delayed_destroy_grace_ms,
        );
        if !block_gc_dependency_plan
            .candidate_block_slab_ids
            .is_empty()
            && !block_gc_dependency_plan.safe_to_reclaim
        {
            reasons.push("block_gc_dependency_blocked".to_string());
        }
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        if !manifest_prune_plan.prunable_manifest_ids.is_empty()
            || !manifest_prune_plan.prunable_marker_manifest_ids.is_empty()
        {
            reasons.push("slot_dump_manifest_prune".to_string());
        }
        if !self
            .interrupted_bucket_dump_installs(request.shard_id)
            .is_empty()
        {
            reasons.push("slot_dump_install_roll_forward_check".to_string());
        }
        if request.invalidate_cache {
            reasons.push("cache_invalidation".to_string());
        }
        StorageLifecyclePlan {
            shard_id: request.shard_id,
            dirty_buckets,
            selected_dump_buckets,
            undumped_wal_records,
            undumped_wal_objects,
            dump_delayed,
            bucket_summaries,
            live_block_slab_ids,
            stale_block_slab_ids,
            reclaim_candidates,
            delayed_destroy_block_slab_ids: delayed_destroy_reports
                .iter()
                .map(|report| report.block_slab_id)
                .collect(),
            reclaimable_physical_bytes: delayed_destroy_reports
                .iter()
                .map(|report| report.physical_bytes)
                .sum(),
            reasons,
        }
    }

    pub fn storage_block_gc_dependency_plan(
        &self,
        shard_id: ShardId,
        candidate_block_slab_ids: impl IntoIterator<Item = u64>,
        shared_store_cursors: impl IntoIterator<Item = StorageBlockGcReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
        checkpoint_snapshot_floor: Option<u64>,
        raft_snapshot_install_floor: Option<u64>,
        delayed_destroy_grace_ms: u64,
    ) -> StorageBlockGcDependencyPlan {
        let mut candidate_block_slab_ids = candidate_block_slab_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        candidate_block_slab_ids.sort_unstable();
        let candidate_set = candidate_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let live_block_slab_ids = self.live_block_slab_ids(shard_id);
        let live_set = live_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let manifests =
            list_bucket_dump_manifests_shared_at(&self.index_dir, shard_id).unwrap_or_default();
        let mut manifest_block_slab_ids = manifests
            .iter()
            .flat_map(|manifest| manifest.block_slab_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        manifest_block_slab_ids.sort_unstable();
        let manifest_set = manifest_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let shared_store_cursors = shared_store_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let delayed_destroy_reports = self
            .block_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        let delayed_destroy_modified = delayed_destroy_reports
            .iter()
            .map(|report| (report.block_slab_id, report.modified_unix_ms))
            .collect::<BTreeMap<_, _>>();
        let now = now_ms();
        let mut dependency_blocks = Vec::new();
        for block_slab_id in &candidate_block_slab_ids {
            if live_set.contains(block_slab_id) {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "live_block_ref".to_string(),
                    owner_id: format!("shard:{shard_id}"),
                    reason: "indexed live page references still point at this page segment"
                        .to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if manifest_set.contains(block_slab_id) {
                let owner_id = manifests
                    .iter()
                    .filter(|manifest| manifest.block_slab_ids.contains(block_slab_id))
                    .map(|manifest| manifest.manifest_id.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "slot_dump_manifest".to_string(),
                    owner_id,
                    reason: "slot dump manifest still names this page segment".to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            for cursor in shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
            {
                if *block_slab_id >= cursor.retain_from_block_slab_id {
                    dependency_blocks.push(StorageBlockGcDependencyBlock {
                        block_slab_id: *block_slab_id,
                        dependency: "shared_store_replay_cursor".to_string(),
                        owner_id: cursor.cursor_id.clone(),
                        retain_from_block_slab_id: Some(cursor.retain_from_block_slab_id),
                        reason: if cursor.reason.is_empty() {
                            "shared-store replay cursor has not advanced past this page segment"
                                .to_string()
                        } else {
                            cursor.reason.clone()
                        },
                        ..StorageBlockGcDependencyBlock::default()
                    });
                }
            }
            for snapshot in raft_snapshot_refs
                .iter()
                .filter(|snapshot| snapshot.shard_id == shard_id)
            {
                if *block_slab_id >= snapshot.index_log_sequence {
                    dependency_blocks.push(StorageBlockGcDependencyBlock {
                        block_slab_id: *block_slab_id,
                        dependency: "raft_snapshot_ref".to_string(),
                        owner_id: snapshot.snapshot_id.clone(),
                        retain_from_block_slab_id: Some(snapshot.index_log_sequence),
                        reason: "Raft snapshot reference has not released this page segment floor"
                            .to_string(),
                        ..StorageBlockGcDependencyBlock::default()
                    });
                }
            }
            if checkpoint_snapshot_floor
                .map(|floor| *block_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "checkpoint_snapshot_floor".to_string(),
                    owner_id: format!("checkpoint:{shard_id}"),
                    retain_from_block_slab_id: checkpoint_snapshot_floor,
                    reason: "checkpoint/snapshot floor still retains this page segment".to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if raft_snapshot_install_floor
                .map(|floor| *block_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "raft_snapshot_install_floor".to_string(),
                    owner_id: format!("raft-install:{shard_id}"),
                    retain_from_block_slab_id: raft_snapshot_install_floor,
                    reason: "Raft snapshot install floor still retains this page segment"
                        .to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if delayed_destroy_grace_ms > 0 {
                if let Some(modified_unix_ms) = delayed_destroy_modified
                    .get(block_slab_id)
                    .and_then(|modified| *modified)
                {
                    let retain_until = modified_unix_ms.saturating_add(delayed_destroy_grace_ms);
                    if now < retain_until {
                        dependency_blocks.push(StorageBlockGcDependencyBlock {
                            block_slab_id: *block_slab_id,
                            dependency: "delayed_destroy_grace_period".to_string(),
                            owner_id: format!("delayed-destroy:{block_slab_id}"),
                            retain_until_unix_ms: Some(retain_until),
                            reason:
                                "delayed-destroy grace period has not elapsed for this page segment"
                                    .to_string(),
                            ..StorageBlockGcDependencyBlock::default()
                        });
                    }
                }
            }
        }
        let blocked_block_slab_ids = dependency_blocks
            .iter()
            .map(|block| block.block_slab_id)
            .collect::<BTreeSet<_>>();
        let dependency_count = |dependency: &str| {
            dependency_blocks
                .iter()
                .filter(|block| block.dependency == dependency)
                .count()
        };
        let live_ref_block_count = dependency_count("live_block_ref");
        let bucket_dump_manifest_block_count = dependency_count("slot_dump_manifest");
        let shared_store_cursor_block_count = dependency_count("shared_store_replay_cursor");
        let raft_snapshot_ref_block_count = dependency_count("raft_snapshot_ref");
        let checkpoint_snapshot_floor_block_count = dependency_count("checkpoint_snapshot_floor");
        let raft_snapshot_install_floor_block_count =
            dependency_count("raft_snapshot_install_floor");
        let delayed_destroy_grace_block_count = dependency_count("delayed_destroy_grace_period");
        let reclaimable_block_slab_ids = candidate_block_slab_ids
            .iter()
            .copied()
            .filter(|id| !blocked_block_slab_ids.contains(id))
            .collect::<Vec<_>>();
        let blocked_block_slab_ids = blocked_block_slab_ids.into_iter().collect::<Vec<_>>();
        let mut blocker_reasons = dependency_blocks
            .iter()
            .map(|block| block.dependency.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if candidate_set.is_empty() {
            blocker_reasons.clear();
        }
        StorageBlockGcDependencyPlan {
            shard_id,
            safe_to_reclaim: !candidate_set.is_empty() && dependency_blocks.is_empty(),
            candidate_block_slab_ids,
            reclaimable_block_slab_ids,
            blocked_block_slab_ids,
            live_block_slab_ids,
            manifest_block_slab_ids,
            shared_store_cursor_count: shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
                .count(),
            checkpoint_snapshot_floor,
            raft_snapshot_install_floor,
            delayed_destroy_grace_ms,
            live_ref_block_count,
            bucket_dump_manifest_block_count,
            shared_store_cursor_block_count,
            raft_snapshot_ref_block_count,
            checkpoint_snapshot_floor_block_count,
            raft_snapshot_install_floor_block_count,
            delayed_destroy_grace_block_count,
            dependency_blocks,
            blocker_reasons,
        }
    }

    /// Clear the dirty state of buckets just captured by `manifest` (a dumped
    /// bucket has its dirty flag cleared), so the storage cycle does not
    /// re-select and re-dump them every round. A bucket re-dirtied since the manifest
    /// snapshot (its current derived generation no longer equals the captured one) is
    /// left dirty, so reclaim never advances past an undumped write. The bucket's
    /// generation is held at the captured (derived) value and its live pages are
    /// untouched, so bucket_dump_summary_matches_current_generation keeps matching and
    /// WAL/index reclaim gating is unchanged.
    pub(super) fn clear_dumped_bucket_dirty_state(
        &self,
        shard_id: ShardId,
        manifest: &BucketDumpManifest,
    ) {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return;
        };
        // Per-bucket derived generation the manifest captured (base + dirty count) --
        // exactly what the reclaim fingerprint compares.
        let captured: std::collections::HashMap<u32, u64> = manifest
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.dirty_generation))
            .collect();
        // Current derived generation BEFORE mutation: detects writes that landed after
        // the dump snapshot (those buckets must stay dirty for the next dump).
        //
        // DO NOT HOIST THIS INTO A SHARED SNAPSHOT. `who_walks_the_shard` shows
        // `bucket_storage_summaries` entered THREE times in one `apply_storage_lifecycle` -- here,
        // in `storage_lifecycle_plan`, and in `create_bucket_dump_manifest` -- and three identical
        // whole-shard walks in one operation look exactly like something to share. Two of them
        // can be. This one cannot, and the reason is the line above rather than anything about
        // locking.
        //
        // This walk exists to be FRESH. It is compared against the generation the manifest
        // CAPTURED, and a bucket whose generation has moved since is skipped so it stays dirty for
        // the next dump. Feed it a snapshot taken when the plan ran and both sides become equal by
        // construction: a bucket written to between the dump and this clear compares equal, is
        // cleared, stops being dirty, is never dumped, and reclaim is then free to advance past the
        // records it still needed. That is silent data loss, discovered on a later restart.
        //
        // The test that would catch it is not obvious either: any fixture that does not write
        // CONCURRENTLY with a dump will pass a hoisted version happily.
        let current: std::collections::HashMap<u32, u64> =
            bucket_storage_summaries(shard, start_routing_bucket, end_routing_bucket)
                .into_iter()
                .map(|summary| (summary.routing_bucket, summary.dirty_generation))
                .collect();
        // Buckets this dump actually clears. Collected first so the dirty set is walked ONCE.
        //
        // The retain used to sit inside this loop, so every qualifying bucket walked every dirty
        // object and re-hashed its key to recompute a routing bucket -- a bucket the caller
        // already knew, and one that `mark_object_dirty` had computed on the line above the
        // insert and thrown away. The work was |dirty objects| x |buckets|, to remove at most
        // |dirty objects| entries: measured at 4 040 000 closure calls to clear 4 000 objects
        // across 1 010 buckets, a 1010x amplification.
        //
        // Nothing in the per-bucket body depends on the dirty set having been cleared, and
        // `current` was captured before any mutation, so hoisting the walk out is the same answer
        // in one pass.
        let mut cleared_buckets: Vec<u32> = Vec::new();
        for bucket_id in manifest.bucket_ids.iter().copied() {
            let Some(&captured_generation) = captured.get(&bucket_id) else {
                continue;
            };
            if current.get(&bucket_id).copied().unwrap_or_default() != captured_generation {
                continue;
            }
            cleared_buckets.push(bucket_id);
            if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&bucket_id) {
                // Hold the generation at the captured (derived) value so the reclaim
                // fingerprint still matches once the dirty objects are cleared.
                bucket.dirty_generation = bucket.dirty_generation.max(captured_generation);
                // THE DUMPED-LOG SEQUENCE IS NOT RECORDED HERE ANY MORE, and the comment that was
                // here said why it did not need to be: "informational; not part of the
                // fingerprint". A per-bucket `last_dump_sequence` was written from
                // `manifest.wal_sequence` at this one site, preserved across rebuild, and read
                // into two reports -- never into a branch, a sort key or a comparison. The
                // manifest that named this bucket is the thing that knows the figure, and the
                // report reads it from there (`merge_last_dump_sequence`) exactly as
                // `BucketStorageSummary` always did.
                bucket.set_dirty(false);
                // The dump captured everything this bucket had, so it holds no claim over the
                // log until it is written to again.
                bucket.first_dirty_wal_sequence = 0;
                bucket.first_dirty_index_log_sequence = 0;
                for block in bucket.block_index.blocks_mut_unaccounted() {
                    block.dirty = false;
                }
            }
        }
        if !cleared_buckets.is_empty() {
            // STRAIGHT TO THE CLEARED BUCKETS' KEYS.
            //
            // The retain this replaces walked the WHOLE dirty set and re-hashed every key to
            // recompute a routing bucket, whether or not that key belonged to a bucket this dump
            // cleared. A round that dumps a slice of the shard -- which is what
            // `max_dump_buckets_per_round` asks for -- paid for the whole set to drop a slice of
            // it. The dirty index is keyed by bucket, so the keys to drop are addressable and
            // the rest are not touched.
            //
            // The counter still measures what the drain LOOKS AT, which is now exactly what it
            // removes: it visits the cleared buckets' key sets and nothing else.
            let dropped = shard.dirty_objects.drain_buckets(&cleared_buckets);
            DIRTY_DRAIN_VISITS.fetch_add(dropped as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleReport {
        let plan = self.storage_lifecycle_plan(request.clone());
        let dump_manifest = if plan.selected_dump_buckets.is_empty() {
            None
        } else {
            self.create_bucket_dump_manifest(request.shard_id, plan.selected_dump_buckets.clone())
                .ok()
        };
        if let Some(manifest) = &dump_manifest {
            // Once a bucket is dumped its dirty flag is cleared, so the storage cycle
            // stops re-selecting and re-dumping it every round. Dumped state is anchored
            // by the WAL watermark (last_dump_sequence / manifest.wal_sequence, the
            // dumped-log sequence), not by the dirty flag.
            self.clear_dumped_bucket_dirty_state(request.shard_id, manifest);
        }
        let (cache_entries_removed, cache_disk_bytes_removed) = if request.invalidate_cache {
            self.cache
                .invalidate_shard(request.shard_id)
                .map(|report| (report.memory_entries_removed, report.disk_bytes_removed))
                .unwrap_or_default()
        } else {
            (0, 0)
        };
        let cache_warmup = if request.warm_cache {
            self.storage_cache_warmup_report(request.shard_id, plan.selected_dump_buckets.clone())
        } else {
            StorageCacheWarmupReport {
                shard_id: request.shard_id,
                selected_buckets: plan.selected_dump_buckets.clone(),
                ..StorageCacheWarmupReport::default()
            }
        };
        let cache_warmup_block_refs = cache_warmup.warmed_block_refs;
        let purge_report = if request.purge_delayed_destroy {
            // THE LIVE SET AS IT IS NOW, not as it was when the collector quarantined these
            // slabs. The destroy below is irreversible and the two moments are whole rounds
            // apart; between them this same round may have written a dump manifest whose
            // embedded index installs pages in a slab that was unreferenced when the collector
            // looked. Assembled exactly as the operator `/gc` path assembles it -- live page
            // refs across every loaded shard, widened by every slab a retained manifest's index
            // can install -- so the two reclaim paths cannot disagree about what is still needed.
            let mut purge_live_block_slab_ids = self.live_block_slab_ids_all_shards();
            for manifest in
                list_bucket_dump_manifests_shared_at(&self.index_dir, request.shard_id)
                    .unwrap_or_default()
            {
                purge_live_block_slab_ids.extend(manifest.block_slab_ids.iter().copied());
            }
            self.block_store
                .purge_delayed_destroy_slabs_selected(
                    crate::block_store::DELAYED_DESTROY_MIN_AGE_MS,
                    purge_live_block_slab_ids,
                    request
                        .purge_delayed_destroy_slab_ids
                        .as_ref()
                        .map(|ids| ids.iter().copied().collect::<BTreeSet<_>>()),
                )
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        // Roll forward interrupted installs BEFORE pruning: prune removes obsolete
        // install markers, which would otherwise drop an interrupted install before it
        // can be recovered (leaving install_roll_forward_reports empty even though an
        // interrupted install was present).
        let install_roll_forward_reports = if request.roll_forward_bucket_dump_installs {
            self.roll_forward_bucket_dump_installs(request.shard_id)
        } else {
            self.bucket_dump_install_roll_forward_reports(request.shard_id)
        };
        let manifest_prune_report = request.prune_bucket_dump_manifests.then(|| {
            self.apply_bucket_dump_manifest_prune_with_follower_cursors(
                request.shard_id,
                request.follower_replay_cursors.clone(),
            )
        });
        let object_lifecycle = self.storage_object_lifecycle_snapshot(request.shard_id);
        let mut report = StorageLifecycleReport {
            shard_id: request.shard_id,
            public_storage_contract: Default::default(),
            public_storage_feature_shapes: Default::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: Default::default(),
            storage_watermark_snapshot: Default::default(),
            storage_gc_snapshot: Default::default(),
            storage_index_snapshot: Default::default(),
            storage_topology_snapshot: Default::default(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: Default::default(),
            plan,
            dump_manifest,
            cache_entries_removed,
            cache_disk_bytes_removed,
            cache_warmup_block_refs,
            cache_warmup,
            delayed_destroy_purged_slabs: purge_report.purged_block_slab_ids,
            delayed_destroy_purged_bytes: purge_report.purged_physical_bytes,
            delayed_destroy_restored_slabs: purge_report.restored_block_slab_ids,
            delayed_destroy_restored_bytes: purge_report.restored_physical_bytes,
            manifest_prune_plan,
            manifest_prune_report,
            install_roll_forward_reports,
            object_lifecycle,
        };
        report.refresh_public_lifecycle_metrics();
        if let Some(shard) = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
        {
            // ONE walk for all four sampling snapshots.
            //
            // Each of these built its own `collect_live_block_entries` -- a fresh Vec of every live
            // page in the shard -- and then sorted all of it to take EIGHT samples. Four walks and
            // four full sorts, for about thirty-two sample rows. `who_walks_the_shard` measured
            // them at 1.0x the shard apiece, 4.0x of `apply_storage_lifecycle`'s 10.0x.
            //
            // Safe here in the way #1586 was and the dirty-state walk in #1607 was NOT: the read
            // lock above covers all four, `&ShardState` is unchanged throughout, and these produce
            // report SAMPLES rather than a decision. A shared snapshot cannot change what the
            // system does; it only makes the four samples describe one moment instead of four.
            let sampling_entries = collect_live_block_entries(shard);
            report.storage_index_snapshot = storage_index_snapshot_with_samples_from_entries(
                request.shard_id,
                &sampling_entries,
                report.storage_index_snapshot,
            );
            report.storage_watermark_snapshot =
                storage_watermark_snapshot_with_samples_from_entries(
                    request.shard_id,
                    shard,
                    &sampling_entries,
                    report.storage_watermark_snapshot,
                );
            report.storage_gc_snapshot = storage_gc_snapshot_with_samples_from_entries(
                request.shard_id,
                shard,
                &sampling_entries,
                report.storage_gc_snapshot,
            );
            report.storage_topology_snapshot = storage_topology_snapshot_with_samples_from_entries(
                request.shard_id,
                shard,
                &sampling_entries,
                report.storage_topology_snapshot,
            );
        }
        report
    }

    pub fn storage_wal_reclaim_plan(
        &self,
        shard_id: ShardId,
        follower_replay_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> StorageWalReclaimPlan {
        WAL_RECLAIM_PLAN_BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let follower_replay_cursors = follower_replay_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let current_wal_sequence = self.write_ahead_log_store().stats(shard_id).last_sequence;
        let current_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let bucket_summaries = self.bucket_storage_summaries(shard_id);
        let current_bucket_fingerprints = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(|shard| bucket_generation_fingerprints_by_bucket(shard_id, shard))
            .unwrap_or_default();
        // What each bucket holds over the two logs when no manifest covers it.
        //
        // A dirty bucket with no durable dump manifest is captured nowhere, so the only answer
        // the plan could give for it was "retain everything" -- floor 0. These two claims say
        // where the bucket's oldest undumped write actually sits, so it can hold the logs from
        // there instead of from the beginning.
        //
        // AND WHICH BUCKETS ARE DIRTY, off their own nodes, in the SAME lock acquisition and the
        // same walk that is already being made over `bucket_map`. The two checks below ask this
        // instead of `dirty_object_count`, which answered a question about the BUCKET by counting
        // OBJECTS in a side index -- see `dirty_bucket_nodes`. It is read here rather than added to
        // `BucketStorageSummary` for the reason the dump ordering's claim is: a summary is stored
        // in the dump manifest and compared field-by-field when a manifest is validated, so a new
        // field there is a durability contract, not a report field.
        let (bucket_claims, dirty_bucket_nodes) = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(|shard| {
                (
                    shard
                        .bucket_index
                        .bucket_map
                        .iter()
                        .map(|(routing_bucket, bucket)| {
                            (
                                *routing_bucket,
                                (
                                    bucket.first_dirty_wal_sequence,
                                    bucket.first_dirty_index_log_sequence,
                                ),
                            )
                        })
                        .collect::<std::collections::HashMap<u32, (u64, u64)>>(),
                    crate::engine::storage_reporting::dirty_bucket_nodes(shard),
                )
            })
            .unwrap_or_default();
        let manifests =
            list_bucket_dump_manifests_shared_at(&self.index_dir, shard_id).unwrap_or_default();
        let mut missing_bucket_generations = Vec::new();
        let mut retained_manifest_ids = BTreeSet::<String>::new();
        let mut durable_wal_frontier = u64::MAX;
        let mut durable_index_log_frontier = u64::MAX;
        let mut covered_bucket_count = 0usize;

        // Decode each manifest's index ONCE, not once per bucket.
        //
        // This sat inside the loop below, so every bucket re-deserialized every manifest's
        // WHOLE shard index out of JSON and rebuilt the generation fingerprints for every
        // bucket in it -- identical work, repeated once per bucket. The manifests are read
        // before the loop and do not change while the plan is computed, so one pass produces
        // the same answer every visit would have.
        //
        // The cost was quadratic in the corpus: a real (non-dry-run) WAL reclaim took 6.5s at
        // 1k records, 23s at 2k and 100s at 4k -- x4 per doubling -- and a 40k shard did not
        // finish in ten minutes with a core pegged at 100%. Reclaim is the only thing that
        // removes WAL and index-log bytes, so a shard large enough to need it was a shard on
        // which it could not run.
        // Fingerprint a manifest only when a bucket actually reaches it.
        //
        // This was an EAGER pass over every retained manifest, and each entry is a full
        // shard-sized walk: `bucket_generation_fingerprints_by_bucket` calls
        // `collect_live_block_entries`. So the plan paid one whole-shard walk PER RETAINED
        // MANIFEST, every round, before knowing whether any bucket would consult them --
        // measured by `which_stages_walk_every_live_block` as the bulk of reclaim_wal's 16x.
        //
        // The search below is newest-first and short-circuits, and most buckets match the
        // newest manifest, so nearly all of those walks were computed and thrown away.
        //
        // `None` = not computed yet. `Some(None)` = computed, and its index would not decode
        // (which matched nothing before and matches nothing now). `Some(Some(map))` = computed.
        // The memo lives for THIS call only, so there is no cross-round staleness question: a
        // manifest's index bytes cannot change while one plan is being built.
        let mut manifest_fingerprints: Vec<
            Option<Option<BTreeMap<u32, BTreeSet<String>>>>,
        > = (0..manifests.len()).map(|_| None).collect();

        // Index each manifest's bucket summaries BY ROUTING BUCKET, once.
        //
        // The loop below asked `manifest.bucket_summaries.iter().any(..)` per bucket, so a shard
        // with N buckets and a manifest carrying N of them ran N*N comparisons -- and
        // `bucket_dump_summary_matches_current_generation` CLONES AND SORTS both slab vectors on
        // every call. At 32,000 buckets that is about a billion of them: measured, reclaim_wal
        // was 21,448 ms of a 23,310 ms round, on a loop whose period is 30 s.
        //
        // Equivalent by construction: the predicate's FIRST condition is
        // `manifest_summary.routing_bucket == current_summary.routing_bucket`, so only summaries
        // sharing a routing bucket could ever match. Duplicates under one bucket are kept in a
        // vector and still tried with `any`, so a manifest carrying two summaries for one bucket
        // behaves as it did.
        let manifest_summaries_by_bucket = manifests
            .iter()
            .map(|manifest| {
                let mut by_bucket =
                    std::collections::HashMap::<u32, Vec<&BucketStorageSummary>>::new();
                for manifest_summary in &manifest.bucket_summaries {
                    by_bucket
                        .entry(manifest_summary.routing_bucket)
                        .or_default()
                        .push(manifest_summary);
                }
                by_bucket
            })
            .collect::<Vec<_>>();

        for summary in &bucket_summaries {
            // Same search as before -- newest manifest first, first match wins -- written as a
            // loop rather than `find` so the fingerprint memo above can be filled on demand.
            //
            // The CHEAP test now comes first. A manifest carrying no summary for this bucket
            // could never match (the predicate's first condition is that the routing buckets are
            // equal), so asking that before fingerprinting skips the walk entirely for every
            // manifest that does not cover this bucket.
            let mut matching_manifest = None;
            for (manifest_index, manifest) in manifests.iter().enumerate().rev() {
                let Some(candidates) =
                    manifest_summaries_by_bucket[manifest_index].get(&summary.routing_bucket)
                else {
                    continue;
                };
                if manifest_fingerprints[manifest_index].is_none() {
                    manifest_fingerprints[manifest_index] = Some(
                        crate::engine::decode_index_bytes(&manifest.index_bytes)
                            .ok()
                            // A STALE-STAMPED MANIFEST FINGERPRINTS NOTHING, exactly as an
                            // undecodable one already did. Not counted: this is a plan, and it
                            // installs nothing.
                            //
                            // THE SAFE DIRECTION IS "NO MATCH", which is why this is a filter and
                            // not a refusal. A manifest that matches a bucket's current generation
                            // is a manifest the plan believes has that bucket's records durably
                            // dumped, and the log beneath them becomes reclaimable. Fingerprints
                            // derived from an image whose fields may mean something else could
                            // match a bucket no dump actually covers, and the reclaim that follows
                            // is not recoverable. Declining to match costs a dump that is kept
                            // longer than it had to be.
                            .filter(|manifest_state| {
                                !crate::engine::persistence::index_stamp_is_stale(manifest_state)
                            })
                            .map(|manifest_state| {
                                // The shard this plan is FOR. A decoded manifest state carries no
                                // stamp of its own -- only `install_shard_state` sets one -- so
                                // asking IT for a shard yields None and fingerprints nothing.
                                bucket_generation_fingerprints_by_bucket(shard_id, &manifest_state)
                            }),
                    );
                }
                // A manifest whose index will not decode matched nothing before and matches
                // nothing now.
                let Some(manifest_bucket_fingerprints) = manifest_fingerprints[manifest_index]
                    .as_ref()
                    .expect("fingerprint memo filled above")
                    .as_ref()
                else {
                    continue;
                };
                if candidates.iter().any(|manifest_summary| {
                    bucket_dump_summary_matches_current_generation(
                        manifest_summary,
                        summary,
                        manifest_bucket_fingerprints,
                        &current_bucket_fingerprints,
                    )
                }) {
                    matching_manifest = Some(manifest);
                    break;
                }
            }
            let Some(manifest) = matching_manifest else {
                // No manifest covers this bucket. It is still allowed to hold the logs only from
                // its own oldest undumped write, PROVIDED it can name that point in both logs.
                //
                // `retain_from_* = frontier + 1`, so a bucket needing everything from `F` onward
                // contributes `F - 1`: records at or below that are reclaimable, `F` and above
                // are kept.
                //
                // BOTH claims are required. They count in different sequences -- one in the
                // write-ahead log, one in the index log -- and a bucket that can place itself in
                // one but not the other has said nothing about the second. A zero in either means
                // NO CLAIM RECORDED, and the only safe reading of "unknown" is the old one:
                // block, and retain everything. Treating unknown as "nothing to retain" is the
                // direction that loses committed records.
                //
                // A CLEAN bucket is the exception, and getting it wrong is what stopped the
                // reclaim entirely. `dirty_object_count == 0` means the bucket holds no undumped
                // write at all, so there is nothing for the log to retain on its behalf and no
                // claim for it to name -- `first_dirty_wal_sequence` is cleared to 0 in the same
                // breath as `dirty = false`, on a DURABLE dump manifest (see
                // `apply_storage_lifecycle`), and a bucket loaded from disk is durable by
                // construction. Reading that 0 as "cannot name its claim" put every clean bucket
                // into `missing_bucket_generations`, which blocks the whole plan.
                //
                // This is the shape #1516 removed from the manifest branch above -- a bucket that
                // needs NOTHING deciding what the log may drop -- surviving in this branch, where
                // it does not merely pin the floor but refuses outright. On an idle shard every
                // bucket ends up here, so the plan reported
                // `slot_generation_without_durable_dump` for all of them and the log could never
                // be reclaimed again: measured on 8,000 records over twelve rounds, 0 of 10,666
                // records freed and `persistent_bytes` flat at 1,344,852 for ever.
                //
                // Counted as covered and contributing no floor, exactly as the manifest branch
                // treats a clean bucket it does have a manifest for.
                //
                // ASKED OF THE BUCKET'S OWN FLAG. "Holds no undumped write" is a fact the bucket
                // records, and the error direction that matters here is a node reading dirty where
                // the set holds nothing -- that bucket skips this arm, names no claim, and blocks
                // the plan. `whether_the_reclaim_checks_can_read_the_bucket_node` measures that
                // direction EMPTY at every state a dump cycle passes through, including nine
                // partial-dump rounds, which is the case a full dump cannot produce because
                // `clear_dumped_bucket_dirty_state` walks `manifest.bucket_ids` and nothing else.
                if !dirty_bucket_nodes.contains(&summary.routing_bucket) {
                    covered_bucket_count = covered_bucket_count.saturating_add(1);
                    continue;
                }
                let (wal_claim, index_log_claim) = bucket_claims
                    .get(&summary.routing_bucket)
                    .copied()
                    .unwrap_or((0, 0));
                if wal_claim > 0 && index_log_claim > 0 {
                    durable_wal_frontier =
                        durable_wal_frontier.min(wal_claim.saturating_sub(1));
                    durable_index_log_frontier =
                        durable_index_log_frontier.min(index_log_claim.saturating_sub(1));
                    covered_bucket_count = covered_bucket_count.saturating_add(1);
                } else {
                    missing_bucket_generations.push(summary.routing_bucket);
                }
                continue;
            };
            retained_manifest_ids.insert(manifest.manifest_id.clone());
            covered_bucket_count = covered_bucket_count.saturating_add(1);
            // EXPERIMENT: a bucket that is CLEAN has no undumped write, so it needs nothing
            // retained on its behalf and must not hold the floor.
            //
            // Asked of the bucket's own flag, the same record the covered arm above reads, so the
            // two arms of this plan cannot disagree about which buckets are dirty.
            if dirty_bucket_nodes.contains(&summary.routing_bucket) {
                durable_wal_frontier = durable_wal_frontier.min(manifest.wal_sequence);
                durable_index_log_frontier =
                    durable_index_log_frontier.min(manifest.index_log_sequence);
            }
        }

        let mut blocker_reasons = Vec::new();
        if bucket_summaries.is_empty() {
            blocker_reasons.push("no_slot_generations_to_anchor_reclaim".to_string());
            durable_wal_frontier = 0;
            durable_index_log_frontier = 0;
        }
        if !missing_bucket_generations.is_empty() {
            blocker_reasons.push("slot_generation_without_durable_dump".to_string());
        }

        if durable_wal_frontier == u64::MAX {
            // EXPERIMENT 2: no bucket held the floor, which means every bucket is dumped and
            // nothing needs the log retained -- so the floor is the CURRENT position, not zero.
            // Zero here reads as "retain everything" and is what took the plan unsafe in the
            // first experiment.
            //
            // THE INVARIANT THIS LINE HAS TO KEEP, and the one place in the plan that could
            // break it. Everywhere else the frontier is a MINIMUM over bucket dump manifests,
            // and `load_shard_with` raises its own replay point to the LATEST of those same
            // manifests -- a minimum over a set cannot exceed a member of it, so the floor can
            // never climb above the point a load starts replaying from. Here the frontier comes
            // from the current log position instead, which no load path consults. It is safe
            // because a cycle DUMPS (`prepare`) before it RECLAIMS (`reclaim_wal`), so a
            // manifest at this position already exists by the time this is read.
            //
            // What makes that load-bearing rather than incidental: the default load path folds
            // no index-log deltas (#1644), so the expiry round's delta (#1633) can advance the
            // served anchor well past the base index FILE's -- measured 9 against 1 -- and
            // reclaim now drops whole segment files (#1622). A floor above the replay point
            // would free records that a load still has to replay, with nothing else holding
            // them. `wal_reclaim_never_frees_what_the_default_load_path_replays`
            // (engine/tests/expiry_scale.rs) asserts the relation against a real load's
            // recorded watermark at every state a production cycle passes through.
            durable_wal_frontier = current_wal_sequence;
        }
        if durable_index_log_frontier == u64::MAX {
            // EXPERIMENT 2, the index-log half, same reasoning.
            durable_index_log_frontier = current_index_log_sequence;
        }
        let mut follower_cursor_block_count = 0usize;
        for cursor in follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
        {
            if cursor.wal_sequence < durable_wal_frontier
                || cursor.index_log_sequence < durable_index_log_frontier
            {
                follower_cursor_block_count = follower_cursor_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "follower_cursor_retains_logs:{}",
                    cursor.follower_id
                ));
            }
        }

        let mut raft_snapshot_block_count = 0usize;
        for snapshot in raft_snapshot_refs
            .iter()
            .filter(|snapshot| snapshot.shard_id == shard_id)
        {
            if snapshot.wal_sequence < durable_wal_frontier
                || snapshot.index_log_sequence < durable_index_log_frontier
            {
                raft_snapshot_block_count = raft_snapshot_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "raft_snapshot_retains_logs:{}",
                    snapshot.snapshot_id
                ));
            }
        }
        // Two different questions were being answered by one boolean.
        //
        // WHETHER the frontier can be trusted: every live generation needs a durable dump behind
        // it, or the lowest manifest sequence does not describe what is actually on disk. These
        // stay absolute -- there is no safe partial answer to a frontier that is wrong.
        let generations_durable = missing_bucket_generations.is_empty()
            && covered_bucket_count == bucket_summaries.len()
            && durable_wal_frontier > 0
            && durable_index_log_frontier > 0;

        // HOW FAR it may be followed: a retention cursor marks what some reader has still to
        // consume. Everything at or below the SLOWEST cursor is behind every reader and can go
        // whether or not that cursor ever advances. Refusing at the cursor instead of clamping to
        // it meant one lagging follower pinned the entire log for as long as it lagged, and the
        // log grew without bound underneath it.
        //
        // The floor is a minimum over followers AND snapshot refs together: they are separate
        // lists but the same question, and taking them apart would let one advance past the other
        // and drop a log the slower one still needs.
        let cursor_wal_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.wal_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.wal_sequence),
            )
            .min();
        let cursor_index_log_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.index_log_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.index_log_sequence),
            )
            .min();

        // Never above the durable frontier, and never above the slowest cursor. With no cursors at
        // all the frontier stands unchanged, which is what it did before.
        let effective_wal_frontier = cursor_wal_floor
            .map_or(durable_wal_frontier, |floor| durable_wal_frontier.min(floor));
        let effective_index_log_frontier = cursor_index_log_floor.map_or(
            durable_index_log_frontier,
            |floor| durable_index_log_frontier.min(floor),
        );

        // A clamp to zero reclaims nothing, which is the right answer for a cursor that has never
        // advanced -- the win here is exactly the span a reader has already consumed, and for a
        // permanently stuck follower that span is empty.
        let safe_to_reclaim =
            generations_durable && effective_wal_frontier > 0 && effective_index_log_frontier > 0;
        let retain_from_wal_sequence = if safe_to_reclaim {
            effective_wal_frontier.saturating_add(1)
        } else {
            0
        };
        let retain_from_index_log_sequence = if safe_to_reclaim {
            effective_index_log_frontier.saturating_add(1)
        } else {
            0
        };

        StorageWalReclaimPlan {
            shard_id,
            safe_to_reclaim,
            durable_bucket_generation_frontier_wal_sequence: durable_wal_frontier,
            durable_bucket_generation_frontier_index_log_sequence: durable_index_log_frontier,
            retain_from_wal_sequence,
            retain_from_index_log_sequence,
            current_wal_sequence,
            current_index_log_sequence,
            covered_bucket_count,
            uncovered_bucket_count: missing_bucket_generations.len(),
            follower_cursor_block_count,
            raft_snapshot_block_count,
            missing_bucket_generations,
            retained_manifest_ids: retained_manifest_ids.into_iter().collect(),
            blocker_reasons,
        }
    }

    pub fn apply_storage_wal_reclaim(
        &self,
        plan: StorageWalReclaimPlan,
    ) -> StorageWalReclaimReport {
        if !plan.safe_to_reclaim {
            return StorageWalReclaimReport {
                plan,
                applied: false,
                ..StorageWalReclaimReport::default()
            };
        }
        // The plan reached `safe_to_reclaim` by finding a durable bucket-dump manifest for
        // every live generation and taking the LOWEST wal sequence among them, so the durable
        // index reflects everything at or below that frontier.
        //
        // This stays the DURABLE frontier, not the cursor-clamped one. The anchor is an upper
        // bound on what may be dropped; `retain_from_wal_sequence` may sit below it because a
        // retention cursor clamped it, and dropping less than the anchor permits is safe. Passing
        // the clamped value here would prove less durability than has actually been established
        // and would be the wrong number for a different reason.
        let durable_index = crate::wal::DurableIndexAnchor::proven_durable_through(
            plan.shard_id,
            plan.durable_bucket_generation_frontier_wal_sequence,
        );
        let wal_gc = self
            .write_ahead_log_store()
            .gc_before_sequence(plan.shard_id, plan.retain_from_wal_sequence, &durable_index)
            .ok();
        StorageWalReclaimReport {
            applied: wal_gc.is_some(),
            wal_records_removed: wal_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            wal_bytes_before: wal_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            wal_bytes_after: wal_gc
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            index_log_records_removed: 0,
            index_log_bytes_before: self.index_log_store.stats(plan.shard_id).bytes_written,
            index_log_bytes_after: self.index_log_store.stats(plan.shard_id).bytes_written,
            // Whole segments dropped by this pass. The gc report has measured them all along;
            // stopping here meant a pass that unlinked 64 files and freed 28 MB reported
            // `wal_records_removed: 0` and byte counts covering only the active segment.
            wal_segments_dropped: wal_gc
                .as_ref()
                .map(|report| report.dropped_segments)
                .unwrap_or_default(),
            wal_segment_bytes_dropped: wal_gc
                .as_ref()
                .map(|report| report.dropped_segment_bytes)
                .unwrap_or_default(),
            plan,
        }
    }

    /// Reclaim the index log on the same terms the background cycle uses.
    ///
    /// [`Self::storage_index_gc_report`] is engine-internal and takes a cycle request for its
    /// thresholds, which is why the periodic scheduler could not reach it: the index log was
    /// truncated only by the cycle endpoint, an explicit `/gc` request, or the embedded proxy's
    /// own reclaim thread. This is the entry that closes that, and it takes the CYCLE's defaults
    /// rather than inventing a second policy -- including
    /// `index_gc_commit_dirty_buckets_before_truncation`, which defaults TRUE and is the safe
    /// order: an index-log record names where a block's bytes live, so dropping one the durable
    /// state does not yet reflect loses the LOCATION of data that is still on disk, which reads
    /// as missing rather than as corruption.
    ///
    /// The plan is recomputed from `request` AFTER the caller's dump has run, which is the order
    /// that makes the safety check meaningful: if that dump cleared the dirty set, the plan now
    /// selects nothing and truncation is safe on its own terms. A stale plan would ask about
    /// buckets that have already been committed.
    pub fn apply_periodic_index_gc(
        &self,
        request: StorageLifecycleRequest,
        lifecycle_report: Option<&StorageLifecycleReport>,
        max_entries_per_round: usize,
    ) -> StorageIndexGcReport {
        let shard_id = request.shard_id;
        // The cursors the CALLER supplied, not an empty pair.
        //
        // This read `Vec::new(), Vec::new()` while holding a request that carries both lists, so a
        // caller that knew about a reader had it silently dropped for the index-log decision --
        // and only for that decision, since the prune below this does honour the same request's
        // cursors. One request, two answers, disagreeing about who is still reading.
        //
        // `retain_from_index_log_sequence` off this plan is what `storage_index_gc_report` hands
        // to `gc_before_sequence_limited`, so the dropped clamp was the difference between keeping
        // an index-log prefix and unlinking it.
        //
        // Cloned before `request` moves into the plan below. `page_gc_raft_snapshot_refs` is where
        // the cycle puts its `raft_snapshot_refs` when it builds this request
        // (`storage_manager_cycle.rs`), so it is the same list under the name this struct uses.
        let follower_replay_cursors = request.follower_replay_cursors.clone();
        let raft_snapshot_refs = request.block_gc_raft_snapshot_refs.clone();
        let plan = self.storage_lifecycle_plan(request);
        let wal_plan =
            self.storage_wal_reclaim_plan(shard_id, follower_replay_cursors, raft_snapshot_refs);
        self.storage_index_gc_report(
            &plan,
            &wal_plan,
            lifecycle_report,
            &StorageManagerCycleRequest {
                shard_id,
                index_gc_max_entries_per_round: max_entries_per_round,
                ..StorageManagerCycleRequest::default()
            },
        )
    }

    pub(super) fn storage_index_gc_report(
        &self,
        plan: &StorageLifecyclePlan,
        wal_plan: &StorageWalReclaimPlan,
        lifecycle_report: Option<&StorageLifecycleReport>,
        request: &StorageManagerCycleRequest,
    ) -> StorageIndexGcReport {
        // THE CHEAP QUESTIONS FIRST.
        //
        // Everything below this point measures the index log, and this report is built on EVERY
        // maintenance round -- every 30 s on the periodic loop -- whether or not the collector can
