// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::engine::timestamped_series::TimestampedSeriesWrite as _;
use super::*;
// FOR THE SLOT'S SERIES SURFACE: `ObjectSlot` implements these, and the names it keeps
// (`len`, `get`, `is_empty`) already matched the call sites -- only the trait was out of
// scope here. Anonymous, because only the methods are wanted.
use crate::engine::timestamped_series::TimestampedSeries as _;

impl Default for TemporalEngine {
    fn default() -> Self {
        Self::with_cache_and_block_store(MultiLayerCache::default(), BlockStore::default())
    }
}

/// THE TWO THINGS THE APPLY PATH DOES TO A TIMESTAMPED SERIES, each dispatched by kind.
///
/// These replace a single function that handed out `&mut HashMap<String, BTreeMap<u64, _>>` for
/// six kinds. That shape was one place, which is what its note said it was for -- but it was one
/// place that named a CONTAINER, so the six could not be narrowed one at a time: the tree recorded
/// exactly that in `the_bucket_index_holds_one_block_out_of_line_and_the_series_maps_hold_none`,
/// where it was a compile-time proof rather than a remark.
///
/// Dispatching the OPERATION instead keeps the single place and removes the coupling. Each arm
/// names its own map, so a kind whose series changes representation changes its own arm and no
/// other, and no caller can be handed a series whose shape it does not know -- there is no series
/// handed out at all.
fn remove_timestamped_series(
    shard: &mut super::state::ShardState,
    kind: &str,
    object_key: &str,
) -> bool {
    match kind {
        "feature" => shard.features_mut().remove(object_key).is_some(),
        "context_index" => shard.context_indexes_mut().remove(object_key).is_some(),
        "context_audit" => shard.context_audits_mut().remove(object_key).is_some(),
        "context_child" => shard.context_children_mut().remove(object_key).is_some(),
        "context_summary" => shard.context_summaries_mut().remove(object_key).is_some(),
        "context_compression" => shard.context_compressions_mut().remove(object_key).is_some(),
        // NOT A TIMESTAMPED KIND. Distinguished from "the key was not there" by the caller, which
        // treats an unknown kind as a refusal to apply and an absent key as nothing to do.
        _ => return false,
    };
    true
}

/// Set or clear one point of one object's series, answering with the addresses that SURVIVE.
///
/// `None` means the kind is not a timestamped series kind. An empty `Vec` means it is, and the
/// object has no points left -- which the shared operation has already taken the key out of the
/// map for.
fn apply_timestamped_point(
    shard: &mut super::state::ShardState,
    kind: &str,
    object_key: &str,
    at: u64,
    address: Option<crate::block_store::ElementEntry>,
) -> Option<Vec<crate::block_store::ElementEntry>> {
    use super::timestamped_series::set_point_for_owned_key as apply;
    Some(match kind {
        "feature" => apply(shard.features_mut(), object_key, at, address),
        "context_index" => apply(shard.context_indexes_mut(), object_key, at, address),
        "context_audit" => apply(shard.context_audits_mut(), object_key, at, address),
        "context_child" => apply(shard.context_children_mut(), object_key, at, address),
        "context_summary" => apply(shard.context_summaries_mut(), object_key, at, address),
        "context_compression" => apply(shard.context_compressions_mut(), object_key, at, address),
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) static LAST_REPLAY_WATERMARK: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Was the serving gate SHUT at the moment WAL replay began? Recorded per replay.
///
/// `recovering` is set by two production sites -- `load_shard_with` on the load path, and
/// `install_bucket_dump_manifest` when a dump lands on an already-loaded shard -- and it is read
/// by `execute`, by the stream-batch apply, by the storage-manager cycle and by the reclaim RPC.
/// Nothing asserted that EITHER site actually shuts it. Every test that exercises the gate
/// publishes the info row itself through `test_publish_recovering_shard`, which is a fixture that
/// sets the flag by hand, so the claim under test was supplied by the test: both production sites
/// were changed to publish `recovering: false` and the whole suite stayed green.
///
/// Recorded at the one point both paths funnel through, so a guard can say "the gate was shut
/// while the real replay ran" about the code that ships rather than about a fixture. A read
/// arriving in that window is answered from an index that describes the shard as it was at the
/// checkpoint, and a write arriving in it interleaves with replay and regresses the anchor.
#[cfg(test)]
pub(crate) static REPLAY_ENTERED_WITH_SERVING_GATE_SHUT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// WHAT EACH PHASE OF A RESTORE COST, recorded where `load_shard_with` moves between them.
///
/// A restore is four jobs in a row -- read the durable checkpoint, publish the shard, replay the
/// WAL tail, fold that tail back into the index -- and from outside it is one call that either
/// answers `Ok` or does not. Nothing said which of the four an expensive restart spent its
/// syscalls in, so nothing could say where to look when a shard takes too long to come back.
///
/// Two quantities are recorded at each boundary:
///
///   * allocations and elapsed nanoseconds, in process, from the counting allocator and the
///     monotonic clock. Neither costs a syscall, so the probe does not perturb the syscall count
///     it sits beside.
///   * a MARKER: one `statx` of a path that does not exist and never will, naming the phase that
///     just ended. `strace -c` reports one total for a process and offers no way to attribute it
///     to a phase; an ordered trace SPLIT at these markers gives per-phase sections whose counts
///     sum to that total by construction, which is the only attribution with a residual of zero.
///
/// Test-only, and inert until `begin()` arms it -- the ordinary suite loads shards constantly and
/// pays one relaxed atomic load per boundary for this.
#[cfg(test)]
pub(crate) mod restore_phase_probe {
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// What one phase of one restore cost.
    #[derive(Clone, Debug, Default)]
    pub(crate) struct PhaseCost {
        pub(crate) phase: String,
        pub(crate) allocs: u64,
        pub(crate) alloc_bytes: u64,
        pub(crate) nanos: u64,
    }

    /// Records the walk decoded, and of those, the ones dropped because the checkpoint
    /// already covers them. Counted where the walk hands each record over -- BEFORE the
    /// sequence test branches, so neither arm can be the one that misses the increment.
    pub(crate) static RECORDS_DECODED: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    pub(crate) static RECORDS_BEHIND_CHECKPOINT: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// One record came off the walk. `behind` is whether the checkpoint already covers it.
    pub(crate) fn record_decoded(behind: bool) {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        RECORDS_DECODED.fetch_add(1, Ordering::Relaxed);
        if behind {
            RECORDS_BEHIND_CHECKPOINT.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct ProbeState {
        at: std::time::Instant,
        allocs: u64,
        alloc_bytes: u64,
        phases: Vec<PhaseCost>,
        /// Write syscalls counted while the engine write lock was held over the index fold,
        /// and after it was released. See `lock_held_begin`.
        writes_under_engine_lock: u64,
        writes_after_engine_lock: u64,
        lock_entry_writes: u64,
        lock_exit_writes: u64,
    }

    thread_local! {
        static STATE: RefCell<Option<ProbeState>> = const { RefCell::new(None) };
    }

    /// Armed process-wide so the cheap path is a relaxed load rather than a thread-local access.
    /// A restore runs on the thread that called it, so the state itself stays thread-local and two
    /// concurrent loads cannot mix their phases.
    static ARMED: AtomicBool = AtomicBool::new(false);

    /// Where the allocation numbers come from, and what a zero from here means.
    ///
    /// `counted_now` answers `None` when the counting allocator is not installed, which is every
    /// build without `alloc-probe`. That is folded to zero here so the phase arithmetic stays one
    /// shape, and the FACT is carried out separately on `RestoreCost::allocations_were_counted`
    /// -- so a zero in a phase row can always be told apart from a build that was not counting.
    /// The tests that read these numbers assert that flag before reading them.
    fn allocs_now() -> (u64, u64) {
        crate::alloc_probe::counted_now().unwrap_or((0, 0))
    }

    /// Write syscalls this process has issued, from `/proc/self/io`.
    ///
    /// `syscw` and not `wchar`: the question this answers is how many times the durable write
    /// path was ENTERED under a lock, which a byte count cannot distinguish from one large write.
    /// Reading this file costs a read syscall and no write syscall, so sampling it does not move
    /// the number being sampled.
    fn write_syscalls_now() -> u64 {
        let Ok(text) = std::fs::read_to_string("/proc/self/io") else {
            return 0;
        };
        for line in text.lines() {
            if let Some(value) = line.strip_prefix("syscw:") {
                return value.trim().parse().unwrap_or(0);
            }
        }
        0
    }

    /// One `statx` naming a path that cannot exist, so an external trace can be cut here.
    ///
    /// The path is absolute and rooted at a name no store directory uses, so the call resolves
    /// to ENOENT in one syscall without touching anything the restore is reading.
    fn marker(phase: &str) {
        let _ = std::fs::symlink_metadata(format!("/ts-restore-phase-marker/{phase}"));
    }

    /// Arm the probe and open the first phase. Idempotent per thread.
    pub(crate) fn begin() {
        let (allocs, alloc_bytes) = allocs_now();
        STATE.with(|cell| {
            *cell.borrow_mut() = Some(ProbeState {
                at: std::time::Instant::now(),
                allocs,
                alloc_bytes,
                phases: Vec::new(),
                writes_under_engine_lock: 0,
                writes_after_engine_lock: 0,
                lock_entry_writes: 0,
                lock_exit_writes: 0,
            });
        });
        RECORDS_DECODED.store(0, Ordering::Relaxed);
        RECORDS_BEHIND_CHECKPOINT.store(0, Ordering::Relaxed);
        ARMED.store(true, Ordering::Relaxed);
        marker("restore_begin");
    }

    /// Close the phase that just ran and open the next.
    ///
    /// The marker is emitted AFTER the in-process sample, so the syscalls a trace attributes to a
    /// phase are exactly the ones the allocation and time deltas cover.
    pub(crate) fn mark(phase: &str) {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let (allocs, alloc_bytes) = allocs_now();
        let now = std::time::Instant::now();
        let mut emit = false;
        STATE.with(|cell| {
            let mut slot = cell.borrow_mut();
            let Some(state) = slot.as_mut() else {
                return;
            };
            state.phases.push(PhaseCost {
                phase: phase.to_string(),
                allocs: allocs.saturating_sub(state.allocs),
                alloc_bytes: alloc_bytes.saturating_sub(state.alloc_bytes),
                nanos: now.duration_since(state.at).as_nanos() as u64,
            });
            state.at = now;
            state.allocs = allocs;
            state.alloc_bytes = alloc_bytes;
            emit = true;
        });
        if emit {
            marker(phase);
        }
    }

    /// The engine write lock has just been taken over the post-replay index fold.
    ///
    /// The fold rebuilds the bucket index and SERIALISES the whole shard under this lock, and the
    /// durable write of those bytes happens after it is released. Whether that is still true is
    /// not visible from the call site -- the lock is a block scope and the persist is the next
    /// statement -- so it is counted here instead of argued about.
    pub(crate) fn lock_held_begin() {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let writes = write_syscalls_now();
        STATE.with(|cell| {
            if let Some(state) = cell.borrow_mut().as_mut() {
                state.lock_entry_writes = writes;
            }
        });
    }

    /// The engine write lock has just been released.
    pub(crate) fn lock_held_end() {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let writes = write_syscalls_now();
        STATE.with(|cell| {
            if let Some(state) = cell.borrow_mut().as_mut() {
                state.writes_under_engine_lock = writes.saturating_sub(state.lock_entry_writes);
                state.lock_exit_writes = writes;
            }
        });
    }

    /// The durable write that follows the fold has finished.
    pub(crate) fn after_engine_lock() {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let writes = write_syscalls_now();
        STATE.with(|cell| {
            if let Some(state) = cell.borrow_mut().as_mut() {
                state.writes_after_engine_lock = writes.saturating_sub(state.lock_exit_writes);
            }
        });
    }

    /// What the restore cost, phase by phase, and the two halves of the lock span.
    pub(crate) fn finish() -> RestoreCost {
        marker("restore_end");
        ARMED.store(false, Ordering::Relaxed);
        STATE.with(|cell| {
            let taken = cell.borrow_mut().take();
            match taken {
                Some(state) => RestoreCost {
                    allocations_were_counted: crate::alloc_probe::counted_now().is_some(),
                    phases: state.phases,
                    writes_under_engine_lock: state.writes_under_engine_lock,
                    writes_after_engine_lock: state.writes_after_engine_lock,
                    records_decoded: RECORDS_DECODED.load(Ordering::Relaxed),
                    records_behind_checkpoint: RECORDS_BEHIND_CHECKPOINT
                        .load(Ordering::Relaxed),
                },
                None => RestoreCost::default(),
            }
        })
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct RestoreCost {
        /// Whether the counting allocator was installed for this restore. False means every
        /// `allocs` and `alloc_bytes` below is a placeholder, not a measurement.
        pub(crate) allocations_were_counted: bool,
        pub(crate) phases: Vec<PhaseCost>,
        pub(crate) writes_under_engine_lock: u64,
        pub(crate) writes_after_engine_lock: u64,
        /// Records the windowed walk decoded during this restore.
        pub(crate) records_decoded: u64,
        /// Of those, the ones the durable checkpoint already covers, decoded and then dropped.
        pub(crate) records_behind_checkpoint: u64,
    }

    impl RestoreCost {
        pub(crate) fn allocs(&self) -> u64 {
            self.phases.iter().map(|phase| phase.allocs).sum()
        }

        pub(crate) fn alloc_bytes(&self) -> u64 {
            self.phases.iter().map(|phase| phase.alloc_bytes).sum()
        }

        pub(crate) fn nanos(&self) -> u64 {
            self.phases.iter().map(|phase| phase.nanos).sum()
        }

        pub(crate) fn phase(&self, name: &str) -> PhaseCost {
            self.phases
                .iter()
                .find(|phase| phase.phase == name)
                .cloned()
                .unwrap_or_default()
        }
    }
}

/// Counts of the times a replay had to fall back to the recorded outcome's own element name,
/// because the page could not be asked.
///
/// A DETECTOR ON A FALLBACK, WHICH IS THE ONE SHAPE WORTH COUNTING. The page is meant to be the
/// authority; every one of these is a write whose page cannot name its own element, so the count is
/// the size of the class that still depends on the outcome carrying the name. It is also what says
/// whether a test exercised the fallback at all -- a fallback nothing reaches is a fallback nothing
/// has checked.
static OUTCOME_NAME_FROM_BLOCK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static OUTCOME_NAME_FROM_COMPONENT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static OUTCOME_NAME_DISAGREED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (from the page, from the outcome's own copy, refused for disagreeing).
pub fn outcome_element_name_sources() -> (u64, u64, u64) {
    (
        OUTCOME_NAME_FROM_BLOCK.load(std::sync::atomic::Ordering::Relaxed),
        OUTCOME_NAME_FROM_COMPONENT.load(std::sync::atomic::Ordering::Relaxed),
        OUTCOME_NAME_DISAGREED.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Reset the three, so a test measures its own replays and not another test's.
pub fn reset_outcome_element_name_sources() {
    OUTCOME_NAME_FROM_BLOCK.store(0, std::sync::atomic::Ordering::Relaxed);
    OUTCOME_NAME_FROM_COMPONENT.store(0, std::sync::atomic::Ordering::Relaxed);
    OUTCOME_NAME_DISAGREED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// What the page an outcome names could say about its own elements.
///
/// `Unreadable` carries WHY, because the reason is what the refusal message has to print and what
/// the next reader of a failed load needs. An `Option` here lost it once already.
enum OutcomeBlockElements {
    /// The frame named these elements, with each one's removed flag.
    Named(Vec<(String, bool)>),
    /// The page could not be asked, for this reason.
    Unreadable(&'static str),
}

impl OutcomeBlockElements {
    fn why(&self) -> &'static str {
        match self {
            OutcomeBlockElements::Named(_) => "names its elements",
            OutcomeBlockElements::Unreadable(why) => why,
        }
    }

    /// THE PRECEDENCE, IN ONE PLACE: the page first, the outcome's own copy as the fallback, and a
    /// refusal only when neither can name the element or when both can and they DISAGREE.
    ///
    /// # WHY THE PAGE WINS WHERE IT CAN ANSWER
    ///
    /// It is the durable statement of what the page holds, and it is the thing a later derivation
    /// will read. Where it answers, nothing else is needed.
    ///
    /// # WHY THE OUTCOME'S COPY IS A FALLBACK AND NOT DEAD
    ///
    /// Because there are reachable writes whose page cannot name its element -- `--bins`'
    /// `matrixark_publish_visibility_can_target_only_written_keys` is one, and the first version of
    /// this code refused its load rather than reading a name that was sitting in the record. A
    /// fallback is not a guess: the outcome's copy is what the write itself recorded about the
    /// element it was about.
    ///
    /// # AND A DISAGREEMENT IS LOUD, WHICH IS THE CLAUSE THAT EARNS ITS KEEP
    ///
    /// If the page names elements and the outcome's component is not among them, then the page in
    /// hand is not the page this outcome is about -- which is the `object_id` pairing defect
    /// recorded in `the_shared_install_pairs_carried_blocks_by_object_and_component`
    /// arriving with evidence. Installing the page's names would file this element's address under
    /// another element's name; installing the component's would file another page's bytes under
    /// this one. Both are corruption, so neither is chosen and the load is refused.
    ///
    /// `want_removed` selects the half the caller is about: an insert installs the LIVE items, a
    /// removal drops the ones the frame marks gone. The disagreement check looks at ALL of them,
    /// because the component names its element whichever state the page records it in.
    fn element_names(
        &self,
        item: &crate::wal::WalOutcomeItem,
        want_removed: bool,
    ) -> Option<Vec<String>> {
        match self {
            OutcomeBlockElements::Named(named) => {
                if let Some(component) = item.component.as_deref() {
                    if !named.iter().any(|(name, _)| name == component) {
                        OUTCOME_NAME_DISAGREED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return None;
                    }
                }
                let wanted: Vec<String> = named
                    .iter()
                    .filter(|(_, removed)| *removed == want_removed)
                    .map(|(name, _)| name.clone())
                    .collect();
                if wanted.is_empty() {
                    return None;
                }
                OUTCOME_NAME_FROM_BLOCK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(wanted)
            }
            OutcomeBlockElements::Unreadable(_) => {
                let component = item.component.clone()?;
                OUTCOME_NAME_FROM_COMPONENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(vec![component])
            }
        }
    }
}

impl TemporalEngine {
    pub fn new(cache: MultiLayerCache) -> Self {
        Self::with_cache_and_block_store(cache, BlockStore::default())
    }

    pub fn with_cache_and_block_store(cache: MultiLayerCache, block_store: BlockStore) -> Self {
        let scratch = crate::scratch::owned_scratch_dir("indexes");
        let mut engine =
            Self::with_cache_block_store_and_index_dir(cache, block_store, scratch.path());
        engine.index_scratch = Some(scratch);
        engine
    }

    pub fn with_cache_block_store_and_index_dir(
        cache: MultiLayerCache,
        block_store: BlockStore,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        let index_dir = index_dir.into();
        let wal_store = LocalWriteAheadLogStore::new(index_dir.join("wals"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        // Install the durable read-by-address fallback for log-backed hot blocks: an eviction
        // handler spills an evicted hot block to a real slab (freeing DRAM + preventing the acked
        // write from reading back as missing). Idempotent + a no-op when disabled.
        hot_block_spill::install_spill_handler(&cache, &block_store);
        Self {
            shards: Arc::default(),
            cache,
            block_store,
            wal_store,
            index_log_store,
            index_dir,
            index_scratch: None,
            configs: Arc::default(),
            infos: Arc::default(),
            admissions: Arc::default(),
            promote_scans: Arc::default(),
            replay_installs: Arc::default(),
            maintenance_mirror: Arc::default(),
            quotas: Arc::new(RwLock::new(crate::engine::quota::QuotaTable::default())),
            compaction_rounds: Arc::default(),
            recovery_probe_cursors: Arc::default(),
            concurrent_commit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            expiry_index_flush_under_lock: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warm_cache_under_shard_guard: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            expiry_index_flush_whole: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // Sampled by default. The exhaustive alternative calls `bucket_storage_summaries`,
            // which reads EVERY live block in the shard to rank every bucket, and then keeps at
            // most `eviction_batch_limit` of them -- so the cost of choosing grew with the store
            // while the work done stayed fixed. The sampler walks a bounded window from a cursor
            // (`samples * batch_limit * scan_turns`) and resumes where it stopped, so a store
            // twice the size costs the same to choose from.
            //
            // The sampled path was written, tested and measured, and the only thing that ever
            // turned it on was a #[cfg(test)] helper.
            evict_sampled_lru: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            eager_cache_warm: Arc::new(std::sync::atomic::AtomicBool::new(
                crate::engine::eager_cache_warm_on_load(),
            )),
            raft_apply_coalesce: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            logged_deletion_index_flush_interval: Arc::new(std::sync::atomic::AtomicU64::new(
                crate::engine::logged_deletion_index_flush_interval_from_env(),
            )),
            visibility_publish_index_flush_interval: Arc::new(
                std::sync::atomic::AtomicU64::new(
                    crate::engine::visibility_publish_index_flush_interval_from_env(),
                ),
            ),
        }
    }

    /// Stop this engine's cache spilling evicted hot blocks to a real slab, so a read has to come
    /// from the log-resident path.
    ///
    /// Five tests want this, and `TS_HOT_PAGE_SPILL=0` was how they asked. Four could not work:
    /// `install_spill_handler` reads that variable during construction and every one of them set
    /// it afterwards, so the handler was already installed and the isolation they described never
    /// happened. `register_eviction_callback` REPLACES the callback, so a no-op takes the handler
    /// off the engine this test built, at the moment it asks.
    #[cfg(test)]
    pub(crate) fn disable_hot_block_spill_for_test(&self) {
        self.cache.register_eviction_callback(|_| {});
    }

    /// Warm this engine's page cache in the BACKGROUND rather than during `load_shard`, so a
    /// restart publishes serving state before the promotion pass finishes.
    ///
    /// A caller that holds the engine says this to the engine. The proxy used to say it to the
    /// process, by setting `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD` and never putting it back.
    pub fn warm_cache_in_background_on_load(&self) {
        self.eager_cache_warm
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether `load_shard` warms the cache as part of the load.
    pub(crate) fn eager_cache_warm(&self) -> bool {
        self.eager_cache_warm
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Take the durable WAL barrier UNDER the `shards` write lock, for a test measuring what
    /// the other side of the lock costs. Scoped to this engine.
    #[cfg(test)]
    /// Pick eviction victims by a sampled scan. The default, kept so a test can say it means
    /// the sampled path rather than relying on what the default happens to be.
    #[cfg(test)]
    pub(crate) fn use_sampled_eviction_for_test(&self) {
        self.evict_sampled_lru
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Rank every bucket by reading every live block, instead of sampling a window.
    ///
    /// No longer the default. Kept because the measurement that justifies the default needs
    /// something to measure against: a test that cannot produce the exhaustive scan cannot show
    /// that the sampled one is cheaper.
    #[cfg(test)]
    pub(crate) fn use_full_scan_eviction_for_test(&self) {
        self.evict_sampled_lru
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn commit_under_lock_for_test(&self) {
        self.concurrent_commit
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Flush the expiry sweep's served-index checkpoint while the shard-table write guard is
    /// still held, the way it was done before the flush was moved out. Scoped to this engine.
    ///
    /// Kept because the measurement that justifies the shorter hold needs something to measure
    /// against: a test that cannot produce the in-lock flush cannot show that it is gone.
    #[cfg(test)]
    pub(crate) fn flush_expiry_index_under_lock_for_test(&self) {
        self.expiry_index_flush_under_lock
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read the warm-up's blocks while the shard-table read guard is still held, the way it was
    /// done before the reads were moved out. Scoped to this engine.
    ///
    /// Kept for the same reason: a guard asserting zero reads under the lock is vacuous unless
    /// an arm in the same process, on the same fixture, can still produce a non-zero one.
    #[cfg(test)]
    pub(crate) fn warm_cache_under_shard_guard_for_test(&self) {
        self.warm_cache_under_shard_guard
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many logically deleted keys this engine lets a shard accumulate before its next
    /// served-index checkpoint re-encodes the whole base index.
    ///
    /// THE ONE READER OF THE SETTING. A test that sets the interval reads it back through this
    /// same accessor and asserts what it got, because a knob a round consults by a different name
    /// than the test sets leaves the arm silently not running.
    pub(crate) fn logged_deletion_index_flush_interval(&self) -> u64 {
        self.logged_deletion_index_flush_interval
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set this engine's logged-deletion checkpoint interval. Scoped to this engine.
    ///
    /// `1` is the per-round flush this interval replaced, kept reachable for the same reason the
    /// whole-index expiry checkpoint below is: a claim that a cost is gone is only falsifiable
    /// against an engine that still pays it, measured in the same process on the same fixture.
    #[cfg(test)]
    pub(crate) fn set_logged_deletion_index_flush_interval_for_test(&self, keys: u64) {
        self.logged_deletion_index_flush_interval
            .store(keys.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// How many published keys this engine lets a shard accumulate before its next served-index
    /// checkpoint re-encodes the whole base index.
    ///
    /// THE ONE READER OF THE SETTING. A test that sets the interval reads it back through this
    /// same accessor and asserts what it got, because a knob a publish consults by a different
    /// name than the test sets leaves the arm silently not running.
    pub(crate) fn visibility_publish_index_flush_interval(&self) -> u64 {
        self.visibility_publish_index_flush_interval
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set this engine's visibility-publish checkpoint interval. Scoped to this engine.
    ///
    /// `1` is the per-publish encode this interval replaced, kept reachable for the same reason
    /// the logged-deletion arm above is: a claim that a cost is gone is only falsifiable against
    /// an engine that still pays it, measured in the same process on the same fixture.
    #[cfg(test)]
    pub(crate) fn set_visibility_publish_index_flush_interval_for_test(&self, keys: u64) {
        self.visibility_publish_index_flush_interval
            .store(keys.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// Make the expiry sweep checkpoint the WHOLE served index, the way it did before the
    /// checkpoint became a delta of what the round removed. Scoped to this engine.
    ///
    /// Kept for the same reason the in-lock flush is kept: a claim about a cost that is gone
    /// needs an arm that still pays it. `an_expiry_round_persists_what_changed` runs both in one
    /// process against the same fixture, so the flat bytes it asserts are measured against a
    /// number that still grows with the shard rather than against nothing.
    #[cfg(test)]
    pub(crate) fn flush_whole_expiry_index_for_test(&self) {
        self.expiry_index_flush_whole
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Apply a committed raft batch one entry at a time, each with its own barrier, for a test
    /// measuring what coalescing saves. Scoped to this engine.
    #[cfg(test)]
    pub(crate) fn apply_raft_batch_per_entry_for_test(&self) {
        self.raft_apply_coalesce
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// The RAW BLOCK an element's index entry names, before any element is selected out of it.
    ///
    /// The one hook a guard needs to assert that a write actually FRAMED its block. Without it the
    /// end-to-end test is vacuous in the one direction that matters: an unframed block reads back
    /// correctly -- that is the whole compatibility design -- so a write path that quietly stopped
    /// framing would pass every value comparison in the suite and be found only by a later stage
    /// that could no longer rebuild an element from its block.
    ///
    /// Goes through `read_block_frame_bytes` and not `read_block_bytes`, deliberately: the latter
    /// is the door that selects, and asking it would hand back the element's value with the frame
    /// already gone.
    #[cfg(test)]
    pub(crate) fn container_block_frame_for_test(
        &self,
        shard_id: ShardId,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) -> Option<Vec<u8>> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id)?;
        let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
        // A NAMED ELEMENT'S ADDRESS COMES FROM THE RESIDENT MAP.
        //
        // This asked `bucket_index_block_address`, which requires
        // `page.component.as_deref() == component` on every branch. An entry carries no element
        // name, so for a container element that resolved NOTHING and this door answered `None` for
        // elements that were present and served:
        // `container_block_element_key::every_container_kind_reads_back_the_value_it_wrote_through_a_
        // framed_page` failed at "no stored page for hash hash-key component field-0000".
        //
        // The resident map is keyed BY the element and is where the production readers that used to
        // resolve this way now go, so this door follows them rather than keeping a resolution the
        // engine no longer has. The index is still asked as a FALLBACK, because it is the door for
        // a kind that has no resident element map -- and because a caller naming a component for
        // such a kind should get the same answer it always did.
        let address = match component {
            Some(component) => {
                super::state::resident_component_address(shard, model_id, object_key, component)
                    .cloned()
                    .or_else(|| {
                        super::bucket_store::bucket_index_block_address(
                            shard,
                            model_id,
                            object_key,
                            Some(component),
                        )
                    })?
            }
            None => super::bucket_store::bucket_index_block_address(
                shard, model_id, object_key, None,
            )?,
        };
        let routing_bucket = super::hashing::block_routing_bucket(
            object_key,
            start_routing_bucket,
            end_routing_bucket,
        );
        super::read_block_frame_bytes(
            &self.cache,
            &self.block_store,
            shard_id,
            &address,
            // The same three terms `bucket_index_block_address` was just asked, above.
            BlockIdentity::of(shard_id, model_id, object_key, component),
            Some(routing_bucket),
        )
    }

