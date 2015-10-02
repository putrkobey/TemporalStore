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

    /// Install a hash field whose block carries NO element frame, the way every block written before
    /// `container_blocks` existed does.
    ///
    /// The one thing a compatibility test needs and cannot otherwise have: bytes in a slab with no
    /// magic in front of them, named by a block index entry and by the resident map, so the product's
    /// own read path resolves them. Everything here is what `Command::HashSet` does, minus the frame
    /// -- the same object id expression, the same routing bucket, the same two registrations -- so
    /// what it installs is the old shape rather than an approximation of it.
    ///
    /// Returns whether the block was written, so a fixture cannot pass by having installed nothing.
    #[cfg(test)]
    pub(crate) fn install_unframed_hash_block_for_test(
        &self,
        shard_id: ShardId,
        object_key: &str,
        field: &str,
        value: &[u8],
    ) -> bool {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return false;
        };
        let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
        // The OBJECT's id, not the element's -- #2019 made that the only shape. The element is
        // named by the component argument below, as it is on the real write path.
        let object_id = super::stable_block_object_id(shard_id, "hash", object_key);
        let routing_bucket = super::hashing::block_routing_bucket(
            object_key,
            start_routing_bucket,
            end_routing_bucket,
        );
        let Ok(address) = super::append_value(
            &self.cache,
            &self.block_store,
            shard_id,
            value,
            Some(object_id),
            Some(field),
            Some(routing_bucket),
            false,
        ) else {
            return false;
        };
        super::recorded_map::install_element::<super::recorded_map::HashKind>(
            shard,
            shard_id,
            object_key,
            Some(field.to_string()),
            std::sync::Arc::from(&field[..]),
            address.clone(),
            true,
            address,
        );
        true
    }

    /// How many recorded outcomes recovery has installed (see `replay_installs`).
    pub(crate) fn replay_installs_for_test(&self) -> u64 {
        self.replay_installs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total per-execute promote reconcile scans this engine has run (see `promote_scans`).
    pub fn promote_scan_count(&self) -> u64 {
        self.promote_scans.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cache(&self) -> MultiLayerCache {
        self.cache.clone()
    }

    pub fn block_store(&self) -> BlockStore {
        self.block_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use block_store; page naming remains only for legacy compatibility"
    )]
    pub fn page_store(&self) -> BlockStore {
        self.block_store()
    }

    pub fn write_ahead_log_store(&self) -> LocalWriteAheadLogStore {
        self.wal_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use write_ahead_log_store; wal naming remains only for legacy compatibility"
    )]
    pub fn wal_store(&self) -> LocalWriteAheadLogStore {
        self.write_ahead_log_store()
    }

    pub fn index_log_store(&self) -> LocalIndexLogStore {
        self.index_log_store.clone()
    }

    pub(crate) fn ingestion_dir(&self) -> PathBuf {
        self.index_dir.join("ingestion")
    }

    pub fn with_local_dirs(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::with_local_dirs_and_block_store_options(
            memory_capacity_bytes,
            cache_dir,
            block_store_dir,
            index_dir,
            BlockStoreOptions::default(),
        )
    }

    pub fn with_local_dirs_and_block_store_options(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        block_store_options: BlockStoreOptions,
    ) -> Self {
        Self::with_local_dirs_block_store_options_and_disk_cache(
            memory_capacity_bytes,
            cache_dir,
            block_store_dir,
            index_dir,
            block_store_options,
            true,
        )
    }

    /// As above, but `disk_cache_tier == false` builds a MEMORY-ONLY cache.
    ///
    /// The tiering policy's disk capacities default to 512 MB of pmem and 16 GB of SSD, so a
    /// node that never wanted a disk tier still preallocates for one. Zeroing both leaves the
    /// memory tier untouched and stops the disk tier existing at all -- which is what takes the
    /// embedded key-value store out of the process, since it is only there to back that tier.
    pub fn with_local_dirs_block_store_options_and_disk_cache(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        block_store_options: BlockStoreOptions,
        disk_cache_tier: bool,
    ) -> Self {
        let cache = if disk_cache_tier {
            MultiLayerCache::new(memory_capacity_bytes, cache_dir)
        } else {
            MultiLayerCache::with_tiering_policy(
                cache_dir,
                matrixcache::CacheTieringPolicy {
                    memory_capacity_bytes,
                    pmem_capacity_bytes: 0,
                    ssd_capacity_bytes: 0,
                    ..Default::default()
                },
                matrixcache::CacheBlockOptions::default(),
            )
        };
        Self::with_cache_block_store_and_index_dir(
            cache,
            BlockStore::with_options(block_store_dir, block_store_options),
            index_dir,
        )
    }

    /// Load a shard on the WHOLE KEYSPACE, which is NO LONGER THE PRODUCTION DEFAULT.
    ///
    /// `startup_load_shard_request` -- the one path a deployment loads a shard through -- defaults
    /// `end_routing_bucket` to [`crate::DEFAULT_END_ROUTING_BUCKET`]. This convenience keeps the
    /// whole keyspace, and the divergence is DELIBERATE rather than an oversight:
    ///
    ///   * several hundred tests call this, and the range decides which arm of `BlockIndexMap`
    ///     every one of them exercises. Moving it would change the shape under all of them at once,
    ///     which is a different change from moving the shipped default; and
    ///   * a fixture that wants the production shape should say so, because at the whole keyspace
    ///     EVERY KEY LANDS ALONE IN ITS OWN BUCKET BY CONSTRUCTION -- the modulus is 4.29 billion.
    ///     #1959 measured "almost every bucket holds exactly one block" off a fixture on this
    ///     function and read it as a property of the workload.
    ///
    /// SO IF YOU ARE MEASURING ANYTHING PER BUCKET, DO NOT USE THIS. Call `load_shard_with` and
    /// name the range. `the_convenience_load_is_deliberately_not_the_production_default` holds the
    /// divergence as an assertion so it cannot drift into an accident.
    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        let _ = self.load_shard_with(request);
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return LoadShardResponse {
                status: Status::error("already_exists", "shard already exists"),
            };
        }

        // THE ROUTING RANGE IS SETTLED HERE, BEFORE ANY DECODE, and it is the one place it can be.
        //
        // A block's bucket is the key's hash modulo the RANGE WIDTH, so a store written on one range
        // holds its blocks under buckets another range never computes -- and nothing re-files them,
        // because the write path stamps an explicit bucket onto every address. Measured: 2,000 of
        // 2,000 blocks of a store written on the whole keyspace come back filed ABOVE the end of a
        // shard reopened on `0..1023`, every record still readable and every block outside the
        // dump's bucket selection, eviction's victim sampling, the reclaim floor and the release
        // pass. After the decode those blocks are already in the wrong buckets in memory, which is
        // why this runs first and why a mismatch is a REFUSAL rather than a warning.
        //
        // See `engine/routing_range_stamp.rs` for the three cases and why the pre-stamp store is
        // honoured rather than refused.
        let mut request = request;
        match crate::engine::routing_range_stamp::decide_routing_range(
            &self.index_dir,
            request.shard_id,
            request.start_routing_bucket,
            request.end_routing_bucket,
        ) {
            crate::engine::routing_range_stamp::RoutingRangeDecision::Refuse {
                code,
                message,
            } => {
                return LoadShardResponse {
                    status: Status::error(code, message),
                };
            }
            crate::engine::routing_range_stamp::RoutingRangeDecision::Load {
                start_routing_bucket,
                end_routing_bucket,
                write_stamp,
            } => {
                // NO OVERRIDE ARM. A load either runs on the range it asked for or was refused
                // above, so there is no range the caller has to be told it did not choose. The
                // warning this replaces said a pre-stamp store "can only have been built on the
                // whole keyspace", which was true of the default and false of the configuration.
                request.start_routing_bucket = start_routing_bucket;
                request.end_routing_bucket = end_routing_bucket;
                if write_stamp {
                    if let Err(error) = crate::engine::routing_range_stamp::write_routing_range_stamp(
                        &self.index_dir,
                        request.shard_id,
                        crate::engine::routing_range_stamp::RoutingRangeStamp {
                            start_routing_bucket,
                            end_routing_bucket,
                        },
                    ) {
                        // A stamp that cannot be written is not fatal to THIS load -- the range in
                        // hand is the right one -- but the NEXT load would read no stamp, see
                        // on-disk state, and honour the whole keyspace instead. Loud, because that
                        // is a silent re-ranging one restart away.
                        tracing::error!(
                            shard_id = request.shard_id,
                            start_routing_bucket,
                            end_routing_bucket,
                            %error,
                            "could not record this shard's routing range; a later load will not \
                             know the range this store was built on"
                        );
                    }
                }
            }
        }
        let request = request;

        #[cfg(test)]
        restore_phase_probe::begin();
        let (loaded, replay_watermark) = if wal_single_barrier() {
            // SINGLE-BARRIER RECOVERY TRUST (base-only). The data-block + delta fdatasyncs are
            // deferred, so neither the served-index delta nor the anchor it advances can be
            // trusted -- they may reference blocks that were never fsync'd. Recover ONLY from
            // durable checkpoints:
            //  * the base index snapshot, materialized durably (fsync) at the last dump/unload --
            //    flush_shard_index fsyncs every block BEFORE advancing its watermark, so every block
            //    at/below the base watermark is on disk; and
            //  * the latest durable dump manifest, if newer than the base file.
            // Then replay the WAL tail from that durable watermark, re-deriving every block written
            // after it (a lost un-synced block is rebuilt, never left dangling) and, via the
            // config-log, re-applying config-driven eviction at the exact frontier. The delta is
            // deliberately NOT folded (load_index_base_only), so each tail record is applied
            // EXACTLY ONCE -- no double-apply of non-idempotent commands (counters, appends).
            let base_only =
                self.load_index_base_only(request.shard_id, self.eager_cache_warm());
            let base_watermark = base_only
                .as_ref()
                .and_then(|state| state.applied_wal_sequence)
                .unwrap_or(0);
            // THIS ARM IS ALSO WHAT MAKES WAL RECLAIM SAFE, which is not obvious from here.
            // `storage_wal_reclaim_plan` takes its floor from the SAME bucket dump manifests
            // (a minimum over them), so the floor can only stay at or below the point this
            // raises the replay to. After an expiry round the base index FILE can sit far behind
            // -- measured, anchor 1 against a delta at 9, with the log already cut to sequence 9
            // -- and it is the manifest read here, not the base file, that covers the difference.
            // Narrowing this arm narrows what a load can reconstruct without narrowing what
            // reclaim is willing to drop.
            // `wal_reclaim_never_frees_what_the_default_load_path_replays`
            // (engine/tests/expiry_scale.rs) fails if the two ever come apart.
            //
            // AND IT HAS TO BE THE MAXIMUM OVER THEM, NOT THE LATEST OF THEM. This read
            // `latest_bucket_dump_manifest_at`, which orders by `index_log_sequence`. That is a
            // MEMBER of the set the floor minimises over but not an UPPER BOUND on it, and the
            // two orderings genuinely come apart: a dump minted while `MATRIXARK_BULK_INGEST` is
            // set takes its `wal_sequence` from the frozen base FILE, so it lands newest in
            // index-log order carrying the LOWEST WAL anchor on disk. Measured on one shard in
            // one process: the floor stood at 9 (retain_from 10) while a load off these same
            // files replayed from 1, leaving sequences (1, 9] -- the expiry tombstones among them
            // -- both reclaimable and required. A MAXIMUM bounds every subset by construction.
            // See `durable_recovery_bucket_dump_manifest_at`.
            match durable_recovery_bucket_dump_manifest_at(&self.index_dir, request.shard_id) {
                Some(manifest) if manifest.wal_sequence > base_watermark => {
                    // A durable dump is newer than the base file (base not materialized at that
                    // dump). Use the manifest's embedded durable index as the recovery base. Read
                    // it directly (not install_bucket_dump_manifest) so the stale-manifest guard --
                    // which refuses to install a manifest older than the delta-advanced index-log
                    // sequence -- cannot block trusting the durable checkpoint over the un-synced
                    // delta.
                    //
                    // THE INDEX-LOG-SEQUENCE GUARD IS WHAT THAT BYPASSES, AND THE FORMAT STAMP IS
                    // NOT. The two are different questions and only one of them is about how far
                    // along the log a manifest sits. `index_log_sequence` says the delta may be
                    // ahead of this checkpoint, which is exactly the thing this arm is right to
                    // ignore -- the un-synced delta is what it does not trust.
                    // `index_format_version` says the BYTES mean something other than what this
                    // binary will read them as, and no amount of durability makes a
                    // misinterpreted field correct. So the arm below refuses on the stamp and
                    // falls back to the base file and the log, which is the same trade
                    // `load_index_inner` makes.
                    //
                    // This path is the reason the stamp needed covering at all: it is reached by
                    // an ORDINARY restart, not by anyone calling an install endpoint, so an old
                    // store with an old dump manifest on disk met no gate whatsoever.
                    match crate::engine::decode_index_bytes(&manifest.index_bytes) {
                        // Counted in the body rather than in the guard, so the guard stays a pure
                        // question: a guard that moved a counter would fire for a pattern that
                        // then fell through to the arm below it.
                        Ok(restored)
                            if crate::engine::persistence::index_stamp_is_stale(&restored) =>
                        {
                            crate::engine::persistence::refuse_stale_index_stamp(&restored);
                            (base_only, base_watermark)
                        }
                        Ok(mut restored) => {
                            // RE-DERIVE THE UNSERIALIZED MODEL MAPS BEFORE REBUILDING OWNERSHIP,
                            // for the reason `install_bucket_dump_manifest` states at its own call
                            // to the same pair. Three of the maps
                            // `collect_model_live_block_entries` walks are `skip_serializing`, so a
                            // manifest index decodes with them EMPTY while its `bucket_map` --
                            // which does serialize -- still names every block they owned.
                            // `rebuild_bucket_block_ownership` clears `bucket_map` and repopulates
                            // it FROM the model maps, so on its own here it deleted exactly the
                            // blocks only the index still knew about, and this restored state is
                            // what becomes the shard.
                            //
                            // Measured before this line existed, on the DEFAULT recovery arm: a
                            // shard dumped holding six hash fields and six strings came back
                            // serving 0 of 6 hash fields and 6 of 6 strings. The install path has
                            // always done this; only this arm did not.
                            //
                            // `hashes` is the map this re-derives. `context_events` and
                            // `context_indexes` are keyed by timestamp and their index entries
                            // carry `component: None`, so their keys are not recoverable from the
                            // index and they stay dropped here -- the same hole the install path
                            // carries, recorded at
                            // `rebuild_unserialized_model_maps_from_bucket_index`.
                            rebuild_unserialized_model_maps_from_bucket_index(&mut restored);
                            // THE WHOLE RANGE, for the same reason `install_bucket_dump_manifest`
                            // keeps it: `restored` is a DECODED MANIFEST INDEX, a whole-shard image
                            // carrying explicit routing buckets from whatever range wrote it, and
                            // `rebuild_bucket_block_ownership` FILTERS on the range it is given. A
                            // load that narrows the range would silently drop every block the
                            // manifest holds outside it rather than re-file it. The promote below
                            // this match passes `request`'s range and should: it runs over this
                            // shard's own live model maps, not over a foreign image.
                            rebuild_bucket_block_ownership(
                                request.shard_id,
                                &mut restored,
                                0,
                                u32::MAX,
                            );
                            (Some(restored), manifest.wal_sequence)
                        }
                        Err(_) => (base_only, base_watermark),
                    }
                }
                _ => (base_only, base_watermark),
            }
        } else {
            // If the latest durable dump manifest is newer than the served index, install
            // it as the load base first (recovers data already dumped into a manifest and
            // then WAL-GC'd): the dumped index is restored first, then
            // startup load replays the WAL on top.
            let installed_manifest_watermark =
                match self.install_latest_manifest_if_newer_on_load(request.shard_id) {
                    Ok(watermark) => watermark,
                    // An index-load failure is fatal. A newer durable manifest
                    // that will not install means the served snapshot + (possibly reclaimed)
                    // WAL cannot be trusted to hold the records it covers -- refuse the load.
                    Err(status) => return LoadShardResponse { status },
                };
            // Fold-aware load: a corrupt served-index delta log is fatal here. Silently
            // folding a holed delta prefix would advance the anchor past a removal/eviction
            // recorded only in the delta -> silent loss. Refuse the load instead.
            let loaded = match self.load_index_checked(request.shard_id, self.eager_cache_warm()) {
                Ok(loaded) => loaded,
                Err(status) => return LoadShardResponse { status },
            };
            // WAL replay watermark, from the dumped-log id read on startup load:
            // installed manifest -> its wal_sequence; no index
            // file -> 0 (fresh/async-only shard, replay whole retained WAL); anchored index
            // -> its anchor; unanchored (pre-field) index -> current last_sequence (replay
            // nothing, safe upgrade).
            let replay_watermark = match installed_manifest_watermark {
                Some(manifest_watermark) => manifest_watermark,
                None => match &loaded {
                    None => 0,
                    Some(state) => state
                        .applied_wal_sequence
                        .unwrap_or_else(|| self.wal_store.stats(request.shard_id).last_sequence),
                },
            };
            (loaded, replay_watermark)
        };
        // What this load decided to SKIP. A recovery that loses a write loses it by choosing a
        // watermark, and that choice is invisible from outside: the shard loads clean, no error
        // is returned, and the value is simply absent afterwards. Recorded so a failing test can
        // say which watermark it was rather than leaving it to be guessed at.
        #[cfg(test)]
        restore_phase_probe::mark("manifest_and_base_index");
        #[cfg(test)]
        LAST_REPLAY_WATERMARK.store(replay_watermark, std::sync::atomic::Ordering::SeqCst);
        // MANIFEST-CONFORMANCE FOLD recovery (gate on only): seed the slab catalog from the folded
        // slab-catalog anchor recovered from the index-log. This is applied AFTER the block
        // store already reconciled its catalog from the durable blocks on open, so it only
        // RESTORES the catalog fields a pure disk scan cannot infer (exact lifecycle state,
        // creation/update timestamps, logical byte count, block-id range) and installs slabs for
        // reclaimed slabs with no live file. It never deletes a reconciled slab and never lowers
        // physical bytes below the slab's real size, so it cannot lose durable state -- it is a
        // metadata refinement over the lossless disk-derived catalog, making the per-write
        // slab-manifest file unnecessary as the catalog's source of truth.
        if let Ok(Some(meta)) = self.index_log_store.latest_slab_catalog(request.shard_id) {
            let _ = self.block_store.install_slab_catalog(&meta.slabs);
        }
        let mut state = loaded.unwrap_or_default();
        promote_model_maps_to_bucket_index_authority(
            request.shard_id,
            &mut state,
            request.start_routing_bucket,
            request.end_routing_bucket,
        );
        // A promote that found the index already in agreement returns without rebuilding, so it
        // is not on its own a seed. This is: every shard entering the engine gets its maintained
        // per-slab live tally derived once, here, before anything can read it.
        crate::engine::storage_bucket_internals::seed_block_slab_live(&mut state);
        // Publish the info row WITH recovering:true BEFORE inserting into `shards`. A
        // concurrent execute() acquires shards.write() first, so if it observes the shard
        // present it is guaranteed (happens-before via the shards lock) to also observe
        // recovering:true and reject the write. Inserting into `shards` first would open a
        // window where the shard is visible but the info row is absent -> the gate defaults
        // to false -> a concurrent write interleaves with replay (the double-apply this gate
        // exists to prevent).
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_bucket: request.start_routing_bucket,
                end_routing_bucket: request.end_routing_bucket,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
                // Serving is gated off until replay below completes.
                recovering: true,
            },
        );
        self.install_shard_state(request.shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(request.shard_id)
            .or_default();
        // Config is not carried in the served-index checkpoint, so restore the last durably-logged
        // config before replay REGARDLESS of barrier mode. This covers the no-replay path (a clean
        // dump with nothing to tail-replay) and post-restart client writes; the replay loop below
        // overrides it with the WAL-sequence-ordered config while re-driving historical records,
        // then restores the latest again. Without this, a reload defaults `Config` and silently
        // resets feature_max_size + the representation-changing extend gate flags.
        if let Some(entry) = self.config_log_entries(request.shard_id).into_iter().last() {
            self.configs
                .write()
                .expect("config lock poisoned")
                .insert(request.shard_id, entry.config);
        }
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(request.shard_id))
            .or_default();
        // Replay any WAL records not yet reflected in the loaded index, rebuilding
        // in-memory state the way startup load replays the wal. Without
        // this an async_storage write (WAL entry recorded, block/index deferred to the
        // dump) is silently lost on restart if the crash beats the dump.
        // Hand the resolver the log ids the index has been carrying. Without this a block whose
        // only durable copy is a WAL record stays unreadable by address after a reload -- the
        // served index points at a synthetic address and the resolver's table starts empty.
        self.rehydrate_wal_resident_blocks(request.shard_id);
        #[cfg(test)]
        restore_phase_probe::mark("publish_and_seed");
        if let Err(status) = self.replay_wal_into_shard(request.shard_id, replay_watermark) {
            // ReplayWal returns DataLoss on a WAL hole and aborts Load. Unwind the
            // partially-loaded shard and refuse the load rather than serve truncated
            // state -- a not-loaded shard is recoverable/re-routable; silent truncation
            // is not.
            self.shards
                .write()
                .expect("engine lock poisoned")
                .remove(&request.shard_id);
            self.infos
                .write()
                .expect("info lock poisoned")
                .remove(&request.shard_id);
            self.configs
                .write()
                .expect("config lock poisoned")
                .remove(&request.shard_id);
            self.admissions
                .write()
                .expect("admission lock poisoned")
                .remove(&AdmissionScope::Shard(request.shard_id));
            return LoadShardResponse { status };
        }
        // Replay succeeded and the shard is consistent: open it for serving. While
        // `recovering` was set, client commands were rejected with a retryable status so no
        // write could interleave with replay and regress the WAL anchor.
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            info.recovering = false;
        }
        // Disk->memory promotion on a normal restart is folded directly into
        // load_index()/reconcile above (gated by this engine's `eager_cache_warm`): the blocks
        // reconcile reads to rebuild the secondary views are promoted into the cache
        // tier in the same pass, so we avoid a second warm pass re-reading every block
        // under the mutex-serialized block store. No-op on a fresh/empty shard.
        #[cfg(test)]
        restore_phase_probe::mark("open_for_serving");
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// If the latest durable bucket-dump manifest is newer than the served index,
    /// install it (validate + preflight + restore embedded index) and return its
    /// wal_sequence as the WAL replay watermark. `Ok(None)` when nothing newer exists.
    /// `Err` when a newer manifest IS present but will not install: treats
    /// an index-load failure as fatal, because once the served snapshot
    /// lags the manifest the intervening WAL records may already be reclaimed, so a
    /// silent fall-back to the stale snapshot would drop them. The caller refuses the
    /// load instead.
    ///
    /// DELIBERATELY STILL `latest_bucket_dump_manifest_at`, unlike the single-barrier arm above.
    /// This is the `TS_WAL_LEGACY_RECOVERY` path, and it FOLDS the index-log deltas, so
    /// `served_anchor` below is the delta-advanced anchor -- at or above every manifest's
    /// `wal_sequence`, because a manifest's anchor is read out of an index export and the served
    /// index is what those exports are taken from. A manifest carrying a LOW anchor therefore
    /// loses the `<= served_anchor` test and is skipped, and the replay point falls back to the
    /// served anchor, which is the highest number available. The divergence that breaks the
    /// base-only arm cannot lower this one.
    pub(super) fn install_latest_manifest_if_newer_on_load(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<u64>, Status> {
        let Some(manifest) = latest_bucket_dump_manifest_at(&self.index_dir, shard_id) else {
            return Ok(None);
        };
        // The CHECKED loader, deliberately -- not the tolerant `load_index`.
        //
        // `load_index` is documented as the tolerant wrapper for probe callers: it turns a corrupt
        // served-index delta into `None`. Read through it, this anchor became 0 on exactly that
        // corruption, every manifest then compared as newer than a served index of 0, and the
        // install below overwrote the durable index -- after which `load_shard_with` reached its
        // own `load_index_checked` and refused the load anyway. So the load failed AFTER
        // destroying the thing it was refusing to read, and the refusal is the case where the
        // durable index matters most.
        //
        // The outcome is unchanged for every other input: this returns the same
        // `index_log_delta_corruption` status that `load_shard_with` would have returned a few
        // lines later, so a corrupt delta still refuses the load. It just refuses BEFORE the
        // install rather than after it. An absent or undecodable BASE index is still `Ok(None)`
        // in the loader and still anchors at 0 -- that is a fresh shard, not a swallowed error,
        // and installing a manifest over it is the whole point of this function.
        //
        // The paragraph above about `served_anchor` being the delta-advanced anchor, at or above
        // every manifest's `wal_sequence`, holds only when the delta actually loaded. It reasons
        // about where a successful fold leaves the anchor and says nothing about the case where
        // there is no fold to reason about, which is the case this guards.
        let served_anchor = self
            .load_index_checked(shard_id, false)?
            .and_then(|state| state.applied_wal_sequence)
            .unwrap_or(0);
        if manifest.wal_sequence <= served_anchor {
            return Ok(None);
        }
        match self.install_bucket_dump_manifest(&manifest) {
            Ok(()) => Ok(Some(manifest.wal_sequence)),
            Err(status) => Err(status),
        }
    }

    /// Replay WAL records with sequence greater than `watermark`, re-driving each
    /// through execute (which rebuilds the bucket index + model maps) WITHOUT
    /// re-appending to the WAL or re-persisting the index per record, then anchor and
    /// persist the reconstructed index once. Matches the WAL replay path,
    /// including its strict sequence-continuity check.
    /// A deterministic rendering of the state outcomes are supposed to reproduce.
    ///
    /// Deliberately not the serialized index: that carries bookkeeping -- the applied watermark,
    /// the log-resident block map -- which legitimately differs between a shard that ran the
    /// commands and one that installed their results. Comparing the maps themselves says
    /// whether the DATA matches, and a mismatch prints as a readable diff rather than as two
    /// blobs of unequal bytes.
    /// The live block entries the bucket index holds, rendered deterministically.
    ///
    /// The typed maps are not the whole shard. The bucket index is durable state that the read
    /// path consults, so a rebuild that gets the maps right and this wrong is still wrong -- and
    /// comparing only the maps would never say so.
    ///
    /// Volatile bookkeeping is deliberately left out: dirty flags, dirty generations and dump
    /// sequences describe what still needs writing out, not what the shard holds, and they
    /// legitimately differ between a shard built by commands and one rebuilt from records.
    pub(super) fn bucket_index_shape_for_test(&self, shard_id: ShardId) -> String {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return String::from("<no shard>");
        };
        let mut out = String::new();
        let identities = shard.bucket_index.block_identities();
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            for (block_ref_key, block) in &bucket.block_index {
                // A page whose neighbours do not name an object prints an empty key and a zero
                // id, which is what this shape has to say about a page nothing names.
                let beside = identities.identity_of(bucket, *block_ref_key, block);
                let object_key = beside
                    .as_ref()
                    .map_or("", |beside| beside.object_key.as_ref());
                let object_id = beside
                    .as_ref()
                    .map_or(0, |beside| block.object_id(shard_id, beside.object_key));
                out.push_str(&format!(
                    "bucket={routing_bucket} ref={block_ref_key} kind={} key={} object_id={} slab={} off={} len={} deleted={} log_backed={}
",
                    block.model_id,
                    object_key,
                    object_id,
                    block.address.block_slab_id(),
                    block.address.offset(),
                    block.address.length(),
                    block.deleted,
                    block.log_backed(),
                ));
            }
        }
        out
    }

    pub(super) fn index_shape_for_test(&self, shard_id: ShardId) -> String {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return String::from("<no shard>");
        };
        let mut out = String::new();
        let mut strings: Vec<_> = shard.strings().iter().collect();
        strings.sort_by(|left, right| left.0.cmp(right.0));
        for (key, address) in strings {
            out.push_str(&format!(
                "string {key} slab={} off={} len={}\n",
                address.block_slab_id(), address.offset(), address.length()
            ));
        }
        for (key, deadline) in shard.expires_at_ms() {
            out.push_str(&format!("expires {key} at={deadline}\n"));
        }
        let mut seen: Vec<_> = shard.seen.iter().collect();
        seen.sort_by(|left, right| left.0.cmp(right.0));
        for (key, set) in seen {
            let mut members: Vec<_> = set.by_member.iter().collect();
            members.sort();
            for (member, at) in members {
                out.push_str(&format!("seen {key} member={member:?} at={at}\n"));
            }
        }
        // Every timestamped series renders the same way, so a kind that stops being installed
        // shows up as a missing line rather than as a map nobody compares.
        let mut entities: Vec<_> = shard.context_entities().iter().collect();
        entities.sort_by(|left, right| left.0.cmp(right.0));
        for (key, members) in entities {
            // `points()` on the slot, and the hash by VALUE: this kind's ordered key is an entity
            // hash rather than an instant, which is why the loop variable keeps its name.
            for (entity_hash, address) in members.points() {
                out.push_str(&format!(
                    "context_entity {key} id={entity_hash} slab={} off={} len={}
",
                    address.block_slab_id(), address.offset(), address.length()
                ));
            }
        }
        let mut counter_blocks: Vec<_> = shard.control_state_blocks().iter().collect();
        counter_blocks.sort_by(|left, right| left.0.cmp(right.0));
        for (key, record) in counter_blocks {
            let address = record.address();
            out.push_str(&format!(
                "control_state_page {key} slab={} off={} len={}
",
                address.block_slab_id(), address.offset(), address.length()
            ));
        }
        let mut counters: Vec<_> = shard.control_state().iter().collect();
        counters.sort_by(|left, right| left.0.cmp(right.0));
        for (key, series) in counters {
            for (bucket_ms, total) in series {
                out.push_str(&format!("control_counter {key} at={bucket_ms} total={total}
"));
            }
        }
        let mut selections: Vec<_> = shard.control_state_selection.iter().collect();
        selections.sort_by(|left, right| left.0.cmp(right.0));
        for (key, selected) in selections {
