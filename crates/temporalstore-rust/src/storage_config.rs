// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

pub const TS_CONTEXT_BLOCK_TARGET_BYTES: &str = "TS_CONTEXT_BLOCK_TARGET_BYTES";
/// Previous name for [`TS_CONTEXT_BLOCK_TARGET_BYTES`], still honoured so a deployment that
/// sets it keeps working. Read only when the current name is unset.
///
/// The constant was renamed to `BLOCK` and kept the `PAGE` spelling as its VALUE, so the name in
/// the source and the name an operator sets had drifted apart -- the same drift
/// [`TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME`] below was written to close.
pub const TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_CONTEXT_PAGE_TARGET_BYTES";
pub const TS_BLOCK_SLAB_TARGET_BYTES: &str = "TS_BLOCK_SLAB_TARGET_BYTES";
pub const TS_STREAM_MAX_BLOB_SIZE: &str = "TS_STREAM_MAX_BLOB_SIZE";
pub const TS_COMPACTION_WATERMARK_BYTES: &str = "TS_COMPACTION_WATERMARK_BYTES";
pub const TS_COLD_SCAN_NO_CACHE_FILL: &str = "TS_COLD_SCAN_NO_CACHE_FILL";
// RETIRED: TS_PAGE_INDEX_CACHE_BYTES and TS_BLOCK_INDEX_CACHE_BYTES.
//
// Both named a budget for an index cache that does not exist: an address lookup is answered
// from the DRAM-resident index with no cache call, and the record cache is bounded by
// TS_CACHE_MEMORY_BYTES. Each parsed into a setting whose only production reader put it in the
// tuning report, so setting either moved one reported number and nothing else -- measured two
// ways before removal, with both set to 1 byte and with a panic planted on the reads.
//
// Nothing reads either name now, so a deployment that still sets one is accepted and ignored
// rather than refused: there is no read to fail. The shipped config loader behaves the same way
// and was measured doing it -- an unmapped key is reported `unmapped config keys (ignored)` and
// the process exits 0. Held true by `the_retired_index_cache_budgets_are_read_by_nothing`.
pub const TS_INDEX_DUMP_WAL_GAP_BYTES: &str = "TS_INDEX_DUMP_WAL_GAP_BYTES";
/// Previous name for [`TS_INDEX_DUMP_WAL_GAP_BYTES`], still honoured so a deployment that
/// sets it keeps working. Read only when the current name is unset.
pub const TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME: &str = "TS_INDEX_DUMP_OPLOG_GAP_BYTES";
/// Shortest time between two catalog dumps of the same shard. The byte gap says a dump is
/// WORTH doing; this says it is not worth doing AGAIN yet.
pub const TS_INDEX_DUMP_MIN_INTERVAL_MS: &str = "TS_INDEX_DUMP_MIN_INTERVAL_MS";
/// How many reclaimable bytes the index log must be carrying before the post-dump sweep is worth
/// rewriting it for.
pub const TS_INDEX_GC_MIN_RECLAIMABLE_BYTES: &str = "TS_INDEX_GC_MIN_RECLAIMABLE_BYTES";

/// Previous name for [`TS_BLOCK_SLAB_TARGET_BYTES`], still honoured so a deployment that sets it
/// keeps working. Read only when the current name is unset.
///
/// The constant was renamed to `SLAB` some time ago and kept the SEGMENT spelling as its value, so
/// the name in the source and the name an operator sets had drifted apart: every doc, config and
/// conformance list said one thing and the code said another. The variable now spells itself the
/// way the rest of this store spells it -- the data structure is a slab, the accessor is
/// `effective_block_slab_target_bytes`, and the field is `block_slab_target_bytes`.
pub const TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_BLOCK_SEGMENT_TARGET_BYTES";

pub const DEFAULT_CONTEXT_BLOCK_TARGET_BYTES: usize = 64 * 1024;
pub const DEFAULT_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 30;

/// THE CEILING THAT MAKES AN OFFSET FIT IN 32 BITS, AND WHY IT IS EXACTLY THIS NUMBER.
///
/// A block address carries its slab and its offset inside that slab in ONE 64-bit word, the
/// slab in the high half and the offset in the low half -- see
/// [`crate::block_store::make_block_address_word`]. The low half holds `0 ..= u32::MAX`, so the
/// engine needs a bound on how large a recorded offset can get, and the only thing that bounds
/// it is this knob.
///
/// The bound is not "the target", it is "the target minus one", and that is a fact about the
/// roll predicate rather than about arithmetic. `block_store::should_roll_before_append` is
/// `write_offset > 0 && write_offset.saturating_add(record_len) > slab_target_bytes`, and every
/// append on both write paths goes through it. So a record is recorded at an offset `W` only
/// when `W == 0` or `W + record_len <= target`; `encode_block_record` always emits at least
/// `BLOCK_RECORD_HEADER_LEN` bytes, so `record_len >= 1` and therefore `W <= target - 1`. An
/// offset can never EQUAL the target.
///
/// That makes `1 << 32` the largest target that still fits: the highest offset a store with
/// that target can record is `2^32 - 1`, which is exactly `u32::MAX`. One byte more of target
/// and the low half overflows.
///
/// This is a REAL REDUCTION IN WHAT AN OPERATOR MAY SET. The knob was a `u64` and accepted any
/// value; a store configured above this now REFUSES TO START rather than recording an address
/// that points somewhere else. It is checked, never clamped: a clamp would turn the
/// misconfiguration into a wrong address later, which is the failure mode this exists to
/// prevent.
pub const MAX_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 32;
pub const DEFAULT_STREAM_MAX_BLOB_SIZE: u64 = 10 * 1024 * 1024;
pub const DEFAULT_COMPACTION_WATERMARK_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_COLD_SCAN_NO_CACHE_FILL: bool = true;
// Undumped index-log-gap threshold that triggers a background catalog/index dump under the
// MANIFEST-CONFORMANCE FOLD (TS_INDEX_CATALOG_FOLD). Matches the
// default of 1 MiB.
pub const DEFAULT_INDEX_DUMP_WAL_GAP_BYTES: u64 = 1024 * 1024;
/// Shortest time between two catalog dumps of the same shard, in milliseconds.
///
/// The byte gap alone leaves the cadence in the hands of whoever calls the threshold check. One
/// caller has a cadence of its own -- the proxy's reclaim thread polls on a timer -- and the
/// other, the background storage cycle, has none: it checks on every round it runs. So under a
/// write burst the gap can be crossed again immediately and the dump fires back to back, and a
/// dump is not cheap: it serializes the whole served index, writes it durably, appends a folded
/// anchor and then rewrites both logs.
///
/// A floor in TIME belongs with the threshold rather than with its callers, so that a caller
/// that polls quickly cannot dump quickly. Waiting also makes each dump worth MORE: the deltas
/// that arrive during the wait fold into the same base rather than into the next one -- the
/// same reason the design being followed delays a dump until its log has accumulated.
///
/// 1.5 s, against the 1 s that design allows between two index-meta updates. Half a second more
/// because what we do on a dump is the heavier of the two -- a whole-index serialize where
/// theirs writes one meta record -- and because 1.5 s still bounds the extra log growth this
/// costs to one and a half seconds of writes.
pub const DEFAULT_INDEX_DUMP_MIN_INTERVAL_MS: u64 = 1_500;
/// Reclaimable bytes the index log must hold before the post-dump sweep rewrites it.
///
/// The sweep reads the whole log, decides which records the durable base already reflects, and
/// writes the survivors to a temp file which it fsyncs and renames over the original. The READ
/// is what decides; the write, the barrier and the rename are what it costs. When little is
/// reclaimable that cost buys almost nothing, and it is paid on every dump.
///
/// 768 KiB, against the 1 MiB the design being followed requires before its index GC runs. Below
/// its own threshold that design skips the round entirely, for the same reason.
///
/// A zero disables the threshold, so any reclaimable byte at all triggers a rewrite. Nothing
/// reclaimable NEVER triggers one regardless: rewriting a log while retaining every record
/// produces the same bytes it started with, so that is arithmetic rather than policy.
pub const DEFAULT_INDEX_GC_MIN_RECLAIMABLE_BYTES: u64 = 768 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTuningConfig {
    pub context_block_target_bytes: usize,
    #[serde(alias = "block_segment_target_bytes")]
    pub block_slab_target_bytes: u64,
    pub stream_max_blob_size: u64,
    pub compaction_watermark_bytes: u64,
    pub cold_scan_no_cache_fill: bool,
    pub index_dump_wal_gap_bytes: u64,
    pub index_dump_min_interval_ms: u64,
    pub index_gc_min_reclaimable_bytes: u64,
}

impl Default for StorageTuningConfig {
    fn default() -> Self {
        Self {
            context_block_target_bytes: DEFAULT_CONTEXT_BLOCK_TARGET_BYTES,
            block_slab_target_bytes: DEFAULT_BLOCK_SLAB_TARGET_BYTES,
            stream_max_blob_size: DEFAULT_STREAM_MAX_BLOB_SIZE,
            compaction_watermark_bytes: DEFAULT_COMPACTION_WATERMARK_BYTES,
            cold_scan_no_cache_fill: DEFAULT_COLD_SCAN_NO_CACHE_FILL,
            index_dump_wal_gap_bytes: DEFAULT_INDEX_DUMP_WAL_GAP_BYTES,
            index_dump_min_interval_ms: DEFAULT_INDEX_DUMP_MIN_INTERVAL_MS,
            index_gc_min_reclaimable_bytes: DEFAULT_INDEX_GC_MIN_RECLAIMABLE_BYTES,
        }
    }
}

impl StorageTuningConfig {
    pub fn from_env() -> Self {
        Self::from_getter(|name| std::env::var(name).ok())
    }

    /// Read the configuration and REFUSE one that cannot be addressed.
    ///
    /// [`Self::from_getter`] panics on the same condition; this is the same check with the
    /// failure returned rather than raised, so a test can feed an out-of-range value and read
    /// the message instead of unwinding.
    pub fn try_from_getter(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let config = Self::parse(get);
        config.validate()?;
        Ok(config)
    }

    /// Whether this configuration can be addressed at all.
    ///
    /// The effective target -- not the raw knob -- is what the roll predicate compares against,
    /// and it is `block_slab_target_bytes.max(stream_max_blob_size)`. So BOTH knobs can push it
    /// over the ceiling and both are named in the failure, because an operator who set only the
    /// blob size within bounds would otherwise be told about a slab target they never touched.
    pub fn validate(&self) -> Result<(), String> {
        let effective = self.effective_slab_target_bytes();
        if effective > MAX_BLOCK_SLAB_TARGET_BYTES {
            return Err(format!(
                "{TS_BLOCK_SLAB_TARGET_BYTES}={} with {TS_STREAM_MAX_BLOB_SIZE}={} gives an \
                 effective slab target of {effective} bytes, above the {MAX_BLOCK_SLAB_TARGET_BYTES} \
                 byte ceiling a block address can name. A block address holds its offset in 32 \
                 bits, so a store with this target could record an offset that does not fit and \
                 would resolve to the wrong block. Lower the target to {MAX_BLOCK_SLAB_TARGET_BYTES} \
                 bytes or less; this is refused at startup rather than clamped, because a clamp \
                 would surface as a mis-addressed read long after the configuration was set",
                self.block_slab_target_bytes, self.stream_max_blob_size
            ));
        }
        Ok(())
    }

    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        let config = Self::parse(get);
        if let Err(message) = config.validate() {
            panic!("{message}");
        }
        config
    }

    fn parse(get: impl Fn(&str) -> Option<String>) -> Self {
        // A variable that is PRESENT AND BLANK is not a value. Without this, `get(NEW)` answers
        // `Some("")`, `or_else` never reaches the previous name below, and a deployment that set
        // the older spelling correctly gets the built-in default instead of what it asked for.
        let get = |name: &str| get(name).filter(|value| !value.trim().is_empty());
        let defaults = Self::default();
        Self {
            context_block_target_bytes: parse_usize(
                get(TS_CONTEXT_BLOCK_TARGET_BYTES)
                    .or_else(|| get(TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME)),
                defaults.context_block_target_bytes,
            )
            .max(1024),
            block_slab_target_bytes: parse_u64(
                get(TS_BLOCK_SLAB_TARGET_BYTES)
                    .or_else(|| get(TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME)),
                defaults.block_slab_target_bytes,
            )
            .max(1024),
            stream_max_blob_size: parse_u64(
                get(TS_STREAM_MAX_BLOB_SIZE),
                defaults.stream_max_blob_size,
            )
            .max(1024),
            compaction_watermark_bytes: parse_u64(
                get(TS_COMPACTION_WATERMARK_BYTES),
                defaults.compaction_watermark_bytes,
            ),
            cold_scan_no_cache_fill: parse_bool(
                get(TS_COLD_SCAN_NO_CACHE_FILL),
                defaults.cold_scan_no_cache_fill,
            ),
            index_dump_wal_gap_bytes: parse_u64(
                get(TS_INDEX_DUMP_WAL_GAP_BYTES)
                    .or_else(|| get(TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME)),
                defaults.index_dump_wal_gap_bytes,
            ),
            index_dump_min_interval_ms: parse_u64(
                get(TS_INDEX_DUMP_MIN_INTERVAL_MS),
                defaults.index_dump_min_interval_ms,
            ),
            index_gc_min_reclaimable_bytes: parse_u64(
                get(TS_INDEX_GC_MIN_RECLAIMABLE_BYTES),
                defaults.index_gc_min_reclaimable_bytes,
            ),
        }
    }

    pub fn effective_slab_target_bytes(self) -> u64 {
        // A slab must be able to hold the largest blob it stores, so the max blob
        // size is a FLOOR, not a ceiling. asserts a record fits within a slab
        // (specification), i.e. seal >= max_blob. The previous
        // `.min` inverted this, clamping the 1GiB seal target down to the 10MiB blob
        // size and sealing slabs 100x too small.
        self.block_slab_target_bytes
            .max(self.stream_max_blob_size)
    }

    pub fn env_names() -> [&'static str; 11] {
        [
            TS_CONTEXT_BLOCK_TARGET_BYTES,
            TS_CONTEXT_BLOCK_TARGET_BYTES_PREVIOUS_NAME,
            TS_BLOCK_SLAB_TARGET_BYTES,
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME,
            TS_STREAM_MAX_BLOB_SIZE,
            TS_COMPACTION_WATERMARK_BYTES,
            TS_COLD_SCAN_NO_CACHE_FILL,
            TS_INDEX_DUMP_WAL_GAP_BYTES,
            TS_INDEX_DUMP_GAP_BYTES_PREVIOUS_NAME,
            TS_INDEX_DUMP_MIN_INTERVAL_MS,
            TS_INDEX_GC_MIN_RECLAIMABLE_BYTES,
        ]
    }
}

pub fn context_block_target_bytes() -> usize {
    StorageTuningConfig::from_env().context_block_target_bytes
}

pub fn effective_block_slab_target_bytes() -> u64 {
    StorageTuningConfig::from_env().effective_slab_target_bytes()
}

/// Undumped index-log threshold (bytes) that triggers a background catalog/index dump under
/// the MANIFEST-CONFORMANCE FOLD. Reads the `TS_INDEX_DUMP_WAL_GAP_BYTES` env override -- or the
/// previous name for it, so existing deployments keep working -- falling back to the 1 MiB
/// default. Only consulted by the catalog fold's threshold dump.
///
/// This value is a FLOOR, not the threshold itself. A dump writes the whole served index, so its
/// cost is the store while what it releases is only what accrued since the previous dump; against
