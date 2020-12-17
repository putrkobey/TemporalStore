// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A CACHE READ COSTS ON THE SERVING PATH.
//!
//! The write path has been measured and cut repeatedly: twenty-seven allocations a stored record
//! came off it through two cache changes and two dependency pin bumps. The second of those bumps
//! said plainly that the read-side savings it carries are not exercised by the write-span ladder,
//! and left the read side unmeasured. This module measures it.
//!
//! THE SERVING READ PATH, AS THE COMPILER NAMES IT rather than as a grep guesses it. Every read
//! door on `MultiLayerCache` -- thirty-one of them, counting the older spellings the crate still
//! carries -- was made private in a local copy of the pinned crate, and the call sites that
//! stopped resolving were collected and retargeted until a pass produced none. That enumeration
//! ends at EIGHT sites in the production library, five distinct doors. Grep cannot do this: the
//! engine calls `.get(` on maps several thousand times, and two of the eight are wrapped method
//! chains whose receiver is on a different line from the call.
//!
//! Of the eight, only these four READ A STORED ANSWER, and all four are charged to
//! `AllocClass::CacheRead`:
//!
//! ```text
//!   engine.rs              read_block_bytes        cache.get           the page, owned
//!   engine.rs              read_block_shared       cache.get_shared    the page, shared
//!   command_validation.rs  cached_response         cache.get           the command answer
//!   compaction.rs          block_memory_resident   cache.get_memory    a residency probe
//! ```
//!
//! The other four are not reads of a stored answer and are deliberately not charged to it:
//! `peek` in `invalidate_if_cached` and `entries_for_shard` in the batched sweep are
//! INVALIDATION, which `AllocClass::CacheInvalidation` already owns, and `peek_tier` with
//! `entries_for_shard` in the storage reports are REPORTING. Two further sites sit inside
//! `#[cfg(test)] mod eviction_round_scale` in a production FILE and are not production ITEMS --
//! classifying those by file rather than by item would have put two test helpers in this table.
//!
//! ONE SERVING READ IS NOT ONE CACHE READ. A `StringGet` enters `cached_response`, which reads the
//! command-answer cache. On a hit it stops there: one cache read. On a miss it calls the source,
//! which resolves an address and calls `read_block_bytes`, reading the page cache as well. The
//! tables below report cache reads per serving read so that the two are never confused, and the
//! measured arms all come out at 1.00 -- a key with no stored answer never reaches a page read,
//! so the absent arm is one cache read too.
//!
//! ABSOLUTE QUANTITIES, NEVER A SHARE. Every figure is allocations and bytes per serving read,
//! with the shape stated. A share re-scales with corpus size, with the regime, and with every
//! change landed since it was taken.
//!
//! THE STORE PATH IS HELD CONSTANT ACROSS EVERY ARM AND IT IS `/tmp/.tmpXXXXXX`, fifteen
//! characters. Allocation BYTES move with the length of the store's directory path at about six
//! bytes per character, an effect that once read as a one percent profile difference until the
//! pair was run properly. Allocation COUNTS are immune to it, which is why the counts carry every
//! claim here and the byte columns are printed beside them rather than asserted on. The arms
//! assert the path length is equal rather than trusting that it is -- and the byte columns do
//! wobble between otherwise identical runs while the counts do not, which is the effect showing
//! itself.
//!
//! THE KEY WIDTH IS HELD CONSTANT TOO: `tenant/7/object/{index:09}` is twenty-five characters at
//! every corpus size used here, so the corpus arms differ in the number of records and in nothing
//! else.

// Unconditional: the two guards at the foot of this file run in an ORDINARY build, where the
// counting allocator is not installed, and they still need the engine and its command types. The
// measurement apparatus above them is gated item by item instead.
#[allow(unused_imports)]
use super::*;

/// Records written in the small arm.
#[cfg(feature = "alloc-probe")]
const SMALL: usize = 512;
/// Records written in the large arm. Eight times the small one, so a per-read cost that grew with
/// the corpus would show as a ratio near eight rather than near one.
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 4_096;
/// Serving reads measured in EVERY arm, held equal so the arms differ in corpus and not in how
/// much work was measured.
#[cfg(feature = "alloc-probe")]
const READS: usize = 512;
/// The stored value's width, in bytes.
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;

/// The key for a record, twenty-five characters wide at every index this module uses.
#[cfg(feature = "alloc-probe")]
fn record_key(index: usize) -> String {
    format!("tenant/7/object/{index:09}")
}

/// What one arm observed: the allocation span, and what the cache itself says it did.
#[cfg(feature = "alloc-probe")]
struct ReadArm {
    counted: crate::alloc_probe::ClassifiedCounts,
    memory_hits: u64,
    disk_hits: u64,
    misses: u64,
}

#[cfg(feature = "alloc-probe")]
impl ReadArm {
    fn cache_reads(&self) -> u64 {
        self.memory_hits + self.disk_hits + self.misses
    }
    fn allocs_per_read(&self) -> f64 {
        self.counted.total.allocs as f64 / READS as f64
    }
    fn bytes_per_read(&self) -> f64 {
        self.counted.total.alloc_bytes as f64 / READS as f64
    }
    fn cache_reads_per_read(&self) -> f64 {
        self.cache_reads() as f64 / READS as f64
    }
}

/// Build an engine on a store path of the fifteen-character `tempfile` shape, and report how long
/// that path is so the caller can assert it did not move between arms.
#[cfg(feature = "alloc-probe")]
fn engine_at(dir: &std::path::Path, memory_capacity_bytes: usize) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        memory_capacity_bytes,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    (engine, dir.to_string_lossy().len())
}

/// Write `records` records, then measure `READS` serving reads of the keys `pick` names.
///
/// The cache's own counters are sampled around the same span, so an arm can say which of the
/// three cases it actually reached instead of assuming it.
#[cfg(feature = "alloc-probe")]
fn measure_reads(
    engine: &TemporalEngine,
    records: usize,
    pick: impl Fn(usize) -> String,
    warm: bool,
) -> ReadArm {
    for index in 0..records {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: record_key(index),
                value: vec![118u8; VALUE_BYTES],
            },
        });
    }
    if warm {
        for offset in 0..READS {
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: pick(offset) },
            });
        }
    }
