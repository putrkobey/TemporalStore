// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE FRAME LAYER SAVES ONCE THE RESPONSE CACHE EVICTS.
//!
//! # THE REGIME THE OTHER TWO PROBES COULD NOT ENTER
//!
//! `the_payload_cache_per_user_byte` priced the two cache layers at `2 * value + c` bytes an entry,
//! and `how_many_keys_a_cached_frame_serves` showed a frame serves exactly one key at every value
//! size. Both ran with a 64 MiB cache over 2,000 keys and recorded **zero evictions**.
//!
//! In that regime the response layer answers every warm read and the frame layer is unreachable by
//! construction. So neither probe could observe the one case where the frame layer pays for itself:
//! a response entry evicted, the value still resident as a frame, the next read served without
//! touching the block store. **Those probes measured the regime that favours dropping it.**
//!
//! # SO THIS ONE FORCES THE OTHER REGIME
//!
//! A cache far smaller than the corpus, a populating pass, then a warm pass whose block-store reads
//! are counted. The printed number is what a warm read costs under pressure.
//!
//! # AND IT REPORTS RATHER THAN ASSERTS THE COMPARISON
//!
//! The question is what the frame put is WORTH, which is a difference between two bodies of
//! production code. One process holds one of them, so no assertion inside a single run can answer
//! it; the arm prints the figure and the comparison is made by running it on both trees. Asserting
//! a remembered number from the other tree instead would pin a constant whose provenance is a note
//! rather than a run.
//!
//! What it DOES assert is that the regime is real -- evictions must have happened -- and that every
//! warm read is ACCOUNTED FOR.
//!
//! # THE ACCOUNTING IDENTITY, AND WHY IT IS NOT A THRESHOLD
//!
//! The first version of this arm asserted that warm reads must reach the block store, reasoning
//! that an evicted entry has nowhere else to come from. Both trees reported **zero** block-store
//! reads with 764 and 1,750 evictions and zero memory hits: the cache has a DISK TIER beneath
//! memory, an evicted entry demotes to it, and a warm read hits it without touching the block
//! store.
//!
//! So the guard is an identity over every source a read can have -- memory hit, disk hit,
//! block-store read -- rather than a threshold on one of them. A threshold on one term cannot
//! catch a read served by a term the arm forgot existed. An identity over all of them can, and
//! did.
//!
//! It also means the frame layer is diluted twice under pressure: the response layer evicts, and
//! what catches the eviction is the cache's own disk tier, not the frame entry.
//!
//! # HOW THE COMPARISON CAME OUT, AND WHY THE FRAME PUT IS STILL THERE
//!
//! Measured on both trees at 2,000 keys x 256 B behind a 64 KiB cache:
//!
//! | | puts | evictions | memory hits | disk-tier hits | block-store reads |
//! |---|---|---|---|---|---|
//! | with the frame put | 4,000 | 3,750 | 0 | 2,000 | 0 |
//! | without it | 2,000 | 1,764 | 0 | 2,000 | 0 |
//!
//! **Service is identical** — the whole warm pass comes from the disk tier either way, and the
//! block store is never touched — while the second copy doubles `puts` and more than doubles
//! evictions. So the second copy costs something real and buys nothing here.
//!
//! **The removal was still reverted, and not for a reason this arm can see.** Dropping the frame
//! put on the string read path broke six arms in `part2` whose subject is the PERSISTENT BLOCK
//! CACHE keyed by `CacheKey::page_with_slot` — a tiny memory cache refilling from that tier without
//! a block-store read. `read_bucket_index_value` is the only door from a string command into
//! `read_block_bytes`, so declining the frame there does not drop a duplicate: it removes that tier
//! entirely for the commonest data kind.
//!
//! The copy with one reader and no tier of its own is the LOGICAL one, the encoded
//! `CommandResponse` under `CacheKey::string(shard, key)` — and it is the larger of the two by the
//! response envelope. That is where the cost this arm prices should be taken, and this arm is the
//! instrument for taking it: run it before and after, on both trees, and read the table above.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_the_frame_layer_saves_under_eviction -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

/// Deliberately far below the corpus, so the response layer MUST evict.
const CACHE_BYTES: usize = 64 * 1024;
const KEYS: usize = 2_000;
const VALUE: usize = 256;

fn noisy_value(index: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x1234_5678_9ABC_DEF0)
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

#[test]
#[ignore = "seeds a store and reads it twice under cache pressure; run by name"]
fn what_the_frame_layer_saves_under_eviction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
