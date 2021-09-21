// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A BLOCK'S BUCKET SHOULD BE A FUNCTION OF ITS KEY ALONE. IT SHOULD NOT.
//!
//! # THE QUESTION
//!
//! A block's routing bucket is `start + FNV-1a-64(object_key) % (end - start + 1)`
//! (`engine/engine_hashing.rs`), so the key is folded INTO the configured range and a key's bucket moves
//! when the range moves. #1977 measured that as a refutation of recomputation: 4,000 keys written
//! on `0..4294967295` and reopened on `0..1023` gave 4,000 of 4,000 recomputed buckets wrong. This
//! module asks the stronger question underneath it -- should the dependence exist at all? -- and
//! answers no, on four measurements that each stand on their own.
//!
//! # 1. THE RANGE IS A SIZING KNOB, NOT AN OWNERSHIP BOUNDARY
//!
//! Read the declarations rather than the names. `DEFAULT_END_ROUTING_BUCKET` (`lib.rs`) says the
//! value "sets the MODULUS"; `Engine::shard_routing_range` says the range "is not a filter over a
//! fixed answer, it is an argument to the placement function"; `docs/runtime_tuning.md` titles the
//! section "a large lever on resident memory" and sweeps the end bucket as a tuning candidate.
//! Nothing anywhere filters a key out of a shard for falling outside the range.
//!
//! And the engine already HAS a range-independent id, one layer up: `client::routing`'s
//! `bucket_id_for_key` is `crc64_jones(key) >> 34` over `1 << 30` buckets, a function of the key
//! and nothing else, and the metaserver publishes a contiguous slice of it per shard. Which shard
//! a key lives on is range-independent today. What `block_routing_bucket` decides is something
//! else entirely: how many keys share one unit of dump, eviction, invalidation and log-reclaim
//! work INSIDE one shard. The two are different concepts at different layers, and treating the
//! intra-shard filing bucket as though it were the inter-shard id is the mistake this module
//! exists to stop.
//!
//! # 2. AT THE SHIPPED DEFAULT A RANGE-FREE ID IS THE SAME FUNCTION, SO IT BUYS NOTHING
//!
//! `bucket_for_object` is `start + hash % count`. At the shipped default `start` is 0 and `count`
//! is 1,024, so `block_routing_bucket(key, 0, 1023)` and a range-free `hash(key) % 1024` are the
//! SAME ARITHMETIC. `a_range_free_id_is_the_same_function_as_the_shipped_default` asserts that
//! key for key at both corpus sizes, and it is why the pages-per-bucket distribution is not
//! measured again here: it cannot differ. `routing_range_default.rs` already reports that
//! distribution as a histogram with p50/p90/p99/MAX and an asserted denominator over five ranges
//! at 4,000 and 40,000 records, and the published table is `docs/runtime_tuning.md` -- 39 pages in
//! the p50 bucket and 50 in the MAX at 40,000 records on `0..1023`, against 1 and 1 on the whole
//! keyspace. A range-free id at a fixed count of 1,024 reproduces the `0..1023` column exactly,
//! because it IS that column.
//!
//! # 3. WHAT IT COSTS IS THE ONLY FILL LEVER THE OPERATOR HAS
//!
//! The fill is `records / bucket-count`, and `docs/runtime_tuning.md` states the consequence
//! plainly: "no fixed bucket count is right for every corpus ... `1023` sits at 3.91 pages a
//! bucket for a 4,000-record store and 39.06 for a 40,000-record one." A range-free id needs a
//! FIXED count to keep buckets full, and a fixed count is exactly what that sentence rules out.
//! `a_fixed_bucket_count_cannot_reproduce_the_fills_the_range_produces` prints the four fills the
//! shipped candidates produce from one key set and shows no single count reaches them.
//!
//! So a range-free id does not remove the knob. It SPLITS it into two -- a count that sizes and a
//! window that owns -- and the engine would ship both where it ships one.
//!
//! # 4. TWO OF THE THREE DUPLICATIONS IT WOULD PAY FOR RECOVER NO BYTES
//!
//! `BucketNode::routing_bucket` duplicates the `BucketMap` key and `BlockAddress::routing_bucket`
//! duplicates it again per block, so removing them looks like the saving that pays for the change.
//! It is zero for EACH FIELD ON ITS OWN, and for the reason both structures already have written
//! down: only a change that takes the tail to eight bytes or fewer, or a whole word out of the
//! eight-aligned group, moves either one. `BucketNode` is 80 bytes of eight-aligned field and a
//! 6-byte tail rounded to 8 -- take four out of the tail and 2 still rounds to 8, so that one is zero
//! and stays zero. `BlockAddress` WAS a 29-byte payload in 32 bytes, and taking four out leaves 25,
//! which rounds back to 32 -- also zero.
//!
//! IT IS NOT ZERO FOR THE PAIR, and the address is 24 bytes now because of it. The routing bucket left
//! in the same change that narrowed `block_id` from 32 bits to 16: four bytes off 29 is 25 and rounds
//! back, two bytes off is 27 and rounds back, and SIX bytes off is 23, which rounds to 24. A verdict on
//! a field in isolation is not a verdict on the field, and this is the third alignment step in this
//! campaign crossed only by a combination.
//! `removing_the_routing_bucket_recovers_no_bytes_alone_and_eight_in_combination` asserts all three
//! counterfactuals over the widths the compiler reports, not over a hand-written list.
//!
//! THE THIRD CARRIER IS THE EXCEPTION, AND IT IS THE ONE USEFUL NUMBER HERE.
//! `BlockLookupRef` -- the entry `object_block_lookup` holds, and the one
//! `bucket_index_target_buckets_for_object_key` reads the bucket out of -- is a `u32` bucket beside
//! a `u64` ref key, 12 bytes of field in 16. Drop the bucket and a bare `u64` is 8: the width
//! HALVES, with no wire change, because that lookup is not persisted.
//!
//! AND THAT SAVING DOES NOT NEED A RANGE-FREE ID. #1980 retracted #1977's reader claim --
//! `ShardState::routing_range()` is on the state the function already takes, and a recomputed
//! bucket matched 408 of 408 objects -- so the 8 bytes sit behind recomputation within a KNOWN
//! range, already measured correct, not behind changing what a bucket id is. The dangerous change
//! and the only real saving are not the same change, and that is the most useful sentence in this
//! module.
//!
//! # WHAT A RE-FILE WOULD ACTUALLY COST, AND IT IS THE ONE PLEASANT SURPRISE
//!
//! It is a REINDEX, not a data move, and by a wider margin than the `BlockAddress` field suggests.
//! A block's bytes live at `(slab, offset)` in the address word, chosen by the single active slab
//! and rolled by SIZE alone (`block_store/block_store_append.rs`) -- the bucket takes no part in placement. And
//! the on-disk block record carries neither the bucket nor the object id nor the key:
//! `encode_block_record` takes both identities and discards them (`let _ = (object_id,
//! routing_bucket);`), and `parse_block_record_header` hardcodes both absent. So no slab byte would
//! move and none would go stale.
//!
//! The bucket comes back from disk in exactly three places -- the index snapshot (as the
//! `BucketMap` key; `BlockAddressWire`'s `rs` was a fourth until the slot was retired, and an
//! address carries no bucket at all now), the index log's `IndexItem`, and the WAL's
//! `WalOutcomeItem` -- and all three carry `object_key` as a string beside it. A re-file is
//! therefore a walk of the index recomputing one `u32` per page.
//!
//! THAT IS NOT A REASON TO DO IT. The migration being cheap is not an argument that the
//! destination is better, and #1973's stamp already collects the whole benefit a re-file could:
//! the range a store was built under is recorded beside its index, read BEFORE the decode, and a
//! disagreeing one is refused. The range is already known at load. What the stamp does NOT give is
//! a bucket that is stable ACROSS ranges, and the measurements above say the engine does not want
//! one.
//!
//! # AND THE FIELD IS NOT ONLY A DUPLICATE
//!
//! `validate_bucket_ownership_index_from_entries` compares the bucket a page CLAIMS on its address
//! against the bucket the index FILED it under. Those are two independent statements of one fact,
//! and the comparison is the only thing that can notice them diverging -- the mis-filing #1973 and
//! #1977 both measured. Dropping the stamped copy to save bytes it does not save would also retire
//! the cross-check that finds the defect class.

use super::*;
use crate::engine::hashing::{block_routing_bucket, routing_bucket_count, stable_object_hash};
use crate::block_store::ElementEntry;
use crate::engine::state::{BlockLookupRef, BucketNode};
use std::collections::BTreeMap;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973),
/// the shipped default and the operator's range.
const OPERATOR_END: u32 = 1_023;

/// The buckets that end produces, and the fixed count a range-free id would have to choose.
const OPERATOR_COUNT: u32 = 1_024;

/// The only range a store built before #1973's stamp can have been built on, frozen for ever as
/// `routing_range_stamp::LEGACY_END_ROUTING_BUCKET`.
const LEGACY_END: u32 = u32::MAX;

/// The corpus sizes every figure in this campaign is taken at.
const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// The key set `routing_range_default.rs` sweeps and `docs/runtime_tuning.md` publishes, so a
/// figure here is comparable with the figure there.
fn routed_keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("fill-{i:06}")).collect()
}

/// A bucket id derived from the key ALONE, over a fixed count. The scheme under study.
fn range_free_bucket(key: &str, fixed_count: u32) -> u32 {
    (stable_object_hash(key) % u64::from(fixed_count)) as u32
}

/// Pages held per bucket, as bucket counts keyed by pages held, with the percentiles taken over
/// the BUCKET population and the denominator printed on every line.
///
/// One page per key, which is what a routed string key produces -- so this is the same quantity
/// `routing_range_default.rs` measures through a populated engine, computed from the id function
/// instead. `the_fill_instrument_reproduces_the_published_figure` is what says the two agree.
#[derive(Debug, Default)]
struct Fill {
    counts: BTreeMap<usize, usize>,
}

impl Fill {
    fn of(keys: &[String], bucket: impl Fn(&str) -> u32) -> Self {
        let mut per_bucket: BTreeMap<u32, usize> = BTreeMap::new();
        for key in keys {
            *per_bucket.entry(bucket(key)).or_default() += 1;
        }
        let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
        for held in per_bucket.values() {
            *counts.entry(*held).or_default() += 1;
        }
        Self { counts }
    }

    /// Occupied buckets. The denominator, and it is printed everywhere it is used.
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn blocks(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
