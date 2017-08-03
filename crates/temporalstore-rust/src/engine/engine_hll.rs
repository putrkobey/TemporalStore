// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded approximate distinct count via a dense HyperLogLog sketch.
//!
//! Control State distinct/CHANGE counts are otherwise exact `BTreeSet` unions, whose memory
//! grows with cardinality — unbounded for high-cardinality long-window distincts. This sketch
//! gives a fixed-memory (2^P registers) approximate count, mergeable register-wise, so it can
//! back a bounded distinct mode (exact until a threshold, then sketch — the DC→LDC shape).
//!
//! Fixed, version-stable hashing (FNV-1a + splitmix64 finalizer) so a persisted sketch stays
//! valid across builds. P=12 (4096 registers, ~1.6% standard error), matching the CPC lgK.
//! Everything here is only reachable behind a config gate; the exact path is unchanged by default.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::state::ShardState;

/// ONE DISTINCT-COUNT BUCKET, IN WHICHEVER REPRESENTATION IT CURRENTLY HOLDS.
///
/// # WHY THIS IS ONE TYPE AND NOT TWO MAPS
///
/// This was `control_state_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>` beside
/// `control_state_change_sketch: HashMap<String, BTreeMap<u64, Hll>>` -- two maps keyed
/// identically, holding the two representations of the same thing. [`record_change`] kept them
/// MUTUALLY EXCLUSIVE per `(key, bucket)` by removing the bucket from the first as it inserted
/// into the second, so they were already one logical map; the exclusivity was maintained by a
/// remove/insert pair rather than by the type, and a bucket in both was representable.
///
/// It is not representable now. That is the whole of what this change buys, and it is worth being
/// precise about what it does NOT buy.
///
/// # IT IS NOT A BUG FIX, AND NOBODY SHOULD LATER DECIDE THAT IT WAS
///
/// A bucket sitting in BOTH maps would still have produced the right answer. [`count_changes`]
/// folds the exact values into an HLL and merges the sketches into the same HLL, and both an
/// `Hll` and a `BTreeSet` absorb a duplicate -- so the overlap state was wasteful and
/// unrepresentable-in-principle, not wrong. The merge removes a map and makes an invariant
/// unbreakable. It repairs nothing, and a future reader should not invent a defect to explain it.
///
/// # IT ALSO COSTS FEWER LOOKUPS, WHICH IS A CONSEQUENCE AND NOT THE REASON
///
/// The old `record_change` hashed the key up to three times on the conversion path -- a `get_mut`
/// on the sketch map, an `entry` on the exact map, a `get_mut` back on the exact map, an `entry`
/// on the sketch map. One `entry` reaches the bucket now and the conversion is a variant swap in
/// place.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) enum DistinctBucket {
    /// Every distinct value, while the bucket is small enough to keep them.
    Exact(BTreeSet<Vec<u8>>),
    /// A fixed-size sketch, once the exact set passed [`DISTINCT_SKETCH_THRESHOLD`] with the
    /// `control_distinct_sketch` gate on. The exact values are gone: this is the conversion, not
    /// a cache beside them.
    Sketch(Hll),
}

impl Default for DistinctBucket {
    fn default() -> Self {
        DistinctBucket::Exact(BTreeSet::new())
    }
}

impl DistinctBucket {
    /// How many distinct values this bucket stands for: exact when it is exact, estimated when it
    /// is sketched. For reporting, never for a decision -- a caller that needs the values
    /// themselves has to match on the variant, which is the point of the variant.
    pub(crate) fn distinct_hint(&self) -> u64 {
        match self {
            DistinctBucket::Exact(set) => set.len() as u64,
            DistinctBucket::Sketch(sketch) => sketch.estimate(),
        }
    }
}

/// A bucket's exact distinct set is converted to a fixed-size HLL once it exceeds this many
/// distinct values, mirroring the control_state DC→LDC upgrade threshold.
pub(crate) const DISTINCT_SKETCH_THRESHOLD: usize = 4096;

const P: u32 = 12;
const M: usize = 1 << P; // 4096 registers

/// Version-stable 64-bit hash: FNV-1a over the bytes, then a splitmix64 finalizer for avalanche.
fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    // splitmix64 finalizer
    let mut z = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

