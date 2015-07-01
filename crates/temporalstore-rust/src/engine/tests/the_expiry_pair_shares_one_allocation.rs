// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two expiry maps hold ONE copy of the key's characters between them, and the stored image
//! did not move.
//!
//! A live object with a deadline was measured keeping its key's characters FIVE times. Two of
//! those five were this pair: `expires_at_ms` owned a `String` and `expiry_by_deadline` owned a
//! second `String` of the same text inside its tuple key. Both are keyed on `Arc<str>` now, and
//! `set_expiry` hands both maps the same handle.
//!
//! # WHY THIS MODULE MEASURES POINTERS RATHER THAN BYTES FOR THE HEADLINE
//!
//! "One copy" is a claim about ALLOCATIONS, not about bytes, and the two answer different
//! questions. The number of distinct allocations holding the key text is exactly countable
//! without an allocator: `Arc::as_ptr` is the identity of the buffer. A byte figure is also
//! reported, from the counting allocator, because an `Arc` handle is not free and a copy count
//! that stood in for a footprint would be the same denominator error this work keeps finding.
//! Both numbers, never one for the other.
//!
//! # AND IT MEASURES THE RESTORED PATH, BECAUSE SERDE DOES NOT RESTORE SHARING
//!
//! `serde` gives each occurrence its own allocation on the way in. `expiry_by_deadline` is
//! `serde(skip)`, so after a decode the mirror is empty and `ensure_expiry_order` rebuilds it --
//! and whether the rebuilt mirror SHARES depends entirely on what that loop spells. It spells
//! `key.clone()`, which on an `Arc` is a refcount bump; spelled `key.to_string()` it would be a
//! second copy, and a probe that only looked at a freshly written store would report one copy
//! while a restored store held two. So every figure here is taken twice.

use super::*;

use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// Distinct allocations holding key text across BOTH expiry maps.
///
/// The identity of an `Arc<str>`'s buffer is its data pointer, so two handles to one allocation
/// collapse to one entry here and two independent allocations of the same text do not. That is
/// the difference the whole change is about, and it is a count rather than a reading.
fn distinct_key_allocations(shard: &crate::engine::state::ShardState) -> usize {
    let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for key in shard.expires_at_ms().keys() {
        seen.insert(Arc::as_ptr(key) as *const u8 as usize);
    }
    for (_, key) in shard.expiry_by_deadline.keys() {
        seen.insert(Arc::as_ptr(key) as *const u8 as usize);
    }
    seen.len()
}

/// A shard carrying `count` deadlines, written through the real `set_expiry`.
fn shard_with_deadlines(count: usize) -> crate::engine::state::ShardState {
    let mut shard = crate::engine::state::ShardState::default();
    for index in 0..count {
        crate::engine::set_expiry(
            &mut shard,
            format!("deadline-key-{index:06}"),
            1_700_000_000_000 + index as u64,
        );
    }
    shard
}

/// ONE ALLOCATION PER DEADLINE, ON THE FRESH PATH AND AFTER A DECODE AND REBUILD.
///
/// This is the headline of the change, stated as the count it is. Before, a deadline cost two
/// independent allocations of its key's characters -- one per map. After, it costs one, held by
/// both.
///
/// THE RESTORED HALF IS THE HALF THAT COULD HAVE SILENTLY FAILED. `serde` does not reconstruct
/// sharing, so the restored figure is whatever `ensure_expiry_order` produces, not whatever
/// `set_expiry` produced. The two are asserted to agree.
#[test]
fn the_expiry_pair_shares_one_allocation_on_both_paths() {
    const DEADLINES: usize = 500;

    // --- FRESH ---
    let fresh = shard_with_deadlines(DEADLINES);
    assert_eq!(
        DEADLINES,
        fresh.expires_at_ms().len(),
        "denominator: the fixture did not file the deadlines it reports"
    );
    assert_eq!(
        DEADLINES,
        fresh.expiry_by_deadline.len(),
        "the mirror and the key-ordered map disagree on their population, so neither figure below \
         means anything"
    );
    let fresh_allocations = distinct_key_allocations(&fresh);

    // --- RESTORED: the real index image, decoded, then the mirror rebuilt as a load would. ---
    let image = crate::engine::encode_index_bytes(&fresh);
    let mut restored = crate::engine::decode_index_bytes(&image)
        .expect("an index this build wrote must decode in this build");
    assert_eq!(
        DEADLINES,
        restored.expires_at_ms().len(),
        "the decode lost deadlines, so the sharing figure below is measuring a different shard"
    );
    // NON-VACUITY FOR THE REBUILD: the mirror must come back EMPTY from a decode, or
    // `ensure_expiry_order` would not run and the restored figure would be the decode's and not
    // the rebuild's.
    assert!(
        restored.expiry_by_deadline.is_empty(),
        "the mirror survived the decode, so it is not `serde(skip)` any more and the rebuild this \
         test is about never fires"
    );
    crate::engine::ensure_expiry_order(&mut restored);
    assert_eq!(
        DEADLINES,
        restored.expiry_by_deadline.len(),
        "the rebuild did not refill the mirror"
    );
    let restored_allocations = distinct_key_allocations(&restored);

    println!(
        "distinct allocations of key text across BOTH expiry maps, at {DEADLINES} deadlines:"
    );
    println!("  fresh    {fresh_allocations:>6}  ({:.2} per deadline)", fresh_allocations as f64 / DEADLINES as f64);
