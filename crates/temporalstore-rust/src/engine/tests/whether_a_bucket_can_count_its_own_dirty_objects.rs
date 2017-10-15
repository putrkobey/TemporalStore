// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN A BUCKET COUNT ITS OWN DIRTY OBJECTS? The per-bucket count, derived two ways and compared.
//!
//! # WHAT THIS IS FOR
//!
//! `BucketStorageSummary::dirty_object_count` is populated from exactly one place -- the
//! `shard.dirty_objects.bucket_counts()` loop at the foot of `bucket_storage_summaries` -- and that
//! set is object-keyed and grows with the ingest. The dirty-BUCKET question has already stopped
//! needing it: `the_block_bit_answers_the_dirty_bucket_question` measures the bucket's own `dirty`
//! bit and its pages' bits answering it exactly. The obvious next step is to answer the per-bucket
//! COUNT the same way, from each bucket's own pages, and retire the set.
//!
//! This measures whether that is possible, and the answer is different for the two questions the
//! field is asked. THE COUNT IS NOT DERIVABLE. THE `> 0` IS, but not from the pages -- from the
//! `dirty` flag on the bucket's own node, which `mark_object_dirty` sets after CREATING the node
//! when the key has none. So a key with no page at all still leaves a node reading dirty, and the
//! three consumers that only ask `> 0` can move where the count cannot. Both results are measured
//! below on one shard.
//!
//! A COUNT OF DIRTY PAGES IS NOT A COUNT OF DIRTY OBJECTS, so the derived figure counts DISTINCT
//! objects having at least one dirty page in the bucket -- identity taken from
//! `BlockKeysByPosition`, which is how a production derivation would have to take it, because a page
//! entry holds no identity of its own. The fixture is built so a page count and an object count are
//! DIFFERENT NUMBERS; otherwise this could not tell a correct derivation from a page tally.
//!
//! # WHAT IT MEASURES
//!
//!     48 keys on a 16-bucket routing range, dumped clean, then three re-dirtied and two removed
//!
//!       buckets with a count, via the SET        8     total via the SET       11
//!       buckets with a count, via the PAGES      3     total via the PAGES      3
//!                                                     total dirty PAGES        9
//!       set members with NO live page at all     8     widest object 3 pages
//!
//!       bucket        0   2   3   6   7  11  12  15
//!       set           1   1   1   1   3   2   1   1
//!       node.dirty()  t   t   t   t   t   t   t   t
//!       objects       0   0   1   0   1   0   0   1
//!
//! The page-derived figure reports 3 of the authority's 11 marks, and FIVE OF THE EIGHT DIRTY
//! BUCKETS report zero. Bucket 7 shows the shortfall is not confined to the buckets the page walk
//! cannot see at all: it holds three marks and the walk finds one, so this is not a factor that
//! could be corrected for.
//!
//! THE NODE FLAG, THOUGH, AGREES WITH THE SET ON EVERY ONE OF THE SIXTEEN BUCKETS -- in both
//! directions, and including the five that hold no page at all. That is asserted here, so the
//! three `> 0` consumers rest on a measurement rather than on this paragraph.
//!
//! # AND THE FLAG IS AN INDEPENDENT RECORD, NOT A CACHE OF THE SET
//!
//! That needed its own experiment, because two sites keep the flag in step with the set --
//! `refresh_one_bucket_runtime_flags` ORs in `dirty_objects.has_bucket(..)`, and the ownership
//! reconstruct ORs in `dirty_objects.contains(key)` -- so the agreement above could have been
//! circular, with the flag merely echoing the set on the very buckets that carry the result. The
//! `has_bucket` doc is itself a reason to suspect it: those OR terms exist because the index "is a
//! strict SUPERSET of what that fold can find", which is this same non-page-backed population.
//!
//! Six arms, each neutralising maintainers of the flag and re-running this test:
//!
//!     A  mark's own `set_dirty(true)` removed ................. PASSES
//!     B  the `has_bucket` OR neutralised ..................... PASSES
//!     C  the `contains` OR neutralised ....................... PASSES
//!     D  all three at once ................................... FAILS, buckets [0, 11, 12]
//!     E  BOTH set-derived ORs off, the mark KEPT .............. PASSES, no-page buckets intact
//!     F  both ORs off AND the mark off (= D) ................. FAILS, buckets [0, 11, 12]
//!
//! A, B and C passing alone says only that the three maintainers are REDUNDANT -- no single
//! removal can decide which record is authoritative, because the survivors cover. E is the arm that
//! decides it: with both set-derived terms gone, the agreement still holds on all sixteen buckets
//! and the five no-page buckets are still reported. So the mark alone maintains the flag, and F
//! differing from E is what shows E is isolating the mark rather than agreeing with nothing.
//!
//! The consequence is the useful part: the node's flag and the set are two records of the same
//! fact, and the flag is not derived from the set. So the `> 0` consumers can move to it and the
//! move survives the set being retired -- while the COUNT still cannot be answered from the bucket
//! at all.
//!
//! # WHY, AND BOTH REASONS WERE REACHED HERE
//!
//!   1. A DIRTY OBJECT NEED NOT HAVE A PAGE. A removal marks a `control_state:cpc:*` record
//!      alongside the data object, and control state is not page-backed -- so there is no `dirty`
//!      bit anywhere that could carry its dirtiness.
//!      `dirty_objects_versus_the_blocks_own_dirty_flags` measures the same thing from the other
//!      end, at 80 of 160. Eight of the eleven marks here are of this kind.
//!   2. THOSE RECORDS ROUTE TO BUCKETS OF THEIR OWN. `DirtyObjectIndex::insert` files a key under
//!      `block_routing_bucket(key, start, end)`, so a `control_state:cpc:*` record contributes 1 to
//!      ITS bucket -- which holds no page at all. That is where the five invisible buckets come
//!      from, and it is why the shortfall is not a uniform factor that could be corrected for.
//!
//! # WHY A PAGE-DERIVED COUNT IS A LOSS AND NOT A WOBBLE
//!
//! Dump selection filters on `summary.dirty_object_count > 0`. A bucket a page walk reads as zero
//! is a bucket that STOPS BEING DUMPED while it holds undumped marks, so the five buckets above are
//! unflushed state that no round would ever pick up. The figure is also a MAGNITUDE downstream --
//! the per-bucket gauge publishes it, and the summary is SERIALIZED into a dump manifest -- and
//! `bucket_storage_summaries` adds the count ONTO `summary.dirty_generation`, which
//! `bucket_dump_summary_matches_current_generation` compares EXACTLY when it validates a stored
//! dump manifest. So moving this count by one moves a durability comparison, not just a report
//! field. What that generation IS is asserted here as an identity -- `node + count` -- rather than
//! left as the observation that the two differ.
//!
//! # AND WHY THE TWO `> 0` DIRECTIONS ARE NOT INTERCHANGEABLE
//!
//! The three presence consumers do not tolerate the same error. The dump filter over-reporting
//! costs an extra dump. The two reclaim checks are the opposite: `dirty_object_count == 0` is the
//! arm that counts a bucket as covered and contributing no WAL floor, so a bucket wrongly reading
//! dirty skips it, cannot name a claim -- its `first_dirty_wal_sequence` was cleared alongside
//! `dirty = false` -- and blocks the whole reclaim plan, which is the regression recorded at that
//! site as 0 of 10,666 records freed. So the node-flag agreement is asserted in BOTH directions,
//! and the over-reporting direction is the one that licenses moving the reclaim checks.
//!
//! A report with non-triviality assertions, plus the clauses that pin the conclusion: the two
//! methods DISAGREE, and they disagree for the stated reason. If a later change makes them agree,
//! this fails and says so -- at which point the producer can move to the derivation.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Per-bucket dirty-object counts the way the producer takes them: off the object-keyed set.
fn authority(shard: &crate::engine::state::ShardState) -> BTreeMap<u32, u64> {
    shard.dirty_objects.bucket_counts().collect()
}
