// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ROUTING RANGE A STORE WAS BUILT UNDER, RECORDED BESIDE ITS INDEX.
//!
//! # WHY THIS FILE EXISTS
//!
//! Routing decides WHERE A KEY'S PAGE IS FILED. A page's bucket is
//! `start + FNV-1a-64(object_key) % (end - start + 1)`, so the range width is the MODULUS and a
//! store written under one range holds its pages under buckets a different range would never
//! compute. Until this file, nothing recorded which range a store was built under: the field on
//! `ShardState` is `#[serde(skip)]` and `Engine::shard_routing_range` falls back to the whole
//! keyspace, so the range lived only in the environment of whichever process happened to load the
//! shard.
//!
//! WHAT THAT COST IS MEASURED, NOT SUPPOSED. A store of 2,000 routed keys written on the whole
//! keyspace and reopened on `0..1023` comes back with **2,000 of 2,000 pages filed in buckets
//! above the shard's own end**. Every record still reads -- which is exactly what makes it quiet --
//! and every one of those pages is outside every per-bucket sweep the shard runs: the dump's bucket
//! selection, eviction's victim sampling, the reclaim floor and the release pass all enumerate
//! against the shard's own range. `docs/runtime_tuning.md` said "Set this before the first ingest"
//! and that sentence was the only thing standing between an operator and that state.
//!
//! Nothing is re-filed on reload, and that is the mechanism: the live write path stamps an explicit
//! bucket onto the address at `append_value`, and `rebuild_bucket_first_index` files a page under
//! its address's own bucket and filters nothing. So a reopened range is consulted only for an
//! address carrying no bucket of its own, and on a populated store there are none.
//!
//! # THE THREE CASES: ONE STAMP, CURRENT OR REFUSE
//!
//! 1. A STAMP THAT AGREES with the requested range: load, change nothing.
//! 2. A STAMP THAT DISAGREES: **REFUSE, before the decode**, naming both ranges and the file. A
//!    refusal is the only honest answer -- the engine cannot re-file the pages and must not
//!    pretend the range is a filter.
//! 3. NO STAMP AT ALL, over a store that already has on-disk state: **REFUSE**, naming the absent
//!    file. This engine cannot establish the range, and GUESSING it is refused: a store built on
//!    the whole keyspace whose keys all happen to fall below 1024 is indistinguishable from one
//!    built on `0..1023` by the SPREAD of its buckets, at a probability of about 2.4e-7 for a
//!    single key. An inference that is usually right is a smaller version of the defect this file
//!    exists to remove: it can be wrong SILENTLY, and a store on the wrong range reads perfectly
//!    while sitting outside every per-bucket sweep the shard runs.
//!
//!    THE WIDTH IS NOT NECESSARILY LOST, THOUGH, AND THIS ARM USED TO SAY IT WAS -- "NOT
//!    RECOVERABLE", because "nothing recorded it". Something did: a pre-stamp address CARRIED the
//!    bucket it was filed under, in the `rs` field retired from `BlockAddressWire` later, beside
//!    the object key the bucket was derived from. That makes the width a SOLVE rather than a
//!    guess, and one that carries its own check:
//!
//!    ```text
//!      bucket = start + FNV-1a-64(object_key) % width
//!      => width divides (FNV-1a-64(object_key) - bucket) for EVERY stored address
//!      => take the gcd of those differences, keep its divisors above the largest bucket seen,
//!         and accept a candidate only if it reproduces EVERY recorded bucket.
//!    ```
//!
//!    A wrong candidate fails that last step on very nearly every address, so unlike the spread
//!    inference above this cannot be quietly wrong: it either reproduces the buckets or it does
//!    not. Carried out on a 13 GB pre-stamp store: the gcd over 12,125 addresses is 4,294,967,295,
//!    exactly one of its divisors exceeds the largest recorded bucket (4,294,914,071), and that
//!    width reproduces 12,125 of 12,125. `width = (end - start).saturating_add(1)`, so a width of
//!    `u32::MAX` means `0..u32::MAX` -- the pre-stamp fallback -- while `0..u32::MAX - 1` computes
//!    the same buckets: the solve fixes the WIDTH, and those two ends are the same modulus.
//!
//!    THE ENGINE DOES NOT DO THIS, deliberately. Those addresses are in the SERVED INDEX, and an
//!    index old enough to carry no stamp is old enough that `decode_index_bytes` refuses its
//!    format version before a byte of it is read. Solving the width belongs to a tool that reads
//!    the old index. What belongs here is not telling an operator the answer is unrecoverable
//!    while it is sitting in their store.
//!
//!    THIS ARM USED TO ADOPT THE WHOLE KEYSPACE rather than refuse, reasoning that it "was the
//!    ONLY default a store could have been built on". That is true of the DEFAULT and false of the
//!    CONFIGURATION: `TS_SHARD_END_ROUTING_BUCKET` is documented, and `docs/runtime_tuning.md`
//!    told operators to set it before the first ingest. So a store built narrow before this file
//!    existed was adopted onto the whole keyspace and quietly mis-ranged -- measured at 600 of 600
//!    blocks in `a_pre_stamp_store_built_narrow_is_adopted_onto_a_range_that_cannot_compute_its_buckets`.
//!    The adoption existed to keep existing deployments starting; before the first milestone there
//!    are none to keep, so the honest answer replaces the convenient one.
//!
//! A store with NO stamp and NO on-disk state is new: it is stamped with the requested range, which
//! is what lets the shipped default move for new stores without touching existing ones.
//!
//! # THE STAMP IS NOT PART OF THE INDEX
//!
//! It is its own small file beside `shard-{id}.index.json`, for two reasons. The serialized index
//! shape does not move, so an older build reads a newer store's index unchanged; and the check runs
//! BEFORE the decode, which is where a mismatch has to be caught -- after the decode the pages are
//! already in the wrong buckets in memory.

use super::*;
// Spelled out rather than taken from `super::*`: the engine's glob does not re-export the derive
// macros, and `#[derive(Serialize)]` fails with "cannot find derive macro" rather than with
// anything that points at the glob.
use serde::{Deserialize, Serialize};

/// A load refused because the stamp names a range the request does not: the range is KNOWN and
/// the configuration disagrees with it.
pub(super) const ROUTING_RANGE_MISMATCH: &str = "routing_range_mismatch";

/// A load refused because the store has state and no stamp: the range is NOT RECOVERABLE.
