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
