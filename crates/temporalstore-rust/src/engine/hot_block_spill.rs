// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Durable read-by-address fallback for log-backed hot pages.
//!
//! Under `async_storage`, a freshly written value is served only from the in-memory cache tier
//! at a SYNTHETIC page address (`block_slab_id == HOT_BLOCK_SLAB_ID`); it is not written to the
//! block store, so it exists on disk only as its WAL record. That has two costs:
//!
//!   1. Correctness -- if the memory-only entry is evicted before the next dump/flush
//!      materializes it, a read misses the cache and `page_store.read(synthetic_address)` finds
//!      no file, so the acked write reads back as MISSING (looks deleted). Only a full reload
//!      (WAL replay) would recover it.
//!   2. Memory footprint -- hot pages are a primary consumer of the DRAM tier because their only
//!      residence is memory.
//!
//! This module fixes both by SPILLING a hot page to a real slab exactly when it is evicted from
//! the cache: an eviction handler writes the evicted bytes to the durable block store and records
//! a redirect from the synthetic address to the real one. Reads consult the redirect on a
//! hot-address cache miss and read the real slab, so an evicted hot page is never read-as-missing.
//! Because the eviction moves the bytes out of DRAM (to the block store / SSD-backed slab) and
//! keeps only a tiny redirect entry, steady-state RSS drops as well.
//!
//! The redirect map is purely live-path state: on crash/reload the WAL is the source of truth and
//! re-derives every hot page from scratch, so the map is never persisted. It is bounded by
//! clearing a shard's entries on unload.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use matrixcache::{CacheEvictionRecord, MultiLayerCache};

use crate::block_store::{ElementEntry, BlockStore};
use crate::types::ShardId;

use super::constants::HOT_BLOCK_SLAB_ID;
