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
