// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAYLOAD ON ALL THREE MEDIA — logical, in memory, on disk — against ONE denominator.
//!
//! # WHY ONE DENOMINATOR
//!
//! The amplification set reports ratios. This reports the three QUANTITIES the payload occupies,
//! each divided by the same thing: the user bytes offered to the store. Every payload figure that
//! went wrong in this area went wrong because its denominator was not the one its numerator was in
//! — a write ratio that was really the compression factor, a cache figure multiplied by that same
//! factor, an index ratio dividing its own numerator.
//!
//!   * **logical** — `logical_bytes_written`, the bytes the caller handed over, before compression.
//!   * **on disk** — the slab files, which depend entirely on how the values compress.
//!   * **in memory** — `cache.memory_bytes`, which sums cached value lengths.
//!
//! # BOTH FIXTURES, BECAUSE ONE OF THE THREE DEPENDS ON THE DATA
//!
//! A run of `b'v'` compresses to almost nothing, so the disk figure reads far below its logical
//! size; incompressible bytes make it slightly above. Reporting one fixture would state a disk cost
//! that is really a statement about the letter `'v'`.
//!
//! The memory figure is indifferent to the distribution since #2279 made the cached payload a
//! msgpack byte string that charges by length alone. Before that change the two fixtures differed
//! there too, by half the value size, so the agreement is worth showing rather than assuming.
//!
//! # THE SHAPE THIS PUTS IN ONE PLACE
//!
//! Memory used to be the dearest of the three media, at 2.082x, because a point read filled TWO
//! cache layers that both held the payload: the command response under `CacheKey::string` and the
//! block frame under `CacheKey::page_with_slot`. **The logical copy is gone**, so memory is now
//! exactly 1.000x — one copy, charged by length — and disk is the only medium whose figure depends
//! on the data.
//!
//! WHICH COPY WENT WAS NOT SETTLED BY THE FRAME-SHARING COUNT, though an earlier reading of this
//! arm said it was. `how_many_keys_a_cached_frame_serves` shows a frame never amortises across
//! keys; it does not show the frame entry is unreachable. The frame key is read by 25 call paths
//! and is the key of the persistent block cache, while the response copy was memory-only with a
//! single reader — so the response copy was the one to drop, and it was the larger of the two by the
//! msgpack envelope. `what_the_frame_layer_saves_under_eviction` holds the before-and-after.
