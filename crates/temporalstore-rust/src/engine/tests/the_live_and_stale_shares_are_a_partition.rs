// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LIVE AND STALE SHARES ARE A PARTITION OF THE BYTES ON DISK, and must behave like one.
//!
//! # WHY A PARTITION IS A STRONGER PUBLISHED SHAPE THAN A RATIO
//!
//! `live_bytes_ppm` and `stale_bytes_ppm` are computed over the same whole —
//! `non_purged_physical_bytes` — so they **sum to 1,000,000** within integer floor rounding. That
//! identity is checkable from the published numbers alone.
//!
//! None of the four amplification ratios on the same struct has that property. Each divides one
//! quantity by another with nothing constraining the answer, which is why
//! `logical_write_amplification_ppm` could carry a name that did not describe its own arithmetic for
//! as long as it did: no identity over the set would have failed, and
//! `the_published_amplification_ratios_are_the_ratio_they_name` pins self-consistency rather than
//! agreement with any definition.
//!
//! # TWO ASSERTIONS, BECAUSE EACH CATCHES WHAT THE OTHER CANNOT
//!
//!   * **The parts sum.** A term computed over the wrong whole fails here immediately.
//!   * **The stale share rises under rewrite, while the live set does not grow.** A figure can sum
//!     correctly and still be constant for the wrong reason — which is exactly how the
//!     slab-granular `space_amplification_ppm` passed every check it had while pinned at 1.000x
//!     over five rewrites of every key. Rewriting the same keys is the only instrument that
//!     separates "nothing is stale" from "stale is not being counted", and asserting BOTH halves is
//!     what makes either one evidence.
//!
//! # THE FLOOR IS EXACTLY ZERO, WHICH THIS ARM FOUND AND ITS OWN DOC GOT WRONG FIRST
//!
//! This section predicted a small non-zero stale share on the first round, because the live term is
//! block-payload bytes and the whole is file bytes, so framing would fall on the stale side.
//! **Measured: exactly 0.00%**, with the parts summing to 1,000,000.
//!
//! The terms agree because `live_block_bytes` sums `BlockAddress::length` — what was STORED, after
//! compression and including the block envelope — and `non_purged_physical_bytes` is the file size
//! of those same bytes. Both sides are stored bytes.
//!
//! Framing is real against a different denominator: 536,000 B on disk for 512,000 B of USER bytes
//! (`the_payload_on_all_three_media`), which is stored bytes over PRE-COMPRESSION bytes — the
//! mixed-unit division these fields exist to avoid. The equality is now asserted below, so if a
//! framing term ever does appear on one side it fails here instead of looking like a documented
//! floor.
