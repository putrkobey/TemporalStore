// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A STRING POINT READ MUST LEAVE THE BLOCK UNDER ITS PHYSICAL KEY.
//!
//! # WHAT THIS EXISTS TO STOP, AND WHY A COMMENT WAS NOT ENOUGH
//!
//! A change was implemented and reverted that stopped caching the block frame on this path, on the
//! reasoning that the payload was cached twice and a frame serves exactly one key. Both halves of
//! that were measured and true. The conclusion did not follow:
//!
//!   * `read_bucket_index_value` is the **only** door from a string command into
//!     `read_block_bytes`, so declining the frame there does not drop a duplicate — it removes the
//!     physically-keyed tier for the commonest data kind;
//!   * that key is the **persistent block cache's** key. Six arms in `part2` assert a tiny memory
//!     cache refills from it *without a block-store read*;
//!   * and since the logical copy was dropped, the frame is the **only** copy. The same change
//!     today would leave a string read with no cache at all.
