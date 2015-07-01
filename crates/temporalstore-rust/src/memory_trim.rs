// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Returning freed heap to the operating system.
//!
//! Rust's `free` hands memory back to the allocator, not to the kernel. On glibc -- what this runs
//! on, since the only `#[global_allocator]` here is a test-only probe -- freed chunks stay in
//! per-thread arenas, and the process keeps its resident pages until the heap top happens to be
//! free or something calls `malloc_trim`.
//!
//! Measured in process: dropping 46 MB of live data returned 2.3% of it; one `malloc_trim(0)`
//! returned 95% of what was retained. A proxy holding 128.6 MB of live data measured 444.9 MB
//! resident, which is the same effect at deployment scale.

/// Ask the allocator to return free heap to the operating system.
///
/// Returns whether anything was released. `false` covers three different things and deliberately
/// does not distinguish them at the call site: the allocator had nothing to give back, the trim is
