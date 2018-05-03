// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A BLOCK ADDRESS COSTS A RECORD, IN BOTH BYTE COLUMNS AND AT BOTH ROUTING RANGES.
//!
//! An address exists twice per stored record -- once in the model map a read resolves through,
//! once inside the page-index entry -- and both live for the life of the shard. So its width
//! multiplies by the record count, and the question "is eight bytes off it worth anything" is
//! answered by a count, not by arithmetic on one struct.
//!
//! THIS MODULE IS WRITTEN TO RUN UNCHANGED ON EITHER SIDE OF THE MERGE. It names no constant
//! and no helper that the merge introduced: it reads `size_of::<BlockAddress>()` and counts the
//! addresses a real seeded shard holds. Dropping the same file into a pristine tree and running
//! it there is the before arm, and that is the only honest way to get one -- an "after" figure
//! computed from a "before" that was never measured is arithmetic wearing a measurement's
//! clothes.
//!
//! FOUR THINGS THIS IS CAREFUL ABOUT, each because getting one of them wrong has already cost
//! this campaign a wrong number:
//!
//!   * BOTH BYTE COLUMNS. `ALLOC_BYTES` charges `layout.size()` -- what the caller asked for --
//!     and `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, what the allocator actually took. An
//!     inline-versus-out-of-line comparison read off the request column alone is biased in a
//!     known direction, so both are reported and the chunk column is the one that decides
//!     whether a width change survives to the heap.
//!   * BOTH ROUTING RANGES. At the default range (`0..=u32::MAX`) every key lands in a bucket of
//!     its own BY CONSTRUCTION, which makes "one page per bucket" an artefact of the fixture
//!     rather than a fact about the workload. `docs/runtime_tuning.md` tells an operator to set
//!     `TS_SHARD_END_ROUTING_BUCKET=1023` before the first ingest, and that is the range whose
//!     numbers describe a store. Both are run and the operator's is named.
//!   * A HISTOGRAM, NEVER A MEAN. A mean of 1.98 pages a bucket in this tree once contained zero
//!     buckets holding exactly two. Percentiles, the maximum and the per-bucket counts are
//!     printed, with the denominator asserted on every row.
//!   * THE STORE PATH LENGTH HELD CONSTANT. Allocation counts in this engine move at about six
//!     bytes per character of store path, which is large enough to swamp what is being measured.
//!     Every arm's path length is recorded and asserted equal.

use super::*;
use std::collections::BTreeMap;
use std::mem::size_of;

// Imported as a NAME rather than spelled at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with, which `docs/runtime_tuning.md` tells an
/// operator to set before the first ingest.
const OPERATOR_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses by default. At this range a key's routing
/// bucket is its hash, so two keys sharing a bucket is a collision rather than a workload.
const DEFAULT_END: u32 = u32::MAX;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "address-word".to_string(),
        shard_uri: "local://address-word/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
