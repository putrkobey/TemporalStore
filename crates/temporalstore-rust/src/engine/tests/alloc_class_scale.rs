// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A STORED RECORD'S MEMORY IS SPENT ON, and whether each part is flat per record.
//!
//! The counting allocator reported one undifferentiated total. At four thousand records "6.0
//! allocations per record" is a fine summary; at a large corpus the same number describes a store
//! whose index is flat per record and one whose index is outgrowing its data, and those two want
//! opposite fixes. `alloc_probe::AllocClass` splits the same allocations by the sink they land in.
//!
//! THE BILL. Ingest of 1,024-byte incompressible values through `batch_execute`, 100 to a batch,
//! debug profile, counted with `--features alloc-probe`:
//!
//! ```text
//!   class                allocs/record   bytes/record   ratio 2,000 -> 20,000
//!   cache_invalidation          35.06            995           1.000
//!   slab_append                  6.00          1,192           1.000
//!   bucket_index                 5.63            676           1.000
//!   staged_outcome               2.06            252           1.000
//!   block_bytes                   2.00          2,115           1.000
//!   dirty_objects                1.31            121           0.999
//!   index_log_delta              1.26            372           1.013
//!   carried_block                 1.06          1,065           1.000
//!   log_record                   0.03          2,386           0.968
//!   nine classes summed         54.42          9,172
//!   SPAN TOTAL                  80.69         11,933           <- independent counter
//!   residual                    26.27          2,761           1.000
//! ```
//!
//! EVERY CLASS IS FLAT PER RECORD ACROSS A TEN-TIMES CORPUS, and so is the residual. Nothing in
//! this write path grows with the size of the store. That is the finding; the classes exist so a
//! later run can say which row stopped being flat.
//!
//! THE TWO COLUMNS DISAGREE ABOUT WHAT MATTERS, which is why both are kept. By CALLS the biggest
//! thing a write does is invalidate one serving-cache key -- 35.06 of the 80.69, more than the
//! other eight classes together. By BYTES that class is fifth, and the biggest is the write-ahead
//! record, which makes 0.03 allocations a record because it is built as whole buffers. A report
//! with only one of these columns gives the opposite answer about where to look.
//!
//! THE CLASSES WERE MEASURED, NOT CHOSEN. Four candidates were scoped, run, and dropped:
//!
//!   * `exec_apply`, everything in `execute_on_shard` outside the named classes: 2.00 allocations
//!     and 15 bytes a record. A catch-all, not a sink; it belongs in the residual.
//!   * `model_map`, the `shard.strings` insert: 1.00 allocations and 276 bytes a record, real but
//!     with NO CHOKE POINT -- fourteen command arms insert into their own map inline. A class
//!     counted at one arm of fourteen is the defect this exercise is about, not a measurement, so
//!     it is in the residual and named here instead.
//!   * `command_clone`, the batch path's write-ahead append around the record encode: 0.02
//!     allocations and 12 bytes a record. Near zero, so it would be noise in every future report.
//!   * per-key CONTEXT. This engine's `context_*` maps are not touched by a value write at all --
//!     they belong to the context commands -- so a class for them would be a row of zeros in every
//!     line of this table.
//!
//! and one candidate was moved rather than dropped: an `execute_on_shard` scope placed at the
//! single-command CALL SITE read exactly ZERO on this workload, because `batch_execute` is a
//! second live caller of the same function. That is the whole argument for charging inside the
//! callee, arrived at from the other direction.
//!
//! WHAT THE RESIDUAL IS. `ClassSpan` measures the span twice: once with the process-wide counter,
//! which the class rows do not feed, and once as the rows. The difference is a reading and can be
//! wrong. A residual computed from the rows it audits cannot fail, and this crate shipped one --
//! `restore_scale`'s allocation residual read zero "by construction" until it was measured and
//! turned out to be five.
//!
//! WHAT THIS ACCOUNTING CANNOT SEE, stated here because a total that silently omits something is
//! worse than a smaller total that says so:
//!
//!   * Anything not allocated through Rust's global allocator. The serving cache is a separate
//!     crate: the bytes this engine hands it are counted as it hands them over, and whatever that
//!     crate then does with them is not.
//!   * Memory-mapped regions and the OS page cache holding slab files. A slab read the kernel
//!     serves from cache costs this accounting the destination buffer and nothing else.
//!   * Stack, static and thread-local storage.
//!   * Allocator retention. These are allocation CALLS and the bytes asked for, not resident set:
//!     71% of one measured proxy's resident memory was memory the allocator had kept rather than
//!     live data, which is the reason this probe exists instead of a resident-size delta.
//!   * Other threads. The class tag is thread-local, so a background thread's allocations land in
//!     no class and inflate the residual rather than a row.
//!
//! and one it can see only approximately: a free is charged to the class in scope WHEN THE FREE
//! HAPPENS. Inside a narrow synchronous primitive that is the class that allocated it, but a block
//! allocated in one class and released in another is charged to the releasing one. The allocated
//! columns carry no such caveat; nothing here asserts on `outstanding` alone.
#![allow(clippy::all)]
// The measuring tests exist only with `alloc-probe`, so their sizes and helpers go unread in an
// ordinary build.
#![allow(dead_code)]
use super::*;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;
const VALUE_BYTES: usize = 1_024;
const BATCH: usize = 100;

/// xorshift64*, seeded by index, so no two records share a payload.
///
/// A repeated byte compresses, and the biggest class by bytes is a compressing encode: a corpus of
/// identical values would report the page-byte class at a fraction of its real size and call that
/// a measurement.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Guards that hold in EVERY build, including one with no counting allocator at all.
// ---------------------------------------------------------------------------------------------

/// Every class owns one row, the rows are dense, and the table is as long as the classes are.
///
/// The compile errors do most of this -- a class with no `slot` and no `label` does not build --
/// but neither of those catches two classes sharing a row, which would make one of them silently
/// report the other's numbers.
#[test]
fn alloc_class_slots_are_dense_and_in_order() {
    use crate::alloc_probe::AllocClass;
    let mut labels: Vec<&'static str> = Vec::new();
    for (index, class) in AllocClass::ALL.iter().enumerate() {
        assert_eq!(
            index,
            AllocClass::ALL
                .iter()
                .position(|candidate| candidate == class)
                .expect("a class in ALL is findable in ALL"),
            "{} appears twice in ALL, so one of its rows is unreachable",
            class.label()
        );
        labels.push(class.label());
    }
    labels.sort_unstable();
    let before = labels.len();
    labels.dedup();
    assert_eq!(
        before,
        labels.len(),
        "two allocation classes share a label, so a report cannot tell them apart"
    );
    assert!(
        AllocClass::ALL.len() >= 6,
        "there were nine classes when this was written; {} is not the table this file documents",
        AllocClass::ALL.len()
    );

    // AND THE SLOTS THEMSELVES, which this test is named for and did not look at.
    //
    // Everything above compares ENTRIES and LABELS. Two classes can be distinct entries with
    // distinct labels and still return the same slot, and then they share one counter row: the
    // later one's charges land on the earlier one, so a report double-counts the first and shows
    // the second as a flat zero in every column. Nothing here noticed. A mutant that pointed a
    // tenth class at the ninth's slot survived this test and every other test in a wide slice
    // around it, which is how it was found.
    //
    // Dense and in order, both asserted: slot `i` belongs to `ALL[i]`.
    for (index, class) in AllocClass::ALL.iter().enumerate() {
        assert_eq!(
            class.slot(),
            index,
            "{} sits at slot {} but is entry {index} of ALL; the ledger indexes its rows by \
             position, so this class and whichever one owns slot {} would share a row",
            class.label(),
            class.slot(),
            class.slot()
        );
    }
}

/// Every declared class is entered by production code.
///
/// THE DEFECT THIS EXISTS FOR. Six counters in this crate have been declared and then incremented
/// at a fraction of their call sites or at none of them; the most recent was declared, reset,
/// read, reported and never incremented, with a document naming the floor it was supposed to hold.
/// A class is worse than a bare counter in that respect, because its row still adds up -- it
/// contributes zero to a sum that reconciles perfectly without it.
///
/// So: the source is scanned for a scope naming each class, in a file that is neither a test nor
/// the module that declares them. A class nobody enters fails here, by name.
///
/// THE CONTROL IS THE PLANTED COUNT. Nine classes are declared and this scan must find a site for
/// every one of them; a scan that had broken finds none, and a scan whose exclusion had started
/// matching everything reports a denominator that collapsed. Both are asserted before the verdict.
#[test]
fn every_alloc_class_has_a_production_scope() {
    use crate::alloc_probe::AllocClass;
    use std::path::Path;
