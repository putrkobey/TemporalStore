// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A LIVE-USER-BYTES DENOMINATOR WOULD COST, AND WHETHER IT CAN BE HAD FOR NOTHING.
//!
//! # THE ONE RATIO IN THE SET THAT IS NOT PUBLISHED
//!
//! The write ratios this store publishes all divide STORED bytes by STORED bytes, which is what
//! makes them unit-matched and compressibility-invariant. The ratio that is missing is
//! logical-bytes-written over LIVE USER BYTES -- how many times over the workload has written the
//! uncompressed data it is keeping. It is absent because nothing holds a live user-byte total: an
//! object record carries `page_size`, which is assigned from `address.length()` and is therefore
//! the STORED length, and `block_slab_live`'s tally sums the same quantity.
//!
//! There are three ways to get the denominator, and they differ by an order of magnitude in cost:
//!
//!   a. AN EXACT SIZE ON THE RECORD. A `u32` of uncompressed length. `ObjectRecord` is 15 bytes of
//!      field in 16, so a `u32` takes it to 19 of field and, at `align_of` 8, to 24 bytes wide --
//!      eight bytes a record, which this arm weighs through the table that holds it rather than
//!      quoting the struct delta.
//!   b. A ONE-BYTE SIZE CLASS IN THE TAIL SLACK. The record pins its own slack --
//!      `size_of::<ObjectRecord>() - 15 == 1` -- so one byte is already paid for and a `u8` code
//!      changes no width at all. It is approximate, and this arm measures by how much.
//!   c. DIVIDING THE STORED TALLY BY THE STORE-WIDE COMPRESSION RATIO. Free, needs no field, and
//!      wrong exactly when the live set compresses differently from the history. This arm is built
//!      to make that visible rather than to assume it.
//!
//! # SO THE CORPUS MIXES COMPRESSIBILITY ON PURPOSE
//!
//! Four cohorts: two sizes crossed with incompressible and highly-compressible values. A uniform
//! corpus cannot tell (c) apart from (a) -- with one compressibility the store-wide ratio IS the
//! live set's ratio and the estimate is exact by construction. The arm asserts the cohorts really
//! did store differently before it reports any error, because an estimate that looks perfect on a
//! fixture that could not challenge it is the fixture trap this campaign keeps finding.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_a_live_user_bytes_denominator_would_cost -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

/// Keys per cohort.
const PER_COHORT: usize = 400;

/// A one-byte size class: 4-bit exponent, 4-bit mantissa, round to nearest.
///
/// Code 0 means zero bytes. Otherwise `value = (16 + m) << e` for `e` in 0..16 and `m` in 0..16,
/// which spans 16 B to 1,015,808 B in steps of one sixteenth -- a worst-case 3.1% per object, and
/// less than that in a sum because the roundings are in both directions. Values above the top class
/// saturate, which is stated rather than hidden: this is a measurement of whether the shape is
/// worth building, and the production encoding would widen the exponent if the corpus needed it.
fn size_class(len: u64) -> u8 {
    if len == 0 {
        return 0;
    }
    let mut best_code: u8 = 1;
    let mut best_error = u64::MAX;
    for code in 1u16..=255 {
        let e = (code - 1) >> 4;
        let m = (code - 1) & 15;
        let value = (16u64 + m as u64) << e;
        let error = value.abs_diff(len);
        if error < best_error {
            best_error = error;
            best_code = code as u8;
        }
    }
    best_code
}

fn declass(code: u8) -> u64 {
    if code == 0 {
        return 0;
    }
    let e = (code as u16 - 1) >> 4;
    let m = (code as u16 - 1) & 15;
    (16u64 + m as u64) << e
}

/// Incompressible bytes: no short run, no small period.
fn noisy(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

