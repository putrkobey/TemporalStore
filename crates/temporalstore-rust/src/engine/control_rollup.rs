// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Multi-resolution rollup ladder for Control State counters.
//!
//! Long-window Control State aggregates (frequency caps / counts over hours→weeks)
//! otherwise scan every raw bucket in the window. This module maintains, per key, a
//! ladder of exact per-bucket sums at 1s → 1m → 1h → 1d, so a wide-window sum-family
//! query reads O(levels) coarse cells plus the sub-second edges instead of O(range).
//!
//! Correctness: a cell stores the exact sum of the increments folded into its span, so
//! a fully-covered window returns a value bit-identical to the raw range scan; partial
//! sub-second edges fall back to the authoritative `shard.control_state` series. The
//! index is a derived, `#[serde(skip)]` view: kept incrementally on counter writes,
//! rebuilt from the authoritative series whenever its recorded `built_len` no longer
//! matches the series length (a safety net against any un-instrumented growth), dropped
//! on delete/expire, and cleared on reconcile. Everything here is gated by
//! `Config.control_rollup_enabled()`; with the gate off nothing in this module runs and
//! the default path is byte-for-byte unchanged.

use std::collections::BTreeMap;

use super::state::ShardState;

/// Rollup bucket widths in milliseconds, finest first. Each is an integer multiple of
/// the previous (1s→1m→1h→1d) so the levels tile cleanly.
pub(crate) const LEVEL_WIDTHS_MS: [u64; 4] = [1_000, 60_000, 3_600_000, 86_400_000];
const FINEST_WIDTH_MS: u64 = LEVEL_WIDTHS_MS[0];

type CellKey = (u8, u64); // (level index, bucket_start_ms)

/// Per-key rollup index: exact per-cell sums plus the control_state series length the
/// index was last built/folded against (staleness guard).
#[derive(Debug, Default, Clone)]
pub(crate) struct RollupEntry {
    cells: BTreeMap<CellKey, i64>,
    built_len: usize,
}

fn bucket_start(ts_ms: u64, width_ms: u64) -> u64 {
    ts_ms - ts_ms % width_ms
}

fn fold_cells(cells: &mut BTreeMap<CellKey, i64>, bucket_ms: u64, amount: i64) {
    for (level, width) in LEVEL_WIDTHS_MS.iter().enumerate() {
        *cells
            .entry((level as u8, bucket_start(bucket_ms, *width)))
            .or_default() += amount;
    }
}

fn build(series: &BTreeMap<u64, i64>) -> BTreeMap<CellKey, i64> {
    let mut cells = BTreeMap::new();
    for (bucket_ms, amount) in series {
        fold_cells(&mut cells, *bucket_ms, *amount);
    }
    cells
}

fn coarsest_fit(cursor: u64, hi: u64) -> (u8, u64) {
    let mut chosen = (0u8, FINEST_WIDTH_MS);
    for (level, width) in LEVEL_WIDTHS_MS.iter().enumerate() {
        if cursor % width == 0 && cursor + width <= hi {
            chosen = (level as u8, *width);
        }
    }
    chosen
}

fn range_sum(cells: &BTreeMap<CellKey, i64>, series: &BTreeMap<u64, i64>, start_ms: u64, end_ms: u64) -> i64 {
    if start_ms > end_ms {
        return 0;
    }
    let e = end_ms.saturating_add(1); // exclusive upper bound
    let w0 = FINEST_WIDTH_MS;
    let mid_lo = start_ms.div_ceil(w0) * w0; // first whole-second boundary >= start
    let mid_hi = (e / w0) * w0; // last whole-second boundary <= end+1
    if mid_lo >= mid_hi {
        // window smaller than one finest bucket: raw scan
        return series.range(start_ms..e).map(|(_, v)| *v).sum();
    }
    let mut sum: i64 = 0;
    sum += series.range(start_ms..mid_lo).map(|(_, v)| *v).sum::<i64>(); // left sub-second edge
    sum += series.range(mid_hi..e).map(|(_, v)| *v).sum::<i64>(); // right sub-second edge
    let mut cursor = mid_lo; // middle covered by coarsest cells
    while cursor < mid_hi {
        let (level, width) = coarsest_fit(cursor, mid_hi);
        if let Some(cell) = cells.get(&(level, cursor)) {
            sum += *cell;
        }
        cursor += width;
    }
    sum
}

/// Aggregators the rollup ladder can serve exactly (the sum family). "count"/"" map to
/// a windowed sum of the stored counter values in `aggregate_control_state_values`.
pub(crate) fn is_sum_family(aggregator: &str) -> bool {
    matches!(aggregator.trim().to_ascii_lowercase().as_str(), "" | "sum" | "count")
}

/// Called AFTER `shard.control_state[key]` has been updated with `amount` at `bucket_ms`.
pub(crate) fn record_increment(shard: &mut ShardState, key: &str, bucket_ms: u64, amount: i64) {
    let series_len = shard.control_state().get(key).map(|s| s.len()).unwrap_or(0);
    match shard.control_state_rollups_mut().get_mut(key) {
        Some(entry) => {
            fold_cells(&mut entry.cells, bucket_ms, amount);
            entry.built_len = series_len;
        }
        None => {
            // First touch this session: build from the full (already-updated) series.
            let cells = shard.control_state().get(key).map(build).unwrap_or_default();
            shard
                .control_state_rollups_mut()
                .insert(key.to_string(), RollupEntry { cells, built_len: series_len });
        }
    }
}

/// Generic sum-family windowed aggregate over any (rollup index map, i64 series) pair.
/// Rebuilds the key's index from `series` when its recorded length no longer matches the
/// series (staleness guard), then covers the window with coarse cells + raw edges. This is
/// the one primitive shared by the control-state counter path and the feature-aggregate path.
pub(crate) fn windowed_sum_series(
    rollups: &mut std::collections::HashMap<String, RollupEntry>,
    series: &BTreeMap<u64, i64>,
    key: &str,
    start_ms: u64,
    end_ms: u64,
) -> i64 {
    let series_len = series.len();
    let stale = rollups
