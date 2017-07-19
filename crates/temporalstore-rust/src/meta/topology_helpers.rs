// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Topology / placement / stats helpers, extracted from meta.rs.

use super::*;
use std::sync::atomic::Ordering;
use std::collections::BTreeSet;
use crate::types::{ShardId, Status};

pub(super) fn table_shard_id(
    table: &TableMetaInfo,
    offset: u64,
) -> Result<ShardId, crate::partition_id::PartitionIdError> {
    Ok(table.first_shard_id + offset)
}

/// Whether `table` owns `shard_id`.
///
/// A table's shard ids are `first_shard_id + offset` for every offset below its
/// shard count, which makes them a contiguous range -- so this is a bounds
/// check, not a search. It used to be written as a search: every candidate id
/// was generated and compared, one per shard in the table.
///
/// That cost is paid per lookup, and the two callers that matter look one up
/// for every registered shard in the cluster. Retention planning and placement
/// were therefore quadratic in the fleet and linear again in the width of each
/// table -- a hundred tables of a hundred shards each turned ten thousand
/// lookups into a hundred million comparisons, on a timer.
///
/// `saturating_add` rather than `+`: `first_shard_id` is supplied by the caller
/// that created the table, and a value near the top of the range would overflow
/// on the way to computing the end of it.
fn table_owns_shard(table: &TableMetaInfo, shard_id: ShardId) -> bool {
    let first = table.first_shard_id;
    shard_id >= first && shard_id < first.saturating_add(table.shard_count)
}

/// Which table owns each registered shard.
///
/// Built once per round. The obvious way -- asking [`table_for_shard`] for every
/// registered shard -- costs shards times tables, because that answers by
/// scanning every table, and the rounds that want this run on an interval
/// holding the read lock that heartbeats wait on.
///
/// A table's shards are a contiguous range, so the ranges sorted by their start
/// answer each shard with a binary search. Ranges are disjoint in any cluster
/// that assigns them the way `add_table` does; when two really do overlap the
/// per-shard scan still decides, because it resolves them by table map order and
/// that is the behaviour to keep.
pub(super) fn shard_owning_tables(state: &MetaState) -> BTreeMap<ShardId, &TableRecord> {
    // Learning the ranges costs a pass over the tables and a sort; the scan
    // costs a pass over the tables per shard. So the index only pays once there
    // are more shards than the log of the table count -- with a handful of
    // shards registered and a large table map, which is what a metaserver looks
    // like just after a restart, building it would be the more expensive way to
    // answer.
    let table_count = state.tables.len();
    let log_tables = (usize::BITS - table_count.leading_zeros()) as usize;
    if state.shards.len() <= log_tables {
        return state
            .shards
            .keys()
            .filter_map(|shard_id| Some((*shard_id, table_for_shard(state, *shard_id)?)))
            .collect();
    }
    let tables_in_order = state.tables.values().collect::<Vec<_>>();
    let mut ranges = tables_in_order
        .iter()
        .enumerate()
        .map(|(order, table)| {
            let first = table.info.first_shard_id;
            (first, first.saturating_add(table.info.shard_count), order)
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|(first, _, _)| *first);
    let disjoint = ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0);
    state
        .shards
        .keys()
        .filter_map(|shard_id| {
            let table = if disjoint {
                let above = ranges.partition_point(|(first, _, _)| first <= shard_id);
                let (first, end, order) = *ranges.get(above.checked_sub(1)?)?;
                (*shard_id >= first && *shard_id < end).then(|| tables_in_order[order])?
            } else {
                table_for_shard(state, *shard_id)?
            };
            Some((*shard_id, table))
        })
        .collect()
}

pub(super) fn table_for_shard<'a>(state: &'a MetaState, shard_id: ShardId) -> Option<&'a TableRecord> {
    if !state.table_ranges_overlap {
        // The shard belongs to the nearest table starting at or below it, if
        // that table's range reaches it. Nothing else can own it: every other
        // table starts above the shard, or ends before the nearest one begins.
        return state
            .table_starts
            .range(..=shard_id)
