// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;

use crate::engine::constants::*;
use crate::block_store::BlockStore;
use crate::block_store::ElementEntry;
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent, ContextEntity,
    ContextEvent, ContextExtractedEventIndexes, ContextIndexLookup, ContextIndexRef, ContextNode,
    ContextPackAudit, ContextSummary, ContextTraversedNode, ContextWire,
    FeaturePoint, InternalContextIndex, ShardId, Status,
};
use matrixcache::MultiLayerCache;

use super::packed_blocks::{read_feature_point, read_feature_point_cached, read_feature_point_cold};
use super::hashing::BlockIdentity;
use super::timestamped_series::TimestampedSeries;
use super::{read_block_bytes, stable_object_hash, ShardState};
/// `prefix` then two decimal parts, joined by colons, in one allocation.
///
/// `format!` would take two: the String it returns, plus one inside the formatting machinery.
/// `write!` into a String that already has capacity takes none, so building the key directly halves
/// its cost. These run on the hot path -- `context_node_key` once per retrieval candidate -- and the
/// bytes are identical to the format strings they replace, which matters because these keys are
/// stored in the shard's maps and the bucket index and compared against keys already written.
fn context_key_2(prefix: &str, first: u64, second: u64) -> String {
    use std::fmt::Write as _;
    // Prefix, two u64 at 20 digits each, and the colon between them.
    let mut key = String::with_capacity(prefix.len() + 41);
    key.push_str(prefix);
    let _ = write!(key, "{first}:{second}");
    key
}

/// As [`context_key_2`], with a third part.
fn context_key_3(prefix: &str, first: u64, second: u64, third: u64) -> String {
    use std::fmt::Write as _;
    let mut key = String::with_capacity(prefix.len() + 62);
    key.push_str(prefix);
    let _ = write!(key, "{first}:{second}:{third}");
    key
}

pub(super) fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:node:", tenant_hash, node_hash)
}

pub(super) fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:event:", tenant_hash, node_hash)
}

pub(super) fn normalize_context_event_storage_keys(node_hash: u64, event: &mut ContextEvent) {
    if event.event_time_ms == 0 {
        event.event_time_ms = super::resolve_now_ms().max(1);
    }
    if event.ingestion_time_ms == 0 {
        event.ingestion_time_ms = event.event_time_ms;
    }
    if event.event_id_hash == 0 {
        event.event_id_hash = stable_object_hash(&format!(
            "context_event:{}:{}:{}",
            node_hash, event.event_time_ms, event.text
        ));
    }
}

pub(super) fn context_index_key(
    tenant_hash: u64,
    index_name: &str,
    index_value_hash: u64,
    scope_hash: u64,
) -> String {
    format!("ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}")
}

pub(super) fn context_index_disabled(
    indexes: &ContextExtractedEventIndexes,
    index: InternalContextIndex,
) -> bool {
    indexes.disabled_indexes.contains(&index)
}

pub(super) fn context_event_kind_hash(event: &ContextEvent) -> u64 {
    u64::from(event.event_type_code())
}

pub(super) fn context_audit_key(tenant_hash: u64, session_hash: u64) -> String {
    context_key_2("ctx:audit:", tenant_hash, session_hash)
}

pub(super) fn context_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:dirty:", tenant_hash, node_hash)
}

pub(super) fn context_embedding_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2(EMBEDDING_DIRTY_PREFIX, tenant_hash, node_hash)
}

/// The prefix the key above carries, named once so the predicate below cannot drift from it.
pub(super) const EMBEDDING_DIRTY_PREFIX: &str = "ctx:embdirty:";

// `is_embedding_dirty_key` WAS HERE, and the typed dirty key retired it.
//
// It recovered a mark's flavour from its key's prefix, and its own doc called that
// load-bearing: `ContextDirtyEntry::tenant_hash` was left ZERO on summary marks, so an
// all-tenant embedding drain would have collected them without the prefix test. `DirtyMark`
// carries the tenant for BOTH flavours and the flavour as a field, so there is no asymmetry
// left to guard and no prefix left to parse.
//
// The PREFIX itself stays, below, because the command surface still builds these key strings:
// `client/client_commands.rs` and `command_validation.rs` advertise the object key a command touches,
// which is a different job from keying a resident map.

pub(super) fn context_entity_key(tenant_hash: u64, node_hash: u64, entity_hash: u64) -> String {
    context_key_3("ctx:entity:", tenant_hash, node_hash, entity_hash)
}

/// Split a persisted per-entity key back into (collection key, entity hash).
///
/// The index stores entities grouped by node, but the PERSISTED page entry still carries the
/// per-entity key `ctx:entity:{tenant}:{node}:{entity_hash}` -- that is what keeps the on-disk
/// shape identical across this fold in both directions. Load calls this to rebuild the grouping.
/// Returns None for a key that does not carry a numeric entity hash, so a malformed or
/// foreign-shaped entry is skipped rather than folded under a wrong node.
pub(super) fn split_context_entity_key(object_key: &str) -> Option<(String, u64)> {
    let (collection_key, entity_hash) = object_key.rsplit_once(':')?;
    if !collection_key.starts_with("ctx:entity:") {
        return None;
    }
    // The collection key itself ends in the node hash, so it must still hold four segments
    // (ctx, entity, tenant, node) once the entity hash is removed -- otherwise this key WAS a
    // collection key and splitting it would silently reinterpret the node hash as an entity.
    if collection_key.split(':').count() != 4 {
        return None;
    }
    Some((collection_key.to_string(), entity_hash.parse::<u64>().ok()?))
}

pub(super) fn context_entity_collection_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:entity:", tenant_hash, node_hash)
}

pub(super) fn context_child_key(tenant_hash: u64, parent_hash: u64) -> String {
    context_key_2("ctx:child:", tenant_hash, parent_hash)
}

pub(super) fn context_summary_key(tenant_hash: u64, node_hash: u64, level: u32) -> String {
    context_key_3("ctx:summary:", tenant_hash, node_hash, u64::from(level))
}

pub(super) fn context_compression_key(tenant_hash: u64, node_hash: u64) -> String {
    context_key_2("ctx:compress:", tenant_hash, node_hash)
}

/// Configurable temporal-compression policy for a context node.
///
/// Hierarchical session archival: keep a recent
/// window of raw memory and progressively compress/fade older windows into summary
/// records once a threshold is crossed. Here the trigger is per-node and evidence-
/// oriented (count- or age-based) rather than per-session token budget, but the shape
/// is the same. Disabled by default; every field is overridable via environment.
#[derive(Debug, Clone)]
pub(super) struct ContextCompressionPolicy {
    pub(super) enabled: bool,
    pub(super) max_raw_events_per_node: usize,
    pub(super) keep_recent_events: usize,
    pub(super) window_size: usize,
    pub(super) max_event_age_ms: u64,
}

impl Default for ContextCompressionPolicy {
    fn default() -> Self {
        Self {
            // OFF, AND NOW FOR A MEASURED REASON RATHER THAN AN UNEXAMINED ONE.
            //
            // The trigger behind this flag was DEAD until the fix in `maybe_auto_compress_-
            // context_node`: it read event times from the series keyed by `event_id_hash`
            // instead of the timeline map, so every time came out 0 and the planner's pending
            // slice was always empty. Nothing noticed, because this default kept anything from
            // reaching it. So turning the flag on was, until now, a no-op.
            //
            // IT WORKS NOW, AND THE COST IS WHY IT STAYS OFF.
            // `the_auto_compression_trigger_folds_a_window_and_keeps_the_raw_events` measures
            // the record count at two sizes: 120 events produce 88 compression records and 200
            // produce 168 -- exactly `events - keep_recent_events`, one stored record per write
            // once a node is past the threshold. A node with ten thousand events would hold
            // about ten thousand compression records beside its raw ones. The fold is
            // non-destructive by design, so that state is ADDED rather than exchanged.
            //
            // That is a retention policy a deployment may well want -- a summary per window is
            // cheaper to read than the window -- but it is not a footprint reduction and must
            // not be defaulted on as if it were one. A deployment that wants it sets
            // `MATRIXARK_CONTEXT_COMPRESSION_ENABLED`, and now gets a trigger that fires.
            enabled: false,
            max_raw_events_per_node: 64,
            keep_recent_events: 32,
            window_size: 32,
            max_event_age_ms: 30 * 24 * 60 * 60 * 1000, // 30 days
        }
    }
}

/// How many nodes each traversal depth keeps, when the request does not say.
///
/// Retrieval breadth is a deployment decision, not a build-time one: a corpus of short playbook
/// steps wants a wider frontier than one of long prose, and the right value is found by measuring
/// recall on the operator's own documents. The request-level `top_k_per_depth` still wins where a
/// caller sets it; this only supplies the default it falls back to.
///
/// Widening costs latency roughly linearly -- traversal work is bounded by depth x top_k x
/// children -- which is why it is capped at CONTEXT_MAX_LIMIT rather than left open.
pub(super) fn default_traversal_top_k() -> usize {
    std::env::var("MATRIXARK_RETRIEVAL_TRAVERSAL_TOP_K")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_TOP_K)
}

/// Ceiling on nodes collected across the whole traversal, when the request does not say.
pub(super) fn default_traversal_candidates() -> usize {
    std::env::var("MATRIXARK_RETRIEVAL_MAX_CANDIDATES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_CANDIDATES)
}

pub(super) fn context_compression_policy_from_env() -> ContextCompressionPolicy {
    fn env_usize(name: &str, default: usize) -> usize {
        crate::env_flag::env_number(name, default)
    }
    fn env_u64(name: &str, default: u64) -> u64 {
        crate::env_flag::env_number(name, default)
    }
    // A local name for the crate vocabulary, not a second one. Spelled out here it accepted
    // `1` and `true` only, and -- worse than the missing words -- it answered `false` for
    // anything it did not recognise instead of falling back to the default it was handed.
    fn env_bool(name: &str, default: bool) -> bool {
        crate::env_flag::env_bool(name, default)
    }
    let d = ContextCompressionPolicy::default();
    ContextCompressionPolicy {
        enabled: env_bool("MATRIXARK_CONTEXT_COMPRESSION_ENABLED", d.enabled),
        max_raw_events_per_node: env_usize(
            "MATRIXARK_CONTEXT_COMPRESSION_MAX_RAW_EVENTS",
            d.max_raw_events_per_node,
        )
        .max(1),
        keep_recent_events: env_usize(
            "MATRIXARK_CONTEXT_COMPRESSION_KEEP_RECENT",
            d.keep_recent_events,
        ),
        window_size: env_usize("MATRIXARK_CONTEXT_COMPRESSION_WINDOW", d.window_size).max(1),
        max_event_age_ms: env_u64("MATRIXARK_CONTEXT_COMPRESSION_MAX_AGE_MS", d.max_event_age_ms),
    }
}

/// One planned compression window: a contiguous run of old event times to fold
/// into a single `ContextCompressionEvent`.
pub(super) struct ContextCompressionWindow {
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) count: usize,
}

/// Decide the next window (if any) to compress for a node, given its event times
/// sorted ascending, the per-node high-water mark (latest already-compressed event
/// time), and the current time. Keeps the newest `keep_recent_events` raw; only
/// compresses events past the watermark; bounds a pass to `window_size` events.
/// Pure and deterministic so it can be unit/example tested.
pub(super) fn plan_next_compression_window(
    policy: &ContextCompressionPolicy,
    event_times_sorted: &[u64],
    watermark: u64,
    now_ms: u64,
) -> Option<ContextCompressionWindow> {
    if !policy.enabled {
        return None;
    }
    let len = event_times_sorted.len();
    if len == 0 {
        return None;
    }
    let keep = policy.keep_recent_events.min(len);
    let compressible = &event_times_sorted[..len - keep];
    let oldest = match compressible.first() {
        Some(t) => *t,
        None => return None,
    };
    let count_trigger = len > policy.max_raw_events_per_node;
    let age_cutoff = now_ms.saturating_sub(policy.max_event_age_ms);
    let age_trigger = oldest < age_cutoff;
    if !count_trigger && !age_trigger {
        return None;
    }
    // Only events strictly newer than the watermark are still pending.
    let start_idx = compressible.partition_point(|&t| t <= watermark);
    let pending = &compressible[start_idx..];
    if pending.is_empty() {
        return None;
    }
    // Under a count trigger take the oldest pending window; under an age-only
    // trigger restrict to events older than the cutoff.
    let eligible: &[u64] = if count_trigger {
        pending
    } else {
        let end = pending.partition_point(|&t| t < age_cutoff);
        &pending[..end]
    };
    if eligible.is_empty() {
        return None;
    }
    let window = &eligible[..eligible.len().min(policy.window_size)];
    Some(ContextCompressionWindow {
        start_ms: *window.first().unwrap(),
        end_ms: *window.last().unwrap(),
        count: window.len(),
    })
}

pub(super) fn context_timeline_key(timestamp_ms: u64, disambiguator: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(disambiguator % CONTEXT_TIMELINE_FANOUT)
}

/// Events of one node within a time window, oldest first, as (timeline_key, address).
///
/// The primary event map is keyed by event id hash so update and delete can address a single
/// event in log n. Hash order is effectively random, so a time window is not a contiguous range
/// there; it is a contiguous range in `context_event_timeline`, which maps timeline_key ->
/// event id. This ranges over that index and dereferences each hit into the primary map, so a
/// time-windowed read stays log n + k instead of scanning the node's entire series.
///
/// Returns a DoubleEndedIterator because every caller reads newest-first (`.rev()`): retrieval
/// wants recent, serving-relevant context before cold history.
pub(super) fn context_event_time_range<'a>(
    shard: &'a super::state::ShardState,
    object_key: &str,
    start_time_ms: u64,
    end_time_ms: u64,
) -> impl DoubleEndedIterator<Item = (u64, &'a ElementEntry)> + 'a {
    let series = shard.context_events().get(object_key);
    let start = context_timeline_start(start_time_ms);
    let end = context_timeline_end(end_time_ms);
    shard
        .context_event_timeline
        .get(object_key)
        .into_iter()
        .flat_map(move |timeline| timeline.range(start..end))
        .filter_map(move |(timeline_key, event_id_hash)| {
            series
                .and_then(|series| series.get(*event_id_hash))
                .map(|address| (*timeline_key, address))
        })
}

pub(super) fn context_timeline_start(timestamp_ms: u64) -> u64 {
    timestamp_ms.saturating_mul(CONTEXT_TIMELINE_FANOUT)
}

pub(super) fn context_timeline_end(timestamp_ms: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(CONTEXT_TIMELINE_FANOUT)
}

pub(super) fn context_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(CONTEXT_DEFAULT_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT)
}

pub(super) fn context_bytes<T: ContextWire>(value: &T) -> Vec<u8> {
    value.encode_context_value()
}

pub(super) fn context_from_bytes<T: ContextWire>(bytes: &[u8]) -> Option<T> {
    T::decode_context_value(bytes)
}

/// The bucket a page's own key routes to, on the range this shard is stamped with.
///
/// THE ONE SPELLING, so that a reader and the write path that produced the page cannot disagree
/// about which bucket the page cache is keyed by. Every context loader here reaches it through this
/// rather than composing `routing_range()` and `block_routing_bucket` for itself.
pub(super) fn shard_block_routing_bucket(shard: &ShardState, object_key: &str) -> Option<u32> {
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    Some(crate::engine::hashing::block_routing_bucket(
        object_key,
        start_routing_bucket,
        end_routing_bucket,
    ))
}

pub(super) fn read_context_value<T: ContextWire>(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &ElementEntry,
    // Which page, stated by the caller: the timeline series this record sits in. The kind differs
    // per caller -- a child ref, a summary and a compression event are three kinds over three maps
    // -- and neither the address nor the timeline key names it.
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
) -> Option<T> {
    let point = read_feature_point(
        cache,
        block_store,
        shard_id,
        timeline_key,
        address,
        identity,
        routing_bucket,
    )?;
    context_from_bytes(&point.value)
}

pub(super) fn read_context_value_cold<T: ContextWire>(
    block_store: &BlockStore,
    timeline_key: u64,
    address: &ElementEntry,
) -> Option<T> {
    let point = read_feature_point_cold(block_store, timeline_key, address)?;
    context_from_bytes(&point.value)
}

pub(super) fn read_context_value_cached<T: ContextWire>(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &ElementEntry,
    // As `read_context_value` above.
    identity: BlockIdentity<'_>,
    packed_block_cache: &mut HashMap<ElementEntry, Option<Vec<FeaturePoint>>>,
    routing_bucket: Option<u32>,
) -> Option<T> {
    let point = read_feature_point_cached(
        cache,
        block_store,
        shard_id,
        timeline_key,
        address,
        identity,
        packed_block_cache,
        routing_bucket,
    )?;
    context_from_bytes(&point.value)
}

pub(super) fn context_event_matches_filter(
    event: &ContextEvent,
    current_valid_only: bool,
    as_of_ms: u64,
