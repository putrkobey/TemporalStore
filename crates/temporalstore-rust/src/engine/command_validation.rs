// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Command inspection, admission control, and precondition validation helpers, split from engine.rs.
use super::*;
// FOR THE SLOT'S SERIES SURFACE: `ObjectSlot` implements these, and the names it keeps
// (`len`, `get`, `is_empty`) already matched the call sites -- only the trait was out of
// scope here. Anonymous, because only the methods are wanted.
use crate::engine::timestamped_series::TimestampedSeries as _;

/// The key a command READS (does not write), for LRU recency tracking. Covers the
/// key-addressed read commands; the hash-addressed context queries are omitted (their
/// buckets refresh recency on the next write -- acceptable, eviction is memory-only).
///
/// ONE KEY, NOT A LIST OF THEM. Every arm of this match yielded exactly one key or none, so
/// the `Vec<String>` this used to return carried at most one element and cloned it to do so.
/// A read command's key is owned by the command for as long as the recency stamp needs it,
/// which is why the borrow is sound and the clone was not buying anything.
pub(super) fn command_read_key(command: &Command) -> Option<&str> {
    match command {
        Command::CommonTtl { key, .. }
        | Command::CommonExists { key, .. }
        | Command::StringGet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashGetAll { key, .. }
        | Command::HashLen { key, .. }
        | Command::SetMembers { key, .. }
        | Command::FeatureQuery { key, .. }
        | Command::SequenceQuery { key, .. } => Some(key.as_str()),
        _ => None,
    }
}

/// Hand each key a command touched, read or write, to `visit` -- for the per-bucket LRU
/// recency stamp, which is the only thing that wants them.
///
/// VISITED RATHER THAN COLLECTED. The two call sites are the same four-line loop: derive a
/// routing bucket from the key and insert a `u32 -> u64`. Neither keeps a key, so the
/// `Vec<String>` this replaces was allocated and cloned into purely to be walked once and
/// dropped -- TWO allocations per warm serving read, measured, of the ten #1952 found.
///
/// A WRITE command's keys are still built and still owned: `command_object_keys` synthesises
/// several of them (a control-state family key, a context node key) and there is nothing to
/// borrow from. Only the borrowing half of the fork got cheaper, which is the half a serving
/// read takes.
pub(super) fn for_each_touched_key(command: &Command, mut visit: impl FnMut(&str)) {
    let object_keys = command_object_keys(command);
    if object_keys.is_empty() {
        if let Some(key) = command_read_key(command) {
            visit(key);
        }
    } else {
        for key in object_keys {
            visit(&key);
        }
    }
}

pub(crate) fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        // Touches no object, so it holds no object keys.
        Command::LeaderEstablish => Vec::new(),
        Command::CommonDelete { key } => associated_record_keys(key),
        Command::CommonPersist { key } => associated_record_keys(key),
        Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::ListPush { key, .. }
        | Command::ListPop { key, .. }
        | Command::ListRange { key, .. }
        | Command::ListLen { key }
        | Command::ZSetAdd { key, .. }
        | Command::ZSetScore { key, .. }
        | Command::ZSetRemove { key, .. }
        | Command::ZSetCard { key }
        | Command::ZSetRange { key, .. }
        | Command::ZSetRangeByScore { key, .. }
        | Command::ZSetIncrBy { key, .. }
        | Command::ZSetPop { key, .. }
        | Command::ZSetRank { key, .. }
        | Command::BucketTake { key, .. }
        | Command::BucketPeek { key, .. }
        | Command::SeenCheck { key, .. }
        | Command::SeenCard { key }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateSelectionSet { key, .. } => vec![key.clone()],
        Command::ControlStateSet { family, key, .. }
        | Command::ControlStateSetAndGet { family, key, .. }
        | Command::ControlStateSetAndGetWithOptions { family, key, .. } => {
            vec![control_state_family_key(*family, key)]
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            vec![context_node_key(*tenant_hash, node.node_hash)]
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_event_key(*tenant_hash, *node_hash)],
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            let mut keys = vec![context_event_key(*tenant_hash, *node_hash)];
            if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_kind",
                    context_event_kind_hash(event),
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Status)
                && indexes.status_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "status",
                    indexes.status_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Source)
                && indexes.source_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "source",
                    indexes.source_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket)
                && indexes.event_time_bucket_ms != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Entity) {
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .map(|entity_hash| {
                            context_index_key(
                                *tenant_hash,
                                "entity",
                                entity_hash,
                                indexes.scope_hash,
                            )
                        }),
                );
            }
            keys
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => vec![context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )],
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            vec![context_audit_key(*tenant_hash, audit.session_hash)]
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_dirty_key(*tenant_hash, *node_hash)],
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_embedding_dirty_key(*tenant_hash, *node_hash)],
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => vec![context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )],
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => vec![context_child_key(*tenant_hash, child_ref.parent_hash)],
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_node_key(*tenant_hash, *node_hash)],
        Command::ContextQueryNodeEmbeddings {
            tenant_hash,
            node_hashes,
        } => node_hashes
            .iter()
            .map(|node_hash| context_node_key(*tenant_hash, *node_hash))
            .collect(),
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => vec![context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )],
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            vec![context_compression_key(*tenant_hash, event.node_hash)]
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_compression_key(*tenant_hash, *node_hash)],
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::ControlStateCount { .. }
        | Command::ControlStateQuery { .. }
        | Command::ControlStateDetail { .. }
        | Command::ControlStateFamilyQuery { .. }
        | Command::ControlStateSelectionQuery { .. }
        | Command::ControlStateManager { .. }
        | Command::ControlStateDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextQueryEmbeddingDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQuerySummaryVectors { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. }
        | Command::ContextQueryNodeContext { .. } => Vec::new(),
    }
}

pub(super) fn command_updates_bucket_index_directly(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::StringDelete { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            // These file their pages through `sync_bucket_index_object_blocks`, which maintains
            // exactly what a rebuild would recompute. Without them here the post-command path took
            // the rebuild branch on EVERY call: measured at twice the shard's page count per call,
            // so a one-point feature append into a 1,024-key store cost 12,717 allocations, none of
            // it about the point being appended.
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
    )
}

/// True when a command mutates only state that no page backs.
///
/// The seen sets, the control-state change and selection maps, and the token buckets live outside the
/// page-backed models, and their durability rides the index-log `key_states` blobs rather than a page
/// entry. So a rebuild of the page index after one of these can only recompute what it already held.
///
/// This is deliberately NOT folded into `command_updates_bucket_index_directly`: these commands do
/// not update that index, and saying they do to skip the rebuild would make the predicate assert
/// something false. They need their own reason.
pub(super) fn command_writes_no_block(command: &Command) -> bool {
    matches!(
        command,
        Command::SeenCheck { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::BucketTake { .. }
    )
}

pub(crate) fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::CommonPersist { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::ListPush { .. }
            | Command::ListPop { .. }
            | Command::ZSetAdd { .. }
            | Command::ZSetRemove { .. }
            | Command::ZSetIncrBy { .. }
            | Command::ZSetPop { .. }
            | Command::BucketTake { .. }
            | Command::SeenCheck { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateChangeAdd { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
            | Command::ControlStateSelectionSet { .. }
            | Command::ContextResourceBlobBegin { .. }
            | Command::ContextResourceBlobAppend { .. }
            | Command::ContextResourceBlobCommit { .. }
            | Command::ContextResourceBlobPut { .. }
            | Command::ContextResourceBlobSweep { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextMarkEmbeddingDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextSetNodeEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}

pub(super) fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    // A configured qps of 0 means UNLIMITED on EVERY scope below, not deny-all. Zero is what
    // an unset knob reads as, so treating it as "refuse everything" turns an intended "no limit
    // here" into a total outage; denying traffic outright has its own controls.
    //
    // The filter has to stay uniform across the three scopes. It once guarded the shard knob
    // alone, so `write_qps: 0` meant no limit while `table_write_qps: 0` a few lines below denied
    // every write to that table -- two knobs of identical shape with opposite meanings.
    if let Some(limit) = (if write_command {
        config.write_qps
    } else {
        config.read_qps
    })
    .filter(|&limit| limit > 0)
    {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
