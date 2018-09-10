// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::Command;

use super::routing::key_is_dropped_by_percent;

pub(super) fn is_write(command: &Command) -> bool {
    // Delegate to the engine's canonical write-command classifier (the same one that gates WAL
    // persistence + dirty marking). The client uses this to refresh table topology before a write
    // (route to the current leader) and to pick the write retry budget, so it MUST agree with the
    // server on what mutates. A hand-maintained subset here had already drifted -- it omitted
    // seven context write commands the engine treats as writes (ContextWriteExtractedEvent,
    // ContextUpsertEntity/ChildRef/Embedding/Summary, ContextWriteCompressionEvent,
    // ContextCompressEvents) -- so issuing one of those through the public execute() API skipped
    // the pre-write topology refresh and got the read retry budget. Delegating keeps them in lockstep.
    crate::engine::is_write_command(command)
}

pub(super) fn command_is_dropped(command: &Command, drop_percent: u8) -> bool {
    // A full drain has to mean every command. The hash below can only judge commands that
    // HAVE a routing key, and the `unwrap_or(false)` on the end answers "keep sending it"
    // for the ones that do not -- the resource-blob upload path, sequence batch queries,
    // node-embedding reads. A table drained to 100 went on sending those.
    //
    // Below 100 that stands on purpose: shedding is per key by construction, so a command
    // with nothing to hash has no share to shed.
    if drop_percent >= 100 {
        return true;
    }
    command_routing_key(command)
        .as_deref()
        .map(|key| key_is_dropped_by_percent(key, drop_percent))
        .unwrap_or(false)
}

pub(super) fn command_key(command: &Command) -> Option<&str> {
    match command {
        // Names no key: it exists to be committed, not to touch a record.
        Command::LeaderEstablish => None,
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::CommonTtl { key }
        | Command::CommonPersist { key }
        | Command::CommonExists { key }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringGet { key }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashGet { key, .. }
        | Command::HashMultiGet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashGetAll { key }
        | Command::HashLen { key }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetMembers { key }
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
        | Command::FeatureQuery { key, .. }
        | Command::FeatureQueryFiltered { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::FeatureAggQuery { key, .. }
        | Command::SequenceAdd { key, .. }
        | Command::SequenceQuery { key, .. }
        | Command::ControlStateIncrement { key, .. }
        | Command::ControlStateIncrementWithOptions { key, .. }
        | Command::ControlStateChangeAdd { key, .. }
        | Command::ControlStateCount { key, .. }
        | Command::ControlStateQuery { key, .. }
        | Command::ControlStateDetail { key, .. }
        | Command::ControlStateSet { key, .. }
        | Command::ControlStateSetAndGet { key, .. }
        | Command::ControlStateSetAndGetWithOptions { key, .. }
        | Command::ControlStateFamilyQuery { key, .. }
        | Command::ControlStateSelectionSet { key, .. }
        | Command::ControlStateSelectionQuery { key }
        | Command::ControlStateManager { key, .. }
        | Command::ControlStateDebug { key, .. } => Some(key),
        Command::SequenceBatchQuery { .. }
        | Command::ContextUpsertNode { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextGetNodes { .. }
        | Command::ContextWriteEvent { .. }
        | Command::ContextWriteExtractedEvent { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextWriteIndexRef { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryIndexIntersection { .. }
        | Command::ContextWritePackAudit { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextMarkSummaryDirty { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextMarkEmbeddingDirty { .. }
        | Command::ContextQueryEmbeddingDirty { .. }
        | Command::ContextUpsertEntity { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextUpsertChildRef { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextSetNodeEmbedding { .. }
        | Command::ContextQueryNodeEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextUpsertSummary { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQuerySummaryVectors { .. }
        | Command::ContextWriteCompressionEvent { .. }
