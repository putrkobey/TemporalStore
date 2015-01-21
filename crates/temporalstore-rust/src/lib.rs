// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![doc = include_str!("../README.md")]

// COMPILED INTO EVERY BUILD, not only test builds.
//
// It used to be `#[cfg(test)]`, which was right while everything that read the counters was a
// test. The allocation-class scopes that say what a write's memory is spent on are not: they sit
// inside the primitives that own each sink, because that is the only placement a new call site
// cannot go around -- the lesson of #1882 and #1899, where counters at one call site of nine saw
// one write in twelve.
//
// The cost of that in a build without the `alloc-probe` feature below is nothing at all:
// `in_class` is its closure, with no atomic, no thread-local and no branch, and what remains is a
// few hundred bytes of never-incremented counters. What the feature changes is not whether the
// module exists but whether anything counts -- and `classified_now` answers `None` rather than a
// table of zeros when nothing does, so an uninstrumented build cannot be read as a clean one.
pub mod alloc_probe;

// Tests only. Counts what a raft snapshot walks, reads, copies and rebuilds, bumped inside the
// primitives that do the work rather than at the call sites that ask for it.
#[cfg(test)]
pub mod snapshot_probe;

// Tests only, and only under the `alloc-probe` feature. RSS cannot resolve a per-request
// allocation change -- 71% of the proxy's resident memory was measured as allocator retention
// rather than live data -- so memory claims about a request path need the allocations counted
// directly.
//
// Feature-gated rather than always-on in tests: with this installed unconditionally, a full
// single-threaded run turned 17 socket-bound proxy and raft tests red that pass individually, as a
// group, and in a full run without it. Two atomics on every allocation in the process is enough to
// disturb tests that wait on sockets, and an instrument that changes the result of the suite
// checking the change is worse than no instrument.
#[cfg(all(test, feature = "alloc-probe"))]
#[global_allocator]
static COUNTING_ALLOCATOR: alloc_probe::CountingAllocator = alloc_probe::CountingAllocator;

pub mod bytes_serde;
pub mod block_store;
pub mod checksum;
#[path = "temporalstore_client.rs"]
pub mod client;
pub mod context_workflow;
pub mod control;
pub mod data_node;
pub mod e2e;
pub mod durability_metrics;
pub mod flush_gate;
pub mod memory_trim;
pub mod engine;
pub mod env_flag;
pub mod http;
pub mod index_log;
pub mod ingestion;
pub(crate) mod log_framing;
#[cfg(feature = "matrixobject")]
pub mod matrixobject_store;
pub mod meta;
pub mod partition_id;
pub mod proxy;
pub mod raft;
pub mod readiness;
pub mod rebalance;
pub mod redis;
mod scratch;
pub mod sdk;
pub mod shared_store;
pub mod storage_backend;
pub mod storage_config;
pub mod storage_descriptor;
pub mod telemetry;
#[path = "command_protocol_types.rs"]
pub mod types;
pub mod fault;
pub mod wal;
/// What an append costs per record and what a replay costs per log -- tests only.
#[cfg(test)]
mod wal_batch_roll;
mod wal_replay_scale;
/// What a WINDOWED replay reads against the log it replays -- tests only.
mod wal_replay_windows;
/// What reading a log piece's base header COSTS -- tests only.
mod wal_base_header_read;
/// What a RESTORE reads, and what no reader claims -- tests only.
mod restore_read_residual;
mod wal_scale;
/// What one log RECORD costs to build and to decode -- tests only.
mod record_cost;
mod wal_proto;
pub mod record_framing;
pub mod index_log_record;
pub mod wal_record;
/// NO FOREIGN TYPE NAMES IN RUST SOURCE -- a repo-wide scan, tests only. The vendor-vocabulary
/// scrub covers product and company names; this covers another implementation's IDENTIFIERS,
/// which is a different failure and had no gate at all until this module.
#[cfg(test)]
mod foreign_type_name_guard;

pub use block_store::{
    ElementEntry, BlockStoreSlabDescriptor, BlockStoreSlabState, BlockStoreSlabSummary,
    BlockStoreOptions, BlockStoreSlabReport, BlockStoreStats, BlockStore, SharedSlabSource,
};
pub use client::{
    crc64_jones, key_is_dropped_by_percent, shard_id_for_key, bucket_id_for_key, stable_key_hash,
    ClientError, ClientOptions, ClientPreflightReport, ClientStats, RequestOptions, TableOptions,
    TemporalStoreClient, TemporalStorePipeline, TemporalStoreTable,
};
pub use context_workflow::{
    context_backfill_embeddings, context_embedding_ref_hash, context_pipeline_manage_report,
    context_provider_from_env,
    context_pipeline_parity_evidence,
    context_skill_registry_from_parsed,
    context_workflow_state_report, default_context_model_providers, extract_context,
    ingest_extract_context, ingest_extract_context_with_l1_gate, ingest_resource_skill_context,
    inject_context,
    reference_open_source_model_profiles, parse_context_resource, parse_context_skill_markdown,
    retrieve_context, run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep,
    select_context_skills_for_retrieval, update_context_resource_lifecycle,
    update_context_skill_registry, validate_resource_skill_secondary_indexes, ContextBlock,
    ContextExtractReport, ContextExtractRequest, ContextIngestExtractReport,
    ContextIngestExtractRequest, ContextIngestExtractSummary, ContextIngestSourceFailure,
    ContextInjectReport, ContextInjectRequest, ContextModelProviderConfig,
    ContextReferenceModelProfile, ContextParsedResourceChunk, ContextPipelineBenchmarkQueryReport,
    ContextPipelineBenchmarkReport, ContextPipelineBenchmarkRequest,
    ContextPipelineBenchmarkSweepProfile, ContextPipelineBenchmarkSweepReport,
    ContextPipelineBenchmarkSweepRequest, ContextPipelineBenchmarkThresholds,
    ContextPipelineManageReport, ContextPipelineParityEvidence, ContextPipelineStageReport,
    ContextPrefilterCandidateDebug, ContextProviderKind, ContextQueryUnderstandingDebug,
    ContextResourceImportKind, ContextResourceLifecycleAction, ContextResourceLifecycleRecord,
    ContextResourceLifecycleReport, ContextResourceLifecycleUpdate, ContextResourceParseReport,
    ContextResourceParseRequest, ContextResourceSkillEmbeddingEvidenceReport,
    ContextResourceSkillIngestReport, ContextResourceSkillIngestRequest,
    ContextResourceSkillModelFanoutReport, ContextResourceSkillSecondaryIndexReport,
    ContextResourceSkillSecondaryIndexValidationReport,
    ContextResourceSkillSecondaryIndexValidationRequest, ContextRetrieveReport,
    ContextRetrieveRequest, ContextSecondaryIndexFamilyValidationReport, ContextSkillIngestInput,
    ContextSkillParseReport, ContextSkillPrecedence, ContextSkillRegistryEntry,
    ContextSkillRegistryReport, ContextSkillRegistryUpdate, ContextSkillSelectionCandidate,
    ContextSkillSelectionReport, ContextSkillSelectionRequest, ContextSourceKind, ContextTier,
    ContextTreeTraversalDebug, ContextWorkflowStateReport,
};
pub use control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ScanStreamRequest,
    ScanStreamResponse, SetConfigRequest, ShardCanonicalStorageStats, StreamKind,
    StreamReadRequest, StreamReadResponse, UnloadShardRequest, UnloadShardResponse,
};
pub use data_node::{
    CompactionRequest, CompactionResponse, DataNodeLifecycleReport, DataNodePreflightReport,
    DataNodeRuntime, DataNodeRuntimeOptions, DataNodeRuntimeStats, DataNodeShardLifecycleState,
    DataNodeTaskKind, DataNodeTaskOutput, DataNodeTaskStatus, DirtyObjectInfo, DumpShardRequest,
    DumpShardResponse, GcRequest, GcResponse, RequestController, SharedWalSink, ShardWorkerInfo,
    StorageLifecycleResponse, StorageLifecycleScheduler,
};
pub use e2e::{
    AsyncStorageJournal, EndToEndWorkflow, EndToEndWorkflowOptions, KillSwitches, RaftWriteMode,
    ReplicaReadPolicy, ReplicationMode, RoutingClient, TemporalStoreClientOptions, WorkflowError,
    WorkflowProxy,
};
pub use engine::golden::{native_api_golden_corpus_report, native_feature_sequence_golden_corpus_report};
pub use engine::reports::{
    GoldenCaseReport, GoldenCorpusReport, RustStorageObservation, ShardCompactionReport,
    ShardCompactionUtilityReport, ShardExpirySweepReport, BucketDumpFaultMatrixReport,
    BucketDumpFaultScenarioReport, BucketDumpFollowerReplayCursor, BucketDumpFollowerRetentionBlock,
    BucketDumpInstallMarker, BucketDumpInstallPreflightReport, BucketDumpInstallRollForwardReport,
    BucketDumpManifest, BucketDumpManifestChainIssue, BucketDumpManifestPrunePlan,
    BucketDumpManifestPruneReport, BucketDumpRaftSnapshotRef, BucketDumpRaftSnapshotRetentionBlock,
    BucketStorageSummary, StorageCacheInspectionReport, StorageCacheInvalidateBucketRequest,
    StorageCacheBucketSummary, StorageCacheWarmupReport, StorageDataStructureApiParityReport,
    StorageFeatureBlockError, StorageFeatureBlockLayoutReport, StorageFeatureBlockTimestampMismatch,
    StorageLifecyclePlan, StorageLifecycleReport, StorageLifecycleRequest,
    StorageLogCompatibilityReport, StorageObjectLifecycleReport,
    StorageBlockFormatCompatibilityReport, StorageProductionReadinessPolicy,
    StorageProductionReadinessReport, StorageProductionReadinessRequest, StorageReclaimCandidate,
    StorageRecoveryBoundaryReport, StorageRecoveryBlockError, StorageRecoveryBlockOwnerMismatch,
    StorageRecoveryReport, StorageRecoverySlabLiveReport, StorageSlabIntegrityReport,
    StorageTimestampedBlockFamilyReport,
};
pub use engine::TemporalEngine;

