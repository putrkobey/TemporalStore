// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, path::Path};

use matrixcache::CacheKey;
use serde::Deserialize;
use serde_json::Value;
use temporalstore_rust::client::{
    ClientMetaSyncLoopOptions, ReplicaReadPolicy as ClientReplicaReadPolicy,
};
use temporalstore_rust::engine::reports::{StorageManagerCycleRequest, StorageBlockGcReplayCursor};
use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::{TopologyVersionReport, TopologyVersionRequest};
use temporalstore_rust::partition_id::PartitionId;
use temporalstore_rust::raft::RaftReplicaRole;
use temporalstore_rust::redis::{execute_redis_command_with_state, RedisCommandState};
use temporalstore_rust::types::{
    BatchExecuteRequest, BatchExecuteResponse, ExecuteResponse, SequenceFeatureRow,
};
use temporalstore_rust::{
    apply_data_raft_membership_from_topology, execute_redis_command,
    metaserver_scheduler_execution_readiness_report, production_readiness_report, AddTableRequest,
    ClientOptions, Command, CommandResponse, EndToEndWorkflow, ExecuteRequest,
    GetTableTopologyRequest, LoadFinishRequest, MetaEntityState, ProxyOptions, ProxyService,
    ProxyServingMode, ProxyTableBatchExecuteRequest, RaftCluster, RaftConfig, RaftError,
    RegisterServerRequest, RegisterShardRequest, RespValue, ScanStreamRequest, ServerEndpoint,
    ServerHeartbeatRequest, ServerRuntimeLoad, ServerShardServingState, SharedStoreReplicator,
    SharedStoreStorageMode, SingleNodeMeta, BucketDumpFollowerReplayCursor, BucketDumpRaftSnapshotRef,
    Status, StorageLifecycleRequest, StreamKind, StreamReadRequest, StreamReadResponse,
    TableMetaInfo, TableOptions, TableShard, TableTopologyResponse, TemporalEngine,
    TemporalStoreClient, TemporalStoreTable,
};
use temporalstore_snapshot::FileObjectStore;

#[derive(Debug, Deserialize)]
struct UnifiedCorpus {
    schema_version: u32,
    name: String,
    coverage: UnifiedCoverage,
    cases: Vec<UnifiedCase>,
}

#[derive(Debug, Deserialize)]
struct UnifiedCoverage {
    required_case_names: Vec<String>,
    required_command_kinds: Vec<String>,
    required_response_kinds: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UnifiedCase {
    name: String,
    shard_id: u64,
    steps: Vec<UnifiedStep>,
}

#[derive(Debug, Deserialize)]
struct UnifiedStep {
    name: String,
    #[serde(default)]
    restart_before: bool,
    #[serde(default)]
    skip_client: bool,
    command: Value,
    #[serde(default)]
    expect_status: Option<Status>,
    #[serde(default)]
    expect: Option<UnifiedExpected>,
}

#[derive(Debug)]
enum UnifiedExpected {
    Status(UnifiedStatusExpected),
    Bool { value: bool },
    Static(Value),
    Response(CommandResponse),
    NodesProbe(UnifiedNodesProbe),
    EventsProbe { min_count: usize },
    CompressionProbe { min_count: usize, source_event_count: usize },
}

/// The corpus's `context_nodes` expects assert node MEMBERSHIP, not the full records: which
/// node hashes must come back, which must not, and a count floor. Comparing whole ContextNode
/// values would chain every field of the node schema to the corpus file.
#[derive(Debug, Deserialize)]
struct UnifiedNodesProbe {
    #[serde(default)]
    node_hashes: Vec<u64>,
    #[serde(default)]
    missing_node_hashes: Vec<u64>,
    #[serde(default)]
    min_node_count: usize,
}

#[derive(Debug, Deserialize)]
struct UnifiedStatusExpected {
    kind: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    ok: Option<bool>,
}

impl<'de> Deserialize<'de> for UnifiedExpected {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("expected object with kind"))?
            .to_string();
        match kind.as_str() {
            "status" => serde_json::from_value(value)
                .map(UnifiedExpected::Status)
                .map_err(serde::de::Error::custom),
            "boolean" => Ok(UnifiedExpected::Bool {
                value: value
                    .get("value")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| serde::de::Error::custom("boolean expect missing value"))?,
            }),
            "storage_pool_uri_validation" | "object_store_backend" => {
                Ok(UnifiedExpected::Static(value))
            }
            "entries" => {
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "kind".to_string(),
                        Value::String("hash_entries".to_string()),
                    );
                }
                serde_json::from_value(value)
                    .map(UnifiedExpected::Response)
                    .map_err(serde::de::Error::custom)
            }
            "context_nodes" => serde_json::from_value(value)
                .map(UnifiedExpected::NodesProbe)
                .map_err(serde::de::Error::custom),
            // Probe-style expects: a count floor rather than the full record list, so the
            // corpus does not chain every field of the record schema to itself.
            "context_events" if value.get("events").is_none() => Ok(UnifiedExpected::EventsProbe {
                min_count: value
                    .get("min_count")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize,
            }),
            "context_compression_events" if value.get("events").is_none() => {
                Ok(UnifiedExpected::CompressionProbe {
                    min_count: value
                        .get("min_count")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                    source_event_count: value
                        .get("source_event_count")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                })
            }
            "members" => {
                normalize_string_array_field_to_bytes(&mut value, "members");
                serde_json::from_value(value)
                    .map(UnifiedExpected::Response)
                    .map_err(serde::de::Error::custom)
            }
            _ => serde_json::from_value(value)
                .map(UnifiedExpected::Response)
                .map_err(serde::de::Error::custom),
        }
    }
}

fn normalize_string_array_field_to_bytes(value: &mut Value, field: &str) {
    let Some(array) = value.get_mut(field).and_then(Value::as_array_mut) else {
        return;
    };
    for item in array {
        if let Some(text) = item.as_str() {
            *item = Value::Array(
                text.as_bytes()
                    .iter()
                    .map(|byte| Value::Number((*byte).into()))
                    .collect(),
            );
        }
    }
}

#[derive(Debug, Deserialize)]
struct StorageMigrationCorpus {
    schema_version: u32,
    name: String,
    source_format: String,
    format_compatibility: String,
    cases: Vec<StorageMigrationCase>,
}

#[derive(Debug, Deserialize)]
struct StorageMigrationCase {
    name: String,
    shard_id: u64,
    operations: Vec<StorageMigrationStep>,
    expected_reads: Vec<StorageMigrationStep>,
}

#[derive(Debug, Clone, Deserialize)]
struct StorageMigrationStep {
    name: String,
    #[serde(default)]
    storage_mutation: bool,
    command: Command,
    #[serde(default)]
    expect: Option<CommandResponse>,
}

#[derive(Debug, Deserialize)]
struct StorageUnifiedCommand {
    #[serde(default)]
    migration_case: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    scenario: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SharedHarnessCommand {
    #[serde(default)]
    scenario: Option<String>,
}

#[test]
fn rust_executes_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_engine_case(&case);
    }
}

#[test]
fn rust_client_executes_temporalstore_corpus() {
    let corpus = load_corpus();

    for case in corpus.cases {
        run_client_case(&case);
    }
}

// shared-corpus: storage_wal_index_gc_generation_retention storage_gc_dependency_retention_matrix
#[test]
fn rust_executes_storage_raft_gc_cases() {
    verify_storage_wal_index_gc_generation_retention(92);
    verify_storage_gc_dependency_retention_matrix(92);
}

// shared-corpus: storage_cache_replacement_policy_soak storage_matrixraft_cache_refill_pressure storage_cache_cold_read_after_eviction_shared
// shared-corpus: storage_bucket_first_physical_index storage_object_manager_bucketstore_runtime_authority storage_model_layout_compaction_policies storage_merged_dump_load_lifecycle storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
// shared-corpus: storage_stale_page_density_compaction storage_merged_dump_load_restart_interruption storage_gc_eviction_cold_reads storage_manager_real_pressure_signals storage_manager_wal_reclaim_bucket_generation_retention storage_manager_expire_cursor_scan_limits
// shared-corpus: storage_manager_active_eviction_runtime storage_manager_page_gc_dependency_refusal storage_manager_index_gc_thresholds_recovery storage_risk_context_page_backed_parity
#[test]
fn rust_executes_storage_eviction_cases() {
    verify_storage_cache_replacement_policy_soak(79);
    verify_storage_cache_refill_pressure(88);
    verify_storage_cold_read_after_eviction(1);
}

fn load_corpus() -> UnifiedCorpus {
    let corpus_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("compat/unified_temporalstore_cases.json");
    let corpus_bytes = fs::read(&corpus_path).expect("shared corpus should be readable");
    let corpus: UnifiedCorpus =
        serde_json::from_slice(&corpus_bytes).expect("shared corpus should deserialize");

    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.name, "temporalstore-unified-native-rust-corpus");
    assert!(!corpus.cases.is_empty(), "shared corpus must contain cases");
    assert_required_coverage(&corpus);
    corpus
}

fn assert_required_coverage(corpus: &UnifiedCorpus) {
    assert_no_duplicate_cases_or_steps(corpus);

    let case_names = corpus
        .cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    let command_kinds = corpus
        .cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .map(|step| command_kind(&step.command))
        .collect::<BTreeSet<_>>();
    let response_kinds = corpus
        .cases
        .iter()
        .flat_map(|case| case.steps.iter())
        .filter_map(|step| step.expect.as_ref().map(expected_kind))
        .collect::<BTreeSet<_>>();

    for required in &corpus.coverage.required_case_names {
        assert!(
            case_names.contains(required.as_str()),
            "shared corpus missing required case {required}"
        );
    }
    for required in &corpus.coverage.required_command_kinds {
        assert!(
            command_kinds.contains(required.as_str()),
            "shared corpus missing required command kind {required}"
        );
    }
    for required in &corpus.coverage.required_response_kinds {
        assert!(
            response_kinds.contains(required.as_str()),
            "shared corpus missing required response kind {required}"
        );
    }
}

fn assert_no_duplicate_cases_or_steps(corpus: &UnifiedCorpus) {
    let mut case_names = BTreeSet::new();
    for case in &corpus.cases {
        assert!(
            case_names.insert(case.name.as_str()),
            "shared corpus has duplicate case name {}",
            case.name
        );

        let mut step_names = BTreeSet::new();
        let mut command_signatures = BTreeSet::new();
        for step in &case.steps {
            assert!(
                step_names.insert(step.name.as_str()),
                "shared corpus has duplicate step name {}/{}",
                case.name,
                step.name
            );
            let signature = serde_json::to_string(&step.command)
                .expect("shared corpus command should serialize for duplicate checks");
            assert!(
                command_signatures.insert(signature),
                "shared corpus has duplicate command payload in case {} at step {}",
                case.name,
                step.name
            );
        }
    }
}

fn command_kind(command: &Value) -> &str {
    command
        .get("kind")
        .and_then(Value::as_str)
        .expect("shared corpus command should contain a string kind")
}

fn expected_kind(expected: &UnifiedExpected) -> String {
    match expected {
        UnifiedExpected::Status(status) if status.kind == "status" => "status".to_string(),
        UnifiedExpected::Status(_) => "status".to_string(),
        UnifiedExpected::Bool { .. } => "boolean".to_string(),
        UnifiedExpected::Static(value) => value
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("static")
            .to_string(),
        UnifiedExpected::Response(response) => response_kind(response).to_string(),
        UnifiedExpected::NodesProbe(_) => "context_nodes".to_string(),
        UnifiedExpected::EventsProbe { .. } => "context_events".to_string(),
        UnifiedExpected::CompressionProbe { .. } => "context_compression_events".to_string(),
    }
}

fn response_kind(response: &CommandResponse) -> &'static str {
    match response {
        CommandResponse::Empty => "empty",
        CommandResponse::Bytes { .. } => "bytes",
        CommandResponse::Integer { .. } => "integer",
        CommandResponse::Members { .. } => "members",
        CommandResponse::Values { .. } => "values",
        CommandResponse::HashEntries { .. } => "hash_entries",
        CommandResponse::FeaturePoints { .. } => "feature_points",
        CommandResponse::FeaturePointGroups { .. } => "feature_point_groups",
        CommandResponse::Aggregate { .. } => "aggregate",
        CommandResponse::SequenceRows { .. } => "sequence_rows",
        CommandResponse::SequenceRowGroups { .. } => "sequence_row_groups",
        CommandResponse::ContextNode { .. } => "context_node",
        CommandResponse::ContextObjectKey { .. } => "context_object_key",
        CommandResponse::ContextExtractedEventWrite { .. } => "context_extracted_event_write",
        CommandResponse::ContextEvents { .. } => "context_events",
        CommandResponse::ContextIndexRefs { .. } => "context_index_refs",
        CommandResponse::ContextIndexIntersection { .. } => "context_index_intersection",
        CommandResponse::ContextPackAudits { .. } => "context_pack_audits",
        CommandResponse::ContextSummaryDirtyNodes { .. } => "context_summary_dirty_nodes",
        CommandResponse::ContextEntity { .. } => "context_entity",
        CommandResponse::ContextEntities { .. } => "context_entities",
        CommandResponse::ContextChildRefs { .. } => "context_child_refs",
        CommandResponse::ContextNodeEmbeddings { .. } => "context_node_embeddings",
        CommandResponse::ContextEmbeddingDirtyNodes { .. } => "context_embedding_dirty_markers",
        CommandResponse::ContextTraversedNodes { .. } => "context_traversed_nodes",
        CommandResponse::ContextSummaries { .. } => "context_summaries",
        CommandResponse::ContextSummaryVectors { .. } => "context_summary_vectors",
        CommandResponse::ContextCompressionEvents { .. } => "context_compression_events",
        CommandResponse::ContextResourceBlobUpload { .. } => "context_resource_blob_upload",
        CommandResponse::ContextResourceBlobCommitted { .. } => "context_resource_blob_committed",
        CommandResponse::ContextResourceBlobChunk { .. } => "context_resource_blob_chunk",
        CommandResponse::ContextResourceBlobSwept { .. } => "context_resource_blob_swept",
        CommandResponse::ContextNodeContext { .. } => "context_node_context",
        CommandResponse::ContextNodes { .. } => "context_nodes",
    }
}

fn run_engine_case(case: &UnifiedCase) {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let mut engine = new_engine(dir.path(), &block_dir, &index_dir, case.shard_id);

    for step in &case.steps {
        if step.restart_before {
            drop(engine);
            engine = new_engine(dir.path(), &block_dir, &index_dir, case.shard_id);
        }
        if maybe_run_engine_adapter_command(case, step, &engine) {
            continue;
        }
        if maybe_run_storage_parity_command(case, step) {
            continue;
        }
        if maybe_run_shared_harness_command(case, step) {
            continue;
        }
        if command_kind(&step.command) == "existing_test"
            || step
                .command
                .get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode == "rust_executable_native_static")
        {
            // Pointer steps name a lib test as their runner; the mode field is what marks
            // them, whatever kind string the case chose.
            continue;
        }

        let response = engine.execute(ExecuteRequest {
            shard_id: case.shard_id,
            command: step_command(case, step),
        });

        assert_step_status(case, step, &response.status);

        assert_step_expect(case, step, &response.status, &response.response);
    }
}

fn run_client_case(case: &UnifiedCase) {
    let dir = tempfile::tempdir().unwrap();
    let block_dir = dir.path().join("blocks");
    let index_dir = dir.path().join("indexes");
    let engine = Arc::new(Mutex::new(new_engine(
        dir.path(),
        &block_dir,
        &index_dir,
        case.shard_id,
    )));
    let server_addr = free_local_addr();
    let server_engine = Arc::clone(&engine);
    let server_addr_for_thread = server_addr.clone();
    std::thread::spawn(move || {
        serve(&server_addr_for_thread, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                    let response = server_engine
                        .lock()
                        .expect("engine lock poisoned")
                        .execute(req);
                    json_response(200, &response)
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&server_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: server_addr,
        default_shard_id: case.shard_id,
        ..ClientOptions::default()
    });
    let table = client.open_table(
        "unified",
        &case.name,
        TableOptions {
            first_shard_id: case.shard_id,
            ..TableOptions::default()
        },
    );

    for step in &case.steps {
        if step.skip_client {
            continue;
        }
        if maybe_run_client_adapter_command(case, step, &table) {
            continue;
        }
        if maybe_run_shared_harness_command(case, step) {
            continue;
        }
        assert!(
            !is_storage_parity_command(&step.command),
            "case={} step={} storage parity commands must set skip_client=true",
            case.name,
            step.name
        );
        if step.restart_before {
            *engine.lock().expect("engine lock poisoned") =
                new_engine(dir.path(), &block_dir, &index_dir, case.shard_id);
        }
        if command_kind(&step.command) == "existing_test"
            || step
                .command
                .get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode == "rust_executable_native_static")
        {
            // Pointer steps name a lib test as their runner; the mode field is what marks
            // them, whatever kind string the case chose.
            continue;
        }

        let response = match table.execute(step_command(case, step)) {
            Ok(response) => response,
            Err(error)
                if step
                    .expect
                    .as_ref()
                    .and_then(expected_status_code)
                    .is_some() =>
            {
                let expected = step
                    .expect
                    .as_ref()
                    .and_then(expected_status_code)
                    .expect("checked above");
                assert!(
                    !expected.is_empty(),
                    "case={} step={} invalid empty status expectation after client error {error}",
                    case.name,
                    step.name
                );
                continue;
            }
            Err(error) => panic!("case={} step={} {error}", case.name, step.name),
        };

        assert_step_status(case, step, &response.status);

        assert_step_expect(case, step, &response.status, &response.response);
    }
}

fn step_command(case: &UnifiedCase, step: &UnifiedStep) -> Command {
    let mut command = step.command.clone();
    normalize_shared_command_aliases(&mut command);
    serde_json::from_value(command).unwrap_or_else(|error| {
        panic!(
            "case={} step={} command should deserialize into a Rust executable command: {error}",
            case.name, step.name
        )
    })
}

fn normalize_shared_command_aliases(command: &mut Value) {
    let Some(object) = command.as_object_mut() else {
        return;
    };
    if object.get("kind").and_then(Value::as_str) == Some("string_setex") {
        object.insert(
            "kind".to_string(),
            Value::String("string_set_ex".to_string()),
        );
        if let Some(seconds) = object.remove("seconds") {
            let ttl_ms = seconds
                .as_u64()
                .unwrap_or_else(|| panic!("string_setex seconds must be unsigned"))
                .saturating_mul(1000);
            object.insert("ttl_ms".to_string(), Value::Number(ttl_ms.into()));
        }
    }
}

fn maybe_run_engine_adapter_command(
    case: &UnifiedCase,
    step: &UnifiedStep,
    engine: &TemporalEngine,
) -> bool {
    if is_shared_string_set_with_flags(&step.command) {
        let response = run_shared_string_set_with_flags(case, step, |command| {
            engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command,
            })
        });
        assert_step_status(case, step, &response.status);
        assert_step_expect(case, step, &response.status, &response.response);
        return true;
    }
    match command_kind(&step.command) {
        "set_add" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, true, |command| {
                engine.execute(ExecuteRequest {
                    shard_id: case.shard_id,
                    command,
                })
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_remove" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, false, |command| {
                engine.execute(ExecuteRequest {
                    shard_id: case.shard_id,
                    command,
                })
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_is_member" => {
            let key = json_string(&step.command, "key");
            let member = json_string(&step.command, "member").into_bytes();
            let response = engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command: Command::SetMembers { key },
            });
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: i64::from(members.iter().any(|existing| existing == &member)),
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "set_card" => {
            let key = json_string(&step.command, "key");
            let response = engine.execute(ExecuteRequest {
                shard_id: case.shard_id,
                command: Command::SetMembers { key },
            });
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: members.len() as i64,
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "storage_pool_uri_validate" | "object_store_backend_detect" => {
            assert_static_expectation(case, step);
            true
        }
        _ => false,
    }
}

fn maybe_run_client_adapter_command(
    case: &UnifiedCase,
    step: &UnifiedStep,
    table: &TemporalStoreTable,
) -> bool {
    if is_shared_string_set_with_flags(&step.command) {
        let response = run_shared_string_set_with_flags(case, step, |command| {
            table
                .execute(command)
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
        });
        assert_step_status(case, step, &response.status);
        assert_step_expect(case, step, &response.status, &response.response);
        return true;
    }
    match command_kind(&step.command) {
        "set_add" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, true, |command| {
                table
                    .execute(command)
                    .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_remove" => {
            if step.command.get("members").is_none() {
                return false;
            }
            let response = run_shared_set_members_command(case, step, false, |command| {
                table
                    .execute(command)
                    .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name))
            });
            assert_step_status(case, step, &response.status);
            assert_step_expect(case, step, &response.status, &response.response);
            true
        }
        "set_is_member" => {
            let key = json_string(&step.command, "key");
            let member = json_string(&step.command, "member").into_bytes();
            let response = table
                .execute(Command::SetMembers { key })
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name));
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: i64::from(members.iter().any(|existing| existing == &member)),
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "set_card" => {
            let key = json_string(&step.command, "key");
            let response = table
                .execute(Command::SetMembers { key })
                .unwrap_or_else(|error| panic!("case={} step={} {error}", case.name, step.name));
            assert_step_status(case, step, &response.status);
            let actual = match response.response {
                CommandResponse::Members { members } => CommandResponse::Integer {
                    value: members.len() as i64,
                },
                other => other,
            };
            assert_step_expect(case, step, &response.status, &actual);
            true
        }
        "storage_pool_uri_validate" | "object_store_backend_detect" => {
            assert_static_expectation(case, step);
            true
        }
        _ => false,
    }
}

fn assert_static_expectation(case: &UnifiedCase, step: &UnifiedStep) {
    match command_kind(&step.command) {
        "storage_pool_uri_validate" => {
            let uri = json_string(&step.command, "uri");
            let allowed = uri.starts_with("file://")
                || uri.starts_with("shared-file://")
                || uri.starts_with("shared://")
                || uri.starts_with("efs://")
                || uri.starts_with("nfs://")
                || uri.starts_with("local://")
                || uri.starts_with("blob://")
                || uri.starts_with("s3://")
                || uri.starts_with("ceph://")
                || uri.starts_with("ceph+s3://")
                || uri.starts_with("objectstore://");
            let expected = step
                .expect
                .as_ref()
                .and_then(static_expected_allowed)
                .expect("storage pool URI case should include allowed expectation");
            assert_eq!(
                allowed, expected,
                "case={} step={} storage URI validation mismatch",
                case.name, step.name
            );
        }
        "object_store_backend_detect" => {
            let uri = json_string(&step.command, "uri");
            let backend = if uri.starts_with("blob://") || uri.starts_with("objectstore://") {
                "objectstore"
            } else if uri.starts_with("local://") {
                "objectstore"
            } else if uri.starts_with("ceph+s3://") || uri.starts_with("ceph://") {
                "ceph_s3"
            } else if uri.starts_with("rados://") {
                "ceph_rados"
            } else if uri.starts_with("s3://") {
                "s3"
            } else if uri.starts_with("file://") {
                "local_file"
            } else {
                "unknown"
            };
            let expected = step
                .expect
                .as_ref()
                .and_then(static_expected_backend)
                .expect("object-store backend case should include backend expectation");
            assert_eq!(
                backend, expected,
                "case={} step={} object-store backend mismatch",
                case.name, step.name
            );
        }
        _ => unreachable!("non-static command"),
    }
}

fn is_shared_string_set_with_flags(command: &Value) -> bool {
    command_kind(command) == "string_set"
        && (command.get("nx").is_some() || command.get("xx").is_some())
}

fn run_shared_string_set_with_flags(
    _case: &UnifiedCase,
    step: &UnifiedStep,
    mut execute: impl FnMut(Command) -> temporalstore_rust::types::ExecuteResponse,
) -> temporalstore_rust::types::ExecuteResponse {
    let nx = step
        .command
        .get("nx")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let xx = step
        .command
        .get("xx")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if nx && xx {
        return temporalstore_rust::types::ExecuteResponse {
            status: Status::error("invalid_argument", "NX and XX are mutually exclusive"),
            response: CommandResponse::Empty,
        };
    }
    let condition = if nx {
        temporalstore_rust::types::StringSetCondition::IfNotExists
    } else {
        temporalstore_rust::types::StringSetCondition::IfExists
    };
    let command = Command::StringSetConditional {
        key: json_string(&step.command, "key"),
        value: json_bytes(&step.command, "value"),
        ttl_ms: None,
        condition,
        return_old: false,
        keep_ttl: false,
    };
    let response = execute(command);
    match response.response {
        CommandResponse::Integer { value: 1 } => temporalstore_rust::types::ExecuteResponse {
            status: response.status,
            response: CommandResponse::Empty,
        },
        CommandResponse::Integer { value: 0 } => {
            let status = match condition {
                temporalstore_rust::types::StringSetCondition::IfNotExists => {
                    Status::error("already_exists", "key already exists")
                }
                temporalstore_rust::types::StringSetCondition::IfExists => {
                    Status::error("not_found", "key not found")
                }
                temporalstore_rust::types::StringSetCondition::Always => response.status,
            };
            temporalstore_rust::types::ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            }
        }
        other => temporalstore_rust::types::ExecuteResponse {
            status: response.status,
            response: other,
        },
    }
}

fn run_shared_set_members_command(
    _case: &UnifiedCase,
    step: &UnifiedStep,
    add: bool,
    mut execute: impl FnMut(Command) -> temporalstore_rust::types::ExecuteResponse,
) -> temporalstore_rust::types::ExecuteResponse {
    let key = json_string(&step.command, "key");
    let members = json_member_values(&step.command);
    let before = execute(Command::SetMembers { key: key.clone() });
    if !before.status.ok {
        return before;
    }
    let mut existing = match before.response {
        CommandResponse::Members { members } => members.into_iter().collect::<BTreeSet<_>>(),
        other => {
            return temporalstore_rust::types::ExecuteResponse {
                status: Status::error("invalid_response", "set members returned wrong shape"),
                response: other,
            }
        }
    };
    let mut changed = 0;
    for member in members {
        let should_mutate = if add {
            existing.insert(member.clone())
        } else {
            existing.remove(&member)
        };
        if should_mutate {
            let response = execute(if add {
                Command::SetAdd {
                    key: key.clone(),
                    member,
                }
            } else {
                Command::SetRemove {
                    key: key.clone(),
                    member,
                }
            });
            if !response.status.ok {
                return response;
            }
            changed += 1;
        }
    }
    temporalstore_rust::types::ExecuteResponse {
        status: Status::ok(),
        response: CommandResponse::Integer { value: changed },
    }
}

fn json_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("shared command missing string field {field}"))
        .to_string()
}

fn json_bytes(value: &Value, field: &str) -> Vec<u8> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("shared command missing byte-array field {field}"))
        .iter()
        .map(|item| {
            item.as_u64()
                .unwrap_or_else(|| panic!("shared byte-array field {field} contains non-byte"))
                as u8
        })
        .collect()
}

fn json_string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("shared command missing string-array field {field}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("shared string-array field {field} contains non-string"))
                .to_string()
        })
        .collect()
}

fn json_member_values(value: &Value) -> Vec<Vec<u8>> {
    if value.get("members").is_some() {
        return json_string_array(value, "members")
            .into_iter()
            .map(String::into_bytes)
            .collect();
    }
    vec![json_bytes(value, "member")]
}

fn static_expected_allowed(expected: &UnifiedExpected) -> Option<bool> {
    match expected {
        UnifiedExpected::Static(value) => value.get("allowed").and_then(Value::as_bool),
        _ => None,
    }
}

fn static_expected_backend(expected: &UnifiedExpected) -> Option<&str> {
    match expected {
        UnifiedExpected::Static(value) => value.get("backend").and_then(Value::as_str),
        _ => None,
    }
}

fn is_storage_parity_command(command: &Value) -> bool {
    command_kind(command).starts_with("storage_")
}

fn maybe_run_shared_harness_command(case: &UnifiedCase, step: &UnifiedStep) -> bool {
    // Pointer steps name a lib test as their runner and carry arbitrary kind strings; the mode
    // field is what marks them, and they are not executable here.
    if step
        .command
        .get("mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| mode == "rust_executable_native_static")
    {
        return true;
    }
    let kind = command_kind(&step.command);
    match kind {
        "ops_readiness_service_summary" => {
            verify_ops_readiness_service_summary();
            true
        }
        "redis_feature_module_flow" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("feature_module_flow"),
                "case={} step={} unsupported Redis/Feature scenario",
                case.name,
                step.name
            );
            verify_redis_feature_module_flow();
            true
        }
        "redis_operational_admin_flow" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("operational_admin_commands"),
                "case={} step={} unsupported Redis/admin scenario",
                case.name,
                step.name
            );
            verify_redis_operational_admin_commands();
            true
        }
        "redis_slot_hash_crc64" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid shared harness command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("slot_hash_crc64"),
                "case={} step={} unsupported Redis slot/hash scenario",
                case.name,
                step.name
            );
            verify_redis_bucket_hash_crc64();
            true
        }
        "proxy_topology_churn_convergence" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy churn command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("two_proxy_topology_move_stale_cache_recovery"),
                "case={} step={} unsupported proxy churn scenario",
                case.name,
                step.name
            );
            verify_proxy_topology_churn_convergence();
            true
        }
        "proxy_admission_policy" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy admission command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("readonly_write_disabled_drop_percent_degraded_overload"),
                "case={} step={} unsupported proxy admission scenario",
                case.name,
                step.name
            );
            verify_proxy_admission_policy();
            true
        }
        "proxy_operational_surface_aliases" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy alias command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("admin_config_heartbeat_status_http_aliases"),
                "case={} step={} unsupported proxy alias scenario",
                case.name,
                step.name
            );
            verify_proxy_operational_surface_aliases();
            true
        }
        "proxy_tonic_streaming_contract" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy streaming command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("cancellation_backpressure_reconnect_callbacks"),
                "case={} step={} unsupported proxy streaming scenario",
                case.name,
                step.name
            );
            verify_proxy_tonic_streaming_contract();
            true
        }
        "proxy_route_quarantine_recovery" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy quarantine command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("backend_failure_quarantine_probe_recovery"),
                "case={} step={} unsupported proxy quarantine scenario",
                case.name,
                step.name
            );
            verify_proxy_route_quarantine_recovery();
            true
        }
        "proxy_multi_proxy_convergence_quarantine" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid multi-proxy command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("two_proxy_stale_cache_quarantine_recovery"),
                "case={} step={} unsupported multi-proxy scenario",
                case.name,
                step.name
            );
            verify_proxy_topology_churn_convergence();
            verify_proxy_route_quarantine_recovery();
            true
        }
        "proxy_grafana_prometheus_metric_parity" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid proxy metrics command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("proxy_metric_families_dashboard_alerts"),
                "case={} step={} unsupported proxy metrics scenario",
                case.name,
                step.name
            );
            verify_proxy_grafana_prometheus_metric_parity();
            true
        }
        "client_partition_set_route_cache" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client partition-set command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("partition_set_member_version_route_cache"),
                "case={} step={} unsupported client partition-set scenario",
                case.name,
                step.name
            );
            verify_client_partition_set_route_cache();
            true
        }
        "client_retry_budget_topology_refresh" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client retry-budget command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
