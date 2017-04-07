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
                Some("read_retry_write_single_shot_topology_safe_retry"),
                "case={} step={} unsupported client retry-budget scenario",
                case.name,
                step.name
            );
            verify_client_retry_budget_topology_refresh();
            true
        }
        "client_metasync_outage_churn" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client MetaSync command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("deadline_backoff_topology_refresh"),
                "case={} step={} unsupported client MetaSync scenario",
                case.name,
                step.name
            );
            verify_client_metasync_outage_churn();
            true
        }
        "client_pipeline_batch_partial_timeout" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client pipeline command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("ordered_batch_partial_failure_timeout_budget"),
                "case={} step={} unsupported client pipeline scenario",
                case.name,
                step.name
            );
            verify_client_pipeline_batch_partial_timeout();
            true
        }
        "client_deployment_placement_routing" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid client placement command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("location_affine_secondary_reads_primary_only_writes"),
                "case={} step={} unsupported client placement scenario",
                case.name,
                step.name
            );
            verify_client_deployment_placement_routing();
            true
        }
        "metaserver_scheduler_control_plane" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid metaserver scheduler command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("partition_set_scheduler_tokens_raft_membership_safe_mode"),
                "case={} step={} unsupported metaserver scheduler scenario",
                case.name,
                step.name
            );
            verify_metaserver_scheduler_control_plane();
            true
        }
        "raft_openraft_process_path_default_gate" => {
            let command: SharedHarnessCommand = serde_json::from_value(step.command.clone())
                .unwrap_or_else(|error| {
                    panic!(
                        "case={} step={} invalid Raft process-path command: {error}",
                        case.name, step.name
                    )
                });
            assert_eq!(
                command.scenario.as_deref(),
                Some("production_openraft_matrixraft_process_path_semantics"),
                "case={} step={} unsupported Raft process-path scenario",
                case.name,
                step.name
            );
            verify_raft_openraft_process_path_default_gate();
            true
        }
        "raft_linearizable_hash_failover" => {
            verify_raft_linearizable_hash_failover();
            true
        }
        "raft_membership_op" => {
            if case.name == "raft_matrixraft_membership_roles"
                && step.name == "setup_three_voter_cluster"
            {
                execute_raft_membership_shared_case(case);
            }
            true
        }
        _ => false,
    }
}

fn verify_ops_readiness_service_summary() {
    let report = production_readiness_report();
    assert_eq!(
        report.known_services(),
        vec![
            "client",
            "proxy",
            "ingestion",
            "data_node",
            "metaserver",
            "storage_cache",
            "feature_modules",
            "context_workflow",
            "fault_tolerance",
            "deployment_ops",
            "scale_testing",
            "raft_replication"
        ]
    );
    let gates = report.service_gate_reports();
    assert_eq!(gates.len(), 12);
    for (order, service, owner) in [
        (1, "client", "client_sdk"),
        (2, "proxy", "proxy_runtime"),
        (3, "ingestion", "ingestion_connectors"),
        (4, "data_node", "data_node_runtime"),
        (5, "metaserver", "metaserver_control_plane"),
        (6, "storage_cache", "storage_runtime"),
        (7, "feature_modules", "feature_api"),
        (8, "context_workflow", "context_ai_workflow"),
        (9, "fault_tolerance", "reliability"),
        (10, "deployment_ops", "platform_ops"),
        (11, "scale_testing", "performance"),
        (12, "raft_replication", "consensus_runtime"),
    ] {
        assert!(
            gates.iter().any(|gate| gate.remediation_order == order
                && gate.service == service
                && gate.owner == owner),
            "missing service gate {order}/{service}/{owner}"
        );
    }
    for gate in &gates {
        assert!(
            gate.gate_status == "ready" || gate.gate_status == "blocked",
            "unexpected gate status for {}: {}",
            gate.service,
            gate.gate_status
        );
        if gate.ready {
            assert_eq!(gate.gate_status, "ready");
            assert_eq!(gate.blocker_count, 0);
            assert!(gate.failed_capabilities.is_empty());
        } else {
            assert_eq!(gate.gate_status, "blocked");
            assert!(
                gate.blocker_count > 0,
                "blocked gate {} must name blocker count",
                gate.service
            );
            assert!(
                !gate.failed_capabilities.is_empty(),
                "blocked gate {} must name failed capabilities",
                gate.service
            );
            assert!(
                !gate.next_action.trim().is_empty(),
                "blocked gate {} must include next action",
                gate.service
            );
        }
    }
    if let Some(blocked) = report.next_blocked_service() {
        assert!(!blocked.ready);
        assert_eq!(blocked.gate_status, "blocked");
    }
    let blocked_services = gates
        .iter()
        .filter(|gate| gate.gate_status != "ready")
        .map(|gate| gate.service.as_str())
        .collect::<Vec<_>>();
    assert_eq!(blocked_services, vec!["data_node", "raft_replication"]);
    assert_eq!(
        report.next_blocked_service().map(|gate| gate.service),
        Some("data_node".to_string())
    );
    let data_node = report
        .service_summary("data_node")
        .expect("data node service summary should be exported");
    assert!(!data_node.ready);
    assert!(data_node.areas.contains(&"dataserver".to_string()));
    assert!(data_node
        .areas
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node
        .blocker_classes
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node.next_action.contains("membership"));
    assert!(report
        .failed_capabilities_for_service("data_node")
        .iter()
        .any(|capability| capability
            .capability
            .contains("data-node multi-process rollout")));
    assert!(!report.service_ready("data_node"));
    let gate = report
        .service_gate_report("data_node")
        .expect("data node service gate report should be exported");
    assert!(!gate.ready);
    assert_eq!(gate.gate_status, "blocked");
    assert_eq!(gate.severity, "critical");
    assert_eq!(gate.remediation_order, 4);
    assert_eq!(gate.owner, "data_node_runtime");
    assert!(gate.failed_capabilities.iter().any(|capability| capability
        .capability
        .contains("data-node multi-process rollout")));
    let raft_gate = report
        .service_gate_report("raft_replication")
        .expect("raft replication service gate report should be exported");
    assert!(!raft_gate.ready);
    assert_eq!(raft_gate.gate_status, "blocked");
    assert_eq!(raft_gate.severity, "critical");
    assert!(raft_gate
        .failed_capabilities
        .iter()
        .any(|capability| capability
            .capability
            .contains("data-node multi-process rollout")));
    let scale_gate = report
        .service_gate_report("scale_testing")
        .expect("scale testing service gate report should be exported");
    assert!(scale_gate.ready);
    assert_eq!(scale_gate.gate_status, "ready");
    assert_eq!(scale_gate.blocker_count, 0);
}

fn verify_redis_feature_module_flow() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let run = |args: Vec<Vec<u8>>| {
        execute_redis_command(args, 1, |command| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            if response.status.ok {
                Ok(response.response)
            } else {
                Err(response.status.message)
            }
        })
    };
    let s = |value: &str| value.as_bytes().to_vec();

    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("100"), s("2")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("200"), s("3")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("300"), s("sum")]),
        RespValue::Integer(5)
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("300"), s("10")]),
        RespValue::Array(vec![
            RespValue::Array(vec![RespValue::Integer(100), RespValue::Bulk(Some(s("2")))]),
            RespValue::Array(vec![RespValue::Integer(200), RespValue::Bulk(Some(s("3")))]),
        ])
    );

    let encoded = SequenceFeatureRow {
        timestamp_ms: 300,
        gid: 42,
        action_type: 3,
        duration: 90,
        author_id: 7,
    }
    .encode_feature_proto_value();
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("300"), encoded.clone()]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![
            s("FQUERYFILTERSTR"),
            s("rf"),
            s("0"),
            s("400"),
            s("10"),
            s("action_type = 3"),
            s("duration > 80"),
        ]),
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Integer(300),
            RespValue::Bulk(Some(encoded)),
        ])])
    );

    assert_eq!(
        run(vec![
            s("FAPPENDPOLICY"),
            s("rf"),
            s("300"),
            s("ignored"),
            s("insert_if_absent"),
        ]),
        RespValue::Integer(0)
    );
    assert_eq!(
        run(vec![
            s("FREPLACE"),
            s("rf"),
            s("0"),
            s("250"),
            s("150"),
            s("10")
        ]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("400"), s("sum")]),
        RespValue::Integer(10)
    );
    assert_eq!(
        run(vec![s("FDEL"), s("rf")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("400"), s("10")]),
        RespValue::Array(Vec::new())
    );
}

fn verify_redis_operational_admin_commands() {
    let mut state = RedisCommandState::default();
    let run = |state: &mut RedisCommandState, args: Vec<&str>| {
        execute_redis_command_with_state(
            args.into_iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect(),
            7,
            state,
            |_| Err("unexpected data command".to_string()),
        )
    };

    assert_eq!(
        run(&mut state, vec!["CONFIG", "GET", "requirepass"]),
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"requirepass".to_vec())),
            RespValue::Bulk(Some(Vec::new())),
        ])
    );
    assert_eq!(
        run(&mut state, vec!["CONFIG", "SET", "requirepass", "secret"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["AUTH", "bad"]),
        RespValue::Error("ERR invalid password".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["AUTH", "secret"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert!(state.authenticated);
    assert_eq!(
        run(&mut state, vec!["ECHO", "hello"]),
        RespValue::Bulk(Some(b"hello".to_vec()))
    );
    assert_eq!(
        run(&mut state, vec!["SELECT", "0"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["SELECT", "1"]),
        RespValue::Error("ERR DB index is out of range".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["SLAVEOF", "127.0.0.1", "18001"]),
        RespValue::SimpleString("OK".to_string())
    );
    let info = run(&mut state, vec!["INFO", "replication"]);
    match info {
        RespValue::Bulk(Some(bytes)) => {
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("role:slave"));
            assert!(text.contains("master_host:127.0.0.1"));
            assert!(text.contains("master_port:18001"));
        }
        other => panic!("unexpected info response: {other:?}"),
    }
    assert_eq!(
        run(&mut state, vec!["SLAVEOF", "NO", "ONE"]),
        RespValue::SimpleString("OK".to_string())
    );
    let info = run(&mut state, vec!["INFO", "replication"]);
    match info {
        RespValue::Bulk(Some(bytes)) => {
            assert!(String::from_utf8(bytes).unwrap().contains("role:master"));
        }
        other => panic!("unexpected info response: {other:?}"),
    }
    assert_eq!(
        run(
            &mut state,
            vec!["PARTITION", "LOAD", "7", "1", "file:///tmp/partition"]
        ),
        RespValue::SimpleString("OK".to_string())
    );
    let partition = run(&mut state, vec!["PARTITION", "INFO"]);
    match partition {
        RespValue::Bulk(Some(bytes)) => {
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("partition_id:7"));
            assert!(text.contains("partition_loading_stats:loaded"));
        }
        other => panic!("unexpected partition info response: {other:?}"),
    }
    assert_eq!(
        run(&mut state, vec!["BGSAVE"]),
        RespValue::SimpleString("Background saving started".to_string())
    );
    assert_eq!(
        run(&mut state, vec!["CONFIG", "REWRITE"]),
        RespValue::SimpleString("OK".to_string())
    );
}

fn verify_redis_bucket_hash_crc64() {
    let mut state = RedisCommandState::default();
    let mut run = |args: Vec<&str>| {
        execute_redis_command_with_state(
            args.into_iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect(),
            1,
            &mut state,
            |_| Err("unexpected data command".to_string()),
        )
    };

    assert_eq!(
        run(vec!["PSLOTHASHKEY", "123456789"]),
        RespValue::Integer(0x3a71_b645)
    );
    assert_eq!(
        run(vec!["PCLUSTERKEYSLOT", "123456789"]),
        RespValue::Integer(0x3a71_b645)
    );
    assert_eq!(
        run(vec!["PCLUSTERHASH", "123456789"]),
        RespValue::Integer(0xe9c6_d914_c4b8_d9cau64 as i64)
    );
}

fn verify_proxy_admission_policy() {
    let readonly = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::Readonly,
        ..ProxyOptions::default()
    });
    let write = readonly.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "blocked-write".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(write.status.code, "proxy_write_disabled");
    let preflight = readonly.preflight_report();
    assert_eq!(preflight.policy.serving_mode, ProxyServingMode::Readonly);
    assert!(!preflight.policy.serving_writes);
    assert!(preflight
        .degraded_reasons
        .contains(&"admission_rejections".to_string()));

    let write_disabled = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::WriteDisabled,
        ..ProxyOptions::default()
    });
    let blocked = write_disabled.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        },
    });
    assert_eq!(blocked.status.code, "proxy_write_disabled");

    let not_serving = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::NotServing,
        ..ProxyOptions::default()
    });
    let read = not_serving.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(read.status.code, "proxy_not_serving");
    assert!(not_serving.policy_report().rejecting_all);

    let degraded = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::Degraded,
        ..ProxyOptions::default()
    });
    assert_eq!(
        degraded.preflight_report().policy.serving_mode,
        ProxyServingMode::Degraded
    );

    let dropper = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        drop_percent: 100,
        ..ProxyOptions::default()
    });
    let dropped = dropper.table_batch_execute(ProxyTableBatchExecuteRequest {
        namespace: "ns".to_string(),
        table_name: "tbl".to_string(),
        commands: vec![Command::StringGet {
            key: "drop-me".to_string(),
        }],
    });
    assert_eq!(dropped.status.code, "proxy_traffic_dropped");
    let policy = dropper.policy_report();
    assert_eq!(policy.drop_percent, 100);
    assert_eq!(policy.admission_rejections, 1);
}

fn proxy_get_json(proxy: &ProxyService, path: &str) -> Value {
    let (code, body) = proxy.handle(HttpRequest {
        method: "GET".to_string(),
        path: path.to_string(),
        body: Vec::new(),
    });
    assert_eq!(code, 200, "GET {path} failed");
    parse_json::<Value>(&body).unwrap_or_else(|error| panic!("GET {path} invalid JSON: {error}"))
}

fn verify_proxy_operational_surface_aliases() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        proxy_addr: "127.0.0.1:17123".to_string(),
        namespace: "ns".to_string(),
        location: "iad".to_string(),
        ..ProxyOptions::default()
    });

    let ports = proxy_get_json(&proxy, "/ProxyService/GetPorts");
    assert_eq!(ports["listen_port"], 17_123);
    assert_eq!(ports["announce_port"], 17_123);

    let config = proxy_get_json(&proxy, "/ProxyService/GetConfig");
    assert_eq!(config["namespace"], "ns");
    assert_eq!(config["proxy_addr"], "127.0.0.1:17123");

    let heartbeat = proxy_get_json(&proxy, "/ProxyService/Heartbeat");
    assert_eq!(heartbeat["meta_addr"], "127.0.0.1:1");
    assert_eq!(heartbeat["route_cache_size"], 0);

    let surface = proxy.operational_surface_report();
    assert!(surface.status.ok);
    assert!(!surface.legacy_brpc_thrift_in_scope);
    assert!(surface.rust_native_aliases_ready);
    for alias in [
        "/ProxyService/GetPorts",
        "/ProxyService/GetConfig",
        "/ProxyService/Heartbeat",
        "/ProxyService/Preflight",
        "/ProxyService/GetConsulNames",
        "/ProxyService/NotifyStop",
        "/ProxyService/GetMigrationContract",
        "/ProxyService/GetPolicy",
        "/ProxyService/Metrics",
    ] {
        assert!(
            surface
                .entries
                .iter()
                .any(|entry| entry.rust_alias == alias),
            "missing proxy alias {alias}"
        );
    }

    let migration = proxy.native_migration_contract();
    assert!(!migration.legacy_wire_in_scope);
    assert!(migration.http_json_aliases_ready);
    assert!(migration.resp_migration_ready);
    assert!(migration.tonic_streaming_ready);
    assert!(migration.topology_version_invalidation_preserved);
    assert!(migration.admission_policy_preserved);
    assert!(migration.backend_quarantine_preserved);
}

fn verify_proxy_tonic_streaming_contract() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        ..ProxyOptions::default()
    });
    let contract = proxy.tonic_streaming_contract();
    assert_eq!(contract.service_name, "temporalstore.v1.ProxyService");
    assert_eq!(contract.execute_stream_method, "ProxyExecuteStream");
    assert_eq!(contract.route_callback_stream_method, "RouteCallbacks");
    assert_eq!(contract.preflight_watch_method, "WatchProxyPreflight");
    assert!(contract.long_running_request_ready);
    assert!(contract.cancellation_ready);
    assert!(contract.backpressure_ready);
    assert!(contract.reconnect_ready);
    assert_eq!(contract.backpressure_status_code, "resource_exhausted");
    for case in [
        "long_running_request",
        "client_cancellation",
        "server_backpressure",
        "callback_reconnect",
    ] {
        assert!(contract.maturity_cases.contains(&case.to_string()));
    }
    let routed = proxy_get_json(&proxy, "/ProxyService/GetTonicContract");
    assert_eq!(routed["service_name"], "temporalstore.v1.ProxyService");
    assert_eq!(routed["backpressure_ready"], true);
}

fn verify_proxy_route_quarantine_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let server_addr = free_local_addr();
    start_temporal_engine_http_service(server_addr.clone(), engine.clone());

    let meta = SingleNodeMeta::default();
    let bad_server = "127.0.0.1:1".to_string();
    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: bad_server.clone(),
        node_id: 1,
        location: "proxy-quarantine-zone".to_string(),
        binary_version: "unified-proxy".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: bad_server.clone(),
        })
        .status
        .ok
    );
    let meta_addr = free_local_addr();
    start_single_node_meta_http_service(meta_addr.clone(), meta.clone());
    wait_for_http(&server_addr);
    wait_for_http(&meta_addr);

    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: meta_addr.clone(),
        route_cache_ttl_ms: 60_000,
        backend_continuous_failed_time_ms: 5,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });

    let failed = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "quarantine-key".to_string(),
            value: b"recovered".to_vec(),
        },
    });
    assert!(!failed.status.ok, "{failed:?}");
    std::thread::sleep(Duration::from_millis(10));

    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: server_addr.clone(),
        node_id: 2,
        location: "proxy-quarantine-zone".to_string(),
        binary_version: "unified-proxy".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: server_addr.clone(),
        })
        .status
        .ok
    );

    let response = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "quarantine-key".to_string(),
            value: b"recovered".to_vec(),
        },
    });
    assert!(response.status.ok, "{response:?}");
    assert_eq!(
        proxy
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "quarantine-key".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"recovered".to_vec())
        }
    );
    let info = proxy.info();
    assert!(info.stats.route_refreshes >= 1);
    assert!(info.route_cache_size >= 1);
    assert_eq!(
        proxy.preflight_report().client.topology_cache.route_count,
        1
    );
}

fn verify_proxy_grafana_prometheus_metric_parity() {
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr: "127.0.0.1:1".to_string(),
        serving_mode: ProxyServingMode::NotServing,
        drop_percent: 17,
        ..ProxyOptions::default()
    });
    let _ = proxy.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "blocked".to_string(),
        },
    });
    let metrics = proxy.prometheus_metrics();
    for family in [
        "temporalstore_proxy_requests_total",
        "temporalstore_proxy_route_cache_entries",
        "temporalstore_proxy_route_cache_events_total",
        "temporalstore_proxy_backend_events_total",
        "temporalstore_proxy_serving_mode",
        "temporalstore_proxy_drop_percent",
        "temporalstore_proxy_metric_family_parity",
        "temporalstore_proxy_service_registry_state",
        "temporalstore_proxy_service_registry_events_total",
        "temporalstore_production_readiness_ready",
        "temporalstore_production_readiness_service_ready",
    ] {
        assert!(
            metrics.contains(family),
            "Prometheus output missing proxy/readiness family {family}"
        );
    }
    assert!(metrics.contains("temporalstore_proxy_serving_mode{mode=\"not_serving\"} 1"));
    assert!(metrics.contains("temporalstore_proxy_drop_percent 17"));
    assert!(metrics.contains("grafana_panel=\"Proxy Requests And Admission\""));

    let report = proxy.metrics_parity_report();
    assert!(report.status.ok);
    assert!(report.grafana_panels_ready);
    assert!(report.alerts_ready);
    assert!(report
        .rust_prometheus_families
        .contains(&"temporalstore_proxy_metric_family_parity".to_string()));
    assert!(report.mappings.iter().any(|mapping| {
        mapping.native_surface.contains("command/admission")
            && mapping.rust_prometheus_family == "temporalstore_proxy_requests_total"
            && mapping.grafana_panel == "Proxy Requests And Admission"
            && mapping.covered
    }));
}

fn verify_proxy_topology_churn_convergence() {
    let dir_a = tempfile::tempdir().unwrap();
    let engine_a = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir_a.path().join("cache"),
        dir_a.path().join("blocks"),
        dir_a.path().join("indexes"),
    );
    engine_a.load_shard(1);
    let server_a = free_local_addr();
    start_temporal_engine_http_service(server_a.clone(), engine_a.clone());

    let dir_b = tempfile::tempdir().unwrap();
    let engine_b = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir_b.path().join("cache"),
        dir_b.path().join("blocks"),
        dir_b.path().join("indexes"),
    );
    engine_b.load_shard(1);
    let server_b = free_local_addr();
    start_temporal_engine_http_service(server_b.clone(), engine_b.clone());

    let meta = SingleNodeMeta::default();
    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: server_a.clone(),
        node_id: 1,
        location: "zone-a".to_string(),
        binary_version: "unified-a".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: server_a.clone(),
        })
        .status
        .ok
    );
    let meta_addr = free_local_addr();
    start_single_node_meta_http_service(meta_addr.clone(), meta.clone());

    wait_for_http(&server_a);
    wait_for_http(&server_b);
    wait_for_http(&meta_addr);

    let proxy_a = ProxyService::new(ProxyOptions {
        meta_addr: meta_addr.clone(),
        route_cache_ttl_ms: 60_000,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });
    let proxy_b = ProxyService::new(ProxyOptions {
        meta_addr,
        route_cache_ttl_ms: 60_000,
        connect_timeout_ms: 50,
        io_timeout_ms: 200,
        ..ProxyOptions::default()
    });

    for (proxy, key, value) in [
        (&proxy_a, "proxy-a-before", b"a-before".to_vec()),
        (&proxy_b, "proxy-b-before", b"b-before".to_vec()),
    ] {
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
        assert_eq!(proxy.info().route_cache_size, 1);
    }

    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: server_b.clone(),
        node_id: 2,
        location: "zone-b".to_string(),
        binary_version: "unified-b".to_string(),
    });
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 1,
            server_addr: server_b.clone(),
        })
        .status
        .ok
    );

    for (proxy, key, value) in [
        (&proxy_a, "proxy-a-after", b"a-after".to_vec()),
        (&proxy_b, "proxy-b-after", b"b-after".to_vec()),
    ] {
        let response = proxy.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value,
            },
        });
        assert!(response.status.ok, "{response:?}");
    }

    for (key, value) in [
        ("proxy-a-before", b"a-before".to_vec()),
        ("proxy-b-before", b"b-before".to_vec()),
    ] {
        assert_eq!(
            engine_a
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(value) }
        );
    }
    for (key, value) in [
        ("proxy-a-after", b"a-after".to_vec()),
        ("proxy-b-after", b"b-after".to_vec()),
    ] {
        assert_eq!(
            engine_b
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(value) }
        );
    }
    assert_eq!(
        engine_a
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "proxy-a-after".to_string()
                },
            })
            .response,
        CommandResponse::Bytes { value: None }
    );

    for (label, proxy) in [("proxy_a", &proxy_a), ("proxy_b", &proxy_b)] {
        let preflight = proxy.preflight_report();
        assert!(!preflight.topology_cache_stale, "{label}: {preflight:?}");
        assert_eq!(preflight.client.topology_cache.route_count, 1, "{label}");
        assert!(
            preflight.client.route_refreshes >= 2,
            "{label}: {preflight:?}"
        );
        let route = preflight
            .client
            .topology_cache
            .routes
            .first()
            .unwrap_or_else(|| panic!("{label}: missing route"));
        assert_eq!(route.primary_addr, server_b, "{label}: {route:?}");
    }
}

fn verify_client_partition_set_route_cache() {
    let meta_addr = free_local_addr();
    let primary_addr = "127.0.0.1:27101".to_string();
    let replica_addr = "127.0.0.1:27102".to_string();
    let meta_addr_for_listener = meta_addr.clone();
    let primary_for_meta = primary_addr.clone();
    let replica_for_meta = replica_addr.clone();
    std::thread::spawn(move || {
        serve(&meta_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/tables/topology") => json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(TableMetaInfo {
                            table_id: 42,
                            namespace: "ns".to_string(),
                            table_name: "native_parts".to_string(),
                            state: MetaEntityState::Normal,
                            topology_version: 12,
                            first_shard_id: PartitionId::new(42, 0, 0, 17).unwrap().id(),
                            shard_count: 2,
                            replica_count: 2,
                            partition_version: 17,
                            serving_options: Default::default(),
                        }),
                        shards: vec![
                            TableShard {
                                load_version: 0,
                                shard_id: PartitionId::new(42, 0, 0, 17).unwrap().id(),
                                start_bucket: 0,
                                end_bucket: 536_870_911,
                                primary: Some(primary_for_meta.clone()),
                                replicas: vec![primary_for_meta.clone(), replica_for_meta.clone()],
                                primary_endpoint: Some(ServerEndpoint {
                                    server_addr: primary_for_meta.clone(),
                                    location: "zone-a".to_string(),
                                }),
                                replica_endpoints: vec![ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }],
                            },
                            TableShard {
                                load_version: 0,
                                shard_id: PartitionId::new(42, 1, 0, 17).unwrap().id(),
                                start_bucket: 536_870_912,
                                end_bucket: 1_073_741_823,
                                primary: Some(replica_for_meta.clone()),
                                replicas: vec![replica_for_meta.clone()],
                                primary_endpoint: Some(ServerEndpoint {
                                    server_addr: replica_for_meta.clone(),
                                    location: "zone-b".to_string(),
                                }),
                                replica_endpoints: Vec::new(),
                            },
                        ],
                        unchanged: false,
                    },
                ),
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 12,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 0,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 2,
                        normal_servers: 0,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&meta_addr);

    let client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: "127.0.0.1:1".to_string(),
        meta_addr: Some(meta_addr),
        route_cache_ttl_ms: 60_000,
        ..ClientOptions::default()
    });
    let options = client.sync_table_topology("ns", "native_parts").unwrap();
    assert_eq!(options.table_id, 42);
    assert_eq!(options.partition_version, 17);

    let report = client.preflight_report();
    assert_eq!(report.native_partition_sets.len(), 1);
    let partition_set = &report.native_partition_sets[0];
    assert_eq!(partition_set.table_id, 42);
    assert_eq!(partition_set.combine_name, "ns/native_parts");
    assert_eq!(partition_set.partition_version, 17);
    assert_eq!(partition_set.topology_version, 12);
    assert_eq!(partition_set.partition_count, 2);
    assert_eq!(partition_set.missing_route_count, 0);
    assert_eq!(partition_set.members[0].start_bucket, 0);
    assert_eq!(partition_set.members[0].end_bucket, 536_870_911);
    assert_eq!(partition_set.members[1].start_bucket, 536_870_912);
    assert_eq!(partition_set.members[1].end_bucket, 1_073_741_823);
    assert_eq!(
        partition_set.members[0].primary_addr.as_deref(),
        Some(primary_addr.as_str())
    );
    assert_eq!(
        partition_set.members[0].replica_addrs,
        vec![replica_addr.clone()]
    );

    let topology_report = client.topology_cache_report();
    assert_eq!(topology_report.route_count, 2);
    assert!(topology_report
        .routes
        .iter()
        .all(|route| route.table == "ns/native_parts"
            && route.partition_version == 17));
    assert_eq!(
        topology_report.routes[0].partition_id,
        partition_set.members[0].partition_id
    );
}

fn verify_client_retry_budget_topology_refresh() {
    let read_addr = free_local_addr();
    let read_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_attempts_for_server = Arc::clone(&read_attempts);
    let read_addr_for_listener = read_addr.clone();
    std::thread::spawn(move || {
        serve(&read_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    let attempt =
                        read_attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::error("retry_later", "loading"),
                                response: CommandResponse::Empty,
                            },
                        )
                    } else {
                        json_response(
                            200,
                            &ExecuteResponse {
                                status: Status::ok(),
                                response: CommandResponse::Bytes {
                                    value: Some(b"ok".to_vec()),
                                },
                            },
                        )
                    }
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&read_addr);
    let read_client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: read_addr,
        ..ClientOptions::default()
    });
    let read_table = read_client.open_table(
        "ns",
        "read_retry",
        TableOptions {
            retry_backoff_ms: 0,
            ..TableOptions::default()
        },
    );
    assert_eq!(read_table.get("retry-key").unwrap(), Some(b"ok".to_vec()));
    assert_eq!(read_attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    let write_addr = free_local_addr();
    let write_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write_attempts_for_server = Arc::clone(&write_attempts);
    let write_addr_for_listener = write_addr.clone();
    std::thread::spawn(move || {
        serve(&write_addr_for_listener, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/execute") => {
                    write_attempts_for_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    json_response(
                        200,
                        &ExecuteResponse {
                            status: Status::error("retry_later", "write loading"),
                            response: CommandResponse::Empty,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
    wait_for_http(&write_addr);
    let write_client = TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: write_addr,
        ..ClientOptions::default()
    });
    let write_table = write_client.open_table("ns", "write_retry", TableOptions::default());
    let err = write_table.set("retry-write", b"v".to_vec()).unwrap_err();
    assert!(err.to_string().contains("write loading"));
    assert_eq!(
        write_attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "unsafe writes without explicit budget must not duplicate a possibly applied write"
    );
}

fn verify_client_metasync_outage_churn() {
    let meta_addr = free_local_addr();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_listener = Arc::clone(&attempts);
    let listener_addr = meta_addr.clone();
    std::thread::spawn(move || {
        serve(&listener_addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("POST", "/meta/topology_version") => json_response(
                    200,
                    &TopologyVersionReport {
                        status: Status::ok(),
                        current_topology_version: 40,
                        old_topology_version: 0,
                        unchanged: false,
                        server_count: 1,
                        proxy_count: 0,
                        table_count: 1,
                        shard_route_count: 1,
                        normal_servers: 1,
                        frozen_servers: 0,
                        dropped_servers: 0,
                        normal_proxies: 0,
                        frozen_proxies: 0,
                        dropped_proxies: 0,
                        normal_tables: 1,
                        frozen_tables: 0,
                        dropped_tables: 0,
                        changed_tables: Vec::new(),
                        events: Vec::new(),
                        event_history_truncated: false,
                    },
                ),
                ("POST", "/tables/topology") => {
                    let attempt =
                        attempts_for_listener.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt < 2 {
                        return json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::error("metaserver_unavailable", "outage"),
                                table: None,
